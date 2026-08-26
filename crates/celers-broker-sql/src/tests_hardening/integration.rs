//! Live-database half of the claim/ack/reject regression suite, gated on
//! `CELERS_TEST_MYSQL_URL`.
//!
//! Split out of `tests_hardening.rs`, which exceeded the 2000-line limit.
//! The parent module keeps the deterministic, server-free tests (SQL text,
//! migration text, and the source-level guards); everything here needs a
//! real MySQL 8.0.1+ / MariaDB 10.6+ server and early-returns with a
//! visible `SKIPPED:` line naming the test when none is configured.

use super::TEST_URL_ENV;
use crate::row_ext::RowExt;
use crate::MysqlBroker;
use celers_core::{Broker, BrokerMessage, SerializedTask};
use oxisql_core::Connection;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

/// The connection string to test against, printing a visible, greppable
/// skip line naming `test_name` when it is not configured.
///
/// A bare early-return with no message here is exactly the defect this
/// suite exists to avoid: a skipped run and a real run both report `ok`,
/// so without this line the test count cannot tell them apart (see this
/// module's own doc comment).
///
/// The label is passed explicitly rather than derived. `#[track_caller]`
/// is the obvious alternative, but it is a no-op on an `async fn` caller
/// on stable Rust (rust-lang/rust#110011) and every caller here is async,
/// so it printed this helper's *own* `file:line` — the same two locations
/// for all 28 gated tests, which named none of them and could not
/// distinguish two skip sites inside one test (e.g.
/// `queues_are_isolated_from_each_other`, which opens two brokers). A
/// caller-supplied name prints what a reader actually needs, and matches
/// `tests::test_mysql_url`'s already-proven signature.
fn test_mysql_url(test_name: &str) -> Option<String> {
    match std::env::var(TEST_URL_ENV) {
        Ok(url) if !url.trim().is_empty() => Some(url),
        _ => {
            eprintln!("SKIPPED: {test_name} (set {TEST_URL_ENV} to run)");
            None
        }
    }
}

/// Build a broker on an isolated logical queue, or `None` when no test
/// server is configured.
async fn broker_on_fresh_queue(test_name: &str) -> Option<(MysqlBroker, String)> {
    let url = test_mysql_url(test_name)?;
    let queue = format!("test_{}", Uuid::new_v4().simple());
    let broker = MysqlBroker::with_queue(&url, &queue)
        .await
        .expect("connecting to CELERS_TEST_MYSQL_URL should succeed");
    broker.migrate().await.expect("migrations should apply");
    Some((broker, queue))
}

/// How many times [`claim_from`] re-issues a claim that came back empty,
/// and how long it waits in between.
const CLAIM_ATTEMPTS: usize = 50;
const CLAIM_RETRY_DELAY: Duration = Duration::from_millis(20);

/// Claim one task, retrying briefly while the claim comes back empty.
///
/// # History
///
/// The single-statement claim this crate used to issue took a next-key
/// lock on the index record *after* the range it scanned, and under
/// parallel test execution that neighbour usually belonged to another
/// test's queue. Measured on this suite's MySQL 8.0 via
/// `performance_schema.data_locks`: claiming from queue `zzprobe_a` held
/// `X` on the `idx_tasks_queue_dequeue` record of `zzprobe_b` *and*
/// `X,REC_NOT_GAP` on that row's primary key, so while such a claim was
/// open, `SKIP LOCKED` skipped the other queue's own pending row and
/// `dequeue()` returned `Ok(None)`.
///
/// That is fixed: the claim is now a non-locking candidate scan followed
/// by a primary-key-only locking read (see [`crate::sql_text`]), which
/// takes no gap locks at all, and
/// [`a_held_claim_on_one_queue_does_not_block_another_queue`] proves a
/// neighbouring queue claims on its *first* attempt with a claim held
/// open. This retry helper is deliberately kept anyway: same-queue
/// contention between a test and its own rival broker is still legitimate,
/// and tests whose subject is not claim concurrency should not fail on it.
/// It must never be used by a test that is *about* claim concurrency — a
/// 50-attempt loop would hide exactly the regression such a test exists to
/// catch.
async fn claim_from(broker: &MysqlBroker) -> BrokerMessage {
    for _ in 0..CLAIM_ATTEMPTS {
        let claimed = broker.dequeue().await.expect("dequeue should succeed");
        if let Some(message) = claimed {
            return message;
        }
        tokio::time::sleep(CLAIM_RETRY_DELAY).await;
    }
    panic!(
        "no task became claimable on queue {} within {CLAIM_ATTEMPTS} attempts",
        broker.queue_name()
    );
}

async fn broker_on_queue(test_name: &str, queue: &str) -> Option<MysqlBroker> {
    let url = test_mysql_url(test_name)?;
    Some(
        MysqlBroker::with_queue(&url, queue)
            .await
            .expect("connecting to CELERS_TEST_MYSQL_URL should succeed"),
    )
}

/// The headline regression: the dequeued task must carry the row's id, and
/// the subsequent ack must actually transition that row.
#[tokio::test]
async fn enqueue_dequeue_ack_round_trip_preserves_the_task_id() {
    let Some((broker, _queue)) =
        broker_on_fresh_queue("enqueue_dequeue_ack_round_trip_preserves_the_task_id").await
    else {
        return;
    };

    let task = SerializedTask::new("round_trip".to_string(), b"payload".to_vec())
        .with_priority(5)
        .with_max_retries(9)
        .with_timeout(45);
    let enqueued_id = broker.enqueue(task).await.expect("enqueue should succeed");

    let message = claim_from(&broker).await;

    assert_eq!(
        message.task.metadata.id, enqueued_id,
        "dequeue must return the persisted task id, not a fresh uuid"
    );
    assert_eq!(message.task.payload, b"payload".to_vec());
    assert_eq!(message.task.metadata.priority, 5);
    assert_eq!(message.task.metadata.max_retries, 9);
    assert_eq!(
        message.task.metadata.timeout_secs,
        Some(45),
        "the execution timeout must survive the round trip"
    );

    broker
        .ack(&message.task.metadata.id, message.receipt_handle.as_deref())
        .await
        .expect("ack should succeed");

    let info = broker
        .get_task(&enqueued_id)
        .await
        .expect("get_task should succeed")
        .expect("the acked task row must still exist");
    assert_eq!(info.state.to_string(), "completed");
}

/// Two brokers on the same database but different logical queues must not
/// see each other's tasks.
#[tokio::test]
async fn queues_are_isolated_from_each_other() {
    let Some((alpha, _alpha_queue)) =
        broker_on_fresh_queue("queues_are_isolated_from_each_other").await
    else {
        return;
    };
    let Some((beta, _beta_queue)) =
        broker_on_fresh_queue("queues_are_isolated_from_each_other").await
    else {
        return;
    };

    let task = SerializedTask::new("isolated".to_string(), b"x".to_vec());
    let task_id = alpha.enqueue(task).await.expect("enqueue should succeed");

    assert_eq!(beta.queue_size().await.expect("queue_size"), 0);
    assert_eq!(alpha.queue_size().await.expect("queue_size"), 1);
    assert!(
        beta.dequeue()
            .await
            .expect("dequeue should succeed")
            .is_none(),
        "a broker must never claim another logical queue's task"
    );

    let claimed = claim_from(&alpha).await;
    assert_eq!(claimed.task.metadata.id, task_id);
}

/// Two brokers whose logical queue names are *adjacent* in
/// `idx_tasks_queue_dequeue`'s order.
///
/// Adjacency is what makes the cross-queue claim test deterministic. The
/// bug it guards is a next-key lock on the index record immediately
/// *after* a claim's scan range, so the queue that gets starved is
/// whichever one sorts next — with independent `test_<uuid>` names that is
/// almost never the other broker under test, and the test would be a coin
/// flip. A shared `zzclaim_<uuid>_` prefix pins it: nothing else in
/// `celers_tasks` can sort between `..._a` and `..._b`, not even a
/// parallel run of the same test, whose own uuid moves its whole pair
/// elsewhere in the index.
async fn adjacent_queue_pair(test_name: &str) -> Option<(MysqlBroker, MysqlBroker)> {
    let url = test_mysql_url(test_name)?;
    let prefix = format!("zzclaim_{}", Uuid::new_v4().simple());
    let alpha = MysqlBroker::with_queue(&url, &format!("{prefix}_a"))
        .await
        .expect("connecting to CELERS_TEST_MYSQL_URL should succeed");
    alpha.migrate().await.expect("migrations should apply");
    let beta = MysqlBroker::with_queue(&url, &format!("{prefix}_b"))
        .await
        .expect("connecting to CELERS_TEST_MYSQL_URL should succeed");
    Some((alpha, beta))
}

/// Read the server's live lock set for `celers_tasks` and assert an open
/// claim holds a record lock on `held_id` and *nothing* on
/// `neighbour_queue`.
///
/// This is the mechanism half of the cross-queue proof: the behavioural
/// assertion beside it could in principle pass by luck of row layout,
/// whereas this reads the actual locks InnoDB took. The recorded failure
/// looked exactly like this (`X` — a next-key lock, note the absent
/// `REC_NOT_GAP` — on `idx_tasks_queue_dequeue`, `lock_data` naming the
/// *other* queue).
///
/// Best-effort by necessity: `performance_schema.data_locks` does not
/// exist on MariaDB 10.6+ (which this crate supports) and reading it needs
/// the `PROCESS` privilege a least-privilege test user typically lacks, so
/// an unavailable probe skips visibly instead of failing. The `held_id`
/// assertion is the probe's own self-check: without it an empty result set
/// would make the real assertion vacuous.
async fn assert_claim_locks_stay_off(
    prober: &MysqlBroker,
    held_id: &str,
    neighbour_queue: &str,
    test_name: &str,
) {
    let rows = match prober
        .connection()
        .query(
            "SELECT index_name, lock_mode, IFNULL(lock_data, '') AS lock_data \
             FROM performance_schema.data_locks \
             WHERE object_schema = DATABASE() AND object_name = 'celers_tasks'",
            &[],
        )
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            eprintln!(
                "SKIPPED (lock-set half of {test_name}): \
                 performance_schema.data_locks unavailable ({e})"
            );
            return;
        }
    };

    let mut saw_the_held_row = false;
    for row in rows.iter() {
        let lock_mode: String = row.col("lock_mode").expect("lock_mode should decode");
        let lock_data: String = row.col("lock_data").expect("lock_data should decode");
        let index_name: Option<String> = row.col("index_name").expect("index_name should decode");
        if lock_data.contains(held_id) {
            saw_the_held_row = true;
        }
        assert!(
            !lock_data.contains(neighbour_queue),
            "a claim on one queue locked {neighbour_queue}'s record \
             (index {index_name:?}, mode {lock_mode}, data {lock_data}); \
             that is the cross-queue next-key lock the two-step claim removed"
        );
    }
    assert!(
        saw_the_held_row,
        "the lock probe saw no lock on the claimed row {held_id}, so it \
         cannot have observed the claim's lock set at all"
    );
}

/// The production bug: a claim held open on one logical queue must not
/// starve a claim on a *different* queue.
///
/// Deliberately single-shot. `claim_from`'s 50-attempt retry loop would
/// hide exactly this regression, because the neighbouring claim becomes
/// possible the instant the holding transaction ends — so the assertion is
/// on beta's **first** `dequeue()`, with alpha's claim still open.
///
/// Measured against this suite's MySQL 8.0 before the fix: alpha's claim
/// held `X` on `idx_tasks_queue_dequeue`'s record for `zzprobe_b` and
/// beta's first claim returned zero rows. After the fix alpha holds a
/// single `PRIMARY ... X,REC_NOT_GAP` on its own row and beta claims
/// immediately.
#[tokio::test]
async fn a_held_claim_on_one_queue_does_not_block_another_queue() {
    const NAME: &str = "a_held_claim_on_one_queue_does_not_block_another_queue";
    let Some((alpha, beta)) = adjacent_queue_pair(NAME).await else {
        return;
    };

    alpha
        .enqueue(SerializedTask::new("alpha_task".to_string(), b"a".to_vec()))
        .await
        .expect("enqueue should succeed");
    let beta_id = beta
        .enqueue(SerializedTask::new("beta_task".to_string(), b"b".to_vec()))
        .await
        .expect("enqueue should succeed");

    // Hold a real claim open on alpha — the very statements `dequeue`
    // issues, in a transaction that is not committed until the end.
    let mut tx = alpha
        .connection()
        .transaction()
        .await
        .expect("beginning a transaction should succeed");
    let held = crate::broker_dequeue::claim_pending_rows(&mut *tx, alpha.queue_name(), None)
        .await
        .expect("alpha's claim should succeed");
    assert_eq!(held.len(), 1, "alpha must claim its own pending task");
    let held_id = held[0].0.to_string();

    assert_claim_locks_stay_off(&beta, &held_id, beta.queue_name(), NAME).await;

    let claimed = beta
        .dequeue()
        .await
        .expect("dequeue should succeed")
        .expect(
            "a claim held open on a neighbouring logical queue must not \
             starve this one — this is the cross-queue gap-lock regression",
        );
    assert_eq!(claimed.task.metadata.id, beta_id);

    tx.rollback().await.expect("rollback should succeed");
}

/// The same guarantee in the other direction, and for a *batch* claim: an
/// open batch claim on one queue must not stop the neighbour claiming.
#[tokio::test]
async fn a_held_batch_claim_does_not_block_a_neighbouring_queue() {
    const NAME: &str = "a_held_batch_claim_does_not_block_a_neighbouring_queue";
    let Some((alpha, beta)) = adjacent_queue_pair(NAME).await else {
        return;
    };

    for index in 0..5 {
        alpha
            .enqueue(SerializedTask::new(format!("alpha_{index}"), b"a".to_vec()))
            .await
            .expect("enqueue should succeed");
    }
    beta.enqueue(SerializedTask::new("beta_task".to_string(), b"b".to_vec()))
        .await
        .expect("enqueue should succeed");

    let mut tx = alpha
        .connection()
        .transaction()
        .await
        .expect("beginning a transaction should succeed");
    let held =
        crate::broker_dequeue::claim_pending_rows(&mut *tx, alpha.queue_name(), Some(5)).await;
    let held = held.expect("alpha's batch claim should succeed");
    assert_eq!(held.len(), 5, "alpha must claim its whole batch");

    let claimed = beta
        .dequeue()
        .await
        .expect("dequeue should succeed")
        .expect("an open batch claim on a neighbouring queue must not starve this one");
    assert_eq!(claimed.task.metadata.name, "beta_task");

    tx.rollback().await.expect("rollback should succeed");
}

/// Same-queue contention still behaves: N brokers racing over N tasks each
/// get exactly one, with no task claimed twice and none left behind.
///
/// The point is lock-order determinism. Two claims with overlapping
/// candidate windows lock by primary key, and InnoDB acquires those in
/// ascending primary-key order during the scan regardless of the candidate
/// list's own order, so there is no inversion to deadlock on. A run that
/// needed the deadlock-retry loop would still pass — but it would be
/// slower and would log warnings — so this asserts the *outcome* that
/// matters: every task claimed exactly once.
#[tokio::test]
async fn concurrent_claims_on_one_queue_partition_the_backlog() {
    const NAME: &str = "concurrent_claims_on_one_queue_partition_the_backlog";
    const WORKERS: usize = 4;
    let Some((broker, queue)) = broker_on_fresh_queue(NAME).await else {
        return;
    };

    let mut expected = Vec::with_capacity(WORKERS);
    for index in 0..WORKERS {
        expected.push(
            broker
                .enqueue(SerializedTask::new(format!("racer_{index}"), b"x".to_vec()))
                .await
                .expect("enqueue should succeed"),
        );
    }

    let mut rivals = Vec::with_capacity(WORKERS);
    for _ in 0..WORKERS {
        let Some(rival) = broker_on_queue(NAME, &queue).await else {
            return;
        };
        rivals.push(rival);
    }

    let claimed = futures::future::join_all(
        rivals
            .iter()
            .map(|rival| async move { rival.dequeue().await.expect("dequeue should succeed") }),
    )
    .await;

    let mut ids: Vec<_> = claimed
        .into_iter()
        .flatten()
        .map(|message| message.task.metadata.id)
        .collect();
    ids.sort();
    let unique = {
        let mut deduped = ids.clone();
        deduped.dedup();
        deduped.len()
    };
    assert_eq!(
        unique,
        ids.len(),
        "no task may be handed to two workers at once"
    );
    assert_eq!(
        ids.len(),
        WORKERS,
        "every racing worker must claim a task; a short claim here means \
         the candidate window starved under {WORKERS}-way contention"
    );
    expected.sort();
    assert_eq!(ids, expected, "the claims must cover exactly the backlog");
}

/// Known gaps #16: `celers-broker-sql` and `celers-backend-db` both
/// auto-migrate into whatever database they are pointed at, and a
/// deployment normally points them at the same one. They used to collide
/// on a shared `celers_task_results` table with incompatible schemas —
/// whichever migrated second died on `ERROR 1072 (42000): Key column
/// 'status' doesn't exist in table`.
///
/// This drives both migrations against one database, in both orders, and
/// then writes and reads a result through each crate's own store.
#[tokio::test]
async fn broker_and_result_backend_coexist_on_one_database() {
    use celers_backend_db::MysqlResultBackend;
    use celers_core::result::{ResultStore, TaskResultValue};

    const NAME: &str = "broker_and_result_backend_coexist_on_one_database";
    let Some(url) = test_mysql_url(NAME) else {
        return;
    };

    let queue = format!("coexist_{}", Uuid::new_v4().simple());
    let broker = MysqlBroker::with_queue(&url, &queue)
        .await
        .expect("connecting to CELERS_TEST_MYSQL_URL should succeed");
    broker
        .migrate()
        .await
        .expect("broker migrations must apply");

    let backend = MysqlResultBackend::new(&url)
        .await
        .expect("connecting the result backend should succeed");
    backend
        .migrate()
        .await
        .expect("the result backend must migrate onto the broker's database");

    // …and back the other way: re-running the broker's migrations with the
    // backend's `celers_task_results` already in place must not fail
    // either. This is the exact direction that produced ERROR 1072.
    broker
        .migrate()
        .await
        .expect("broker migrations must survive the backend's table existing");

    // Both tables are present, each with its own schema.
    for (table, column) in [
        ("celers_broker_results", "traceback"),
        ("celers_task_results", "result_state"),
    ] {
        let rows = broker
            .connection()
            .query(
                "SELECT 1 AS present FROM information_schema.columns \
                 WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?",
                &[&table, &column],
            )
            .await
            .expect("information_schema query should succeed");
        assert!(
            !rows.is_empty(),
            "{table} must exist with its own `{column}` column"
        );
    }

    // A write/read round trip through the broker's own result store.
    let broker_task = Uuid::new_v4();
    broker
        .store_result(
            &broker_task,
            "coexist_broker",
            crate::TaskResultStatus::Success,
            Some(serde_json::json!({"via": "broker"})),
            None,
            None,
            Some(7),
        )
        .await
        .expect("the broker must store its own result");
    let stored = broker
        .get_result(&broker_task)
        .await
        .expect("the broker must read its own result")
        .expect("the result must be there");
    assert_eq!(stored.result, Some(serde_json::json!({"via": "broker"})));
    assert_eq!(stored.runtime_ms, Some(7));

    // …and one through the result backend, on the same database.
    let backend_task = Uuid::new_v4();
    backend
        .store_result(
            backend_task,
            TaskResultValue::Success(serde_json::json!({"via": "backend"})),
        )
        .await
        .expect("the result backend must store its result");
    let round_tripped = backend
        .get_result(backend_task)
        .await
        .expect("the result backend must read its result")
        .expect("the result must be there");
    assert!(
        matches!(
            round_tripped,
            TaskResultValue::Success(ref value) if *value == serde_json::json!({"via": "backend"})
        ),
        "the backend read back {round_tripped:?}"
    );

    // Neither store can see the other's row: they are separate tables.
    assert!(
        broker
            .get_result(&backend_task)
            .await
            .expect("get_result should succeed")
            .is_none(),
        "the broker's store must not read the result backend's rows"
    );

    broker
        .delete_result(&broker_task)
        .await
        .expect("cleanup should succeed");
    backend
        .forget(backend_task)
        .await
        .expect("cleanup should succeed");
}

/// A batch claim larger than the candidate window's headroom cap must
/// still return the whole batch.
///
/// The window `dequeue_candidate_sql` uses is `limit * 4`, bounded by
/// `CLAIM_CANDIDATE_MAX` (512). Applying that bound to the *request*
/// rather than to the extra headroom would silently cap every
/// `dequeue_batch(n > 512)` at 512 — a claim quietly returning fewer tasks
/// than an idle queue can supply. The size here is deliberately just over
/// the cap.
#[tokio::test]
async fn a_batch_claim_larger_than_the_candidate_cap_returns_everything() {
    const NAME: &str = "a_batch_claim_larger_than_the_candidate_cap_returns_everything";
    const BATCH: usize = 520;
    let Some((broker, queue)) = broker_on_fresh_queue(NAME).await else {
        return;
    };

    let tasks: Vec<SerializedTask> = (0..BATCH)
        .map(|index| SerializedTask::new(format!("bulk_{index}"), b"x".to_vec()))
        .collect();
    let enqueued = broker
        .enqueue_batch_impl(tasks)
        .await
        .expect("enqueue_batch should succeed");
    assert_eq!(enqueued.len(), BATCH);

    let claimed = broker
        .dequeue_batch_impl(BATCH)
        .await
        .expect("dequeue_batch should succeed");
    assert_eq!(
        claimed.len(),
        BATCH,
        "a batch claim above the candidate headroom cap must not be truncated"
    );

    let mut claimed_ids: Vec<_> = claimed
        .into_iter()
        .map(|message| message.task.metadata.id)
        .collect();
    claimed_ids.sort();
    let mut expected = enqueued;
    expected.sort();
    assert_eq!(claimed_ids, expected, "no task may be claimed twice");

    // Isolation does not depend on this — the queue name is never reused
    // — but this test writes 520 rows per run, so it drops them rather
    // than growing the shared table without bound.
    broker
        .connection()
        .execute("DELETE FROM celers_tasks WHERE queue_name = ?", &[&queue])
        .await
        .expect("cleanup of the test queue should succeed");
}

/// Every text column of a stored result must read back.
///
/// MySQL sends `TEXT`/`LONGTEXT` over the same wire type as `BLOB`, so
/// `oxisql-mysql` hands them back as `Value::Blob`, which
/// `col::<Option<String>>` rejects. The failure only showed on a column
/// that actually held a value — a `NULL` decoded fine — so `store_result`
/// followed by `get_result` failed with `type mismatch: expected Text, got
/// Blob` for every result that had one, while a result with none looked
/// healthy. This populates `result`, `error` *and* `traceback` so the
/// round trip cannot pass by being empty.
#[tokio::test]
async fn a_stored_result_reads_back_through_every_text_column() {
    const NAME: &str = "a_stored_result_reads_back_through_every_text_column";
    let Some((broker, _queue)) = broker_on_fresh_queue(NAME).await else {
        return;
    };

    let task_id = Uuid::new_v4();
    broker
        .store_result(
            &task_id,
            "text_columns",
            crate::TaskResultStatus::Failure,
            Some(serde_json::json!({"partial": [1, 2, 3]})),
            Some("boom"),
            Some("Traceback (most recent call last):\n  ..."),
            Some(1234),
        )
        .await
        .expect("store_result should succeed");

    let stored = broker
        .get_result(&task_id)
        .await
        .expect("get_result must decode every text column")
        .expect("the stored result must be there");
    assert_eq!(stored.task_id, task_id);
    assert_eq!(stored.task_name, "text_columns");
    assert_eq!(stored.status, crate::TaskResultStatus::Failure);
    assert_eq!(
        stored.result,
        Some(serde_json::json!({"partial": [1, 2, 3]}))
    );
    assert_eq!(stored.error.as_deref(), Some("boom"));
    assert!(stored
        .traceback
        .as_deref()
        .is_some_and(|t| t.starts_with("Traceback")));
    assert_eq!(stored.runtime_ms, Some(1234));

    // The batch read is a second, independent decoder over the same
    // columns, and carried the same defect — plus a non-optional `String`
    // for `result`, which failed outright on a stored `NULL`.
    let batched_id = Uuid::new_v4();
    broker
        .store_result_batch(&[crate::BatchResultInput {
            task_id: batched_id,
            task_name: "batched_text_columns".to_string(),
            status: crate::TaskResultStatus::Success,
            result: None,
            error: None,
            traceback: None,
            runtime_ms: None,
        }])
        .await
        .expect("store_result_batch should succeed");

    let mut fetched = broker
        .get_result_batch(&[task_id, batched_id])
        .await
        .expect("get_result_batch must decode every text column");
    fetched.sort_by(|a, b| a.task_name.cmp(&b.task_name));
    assert_eq!(fetched.len(), 2, "both results must come back");
    assert_eq!(fetched[0].task_name, "batched_text_columns");
    // `store_result_batch` writes JSON `null` for a missing result rather
    // than SQL `NULL`, which its own comment documents as deliberate ("a
    // *missing* result legitimately stores JSON `null`; a result that fails
    // to serialize must not"). `store_result`'s single-row path stores SQL
    // `NULL` instead and reads back as `None`. Whether that asymmetry between
    // the two store paths is wanted is an open question, recorded as a
    // followup rather than decided here — this assertion only pins today's
    // behaviour. What it is really guarding is that the batch read *decodes
    // at all*: it previously took `result` as a non-optional `String`, so a
    // row holding SQL `NULL` failed the whole batch.
    assert_eq!(
        fetched[0].result,
        Some(serde_json::Value::Null),
        "a batch-stored missing result reads back as JSON null"
    );
    assert_eq!(fetched[1].error.as_deref(), Some("boom"));
    assert_eq!(
        fetched[1].result,
        Some(serde_json::json!({"partial": [1, 2, 3]}))
    );

    broker
        .delete_result(&task_id)
        .await
        .expect("cleanup should succeed");
    broker
        .delete_result(&batched_id)
        .await
        .expect("cleanup should succeed");
}

/// A broker reconnected to the same queue name sees the same backlog —
/// the queue label is persisted, not per-instance state.
#[tokio::test]
async fn queue_membership_survives_reconnection() {
    let Some((broker, queue)) =
        broker_on_fresh_queue("queue_membership_survives_reconnection").await
    else {
        return;
    };
    let task = SerializedTask::new("persisted_queue".to_string(), b"x".to_vec());
    broker.enqueue(task).await.expect("enqueue should succeed");

    let Some(reconnected) = broker_on_queue("queue_membership_survives_reconnection", &queue).await
    else {
        return;
    };
    assert_eq!(reconnected.queue_size().await.expect("queue_size"), 1);
    let reclaimed = claim_from(&reconnected).await;
    assert_eq!(reclaimed.task.metadata.name, "persisted_queue");
}

/// `max_retries` large enough to overflow the old `2_i64.pow` backoff.
#[tokio::test]
async fn reject_with_a_huge_retry_budget_does_not_panic() {
    let Some((broker, _queue)) =
        broker_on_fresh_queue("reject_with_a_huge_retry_budget_does_not_panic").await
    else {
        return;
    };

    let task =
        SerializedTask::new("huge_retry_budget".to_string(), b"x".to_vec()).with_max_retries(1000);
    broker.enqueue(task).await.expect("enqueue should succeed");

    let message = claim_from(&broker).await;

    broker
        .reject(
            &message.task.metadata.id,
            message.receipt_handle.as_deref(),
            true,
        )
        .await
        .expect("reject must not panic or fail for a large retry budget");

    let info = broker
        .get_task(&message.task.metadata.id)
        .await
        .expect("get_task")
        .expect("rejected task must be requeued, not dropped");
    assert_eq!(info.state.to_string(), "pending");
}

/// Ack and reject hooks used to be accepted and never run.
#[tokio::test]
async fn ack_hooks_actually_fire() {
    let Some((broker, _queue)) = broker_on_fresh_queue("ack_hooks_actually_fire").await else {
        return;
    };

    let before = Arc::new(AtomicUsize::new(0));
    let after = Arc::new(AtomicUsize::new(0));
    let dequeued = Arc::new(AtomicUsize::new(0));

    let before_counter = Arc::clone(&before);
    broker
        .add_hook(crate::TaskHook::BeforeAck(Arc::new(move |_ctx, _task| {
            let counter = Arc::clone(&before_counter);
            Box::pin(async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        })))
        .await;

    let after_counter = Arc::clone(&after);
    broker
        .add_hook(crate::TaskHook::AfterAck(Arc::new(move |_ctx, _task| {
            let counter = Arc::clone(&after_counter);
            Box::pin(async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        })))
        .await;

    let dequeue_counter = Arc::clone(&dequeued);
    broker
        .add_hook(crate::TaskHook::AfterDequeue(Arc::new(
            move |_ctx, _task| {
                let counter = Arc::clone(&dequeue_counter);
                Box::pin(async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            },
        )))
        .await;

    let task = SerializedTask::new("hooked".to_string(), b"x".to_vec());
    broker.enqueue(task).await.expect("enqueue should succeed");
    let message = claim_from(&broker).await;
    broker
        .ack(&message.task.metadata.id, message.receipt_handle.as_deref())
        .await
        .expect("ack should succeed");

    assert_eq!(dequeued.load(Ordering::SeqCst), 1, "AfterDequeue must fire");
    assert_eq!(before.load(Ordering::SeqCst), 1, "BeforeAck must fire");
    assert_eq!(after.load(Ordering::SeqCst), 1, "AfterAck must fire");
}

/// A `BeforeDequeue` hook that errors must leave the task claimable.
#[tokio::test]
async fn before_dequeue_hook_error_rolls_the_claim_back() {
    let Some((broker, _queue)) =
        broker_on_fresh_queue("before_dequeue_hook_error_rolls_the_claim_back").await
    else {
        return;
    };

    let task = SerializedTask::new("vetoed".to_string(), b"x".to_vec());
    let task_id = broker.enqueue(task).await.expect("enqueue should succeed");

    broker
        .add_hook(crate::TaskHook::BeforeDequeue(Arc::new(|_ctx, _task| {
            Box::pin(async move { Err(celers_core::CelersError::Other("vetoed".to_string())) })
        })))
        .await;

    // Retried like every other claim in this module: a claim starved by
    // a neighbouring queue's lock returns `Ok(None)` without ever
    // reaching the hook, which would read here as "the hook did not
    // veto". See `claim_from` for the measured mechanism.
    let mut vetoed = false;
    for _ in 0..CLAIM_ATTEMPTS {
        if broker.dequeue().await.is_err() {
            vetoed = true;
            break;
        }
        tokio::time::sleep(CLAIM_RETRY_DELAY).await;
    }
    assert!(
        vetoed,
        "a vetoing BeforeDequeue hook must surface its error"
    );

    broker.clear_hooks().await;
    let info = broker
        .get_task(&task_id)
        .await
        .expect("get_task")
        .expect("the vetoed task must still exist");
    assert_eq!(
        info.state.to_string(),
        "pending",
        "a vetoed claim must be rolled back, leaving the task pending"
    );
    assert_eq!(info.retry_count, 0, "a vetoed claim must not burn a retry");
}

/// `with_transaction` used to drop the transaction without committing, so
/// every write inside was silently rolled back.
#[tokio::test]
async fn with_transaction_commits_its_writes() {
    let Some((broker, _queue)) = broker_on_fresh_queue("with_transaction_commits_its_writes").await
    else {
        return;
    };

    let task = SerializedTask::new("committed".to_string(), b"x".to_vec());
    let task_id = broker.enqueue(task).await.expect("enqueue should succeed");
    let id_text = task_id.to_string();

    broker
        .with_transaction(move |tx| {
            let id_text = id_text.clone();
            Box::pin(async move {
                tx.execute(
                    "UPDATE celers_tasks SET priority = 77 WHERE id = ?",
                    &[&id_text],
                )
                .await
                .map_err(|e| celers_core::CelersError::Other(e.to_string()))?;
                Ok(())
            })
        })
        .await
        .expect("with_transaction should succeed");

    let info = broker
        .get_task(&task_id)
        .await
        .expect("get_task")
        .expect("task must exist");
    assert_eq!(
        info.priority, 77,
        "with_transaction must commit the callback's writes"
    );
}

/// A failing callback must roll back rather than partially commit.
#[tokio::test]
async fn with_transaction_rolls_back_on_error() {
    let Some((broker, _queue)) =
        broker_on_fresh_queue("with_transaction_rolls_back_on_error").await
    else {
        return;
    };

    let task = SerializedTask::new("rolled_back".to_string(), b"x".to_vec());
    let task_id = broker.enqueue(task).await.expect("enqueue should succeed");
    let id_text = task_id.to_string();

    let outcome: celers_core::Result<()> = broker
        .with_transaction(move |tx| {
            let id_text = id_text.clone();
            Box::pin(async move {
                tx.execute(
                    "UPDATE celers_tasks SET priority = 55 WHERE id = ?",
                    &[&id_text],
                )
                .await
                .map_err(|e| celers_core::CelersError::Other(e.to_string()))?;
                Err(celers_core::CelersError::Other("deliberate".to_string()))
            })
        })
        .await;

    assert!(outcome.is_err());
    let info = broker
        .get_task(&task_id)
        .await
        .expect("get_task")
        .expect("task must exist");
    assert_ne!(info.priority, 55, "a failed callback must roll back");
}

/// Count the tasks named `task_name` that are sitting in `celers_tasks`.
///
/// `process_recurring_tasks` enqueues through `self.enqueue`, so the rows
/// it writes land in the calling broker's own queue — but the *name* is
/// what identifies them as this test's, and a uuid-suffixed name cannot
/// collide with anything another run left behind.
async fn tasks_named(broker: &MysqlBroker, task_name: &str) -> i64 {
    let rows = broker
        .connection()
        .query(
            "SELECT COUNT(*) AS c FROM celers_tasks WHERE task_name = ?",
            &[&task_name],
        )
        .await
        .expect("counting enqueued rows should succeed");
    rows.first()
        .map(|row| row.col::<i64>("c"))
        .transpose()
        .expect("the count column must decode")
        .unwrap_or(0)
}

/// Two concurrent schedulers must enqueue a due recurring task once, not
/// twice.
///
/// # Why this counts rows instead of return values
///
/// `process_recurring_tasks` scans **every** `__recurring__%` row in the
/// database (`celers_broker_results` is not queue-scoped) and returns how
/// many of them *it* claimed, so its return value counts every other
/// run's leftover configuration as well as this test's own. Registrations
/// are durable by design and nothing sweeps them, so on a database that
/// has served this suite more than once the old `first + second == 1`
/// assertion fails on arithmetic that has nothing to do with the property
/// under test: a live MySQL carrying 39 leftover due configurations
/// returned `20 + 19`, which is 39 configurations claimed exactly once
/// each — the correct behaviour — reported as a failure.
///
/// The property is "*this* configuration is enqueued exactly once", so
/// this counts the rows carrying this test's uuid-suffixed task name.
/// That is strictly stronger than the sum: a double claim would show up
/// as 2 no matter how many neighbours are due, and no number of
/// neighbours can make it anything but 1 when the claim works.
#[tokio::test]
async fn recurring_tasks_are_claimed_exactly_once() {
    use crate::{RecurringSchedule, RecurringTaskConfig};

    let Some((broker, queue)) =
        broker_on_fresh_queue("recurring_tasks_are_claimed_exactly_once").await
    else {
        return;
    };
    let Some(rival) = broker_on_queue("recurring_tasks_are_claimed_exactly_once", &queue).await
    else {
        return;
    };

    let task_name = format!("recurring_{}", Uuid::new_v4().simple());
    let config = RecurringTaskConfig {
        task_name: task_name.clone(),
        schedule: RecurringSchedule::EverySeconds(3600),
        payload: b"x".to_vec(),
        priority: 0,
        enabled: true,
        last_run: None,
        next_run: chrono::Utc::now() - chrono::Duration::seconds(60),
    };
    let config_id = broker
        .register_recurring_task(config)
        .await
        .expect("registering a recurring task should succeed");

    let (first, second) = tokio::join!(
        broker.process_recurring_tasks(),
        rival.process_recurring_tasks()
    );
    let first = first.expect("process_recurring_tasks should succeed");
    let second = second.expect("process_recurring_tasks should succeed");

    let enqueued = tasks_named(&broker, &task_name).await;

    // Clean up before asserting, so a failure does not also leak this
    // test's configuration into the next run the way the rows that
    // exposed this defect were leaked.
    broker
        .delete_recurring_task(&config_id)
        .await
        .expect("deleting this test's recurring configuration should succeed");
    broker
        .connection()
        .execute(
            "DELETE FROM celers_tasks WHERE task_name = ?",
            &[&task_name],
        )
        .await
        .expect("deleting this test's enqueued rows should succeed");

    assert_eq!(
        enqueued, 1,
        "a due recurring task must be enqueued by exactly one scheduler, \
         got {enqueued} rows named {task_name} (the two schedulers claimed \
         {first} and {second} configurations in total, this test's included)"
    );
}

/// `purge_terminal_tasks` must delete only terminal (acked) rows, never a
/// still-pending one — and the nested-derived-table `DELETE` built by
/// `sql_text::purge_terminal_tasks_sql` must actually parse and execute
/// on a live server (its two MySQL-specific workarounds, for `ERROR
/// 1093` and `ERROR 1235`, are untestable without one).
#[tokio::test]
async fn retention_purges_only_terminal_tasks() {
    let Some((broker, _queue)) =
        broker_on_fresh_queue("retention_purges_only_terminal_tasks").await
    else {
        return;
    };

    let completed_id = broker
        .enqueue(SerializedTask::new("done".to_string(), b"x".to_vec()))
        .await
        .expect("enqueue should succeed");
    let msg = claim_from(&broker).await;
    broker
        .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
        .await
        .expect("ack should succeed");

    let pending_id = broker
        .enqueue(SerializedTask::new(
            "still_waiting".to_string(),
            b"x".to_vec(),
        ))
        .await
        .expect("enqueue should succeed");

    // Zero retention: everything terminal is eligible immediately, so
    // the assertion needs no sleeping.
    let deleted = broker
        .purge_terminal_tasks(std::time::Duration::from_secs(0), 100, 5)
        .await
        .expect("purge_terminal_tasks should succeed");
    assert_eq!(deleted, 1, "exactly the one acked task must be purged");

    assert!(
        broker
            .get_task(&completed_id)
            .await
            .expect("get_task should succeed")
            .is_none(),
        "the completed task must be gone"
    );
    assert!(
        broker
            .get_task(&pending_id)
            .await
            .expect("get_task should succeed")
            .is_some(),
        "the still-pending task must survive"
    );
}

/// A broker's purge must never delete another logical queue's terminal
/// tasks, matching every other claim/read path's queue isolation.
#[tokio::test]
async fn retention_purge_is_scoped_to_the_owning_queue() {
    let Some((alpha, _alpha_queue)) =
        broker_on_fresh_queue("retention_purge_is_scoped_to_the_owning_queue").await
    else {
        return;
    };
    let Some((beta, _beta_queue)) =
        broker_on_fresh_queue("retention_purge_is_scoped_to_the_owning_queue").await
    else {
        return;
    };

    let task = SerializedTask::new("alpha_done".to_string(), b"x".to_vec());
    alpha.enqueue(task).await.expect("enqueue should succeed");
    let msg = claim_from(&alpha).await;
    alpha
        .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
        .await
        .expect("ack should succeed");

    let deleted = beta
        .purge_terminal_tasks(std::time::Duration::from_secs(0), 100, 5)
        .await
        .expect("purge_terminal_tasks should succeed");
    assert_eq!(
        deleted, 0,
        "beta must not purge alpha's terminal tasks from a different queue"
    );

    assert_eq!(
        alpha
            .get_task(&msg.task.metadata.id)
            .await
            .expect("get_task should succeed")
            .expect("alpha's completed task must be untouched")
            .state
            .to_string(),
        "completed"
    );
}

/// Two brokers on different logical queues must not dedupe against each
/// other: an identical `dedup_key` must not make the second broker's
/// `enqueue_deduplicated` hand back the first broker's task id — that id
/// is invisible to the second broker's queue-scoped `dequeue`, so doing
/// so would silently lose the second task.
#[tokio::test]
async fn dedup_does_not_cross_queue_boundaries() {
    let Some((alpha, _alpha_queue)) =
        broker_on_fresh_queue("dedup_does_not_cross_queue_boundaries").await
    else {
        return;
    };
    let Some((beta, _beta_queue)) =
        broker_on_fresh_queue("dedup_does_not_cross_queue_boundaries").await
    else {
        return;
    };

    let dedup_key = format!("shared_key_{}", Uuid::new_v4().simple());
    let alpha_task = SerializedTask::new("alpha_job".to_string(), b"a".to_vec());
    let beta_task = SerializedTask::new("beta_job".to_string(), b"b".to_vec());

    let alpha_id = alpha
        .enqueue_deduplicated(alpha_task, &dedup_key)
        .await
        .expect("alpha's enqueue_deduplicated should succeed");
    let beta_id = beta
        .enqueue_deduplicated(beta_task, &dedup_key)
        .await
        .expect("beta's enqueue_deduplicated should succeed");

    assert_ne!(
        alpha_id, beta_id,
        "the same dedup_key on two different queues must not collide"
    );
    assert_eq!(
        alpha.queue_size().await.expect("queue_size"),
        1,
        "alpha's own task must have been inserted, not skipped"
    );
    assert_eq!(
        beta.queue_size().await.expect("queue_size"),
        1,
        "beta's own task must have been inserted, not skipped"
    );
}

/// The same `dedup_key` submitted twice on the *same* queue must still
/// dedupe — the queue predicate narrows the match, it must not disable
/// deduplication altogether.
#[tokio::test]
async fn dedup_still_applies_within_the_same_queue() {
    let Some((broker, _queue)) =
        broker_on_fresh_queue("dedup_still_applies_within_the_same_queue").await
    else {
        return;
    };

    let dedup_key = format!("same_queue_key_{}", Uuid::new_v4().simple());
    let first_id = broker
        .enqueue_deduplicated(
            SerializedTask::new("first".to_string(), b"x".to_vec()),
            &dedup_key,
        )
        .await
        .expect("first enqueue_deduplicated should succeed");
    let second_id = broker
        .enqueue_deduplicated(
            SerializedTask::new("second".to_string(), b"y".to_vec()),
            &dedup_key,
        )
        .await
        .expect("second enqueue_deduplicated should succeed");

    assert_eq!(
        first_id, second_id,
        "a repeated dedup_key on the same queue must return the existing task id"
    );
    assert_eq!(
        broker.queue_size().await.expect("queue_size"),
        1,
        "the second call must not have inserted a duplicate row"
    );
}

// ========== Revocation (idx 1: durable revoked-task set, polled) ==========

#[tokio::test]
async fn revoke_removes_a_pending_task_and_is_revoked_reflects_it() {
    let Some((broker, _queue)) =
        broker_on_fresh_queue("revoke_removes_a_pending_task_and_is_revoked_reflects_it").await
    else {
        return;
    };

    let task = SerializedTask::new("revoke_pending".to_string(), vec![]);
    let task_id = broker.enqueue(task).await.expect("enqueue");

    assert!(
        !broker.is_revoked(&task_id).await.expect("is_revoked"),
        "a freshly enqueued task must not already be revoked"
    );

    let recorded = broker.revoke(&task_id, false).await.expect("revoke");
    assert!(recorded, "revoke() must report the revocation as recorded");

    assert!(
        broker.is_revoked(&task_id).await.expect("is_revoked"),
        "revoke() must be durably visible through is_revoked()"
    );

    assert!(
        broker.dequeue().await.expect("dequeue").is_none(),
        "a revoked pending task must not be claimable"
    );
}

#[tokio::test]
async fn revoke_is_observed_by_the_poller_within_one_interval() {
    let Some((broker, _queue)) =
        broker_on_fresh_queue("revoke_is_observed_by_the_poller_within_one_interval").await
    else {
        return;
    };
    // A one-second poll interval keeps the test's wait bounded without
    // being so short it turns into a busy loop against the server.
    let broker = broker.with_revocation_poll_interval(1);

    let mut stream = broker
        .subscribe_revocations()
        .await
        .expect("subscribe_revocations")
        .expect("MysqlBroker must publish a revocation stream");

    let task = SerializedTask::new("revoke_notice".to_string(), vec![]);
    let task_id = broker.enqueue(task).await.expect("enqueue");

    broker
        .revoke(&task_id, true)
        .await
        .expect("revoke with terminate=true");

    let notice = tokio::time::timeout(std::time::Duration::from_secs(10), stream.recv())
        .await
        .expect("a notice must arrive within a few poll intervals")
        .expect("recv must not error")
        .expect("recv must not report the stream as ended");

    assert_eq!(notice.task_id, task_id);
    assert!(
        notice.terminate,
        "terminate=true must survive the round trip through TINYINT(1)"
    );
}

/// Regression test for a real bug caught in review: an earlier draft of
/// `POLL_REVOCATIONS` used a `revoked_at >= ?` cursor, which — once the
/// watermark reached a row's own timestamp — matched that same row on
/// every subsequent poll forever (the watermark can only advance past a
/// *different*, newer row), redelivering the same notice indefinitely
/// whenever nothing newer ever arrives. This is the common case, not a
/// rare one: it fires for every revocation that happens to be the most
/// recent one so far, which is most revocations most of the time.
#[tokio::test]
async fn a_single_revocation_is_delivered_exactly_once_not_forever() {
    let Some((broker, _queue)) =
        broker_on_fresh_queue("a_single_revocation_is_delivered_exactly_once_not_forever").await
    else {
        return;
    };
    let broker = broker.with_revocation_poll_interval(1);

    let mut stream = broker
        .subscribe_revocations()
        .await
        .expect("subscribe_revocations")
        .expect("MysqlBroker must publish a revocation stream");

    let task = SerializedTask::new("revoke_once".to_string(), vec![]);
    let task_id = broker.enqueue(task).await.expect("enqueue");
    broker.revoke(&task_id, false).await.expect("revoke");

    let first = tokio::time::timeout(std::time::Duration::from_secs(10), stream.recv())
        .await
        .expect("a notice must arrive within a few poll intervals")
        .expect("recv must not error")
        .expect("recv must not report the stream as ended");
    assert_eq!(first.task_id, task_id);

    // No second revocation is ever issued. Across several more poll
    // intervals, `recv()` must not produce another notice for the same
    // (never re-revoked) task — that would be the infinite-redelivery
    // bug the `>` cursor exists to prevent.
    let second = tokio::time::timeout(std::time::Duration::from_secs(5), stream.recv()).await;
    assert!(
        second.is_err(),
        "a single revocation must be delivered exactly once, not redelivered every poll \
         (got a second notice: {second:?})"
    );
}

// ---------- The dead-letter move ----------

/// Count the dead-letter rows carrying `task_id`.
///
/// `celers_dead_letter_queue` has no `queue_name` column, so the DLQ is
/// database-wide: `get_statistics().dlq` and `list_dlq()` see every
/// queue's rows and every previous run's leftovers. A test can only
/// address its own row, and only by the task id it minted.
async fn dlq_rows_for(broker: &MysqlBroker, task_id: &Uuid) -> i64 {
    let rows = broker
        .connection()
        .query(
            "SELECT COUNT(*) AS c FROM celers_dead_letter_queue WHERE task_id = ?",
            &[&task_id.to_string()],
        )
        .await
        .expect("counting dead-letter rows should succeed");
    rows.first()
        .map(|row| row.col::<i64>("c"))
        .transpose()
        .expect("the count column must decode")
        .unwrap_or(0)
}

/// Remove this test's dead-letter row, which queue-scoped cleanup cannot
/// reach.
async fn purge_dlq_rows_for(broker: &MysqlBroker, task_id: &Uuid) {
    broker
        .connection()
        .execute(
            "DELETE FROM celers_dead_letter_queue WHERE task_id = ?",
            &[&task_id.to_string()],
        )
        .await
        .expect("dead-letter cleanup should succeed");
}

/// The headline check for the stored-procedure replacement: a task that
/// exhausts its retry budget must actually land in the dead-letter queue,
/// and leave `celers_tasks`.
///
/// `CALL move_to_dlq(?)` could never work — MySQL will not create the
/// procedure over the prepared-statement protocol — so this path was
/// rewritten as [`crate::dlq_move::DLQ_INSERT_SQL`] +
/// [`crate::dlq_move::DLQ_DELETE_SQL`] in one transaction. Nothing had
/// ever run that rewrite against a real server.
#[tokio::test]
async fn retry_exhaustion_moves_the_task_into_the_dead_letter_queue() {
    let Some((broker, _queue)) =
        broker_on_fresh_queue("retry_exhaustion_moves_the_task_into_the_dead_letter_queue").await
    else {
        return;
    };

    // `max_retries = 1` and a claim that increments `retry_count` to 1
    // make the very next reject the exhausting one: `reject` moves the
    // task to the DLQ once `retry_count >= max_retries`.
    let task =
        SerializedTask::new("exhausted".to_string(), b"dlq-payload".to_vec()).with_max_retries(1);
    let task_id = broker.enqueue(task).await.expect("enqueue should succeed");

    let message = claim_from(&broker).await;
    // The claim increments the persisted `retry_count` (the dequeued
    // message still carries the pre-increment value it selected), which
    // is what `reject` compares against `max_retries`.
    let claimed = broker
        .get_task(&task_id)
        .await
        .expect("get_task should succeed")
        .expect("the claimed task must still exist");
    assert_eq!(
        claimed.retry_count, 1,
        "the claim burns the single retry this task was given"
    );

    assert_eq!(
        dlq_rows_for(&broker, &task_id).await,
        0,
        "nothing may be dead-lettered before the budget is spent"
    );

    broker
        .reject(
            &message.task.metadata.id,
            message.receipt_handle.as_deref(),
            true,
        )
        .await
        .expect("reject should succeed");

    assert_eq!(
        dlq_rows_for(&broker, &task_id).await,
        1,
        "an exhausted task must be copied into the dead-letter queue"
    );
    assert!(
        broker
            .get_task(&task_id)
            .await
            .expect("get_task should succeed")
            .is_none(),
        "the source row must be deleted, not left behind as a duplicate"
    );
    assert_eq!(
        broker.queue_size().await.expect("queue_size"),
        0,
        "a dead-lettered task must not still count against the queue"
    );

    purge_dlq_rows_for(&broker, &task_id).await;
}

/// The dead-lettered copy must carry the task's payload and retry count,
/// not an empty shell — `DLQ_INSERT_SQL` selects those columns from the
/// row it is about to delete, so a wrong column order would silently
/// scramble them.
#[tokio::test]
async fn the_dead_letter_copy_keeps_the_payload_and_retry_count() {
    let Some((broker, _queue)) =
        broker_on_fresh_queue("the_dead_letter_copy_keeps_the_payload_and_retry_count").await
    else {
        return;
    };

    let task = SerializedTask::new("exhausted_body".to_string(), b"body-bytes".to_vec())
        .with_max_retries(1);
    let task_id = broker.enqueue(task).await.expect("enqueue should succeed");

    let message = claim_from(&broker).await;
    broker
        .reject(
            &message.task.metadata.id,
            message.receipt_handle.as_deref(),
            true,
        )
        .await
        .expect("reject should succeed");

    let rows = broker
        .connection()
        .query(
            "SELECT task_name, payload, retry_count FROM celers_dead_letter_queue \
             WHERE task_id = ?",
            &[&task_id.to_string()],
        )
        .await
        .expect("reading the dead-letter row should succeed");
    let row = rows.first().expect("the dead-letter row must exist");

    let task_name: String = row.col("task_name").expect("task_name must decode");
    let payload: Vec<u8> = row.col("payload").expect("payload must decode");
    let retry_count: i32 = row.col("retry_count").expect("retry_count must decode");

    assert_eq!(task_name, "exhausted_body");
    assert_eq!(payload, b"body-bytes".to_vec());
    assert_eq!(retry_count, 1);

    purge_dlq_rows_for(&broker, &task_id).await;
}

/// `reject_batch` carries its own copy of the two dead-letter statements
/// (it runs them inside the batch's transaction rather than opening a
/// second one), so it needs its own live check.
#[tokio::test]
async fn reject_batch_moves_an_exhausted_task_into_the_dead_letter_queue() {
    let Some((broker, _queue)) =
        broker_on_fresh_queue("reject_batch_moves_an_exhausted_task_into_the_dead_letter_queue")
            .await
    else {
        return;
    };

    let exhausted =
        SerializedTask::new("batch_exhausted".to_string(), b"x".to_vec()).with_max_retries(1);
    let exhausted_id = broker
        .enqueue(exhausted)
        .await
        .expect("enqueue should succeed");
    let survivor =
        SerializedTask::new("batch_survivor".to_string(), b"y".to_vec()).with_max_retries(9);
    let survivor_id = broker
        .enqueue(survivor)
        .await
        .expect("enqueue should succeed");

    let first = claim_from(&broker).await;
    let second = claim_from(&broker).await;

    let rejected = broker
        .reject_batch(&[
            (first.task.metadata.id, first.receipt_handle.clone(), true),
            (second.task.metadata.id, second.receipt_handle.clone(), true),
        ])
        .await
        .expect("reject_batch should succeed");
    assert_eq!(rejected, 2, "both tasks must be accounted for");

    assert_eq!(
        dlq_rows_for(&broker, &exhausted_id).await,
        1,
        "the task past its retry budget must be dead-lettered"
    );
    assert!(
        broker
            .get_task(&exhausted_id)
            .await
            .expect("get_task should succeed")
            .is_none(),
        "the dead-lettered task's source row must be gone"
    );

    assert_eq!(
        dlq_rows_for(&broker, &survivor_id).await,
        0,
        "a task with retries left must not be dead-lettered"
    );
    let survivor_info = broker
        .get_task(&survivor_id)
        .await
        .expect("get_task should succeed")
        .expect("the requeued task must still exist");
    assert_eq!(survivor_info.state.to_string(), "pending");

    purge_dlq_rows_for(&broker, &exhausted_id).await;
}

// ---------- Server-error classification ----------

/// Pins the formatting `crate::mysql_error`'s predicates parse.
///
/// Those predicates recover a MySQL error *number* from the formatted
/// message, because `oxisql-mysql` discards the numeric code when it maps
/// a server error to `OxiSqlError::Execution(String)`. That only works as
/// long as the driver keeps rendering server errors as
/// `ERROR <code> (<sqlstate>): <message>`. The hermetic tests in
/// `mysql_error.rs` assert the parsing; this one asserts the *format*, by
/// provoking a real server error (`1146`, unknown table) through the real
/// driver and matching it by number.
#[tokio::test]
async fn a_real_server_error_is_recognised_by_its_code() {
    let Some((broker, _queue)) =
        broker_on_fresh_queue("a_real_server_error_is_recognised_by_its_code").await
    else {
        return;
    };

    let error = broker
        .connection()
        .query("SELECT 1 FROM celers_no_such_table_exists", &[])
        .await
        .expect_err("querying a missing table must fail");

    assert!(
        crate::mysql_error::is_mysql_server_error(&error, 1146),
        "a real ER_NO_SUCH_TABLE must be matched by number; \
         the driver's formatting may have changed: {error}"
    );
    assert!(
        !crate::mysql_error::is_deadlock(&error),
        "an unrelated server error must not be mistaken for a deadlock"
    );
}

#[tokio::test]
async fn revocation_lapses_once_its_ttl_elapses() {
    let Some((broker, _queue)) =
        broker_on_fresh_queue("revocation_lapses_once_its_ttl_elapses").await
    else {
        return;
    };
    let broker = broker.with_revocation_ttl(1);

    let task = SerializedTask::new("revoke_ttl".to_string(), vec![]);
    let task_id = broker.enqueue(task).await.expect("enqueue");
    broker.revoke(&task_id, false).await.expect("revoke");
    assert!(broker.is_revoked(&task_id).await.expect("is_revoked"));

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    assert!(
        !broker.is_revoked(&task_id).await.expect("is_revoked"),
        "a revocation older than its TTL must lapse"
    );
}
