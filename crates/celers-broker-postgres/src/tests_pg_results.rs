//! Live-PostgreSQL tests for the broker's own result store
//! (`celers_broker_results`, migration `009_broker_results.sql`) and for the
//! two analytics entry points that read `celers_tasks.updated_at` (migration
//! `010_task_updated_at.sql`).
//!
//! # Why this file exists
//!
//! Every statement covered here was **unreachable** against a real server
//! until those two migrations landed:
//!
//! * `results.rs`'s six functions and `analytics.rs`'s `store_results_batch`
//!   addressed a result table no migration created, so each failed with
//!   `relation ... does not exist`. The `::text::uuid` / `::text::jsonb` /
//!   `::text::timestamptz` casts on those paths had never once been executed.
//! * `get_state_transition_history` and
//!   `detect_abnormal_state_duration("processing", ..)` read a
//!   `celers_tasks.updated_at` column that did not exist, so both failed with
//!   `column "updated_at" does not exist`.
//!
//! Gated on `CELERS_TEST_POSTGRES_URL` exactly like `tests_pg.rs`: without it
//! each test logs a skip line and returns.
//!
//! # Isolation
//!
//! `celers_broker_results` has no `queue_name` column — a result is addressed
//! by task id alone — so these tests cannot lean on `tests_pg.rs`'s
//! private-queue trick for the result rows themselves. They use freshly
//! minted `Uuid`s instead and only ever assert on rows they created, which is
//! isolation enough for a table keyed by a v4 UUID. The one global-sweep
//! function, `archive_results`, is exercised against a row deliberately
//! backdated past the cutoff so that concurrently running tests' just-written
//! rows are never in range.

#![cfg(test)]

use std::time::Duration;

use celers_core::{Broker, SerializedTask};
use uuid::Uuid;

use crate::row_ext::{uuid_param, RowExt};
use crate::tests_pg::broker_on_new_queue;
use crate::{PostgresBroker, TaskResult, TaskResultStatus};

/// Macro sugar for the "skip when unconfigured" preamble, mirroring
/// `tests_pg.rs`'s own.
macro_rules! broker_or_skip {
    ($name:literal) => {
        match broker_on_new_queue($name).await {
            Some(pair) => pair,
            None => return,
        }
    };
}

/// Store one `SUCCESS` result with a JSON payload and a runtime.
async fn store_success(broker: &PostgresBroker, task_id: &Uuid, name: &str, runtime_ms: i64) {
    broker
        .store_result(
            task_id,
            name,
            TaskResultStatus::Success,
            Some(serde_json::json!({ "value": 42, "name": name })),
            None,
            None,
            Some(runtime_ms),
        )
        .await
        .expect("store_result");
}

/// Remove every result row this test created, so a long-lived dev database
/// does not accumulate them.
async fn cleanup(broker: &PostgresBroker, ids: &[Uuid]) {
    broker
        .delete_results_batch(ids)
        .await
        .expect("cleanup delete_results_batch");
}

// ── results.rs: store_result / get_result ──────────────────────────────────

#[tokio::test]
async fn store_result_then_get_result_round_trips_every_column() {
    let (broker, _queue) = broker_or_skip!("store_result_then_get_result_round_trips");

    let task_id = Uuid::new_v4();
    store_success(&broker, &task_id, "round_trip_task", 1234).await;

    let stored = broker
        .get_result(&task_id)
        .await
        .expect("get_result")
        .expect("the result just stored must be readable");

    assert_eq!(
        stored.task_id, task_id,
        "the UUID must survive the round trip"
    );
    assert_eq!(stored.task_name, "round_trip_task");
    assert_eq!(stored.status, TaskResultStatus::Success);
    assert_eq!(
        stored.result,
        Some(serde_json::json!({ "value": 42, "name": "round_trip_task" })),
        "the JSONB payload must come back byte-for-byte equal as JSON"
    );
    assert_eq!(stored.error, None);
    assert_eq!(stored.traceback, None);
    assert_eq!(stored.runtime_ms, Some(1234));
    assert!(
        stored.completed_at.is_some(),
        "a terminal status must stamp completed_at"
    );

    cleanup(&broker, &[task_id]).await;
}

#[tokio::test]
async fn store_result_leaves_completed_at_null_for_a_non_terminal_status() {
    let (broker, _queue) = broker_or_skip!("store_result_non_terminal_completed_at");

    let task_id = Uuid::new_v4();
    broker
        .store_result(
            &task_id,
            "in_flight_task",
            TaskResultStatus::Started,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("store_result");

    let stored = broker
        .get_result(&task_id)
        .await
        .expect("get_result")
        .expect("stored result");
    assert_eq!(stored.status, TaskResultStatus::Started);
    assert!(
        stored.completed_at.is_none(),
        "STARTED is not terminal, so completed_at must stay NULL"
    );
    assert_eq!(stored.runtime_ms, None);

    cleanup(&broker, &[task_id]).await;
}

/// `results.rs` goes out of its way to keep SQL `NULL` distinguishable from
/// the JSON literal `null` (see `task_result_json_column`'s doc comment). That
/// distinction only exists against a real server.
#[tokio::test]
async fn a_sql_null_result_and_a_json_null_result_stay_distinguishable() {
    let (broker, _queue) = broker_or_skip!("sql_null_vs_json_null_result");

    let absent = Uuid::new_v4();
    let json_null = Uuid::new_v4();

    broker
        .store_result(
            &absent,
            "no_result",
            TaskResultStatus::Success,
            None,
            None,
            None,
            None,
        )
        .await
        .expect("store_result with no payload");
    broker
        .store_result(
            &json_null,
            "null_result",
            TaskResultStatus::Success,
            Some(serde_json::Value::Null),
            None,
            None,
            None,
        )
        .await
        .expect("store_result with a JSON null payload");

    let absent_row = broker
        .get_result(&absent)
        .await
        .expect("get_result")
        .expect("row");
    let json_null_row = broker
        .get_result(&json_null)
        .await
        .expect("get_result")
        .expect("row");

    assert_eq!(
        absent_row.result, None,
        "a None payload must store SQL NULL and read back as None"
    );
    assert_eq!(
        json_null_row.result,
        Some(serde_json::Value::Null),
        "a Some(Value::Null) payload must store the JSON literal null and read back as such"
    );

    cleanup(&broker, &[absent, json_null]).await;
}

#[tokio::test]
async fn store_result_upserts_on_conflict_instead_of_failing() {
    let (broker, _queue) = broker_or_skip!("store_result_upserts_on_conflict");

    let task_id = Uuid::new_v4();
    broker
        .store_result(
            &task_id,
            "retried_task",
            TaskResultStatus::Retry,
            None,
            Some("first attempt blew up"),
            Some("Traceback (most recent call last): ..."),
            Some(10),
        )
        .await
        .expect("first store_result");

    let created_at = broker
        .get_result(&task_id)
        .await
        .expect("get_result")
        .expect("row")
        .created_at;

    // Same task id, terminal status this time: the ON CONFLICT branch.
    broker
        .store_result(
            &task_id,
            "retried_task",
            TaskResultStatus::Success,
            Some(serde_json::json!([1, 2, 3])),
            None,
            None,
            Some(99),
        )
        .await
        .expect("second store_result must upsert, not conflict");

    let updated = broker
        .get_result(&task_id)
        .await
        .expect("get_result")
        .expect("row");
    assert_eq!(updated.status, TaskResultStatus::Success);
    assert_eq!(updated.result, Some(serde_json::json!([1, 2, 3])));
    assert_eq!(
        updated.error, None,
        "the conflict branch must overwrite error, not keep the stale one"
    );
    assert_eq!(updated.traceback, None);
    assert_eq!(updated.runtime_ms, Some(99));
    assert_eq!(
        updated.created_at, created_at,
        "created_at is not in the DO UPDATE list, so it must be preserved"
    );

    cleanup(&broker, &[task_id]).await;
}

#[tokio::test]
async fn get_result_returns_none_for_a_task_with_no_stored_result() {
    let (broker, _queue) = broker_or_skip!("get_result_none_for_unknown_task");

    let unknown = broker
        .get_result(&Uuid::new_v4())
        .await
        .expect("get_result");
    assert!(unknown.is_none());
}

// ── results.rs: delete_result ──────────────────────────────────────────────

#[tokio::test]
async fn delete_result_reports_whether_a_row_was_actually_removed() {
    let (broker, _queue) = broker_or_skip!("delete_result_reports_removal");

    let task_id = Uuid::new_v4();
    store_success(&broker, &task_id, "doomed_task", 5).await;

    assert!(
        broker.delete_result(&task_id).await.expect("delete_result"),
        "deleting an existing result must report true"
    );
    assert!(
        broker
            .get_result(&task_id)
            .await
            .expect("get_result")
            .is_none(),
        "the row must really be gone"
    );
    assert!(
        !broker.delete_result(&task_id).await.expect("delete_result"),
        "deleting an already-deleted result must report false, not error"
    );
}

// ── results.rs: batch reads and writes ─────────────────────────────────────

#[tokio::test]
async fn get_results_batch_returns_only_the_requested_ids() {
    let (broker, _queue) = broker_or_skip!("get_results_batch_returns_requested_ids");

    let wanted: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();
    let unwanted = Uuid::new_v4();
    for (i, id) in wanted.iter().enumerate() {
        store_success(&broker, id, &format!("batch_task_{i}"), i as i64).await;
    }
    store_success(&broker, &unwanted, "not_in_the_batch", 0).await;

    let fetched = broker
        .get_results_batch(&wanted)
        .await
        .expect("get_results_batch");
    assert_eq!(fetched.len(), 3, "one row per requested id");
    let fetched_ids: Vec<Uuid> = fetched.iter().map(|r| r.task_id).collect();
    for id in &wanted {
        assert!(fetched_ids.contains(id), "missing {id} from the batch");
    }
    assert!(
        !fetched_ids.contains(&unwanted),
        "the generated IN (...) list must not widen to unrelated rows"
    );
    for row in &fetched {
        assert_eq!(row.status, TaskResultStatus::Success);
        assert!(row.result.is_some(), "the JSONB payload must decode");
    }

    let mut all = wanted.clone();
    all.push(unwanted);
    cleanup(&broker, &all).await;
}

#[tokio::test]
async fn the_batch_result_functions_short_circuit_on_an_empty_slice() {
    let (broker, _queue) = broker_or_skip!("batch_result_functions_short_circuit");

    // Both guard against an empty slice *before* building SQL, because an
    // `IN ()` list is a syntax error rather than an empty match.
    assert!(broker
        .get_results_batch(&[])
        .await
        .expect("get_results_batch on an empty slice")
        .is_empty());
    assert_eq!(
        broker
            .delete_results_batch(&[])
            .await
            .expect("delete_results_batch on an empty slice"),
        0
    );
}

#[tokio::test]
async fn delete_results_batch_removes_exactly_the_listed_ids() {
    let (broker, _queue) = broker_or_skip!("delete_results_batch_removes_listed_ids");

    let doomed: Vec<Uuid> = (0..2).map(|_| Uuid::new_v4()).collect();
    let survivor = Uuid::new_v4();
    for id in &doomed {
        store_success(&broker, id, "doomed_batch_task", 1).await;
    }
    store_success(&broker, &survivor, "surviving_task", 1).await;

    let deleted = broker
        .delete_results_batch(&doomed)
        .await
        .expect("delete_results_batch");
    assert_eq!(deleted, 2);
    for id in &doomed {
        assert!(broker.get_result(id).await.expect("get_result").is_none());
    }
    assert!(
        broker
            .get_result(&survivor)
            .await
            .expect("get_result")
            .is_some(),
        "an id outside the list must be untouched"
    );

    cleanup(&broker, &[survivor]).await;
}

// ── results.rs: archive_results ────────────────────────────────────────────

#[tokio::test]
async fn archive_results_deletes_only_results_completed_before_the_cutoff() {
    let (broker, _queue) = broker_or_skip!("archive_results_respects_the_cutoff");

    let stale = Uuid::new_v4();
    let fresh = Uuid::new_v4();
    store_success(&broker, &stale, "stale_task", 1).await;
    store_success(&broker, &fresh, "fresh_task", 1).await;

    // Backdate one row well past the cutoff. Every other row in the table —
    // including rows written by concurrently running tests — has
    // `completed_at` within seconds of now, so a one-hour cutoff cannot reach
    // them.
    let conn = broker.connection().await.expect("pooled connection");
    let stale_param = uuid_param(&stale);
    conn.execute(
        "UPDATE celers_broker_results SET completed_at = NOW() - INTERVAL '2 hours' \
         WHERE task_id = $1::text::uuid",
        &[&stale_param],
    )
    .await
    .expect("backdate the stale result");
    drop(conn);

    let archived = broker
        .archive_results(Duration::from_secs(3600))
        .await
        .expect("archive_results");
    assert!(archived >= 1, "the backdated row must have been archived");

    assert!(
        broker
            .get_result(&stale)
            .await
            .expect("get_result")
            .is_none(),
        "a result completed before the cutoff must be archived away"
    );
    assert!(
        broker
            .get_result(&fresh)
            .await
            .expect("get_result")
            .is_some(),
        "a result completed after the cutoff must survive"
    );

    cleanup(&broker, &[fresh]).await;
}

// ── analytics.rs: store_results_batch ──────────────────────────────────────

fn sample_result(task_id: Uuid, name: &str, status: TaskResultStatus) -> TaskResult {
    TaskResult {
        task_id,
        task_name: name.to_string(),
        status,
        result: Some(serde_json::json!({ "batched": true })),
        error: None,
        traceback: None,
        created_at: chrono::Utc::now(),
        completed_at: Some(chrono::Utc::now()),
        runtime_ms: Some(7),
    }
}

/// `store_results_batch` binds neither `task_name` nor `created_at`, which is
/// exactly why `009_broker_results.sql` gives both a default. Inserting a
/// *new* row through it is the path that proves the defaults are there.
#[tokio::test]
async fn store_results_batch_inserts_new_rows_without_binding_task_name() {
    let (broker, _queue) = broker_or_skip!("store_results_batch_inserts_new_rows");

    let ids: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();
    let batch: Vec<TaskResult> = ids
        .iter()
        .map(|id| sample_result(*id, "batched_task", TaskResultStatus::Success))
        .collect();

    let stored = broker
        .store_results_batch(&batch)
        .await
        .expect("store_results_batch");
    assert_eq!(stored, 3);

    for id in &ids {
        let row = broker
            .get_result(id)
            .await
            .expect("get_result")
            .expect("batched row");
        assert_eq!(row.status, TaskResultStatus::Success);
        assert_eq!(row.result, Some(serde_json::json!({ "batched": true })));
        assert_eq!(
            row.task_name, "",
            "the statement binds no task_name, so the column default applies"
        );
    }

    cleanup(&broker, &ids).await;
}

#[tokio::test]
async fn store_results_batch_updates_an_existing_row_through_its_conflict_branch() {
    let (broker, _queue) = broker_or_skip!("store_results_batch_conflict_branch");

    let task_id = Uuid::new_v4();
    store_success(&broker, &task_id, "pre_existing_task", 3).await;

    let mut failed = sample_result(task_id, "pre_existing_task", TaskResultStatus::Failure);
    failed.result = None;
    failed.error = Some("it failed the second time".to_string());
    failed.traceback = Some("Traceback ...".to_string());

    assert_eq!(
        broker
            .store_results_batch(&[failed])
            .await
            .expect("store_results_batch"),
        1
    );

    let row = broker
        .get_result(&task_id)
        .await
        .expect("get_result")
        .expect("row");
    assert_eq!(row.status, TaskResultStatus::Failure);
    assert_eq!(row.result, None);
    assert_eq!(row.error.as_deref(), Some("it failed the second time"));
    assert_eq!(
        row.task_name, "pre_existing_task",
        "task_name is not in the DO UPDATE list, so the original value stands"
    );

    // `updated_at` is the one column only this statement's conflict branch
    // ever writes, so it is worth reading back directly.
    let conn = broker.connection().await.expect("pooled connection");
    let id_param = uuid_param(&task_id);
    let rows = conn
        .query(
            "SELECT (updated_at > created_at) AS bumped FROM celers_broker_results \
             WHERE task_id = $1::text::uuid",
            &[&id_param],
        )
        .await
        .expect("read updated_at");
    let bumped: bool = rows
        .first()
        .expect("one row")
        .col("bumped")
        .expect("read bumped");
    assert!(
        bumped,
        "the conflict branch sets updated_at = NOW(), which must be later than created_at"
    );
    drop(conn);

    cleanup(&broker, &[task_id]).await;
}

#[tokio::test]
async fn store_results_batch_short_circuits_on_an_empty_slice() {
    let (broker, _queue) = broker_or_skip!("store_results_batch_empty_slice");
    assert_eq!(
        broker
            .store_results_batch(&[])
            .await
            .expect("store_results_batch on an empty slice"),
        0
    );
}

// ── The table-name collision this crate used to have ───────────────────────

/// `celers-backend-db`'s `PostgresResultBackend` owns `celers_task_results`,
/// with an incompatible schema, on the same server this broker migrates. This
/// crate's result statements used to name that table, so whichever crate
/// migrated second was broken. They must now coexist.
#[tokio::test]
async fn the_broker_result_table_does_not_collide_with_the_result_backends() {
    let (broker, _queue) = broker_or_skip!("broker_results_do_not_collide_with_backend");

    let conn = broker.connection().await.expect("pooled connection");
    let rows = conn
        .query(
            "SELECT table_name, column_name FROM information_schema.columns \
             WHERE table_schema = 'public' \
               AND ((table_name = 'celers_broker_results' AND column_name = 'traceback') \
                 OR (table_name = 'celers_task_results' AND column_name = 'result_state'))",
            &[],
        )
        .await
        .expect("read information_schema.columns");
    let found: Vec<String> = rows
        .iter()
        .map(|row| row.col::<String>("table_name").expect("table_name"))
        .collect();
    drop(conn);

    assert!(
        found.iter().any(|t| t == "celers_broker_results"),
        "migration 009 must create celers_broker_results with this crate's own \
         `traceback` column; found: {found:?}"
    );

    // `celers_task_results` only exists if `celers-backend-db` has also
    // migrated against this database. When it has, migration 009 must have
    // left its schema alone.
    if found.iter().any(|t| t == "celers_task_results") {
        // Nothing further to assert: finding `result_state` on it *is* the
        // assertion that this crate did not overwrite the backend's table.
    } else {
        eprintln!(
            "note: celers-backend-db has not migrated against this database, \
             so only the broker half of the coexistence check ran"
        );
    }
}

// ── analytics.rs: celers_tasks.updated_at ──────────────────────────────────

/// Migration `010` adds the column; the ~35 `UPDATE celers_tasks` statements
/// in the crate are what keep it current. This is the end-to-end proof that
/// the sweep reaches the delivery path.
#[tokio::test]
async fn every_task_state_change_advances_updated_at() {
    let (broker, _queue) = broker_or_skip!("task_state_changes_advance_updated_at");

    let task_id = broker
        .enqueue(SerializedTask::new("updated_at_task".to_string(), vec![]))
        .await
        .expect("enqueue");

    let read_updated_at = |id: Uuid| {
        let broker = &broker;
        async move {
            let conn = broker.connection().await.expect("pooled connection");
            let id_param = uuid_param(&id);
            let rows = conn
                .query(
                    "SELECT updated_at FROM celers_tasks WHERE id = $1::text::uuid",
                    &[&id_param],
                )
                .await
                .expect("read updated_at");
            rows.first()
                .expect("the task row")
                .col::<chrono::DateTime<chrono::Utc>>("updated_at")
                .expect("updated_at must exist and be a timestamptz")
        }
    };

    let after_enqueue = read_updated_at(task_id).await;

    // `dequeue` is `UPDATE ... SET state = 'processing', ..., updated_at = NOW()`.
    let msg = broker
        .dequeue()
        .await
        .expect("dequeue")
        .expect("the task just enqueued");
    let after_dequeue = read_updated_at(task_id).await;
    assert!(
        after_dequeue >= after_enqueue,
        "claiming a task must not move updated_at backwards"
    );

    // `ack` likewise.
    broker
        .ack(&task_id, msg.receipt_handle.as_deref())
        .await
        .expect("ack");
    let after_ack = read_updated_at(task_id).await;
    assert!(
        after_ack > after_dequeue,
        "acking a task must advance updated_at ({after_ack} must be later than {after_dequeue})"
    );
}

// ── analytics.rs: get_state_transition_history ─────────────────────────────

/// # A note on `duration_ms`, which is always `None` here
///
/// `get_state_transition_history` windows with
/// `LAG(..) OVER (PARTITION BY id ORDER BY updated_at)` over `celers_tasks`,
/// whose `id` is the **primary key**. Every partition therefore holds exactly
/// one row, so `LAG` has nothing to look back at: `from_state` always
/// `COALESCE`s to `'created'` and `duration_ms` is always `NULL`, on any
/// data, forever. Adding `updated_at` makes the query *run* and makes its
/// `to_state` / `transition_time` / windowing / ordering correct — it cannot
/// make the `LAG` meaningful, because a real transition history needs a
/// per-transition append log that `celers_tasks` is not.
///
/// This test asserts what the query genuinely returns rather than pretending
/// otherwise; the structural limitation is reported as a followup.
#[tokio::test]
async fn state_transition_history_reads_updated_at_for_a_single_task() {
    let (broker, _queue) = broker_or_skip!("state_transition_history_single_task");

    let task_id = broker
        .enqueue(SerializedTask::new("history_task".to_string(), vec![]))
        .await
        .expect("enqueue");
    let msg = broker.dequeue().await.expect("dequeue").expect("a task");
    broker
        .ack(&task_id, msg.receipt_handle.as_deref())
        .await
        .expect("ack");

    let transitions = broker
        .get_state_transition_history(Some(task_id), 24, 100)
        .await
        .expect("get_state_transition_history must no longer fail on a missing column");

    assert_eq!(
        transitions.len(),
        1,
        "celers_tasks holds one row per task, so the id-scoped branch returns one row"
    );
    let (id, task_name, from_state, to_state, transition_time, duration_ms) = &transitions[0];
    assert_eq!(*id, task_id);
    assert_eq!(task_name, "history_task");
    assert_eq!(
        to_state, "completed",
        "to_state is the row's current state, which ack set to completed"
    );
    assert_eq!(
        from_state, "created",
        "one row per partition means LAG(state) is NULL and COALESCEs to 'created'"
    );
    assert!(
        duration_ms.is_none(),
        "LAG(updated_at) over a single-row partition is NULL, so no duration can be computed"
    );
    assert!(
        *transition_time <= chrono::Utc::now(),
        "transition_time is the row's updated_at and cannot be in the future"
    );
}

#[tokio::test]
async fn state_transition_history_is_queue_scoped_and_window_filtered() {
    let (broker, _queue) = broker_or_skip!("state_transition_history_queue_scoped");

    let mut ids = Vec::new();
    for i in 0..3 {
        ids.push(
            broker
                .enqueue(SerializedTask::new(format!("history_task_{i}"), vec![]))
                .await
                .expect("enqueue"),
        );
    }

    let transitions = broker
        .get_state_transition_history(None, 24, 100)
        .await
        .expect("get_state_transition_history");
    assert_eq!(
        transitions.len(),
        3,
        "the unscoped branch must return this queue's rows and only this queue's"
    );
    let returned: Vec<Uuid> = transitions.iter().map(|t| t.0).collect();
    for id in &ids {
        assert!(returned.contains(id));
    }

    // Ordered by `updated_at DESC`.
    let times: Vec<_> = transitions.iter().map(|t| t.4).collect();
    for pair in times.windows(2) {
        assert!(
            pair[0] >= pair[1],
            "rows must come back in updated_at DESC order"
        );
    }

    // A zero-hour window excludes everything: `updated_at >= NOW() - 0` is
    // only true for a row updated in the future.
    let none_in_window = broker
        .get_state_transition_history(None, 0, 100)
        .await
        .expect("get_state_transition_history with a zero window");
    assert!(
        none_in_window.is_empty(),
        "the hours parameter must really filter on updated_at"
    );

    // And `limit` is honoured.
    let limited = broker
        .get_state_transition_history(None, 24, 2)
        .await
        .expect("get_state_transition_history with a limit");
    assert_eq!(limited.len(), 2);
}

// ── analytics.rs: detect_abnormal_state_duration ───────────────────────────

#[tokio::test]
async fn detect_abnormal_state_duration_finds_a_task_stuck_in_processing() {
    let (broker, _queue) = broker_or_skip!("detect_abnormal_state_duration_processing");

    let stuck = broker
        .enqueue(SerializedTask::new("stuck_task".to_string(), vec![]))
        .await
        .expect("enqueue");
    broker.dequeue().await.expect("dequeue").expect("a task");

    // Nothing is stuck yet.
    let none_yet = broker
        .detect_abnormal_state_duration("processing", 3600, 50)
        .await
        .expect("detect_abnormal_state_duration must no longer fail on a missing column");
    assert!(
        !none_yet.iter().any(|(id, ..)| *id == stuck),
        "a task claimed a moment ago is not stuck"
    );

    // Age the claim past the threshold. `started_at` is what the
    // "processing" branch prefers, and `updated_at` is its fallback for a row
    // whose worker never recorded one — both are backdated here so the
    // `COALESCE` reports the same age either way.
    let conn = broker.connection().await.expect("pooled connection");
    let id_param = uuid_param(&stuck);
    conn.execute(
        "UPDATE celers_tasks \
            SET started_at = NOW() - INTERVAL '2 hours', \
                updated_at = NOW() - INTERVAL '2 hours' \
          WHERE id = $1::text::uuid",
        &[&id_param],
    )
    .await
    .expect("backdate the claim");
    drop(conn);

    let detected = broker
        .detect_abnormal_state_duration("processing", 3600, 50)
        .await
        .expect("detect_abnormal_state_duration");
    let found = detected
        .iter()
        .find(|(id, ..)| *id == stuck)
        .expect("the backdated task must be reported as stuck");
    assert_eq!(found.1, "stuck_task");
    assert!(
        found.2 >= 7000,
        "a two-hour-old claim must report roughly 7200 seconds, got {}",
        found.2
    );
    assert_eq!(found.3, 0, "the task has not been retried");
}

/// The "processing" branch's `COALESCE(started_at, updated_at)` is the whole
/// reason `updated_at` had to exist: a row claimed by a worker that never
/// wrote `started_at` still has to age.
#[tokio::test]
async fn detect_abnormal_state_duration_falls_back_to_updated_at() {
    let (broker, _queue) = broker_or_skip!("detect_abnormal_state_duration_updated_at_fallback");

    let orphan = broker
        .enqueue(SerializedTask::new("orphan_task".to_string(), vec![]))
        .await
        .expect("enqueue");
    broker.dequeue().await.expect("dequeue").expect("a task");

    let conn = broker.connection().await.expect("pooled connection");
    let id_param = uuid_param(&orphan);
    conn.execute(
        "UPDATE celers_tasks \
            SET started_at = NULL, \
                updated_at = NOW() - INTERVAL '3 hours' \
          WHERE id = $1::text::uuid",
        &[&id_param],
    )
    .await
    .expect("clear started_at and backdate updated_at");
    drop(conn);

    let detected = broker
        .detect_abnormal_state_duration("processing", 3600, 50)
        .await
        .expect("detect_abnormal_state_duration");
    let found = detected
        .iter()
        .find(|(id, ..)| *id == orphan)
        .expect("a claim with no started_at must age off updated_at instead");
    assert!(
        found.2 >= 10_000,
        "a three-hour-old updated_at must report roughly 10800 seconds, got {}",
        found.2
    );
}

#[tokio::test]
async fn detect_abnormal_state_duration_ages_pending_tasks_from_created_at() {
    let (broker, _queue) = broker_or_skip!("detect_abnormal_state_duration_pending");

    let old = broker
        .enqueue(SerializedTask::new("old_pending_task".to_string(), vec![]))
        .await
        .expect("enqueue");

    let conn = broker.connection().await.expect("pooled connection");
    let id_param = uuid_param(&old);
    conn.execute(
        "UPDATE celers_tasks SET created_at = NOW() - INTERVAL '90 minutes' \
          WHERE id = $1::text::uuid",
        &[&id_param],
    )
    .await
    .expect("backdate created_at");
    drop(conn);

    let detected = broker
        .detect_abnormal_state_duration("pending", 3600, 50)
        .await
        .expect("detect_abnormal_state_duration");
    let found = detected
        .iter()
        .find(|(id, ..)| *id == old)
        .expect("a 90-minute-old pending task exceeds a one-hour threshold");
    assert!(found.2 >= 5000, "got {}", found.2);
}

#[tokio::test]
async fn detect_abnormal_state_duration_rejects_an_unsupported_state() {
    let (broker, _queue) = broker_or_skip!("detect_abnormal_state_duration_unsupported");

    let err = broker
        .detect_abnormal_state_duration("completed", 60, 10)
        .await
        .expect_err("only 'processing' and 'pending' have an age definition");
    assert!(
        err.to_string().contains("Unsupported state"),
        "the error must name the problem: {err}"
    );
}
