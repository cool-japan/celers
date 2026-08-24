//! Integration tests that run against a real PostgreSQL instance.
//!
//! These are **not** `#[ignore]`d. They are gated on the presence of
//! `CELERS_TEST_POSTGRES_URL`: without it each test logs a skip line and
//! returns, so `cargo test` stays green on a machine with no database; with it
//! (a CI service container, or a local
//! `docker run -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres`) the whole
//! suite runs by default rather than needing `-- --ignored`, which is how the
//! delivery-identity bug survived earlier hardening passes.
//!
//! ```text
//! CELERS_TEST_POSTGRES_URL=postgres://postgres:postgres@localhost/celers_test \
//!     cargo nextest run -p celers-broker-postgres
//! ```
//!
//! Every test allocates its own randomly named logical queue, so the tests are
//! isolated from each other and from whatever else lives in the database — no
//! shared fixture, no ordering requirements, no cleanup step that can leave the
//! next run poisoned.

#![cfg(test)]

use celers_core::{Broker, SerializedTask};
use uuid::Uuid;

use crate::{DbTaskState, DeduplicationConfig, PostgresBroker, RetryStrategy};

/// The connection string to test against, if one is configured.
fn test_pg_url() -> Option<String> {
    match std::env::var("CELERS_TEST_POSTGRES_URL") {
        Ok(url) if !url.trim().is_empty() => Some(url),
        _ => None,
    }
}

/// A queue label unique to one test run.
fn unique_queue() -> String {
    format!("itest_{}", Uuid::new_v4().simple())
}

/// Connect a migrated broker on a private queue, or `None` when no test
/// database is configured.
async fn broker_on_new_queue(test_name: &str) -> Option<(PostgresBroker, String)> {
    let url = match test_pg_url() {
        Some(url) => url,
        None => {
            eprintln!("skipping {test_name}: CELERS_TEST_POSTGRES_URL is not set");
            return None;
        }
    };
    let queue = unique_queue();
    let broker = PostgresBroker::with_queue(&url, &queue)
        .await
        .expect("connect to CELERS_TEST_POSTGRES_URL");
    broker.migrate().await.expect("run migrations");
    Some((broker, queue))
}

/// Macro sugar for the "skip when unconfigured" preamble.
macro_rules! broker_or_skip {
    ($name:literal) => {
        match broker_on_new_queue($name).await {
            Some(pair) => pair,
            None => return,
        }
    };
}

/// Push one task through to the dead-letter queue, so DLQ-inspection tests
/// have a real row to query.
///
/// `max_retries = 0` means the very first `reject(.., requeue = true)`
/// already exceeds the retry budget (`retry_count + 1 > max_retries` is
/// `1 > 0`), which routes it through `move_to_dlq` — see `broker_trait.rs`'s
/// `reject`. The `Broker::reject` trait method itself takes no error
/// message, so one is written directly onto the row first, the way any real
/// failure path would before rejecting.
async fn fail_task_into_dlq(broker: &PostgresBroker, task_name: &str, error_message: &str) -> Uuid {
    let task = SerializedTask::new(task_name.to_string(), vec![]).with_max_retries(0);
    let task_id = broker.enqueue(task).await.expect("enqueue");
    let msg = broker.dequeue().await.expect("dequeue").expect("a task");

    let conn = broker.connection().await.expect("pooled connection");
    let id_param = crate::row_ext::uuid_param(&task_id);
    conn.execute(
        "UPDATE celers_tasks SET error_message = $1 WHERE id = $2",
        &[&error_message, &id_param],
    )
    .await
    .expect("set error_message before reject");
    drop(conn);

    broker
        .reject(&task_id, msg.receipt_handle.as_deref(), true)
        .await
        .expect("reject with retry budget exhausted");

    task_id
}

#[tokio::test]
async fn dequeue_returns_the_database_row_id_so_ack_completes_the_task() {
    let (broker, _queue) = broker_or_skip!("dequeue_returns_the_database_row_id");

    let mut task = SerializedTask::new("identity_task".to_string(), vec![1, 2, 3, 4]);
    task.metadata.priority = 7;
    task.metadata.max_retries = 9;
    let enqueued_id = broker.enqueue(task).await.expect("enqueue");

    let msg = broker
        .dequeue()
        .await
        .expect("dequeue")
        .expect("a task should be available");

    // The regression this suite exists for: the message must carry the row's
    // identity, not a freshly minted UUID.
    assert_eq!(
        msg.task.metadata.id, enqueued_id,
        "dequeue must return the database row id"
    );
    assert!(
        broker
            .get_task(&msg.task.metadata.id)
            .await
            .expect("get_task")
            .is_some(),
        "the id returned by dequeue must exist in celers_tasks"
    );
    // Column values must survive the round trip, not be reset to
    // `TaskMetadata::new` defaults.
    assert_eq!(msg.task.metadata.priority, 7);
    assert_eq!(msg.task.metadata.max_retries, 9);
    assert_eq!(msg.task.metadata.name, "identity_task");
    assert_eq!(msg.task.payload, vec![1, 2, 3, 4]);
    // The receipt handle carries identity now, not a retry counter.
    assert_eq!(
        msg.receipt_handle.as_deref(),
        Some(enqueued_id.to_string().as_str())
    );

    broker
        .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
        .await
        .expect("ack");

    let stored = broker
        .get_task(&enqueued_id)
        .await
        .expect("get_task")
        .expect("task row still present");
    assert_eq!(
        stored.state,
        DbTaskState::Completed,
        "ack must actually complete the row, not silently match zero rows"
    );
    assert_eq!(broker.queue_size().await.expect("queue_size"), 0);
}

#[tokio::test]
async fn first_delivery_does_not_consume_a_retry() {
    let (broker, _queue) = broker_or_skip!("first_delivery_does_not_consume_a_retry");

    let task = SerializedTask::new("budget_task".to_string(), vec![]);
    let task_id = broker.enqueue(task).await.expect("enqueue");

    broker.dequeue().await.expect("dequeue").expect("a task");

    let stored = broker
        .get_task(&task_id)
        .await
        .expect("get_task")
        .expect("task row");
    assert_eq!(
        stored.retry_count, 0,
        "a task that has never failed must not arrive with a retry already spent"
    );
    assert_eq!(stored.state, DbTaskState::Processing);
}

#[tokio::test]
async fn retry_budget_allows_initial_attempt_plus_max_retries() {
    let (broker, _queue) = broker_or_skip!("retry_budget_allows_initial_attempt_plus_max_retries");
    // Immediate backoff keeps the test deterministic: a requeued task is
    // eligible again straight away, with no sleeping.
    let mut broker = broker;
    broker.set_retry_strategy(RetryStrategy::Immediate);

    let mut task = SerializedTask::new("retry_task".to_string(), vec![]);
    task.metadata.max_retries = 2;
    let task_id = broker.enqueue(task).await.expect("enqueue");

    // Initial attempt + 2 retries = 3 deliveries before the DLQ.
    for expected_retry_count in 1..=2 {
        let msg = broker
            .dequeue()
            .await
            .expect("dequeue")
            .expect("task should be redelivered");
        assert_eq!(msg.task.metadata.id, task_id);
        broker
            .reject(&task_id, msg.receipt_handle.as_deref(), true)
            .await
            .expect("reject");
        let stored = broker
            .get_task(&task_id)
            .await
            .expect("get_task")
            .expect("task still queued");
        assert_eq!(stored.retry_count, expected_retry_count);
        assert_eq!(stored.state, DbTaskState::Pending);
    }

    // Third delivery exhausts the budget: the task moves to the DLQ.
    let msg = broker
        .dequeue()
        .await
        .expect("dequeue")
        .expect("third delivery");
    broker
        .reject(&task_id, msg.receipt_handle.as_deref(), true)
        .await
        .expect("final reject");

    assert!(
        broker.get_task(&task_id).await.expect("get_task").is_none(),
        "an exhausted task must be moved out of the dispatch table"
    );
    let dlq = broker.list_dlq(10, 0).await.expect("list_dlq");
    assert_eq!(dlq.len(), 1, "exactly one DLQ row for the exhausted task");
    assert_eq!(dlq[0].task_id, task_id);
}

#[tokio::test]
async fn ack_of_an_unknown_task_is_reported_not_swallowed() {
    let (broker, _queue) = broker_or_skip!("ack_of_an_unknown_task_is_reported");

    let phantom = Uuid::new_v4();
    let result = broker.ack(&phantom, None).await;
    assert!(
        result.is_err(),
        "acking an id that exists in no row must surface an error"
    );
}

#[tokio::test]
async fn queues_are_isolated_from_each_other() {
    let (broker_a, _queue_a) = broker_or_skip!("queues_are_isolated_from_each_other");
    let url = match test_pg_url() {
        Some(url) => url,
        None => return,
    };
    let broker_b = PostgresBroker::with_queue(&url, &unique_queue())
        .await
        .expect("second broker");

    broker_a
        .enqueue(SerializedTask::new("a_task".to_string(), vec![]))
        .await
        .expect("enqueue into A");

    assert!(
        broker_b.dequeue().await.expect("dequeue from B").is_none(),
        "a task enqueued on one logical queue must not be served to another"
    );
    assert_eq!(broker_b.queue_size().await.expect("size B"), 0);
    assert_eq!(broker_a.queue_size().await.expect("size A"), 1);
    assert!(broker_a.dequeue().await.expect("dequeue from A").is_some());
}

#[tokio::test]
async fn skip_locked_hands_distinct_rows_to_concurrent_claimers() {
    let (broker1, queue) = broker_or_skip!("skip_locked_hands_distinct_rows");
    let url = match test_pg_url() {
        Some(url) => url,
        None => return,
    };
    let broker2 = PostgresBroker::with_queue(&url, &queue)
        .await
        .expect("second broker on the same queue");

    for i in 0..10u8 {
        broker1
            .enqueue(SerializedTask::new(format!("task_{i}"), vec![i]))
            .await
            .expect("enqueue");
    }

    let (msg1, msg2) = tokio::join!(broker1.dequeue(), broker2.dequeue());
    let msg1 = msg1.expect("dequeue 1").expect("a task for claimer 1");
    let msg2 = msg2.expect("dequeue 2").expect("a task for claimer 2");

    // Asserting on the DATABASE identity: with synthesised ids this assertion
    // passed vacuously even when both claimers took the same row.
    assert_ne!(msg1.task.metadata.id, msg2.task.metadata.id);
    for id in [msg1.task.metadata.id, msg2.task.metadata.id] {
        let stored = broker1
            .get_task(&id)
            .await
            .expect("get_task")
            .expect("claimed row exists");
        assert_eq!(stored.state, DbTaskState::Processing);
    }
    assert_eq!(broker1.queue_size().await.expect("queue_size"), 8);
}

#[tokio::test]
async fn dequeue_batch_returns_distinct_ackable_rows() {
    let (broker, _queue) = broker_or_skip!("dequeue_batch_returns_distinct_ackable_rows");

    let tasks: Vec<SerializedTask> = (0..5u8)
        .map(|i| SerializedTask::new(format!("batch_{i}"), vec![i]))
        .collect();
    let ids = broker.enqueue_batch(tasks).await.expect("enqueue_batch");
    assert_eq!(ids.len(), 5);

    let messages = broker.dequeue_batch(5).await.expect("dequeue_batch");
    assert_eq!(messages.len(), 5);

    let mut claimed: Vec<Uuid> = messages.iter().map(|m| m.task.metadata.id).collect();
    claimed.sort();
    claimed.dedup();
    assert_eq!(claimed.len(), 5, "every claimed id must be distinct");
    for id in &claimed {
        assert!(ids.contains(id), "claimed id must be one that was enqueued");
    }

    let acks: Vec<_> = messages
        .iter()
        .map(|m| (m.task.metadata.id, m.receipt_handle.clone()))
        .collect();
    broker.ack_batch(&acks).await.expect("ack_batch");

    for id in &claimed {
        let stored = broker
            .get_task(id)
            .await
            .expect("get_task")
            .expect("row present");
        assert_eq!(stored.state, DbTaskState::Completed);
    }
}

#[tokio::test]
async fn periodic_schedules_round_trip_through_their_own_table() {
    let (broker, _queue) = broker_or_skip!("periodic_schedules_round_trip");

    let schedule_id = broker
        .schedule_periodic_task(
            "daily_cleanup",
            "0 2 * * *",
            serde_json::json!({ "action": "cleanup", "max_age_days": 7 }),
            5,
        )
        .await
        .expect("schedule_periodic_task");

    let schedules = broker
        .list_periodic_schedules()
        .await
        .expect("list_periodic_schedules");
    assert_eq!(schedules.len(), 1);
    assert_eq!(schedules[0].schedule_id, schedule_id);
    assert_eq!(schedules[0].task_name, "daily_cleanup");
    assert_eq!(schedules[0].cron_expression, "0 2 * * *");
    assert_eq!(schedules[0].priority, 5);
    assert!(schedules[0].enabled);
    assert_eq!(schedules[0].payload["action"], "cleanup");

    // A schedule must never be claimable as if it were a task.
    assert!(broker.dequeue().await.expect("dequeue").is_none());
    assert_eq!(broker.queue_size().await.expect("queue_size"), 0);

    assert!(broker
        .cancel_periodic_schedule(&schedule_id)
        .await
        .expect("cancel_periodic_schedule"));
    assert!(broker
        .list_periodic_schedules()
        .await
        .expect("list after cancel")
        .is_empty());
}

#[tokio::test]
async fn archiving_moves_completed_tasks_into_the_history_table() {
    let (broker, _queue) = broker_or_skip!("archiving_moves_completed_tasks");

    let task_id = broker
        .enqueue(SerializedTask::new("archive_me".to_string(), vec![9]))
        .await
        .expect("enqueue");
    let msg = broker.dequeue().await.expect("dequeue").expect("a task");
    broker
        .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
        .await
        .expect("ack");

    // `batch_archive_completed` exercises the bounded `id IN (SELECT ... LIMIT
    // n)` predicate in both the INSERT and the DELETE.
    let archived = broker
        .batch_archive_completed(-1, 100)
        .await
        .expect("batch_archive_completed");
    assert_eq!(archived, 1, "the completed task should have been archived");
    assert!(
        broker.get_task(&task_id).await.expect("get_task").is_none(),
        "an archived task must be gone from the dispatch table"
    );

    // Assert the other half of the move: the row really landed in
    // `celers_task_history`. The historical `INSERT INTO celers_task_history
    // SELECT *` form could never do this — six history columns against
    // sixteen task columns is an arity error before any data moves.
    let conn = broker.connection().await.expect("pooled connection");
    let id_param = crate::row_ext::uuid_param(&task_id);
    let rows = conn
        .query(
            "SELECT state FROM celers_task_history WHERE task_id = $1",
            &[&id_param],
        )
        .await
        .expect("read celers_task_history");
    assert_eq!(
        rows.len(),
        1,
        "exactly one history row for the archived task"
    );
    let state: String = crate::row_ext::RowExt::col(&rows[0], "state").expect("history state");
    assert_eq!(state, "completed");
}

#[tokio::test]
async fn task_groups_remember_the_name_they_were_created_with() {
    let (broker, _queue) = broker_or_skip!("task_groups_remember_their_name");

    let group_id = broker
        .create_task_group("data_import_batch_2024_01", Some("Import customer data"))
        .await
        .expect("create_task_group");

    let found = broker
        .find_task_group_by_name("data_import_batch_2024_01")
        .await
        .expect("find_task_group_by_name")
        .expect("the group should be findable by name");
    assert_eq!(found.0, group_id);
    assert_eq!(found.1.as_deref(), Some("Import customer data"));

    let task_id = broker
        .enqueue(SerializedTask::new("grouped".to_string(), vec![]))
        .await
        .expect("enqueue");
    broker
        .add_tasks_to_group(&group_id, &[task_id])
        .await
        .expect("add_tasks_to_group");

    let status = broker
        .get_task_group_status(&group_id)
        .await
        .expect("get_task_group_status")
        .expect("group has members");
    assert_eq!(
        status.group_name, "data_import_batch_2024_01",
        "group_name must be the caller's name, not the group id echoed back"
    );
    assert_eq!(status.description.as_deref(), Some("Import customer data"));
    assert_eq!(status.total_tasks, 1);
}

#[tokio::test]
async fn cancel_is_queue_scoped_and_state_guarded() {
    let (broker, _queue) = broker_or_skip!("cancel_is_queue_scoped_and_state_guarded");

    let task_id = broker
        .enqueue(SerializedTask::new("cancel_me".to_string(), vec![]))
        .await
        .expect("enqueue");
    assert!(broker.cancel(&task_id).await.expect("cancel"));
    let stored = broker
        .get_task(&task_id)
        .await
        .expect("get_task")
        .expect("row present");
    assert_eq!(stored.state, DbTaskState::Cancelled);

    // Cancelling again matches no row (the state guard held).
    assert!(!broker.cancel(&task_id).await.expect("second cancel"));
    assert!(broker.dequeue().await.expect("dequeue").is_none());
}

#[tokio::test]
async fn retention_purges_only_terminal_tasks() {
    let (broker, _queue) = broker_or_skip!("retention_purges_only_terminal_tasks");

    let completed_id = broker
        .enqueue(SerializedTask::new("done".to_string(), vec![]))
        .await
        .expect("enqueue");
    let msg = broker.dequeue().await.expect("dequeue").expect("a task");
    broker
        .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
        .await
        .expect("ack");
    let pending_id = broker
        .enqueue(SerializedTask::new("still_waiting".to_string(), vec![]))
        .await
        .expect("enqueue pending");

    // Zero retention: everything terminal is eligible immediately, so the
    // assertion needs no sleeping.
    let deleted = broker
        .purge_terminal_tasks(std::time::Duration::from_secs(0), 100, 5)
        .await
        .expect("purge_terminal_tasks");
    assert_eq!(deleted, 1);
    assert!(broker
        .get_task(&completed_id)
        .await
        .expect("get_task")
        .is_none());
    assert!(broker
        .get_task(&pending_id)
        .await
        .expect("get_task")
        .is_some());
}

#[tokio::test]
async fn performance_baseline_survives_retention_and_archiving() {
    let (broker, _queue) = broker_or_skip!("performance_baseline_survives_retention_and_archiving");

    // The baseline is stored as a `state = 'completed'` marker row in
    // `celers_tasks` (see `analytics::store_performance_baseline`), which
    // makes it look terminal-and-eligible to both sweeps below. `sql.rs`
    // excludes `task_name = '__baseline__'` from both predicates; this
    // exercises that exclusion against a real server instead of just the
    // generated SQL text.
    broker
        .store_performance_baseline("hardening_pass")
        .await
        .expect("store_performance_baseline");

    let purged = broker
        .purge_terminal_tasks(std::time::Duration::from_secs(0), 100, 5)
        .await
        .expect("purge_terminal_tasks");
    assert_eq!(
        purged, 0,
        "retention must not delete the baseline marker row"
    );

    let archived = broker
        .batch_archive_completed(-1, 100)
        .await
        .expect("batch_archive_completed");
    assert_eq!(
        archived, 0,
        "archiving must not move the baseline marker row out of celers_tasks"
    );

    let comparison = broker
        .compare_to_baseline("hardening_pass")
        .await
        .expect("compare_to_baseline")
        .expect("baseline must still be readable after both sweeps");
    assert!(comparison.contains_key("throughput_change"));
}

#[tokio::test]
async fn pool_reports_real_occupancy_and_survives_concurrent_work() {
    let url = match test_pg_url() {
        Some(url) => url,
        None => {
            eprintln!("skipping pool_reports_real_occupancy: CELERS_TEST_POSTGRES_URL is not set");
            return;
        }
    };
    let broker = PostgresBroker::with_pool_config(&url, &unique_queue(), 4, 30)
        .await
        .expect("connect with a 4-slot pool");
    broker.migrate().await.expect("migrate");

    assert_eq!(broker.pool_size(), 4);
    broker.health_check().await.expect("health_check");

    // Drive several statements concurrently; with a real pool these run on
    // different connections instead of serialising behind one mutex.
    let results = tokio::join!(
        broker.queue_size(),
        broker.queue_size(),
        broker.queue_size(),
        broker.queue_size(),
    );
    for result in [results.0, results.1, results.2, results.3] {
        result.expect("concurrent queue_size");
    }

    let metrics = broker.get_pool_metrics();
    assert_eq!(metrics.max_size, 4);
    assert!(metrics.size > 0, "at least one connection is established");
    assert!(metrics.size <= metrics.max_size);
    assert_eq!(metrics.size, metrics.idle + metrics.in_use);

    // With honest metrics the health monitor no longer reports a canned
    // "optimal" from a hardcoded zero-sized pool.
    let health = broker.monitor_pool_health().await.expect("pool health");
    assert_eq!(health.max_size, 4);
    assert!(health.utilization_percent.is_finite());
}

// ========== Notifications ==========

#[tokio::test]
async fn create_notification_listener_subscribes_to_the_queues_channel() {
    let (broker, queue) =
        broker_or_skip!("create_notification_listener_subscribes_to_the_queues_channel");

    let listener = broker
        .create_notification_listener()
        .await
        .expect("create_notification_listener");
    assert_eq!(listener.channel(), format!("celers_tasks_{queue}"));
}

// ========== Deduplication ==========

#[tokio::test]
async fn enqueue_idempotent_returns_the_same_task_id_for_a_duplicate_key() {
    let (broker, _queue) = broker_or_skip!("enqueue_idempotent_returns_the_same_task_id");
    let config = DeduplicationConfig::default();

    let first_id = broker
        .enqueue_idempotent(
            SerializedTask::new("charge_card".to_string(), vec![1]),
            "order-42",
            &config,
        )
        .await
        .expect("enqueue_idempotent (first)");

    let second_id = broker
        .enqueue_idempotent(
            SerializedTask::new("charge_card".to_string(), vec![2]),
            "order-42",
            &config,
        )
        .await
        .expect("enqueue_idempotent (duplicate)");

    assert_eq!(
        first_id, second_id,
        "a duplicate idempotency key must return the original task id"
    );
    // Only the first call actually inserted a task row.
    assert_eq!(broker.queue_size().await.expect("queue_size"), 1);
}

#[tokio::test]
async fn check_deduplication_reports_the_stored_entry_and_none_for_an_unknown_key() {
    let (broker, _queue) = broker_or_skip!("check_deduplication_reports_the_stored_entry");
    let config = DeduplicationConfig::default();

    let task_id = broker
        .enqueue_idempotent(
            SerializedTask::new("send_receipt".to_string(), vec![]),
            "check-key",
            &config,
        )
        .await
        .expect("enqueue_idempotent");

    let info = broker
        .check_deduplication("check-key")
        .await
        .expect("check_deduplication")
        .expect("the key was just inserted");
    assert_eq!(info.task_id, task_id);
    assert_eq!(info.idempotency_key, "check-key");
    assert_eq!(info.task_name, "send_receipt");
    assert_eq!(info.duplicate_count, 0, "no duplicate has been seen yet");

    assert!(broker
        .check_deduplication("no-such-key")
        .await
        .expect("check_deduplication (miss)")
        .is_none());
}

#[tokio::test]
async fn cleanup_deduplication_deletes_only_expired_entries() {
    let (broker, _queue) = broker_or_skip!("cleanup_deduplication_deletes_only_expired_entries");

    // A negative window makes `expires_at` land in the past the moment the
    // row is written, without needing to sleep past a real TTL.
    let expired_config = DeduplicationConfig {
        enabled: true,
        window_secs: -5,
    };
    broker
        .enqueue_idempotent(
            SerializedTask::new("stale".to_string(), vec![]),
            "cleanup-expired-key",
            &expired_config,
        )
        .await
        .expect("enqueue_idempotent (already expired)");

    let live_config = DeduplicationConfig::default();
    broker
        .enqueue_idempotent(
            SerializedTask::new("fresh".to_string(), vec![]),
            "cleanup-live-key",
            &live_config,
        )
        .await
        .expect("enqueue_idempotent (still active)");

    // `cleanup_deduplication` deletes across every queue on the database
    // (see deduplication.rs — unlike its sibling queries here it does not
    // filter on `queue_name`), so assert on this test's own two keys rather
    // than the aggregate deleted count.
    broker
        .cleanup_deduplication()
        .await
        .expect("cleanup_deduplication");

    let conn = broker.connection().await.expect("pooled connection");
    let expired_key = "cleanup-expired-key";
    let live_key = "cleanup-live-key";
    let remaining = conn
        .query(
            "SELECT idempotency_key FROM celers_deduplication WHERE idempotency_key IN ($1, $2)",
            &[&expired_key, &live_key],
        )
        .await
        .expect("read celers_deduplication");
    let remaining_keys: Vec<String> = remaining
        .iter()
        .map(|row| crate::row_ext::RowExt::col(row, "idempotency_key").expect("idempotency_key"))
        .collect();
    assert!(
        !remaining_keys.contains(&expired_key.to_string()),
        "the expired entry must be gone"
    );
    assert!(
        remaining_keys.contains(&live_key.to_string()),
        "the still-active entry must survive"
    );
}

#[tokio::test]
async fn get_deduplication_stats_counts_active_entries_and_blocked_duplicates() {
    let (broker, _queue) = broker_or_skip!("get_deduplication_stats_counts_active_entries");
    let config = DeduplicationConfig::default();

    broker
        .enqueue_idempotent(
            SerializedTask::new("send_invoice".to_string(), vec![]),
            "stats-key",
            &config,
        )
        .await
        .expect("enqueue_idempotent (first)");
    // A duplicate call against the same key increments duplicate_count
    // without inserting a second task.
    broker
        .enqueue_idempotent(
            SerializedTask::new("send_invoice".to_string(), vec![]),
            "stats-key",
            &config,
        )
        .await
        .expect("enqueue_idempotent (duplicate)");

    let (active_entries, total_duplicates) = broker
        .get_deduplication_stats()
        .await
        .expect("get_deduplication_stats");
    assert!(active_entries >= 1);
    assert!(total_duplicates >= 1);
}

// ========== TTL expiry ==========

#[tokio::test]
async fn expire_tasks_by_ttl_only_touches_the_named_task_type() {
    let (broker, _queue) = broker_or_skip!("expire_tasks_by_ttl_only_touches_the_named_task_type");

    let target_id = broker
        .enqueue(SerializedTask::new("stale_report".to_string(), vec![]))
        .await
        .expect("enqueue target");
    let other_id = broker
        .enqueue(SerializedTask::new("other_task".to_string(), vec![]))
        .await
        .expect("enqueue other");

    // A negative TTL makes `created_at < NOW() - ttl_secs seconds` true the
    // instant the row exists, without sleeping past a real TTL.
    let expired = broker
        .expire_tasks_by_ttl("stale_report", -1)
        .await
        .expect("expire_tasks_by_ttl");
    assert_eq!(expired, 1);

    let target = broker
        .get_task(&target_id)
        .await
        .expect("get_task target")
        .expect("row present");
    assert_eq!(target.state, DbTaskState::Cancelled);
    assert_eq!(
        target.error_message.as_deref(),
        Some("Task expired due to TTL")
    );

    let other = broker
        .get_task(&other_id)
        .await
        .expect("get_task other")
        .expect("row present");
    assert_eq!(
        other.state,
        DbTaskState::Pending,
        "a different task_name must not be touched"
    );
}

#[tokio::test]
async fn expire_all_tasks_by_ttl_touches_every_task_type_in_the_queue() {
    let (broker, _queue) = broker_or_skip!("expire_all_tasks_by_ttl_touches_every_task_type");

    broker
        .enqueue(SerializedTask::new("type_a".to_string(), vec![]))
        .await
        .expect("enqueue a");
    broker
        .enqueue(SerializedTask::new("type_b".to_string(), vec![]))
        .await
        .expect("enqueue b");

    let expired = broker
        .expire_all_tasks_by_ttl(-1)
        .await
        .expect("expire_all_tasks_by_ttl");
    assert_eq!(expired, 2);
    assert_eq!(broker.queue_size().await.expect("queue_size"), 0);
}

// ========== Performance analytics ==========

#[tokio::test]
async fn get_task_percentiles_returns_ordered_finite_percentiles() {
    let (broker, _queue) =
        broker_or_skip!("get_task_percentiles_returns_ordered_finite_percentiles");

    for _ in 0..3 {
        broker
            .enqueue(SerializedTask::new("timed_task".to_string(), vec![]))
            .await
            .expect("enqueue");
        let msg = broker.dequeue().await.expect("dequeue").expect("a task");
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        broker
            .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
            .await
            .expect("ack");
    }

    let (p50, p95, p99) = broker
        .get_task_percentiles("timed_task")
        .await
        .expect("get_task_percentiles");
    assert!(p50.is_finite() && p50 >= 0.0);
    assert!(p95.is_finite() && p95 >= p50);
    assert!(p99.is_finite() && p99 >= p95);
}

#[tokio::test]
async fn get_slowest_tasks_orders_by_duration_and_respects_the_limit() {
    let (broker, _queue) =
        broker_or_skip!("get_slowest_tasks_orders_by_duration_and_respects_the_limit");

    for i in 0..3u64 {
        broker
            .enqueue(SerializedTask::new(format!("slow_task_{i}"), vec![]))
            .await
            .expect("enqueue");
        let msg = broker.dequeue().await.expect("dequeue").expect("a task");
        // Stagger the runtimes so ordering is meaningfully exercised, not
        // just three near-identical durations.
        tokio::time::sleep(std::time::Duration::from_millis(5 * (i + 1))).await;
        broker
            .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
            .await
            .expect("ack");
    }

    let slowest = broker
        .get_slowest_tasks(2)
        .await
        .expect("get_slowest_tasks");
    assert_eq!(slowest.len(), 2, "the limit must be respected");
    let duration = |t: &crate::TaskInfo| {
        t.completed_at.expect("completed_at") - t.started_at.expect("started_at")
    };
    assert!(
        duration(&slowest[0]) >= duration(&slowest[1]),
        "results must be ordered slowest first"
    );
}

// ========== Rate limiting ==========

#[tokio::test]
async fn get_task_rate_counts_completions_within_the_window() {
    let (broker, _queue) = broker_or_skip!("get_task_rate_counts_completions_within_the_window");

    for _ in 0..2 {
        broker
            .enqueue(SerializedTask::new("rated_task".to_string(), vec![]))
            .await
            .expect("enqueue");
        let msg = broker.dequeue().await.expect("dequeue").expect("a task");
        broker
            .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
            .await
            .expect("ack");
    }

    let rate = broker
        .get_task_rate("rated_task", 3600)
        .await
        .expect("get_task_rate");
    assert_eq!(rate, 2);

    // A window that could not possibly contain "now" sees nothing: by the
    // time this second query runs, every completed row's `completed_at` is
    // already in the past.
    let empty_rate = broker
        .get_task_rate("rated_task", 0)
        .await
        .expect("get_task_rate (zero window)");
    assert_eq!(empty_rate, 0);
}

#[tokio::test]
async fn is_rate_limited_compares_the_live_rate_against_the_configured_max() {
    let (broker, _queue) = broker_or_skip!("is_rate_limited_compares_the_live_rate_against_max");

    for _ in 0..3 {
        broker
            .enqueue(SerializedTask::new("throttled_task".to_string(), vec![]))
            .await
            .expect("enqueue");
        let msg = broker.dequeue().await.expect("dequeue").expect("a task");
        broker
            .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
            .await
            .expect("ack");
    }

    assert!(
        broker
            .is_rate_limited("throttled_task", 3, 3600)
            .await
            .expect("is_rate_limited (at max)"),
        "a rate equal to max_count must count as limited"
    );
    assert!(!broker
        .is_rate_limited("throttled_task", 10, 3600)
        .await
        .expect("is_rate_limited (under max)"));
}

// ========== Dynamic priority ==========

#[tokio::test]
async fn boost_task_priority_raises_pending_tasks_of_the_named_type_only() {
    let (broker, _queue) =
        broker_or_skip!("boost_task_priority_raises_pending_tasks_of_the_named_type");

    let boosted_id = broker
        .enqueue(SerializedTask::new("boost_me".to_string(), vec![]).with_priority(10))
        .await
        .expect("enqueue boosted");
    let other_id = broker
        .enqueue(SerializedTask::new("leave_me".to_string(), vec![]).with_priority(10))
        .await
        .expect("enqueue other");

    let updated = broker
        .boost_task_priority("boost_me", 5)
        .await
        .expect("boost_task_priority");
    assert_eq!(updated, 1);

    let boosted = broker
        .get_task(&boosted_id)
        .await
        .expect("get_task boosted")
        .expect("row present");
    assert_eq!(boosted.priority, 15);

    let other = broker
        .get_task(&other_id)
        .await
        .expect("get_task other")
        .expect("row present");
    assert_eq!(
        other.priority, 10,
        "a different task_name must not be boosted"
    );
}

#[tokio::test]
async fn set_task_priority_overwrites_the_priority_of_named_ids_only() {
    let (broker, _queue) =
        broker_or_skip!("set_task_priority_overwrites_the_priority_of_named_ids");

    let target_id = broker
        .enqueue(SerializedTask::new("priority_target".to_string(), vec![]).with_priority(1))
        .await
        .expect("enqueue target");
    let untouched_id = broker
        .enqueue(SerializedTask::new("priority_bystander".to_string(), vec![]).with_priority(1))
        .await
        .expect("enqueue bystander");

    let updated = broker
        .set_task_priority(&[target_id], 999)
        .await
        .expect("set_task_priority");
    assert_eq!(updated, 1);

    let target = broker
        .get_task(&target_id)
        .await
        .expect("get_task target")
        .expect("row present");
    assert_eq!(target.priority, 999);

    let untouched = broker
        .get_task(&untouched_id)
        .await
        .expect("get_task bystander")
        .expect("row present");
    assert_eq!(untouched.priority, 1, "ids not passed in must be untouched");

    // Empty input is a documented no-op, not an error.
    let noop = broker
        .set_task_priority(&[], 42)
        .await
        .expect("set_task_priority (empty)");
    assert_eq!(noop, 0);
}

// ========== DLQ analytics ==========

#[tokio::test]
async fn get_dlq_stats_by_task_groups_counts_by_task_name() {
    let (broker, _queue) = broker_or_skip!("get_dlq_stats_by_task_groups_counts_by_task_name");

    fail_task_into_dlq(&broker, "dlq_task_a", "boom a").await;
    fail_task_into_dlq(&broker, "dlq_task_a", "boom a again").await;
    fail_task_into_dlq(&broker, "dlq_task_b", "boom b").await;

    let stats = broker
        .get_dlq_stats_by_task()
        .await
        .expect("get_dlq_stats_by_task");
    assert_eq!(stats.get("dlq_task_a").copied(), Some(2));
    assert_eq!(stats.get("dlq_task_b").copied(), Some(1));
}

#[tokio::test]
async fn get_dlq_error_patterns_ranks_the_most_common_error_first() {
    let (broker, _queue) =
        broker_or_skip!("get_dlq_error_patterns_ranks_the_most_common_error_first");

    fail_task_into_dlq(&broker, "flaky_a", "connection reset").await;
    fail_task_into_dlq(&broker, "flaky_b", "connection reset").await;
    fail_task_into_dlq(&broker, "flaky_c", "out of memory").await;

    let patterns = broker
        .get_dlq_error_patterns(10)
        .await
        .expect("get_dlq_error_patterns");
    assert_eq!(
        patterns
            .first()
            .map(|(msg, count)| (msg.as_deref(), *count)),
        Some((Some("connection reset"), 2)),
        "the more frequent error must sort first"
    );

    let limited = broker
        .get_dlq_error_patterns(1)
        .await
        .expect("get_dlq_error_patterns (limit 1)");
    assert_eq!(limited.len(), 1, "the limit must be respected");
}

#[tokio::test]
async fn get_recent_dlq_tasks_only_returns_failures_within_the_window() {
    let (broker, _queue) =
        broker_or_skip!("get_recent_dlq_tasks_only_returns_failures_within_window");

    let dlq_task_id = fail_task_into_dlq(&broker, "recent_failure", "timed out").await;

    let recent = broker
        .get_recent_dlq_tasks(3600)
        .await
        .expect("get_recent_dlq_tasks");
    assert!(
        recent.iter().any(|t| t.task_id == dlq_task_id),
        "the task just failed must show up inside the window"
    );

    let too_narrow = broker
        .get_recent_dlq_tasks(0)
        .await
        .expect("get_recent_dlq_tasks (zero window)");
    assert!(
        !too_narrow.iter().any(|t| t.task_id == dlq_task_id),
        "a zero-width window must not include a past failure"
    );
}

// ========== Cancellation with reason ==========

#[tokio::test]
async fn cancel_with_reason_records_a_prefixed_error_message() {
    let (broker, _queue) = broker_or_skip!("cancel_with_reason_records_a_prefixed_error_message");

    let task_id = broker
        .enqueue(SerializedTask::new(
            "cancel_reason_target".to_string(),
            vec![],
        ))
        .await
        .expect("enqueue");

    broker
        .cancel_with_reason(&task_id, "duplicate submission")
        .await
        .expect("cancel_with_reason");

    let stored = broker
        .get_task(&task_id)
        .await
        .expect("get_task")
        .expect("row present");
    assert_eq!(stored.state, DbTaskState::Cancelled);
    assert_eq!(
        stored.error_message.as_deref(),
        Some("Cancelled: duplicate submission")
    );
}

#[tokio::test]
async fn cancel_batch_with_reason_cancels_only_the_listed_ids() {
    let (broker, _queue) = broker_or_skip!("cancel_batch_with_reason_cancels_only_the_listed_ids");

    let cancel_id_1 = broker
        .enqueue(SerializedTask::new("batch_cancel_a".to_string(), vec![]))
        .await
        .expect("enqueue a");
    let cancel_id_2 = broker
        .enqueue(SerializedTask::new("batch_cancel_b".to_string(), vec![]))
        .await
        .expect("enqueue b");
    let survivor_id = broker
        .enqueue(SerializedTask::new(
            "batch_cancel_survivor".to_string(),
            vec![],
        ))
        .await
        .expect("enqueue survivor");

    let cancelled = broker
        .cancel_batch_with_reason(&[cancel_id_1, cancel_id_2], "maintenance window")
        .await
        .expect("cancel_batch_with_reason");
    assert_eq!(cancelled, 2);

    for id in [cancel_id_1, cancel_id_2] {
        let stored = broker
            .get_task(&id)
            .await
            .expect("get_task")
            .expect("row present");
        assert_eq!(stored.state, DbTaskState::Cancelled);
        assert_eq!(
            stored.error_message.as_deref(),
            Some("Cancelled: maintenance window")
        );
    }

    let survivor = broker
        .get_task(&survivor_id)
        .await
        .expect("get_task survivor")
        .expect("row present");
    assert_eq!(survivor.state, DbTaskState::Pending);

    // Empty input is a documented no-op, not an error.
    let noop = broker
        .cancel_batch_with_reason(&[], "unused")
        .await
        .expect("cancel_batch_with_reason (empty)");
    assert_eq!(noop, 0);
}

#[tokio::test]
async fn get_cancellation_reasons_groups_and_counts_reasons() {
    let (broker, _queue) = broker_or_skip!("get_cancellation_reasons_groups_and_counts_reasons");

    let id_a = broker
        .enqueue(SerializedTask::new("reason_a".to_string(), vec![]))
        .await
        .expect("enqueue a");
    let id_b = broker
        .enqueue(SerializedTask::new("reason_b".to_string(), vec![]))
        .await
        .expect("enqueue b");
    let id_c = broker
        .enqueue(SerializedTask::new("reason_c".to_string(), vec![]))
        .await
        .expect("enqueue c");

    broker
        .cancel_with_reason(&id_a, "budget cut")
        .await
        .expect("cancel a");
    broker
        .cancel_with_reason(&id_b, "budget cut")
        .await
        .expect("cancel b");
    broker
        .cancel_with_reason(&id_c, "operator request")
        .await
        .expect("cancel c");

    let reasons = broker
        .get_cancellation_reasons(10)
        .await
        .expect("get_cancellation_reasons");
    let budget_cut_count = reasons
        .iter()
        .find(|(reason, _)| reason.as_deref() == Some("Cancelled: budget cut"))
        .map(|(_, count)| *count);
    assert_eq!(budget_cut_count, Some(2));
    let operator_count = reasons
        .iter()
        .find(|(reason, _)| reason.as_deref() == Some("Cancelled: operator request"))
        .map(|(_, count)| *count);
    assert_eq!(operator_count, Some(1));
}
