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

use crate::{DbTaskState, PostgresBroker, RetryStrategy};

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
