//! Regression tests for the poison-pill quarantine gate (repeated failures
//! stop a task from ever running again) and for the health accounting the
//! worker and its handle share.

use super::doubles::{serialized, wait_until, AlwaysFailingTask, CountingTask, RecordingBroker};

use crate::types::WorkerConfig;
use crate::worker_core::Worker;

use celers_core::{BrokerMessage, NoOpEventEmitter, TaskId, TaskRegistry};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// A task that keeps failing accumulates strikes and, once quarantined, is
/// dead-lettered without ever executing again — instead of cycling through the
/// broker forever.
#[tokio::test]
async fn test_poison_pill_quarantine_stops_a_repeatedly_failing_task() {
    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(AlwaysFailingTask {
            runs: Arc::clone(&runs),
        })
        .await;

    // Budget for 5 attempts; quarantine trips after 2 failed executions.
    let task = serialized("failing_task").with_max_retries(5);
    let task_id = task.metadata.id;
    let broker = RecordingBroker::new(vec![BrokerMessage::new(task)], true);

    let detector = Arc::new(crate::poison_pill::PoisonPillDetector::new(
        crate::poison_pill::PoisonPillConfig::new().with_threshold(2),
    ));

    let config = WorkerConfig {
        poll_interval_ms: 10,
        enable_dlq: true,
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config)
            .with_poison_pill(Arc::clone(&detector));
    let dlq = worker.dlq_handler().cloned().expect("dlq enabled");
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    let mut quarantined = false;
    for _ in 0..400 {
        if detector.is_poison(&task_id).await {
            quarantined = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        quarantined,
        "two failed executions must trip a threshold-2 detector (runs: {})",
        runs.load(Ordering::Relaxed)
    );

    // Once quarantined the task is dead-lettered on its next delivery instead
    // of being executed again.
    wait_until("the quarantined task to be dead-lettered", || {
        !broker.rejected().is_empty()
    })
    .await;
    let runs_at_quarantine = runs.load(Ordering::Relaxed);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        runs.load(Ordering::Relaxed),
        runs_at_quarantine,
        "a quarantined task must never execute again"
    );

    let entries = dlq.get_entries().await;
    assert!(
        entries.iter().any(
            |entry| entry.metadata.get("failure_type").map(String::as_str) == Some("poison_pill")
        ),
        "the quarantine must be observable in the DLQ: {:?}",
        entries
            .iter()
            .map(|entry| entry.metadata.clone())
            .collect::<Vec<_>>()
    );

    handle.shutdown().await.expect("shutdown");
}

/// A re-attempt whose previous failure this worker never saw (another worker's,
/// or one that died mid-task) is the only evidence a poison pill leaves, so it
/// gets its own strike. A retry this worker *did* fail must not be
/// double-counted.
#[tokio::test]
async fn test_redelivery_strike_only_counts_unseen_failures() {
    let broker = RecordingBroker::new(Vec::new(), false);
    let detector = Arc::new(crate::poison_pill::PoisonPillDetector::new(
        crate::poison_pill::PoisonPillConfig::new().with_threshold(10),
    ));
    let worker: Worker<RecordingBroker, NoOpEventEmitter> = Worker::new_from_arc(
        Arc::clone(&broker),
        TaskRegistry::new(),
        WorkerConfig::default(),
    )
    .with_poison_pill(Arc::clone(&detector));

    // A first delivery is never a redelivery.
    let fresh = TaskId::new_v4();
    assert!(!worker.is_quarantined(fresh, 0).await);
    assert_eq!(detector.strike_count(&fresh).await, 0);

    // A re-attempt this detector has no record of: strike.
    let foreign = TaskId::new_v4();
    assert!(!worker.is_quarantined(foreign, 1).await);
    assert_eq!(detector.strike_count(&foreign).await, 1);

    // A re-attempt whose failure this worker already recorded: no second strike.
    let ours = TaskId::new_v4();
    detector.record_failure(ours, "boom").await;
    assert_eq!(detector.strike_count(&ours).await, 1);
    assert!(!worker.is_quarantined(ours, 2).await);
    assert_eq!(
        detector.strike_count(&ours).await,
        1,
        "a failure this worker recorded must not be counted twice on redelivery"
    );

    // Without a detector the gate is a no-op.
    let bare: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(broker, TaskRegistry::new(), WorkerConfig::default());
    assert!(!bare.is_quarantined(TaskId::new_v4(), 9).await);
}

/// Health accounting is fed by real executions and readable through the handle
/// the worker leaves behind, so an embedder can serve liveness/readiness.
#[tokio::test]
async fn test_health_tracks_real_task_outcomes() {
    let ok_runs = Arc::new(AtomicUsize::new(0));
    let bad_runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&ok_runs),
            name: "quick_task",
        })
        .await;
    registry
        .register(AlwaysFailingTask {
            runs: Arc::clone(&bad_runs),
        })
        .await;

    let messages = vec![
        BrokerMessage::new(serialized("quick_task")),
        BrokerMessage::new(serialized("failing_task").with_max_retries(0)),
    ];
    let broker = RecordingBroker::new(messages, false);

    let config = WorkerConfig {
        poll_interval_ms: 10,
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let health = worker.health();
    assert_eq!(health.get_health().tasks_processed, 0);

    let handle = worker.run_with_shutdown().await.expect("worker starts");
    // The handle exposes the same shared accounting.
    let from_handle = handle.health();

    wait_until("both tasks to be accounted for", || {
        let info = from_handle.get_health();
        info.tasks_processed == 1 && info.tasks_failed == 1
    })
    .await;

    let info = health.get_health();
    assert_eq!(info.tasks_processed, 1, "one success");
    assert_eq!(info.tasks_failed, 1, "one failure");
    assert_eq!(info.consecutive_failures, 1);
    wait_until("the worker to report itself idle again", || {
        !health.get_health().is_processing
    })
    .await;

    handle.shutdown().await.expect("shutdown");
}
