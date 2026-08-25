//! Regression tests for one failed attempt: backoff arithmetic, panic
//! cleanup, and the worker-driven retry state machine that terminates a
//! permanently failing task after exactly `max_retries` attempts.

use super::doubles::{serialized, wait_until, AlwaysFailingTask, PanickingTask, RecordingBroker};

use crate::types::WorkerConfig;
use crate::worker_core::Worker;

use celers_core::{BrokerMessage, NoOpEventEmitter, TaskRegistry, TaskState};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[tokio::test]
async fn test_backoff_delay_does_not_overflow_for_large_retry_counts() {
    // `base * 2u64.pow(retry_count)` panicked on overflow for retry_count >= 64.
    let broker = RecordingBroker::new(Vec::new(), false);
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(broker, TaskRegistry::new(), WorkerConfig::default());

    assert_eq!(worker.calculate_backoff_delay(0).as_millis(), 1000);
    assert_eq!(worker.calculate_backoff_delay(1).as_millis(), 2000);
    assert_eq!(worker.calculate_backoff_delay(64).as_millis(), 60_000);
    assert_eq!(worker.calculate_backoff_delay(u32::MAX).as_millis(), 60_000);
}

/// idx 158: a panicking handler must not skip cleanup. The task is failed like
/// any other error (so it is acked/rejected rather than stranded), the active
/// count returns to zero and the panic is counted.
#[tokio::test]
async fn test_panicking_task_is_failed_and_cleans_up() {
    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(PanickingTask {
            runs: Arc::clone(&runs),
        })
        .await;

    let task = serialized("panicking_task").with_max_retries(0);
    let task_id = task.metadata.id;
    let broker = RecordingBroker::new(vec![BrokerMessage::new(task)], false);

    let config = WorkerConfig {
        poll_interval_ms: 10,
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let stats = worker.stats_arc();
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("panicking task to be rejected", || {
        !broker.rejected().is_empty()
    })
    .await;

    assert_eq!(broker.rejected(), vec![task_id]);
    assert!(
        broker.acked().is_empty(),
        "a panicked task must not be acked"
    );
    assert_eq!(runs.load(Ordering::Relaxed), 1);
    assert_eq!(stats.panicked(), 1, "the panic must be counted");

    // Cleanup ran: the active counter is back to zero, so drain can complete.
    wait_until("active count to return to zero", || stats.active() == 0).await;
    assert_eq!(stats.processed(), 1);

    handle.shutdown().await.expect("shutdown");
}

/// idx 166 + 167: the worker itself advances the retry state and applies the
/// configured backoff, so a permanently failing task terminates after exactly
/// `max_retries` attempts even on a broker whose `reject(requeue = true)`
/// returns the task unchanged.
#[tokio::test]
async fn test_retry_advances_state_and_terminates_after_max_retries() {
    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(AlwaysFailingTask {
            runs: Arc::clone(&runs),
        })
        .await;

    // One retry allowed: attempt 0 re-enqueues with Retrying(1), attempt 1 is
    // terminal.
    let task = serialized("failing_task").with_max_retries(1);
    let task_id = task.metadata.id;
    let broker = RecordingBroker::new(vec![BrokerMessage::new(task)], true);

    let config = WorkerConfig {
        poll_interval_ms: 10,
        enable_dlq: true,
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let dlq = worker.dlq_handler().cloned().expect("dlq enabled");
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("failing task to dead-letter", || {
        !broker.rejected().is_empty()
    })
    .await;

    // Exactly two executions: the original and one retry.
    assert_eq!(
        runs.load(Ordering::Relaxed),
        2,
        "task must stop after its retry budget is spent"
    );

    let enqueued = broker.enqueued();
    assert_eq!(enqueued.len(), 1, "exactly one retry copy is re-enqueued");
    let (retry_task, delay_secs) = &enqueued[0];
    assert_eq!(retry_task.metadata.id, task_id, "retry keeps the task id");
    assert_eq!(
        retry_task.metadata.state,
        TaskState::Retrying(1),
        "the worker must advance the retry state itself"
    );
    assert_eq!(
        *delay_secs, 1,
        "the retry must be scheduled with the configured backoff"
    );

    // The original delivery was acked (the retry copy replaced it) and the
    // final attempt was dead-lettered.
    assert_eq!(broker.acked(), vec![task_id]);
    assert_eq!(broker.rejected(), vec![task_id]);
    assert_eq!(dlq.size().await, 1, "the terminal failure lands in the DLQ");

    handle.shutdown().await.expect("shutdown");
}
