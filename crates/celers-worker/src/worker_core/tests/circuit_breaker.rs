//! Regression tests for the per-task circuit breaker wired into the run
//! loop: every failed attempt counts (not just the terminal one), an open
//! circuit dead-letters instead of vanishing silently, and a half-open
//! probe-budget miss defers rather than dead-lettering.

use super::doubles::{
    serialized, wait_until, AlwaysFailingTask, BlockingTask, CapturingEmitter, CountingTask,
    RecordingBroker,
};

use crate::types::WorkerConfig;
use crate::worker_core::Worker;

use celers_core::{BrokerMessage, NoOpEventEmitter, TaskRegistry};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

/// idx 172: the circuit breaker sees *every* failed execution, not just the
/// final retries-exhausted one. With a threshold of 2, two failed attempts of
/// the same task must trip the circuit — previously it took
/// `threshold * (max_retries + 1)` real failures.
#[tokio::test]
async fn test_circuit_breaker_counts_every_failed_attempt() {
    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(AlwaysFailingTask {
            runs: Arc::clone(&runs),
        })
        .await;

    // One retry: two executions in total, both failures.
    let task = serialized("failing_task").with_max_retries(1);
    let broker = RecordingBroker::new(vec![BrokerMessage::new(task)], true);

    let config = WorkerConfig {
        poll_interval_ms: 10,
        enable_circuit_breaker: true,
        circuit_breaker_config: crate::circuit_breaker::CircuitBreakerConfig {
            failure_threshold: 2,
            ..Default::default()
        },
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let breaker = worker.circuit_breaker.clone().expect("breaker enabled");
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    let mut opened = false;
    for _ in 0..400 {
        if breaker.get_state("failing_task").await.is_open() {
            opened = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(
        opened,
        "two failed executions must trip a threshold-2 breaker (runs: {})",
        runs.load(Ordering::Relaxed)
    );
    assert_eq!(
        runs.load(Ordering::Relaxed),
        2,
        "the retry budget allows exactly two attempts"
    );

    handle.shutdown().await.expect("shutdown");
}

/// idx 173: an open circuit is a terminal, observable outcome — `task-failed`
/// plus a DLQ entry — not a silent disappearance.
#[tokio::test]
async fn test_open_circuit_emits_failure_and_dead_letters() {
    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&runs),
            name: "quick_task",
        })
        .await;

    let task = serialized("quick_task");
    let task_id = task.metadata.id;
    let broker = RecordingBroker::new(vec![BrokerMessage::new(task)], false);

    let config = WorkerConfig {
        poll_interval_ms: 10,
        enable_circuit_breaker: true,
        circuit_breaker_config: crate::circuit_breaker::CircuitBreakerConfig {
            failure_threshold: 1,
            ..Default::default()
        },
        enable_dlq: true,
        enable_events: true,
        ..Default::default()
    };
    let emitter = CapturingEmitter::default();
    let worker =
        Worker::with_event_emitter_from_arc(Arc::clone(&broker), registry, config, emitter.clone());
    let dlq = worker.dlq_handler().cloned().expect("dlq enabled");
    let breaker = worker.circuit_breaker.clone().expect("breaker enabled");

    // Trip the breaker before the worker sees the message.
    breaker.record_failure("quick_task").await;

    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("the task to be dead-lettered", || {
        !broker.rejected().is_empty()
    })
    .await;

    assert_eq!(broker.rejected(), vec![task_id]);
    assert_eq!(runs.load(Ordering::Relaxed), 0, "the task must not run");
    assert_eq!(dlq.size().await, 1, "an open circuit records a DLQ entry");

    wait_until("a task-failed event", || {
        emitter.task_event_names().contains(&"failed")
    })
    .await;

    handle.shutdown().await.expect("shutdown");
}

/// idx 172, worker-loop half: a task rejected because the *half-open* probe
/// budget is in use is deferred (requeued), not dead-lettered. Only a genuinely
/// OPEN circuit is terminal — a recovering circuit must not permanently fail
/// the traffic it is about to start serving again.
#[tokio::test]
async fn test_half_open_probe_budget_defers_instead_of_dead_lettering() {
    let started = Arc::new(AtomicUsize::new(0));
    let finished = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());
    let registry = TaskRegistry::new();
    registry
        .register(BlockingTask {
            started: Arc::clone(&started),
            finished: Arc::clone(&finished),
            release: Arc::clone(&release),
        })
        .await;

    let messages = vec![
        BrokerMessage::new(serialized("blocking_task")),
        BrokerMessage::new(serialized("blocking_task")),
    ];
    let broker = RecordingBroker::new(messages, false);

    let config = WorkerConfig {
        poll_interval_ms: 10,
        concurrency: 4,
        enable_dlq: true,
        enable_circuit_breaker: true,
        circuit_breaker_config: crate::circuit_breaker::CircuitBreakerConfig {
            failure_threshold: 1,
            success_threshold: 5,
            // Already past the recovery window, so the first `should_allow`
            // flips the circuit straight to half-open.
            timeout_secs: 0,
            window_secs: 60,
            half_open_max_concurrent: 1,
        },
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let breaker = worker.circuit_breaker.clone().expect("breaker enabled");
    let dlq = worker.dlq_handler().cloned().expect("dlq enabled");

    // Open the circuit before the worker ever polls.
    breaker.record_failure("blocking_task").await;
    assert!(breaker.get_state("blocking_task").await.is_open());

    let handle = worker.run_with_shutdown().await.expect("worker starts");

    // The first task takes the single probe slot and blocks; the second finds
    // the budget spent.
    wait_until("the probe task to start", || {
        started.load(Ordering::Relaxed) == 1
    })
    .await;
    wait_until("the over-budget task to be requeued", || {
        !broker.requeued().is_empty()
    })
    .await;

    assert!(
        broker.rejected().is_empty(),
        "a half-open probe-budget miss must never be dead-lettered"
    );
    assert_eq!(
        dlq.size().await,
        0,
        "a deferred task is not a failed task: {:?}",
        dlq.get_entries().await
    );
    assert!(
        breaker.get_state("blocking_task").await.is_half_open(),
        "the circuit is still probing"
    );

    release.notify_waiters();
    handle.shutdown().await.expect("shutdown");
}
