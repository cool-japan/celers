//! Regression tests for the worker's canvas integration: running every step
//! of a chain, the legacy bare `on_success_link`, and why an unclaimed
//! (shutdown-drained) delivery must not double-enqueue the chain successor.
//!
//! The whole module is gated on the `canvas` feature by its `mod` declaration
//! in [`super`]; the per-item `#[cfg(feature = "canvas")]` attributes below
//! are the original, harmlessly redundant markers.

use super::doubles::{serialized, wait_until, BlockingTask, CountingTask, RecordingBroker};

use crate::types::WorkerConfig;
use crate::worker_core::Worker;

use celers_core::{BrokerMessage, NoOpEventEmitter, TaskRegistry};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

/// The success path must advance the task's workflow. Before this,
/// `workflows::handle_workflow_completion` had zero production callers: chain
/// continuation, branch/switch evaluation and chord barriers were implemented
/// and unit-proven but never invoked by a running worker, so every chain
/// stopped after its first step.
#[cfg(feature = "canvas")]
#[tokio::test]
async fn test_worker_runs_every_step_of_a_canvas_chain() {
    let step_a = Arc::new(AtomicUsize::new(0));
    let step_b = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&step_a),
            name: "chain_step_a",
        })
        .await;
    registry
        .register(CountingTask {
            runs: Arc::clone(&step_b),
            name: "chain_step_b",
        })
        .await;

    // `redeliver: true` makes everything the worker enqueues available for the
    // next dequeue, so the chain's own continuation comes back around.
    let broker = RecordingBroker::new(Vec::new(), true);
    celers_canvas::Chain::new()
        .then("chain_step_a", Vec::new())
        .then("chain_step_b", Vec::new())
        .apply(&*broker)
        .await
        .expect("chain dispatches");

    let config = WorkerConfig {
        poll_interval_ms: 10,
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("the chain's second step to run", || {
        step_b.load(Ordering::Relaxed) == 1
    })
    .await;

    assert_eq!(step_a.load(Ordering::Relaxed), 1);
    assert_eq!(
        step_b.load(Ordering::Relaxed),
        1,
        "the worker must enqueue the chain tail after the head succeeds"
    );

    handle.shutdown().await.expect("shutdown");
}

/// The legacy `on_success_link` path (a bare successor *name*, no canvas tail)
/// is driven by the same call site.
#[cfg(feature = "canvas")]
#[tokio::test]
async fn test_worker_enqueues_a_bare_on_success_link() {
    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&runs),
            name: "quick_task",
        })
        .await;

    let task = serialized("quick_task").with_on_success_link("follow_up".to_string());
    let broker = RecordingBroker::new(vec![BrokerMessage::new(task)], false);

    let config = WorkerConfig {
        poll_interval_ms: 10,
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("the link to be enqueued", || {
        broker
            .enqueued()
            .iter()
            .any(|(task, _)| task.metadata.name == "follow_up")
    })
    .await;

    assert_eq!(runs.load(Ordering::Relaxed), 1);

    handle.shutdown().await.expect("shutdown");
}

/// The disposition token also owns advancing the workflow: a task whose
/// delivery was already requeued by the shutdown drain must not enqueue its
/// chain successor, or the redelivery enqueues it a second time.
#[cfg(feature = "canvas")]
#[tokio::test]
async fn test_unclaimed_delivery_does_not_double_enqueue_the_chain() {
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

    let task = serialized("blocking_task").with_on_success_link("follow_up".to_string());
    let task_id = task.metadata.id;
    let broker = RecordingBroker::new(vec![BrokerMessage::new(task)], false);

    let config = WorkerConfig {
        poll_interval_ms: 10,
        graceful_shutdown: true,
        // Deadline expires while the task is still blocked, so the drain
        // requeues the delivery and the task loses its claim.
        shutdown_timeout_secs: 1,
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("the task to start", || started.load(Ordering::Relaxed) == 1).await;
    handle.shutdown().await.expect("shutdown");

    wait_until("the drain deadline to requeue the delivery", || {
        broker.requeued() == vec![task_id]
    })
    .await;

    // Now let the task finish: it no longer owns the disposition.
    release.notify_waiters();
    wait_until("the task to finish", || {
        finished.load(Ordering::Relaxed) == 1
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert!(
        broker
            .enqueued()
            .iter()
            .all(|(task, _)| task.metadata.name != "follow_up"),
        "an unclaimed delivery must leave workflow continuation to its redelivery"
    );
    assert!(
        broker.acked().is_empty(),
        "and must not ack a delivery it no longer owns"
    );
}
