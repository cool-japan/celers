//! Regression test: lifecycle events flow through the buffered
//! `emit_batch` path (not one round trip per event) while keeping their
//! per-task order.

use super::doubles::{serialized, wait_until, CapturingEmitter, CountingTask, RecordingBroker};

use crate::types::WorkerConfig;
use crate::worker_core::Worker;

use celers_core::{BrokerMessage, TaskRegistry};

use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

/// idx 301: lifecycle events leave the critical path through the buffered
/// emitter (`emit_batch`) while keeping their per-task order.
#[tokio::test]
async fn test_lifecycle_events_are_emitted_in_order_via_batches() {
    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&runs),
            name: "quick_task",
        })
        .await;

    let broker = RecordingBroker::new(vec![BrokerMessage::new(serialized("quick_task"))], false);

    let config = WorkerConfig {
        poll_interval_ms: 10,
        enable_events: true,
        ..Default::default()
    };
    let emitter = CapturingEmitter::default();
    let worker =
        Worker::with_event_emitter_from_arc(Arc::clone(&broker), registry, config, emitter.clone());
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("the task lifecycle events", || {
        let names = emitter.task_event_names();
        names.contains(&"received") && names.contains(&"started") && names.contains(&"succeeded")
    })
    .await;

    let names = emitter.task_event_names();
    let received = names.iter().position(|n| *n == "received");
    let started = names.iter().position(|n| *n == "started");
    let succeeded = names.iter().position(|n| *n == "succeeded");
    assert!(
        received < started && started < succeeded,
        "per-task event order must be preserved, got {names:?}"
    );
    assert!(
        emitter.batch_calls() >= 1,
        "task events must be flushed through emit_batch, not one round trip each"
    );

    handle.shutdown().await.expect("shutdown");
}
