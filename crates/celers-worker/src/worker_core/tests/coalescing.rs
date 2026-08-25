//! Regression tests for message coalescing: the default key is id-scoped
//! (lossless), and `coalesce_require_same_task_id` keeps distinct
//! submissions that happen to share arguments from being merged away.

use super::doubles::{serialized, wait_until, CountingTask, RecordingBroker};

use crate::types::WorkerConfig;
use crate::worker_core::Worker;

use celers_core::{BrokerMessage, NoOpEventEmitter, TaskRegistry};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// idx 161: with `coalesce_require_same_task_id`, distinct submissions that
/// happen to share arguments all run — only true redelivery duplicates are
/// collapsed, so no caller is left waiting on a result that never comes.
#[tokio::test]
async fn test_id_scoped_coalescing_keeps_distinct_submissions() {
    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&runs),
            name: "quick_task",
        })
        .await;

    // Three distinct ids with identical payloads, plus one true duplicate of
    // the first delivery.
    let first = serialized("quick_task");
    let duplicate = first.clone();
    let messages = vec![
        BrokerMessage::new(first),
        BrokerMessage::new(duplicate),
        BrokerMessage::new(serialized("quick_task")),
        BrokerMessage::new(serialized("quick_task")),
    ];
    let broker = RecordingBroker::new(messages, false);

    let config = WorkerConfig {
        poll_interval_ms: 10,
        enable_batch_dequeue: true,
        batch_size: 10,
        enable_coalescing: true,
        coalesce_require_same_task_id: true,
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    // Three distinct tasks execute; only the repeated delivery is coalesced.
    wait_until("all distinct submissions to run", || {
        runs.load(Ordering::Relaxed) == 3
    })
    .await;
    wait_until("every message to be acked", || broker.acked().len() == 4).await;

    handle.shutdown().await.expect("shutdown");
}

/// idx 161: the *default* coalescing key is id-scoped, so enabling coalescing
/// can no longer silently destroy independent submissions that happen to share
/// their arguments.
#[tokio::test]
async fn test_coalescing_defaults_to_the_lossless_id_scoped_key() {
    assert!(
        WorkerConfig::default().coalesce_require_same_task_id,
        "the default must be the lossless key"
    );

    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&runs),
            name: "quick_task",
        })
        .await;

    // Three independent submissions with byte-identical payloads.
    let messages = vec![
        BrokerMessage::new(serialized("quick_task")),
        BrokerMessage::new(serialized("quick_task")),
        BrokerMessage::new(serialized("quick_task")),
    ];
    let broker = RecordingBroker::new(messages, false);

    let config = WorkerConfig {
        poll_interval_ms: 10,
        enable_batch_dequeue: true,
        batch_size: 10,
        enable_coalescing: true,
        // Deliberately *not* setting `coalesce_require_same_task_id`.
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("every distinct submission to run", || {
        runs.load(Ordering::Relaxed) == 3
    })
    .await;
    wait_until("every message to be acked", || broker.acked().len() == 3).await;

    handle.shutdown().await.expect("shutdown");
}
