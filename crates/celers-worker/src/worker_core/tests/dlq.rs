//! Regression tests for dead-letter-queue wiring: the cleanup interval is
//! bounded, the worker actually runs the configured TTL sweep, and
//! `Worker::connect` opens the configured backend rather than silently
//! downgrading to an in-memory one.

use super::doubles::{serialized, RecordingBroker};

use crate::types::WorkerConfig;
use crate::worker_core::Worker;

use celers_core::{NoOpEventEmitter, TaskId, TaskRegistry};

use std::sync::Arc;
use std::time::Duration;

#[test]
fn test_dlq_cleanup_interval_is_bounded() {
    // Sweeping once per TTL keeps a short TTL honest...
    assert_eq!(
        crate::worker_core::support::dlq_cleanup_interval(30),
        Duration::from_secs(30)
    );
    // ...without letting a multi-day TTL mean "never swept in practice".
    assert_eq!(
        crate::worker_core::support::dlq_cleanup_interval(7 * 24 * 3600),
        Duration::from_secs(3600)
    );
    // `tokio::time::interval` panics on a zero period.
    assert_eq!(
        crate::worker_core::support::dlq_cleanup_interval(0),
        Duration::from_secs(1)
    );
}

/// A configured `ttl_seconds` used to be decoration: nothing ran the sweep, so
/// expired dead-letter entries accumulated for the life of the process.
#[tokio::test]
async fn test_worker_runs_the_dlq_ttl_sweep() {
    let broker = RecordingBroker::new(Vec::new(), false);
    let config = WorkerConfig {
        poll_interval_ms: 10,
        enable_dlq: true,
        dlq_config: crate::dlq::DlqConfig::new(true).with_ttl(1),
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), TaskRegistry::new(), config);
    let dlq = worker.dlq_handler().cloned().expect("dlq enabled");
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    // Back-date the entry so the very first sweep reclaims it.
    let mut entry = crate::dlq::DlqEntry::new(
        serialized("stale_task"),
        TaskId::new_v4(),
        0,
        "boom".to_string(),
        "test-host".to_string(),
    );
    entry.dlq_timestamp = entry.dlq_timestamp.saturating_sub(3_600);
    dlq.add_entry(entry).await.expect("entry is recorded");
    assert_eq!(dlq.size().await, 1);

    let mut swept = false;
    for _ in 0..300 {
        if dlq.size().await == 0 {
            swept = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        swept,
        "a configured DLQ TTL must actually reclaim expired entries"
    );

    handle.shutdown().await.expect("shutdown");
}

/// `Worker::connect` opens the configured DLQ backend instead of silently
/// downgrading a persistent dead-letter queue to a volatile in-memory one.
#[tokio::test]
async fn test_connect_opens_the_configured_dlq_backend() {
    let config = WorkerConfig {
        enable_dlq: true,
        dlq_config: crate::dlq::DlqConfig::new(true),
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> = Worker::connect(
        Arc::try_unwrap(RecordingBroker::new(Vec::new(), false))
            .ok()
            .expect("sole owner"),
        TaskRegistry::new(),
        config,
    )
    .await
    .expect("the in-memory backend always connects");

    let dlq = worker.dlq_handler().cloned().expect("dlq enabled");
    assert!(dlq.is_enabled());
    assert_eq!(dlq.size().await, 0);

    // A backend whose feature is not compiled in is a hard error, not a silent
    // downgrade to memory.
    #[cfg(not(feature = "redis"))]
    {
        let config = WorkerConfig {
            enable_dlq: true,
            dlq_config: crate::dlq::DlqConfig::new(true).with_storage(
                crate::dlq::DlqStorageBackend::Redis {
                    url: "redis://127.0.0.1:6379".to_string(),
                    key_prefix: Some("celers:test".to_string()),
                },
            ),
            ..Default::default()
        };
        let failed: std::result::Result<Worker<RecordingBroker, NoOpEventEmitter>, _> =
            Worker::connect(
                Arc::try_unwrap(RecordingBroker::new(Vec::new(), false))
                    .ok()
                    .expect("sole owner"),
                TaskRegistry::new(),
                config,
            )
            .await;
        assert!(
            failed.is_err(),
            "an unavailable DLQ backend must surface, not degrade to memory"
        );
    }
}
