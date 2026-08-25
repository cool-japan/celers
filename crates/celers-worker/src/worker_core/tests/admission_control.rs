//! Regression tests for admission control that must defer rather than
//! execute: a cluster-wide rate limiter keyed by queue, a missing feature
//! flag, and strict routing's allow-list.

use super::doubles::{
    serialized, wait_until, AlwaysRedeliverBroker, CountingTask, RecordingBroker,
};

use crate::coordinated_rate_limit::WorkerRateLimitCoordinator;
use crate::feature_flags::{FeatureFlags, TaskFeatureRequirements};
use crate::routing::{RoutingStrategy, WorkerTags};
use crate::types::WorkerConfig;
use crate::worker_core::Worker;

use celers_core::rate_limit::RateLimitConfig;
use celers_core::rate_limit_distributed::InMemoryDistributedBackend;
use celers_core::{BrokerMessage, NoOpEventEmitter, TaskRegistry};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// idx 176: the cluster-wide rate limiter is keyed by the worker's configured
/// queue, not by a hard-coded `"default"` literal.
#[tokio::test]
async fn test_rate_limit_uses_the_configured_queue_name() {
    let backend: Arc<dyn celers_core::rate_limit_distributed::DistributedRateLimitBackend> =
        Arc::new(InMemoryDistributedBackend::new());
    let coordinator = WorkerRateLimitCoordinator::new(
        Arc::clone(&backend),
        RateLimitConfig::new(0.0).with_burst(1),
    )
    .with_key_strategy(crate::coordinated_rate_limit::RateLimitKeyStrategy::Queue);

    // Exhaust the budget of the "images" queue only.
    assert!(coordinator
        .acquire("quick_task", "images")
        .await
        .expect("acquire")
        .is_allowed());

    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&runs),
            name: "quick_task",
        })
        .await;

    let task = serialized("quick_task");
    let broker = RecordingBroker::new(vec![BrokerMessage::new(task)], false);

    let config = WorkerConfig {
        poll_interval_ms: 10,
        queue_name: "images".to_string(),
        defer_delay_ms: 10,
        defer_max_delay_ms: 20,
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config)
            .with_rate_limit_coordinator(coordinator);
    let stats = worker.stats_arc();
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("the task to be rate limited", || stats.rate_limited() >= 1).await;
    assert_eq!(
        runs.load(Ordering::Relaxed),
        0,
        "the queue's budget was exhausted, so nothing may run"
    );

    handle.shutdown().await.expect("shutdown");
}

/// idx 182: feature-flag admission. A worker missing a required feature defers
/// the task instead of executing it.
#[tokio::test]
async fn test_feature_requirements_gate_execution() {
    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&runs),
            name: "quick_task",
        })
        .await;

    let broker = AlwaysRedeliverBroker::new(BrokerMessage::new(serialized("quick_task")));

    let mut requirements = std::collections::HashMap::new();
    requirements.insert(
        "quick_task".to_string(),
        TaskFeatureRequirements::new().require("gpu"),
    );

    let config = WorkerConfig {
        poll_interval_ms: 10,
        defer_delay_ms: 3_000,
        defer_max_delay_ms: 3_000,
        feature_flags: FeatureFlags::from_features(["cpu"]),
        task_feature_requirements: requirements,
        ..Default::default()
    };
    let worker: Worker<AlwaysRedeliverBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let stats = worker.stats_arc();
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("the feature-gated deferral", || stats.deferred() >= 1).await;
    assert_eq!(runs.load(Ordering::Relaxed), 0);

    handle.shutdown().await.expect("shutdown");
}

/// idx 182: `routing_strategy` is honoured. Under `Strict` a task type that is
/// not on the worker's allow-list is deferred, where `Lenient` admits it.
#[tokio::test]
async fn test_strict_routing_requires_an_explicit_allow_list_entry() {
    let runs = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&runs),
            name: "quick_task",
        })
        .await;

    let broker = AlwaysRedeliverBroker::new(BrokerMessage::new(serialized("quick_task")));

    let config = WorkerConfig {
        poll_interval_ms: 10,
        defer_delay_ms: 3_000,
        defer_max_delay_ms: 3_000,
        enable_routing: true,
        routing_strategy: RoutingStrategy::Strict,
        worker_tags: WorkerTags::new().with_task_type("other_task"),
        ..Default::default()
    };
    let worker: Worker<AlwaysRedeliverBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let stats = worker.stats_arc();
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("the routing deferral", || stats.deferred() >= 1).await;
    assert_eq!(runs.load(Ordering::Relaxed), 0);
    handle.shutdown().await.expect("shutdown");

    // The same worker under the default (lenient) strategy runs the task.
    let runs_lenient = Arc::new(AtomicUsize::new(0));
    let registry = TaskRegistry::new();
    registry
        .register(CountingTask {
            runs: Arc::clone(&runs_lenient),
            name: "quick_task",
        })
        .await;
    let broker = RecordingBroker::new(vec![BrokerMessage::new(serialized("quick_task"))], false);
    let config = WorkerConfig {
        poll_interval_ms: 10,
        enable_routing: true,
        routing_strategy: RoutingStrategy::Lenient,
        worker_tags: WorkerTags::new(),
        ..Default::default()
    };
    let worker: Worker<RecordingBroker, NoOpEventEmitter> =
        Worker::new_from_arc(Arc::clone(&broker), registry, config);
    let handle = worker.run_with_shutdown().await.expect("worker starts");

    wait_until("the lenient worker to run the task", || {
        runs_lenient.load(Ordering::Relaxed) == 1
    })
    .await;
    handle.shutdown().await.expect("shutdown");
}
