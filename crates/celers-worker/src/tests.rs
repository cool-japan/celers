#![cfg(test)]

use crate::types::{DynamicConfig, WorkerConfig, WorkerMode, WorkerStats};
use crate::worker_core::Worker;

use celers_core::{Broker, NoOpEventEmitter, Result, TaskRegistry};
use std::sync::atomic::AtomicU8;
use std::sync::{Arc, RwLock};

#[test]
fn test_backoff_calculation() {
    let config = WorkerConfig::default();
    let dynamic_config = DynamicConfig::default();
    let worker: Worker<MockBroker, NoOpEventEmitter> = Worker {
        broker: Arc::new(MockBroker),
        registry: Arc::new(TaskRegistry::new()),
        config,
        circuit_breaker: None,
        dlq_handler: None,
        shutdown_tx: None,
        event_emitter: Arc::new(NoOpEventEmitter::new()),
        stats: Arc::new(WorkerStats::new()),
        mode: Arc::new(AtomicU8::new(WorkerMode::Normal as u8)),
        dynamic_config: Arc::new(RwLock::new(dynamic_config)),
        middleware_stack: None,
        revocation_watcher: None,
        rate_limit_coordinator: None,
        affinity_registry: None,
    };

    assert_eq!(worker.calculate_backoff_delay(0).as_millis(), 1000);
    assert_eq!(worker.calculate_backoff_delay(1).as_millis(), 2000);
    assert_eq!(worker.calculate_backoff_delay(2).as_millis(), 4000);
    assert_eq!(worker.calculate_backoff_delay(3).as_millis(), 8000);

    // Should cap at max delay
    assert_eq!(worker.calculate_backoff_delay(10).as_millis(), 60000);
}

#[test]
fn test_worker_stats() {
    let stats = WorkerStats::new();

    assert_eq!(stats.active(), 0);
    assert_eq!(stats.processed(), 0);

    stats.task_started();
    assert_eq!(stats.active(), 1);
    assert_eq!(stats.processed(), 0);

    stats.task_started();
    assert_eq!(stats.active(), 2);

    stats.task_completed();
    assert_eq!(stats.active(), 1);
    assert_eq!(stats.processed(), 1);

    stats.task_completed();
    assert_eq!(stats.active(), 0);
    assert_eq!(stats.processed(), 2);

    // Revocation and rate-limit counters
    assert_eq!(stats.revoked(), 0);
    assert_eq!(stats.rate_limited(), 0);
    stats.task_revoked();
    stats.task_revoked();
    assert_eq!(stats.revoked(), 2);
    stats.task_rate_limited();
    assert_eq!(stats.rate_limited(), 1);

    // Clone preserves all counters
    let cloned = stats.clone();
    assert_eq!(cloned.processed(), 2);
    assert_eq!(cloned.revoked(), 2);
    assert_eq!(cloned.rate_limited(), 1);
}

#[test]
fn test_worker_config_default() {
    let config = WorkerConfig::default();
    assert_eq!(config.concurrency, 4);
    assert_eq!(config.poll_interval_ms, 1000);
    assert!(config.graceful_shutdown);
    assert_eq!(config.max_retries, 3);
    assert!(!config.enable_batch_dequeue);
    assert!(!config.enable_circuit_breaker);
    assert!(!config.track_memory_usage);
}

#[test]
fn test_worker_config_predicates() {
    let mut config = WorkerConfig::default();

    assert!(!config.has_batch_dequeue());
    assert!(!config.has_circuit_breaker());
    assert!(!config.has_memory_tracking());
    assert!(!config.has_result_size_limit());
    assert!(config.has_graceful_shutdown());
    assert!(!config.has_events());
    assert!(!config.has_heartbeat());

    config.enable_batch_dequeue = true;
    config.enable_circuit_breaker = true;
    config.track_memory_usage = true;
    config.max_result_size_bytes = 1024;
    config.graceful_shutdown = false;
    config.enable_events = true;
    config.heartbeat_interval_secs = 30;

    assert!(config.has_batch_dequeue());
    assert!(config.has_circuit_breaker());
    assert!(config.has_memory_tracking());
    assert!(config.has_result_size_limit());
    assert!(!config.has_graceful_shutdown());
    assert!(config.has_events());
    assert!(config.has_heartbeat());
}

#[test]
fn test_worker_config_validate_concurrency_zero() {
    let config = WorkerConfig {
        concurrency: 0,
        ..Default::default()
    };
    let result = config.validate();
    assert!(result.is_err());
    assert_eq!(
        result.expect_err("expected error"),
        "Concurrency must be at least 1"
    );
}

#[test]
fn test_worker_config_validate_batch_size_zero() {
    let config = WorkerConfig {
        enable_batch_dequeue: true,
        batch_size: 0,
        ..Default::default()
    };
    let result = config.validate();
    assert!(result.is_err());
    assert_eq!(
        result.expect_err("expected error"),
        "Batch size must be at least 1 when batch dequeue is enabled"
    );
}

#[test]
fn test_worker_config_validate_timeout_zero() {
    let config = WorkerConfig {
        default_timeout_secs: 0,
        ..Default::default()
    };
    let result = config.validate();
    assert!(result.is_err());
    assert_eq!(
        result.expect_err("expected error"),
        "Default timeout must be at least 1 second"
    );
}

#[test]
fn test_worker_config_validate_retry_delays() {
    let config = WorkerConfig {
        retry_base_delay_ms: 0,
        ..Default::default()
    };
    let result = config.validate();
    assert!(result.is_err());
    assert_eq!(
        result.expect_err("expected error"),
        "Retry base delay must be at least 1ms"
    );

    let config = WorkerConfig {
        retry_base_delay_ms: 1000,
        retry_max_delay_ms: 500,
        ..Default::default()
    };
    let result = config.validate();
    assert!(result.is_err());
    assert_eq!(
        result.expect_err("expected error"),
        "Max retry delay must be greater than or equal to base delay"
    );
}

#[test]
fn test_worker_config_display() {
    let config = WorkerConfig::default();
    let display = format!("{}", config);
    assert!(display.contains("WorkerConfig"));
    assert!(display.contains("concurrency=4"));
    assert!(display.contains("poll=1000ms"));
    assert!(display.contains("retries=3"));
    assert!(display.contains("timeout=300s"));
}

#[test]
fn test_worker_config_display_with_features() {
    let config = WorkerConfig {
        concurrency: 8,
        enable_batch_dequeue: true,
        batch_size: 20,
        enable_circuit_breaker: true,
        track_memory_usage: true,
        max_result_size_bytes: 1048576,
        ..Default::default()
    };
    let display = format!("{}", config);
    assert!(display.contains("batch=20"));
    assert!(display.contains("circuit_breaker=enabled"));
    assert!(display.contains("memory_tracking=enabled"));
    assert!(display.contains("max_result=1048576B"));
}

#[test]
fn test_worker_config_builder_basic() {
    let config = WorkerConfig::builder()
        .concurrency(10)
        .max_retries(5)
        .default_timeout_secs(600)
        .build()
        .expect("valid config");

    assert_eq!(config.concurrency, 10);
    assert_eq!(config.max_retries, 5);
    assert_eq!(config.default_timeout_secs, 600);
}

#[test]
fn test_worker_config_builder_batch_settings() {
    let config = WorkerConfig::builder()
        .enable_batch_dequeue(true)
        .batch_size(25)
        .build()
        .expect("valid config");

    assert!(config.enable_batch_dequeue);
    assert_eq!(config.batch_size, 25);
}

#[test]
fn test_worker_config_builder_validation_errors() {
    // Zero concurrency should fail
    let result = WorkerConfig::builder().concurrency(0).build();
    assert!(result.is_err());

    // Batch dequeue with zero batch size should fail
    let result = WorkerConfig::builder()
        .enable_batch_dequeue(true)
        .batch_size(0)
        .build();
    assert!(result.is_err());

    // Zero timeout should fail
    let result = WorkerConfig::builder().default_timeout_secs(0).build();
    assert!(result.is_err());

    // Invalid retry delays should fail
    let result = WorkerConfig::builder()
        .retry_base_delay_ms(2000)
        .retry_max_delay_ms(1000)
        .build();
    assert!(result.is_err());
}

#[test]
fn test_worker_config_builder_preset_high_throughput() {
    let config = WorkerConfig::builder()
        .preset_high_throughput()
        .build()
        .expect("valid config");

    assert_eq!(config.concurrency, 16);
    assert!(config.enable_batch_dequeue);
    assert_eq!(config.batch_size, 20);
    assert!(config.enable_circuit_breaker);
    assert!(config.track_memory_usage);
}

#[test]
fn test_worker_config_builder_preset_low_latency() {
    let config = WorkerConfig::builder()
        .preset_low_latency()
        .build()
        .expect("valid config");

    assert_eq!(config.concurrency, 8);
    assert_eq!(config.poll_interval_ms, 100);
    assert!(!config.enable_batch_dequeue);
    assert!(config.enable_circuit_breaker);
}

#[test]
fn test_worker_config_builder_preset_reliable() {
    let config = WorkerConfig::builder()
        .preset_reliable()
        .build()
        .expect("valid config");

    assert_eq!(config.concurrency, 4);
    assert_eq!(config.max_retries, 5);
    assert!(config.enable_circuit_breaker);
    assert!(config.graceful_shutdown);
}

#[test]
fn test_worker_config_builder_preset_development() {
    let config = WorkerConfig::builder()
        .preset_development()
        .build()
        .expect("valid config");

    assert_eq!(config.concurrency, 2);
    assert_eq!(config.poll_interval_ms, 500);
    assert!(!config.enable_circuit_breaker);
    assert!(config.track_memory_usage);
}

#[test]
fn test_worker_config_builder_build_unchecked() {
    // Should allow invalid config when using build_unchecked
    let config = WorkerConfig::builder().concurrency(0).build_unchecked();

    assert_eq!(config.concurrency, 0);
}

#[test]
fn test_worker_config_builder_events() {
    let config = WorkerConfig::builder()
        .hostname("my-worker")
        .enable_events(true)
        .heartbeat_interval_secs(30)
        .build()
        .expect("valid config");

    assert_eq!(config.hostname, "my-worker");
    assert!(config.enable_events);
    assert_eq!(config.heartbeat_interval_secs, 30);
    assert!(config.has_events());
    assert!(config.has_heartbeat());
}

#[test]
fn test_worker_config_display_with_events() {
    let config = WorkerConfig {
        enable_events: true,
        heartbeat_interval_secs: 60,
        ..Default::default()
    };
    let display = format!("{}", config);
    assert!(display.contains("events=enabled"));
    assert!(display.contains("heartbeat=60s"));
}

#[test]
fn test_worker_config_for_development() {
    let config = WorkerConfig::for_development();
    assert_eq!(config.concurrency, 2);
    assert_eq!(config.poll_interval_ms, 500);
    assert!(config.track_memory_usage);
    assert_eq!(config.metadata.environment(), Some("development"));
}

#[test]
fn test_worker_config_for_staging() {
    let config = WorkerConfig::for_staging();
    assert_eq!(config.concurrency, 8);
    assert!(config.enable_circuit_breaker);
    assert!(config.track_memory_usage);
    assert!(config.enable_events);
    assert_eq!(config.heartbeat_interval_secs, 30);
    assert_eq!(config.metadata.environment(), Some("staging"));
}

#[test]
fn test_worker_config_for_production() {
    let config = WorkerConfig::for_production();
    assert_eq!(config.concurrency, 16);
    assert!(config.enable_batch_dequeue);
    assert!(config.enable_circuit_breaker);
    assert_eq!(config.max_retries, 5);
    assert!(config.enable_events);
    assert_eq!(config.heartbeat_interval_secs, 30);
    assert_eq!(config.metadata.environment(), Some("production"));
}

#[test]
fn test_worker_config_from_env() {
    // Test default (development)
    std::env::remove_var("CELERS_ENV");
    let config = WorkerConfig::from_env();
    assert_eq!(config.metadata.environment(), Some("development"));

    // Test production
    std::env::set_var("CELERS_ENV", "production");
    let config = WorkerConfig::from_env();
    assert_eq!(config.metadata.environment(), Some("production"));

    // Test prod alias
    std::env::set_var("CELERS_ENV", "prod");
    let config = WorkerConfig::from_env();
    assert_eq!(config.metadata.environment(), Some("production"));

    // Test staging
    std::env::set_var("CELERS_ENV", "staging");
    let config = WorkerConfig::from_env();
    assert_eq!(config.metadata.environment(), Some("staging"));

    // Test stage alias
    std::env::set_var("CELERS_ENV", "stage");
    let config = WorkerConfig::from_env();
    assert_eq!(config.metadata.environment(), Some("staging"));

    // Test dev alias
    std::env::set_var("CELERS_ENV", "dev");
    let config = WorkerConfig::from_env();
    assert_eq!(config.metadata.environment(), Some("development"));

    // Clean up
    std::env::remove_var("CELERS_ENV");
}

// Mock broker for testing
struct MockBroker;

#[async_trait::async_trait]
impl Broker for MockBroker {
    async fn enqueue(&self, _task: celers_core::SerializedTask) -> Result<celers_core::TaskId> {
        Ok(celers_core::TaskId::new_v4())
    }

    async fn dequeue(&self) -> Result<Option<celers_core::BrokerMessage>> {
        Ok(None)
    }

    async fn ack(
        &self,
        _task_id: &celers_core::TaskId,
        _receipt_handle: Option<&str>,
    ) -> Result<()> {
        Ok(())
    }

    async fn reject(
        &self,
        _task_id: &celers_core::TaskId,
        _receipt_handle: Option<&str>,
        _requeue: bool,
    ) -> Result<()> {
        Ok(())
    }

    async fn queue_size(&self) -> Result<usize> {
        Ok(0)
    }

    async fn cancel(&self, _task_id: &celers_core::TaskId) -> Result<bool> {
        Ok(false)
    }
}

#[tokio::test]
async fn test_mock_broker_enqueue_returns_unique_ids() {
    use celers_core::Broker;
    let broker = MockBroker;
    let task1 = celers_core::SerializedTask::new("test_task_1".to_string(), vec![1, 2, 3]);
    let task2 = celers_core::SerializedTask::new("test_task_2".to_string(), vec![4, 5, 6]);
    let id1 = broker.enqueue(task1).await.expect("enqueue should succeed");
    let id2 = broker.enqueue(task2).await.expect("enqueue should succeed");
    assert_ne!(id1, id2, "Each enqueue should return a unique TaskId");
}

// ---------------------------------------------------------------------------
// Integration tests: cooperative cancellation + distributed rate limiting
// wired into the worker execution loop.
// ---------------------------------------------------------------------------

mod execution_loop_integration {
    use crate::coordinated_rate_limit::WorkerRateLimitCoordinator;
    use crate::execution_context::{is_cancelled, RevocationWatcher};
    use crate::types::WorkerConfig;
    use crate::worker_core::Worker;

    use celers_core::rate_limit::RateLimitConfig;
    use celers_core::rate_limit_distributed::InMemoryDistributedBackend;
    use celers_core::{
        Broker, BrokerMessage, NoOpEventEmitter, Result, SerializedTask, Task, TaskId, TaskRegistry,
    };

    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    /// A broker that hands out a fixed set of messages once, then keeps returning
    /// `None`. Records ack/reject(requeue) calls per task id so tests can assert
    /// the terminal disposition. Requeued tasks are *not* re-delivered (we only
    /// want to observe the gating decision once).
    struct ControlledBroker {
        pending: Mutex<Vec<BrokerMessage>>,
        acked: Mutex<HashMap<TaskId, usize>>,
        requeued: Mutex<HashMap<TaskId, usize>>,
        rejected: Mutex<HashMap<TaskId, usize>>,
    }

    impl ControlledBroker {
        fn new(messages: Vec<BrokerMessage>) -> Arc<Self> {
            Arc::new(Self {
                pending: Mutex::new(messages),
                acked: Mutex::new(HashMap::new()),
                requeued: Mutex::new(HashMap::new()),
                rejected: Mutex::new(HashMap::new()),
            })
        }

        fn ack_count(&self, id: &TaskId) -> usize {
            self.acked
                .lock()
                .expect("lock")
                .get(id)
                .copied()
                .unwrap_or(0)
        }

        fn requeue_count(&self, id: &TaskId) -> usize {
            self.requeued
                .lock()
                .expect("lock")
                .get(id)
                .copied()
                .unwrap_or(0)
        }

        fn reject_count(&self, id: &TaskId) -> usize {
            self.rejected
                .lock()
                .expect("lock")
                .get(id)
                .copied()
                .unwrap_or(0)
        }
    }

    #[async_trait::async_trait]
    impl Broker for ControlledBroker {
        async fn enqueue(&self, _task: SerializedTask) -> Result<TaskId> {
            Ok(TaskId::new_v4())
        }

        async fn dequeue(&self) -> Result<Option<BrokerMessage>> {
            Ok(self.pending.lock().expect("lock").pop())
        }

        async fn ack(&self, task_id: &TaskId, _receipt_handle: Option<&str>) -> Result<()> {
            *self
                .acked
                .lock()
                .expect("lock")
                .entry(*task_id)
                .or_insert(0) += 1;
            Ok(())
        }

        async fn reject(
            &self,
            task_id: &TaskId,
            _receipt_handle: Option<&str>,
            requeue: bool,
        ) -> Result<()> {
            let map = if requeue {
                &self.requeued
            } else {
                &self.rejected
            };
            *map.lock().expect("lock").entry(*task_id).or_insert(0) += 1;
            Ok(())
        }

        async fn queue_size(&self) -> Result<usize> {
            Ok(self.pending.lock().expect("lock").len())
        }

        async fn cancel(&self, _task_id: &TaskId) -> Result<bool> {
            Ok(false)
        }
    }

    #[derive(Serialize, Deserialize)]
    struct Empty {}

    /// A cooperative long-running task: loops checking the ambient cancellation
    /// token. Increments a shared counter each iteration so the test can detect
    /// that it actually started. Returns once cancelled (or after a generous
    /// safety bound so the test cannot hang).
    struct LongTask {
        iterations: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl Task for LongTask {
        type Input = Empty;
        type Output = Empty;

        async fn execute(&self, _input: Self::Input) -> celers_core::Result<Self::Output> {
            for _ in 0..50_000 {
                if is_cancelled() {
                    break;
                }
                self.iterations.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            Ok(Empty {})
        }

        fn name(&self) -> &'static str {
            "long_task"
        }
    }

    /// A trivial task that completes immediately. Counts executions.
    struct QuickTask {
        runs: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl Task for QuickTask {
        type Input = Empty;
        type Output = Empty;

        async fn execute(&self, _input: Self::Input) -> celers_core::Result<Self::Output> {
            self.runs.fetch_add(1, Ordering::Relaxed);
            Ok(Empty {})
        }

        fn name(&self) -> &'static str {
            "quick_task"
        }
    }

    fn serialized(name: &str) -> SerializedTask {
        SerializedTask::new(
            name.to_string(),
            serde_json::to_vec(&Empty {}).expect("serialize empty"),
        )
    }

    /// A long-running task that is revoked mid-flight transitions to Revoked:
    /// the worker acks it (removing it) and increments the revoked counter, and
    /// the cooperative task observes the token and stops.
    #[tokio::test]
    async fn test_running_task_observes_token_and_is_revoked() {
        let iterations = Arc::new(AtomicUsize::new(0));
        let registry = TaskRegistry::new();
        registry
            .register(LongTask {
                iterations: Arc::clone(&iterations),
            })
            .await;

        let task = serialized("long_task");
        let task_id = task.metadata.id;
        let broker = ControlledBroker::new(vec![BrokerMessage::new(task)]);

        let watcher = RevocationWatcher::new();
        let publisher = watcher.publisher();

        let config = WorkerConfig {
            poll_interval_ms: 10,
            ..Default::default()
        };
        let worker: Worker<ControlledBroker, NoOpEventEmitter> =
            Worker::new_from_arc(Arc::clone(&broker), registry, config)
                .with_revocation_watcher(watcher);
        let stats = worker.stats_arc();

        let handle = worker.run_with_shutdown().await.expect("worker starts");

        // Wait until the task is actually in-flight (it has started iterating).
        for _ in 0..200 {
            if iterations.load(Ordering::Relaxed) > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            iterations.load(Ordering::Relaxed) > 0,
            "long task should have started"
        );

        // Revoke the in-flight task via the broker's revocation Pub/Sub.
        publisher.revoke(task_id);

        // The task should transition to Revoked: acked + counted.
        let mut revoked_ok = false;
        for _ in 0..400 {
            if stats.revoked() >= 1 && broker.ack_count(&task_id) >= 1 {
                revoked_ok = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            revoked_ok,
            "task should be revoked (acked) — revoked={}, ack={}",
            stats.revoked(),
            broker.ack_count(&task_id)
        );

        // It must not have been requeued or hard-rejected.
        assert_eq!(broker.requeue_count(&task_id), 0);
        assert_eq!(broker.reject_count(&task_id), 0);

        // The cooperative task stopped well before its safety bound.
        let iters = iterations.load(Ordering::Relaxed);
        assert!(
            iters < 50_000,
            "task should have stopped early, did {iters}"
        );

        handle.shutdown().await.expect("shutdown");
    }

    /// Revoking an unrelated id does nothing: the running task completes normally
    /// (acked, not revoked).
    #[tokio::test]
    async fn test_revoke_unrelated_id_does_nothing() {
        let runs = Arc::new(AtomicUsize::new(0));
        let registry = TaskRegistry::new();
        registry
            .register(QuickTask {
                runs: Arc::clone(&runs),
            })
            .await;

        let task = serialized("quick_task");
        let task_id = task.metadata.id;
        let broker = ControlledBroker::new(vec![BrokerMessage::new(task)]);

        let watcher = RevocationWatcher::new();
        let publisher = watcher.publisher();
        // Spawn the watcher so the unrelated signal is actually processed.
        // (run_with_shutdown also spawns one; both share the registry.)

        let config = WorkerConfig {
            poll_interval_ms: 10,
            ..Default::default()
        };
        let worker: Worker<ControlledBroker, NoOpEventEmitter> =
            Worker::new_from_arc(Arc::clone(&broker), registry, config)
                .with_revocation_watcher(watcher);
        let stats = worker.stats_arc();
        let handle = worker.run_with_shutdown().await.expect("worker starts");

        // Fire a revocation for a completely unrelated id.
        let unrelated = TaskId::new_v4();
        // Give the watcher a moment to subscribe.
        for _ in 0..100 {
            if publisher.subscriber_count() > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        publisher.revoke(unrelated);

        // The real task should run to completion and be acked.
        let mut completed = false;
        for _ in 0..200 {
            if broker.ack_count(&task_id) >= 1 {
                completed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(completed, "unrelated revoke must not affect the real task");
        assert_eq!(runs.load(Ordering::Relaxed), 1, "task should have executed");
        assert_eq!(stats.revoked(), 0, "nothing should be revoked");

        handle.shutdown().await.expect("shutdown");
    }

    /// The distributed rate limiter gates execution: with a burst of 1 and no
    /// refill, only the first task executes; subsequent ones are deferred
    /// (requeued) and counted as rate-limited.
    #[tokio::test]
    async fn test_rate_limiter_defers_past_cap() {
        let runs = Arc::new(AtomicUsize::new(0));
        let registry = TaskRegistry::new();
        registry
            .register(QuickTask {
                runs: Arc::clone(&runs),
            })
            .await;

        // Two tasks of the same type; burst 1, rate 0 -> only one may run.
        let t1 = serialized("quick_task");
        let t2 = serialized("quick_task");
        let id1 = t1.metadata.id;
        let id2 = t2.metadata.id;
        // pending is popped from the back, so push t2 then t1 to deliver t1 first.
        let broker = ControlledBroker::new(vec![BrokerMessage::new(t2), BrokerMessage::new(t1)]);

        let backend = Arc::new(InMemoryDistributedBackend::new());
        let coordinator =
            WorkerRateLimitCoordinator::new(backend, RateLimitConfig::new(0.0).with_burst(1));

        let config = WorkerConfig {
            poll_interval_ms: 10,
            ..Default::default()
        };
        let worker: Worker<ControlledBroker, NoOpEventEmitter> =
            Worker::new_from_arc(Arc::clone(&broker), registry, config)
                .with_rate_limit_coordinator(coordinator);
        let stats = worker.stats_arc();
        let handle = worker.run_with_shutdown().await.expect("worker starts");

        // One task runs, one is deferred.
        let mut ok = false;
        for _ in 0..300 {
            let one_ran = runs.load(Ordering::Relaxed) == 1;
            let one_acked = broker.ack_count(&id1) + broker.ack_count(&id2) == 1;
            let one_deferred = broker.requeue_count(&id1) + broker.requeue_count(&id2) >= 1;
            if one_ran && one_acked && one_deferred {
                ok = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            ok,
            "exactly one task should run, the other deferred — runs={}, ack={}, requeue={}, rl={}",
            runs.load(Ordering::Relaxed),
            broker.ack_count(&id1) + broker.ack_count(&id2),
            broker.requeue_count(&id1) + broker.requeue_count(&id2),
            stats.rate_limited(),
        );
        assert!(
            stats.rate_limited() >= 1,
            "should record a rate-limited task"
        );

        handle.shutdown().await.expect("shutdown");
    }

    /// After the limiter refills, a previously-denied task is allowed and runs.
    #[tokio::test]
    async fn test_rate_limiter_allows_after_refill() {
        let runs = Arc::new(AtomicUsize::new(0));

        // Pre-exhaust the shared backend for "quick_task" so the first dequeue is
        // denied, then it refills (50/sec) and the retry succeeds.
        let backend = Arc::new(InMemoryDistributedBackend::new());
        let coordinator = WorkerRateLimitCoordinator::new(
            backend as Arc<dyn celers_core::rate_limit_distributed::DistributedRateLimitBackend>,
            RateLimitConfig::new(50.0).with_burst(1),
        );
        // Consume the only token up front.
        assert!(coordinator
            .acquire("quick_task", "default")
            .await
            .expect("acquire")
            .is_allowed());
        assert!(coordinator
            .acquire("quick_task", "default")
            .await
            .expect("acquire")
            .is_denied());

        let registry = TaskRegistry::new();
        registry
            .register(QuickTask {
                runs: Arc::clone(&runs),
            })
            .await;

        // Deliver the same task repeatedly: each requeue is re-popped because we
        // keep re-pushing on reject(requeue=true) below via a self-refilling broker.
        let task = serialized("quick_task");
        let task_id = task.metadata.id;
        let broker = RefillingBroker::new(BrokerMessage::new(task));

        let config = WorkerConfig {
            poll_interval_ms: 10,
            ..Default::default()
        };
        let worker: Worker<RefillingBroker, NoOpEventEmitter> =
            Worker::new_from_arc(Arc::clone(&broker), registry, config)
                .with_rate_limit_coordinator(coordinator);
        let handle = worker.run_with_shutdown().await.expect("worker starts");

        // Initially denied (deferred). Eventually the bucket refills and it runs.
        let mut ran = false;
        for _ in 0..400 {
            if runs.load(Ordering::Relaxed) >= 1 {
                ran = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(ran, "task should run after the limiter refills");
        assert!(
            broker.requeue_count(&task_id) >= 1,
            "task should have been deferred at least once before running"
        );

        handle.shutdown().await.expect("shutdown");
    }

    /// A broker that always re-delivers a single message: on reject(requeue) the
    /// message becomes available again; on ack it stops. Used to test that a
    /// deferred (rate-limited) task is eventually allowed after refill.
    struct RefillingBroker {
        message: BrokerMessage,
        available: Mutex<bool>,
        requeued: Mutex<HashMap<TaskId, usize>>,
        done: Mutex<bool>,
    }

    impl RefillingBroker {
        fn new(message: BrokerMessage) -> Arc<Self> {
            Arc::new(Self {
                message,
                available: Mutex::new(true),
                requeued: Mutex::new(HashMap::new()),
                done: Mutex::new(false),
            })
        }

        fn requeue_count(&self, id: &TaskId) -> usize {
            self.requeued
                .lock()
                .expect("lock")
                .get(id)
                .copied()
                .unwrap_or(0)
        }
    }

    #[async_trait::async_trait]
    impl Broker for RefillingBroker {
        async fn enqueue(&self, _task: SerializedTask) -> Result<TaskId> {
            Ok(TaskId::new_v4())
        }

        async fn dequeue(&self) -> Result<Option<BrokerMessage>> {
            if *self.done.lock().expect("lock") {
                return Ok(None);
            }
            let mut avail = self.available.lock().expect("lock");
            if *avail {
                *avail = false;
                Ok(Some(self.message.clone()))
            } else {
                Ok(None)
            }
        }

        async fn ack(&self, _task_id: &TaskId, _receipt_handle: Option<&str>) -> Result<()> {
            *self.done.lock().expect("lock") = true;
            Ok(())
        }

        async fn reject(
            &self,
            task_id: &TaskId,
            _receipt_handle: Option<&str>,
            requeue: bool,
        ) -> Result<()> {
            if requeue {
                *self
                    .requeued
                    .lock()
                    .expect("lock")
                    .entry(*task_id)
                    .or_insert(0) += 1;
                // Make the message available for the next dequeue (the defer).
                *self.available.lock().expect("lock") = true;
            } else {
                *self.done.lock().expect("lock") = true;
            }
            Ok(())
        }

        async fn queue_size(&self) -> Result<usize> {
            Ok(usize::from(*self.available.lock().expect("lock")))
        }

        async fn cancel(&self, _task_id: &TaskId) -> Result<bool> {
            Ok(false)
        }
    }

    /// With coalescing enabled, a batch of duplicate tasks (same coalescing key)
    /// is collapsed in the worker loop: only one survivor executes and the
    /// dropped duplicates are acked (removed) rather than executed.
    #[tokio::test]
    async fn test_loop_coalesces_duplicate_tasks() {
        use crate::batching::{BatchConfig, CoalesceStrategy};

        let runs = Arc::new(AtomicUsize::new(0));
        let registry = TaskRegistry::new();
        registry
            .register(QuickTask {
                runs: Arc::clone(&runs),
            })
            .await;

        // Three messages of the same task with identical payloads => identical
        // coalescing keys. Two should be dropped (acked) and one executed.
        let dup_a = serialized("quick_task");
        let dup_b = serialized("quick_task");
        let dup_c = serialized("quick_task");
        let id_a = dup_a.metadata.id;
        let id_b = dup_b.metadata.id;
        let id_c = dup_c.metadata.id;
        let broker = ControlledBroker::new(vec![
            BrokerMessage::new(dup_a),
            BrokerMessage::new(dup_b),
            BrokerMessage::new(dup_c),
        ]);

        let config = WorkerConfig {
            poll_interval_ms: 10,
            enable_batch_dequeue: true,
            batch_size: 10,
            enable_coalescing: true,
            coalescing_config: BatchConfig {
                max_batch_size: 10,
                max_wait_ms: 0,
                coalesce: true,
                strategy: CoalesceStrategy::KeepFirst,
            },
            ..Default::default()
        };
        let worker: Worker<ControlledBroker, NoOpEventEmitter> =
            Worker::new_from_arc(Arc::clone(&broker), registry, config);
        let handle = worker.run_with_shutdown().await.expect("worker starts");

        // Exactly one survivor executes; both duplicates are acked without running.
        let mut ok = false;
        for _ in 0..200 {
            let acked = broker.ack_count(&id_a) + broker.ack_count(&id_b) + broker.ack_count(&id_c);
            // 3 total acks expected: 2 coalesced-dropped + 1 executed survivor.
            if runs.load(Ordering::Relaxed) == 1 && acked == 3 {
                ok = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            ok,
            "expected exactly 1 execution and 3 acks, got runs={}, acks={}",
            runs.load(Ordering::Relaxed),
            broker.ack_count(&id_a) + broker.ack_count(&id_b) + broker.ack_count(&id_c),
        );
        // Duplicates must never execute more than once in total.
        assert_eq!(runs.load(Ordering::Relaxed), 1, "only the survivor runs");

        handle.shutdown().await.expect("shutdown");
    }

    /// With adaptive polling enabled, the worker still drains and executes
    /// available work (the controller governs only the empty-queue sleep).
    #[tokio::test]
    async fn test_loop_adaptive_poll_processes_work() {
        use crate::adaptive_poll::AdaptivePollConfig;

        let runs = Arc::new(AtomicUsize::new(0));
        let registry = TaskRegistry::new();
        registry
            .register(QuickTask {
                runs: Arc::clone(&runs),
            })
            .await;

        let task = serialized("quick_task");
        let id = task.metadata.id;
        let broker = ControlledBroker::new(vec![BrokerMessage::new(task)]);

        let config = WorkerConfig {
            poll_interval_ms: 10,
            enable_adaptive_poll: true,
            adaptive_poll_config: AdaptivePollConfig {
                min_interval_ms: 5,
                max_interval_ms: 40,
                backoff_numerator: 2,
                backoff_denominator: 1,
                empty_streak_before_backoff: 1,
                high_depth_threshold: 4,
            },
            ..Default::default()
        };
        let worker: Worker<ControlledBroker, NoOpEventEmitter> =
            Worker::new_from_arc(Arc::clone(&broker), registry, config);
        let handle = worker.run_with_shutdown().await.expect("worker starts");

        let mut done = false;
        for _ in 0..200 {
            if runs.load(Ordering::Relaxed) == 1 && broker.ack_count(&id) >= 1 {
                done = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(done, "adaptive-poll worker must still process the task");

        handle.shutdown().await.expect("shutdown");
    }

    /// A worker whose labels do NOT satisfy a task's affinity defers (requeues)
    /// the task without executing it.
    #[tokio::test]
    async fn test_affinity_defers_unservable_task() {
        use crate::affinity::{AffinityRegistry, TaskAffinity};
        use crate::WorkerLabels;

        let runs = Arc::new(AtomicUsize::new(0));
        let registry = TaskRegistry::new();
        registry
            .register(QuickTask {
                runs: Arc::clone(&runs),
            })
            .await;

        let task = serialized("quick_task");
        let id = task.metadata.id;
        let broker = ControlledBroker::new(vec![BrokerMessage::new(task)]);

        // Worker has only "cpu" but the task requires "gpu" -> defer.
        let config = WorkerConfig {
            poll_interval_ms: 10,
            worker_labels: WorkerLabels::from_iter(["cpu"]),
            ..Default::default()
        };
        let affinity =
            AffinityRegistry::new().with_task("quick_task", TaskAffinity::new().require("gpu"));

        let worker: Worker<ControlledBroker, NoOpEventEmitter> =
            Worker::new_from_arc(Arc::clone(&broker), registry, config).with_affinity(affinity);
        let handle = worker.run_with_shutdown().await.expect("worker starts");

        // Wait until the task has been deferred (requeued) at least once.
        let mut deferred = false;
        for _ in 0..200 {
            if broker.requeue_count(&id) >= 1 {
                deferred = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            deferred,
            "task must be deferred when affinity is unsatisfied"
        );
        // It must never have executed nor been acked.
        assert_eq!(runs.load(Ordering::Relaxed), 0, "task must not execute");
        assert_eq!(broker.ack_count(&id), 0, "deferred task must not be acked");

        handle.shutdown().await.expect("shutdown");
    }

    /// A worker whose labels satisfy a task's affinity executes it normally.
    #[tokio::test]
    async fn test_affinity_admits_servable_task() {
        use crate::affinity::{AffinityRegistry, TaskAffinity};
        use crate::WorkerLabels;

        let runs = Arc::new(AtomicUsize::new(0));
        let registry = TaskRegistry::new();
        registry
            .register(QuickTask {
                runs: Arc::clone(&runs),
            })
            .await;

        let task = serialized("quick_task");
        let id = task.metadata.id;
        let broker = ControlledBroker::new(vec![BrokerMessage::new(task)]);

        // Worker has the required label and lacks the anti-affinity one -> admit.
        let config = WorkerConfig {
            poll_interval_ms: 10,
            worker_labels: WorkerLabels::from_iter(["gpu", "region:eu"]),
            ..Default::default()
        };
        let affinity = AffinityRegistry::new().with_task(
            "quick_task",
            TaskAffinity::new()
                .require("gpu")
                .prefer("region:eu")
                .anti("spot"),
        );

        let worker: Worker<ControlledBroker, NoOpEventEmitter> =
            Worker::new_from_arc(Arc::clone(&broker), registry, config).with_affinity(affinity);
        let handle = worker.run_with_shutdown().await.expect("worker starts");

        let mut done = false;
        for _ in 0..200 {
            if runs.load(Ordering::Relaxed) == 1 && broker.ack_count(&id) >= 1 {
                done = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(done, "task must execute when affinity is satisfied");
        assert_eq!(broker.requeue_count(&id), 0, "servable task must not defer");

        handle.shutdown().await.expect("shutdown");
    }
}
