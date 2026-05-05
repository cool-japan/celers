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
    let id1 = broker
        .enqueue(task1)
        .await
        .expect("enqueue should succeed");
    let id2 = broker
        .enqueue(task2)
        .await
        .expect("enqueue should succeed");
    assert_ne!(id1, id2, "Each enqueue should return a unique TaskId");
}
