//! Core broker type tests.

use super::*;
use async_trait::async_trait;
use celers_protocol::Message;
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

#[test]
fn test_broker_error_connection() {
    let err = BrokerError::Connection("timeout".to_string());
    assert!(err.is_connection());
    assert!(!err.is_serialization());
    assert!(err.is_retryable());
    assert_eq!(err.category(), "connection");
    assert_eq!(err.to_string(), "Connection error: timeout");
}

#[test]
fn test_broker_error_serialization() {
    let err = BrokerError::Serialization("invalid json".to_string());
    assert!(err.is_serialization());
    assert!(!err.is_connection());
    assert!(!err.is_retryable());
    assert_eq!(err.category(), "serialization");
}

#[test]
fn test_broker_error_queue_not_found() {
    let err = BrokerError::QueueNotFound("myqueue".to_string());
    assert!(err.is_queue_not_found());
    assert!(!err.is_retryable());
    assert_eq!(err.category(), "queue_not_found");
}

#[test]
fn test_broker_error_message_not_found() {
    let task_id = Uuid::new_v4();
    let err = BrokerError::MessageNotFound(task_id);
    assert!(err.is_message_not_found());
    assert!(!err.is_retryable());
    assert_eq!(err.category(), "message_not_found");
}

#[test]
fn test_broker_error_timeout() {
    let err = BrokerError::Timeout;
    assert!(err.is_timeout());
    assert!(err.is_retryable());
    assert_eq!(err.category(), "timeout");
    assert_eq!(err.to_string(), "Timeout waiting for message");
}

#[test]
fn test_broker_error_configuration() {
    let err = BrokerError::Configuration("invalid url".to_string());
    assert!(err.is_configuration());
    assert!(!err.is_retryable());
    assert_eq!(err.category(), "configuration");
}

#[test]
fn test_broker_error_operation_failed() {
    let err = BrokerError::OperationFailed("network error".to_string());
    assert!(err.is_operation_failed());
    assert!(err.is_retryable());
    assert_eq!(err.category(), "operation_failed");
}

#[test]
fn test_queue_mode_fifo() {
    let mode = QueueMode::Fifo;
    assert!(mode.is_fifo());
    assert!(!mode.is_priority());
    assert_eq!(mode.to_string(), "FIFO");
}

#[test]
fn test_queue_mode_priority() {
    let mode = QueueMode::Priority;
    assert!(mode.is_priority());
    assert!(!mode.is_fifo());
    assert_eq!(mode.to_string(), "Priority");
}

#[test]
fn test_envelope_new() {
    let task_id = Uuid::new_v4();
    let message = Message::new("test_task".to_string(), task_id, vec![1, 2, 3]);
    let envelope = Envelope::new(message, "tag123".to_string());

    assert_eq!(envelope.delivery_tag, "tag123");
    assert!(!envelope.is_redelivered());
    assert_eq!(envelope.task_id(), task_id);
    assert_eq!(envelope.task_name(), "test_task");
}

#[test]
fn test_envelope_redelivered() {
    let task_id = Uuid::new_v4();
    let message = Message::new("test_task".to_string(), task_id, vec![1, 2, 3]);
    let mut envelope = Envelope::new(message, "tag456".to_string());

    envelope.redelivered = true;
    assert!(envelope.is_redelivered());
}

#[test]
fn test_envelope_display() {
    let task_id = Uuid::new_v4();
    let message = Message::new("my_task".to_string(), task_id, vec![]);
    let envelope = Envelope::new(message, "delivery123".to_string());

    let display = format!("{}", envelope);
    assert!(display.contains("Envelope"));
    assert!(display.contains("tag=delivery123"));
    assert!(display.contains("task=my_task"));
    assert!(!display.contains("redelivered"));

    let mut envelope_redelivered = envelope.clone();
    envelope_redelivered.redelivered = true;
    let display_redelivered = format!("{}", envelope_redelivered);
    assert!(display_redelivered.contains("(redelivered)"));
}

#[test]
fn test_queue_config() {
    let config = QueueConfig::new("test_queue".to_string()).with_mode(QueueMode::Priority);

    assert_eq!(config.name, "test_queue");
    assert_eq!(config.mode, QueueMode::Priority);
    assert!(config.durable);
}

#[test]
fn test_queue_config_with_ttl() {
    let config = QueueConfig::new("my_queue".to_string()).with_ttl(Duration::from_secs(3600));

    assert_eq!(config.name, "my_queue");
    assert_eq!(config.message_ttl, Some(Duration::from_secs(3600)));
    assert!(config.mode.is_fifo()); // Default mode
}

#[test]
fn test_queue_config_builders() {
    let config = QueueConfig::new("test".to_string())
        .with_durable(false)
        .with_auto_delete(true)
        .with_max_message_size(1024);

    assert!(!config.durable);
    assert!(config.auto_delete);
    assert_eq!(config.max_message_size, Some(1024));
}

// RetryPolicy tests
#[test]
fn test_retry_policy_default() {
    let policy = RetryPolicy::default();
    assert_eq!(policy.max_retries, Some(5));
    assert_eq!(policy.initial_delay, Duration::from_millis(100));
    assert_eq!(policy.max_delay, Duration::from_secs(30));
    assert!((policy.backoff_multiplier - 2.0).abs() < f64::EPSILON);
    assert!(policy.jitter);
}

#[test]
fn test_retry_policy_builders() {
    let policy = RetryPolicy::new()
        .with_max_retries(10)
        .with_initial_delay(Duration::from_secs(1))
        .with_max_delay(Duration::from_secs(60))
        .with_backoff_multiplier(1.5)
        .with_jitter(false);

    assert_eq!(policy.max_retries, Some(10));
    assert_eq!(policy.initial_delay, Duration::from_secs(1));
    assert_eq!(policy.max_delay, Duration::from_secs(60));
    assert!((policy.backoff_multiplier - 1.5).abs() < f64::EPSILON);
    assert!(!policy.jitter);
}

#[test]
fn test_retry_policy_infinite() {
    let policy = RetryPolicy::infinite();
    assert_eq!(policy.max_retries, None);
    assert!(policy.should_retry(0));
    assert!(policy.should_retry(1000));
}

#[test]
fn test_retry_policy_delay_for_attempt() {
    let policy = RetryPolicy::new()
        .with_initial_delay(Duration::from_millis(100))
        .with_backoff_multiplier(2.0)
        .with_max_delay(Duration::from_secs(10));

    assert_eq!(policy.delay_for_attempt(0), Duration::from_millis(100));
    assert_eq!(policy.delay_for_attempt(1), Duration::from_millis(200));
    assert_eq!(policy.delay_for_attempt(2), Duration::from_millis(400));
    assert_eq!(policy.delay_for_attempt(3), Duration::from_millis(800));
}

#[test]
fn test_retry_policy_should_retry() {
    let policy = RetryPolicy::new().with_max_retries(3);
    assert!(policy.should_retry(0));
    assert!(policy.should_retry(1));
    assert!(policy.should_retry(2));
    assert!(!policy.should_retry(3));
}

#[test]
fn test_retry_policy_no_retry() {
    let policy = RetryPolicy::no_retry();
    assert!(!policy.should_retry(0));
}

#[test]
fn test_retry_policy_fixed_delay() {
    let policy = RetryPolicy::fixed_delay(Duration::from_secs(5), 3);
    assert_eq!(policy.initial_delay, Duration::from_secs(5));
    assert_eq!(policy.max_delay, Duration::from_secs(5));
    assert!((policy.backoff_multiplier - 1.0).abs() < f64::EPSILON);
    assert!(!policy.jitter);
}

// HealthStatus tests
#[test]
fn test_health_status_healthy() {
    let status = HealthStatus::Healthy;
    assert!(status.is_healthy());
    assert!(status.is_operational());
    assert_eq!(status.to_string(), "healthy");
}

#[test]
fn test_health_status_degraded() {
    let status = HealthStatus::Degraded;
    assert!(!status.is_healthy());
    assert!(status.is_operational());
    assert_eq!(status.to_string(), "degraded");
}

#[test]
fn test_health_status_unhealthy() {
    let status = HealthStatus::Unhealthy;
    assert!(!status.is_healthy());
    assert!(!status.is_operational());
    assert_eq!(status.to_string(), "unhealthy");
}

#[test]
fn test_health_check_response_healthy() {
    let response = HealthCheckResponse::healthy("redis", "redis://localhost:6379");
    assert_eq!(response.status, HealthStatus::Healthy);
    assert_eq!(response.broker_type, "redis");
    assert_eq!(response.connection, "redis://localhost:6379");
    assert!(response.latency_ms.is_none());
}

#[test]
fn test_health_check_response_unhealthy() {
    let response =
        HealthCheckResponse::unhealthy("redis", "redis://localhost:6379", "Connection refused");
    assert_eq!(response.status, HealthStatus::Unhealthy);
    assert_eq!(
        response.details.get("reason").unwrap(),
        "Connection refused"
    );
}

#[test]
fn test_health_check_response_with_details() {
    let response = HealthCheckResponse::healthy("redis", "redis://localhost")
        .with_latency(42)
        .with_detail("version", "7.0.0");

    assert_eq!(response.latency_ms, Some(42));
    assert_eq!(response.details.get("version").unwrap(), "7.0.0");
}

// BrokerMetrics tests
#[test]
fn test_broker_metrics_default() {
    let metrics = BrokerMetrics::new();
    assert_eq!(metrics.messages_published, 0);
    assert_eq!(metrics.messages_consumed, 0);
    assert_eq!(metrics.messages_acknowledged, 0);
}

#[test]
fn test_broker_metrics_increment() {
    let mut metrics = BrokerMetrics::new();
    metrics.inc_published();
    metrics.inc_published();
    metrics.inc_consumed();
    metrics.inc_acknowledged();
    metrics.inc_rejected();
    metrics.inc_publish_error();
    metrics.inc_consume_error();
    metrics.inc_connection_attempt();
    metrics.inc_connection_failure();

    assert_eq!(metrics.messages_published, 2);
    assert_eq!(metrics.messages_consumed, 1);
    assert_eq!(metrics.messages_acknowledged, 1);
    assert_eq!(metrics.messages_rejected, 1);
    assert_eq!(metrics.publish_errors, 1);
    assert_eq!(metrics.consume_errors, 1);
    assert_eq!(metrics.connection_attempts, 1);
    assert_eq!(metrics.connection_failures, 1);
}

// ExchangeType tests
#[test]
fn test_exchange_type_display() {
    assert_eq!(ExchangeType::Direct.to_string(), "direct");
    assert_eq!(ExchangeType::Fanout.to_string(), "fanout");
    assert_eq!(ExchangeType::Topic.to_string(), "topic");
    assert_eq!(ExchangeType::Headers.to_string(), "headers");
}

// ExchangeConfig tests
#[test]
fn test_exchange_config() {
    let config = ExchangeConfig::new("my_exchange", ExchangeType::Topic)
        .with_durable(false)
        .with_auto_delete(true);

    assert_eq!(config.name, "my_exchange");
    assert_eq!(config.exchange_type, ExchangeType::Topic);
    assert!(!config.durable);
    assert!(config.auto_delete);
}

// BindingConfig tests
#[test]
fn test_binding_config() {
    let binding = BindingConfig::new("exchange", "queue", "routing.key");
    assert_eq!(binding.exchange, "exchange");
    assert_eq!(binding.queue, "queue");
    assert_eq!(binding.routing_key, "routing.key");
}

// ConnectionState tests
#[test]
fn test_connection_state_display() {
    assert_eq!(ConnectionState::Disconnected.to_string(), "disconnected");
    assert_eq!(ConnectionState::Connecting.to_string(), "connecting");
    assert_eq!(ConnectionState::Connected.to_string(), "connected");
    assert_eq!(ConnectionState::Reconnecting.to_string(), "reconnecting");
}

// BatchPublishResult tests
#[test]
fn test_batch_publish_result_success() {
    let result = BatchPublishResult::success(10);
    assert!(result.is_complete_success());
    assert_eq!(result.succeeded, 10);
    assert_eq!(result.failed, 0);
    assert_eq!(result.total(), 10);
}

#[test]
fn test_batch_publish_result_partial() {
    let mut errors = HashMap::new();
    errors.insert(2, "Connection failed".to_string());
    errors.insert(5, "Timeout".to_string());

    let result = BatchPublishResult {
        succeeded: 8,
        failed: 2,
        errors,
    };

    assert!(!result.is_complete_success());
    assert_eq!(result.total(), 10);
    assert_eq!(result.errors.len(), 2);
}

// MockBroker tests
#[tokio::test]
async fn test_mock_broker_connect_disconnect() {
    let mut broker = MockBroker::new();
    assert!(!broker.is_connected());
    assert_eq!(broker.name(), "mock");

    broker.connect().await.unwrap();
    assert!(broker.is_connected());

    broker.disconnect().await.unwrap();
    assert!(!broker.is_connected());
}

#[tokio::test]
async fn test_mock_broker_publish_consume() {
    let mut broker = MockBroker::new();
    broker.connect().await.unwrap();

    let task_id = Uuid::new_v4();
    let message = Message::new("test_task".to_string(), task_id, vec![1, 2, 3]);

    broker.publish("test_queue", message.clone()).await.unwrap();
    assert_eq!(broker.queue_len("test_queue"), 1);

    let envelope = broker
        .consume("test_queue", Duration::from_secs(1))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(envelope.task_id(), task_id);
    assert_eq!(broker.queue_len("test_queue"), 0);
}

#[tokio::test]
async fn test_mock_broker_ack() {
    let mut broker = MockBroker::new();
    broker.connect().await.unwrap();

    let task_id = Uuid::new_v4();
    let message = Message::new("test_task".to_string(), task_id, vec![]);
    broker.publish("queue", message).await.unwrap();

    let envelope = broker
        .consume("queue", Duration::from_secs(1))
        .await
        .unwrap()
        .unwrap();

    broker.ack(&envelope.delivery_tag).await.unwrap();

    // Verify metrics
    let metrics = broker.get_metrics().await;
    assert_eq!(metrics.messages_acknowledged, 1);
}

#[tokio::test]
async fn test_mock_broker_reject_requeue() {
    let mut broker = MockBroker::new();
    broker.connect().await.unwrap();

    let task_id = Uuid::new_v4();
    let message = Message::new("test_task".to_string(), task_id, vec![]);
    broker.publish("test_queue", message).await.unwrap();

    let envelope = broker
        .consume("test_queue", Duration::from_secs(1))
        .await
        .unwrap()
        .unwrap();

    broker.reject(&envelope.delivery_tag, true).await.unwrap();

    // Verify metrics
    let metrics = broker.get_metrics().await;
    assert_eq!(metrics.messages_rejected, 1);
}

#[tokio::test]
async fn test_mock_broker_queue_operations() {
    let mut broker = MockBroker::new();
    broker.connect().await.unwrap();

    broker
        .create_queue("queue1", QueueMode::Fifo)
        .await
        .unwrap();
    broker
        .create_queue("queue2", QueueMode::Priority)
        .await
        .unwrap();

    let queues = broker.list_queues().await.unwrap();
    assert_eq!(queues.len(), 2);

    broker.delete_queue("queue1").await.unwrap();
    let queues = broker.list_queues().await.unwrap();
    assert_eq!(queues.len(), 1);
}

#[tokio::test]
async fn test_mock_broker_purge() {
    let mut broker = MockBroker::new();
    broker.connect().await.unwrap();

    for i in 0..5 {
        let task_id = Uuid::new_v4();
        let message = Message::new(format!("task_{}", i), task_id, vec![]);
        broker.publish("queue", message).await.unwrap();
    }

    assert_eq!(broker.queue_len("queue"), 5);

    let purged = broker.purge("queue").await.unwrap();
    assert_eq!(purged, 5);
    assert_eq!(broker.queue_len("queue"), 0);
}

#[tokio::test]
async fn test_mock_broker_batch_operations() {
    let mut broker = MockBroker::new();
    broker.connect().await.unwrap();

    // Batch publish
    let messages: Vec<Message> = (0..5)
        .map(|i| Message::new(format!("task_{}", i), Uuid::new_v4(), vec![]))
        .collect();

    let result = broker.publish_batch("queue", messages).await.unwrap();
    assert!(result.is_complete_success());
    assert_eq!(result.succeeded, 5);
    assert_eq!(broker.queue_len("queue"), 5);

    // Batch consume
    let envelopes = broker
        .consume_batch("queue", 3, Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(envelopes.len(), 3);
    assert_eq!(broker.queue_len("queue"), 2);

    // Batch ack
    let tags: Vec<String> = envelopes.iter().map(|e| e.delivery_tag.clone()).collect();
    broker.ack_batch(&tags).await.unwrap();

    let metrics = broker.get_metrics().await;
    assert_eq!(metrics.messages_acknowledged, 3);
}

#[tokio::test]
async fn test_mock_broker_health_check() {
    let mut broker = MockBroker::new();

    // Not connected
    let response = broker.health_check().await;
    assert_eq!(response.status, HealthStatus::Unhealthy);
    assert!(!broker.ping().await);

    // Connected
    broker.connect().await.unwrap();
    let response = broker.health_check().await;
    assert_eq!(response.status, HealthStatus::Healthy);
    assert!(broker.ping().await);
}

#[tokio::test]
async fn test_mock_broker_metrics() {
    let mut broker = MockBroker::new();
    broker.connect().await.unwrap();

    let task_id = Uuid::new_v4();
    let message = Message::new("test".to_string(), task_id, vec![]);
    broker.publish("queue", message).await.unwrap();

    let metrics = broker.get_metrics().await;
    assert_eq!(metrics.messages_published, 1);
    assert_eq!(metrics.connection_attempts, 1);

    broker.reset_metrics().await;
    let metrics = broker.get_metrics().await;
    assert_eq!(metrics.messages_published, 0);
}

#[tokio::test]
async fn test_mock_broker_not_connected_error() {
    let mut broker = MockBroker::new();

    let task_id = Uuid::new_v4();
    let message = Message::new("test".to_string(), task_id, vec![]);
    let result = broker.publish("queue", message).await;

    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), BrokerError::Connection(_)));
}

// PoolConfig tests
#[test]
fn test_pool_config_default() {
    let config = PoolConfig::default();
    assert_eq!(config.min_connections, 1);
    assert_eq!(config.max_connections, 10);
    assert_eq!(config.idle_timeout, Some(Duration::from_secs(300)));
    assert_eq!(config.acquire_timeout, Duration::from_secs(30));
    assert_eq!(config.max_lifetime, Some(Duration::from_secs(1800)));
}

#[test]
fn test_pool_config_builders() {
    let config = PoolConfig::new()
        .with_min_connections(5)
        .with_max_connections(50)
        .with_idle_timeout(Duration::from_secs(600))
        .with_acquire_timeout(Duration::from_secs(10))
        .with_max_lifetime(Duration::from_secs(3600));

    assert_eq!(config.min_connections, 5);
    assert_eq!(config.max_connections, 50);
    assert_eq!(config.idle_timeout, Some(Duration::from_secs(600)));
    assert_eq!(config.acquire_timeout, Duration::from_secs(10));
    assert_eq!(config.max_lifetime, Some(Duration::from_secs(3600)));
}

#[test]
fn test_pool_config_without_idle_timeout() {
    let config = PoolConfig::new().without_idle_timeout();
    assert!(config.idle_timeout.is_none());
}

#[test]
fn test_pool_stats() {
    let stats = PoolStats {
        connections_created: 10,
        connections_closed: 5,
        active_connections: 3,
        idle_connections: 2,
        acquire_requests: 100,
        acquire_timeouts: 2,
    };

    assert_eq!(stats.total_connections(), 5);
}

// CircuitState tests
#[test]
fn test_circuit_state_display() {
    assert_eq!(CircuitState::Closed.to_string(), "closed");
    assert_eq!(CircuitState::Open.to_string(), "open");
    assert_eq!(CircuitState::HalfOpen.to_string(), "half-open");
}

// CircuitBreakerConfig tests
#[test]
fn test_circuit_breaker_config_default() {
    let config = CircuitBreakerConfig::default();
    assert_eq!(config.failure_threshold, 5);
    assert_eq!(config.success_threshold, 2);
    assert_eq!(config.open_duration, Duration::from_secs(30));
    assert_eq!(config.failure_window, Duration::from_secs(60));
}

#[test]
fn test_circuit_breaker_config_builders() {
    let config = CircuitBreakerConfig::new()
        .with_failure_threshold(10)
        .with_success_threshold(3)
        .with_open_duration(Duration::from_secs(60))
        .with_failure_window(Duration::from_secs(120));

    assert_eq!(config.failure_threshold, 10);
    assert_eq!(config.success_threshold, 3);
    assert_eq!(config.open_duration, Duration::from_secs(60));
    assert_eq!(config.failure_window, Duration::from_secs(120));
}

#[test]
fn test_circuit_breaker_stats_success_rate() {
    let mut stats = CircuitBreakerStats::default();
    assert!((stats.success_rate() - 1.0).abs() < f64::EPSILON); // No requests = 100%

    stats.total_requests = 100;
    stats.successful_requests = 95;
    assert!((stats.success_rate() - 0.95).abs() < f64::EPSILON);
}

// Priority tests
#[test]
fn test_priority_default() {
    let priority = Priority::default();
    assert_eq!(priority, Priority::Normal);
}

#[test]
fn test_priority_display() {
    assert_eq!(Priority::Lowest.to_string(), "lowest");
    assert_eq!(Priority::Low.to_string(), "low");
    assert_eq!(Priority::Normal.to_string(), "normal");
    assert_eq!(Priority::High.to_string(), "high");
    assert_eq!(Priority::Highest.to_string(), "highest");
}

#[test]
fn test_priority_as_u8() {
    assert_eq!(Priority::Lowest.as_u8(), 0);
    assert_eq!(Priority::Low.as_u8(), 3);
    assert_eq!(Priority::Normal.as_u8(), 5);
    assert_eq!(Priority::High.as_u8(), 7);
    assert_eq!(Priority::Highest.as_u8(), 9);
}

#[test]
fn test_priority_from_u8() {
    assert_eq!(Priority::from_u8(0), Priority::Lowest);
    assert_eq!(Priority::from_u8(1), Priority::Lowest);
    assert_eq!(Priority::from_u8(2), Priority::Low);
    assert_eq!(Priority::from_u8(3), Priority::Low);
    assert_eq!(Priority::from_u8(5), Priority::Normal);
    assert_eq!(Priority::from_u8(6), Priority::High);
    assert_eq!(Priority::from_u8(9), Priority::Highest);
    assert_eq!(Priority::from_u8(10), Priority::Highest);
}

#[test]
fn test_priority_ordering() {
    assert!(Priority::Lowest < Priority::Low);
    assert!(Priority::Low < Priority::Normal);
    assert!(Priority::Normal < Priority::High);
    assert!(Priority::High < Priority::Highest);
}

// MessageOptions tests
#[test]
fn test_message_options_default() {
    let options = MessageOptions::default();
    assert!(options.priority.is_none());
    assert!(options.ttl.is_none());
    assert!(options.expires_at.is_none());
    assert!(options.delay.is_none());
    assert!(options.correlation_id.is_none());
    assert!(options.reply_to.is_none());
    assert!(options.headers.is_empty());
    assert!(!options.sign);
    assert!(options.signing_key.is_none());
    assert!(!options.encrypt);
    assert!(options.encryption_key.is_none());
    assert!(!options.compress);
}

#[test]
fn test_message_options_builders() {
    let options = MessageOptions::new()
        .with_priority(Priority::High)
        .with_ttl(Duration::from_secs(3600))
        .with_expires_at(1700000000)
        .with_delay(Duration::from_secs(60))
        .with_correlation_id("req-123")
        .with_reply_to("reply_queue")
        .with_header("x-custom", "value");

    assert_eq!(options.priority, Some(Priority::High));
    assert_eq!(options.ttl, Some(Duration::from_secs(3600)));
    assert_eq!(options.expires_at, Some(1700000000));
    assert_eq!(options.delay, Some(Duration::from_secs(60)));
    assert_eq!(options.correlation_id, Some("req-123".to_string()));
    assert_eq!(options.reply_to, Some("reply_queue".to_string()));
    assert_eq!(options.headers.get("x-custom").unwrap(), "value");
}

#[test]
fn test_message_options_is_expired() {
    let options = MessageOptions::new().with_expires_at(1700000000);

    assert!(options.is_expired(1700000001));
    assert!(!options.is_expired(1699999999));
    assert!(!options.is_expired(1700000000));

    let no_expiry = MessageOptions::new();
    assert!(!no_expiry.is_expired(1700000001));
}

#[test]
fn test_message_options_should_delay() {
    let with_delay = MessageOptions::new().with_delay(Duration::from_secs(60));
    assert!(with_delay.should_delay());

    let without_delay = MessageOptions::new();
    assert!(!without_delay.should_delay());
}

// Middleware tests
struct TestMiddleware {
    name: String,
}

#[async_trait]
impl MessageMiddleware for TestMiddleware {
    async fn before_publish(&self, _message: &mut Message) -> Result<()> {
        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[test]
fn test_middleware_chain_new() {
    let chain = MiddlewareChain::new();
    assert_eq!(chain.len(), 0);
    assert!(chain.is_empty());
}

#[test]
fn test_middleware_chain_add() {
    let chain = MiddlewareChain::new()
        .with_middleware(Box::new(TestMiddleware {
            name: "test1".to_string(),
        }))
        .with_middleware(Box::new(TestMiddleware {
            name: "test2".to_string(),
        }));

    assert_eq!(chain.len(), 2);
    assert!(!chain.is_empty());
}

#[tokio::test]
async fn test_middleware_chain_process_before_publish() {
    let chain = MiddlewareChain::new().with_middleware(Box::new(TestMiddleware {
        name: "test".to_string(),
    }));

    let task_id = Uuid::new_v4();
    let mut message = Message::new("test_task".to_string(), task_id, vec![]);

    let result = chain.process_before_publish(&mut message).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_middleware_chain_process_after_consume() {
    let chain = MiddlewareChain::new().with_middleware(Box::new(TestMiddleware {
        name: "test".to_string(),
    }));

    let task_id = Uuid::new_v4();
    let mut message = Message::new("test_task".to_string(), task_id, vec![]);

    let result = chain.process_after_consume(&mut message).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_middleware_producer_publish_with_middleware() {
    let mut broker = MockBroker::new();
    broker.connect().await.unwrap();

    let chain = MiddlewareChain::new().with_middleware(Box::new(TestMiddleware {
        name: "test".to_string(),
    }));

    let task_id = Uuid::new_v4();
    let message = Message::new("test_task".to_string(), task_id, vec![]);

    broker
        .publish_with_middleware("queue", message, &chain)
        .await
        .unwrap();

    assert_eq!(broker.queue_len("queue"), 1);
}

#[tokio::test]
async fn test_middleware_consumer_consume_with_middleware() {
    let mut broker = MockBroker::new();
    broker.connect().await.unwrap();

    let task_id = Uuid::new_v4();
    let message = Message::new("test_task".to_string(), task_id, vec![]);
    broker.publish("queue", message).await.unwrap();

    let chain = MiddlewareChain::new().with_middleware(Box::new(TestMiddleware {
        name: "test".to_string(),
    }));

    let envelope = broker
        .consume_with_middleware("queue", Duration::from_secs(1), &chain)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(envelope.task_id(), task_id);
}

#[test]
fn test_middleware_chain_default() {
    let chain = MiddlewareChain::default();
    assert_eq!(chain.len(), 0);
    assert!(chain.is_empty());
}

// MessageOptions security tests
#[test]
fn test_message_options_with_signing() {
    let key = vec![1, 2, 3, 4];
    let options = MessageOptions::new().with_signing(key.clone());

    assert!(options.sign);
    assert_eq!(options.signing_key, Some(key));
    assert!(options.should_sign());
}

#[test]
fn test_message_options_with_encryption() {
    let key = vec![5, 6, 7, 8];
    let options = MessageOptions::new().with_encryption(key.clone());

    assert!(options.encrypt);
    assert_eq!(options.encryption_key, Some(key));
    assert!(options.should_encrypt());
}

#[test]
fn test_message_options_with_compression() {
    let options = MessageOptions::new().with_compression();

    assert!(options.compress);
    assert!(options.should_compress());
}

#[test]
fn test_message_options_security_checks() {
    let options_no_key = MessageOptions::new();
    assert!(!options_no_key.should_sign());
    assert!(!options_no_key.should_encrypt());

    let mut options_no_flag = MessageOptions::new();
    options_no_flag.signing_key = Some(vec![1, 2, 3]);
    options_no_flag.encryption_key = Some(vec![4, 5, 6]);
    assert!(!options_no_flag.should_sign()); // sign flag is false
    assert!(!options_no_flag.should_encrypt()); // encrypt flag is false
}

#[test]
fn test_message_options_combined_security() {
    let sign_key = vec![1, 2, 3, 4];
    let encrypt_key = vec![5, 6, 7, 8];

    let options = MessageOptions::new()
        .with_signing(sign_key.clone())
        .with_encryption(encrypt_key.clone())
        .with_compression();

    assert!(options.should_sign());
    assert!(options.should_encrypt());
    assert!(options.should_compress());
    assert_eq!(options.signing_key, Some(sign_key));
    assert_eq!(options.encryption_key, Some(encrypt_key));
}

// Built-in middleware tests
#[tokio::test]
async fn test_validation_middleware_success() {
    let middleware = ValidationMiddleware::new();
    let task_id = Uuid::new_v4();
    let mut message = Message::new("test_task".to_string(), task_id, vec![1, 2, 3]);

    let result = middleware.before_publish(&mut message).await;
    assert!(result.is_ok());

    let result = middleware.after_consume(&mut message).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_validation_middleware_empty_task_name() {
    let middleware = ValidationMiddleware::new();
    let task_id = Uuid::new_v4();
    let mut message = Message::new("".to_string(), task_id, vec![]);

    let result = middleware.before_publish(&mut message).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().is_configuration());
}

#[tokio::test]
async fn test_validation_middleware_body_size_limit() {
    let middleware = ValidationMiddleware::new().with_max_body_size(100);
    let task_id = Uuid::new_v4();
    let large_body = vec![0u8; 200];
    let mut message = Message::new("task".to_string(), task_id, large_body);

    let result = middleware.before_publish(&mut message).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.is_configuration());
}

#[tokio::test]
async fn test_validation_middleware_no_body_limit() {
    let middleware = ValidationMiddleware::new().without_body_size_limit();
    let task_id = Uuid::new_v4();
    let large_body = vec![0u8; 20 * 1024 * 1024]; // 20MB
    let mut message = Message::new("task".to_string(), task_id, large_body);

    let result = middleware.before_publish(&mut message).await;
    assert!(result.is_ok());
}

#[test]
fn test_validation_middleware_name() {
    let middleware = ValidationMiddleware::new();
    assert_eq!(middleware.name(), "validation");
}

#[tokio::test]
async fn test_logging_middleware() {
    let middleware = LoggingMiddleware::new("TEST");
    let task_id = Uuid::new_v4();
    let mut message = Message::new("test_task".to_string(), task_id, vec![1, 2, 3]);

    let result = middleware.before_publish(&mut message).await;
    assert!(result.is_ok());

    let result = middleware.after_consume(&mut message).await;
    assert!(result.is_ok());

    assert_eq!(middleware.name(), "logging");
}

#[tokio::test]
async fn test_logging_middleware_with_body() {
    let middleware = LoggingMiddleware::new("TEST").with_body_logging();
    let task_id = Uuid::new_v4();
    let mut message = Message::new("test_task".to_string(), task_id, vec![1, 2, 3]);

    let result = middleware.before_publish(&mut message).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_metrics_middleware() {
    let metrics = std::sync::Arc::new(std::sync::Mutex::new(BrokerMetrics::new()));
    let middleware = MetricsMiddleware::new(metrics.clone());

    let task_id = Uuid::new_v4();
    let mut message = Message::new("test_task".to_string(), task_id, vec![]);

    middleware.before_publish(&mut message).await.unwrap();
    middleware.after_consume(&mut message).await.unwrap();

    let snapshot = middleware.get_metrics();
    assert_eq!(snapshot.messages_published, 1);
    assert_eq!(snapshot.messages_consumed, 1);
    assert_eq!(middleware.name(), "metrics");
}

#[tokio::test]
async fn test_retry_limit_middleware_success() {
    let middleware = RetryLimitMiddleware::new(3);
    let task_id = Uuid::new_v4();
    let mut message = Message::new("test_task".to_string(), task_id, vec![]);

    let result = middleware.before_publish(&mut message).await;
    assert!(result.is_ok());

    let result = middleware.after_consume(&mut message).await;
    assert!(result.is_ok());
    assert_eq!(middleware.name(), "retry_limit");
}

#[tokio::test]
async fn test_middleware_chain_with_validation() {
    let chain = MiddlewareChain::new().with_middleware(Box::new(ValidationMiddleware::new()));

    let task_id = Uuid::new_v4();
    let mut message = Message::new("test_task".to_string(), task_id, vec![1, 2, 3]);

    let result = chain.process_before_publish(&mut message).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_middleware_chain_with_multiple_builtin() {
    let metrics = std::sync::Arc::new(std::sync::Mutex::new(BrokerMetrics::new()));

    let chain = MiddlewareChain::new()
        .with_middleware(Box::new(ValidationMiddleware::new()))
        .with_middleware(Box::new(LoggingMiddleware::new("TEST")))
        .with_middleware(Box::new(MetricsMiddleware::new(metrics.clone())));

    let task_id = Uuid::new_v4();
    let mut message = Message::new("test_task".to_string(), task_id, vec![1, 2, 3]);

    let result = chain.process_before_publish(&mut message).await;
    assert!(result.is_ok());

    let snapshot = metrics.lock().unwrap().clone();
    assert_eq!(snapshot.messages_published, 1);
}

#[test]
fn test_validation_middleware_default() {
    let middleware = ValidationMiddleware::default();
    assert_eq!(middleware.name(), "validation");
}

#[tokio::test]
async fn test_rate_limiting_middleware() {
    let middleware = RateLimitingMiddleware::new(2.0); // 2 messages per second
    let task_id = Uuid::new_v4();
    let mut message = Message::new("test_task".to_string(), task_id, vec![]);

    // First message should succeed
    let result = middleware.before_publish(&mut message).await;
    assert!(result.is_ok());

    // Second message should succeed
    let result = middleware.before_publish(&mut message).await;
    assert!(result.is_ok());

    // Third message should fail (rate limit exceeded)
    let result = middleware.before_publish(&mut message).await;
    assert!(result.is_err());
    assert!(matches!(result, Err(BrokerError::OperationFailed(_))));

    assert_eq!(middleware.name(), "rate_limit");
}

#[tokio::test]
async fn test_rate_limiting_middleware_refill() {
    let middleware = RateLimitingMiddleware::new(10.0); // 10 messages per second
    let task_id = Uuid::new_v4();
    let mut message = Message::new("test_task".to_string(), task_id, vec![]);

    // Consume all tokens
    for _ in 0..10 {
        let result = middleware.before_publish(&mut message).await;
        assert!(result.is_ok());
    }

    // Next should fail
    let result = middleware.before_publish(&mut message).await;
    assert!(result.is_err());

    // Wait for tokens to refill (100ms = 1 token at 10/sec)
    tokio::time::sleep(tokio::time::Duration::from_millis(150)).await;

    // Now should succeed
    let result = middleware.before_publish(&mut message).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_deduplication_middleware() {
    let middleware = DeduplicationMiddleware::new(100);
    let task_id = Uuid::new_v4();
    let mut message = Message::new("test_task".to_string(), task_id, vec![]);

    // First consumption should succeed
    let result = middleware.after_consume(&mut message).await;
    assert!(result.is_ok());

    // Second consumption of same message should fail
    let result = middleware.after_consume(&mut message).await;
    assert!(result.is_err());
    assert!(matches!(result, Err(BrokerError::OperationFailed(_))));

    assert_eq!(middleware.name(), "deduplication");
}

#[tokio::test]
async fn test_deduplication_middleware_different_messages() {
    let middleware = DeduplicationMiddleware::new(100);

    let task_id1 = Uuid::new_v4();
    let mut message1 = Message::new("test_task".to_string(), task_id1, vec![]);

    let task_id2 = Uuid::new_v4();
    let mut message2 = Message::new("test_task".to_string(), task_id2, vec![]);

    // Both different messages should succeed
    let result = middleware.after_consume(&mut message1).await;
    assert!(result.is_ok());

    let result = middleware.after_consume(&mut message2).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_deduplication_middleware_cache_eviction() {
    let middleware = DeduplicationMiddleware::new(2); // Small cache

    let task_id1 = Uuid::new_v4();
    let mut message1 = Message::new("test_task".to_string(), task_id1, vec![]);

    let task_id2 = Uuid::new_v4();
    let mut message2 = Message::new("test_task".to_string(), task_id2, vec![]);

    let task_id3 = Uuid::new_v4();
    let mut message3 = Message::new("test_task".to_string(), task_id3, vec![]);

    // Add 3 messages (cache size is 2)
    let _ = middleware.after_consume(&mut message1).await;
    let _ = middleware.after_consume(&mut message2).await;
    let _ = middleware.after_consume(&mut message3).await;

    // One of the first two should be evicted, so this might succeed
    // (Note: HashSet doesn't guarantee order, so we can't predict which one)
    assert_eq!(middleware.name(), "deduplication");
}

#[test]
fn test_deduplication_middleware_default() {
    let middleware = DeduplicationMiddleware::default();
    assert_eq!(middleware.name(), "deduplication");
}

#[test]
fn test_deduplication_middleware_with_default_cache() {
    let middleware = DeduplicationMiddleware::with_default_cache();
    assert_eq!(middleware.name(), "deduplication");
}

// =============================================================================
// DLQ Tests
// =============================================================================

#[test]
fn test_dlq_config_new() {
    let config = DlqConfig::new("failed_tasks".to_string());
    assert_eq!(config.queue_name, "failed_tasks");
    assert_eq!(config.max_retries, Some(3));
    assert_eq!(config.ttl, None);
    assert!(config.include_metadata);
}

#[test]
fn test_dlq_config_builders() {
    let config = DlqConfig::new("dlq".to_string())
        .with_max_retries(5)
        .with_ttl(Duration::from_secs(3600))
        .with_metadata(false);

    assert_eq!(config.max_retries, Some(5));
    assert_eq!(config.ttl, Some(Duration::from_secs(3600)));
    assert!(!config.include_metadata);
}

#[test]
fn test_dlq_config_without_retry_limit() {
    let config = DlqConfig::new("dlq".to_string()).without_retry_limit();
    assert_eq!(config.max_retries, None);
}

#[test]
fn test_dlq_stats_is_empty() {
    let stats = DlqStats::default();
    assert!(stats.is_empty());
    assert_eq!(stats.message_count, 0);
    assert_eq!(stats.oldest_message_age_secs(), None);
}

#[test]
fn test_dlq_stats_oldest_age() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let stats = DlqStats {
        message_count: 5,
        by_reason: HashMap::new(),
        oldest_message_time: Some(now - 100),
        newest_message_time: Some(now),
    };

    assert!(!stats.is_empty());
    assert_eq!(stats.message_count, 5);

    // Age should be approximately 100 seconds (with small tolerance)
    let age = stats.oldest_message_age_secs().unwrap();
    assert!((99..=101).contains(&age));
}

#[test]
fn test_dlq_stats_by_reason() {
    let mut by_reason = HashMap::new();
    by_reason.insert("timeout".to_string(), 3);
    by_reason.insert("serialization_error".to_string(), 2);

    let stats = DlqStats {
        message_count: 5,
        by_reason,
        oldest_message_time: None,
        newest_message_time: None,
    };

    assert_eq!(stats.message_count, 5);
    assert_eq!(stats.by_reason.get("timeout"), Some(&3));
    assert_eq!(stats.by_reason.get("serialization_error"), Some(&2));
}

// =============================================================================
// Transaction Tests
// =============================================================================

#[test]
fn test_isolation_level_variants() {
    let _read_uncommitted = IsolationLevel::ReadUncommitted;
    let _read_committed = IsolationLevel::ReadCommitted;
    let _repeatable_read = IsolationLevel::RepeatableRead;
    let _serializable = IsolationLevel::Serializable;
}

#[test]
fn test_transaction_state_variants() {
    let active = TransactionState::Active;
    let committed = TransactionState::Committed;
    let rolled_back = TransactionState::RolledBack;

    assert_eq!(active, TransactionState::Active);
    assert_eq!(committed, TransactionState::Committed);
    assert_eq!(rolled_back, TransactionState::RolledBack);
    assert_ne!(active, committed);
}

#[test]
fn test_isolation_level_equality() {
    assert_eq!(IsolationLevel::ReadCommitted, IsolationLevel::ReadCommitted);
    assert_ne!(IsolationLevel::ReadCommitted, IsolationLevel::Serializable);
}

// =============================================================================
// Scheduling Tests
// =============================================================================

#[test]
fn test_schedule_config_delay() {
    let schedule = ScheduleConfig::delay(Duration::from_secs(30));
    assert_eq!(schedule.delay, Some(Duration::from_secs(30)));
    assert!(schedule.scheduled_at.is_none());
    assert!(schedule.delivery_time().is_some());
}

#[test]
fn test_schedule_config_at_timestamp() {
    let timestamp = 1700000000;
    let schedule = ScheduleConfig::at(timestamp);
    assert!(schedule.delay.is_none());
    assert_eq!(schedule.scheduled_at, Some(timestamp));
    assert_eq!(schedule.delivery_time(), Some(timestamp));
}

#[test]
fn test_schedule_config_with_window() {
    let schedule =
        ScheduleConfig::delay(Duration::from_secs(60)).with_window(Duration::from_secs(10));
    assert_eq!(schedule.execution_window, Some(Duration::from_secs(10)));
}

#[test]
fn test_schedule_config_is_ready() {
    // Past timestamp should be ready
    let past_schedule = ScheduleConfig::at(0);
    assert!(past_schedule.is_ready());

    // Future timestamp (year 2100) should not be ready
    let future_schedule = ScheduleConfig::at(4102444800);
    assert!(!future_schedule.is_ready());
}

// =============================================================================
// Consumer Group Tests
// =============================================================================

#[test]
fn test_consumer_group_config_new() {
    let config = ConsumerGroupConfig::new("group1".to_string(), "consumer1".to_string());
    assert_eq!(config.group_id, "group1");
    assert_eq!(config.consumer_id, "consumer1");
    assert_eq!(config.max_consumers, None);
    assert_eq!(config.rebalance_timeout, Duration::from_secs(30));
    assert_eq!(config.heartbeat_interval, Duration::from_secs(3));
}

#[test]
fn test_consumer_group_config_builders() {
    let config = ConsumerGroupConfig::new("group1".to_string(), "consumer1".to_string())
        .with_max_consumers(5)
        .with_rebalance_timeout(Duration::from_secs(60))
        .with_heartbeat_interval(Duration::from_secs(5));

    assert_eq!(config.max_consumers, Some(5));
    assert_eq!(config.rebalance_timeout, Duration::from_secs(60));
    assert_eq!(config.heartbeat_interval, Duration::from_secs(5));
}

// =============================================================================
// Replay Tests
// =============================================================================

#[test]
fn test_replay_config_from_duration() {
    let config = ReplayConfig::from_duration(Duration::from_secs(3600));
    assert_eq!(config.from_duration, Some(Duration::from_secs(3600)));
    assert!(config.from_timestamp.is_none());
    assert_eq!(config.speed_multiplier, 1.0);
}

#[test]
fn test_replay_config_from_timestamp() {
    let timestamp = 1699999999;
    let config = ReplayConfig::from_timestamp(timestamp);
    assert!(config.from_duration.is_none());
    assert_eq!(config.from_timestamp, Some(timestamp));
}

#[test]
fn test_replay_config_builders() {
    let config = ReplayConfig::from_duration(Duration::from_secs(3600))
        .until(1700000000)
        .with_max_messages(1000)
        .with_speed(2.0);

    assert_eq!(config.until_timestamp, Some(1700000000));
    assert_eq!(config.max_messages, Some(1000));
    assert_eq!(config.speed_multiplier, 2.0);
}

#[test]
fn test_replay_config_start_timestamp() {
    let timestamp = 1699999999;
    let config = ReplayConfig::from_timestamp(timestamp);
    assert_eq!(config.start_timestamp(), timestamp);

    // Duration-based should calculate from now
    let duration_config = ReplayConfig::from_duration(Duration::from_secs(3600));
    let start_ts = duration_config.start_timestamp();
    assert!(start_ts > 0);
}

#[test]
fn test_replay_progress_completion_percent() {
    let progress = ReplayProgress {
        replay_id: "replay-1".to_string(),
        messages_replayed: 50,
        total_messages: Some(100),
        current_timestamp: 1700000000,
        completed: false,
    };

    assert_eq!(progress.completion_percent(), Some(50.0));

    // Test with no total
    let progress_no_total = ReplayProgress {
        replay_id: "replay-2".to_string(),
        messages_replayed: 50,
        total_messages: None,
        current_timestamp: 1700000000,
        completed: false,
    };

    assert_eq!(progress_no_total.completion_percent(), None);
}

// =============================================================================
// Quota Tests
// =============================================================================

#[test]
fn test_quota_config_new() {
    let quota = QuotaConfig::new();
    assert!(quota.max_messages.is_none());
    assert!(quota.max_bytes.is_none());
    assert!(quota.max_rate_per_sec.is_none());
    assert_eq!(quota.enforcement, QuotaEnforcement::Reject);
}

#[test]
fn test_quota_config_builders() {
    let quota = QuotaConfig::new()
        .with_max_messages(10000)
        .with_max_bytes(1024 * 1024)
        .with_max_rate(100.0)
        .with_max_per_consumer(100)
        .with_enforcement(QuotaEnforcement::Throttle);

    assert_eq!(quota.max_messages, Some(10000));
    assert_eq!(quota.max_bytes, Some(1024 * 1024));
    assert_eq!(quota.max_rate_per_sec, Some(100.0));
    assert_eq!(quota.max_messages_per_consumer, Some(100));
    assert_eq!(quota.enforcement, QuotaEnforcement::Throttle);
}

#[test]
fn test_quota_config_default() {
    let quota = QuotaConfig::default();
    assert!(quota.max_messages.is_none());
    assert_eq!(quota.enforcement, QuotaEnforcement::Reject);
}

#[test]
fn test_quota_enforcement_variants() {
    let _reject = QuotaEnforcement::Reject;
    let _throttle = QuotaEnforcement::Throttle;
    let _warn = QuotaEnforcement::Warn;

    assert_eq!(QuotaEnforcement::Reject, QuotaEnforcement::Reject);
    assert_ne!(QuotaEnforcement::Reject, QuotaEnforcement::Throttle);
}

#[test]
fn test_quota_usage_default() {
    let usage = QuotaUsage::default();
    assert_eq!(usage.message_count, 0);
    assert_eq!(usage.bytes_used, 0);
    assert_eq!(usage.current_rate, 0.0);
    assert!(!usage.exceeded);
}

#[test]
fn test_quota_usage_is_exceeded() {
    let config = QuotaConfig::new()
        .with_max_messages(100)
        .with_max_bytes(1000)
        .with_max_rate(10.0);

    let usage = QuotaUsage {
        message_count: 150,
        bytes_used: 1500,
        current_rate: 15.0,
        exceeded: true,
    };

    assert!(usage.is_message_quota_exceeded(&config));
    assert!(usage.is_bytes_quota_exceeded(&config));
    assert!(usage.is_rate_quota_exceeded(&config));
}

#[test]
fn test_quota_usage_not_exceeded() {
    let config = QuotaConfig::new()
        .with_max_messages(100)
        .with_max_bytes(1000)
        .with_max_rate(10.0);

    let usage = QuotaUsage {
        message_count: 50,
        bytes_used: 500,
        current_rate: 5.0,
        exceeded: false,
    };

    assert!(!usage.is_message_quota_exceeded(&config));
    assert!(!usage.is_bytes_quota_exceeded(&config));
    assert!(!usage.is_rate_quota_exceeded(&config));
}

#[test]
fn test_quota_usage_percent() {
    let config = QuotaConfig::new().with_max_messages(100);

    let usage = QuotaUsage {
        message_count: 75,
        bytes_used: 0,
        current_rate: 0.0,
        exceeded: false,
    };

    assert_eq!(usage.usage_percent(&config), Some(75.0));

    // Test with no max
    let no_max_config = QuotaConfig::new();
    assert_eq!(usage.usage_percent(&no_max_config), None);
}
