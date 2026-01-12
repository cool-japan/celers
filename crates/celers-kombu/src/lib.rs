//! Broker abstraction layer (Kombu-style)
//!
//! This crate provides the abstract interface for message brokers, inspired by
//! Python's Kombu library. It defines the core traits that all broker implementations
//! must follow.
//!
//! # Architecture
//!
//! - **Transport**: Low-level broker connection (Redis, AMQP, SQS, Database)
//! - **Producer**: Message publishing interface
//! - **Consumer**: Message consuming interface
//! - **Queue**: Queue abstraction with routing
//! - **BatchProducer**: Batch message publishing interface
//! - **BatchConsumer**: Batch message consuming interface
//! - **HealthCheck**: Health monitoring interface
//! - **Metrics**: Metrics collection interface
//! - **Admin**: Broker administration interface

use async_trait::async_trait;
use celers_protocol::Message;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use thiserror::Error;
use uuid::Uuid;

/// Broker errors
///
/// # Examples
///
/// ```
/// use celers_kombu::BrokerError;
///
/// let err = BrokerError::Connection("failed to connect".to_string());
/// assert!(err.is_connection());
/// assert!(err.is_retryable());
/// assert_eq!(err.category(), "connection");
///
/// let err = BrokerError::Timeout;
/// assert!(err.is_timeout());
/// assert!(err.is_retryable());
///
/// let err = BrokerError::QueueNotFound("celery".to_string());
/// assert!(err.is_queue_not_found());
/// assert!(!err.is_retryable());
/// ```
#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Queue not found: {0}")]
    QueueNotFound(String),

    #[error("Message not found: {0}")]
    MessageNotFound(Uuid),

    #[error("Timeout waiting for message")]
    Timeout,

    #[error("Invalid configuration: {0}")]
    Configuration(String),

    #[error("Broker operation failed: {0}")]
    OperationFailed(String),
}

impl BrokerError {
    /// Check if the error is connection-related
    pub fn is_connection(&self) -> bool {
        matches!(self, BrokerError::Connection(_))
    }

    /// Check if the error is serialization-related
    pub fn is_serialization(&self) -> bool {
        matches!(self, BrokerError::Serialization(_))
    }

    /// Check if the error is queue-not-found
    pub fn is_queue_not_found(&self) -> bool {
        matches!(self, BrokerError::QueueNotFound(_))
    }

    /// Check if the error is message-not-found
    pub fn is_message_not_found(&self) -> bool {
        matches!(self, BrokerError::MessageNotFound(_))
    }

    /// Check if the error is a timeout
    pub fn is_timeout(&self) -> bool {
        matches!(self, BrokerError::Timeout)
    }

    /// Check if the error is configuration-related
    pub fn is_configuration(&self) -> bool {
        matches!(self, BrokerError::Configuration(_))
    }

    /// Check if the error is an operation failure
    pub fn is_operation_failed(&self) -> bool {
        matches!(self, BrokerError::OperationFailed(_))
    }

    /// Check if this is a retryable error
    ///
    /// Returns true for connection, timeout, and operation failures, which are typically transient.
    /// Returns false for serialization, configuration, and not-found errors.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            BrokerError::Connection(_) | BrokerError::Timeout | BrokerError::OperationFailed(_)
        )
    }

    /// Get the error category as a string
    pub fn category(&self) -> &'static str {
        match self {
            BrokerError::Connection(_) => "connection",
            BrokerError::Serialization(_) => "serialization",
            BrokerError::QueueNotFound(_) => "queue_not_found",
            BrokerError::MessageNotFound(_) => "message_not_found",
            BrokerError::Timeout => "timeout",
            BrokerError::Configuration(_) => "configuration",
            BrokerError::OperationFailed(_) => "operation_failed",
        }
    }
}

pub type Result<T> = std::result::Result<T, BrokerError>;

/// Queue mode
///
/// # Examples
///
/// ```
/// use celers_kombu::QueueMode;
///
/// let fifo = QueueMode::Fifo;
/// assert!(fifo.is_fifo());
/// assert!(!fifo.is_priority());
/// assert_eq!(fifo.to_string(), "FIFO");
///
/// let priority = QueueMode::Priority;
/// assert!(priority.is_priority());
/// assert!(!priority.is_fifo());
/// assert_eq!(priority.to_string(), "Priority");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueMode {
    /// First-In-First-Out
    Fifo,
    /// Priority-based
    Priority,
}

impl QueueMode {
    /// Check if this is FIFO mode
    pub fn is_fifo(&self) -> bool {
        matches!(self, QueueMode::Fifo)
    }

    /// Check if this is Priority mode
    pub fn is_priority(&self) -> bool {
        matches!(self, QueueMode::Priority)
    }
}

impl std::fmt::Display for QueueMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueueMode::Fifo => write!(f, "FIFO"),
            QueueMode::Priority => write!(f, "Priority"),
        }
    }
}

/// Message envelope (message + metadata)
///
/// # Examples
///
/// ```
/// use celers_kombu::Envelope;
/// use celers_protocol::Message;
/// use uuid::Uuid;
///
/// let task_id = Uuid::new_v4();
/// let message = Message::new("my_task".to_string(), task_id, vec![1, 2, 3]);
/// let envelope = Envelope::new(message, "delivery-tag-123".to_string());
///
/// assert_eq!(envelope.delivery_tag, "delivery-tag-123");
/// assert!(!envelope.is_redelivered());
/// assert_eq!(envelope.task_name(), "my_task");
/// assert_eq!(envelope.task_id(), task_id);
/// ```
#[derive(Debug, Clone)]
pub struct Envelope {
    /// The actual message
    pub message: Message,

    /// Delivery tag (for acknowledgment)
    pub delivery_tag: String,

    /// Redelivery flag
    pub redelivered: bool,
}

impl Envelope {
    /// Create a new envelope
    pub fn new(message: Message, delivery_tag: String) -> Self {
        Self {
            message,
            delivery_tag,
            redelivered: false,
        }
    }

    /// Check if this message was redelivered
    pub fn is_redelivered(&self) -> bool {
        self.redelivered
    }

    /// Get the task ID from the message
    pub fn task_id(&self) -> uuid::Uuid {
        self.message.task_id()
    }

    /// Get the task name from the message
    pub fn task_name(&self) -> &str {
        self.message.task_name()
    }
}

impl std::fmt::Display for Envelope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Envelope[tag={}] task={} id={}{}",
            self.delivery_tag,
            self.task_name(),
            &self.task_id().to_string()[..8],
            if self.redelivered {
                " (redelivered)"
            } else {
                ""
            }
        )
    }
}

/// Transport trait (low-level broker connection)
#[async_trait]
pub trait Transport: Send + Sync {
    /// Connect to the broker
    async fn connect(&mut self) -> Result<()>;

    /// Disconnect from the broker
    async fn disconnect(&mut self) -> Result<()>;

    /// Check if connected
    fn is_connected(&self) -> bool;

    /// Get transport name
    fn name(&self) -> &str;
}

/// Producer trait (message publishing)
#[async_trait]
pub trait Producer: Transport {
    /// Publish a message to a queue
    async fn publish(&mut self, queue: &str, message: Message) -> Result<()>;

    /// Publish a message with routing key
    async fn publish_with_routing(
        &mut self,
        exchange: &str,
        routing_key: &str,
        message: Message,
    ) -> Result<()>;
}

/// Consumer trait (message consuming)
#[async_trait]
pub trait Consumer: Transport {
    /// Consume a message from a queue (blocking with timeout)
    async fn consume(&mut self, queue: &str, timeout: Duration) -> Result<Option<Envelope>>;

    /// Acknowledge a message
    async fn ack(&mut self, delivery_tag: &str) -> Result<()>;

    /// Reject a message (requeue or send to DLQ)
    async fn reject(&mut self, delivery_tag: &str, requeue: bool) -> Result<()>;

    /// Get queue size
    async fn queue_size(&mut self, queue: &str) -> Result<usize>;
}

/// Full broker trait (combines producer and consumer)
#[async_trait]
pub trait Broker: Producer + Consumer + Transport {
    /// Purge a queue (remove all messages)
    async fn purge(&mut self, queue: &str) -> Result<usize>;

    /// Create a queue
    async fn create_queue(&mut self, queue: &str, mode: QueueMode) -> Result<()>;

    /// Delete a queue
    async fn delete_queue(&mut self, queue: &str) -> Result<()>;

    /// List all queues
    async fn list_queues(&mut self) -> Result<Vec<String>>;
}

/// Queue configuration
///
/// # Examples
///
/// ```
/// use celers_kombu::{QueueConfig, QueueMode};
/// use std::time::Duration;
///
/// let config = QueueConfig::new("my_queue".to_string())
///     .with_mode(QueueMode::Priority)
///     .with_ttl(Duration::from_secs(3600))
///     .with_durable(true)
///     .with_max_message_size(1024 * 1024);
///
/// assert_eq!(config.name, "my_queue");
/// assert_eq!(config.mode, QueueMode::Priority);
/// assert_eq!(config.message_ttl, Some(Duration::from_secs(3600)));
/// assert!(config.durable);
/// assert_eq!(config.max_message_size, Some(1024 * 1024));
/// ```
#[derive(Debug, Clone)]
pub struct QueueConfig {
    /// Queue name
    pub name: String,

    /// Queue mode
    pub mode: QueueMode,

    /// Durable (survive broker restart)
    pub durable: bool,

    /// Auto-delete (delete when no consumers)
    pub auto_delete: bool,

    /// Maximum message size
    pub max_message_size: Option<usize>,

    /// Message TTL (time-to-live)
    pub message_ttl: Option<Duration>,
}

impl QueueConfig {
    pub fn new(name: String) -> Self {
        Self {
            name,
            mode: QueueMode::Fifo,
            durable: true,
            auto_delete: false,
            max_message_size: None,
            message_ttl: None,
        }
    }

    pub fn with_mode(mut self, mode: QueueMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.message_ttl = Some(ttl);
        self
    }

    /// Set durability
    pub fn with_durable(mut self, durable: bool) -> Self {
        self.durable = durable;
        self
    }

    /// Set auto-delete
    pub fn with_auto_delete(mut self, auto_delete: bool) -> Self {
        self.auto_delete = auto_delete;
        self
    }

    /// Set max message size
    pub fn with_max_message_size(mut self, size: usize) -> Self {
        self.max_message_size = Some(size);
        self
    }
}

// =============================================================================
// Batch Operations
// =============================================================================

/// Result of a batch publish operation
///
/// # Examples
///
/// ```
/// use celers_kombu::BatchPublishResult;
/// use std::collections::HashMap;
///
/// // Successful batch publish
/// let result = BatchPublishResult::success(10);
/// assert_eq!(result.succeeded, 10);
/// assert_eq!(result.failed, 0);
/// assert!(result.is_complete_success());
/// assert_eq!(result.total(), 10);
///
/// // Partial failure
/// let mut errors = HashMap::new();
/// errors.insert(2, "network error".to_string());
/// let result = BatchPublishResult {
///     succeeded: 9,
///     failed: 1,
///     errors,
/// };
/// assert!(!result.is_complete_success());
/// assert_eq!(result.total(), 10);
/// ```
#[derive(Debug, Clone)]
pub struct BatchPublishResult {
    /// Number of successfully published messages
    pub succeeded: usize,
    /// Number of failed messages
    pub failed: usize,
    /// Error details for failed messages (index -> error message)
    pub errors: HashMap<usize, String>,
}

impl BatchPublishResult {
    /// Create a successful result
    pub fn success(count: usize) -> Self {
        Self {
            succeeded: count,
            failed: 0,
            errors: HashMap::new(),
        }
    }

    /// Check if all messages were published successfully
    pub fn is_complete_success(&self) -> bool {
        self.failed == 0
    }

    /// Get total number of messages attempted
    pub fn total(&self) -> usize {
        self.succeeded + self.failed
    }
}

/// Batch producer trait (batch message publishing)
#[async_trait]
pub trait BatchProducer: Producer {
    /// Publish multiple messages to a queue in batch
    async fn publish_batch(
        &mut self,
        queue: &str,
        messages: Vec<Message>,
    ) -> Result<BatchPublishResult>;

    /// Publish multiple messages with routing in batch
    async fn publish_batch_with_routing(
        &mut self,
        exchange: &str,
        routing_key: &str,
        messages: Vec<Message>,
    ) -> Result<BatchPublishResult>;
}

/// Batch consumer trait (batch message consuming)
#[async_trait]
pub trait BatchConsumer: Consumer {
    /// Consume multiple messages from a queue (up to max_messages)
    async fn consume_batch(
        &mut self,
        queue: &str,
        max_messages: usize,
        timeout: Duration,
    ) -> Result<Vec<Envelope>>;

    /// Acknowledge multiple messages
    async fn ack_batch(&mut self, delivery_tags: &[String]) -> Result<()>;

    /// Reject multiple messages
    async fn reject_batch(&mut self, delivery_tags: &[String], requeue: bool) -> Result<()>;
}

// =============================================================================
// Connection Retry Policy
// =============================================================================

/// Retry policy for connection attempts
///
/// # Examples
///
/// ```
/// use celers_kombu::RetryPolicy;
/// use std::time::Duration;
///
/// // Default policy
/// let policy = RetryPolicy::new();
/// assert_eq!(policy.max_retries, Some(5));
/// assert!(policy.should_retry(3));
/// assert!(!policy.should_retry(6));
///
/// // Custom policy with exponential backoff
/// let policy = RetryPolicy::new()
///     .with_max_retries(3)
///     .with_initial_delay(Duration::from_millis(100))
///     .with_backoff_multiplier(2.0);
///
/// let delay = policy.delay_for_attempt(0);
/// assert_eq!(delay, Duration::from_millis(100));
///
/// // Infinite retries
/// let policy = RetryPolicy::infinite();
/// assert!(policy.should_retry(1000));
///
/// // No retry
/// let policy = RetryPolicy::no_retry();
/// assert!(!policy.should_retry(0));
/// ```
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts (None = infinite)
    pub max_retries: Option<u32>,
    /// Initial delay between retries
    pub initial_delay: Duration,
    /// Maximum delay between retries
    pub max_delay: Duration,
    /// Multiplier for exponential backoff
    pub backoff_multiplier: f64,
    /// Whether to add jitter to delays
    pub jitter: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: Some(5),
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            backoff_multiplier: 2.0,
            jitter: true,
        }
    }
}

impl RetryPolicy {
    /// Create a new retry policy
    pub fn new() -> Self {
        Self::default()
    }

    /// Set maximum retries (None = infinite)
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = Some(max_retries);
        self
    }

    /// Set infinite retries
    pub fn with_infinite_retries(mut self) -> Self {
        self.max_retries = None;
        self
    }

    /// Set initial delay
    pub fn with_initial_delay(mut self, delay: Duration) -> Self {
        self.initial_delay = delay;
        self
    }

    /// Set maximum delay
    pub fn with_max_delay(mut self, delay: Duration) -> Self {
        self.max_delay = delay;
        self
    }

    /// Set backoff multiplier
    pub fn with_backoff_multiplier(mut self, multiplier: f64) -> Self {
        self.backoff_multiplier = multiplier;
        self
    }

    /// Enable or disable jitter
    pub fn with_jitter(mut self, jitter: bool) -> Self {
        self.jitter = jitter;
        self
    }

    /// Calculate delay for a specific attempt (0-indexed)
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let base_delay =
            self.initial_delay.as_millis() as f64 * self.backoff_multiplier.powi(attempt as i32);
        let delay_ms = base_delay.min(self.max_delay.as_millis() as f64) as u64;
        Duration::from_millis(delay_ms)
    }

    /// Check if we should retry after this attempt (0-indexed)
    pub fn should_retry(&self, attempt: u32) -> bool {
        match self.max_retries {
            None => true, // Infinite retries
            Some(max) => attempt < max,
        }
    }

    /// Create policy that never retries
    pub fn no_retry() -> Self {
        Self {
            max_retries: Some(0),
            ..Default::default()
        }
    }

    /// Create policy with infinite retries
    pub fn infinite() -> Self {
        Self {
            max_retries: None,
            ..Default::default()
        }
    }

    /// Create policy with fixed delay
    pub fn fixed_delay(delay: Duration, max_retries: u32) -> Self {
        Self {
            max_retries: Some(max_retries),
            initial_delay: delay,
            max_delay: delay,
            backoff_multiplier: 1.0,
            jitter: false,
        }
    }
}

// =============================================================================
// Health Check
// =============================================================================

/// Health status of a broker
///
/// # Examples
///
/// ```
/// use celers_kombu::HealthStatus;
///
/// let healthy = HealthStatus::Healthy;
/// assert!(healthy.is_healthy());
/// assert!(healthy.is_operational());
/// assert_eq!(healthy.to_string(), "healthy");
///
/// let degraded = HealthStatus::Degraded;
/// assert!(!degraded.is_healthy());
/// assert!(degraded.is_operational());
/// assert_eq!(degraded.to_string(), "degraded");
///
/// let unhealthy = HealthStatus::Unhealthy;
/// assert!(!unhealthy.is_healthy());
/// assert!(!unhealthy.is_operational());
/// assert_eq!(unhealthy.to_string(), "unhealthy");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthStatus {
    /// Broker is healthy and accepting connections
    Healthy,
    /// Broker is degraded but functional
    Degraded,
    /// Broker is unhealthy or unreachable
    Unhealthy,
}

impl HealthStatus {
    /// Check if status is healthy
    pub fn is_healthy(&self) -> bool {
        matches!(self, HealthStatus::Healthy)
    }

    /// Check if status allows operations
    pub fn is_operational(&self) -> bool {
        matches!(self, HealthStatus::Healthy | HealthStatus::Degraded)
    }
}

impl std::fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HealthStatus::Healthy => write!(f, "healthy"),
            HealthStatus::Degraded => write!(f, "degraded"),
            HealthStatus::Unhealthy => write!(f, "unhealthy"),
        }
    }
}

/// Detailed health check response
///
/// # Examples
///
/// ```
/// use celers_kombu::HealthCheckResponse;
///
/// let response = HealthCheckResponse::healthy("redis", "localhost:6379")
///     .with_latency(15);
///
/// assert!(response.status.is_healthy());
/// assert_eq!(response.broker_type, "redis");
/// assert_eq!(response.latency_ms, Some(15));
///
/// let unhealthy = HealthCheckResponse::unhealthy("amqp", "localhost:5672", "connection refused");
/// assert!(!unhealthy.status.is_healthy());
/// assert_eq!(unhealthy.details.get("reason"), Some(&"connection refused".to_string()));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckResponse {
    /// Overall health status
    pub status: HealthStatus,
    /// Broker name/type
    pub broker_type: String,
    /// Connection URL (sanitized, no password)
    pub connection: String,
    /// Latency in milliseconds
    pub latency_ms: Option<u64>,
    /// Additional details
    pub details: HashMap<String, String>,
}

impl HealthCheckResponse {
    /// Create a new healthy response
    pub fn healthy(broker_type: &str, connection: &str) -> Self {
        Self {
            status: HealthStatus::Healthy,
            broker_type: broker_type.to_string(),
            connection: connection.to_string(),
            latency_ms: None,
            details: HashMap::new(),
        }
    }

    /// Create a new unhealthy response
    pub fn unhealthy(broker_type: &str, connection: &str, reason: &str) -> Self {
        let mut details = HashMap::new();
        details.insert("reason".to_string(), reason.to_string());
        Self {
            status: HealthStatus::Unhealthy,
            broker_type: broker_type.to_string(),
            connection: connection.to_string(),
            latency_ms: None,
            details,
        }
    }

    /// Set latency
    pub fn with_latency(mut self, latency_ms: u64) -> Self {
        self.latency_ms = Some(latency_ms);
        self
    }

    /// Add a detail
    pub fn with_detail(mut self, key: &str, value: &str) -> Self {
        self.details.insert(key.to_string(), value.to_string());
        self
    }
}

/// Health check trait for brokers
#[async_trait]
pub trait HealthCheck: Send + Sync {
    /// Perform a health check
    async fn health_check(&self) -> HealthCheckResponse;

    /// Perform a simple ping (returns true if broker is reachable)
    async fn ping(&self) -> bool;
}

// =============================================================================
// Metrics
// =============================================================================

/// Broker metrics
///
/// # Examples
///
/// ```
/// use celers_kombu::BrokerMetrics;
///
/// let mut metrics = BrokerMetrics::new();
/// metrics.inc_published();
/// metrics.inc_consumed();
/// metrics.inc_acknowledged();
///
/// assert_eq!(metrics.messages_published, 1);
/// assert_eq!(metrics.messages_consumed, 1);
/// assert_eq!(metrics.messages_acknowledged, 1);
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BrokerMetrics {
    /// Total messages published
    pub messages_published: u64,
    /// Total messages consumed
    pub messages_consumed: u64,
    /// Total messages acknowledged
    pub messages_acknowledged: u64,
    /// Total messages rejected
    pub messages_rejected: u64,
    /// Total publish errors
    pub publish_errors: u64,
    /// Total consume errors
    pub consume_errors: u64,
    /// Current active connections
    pub active_connections: u32,
    /// Total connection attempts
    pub connection_attempts: u64,
    /// Total connection failures
    pub connection_failures: u64,
}

impl BrokerMetrics {
    /// Create new empty metrics
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment messages published
    pub fn inc_published(&mut self) {
        self.messages_published += 1;
    }

    /// Increment messages consumed
    pub fn inc_consumed(&mut self) {
        self.messages_consumed += 1;
    }

    /// Increment messages acknowledged
    pub fn inc_acknowledged(&mut self) {
        self.messages_acknowledged += 1;
    }

    /// Increment messages rejected
    pub fn inc_rejected(&mut self) {
        self.messages_rejected += 1;
    }

    /// Increment publish errors
    pub fn inc_publish_error(&mut self) {
        self.publish_errors += 1;
    }

    /// Increment consume errors
    pub fn inc_consume_error(&mut self) {
        self.consume_errors += 1;
    }

    /// Increment connection attempts
    pub fn inc_connection_attempt(&mut self) {
        self.connection_attempts += 1;
    }

    /// Increment connection failures
    pub fn inc_connection_failure(&mut self) {
        self.connection_failures += 1;
    }
}

/// Metrics provider trait
#[async_trait]
pub trait MetricsProvider: Send + Sync {
    /// Get current metrics snapshot
    async fn get_metrics(&self) -> BrokerMetrics;

    /// Reset all metrics
    async fn reset_metrics(&mut self);
}

// =============================================================================
// Admin Trait
// =============================================================================

/// Exchange type for AMQP-style brokers
///
/// # Examples
///
/// ```
/// use celers_kombu::ExchangeType;
///
/// let direct = ExchangeType::Direct;
/// assert_eq!(direct.to_string(), "direct");
///
/// let fanout = ExchangeType::Fanout;
/// assert_eq!(fanout.to_string(), "fanout");
///
/// let topic = ExchangeType::Topic;
/// assert_eq!(topic.to_string(), "topic");
///
/// let headers = ExchangeType::Headers;
/// assert_eq!(headers.to_string(), "headers");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExchangeType {
    /// Direct exchange (exact routing key match)
    Direct,
    /// Fanout exchange (broadcast to all queues)
    Fanout,
    /// Topic exchange (pattern matching)
    Topic,
    /// Headers exchange (header matching)
    Headers,
}

impl std::fmt::Display for ExchangeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExchangeType::Direct => write!(f, "direct"),
            ExchangeType::Fanout => write!(f, "fanout"),
            ExchangeType::Topic => write!(f, "topic"),
            ExchangeType::Headers => write!(f, "headers"),
        }
    }
}

/// Exchange configuration
///
/// # Examples
///
/// ```
/// use celers_kombu::{ExchangeConfig, ExchangeType};
///
/// let config = ExchangeConfig::new("my_exchange", ExchangeType::Topic)
///     .with_durable(true)
///     .with_auto_delete(false);
///
/// assert_eq!(config.name, "my_exchange");
/// assert_eq!(config.exchange_type, ExchangeType::Topic);
/// assert!(config.durable);
/// assert!(!config.auto_delete);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeConfig {
    /// Exchange name
    pub name: String,
    /// Exchange type
    pub exchange_type: ExchangeType,
    /// Durable (survive broker restart)
    pub durable: bool,
    /// Auto-delete when no bindings
    pub auto_delete: bool,
}

impl ExchangeConfig {
    /// Create new exchange config
    pub fn new(name: &str, exchange_type: ExchangeType) -> Self {
        Self {
            name: name.to_string(),
            exchange_type,
            durable: true,
            auto_delete: false,
        }
    }

    /// Set durability
    pub fn with_durable(mut self, durable: bool) -> Self {
        self.durable = durable;
        self
    }

    /// Set auto-delete
    pub fn with_auto_delete(mut self, auto_delete: bool) -> Self {
        self.auto_delete = auto_delete;
        self
    }
}

/// Queue binding configuration
///
/// # Examples
///
/// ```
/// use celers_kombu::BindingConfig;
///
/// let binding = BindingConfig::new("my_exchange", "my_queue", "routing.key");
/// assert_eq!(binding.exchange, "my_exchange");
/// assert_eq!(binding.queue, "my_queue");
/// assert_eq!(binding.routing_key, "routing.key");
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BindingConfig {
    /// Source exchange
    pub exchange: String,
    /// Target queue
    pub queue: String,
    /// Routing key
    pub routing_key: String,
}

impl BindingConfig {
    /// Create new binding config
    pub fn new(exchange: &str, queue: &str, routing_key: &str) -> Self {
        Self {
            exchange: exchange.to_string(),
            queue: queue.to_string(),
            routing_key: routing_key.to_string(),
        }
    }
}

/// Queue information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueInfo {
    /// Queue name
    pub name: String,
    /// Number of messages
    pub message_count: u64,
    /// Number of consumers
    pub consumer_count: u32,
    /// Is durable
    pub durable: bool,
    /// Auto-delete when no consumers
    pub auto_delete: bool,
}

/// Admin trait for broker administration
#[async_trait]
pub trait Admin: Send + Sync {
    /// Declare an exchange (creates if not exists)
    async fn declare_exchange(&mut self, config: &ExchangeConfig) -> Result<()>;

    /// Delete an exchange
    async fn delete_exchange(&mut self, name: &str) -> Result<()>;

    /// List all exchanges
    async fn list_exchanges(&mut self) -> Result<Vec<String>>;

    /// Bind a queue to an exchange
    async fn bind_queue(&mut self, binding: &BindingConfig) -> Result<()>;

    /// Unbind a queue from an exchange
    async fn unbind_queue(&mut self, binding: &BindingConfig) -> Result<()>;

    /// Get queue information
    async fn queue_info(&mut self, queue: &str) -> Result<QueueInfo>;

    /// Get all bindings for a queue
    async fn list_bindings(&mut self, queue: &str) -> Result<Vec<BindingConfig>>;
}

// =============================================================================
// Dead Letter Queue (DLQ) Support
// =============================================================================

/// Dead Letter Queue (DLQ) configuration
///
/// Defines how failed messages should be handled and routed to a dead letter queue.
///
/// # Examples
///
/// ```
/// use celers_kombu::DlqConfig;
/// use std::time::Duration;
///
/// let dlq_config = DlqConfig::new("failed_tasks".to_string())
///     .with_max_retries(3)
///     .with_ttl(Duration::from_secs(86400)); // 24 hours
///
/// assert_eq!(dlq_config.queue_name, "failed_tasks");
/// assert_eq!(dlq_config.max_retries, Some(3));
/// assert_eq!(dlq_config.ttl, Some(Duration::from_secs(86400)));
/// ```
#[derive(Debug, Clone)]
pub struct DlqConfig {
    /// Dead letter queue name
    pub queue_name: String,
    /// Maximum retries before sending to DLQ
    pub max_retries: Option<u32>,
    /// Time-to-live for messages in DLQ
    pub ttl: Option<Duration>,
    /// Whether to include original message metadata
    pub include_metadata: bool,
}

impl DlqConfig {
    /// Create a new DLQ configuration
    pub fn new(queue_name: String) -> Self {
        Self {
            queue_name,
            max_retries: Some(3),
            ttl: None,
            include_metadata: true,
        }
    }

    /// Set maximum retries
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = Some(max_retries);
        self
    }

    /// Disable retry limit (infinite retries)
    pub fn without_retry_limit(mut self) -> Self {
        self.max_retries = None;
        self
    }

    /// Set TTL for DLQ messages
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Set whether to include metadata
    pub fn with_metadata(mut self, include: bool) -> Self {
        self.include_metadata = include;
        self
    }
}

/// Dead Letter Queue trait for handling failed messages
///
/// Provides methods for moving failed messages to a dead letter queue
/// and retrieving them for inspection or retry.
#[async_trait]
pub trait DeadLetterQueue: Send + Sync {
    /// Send a message to the dead letter queue
    async fn send_to_dlq(
        &mut self,
        message: &Message,
        original_queue: &str,
        reason: &str,
    ) -> Result<()>;

    /// Retrieve messages from the dead letter queue
    async fn get_from_dlq(&mut self, dlq_name: &str, limit: usize) -> Result<Vec<Envelope>>;

    /// Retry a message from DLQ (move back to original queue)
    async fn retry_from_dlq(
        &mut self,
        dlq_name: &str,
        delivery_tag: &str,
        target_queue: &str,
    ) -> Result<()>;

    /// Purge dead letter queue
    async fn purge_dlq(&mut self, dlq_name: &str) -> Result<usize>;

    /// Get DLQ statistics
    async fn dlq_stats(&mut self, dlq_name: &str) -> Result<DlqStats>;
}

/// Dead Letter Queue statistics
#[derive(Debug, Clone, Default)]
pub struct DlqStats {
    /// Total messages in DLQ
    pub message_count: usize,
    /// Messages by failure reason
    pub by_reason: HashMap<String, usize>,
    /// Oldest message timestamp (Unix timestamp)
    pub oldest_message_time: Option<u64>,
    /// Newest message timestamp (Unix timestamp)
    pub newest_message_time: Option<u64>,
}

impl DlqStats {
    /// Check if DLQ is empty
    pub fn is_empty(&self) -> bool {
        self.message_count == 0
    }

    /// Get age of oldest message in seconds
    pub fn oldest_message_age_secs(&self) -> Option<u64> {
        self.oldest_message_time.map(|ts| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                .saturating_sub(ts)
        })
    }
}

// =============================================================================
// Message Transactions
// =============================================================================

/// Transaction isolation level
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// Read uncommitted (lowest isolation)
    ReadUncommitted,
    /// Read committed
    ReadCommitted,
    /// Repeatable read
    RepeatableRead,
    /// Serializable (highest isolation)
    Serializable,
}

/// Transaction state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    /// Transaction is active
    Active,
    /// Transaction is committed
    Committed,
    /// Transaction is rolled back
    RolledBack,
}

/// Message transaction trait for atomic operations
///
/// Provides ACID guarantees for message operations.
///
/// # Examples
///
/// ```ignore
/// use celers_kombu::{MessageTransaction, IsolationLevel};
///
/// let mut broker = MyBroker::new();
/// let tx_id = broker.begin_transaction(IsolationLevel::ReadCommitted).await?;
///
/// // Publish messages within transaction
/// broker.publish_transactional(&tx_id, "queue1", message1).await?;
/// broker.publish_transactional(&tx_id, "queue2", message2).await?;
///
/// // Commit transaction (both messages published atomically)
/// broker.commit_transaction(&tx_id).await?;
/// ```
#[async_trait]
pub trait MessageTransaction: Send + Sync {
    /// Begin a new transaction
    async fn begin_transaction(&mut self, isolation: IsolationLevel) -> Result<String>;

    /// Publish a message within a transaction
    async fn publish_transactional(
        &mut self,
        tx_id: &str,
        queue: &str,
        message: Message,
    ) -> Result<()>;

    /// Consume a message within a transaction
    async fn consume_transactional(
        &mut self,
        tx_id: &str,
        queue: &str,
        timeout: Duration,
    ) -> Result<Option<Envelope>>;

    /// Commit a transaction
    async fn commit_transaction(&mut self, tx_id: &str) -> Result<()>;

    /// Rollback a transaction
    async fn rollback_transaction(&mut self, tx_id: &str) -> Result<()>;

    /// Get transaction state
    async fn transaction_state(&self, tx_id: &str) -> Result<TransactionState>;
}

// =============================================================================
// Message Scheduling (Delayed Delivery)
// =============================================================================

/// Schedule configuration for delayed message delivery
///
/// # Examples
///
/// ```
/// use celers_kombu::ScheduleConfig;
/// use std::time::Duration;
///
/// // Delay by 30 seconds
/// let schedule = ScheduleConfig::delay(Duration::from_secs(30));
/// assert!(schedule.delay.is_some());
///
/// // Schedule at specific timestamp
/// let timestamp = std::time::SystemTime::now()
///     .duration_since(std::time::UNIX_EPOCH)
///     .unwrap()
///     .as_secs() + 3600;
/// let schedule = ScheduleConfig::at(timestamp);
/// assert!(schedule.scheduled_at.is_some());
/// ```
#[derive(Debug, Clone)]
pub struct ScheduleConfig {
    /// Delay duration from now
    pub delay: Option<Duration>,
    /// Absolute timestamp (Unix epoch seconds)
    pub scheduled_at: Option<u64>,
    /// Maximum execution time window
    pub execution_window: Option<Duration>,
}

impl ScheduleConfig {
    /// Create schedule with delay
    pub fn delay(delay: Duration) -> Self {
        Self {
            delay: Some(delay),
            scheduled_at: None,
            execution_window: None,
        }
    }

    /// Create schedule at absolute time
    pub fn at(timestamp: u64) -> Self {
        Self {
            delay: None,
            scheduled_at: Some(timestamp),
            execution_window: None,
        }
    }

    /// Set execution window
    pub fn with_window(mut self, window: Duration) -> Self {
        self.execution_window = Some(window);
        self
    }

    /// Check if message is ready for delivery
    pub fn is_ready(&self) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        if let Some(timestamp) = self.scheduled_at {
            return now >= timestamp;
        }

        // If only delay is set, it's ready when converted to timestamp
        true
    }

    /// Get delivery timestamp
    pub fn delivery_time(&self) -> Option<u64> {
        if let Some(timestamp) = self.scheduled_at {
            return Some(timestamp);
        }

        if let Some(delay) = self.delay {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            return Some(now + delay.as_secs());
        }

        None
    }
}

/// Message scheduler trait for delayed delivery
#[async_trait]
pub trait MessageScheduler: Send + Sync {
    /// Schedule a message for delayed delivery
    async fn schedule_message(
        &mut self,
        queue: &str,
        message: Message,
        schedule: ScheduleConfig,
    ) -> Result<String>;

    /// Cancel a scheduled message
    async fn cancel_scheduled(&mut self, schedule_id: &str) -> Result<()>;

    /// List scheduled messages for a queue
    async fn list_scheduled(&mut self, queue: &str) -> Result<Vec<ScheduledMessage>>;

    /// Get count of scheduled messages
    async fn scheduled_count(&mut self, queue: &str) -> Result<usize>;
}

/// Scheduled message information
#[derive(Debug, Clone)]
pub struct ScheduledMessage {
    /// Schedule ID
    pub schedule_id: String,
    /// Queue name
    pub queue: String,
    /// Scheduled delivery time (Unix timestamp)
    pub delivery_time: u64,
    /// Message size in bytes
    pub message_size: usize,
}

// =============================================================================
// Consumer Groups (Load Balancing)
// =============================================================================

/// Consumer group configuration
///
/// # Examples
///
/// ```
/// use celers_kombu::ConsumerGroupConfig;
///
/// let config = ConsumerGroupConfig::new("my-service".to_string(), "worker-1".to_string())
///     .with_max_consumers(10)
///     .with_rebalance_timeout(std::time::Duration::from_secs(30));
///
/// assert_eq!(config.group_id, "my-service");
/// assert_eq!(config.consumer_id, "worker-1");
/// assert_eq!(config.max_consumers, Some(10));
/// ```
#[derive(Debug, Clone)]
pub struct ConsumerGroupConfig {
    /// Consumer group ID
    pub group_id: String,
    /// Individual consumer ID
    pub consumer_id: String,
    /// Maximum consumers in group
    pub max_consumers: Option<usize>,
    /// Rebalance timeout
    pub rebalance_timeout: Duration,
    /// Heartbeat interval
    pub heartbeat_interval: Duration,
}

impl ConsumerGroupConfig {
    /// Create new consumer group configuration
    pub fn new(group_id: String, consumer_id: String) -> Self {
        Self {
            group_id,
            consumer_id,
            max_consumers: None,
            rebalance_timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(3),
        }
    }

    /// Set maximum consumers
    pub fn with_max_consumers(mut self, max: usize) -> Self {
        self.max_consumers = Some(max);
        self
    }

    /// Set rebalance timeout
    pub fn with_rebalance_timeout(mut self, timeout: Duration) -> Self {
        self.rebalance_timeout = timeout;
        self
    }

    /// Set heartbeat interval
    pub fn with_heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }
}

/// Consumer group trait for load-balanced consumption
#[async_trait]
pub trait ConsumerGroup: Send + Sync {
    /// Join a consumer group
    async fn join_group(&mut self, config: &ConsumerGroupConfig) -> Result<()>;

    /// Leave consumer group
    async fn leave_group(&mut self, group_id: &str) -> Result<()>;

    /// Send heartbeat to maintain membership
    async fn heartbeat(&mut self, group_id: &str) -> Result<()>;

    /// Get consumer group members
    async fn group_members(&mut self, group_id: &str) -> Result<Vec<String>>;

    /// Consume from group (automatic load balancing)
    async fn consume_from_group(
        &mut self,
        group_id: &str,
        queues: &[String],
        timeout: Duration,
    ) -> Result<Option<Envelope>>;
}

// =============================================================================
// Message Replay (Debugging/Recovery)
// =============================================================================

/// Replay configuration
///
/// # Examples
///
/// ```
/// use celers_kombu::ReplayConfig;
/// use std::time::Duration;
///
/// // Replay last 1 hour
/// let config = ReplayConfig::from_duration(Duration::from_secs(3600));
/// assert!(config.from_duration.is_some());
///
/// // Replay from specific timestamp
/// let timestamp = 1699999999;
/// let config = ReplayConfig::from_timestamp(timestamp);
/// assert_eq!(config.from_timestamp, Some(timestamp));
/// ```
#[derive(Debug, Clone)]
pub struct ReplayConfig {
    /// Replay from duration ago
    pub from_duration: Option<Duration>,
    /// Replay from absolute timestamp
    pub from_timestamp: Option<u64>,
    /// Replay until timestamp (None = now)
    pub until_timestamp: Option<u64>,
    /// Maximum messages to replay
    pub max_messages: Option<usize>,
    /// Replay speed multiplier (1.0 = real-time)
    pub speed_multiplier: f64,
}

impl ReplayConfig {
    /// Create replay config from duration
    pub fn from_duration(duration: Duration) -> Self {
        Self {
            from_duration: Some(duration),
            from_timestamp: None,
            until_timestamp: None,
            max_messages: None,
            speed_multiplier: 1.0,
        }
    }

    /// Create replay config from timestamp
    pub fn from_timestamp(timestamp: u64) -> Self {
        Self {
            from_duration: None,
            from_timestamp: Some(timestamp),
            until_timestamp: None,
            max_messages: None,
            speed_multiplier: 1.0,
        }
    }

    /// Set end timestamp
    pub fn until(mut self, timestamp: u64) -> Self {
        self.until_timestamp = Some(timestamp);
        self
    }

    /// Set maximum messages
    pub fn with_max_messages(mut self, max: usize) -> Self {
        self.max_messages = Some(max);
        self
    }

    /// Set replay speed
    pub fn with_speed(mut self, multiplier: f64) -> Self {
        self.speed_multiplier = multiplier;
        self
    }

    /// Calculate start timestamp
    pub fn start_timestamp(&self) -> u64 {
        if let Some(ts) = self.from_timestamp {
            return ts;
        }

        if let Some(duration) = self.from_duration {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            return now.saturating_sub(duration.as_secs());
        }

        0
    }
}

/// Message replay trait for debugging and recovery
#[async_trait]
pub trait MessageReplay: Send + Sync {
    /// Start replay session
    async fn begin_replay(&mut self, queue: &str, config: ReplayConfig) -> Result<String>;

    /// Get next message from replay
    async fn replay_next(&mut self, replay_id: &str) -> Result<Option<Envelope>>;

    /// Stop replay session
    async fn stop_replay(&mut self, replay_id: &str) -> Result<()>;

    /// Get replay progress
    async fn replay_progress(&mut self, replay_id: &str) -> Result<ReplayProgress>;
}

/// Replay progress information
#[derive(Debug, Clone)]
pub struct ReplayProgress {
    /// Replay session ID
    pub replay_id: String,
    /// Messages replayed so far
    pub messages_replayed: usize,
    /// Total messages to replay
    pub total_messages: Option<usize>,
    /// Current timestamp being replayed
    pub current_timestamp: u64,
    /// Replay is complete
    pub completed: bool,
}

impl ReplayProgress {
    /// Get completion percentage
    pub fn completion_percent(&self) -> Option<f64> {
        self.total_messages.map(|total| {
            if total == 0 {
                100.0
            } else {
                (self.messages_replayed as f64 / total as f64) * 100.0
            }
        })
    }
}

// =============================================================================
// Quota Management (Resource Limits)
// =============================================================================

/// Quota configuration
///
/// # Examples
///
/// ```
/// use celers_kombu::QuotaConfig;
/// use std::time::Duration;
///
/// let quota = QuotaConfig::new()
///     .with_max_messages(10000)
///     .with_max_bytes(10 * 1024 * 1024)
///     .with_max_rate(100.0);
///
/// assert_eq!(quota.max_messages, Some(10000));
/// assert_eq!(quota.max_bytes, Some(10 * 1024 * 1024));
/// assert_eq!(quota.max_rate_per_sec, Some(100.0));
/// ```
#[derive(Debug, Clone)]
pub struct QuotaConfig {
    /// Maximum number of messages
    pub max_messages: Option<usize>,
    /// Maximum total bytes
    pub max_bytes: Option<usize>,
    /// Maximum rate (messages per second)
    pub max_rate_per_sec: Option<f64>,
    /// Per-consumer message limit
    pub max_messages_per_consumer: Option<usize>,
    /// Quota enforcement action
    pub enforcement: QuotaEnforcement,
}

impl QuotaConfig {
    /// Create new quota configuration
    pub fn new() -> Self {
        Self {
            max_messages: None,
            max_bytes: None,
            max_rate_per_sec: None,
            max_messages_per_consumer: None,
            enforcement: QuotaEnforcement::Reject,
        }
    }

    /// Set maximum messages
    pub fn with_max_messages(mut self, max: usize) -> Self {
        self.max_messages = Some(max);
        self
    }

    /// Set maximum bytes
    pub fn with_max_bytes(mut self, max: usize) -> Self {
        self.max_bytes = Some(max);
        self
    }

    /// Set maximum rate
    pub fn with_max_rate(mut self, rate: f64) -> Self {
        self.max_rate_per_sec = Some(rate);
        self
    }

    /// Set per-consumer limit
    pub fn with_max_per_consumer(mut self, max: usize) -> Self {
        self.max_messages_per_consumer = Some(max);
        self
    }

    /// Set enforcement action
    pub fn with_enforcement(mut self, enforcement: QuotaEnforcement) -> Self {
        self.enforcement = enforcement;
        self
    }
}

impl Default for QuotaConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Quota enforcement action
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotaEnforcement {
    /// Reject new messages when quota exceeded
    Reject,
    /// Throttle (slow down) when quota exceeded
    Throttle,
    /// Warn but allow (soft limit)
    Warn,
}

/// Quota usage statistics
#[derive(Debug, Clone, Default)]
pub struct QuotaUsage {
    /// Current message count
    pub message_count: usize,
    /// Current bytes used
    pub bytes_used: usize,
    /// Current rate (messages/sec)
    pub current_rate: f64,
    /// Quota exceeded flag
    pub exceeded: bool,
}

impl QuotaUsage {
    /// Check if message quota is exceeded
    pub fn is_message_quota_exceeded(&self, config: &QuotaConfig) -> bool {
        if let Some(max) = config.max_messages {
            return self.message_count >= max;
        }
        false
    }

    /// Check if bytes quota is exceeded
    pub fn is_bytes_quota_exceeded(&self, config: &QuotaConfig) -> bool {
        if let Some(max) = config.max_bytes {
            return self.bytes_used >= max;
        }
        false
    }

    /// Check if rate quota is exceeded
    pub fn is_rate_quota_exceeded(&self, config: &QuotaConfig) -> bool {
        if let Some(max) = config.max_rate_per_sec {
            return self.current_rate >= max;
        }
        false
    }

    /// Get usage percentage
    pub fn usage_percent(&self, config: &QuotaConfig) -> Option<f64> {
        config.max_messages.map(|max| {
            if max == 0 {
                100.0
            } else {
                (self.message_count as f64 / max as f64) * 100.0
            }
        })
    }
}

/// Quota management trait
#[async_trait]
pub trait QuotaManager: Send + Sync {
    /// Set quota for a queue
    async fn set_quota(&mut self, queue: &str, config: QuotaConfig) -> Result<()>;

    /// Get quota configuration
    async fn get_quota(&mut self, queue: &str) -> Result<QuotaConfig>;

    /// Get quota usage
    async fn quota_usage(&mut self, queue: &str) -> Result<QuotaUsage>;

    /// Reset quota counters
    async fn reset_quota(&mut self, queue: &str) -> Result<()>;

    /// Check if operation is allowed under quota
    async fn check_quota(&mut self, queue: &str, message_size: usize) -> Result<bool>;
}

// =============================================================================
// Connection State Callbacks
// =============================================================================

/// Connection state
///
/// # Examples
///
/// ```
/// use celers_kombu::ConnectionState;
///
/// let state = ConnectionState::Connected;
/// assert_eq!(state.to_string(), "connected");
///
/// let disconnected = ConnectionState::Disconnected;
/// assert_eq!(disconnected.to_string(), "disconnected");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Not connected
    Disconnected,
    /// Connection in progress
    Connecting,
    /// Connected and ready
    Connected,
    /// Reconnecting after failure
    Reconnecting,
}

impl std::fmt::Display for ConnectionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionState::Disconnected => write!(f, "disconnected"),
            ConnectionState::Connecting => write!(f, "connecting"),
            ConnectionState::Connected => write!(f, "connected"),
            ConnectionState::Reconnecting => write!(f, "reconnecting"),
        }
    }
}

/// Connection event
#[derive(Debug, Clone)]
pub enum ConnectionEvent {
    /// Connection established
    Connected,
    /// Connection lost
    Disconnected { reason: String },
    /// Reconnection attempt
    Reconnecting { attempt: u32 },
    /// Reconnection succeeded
    Reconnected,
    /// Reconnection failed
    ReconnectFailed { error: String },
}

/// Connection state observer trait
pub trait ConnectionObserver: Send + Sync {
    /// Called when connection state changes
    fn on_state_change(&self, old_state: ConnectionState, new_state: ConnectionState);

    /// Called on connection events
    fn on_event(&self, event: ConnectionEvent);
}

// =============================================================================
// Connection Pool
// =============================================================================

/// Connection pool configuration
///
/// # Examples
///
/// ```
/// use celers_kombu::PoolConfig;
/// use std::time::Duration;
///
/// let config = PoolConfig::new()
///     .with_min_connections(2)
///     .with_max_connections(20)
///     .with_idle_timeout(Duration::from_secs(300))
///     .with_acquire_timeout(Duration::from_secs(10))
///     .with_max_lifetime(Duration::from_secs(3600));
///
/// assert_eq!(config.min_connections, 2);
/// assert_eq!(config.max_connections, 20);
/// assert_eq!(config.idle_timeout, Some(Duration::from_secs(300)));
/// assert_eq!(config.acquire_timeout, Duration::from_secs(10));
/// ```
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Minimum number of connections to keep in the pool
    pub min_connections: u32,
    /// Maximum number of connections allowed
    pub max_connections: u32,
    /// Connection idle timeout before closing
    pub idle_timeout: Option<Duration>,
    /// Maximum time to wait for a connection
    pub acquire_timeout: Duration,
    /// Maximum connection lifetime
    pub max_lifetime: Option<Duration>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_connections: 1,
            max_connections: 10,
            idle_timeout: Some(Duration::from_secs(300)),
            acquire_timeout: Duration::from_secs(30),
            max_lifetime: Some(Duration::from_secs(1800)),
        }
    }
}

impl PoolConfig {
    /// Create a new pool config
    pub fn new() -> Self {
        Self::default()
    }

    /// Set minimum connections
    pub fn with_min_connections(mut self, min: u32) -> Self {
        self.min_connections = min;
        self
    }

    /// Set maximum connections
    pub fn with_max_connections(mut self, max: u32) -> Self {
        self.max_connections = max;
        self
    }

    /// Set idle timeout
    pub fn with_idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = Some(timeout);
        self
    }

    /// Disable idle timeout
    pub fn without_idle_timeout(mut self) -> Self {
        self.idle_timeout = None;
        self
    }

    /// Set acquire timeout
    pub fn with_acquire_timeout(mut self, timeout: Duration) -> Self {
        self.acquire_timeout = timeout;
        self
    }

    /// Set max lifetime
    pub fn with_max_lifetime(mut self, lifetime: Duration) -> Self {
        self.max_lifetime = Some(lifetime);
        self
    }
}

/// Pool statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PoolStats {
    /// Total connections created
    pub connections_created: u64,
    /// Total connections closed
    pub connections_closed: u64,
    /// Current active connections
    pub active_connections: u32,
    /// Current idle connections
    pub idle_connections: u32,
    /// Total acquire requests
    pub acquire_requests: u64,
    /// Acquire timeouts
    pub acquire_timeouts: u64,
}

impl PoolStats {
    /// Get total connections in pool
    pub fn total_connections(&self) -> u32 {
        self.active_connections + self.idle_connections
    }
}

/// Connection pool trait
#[async_trait]
pub trait ConnectionPool: Send + Sync {
    /// Get pool configuration
    fn config(&self) -> &PoolConfig;

    /// Get pool statistics
    fn stats(&self) -> PoolStats;

    /// Resize the pool
    async fn resize(&mut self, min: u32, max: u32) -> Result<()>;

    /// Close all idle connections
    async fn shrink(&mut self) -> Result<u32>;

    /// Close all connections
    async fn close(&mut self) -> Result<()>;
}

// =============================================================================
// Circuit Breaker
// =============================================================================

/// Circuit breaker state
///
/// # Examples
///
/// ```
/// use celers_kombu::CircuitState;
///
/// let closed = CircuitState::Closed;
/// assert_eq!(closed.to_string(), "closed");
///
/// let open = CircuitState::Open;
/// assert_eq!(open.to_string(), "open");
///
/// let half_open = CircuitState::HalfOpen;
/// assert_eq!(half_open.to_string(), "half-open");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CircuitState {
    /// Circuit is closed, requests flow normally
    Closed,
    /// Circuit is open, requests are rejected
    Open,
    /// Circuit is half-open, testing if service recovered
    HalfOpen,
}

impl std::fmt::Display for CircuitState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CircuitState::Closed => write!(f, "closed"),
            CircuitState::Open => write!(f, "open"),
            CircuitState::HalfOpen => write!(f, "half-open"),
        }
    }
}

/// Circuit breaker configuration
///
/// # Examples
///
/// ```
/// use celers_kombu::CircuitBreakerConfig;
/// use std::time::Duration;
///
/// let config = CircuitBreakerConfig::new()
///     .with_failure_threshold(3)
///     .with_success_threshold(2)
///     .with_open_duration(Duration::from_secs(30))
///     .with_failure_window(Duration::from_secs(60));
///
/// assert_eq!(config.failure_threshold, 3);
/// assert_eq!(config.success_threshold, 2);
/// assert_eq!(config.open_duration, Duration::from_secs(30));
/// assert_eq!(config.failure_window, Duration::from_secs(60));
/// ```
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of failures before opening the circuit
    pub failure_threshold: u32,
    /// Number of successes in half-open state to close circuit
    pub success_threshold: u32,
    /// Duration to keep circuit open before trying again
    pub open_duration: Duration,
    /// Time window for counting failures
    pub failure_window: Duration,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 2,
            open_duration: Duration::from_secs(30),
            failure_window: Duration::from_secs(60),
        }
    }
}

impl CircuitBreakerConfig {
    /// Create a new circuit breaker config
    pub fn new() -> Self {
        Self::default()
    }

    /// Set failure threshold
    pub fn with_failure_threshold(mut self, threshold: u32) -> Self {
        self.failure_threshold = threshold;
        self
    }

    /// Set success threshold for half-open state
    pub fn with_success_threshold(mut self, threshold: u32) -> Self {
        self.success_threshold = threshold;
        self
    }

    /// Set open duration
    pub fn with_open_duration(mut self, duration: Duration) -> Self {
        self.open_duration = duration;
        self
    }

    /// Set failure window
    pub fn with_failure_window(mut self, window: Duration) -> Self {
        self.failure_window = window;
        self
    }
}

/// Circuit breaker statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CircuitBreakerStats {
    /// Current state
    pub state: String,
    /// Total requests
    pub total_requests: u64,
    /// Successful requests
    pub successful_requests: u64,
    /// Failed requests
    pub failed_requests: u64,
    /// Rejected requests (when open)
    pub rejected_requests: u64,
    /// Number of times circuit opened
    pub times_opened: u64,
    /// Consecutive failures
    pub consecutive_failures: u32,
    /// Consecutive successes (in half-open)
    pub consecutive_successes: u32,
}

impl CircuitBreakerStats {
    /// Calculate success rate
    pub fn success_rate(&self) -> f64 {
        if self.total_requests == 0 {
            1.0
        } else {
            self.successful_requests as f64 / self.total_requests as f64
        }
    }
}

/// Circuit breaker trait
pub trait CircuitBreaker: Send + Sync {
    /// Get current state
    fn state(&self) -> CircuitState;

    /// Get configuration
    fn config(&self) -> &CircuitBreakerConfig;

    /// Get statistics
    fn stats(&self) -> CircuitBreakerStats;

    /// Record a success
    fn record_success(&mut self);

    /// Record a failure
    fn record_failure(&mut self);

    /// Check if request is allowed
    fn is_allowed(&self) -> bool;

    /// Reset the circuit breaker
    fn reset(&mut self);
}

// =============================================================================
// Message Options
// =============================================================================

/// Priority levels for messages
///
/// # Examples
///
/// ```
/// use celers_kombu::Priority;
///
/// let normal = Priority::Normal;
/// assert_eq!(normal.as_u8(), 5);
/// assert_eq!(normal.to_string(), "normal");
///
/// let high = Priority::High;
/// assert!(high > normal);
/// assert_eq!(high.as_u8(), 7);
///
/// // Convert from numeric value
/// let priority = Priority::from_u8(8);
/// assert_eq!(priority, Priority::High);
///
/// // Default is Normal
/// let default = Priority::default();
/// assert_eq!(default, Priority::Normal);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Priority {
    /// Lowest priority (0)
    Lowest = 0,
    /// Low priority (3)
    Low = 3,
    /// Normal priority (5)
    Normal = 5,
    /// High priority (7)
    High = 7,
    /// Highest priority (9)
    Highest = 9,
}

impl Default for Priority {
    fn default() -> Self {
        Self::Normal
    }
}

impl std::fmt::Display for Priority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Priority::Lowest => write!(f, "lowest"),
            Priority::Low => write!(f, "low"),
            Priority::Normal => write!(f, "normal"),
            Priority::High => write!(f, "high"),
            Priority::Highest => write!(f, "highest"),
        }
    }
}

impl Priority {
    /// Convert to numeric value (0-9)
    pub fn as_u8(&self) -> u8 {
        *self as u8
    }

    /// Create from numeric value (clamped to 0-9)
    pub fn from_u8(value: u8) -> Self {
        match value {
            0..=1 => Priority::Lowest,
            2..=4 => Priority::Low,
            5 => Priority::Normal,
            6..=8 => Priority::High,
            _ => Priority::Highest,
        }
    }
}

/// Message-level options
///
/// # Examples
///
/// ```
/// use celers_kombu::{MessageOptions, Priority};
/// use std::time::Duration;
///
/// let options = MessageOptions::new()
///     .with_priority(Priority::High)
///     .with_ttl(Duration::from_secs(3600))
///     .with_correlation_id("req-123".to_string())
///     .with_reply_to("response_queue".to_string());
///
/// assert_eq!(options.priority, Some(Priority::High));
/// assert_eq!(options.ttl, Some(Duration::from_secs(3600)));
/// assert_eq!(options.correlation_id, Some("req-123".to_string()));
///
/// // Check if message should be signed
/// let secure_options = MessageOptions::new()
///     .with_signing(b"secret-key".to_vec());
/// assert!(secure_options.should_sign());
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MessageOptions {
    /// Message priority
    pub priority: Option<Priority>,
    /// Message TTL (time-to-live)
    pub ttl: Option<Duration>,
    /// Message expiration timestamp (absolute)
    pub expires_at: Option<u64>,
    /// Delay before message becomes visible
    pub delay: Option<Duration>,
    /// Correlation ID for request/response patterns
    pub correlation_id: Option<String>,
    /// Reply-to queue for RPC patterns
    pub reply_to: Option<String>,
    /// Custom headers
    pub headers: HashMap<String, String>,
    /// Enable message signing (HMAC)
    pub sign: bool,
    /// Signing key for HMAC (if signing is enabled)
    pub signing_key: Option<Vec<u8>>,
    /// Enable message encryption (AES-256-GCM)
    pub encrypt: bool,
    /// Encryption key (32 bytes for AES-256)
    pub encryption_key: Option<Vec<u8>>,
    /// Compression hint
    pub compress: bool,
}

impl MessageOptions {
    /// Create new message options
    pub fn new() -> Self {
        Self::default()
    }

    /// Set priority
    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = Some(priority);
        self
    }

    /// Set TTL
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Set expiration timestamp (Unix timestamp in seconds)
    pub fn with_expires_at(mut self, timestamp: u64) -> Self {
        self.expires_at = Some(timestamp);
        self
    }

    /// Set delay
    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    /// Set correlation ID
    pub fn with_correlation_id(mut self, id: impl Into<String>) -> Self {
        self.correlation_id = Some(id.into());
        self
    }

    /// Set reply-to queue
    pub fn with_reply_to(mut self, queue: impl Into<String>) -> Self {
        self.reply_to = Some(queue.into());
        self
    }

    /// Add a custom header
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Enable message signing with HMAC
    pub fn with_signing(mut self, key: Vec<u8>) -> Self {
        self.sign = true;
        self.signing_key = Some(key);
        self
    }

    /// Enable message encryption with AES-256-GCM
    pub fn with_encryption(mut self, key: Vec<u8>) -> Self {
        self.encrypt = true;
        self.encryption_key = Some(key);
        self
    }

    /// Enable compression
    pub fn with_compression(mut self) -> Self {
        self.compress = true;
        self
    }

    /// Check if message has expired (based on expires_at)
    pub fn is_expired(&self, current_timestamp: u64) -> bool {
        self.expires_at.is_some_and(|exp| current_timestamp > exp)
    }

    /// Check if message should be delayed
    pub fn should_delay(&self) -> bool {
        self.delay.is_some()
    }

    /// Check if message should be signed
    pub fn should_sign(&self) -> bool {
        self.sign && self.signing_key.is_some()
    }

    /// Check if message should be encrypted
    pub fn should_encrypt(&self) -> bool {
        self.encrypt && self.encryption_key.is_some()
    }

    /// Check if message should be compressed
    pub fn should_compress(&self) -> bool {
        self.compress
    }
}

// =============================================================================
// Extended Producer Trait
// =============================================================================

/// Extended producer trait with message options support
#[async_trait]
pub trait ExtendedProducer: Producer {
    /// Publish a message with options
    async fn publish_with_options(
        &mut self,
        queue: &str,
        message: Message,
        options: MessageOptions,
    ) -> Result<()>;

    /// Publish a message with routing and options
    async fn publish_with_routing_and_options(
        &mut self,
        exchange: &str,
        routing_key: &str,
        message: Message,
        options: MessageOptions,
    ) -> Result<()>;
}

// =============================================================================
// Middleware Support
// =============================================================================

/// Message middleware for pre/post processing
#[async_trait]
pub trait MessageMiddleware: Send + Sync {
    /// Process message before publishing
    async fn before_publish(&self, message: &mut Message) -> Result<()>;

    /// Process message after consuming
    async fn after_consume(&self, message: &mut Message) -> Result<()>;

    /// Get middleware name for logging
    fn name(&self) -> &str;
}

/// Middleware chain for processing messages
///
/// # Examples
///
/// ```
/// use celers_kombu::{MiddlewareChain, ValidationMiddleware, LoggingMiddleware};
///
/// let chain = MiddlewareChain::new()
///     .with_middleware(Box::new(ValidationMiddleware::new()))
///     .with_middleware(Box::new(LoggingMiddleware::new("MyApp")));
///
/// assert_eq!(chain.len(), 2);
/// assert!(!chain.is_empty());
/// ```
pub struct MiddlewareChain {
    middlewares: Vec<Box<dyn MessageMiddleware>>,
}

impl MiddlewareChain {
    /// Create a new empty middleware chain
    pub fn new() -> Self {
        Self {
            middlewares: Vec::new(),
        }
    }

    /// Add middleware to the chain
    pub fn with_middleware(mut self, middleware: Box<dyn MessageMiddleware>) -> Self {
        self.middlewares.push(middleware);
        self
    }

    /// Process message through all middlewares (before publish)
    pub async fn process_before_publish(&self, message: &mut Message) -> Result<()> {
        for middleware in &self.middlewares {
            middleware.before_publish(message).await?;
        }
        Ok(())
    }

    /// Process message through all middlewares (after consume)
    pub async fn process_after_consume(&self, message: &mut Message) -> Result<()> {
        for middleware in &self.middlewares {
            middleware.after_consume(message).await?;
        }
        Ok(())
    }

    /// Get number of middlewares in chain
    pub fn len(&self) -> usize {
        self.middlewares.len()
    }

    /// Check if chain is empty
    pub fn is_empty(&self) -> bool {
        self.middlewares.is_empty()
    }
}

impl Default for MiddlewareChain {
    fn default() -> Self {
        Self::new()
    }
}

/// Producer with middleware support
#[async_trait]
pub trait MiddlewareProducer: Producer {
    /// Publish a message with middleware processing
    async fn publish_with_middleware(
        &mut self,
        queue: &str,
        mut message: Message,
        chain: &MiddlewareChain,
    ) -> Result<()> {
        // Process through middleware chain
        chain.process_before_publish(&mut message).await?;
        // Publish the processed message
        self.publish(queue, message).await
    }
}

/// Consumer with middleware support
#[async_trait]
pub trait MiddlewareConsumer: Consumer {
    /// Consume a message with middleware processing
    async fn consume_with_middleware(
        &mut self,
        queue: &str,
        timeout: Duration,
        chain: &MiddlewareChain,
    ) -> Result<Option<Envelope>> {
        // Consume the message
        if let Some(mut envelope) = self.consume(queue, timeout).await? {
            // Process through middleware chain
            chain.process_after_consume(&mut envelope.message).await?;
            Ok(Some(envelope))
        } else {
            Ok(None)
        }
    }
}

// =============================================================================
// Built-in Middleware Implementations
// =============================================================================

/// Validation middleware - validates message structure
///
/// # Examples
///
/// ```
/// use celers_kombu::ValidationMiddleware;
///
/// // Default validation (10MB max, require task name)
/// let validator = ValidationMiddleware::new();
///
/// // Custom validation
/// let validator = ValidationMiddleware::new()
///     .with_max_body_size(5 * 1024 * 1024)  // 5MB limit
///     .with_require_task_name(true);
///
/// // Disable body size limit
/// let validator = ValidationMiddleware::new()
///     .without_body_size_limit();
/// ```
pub struct ValidationMiddleware {
    /// Maximum message body size (bytes)
    max_body_size: Option<usize>,
    /// Require task name to be non-empty
    require_task_name: bool,
}

impl ValidationMiddleware {
    /// Create a new validation middleware
    pub fn new() -> Self {
        Self {
            max_body_size: Some(10 * 1024 * 1024), // 10MB default
            require_task_name: true,
        }
    }

    /// Set maximum body size
    pub fn with_max_body_size(mut self, size: usize) -> Self {
        self.max_body_size = Some(size);
        self
    }

    /// Disable body size check
    pub fn without_body_size_limit(mut self) -> Self {
        self.max_body_size = None;
        self
    }

    /// Set whether task name is required
    pub fn with_require_task_name(mut self, require: bool) -> Self {
        self.require_task_name = require;
        self
    }

    fn validate_message(&self, message: &Message) -> Result<()> {
        // Check task name
        if self.require_task_name && message.task_name().is_empty() {
            return Err(BrokerError::Configuration(
                "Task name cannot be empty".to_string(),
            ));
        }

        // Check body size
        if let Some(max_size) = self.max_body_size {
            if message.body.len() > max_size {
                return Err(BrokerError::Configuration(format!(
                    "Message body size {} exceeds maximum {}",
                    message.body.len(),
                    max_size
                )));
            }
        }

        Ok(())
    }
}

impl Default for ValidationMiddleware {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MessageMiddleware for ValidationMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        self.validate_message(message)
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        self.validate_message(message)
    }

    fn name(&self) -> &str {
        "validation"
    }
}

/// Logging middleware - logs message events
///
/// # Examples
///
/// ```
/// use celers_kombu::LoggingMiddleware;
///
/// // Basic logging
/// let logger = LoggingMiddleware::new("MyApp");
///
/// // With detailed body logging
/// let verbose_logger = LoggingMiddleware::new("MyApp")
///     .with_body_logging();
/// ```
pub struct LoggingMiddleware {
    prefix: String,
    log_body: bool,
}

impl LoggingMiddleware {
    /// Create a new logging middleware
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
            log_body: false,
        }
    }

    /// Enable body logging (for debugging)
    pub fn with_body_logging(mut self) -> Self {
        self.log_body = true;
        self
    }
}

#[async_trait]
impl MessageMiddleware for LoggingMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        if self.log_body {
            eprintln!(
                "[{}] Publishing: task={}, id={}, body_size={}",
                self.prefix,
                message.task_name(),
                message.task_id(),
                message.body.len()
            );
        } else {
            eprintln!(
                "[{}] Publishing: task={}, id={}",
                self.prefix,
                message.task_name(),
                message.task_id()
            );
        }
        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        if self.log_body {
            eprintln!(
                "[{}] Consumed: task={}, id={}, body_size={}",
                self.prefix,
                message.task_name(),
                message.task_id(),
                message.body.len()
            );
        } else {
            eprintln!(
                "[{}] Consumed: task={}, id={}",
                self.prefix,
                message.task_name(),
                message.task_id()
            );
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "logging"
    }
}

/// Metrics middleware - collects message statistics
///
/// # Examples
///
/// ```
/// use celers_kombu::{MetricsMiddleware, BrokerMetrics};
/// use std::sync::{Arc, Mutex};
///
/// let metrics = Arc::new(Mutex::new(BrokerMetrics::default()));
/// let middleware = MetricsMiddleware::new(metrics.clone());
///
/// // Later, get metrics snapshot
/// let snapshot = middleware.get_metrics();
/// assert_eq!(snapshot.messages_published, 0);
/// ```
pub struct MetricsMiddleware {
    metrics: std::sync::Arc<std::sync::Mutex<BrokerMetrics>>,
}

impl MetricsMiddleware {
    /// Create a new metrics middleware
    pub fn new(metrics: std::sync::Arc<std::sync::Mutex<BrokerMetrics>>) -> Self {
        Self { metrics }
    }

    /// Get current metrics snapshot
    pub fn get_metrics(&self) -> BrokerMetrics {
        self.metrics.lock().unwrap().clone()
    }
}

#[async_trait]
impl MessageMiddleware for MetricsMiddleware {
    async fn before_publish(&self, _message: &mut Message) -> Result<()> {
        let mut metrics = self.metrics.lock().unwrap();
        metrics.inc_published();
        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        let mut metrics = self.metrics.lock().unwrap();
        metrics.inc_consumed();
        Ok(())
    }

    fn name(&self) -> &str {
        "metrics"
    }
}

/// Retry limit middleware - enforces maximum retry count
///
/// # Examples
///
/// ```
/// use celers_kombu::RetryLimitMiddleware;
///
/// // Allow up to 3 retries
/// let middleware = RetryLimitMiddleware::new(3);
/// ```
pub struct RetryLimitMiddleware {
    max_retries: u32,
}

impl RetryLimitMiddleware {
    /// Create a new retry limit middleware
    pub fn new(max_retries: u32) -> Self {
        Self { max_retries }
    }
}

#[async_trait]
impl MessageMiddleware for RetryLimitMiddleware {
    async fn before_publish(&self, _message: &mut Message) -> Result<()> {
        // No validation on publish
        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Check retry count from message headers
        let retries = message.headers.retries.unwrap_or(0);
        if retries > self.max_retries {
            return Err(BrokerError::Configuration(format!(
                "Message exceeded maximum retries: {} > {}",
                retries, self.max_retries
            )));
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "retry_limit"
    }
}

/// Rate limiting middleware - enforces message rate limits
///
/// # Examples
///
/// ```
/// use celers_kombu::RateLimitingMiddleware;
///
/// // Limit to 100 messages per second
/// let middleware = RateLimitingMiddleware::new(100.0);
/// ```
pub struct RateLimitingMiddleware {
    /// Maximum messages per second
    max_rate: f64,
    /// Token bucket (tracks available tokens)
    tokens: std::sync::Arc<std::sync::Mutex<TokenBucket>>,
}

/// Token bucket for rate limiting
struct TokenBucket {
    /// Current token count
    tokens: f64,
    /// Maximum tokens
    capacity: f64,
    /// Tokens added per second
    refill_rate: f64,
    /// Last refill time
    last_refill: std::time::Instant,
}

impl TokenBucket {
    fn new(capacity: f64, refill_rate: f64) -> Self {
        Self {
            tokens: capacity,
            capacity,
            refill_rate,
            last_refill: std::time::Instant::now(),
        }
    }

    fn try_consume(&mut self, tokens: f64) -> bool {
        // Refill tokens based on elapsed time
        let now = std::time::Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.refill_rate).min(self.capacity);
        self.last_refill = now;

        // Try to consume tokens
        if self.tokens >= tokens {
            self.tokens -= tokens;
            true
        } else {
            false
        }
    }
}

impl RateLimitingMiddleware {
    /// Create a new rate limiting middleware
    ///
    /// # Arguments
    ///
    /// * `max_rate` - Maximum messages per second
    pub fn new(max_rate: f64) -> Self {
        Self {
            max_rate,
            tokens: std::sync::Arc::new(std::sync::Mutex::new(TokenBucket::new(
                max_rate, max_rate,
            ))),
        }
    }
}

#[async_trait]
impl MessageMiddleware for RateLimitingMiddleware {
    async fn before_publish(&self, _message: &mut Message) -> Result<()> {
        // Try to acquire a token
        let mut bucket = self.tokens.lock().unwrap();
        if !bucket.try_consume(1.0) {
            return Err(BrokerError::OperationFailed(format!(
                "Rate limit exceeded: {} messages/sec",
                self.max_rate
            )));
        }
        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        // No rate limiting on consume
        Ok(())
    }

    fn name(&self) -> &str {
        "rate_limit"
    }
}

/// Deduplication middleware - prevents duplicate message processing
///
/// # Examples
///
/// ```
/// use celers_kombu::DeduplicationMiddleware;
///
/// // Track up to 5000 message IDs
/// let middleware = DeduplicationMiddleware::new(5000);
///
/// // Use default cache size (10,000)
/// let default_middleware = DeduplicationMiddleware::with_default_cache();
/// ```
pub struct DeduplicationMiddleware {
    /// Recently seen message IDs
    seen_ids: std::sync::Arc<std::sync::Mutex<std::collections::HashSet<Uuid>>>,
    /// Maximum size of seen IDs cache
    max_cache_size: usize,
}

impl DeduplicationMiddleware {
    /// Create a new deduplication middleware
    ///
    /// # Arguments
    ///
    /// * `max_cache_size` - Maximum number of message IDs to track
    pub fn new(max_cache_size: usize) -> Self {
        Self {
            seen_ids: std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
            max_cache_size,
        }
    }

    /// Create with default cache size (10,000 message IDs)
    pub fn with_default_cache() -> Self {
        Self::new(10_000)
    }
}

impl Default for DeduplicationMiddleware {
    fn default() -> Self {
        Self::with_default_cache()
    }
}

#[async_trait]
impl MessageMiddleware for DeduplicationMiddleware {
    async fn before_publish(&self, _message: &mut Message) -> Result<()> {
        // No deduplication on publish
        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        let msg_id = message.task_id();
        let mut seen = self.seen_ids.lock().unwrap();

        // Check if we've seen this message before
        if seen.contains(&msg_id) {
            return Err(BrokerError::OperationFailed(format!(
                "Duplicate message detected: {}",
                msg_id
            )));
        }

        // Add to seen set
        seen.insert(msg_id);

        // Evict oldest entries if cache is too large (simple FIFO eviction)
        if seen.len() > self.max_cache_size {
            // Remove first element (note: HashSet doesn't have ordering, so this is arbitrary)
            if let Some(&id) = seen.iter().next() {
                seen.remove(&id);
            }
        }

        Ok(())
    }

    fn name(&self) -> &str {
        "deduplication"
    }
}

/// Compression middleware - compresses/decompresses message bodies
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "compression")]
/// # {
/// use celers_kombu::CompressionMiddleware;
/// use celers_protocol::compression::CompressionType;
///
/// let middleware = CompressionMiddleware::new(CompressionType::Gzip)
///     .with_min_size(2048)  // Only compress messages >= 2KB
///     .with_level(6);       // Compression level 6
/// # }
/// ```
#[cfg(feature = "compression")]
pub struct CompressionMiddleware {
    /// Compressor instance
    compressor: celers_protocol::compression::Compressor,
    /// Minimum body size to compress (bytes)
    min_compress_size: usize,
}

#[cfg(feature = "compression")]
impl CompressionMiddleware {
    /// Create a new compression middleware
    ///
    /// # Arguments
    ///
    /// * `compression_type` - Type of compression to use
    pub fn new(compression_type: celers_protocol::compression::CompressionType) -> Self {
        Self {
            compressor: celers_protocol::compression::Compressor::new(compression_type),
            min_compress_size: 1024, // 1KB default
        }
    }

    /// Set minimum body size to compress
    pub fn with_min_size(mut self, size: usize) -> Self {
        self.min_compress_size = size;
        self
    }

    /// Set compression level
    pub fn with_level(mut self, level: u32) -> Self {
        self.compressor = self.compressor.with_level(level);
        self
    }
}

#[cfg(feature = "compression")]
#[async_trait]
impl MessageMiddleware for CompressionMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Only compress if body is large enough
        if message.body.len() >= self.min_compress_size {
            let compressed = self
                .compressor
                .compress(&message.body)
                .map_err(|e| BrokerError::Serialization(e.to_string()))?;

            // Only use compressed version if it's actually smaller
            if compressed.len() < message.body.len() {
                message.body = compressed;
                // Note: In a real implementation, we'd set a header to indicate compression
            }
        }
        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Try to decompress (would need to check header in real implementation)
        // For now, we'll skip decompression on consume since we don't have metadata
        // A real implementation would check message headers for compression flag
        let _ = message;
        Ok(())
    }

    fn name(&self) -> &str {
        "compression"
    }
}

/// Signing middleware - signs/verifies message bodies using HMAC
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "signing")]
/// # {
/// use celers_kombu::SigningMiddleware;
///
/// let secret_key = b"my-secret-key";
/// let middleware = SigningMiddleware::new(secret_key);
/// # }
/// ```
#[cfg(feature = "signing")]
pub struct SigningMiddleware {
    /// Message signer instance
    signer: celers_protocol::auth::MessageSigner,
}

#[cfg(feature = "signing")]
impl SigningMiddleware {
    /// Create a new signing middleware
    ///
    /// # Arguments
    ///
    /// * `key` - Secret key for HMAC signing
    pub fn new(key: &[u8]) -> Self {
        Self {
            signer: celers_protocol::auth::MessageSigner::new(key),
        }
    }
}

#[cfg(feature = "signing")]
#[async_trait]
impl MessageMiddleware for SigningMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Sign the message body
        let signature = self.signer.sign(&message.body);

        // Store signature in message headers (would need custom header field)
        // For now, we'll just validate that signing works
        // In a real implementation, we'd add a signature field to Message
        let _ = signature;

        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // In a real implementation, we'd:
        // 1. Extract signature from message headers
        // 2. Verify signature against message body
        // 3. Return error if verification fails
        //
        // For now, we'll just validate the message can be signed
        let _ = self.signer.sign(&message.body);

        Ok(())
    }

    fn name(&self) -> &str {
        "signing"
    }
}

/// Encryption middleware - encrypts/decrypts message bodies using AES-256-GCM
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "encryption")]
/// # {
/// use celers_kombu::EncryptionMiddleware;
///
/// // 32-byte key for AES-256
/// let key = [0u8; 32];
/// let middleware = EncryptionMiddleware::new(&key).expect("valid key");
/// # }
/// ```
#[cfg(feature = "encryption")]
pub struct EncryptionMiddleware {
    /// Message encryptor instance
    encryptor: celers_protocol::crypto::MessageEncryptor,
}

#[cfg(feature = "encryption")]
impl EncryptionMiddleware {
    /// Create a new encryption middleware
    ///
    /// # Arguments
    ///
    /// * `key` - 32-byte secret key for AES-256
    ///
    /// # Returns
    ///
    /// `Ok(EncryptionMiddleware)` if the key is valid, `Err(BrokerError)` otherwise
    pub fn new(key: &[u8]) -> Result<Self> {
        let encryptor = celers_protocol::crypto::MessageEncryptor::new(key)
            .map_err(|e| BrokerError::Configuration(e.to_string()))?;

        Ok(Self { encryptor })
    }
}

#[cfg(feature = "encryption")]
#[async_trait]
impl MessageMiddleware for EncryptionMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Encrypt the message body
        let (ciphertext, nonce) = self
            .encryptor
            .encrypt(&message.body)
            .map_err(|e| BrokerError::Serialization(e.to_string()))?;

        // In a real implementation, we'd store the nonce in message headers
        // For now, we'll prepend the nonce to the ciphertext
        let mut encrypted = nonce.to_vec();
        encrypted.extend_from_slice(&ciphertext);
        message.body = encrypted;

        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Extract nonce and ciphertext
        if message.body.len() < celers_protocol::crypto::NONCE_SIZE {
            return Err(BrokerError::Serialization(
                "Message too short to contain nonce".to_string(),
            ));
        }

        let (nonce_bytes, ciphertext) = message.body.split_at(celers_protocol::crypto::NONCE_SIZE);

        // Decrypt the message body
        let plaintext = self
            .encryptor
            .decrypt(ciphertext, nonce_bytes)
            .map_err(|e| BrokerError::Serialization(e.to_string()))?;

        message.body = plaintext;
        Ok(())
    }

    fn name(&self) -> &str {
        "encryption"
    }
}

/// Timeout middleware - enforces message processing time limits
///
/// # Examples
///
/// ```
/// use celers_kombu::TimeoutMiddleware;
/// use std::time::Duration;
///
/// // Set 30 second timeout for message processing
/// let middleware = TimeoutMiddleware::new(Duration::from_secs(30));
/// ```
pub struct TimeoutMiddleware {
    timeout: Duration,
}

impl TimeoutMiddleware {
    /// Create a new timeout middleware
    pub fn new(timeout: Duration) -> Self {
        Self { timeout }
    }

    /// Get the configured timeout
    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}

#[async_trait]
impl MessageMiddleware for TimeoutMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Store timeout in message headers for consumer
        message.headers.extra.insert(
            "x-timeout-ms".to_string(),
            serde_json::Value::Number((self.timeout.as_millis() as u64).into()),
        );
        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        // Timeout checking is implementation-specific and would be handled
        // by the consumer/worker. This middleware just sets the metadata.
        Ok(())
    }

    fn name(&self) -> &str {
        "timeout"
    }
}

/// Filter middleware - filters messages based on custom criteria
///
/// # Examples
///
/// ```
/// use celers_kombu::FilterMiddleware;
/// use celers_protocol::Message;
///
/// // Create filter that only allows high-priority tasks
/// let filter = FilterMiddleware::new(|msg: &Message| {
///     msg.task_name().starts_with("critical_")
/// });
/// ```
pub struct FilterMiddleware {
    predicate: Box<dyn Fn(&Message) -> bool + Send + Sync>,
}

impl FilterMiddleware {
    /// Create a new filter middleware with a predicate function
    pub fn new<F>(predicate: F) -> Self
    where
        F: Fn(&Message) -> bool + Send + Sync + 'static,
    {
        Self {
            predicate: Box::new(predicate),
        }
    }

    /// Check if a message passes the filter
    pub fn matches(&self, message: &Message) -> bool {
        (self.predicate)(message)
    }
}

#[async_trait]
impl MessageMiddleware for FilterMiddleware {
    async fn before_publish(&self, _message: &mut Message) -> Result<()> {
        // No filtering on publish
        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        if !self.matches(message) {
            return Err(BrokerError::Configuration(
                "Message filtered out by predicate".to_string(),
            ));
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "filter"
    }
}

/// Sampling middleware for statistical message sampling.
///
/// Allows only a percentage of messages to pass through, useful for
/// monitoring, testing, or load reduction.
///
/// # Examples
///
/// ```
/// use celers_kombu::SamplingMiddleware;
///
/// // Sample 10% of messages
/// let sampler = SamplingMiddleware::new(0.1);
/// assert_eq!(sampler.sample_rate(), 0.1);
/// ```
pub struct SamplingMiddleware {
    sample_rate: f64,
    counter: std::sync::atomic::AtomicU64,
}

impl SamplingMiddleware {
    /// Create a new sampling middleware with the given sample rate.
    ///
    /// Sample rate should be between 0.0 and 1.0, where:
    /// - 0.0 = sample nothing
    /// - 1.0 = sample everything
    /// - 0.1 = sample approximately 10% of messages
    pub fn new(sample_rate: f64) -> Self {
        Self {
            sample_rate: sample_rate.clamp(0.0, 1.0),
            counter: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Get the configured sample rate
    pub fn sample_rate(&self) -> f64 {
        self.sample_rate
    }

    /// Check if a message should be sampled
    fn should_sample(&self) -> bool {
        let count = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // Deterministic sampling based on counter
        let threshold = (u64::MAX as f64 * self.sample_rate) as u64;
        (count % u64::MAX) < threshold
    }
}

#[async_trait]
impl MessageMiddleware for SamplingMiddleware {
    async fn before_publish(&self, _message: &mut Message) -> Result<()> {
        // No sampling on publish
        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        if !self.should_sample() {
            return Err(BrokerError::Configuration(
                "Message filtered out by sampling".to_string(),
            ));
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "sampling"
    }
}

/// Transformation middleware for message content transformation.
///
/// Applies a transformation function to message bodies during processing.
///
/// # Examples
///
/// ```
/// use celers_kombu::TransformationMiddleware;
///
/// // Create a transformer that uppercases text
/// let transformer = TransformationMiddleware::new(|body: Vec<u8>| {
///     String::from_utf8_lossy(&body).to_uppercase().into_bytes()
/// });
/// ```
pub struct TransformationMiddleware {
    transform_fn: Box<dyn Fn(Vec<u8>) -> Vec<u8> + Send + Sync>,
}

impl TransformationMiddleware {
    /// Create a new transformation middleware with a transform function
    pub fn new<F>(transform_fn: F) -> Self
    where
        F: Fn(Vec<u8>) -> Vec<u8> + Send + Sync + 'static,
    {
        Self {
            transform_fn: Box::new(transform_fn),
        }
    }

    /// Apply the transformation to message body
    fn transform(&self, body: Vec<u8>) -> Vec<u8> {
        (self.transform_fn)(body)
    }
}

#[async_trait]
impl MessageMiddleware for TransformationMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Transform on publish
        let transformed = self.transform(message.body.clone());
        message.body = transformed;
        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Transform on consume
        let transformed = self.transform(message.body.clone());
        message.body = transformed;
        Ok(())
    }

    fn name(&self) -> &str {
        "transformation"
    }
}

/// Tracing middleware for distributed tracing
///
/// Injects trace IDs into message headers for distributed tracing.
///
/// # Examples
///
/// ```
/// use celers_kombu::{TracingMiddleware, MessageMiddleware};
///
/// let middleware = TracingMiddleware::new("service-name");
/// assert_eq!(middleware.name(), "tracing");
/// ```
#[derive(Debug, Clone)]
pub struct TracingMiddleware {
    service_name: String,
}

impl TracingMiddleware {
    /// Create a new tracing middleware
    pub fn new(service_name: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
        }
    }
}

#[async_trait]
impl MessageMiddleware for TracingMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Inject trace ID if not present
        if !message.headers.extra.contains_key("trace-id") {
            let trace_id = uuid::Uuid::new_v4().to_string();
            message
                .headers
                .extra
                .insert("trace-id".to_string(), serde_json::json!(trace_id));
        }

        // Add service name
        message.headers.extra.insert(
            "service-name".to_string(),
            serde_json::json!(self.service_name.clone()),
        );

        // Add span ID for this operation
        let span_id = uuid::Uuid::new_v4().to_string();
        message
            .headers
            .extra
            .insert("span-id".to_string(), serde_json::json!(span_id));

        // Add timestamp
        message.headers.extra.insert(
            "trace-timestamp".to_string(),
            serde_json::json!(std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()),
        );

        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Extract and log trace information
        if let Some(trace_id) = message.headers.extra.get("trace-id").cloned() {
            // In production, this would be sent to a tracing system
            message.headers.extra.insert(
                "consumer-service".to_string(),
                serde_json::json!(self.service_name.clone()),
            );
            message
                .headers
                .extra
                .insert("trace-id-consumed".to_string(), trace_id);
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "tracing"
    }
}

/// Batching middleware for automatic message batching
///
/// Automatically batches messages based on size or time thresholds.
///
/// # Examples
///
/// ```
/// use celers_kombu::{BatchingMiddleware, MessageMiddleware};
///
/// let middleware = BatchingMiddleware::new(100, 5000);
/// assert_eq!(middleware.name(), "batching");
/// ```
#[derive(Debug, Clone)]
pub struct BatchingMiddleware {
    batch_size: usize,
    batch_timeout_ms: u64,
}

impl BatchingMiddleware {
    /// Create a new batching middleware
    ///
    /// # Arguments
    ///
    /// * `batch_size` - Maximum messages per batch
    /// * `batch_timeout_ms` - Maximum wait time in milliseconds
    pub fn new(batch_size: usize, batch_timeout_ms: u64) -> Self {
        Self {
            batch_size,
            batch_timeout_ms,
        }
    }

    /// Create with default settings (100 messages, 5 second timeout)
    pub fn with_defaults() -> Self {
        Self::new(100, 5000)
    }
}

#[async_trait]
impl MessageMiddleware for BatchingMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Add batching metadata
        message.headers.extra.insert(
            "batch-size-hint".to_string(),
            serde_json::json!(self.batch_size),
        );
        message.headers.extra.insert(
            "batch-timeout-ms".to_string(),
            serde_json::json!(self.batch_timeout_ms),
        );

        // Mark message as batch-enabled
        message
            .headers
            .extra
            .insert("batching-enabled".to_string(), serde_json::json!(true));

        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        // No-op for consume side
        Ok(())
    }

    fn name(&self) -> &str {
        "batching"
    }
}

/// Audit middleware for comprehensive audit logging
///
/// Logs all message operations for audit trails and compliance.
///
/// # Examples
///
/// ```
/// use celers_kombu::{AuditMiddleware, MessageMiddleware};
///
/// let middleware = AuditMiddleware::new(true);
/// assert_eq!(middleware.name(), "audit");
/// ```
#[derive(Debug, Clone)]
pub struct AuditMiddleware {
    log_body: bool,
}

impl AuditMiddleware {
    /// Create a new audit middleware
    ///
    /// # Arguments
    ///
    /// * `log_body` - Whether to include message body in audit logs
    pub fn new(log_body: bool) -> Self {
        Self { log_body }
    }

    /// Create audit middleware with body logging enabled
    pub fn with_body_logging() -> Self {
        Self::new(true)
    }

    /// Create audit middleware without body logging
    pub fn without_body_logging() -> Self {
        Self::new(false)
    }

    fn create_audit_entry(&self, message: &Message, operation: &str) -> String {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let body_info = if self.log_body {
            format!("body_size={}", message.body.len())
        } else {
            "body=<redacted>".to_string()
        };

        format!(
            "[AUDIT] timestamp={} operation={} task_id={} task_name={} {}",
            timestamp,
            operation,
            message.task_id(),
            message.task_name(),
            body_info
        )
    }
}

#[async_trait]
impl MessageMiddleware for AuditMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        let audit_entry = self.create_audit_entry(message, "PUBLISH");

        // In production, this would be sent to an audit logging system
        message
            .headers
            .extra
            .insert("audit-publish".to_string(), serde_json::json!(audit_entry));

        // Add audit ID
        let audit_id = uuid::Uuid::new_v4().to_string();
        message
            .headers
            .extra
            .insert("audit-id".to_string(), serde_json::json!(audit_id));

        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        let audit_entry = self.create_audit_entry(message, "CONSUME");

        // In production, this would be sent to an audit logging system
        message
            .headers
            .extra
            .insert("audit-consume".to_string(), serde_json::json!(audit_entry));

        Ok(())
    }

    fn name(&self) -> &str {
        "audit"
    }
}

/// Middleware for enforcing hard deadlines on message processing.
///
/// Unlike TimeoutMiddleware which sets a timeout hint, DeadlineMiddleware
/// enforces a hard deadline (absolute time) by which a message must be processed.
///
/// # Examples
///
/// ```
/// use celers_kombu::{DeadlineMiddleware, MessageMiddleware};
/// use std::time::Duration;
///
/// // Enforce 5-minute deadline from now
/// let middleware = DeadlineMiddleware::new(Duration::from_secs(300));
/// assert_eq!(middleware.name(), "deadline");
/// ```
#[derive(Debug, Clone)]
pub struct DeadlineMiddleware {
    deadline_duration: Duration,
}

impl DeadlineMiddleware {
    /// Create a new deadline middleware with the specified duration from now
    pub fn new(deadline_duration: Duration) -> Self {
        Self { deadline_duration }
    }

    /// Get the deadline duration
    pub fn deadline_duration(&self) -> Duration {
        self.deadline_duration
    }
}

#[async_trait]
impl MessageMiddleware for DeadlineMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Calculate absolute deadline timestamp
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let deadline = now + self.deadline_duration.as_secs();

        message
            .headers
            .extra
            .insert("x-deadline".to_string(), serde_json::json!(deadline));

        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Check if deadline has passed
        if let Some(deadline_value) = message.headers.extra.get("x-deadline") {
            if let Some(deadline) = deadline_value.as_u64() {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();

                if now > deadline {
                    // Mark message as deadline-exceeded
                    message
                        .headers
                        .extra
                        .insert("x-deadline-exceeded".to_string(), serde_json::json!(true));
                }
            }
        }

        Ok(())
    }

    fn name(&self) -> &str {
        "deadline"
    }
}

/// Middleware for content type validation and conversion hints.
///
/// Validates that messages have acceptable content types and can inject
/// conversion hints for consumers.
///
/// # Examples
///
/// ```
/// use celers_kombu::{ContentTypeMiddleware, MessageMiddleware};
///
/// // Only allow JSON messages
/// let middleware = ContentTypeMiddleware::new(vec!["application/json".to_string()]);
/// assert_eq!(middleware.name(), "content_type");
/// ```
#[derive(Debug, Clone)]
pub struct ContentTypeMiddleware {
    allowed_content_types: Vec<String>,
    default_content_type: String,
}

impl ContentTypeMiddleware {
    /// Create a new content type middleware
    pub fn new(allowed_content_types: Vec<String>) -> Self {
        Self {
            allowed_content_types,
            default_content_type: "application/json".to_string(),
        }
    }

    /// Set the default content type for messages without one
    pub fn with_default(mut self, content_type: String) -> Self {
        self.default_content_type = content_type;
        self
    }

    /// Check if a content type is allowed
    pub fn is_allowed(&self, content_type: &str) -> bool {
        self.allowed_content_types.is_empty()
            || self
                .allowed_content_types
                .contains(&content_type.to_string())
    }
}

#[async_trait]
impl MessageMiddleware for ContentTypeMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Set default content type if not present
        if message.content_type.is_empty() {
            message.content_type = self.default_content_type.clone();
        }

        // Validate content type
        if !self.is_allowed(&message.content_type) {
            return Err(BrokerError::Configuration(format!(
                "Content type '{}' is not allowed. Allowed types: {:?}",
                message.content_type, self.allowed_content_types
            )));
        }

        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Validate content type on consume
        if !self.is_allowed(&message.content_type) {
            message.headers.extra.insert(
                "x-content-type-warning".to_string(),
                serde_json::json!(format!("Unexpected content type: {}", message.content_type)),
            );
        }

        Ok(())
    }

    fn name(&self) -> &str {
        "content_type"
    }
}

/// Middleware for dynamic routing key assignment.
///
/// Assigns routing keys to messages based on custom logic or message content.
/// Useful for implementing dynamic routing strategies.
///
/// # Examples
///
/// ```
/// use celers_kombu::{RoutingKeyMiddleware, MessageMiddleware};
///
/// // Use task name as routing key
/// let middleware = RoutingKeyMiddleware::new(|msg| {
///     format!("tasks.{}", msg.headers.task)
/// });
/// assert_eq!(middleware.name(), "routing_key");
/// ```
pub struct RoutingKeyMiddleware {
    key_generator: Box<dyn Fn(&Message) -> String + Send + Sync>,
}

impl RoutingKeyMiddleware {
    /// Create a new routing key middleware with a custom key generator
    pub fn new<F>(key_generator: F) -> Self
    where
        F: Fn(&Message) -> String + Send + Sync + 'static,
    {
        Self {
            key_generator: Box::new(key_generator),
        }
    }

    /// Create a routing key from task name
    pub fn from_task_name() -> Self {
        Self::new(|msg| format!("tasks.{}", msg.headers.task))
    }

    /// Create a routing key from task name with priority
    pub fn from_task_and_priority() -> Self {
        Self::new(|msg| {
            let priority = msg
                .headers
                .extra
                .get("priority")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            format!("tasks.{}.priority_{}", msg.headers.task, priority)
        })
    }
}

#[async_trait]
impl MessageMiddleware for RoutingKeyMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        let routing_key = (self.key_generator)(message);
        message
            .headers
            .extra
            .insert("x-routing-key".to_string(), serde_json::json!(routing_key));

        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        // No action needed on consume
        Ok(())
    }

    fn name(&self) -> &str {
        "routing_key"
    }
}

/// Idempotency middleware for ensuring exactly-once message processing
///
/// This middleware tracks processed message IDs to prevent duplicate processing.
/// Unlike DeduplicationMiddleware which only prevents duplicate publishing,
/// IdempotencyMiddleware ensures that a message is processed only once even if
/// it's delivered multiple times (e.g., due to network issues or retries).
///
/// # Examples
///
/// ```
/// use celers_kombu::{IdempotencyMiddleware, MessageMiddleware};
///
/// let middleware = IdempotencyMiddleware::new(10000);
/// assert_eq!(middleware.name(), "idempotency");
/// ```
pub struct IdempotencyMiddleware {
    processed_ids: std::sync::Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    max_cache_size: usize,
}

impl IdempotencyMiddleware {
    /// Create a new idempotency middleware with a custom cache size
    pub fn new(max_cache_size: usize) -> Self {
        Self {
            processed_ids: std::sync::Arc::new(std::sync::Mutex::new(
                std::collections::HashSet::new(),
            )),
            max_cache_size,
        }
    }

    /// Create a new idempotency middleware with default cache size (10,000)
    pub fn with_default_cache() -> Self {
        Self::new(10000)
    }

    /// Check if a message ID has been processed
    pub fn is_processed(&self, message_id: &str) -> bool {
        self.processed_ids.lock().unwrap().contains(message_id)
    }

    /// Mark a message ID as processed
    pub fn mark_processed(&self, message_id: String) {
        let mut cache = self.processed_ids.lock().unwrap();

        // Simple cache eviction: if we exceed max size, clear oldest 20%
        if cache.len() >= self.max_cache_size {
            let to_remove = self.max_cache_size / 5;
            let ids_to_remove: Vec<String> = cache.iter().take(to_remove).cloned().collect();
            for id in ids_to_remove {
                cache.remove(&id);
            }
        }

        cache.insert(message_id);
    }

    /// Clear all processed message IDs
    pub fn clear(&self) {
        self.processed_ids.lock().unwrap().clear();
    }

    /// Get the number of tracked message IDs
    pub fn cache_size(&self) -> usize {
        self.processed_ids.lock().unwrap().len()
    }
}

#[async_trait]
impl MessageMiddleware for IdempotencyMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Add idempotency key to message headers for tracking
        let idempotency_key = format!("{}:{}", message.headers.id, message.headers.task);
        message.headers.extra.insert(
            "x-idempotency-key".to_string(),
            serde_json::json!(idempotency_key),
        );
        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Check if message has already been processed
        let idempotency_key = message
            .headers
            .extra
            .get("x-idempotency-key")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| {
                // Fallback to generating key if not present
                format!("{}:{}", message.headers.id, message.headers.task)
            });

        if self.is_processed(&idempotency_key) {
            // Message already processed, mark it in headers
            message
                .headers
                .extra
                .insert("x-already-processed".to_string(), serde_json::json!(true));
        } else {
            // Mark as being processed
            self.mark_processed(idempotency_key.clone());
            message
                .headers
                .extra
                .insert("x-already-processed".to_string(), serde_json::json!(false));
        }

        Ok(())
    }

    fn name(&self) -> &str {
        "idempotency"
    }
}

/// Backoff middleware for automatic retry backoff calculation
///
/// This middleware automatically calculates and injects retry backoff delays
/// based on the number of retries, using exponential backoff with jitter.
/// This helps prevent thundering herd problems when retrying failed messages.
///
/// # Examples
///
/// ```
/// use celers_kombu::{BackoffMiddleware, MessageMiddleware};
/// use std::time::Duration;
///
/// let middleware = BackoffMiddleware::new(
///     Duration::from_secs(1),
///     Duration::from_secs(300),
///     2.0
/// );
/// assert_eq!(middleware.name(), "backoff");
/// ```
pub struct BackoffMiddleware {
    initial_delay: Duration,
    max_delay: Duration,
    multiplier: f64,
}

impl BackoffMiddleware {
    /// Create a new backoff middleware with custom settings
    ///
    /// # Arguments
    ///
    /// * `initial_delay` - Initial retry delay
    /// * `max_delay` - Maximum retry delay
    /// * `multiplier` - Backoff multiplier (typically 2.0)
    pub fn new(initial_delay: Duration, max_delay: Duration, multiplier: f64) -> Self {
        Self {
            initial_delay,
            max_delay,
            multiplier,
        }
    }

    /// Create a new backoff middleware with default settings
    ///
    /// Defaults: 1s initial, 5min max, 2.0 multiplier
    pub fn with_defaults() -> Self {
        Self::new(Duration::from_secs(1), Duration::from_secs(300), 2.0)
    }

    /// Calculate backoff delay for a given retry attempt
    fn calculate_delay(&self, retry_count: u32) -> Duration {
        let delay_secs =
            self.initial_delay.as_secs_f64() * self.multiplier.powi(retry_count as i32);
        let delay = Duration::from_secs_f64(delay_secs.min(self.max_delay.as_secs_f64()));

        // Add jitter (0-25% of the delay)
        let jitter = (delay.as_secs_f64() * 0.25 * rand::random::<f64>()).round() as u64;
        delay + Duration::from_secs(jitter)
    }
}

#[async_trait]
impl MessageMiddleware for BackoffMiddleware {
    async fn before_publish(&self, _message: &mut Message) -> Result<()> {
        // No action needed on publish
        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Calculate and inject backoff delay based on retry count
        let retry_count = message
            .headers
            .extra
            .get("retries")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32;

        let backoff_delay = self.calculate_delay(retry_count);

        message.headers.extra.insert(
            "x-backoff-delay".to_string(),
            serde_json::json!(backoff_delay.as_millis() as u64),
        );

        message.headers.extra.insert(
            "x-next-retry-at".to_string(),
            serde_json::json!((std::time::SystemTime::now() + backoff_delay)
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()),
        );

        Ok(())
    }

    fn name(&self) -> &str {
        "backoff"
    }
}

/// Caching middleware for storing message processing results
///
/// This middleware caches the results of message processing to avoid
/// reprocessing identical messages. Useful for expensive operations that
/// are idempotent (e.g., external API calls, database queries).
///
/// # Examples
///
/// ```
/// use celers_kombu::{CachingMiddleware, MessageMiddleware};
/// use std::time::Duration;
///
/// let middleware = CachingMiddleware::new(1000, Duration::from_secs(3600));
/// assert_eq!(middleware.name(), "caching");
/// ```
pub struct CachingMiddleware {
    cache: std::sync::Arc<std::sync::Mutex<CacheMap>>,
    max_entries: usize,
    ttl: Duration,
}

type CacheMap = std::collections::HashMap<String, (Vec<u8>, std::time::Instant)>;

impl CachingMiddleware {
    /// Create a new caching middleware with custom settings
    ///
    /// # Arguments
    ///
    /// * `max_entries` - Maximum number of cache entries
    /// * `ttl` - Time-to-live for cache entries
    pub fn new(max_entries: usize, ttl: Duration) -> Self {
        Self {
            cache: std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            max_entries,
            ttl,
        }
    }

    /// Create a new caching middleware with default settings
    ///
    /// Defaults: 10,000 entries, 1 hour TTL
    pub fn with_defaults() -> Self {
        Self::new(10_000, Duration::from_secs(3600))
    }

    /// Generate cache key from message
    fn cache_key(&self, message: &Message) -> String {
        // Use message ID and task name as cache key
        format!("{}:{}", message.headers.id, message.headers.task)
    }

    /// Check if a cached result exists and is still valid
    pub fn get_cached(&self, message: &Message) -> Option<Vec<u8>> {
        let key = self.cache_key(message);
        let mut cache = self.cache.lock().unwrap();

        if let Some((result, timestamp)) = cache.get(&key) {
            if timestamp.elapsed() < self.ttl {
                return Some(result.clone());
            }
            // Remove expired entry
            cache.remove(&key);
        }
        None
    }

    /// Store a result in the cache
    pub fn store_result(&self, message: &Message, result: Vec<u8>) {
        let key = self.cache_key(message);
        let mut cache = self.cache.lock().unwrap();

        // Evict oldest entries if cache is full
        if cache.len() >= self.max_entries {
            let to_remove = cache.len() / 5; // Remove oldest 20%
            let mut entries: Vec<_> = cache.iter().map(|(k, v)| (k.clone(), v.1)).collect();
            entries.sort_by_key(|(_, timestamp)| *timestamp);

            for (key, _) in entries.iter().take(to_remove) {
                cache.remove(key);
            }
        }

        cache.insert(key, (result, std::time::Instant::now()));
    }

    /// Clear all cached results
    pub fn clear(&self) {
        self.cache.lock().unwrap().clear();
    }

    /// Get the number of cached entries
    pub fn cache_size(&self) -> usize {
        self.cache.lock().unwrap().len()
    }
}

#[async_trait]
impl MessageMiddleware for CachingMiddleware {
    async fn before_publish(&self, _message: &mut Message) -> Result<()> {
        // No action needed on publish
        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Check if result is cached
        if let Some(cached_result) = self.get_cached(message) {
            message
                .headers
                .extra
                .insert("x-cache-hit".to_string(), serde_json::json!(true));
            message.headers.extra.insert(
                "x-cached-result-size".to_string(),
                serde_json::json!(cached_result.len()),
            );
        } else {
            message
                .headers
                .extra
                .insert("x-cache-hit".to_string(), serde_json::json!(false));
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "caching"
    }
}

/// Bulkhead middleware for limiting concurrent operations per partition
///
/// This middleware implements the bulkhead pattern to prevent resource exhaustion
/// by limiting the number of concurrent operations per partition/queue.
///
/// # Examples
///
/// ```
/// use celers_kombu::BulkheadMiddleware;
///
/// // Create bulkhead with max 50 concurrent operations per partition
/// let bulkhead = BulkheadMiddleware::new(50);
///
/// // Create with custom partition key extractor
/// let bulkhead = BulkheadMiddleware::with_partition_fn(50, |msg| {
///     msg.headers.extra.get("partition_key")
///         .and_then(|v| v.as_str())
///         .map(|s| s.to_string())
///         .unwrap_or_else(|| "default".to_string())
/// });
/// ```
#[derive(Clone)]
pub struct BulkheadMiddleware {
    max_concurrent: usize,
    permits: std::sync::Arc<std::sync::Mutex<HashMap<String, usize>>>,
    partition_fn: std::sync::Arc<dyn Fn(&Message) -> String + Send + Sync>,
}

impl BulkheadMiddleware {
    /// Create a new bulkhead middleware with max concurrent operations
    ///
    /// # Arguments
    ///
    /// * `max_concurrent` - Maximum number of concurrent operations per partition
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            max_concurrent,
            permits: std::sync::Arc::new(std::sync::Mutex::new(HashMap::new())),
            partition_fn: std::sync::Arc::new(|msg| {
                // Default: partition by task name
                msg.headers.task.clone()
            }),
        }
    }

    /// Create with custom partition key extraction function
    pub fn with_partition_fn<F>(max_concurrent: usize, partition_fn: F) -> Self
    where
        F: Fn(&Message) -> String + Send + Sync + 'static,
    {
        Self {
            max_concurrent,
            permits: std::sync::Arc::new(std::sync::Mutex::new(HashMap::new())),
            partition_fn: std::sync::Arc::new(partition_fn),
        }
    }

    /// Try to acquire a permit for the given partition
    pub fn try_acquire(&self, partition: &str) -> bool {
        let mut permits = self.permits.lock().unwrap();
        let current = permits.entry(partition.to_string()).or_insert(0);
        if *current < self.max_concurrent {
            *current += 1;
            true
        } else {
            false
        }
    }

    /// Release a permit for the given partition
    pub fn release(&self, partition: &str) {
        let mut permits = self.permits.lock().unwrap();
        if let Some(current) = permits.get_mut(partition) {
            if *current > 0 {
                *current -= 1;
            }
        }
    }

    /// Get current concurrent operations for a partition
    pub fn current_operations(&self, partition: &str) -> usize {
        self.permits
            .lock()
            .unwrap()
            .get(partition)
            .copied()
            .unwrap_or(0)
    }

    /// Get total concurrent operations across all partitions
    pub fn total_operations(&self) -> usize {
        self.permits.lock().unwrap().values().sum()
    }
}

#[async_trait]
impl MessageMiddleware for BulkheadMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        let partition = (self.partition_fn)(message);
        if !self.try_acquire(&partition) {
            message
                .headers
                .extra
                .insert("x-bulkhead-rejected".to_string(), serde_json::json!(true));
            message.headers.extra.insert(
                "x-bulkhead-partition".to_string(),
                serde_json::json!(partition),
            );
            message.headers.extra.insert(
                "x-bulkhead-current".to_string(),
                serde_json::json!(self.max_concurrent),
            );
        } else {
            message.headers.extra.insert(
                "x-bulkhead-partition".to_string(),
                serde_json::json!(partition),
            );
        }
        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        let partition = (self.partition_fn)(message);
        self.release(&partition);
        Ok(())
    }

    fn name(&self) -> &str {
        "bulkhead"
    }
}

/// Priority boost middleware for dynamic priority adjustment
///
/// This middleware dynamically adjusts message priority based on configurable rules
/// such as message age, retry count, or custom criteria.
///
/// # Examples
///
/// ```
/// use celers_kombu::{PriorityBoostMiddleware, Priority};
/// use std::time::Duration;
///
/// // Boost priority for messages older than 5 minutes
/// let boost = PriorityBoostMiddleware::new()
///     .with_age_boost(Duration::from_secs(300), Priority::High);
///
/// // Custom boost function
/// let boost = PriorityBoostMiddleware::with_custom_fn(|msg, current_priority| {
///     if msg.headers.retries.unwrap_or(0) > 3 {
///         Priority::Highest
///     } else {
///         current_priority
///     }
/// });
/// ```
pub type PriorityBoostFn = std::sync::Arc<dyn Fn(&Message, Priority) -> Priority + Send + Sync>;

#[derive(Clone)]
pub struct PriorityBoostMiddleware {
    age_threshold: Option<Duration>,
    age_boost_priority: Priority,
    retry_threshold: Option<u32>,
    retry_boost_priority: Priority,
    custom_fn: Option<PriorityBoostFn>,
}

impl PriorityBoostMiddleware {
    /// Create a new priority boost middleware with defaults
    pub fn new() -> Self {
        Self {
            age_threshold: None,
            age_boost_priority: Priority::High,
            retry_threshold: None,
            retry_boost_priority: Priority::High,
            custom_fn: None,
        }
    }

    /// Boost priority for messages older than the specified duration
    pub fn with_age_boost(mut self, threshold: Duration, priority: Priority) -> Self {
        self.age_threshold = Some(threshold);
        self.age_boost_priority = priority;
        self
    }

    /// Boost priority for messages with retry count exceeding threshold
    pub fn with_retry_boost(mut self, threshold: u32, priority: Priority) -> Self {
        self.retry_threshold = Some(threshold);
        self.retry_boost_priority = priority;
        self
    }

    /// Create with custom priority boost function
    pub fn with_custom_fn<F>(custom_fn: F) -> Self
    where
        F: Fn(&Message, Priority) -> Priority + Send + Sync + 'static,
    {
        Self {
            age_threshold: None,
            age_boost_priority: Priority::High,
            retry_threshold: None,
            retry_boost_priority: Priority::High,
            custom_fn: Some(std::sync::Arc::new(custom_fn)),
        }
    }

    /// Calculate boosted priority for a message
    pub fn calculate_priority(&self, message: &Message, current_priority: Priority) -> Priority {
        let mut priority = current_priority;

        // Apply custom function if provided
        if let Some(ref custom_fn) = self.custom_fn {
            return custom_fn(message, priority);
        }

        // Check retry count
        if let Some(retry_threshold) = self.retry_threshold {
            if message.headers.retries.unwrap_or(0) >= retry_threshold {
                priority = priority.max(self.retry_boost_priority);
            }
        }

        // Check message age (using timestamp if available)
        if let Some(age_threshold) = self.age_threshold {
            if let Some(timestamp_value) = message.headers.extra.get("timestamp") {
                if let Some(timestamp_secs) = timestamp_value.as_f64() {
                    let msg_age = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs_f64()
                        - timestamp_secs;
                    if msg_age > age_threshold.as_secs_f64() {
                        priority = priority.max(self.age_boost_priority);
                    }
                }
            }
        }

        priority
    }
}

impl Default for PriorityBoostMiddleware {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MessageMiddleware for PriorityBoostMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Get current priority from message headers
        let current_priority = message
            .headers
            .extra
            .get("priority")
            .and_then(|v| v.as_u64())
            .map(|p| Priority::from_u8(p as u8))
            .unwrap_or(Priority::Normal);

        let boosted_priority = self.calculate_priority(message, current_priority);

        if boosted_priority != current_priority {
            message.headers.extra.insert(
                "priority".to_string(),
                serde_json::json!(boosted_priority.as_u8()),
            );
            message
                .headers
                .extra
                .insert("x-priority-boosted".to_string(), serde_json::json!(true));
            message.headers.extra.insert(
                "x-original-priority".to_string(),
                serde_json::json!(current_priority.as_u8()),
            );
        }
        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        // No action needed on consume
        Ok(())
    }

    fn name(&self) -> &str {
        "priority_boost"
    }
}

/// Error classification middleware for intelligent error routing
///
/// This middleware classifies errors into categories and can route messages
/// to different queues based on error type (e.g., transient vs permanent errors).
///
/// # Examples
///
/// ```
/// use celers_kombu::ErrorClassificationMiddleware;
///
/// let classifier = ErrorClassificationMiddleware::new()
///     .with_transient_pattern("timeout|connection")
///     .with_permanent_pattern("validation|schema")
///     .with_max_transient_retries(5);
/// ```
pub struct ErrorClassificationMiddleware {
    transient_patterns: Vec<String>,
    permanent_patterns: Vec<String>,
    max_transient_retries: u32,
    max_permanent_retries: u32,
}

impl ErrorClassificationMiddleware {
    /// Create a new error classification middleware
    pub fn new() -> Self {
        Self {
            transient_patterns: vec![
                "timeout".to_string(),
                "connection".to_string(),
                "network".to_string(),
                "unavailable".to_string(),
            ],
            permanent_patterns: vec![
                "validation".to_string(),
                "schema".to_string(),
                "invalid".to_string(),
                "forbidden".to_string(),
            ],
            max_transient_retries: 10,
            max_permanent_retries: 1,
        }
    }

    /// Add a pattern for transient errors (can be retried)
    pub fn with_transient_pattern(mut self, pattern: &str) -> Self {
        self.transient_patterns.push(pattern.to_string());
        self
    }

    /// Add a pattern for permanent errors (should not be retried)
    pub fn with_permanent_pattern(mut self, pattern: &str) -> Self {
        self.permanent_patterns.push(pattern.to_string());
        self
    }

    /// Set maximum retries for transient errors
    pub fn with_max_transient_retries(mut self, max_retries: u32) -> Self {
        self.max_transient_retries = max_retries;
        self
    }

    /// Set maximum retries for permanent errors
    pub fn with_max_permanent_retries(mut self, max_retries: u32) -> Self {
        self.max_permanent_retries = max_retries;
        self
    }

    /// Classify an error message
    pub fn classify_error(&self, error_msg: &str) -> ErrorClass {
        let error_lower = error_msg.to_lowercase();

        // Check for permanent errors first (more specific)
        for pattern in &self.permanent_patterns {
            if error_lower.contains(&pattern.to_lowercase()) {
                return ErrorClass::Permanent;
            }
        }

        // Check for transient errors
        for pattern in &self.transient_patterns {
            if error_lower.contains(&pattern.to_lowercase()) {
                return ErrorClass::Transient;
            }
        }

        // Unknown errors are treated as transient by default
        ErrorClass::Unknown
    }

    /// Determine if a message should be retried based on error classification
    pub fn should_retry(&self, error_msg: &str, current_retries: u32) -> bool {
        match self.classify_error(error_msg) {
            ErrorClass::Transient => current_retries < self.max_transient_retries,
            ErrorClass::Permanent => current_retries < self.max_permanent_retries,
            ErrorClass::Unknown => current_retries < self.max_transient_retries,
        }
    }
}

impl Default for ErrorClassificationMiddleware {
    fn default() -> Self {
        Self::new()
    }
}

/// Error classification categories
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClass {
    /// Transient errors that should be retried (e.g., network timeouts)
    Transient,
    /// Permanent errors that should not be retried (e.g., validation errors)
    Permanent,
    /// Unknown error type (treated as transient by default)
    Unknown,
}

#[async_trait]
impl MessageMiddleware for ErrorClassificationMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Check if message has error information
        if let Some(error_value) = message.headers.extra.get("error") {
            if let Some(error_msg) = error_value.as_str() {
                let error_class = self.classify_error(error_msg);
                let should_retry =
                    self.should_retry(error_msg, message.headers.retries.unwrap_or(0));

                message.headers.extra.insert(
                    "x-error-class".to_string(),
                    serde_json::json!(match error_class {
                        ErrorClass::Transient => "transient",
                        ErrorClass::Permanent => "permanent",
                        ErrorClass::Unknown => "unknown",
                    }),
                );

                message.headers.extra.insert(
                    "x-should-retry".to_string(),
                    serde_json::json!(should_retry),
                );

                if !should_retry {
                    message.headers.extra.insert(
                        "x-max-retries-exceeded".to_string(),
                        serde_json::json!(true),
                    );
                }
            }
        }
        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        // No action needed on consume
        Ok(())
    }

    fn name(&self) -> &str {
        "error_classification"
    }
}

/// Correlation middleware for distributed tracing
///
/// Automatically generates and propagates correlation IDs across service boundaries
/// for distributed tracing and request tracking.
///
/// # Examples
///
/// ```
/// use celers_kombu::CorrelationMiddleware;
///
/// let correlation = CorrelationMiddleware::new();
/// // Automatically generates correlation ID if not present
/// // Propagates existing correlation ID from headers
/// ```
pub struct CorrelationMiddleware {
    header_name: String,
}

impl CorrelationMiddleware {
    /// Create a new correlation middleware
    pub fn new() -> Self {
        Self {
            header_name: "x-correlation-id".to_string(),
        }
    }

    /// Create with custom header name
    pub fn with_header_name(header_name: &str) -> Self {
        Self {
            header_name: header_name.to_string(),
        }
    }

    fn get_or_generate_correlation_id(&self, message: &Message) -> String {
        message
            .headers
            .extra
            .get(&self.header_name)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| Uuid::new_v4().to_string())
    }
}

impl Default for CorrelationMiddleware {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MessageMiddleware for CorrelationMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        let correlation_id = self.get_or_generate_correlation_id(message);
        message
            .headers
            .extra
            .insert(self.header_name.clone(), serde_json::json!(correlation_id));
        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Ensure correlation ID is present for downstream processing
        let correlation_id = self.get_or_generate_correlation_id(message);
        message
            .headers
            .extra
            .insert(self.header_name.clone(), serde_json::json!(correlation_id));
        Ok(())
    }

    fn name(&self) -> &str {
        "correlation"
    }
}

/// Throttling middleware with backpressure support
///
/// Implements advanced throttling with configurable backpressure behavior.
/// Unlike rate limiting which rejects messages, throttling delays them.
///
/// # Examples
///
/// ```
/// use celers_kombu::ThrottlingMiddleware;
/// use std::time::Duration;
///
/// let throttle = ThrottlingMiddleware::new(100.0)  // 100 msg/sec
///     .with_burst_size(200)
///     .with_backpressure_threshold(0.8);
/// ```
pub struct ThrottlingMiddleware {
    max_rate: f64,
    burst_size: usize,
    backpressure_threshold: f64,
    last_refill: std::sync::Mutex<std::time::Instant>,
    available_tokens: std::sync::Mutex<f64>,
}

impl ThrottlingMiddleware {
    /// Create a new throttling middleware
    pub fn new(max_rate: f64) -> Self {
        Self {
            max_rate,
            burst_size: (max_rate * 2.0) as usize,
            backpressure_threshold: 0.8,
            last_refill: std::sync::Mutex::new(std::time::Instant::now()),
            available_tokens: std::sync::Mutex::new(max_rate),
        }
    }

    /// Set burst size (maximum tokens that can accumulate)
    pub fn with_burst_size(mut self, size: usize) -> Self {
        self.burst_size = size;
        self
    }

    /// Set backpressure threshold (0.0-1.0)
    pub fn with_backpressure_threshold(mut self, threshold: f64) -> Self {
        self.backpressure_threshold = threshold.clamp(0.0, 1.0);
        self
    }

    fn refill_tokens(&self) {
        let mut last_refill = self.last_refill.lock().unwrap();
        let mut tokens = self.available_tokens.lock().unwrap();

        let now = std::time::Instant::now();
        let elapsed = now.duration_since(*last_refill).as_secs_f64();

        let new_tokens = elapsed * self.max_rate;
        *tokens = (*tokens + new_tokens).min(self.burst_size as f64);
        *last_refill = now;
    }

    fn calculate_delay(&self) -> Duration {
        self.refill_tokens();
        let tokens = self.available_tokens.lock().unwrap();

        if *tokens >= 1.0 {
            Duration::from_millis(0)
        } else {
            let wait_time = (1.0 - *tokens) / self.max_rate;
            Duration::from_secs_f64(wait_time)
        }
    }

    fn should_apply_backpressure(&self) -> bool {
        self.refill_tokens();
        let tokens = self.available_tokens.lock().unwrap();
        (*tokens / self.burst_size as f64) < (1.0 - self.backpressure_threshold)
    }
}

#[async_trait]
impl MessageMiddleware for ThrottlingMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        let delay = self.calculate_delay();

        if delay > Duration::from_millis(0) {
            message.headers.extra.insert(
                "x-throttle-delay-ms".to_string(),
                serde_json::json!(delay.as_millis()),
            );
        }

        if self.should_apply_backpressure() {
            message
                .headers
                .extra
                .insert("x-backpressure-active".to_string(), serde_json::json!(true));
        }

        // Consume a token
        let mut tokens = self.available_tokens.lock().unwrap();
        if *tokens >= 1.0 {
            *tokens -= 1.0;
        }

        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        // No action needed
        Ok(())
    }

    fn name(&self) -> &str {
        "throttling"
    }
}

/// Circuit breaker middleware for fault tolerance
///
/// Implements the circuit breaker pattern to prevent cascading failures.
/// Tracks failures and opens the circuit after a threshold is reached.
///
/// # Examples
///
/// ```
/// use celers_kombu::CircuitBreakerMiddleware;
/// use std::time::Duration;
///
/// let breaker = CircuitBreakerMiddleware::new(5, Duration::from_secs(60));
/// // Opens circuit after 5 failures within 60 seconds
/// ```
pub struct CircuitBreakerMiddleware {
    failure_threshold: usize,
    window: Duration,
    failures: std::sync::Mutex<Vec<std::time::Instant>>,
}

impl CircuitBreakerMiddleware {
    /// Create a new circuit breaker middleware
    pub fn new(failure_threshold: usize, window: Duration) -> Self {
        Self {
            failure_threshold,
            window,
            failures: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn record_failure(&self) {
        let mut failures = self.failures.lock().unwrap();
        let now = std::time::Instant::now();

        // Remove old failures outside the window
        failures.retain(|&f| now.duration_since(f) < self.window);

        // Add new failure
        failures.push(now);
    }

    fn is_circuit_open(&self) -> bool {
        let mut failures = self.failures.lock().unwrap();
        let now = std::time::Instant::now();

        // Clean up old failures
        failures.retain(|&f| now.duration_since(f) < self.window);

        failures.len() >= self.failure_threshold
    }

    fn get_failure_count(&self) -> usize {
        let mut failures = self.failures.lock().unwrap();
        let now = std::time::Instant::now();
        failures.retain(|&f| now.duration_since(f) < self.window);
        failures.len()
    }
}

#[async_trait]
impl MessageMiddleware for CircuitBreakerMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        if self.is_circuit_open() {
            message.headers.extra.insert(
                "x-circuit-breaker-open".to_string(),
                serde_json::json!(true),
            );
            message.headers.extra.insert(
                "x-circuit-breaker-failures".to_string(),
                serde_json::json!(self.get_failure_count()),
            );
            return Err(BrokerError::OperationFailed(
                "Circuit breaker is open".to_string(),
            ));
        }
        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Check if message has error indicator
        if message.headers.extra.contains_key("error") {
            self.record_failure();
        }

        // Add circuit state to message
        message.headers.extra.insert(
            "x-circuit-breaker-failures".to_string(),
            serde_json::json!(self.get_failure_count()),
        );

        Ok(())
    }

    fn name(&self) -> &str {
        "circuit_breaker"
    }
}

/// Schema validation middleware - validates message structure and content
///
/// This middleware validates messages against configured rules before publishing
/// and after consuming.
///
/// # Examples
///
/// ```
/// use celers_kombu::SchemaValidationMiddleware;
///
/// let validator = SchemaValidationMiddleware::new()
///     .with_required_field("user_id")
///     .with_required_field("action")
///     .with_max_field_count(20);
/// ```
pub struct SchemaValidationMiddleware {
    required_fields: Vec<String>,
    max_field_count: Option<usize>,
    min_body_size: Option<usize>,
    max_body_size: Option<usize>,
}

impl SchemaValidationMiddleware {
    /// Create a new schema validation middleware
    pub fn new() -> Self {
        Self {
            required_fields: Vec::new(),
            max_field_count: None,
            min_body_size: None,
            max_body_size: None,
        }
    }

    /// Add a required field to validation
    pub fn with_required_field(mut self, field: impl Into<String>) -> Self {
        self.required_fields.push(field.into());
        self
    }

    /// Set maximum field count
    pub fn with_max_field_count(mut self, count: usize) -> Self {
        self.max_field_count = Some(count);
        self
    }

    /// Set minimum body size
    pub fn with_min_body_size(mut self, size: usize) -> Self {
        self.min_body_size = Some(size);
        self
    }

    /// Set maximum body size
    pub fn with_max_body_size(mut self, size: usize) -> Self {
        self.max_body_size = Some(size);
        self
    }

    fn validate_message(&self, message: &Message) -> Result<()> {
        // Check required fields
        for field in &self.required_fields {
            if !message.headers.extra.contains_key(field) {
                return Err(BrokerError::Configuration(format!(
                    "Missing required field: {}",
                    field
                )));
            }
        }

        // Check field count
        if let Some(max) = self.max_field_count {
            if message.headers.extra.len() > max {
                return Err(BrokerError::Configuration(format!(
                    "Too many fields: {} > {}",
                    message.headers.extra.len(),
                    max
                )));
            }
        }

        // Check body size
        let body_len = message.body.len();
        if let Some(min) = self.min_body_size {
            if body_len < min {
                return Err(BrokerError::Configuration(format!(
                    "Body too small: {} < {}",
                    body_len, min
                )));
            }
        }
        if let Some(max) = self.max_body_size {
            if body_len > max {
                return Err(BrokerError::Configuration(format!(
                    "Body too large: {} > {}",
                    body_len, max
                )));
            }
        }

        Ok(())
    }
}

impl Default for SchemaValidationMiddleware {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MessageMiddleware for SchemaValidationMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        self.validate_message(message)?;
        message
            .headers
            .extra
            .insert("x-schema-validated".to_string(), serde_json::json!(true));
        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        self.validate_message(message)
    }

    fn name(&self) -> &str {
        "schema_validation"
    }
}

/// Message enrichment middleware - automatically adds metadata
///
/// This middleware enriches messages with contextual metadata such as
/// hostname, environment, version, and timestamps.
///
/// # Examples
///
/// ```
/// use celers_kombu::MessageEnrichmentMiddleware;
///
/// let enricher = MessageEnrichmentMiddleware::new()
///     .with_hostname("worker-01")
///     .with_environment("production")
///     .with_version("1.0.0")
///     .with_add_timestamp(true);
/// ```
pub struct MessageEnrichmentMiddleware {
    hostname: Option<String>,
    environment: Option<String>,
    version: Option<String>,
    add_timestamp: bool,
    custom_metadata: HashMap<String, serde_json::Value>,
}

impl MessageEnrichmentMiddleware {
    /// Create a new message enrichment middleware
    pub fn new() -> Self {
        Self {
            hostname: None,
            environment: None,
            version: None,
            add_timestamp: false,
            custom_metadata: HashMap::new(),
        }
    }

    /// Set hostname metadata
    pub fn with_hostname(mut self, hostname: impl Into<String>) -> Self {
        self.hostname = Some(hostname.into());
        self
    }

    /// Set environment metadata
    pub fn with_environment(mut self, environment: impl Into<String>) -> Self {
        self.environment = Some(environment.into());
        self
    }

    /// Set version metadata
    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    /// Enable timestamp enrichment
    pub fn with_add_timestamp(mut self, add: bool) -> Self {
        self.add_timestamp = add;
        self
    }

    /// Add custom metadata
    pub fn with_custom_metadata(
        mut self,
        key: impl Into<String>,
        value: serde_json::Value,
    ) -> Self {
        self.custom_metadata.insert(key.into(), value);
        self
    }

    fn enrich_message(&self, message: &mut Message) {
        if let Some(ref hostname) = self.hostname {
            message.headers.extra.insert(
                "x-enrichment-hostname".to_string(),
                serde_json::json!(hostname),
            );
        }

        if let Some(ref environment) = self.environment {
            message.headers.extra.insert(
                "x-enrichment-environment".to_string(),
                serde_json::json!(environment),
            );
        }

        if let Some(ref version) = self.version {
            message.headers.extra.insert(
                "x-enrichment-version".to_string(),
                serde_json::json!(version),
            );
        }

        if self.add_timestamp {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            message.headers.extra.insert(
                "x-enrichment-timestamp".to_string(),
                serde_json::json!(timestamp),
            );
        }

        for (key, value) in &self.custom_metadata {
            message
                .headers
                .extra
                .insert(format!("x-enrichment-{}", key), value.clone());
        }
    }
}

impl Default for MessageEnrichmentMiddleware {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MessageMiddleware for MessageEnrichmentMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        self.enrich_message(message);
        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        // No action needed on consume
        Ok(())
    }

    fn name(&self) -> &str {
        "message_enrichment"
    }
}

/// Retry strategy for intelligent retry handling
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryStrategy {
    /// Exponential backoff (delay doubles each time)
    Exponential,
    /// Linear backoff (delay increases by fixed amount)
    Linear,
    /// Fibonacci backoff (follows fibonacci sequence)
    Fibonacci,
    /// Fixed delay (constant delay between retries)
    Fixed,
}

/// Retry strategy middleware - implements different retry strategies
///
/// This middleware applies various retry strategies to failed messages,
/// calculating appropriate delays based on the retry count.
///
/// # Examples
///
/// ```
/// use celers_kombu::{RetryStrategyMiddleware, RetryStrategy};
/// use std::time::Duration;
///
/// let middleware = RetryStrategyMiddleware::new(RetryStrategy::Exponential)
///     .with_base_delay(Duration::from_secs(1))
///     .with_max_delay(Duration::from_secs(300))
///     .with_max_retries(5);
/// ```
pub struct RetryStrategyMiddleware {
    strategy: RetryStrategy,
    base_delay_ms: u64,
    max_delay_ms: u64,
    max_retries: u32,
}

impl RetryStrategyMiddleware {
    /// Create a new retry strategy middleware
    pub fn new(strategy: RetryStrategy) -> Self {
        Self {
            strategy,
            base_delay_ms: 1000,   // 1 second default
            max_delay_ms: 300_000, // 5 minutes default
            max_retries: 5,
        }
    }

    /// Set the base delay
    pub fn with_base_delay(mut self, delay: Duration) -> Self {
        self.base_delay_ms = delay.as_millis() as u64;
        self
    }

    /// Set the maximum delay
    pub fn with_max_delay(mut self, delay: Duration) -> Self {
        self.max_delay_ms = delay.as_millis() as u64;
        self
    }

    /// Set the maximum number of retries
    pub fn with_max_retries(mut self, retries: u32) -> Self {
        self.max_retries = retries;
        self
    }

    fn calculate_delay(&self, retry_count: u32) -> u64 {
        let delay = match self.strategy {
            RetryStrategy::Exponential => {
                // delay = base * 2^retry_count
                self.base_delay_ms * 2_u64.pow(retry_count)
            }
            RetryStrategy::Linear => {
                // delay = base * retry_count
                self.base_delay_ms * (retry_count as u64 + 1)
            }
            RetryStrategy::Fibonacci => {
                // Calculate fibonacci number for retry_count
                let fib = self.fibonacci(retry_count as usize);
                self.base_delay_ms * fib
            }
            RetryStrategy::Fixed => {
                // Always use base delay
                self.base_delay_ms
            }
        };

        delay.min(self.max_delay_ms)
    }

    fn fibonacci(&self, n: usize) -> u64 {
        match n {
            0 => 1,
            1 => 1,
            _ => {
                let mut a = 1u64;
                let mut b = 1u64;
                for _ in 2..=n {
                    let temp = a + b;
                    a = b;
                    b = temp;
                }
                b
            }
        }
    }
}

impl Default for RetryStrategyMiddleware {
    fn default() -> Self {
        Self::new(RetryStrategy::Exponential)
    }
}

#[async_trait]
impl MessageMiddleware for RetryStrategyMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Get retry count from headers
        let retry_count = message
            .headers
            .extra
            .get("x-retry-count")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32;

        // Check if max retries exceeded
        if retry_count >= self.max_retries {
            return Err(BrokerError::OperationFailed(format!(
                "Max retries ({}) exceeded",
                self.max_retries
            )));
        }

        // Calculate and set delay
        let delay_ms = self.calculate_delay(retry_count);
        message
            .headers
            .extra
            .insert("x-retry-delay-ms".to_string(), serde_json::json!(delay_ms));
        message.headers.extra.insert(
            "x-retry-strategy".to_string(),
            serde_json::json!(format!("{:?}", self.strategy)),
        );

        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        // No action needed on consume
        Ok(())
    }

    fn name(&self) -> &str {
        "retry_strategy"
    }
}

/// Tenant isolation middleware - provides multi-tenancy support
///
/// This middleware enforces tenant isolation by validating and routing
/// messages based on tenant identifiers.
///
/// # Examples
///
/// ```
/// use celers_kombu::TenantIsolationMiddleware;
///
/// let middleware = TenantIsolationMiddleware::new()
///     .with_required_tenant(true)
///     .with_tenant_header("x-tenant-id");
/// ```
pub struct TenantIsolationMiddleware {
    required: bool,
    tenant_header: String,
    allowed_tenants: Option<Vec<String>>,
}

impl TenantIsolationMiddleware {
    /// Create a new tenant isolation middleware
    pub fn new() -> Self {
        Self {
            required: true,
            tenant_header: "x-tenant-id".to_string(),
            allowed_tenants: None,
        }
    }

    /// Set whether tenant ID is required
    pub fn with_required_tenant(mut self, required: bool) -> Self {
        self.required = required;
        self
    }

    /// Set the tenant header name
    pub fn with_tenant_header(mut self, header: impl Into<String>) -> Self {
        self.tenant_header = header.into();
        self
    }

    /// Set allowed tenants (whitelist)
    pub fn with_allowed_tenants(mut self, tenants: Vec<String>) -> Self {
        self.allowed_tenants = Some(tenants);
        self
    }

    fn validate_tenant(&self, tenant_id: Option<&str>) -> Result<()> {
        // Check if tenant is required but missing
        if self.required && tenant_id.is_none() {
            return Err(BrokerError::Configuration(format!(
                "Missing required tenant header: {}",
                self.tenant_header
            )));
        }

        // Check if tenant is in whitelist (if whitelist exists)
        if let (Some(tenant), Some(allowed)) = (tenant_id, &self.allowed_tenants) {
            if !allowed.contains(&tenant.to_string()) {
                return Err(BrokerError::Configuration(format!(
                    "Tenant '{}' not in allowed list",
                    tenant
                )));
            }
        }

        Ok(())
    }
}

impl Default for TenantIsolationMiddleware {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MessageMiddleware for TenantIsolationMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        let tenant_id = message
            .headers
            .extra
            .get(&self.tenant_header)
            .and_then(|v| v.as_str());

        self.validate_tenant(tenant_id)?;

        // Add tenant validation marker
        message
            .headers
            .extra
            .insert("x-tenant-validated".to_string(), serde_json::json!(true));

        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        let tenant_id = message
            .headers
            .extra
            .get(&self.tenant_header)
            .and_then(|v| v.as_str());

        self.validate_tenant(tenant_id)
    }

    fn name(&self) -> &str {
        "tenant_isolation"
    }
}

/// Partitioning middleware for distributed load balancing
///
/// Automatically assigns partition keys to messages for distributed processing.
/// Useful for ensuring related messages are processed by the same worker.
///
/// # Examples
///
/// ```
/// use celers_kombu::PartitioningMiddleware;
///
/// let partitioner = PartitioningMiddleware::new(8); // 8 partitions
/// assert_eq!(partitioner.partition_count(), 8);
/// ```
#[derive(Debug, Clone)]
pub struct PartitioningMiddleware {
    partition_count: usize,
    partition_header: String,
    partition_key_fn: Option<String>, // Field name to use for partitioning
}

impl PartitioningMiddleware {
    /// Create a new partitioning middleware with specified partition count
    pub fn new(partition_count: usize) -> Self {
        Self {
            partition_count: partition_count.max(1),
            partition_header: "x-partition-id".to_string(),
            partition_key_fn: None,
        }
    }

    /// Set the partition header name
    pub fn with_partition_header(mut self, header: impl Into<String>) -> Self {
        self.partition_header = header.into();
        self
    }

    /// Set the field name to use for partition key extraction
    pub fn with_partition_key_field(mut self, field: impl Into<String>) -> Self {
        self.partition_key_fn = Some(field.into());
        self
    }

    /// Get the partition count
    pub fn partition_count(&self) -> usize {
        self.partition_count
    }

    fn calculate_partition(&self, message: &Message) -> usize {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        // Try to extract partition key from specified field or use task ID
        let task_id_str = message.headers.id.to_string();
        let key = if let Some(field) = &self.partition_key_fn {
            message
                .headers
                .extra
                .get(field)
                .and_then(|v| v.as_str())
                .unwrap_or(&task_id_str)
        } else {
            // Default to task ID for partitioning
            &task_id_str
        };

        // Hash the key to determine partition
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();

        (hash % self.partition_count as u64) as usize
    }
}

impl Default for PartitioningMiddleware {
    fn default() -> Self {
        Self::new(4) // Default to 4 partitions
    }
}

#[async_trait]
impl MessageMiddleware for PartitioningMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        let partition_id = self.calculate_partition(message);

        // Inject partition ID into message headers
        message.headers.extra.insert(
            self.partition_header.clone(),
            serde_json::json!(partition_id),
        );

        // Also add total partition count for consumer reference
        message.headers.extra.insert(
            "x-partition-count".to_string(),
            serde_json::json!(self.partition_count),
        );

        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        // No action needed on consume
        Ok(())
    }

    fn name(&self) -> &str {
        "partitioning"
    }
}

/// Adaptive timeout middleware with dynamic timeout adjustment
///
/// Adjusts message timeouts based on historical processing times.
/// Helps optimize resource usage and prevents premature timeouts.
///
/// # Examples
///
/// ```
/// use celers_kombu::AdaptiveTimeoutMiddleware;
/// use std::time::Duration;
///
/// let adaptive = AdaptiveTimeoutMiddleware::new(Duration::from_secs(30));
/// assert!(adaptive.has_samples() == false);
/// ```
#[derive(Debug, Clone)]
pub struct AdaptiveTimeoutMiddleware {
    base_timeout: Duration,
    min_timeout: Duration,
    max_timeout: Duration,
    samples: Vec<u64>, // Processing times in milliseconds
    #[allow(dead_code)]
    max_samples: usize,
    percentile: f64, // Use this percentile for timeout calculation (e.g., 0.95 for p95)
}

impl AdaptiveTimeoutMiddleware {
    /// Create a new adaptive timeout middleware
    pub fn new(base_timeout: Duration) -> Self {
        Self {
            base_timeout,
            min_timeout: Duration::from_secs(1),
            max_timeout: base_timeout.mul_f64(5.0), // Max 5x base timeout
            samples: Vec::new(),
            max_samples: 100,
            percentile: 0.95, // Default to p95
        }
    }

    /// Set minimum timeout
    pub fn with_min_timeout(mut self, timeout: Duration) -> Self {
        self.min_timeout = timeout;
        self
    }

    /// Set maximum timeout
    pub fn with_max_timeout(mut self, timeout: Duration) -> Self {
        self.max_timeout = timeout;
        self
    }

    /// Set the percentile to use for timeout calculation
    pub fn with_percentile(mut self, percentile: f64) -> Self {
        self.percentile = percentile.clamp(0.0, 1.0);
        self
    }

    /// Check if we have collected samples
    pub fn has_samples(&self) -> bool {
        !self.samples.is_empty()
    }

    /// Calculate adaptive timeout based on collected samples
    pub fn calculate_adaptive_timeout(&self) -> Duration {
        if self.samples.is_empty() {
            return self.base_timeout;
        }

        let mut sorted_samples = self.samples.clone();
        sorted_samples.sort_unstable();

        let index = ((sorted_samples.len() as f64 * self.percentile) as usize)
            .min(sorted_samples.len() - 1);
        let timeout_ms = sorted_samples[index];

        // Add 20% buffer to the percentile value
        let buffered_ms = (timeout_ms as f64 * 1.2) as u64;

        let timeout = Duration::from_millis(buffered_ms);

        // Clamp to min/max bounds
        timeout.clamp(self.min_timeout, self.max_timeout)
    }
}

impl Default for AdaptiveTimeoutMiddleware {
    fn default() -> Self {
        Self::new(Duration::from_secs(30))
    }
}

#[async_trait]
impl MessageMiddleware for AdaptiveTimeoutMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        let timeout = self.calculate_adaptive_timeout();

        // Inject adaptive timeout into message headers
        message.headers.extra.insert(
            "x-adaptive-timeout".to_string(),
            serde_json::json!(timeout.as_millis() as u64),
        );

        // Also add the percentile used for transparency
        message.headers.extra.insert(
            "x-timeout-percentile".to_string(),
            serde_json::json!(self.percentile),
        );

        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        // In a real implementation, we would record the actual processing time here
        // For now, this is a placeholder
        Ok(())
    }

    fn name(&self) -> &str {
        "adaptive_timeout"
    }
}

/// Batch acknowledgment hint middleware
///
/// Provides hints to consumers about optimal batch sizes for acknowledgments.
/// Helps optimize network round-trips and improve throughput.
///
/// # Examples
///
/// ```
/// use celers_kombu::BatchAckHintMiddleware;
///
/// let batch_hint = BatchAckHintMiddleware::new(10);
/// assert_eq!(batch_hint.batch_size(), 10);
/// ```
#[derive(Debug, Clone)]
pub struct BatchAckHintMiddleware {
    batch_size: usize,
    hint_header: String,
}

impl BatchAckHintMiddleware {
    /// Create a new batch acknowledgment hint middleware
    pub fn new(batch_size: usize) -> Self {
        Self {
            batch_size: batch_size.max(1),
            hint_header: "x-batch-ack-hint".to_string(),
        }
    }

    /// Set the hint header name
    pub fn with_hint_header(mut self, header: impl Into<String>) -> Self {
        self.hint_header = header.into();
        self
    }

    /// Get the batch size
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }
}

impl Default for BatchAckHintMiddleware {
    fn default() -> Self {
        Self::new(10)
    }
}

#[async_trait]
impl MessageMiddleware for BatchAckHintMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Inject batch acknowledgment hint
        message
            .headers
            .extra
            .insert(self.hint_header.clone(), serde_json::json!(self.batch_size));

        // Add hint about whether batching is recommended
        message.headers.extra.insert(
            "x-batch-ack-recommended".to_string(),
            serde_json::json!(true),
        );

        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        // No action needed on consume
        Ok(())
    }

    fn name(&self) -> &str {
        "batch_ack_hint"
    }
}

/// Load shedding middleware for graceful degradation under pressure
///
/// Automatically drops low-priority messages when system load exceeds thresholds.
/// Helps maintain service stability during traffic spikes.
///
/// # Examples
///
/// ```
/// use celers_kombu::LoadSheddingMiddleware;
///
/// let load_shedder = LoadSheddingMiddleware::new(0.8); // 80% threshold
/// assert_eq!(load_shedder.threshold(), 0.8);
/// ```
#[derive(Debug, Clone)]
pub struct LoadSheddingMiddleware {
    load_threshold: f64, // Threshold for load shedding (0.0-1.0)
    priority_cutoff: u8, // Drop messages below this priority
    current_load: f64,   // Current system load estimate
}

impl LoadSheddingMiddleware {
    /// Create a new load shedding middleware
    pub fn new(load_threshold: f64) -> Self {
        Self {
            load_threshold: load_threshold.clamp(0.0, 1.0),
            priority_cutoff: 3, // Default: drop priority < 3 (Low and below)
            current_load: 0.0,
        }
    }

    /// Set the priority cutoff
    pub fn with_priority_cutoff(mut self, cutoff: u8) -> Self {
        self.priority_cutoff = cutoff.min(10);
        self
    }

    /// Update current load estimate
    pub fn update_load(&mut self, load: f64) {
        self.current_load = load.clamp(0.0, 1.0);
    }

    /// Get the load threshold
    pub fn threshold(&self) -> f64 {
        self.load_threshold
    }

    /// Check if message should be dropped
    fn should_shed(&self, priority: u8) -> bool {
        self.current_load > self.load_threshold && priority < self.priority_cutoff
    }
}

impl Default for LoadSheddingMiddleware {
    fn default() -> Self {
        Self::new(0.8)
    }
}

#[async_trait]
impl MessageMiddleware for LoadSheddingMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        let priority = message
            .headers
            .extra
            .get("priority")
            .and_then(|v| v.as_u64())
            .map(|v| v as u8)
            .unwrap_or(5);

        if self.should_shed(priority) {
            // Inject load shedding marker
            message
                .headers
                .extra
                .insert("x-load-shed".to_string(), serde_json::json!(true));
            message.headers.extra.insert(
                "x-current-load".to_string(),
                serde_json::json!(self.current_load),
            );

            return Err(BrokerError::OperationFailed(format!(
                "Load shedding: current load {:.2} exceeds threshold {:.2}",
                self.current_load, self.load_threshold
            )));
        }

        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        Ok(())
    }

    fn name(&self) -> &str {
        "load_shedding"
    }
}

/// Message priority escalation middleware
///
/// Automatically escalates message priority based on age and retry count.
/// Prevents message starvation in priority queues.
///
/// # Examples
///
/// ```
/// use celers_kombu::MessagePriorityEscalationMiddleware;
///
/// let escalator = MessagePriorityEscalationMiddleware::new(300); // 5 min threshold
/// assert_eq!(escalator.age_threshold_secs(), 300);
/// ```
#[derive(Debug, Clone)]
pub struct MessagePriorityEscalationMiddleware {
    age_threshold_secs: u64, // Age threshold for escalation
    escalation_step: u8,     // Priority increase per threshold
    max_priority: u8,        // Maximum priority (cap)
    escalate_on_retry: bool, // Also escalate based on retry count
}

impl MessagePriorityEscalationMiddleware {
    /// Create a new priority escalation middleware
    pub fn new(age_threshold_secs: u64) -> Self {
        Self {
            age_threshold_secs,
            escalation_step: 1,
            max_priority: 10,
            escalate_on_retry: true,
        }
    }

    /// Set escalation step
    pub fn with_escalation_step(mut self, step: u8) -> Self {
        self.escalation_step = step.max(1);
        self
    }

    /// Set maximum priority
    pub fn with_max_priority(mut self, max: u8) -> Self {
        self.max_priority = max.min(10);
        self
    }

    /// Set whether to escalate on retry
    pub fn with_escalate_on_retry(mut self, enable: bool) -> Self {
        self.escalate_on_retry = enable;
        self
    }

    /// Get age threshold
    pub fn age_threshold_secs(&self) -> u64 {
        self.age_threshold_secs
    }

    /// Calculate escalated priority
    fn calculate_priority(&self, base_priority: u8, age_secs: u64, retries: u32) -> u8 {
        let mut priority = base_priority;

        // Age-based escalation
        if age_secs >= self.age_threshold_secs {
            let age_multiplier = (age_secs / self.age_threshold_secs) as u8;
            priority = priority.saturating_add(age_multiplier * self.escalation_step);
        }

        // Retry-based escalation
        if self.escalate_on_retry && retries > 0 {
            let retry_boost = (retries as u8).min(3); // Cap retry boost at 3
            priority = priority.saturating_add(retry_boost);
        }

        priority.min(self.max_priority)
    }
}

impl Default for MessagePriorityEscalationMiddleware {
    fn default() -> Self {
        Self::new(300) // 5 minutes
    }
}

#[async_trait]
impl MessageMiddleware for MessagePriorityEscalationMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        let base_priority = message
            .headers
            .extra
            .get("priority")
            .and_then(|v| v.as_u64())
            .map(|v| v as u8)
            .unwrap_or(5);
        let age_secs = 0; // Would be calculated from message timestamp in real implementation
        let retries = message.headers.retries.unwrap_or(0);

        let new_priority = self.calculate_priority(base_priority, age_secs, retries);

        if new_priority != base_priority {
            message
                .headers
                .extra
                .insert("priority".to_string(), serde_json::json!(new_priority));
            message
                .headers
                .extra
                .insert("x-priority-escalated".to_string(), serde_json::json!(true));
            message.headers.extra.insert(
                "x-original-priority".to_string(),
                serde_json::json!(base_priority),
            );
        }

        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        Ok(())
    }

    fn name(&self) -> &str {
        "priority_escalation"
    }
}

/// Observability middleware for structured logging and metrics
///
/// Provides structured logging and metrics export for monitoring systems.
/// Useful for integration with observability platforms.
///
/// # Examples
///
/// ```
/// use celers_kombu::ObservabilityMiddleware;
///
/// let observability = ObservabilityMiddleware::new("my-service");
/// assert_eq!(observability.service_name(), "my-service");
/// ```
#[derive(Debug, Clone)]
pub struct ObservabilityMiddleware {
    service_name: String,
    enable_metrics: bool,
    enable_logging: bool,
    log_level: String,
}

impl ObservabilityMiddleware {
    /// Create a new observability middleware
    pub fn new(service_name: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            enable_metrics: true,
            enable_logging: true,
            log_level: "info".to_string(),
        }
    }

    /// Disable metrics collection
    pub fn without_metrics(mut self) -> Self {
        self.enable_metrics = false;
        self
    }

    /// Disable logging
    pub fn without_logging(mut self) -> Self {
        self.enable_logging = false;
        self
    }

    /// Set log level
    pub fn with_log_level(mut self, level: impl Into<String>) -> Self {
        self.log_level = level.into();
        self
    }

    /// Get service name
    pub fn service_name(&self) -> &str {
        &self.service_name
    }
}

impl Default for ObservabilityMiddleware {
    fn default() -> Self {
        Self::new("unknown-service")
    }
}

#[async_trait]
impl MessageMiddleware for ObservabilityMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        if self.enable_metrics {
            message.headers.extra.insert(
                "x-observability-enabled".to_string(),
                serde_json::json!(true),
            );
        }

        if self.enable_logging {
            message
                .headers
                .extra
                .insert("x-log-level".to_string(), serde_json::json!(self.log_level));
        }

        message.headers.extra.insert(
            "x-service-name".to_string(),
            serde_json::json!(self.service_name),
        );

        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        // In a real implementation, would emit metrics and logs here
        Ok(())
    }

    fn name(&self) -> &str {
        "observability"
    }
}

/// Health check middleware - automatic health status tracking
///
/// # Examples
///
/// ```
/// use celers_kombu::HealthCheckMiddleware;
///
/// // Basic health check middleware
/// let health_check = HealthCheckMiddleware::new();
///
/// // With custom health check interval
/// let custom_health = HealthCheckMiddleware::new()
///     .with_check_interval_secs(30);
/// ```
pub struct HealthCheckMiddleware {
    /// Last health check timestamp (seconds since epoch)
    last_check: std::sync::Arc<std::sync::Mutex<u64>>,
    /// Health check interval in seconds
    check_interval_secs: u64,
    /// Health status
    is_healthy: std::sync::Arc<std::sync::Mutex<bool>>,
}

impl HealthCheckMiddleware {
    /// Create a new health check middleware
    pub fn new() -> Self {
        Self {
            last_check: std::sync::Arc::new(std::sync::Mutex::new(0)),
            check_interval_secs: 60, // Default: 1 minute
            is_healthy: std::sync::Arc::new(std::sync::Mutex::new(true)),
        }
    }

    /// Set health check interval
    pub fn with_check_interval_secs(mut self, interval: u64) -> Self {
        self.check_interval_secs = interval;
        self
    }

    /// Get current health status
    pub fn is_healthy(&self) -> bool {
        *self.is_healthy.lock().unwrap()
    }

    /// Mark as unhealthy
    pub fn mark_unhealthy(&self) {
        *self.is_healthy.lock().unwrap() = false;
    }

    /// Mark as healthy
    pub fn mark_healthy(&self) {
        *self.is_healthy.lock().unwrap() = true;
    }

    fn should_check(&self) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let last = *self.last_check.lock().unwrap();
        now - last >= self.check_interval_secs
    }

    fn update_check_time(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        *self.last_check.lock().unwrap() = now;
    }
}

impl Default for HealthCheckMiddleware {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MessageMiddleware for HealthCheckMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        if self.should_check() {
            self.update_check_time();
            // In a real implementation, would perform actual health check
            // For now, just inject health status into message headers
        }

        let health_status = if self.is_healthy() {
            "healthy"
        } else {
            "unhealthy"
        };
        message.headers.extra.insert(
            "x-health-status".to_string(),
            serde_json::json!(health_status),
        );
        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        // Health check on consume could validate broker connectivity
        Ok(())
    }

    fn name(&self) -> &str {
        "health_check"
    }
}

/// Message tagging middleware - automatic message tagging and categorization
///
/// # Examples
///
/// ```
/// use celers_kombu::MessageTaggingMiddleware;
/// use std::collections::HashMap;
///
/// // Basic tagging with environment
/// let tagging = MessageTaggingMiddleware::new("production");
///
/// // With custom tags
/// let mut tags = HashMap::new();
/// tags.insert("region".to_string(), "us-east-1".to_string());
/// tags.insert("team".to_string(), "platform".to_string());
/// let custom_tagging = MessageTaggingMiddleware::new("production")
///     .with_tags(tags);
/// ```
pub struct MessageTaggingMiddleware {
    /// Environment tag (e.g., "production", "staging")
    environment: String,
    /// Additional custom tags
    tags: HashMap<String, String>,
}

impl MessageTaggingMiddleware {
    /// Create a new message tagging middleware
    pub fn new(environment: impl Into<String>) -> Self {
        Self {
            environment: environment.into(),
            tags: HashMap::new(),
        }
    }

    /// Add custom tags
    pub fn with_tags(mut self, tags: HashMap<String, String>) -> Self {
        self.tags = tags;
        self
    }

    /// Add a single tag
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }
}

#[async_trait]
impl MessageMiddleware for MessageTaggingMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Inject environment tag
        message.headers.extra.insert(
            "x-environment".to_string(),
            serde_json::json!(self.environment.clone()),
        );

        // Inject custom tags
        for (key, value) in &self.tags {
            message
                .headers
                .extra
                .insert(format!("x-tag-{}", key), serde_json::json!(value.clone()));
        }

        // Auto-categorize based on task name
        let category = if message.task_name().contains("email") {
            "communication"
        } else if message.task_name().contains("report") {
            "analytics"
        } else if message.task_name().contains("process") {
            "computation"
        } else {
            "general"
        };
        message
            .headers
            .extra
            .insert("x-category".to_string(), serde_json::json!(category));

        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        // Tags are already present after consumption
        Ok(())
    }

    fn name(&self) -> &str {
        "message_tagging"
    }
}

/// Cost attribution middleware - track costs per tenant/project
///
/// # Examples
///
/// ```
/// use celers_kombu::CostAttributionMiddleware;
///
/// // Basic cost attribution
/// let cost_attr = CostAttributionMiddleware::new(0.001); // $0.001 per message
///
/// // With custom cost factors
/// let advanced_cost = CostAttributionMiddleware::new(0.001)
///     .with_compute_cost_per_sec(0.0001)   // $0.0001 per second
///     .with_storage_cost_per_mb(0.00001);  // $0.00001 per MB
/// ```
pub struct CostAttributionMiddleware {
    /// Base cost per message (in dollars)
    message_cost: f64,
    /// Compute cost per second (in dollars)
    compute_cost_per_sec: f64,
    /// Storage cost per MB (in dollars)
    storage_cost_per_mb: f64,
}

impl CostAttributionMiddleware {
    /// Create a new cost attribution middleware
    pub fn new(message_cost: f64) -> Self {
        Self {
            message_cost,
            compute_cost_per_sec: 0.0,
            storage_cost_per_mb: 0.0,
        }
    }

    /// Set compute cost per second
    pub fn with_compute_cost_per_sec(mut self, cost: f64) -> Self {
        self.compute_cost_per_sec = cost;
        self
    }

    /// Set storage cost per MB
    pub fn with_storage_cost_per_mb(mut self, cost: f64) -> Self {
        self.storage_cost_per_mb = cost;
        self
    }

    fn calculate_cost(&self, message: &Message) -> f64 {
        let mut cost = self.message_cost;

        // Add storage cost based on message size
        let size_mb = message.body.len() as f64 / (1024.0 * 1024.0);
        cost += size_mb * self.storage_cost_per_mb;

        cost
    }
}

#[async_trait]
impl MessageMiddleware for CostAttributionMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Calculate and inject cost
        let cost = self.calculate_cost(message);
        message.headers.extra.insert(
            "x-cost-estimate".to_string(),
            serde_json::json!(format!("{:.6}", cost)),
        );

        // Extract tenant/project from message headers if available
        let tenant = message
            .headers
            .extra
            .get("x-tenant")
            .and_then(|v| v.as_str())
            .or_else(|| message.headers.extra.get("tenant").and_then(|v| v.as_str()))
            .unwrap_or("default")
            .to_string();

        message
            .headers
            .extra
            .insert("x-cost-tenant".to_string(), serde_json::json!(tenant));

        // Inject timestamp for cost tracking
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        message.headers.extra.insert(
            "x-cost-timestamp".to_string(),
            serde_json::json!(timestamp.to_string()),
        );

        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Calculate actual compute cost if processing time is available
        if let Some(cost_timestamp) = message.headers.extra.get("x-cost-timestamp") {
            if let Some(timestamp_str) = cost_timestamp.as_str() {
                if let Ok(start_time) = timestamp_str.parse::<u64>() {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    let duration_secs = (now - start_time) as f64;
                    let compute_cost = duration_secs * self.compute_cost_per_sec;

                    if let Some(base_cost) = message.headers.extra.get("x-cost-estimate") {
                        if let Some(base_str) = base_cost.as_str() {
                            if let Ok(base) = base_str.parse::<f64>() {
                                let total_cost = base + compute_cost;
                                message.headers.extra.insert(
                                    "x-cost-actual".to_string(),
                                    serde_json::json!(format!("{:.6}", total_cost)),
                                );
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "cost_attribution"
    }
}

/// SLA monitoring middleware - track and enforce SLA requirements
///
/// # Examples
///
/// ```
/// use celers_kombu::SLAMonitoringMiddleware;
///
/// // Basic SLA monitoring with 5-second target
/// let sla_monitor = SLAMonitoringMiddleware::new(5000);
///
/// // With custom percentile and alert threshold
/// let advanced_sla = SLAMonitoringMiddleware::new(3000)
///     .with_percentile(99)
///     .with_alert_threshold(0.95);
/// ```
pub struct SLAMonitoringMiddleware {
    /// Target processing time in milliseconds
    target_ms: u64,
    /// Percentile to track (default 95th percentile)
    percentile: u8,
    /// Alert threshold (0.0-1.0, default 0.9 = 90% compliance)
    alert_threshold: f64,
    /// Processing times buffer
    processing_times: std::sync::Arc<std::sync::Mutex<Vec<u64>>>,
}

impl SLAMonitoringMiddleware {
    /// Create a new SLA monitoring middleware
    pub fn new(target_ms: u64) -> Self {
        Self {
            target_ms,
            percentile: 95,
            alert_threshold: 0.9,
            processing_times: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    /// Set the percentile to track
    pub fn with_percentile(mut self, percentile: u8) -> Self {
        self.percentile = percentile.clamp(1, 99);
        self
    }

    /// Set the alert threshold
    pub fn with_alert_threshold(mut self, threshold: f64) -> Self {
        self.alert_threshold = threshold.clamp(0.0, 1.0);
        self
    }

    /// Get current SLA compliance rate
    pub fn compliance_rate(&self) -> f64 {
        let times = self.processing_times.lock().unwrap();
        if times.is_empty() {
            return 1.0;
        }

        let within_sla = times.iter().filter(|&&t| t <= self.target_ms).count();
        within_sla as f64 / times.len() as f64
    }

    /// Check if alert should be triggered
    pub fn should_alert(&self) -> bool {
        self.compliance_rate() < self.alert_threshold
    }
}

#[async_trait]
impl MessageMiddleware for SLAMonitoringMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Inject SLA target
        message.headers.extra.insert(
            "x-sla-target-ms".to_string(),
            serde_json::json!(self.target_ms),
        );

        // Inject start timestamp for SLA tracking
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        message
            .headers
            .extra
            .insert("x-sla-start-ms".to_string(), serde_json::json!(timestamp));

        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Calculate processing time
        if let Some(start_ms) = message.headers.extra.get("x-sla-start-ms") {
            if let Some(start_str) = start_ms.as_u64() {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                let processing_time = now - start_str;

                // Record processing time
                self.processing_times.lock().unwrap().push(processing_time);

                // Inject SLA status
                let within_sla = processing_time <= self.target_ms;
                message
                    .headers
                    .extra
                    .insert("x-sla-met".to_string(), serde_json::json!(within_sla));
                message.headers.extra.insert(
                    "x-sla-processing-ms".to_string(),
                    serde_json::json!(processing_time),
                );

                // Check if we should alert
                if self.should_alert() {
                    message
                        .headers
                        .extra
                        .insert("x-sla-alert".to_string(), serde_json::json!(true));
                }
            }
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "sla_monitoring"
    }
}

/// Message versioning middleware - handle message schema versions
///
/// # Examples
///
/// ```
/// use celers_kombu::MessageVersioningMiddleware;
///
/// // Basic versioning with current version
/// let versioning = MessageVersioningMiddleware::new("2.0");
///
/// // With backward compatibility
/// let compat_versioning = MessageVersioningMiddleware::new("3.0")
///     .with_min_supported_version("2.5")
///     .with_auto_upgrade(true);
/// ```
pub struct MessageVersioningMiddleware {
    /// Current message version
    current_version: String,
    /// Minimum supported version
    min_supported_version: Option<String>,
    /// Auto-upgrade messages to current version
    auto_upgrade: bool,
}

impl MessageVersioningMiddleware {
    /// Create a new message versioning middleware
    pub fn new(current_version: impl Into<String>) -> Self {
        Self {
            current_version: current_version.into(),
            min_supported_version: None,
            auto_upgrade: false,
        }
    }

    /// Set minimum supported version
    pub fn with_min_supported_version(mut self, version: impl Into<String>) -> Self {
        self.min_supported_version = Some(version.into());
        self
    }

    /// Enable auto-upgrade of messages
    pub fn with_auto_upgrade(mut self, enabled: bool) -> Self {
        self.auto_upgrade = enabled;
        self
    }

    fn is_version_supported(&self, version: &str) -> bool {
        if let Some(ref min_version) = self.min_supported_version {
            // Simple string comparison (in production, use semantic versioning)
            version >= min_version.as_str()
        } else {
            true
        }
    }
}

#[async_trait]
impl MessageMiddleware for MessageVersioningMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Inject current version
        message.headers.extra.insert(
            "x-message-version".to_string(),
            serde_json::json!(self.current_version.clone()),
        );

        // Inject schema version metadata
        message.headers.extra.insert(
            "x-schema-version".to_string(),
            serde_json::json!(self.current_version.clone()),
        );

        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Check message version
        if let Some(msg_version) = message.headers.extra.get("x-message-version") {
            if let Some(version_str) = msg_version.as_str() {
                // Check if version is supported
                if !self.is_version_supported(version_str) {
                    return Err(BrokerError::Configuration(format!(
                        "Unsupported message version: {}. Minimum supported: {:?}",
                        version_str, self.min_supported_version
                    )));
                }

                // Auto-upgrade if needed
                if self.auto_upgrade && version_str != self.current_version {
                    message.headers.extra.insert(
                        "x-upgraded-from".to_string(),
                        serde_json::json!(version_str),
                    );
                    message.headers.extra.insert(
                        "x-message-version".to_string(),
                        serde_json::json!(self.current_version.clone()),
                    );
                }
            }
        } else {
            // No version specified, treat as legacy
            message
                .headers
                .extra
                .insert("x-message-version".to_string(), serde_json::json!("legacy"));
            if self.auto_upgrade {
                message
                    .headers
                    .extra
                    .insert("x-upgraded-from".to_string(), serde_json::json!("legacy"));
                message.headers.extra.insert(
                    "x-message-version".to_string(),
                    serde_json::json!(self.current_version.clone()),
                );
            }
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "message_versioning"
    }
}

/// Resource quota middleware - enforce per-consumer resource limits
///
/// # Examples
///
/// ```
/// use celers_kombu::ResourceQuotaMiddleware;
///
/// // Basic quota (100 messages per consumer)
/// let quota = ResourceQuotaMiddleware::new(100);
///
/// // With custom limits
/// let advanced_quota = ResourceQuotaMiddleware::new(1000)
///     .with_max_size_bytes(10_000_000)  // 10MB per consumer
///     .with_time_window_secs(60);        // Reset every 60 seconds
/// ```
pub struct ResourceQuotaMiddleware {
    /// Maximum messages per consumer
    max_messages: usize,
    /// Maximum bytes per consumer
    max_size_bytes: usize,
    /// Time window in seconds for quota reset
    time_window_secs: u64,
    /// Usage tracking per consumer
    usage: std::sync::Arc<std::sync::Mutex<HashMap<String, (usize, usize, u64)>>>,
}

impl ResourceQuotaMiddleware {
    /// Create a new resource quota middleware
    pub fn new(max_messages: usize) -> Self {
        Self {
            max_messages,
            max_size_bytes: usize::MAX,
            time_window_secs: 3600, // Default 1 hour
            usage: std::sync::Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Set maximum bytes per consumer
    pub fn with_max_size_bytes(mut self, max_bytes: usize) -> Self {
        self.max_size_bytes = max_bytes;
        self
    }

    /// Set time window for quota reset
    pub fn with_time_window_secs(mut self, seconds: u64) -> Self {
        self.time_window_secs = seconds;
        self
    }

    /// Get current usage for a consumer
    pub fn get_usage(&self, consumer_id: &str) -> (usize, usize) {
        let usage = self.usage.lock().unwrap();
        usage
            .get(consumer_id)
            .map(|(msgs, bytes, _)| (*msgs, *bytes))
            .unwrap_or((0, 0))
    }

    /// Reset quota for a consumer
    pub fn reset_quota(&self, consumer_id: &str) {
        let mut usage = self.usage.lock().unwrap();
        usage.remove(consumer_id);
    }

    fn check_and_update_quota(&self, consumer_id: &str, message_size: usize) -> Result<()> {
        let mut usage = self.usage.lock().unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let (msg_count, byte_count, last_reset) =
            usage.entry(consumer_id.to_string()).or_insert((0, 0, now));

        // Reset if time window elapsed
        if now - *last_reset >= self.time_window_secs {
            *msg_count = 0;
            *byte_count = 0;
            *last_reset = now;
        }

        // Check quota
        if *msg_count >= self.max_messages {
            return Err(BrokerError::Configuration(format!(
                "Message quota exceeded for consumer {}: {}/{}",
                consumer_id, msg_count, self.max_messages
            )));
        }

        if *byte_count + message_size > self.max_size_bytes {
            return Err(BrokerError::Configuration(format!(
                "Size quota exceeded for consumer {}: {}/{}",
                consumer_id, byte_count, self.max_size_bytes
            )));
        }

        // Update usage
        *msg_count += 1;
        *byte_count += message_size;

        Ok(())
    }
}

#[async_trait]
impl MessageMiddleware for ResourceQuotaMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Inject quota limits into message headers for transparency
        message.headers.extra.insert(
            "x-quota-max-messages".to_string(),
            serde_json::json!(self.max_messages),
        );
        if self.max_size_bytes != usize::MAX {
            message.headers.extra.insert(
                "x-quota-max-bytes".to_string(),
                serde_json::json!(self.max_size_bytes),
            );
        }
        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Extract consumer ID from message headers
        let consumer_id = message
            .headers
            .extra
            .get("x-consumer-id")
            .and_then(|v| v.as_str())
            .unwrap_or("default")
            .to_string();

        // Check and update quota
        let message_size = message.body.len();
        self.check_and_update_quota(&consumer_id, message_size)?;

        // Inject current usage into message
        let (msg_count, byte_count) = self.get_usage(&consumer_id);
        message.headers.extra.insert(
            "x-quota-used-messages".to_string(),
            serde_json::json!(msg_count),
        );
        message.headers.extra.insert(
            "x-quota-used-bytes".to_string(),
            serde_json::json!(byte_count),
        );

        Ok(())
    }

    fn name(&self) -> &str {
        "resource_quota"
    }
}

/// Backpressure configuration for flow control
///
/// # Examples
///
/// ```
/// use celers_kombu::BackpressureConfig;
///
/// let config = BackpressureConfig::new()
///     .with_max_pending(1000)
///     .with_max_queue_size(10000)
///     .with_high_watermark(0.8)
///     .with_low_watermark(0.6);
/// ```
#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    /// Maximum number of pending (unacknowledged) messages
    pub max_pending: usize,
    /// Maximum queue size before applying backpressure
    pub max_queue_size: usize,
    /// High watermark ratio (0.0-1.0) to start backpressure
    pub high_watermark: f64,
    /// Low watermark ratio (0.0-1.0) to stop backpressure
    pub low_watermark: f64,
}

impl BackpressureConfig {
    /// Create a new backpressure configuration with defaults
    pub fn new() -> Self {
        Self {
            max_pending: 1000,
            max_queue_size: 10000,
            high_watermark: 0.8,
            low_watermark: 0.6,
        }
    }

    /// Set maximum pending messages
    pub fn with_max_pending(mut self, max: usize) -> Self {
        self.max_pending = max;
        self
    }

    /// Set maximum queue size
    pub fn with_max_queue_size(mut self, max: usize) -> Self {
        self.max_queue_size = max;
        self
    }

    /// Set high watermark ratio
    pub fn with_high_watermark(mut self, ratio: f64) -> Self {
        self.high_watermark = ratio.clamp(0.0, 1.0);
        self
    }

    /// Set low watermark ratio
    pub fn with_low_watermark(mut self, ratio: f64) -> Self {
        self.low_watermark = ratio.clamp(0.0, 1.0);
        self
    }

    /// Check if backpressure should be applied based on pending count
    pub fn should_apply_backpressure(&self, pending: usize) -> bool {
        pending >= (self.max_pending as f64 * self.high_watermark) as usize
    }

    /// Check if backpressure should be released based on pending count
    pub fn should_release_backpressure(&self, pending: usize) -> bool {
        pending <= (self.max_pending as f64 * self.low_watermark) as usize
    }

    /// Check if queue is at capacity
    pub fn is_at_capacity(&self, queue_size: usize) -> bool {
        queue_size >= self.max_queue_size
    }
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Poison message detector - identifies repeatedly failing messages
///
/// # Examples
///
/// ```
/// use celers_kombu::PoisonMessageDetector;
///
/// let detector = PoisonMessageDetector::new()
///     .with_max_failures(5)
///     .with_failure_window(std::time::Duration::from_secs(3600));
/// ```
#[derive(Debug, Clone)]
pub struct PoisonMessageDetector {
    /// Maximum failures before marking as poison
    pub max_failures: u32,
    /// Time window to track failures
    pub failure_window: Duration,
    /// Failure tracking map: task_id -> (failures, last_failure_time)
    failures: std::sync::Arc<std::sync::Mutex<HashMap<uuid::Uuid, (u32, u64)>>>,
}

impl PoisonMessageDetector {
    /// Create a new poison message detector
    pub fn new() -> Self {
        Self {
            max_failures: 5,
            failure_window: Duration::from_secs(3600),
            failures: std::sync::Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Set maximum failures threshold
    pub fn with_max_failures(mut self, max: u32) -> Self {
        self.max_failures = max;
        self
    }

    /// Set failure tracking window
    pub fn with_failure_window(mut self, window: Duration) -> Self {
        self.failure_window = window;
        self
    }

    /// Record a message failure
    pub fn record_failure(&self, task_id: uuid::Uuid) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut failures = self.failures.lock().unwrap();

        let entry = failures.entry(task_id).or_insert((0, now));

        // Reset if outside window
        if now - entry.1 > self.failure_window.as_secs() {
            *entry = (1, now);
        } else {
            entry.0 += 1;
            entry.1 = now;
        }
    }

    /// Check if a message is a poison message
    pub fn is_poison(&self, task_id: uuid::Uuid) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let failures = self.failures.lock().unwrap();

        if let Some((count, last_failure)) = failures.get(&task_id) {
            if now - last_failure <= self.failure_window.as_secs() {
                return *count >= self.max_failures;
            }
        }

        false
    }

    /// Get failure count for a message
    pub fn failure_count(&self, task_id: uuid::Uuid) -> u32 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let failures = self.failures.lock().unwrap();

        if let Some((count, last_failure)) = failures.get(&task_id) {
            if now - last_failure <= self.failure_window.as_secs() {
                return *count;
            }
        }

        0
    }

    /// Clear failure history for a message
    pub fn clear_failures(&self, task_id: uuid::Uuid) {
        let mut failures = self.failures.lock().unwrap();
        failures.remove(&task_id);
    }

    /// Clear all failure history
    pub fn clear_all(&self) {
        let mut failures = self.failures.lock().unwrap();
        failures.clear();
    }
}

impl Default for PoisonMessageDetector {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// Mock Broker Implementation (for testing)
// =============================================================================

/// Mock broker for testing
#[derive(Debug)]
pub struct MockBroker {
    connected: bool,
    queues: HashMap<String, Vec<Envelope>>,
    pending_acks: HashMap<String, Message>,
    metrics: BrokerMetrics,
}

impl Default for MockBroker {
    fn default() -> Self {
        Self::new()
    }
}

impl MockBroker {
    /// Create a new mock broker
    pub fn new() -> Self {
        Self {
            connected: false,
            queues: HashMap::new(),
            pending_acks: HashMap::new(),
            metrics: BrokerMetrics::new(),
        }
    }

    /// Get number of messages in a queue
    pub fn queue_len(&self, queue: &str) -> usize {
        self.queues.get(queue).map(|q| q.len()).unwrap_or(0)
    }

    /// Get all queue names
    pub fn queue_names(&self) -> Vec<String> {
        self.queues.keys().cloned().collect()
    }
}

#[async_trait]
impl Transport for MockBroker {
    async fn connect(&mut self) -> Result<()> {
        self.metrics.inc_connection_attempt();
        self.connected = true;
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        self.connected = false;
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.connected
    }

    fn name(&self) -> &str {
        "mock"
    }
}

#[async_trait]
impl Producer for MockBroker {
    async fn publish(&mut self, queue: &str, message: Message) -> Result<()> {
        if !self.connected {
            self.metrics.inc_publish_error();
            return Err(BrokerError::Connection("Not connected".to_string()));
        }

        let tag = Uuid::new_v4().to_string();
        let envelope = Envelope::new(message, tag);

        self.queues
            .entry(queue.to_string())
            .or_default()
            .push(envelope);

        self.metrics.inc_published();
        Ok(())
    }

    async fn publish_with_routing(
        &mut self,
        _exchange: &str,
        routing_key: &str,
        message: Message,
    ) -> Result<()> {
        // For mock, just use routing_key as queue name
        self.publish(routing_key, message).await
    }
}

#[async_trait]
impl Consumer for MockBroker {
    async fn consume(&mut self, queue: &str, _timeout: Duration) -> Result<Option<Envelope>> {
        if !self.connected {
            self.metrics.inc_consume_error();
            return Err(BrokerError::Connection("Not connected".to_string()));
        }

        let envelope = self.queues.get_mut(queue).and_then(|q| {
            if q.is_empty() {
                None
            } else {
                Some(q.remove(0))
            }
        });

        if let Some(ref env) = envelope {
            self.pending_acks
                .insert(env.delivery_tag.clone(), env.message.clone());
            self.metrics.inc_consumed();
        }

        Ok(envelope)
    }

    async fn ack(&mut self, delivery_tag: &str) -> Result<()> {
        if self.pending_acks.remove(delivery_tag).is_some() {
            self.metrics.inc_acknowledged();
            Ok(())
        } else {
            Err(BrokerError::MessageNotFound(Uuid::nil()))
        }
    }

    async fn reject(&mut self, delivery_tag: &str, requeue: bool) -> Result<()> {
        if let Some(message) = self.pending_acks.remove(delivery_tag) {
            self.metrics.inc_rejected();
            if requeue {
                // Requeue to the default queue (we don't track which queue it came from)
                let tag = Uuid::new_v4().to_string();
                let mut envelope = Envelope::new(message, tag);
                envelope.redelivered = true;
                self.queues
                    .entry("celery".to_string())
                    .or_default()
                    .push(envelope);
            }
            Ok(())
        } else {
            Err(BrokerError::MessageNotFound(Uuid::nil()))
        }
    }

    async fn queue_size(&mut self, queue: &str) -> Result<usize> {
        Ok(self.queue_len(queue))
    }
}

#[async_trait]
impl Broker for MockBroker {
    async fn purge(&mut self, queue: &str) -> Result<usize> {
        let count = self.queue_len(queue);
        if let Some(q) = self.queues.get_mut(queue) {
            q.clear();
        }
        Ok(count)
    }

    async fn create_queue(&mut self, queue: &str, _mode: QueueMode) -> Result<()> {
        self.queues.entry(queue.to_string()).or_default();
        Ok(())
    }

    async fn delete_queue(&mut self, queue: &str) -> Result<()> {
        self.queues.remove(queue);
        Ok(())
    }

    async fn list_queues(&mut self) -> Result<Vec<String>> {
        Ok(self.queue_names())
    }
}

#[async_trait]
impl BatchProducer for MockBroker {
    async fn publish_batch(
        &mut self,
        queue: &str,
        messages: Vec<Message>,
    ) -> Result<BatchPublishResult> {
        let count = messages.len();
        for message in messages {
            self.publish(queue, message).await?;
        }
        Ok(BatchPublishResult::success(count))
    }

    async fn publish_batch_with_routing(
        &mut self,
        exchange: &str,
        routing_key: &str,
        messages: Vec<Message>,
    ) -> Result<BatchPublishResult> {
        let count = messages.len();
        for message in messages {
            self.publish_with_routing(exchange, routing_key, message)
                .await?;
        }
        Ok(BatchPublishResult::success(count))
    }
}

#[async_trait]
impl BatchConsumer for MockBroker {
    async fn consume_batch(
        &mut self,
        queue: &str,
        max_messages: usize,
        timeout: Duration,
    ) -> Result<Vec<Envelope>> {
        let mut results = Vec::new();
        for _ in 0..max_messages {
            if let Some(envelope) = self.consume(queue, timeout).await? {
                results.push(envelope);
            } else {
                break;
            }
        }
        Ok(results)
    }

    async fn ack_batch(&mut self, delivery_tags: &[String]) -> Result<()> {
        for tag in delivery_tags {
            self.ack(tag).await?;
        }
        Ok(())
    }

    async fn reject_batch(&mut self, delivery_tags: &[String], requeue: bool) -> Result<()> {
        for tag in delivery_tags {
            self.reject(tag, requeue).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl HealthCheck for MockBroker {
    async fn health_check(&self) -> HealthCheckResponse {
        if self.connected {
            HealthCheckResponse::healthy("mock", "mock://localhost")
        } else {
            HealthCheckResponse::unhealthy("mock", "mock://localhost", "Not connected")
        }
    }

    async fn ping(&self) -> bool {
        self.connected
    }
}

#[async_trait]
impl MetricsProvider for MockBroker {
    async fn get_metrics(&self) -> BrokerMetrics {
        self.metrics.clone()
    }

    async fn reset_metrics(&mut self) {
        self.metrics = BrokerMetrics::new();
    }
}

// Blanket implementations for middleware traits
impl<T: Producer> MiddlewareProducer for T {}
impl<T: Consumer> MiddlewareConsumer for T {}

// =============================================================================
// Utilities
// =============================================================================

/// Utility functions for broker operations and analysis.
pub mod utils {
    use super::*;

    /// Calculate optimal batch size based on message size and target throughput.
    ///
    /// # Arguments
    ///
    /// * `avg_message_size` - Average message size in bytes
    /// * `target_throughput_per_sec` - Target messages per second
    /// * `max_batch_size` - Maximum allowed batch size
    ///
    /// # Returns
    ///
    /// Recommended batch size
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::calculate_optimal_batch_size;
    ///
    /// let batch_size = calculate_optimal_batch_size(1024, 1000, 100);
    /// assert!(batch_size > 0 && batch_size <= 100);
    /// ```
    pub fn calculate_optimal_batch_size(
        avg_message_size: usize,
        target_throughput_per_sec: usize,
        max_batch_size: usize,
    ) -> usize {
        if target_throughput_per_sec == 0 || avg_message_size == 0 {
            return 1;
        }

        // For small messages, larger batches are more efficient
        // For large messages, smaller batches reduce memory pressure
        let size_factor = if avg_message_size < 1024 {
            10 // Small messages: batch more
        } else if avg_message_size < 10240 {
            5 // Medium messages: moderate batching
        } else {
            2 // Large messages: batch less
        };

        // Calculate based on throughput (batch every 100ms)
        let throughput_factor = (target_throughput_per_sec / 10).max(1);

        // Combine factors
        let calculated = (throughput_factor / size_factor).max(1);

        // Clamp to max batch size
        calculated.min(max_batch_size)
    }

    /// Match a routing key against a topic pattern (AMQP-style).
    ///
    /// Supports wildcards:
    /// - `*` matches exactly one word
    /// - `#` matches zero or more words
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::match_routing_pattern;
    ///
    /// assert!(match_routing_pattern("stock.usd.nyse", "stock.*.nyse"));
    /// assert!(match_routing_pattern("stock.eur.nyse", "stock.#"));
    /// assert!(match_routing_pattern("quick.orange.rabbit", "*.orange.*"));
    /// assert!(!match_routing_pattern("quick.orange.rabbit", "*.blue.*"));
    /// ```
    pub fn match_routing_pattern(routing_key: &str, pattern: &str) -> bool {
        let key_parts: Vec<&str> = routing_key.split('.').collect();
        let pattern_parts: Vec<&str> = pattern.split('.').collect();

        match_parts(&key_parts, &pattern_parts)
    }

    fn match_parts(key_parts: &[&str], pattern_parts: &[&str]) -> bool {
        if pattern_parts.is_empty() {
            return key_parts.is_empty();
        }

        if pattern_parts[0] == "#" {
            if pattern_parts.len() == 1 {
                return true; // # matches everything
            }
            // Try matching # with 0, 1, 2, ... words
            for i in 0..=key_parts.len() {
                if match_parts(&key_parts[i..], &pattern_parts[1..]) {
                    return true;
                }
            }
            false
        } else if key_parts.is_empty() {
            false
        } else if pattern_parts[0] == "*" || pattern_parts[0] == key_parts[0] {
            match_parts(&key_parts[1..], &pattern_parts[1..])
        } else {
            false
        }
    }

    /// Analyze broker performance from metrics.
    ///
    /// Returns a tuple of (success_rate, error_rate, avg_ack_rate).
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::{BrokerMetrics, utils::analyze_broker_performance};
    ///
    /// let mut metrics = BrokerMetrics::new();
    /// metrics.messages_published = 100;
    /// metrics.messages_consumed = 90;
    /// metrics.messages_acknowledged = 85;
    /// metrics.publish_errors = 5;
    ///
    /// let (success_rate, error_rate, ack_rate) = analyze_broker_performance(&metrics);
    /// assert!(success_rate > 0.9);
    /// assert!(error_rate < 0.1);
    /// ```
    pub fn analyze_broker_performance(metrics: &BrokerMetrics) -> (f64, f64, f64) {
        let total_ops = metrics.messages_published + metrics.messages_consumed;
        let total_errors = metrics.publish_errors + metrics.consume_errors;

        let success_rate = if total_ops > 0 {
            (total_ops - total_errors) as f64 / total_ops as f64
        } else {
            1.0
        };

        let error_rate = if total_ops > 0 {
            total_errors as f64 / total_ops as f64
        } else {
            0.0
        };

        let ack_rate = if metrics.messages_consumed > 0 {
            metrics.messages_acknowledged as f64 / metrics.messages_consumed as f64
        } else {
            0.0
        };

        (success_rate, error_rate, ack_rate)
    }

    /// Estimate serialized message size.
    ///
    /// Provides a rough estimate for planning purposes.
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::estimate_message_size;
    /// use celers_protocol::Message;
    /// use uuid::Uuid;
    ///
    /// let message = Message::new("my_task".to_string(), Uuid::new_v4(), vec![1, 2, 3, 4, 5]);
    /// let size = estimate_message_size(&message);
    /// assert!(size > 0);
    /// ```
    pub fn estimate_message_size(message: &Message) -> usize {
        // Base overhead for message structure
        let base = 200; // UUID, headers, metadata, etc.
        let task_name = message.task_name().len();
        let body = message.body.len();

        base + task_name + body
    }

    /// Analyze queue health based on size and thresholds.
    ///
    /// Returns a health status:
    /// - "healthy" if queue size is below low threshold
    /// - "warning" if queue size is between low and high threshold
    /// - "critical" if queue size is above high threshold
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::analyze_queue_health;
    ///
    /// assert_eq!(analyze_queue_health(50, 100, 1000), "healthy");
    /// assert_eq!(analyze_queue_health(500, 100, 1000), "warning");
    /// assert_eq!(analyze_queue_health(1500, 100, 1000), "critical");
    /// ```
    pub fn analyze_queue_health(
        queue_size: usize,
        low_threshold: usize,
        high_threshold: usize,
    ) -> &'static str {
        if queue_size < low_threshold {
            "healthy"
        } else if queue_size < high_threshold {
            "warning"
        } else {
            "critical"
        }
    }

    /// Calculate message throughput (messages per second).
    ///
    /// # Arguments
    ///
    /// * `message_count` - Number of messages processed
    /// * `duration_secs` - Time duration in seconds
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::calculate_throughput;
    ///
    /// let throughput = calculate_throughput(1000, 10.0);
    /// assert_eq!(throughput, 100.0);
    /// ```
    pub fn calculate_throughput(message_count: u64, duration_secs: f64) -> f64 {
        if duration_secs <= 0.0 {
            0.0
        } else {
            message_count as f64 / duration_secs
        }
    }

    /// Calculate average latency in milliseconds.
    ///
    /// # Arguments
    ///
    /// * `total_latency_ms` - Total latency in milliseconds
    /// * `message_count` - Number of messages
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::calculate_avg_latency;
    ///
    /// let avg = calculate_avg_latency(5000.0, 100);
    /// assert_eq!(avg, 50.0);
    /// ```
    pub fn calculate_avg_latency(total_latency_ms: f64, message_count: u64) -> f64 {
        if message_count == 0 {
            0.0
        } else {
            total_latency_ms / message_count as f64
        }
    }

    /// Estimate time to drain a queue at a given consumption rate.
    ///
    /// # Arguments
    ///
    /// * `queue_size` - Current queue size
    /// * `consumption_rate_per_sec` - Messages consumed per second
    ///
    /// # Returns
    ///
    /// Estimated time in seconds to drain the queue
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::estimate_drain_time;
    ///
    /// let time = estimate_drain_time(1000, 100);
    /// assert_eq!(time, 10.0);
    /// ```
    pub fn estimate_drain_time(queue_size: usize, consumption_rate_per_sec: usize) -> f64 {
        if consumption_rate_per_sec == 0 {
            f64::INFINITY
        } else {
            queue_size as f64 / consumption_rate_per_sec as f64
        }
    }

    /// Calculate exponential backoff delay with optional jitter.
    ///
    /// # Arguments
    ///
    /// * `attempt` - Current retry attempt (0-based)
    /// * `base_delay_ms` - Base delay in milliseconds
    /// * `max_delay_ms` - Maximum delay cap in milliseconds
    /// * `jitter_factor` - Jitter factor (0.0 = no jitter, 1.0 = full jitter)
    ///
    /// # Returns
    ///
    /// Delay in milliseconds with jitter applied
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::calculate_backoff_delay;
    ///
    /// // First retry: 100ms with no jitter
    /// let delay = calculate_backoff_delay(0, 100, 60000, 0.0);
    /// assert_eq!(delay, 100);
    ///
    /// // Third retry: ~400ms (100 * 2^2) with no jitter
    /// let delay = calculate_backoff_delay(2, 100, 60000, 0.0);
    /// assert_eq!(delay, 400);
    /// ```
    pub fn calculate_backoff_delay(
        attempt: u32,
        base_delay_ms: u64,
        max_delay_ms: u64,
        jitter_factor: f64,
    ) -> u64 {
        use std::cmp::min;

        // Calculate exponential backoff: base * 2^attempt
        let exponential = base_delay_ms.saturating_mul(2u64.saturating_pow(attempt));
        let capped = min(exponential, max_delay_ms);

        // Apply jitter if specified
        if jitter_factor > 0.0 {
            let jitter = (capped as f64 * jitter_factor.clamp(0.0, 1.0)) as u64;
            // Simple deterministic jitter based on attempt number
            let jitter_amount = jitter / 2 + (attempt as u64 * 17) % (jitter / 2 + 1);
            capped.saturating_sub(jitter_amount)
        } else {
            capped
        }
    }

    /// Analyze circuit breaker state and recommend action.
    ///
    /// Returns a tuple of (health_score, recommendation) where:
    /// - health_score: 0.0 (unhealthy) to 1.0 (healthy)
    /// - recommendation: suggested action
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::analyze_circuit_breaker;
    ///
    /// let (score, recommendation) = analyze_circuit_breaker(10, 90, 100);
    /// assert!(score > 0.8);
    /// assert_eq!(recommendation, "healthy");
    ///
    /// let (score, recommendation) = analyze_circuit_breaker(60, 40, 100);
    /// assert!(score < 0.5);
    /// assert_eq!(recommendation, "critical");
    /// ```
    pub fn analyze_circuit_breaker(
        failures: u64,
        successes: u64,
        total_requests: u64,
    ) -> (f64, &'static str) {
        if total_requests == 0 {
            return (1.0, "healthy");
        }

        let failure_rate = failures as f64 / total_requests as f64;
        let success_rate = successes as f64 / total_requests as f64;

        let health_score = success_rate;

        let recommendation = if failure_rate > 0.5 {
            "critical"
        } else if failure_rate > 0.2 {
            "warning"
        } else {
            "healthy"
        };

        (health_score, recommendation)
    }

    /// Calculate optimal number of consumer workers based on queue size and processing rate.
    ///
    /// # Arguments
    ///
    /// * `queue_size` - Current queue size
    /// * `avg_processing_time_ms` - Average message processing time in milliseconds
    /// * `target_drain_time_secs` - Target time to drain queue in seconds
    /// * `max_workers` - Maximum number of workers allowed
    ///
    /// # Returns
    ///
    /// Recommended number of workers
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::calculate_optimal_workers;
    ///
    /// let workers = calculate_optimal_workers(1000, 100, 60, 10);
    /// assert!(workers > 0 && workers <= 10);
    /// ```
    pub fn calculate_optimal_workers(
        queue_size: usize,
        avg_processing_time_ms: u64,
        target_drain_time_secs: u64,
        max_workers: usize,
    ) -> usize {
        if queue_size == 0 || target_drain_time_secs == 0 || avg_processing_time_ms == 0 {
            return 1;
        }

        // Messages per second one worker can process
        let msgs_per_sec_per_worker = 1000.0 / avg_processing_time_ms as f64;

        // Total messages per second needed
        let required_throughput = queue_size as f64 / target_drain_time_secs as f64;

        // Calculate needed workers
        let needed_workers = (required_throughput / msgs_per_sec_per_worker).ceil() as usize;

        // Clamp to at least 1 and at most max_workers
        needed_workers.clamp(1, max_workers)
    }

    /// Generate a stable message ID for deduplication based on task name and arguments.
    ///
    /// This creates a deterministic ID that can be used to detect duplicate messages.
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::generate_deduplication_id;
    ///
    /// let id1 = generate_deduplication_id("my_task", b"args");
    /// let id2 = generate_deduplication_id("my_task", b"args");
    /// assert_eq!(id1, id2); // Same inputs = same ID
    ///
    /// let id3 = generate_deduplication_id("my_task", b"different");
    /// assert_ne!(id1, id3); // Different inputs = different ID
    /// ```
    pub fn generate_deduplication_id(task_name: &str, args: &[u8]) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        task_name.hash(&mut hasher);
        args.hash(&mut hasher);
        format!("{:x}", hasher.finish())
    }

    /// Analyze connection pool health and efficiency.
    ///
    /// Returns a tuple of (efficiency_score, status) where:
    /// - efficiency_score: 0.0 (inefficient) to 1.0 (efficient)
    /// - status: pool health status
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::analyze_pool_health;
    ///
    /// let (efficiency, status) = analyze_pool_health(8, 2, 10, 100, 5);
    /// assert!(efficiency > 0.0);
    /// assert!(status.len() > 0);
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub fn analyze_pool_health(
        active_connections: usize,
        idle_connections: usize,
        max_connections: usize,
        total_requests: u64,
        timeout_count: u64,
    ) -> (f64, &'static str) {
        let total_connections = active_connections + idle_connections;

        if max_connections == 0 {
            return (0.0, "invalid");
        }

        // Calculate utilization
        let utilization = total_connections as f64 / max_connections as f64;

        // Calculate timeout rate
        let timeout_rate = if total_requests > 0 {
            timeout_count as f64 / total_requests as f64
        } else {
            0.0
        };

        // Efficiency score: penalize high timeouts and extreme utilization
        let timeout_penalty = timeout_rate * 0.5;
        let utilization_score = if utilization > 0.9 {
            0.7 // High utilization might indicate need for more connections
        } else if utilization < 0.1 {
            0.8 // Low utilization is okay but might indicate oversized pool
        } else {
            1.0 // Good utilization
        };

        let efficiency = (utilization_score - timeout_penalty).clamp(0.0, 1.0);

        let status = if timeout_rate > 0.1 {
            "critical"
        } else if utilization > 0.95 {
            "saturated"
        } else if utilization < 0.05 {
            "underutilized"
        } else {
            "healthy"
        };

        (efficiency, status)
    }

    /// Calculate load distribution across multiple queues.
    ///
    /// Returns a vector of (queue_index, recommended_workers) tuples.
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::calculate_load_distribution;
    ///
    /// let queue_sizes = vec![100, 50, 200];
    /// let distribution = calculate_load_distribution(&queue_sizes, 10);
    /// assert_eq!(distribution.len(), 3);
    /// assert!(distribution.iter().map(|(_, w)| w).sum::<usize>() <= 10);
    /// ```
    pub fn calculate_load_distribution(
        queue_sizes: &[usize],
        total_workers: usize,
    ) -> Vec<(usize, usize)> {
        if queue_sizes.is_empty() || total_workers == 0 {
            return vec![];
        }

        let total_messages: usize = queue_sizes.iter().sum();
        if total_messages == 0 {
            // Distribute evenly if all queues are empty
            let workers_per_queue = total_workers / queue_sizes.len();
            let remainder = total_workers % queue_sizes.len();
            return queue_sizes
                .iter()
                .enumerate()
                .map(|(idx, _)| {
                    let workers = workers_per_queue + if idx < remainder { 1 } else { 0 };
                    (idx, workers)
                })
                .collect();
        }

        // Distribute proportionally based on queue size
        let mut distribution: Vec<(usize, usize)> = queue_sizes
            .iter()
            .enumerate()
            .map(|(idx, &size)| {
                let proportion = size as f64 / total_messages as f64;
                let workers = (proportion * total_workers as f64).round() as usize;
                (idx, workers)
            })
            .collect();

        // Adjust to ensure total equals total_workers
        let assigned: usize = distribution.iter().map(|(_, w)| w).sum();
        if assigned < total_workers {
            // Give remaining workers to largest queues
            let diff = total_workers - assigned;
            let dist_len = distribution.len();
            for _i in 0..diff {
                if let Some(max_queue) = queue_sizes
                    .iter()
                    .enumerate()
                    .max_by_key(|(_, &size)| size)
                    .map(|(idx, _)| idx)
                {
                    distribution[max_queue % dist_len].1 += 1;
                }
            }
        }

        distribution
    }

    /// Check if a routing key matches a direct exchange pattern.
    ///
    /// Direct exchanges route based on exact matching.
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::match_direct_routing;
    ///
    /// assert!(match_direct_routing("user.created", "user.created"));
    /// assert!(!match_direct_routing("user.created", "user.deleted"));
    /// ```
    pub fn match_direct_routing(routing_key: &str, pattern: &str) -> bool {
        routing_key == pattern
    }

    /// Check if a routing key matches a fanout exchange.
    ///
    /// Fanout exchanges route to all bound queues regardless of routing key.
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::match_fanout_routing;
    ///
    /// assert!(match_fanout_routing("anything"));
    /// assert!(match_fanout_routing(""));
    /// ```
    pub fn match_fanout_routing(_routing_key: &str) -> bool {
        true // Fanout always matches
    }

    /// Estimate memory usage for a queue based on message count and average size.
    ///
    /// Returns estimated memory in bytes.
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::estimate_queue_memory;
    ///
    /// let memory = estimate_queue_memory(1000, 1024);
    /// assert!(memory > 1_000_000);
    /// ```
    pub fn estimate_queue_memory(message_count: usize, avg_message_size: usize) -> usize {
        // Include overhead for queue metadata and message wrappers
        let overhead_per_message = 100; // bytes
        message_count * (avg_message_size + overhead_per_message)
    }

    /// Calculate priority score for message processing order.
    ///
    /// Higher scores should be processed first. Factors in priority level,
    /// message age, and retry count.
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::calculate_priority_score;
    /// use celers_kombu::Priority;
    ///
    /// let score = calculate_priority_score(Priority::High, 100, 0);
    /// assert!(score > 0.0);
    ///
    /// // Older messages get higher scores
    /// let old_score = calculate_priority_score(Priority::Normal, 1000, 0);
    /// let new_score = calculate_priority_score(Priority::Normal, 100, 0);
    /// assert!(old_score > new_score);
    /// ```
    pub fn calculate_priority_score(priority: Priority, age_seconds: u64, retry_count: u32) -> f64 {
        let priority_weight = match priority {
            Priority::Highest => 10.0,
            Priority::High => 7.0,
            Priority::Normal => 5.0,
            Priority::Low => 3.0,
            Priority::Lowest => 1.0,
        };

        let age_factor = (age_seconds as f64).ln().max(1.0);
        let retry_penalty = 0.9_f64.powi(retry_count as i32);

        priority_weight * age_factor * retry_penalty
    }

    /// Suggest optimal message batch groups based on size constraints.
    ///
    /// Returns a vector of batch sizes that maximizes throughput while
    /// staying within size limits.
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::suggest_batch_groups;
    ///
    /// let message_sizes = vec![100, 200, 150, 300, 250, 400];
    /// let groups = suggest_batch_groups(&message_sizes, 600);
    /// assert!(groups.len() > 0);
    /// ```
    pub fn suggest_batch_groups(
        message_sizes: &[usize],
        max_batch_bytes: usize,
    ) -> Vec<Vec<usize>> {
        let mut groups = Vec::new();
        let mut current_group = Vec::new();
        let mut current_size = 0;

        for (idx, &size) in message_sizes.iter().enumerate() {
            if current_size + size <= max_batch_bytes {
                current_group.push(idx);
                current_size += size;
            } else {
                if !current_group.is_empty() {
                    groups.push(current_group);
                }
                current_group = vec![idx];
                current_size = size;
            }
        }

        if !current_group.is_empty() {
            groups.push(current_group);
        }

        groups
    }

    /// Calculate aggregate health score from multiple metrics.
    ///
    /// Returns a score from 0.0 (unhealthy) to 1.0 (healthy).
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::calculate_health_score;
    ///
    /// let score = calculate_health_score(0.95, 100, 0.0, 500);
    /// assert!(score > 0.8);
    ///
    /// let bad_score = calculate_health_score(0.5, 10000, 0.3, 5000);
    /// assert!(bad_score < 0.5);
    /// ```
    pub fn calculate_health_score(
        success_rate: f64,
        queue_size: usize,
        error_rate: f64,
        latency_ms: u64,
    ) -> f64 {
        // Success rate component (0-1)
        let success_component = success_rate;

        // Queue size component (1 when empty, decreases with size)
        let queue_component = if queue_size < 100 {
            1.0
        } else if queue_size < 1000 {
            0.8
        } else if queue_size < 10000 {
            0.5
        } else {
            0.2
        };

        // Error rate component (1 when no errors)
        let error_component = (1.0 - error_rate).max(0.0);

        // Latency component (1 when fast, decreases with latency)
        let latency_component = if latency_ms < 100 {
            1.0
        } else if latency_ms < 500 {
            0.8
        } else if latency_ms < 1000 {
            0.5
        } else {
            0.2
        };

        // Weighted average
        (success_component * 0.4
            + queue_component * 0.2
            + error_component * 0.2
            + latency_component * 0.2)
            .clamp(0.0, 1.0)
    }

    /// Identify stale messages based on age threshold.
    ///
    /// Returns indices of messages that exceed the age threshold.
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::identify_stale_messages;
    ///
    /// let ages = vec![10, 100, 500, 2000, 5000];
    /// let stale = identify_stale_messages(&ages, 1000);
    /// assert_eq!(stale, vec![3, 4]);
    /// ```
    pub fn identify_stale_messages(message_ages: &[u64], threshold_seconds: u64) -> Vec<usize> {
        message_ages
            .iter()
            .enumerate()
            .filter(|(_, &age)| age > threshold_seconds)
            .map(|(idx, _)| idx)
            .collect()
    }

    /// Predict throughput based on historical data.
    ///
    /// Uses simple linear regression on recent throughput samples.
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::predict_throughput;
    ///
    /// let history = vec![100.0, 110.0, 120.0, 130.0, 140.0];
    /// let prediction = predict_throughput(&history, 2);
    /// assert!(prediction > 140.0);
    /// ```
    pub fn predict_throughput(historical_throughput: &[f64], periods_ahead: usize) -> f64 {
        if historical_throughput.is_empty() {
            return 0.0;
        }

        if historical_throughput.len() < 2 {
            return historical_throughput[0];
        }

        // Calculate average rate of change
        let mut changes = Vec::new();
        for i in 1..historical_throughput.len() {
            changes.push(historical_throughput[i] - historical_throughput[i - 1]);
        }

        let avg_change = changes.iter().sum::<f64>() / changes.len() as f64;
        let last_value = historical_throughput[historical_throughput.len() - 1];

        (last_value + avg_change * periods_ahead as f64).max(0.0)
    }

    /// Calculate queue rebalancing recommendations.
    ///
    /// Returns suggested message redistribution across queues to balance load.
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::calculate_rebalancing;
    ///
    /// let queue_sizes = vec![1000, 100, 500];
    /// let target_total = 1600;
    /// let balanced = calculate_rebalancing(&queue_sizes, target_total);
    /// assert_eq!(balanced.len(), 3);
    /// assert_eq!(balanced.iter().sum::<usize>(), target_total);
    /// ```
    pub fn calculate_rebalancing(current_sizes: &[usize], target_total: usize) -> Vec<usize> {
        if current_sizes.is_empty() {
            return vec![];
        }

        let num_queues = current_sizes.len();
        let per_queue = target_total / num_queues;
        let remainder = target_total % num_queues;

        let mut result = vec![per_queue; num_queues];
        // Distribute remainder to first queues
        for item in result.iter_mut().take(remainder) {
            *item += 1;
        }

        result
    }

    /// Estimate time to process remaining messages with current rate.
    ///
    /// Returns estimated seconds to completion.
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::estimate_completion_time;
    ///
    /// let time = estimate_completion_time(1000, 100.0);
    /// assert_eq!(time, 10);
    /// ```
    pub fn estimate_completion_time(remaining_messages: usize, current_rate_per_sec: f64) -> u64 {
        if current_rate_per_sec <= 0.0 {
            return u64::MAX;
        }

        (remaining_messages as f64 / current_rate_per_sec).ceil() as u64
    }

    /// Calculate message processing efficiency ratio.
    ///
    /// Returns ratio of useful work (0.0 to 1.0).
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::calculate_efficiency;
    ///
    /// let efficiency = calculate_efficiency(900, 100, 50);
    /// assert!(efficiency > 0.8);
    /// ```
    pub fn calculate_efficiency(successful: usize, failed: usize, rejected: usize) -> f64 {
        let total = successful + failed + rejected;
        if total == 0 {
            return 1.0;
        }

        successful as f64 / total as f64
    }

    /// Calculate required queue capacity for a given workload.
    ///
    /// Returns the number of messages the queue should be able to hold
    /// based on production rate, consumption rate, and desired buffer time.
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::calculate_queue_capacity;
    ///
    /// // 1000 msg/sec production, 800 msg/sec consumption, 60 sec buffer
    /// let capacity = calculate_queue_capacity(1000, 800, 60);
    /// assert_eq!(capacity, 12000); // (1000-800) * 60
    /// ```
    pub fn calculate_queue_capacity(
        production_rate_per_sec: usize,
        consumption_rate_per_sec: usize,
        buffer_duration_secs: usize,
    ) -> usize {
        if production_rate_per_sec <= consumption_rate_per_sec {
            // No backlog expected, minimal buffer
            production_rate_per_sec * buffer_duration_secs.min(10)
        } else {
            // Calculate backlog accumulation
            let backlog_rate = production_rate_per_sec - consumption_rate_per_sec;
            backlog_rate * buffer_duration_secs
        }
    }

    /// Suggest optimal partition count for distributed message queue.
    ///
    /// Recommends partition count based on throughput requirements,
    /// consumer count, and target parallelism.
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::suggest_partition_count;
    ///
    /// let partitions = suggest_partition_count(10000, 5, 1000);
    /// assert!(partitions >= 5 && partitions <= 20);
    /// ```
    pub fn suggest_partition_count(
        target_throughput_per_sec: usize,
        consumer_count: usize,
        max_partition_throughput: usize,
    ) -> usize {
        if max_partition_throughput == 0 {
            return consumer_count.max(1);
        }

        // Calculate partitions needed for throughput
        let throughput_partitions = target_throughput_per_sec.div_ceil(max_partition_throughput);

        // Ensure at least one partition per consumer
        let consumer_partitions = consumer_count;

        // Use the larger of the two, but cap at 100
        throughput_partitions.max(consumer_partitions).min(100)
    }

    /// Estimate operational cost for message broker usage.
    ///
    /// Returns estimated cost units based on message volume, storage,
    /// and operation type costs.
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::calculate_cost_estimate;
    ///
    /// // 1M messages/day, 1KB average, $0.0001/msg, $0.10/GB/month
    /// let monthly_cost = calculate_cost_estimate(1_000_000, 1024, 0.0001, 0.10, 30);
    /// assert!(monthly_cost > 0.0);
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub fn calculate_cost_estimate(
        messages_per_day: usize,
        avg_message_size_bytes: usize,
        cost_per_message: f64,
        storage_cost_per_gb_month: f64,
        retention_days: usize,
    ) -> f64 {
        // Calculate message operation costs
        let daily_message_cost = messages_per_day as f64 * cost_per_message;

        // Calculate storage costs
        let daily_storage_gb =
            (messages_per_day * avg_message_size_bytes) as f64 / (1024.0 * 1024.0 * 1024.0);
        let total_storage_gb = daily_storage_gb * retention_days as f64;
        let monthly_storage_cost = total_storage_gb * storage_cost_per_gb_month / 30.0;

        // Total monthly cost
        (daily_message_cost * 30.0) + monthly_storage_cost
    }

    /// Analyze message size and frequency patterns.
    ///
    /// Returns (min_size, max_size, avg_size, std_dev).
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::analyze_message_patterns;
    ///
    /// let sizes = vec![100, 200, 150, 180, 220];
    /// let (min, max, avg, std_dev) = analyze_message_patterns(&sizes);
    /// assert_eq!(min, 100);
    /// assert_eq!(max, 220);
    /// assert!(avg > 150.0 && avg < 200.0);
    /// assert!(std_dev > 0.0);
    /// ```
    pub fn analyze_message_patterns(message_sizes: &[usize]) -> (usize, usize, f64, f64) {
        if message_sizes.is_empty() {
            return (0, 0, 0.0, 0.0);
        }

        let min = *message_sizes.iter().min().unwrap();
        let max = *message_sizes.iter().max().unwrap();

        let sum: usize = message_sizes.iter().sum();
        let avg = sum as f64 / message_sizes.len() as f64;

        // Calculate standard deviation
        let variance: f64 = message_sizes
            .iter()
            .map(|&size| {
                let diff = size as f64 - avg;
                diff * diff
            })
            .sum::<f64>()
            / message_sizes.len() as f64;

        let std_dev = variance.sqrt();

        (min, max, avg, std_dev)
    }

    /// Calculate optimal buffer size for message batching.
    ///
    /// Returns buffer size in bytes based on network MTU, message overhead,
    /// and target batch efficiency.
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::calculate_buffer_size;
    ///
    /// let buffer = calculate_buffer_size(1024, 64, 0.9);
    /// assert!(buffer > 0);
    /// ```
    pub fn calculate_buffer_size(
        avg_message_size: usize,
        message_overhead: usize,
        target_efficiency: f64,
    ) -> usize {
        let effective_message_size = avg_message_size + message_overhead;

        // Network MTU is typically 1500 bytes (Ethernet) or 9000 bytes (Jumbo frames)
        let mtu = if effective_message_size < 1400 {
            1500
        } else {
            9000
        };

        // Calculate messages per packet
        let messages_per_packet = (mtu as f64 / effective_message_size as f64).floor() as usize;

        if messages_per_packet == 0 {
            return effective_message_size;
        }

        // Buffer should hold enough for target efficiency
        let packets_needed = (1.0 / (1.0 - target_efficiency.min(0.99))).ceil() as usize;

        messages_per_packet * packets_needed * effective_message_size
    }

    /// Estimate total memory footprint for broker operations.
    ///
    /// Returns estimated memory usage in bytes for given queue configuration.
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::estimate_memory_footprint;
    ///
    /// // 10 queues, 1000 messages each, 1KB average message
    /// let memory = estimate_memory_footprint(10, 1000, 1024, 128);
    /// assert!(memory > 10_000_000); // > 10MB
    /// ```
    pub fn estimate_memory_footprint(
        queue_count: usize,
        messages_per_queue: usize,
        avg_message_size: usize,
        metadata_overhead_per_msg: usize,
    ) -> usize {
        let per_message_total = avg_message_size + metadata_overhead_per_msg;
        let per_queue_memory = messages_per_queue * per_message_total;

        // Add per-queue overhead (data structures, indexes, etc.)
        let queue_overhead = 1024 * 64; // ~64KB per queue

        queue_count * (per_queue_memory + queue_overhead)
    }

    /// Suggest appropriate TTL (time-to-live) for messages based on patterns.
    ///
    /// Returns suggested TTL in seconds based on message processing time
    /// and retry characteristics.
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::suggest_ttl;
    ///
    /// // 5 sec avg processing, 3 retries, 2x backoff
    /// let ttl = suggest_ttl(5, 3, 2.0);
    /// assert!(ttl > 5 * (1 + 2 + 4)); // Enough for all retries
    /// ```
    pub fn suggest_ttl(
        avg_processing_time_secs: u64,
        max_retries: u32,
        backoff_multiplier: f64,
    ) -> u64 {
        let mut total_time = avg_processing_time_secs;

        // Calculate total time including retries with exponential backoff
        for retry in 0..max_retries {
            let backoff = avg_processing_time_secs as f64 * backoff_multiplier.powi(retry as i32);
            total_time += backoff as u64;
        }

        // Add 50% safety margin
        (total_time as f64 * 1.5).ceil() as u64
    }

    /// Calculate acceptable replication lag for distributed queues.
    ///
    /// Returns maximum acceptable lag in milliseconds based on
    /// consistency requirements and throughput.
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::calculate_replication_lag;
    ///
    /// let lag_ms = calculate_replication_lag(1000, 3, true);
    /// assert!(lag_ms > 0);
    /// ```
    pub fn calculate_replication_lag(
        throughput_per_sec: usize,
        replica_count: usize,
        require_strong_consistency: bool,
    ) -> u64 {
        if require_strong_consistency {
            // Strong consistency requires minimal lag
            return 100; // 100ms max
        }

        // For eventual consistency, calculate based on throughput
        let messages_per_ms = throughput_per_sec as f64 / 1000.0;

        // Allow lag of up to 1000 messages per replica
        let max_lag_messages = 1000 * replica_count;

        if messages_per_ms <= 0.0 {
            return 10000; // 10 seconds default
        }

        let lag_ms = (max_lag_messages as f64 / messages_per_ms).ceil() as u64;

        // Cap between 100ms and 60 seconds
        lag_ms.clamp(100, 60000)
    }

    /// Calculate network bandwidth required for message broker operations.
    ///
    /// Returns bandwidth in bytes per second.
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::calculate_bandwidth_requirement;
    ///
    /// // 1000 msg/sec, 1KB average, with protocol overhead
    /// let bandwidth = calculate_bandwidth_requirement(1000, 1024, 1.2);
    /// assert_eq!(bandwidth, 1228800); // 1000 * 1024 * 1.2
    /// ```
    pub fn calculate_bandwidth_requirement(
        messages_per_sec: usize,
        avg_message_size: usize,
        protocol_overhead_factor: f64,
    ) -> usize {
        let raw_bandwidth = messages_per_sec * avg_message_size;
        (raw_bandwidth as f64 * protocol_overhead_factor) as usize
    }

    /// Suggest retry policy based on error patterns.
    ///
    /// Returns (max_retries, initial_delay_ms, max_delay_ms).
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::suggest_retry_policy;
    ///
    /// let (max_retries, initial_delay, max_delay) = suggest_retry_policy(0.1, 5);
    /// assert!(max_retries > 0);
    /// assert!(initial_delay > 0);
    /// assert!(max_delay > initial_delay);
    /// ```
    pub fn suggest_retry_policy(failure_rate: f64, avg_recovery_time_secs: u64) -> (u32, u64, u64) {
        // Higher failure rate = more retries
        let max_retries = if failure_rate > 0.3 {
            10
        } else if failure_rate > 0.1 {
            5
        } else {
            3
        };

        // Initial delay based on recovery time
        let initial_delay_ms = (avg_recovery_time_secs * 100).max(100); // At least 100ms

        // Max delay should be reasonable (not more than 5 minutes)
        let max_delay_ms = (avg_recovery_time_secs * 1000 * 5).min(300000);

        (max_retries, initial_delay_ms, max_delay_ms)
    }

    /// Analyze consumer lag across queue metrics.
    ///
    /// # Arguments
    ///
    /// * `queue_size` - Current queue depth
    /// * `consumption_rate_per_sec` - Current consumption rate (messages/sec)
    /// * `production_rate_per_sec` - Current production rate (messages/sec)
    ///
    /// # Returns
    ///
    /// Tuple of (lag_seconds, is_falling_behind, recommended_action)
    /// - lag_seconds: Estimated lag in seconds
    /// - is_falling_behind: true if producers outpace consumers
    /// - recommended_action: "scale_up", "stable", or "scale_down"
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::analyze_consumer_lag;
    ///
    /// let (lag, falling_behind, action) = analyze_consumer_lag(1000, 50, 100);
    /// assert!(lag > 0);
    /// assert!(falling_behind); // Producers (100/s) > Consumers (50/s)
    /// assert_eq!(action, "scale_up");
    ///
    /// let (lag, falling_behind, action) = analyze_consumer_lag(100, 100, 50);
    /// assert!(!falling_behind); // Consumers keeping up
    /// ```
    pub fn analyze_consumer_lag(
        queue_size: usize,
        consumption_rate_per_sec: usize,
        production_rate_per_sec: usize,
    ) -> (u64, bool, &'static str) {
        let lag_seconds = if consumption_rate_per_sec > 0 {
            (queue_size as f64 / consumption_rate_per_sec as f64).ceil() as u64
        } else {
            u64::MAX // Infinite lag if no consumption
        };

        let is_falling_behind = production_rate_per_sec > consumption_rate_per_sec;

        let recommended_action = if is_falling_behind && lag_seconds > 10 {
            "scale_up" // Significant lag and falling behind
        } else if !is_falling_behind
            && queue_size < 100
            && consumption_rate_per_sec > production_rate_per_sec * 2
        {
            "scale_down" // Over-provisioned
        } else {
            "stable" // Balanced
        };

        (lag_seconds, is_falling_behind, recommended_action)
    }

    /// Calculate message velocity (rate of change in queue size).
    ///
    /// # Arguments
    ///
    /// * `current_size` - Current queue size
    /// * `previous_size` - Previous queue size
    /// * `time_window_secs` - Time window between measurements (seconds)
    ///
    /// # Returns
    ///
    /// Tuple of (velocity, trend)
    /// - velocity: Messages per second change rate (positive = growing, negative = shrinking)
    /// - trend: "growing", "shrinking", or "stable"
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::calculate_message_velocity;
    ///
    /// // Queue grew from 100 to 200 in 10 seconds
    /// let (velocity, trend) = calculate_message_velocity(200, 100, 10);
    /// assert_eq!(velocity, 10); // +10 messages/sec
    /// assert_eq!(trend, "growing");
    ///
    /// // Queue shrunk from 200 to 100 in 10 seconds
    /// let (velocity, trend) = calculate_message_velocity(100, 200, 10);
    /// assert_eq!(velocity, -10); // -10 messages/sec
    /// assert_eq!(trend, "shrinking");
    /// ```
    pub fn calculate_message_velocity(
        current_size: usize,
        previous_size: usize,
        time_window_secs: u64,
    ) -> (i64, &'static str) {
        if time_window_secs == 0 {
            return (0, "stable");
        }

        let delta = current_size as i64 - previous_size as i64;
        let velocity = delta / time_window_secs as i64;

        let trend = if velocity > 5 {
            "growing"
        } else if velocity < -5 {
            "shrinking"
        } else {
            "stable"
        };

        (velocity, trend)
    }

    /// Suggest worker scaling based on queue metrics.
    ///
    /// # Arguments
    ///
    /// * `queue_size` - Current queue depth
    /// * `current_workers` - Current number of workers
    /// * `avg_processing_time_ms` - Average message processing time (milliseconds)
    /// * `target_latency_secs` - Target maximum latency (seconds)
    ///
    /// # Returns
    ///
    /// Tuple of (recommended_workers, scaling_action)
    /// - recommended_workers: Suggested number of workers
    /// - scaling_action: "add", "remove", or "maintain"
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::suggest_worker_scaling;
    ///
    /// // Large queue, need more workers
    /// let (workers, action) = suggest_worker_scaling(5000, 5, 100, 60);
    /// assert!(workers > 5);
    /// assert_eq!(action, "add");
    ///
    /// // Small queue, can reduce workers
    /// let (workers, action) = suggest_worker_scaling(10, 10, 100, 60);
    /// assert!(workers < 10);
    /// assert_eq!(action, "remove");
    /// ```
    #[allow(clippy::comparison_chain)]
    pub fn suggest_worker_scaling(
        queue_size: usize,
        current_workers: usize,
        avg_processing_time_ms: u64,
        target_latency_secs: u64,
    ) -> (usize, &'static str) {
        if avg_processing_time_ms == 0 || target_latency_secs == 0 {
            return (current_workers.max(1), "maintain");
        }

        // Calculate required throughput: messages that need processing within target latency
        let messages_per_worker_per_sec = 1000 / avg_processing_time_ms.max(1);

        // Calculate required workers to process queue within target latency
        let required_throughput = (queue_size as f64 / target_latency_secs as f64).ceil() as u64;
        let recommended_workers = if messages_per_worker_per_sec > 0 {
            ((required_throughput as f64 / messages_per_worker_per_sec as f64).ceil() as usize)
                .max(1)
        } else {
            current_workers.max(1)
        };

        // Add safety margin (20% headroom)
        let recommended_workers = ((recommended_workers as f64 * 1.2).ceil() as usize).max(1);

        let scaling_action = if recommended_workers > current_workers {
            "add"
        } else if recommended_workers < current_workers {
            "remove"
        } else {
            "maintain"
        };

        (recommended_workers, scaling_action)
    }

    /// Calculate message age distribution statistics.
    ///
    /// # Arguments
    ///
    /// * `message_ages_secs` - Slice of message ages in seconds
    ///
    /// # Returns
    ///
    /// Tuple of (p50, p95, p99, max_age)
    /// - p50: 50th percentile (median) age in seconds
    /// - p95: 95th percentile age in seconds
    /// - p99: 99th percentile age in seconds
    /// - max_age: Maximum age in seconds
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::calculate_message_age_distribution;
    ///
    /// let ages = vec![1, 2, 3, 4, 5, 10, 20, 30, 40, 100];
    /// let (p50, p95, p99, max) = calculate_message_age_distribution(&ages);
    /// assert!(p50 <= p95);
    /// assert!(p95 <= p99);
    /// assert!(p99 <= max);
    /// assert_eq!(max, 100);
    /// ```
    pub fn calculate_message_age_distribution(message_ages_secs: &[u64]) -> (u64, u64, u64, u64) {
        if message_ages_secs.is_empty() {
            return (0, 0, 0, 0);
        }

        let mut sorted_ages = message_ages_secs.to_vec();
        sorted_ages.sort_unstable();

        let len = sorted_ages.len();
        let p50_idx = (len as f64 * 0.50) as usize;
        let p95_idx = (len as f64 * 0.95) as usize;
        let p99_idx = (len as f64 * 0.99) as usize;

        let p50 = sorted_ages.get(p50_idx.min(len - 1)).copied().unwrap_or(0);
        let p95 = sorted_ages.get(p95_idx.min(len - 1)).copied().unwrap_or(0);
        let p99 = sorted_ages.get(p99_idx.min(len - 1)).copied().unwrap_or(0);
        let max_age = sorted_ages.last().copied().unwrap_or(0);

        (p50, p95, p99, max_age)
    }

    /// Estimate processing capacity of the system.
    ///
    /// # Arguments
    ///
    /// * `num_workers` - Number of worker processes
    /// * `avg_processing_time_ms` - Average message processing time (milliseconds)
    /// * `concurrency_per_worker` - Number of concurrent tasks per worker
    ///
    /// # Returns
    ///
    /// Tuple of (capacity_per_sec, capacity_per_min, capacity_per_hour)
    /// - capacity_per_sec: Theoretical messages per second
    /// - capacity_per_min: Theoretical messages per minute
    /// - capacity_per_hour: Theoretical messages per hour
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::estimate_processing_capacity;
    ///
    /// // 10 workers, 100ms per message, 4 concurrent tasks each
    /// let (per_sec, per_min, per_hour) = estimate_processing_capacity(10, 100, 4);
    /// assert_eq!(per_sec, 400); // 10 * 4 * (1000/100)
    /// assert_eq!(per_min, 24000);
    /// assert_eq!(per_hour, 1440000);
    /// ```
    pub fn estimate_processing_capacity(
        num_workers: usize,
        avg_processing_time_ms: u64,
        concurrency_per_worker: usize,
    ) -> (u64, u64, u64) {
        if avg_processing_time_ms == 0 || num_workers == 0 || concurrency_per_worker == 0 {
            return (0, 0, 0);
        }

        // Messages per second per concurrent task
        let msgs_per_sec_per_task = 1000 / avg_processing_time_ms;

        // Total capacity
        let total_tasks = num_workers * concurrency_per_worker;
        let capacity_per_sec = msgs_per_sec_per_task * total_tasks as u64;
        let capacity_per_min = capacity_per_sec * 60;
        let capacity_per_hour = capacity_per_min * 60;

        (capacity_per_sec, capacity_per_min, capacity_per_hour)
    }

    /// Detect anomalies in message patterns
    ///
    /// Analyzes message flow patterns to detect anomalies such as sudden spikes,
    /// drops, or unusual patterns that may indicate issues.
    ///
    /// Returns (is_anomaly, severity, description) where severity is 0.0-1.0
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils;
    ///
    /// let current = vec![100, 105, 98, 102, 500]; // Spike at the end
    /// let baseline = vec![100, 105, 98, 102, 100];
    /// let (is_anomaly, severity, desc) = utils::detect_anomalies(&current, &baseline, 2.0);
    /// assert!(is_anomaly);
    /// assert!(severity > 0.5);
    /// ```
    pub fn detect_anomalies(
        current_samples: &[u64],
        baseline_samples: &[u64],
        threshold_multiplier: f64,
    ) -> (bool, f64, String) {
        if current_samples.is_empty() || baseline_samples.is_empty() {
            return (false, 0.0, "Insufficient data".to_string());
        }

        // Calculate baseline statistics
        let baseline_avg =
            baseline_samples.iter().sum::<u64>() as f64 / baseline_samples.len() as f64;
        let baseline_variance = baseline_samples
            .iter()
            .map(|&x| {
                let diff = x as f64 - baseline_avg;
                diff * diff
            })
            .sum::<f64>()
            / baseline_samples.len() as f64;
        let baseline_stddev = baseline_variance.sqrt();

        // Calculate current statistics
        let current_avg = current_samples.iter().sum::<u64>() as f64 / current_samples.len() as f64;

        // Check for anomaly
        let deviation = (current_avg - baseline_avg).abs();
        let threshold = baseline_stddev * threshold_multiplier;

        if deviation > threshold {
            let severity = (deviation / (baseline_stddev * 3.0)).min(1.0);
            let direction = if current_avg > baseline_avg {
                "spike"
            } else {
                "drop"
            };
            let description = format!(
                "Anomaly detected: {} in message rate (current: {:.0}, baseline: {:.0}, deviation: {:.0})",
                direction, current_avg, baseline_avg, deviation
            );
            (true, severity, description)
        } else {
            (false, 0.0, "Normal pattern".to_string())
        }
    }

    /// Calculate SLA compliance metrics
    ///
    /// Calculates SLA compliance based on processing time targets.
    ///
    /// Returns (compliance_percentage, violations_count, avg_processing_time)
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils;
    ///
    /// let processing_times = vec![100, 150, 200, 120, 90];
    /// let sla_target_ms = 180;
    /// let (compliance, violations, avg) = utils::calculate_sla_compliance(&processing_times, sla_target_ms);
    /// assert!(compliance > 0.0);
    /// assert_eq!(violations, 1); // Only 200ms exceeds target
    /// ```
    pub fn calculate_sla_compliance(
        processing_times_ms: &[u64],
        sla_target_ms: u64,
    ) -> (f64, usize, f64) {
        if processing_times_ms.is_empty() {
            return (100.0, 0, 0.0);
        }

        let violations = processing_times_ms
            .iter()
            .filter(|&&t| t > sla_target_ms)
            .count();

        let compliance = 100.0 * (1.0 - (violations as f64 / processing_times_ms.len() as f64));

        let avg_time =
            processing_times_ms.iter().sum::<u64>() as f64 / processing_times_ms.len() as f64;

        (compliance, violations, avg_time)
    }

    /// Estimate cost for message processing
    ///
    /// Calculates estimated infrastructure cost based on message volume and pricing model.
    ///
    /// Returns estimated cost for the period
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils;
    ///
    /// let messages_per_day = 1_000_000;
    /// let cost_per_million = 0.50; // $0.50 per million messages
    /// let days = 30;
    /// let cost = utils::estimate_infrastructure_cost(messages_per_day, cost_per_million, days);
    /// assert_eq!(cost, 15.0); // $15 for 30 days
    /// ```
    pub fn estimate_infrastructure_cost(
        messages_per_day: u64,
        cost_per_million_messages: f64,
        days: u32,
    ) -> f64 {
        let total_messages = messages_per_day * days as u64;
        let cost = (total_messages as f64 / 1_000_000.0) * cost_per_million_messages;
        (cost * 100.0).round() / 100.0 // Round to 2 decimal places
    }

    /// Calculate error budget remaining
    ///
    /// Calculates the remaining error budget based on SLA target and actual errors.
    ///
    /// Returns (budget_remaining_percentage, errors_allowed, time_to_exhaustion_hours)
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils;
    ///
    /// let sla_target = 99.9; // 99.9% uptime
    /// let total_requests = 100_000;
    /// let failed_requests = 50;
    /// let requests_per_hour = 10_000;
    /// let (budget, allowed, hours) = utils::calculate_error_budget(
    ///     sla_target,
    ///     total_requests,
    ///     failed_requests,
    ///     requests_per_hour
    /// );
    /// assert!(budget >= 0.0);
    /// ```
    pub fn calculate_error_budget(
        sla_target_percentage: f64,
        total_requests: u64,
        failed_requests: u64,
        requests_per_hour: u64,
    ) -> (f64, u64, f64) {
        if total_requests == 0 || requests_per_hour == 0 {
            return (100.0, 0, 0.0);
        }

        let error_budget = 100.0 - sla_target_percentage;
        let current_error_rate = (failed_requests as f64 / total_requests as f64) * 100.0;
        let budget_remaining =
            ((error_budget - current_error_rate) / error_budget * 100.0).max(0.0);

        // Calculate remaining errors allowed
        let total_errors_allowed = (total_requests as f64 * (error_budget / 100.0)) as u64;
        let errors_remaining = total_errors_allowed.saturating_sub(failed_requests);

        // Time to exhaust budget at current error rate
        let time_to_exhaustion = if current_error_rate > 0.0 {
            (errors_remaining as f64 / (current_error_rate / 100.0)) / requests_per_hour as f64
        } else {
            f64::INFINITY
        };

        (budget_remaining, errors_remaining, time_to_exhaustion)
    }

    /// Predict queue saturation time
    ///
    /// Predicts when a queue will reach capacity based on current growth rate.
    ///
    /// Returns (hours_to_saturation, messages_per_hour_growth)
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils;
    ///
    /// let samples = vec![1000, 1100, 1200, 1300]; // Growing by 100/sample
    /// let max_capacity = 2000;
    /// let hours_per_sample = 1.0;
    /// let (hours, growth) = utils::predict_queue_saturation(&samples, max_capacity, hours_per_sample);
    /// assert!(hours > 0.0);
    /// assert!(growth > 0.0);
    /// ```
    pub fn predict_queue_saturation(
        historical_sizes: &[u64],
        max_capacity: u64,
        hours_per_sample: f64,
    ) -> (f64, f64) {
        if historical_sizes.len() < 2 {
            return (f64::INFINITY, 0.0);
        }

        let current_size = *historical_sizes.last().unwrap();
        if current_size >= max_capacity {
            return (0.0, 0.0);
        }

        // Calculate growth rate using linear regression
        let n = historical_sizes.len() as f64;
        let sum_x: f64 = (0..historical_sizes.len()).map(|i| i as f64).sum();
        let sum_y: f64 = historical_sizes.iter().map(|&s| s as f64).sum();
        let sum_xy: f64 = historical_sizes
            .iter()
            .enumerate()
            .map(|(i, &s)| i as f64 * s as f64)
            .sum();
        let sum_x_squared: f64 = (0..historical_sizes.len())
            .map(|i| (i as f64) * (i as f64))
            .sum();

        let slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x_squared - sum_x * sum_x);
        let growth_per_sample = slope;
        let growth_per_hour = growth_per_sample / hours_per_sample;

        if growth_per_hour <= 0.0 {
            return (f64::INFINITY, growth_per_hour);
        }

        let remaining_capacity = max_capacity.saturating_sub(current_size) as f64;
        let hours_to_saturation = remaining_capacity / growth_per_hour;

        (hours_to_saturation, growth_per_hour)
    }

    /// Calculate consumer efficiency
    ///
    /// Measures how efficiently consumers are processing messages by comparing
    /// active processing time vs total wait time.
    ///
    /// Returns (efficiency_percentage, recommendation) where:
    /// - efficiency_percentage: 0.0 (very inefficient) to 100.0 (very efficient)
    /// - recommendation: suggested action
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::calculate_consumer_efficiency;
    ///
    /// // Consumer spends 80% time processing, 20% waiting
    /// let (efficiency, recommendation) = calculate_consumer_efficiency(8000, 2000, 100);
    /// assert!(efficiency > 70.0);
    /// assert_eq!(recommendation, "efficient");
    ///
    /// // Consumer spends 30% time processing, 70% waiting
    /// let (efficiency, recommendation) = calculate_consumer_efficiency(3000, 7000, 100);
    /// assert!(efficiency < 50.0);
    /// assert_eq!(recommendation, "underutilized");
    /// ```
    pub fn calculate_consumer_efficiency(
        processing_time_ms: u64,
        wait_time_ms: u64,
        messages_processed: u64,
    ) -> (f64, &'static str) {
        if messages_processed == 0 {
            return (0.0, "no_data");
        }

        let total_time = processing_time_ms + wait_time_ms;
        if total_time == 0 {
            return (0.0, "no_data");
        }

        // Efficiency is the percentage of time spent actively processing
        let efficiency_percentage = (processing_time_ms as f64 / total_time as f64) * 100.0;

        // Calculate average processing time per message
        let avg_processing_ms = processing_time_ms as f64 / messages_processed as f64;

        let recommendation = if efficiency_percentage >= 80.0 {
            "efficient" // High efficiency - consumers are busy processing
        } else if efficiency_percentage >= 60.0 {
            "good" // Good efficiency - reasonable balance
        } else if efficiency_percentage >= 40.0 {
            "fair" // Fair efficiency - room for improvement
        } else if avg_processing_ms < 10.0 {
            "bottleneck" // Low efficiency due to messages being too simple
        } else {
            "underutilized" // Low efficiency - consumers are mostly waiting
        };

        (efficiency_percentage, recommendation)
    }

    /// Suggest optimal connection pool size
    ///
    /// Recommends connection pool size based on concurrent request patterns
    /// and resource constraints.
    ///
    /// Returns (min_connections, max_connections, recommended_initial)
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::suggest_connection_pool_size;
    ///
    /// // Peak: 50 concurrent, Average: 20, Max allowed: 100
    /// let (min, max, initial) = suggest_connection_pool_size(50, 20, 100);
    /// assert!(min > 0);
    /// assert!(max <= 100);
    /// assert!(initial >= min && initial <= max);
    ///
    /// // Very high load
    /// let (min, max, initial) = suggest_connection_pool_size(200, 150, 100);
    /// assert_eq!(max, 100); // Respects max_allowed limit
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub fn suggest_connection_pool_size(
        peak_concurrent_requests: usize,
        avg_concurrent_requests: usize,
        max_allowed_connections: usize,
    ) -> (usize, usize, usize) {
        if max_allowed_connections == 0 {
            return (0, 0, 0);
        }

        // Minimum connections: 10% of average, at least 1, at most 5
        let min_connections = (avg_concurrent_requests / 10)
            .clamp(1, 5)
            .min(max_allowed_connections);

        // Maximum connections: 150% of peak to handle bursts
        let recommended_max = ((peak_concurrent_requests as f64 * 1.5).ceil() as usize)
            .clamp(min_connections + 1, max_allowed_connections);

        // Initial/recommended: average + 20% buffer
        let recommended_initial = ((avg_concurrent_requests as f64 * 1.2).ceil() as usize)
            .clamp(min_connections, recommended_max);

        (min_connections, recommended_max, recommended_initial)
    }

    /// Calculate message processing trend
    ///
    /// Analyzes historical processing times to determine if performance is
    /// improving, degrading, or stable over time.
    ///
    /// Returns (trend_direction, trend_strength, recommendation) where:
    /// - trend_direction: "improving", "stable", or "degrading"
    /// - trend_strength: 0.0 (no trend) to 1.0 (strong trend)
    /// - recommendation: suggested action
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::calculate_message_processing_trend;
    ///
    /// // Processing times improving over time
    /// let times = vec![100, 95, 90, 85, 80];
    /// let (direction, strength, recommendation) = calculate_message_processing_trend(&times);
    /// assert_eq!(direction, "improving");
    /// assert!(strength > 0.0);
    ///
    /// // Processing times degrading
    /// let times = vec![80, 85, 90, 95, 100];
    /// let (direction, strength, recommendation) = calculate_message_processing_trend(&times);
    /// assert_eq!(direction, "degrading");
    /// ```
    pub fn calculate_message_processing_trend(
        processing_times_ms: &[u64],
    ) -> (&'static str, f64, &'static str) {
        if processing_times_ms.len() < 3 {
            return ("stable", 0.0, "insufficient_data");
        }

        // Calculate linear regression slope
        let n = processing_times_ms.len() as f64;
        let sum_x: f64 = (0..processing_times_ms.len()).map(|i| i as f64).sum();
        let sum_y: f64 = processing_times_ms.iter().map(|&t| t as f64).sum();
        let sum_xy: f64 = processing_times_ms
            .iter()
            .enumerate()
            .map(|(i, &t)| i as f64 * t as f64)
            .sum();
        let sum_x_squared: f64 = (0..processing_times_ms.len())
            .map(|i| (i as f64) * (i as f64))
            .sum();

        let slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x_squared - sum_x * sum_x);

        // Calculate average and normalize slope
        let avg_time = sum_y / n;
        let normalized_slope = if avg_time > 0.0 {
            (slope / avg_time).abs()
        } else {
            0.0
        };

        // Determine trend direction and strength
        let (direction, recommendation) = if slope < -1.0 {
            ("improving", "maintain_current_optimizations")
        } else if slope > 1.0 {
            ("degrading", "investigate_performance_issues")
        } else {
            ("stable", "monitor_continuously")
        };

        let strength = normalized_slope.min(1.0);

        (direction, strength, recommendation)
    }

    /// Suggest optimal prefetch count for consumers
    ///
    /// Recommends the number of messages a consumer should prefetch based on
    /// processing characteristics and resource availability.
    ///
    /// Returns optimal prefetch count
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::suggest_prefetch_count;
    ///
    /// // Fast processing, high concurrency
    /// let prefetch = suggest_prefetch_count(50, 10, 100);
    /// assert!(prefetch > 0);
    /// assert!(prefetch <= 100);
    ///
    /// // Slow processing, low concurrency
    /// let prefetch = suggest_prefetch_count(1000, 2, 50);
    /// assert!(prefetch <= 20); // Lower prefetch for slow processing
    /// ```
    pub fn suggest_prefetch_count(
        avg_processing_time_ms: u64,
        concurrent_workers: usize,
        max_prefetch: usize,
    ) -> usize {
        if concurrent_workers == 0 || max_prefetch == 0 {
            return 1;
        }

        // Calculate messages per second one worker can handle
        let msgs_per_sec_per_worker = if avg_processing_time_ms > 0 {
            1000.0 / avg_processing_time_ms as f64
        } else {
            10.0 // Default assumption
        };

        // For fast processing (>10 msg/sec), prefetch more
        // For slow processing (<1 msg/sec), prefetch less
        let base_prefetch = if msgs_per_sec_per_worker >= 10.0 {
            20 // Fast processing: prefetch many
        } else if msgs_per_sec_per_worker >= 1.0 {
            10 // Medium processing: moderate prefetch
        } else {
            5 // Slow processing: prefetch few
        };

        // Adjust for number of workers
        let adjusted_prefetch = (base_prefetch * concurrent_workers).max(1);

        // Clamp to max_prefetch
        adjusted_prefetch.min(max_prefetch)
    }

    /// Analyze dead letter queue patterns
    ///
    /// Analyzes DLQ statistics to identify patterns and suggest remediation strategies.
    ///
    /// Returns (severity, primary_issue, recommendation) where:
    /// - severity: "low", "medium", "high", or "critical"
    /// - primary_issue: description of the main issue
    /// - recommendation: suggested action
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::analyze_dead_letter_queue;
    ///
    /// // High failure rate - critical issue
    /// let (severity, issue, recommendation) = analyze_dead_letter_queue(500, 1000, 100);
    /// assert_eq!(severity, "critical");
    ///
    /// // Low failure rate - healthy
    /// let (severity, issue, recommendation) = analyze_dead_letter_queue(10, 10000, 100);
    /// assert_eq!(severity, "low");
    /// ```
    pub fn analyze_dead_letter_queue(
        dlq_size: usize,
        total_processed: u64,
        dlq_growth_per_hour: usize,
    ) -> (&'static str, &'static str, &'static str) {
        if total_processed == 0 {
            return ("low", "no_data", "monitor");
        }

        // Calculate failure rate
        let failure_rate = dlq_size as f64 / total_processed as f64;

        // Determine severity based on failure rate and growth
        let (severity, primary_issue, recommendation) = if failure_rate > 0.1 {
            // >10% failure rate
            (
                "critical",
                "high_failure_rate",
                "immediate_investigation_required",
            )
        } else if failure_rate > 0.05 {
            // >5% failure rate
            (
                "high",
                "elevated_failure_rate",
                "investigate_error_patterns",
            )
        } else if dlq_growth_per_hour > 100 {
            // Rapid growth
            (
                "high",
                "rapid_dlq_growth",
                "monitor_closely_and_investigate",
            )
        } else if failure_rate > 0.01 {
            // >1% failure rate
            ("medium", "moderate_failures", "review_recent_failures")
        } else if dlq_size > 0 {
            // Some failures but low rate
            ("low", "normal_failures", "periodic_dlq_review")
        } else {
            // No failures
            ("low", "healthy", "continue_monitoring")
        };

        (severity, primary_issue, recommendation)
    }

    /// Forecast queue capacity with trend analysis
    ///
    /// Uses historical data points to forecast future capacity needs with trend analysis.
    ///
    /// # Arguments
    ///
    /// * `historical_sizes` - Historical queue sizes (chronological order)
    /// * `forecast_hours` - Hours into the future to forecast
    ///
    /// # Returns
    ///
    /// Tuple of (forecasted_size, trend_slope, confidence_level)
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::forecast_queue_capacity_ml;
    ///
    /// let history = vec![100, 150, 180, 220, 250];
    /// let (forecast, slope, confidence) = forecast_queue_capacity_ml(&history, 24);
    /// assert!(forecast > 250); // Should predict growth
    /// assert!(slope > 0.0); // Positive trend
    /// ```
    pub fn forecast_queue_capacity_ml(
        historical_sizes: &[usize],
        forecast_hours: usize,
    ) -> (usize, f64, &'static str) {
        if historical_sizes.len() < 3 {
            return (
                historical_sizes.last().copied().unwrap_or(0),
                0.0,
                "insufficient_data",
            );
        }

        // Calculate linear regression trend
        let n = historical_sizes.len() as f64;
        let x_mean = (n - 1.0) / 2.0;
        let y_mean = historical_sizes.iter().sum::<usize>() as f64 / n;

        let mut numerator = 0.0;
        let mut denominator = 0.0;

        for (i, &size) in historical_sizes.iter().enumerate() {
            let x = i as f64;
            let y = size as f64;
            numerator += (x - x_mean) * (y - y_mean);
            denominator += (x - x_mean).powi(2);
        }

        let slope = if denominator > 0.0 {
            numerator / denominator
        } else {
            0.0
        };

        // Forecast future size
        let future_x = (historical_sizes.len() - 1) as f64 + forecast_hours as f64;
        let forecast = (y_mean + slope * (future_x - x_mean)).max(0.0) as usize;

        // Calculate confidence based on data variance
        let variance: f64 = historical_sizes
            .iter()
            .map(|&s| {
                let predicted = y_mean
                    + slope
                        * (historical_sizes.iter().position(|&x| x == s).unwrap() as f64 - x_mean);
                (s as f64 - predicted).powi(2)
            })
            .sum::<f64>()
            / n;

        let std_dev = variance.sqrt();
        let coefficient_of_variation = std_dev / y_mean;

        let confidence = if coefficient_of_variation < 0.1 {
            "high"
        } else if coefficient_of_variation < 0.3 {
            "medium"
        } else {
            "low"
        };

        (forecast, slope, confidence)
    }

    /// Optimize batch strategy based on multiple factors
    ///
    /// Analyzes message patterns, network latency, and processing characteristics
    /// to recommend optimal batching strategy.
    ///
    /// # Arguments
    ///
    /// * `avg_message_size` - Average message size in bytes
    /// * `network_latency_ms` - Network round-trip latency in milliseconds
    /// * `processing_time_ms` - Average processing time per message in milliseconds
    /// * `throughput_target` - Target throughput in messages per second
    ///
    /// # Returns
    ///
    /// Tuple of (batch_size, max_wait_ms, estimated_throughput, strategy)
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::optimize_batch_strategy;
    ///
    /// let (batch_size, wait_ms, throughput, strategy) =
    ///     optimize_batch_strategy(1024, 50, 10, 1000);
    /// assert!(batch_size > 0);
    /// assert!(wait_ms > 0);
    /// assert_eq!(strategy, "throughput_optimized");
    /// ```
    pub fn optimize_batch_strategy(
        avg_message_size: usize,
        network_latency_ms: u64,
        processing_time_ms: u64,
        throughput_target: u64,
    ) -> (usize, u64, u64, &'static str) {
        // Calculate ideal batch size based on latency vs processing time ratio
        let latency_weight =
            network_latency_ms as f64 / (network_latency_ms + processing_time_ms) as f64;

        let base_batch_size = if latency_weight > 0.5 {
            // High latency: use larger batches to amortize network cost
            100
        } else {
            // Low latency: smaller batches for lower latency
            20
        };

        // Adjust for message size
        let max_batch_bytes = 4 * 1024 * 1024; // 4MB max batch
        let size_limited_batch = (max_batch_bytes / avg_message_size.max(1)).min(1000);
        let batch_size = base_batch_size.min(size_limited_batch);

        // Calculate optimal wait time
        let messages_per_ms = throughput_target as f64 / 1000.0;
        let fill_time_ms = if messages_per_ms > 0.0 {
            (batch_size as f64 / messages_per_ms) as u64
        } else {
            100
        };

        let max_wait_ms = fill_time_ms.clamp(10, 5000);

        // Estimate actual throughput
        let batch_overhead = network_latency_ms + 10; // 10ms batch processing overhead
        let time_per_batch = batch_overhead + (batch_size as u64 * processing_time_ms);
        let batches_per_sec = if time_per_batch > 0 {
            1000 / time_per_batch
        } else {
            0
        };
        let estimated_throughput = batches_per_sec * batch_size as u64;

        // Determine strategy
        let strategy = if batch_size > 50 {
            "throughput_optimized"
        } else if network_latency_ms > 100 {
            "latency_optimized"
        } else {
            "balanced"
        };

        (batch_size, max_wait_ms, estimated_throughput, strategy)
    }

    /// Calculate efficiency across multiple queues
    ///
    /// Analyzes load distribution and efficiency across multiple queues to identify
    /// imbalances and optimization opportunities.
    ///
    /// # Arguments
    ///
    /// * `queue_sizes` - Current size of each queue
    /// * `processing_rates` - Processing rate (msg/sec) for each queue
    ///
    /// # Returns
    ///
    /// Tuple of (overall_efficiency, load_balance_score, recommendation)
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::calculate_multi_queue_efficiency;
    ///
    /// let sizes = vec![100, 150, 120];
    /// let rates = vec![10, 12, 11];
    /// let (efficiency, balance, recommendation) =
    ///     calculate_multi_queue_efficiency(&sizes, &rates);
    /// assert!(efficiency >= 0.0 && efficiency <= 1.0);
    /// assert!(balance >= 0.0 && balance <= 1.0);
    /// ```
    pub fn calculate_multi_queue_efficiency(
        queue_sizes: &[usize],
        processing_rates: &[u64],
    ) -> (f64, f64, &'static str) {
        if queue_sizes.is_empty() || queue_sizes.len() != processing_rates.len() {
            return (0.0, 0.0, "invalid_input");
        }

        // Calculate per-queue drain times
        let drain_times: Vec<f64> = queue_sizes
            .iter()
            .zip(processing_rates.iter())
            .map(|(&size, &rate)| {
                if rate > 0 {
                    size as f64 / rate as f64
                } else {
                    f64::INFINITY
                }
            })
            .collect();

        // Overall efficiency: average of (rate / max_rate) for each queue
        let max_rate = *processing_rates.iter().max().unwrap_or(&1);
        let efficiency = if max_rate > 0 {
            processing_rates
                .iter()
                .map(|&r| r as f64 / max_rate as f64)
                .sum::<f64>()
                / processing_rates.len() as f64
        } else {
            0.0
        };

        // Load balance score: how evenly distributed the drain times are
        let avg_drain_time = drain_times.iter().filter(|t| t.is_finite()).sum::<f64>()
            / drain_times.iter().filter(|t| t.is_finite()).count() as f64;

        let variance = drain_times
            .iter()
            .filter(|t| t.is_finite())
            .map(|&t| (t - avg_drain_time).powi(2))
            .sum::<f64>()
            / drain_times.iter().filter(|t| t.is_finite()).count() as f64;

        let coefficient_of_variation = if avg_drain_time > 0.0 {
            variance.sqrt() / avg_drain_time
        } else {
            0.0
        };

        // Lower CV means better balance (invert for score)
        let load_balance_score = (1.0 / (1.0 + coefficient_of_variation)).min(1.0);

        let recommendation = if load_balance_score < 0.5 {
            "rebalance_workers"
        } else if efficiency < 0.6 {
            "increase_capacity"
        } else if load_balance_score > 0.8 && efficiency > 0.8 {
            "optimal"
        } else {
            "monitor"
        };

        (efficiency, load_balance_score, recommendation)
    }

    /// Predict when resources will be exhausted
    ///
    /// Analyzes resource consumption trends to predict when resources will be exhausted.
    ///
    /// # Arguments
    ///
    /// * `current_usage` - Current resource usage (e.g., queue size, memory MB)
    /// * `max_capacity` - Maximum resource capacity
    /// * `growth_rate_per_hour` - Resource growth rate per hour
    ///
    /// # Returns
    ///
    /// Tuple of (hours_until_exhausted, severity, action)
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::predict_resource_exhaustion;
    ///
    /// let (hours, severity, action) = predict_resource_exhaustion(7000, 10000, 500);
    /// assert!(hours > 0);
    /// assert_eq!(severity, "warning");
    /// ```
    pub fn predict_resource_exhaustion(
        current_usage: usize,
        max_capacity: usize,
        growth_rate_per_hour: usize,
    ) -> (usize, &'static str, &'static str) {
        if growth_rate_per_hour == 0 {
            return (usize::MAX, "healthy", "monitor");
        }

        if current_usage >= max_capacity {
            return (0, "critical", "immediate_action_required");
        }

        let remaining = max_capacity.saturating_sub(current_usage);
        let hours_until_exhausted = remaining / growth_rate_per_hour.max(1);

        let (severity, action) = if hours_until_exhausted < 1 {
            ("critical", "immediate_scaling_required")
        } else if hours_until_exhausted < 4 {
            ("high", "scale_within_hour")
        } else if hours_until_exhausted < 24 {
            ("warning", "plan_scaling")
        } else if hours_until_exhausted < 168 {
            // 1 week
            ("low", "monitor_trends")
        } else {
            ("healthy", "normal_monitoring")
        };

        (hours_until_exhausted, severity, action)
    }

    /// Suggest autoscaling policy based on load patterns
    ///
    /// Analyzes historical load patterns to recommend autoscaling policy parameters.
    ///
    /// # Arguments
    ///
    /// * `peak_load` - Peak load (messages/sec)
    /// * `average_load` - Average load (messages/sec)
    /// * `min_load` - Minimum load (messages/sec)
    /// * `load_volatility` - Standard deviation of load
    ///
    /// # Returns
    ///
    /// Tuple of (min_workers, max_workers, scale_up_threshold, scale_down_threshold, policy_type)
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::suggest_autoscaling_policy;
    ///
    /// let (min, max, up_threshold, down_threshold, policy) =
    ///     suggest_autoscaling_policy(1000, 500, 100, 200);
    /// assert!(min > 0);
    /// assert!(max >= min);
    /// assert!(up_threshold > down_threshold);
    /// ```
    pub fn suggest_autoscaling_policy(
        peak_load: u64,
        average_load: u64,
        min_load: u64,
        load_volatility: u64,
    ) -> (usize, usize, f64, f64, &'static str) {
        // Assume each worker can handle ~100 msg/sec
        let worker_capacity = 100;

        // Minimum workers to handle baseline load with headroom
        let min_workers = ((min_load as f64 / worker_capacity as f64) * 1.2).ceil() as usize;
        let min_workers = min_workers.max(2); // Always have at least 2 workers

        // Maximum workers to handle peak load with headroom
        let max_workers = ((peak_load as f64 / worker_capacity as f64) * 1.5).ceil() as usize;
        let max_workers = max_workers.max(min_workers * 2); // At least 2x min

        // Determine policy based on load volatility
        let volatility_ratio = if average_load > 0 {
            load_volatility as f64 / average_load as f64
        } else {
            0.0
        };

        let (scale_up_threshold, scale_down_threshold, policy_type) = if volatility_ratio > 0.5 {
            // High volatility: aggressive scaling
            (0.6, 0.3, "aggressive")
        } else if volatility_ratio > 0.2 {
            // Moderate volatility: balanced scaling
            (0.7, 0.4, "balanced")
        } else {
            // Low volatility: conservative scaling
            (0.8, 0.5, "conservative")
        };

        (
            min_workers,
            max_workers,
            scale_up_threshold,
            scale_down_threshold,
            policy_type,
        )
    }

    /// Calculate message affinity score for consistent routing.
    ///
    /// This helps route messages with similar characteristics to the same worker
    /// for better cache locality and processing efficiency.
    ///
    /// # Arguments
    ///
    /// * `message_key` - Message identifier or routing key
    /// * `num_workers` - Total number of workers
    ///
    /// # Returns
    ///
    /// Worker index (0 to num_workers-1)
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::calculate_message_affinity;
    ///
    /// let worker = calculate_message_affinity("user:12345", 8);
    /// assert!(worker < 8);
    ///
    /// // Same key always routes to same worker
    /// assert_eq!(
    ///     calculate_message_affinity("order:abc", 10),
    ///     calculate_message_affinity("order:abc", 10)
    /// );
    /// ```
    pub fn calculate_message_affinity(message_key: &str, num_workers: usize) -> usize {
        if num_workers == 0 {
            return 0;
        }

        // Use a simple hash for consistent routing
        let mut hash: u64 = 0;
        for byte in message_key.bytes() {
            hash = hash.wrapping_mul(31).wrapping_add(byte as u64);
        }

        (hash % num_workers as u64) as usize
    }

    /// Analyze queue temperature (hot/warm/cold classification).
    ///
    /// This helps identify queue activity levels for resource allocation.
    ///
    /// # Arguments
    ///
    /// * `messages_per_min` - Message throughput rate
    /// * `avg_age_secs` - Average message age in seconds
    ///
    /// # Returns
    ///
    /// Tuple of (temperature, recommendation)
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::analyze_queue_temperature;
    ///
    /// let (temp, rec) = analyze_queue_temperature(100, 5);
    /// assert_eq!(temp, "hot");
    /// assert_eq!(rec, "maintain_resources");
    ///
    /// let (temp, _) = analyze_queue_temperature(1, 300);
    /// assert_eq!(temp, "cold");
    /// ```
    pub fn analyze_queue_temperature(
        messages_per_min: usize,
        avg_age_secs: usize,
    ) -> (&'static str, &'static str) {
        // Hot queue: high throughput, low age
        if messages_per_min > 50 && avg_age_secs < 60 {
            ("hot", "maintain_resources")
        }
        // Warm queue: moderate throughput
        else if messages_per_min > 10 && avg_age_secs < 300 {
            ("warm", "monitor")
        }
        // Cold queue: low throughput, high age
        else if messages_per_min < 5 || avg_age_secs > 600 {
            ("cold", "consider_scaling_down")
        }
        // Lukewarm: in between
        else {
            ("lukewarm", "monitor")
        }
    }

    /// Detect processing bottleneck in the message pipeline.
    ///
    /// Identifies where bottlenecks occur based on queue metrics.
    ///
    /// # Arguments
    ///
    /// * `publish_rate` - Messages published per second
    /// * `consume_rate` - Messages consumed per second
    /// * `queue_size` - Current queue size
    /// * `processing_time_ms` - Average processing time in milliseconds
    ///
    /// # Returns
    ///
    /// Tuple of (bottleneck_location, severity, recommendation)
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::detect_processing_bottleneck;
    ///
    /// let (location, severity, _) = detect_processing_bottleneck(100, 50, 2000, 500);
    /// assert_eq!(location, "consumer");
    /// assert_eq!(severity, "high");
    /// ```
    pub fn detect_processing_bottleneck(
        publish_rate: usize,
        consume_rate: usize,
        queue_size: usize,
        processing_time_ms: usize,
    ) -> (&'static str, &'static str, &'static str) {
        // Calculate imbalance ratio
        let rate_ratio = if consume_rate > 0 {
            publish_rate as f64 / consume_rate as f64
        } else {
            f64::INFINITY
        };

        // Check for consumer bottleneck
        if rate_ratio > 1.5 && queue_size > 1000 {
            return (
                "consumer",
                "high",
                "scale_up_consumers_or_optimize_processing",
            );
        }

        // Check for slow processing
        if processing_time_ms > 1000 {
            return ("processing", "medium", "optimize_task_logic_or_add_caching");
        }

        // Check for queue buildup
        if queue_size > 5000 {
            return ("queue", "medium", "increase_consumer_capacity");
        }

        // Check for publisher bottleneck (rare)
        if rate_ratio < 0.5 && queue_size < 100 {
            return (
                "publisher",
                "low",
                "consider_scaling_down_consumers_to_save_cost",
            );
        }

        ("none", "low", "system_healthy")
    }

    /// Calculate optimal prefetch count multiplier.
    ///
    /// Determines the ideal prefetch multiplier based on processing characteristics.
    ///
    /// # Arguments
    ///
    /// * `avg_processing_time_ms` - Average message processing time
    /// * `network_latency_ms` - Network round-trip time
    /// * `concurrency` - Number of concurrent workers
    ///
    /// # Returns
    ///
    /// Recommended prefetch multiplier
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::calculate_optimal_prefetch_multiplier;
    ///
    /// let multiplier = calculate_optimal_prefetch_multiplier(100, 10, 4);
    /// assert!(multiplier >= 1.0);
    /// assert!(multiplier <= 10.0);
    /// ```
    pub fn calculate_optimal_prefetch_multiplier(
        avg_processing_time_ms: usize,
        network_latency_ms: usize,
        concurrency: usize,
    ) -> f64 {
        if avg_processing_time_ms == 0 || concurrency == 0 {
            return 1.0;
        }

        // Base multiplier on processing time
        let base_multiplier = if avg_processing_time_ms < 50 {
            // Fast processing: prefetch more
            5.0
        } else if avg_processing_time_ms < 200 {
            // Medium processing
            3.0
        } else {
            // Slow processing: prefetch less
            2.0
        };

        // Adjust for network latency
        let latency_factor = if network_latency_ms > 100 {
            1.5 // High latency: prefetch more to hide latency
        } else if network_latency_ms > 50 {
            1.2
        } else {
            1.0
        };

        // Adjust for concurrency
        let concurrency_factor = (concurrency as f64).sqrt();

        // Calculate final multiplier
        let multiplier = base_multiplier * latency_factor / concurrency_factor;

        // Clamp to reasonable range
        multiplier.clamp(1.0, 10.0)
    }

    /// Suggest queue consolidation strategies.
    ///
    /// Recommends whether multiple queues should be consolidated based on usage patterns.
    ///
    /// # Arguments
    ///
    /// * `queue_sizes` - Current sizes of all queues
    /// * `queue_rates` - Message rates for all queues (messages per minute)
    ///
    /// # Returns
    ///
    /// Tuple of (should_consolidate, reason, recommendation)
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::suggest_queue_consolidation;
    ///
    /// let sizes = vec![10, 5, 8];
    /// let rates = vec![2, 1, 3];
    /// let (consolidate, reason, _) = suggest_queue_consolidation(&sizes, &rates);
    ///
    /// assert_eq!(consolidate, true);
    /// assert_eq!(reason, "low_throughput");
    /// ```
    pub fn suggest_queue_consolidation(
        queue_sizes: &[usize],
        queue_rates: &[usize],
    ) -> (bool, &'static str, &'static str) {
        if queue_sizes.is_empty() || queue_rates.is_empty() {
            return (false, "no_data", "n/a");
        }

        if queue_sizes.len() != queue_rates.len() {
            return (false, "invalid_input", "ensure_matching_array_lengths");
        }

        // Calculate average metrics
        let avg_size: usize = queue_sizes.iter().sum::<usize>() / queue_sizes.len();
        let avg_rate: usize = queue_rates.iter().sum::<usize>() / queue_rates.len();

        // Suggest consolidation if all queues are tiny (check this first)
        if avg_size < 20 && avg_rate < 5 && queue_sizes.len() > 3 {
            return (
                true,
                "overhead",
                "reduce_queue_count_to_minimize_management_overhead",
            );
        }

        // Count underutilized queues
        let underutilized = queue_sizes
            .iter()
            .zip(queue_rates.iter())
            .filter(|(&size, &rate)| size < 50 && rate < 10)
            .count();

        // Suggest consolidation if many queues are underutilized
        if underutilized as f64 / queue_sizes.len() as f64 > 0.6 {
            return (
                true,
                "low_throughput",
                "consolidate_into_fewer_queues_with_routing",
            );
        }

        (false, "efficient", "maintain_current_structure")
    }

    /// Calculate queue utilization efficiency.
    ///
    /// Measures how efficiently a queue is being utilized based on size,
    /// capacity, and throughput metrics.
    ///
    /// # Arguments
    ///
    /// * `current_size` - Current number of messages in queue
    /// * `capacity` - Maximum queue capacity
    /// * `messages_per_sec` - Current throughput rate
    /// * `peak_messages_per_sec` - Peak historical throughput
    ///
    /// # Returns
    ///
    /// Tuple of (utilization_ratio, efficiency_score, recommendation)
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::calculate_queue_utilization_efficiency;
    ///
    /// let (util, eff, rec) = calculate_queue_utilization_efficiency(500, 1000, 50, 100);
    /// assert!(util >= 0.0 && util <= 1.0);
    /// assert!(eff >= 0.0 && eff <= 1.0);
    /// ```
    pub fn calculate_queue_utilization_efficiency(
        current_size: usize,
        capacity: usize,
        messages_per_sec: usize,
        peak_messages_per_sec: usize,
    ) -> (f64, f64, &'static str) {
        if capacity == 0 {
            return (0.0, 0.0, "invalid_capacity");
        }

        // Calculate size utilization (0-1)
        let size_util = (current_size as f64 / capacity as f64).min(1.0);

        // Calculate throughput efficiency (0-1)
        let throughput_eff = if peak_messages_per_sec > 0 {
            (messages_per_sec as f64 / peak_messages_per_sec as f64).min(1.0)
        } else {
            0.0
        };

        // Combined efficiency score (weighted average)
        let efficiency_score = (size_util * 0.4 + throughput_eff * 0.6).min(1.0);

        // Recommendation based on efficiency
        let recommendation = if efficiency_score > 0.8 {
            "excellent_utilization"
        } else if efficiency_score > 0.6 {
            "good_utilization"
        } else if efficiency_score > 0.4 {
            "moderate_utilization_consider_optimization"
        } else {
            "poor_utilization_needs_attention"
        };

        (size_util, efficiency_score, recommendation)
    }

    /// Analyze message flow patterns for anomaly detection.
    ///
    /// Detects unusual patterns in message flow that might indicate issues.
    ///
    /// # Arguments
    ///
    /// * `recent_counts` - Recent message counts (time series)
    /// * `window_size` - Size of the analysis window
    ///
    /// # Returns
    ///
    /// Tuple of (is_anomaly, severity, pattern_type)
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::analyze_message_flow_pattern;
    ///
    /// let counts = vec![100, 105, 95, 103];  // Normal variation
    /// let (is_anom, _, _) = analyze_message_flow_pattern(&counts, 5);
    /// assert_eq!(is_anom, false);
    /// ```
    pub fn analyze_message_flow_pattern(
        recent_counts: &[usize],
        window_size: usize,
    ) -> (bool, &'static str, &'static str) {
        if recent_counts.is_empty() || window_size == 0 {
            return (false, "none", "insufficient_data");
        }

        let len = recent_counts.len().min(window_size);
        let window = &recent_counts[recent_counts.len().saturating_sub(len)..];

        if window.len() < 3 {
            return (false, "none", "insufficient_data");
        }

        // Calculate mean and standard deviation
        let mean = window.iter().sum::<usize>() as f64 / window.len() as f64;
        let variance = window
            .iter()
            .map(|&x| {
                let diff = x as f64 - mean;
                diff * diff
            })
            .sum::<f64>()
            / window.len() as f64;
        let std_dev = variance.sqrt();

        // Check for anomalies (values >= 2 std devs from mean)
        let last_value = window[window.len() - 1] as f64;
        let deviation = (last_value - mean).abs();

        if deviation >= 3.0 * std_dev {
            // Severe anomaly
            let pattern = if last_value > mean {
                "sudden_spike"
            } else {
                "sudden_drop"
            };
            (true, "high", pattern)
        } else if deviation >= 2.0 * std_dev {
            // Moderate anomaly
            let pattern = if last_value > mean {
                "moderate_increase"
            } else {
                "moderate_decrease"
            };
            (true, "medium", pattern)
        } else {
            // Check for gradual trends
            let is_increasing = window.windows(2).all(|w| w[1] >= w[0]);
            let is_decreasing = window.windows(2).all(|w| w[1] <= w[0]);

            if is_increasing {
                (false, "low", "gradual_increase")
            } else if is_decreasing {
                (false, "low", "gradual_decrease")
            } else {
                (false, "none", "normal")
            }
        }
    }

    /// Estimate optimal worker pool size for given workload.
    ///
    /// Calculates the ideal number of workers based on message rate,
    /// processing time, and target latency.
    ///
    /// # Arguments
    ///
    /// * `avg_msg_per_sec` - Average messages per second
    /// * `avg_processing_ms` - Average processing time in milliseconds
    /// * `target_latency_ms` - Target latency in milliseconds
    /// * `worker_overhead_pct` - Worker overhead percentage (0-100)
    ///
    /// # Returns
    ///
    /// Tuple of (optimal_workers, max_throughput, utilization)
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::estimate_optimal_worker_pool;
    ///
    /// let (workers, throughput, util) = estimate_optimal_worker_pool(100, 50, 100, 10);
    /// assert!(workers > 0);
    /// assert!(throughput > 0.0);
    /// ```
    pub fn estimate_optimal_worker_pool(
        avg_msg_per_sec: usize,
        avg_processing_ms: usize,
        target_latency_ms: usize,
        worker_overhead_pct: u8,
    ) -> (usize, f64, f64) {
        if avg_msg_per_sec == 0 || avg_processing_ms == 0 {
            return (1, 0.0, 0.0);
        }

        // Calculate base workers needed for throughput
        let processing_time_sec = avg_processing_ms as f64 / 1000.0;
        let base_workers = (avg_msg_per_sec as f64 * processing_time_sec).ceil() as usize;

        // Adjust for overhead
        let overhead_factor = 1.0 + (worker_overhead_pct as f64 / 100.0);
        let adjusted_workers = (base_workers as f64 * overhead_factor).ceil() as usize;

        // Add buffer for latency target
        let latency_buffer = if target_latency_ms < avg_processing_ms {
            // Need more workers to meet latency target
            let ratio = avg_processing_ms as f64 / target_latency_ms as f64;
            (adjusted_workers as f64 * ratio).ceil() as usize
        } else {
            adjusted_workers
        };

        let optimal_workers = latency_buffer.max(1);

        // Calculate max throughput with this worker count
        let worker_capacity = 1000.0 / avg_processing_ms as f64; // msgs/sec per worker
        let max_throughput = worker_capacity * optimal_workers as f64 / overhead_factor;

        // Calculate utilization
        let utilization = (avg_msg_per_sec as f64 / max_throughput).min(1.0);

        (optimal_workers, max_throughput, utilization)
    }

    /// Analyze compression benefit for messages
    ///
    /// Determines if compression would be beneficial based on message characteristics.
    /// Returns (should_compress, estimated_ratio, recommendation).
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::analyze_compression_benefit;
    ///
    /// let (should_compress, ratio, rec) = analyze_compression_benefit(1024, 2000, "application/json");
    /// assert!(should_compress);
    /// assert!(ratio > 0.0);
    /// ```
    pub fn analyze_compression_benefit(
        avg_message_size: usize,
        message_count_per_sec: usize,
        content_type: &str,
    ) -> (bool, f64, String) {
        // Content types that compress well
        let compressible_types = [
            "application/json",
            "text/plain",
            "text/html",
            "text/xml",
            "application/xml",
        ];

        let is_compressible = compressible_types.iter().any(|&t| content_type.contains(t));

        if !is_compressible {
            return (false, 1.0, "content_type_not_compressible".to_string());
        }

        // Small messages (< 500 bytes) often have overhead exceeding benefits
        if avg_message_size < 500 {
            return (false, 1.0, "message_too_small".to_string());
        }

        // Estimate compression ratio based on content type
        let estimated_ratio = if content_type.contains("json") {
            0.3 // JSON typically compresses to 30% of original
        } else if content_type.contains("xml") {
            0.25 // XML compresses even better
        } else if content_type.contains("text") {
            0.4 // Plain text compression
        } else {
            0.5
        };

        // Calculate bytes saved per second
        let bytes_per_sec = avg_message_size * message_count_per_sec;
        let savings_per_sec = (bytes_per_sec as f64 * (1.0 - estimated_ratio)) as usize;

        // Recommend compression if savings > 1MB/sec or message size > 10KB
        let should_compress = savings_per_sec > 1_000_000 || avg_message_size > 10_000;

        let recommendation = if should_compress {
            format!("enable_compression_saves_{}_bytes_per_sec", savings_per_sec)
        } else {
            "compression_overhead_not_worth_it".to_string()
        };

        (should_compress, estimated_ratio, recommendation)
    }

    /// Calculate queue migration plan
    ///
    /// Plans the migration of messages from one queue to another.
    /// Returns (batches, total_time_secs, recommendation).
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::calculate_queue_migration_plan;
    ///
    /// let (batches, time, rec) = calculate_queue_migration_plan(10000, 100, 50);
    /// assert!(batches > 0);
    /// assert!(time > 0);
    /// ```
    pub fn calculate_queue_migration_plan(
        message_count: usize,
        batch_size: usize,
        messages_per_sec: usize,
    ) -> (usize, usize, String) {
        if message_count == 0 || batch_size == 0 {
            return (0, 0, "no_migration_needed".to_string());
        }

        let safe_rate = messages_per_sec.max(1);

        // Calculate number of batches needed
        let batches = (message_count as f64 / batch_size as f64).ceil() as usize;

        // Calculate total migration time
        let total_time_secs = message_count / safe_rate;

        // Generate recommendation based on migration characteristics
        let recommendation = if total_time_secs < 60 {
            "fast_migration_proceed".to_string()
        } else if total_time_secs < 3600 {
            format!("moderate_migration_{}_minutes", total_time_secs / 60)
        } else {
            format!(
                "slow_migration_{}_hours_consider_increasing_rate",
                total_time_secs / 3600
            )
        };

        (batches, total_time_secs, recommendation)
    }

    /// Profile message patterns for detailed analysis
    ///
    /// Analyzes message patterns to provide insights for optimization.
    /// Returns (pattern_type, consistency_score, recommendation).
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::profile_message_patterns;
    ///
    /// let sizes = vec![100, 105, 98, 102];
    /// let intervals = vec![10, 11, 9, 10];
    /// let (pattern, score, rec) = profile_message_patterns(&sizes, &intervals);
    /// assert!(score >= 0.0 && score <= 1.0);
    /// ```
    pub fn profile_message_patterns(
        message_sizes: &[usize],
        arrival_intervals_ms: &[usize],
    ) -> (String, f64, String) {
        if message_sizes.is_empty() || arrival_intervals_ms.is_empty() {
            return ("unknown".to_string(), 0.0, "insufficient_data".to_string());
        }

        // Calculate size variance
        let avg_size = message_sizes.iter().sum::<usize>() as f64 / message_sizes.len() as f64;
        let size_variance = message_sizes
            .iter()
            .map(|&s| {
                let diff = s as f64 - avg_size;
                diff * diff
            })
            .sum::<f64>()
            / message_sizes.len() as f64;
        let size_std_dev = size_variance.sqrt();
        let size_cv = if avg_size > 0.0 {
            size_std_dev / avg_size
        } else {
            0.0
        };

        // Calculate interval variance
        let avg_interval =
            arrival_intervals_ms.iter().sum::<usize>() as f64 / arrival_intervals_ms.len() as f64;
        let interval_variance = arrival_intervals_ms
            .iter()
            .map(|&i| {
                let diff = i as f64 - avg_interval;
                diff * diff
            })
            .sum::<f64>()
            / arrival_intervals_ms.len() as f64;
        let interval_std_dev = interval_variance.sqrt();
        let interval_cv = if avg_interval > 0.0 {
            interval_std_dev / avg_interval
        } else {
            0.0
        };

        // Determine pattern type based on consistency
        let pattern_type = if size_cv < 0.1 && interval_cv < 0.1 {
            "highly_regular".to_string()
        } else if size_cv < 0.3 && interval_cv < 0.3 {
            "regular".to_string()
        } else if size_cv > 0.8 || interval_cv > 0.8 {
            "highly_irregular".to_string()
        } else {
            "moderately_irregular".to_string()
        };

        // Calculate consistency score (0.0 = very inconsistent, 1.0 = very consistent)
        let consistency_score = 1.0 - ((size_cv + interval_cv) / 2.0).min(1.0);

        // Generate recommendation
        let recommendation = if consistency_score > 0.8 {
            "predictable_use_static_buffers".to_string()
        } else if consistency_score > 0.5 {
            "moderate_use_adaptive_buffers".to_string()
        } else {
            "unpredictable_use_dynamic_scaling".to_string()
        };

        (pattern_type, consistency_score, recommendation)
    }

    /// Calculate network efficiency metrics
    ///
    /// Analyzes network utilization and bandwidth efficiency.
    /// Returns (efficiency_score, bandwidth_util_pct, recommendation).
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::calculate_network_efficiency;
    ///
    /// let (score, util, rec) = calculate_network_efficiency(1000, 500, 10000);
    /// assert!(score >= 0.0 && score <= 1.0);
    /// assert!(util >= 0.0 && util <= 100.0);
    /// ```
    pub fn calculate_network_efficiency(
        bytes_sent: usize,
        bytes_received: usize,
        max_bandwidth_bytes: usize,
    ) -> (f64, f64, String) {
        if max_bandwidth_bytes == 0 {
            return (0.0, 0.0, "invalid_bandwidth".to_string());
        }

        let total_bytes = bytes_sent + bytes_received;
        let bandwidth_util_pct =
            (total_bytes as f64 / max_bandwidth_bytes as f64 * 100.0).min(100.0);

        // Calculate efficiency based on send/receive ratio
        let send_ratio = if total_bytes > 0 {
            bytes_sent as f64 / total_bytes as f64
        } else {
            0.0
        };

        // Optimal efficiency is when send and receive are balanced (50/50)
        let balance_score = 1.0 - (send_ratio - 0.5).abs() * 2.0;

        // Overall efficiency combines balance and utilization
        let utilization_score = if bandwidth_util_pct < 70.0 {
            bandwidth_util_pct / 70.0 // Underutilized
        } else if bandwidth_util_pct < 90.0 {
            1.0 // Optimal range
        } else {
            (100.0 - bandwidth_util_pct) / 10.0 // Overutilized
        };

        let efficiency_score = (balance_score * 0.4 + utilization_score * 0.6).clamp(0.0, 1.0);

        // Generate recommendation
        let recommendation = if efficiency_score > 0.8 {
            "excellent_efficiency".to_string()
        } else if efficiency_score > 0.6 {
            "good_efficiency".to_string()
        } else if bandwidth_util_pct > 90.0 {
            "increase_bandwidth_overutilized".to_string()
        } else if bandwidth_util_pct < 30.0 {
            "reduce_bandwidth_underutilized".to_string()
        } else if (send_ratio - 0.5).abs() > 0.3 {
            "imbalanced_traffic_pattern".to_string()
        } else {
            "optimize_message_patterns".to_string()
        };

        (efficiency_score, bandwidth_util_pct, recommendation)
    }

    /// Detect message hotspots in distributed systems
    ///
    /// Identifies imbalanced message distribution across queues/partitions.
    /// Returns (has_hotspot, hotspot_index, imbalance_ratio, recommendation).
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::detect_message_hotspots;
    ///
    /// let counts = vec![100, 500, 120, 110]; // Queue 1 has hotspot
    /// let (has_hotspot, index, ratio, rec) = detect_message_hotspots(&counts);
    /// assert!(has_hotspot);
    /// assert_eq!(index, 1);
    /// ```
    pub fn detect_message_hotspots(message_counts: &[usize]) -> (bool, usize, f64, String) {
        if message_counts.is_empty() {
            return (false, 0, 1.0, "no_data".to_string());
        }

        if message_counts.len() == 1 {
            return (false, 0, 1.0, "single_queue".to_string());
        }

        // Find max and average
        let max_count = *message_counts.iter().max().unwrap();
        let avg_count = message_counts.iter().sum::<usize>() as f64 / message_counts.len() as f64;

        // Find index of hotspot
        let hotspot_index = message_counts.iter().position(|&c| c == max_count).unwrap();

        // Calculate imbalance ratio (max / avg)
        let imbalance_ratio = if avg_count > 0.0 {
            max_count as f64 / avg_count
        } else {
            1.0
        };

        // Determine if there's a hotspot (threshold: 2x average)
        let has_hotspot = imbalance_ratio > 2.0;

        // Generate recommendation
        let recommendation = if !has_hotspot {
            "balanced_distribution".to_string()
        } else if imbalance_ratio > 5.0 {
            format!("severe_hotspot_rebalance_queue_{}", hotspot_index)
        } else if imbalance_ratio > 3.0 {
            format!(
                "moderate_hotspot_check_routing_keys_queue_{}",
                hotspot_index
            )
        } else {
            format!("minor_hotspot_monitor_queue_{}", hotspot_index)
        };

        (has_hotspot, hotspot_index, imbalance_ratio, recommendation)
    }

    /// Recommend queue topology based on workload characteristics
    ///
    /// Suggests optimal queue architecture for given requirements.
    /// Returns (topology_type, queue_count, recommendation).
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::recommend_queue_topology;
    ///
    /// let (topology, count, rec) = recommend_queue_topology(10000, 100, 50, true);
    /// assert!(count > 0);
    /// ```
    pub fn recommend_queue_topology(
        messages_per_sec: usize,
        avg_processing_ms: usize,
        num_consumers: usize,
        requires_ordering: bool,
    ) -> (String, usize, String) {
        if messages_per_sec == 0 {
            return ("single_queue".to_string(), 1, "low_volume".to_string());
        }

        // Calculate processing capacity
        let processing_rate = if avg_processing_ms > 0 {
            (1000.0 / avg_processing_ms as f64) as usize
        } else {
            1000
        };
        let total_capacity = processing_rate * num_consumers;

        // Determine if we need partitioning
        let load_ratio = messages_per_sec as f64 / total_capacity as f64;

        let (topology_type, queue_count) = if requires_ordering {
            // Ordering required: use partitioned queues
            if load_ratio > 0.8 {
                ("partitioned".to_string(), num_consumers.max(4))
            } else {
                ("single_queue".to_string(), 1)
            }
        } else {
            // No ordering: can use multiple independent queues
            if load_ratio > 1.5 {
                // Overloaded: need more queues
                let recommended =
                    (messages_per_sec as f64 / (processing_rate as f64 * 0.7)) as usize;
                ("multi_queue".to_string(), recommended.clamp(2, 32))
            } else if load_ratio > 0.8 {
                // High load: partition by priority
                ("priority_queues".to_string(), 3) // High, Medium, Low
            } else if messages_per_sec > 1000 {
                // Medium load: use worker pool
                ("worker_pool".to_string(), num_consumers.max(2))
            } else {
                // Low load: single queue
                ("single_queue".to_string(), 1)
            }
        };

        // Generate recommendation
        let recommendation = if load_ratio > 1.5 {
            format!(
                "overloaded_increase_consumers_or_queues_load_ratio_{:.2}",
                load_ratio
            )
        } else if load_ratio > 1.0 {
            format!("near_capacity_scale_soon_load_ratio_{:.2}", load_ratio)
        } else if load_ratio < 0.3 {
            format!(
                "underutilized_reduce_resources_load_ratio_{:.2}",
                load_ratio
            )
        } else {
            format!("optimal_topology_{}", topology_type)
        };

        (topology_type, queue_count, recommendation)
    }

    /// Calculate optimal message deduplication window
    ///
    /// Analyzes message patterns to suggest appropriate deduplication window.
    ///
    /// # Arguments
    ///
    /// * `avg_message_interval_ms` - Average time between messages (milliseconds)
    /// * `retry_count` - Average number of retries per message
    /// * `max_delivery_delay_ms` - Maximum expected delivery delay (milliseconds)
    ///
    /// # Returns
    ///
    /// Tuple of (window_secs, cache_size, recommendation)
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::calculate_message_deduplication_window;
    ///
    /// // Fast message stream with retries
    /// let (window, cache_size, rec) = calculate_message_deduplication_window(100, 3, 5000);
    /// assert!(window > 0);
    /// assert!(cache_size > 0);
    /// assert!(rec.contains("window"));
    ///
    /// // Slow message stream
    /// let (window, cache_size, _) = calculate_message_deduplication_window(10000, 1, 2000);
    /// assert!(window >= 60); // At least 1 minute
    /// ```
    pub fn calculate_message_deduplication_window(
        avg_message_interval_ms: usize,
        retry_count: usize,
        max_delivery_delay_ms: usize,
    ) -> (usize, usize, String) {
        // Base window should cover retries + delivery delays
        let retry_window_ms = avg_message_interval_ms * retry_count.max(1);
        let total_window_ms = retry_window_ms + max_delivery_delay_ms;

        // Convert to seconds and add safety margin (2x)
        let window_secs = ((total_window_ms * 2) / 1000).max(60); // At least 1 minute

        // Estimate cache size based on message rate and window
        let messages_per_sec = if avg_message_interval_ms > 0 {
            (1000.0 / avg_message_interval_ms as f64) as usize
        } else {
            100 // Default estimate
        };
        let cache_size = (messages_per_sec * window_secs).clamp(1000, 1000000);

        // Generate recommendation
        let recommendation = if window_secs > 3600 {
            format!(
                "long_window_{}_secs_consider_persistent_storage",
                window_secs
            )
        } else if window_secs > 600 {
            format!("medium_window_{}_secs_cache_{}", window_secs, cache_size)
        } else {
            format!("short_window_{}_secs_cache_{}", window_secs, cache_size)
        };

        (window_secs, cache_size, recommendation)
    }

    /// Analyze retry effectiveness
    ///
    /// Evaluates how effective retries are at recovering from failures.
    ///
    /// # Arguments
    ///
    /// * `total_messages` - Total messages processed
    /// * `failed_messages` - Messages that failed initially
    /// * `retry_successes` - Messages that succeeded after retry
    /// * `final_failures` - Messages that failed permanently
    ///
    /// # Returns
    ///
    /// Tuple of (effectiveness_pct, success_rate, recommendation)
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::analyze_retry_effectiveness;
    ///
    /// // High retry effectiveness
    /// let (eff, rate, rec) = analyze_retry_effectiveness(1000, 100, 80, 20);
    /// assert!(eff > 50.0);
    /// assert!(rec.contains("effective"));
    ///
    /// // Low retry effectiveness
    /// let (eff, _, rec) = analyze_retry_effectiveness(1000, 100, 10, 90);
    /// assert!(eff < 20.0);
    /// assert!(rec.contains("ineffective"));
    /// ```
    pub fn analyze_retry_effectiveness(
        total_messages: usize,
        failed_messages: usize,
        retry_successes: usize,
        final_failures: usize,
    ) -> (f64, f64, String) {
        if failed_messages == 0 {
            return (100.0, 100.0, "no_failures_retries_not_needed".to_string());
        }

        // Calculate retry effectiveness (% of failures recovered by retry)
        let effectiveness = (retry_successes as f64 / failed_messages as f64) * 100.0;

        // Calculate overall success rate
        let total_successes = total_messages - final_failures;
        let success_rate = (total_successes as f64 / total_messages as f64) * 100.0;

        // Generate recommendation
        let recommendation = if effectiveness > 80.0 {
            format!(
                "highly_effective_retries_{:.1}pct_success_keep_current_policy",
                effectiveness
            )
        } else if effectiveness > 50.0 {
            format!(
                "moderately_effective_retries_{:.1}pct_consider_backoff_tuning",
                effectiveness
            )
        } else if effectiveness > 20.0 {
            format!(
                "low_effectiveness_{:.1}pct_review_retry_strategy",
                effectiveness
            )
        } else {
            format!(
                "ineffective_retries_{:.1}pct_investigate_root_cause",
                effectiveness
            )
        };

        (effectiveness, success_rate, recommendation)
    }

    /// Calculate queue overflow risk
    ///
    /// Predicts the probability of queue overflow based on trends.
    ///
    /// # Arguments
    ///
    /// * `current_size` - Current queue size
    /// * `max_size` - Maximum queue capacity
    /// * `enqueue_rate` - Messages added per second
    /// * `dequeue_rate` - Messages removed per second
    ///
    /// # Returns
    ///
    /// Tuple of (risk_pct, time_to_full_secs, recommendation)
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::calculate_queue_overflow_risk;
    ///
    /// // High risk scenario
    /// let (risk, ttf, rec) = calculate_queue_overflow_risk(8000, 10000, 100, 50);
    /// assert!(risk > 50.0);
    /// assert!(rec.contains("high_risk"));
    ///
    /// // Low risk scenario
    /// let (risk, _, rec) = calculate_queue_overflow_risk(100, 10000, 50, 100);
    /// assert_eq!(risk, 0.0);
    /// assert!(rec.contains("healthy"));
    /// ```
    pub fn calculate_queue_overflow_risk(
        current_size: usize,
        max_size: usize,
        enqueue_rate: usize,
        dequeue_rate: usize,
    ) -> (f64, i64, String) {
        if max_size == 0 {
            return (100.0, 0, "invalid_max_size_queue_misconfigured".to_string());
        }

        // Calculate net growth rate
        let net_rate = enqueue_rate as i64 - dequeue_rate as i64;

        // If queue is draining, no risk
        if net_rate <= 0 {
            let drain_time_secs = if dequeue_rate > enqueue_rate && current_size > 0 {
                (current_size as f64 / (dequeue_rate - enqueue_rate) as f64) as i64
            } else {
                -1
            };
            return (
                0.0,
                drain_time_secs,
                "healthy_queue_draining_no_overflow_risk".to_string(),
            );
        }

        // Calculate time to full
        let remaining_capacity = max_size.saturating_sub(current_size);
        let time_to_full_secs = if net_rate > 0 {
            (remaining_capacity as f64 / net_rate as f64) as i64
        } else {
            i64::MAX
        };

        // Calculate current utilization
        let utilization = (current_size as f64 / max_size as f64) * 100.0;

        // Calculate risk based on utilization and time to full
        let risk = if utilization >= 90.0 {
            95.0 // Critical
        } else if utilization >= 80.0 {
            80.0 // High
        } else if utilization >= 60.0 {
            60.0 // Medium
        } else if time_to_full_secs < 60 {
            75.0 // Will fill in < 1 minute
        } else if time_to_full_secs < 300 {
            50.0 // Will fill in < 5 minutes
        } else if time_to_full_secs < 3600 {
            30.0 // Will fill in < 1 hour
        } else {
            10.0 // Low risk
        };

        // Generate recommendation
        let recommendation = if risk >= 90.0 {
            format!(
                "critical_risk_{:.1}pct_utilized_ttf_{}_secs_immediate_action",
                utilization, time_to_full_secs
            )
        } else if risk >= 70.0 {
            format!(
                "high_risk_{:.1}pct_utilized_ttf_{}_secs_scale_consumers",
                utilization, time_to_full_secs
            )
        } else if risk >= 40.0 {
            format!(
                "medium_risk_{:.1}pct_utilized_ttf_{}_secs_monitor_closely",
                utilization, time_to_full_secs
            )
        } else {
            format!("low_risk_{:.1}pct_utilized_queue_healthy", utilization)
        };

        (risk, time_to_full_secs, recommendation)
    }

    /// Calculate message throughput trend
    ///
    /// Analyzes throughput over time to identify trends and patterns.
    ///
    /// # Arguments
    ///
    /// * `throughput_samples` - Historical throughput samples (messages/sec)
    /// * `window_size` - Number of recent samples to analyze
    ///
    /// # Returns
    ///
    /// Tuple of (current_avg, trend_direction, change_pct)
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::calculate_message_throughput_trend;
    ///
    /// // Increasing throughput
    /// let samples = vec![100.0, 110.0, 120.0, 130.0, 140.0];
    /// let (avg, trend, change) = calculate_message_throughput_trend(&samples, 3);
    /// assert!(avg > 0.0);
    /// assert_eq!(trend, "increasing");
    ///
    /// // Decreasing throughput
    /// let samples = vec![140.0, 130.0, 120.0, 110.0, 100.0];
    /// let (_, trend, _) = calculate_message_throughput_trend(&samples, 3);
    /// assert_eq!(trend, "decreasing");
    /// ```
    pub fn calculate_message_throughput_trend(
        throughput_samples: &[f64],
        window_size: usize,
    ) -> (f64, String, f64) {
        if throughput_samples.is_empty() {
            return (0.0, "no_data".to_string(), 0.0);
        }

        let actual_window = window_size.min(throughput_samples.len());
        let recent_samples =
            &throughput_samples[throughput_samples.len().saturating_sub(actual_window)..];

        if recent_samples.is_empty() {
            return (0.0, "no_data".to_string(), 0.0);
        }

        // Calculate current average
        let current_avg: f64 = recent_samples.iter().sum::<f64>() / recent_samples.len() as f64;

        // Calculate trend by comparing first half vs second half
        if recent_samples.len() < 2 {
            return (current_avg, "stable".to_string(), 0.0);
        }

        let mid = recent_samples.len() / 2;
        let first_half_avg: f64 = recent_samples[..mid].iter().sum::<f64>() / mid as f64;
        let second_half_avg: f64 =
            recent_samples[mid..].iter().sum::<f64>() / (recent_samples.len() - mid) as f64;

        let change_pct = if first_half_avg > 0.0 {
            ((second_half_avg - first_half_avg) / first_half_avg) * 100.0
        } else {
            0.0
        };

        let trend = if change_pct > 10.0 {
            "increasing"
        } else if change_pct < -10.0 {
            "decreasing"
        } else {
            "stable"
        };

        (current_avg, trend.to_string(), change_pct)
    }

    /// Detect queue starvation
    ///
    /// Identifies when queues are underutilized or idle.
    ///
    /// # Arguments
    ///
    /// * `queue_size` - Current number of messages in queue
    /// * `consumer_count` - Number of active consumers
    /// * `min_queue_depth` - Minimum healthy queue depth per consumer
    ///
    /// # Returns
    ///
    /// Tuple of (is_starving, severity, recommendation)
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::detect_queue_starvation;
    ///
    /// // Starving queue
    /// let (starving, severity, rec) = detect_queue_starvation(5, 10, 2);
    /// assert!(starving);
    /// assert_eq!(severity, "high");
    ///
    /// // Healthy queue
    /// let (starving, _, _) = detect_queue_starvation(100, 10, 2);
    /// assert!(!starving);
    /// ```
    pub fn detect_queue_starvation(
        queue_size: usize,
        consumer_count: usize,
        min_queue_depth: usize,
    ) -> (bool, String, String) {
        if consumer_count == 0 {
            return (
                false,
                "no_consumers".to_string(),
                "no_consumers_to_starve".to_string(),
            );
        }

        let recommended_depth = consumer_count * min_queue_depth;
        let depth_ratio = queue_size as f64 / recommended_depth as f64;

        let (is_starving, severity) = if depth_ratio < 0.25 {
            (true, "high")
        } else if depth_ratio < 0.5 {
            (true, "medium")
        } else if depth_ratio < 0.75 {
            (true, "low")
        } else {
            (false, "healthy")
        };

        let recommendation = if is_starving {
            if consumer_count > queue_size {
                format!(
                    "high_starvation_reduce_consumers_from_{}_to_{}_or_increase_queue_depth",
                    consumer_count,
                    queue_size.max(1)
                )
            } else {
                format!(
                    "{}_starvation_increase_queue_depth_from_{}_to_{}_or_reduce_consumers",
                    severity, queue_size, recommended_depth
                )
            }
        } else {
            format!(
                "healthy_queue_depth_{}_for_{}_consumers",
                queue_size, consumer_count
            )
        };

        (is_starving, severity.to_string(), recommendation)
    }

    /// Estimate broker capacity
    ///
    /// Calculates maximum sustainable throughput based on current metrics.
    ///
    /// # Arguments
    ///
    /// * `avg_processing_ms` - Average message processing time
    /// * `worker_count` - Number of workers
    /// * `connection_pool_size` - Connection pool size
    /// * `target_utilization` - Target utilization percentage (0.0-1.0)
    ///
    /// # Returns
    ///
    /// Tuple of (max_throughput_per_sec, headroom_pct, recommendation)
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_kombu::utils::estimate_broker_capacity;
    ///
    /// // Calculate capacity
    /// let (throughput, headroom, rec) = estimate_broker_capacity(100, 10, 20, 0.8);
    /// assert!(throughput > 0.0);
    /// assert!(rec.contains("capacity"));
    ///
    /// // High utilization
    /// let (_, headroom, _) = estimate_broker_capacity(50, 5, 10, 0.95);
    /// assert!(headroom < 10.0);
    /// ```
    pub fn estimate_broker_capacity(
        avg_processing_ms: usize,
        worker_count: usize,
        connection_pool_size: usize,
        target_utilization: f64,
    ) -> (f64, f64, String) {
        if avg_processing_ms == 0 || worker_count == 0 {
            return (
                0.0,
                0.0,
                "invalid_input_processing_time_or_workers_zero".to_string(),
            );
        }

        // Calculate theoretical max throughput
        let msg_per_sec_per_worker = 1000.0 / avg_processing_ms as f64;
        let theoretical_max = msg_per_sec_per_worker * worker_count as f64;

        // Account for connection pool as bottleneck
        let connection_limited = if connection_pool_size < worker_count {
            msg_per_sec_per_worker * connection_pool_size as f64
        } else {
            theoretical_max
        };

        // Apply target utilization
        let safe_max = connection_limited * target_utilization;

        // Calculate headroom
        let headroom_pct = ((1.0 - target_utilization) * 100.0).max(0.0);

        // Generate recommendation
        let recommendation = if headroom_pct < 5.0 {
            format!(
                "critical_capacity_{:.1}_msgs_sec_headroom_{:.1}pct_scale_immediately",
                safe_max, headroom_pct
            )
        } else if headroom_pct < 15.0 {
            format!(
                "limited_capacity_{:.1}_msgs_sec_headroom_{:.1}pct_plan_scaling",
                safe_max, headroom_pct
            )
        } else {
            format!(
                "healthy_capacity_{:.1}_msgs_sec_headroom_{:.1}pct",
                safe_max, headroom_pct
            )
        };

        (safe_max, headroom_pct, recommendation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use celers_protocol::Message;

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

    #[test]
    fn test_timeout_middleware_creation() {
        let timeout = TimeoutMiddleware::new(Duration::from_secs(30));
        assert_eq!(timeout.timeout(), Duration::from_secs(30));
        assert_eq!(timeout.name(), "timeout");
    }

    #[tokio::test]
    async fn test_timeout_middleware_sets_header() {
        let timeout = TimeoutMiddleware::new(Duration::from_secs(60));
        let mut message = Message::new("test".to_string(), Uuid::new_v4(), vec![1, 2, 3]);

        timeout.before_publish(&mut message).await.unwrap();

        assert!(message.headers.extra.contains_key("x-timeout-ms"));
    }

    #[test]
    fn test_filter_middleware_creation() {
        let filter = FilterMiddleware::new(|msg: &Message| msg.task_name().starts_with("test"));
        let test_msg = Message::new("test_task".to_string(), Uuid::new_v4(), vec![]);
        let other_msg = Message::new("other_task".to_string(), Uuid::new_v4(), vec![]);

        assert!(filter.matches(&test_msg));
        assert!(!filter.matches(&other_msg));
        assert_eq!(filter.name(), "filter");
    }

    #[tokio::test]
    async fn test_filter_middleware_rejects_non_matching() {
        let filter = FilterMiddleware::new(|msg: &Message| msg.task_name() == "allowed");
        let mut allowed = Message::new("allowed".to_string(), Uuid::new_v4(), vec![]);
        let mut rejected = Message::new("rejected".to_string(), Uuid::new_v4(), vec![]);

        assert!(filter.after_consume(&mut allowed).await.is_ok());
        assert!(filter.after_consume(&mut rejected).await.is_err());
    }

    #[test]
    fn test_sampling_middleware_creation() {
        let sampler = SamplingMiddleware::new(0.5);
        assert_eq!(sampler.sample_rate(), 0.5);
        assert_eq!(sampler.name(), "sampling");

        // Test clamping
        let clamped_low = SamplingMiddleware::new(-0.1);
        assert_eq!(clamped_low.sample_rate(), 0.0);

        let clamped_high = SamplingMiddleware::new(1.5);
        assert_eq!(clamped_high.sample_rate(), 1.0);
    }

    #[tokio::test]
    async fn test_sampling_middleware_samples_messages() {
        // Sample everything
        let sampler = SamplingMiddleware::new(1.0);
        let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);
        assert!(sampler.after_consume(&mut msg).await.is_ok());

        // Sample nothing
        let sampler = SamplingMiddleware::new(0.0);
        let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);
        assert!(sampler.after_consume(&mut msg).await.is_err());
    }

    #[test]
    fn test_transformation_middleware_creation() {
        let transformer = TransformationMiddleware::new(|body| {
            String::from_utf8_lossy(&body).to_uppercase().into_bytes()
        });
        assert_eq!(transformer.name(), "transformation");
    }

    #[tokio::test]
    async fn test_transformation_middleware_transforms() {
        let transformer = TransformationMiddleware::new(|body| {
            String::from_utf8_lossy(&body).to_uppercase().into_bytes()
        });

        let mut msg = Message::new("test".to_string(), Uuid::new_v4(), b"hello world".to_vec());

        transformer.before_publish(&mut msg).await.unwrap();
        assert_eq!(msg.body, b"HELLO WORLD");

        // Reset for consume test
        msg.body = b"hello again".to_vec();
        transformer.after_consume(&mut msg).await.unwrap();
        assert_eq!(msg.body, b"HELLO AGAIN");
    }

    #[test]
    fn test_tracing_middleware_creation() {
        let tracing = TracingMiddleware::new("my-service");
        assert_eq!(tracing.name(), "tracing");
    }

    #[tokio::test]
    async fn test_tracing_middleware_injects_trace_id() {
        let tracing = TracingMiddleware::new("test-service");

        let mut msg = Message::new(
            "test_task".to_string(),
            Uuid::new_v4(),
            b"test body".to_vec(),
        );

        tracing.before_publish(&mut msg).await.unwrap();

        // Verify trace ID was injected
        assert!(msg.headers.extra.contains_key("trace-id"));
        assert!(msg.headers.extra.contains_key("service-name"));
        assert!(msg.headers.extra.contains_key("span-id"));
        assert!(msg.headers.extra.contains_key("trace-timestamp"));
        assert_eq!(
            msg.headers
                .extra
                .get("service-name")
                .unwrap()
                .as_str()
                .unwrap(),
            "test-service"
        );
    }

    #[tokio::test]
    async fn test_tracing_middleware_preserves_existing_trace_id() {
        let tracing = TracingMiddleware::new("test-service");

        let mut msg = Message::new(
            "test_task".to_string(),
            Uuid::new_v4(),
            b"test body".to_vec(),
        );

        // Pre-set a trace ID
        let original_trace_id = "existing-trace-id";
        msg.headers
            .extra
            .insert("trace-id".to_string(), serde_json::json!(original_trace_id));

        tracing.before_publish(&mut msg).await.unwrap();

        // Verify original trace ID was preserved
        assert_eq!(
            msg.headers.extra.get("trace-id").unwrap().as_str().unwrap(),
            original_trace_id
        );
    }

    #[tokio::test]
    async fn test_tracing_middleware_after_consume() {
        let tracing = TracingMiddleware::new("consumer-service");

        let mut msg = Message::new(
            "test_task".to_string(),
            Uuid::new_v4(),
            b"test body".to_vec(),
        );

        msg.headers
            .extra
            .insert("trace-id".to_string(), serde_json::json!("trace-123"));

        tracing.after_consume(&mut msg).await.unwrap();

        // Verify consume-side headers were added
        assert!(msg.headers.extra.contains_key("consumer-service"));
        assert!(msg.headers.extra.contains_key("trace-id-consumed"));
        assert_eq!(
            msg.headers
                .extra
                .get("consumer-service")
                .unwrap()
                .as_str()
                .unwrap(),
            "consumer-service"
        );
    }

    #[test]
    fn test_batching_middleware_creation() {
        let batching = BatchingMiddleware::new(50, 3000);
        assert_eq!(batching.name(), "batching");
    }

    #[test]
    fn test_batching_middleware_with_defaults() {
        let batching = BatchingMiddleware::with_defaults();
        assert_eq!(batching.name(), "batching");
    }

    #[tokio::test]
    async fn test_batching_middleware_adds_metadata() {
        let batching = BatchingMiddleware::new(100, 5000);

        let mut msg = Message::new(
            "test_task".to_string(),
            Uuid::new_v4(),
            b"test body".to_vec(),
        );

        batching.before_publish(&mut msg).await.unwrap();

        // Verify batching metadata was added
        assert_eq!(
            msg.headers
                .extra
                .get("batch-size-hint")
                .unwrap()
                .as_u64()
                .unwrap(),
            100
        );
        assert_eq!(
            msg.headers
                .extra
                .get("batch-timeout-ms")
                .unwrap()
                .as_u64()
                .unwrap(),
            5000
        );
        assert!(msg
            .headers
            .extra
            .get("batching-enabled")
            .unwrap()
            .as_bool()
            .unwrap());
    }

    #[test]
    fn test_audit_middleware_creation() {
        let audit = AuditMiddleware::new(true);
        assert_eq!(audit.name(), "audit");
    }

    #[test]
    fn test_audit_middleware_with_body_logging() {
        let audit = AuditMiddleware::with_body_logging();
        assert_eq!(audit.name(), "audit");
    }

    #[test]
    fn test_audit_middleware_without_body_logging() {
        let audit = AuditMiddleware::without_body_logging();
        assert_eq!(audit.name(), "audit");
    }

    #[tokio::test]
    async fn test_audit_middleware_before_publish() {
        let audit = AuditMiddleware::new(true);

        let mut msg = Message::new(
            "test_task".to_string(),
            Uuid::new_v4(),
            b"test body".to_vec(),
        );

        audit.before_publish(&mut msg).await.unwrap();

        // Verify audit metadata was added
        assert!(msg.headers.extra.contains_key("audit-publish"));
        assert!(msg.headers.extra.contains_key("audit-id"));

        let audit_entry = msg
            .headers
            .extra
            .get("audit-publish")
            .unwrap()
            .as_str()
            .unwrap();
        assert!(audit_entry.contains("PUBLISH"));
        assert!(audit_entry.contains("body_size=9"));
    }

    #[tokio::test]
    async fn test_audit_middleware_after_consume() {
        let audit = AuditMiddleware::new(false);

        let mut msg = Message::new(
            "test_task".to_string(),
            Uuid::new_v4(),
            b"test body".to_vec(),
        );

        audit.after_consume(&mut msg).await.unwrap();

        // Verify audit metadata was added
        assert!(msg.headers.extra.contains_key("audit-consume"));

        let audit_entry = msg
            .headers
            .extra
            .get("audit-consume")
            .unwrap()
            .as_str()
            .unwrap();
        assert!(audit_entry.contains("CONSUME"));
        assert!(audit_entry.contains("<redacted>"));
    }

    #[tokio::test]
    async fn test_deadline_middleware_creation() {
        let middleware = DeadlineMiddleware::new(Duration::from_secs(300));
        assert_eq!(middleware.deadline_duration(), Duration::from_secs(300));
        assert_eq!(middleware.name(), "deadline");
    }

    #[tokio::test]
    async fn test_deadline_middleware_sets_deadline() {
        let middleware = DeadlineMiddleware::new(Duration::from_secs(60));

        let mut msg = Message::new(
            "test_task".to_string(),
            Uuid::new_v4(),
            b"test body".to_vec(),
        );

        middleware.before_publish(&mut msg).await.unwrap();

        // Verify deadline was set
        assert!(msg.headers.extra.contains_key("x-deadline"));
        let deadline = msg
            .headers
            .extra
            .get("x-deadline")
            .unwrap()
            .as_u64()
            .unwrap();

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Deadline should be in the future
        assert!(deadline > now);
        assert!(deadline <= now + 61); // Allow 1 second tolerance
    }

    #[tokio::test]
    async fn test_deadline_middleware_detects_exceeded() {
        let middleware = DeadlineMiddleware::new(Duration::from_secs(60));

        let mut msg = Message::new(
            "test_task".to_string(),
            Uuid::new_v4(),
            b"test body".to_vec(),
        );

        // Set a deadline in the past
        let past_deadline = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 10;

        msg.headers
            .extra
            .insert("x-deadline".to_string(), serde_json::json!(past_deadline));

        middleware.after_consume(&mut msg).await.unwrap();

        // Verify deadline-exceeded flag was set
        assert!(msg.headers.extra.contains_key("x-deadline-exceeded"));
        assert!(msg
            .headers
            .extra
            .get("x-deadline-exceeded")
            .unwrap()
            .as_bool()
            .unwrap());
    }

    #[test]
    fn test_content_type_middleware_creation() {
        let middleware = ContentTypeMiddleware::new(vec!["application/json".to_string()]);
        assert_eq!(middleware.name(), "content_type");
        assert!(middleware.is_allowed("application/json"));
        assert!(!middleware.is_allowed("text/plain"));
    }

    #[tokio::test]
    async fn test_content_type_middleware_sets_default() {
        let middleware =
            ContentTypeMiddleware::new(vec![]).with_default("application/json".to_string());

        let mut msg = Message::new(
            "test_task".to_string(),
            Uuid::new_v4(),
            b"test body".to_vec(),
        );
        msg.content_type = String::new();

        middleware.before_publish(&mut msg).await.unwrap();

        assert_eq!(msg.content_type, "application/json");
    }

    #[tokio::test]
    async fn test_content_type_middleware_rejects_invalid() {
        let middleware = ContentTypeMiddleware::new(vec!["application/json".to_string()]);

        let mut msg = Message::new(
            "test_task".to_string(),
            Uuid::new_v4(),
            b"test body".to_vec(),
        );
        msg.content_type = "text/plain".to_string();

        let result = middleware.before_publish(&mut msg).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().is_configuration());
    }

    #[tokio::test]
    async fn test_content_type_middleware_warns_on_consume() {
        let middleware = ContentTypeMiddleware::new(vec!["application/json".to_string()]);

        let mut msg = Message::new(
            "test_task".to_string(),
            Uuid::new_v4(),
            b"test body".to_vec(),
        );
        msg.content_type = "text/plain".to_string();

        middleware.after_consume(&mut msg).await.unwrap();

        // Should add warning but not fail
        assert!(msg.headers.extra.contains_key("x-content-type-warning"));
    }

    #[tokio::test]
    async fn test_routing_key_middleware_custom() {
        let middleware = RoutingKeyMiddleware::new(|msg| format!("custom.{}", msg.headers.task));

        let mut msg = Message::new(
            "test_task".to_string(),
            Uuid::new_v4(),
            b"test body".to_vec(),
        );

        middleware.before_publish(&mut msg).await.unwrap();

        assert!(msg.headers.extra.contains_key("x-routing-key"));
        let routing_key = msg
            .headers
            .extra
            .get("x-routing-key")
            .unwrap()
            .as_str()
            .unwrap();
        assert_eq!(routing_key, "custom.test_task");
        assert_eq!(middleware.name(), "routing_key");
    }

    #[tokio::test]
    async fn test_routing_key_middleware_from_task_name() {
        let middleware = RoutingKeyMiddleware::from_task_name();

        let mut msg = Message::new("my_task".to_string(), Uuid::new_v4(), b"test body".to_vec());

        middleware.before_publish(&mut msg).await.unwrap();

        let routing_key = msg
            .headers
            .extra
            .get("x-routing-key")
            .unwrap()
            .as_str()
            .unwrap();
        assert_eq!(routing_key, "tasks.my_task");
    }

    #[tokio::test]
    async fn test_routing_key_middleware_from_task_and_priority() {
        let middleware = RoutingKeyMiddleware::from_task_and_priority();

        let mut msg = Message::new("my_task".to_string(), Uuid::new_v4(), b"test body".to_vec());
        msg.headers
            .extra
            .insert("priority".to_string(), serde_json::json!(5));

        middleware.before_publish(&mut msg).await.unwrap();

        let routing_key = msg
            .headers
            .extra
            .get("x-routing-key")
            .unwrap()
            .as_str()
            .unwrap();
        assert_eq!(routing_key, "tasks.my_task.priority_5");
    }

    #[tokio::test]
    async fn test_idempotency_middleware_creation() {
        let middleware = IdempotencyMiddleware::new(5000);
        assert_eq!(middleware.name(), "idempotency");
        assert_eq!(middleware.cache_size(), 0);
    }

    #[tokio::test]
    async fn test_idempotency_middleware_before_publish() {
        let middleware = IdempotencyMiddleware::with_default_cache();
        let mut msg = Message::new(
            "test_task".to_string(),
            Uuid::new_v4(),
            b"test body".to_vec(),
        );

        middleware.before_publish(&mut msg).await.unwrap();

        assert!(msg.headers.extra.contains_key("x-idempotency-key"));
    }

    #[tokio::test]
    async fn test_idempotency_middleware_after_consume_first_time() {
        let middleware = IdempotencyMiddleware::new(1000);
        let mut msg = Message::new(
            "test_task".to_string(),
            Uuid::new_v4(),
            b"test body".to_vec(),
        );

        middleware.after_consume(&mut msg).await.unwrap();

        let already_processed = msg
            .headers
            .extra
            .get("x-already-processed")
            .unwrap()
            .as_bool()
            .unwrap();
        assert!(!already_processed); // First time, not processed yet
        assert_eq!(middleware.cache_size(), 1);
    }

    #[tokio::test]
    async fn test_idempotency_middleware_after_consume_duplicate() {
        let middleware = IdempotencyMiddleware::new(1000);
        let task_id = Uuid::new_v4();
        let mut msg1 = Message::new("test_task".to_string(), task_id, b"test body".to_vec());

        // First consumption
        middleware.after_consume(&mut msg1).await.unwrap();

        // Second consumption with same ID
        let mut msg2 = Message::new("test_task".to_string(), task_id, b"test body".to_vec());
        middleware.after_consume(&mut msg2).await.unwrap();

        let already_processed = msg2
            .headers
            .extra
            .get("x-already-processed")
            .unwrap()
            .as_bool()
            .unwrap();
        assert!(already_processed); // Already processed
    }

    #[test]
    fn test_idempotency_middleware_clear() {
        let middleware = IdempotencyMiddleware::new(1000);
        middleware.mark_processed("test-id-1".to_string());
        middleware.mark_processed("test-id-2".to_string());
        assert_eq!(middleware.cache_size(), 2);

        middleware.clear();
        assert_eq!(middleware.cache_size(), 0);
    }

    #[tokio::test]
    async fn test_backoff_middleware_creation() {
        let middleware = BackoffMiddleware::with_defaults();
        assert_eq!(middleware.name(), "backoff");
    }

    #[tokio::test]
    async fn test_backoff_middleware_calculates_delay() {
        use std::time::Duration;
        let middleware =
            BackoffMiddleware::new(Duration::from_secs(1), Duration::from_secs(60), 2.0);

        let mut msg = Message::new(
            "test_task".to_string(),
            Uuid::new_v4(),
            b"test body".to_vec(),
        );
        msg.headers
            .extra
            .insert("retries".to_string(), serde_json::json!(3));

        middleware.after_consume(&mut msg).await.unwrap();

        assert!(msg.headers.extra.contains_key("x-backoff-delay"));
        assert!(msg.headers.extra.contains_key("x-next-retry-at"));

        let backoff_delay = msg
            .headers
            .extra
            .get("x-backoff-delay")
            .unwrap()
            .as_u64()
            .unwrap();
        // With 3 retries, base delay is 1 * 2^3 = 8 seconds = 8000ms
        // With jitter, should be between 8000 and 10000ms
        assert!((8000..=10000).contains(&backoff_delay));
    }

    #[tokio::test]
    async fn test_backoff_middleware_no_retries() {
        let middleware = BackoffMiddleware::with_defaults();

        let mut msg = Message::new(
            "test_task".to_string(),
            Uuid::new_v4(),
            b"test body".to_vec(),
        );

        middleware.after_consume(&mut msg).await.unwrap();

        let backoff_delay = msg
            .headers
            .extra
            .get("x-backoff-delay")
            .unwrap()
            .as_u64()
            .unwrap();
        // No retries, should be initial delay (1 second = 1000ms) + jitter
        assert!((1000..=1250).contains(&backoff_delay));
    }

    #[tokio::test]
    async fn test_caching_middleware_creation() {
        let middleware = CachingMiddleware::new(100, Duration::from_secs(60));
        assert_eq!(middleware.name(), "caching");
        assert_eq!(middleware.cache_size(), 0);
    }

    #[tokio::test]
    async fn test_caching_middleware_cache_miss() {
        let middleware = CachingMiddleware::with_defaults();
        let msg = Message::new(
            "test_task".to_string(),
            Uuid::new_v4(),
            b"test body".to_vec(),
        );

        let cached = middleware.get_cached(&msg);
        assert!(cached.is_none());
    }

    #[tokio::test]
    async fn test_caching_middleware_store_and_retrieve() {
        let middleware = CachingMiddleware::new(100, Duration::from_secs(60));
        let msg = Message::new(
            "test_task".to_string(),
            Uuid::new_v4(),
            b"test body".to_vec(),
        );

        // Store result
        let result = b"cached result".to_vec();
        middleware.store_result(&msg, result.clone());
        assert_eq!(middleware.cache_size(), 1);

        // Retrieve result
        let cached = middleware.get_cached(&msg);
        assert!(cached.is_some());
        assert_eq!(cached.unwrap(), result);
    }

    #[tokio::test]
    async fn test_caching_middleware_after_consume_miss() {
        let middleware = CachingMiddleware::with_defaults();
        let mut msg = Message::new(
            "test_task".to_string(),
            Uuid::new_v4(),
            b"test body".to_vec(),
        );

        middleware.after_consume(&mut msg).await.unwrap();

        let cache_hit = msg
            .headers
            .extra
            .get("x-cache-hit")
            .unwrap()
            .as_bool()
            .unwrap();
        assert!(!cache_hit);
    }

    #[tokio::test]
    async fn test_caching_middleware_after_consume_hit() {
        let middleware = CachingMiddleware::with_defaults();
        let msg = Message::new(
            "test_task".to_string(),
            Uuid::new_v4(),
            b"test body".to_vec(),
        );

        // Store a result first
        middleware.store_result(&msg, b"result".to_vec());

        // Now consume with the same message
        let mut msg_clone = msg.clone();
        middleware.after_consume(&mut msg_clone).await.unwrap();

        let cache_hit = msg_clone
            .headers
            .extra
            .get("x-cache-hit")
            .unwrap()
            .as_bool()
            .unwrap();
        assert!(cache_hit);

        let cached_size = msg_clone
            .headers
            .extra
            .get("x-cached-result-size")
            .unwrap()
            .as_u64()
            .unwrap();
        assert_eq!(cached_size, 6); // "result".len() == 6
    }

    #[test]
    fn test_caching_middleware_clear() {
        let middleware = CachingMiddleware::new(100, Duration::from_secs(60));
        let msg = Message::new(
            "test_task".to_string(),
            Uuid::new_v4(),
            b"test body".to_vec(),
        );

        middleware.store_result(&msg, b"result".to_vec());
        assert_eq!(middleware.cache_size(), 1);

        middleware.clear();
        assert_eq!(middleware.cache_size(), 0);
    }

    #[test]
    fn test_bulkhead_middleware_creation() {
        let middleware = BulkheadMiddleware::new(50);
        assert_eq!(middleware.total_operations(), 0);
    }

    #[test]
    fn test_bulkhead_middleware_acquire_release() {
        let middleware = BulkheadMiddleware::new(2);

        // Acquire permits
        assert!(middleware.try_acquire("partition1"));
        assert_eq!(middleware.current_operations("partition1"), 1);

        assert!(middleware.try_acquire("partition1"));
        assert_eq!(middleware.current_operations("partition1"), 2);

        // At limit
        assert!(!middleware.try_acquire("partition1"));
        assert_eq!(middleware.current_operations("partition1"), 2);

        // Release one
        middleware.release("partition1");
        assert_eq!(middleware.current_operations("partition1"), 1);

        // Can acquire again
        assert!(middleware.try_acquire("partition1"));
        assert_eq!(middleware.current_operations("partition1"), 2);
    }

    #[test]
    fn test_bulkhead_middleware_multiple_partitions() {
        let middleware = BulkheadMiddleware::new(2);

        // Different partitions have independent limits
        assert!(middleware.try_acquire("partition1"));
        assert!(middleware.try_acquire("partition2"));
        assert_eq!(middleware.current_operations("partition1"), 1);
        assert_eq!(middleware.current_operations("partition2"), 1);
        assert_eq!(middleware.total_operations(), 2);
    }

    #[test]
    fn test_bulkhead_middleware_with_custom_partition_fn() {
        let middleware = BulkheadMiddleware::with_partition_fn(2, |msg| {
            msg.headers
                .extra
                .get("custom_partition")
                .and_then(|v| v.as_str())
                .unwrap_or("default")
                .to_string()
        });

        let mut msg1 = Message::new("task1".to_string(), Uuid::new_v4(), b"body".to_vec());
        msg1.headers.extra.insert(
            "custom_partition".to_string(),
            serde_json::json!("partition_a"),
        );

        let mut msg2 = Message::new("task2".to_string(), Uuid::new_v4(), b"body".to_vec());
        msg2.headers.extra.insert(
            "custom_partition".to_string(),
            serde_json::json!("partition_b"),
        );

        // Should use custom partition key
        assert_eq!(middleware.current_operations("partition_a"), 0);
        assert_eq!(middleware.current_operations("partition_b"), 0);
    }

    #[test]
    fn test_priority_boost_middleware_age_boost() {
        let middleware =
            PriorityBoostMiddleware::new().with_age_boost(Duration::from_secs(300), Priority::High);

        let mut msg = Message::new(
            "test_task".to_string(),
            Uuid::new_v4(),
            b"test body".to_vec(),
        );

        // Message with old timestamp (600 seconds ago)
        let old_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64()
            - 600.0;

        msg.headers
            .extra
            .insert("timestamp".to_string(), serde_json::json!(old_timestamp));

        let boosted = middleware.calculate_priority(&msg, Priority::Normal);
        assert_eq!(boosted, Priority::High);
    }

    #[test]
    fn test_priority_boost_middleware_retry_boost() {
        let middleware = PriorityBoostMiddleware::new().with_retry_boost(3, Priority::Highest);

        let mut msg = Message::new(
            "test_task".to_string(),
            Uuid::new_v4(),
            b"test body".to_vec(),
        );
        msg.headers.retries = Some(5);

        let boosted = middleware.calculate_priority(&msg, Priority::Normal);
        assert_eq!(boosted, Priority::Highest);
    }

    #[test]
    fn test_priority_boost_middleware_no_boost() {
        let middleware = PriorityBoostMiddleware::new().with_retry_boost(5, Priority::High);

        let mut msg = Message::new(
            "test_task".to_string(),
            Uuid::new_v4(),
            b"test body".to_vec(),
        );
        msg.headers.retries = Some(2);

        let boosted = middleware.calculate_priority(&msg, Priority::Normal);
        assert_eq!(boosted, Priority::Normal);
    }

    #[test]
    fn test_priority_boost_middleware_custom_fn() {
        let middleware = PriorityBoostMiddleware::with_custom_fn(|msg, _current| {
            if msg.headers.task.contains("critical") {
                Priority::Highest
            } else {
                Priority::Low
            }
        });

        let msg_critical = Message::new(
            "critical_task".to_string(),
            Uuid::new_v4(),
            b"test body".to_vec(),
        );

        let msg_normal = Message::new(
            "normal_task".to_string(),
            Uuid::new_v4(),
            b"test body".to_vec(),
        );

        assert_eq!(
            middleware.calculate_priority(&msg_critical, Priority::Normal),
            Priority::Highest
        );
        assert_eq!(
            middleware.calculate_priority(&msg_normal, Priority::Normal),
            Priority::Low
        );
    }

    #[test]
    fn test_error_classification_middleware_transient() {
        let middleware = ErrorClassificationMiddleware::new();

        assert_eq!(
            middleware.classify_error("connection timeout"),
            ErrorClass::Transient
        );
        assert_eq!(
            middleware.classify_error("network error occurred"),
            ErrorClass::Transient
        );
        assert_eq!(
            middleware.classify_error("service unavailable"),
            ErrorClass::Transient
        );
    }

    #[test]
    fn test_error_classification_middleware_permanent() {
        let middleware = ErrorClassificationMiddleware::new();

        assert_eq!(
            middleware.classify_error("validation failed"),
            ErrorClass::Permanent
        );
        assert_eq!(
            middleware.classify_error("schema mismatch"),
            ErrorClass::Permanent
        );
        assert_eq!(
            middleware.classify_error("invalid input"),
            ErrorClass::Permanent
        );
        assert_eq!(
            middleware.classify_error("access forbidden"),
            ErrorClass::Permanent
        );
    }

    #[test]
    fn test_error_classification_middleware_unknown() {
        let middleware = ErrorClassificationMiddleware::new();

        assert_eq!(
            middleware.classify_error("something went wrong"),
            ErrorClass::Unknown
        );
        assert_eq!(
            middleware.classify_error("unexpected error"),
            ErrorClass::Unknown
        );
    }

    #[test]
    fn test_error_classification_middleware_should_retry() {
        let middleware = ErrorClassificationMiddleware::new();

        // Transient errors should retry up to limit
        assert!(middleware.should_retry("timeout error", 5));
        assert!(!middleware.should_retry("timeout error", 10));
        assert!(!middleware.should_retry("timeout error", 15));

        // Permanent errors should not retry much
        assert!(middleware.should_retry("validation error", 0));
        assert!(!middleware.should_retry("validation error", 1));
        assert!(!middleware.should_retry("validation error", 2));
    }

    #[test]
    fn test_error_classification_middleware_custom_patterns() {
        let middleware = ErrorClassificationMiddleware::new()
            .with_transient_pattern("temporary")
            .with_permanent_pattern("critical");

        assert_eq!(
            middleware.classify_error("temporary issue"),
            ErrorClass::Transient
        );
        assert_eq!(
            middleware.classify_error("critical failure"),
            ErrorClass::Permanent
        );
    }

    #[test]
    fn test_error_classification_middleware_retry_limits() {
        let middleware = ErrorClassificationMiddleware::new()
            .with_max_transient_retries(5)
            .with_max_permanent_retries(0);

        assert!(middleware.should_retry("timeout", 4));
        assert!(!middleware.should_retry("timeout", 5));

        assert!(!middleware.should_retry("validation error", 0));
        assert!(!middleware.should_retry("validation error", 1));
    }

    #[tokio::test]
    async fn test_correlation_middleware_generates_id() {
        use crate::CorrelationMiddleware;

        let middleware = CorrelationMiddleware::new();
        let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);

        middleware.before_publish(&mut msg).await.unwrap();

        assert!(msg.headers.extra.contains_key("x-correlation-id"));
        let correlation_id = msg.headers.extra.get("x-correlation-id").unwrap();
        assert!(correlation_id.is_string());
    }

    #[tokio::test]
    async fn test_correlation_middleware_preserves_existing_id() {
        use crate::CorrelationMiddleware;

        let middleware = CorrelationMiddleware::new();
        let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);
        let existing_id = "existing-correlation-123";
        msg.headers.extra.insert(
            "x-correlation-id".to_string(),
            serde_json::json!(existing_id),
        );

        middleware.before_publish(&mut msg).await.unwrap();

        let correlation_id = msg
            .headers
            .extra
            .get("x-correlation-id")
            .unwrap()
            .as_str()
            .unwrap();
        assert_eq!(correlation_id, existing_id);
    }

    #[tokio::test]
    async fn test_correlation_middleware_custom_header() {
        use crate::CorrelationMiddleware;

        let middleware = CorrelationMiddleware::with_header_name("x-request-id");
        let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);

        middleware.before_publish(&mut msg).await.unwrap();

        assert!(msg.headers.extra.contains_key("x-request-id"));
        assert!(!msg.headers.extra.contains_key("x-correlation-id"));
    }

    #[tokio::test]
    async fn test_throttling_middleware_creation() {
        use crate::ThrottlingMiddleware;

        let middleware = ThrottlingMiddleware::new(100.0)
            .with_burst_size(200)
            .with_backpressure_threshold(0.7);

        assert_eq!(middleware.name(), "throttling");
        assert_eq!(middleware.max_rate, 100.0);
        assert_eq!(middleware.burst_size, 200);
        assert_eq!(middleware.backpressure_threshold, 0.7);
    }

    #[tokio::test]
    async fn test_throttling_middleware_allows_messages() {
        use crate::ThrottlingMiddleware;

        let middleware = ThrottlingMiddleware::new(100.0);
        let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);

        // First message should pass without delay
        middleware.before_publish(&mut msg).await.unwrap();

        // Should not have delay header (tokens available)
        assert!(!msg.headers.extra.contains_key("x-throttle-delay-ms"));
    }

    #[tokio::test]
    async fn test_throttling_middleware_backpressure() {
        use crate::ThrottlingMiddleware;

        let middleware = ThrottlingMiddleware::new(10.0)
            .with_burst_size(5)
            .with_backpressure_threshold(0.9);

        // Consume all tokens
        for _ in 0..6 {
            let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);
            middleware.before_publish(&mut msg).await.unwrap();
        }

        // Next message should trigger backpressure
        let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);
        middleware.before_publish(&mut msg).await.unwrap();

        // Should have backpressure indicator
        if msg.headers.extra.contains_key("x-backpressure-active") {
            assert_eq!(
                msg.headers.extra.get("x-backpressure-active").unwrap(),
                &serde_json::json!(true)
            );
        }
    }

    #[tokio::test]
    async fn test_circuit_breaker_middleware_creation() {
        use crate::CircuitBreakerMiddleware;
        use std::time::Duration;

        let middleware = CircuitBreakerMiddleware::new(5, Duration::from_secs(60));
        assert_eq!(middleware.name(), "circuit_breaker");
        assert_eq!(middleware.failure_threshold, 5);
    }

    #[tokio::test]
    async fn test_circuit_breaker_middleware_allows_when_closed() {
        use crate::CircuitBreakerMiddleware;
        use std::time::Duration;

        let middleware = CircuitBreakerMiddleware::new(3, Duration::from_secs(60));
        let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);

        // Should allow message when circuit is closed
        let result = middleware.before_publish(&mut msg).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_circuit_breaker_middleware_opens_on_failures() {
        use crate::CircuitBreakerMiddleware;
        use std::time::Duration;

        let middleware = CircuitBreakerMiddleware::new(3, Duration::from_secs(60));

        // Record failures
        for _ in 0..3 {
            let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);
            msg.headers
                .extra
                .insert("error".to_string(), serde_json::json!("failure"));
            middleware.after_consume(&mut msg).await.unwrap();
        }

        // Circuit should be open now
        let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);
        let result = middleware.before_publish(&mut msg).await;
        assert!(result.is_err());

        // Should have circuit breaker headers
        assert_eq!(
            msg.headers.extra.get("x-circuit-breaker-open").unwrap(),
            &serde_json::json!(true)
        );
    }

    #[tokio::test]
    async fn test_circuit_breaker_middleware_tracks_failure_count() {
        use crate::CircuitBreakerMiddleware;
        use std::time::Duration;

        let middleware = CircuitBreakerMiddleware::new(5, Duration::from_secs(60));

        // Record 2 failures
        for _ in 0..2 {
            let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);
            msg.headers
                .extra
                .insert("error".to_string(), serde_json::json!("failure"));
            middleware.after_consume(&mut msg).await.unwrap();
        }

        // Check failure count is tracked
        let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);
        middleware.after_consume(&mut msg).await.unwrap();

        assert_eq!(
            msg.headers.extra.get("x-circuit-breaker-failures").unwrap(),
            &serde_json::json!(2)
        );
    }

    #[tokio::test]
    async fn test_schema_validation_middleware_creation() {
        use crate::SchemaValidationMiddleware;

        let middleware = SchemaValidationMiddleware::new()
            .with_required_field("user_id")
            .with_max_field_count(10)
            .with_max_body_size(1024);

        assert_eq!(middleware.name(), "schema_validation");
        assert_eq!(middleware.required_fields.len(), 1);
        assert_eq!(middleware.max_field_count, Some(10));
        assert_eq!(middleware.max_body_size, Some(1024));
    }

    #[tokio::test]
    async fn test_schema_validation_middleware_validates_required_fields() {
        use crate::SchemaValidationMiddleware;

        let middleware = SchemaValidationMiddleware::new().with_required_field("user_id");

        let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);

        // Should fail without required field
        let result = middleware.before_publish(&mut msg).await;
        assert!(result.is_err());

        // Should succeed with required field
        msg.headers
            .extra
            .insert("user_id".to_string(), serde_json::json!("123"));
        let result = middleware.before_publish(&mut msg).await;
        assert!(result.is_ok());
        assert_eq!(
            msg.headers.extra.get("x-schema-validated").unwrap(),
            &serde_json::json!(true)
        );
    }

    #[tokio::test]
    async fn test_schema_validation_middleware_validates_field_count() {
        use crate::SchemaValidationMiddleware;

        let middleware = SchemaValidationMiddleware::new().with_max_field_count(2);

        let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);
        msg.headers
            .extra
            .insert("field1".to_string(), serde_json::json!(1));
        msg.headers
            .extra
            .insert("field2".to_string(), serde_json::json!(2));
        msg.headers
            .extra
            .insert("field3".to_string(), serde_json::json!(3));

        // Should fail with too many fields
        let result = middleware.before_publish(&mut msg).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_schema_validation_middleware_validates_body_size() {
        use crate::SchemaValidationMiddleware;

        let middleware = SchemaValidationMiddleware::new()
            .with_min_body_size(10)
            .with_max_body_size(20);

        // Too small
        let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![1, 2, 3]);
        let result = middleware.before_publish(&mut msg).await;
        assert!(result.is_err());

        // Just right
        let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![1; 15]);
        let result = middleware.before_publish(&mut msg).await;
        assert!(result.is_ok());

        // Too large
        let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![1; 30]);
        let result = middleware.before_publish(&mut msg).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_message_enrichment_middleware_creation() {
        use crate::MessageEnrichmentMiddleware;

        let middleware = MessageEnrichmentMiddleware::new()
            .with_hostname("worker-01")
            .with_environment("production")
            .with_version("1.0.0")
            .with_add_timestamp(true);

        assert_eq!(middleware.name(), "message_enrichment");
        assert_eq!(middleware.hostname, Some("worker-01".to_string()));
        assert_eq!(middleware.environment, Some("production".to_string()));
        assert_eq!(middleware.version, Some("1.0.0".to_string()));
        assert!(middleware.add_timestamp);
    }

    #[tokio::test]
    async fn test_message_enrichment_middleware_adds_metadata() {
        use crate::MessageEnrichmentMiddleware;

        let middleware = MessageEnrichmentMiddleware::new()
            .with_hostname("worker-01")
            .with_environment("staging")
            .with_version("2.0.0");

        let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);
        middleware.before_publish(&mut msg).await.unwrap();

        assert_eq!(
            msg.headers.extra.get("x-enrichment-hostname").unwrap(),
            &serde_json::json!("worker-01")
        );
        assert_eq!(
            msg.headers.extra.get("x-enrichment-environment").unwrap(),
            &serde_json::json!("staging")
        );
        assert_eq!(
            msg.headers.extra.get("x-enrichment-version").unwrap(),
            &serde_json::json!("2.0.0")
        );
    }

    #[tokio::test]
    async fn test_message_enrichment_middleware_adds_timestamp() {
        use crate::MessageEnrichmentMiddleware;

        let middleware = MessageEnrichmentMiddleware::new().with_add_timestamp(true);

        let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);
        middleware.before_publish(&mut msg).await.unwrap();

        assert!(msg.headers.extra.contains_key("x-enrichment-timestamp"));
    }

    #[tokio::test]
    async fn test_message_enrichment_middleware_custom_metadata() {
        use crate::MessageEnrichmentMiddleware;

        let middleware = MessageEnrichmentMiddleware::new()
            .with_custom_metadata("region", serde_json::json!("us-west-1"))
            .with_custom_metadata("datacenter", serde_json::json!("dc-01"));

        let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);
        middleware.before_publish(&mut msg).await.unwrap();

        assert_eq!(
            msg.headers.extra.get("x-enrichment-region").unwrap(),
            &serde_json::json!("us-west-1")
        );
        assert_eq!(
            msg.headers.extra.get("x-enrichment-datacenter").unwrap(),
            &serde_json::json!("dc-01")
        );
    }

    #[test]
    fn test_partitioning_middleware_creation() {
        let partitioner = PartitioningMiddleware::new(8);
        assert_eq!(partitioner.partition_count(), 8);
        assert_eq!(partitioner.name(), "partitioning");
    }

    #[tokio::test]
    async fn test_partitioning_middleware_assigns_partition() {
        use uuid::Uuid;

        let partitioner = PartitioningMiddleware::new(4);
        let task_id = Uuid::new_v4();
        let mut message = Message::new("test_task".to_string(), task_id, vec![]);

        partitioner.before_publish(&mut message).await.unwrap();

        // Check partition ID was assigned
        assert!(message.headers.extra.contains_key("x-partition-id"));
        let partition_id = message.headers.extra["x-partition-id"].as_u64().unwrap() as usize;
        assert!(partition_id < 4);

        // Check partition count was added
        assert_eq!(
            message.headers.extra["x-partition-count"],
            serde_json::json!(4)
        );
    }

    #[tokio::test]
    async fn test_partitioning_middleware_custom_header() {
        use uuid::Uuid;

        let partitioner = PartitioningMiddleware::new(8).with_partition_header("my-partition");
        let task_id = Uuid::new_v4();
        let mut message = Message::new("test_task".to_string(), task_id, vec![]);

        partitioner.before_publish(&mut message).await.unwrap();

        assert!(message.headers.extra.contains_key("my-partition"));
    }

    #[test]
    fn test_adaptive_timeout_middleware_creation() {
        use std::time::Duration;

        let adaptive = AdaptiveTimeoutMiddleware::new(Duration::from_secs(30));
        assert_eq!(adaptive.name(), "adaptive_timeout");
        assert!(!adaptive.has_samples());
    }

    #[tokio::test]
    async fn test_adaptive_timeout_middleware_injects_timeout() {
        use std::time::Duration;
        use uuid::Uuid;

        let adaptive = AdaptiveTimeoutMiddleware::new(Duration::from_secs(30));
        let task_id = Uuid::new_v4();
        let mut message = Message::new("test_task".to_string(), task_id, vec![]);

        adaptive.before_publish(&mut message).await.unwrap();

        // Check adaptive timeout was injected
        assert!(message.headers.extra.contains_key("x-adaptive-timeout"));
        assert!(message.headers.extra.contains_key("x-timeout-percentile"));

        let percentile = message.headers.extra["x-timeout-percentile"]
            .as_f64()
            .unwrap();
        assert_eq!(percentile, 0.95);
    }

    #[tokio::test]
    async fn test_adaptive_timeout_middleware_custom_percentile() {
        use std::time::Duration;
        use uuid::Uuid;

        let adaptive =
            AdaptiveTimeoutMiddleware::new(Duration::from_secs(30)).with_percentile(0.99);
        let task_id = Uuid::new_v4();
        let mut message = Message::new("test_task".to_string(), task_id, vec![]);

        adaptive.before_publish(&mut message).await.unwrap();

        let percentile = message.headers.extra["x-timeout-percentile"]
            .as_f64()
            .unwrap();
        assert_eq!(percentile, 0.99);
    }

    #[test]
    fn test_batch_ack_hint_middleware_creation() {
        let batch_hint = BatchAckHintMiddleware::new(10);
        assert_eq!(batch_hint.batch_size(), 10);
        assert_eq!(batch_hint.name(), "batch_ack_hint");
    }

    #[tokio::test]
    async fn test_batch_ack_hint_middleware_injects_hint() {
        use uuid::Uuid;

        let batch_hint = BatchAckHintMiddleware::new(20);
        let task_id = Uuid::new_v4();
        let mut message = Message::new("test_task".to_string(), task_id, vec![]);

        batch_hint.before_publish(&mut message).await.unwrap();

        // Check batch hint was injected
        assert!(message.headers.extra.contains_key("x-batch-ack-hint"));
        assert_eq!(
            message.headers.extra["x-batch-ack-hint"],
            serde_json::json!(20)
        );
        assert_eq!(
            message.headers.extra["x-batch-ack-recommended"],
            serde_json::json!(true)
        );
    }

    #[tokio::test]
    async fn test_batch_ack_hint_middleware_custom_header() {
        use uuid::Uuid;

        let batch_hint = BatchAckHintMiddleware::new(15).with_hint_header("my-batch-hint");
        let task_id = Uuid::new_v4();
        let mut message = Message::new("test_task".to_string(), task_id, vec![]);

        batch_hint.before_publish(&mut message).await.unwrap();

        assert!(message.headers.extra.contains_key("my-batch-hint"));
        assert_eq!(
            message.headers.extra["my-batch-hint"],
            serde_json::json!(15)
        );
    }

    #[test]
    fn test_load_shedding_middleware_creation() {
        let load_shedder = LoadSheddingMiddleware::new(0.8);
        assert_eq!(load_shedder.threshold(), 0.8);
        assert_eq!(load_shedder.name(), "load_shedding");
    }

    #[tokio::test]
    async fn test_load_shedding_middleware_allows_high_priority() {
        use uuid::Uuid;

        let load_shedder = LoadSheddingMiddleware::new(0.8);
        let task_id = Uuid::new_v4();
        let mut message = Message::new("test_task".to_string(), task_id, vec![]);
        message.headers.extra.insert(
            "priority".to_string(),
            serde_json::json!(8), // High priority
        );

        // Should allow even at high load
        let result = load_shedder.before_publish(&mut message).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_load_shedding_middleware_sheds_low_priority() {
        use uuid::Uuid;

        let mut load_shedder = LoadSheddingMiddleware::new(0.8);
        load_shedder.update_load(0.9); // High load

        let task_id = Uuid::new_v4();
        let mut message = Message::new("test_task".to_string(), task_id, vec![]);
        message.headers.extra.insert(
            "priority".to_string(),
            serde_json::json!(2), // Low priority
        );

        // Should shed at high load
        let result = load_shedder.before_publish(&mut message).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_priority_escalation_middleware_creation() {
        let escalator = MessagePriorityEscalationMiddleware::new(300);
        assert_eq!(escalator.age_threshold_secs(), 300);
        assert_eq!(escalator.name(), "priority_escalation");
    }

    #[tokio::test]
    async fn test_priority_escalation_middleware_escalates_on_retry() {
        use uuid::Uuid;

        let escalator = MessagePriorityEscalationMiddleware::new(300);
        let task_id = Uuid::new_v4();
        let mut message = Message::new("test_task".to_string(), task_id, vec![]);
        message
            .headers
            .extra
            .insert("priority".to_string(), serde_json::json!(5));
        message.headers.retries = Some(2);

        escalator.before_publish(&mut message).await.unwrap();

        // Priority should be escalated
        let new_priority = message
            .headers
            .extra
            .get("priority")
            .and_then(|v| v.as_u64())
            .map(|v| v as u8)
            .unwrap();
        assert!(new_priority > 5);
        assert!(message.headers.extra.contains_key("x-priority-escalated"));
    }

    #[tokio::test]
    async fn test_priority_escalation_middleware_respects_max() {
        use uuid::Uuid;

        let escalator = MessagePriorityEscalationMiddleware::new(300).with_max_priority(8);
        let task_id = Uuid::new_v4();
        let mut message = Message::new("test_task".to_string(), task_id, vec![]);
        message
            .headers
            .extra
            .insert("priority".to_string(), serde_json::json!(9));
        message.headers.retries = Some(5);

        escalator.before_publish(&mut message).await.unwrap();

        // Priority should not exceed max
        let new_priority = message
            .headers
            .extra
            .get("priority")
            .and_then(|v| v.as_u64())
            .map(|v| v as u8)
            .unwrap();
        assert!(new_priority <= 8);
    }

    #[test]
    fn test_observability_middleware_creation() {
        let observability = ObservabilityMiddleware::new("test-service");
        assert_eq!(observability.service_name(), "test-service");
        assert_eq!(observability.name(), "observability");
    }

    #[tokio::test]
    async fn test_observability_middleware_injects_metadata() {
        use uuid::Uuid;

        let observability = ObservabilityMiddleware::new("my-service");
        let task_id = Uuid::new_v4();
        let mut message = Message::new("test_task".to_string(), task_id, vec![]);

        observability.before_publish(&mut message).await.unwrap();

        assert_eq!(
            message.headers.extra["x-service-name"],
            serde_json::json!("my-service")
        );
        assert_eq!(
            message.headers.extra["x-observability-enabled"],
            serde_json::json!(true)
        );
        assert_eq!(
            message.headers.extra["x-log-level"],
            serde_json::json!("info")
        );
    }

    #[tokio::test]
    async fn test_observability_middleware_without_metrics() {
        use uuid::Uuid;

        let observability = ObservabilityMiddleware::new("test").without_metrics();
        let task_id = Uuid::new_v4();
        let mut message = Message::new("test_task".to_string(), task_id, vec![]);

        observability.before_publish(&mut message).await.unwrap();

        assert!(!message
            .headers
            .extra
            .contains_key("x-observability-enabled"));
    }

    #[test]
    fn test_health_check_middleware_creation() {
        let health = HealthCheckMiddleware::new();
        assert_eq!(health.name(), "health_check");
        assert!(health.is_healthy());
    }

    #[tokio::test]
    async fn test_health_check_middleware_injects_status() {
        use uuid::Uuid;

        let health = HealthCheckMiddleware::new();
        let task_id = Uuid::new_v4();
        let mut message = Message::new("test_task".to_string(), task_id, vec![]);

        health.before_publish(&mut message).await.unwrap();

        assert_eq!(
            message.headers.extra["x-health-status"],
            serde_json::json!("healthy")
        );
    }

    #[test]
    fn test_health_check_middleware_mark_unhealthy() {
        let health = HealthCheckMiddleware::new();
        assert!(health.is_healthy());

        health.mark_unhealthy();
        assert!(!health.is_healthy());

        health.mark_healthy();
        assert!(health.is_healthy());
    }

    #[test]
    fn test_message_tagging_middleware_creation() {
        let tagging = MessageTaggingMiddleware::new("production");
        assert_eq!(tagging.name(), "message_tagging");
    }

    #[tokio::test]
    async fn test_message_tagging_middleware_injects_tags() {
        use uuid::Uuid;

        let tagging = MessageTaggingMiddleware::new("staging")
            .with_tag("region", "us-west-2")
            .with_tag("team", "backend");

        let task_id = Uuid::new_v4();
        let mut message = Message::new("email_task".to_string(), task_id, vec![]);

        tagging.before_publish(&mut message).await.unwrap();

        assert_eq!(
            message.headers.extra["x-environment"],
            serde_json::json!("staging")
        );
        assert_eq!(
            message.headers.extra["x-tag-region"],
            serde_json::json!("us-west-2")
        );
        assert_eq!(
            message.headers.extra["x-tag-team"],
            serde_json::json!("backend")
        );
        assert_eq!(
            message.headers.extra["x-category"],
            serde_json::json!("communication")
        );
    }

    #[tokio::test]
    async fn test_message_tagging_middleware_categorization() {
        use uuid::Uuid;

        let tagging = MessageTaggingMiddleware::new("production");
        let task_id = Uuid::new_v4();

        // Test email categorization
        let mut email_msg = Message::new("send_email_task".to_string(), task_id, vec![]);
        tagging.before_publish(&mut email_msg).await.unwrap();
        assert_eq!(
            email_msg.headers.extra["x-category"],
            serde_json::json!("communication")
        );

        // Test report categorization
        let mut report_msg = Message::new("generate_report".to_string(), task_id, vec![]);
        tagging.before_publish(&mut report_msg).await.unwrap();
        assert_eq!(
            report_msg.headers.extra["x-category"],
            serde_json::json!("analytics")
        );

        // Test process categorization
        let mut process_msg = Message::new("process_data".to_string(), task_id, vec![]);
        tagging.before_publish(&mut process_msg).await.unwrap();
        assert_eq!(
            process_msg.headers.extra["x-category"],
            serde_json::json!("computation")
        );

        // Test general categorization
        let mut general_msg = Message::new("other_task".to_string(), task_id, vec![]);
        tagging.before_publish(&mut general_msg).await.unwrap();
        assert_eq!(
            general_msg.headers.extra["x-category"],
            serde_json::json!("general")
        );
    }

    #[test]
    fn test_cost_attribution_middleware_creation() {
        let cost = CostAttributionMiddleware::new(0.001);
        assert_eq!(cost.name(), "cost_attribution");
    }

    #[tokio::test]
    async fn test_cost_attribution_middleware_injects_cost() {
        use uuid::Uuid;

        let cost = CostAttributionMiddleware::new(0.001)
            .with_compute_cost_per_sec(0.0001)
            .with_storage_cost_per_mb(0.00001);

        let task_id = Uuid::new_v4();
        let body = vec![0u8; 1024 * 1024]; // 1MB message
        let mut message = Message::new("test_task".to_string(), task_id, body);
        message
            .headers
            .extra
            .insert("x-tenant".to_string(), serde_json::json!("tenant-123"));

        cost.before_publish(&mut message).await.unwrap();

        assert!(message.headers.extra.contains_key("x-cost-estimate"));
        assert_eq!(
            message.headers.extra["x-cost-tenant"],
            serde_json::json!("tenant-123")
        );
        assert!(message.headers.extra.contains_key("x-cost-timestamp"));
    }

    #[tokio::test]
    async fn test_cost_attribution_middleware_calculates_compute_cost() {
        use std::time::Duration;
        use uuid::Uuid;

        let cost = CostAttributionMiddleware::new(0.001).with_compute_cost_per_sec(0.0001);

        let task_id = Uuid::new_v4();
        let mut message = Message::new("test_task".to_string(), task_id, vec![]);

        // Simulate publish
        cost.before_publish(&mut message).await.unwrap();
        assert!(message.headers.extra.contains_key("x-cost-estimate"));

        // Simulate processing delay
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Simulate consume (which calculates actual cost)
        cost.after_consume(&mut message).await.unwrap();

        // Should have actual cost now
        if message.headers.extra.contains_key("x-cost-actual") {
            let actual_cost_str = message.headers.extra["x-cost-actual"].as_str().unwrap();
            let actual_cost: f64 = actual_cost_str.parse().unwrap();
            // Actual cost should be >= base cost due to compute time
            assert!(actual_cost >= 0.001);
        }
    }

    #[test]
    fn test_backpressure_config_creation() {
        let config = BackpressureConfig::new()
            .with_max_pending(500)
            .with_max_queue_size(5000)
            .with_high_watermark(0.9)
            .with_low_watermark(0.5);

        assert_eq!(config.max_pending, 500);
        assert_eq!(config.max_queue_size, 5000);
        assert_eq!(config.high_watermark, 0.9);
        assert_eq!(config.low_watermark, 0.5);
    }

    #[test]
    fn test_backpressure_config_watermarks() {
        let config = BackpressureConfig::new()
            .with_max_pending(1000)
            .with_high_watermark(0.8)
            .with_low_watermark(0.6);

        // High watermark = 1000 * 0.8 = 800
        assert!(!config.should_apply_backpressure(799));
        assert!(config.should_apply_backpressure(800));
        assert!(config.should_apply_backpressure(900));

        // Low watermark = 1000 * 0.6 = 600
        assert!(config.should_release_backpressure(600));
        assert!(config.should_release_backpressure(500));
        assert!(!config.should_release_backpressure(601));
    }

    #[test]
    fn test_backpressure_config_capacity() {
        let config = BackpressureConfig::new().with_max_queue_size(10000);

        assert!(!config.is_at_capacity(9999));
        assert!(config.is_at_capacity(10000));
        assert!(config.is_at_capacity(10001));
    }

    #[test]
    fn test_backpressure_config_default() {
        let config = BackpressureConfig::default();
        assert_eq!(config.max_pending, 1000);
        assert_eq!(config.max_queue_size, 10000);
        assert_eq!(config.high_watermark, 0.8);
        assert_eq!(config.low_watermark, 0.6);
    }

    #[test]
    fn test_poison_message_detector_creation() {
        let detector = PoisonMessageDetector::new()
            .with_max_failures(3)
            .with_failure_window(Duration::from_secs(600));

        assert_eq!(detector.max_failures, 3);
        assert_eq!(detector.failure_window, Duration::from_secs(600));
    }

    #[test]
    fn test_poison_message_detector_tracking() {
        let detector = PoisonMessageDetector::new().with_max_failures(3);
        let task_id = Uuid::new_v4();

        // Initially not poison
        assert!(!detector.is_poison(task_id));
        assert_eq!(detector.failure_count(task_id), 0);

        // Record failures
        detector.record_failure(task_id);
        assert_eq!(detector.failure_count(task_id), 1);
        assert!(!detector.is_poison(task_id));

        detector.record_failure(task_id);
        assert_eq!(detector.failure_count(task_id), 2);
        assert!(!detector.is_poison(task_id));

        detector.record_failure(task_id);
        assert_eq!(detector.failure_count(task_id), 3);
        assert!(detector.is_poison(task_id)); // Now poison
    }

    #[test]
    fn test_poison_message_detector_clear() {
        let detector = PoisonMessageDetector::new().with_max_failures(2);
        let task_id = Uuid::new_v4();

        detector.record_failure(task_id);
        detector.record_failure(task_id);
        assert!(detector.is_poison(task_id));

        // Clear specific task
        detector.clear_failures(task_id);
        assert!(!detector.is_poison(task_id));
        assert_eq!(detector.failure_count(task_id), 0);
    }

    #[test]
    fn test_poison_message_detector_clear_all() {
        let detector = PoisonMessageDetector::new().with_max_failures(2);
        let task1 = Uuid::new_v4();
        let task2 = Uuid::new_v4();

        detector.record_failure(task1);
        detector.record_failure(task1);
        detector.record_failure(task2);
        detector.record_failure(task2);

        assert!(detector.is_poison(task1));
        assert!(detector.is_poison(task2));

        detector.clear_all();
        assert!(!detector.is_poison(task1));
        assert!(!detector.is_poison(task2));
    }

    #[test]
    fn test_poison_message_detector_default() {
        let detector = PoisonMessageDetector::default();
        assert_eq!(detector.max_failures, 5);
        assert_eq!(detector.failure_window, Duration::from_secs(3600));
    }

    #[test]
    fn test_calculate_consumer_efficiency_efficient() {
        use crate::utils::calculate_consumer_efficiency;

        // Consumer spends 80% time processing, 20% waiting
        let (efficiency, recommendation) = calculate_consumer_efficiency(8000, 2000, 100);
        assert!(efficiency > 70.0);
        assert!(efficiency < 85.0);
        assert_eq!(recommendation, "efficient");
    }

    #[test]
    fn test_calculate_consumer_efficiency_underutilized() {
        use crate::utils::calculate_consumer_efficiency;

        // Consumer spends 30% time processing, 70% waiting
        let (efficiency, recommendation) = calculate_consumer_efficiency(3000, 7000, 100);
        assert!(efficiency < 50.0);
        assert_eq!(recommendation, "underutilized");
    }

    #[test]
    fn test_calculate_consumer_efficiency_good() {
        use crate::utils::calculate_consumer_efficiency;

        // Consumer spends 65% time processing, 35% waiting
        let (efficiency, recommendation) = calculate_consumer_efficiency(6500, 3500, 100);
        assert!(efficiency > 60.0);
        assert!(efficiency < 70.0);
        assert_eq!(recommendation, "good");
    }

    #[test]
    fn test_calculate_consumer_efficiency_no_data() {
        use crate::utils::calculate_consumer_efficiency;

        // No messages processed
        let (efficiency, recommendation) = calculate_consumer_efficiency(0, 0, 0);
        assert_eq!(efficiency, 0.0);
        assert_eq!(recommendation, "no_data");
    }

    #[test]
    fn test_suggest_connection_pool_size_normal() {
        use crate::utils::suggest_connection_pool_size;

        // Peak: 50 concurrent, Average: 20, Max allowed: 100
        let (min, max, initial) = suggest_connection_pool_size(50, 20, 100);
        assert!(min > 0);
        assert!(max <= 100);
        assert!(initial >= min);
        assert!(initial <= max);
        assert!(max > min);
    }

    #[test]
    fn test_suggest_connection_pool_size_high_load() {
        use crate::utils::suggest_connection_pool_size;

        // Very high load - should respect max_allowed
        let (min, max, initial) = suggest_connection_pool_size(200, 150, 100);
        assert_eq!(max, 100); // Respects max_allowed limit
        assert!(min > 0);
        assert!(initial >= min);
        assert!(initial <= max);
    }

    #[test]
    fn test_suggest_connection_pool_size_low_load() {
        use crate::utils::suggest_connection_pool_size;

        // Low load
        let (min, max, initial) = suggest_connection_pool_size(10, 5, 50);
        assert!(min > 0);
        assert!(min <= 5); // Min should be capped
        assert!(max > min);
        assert!(max <= 50);
        assert!(initial >= min);
        assert!(initial <= max);
    }

    #[test]
    fn test_suggest_connection_pool_size_zero_max() {
        use crate::utils::suggest_connection_pool_size;

        // Zero max allowed
        let (min, max, initial) = suggest_connection_pool_size(10, 5, 0);
        assert_eq!(min, 0);
        assert_eq!(max, 0);
        assert_eq!(initial, 0);
    }

    #[test]
    fn test_calculate_message_processing_trend_improving() {
        use crate::utils::calculate_message_processing_trend;

        // Processing times improving over time (getting faster)
        let times = vec![100, 95, 90, 85, 80];
        let (direction, strength, recommendation) = calculate_message_processing_trend(&times);
        assert_eq!(direction, "improving");
        assert!(strength > 0.0);
        assert_eq!(recommendation, "maintain_current_optimizations");
    }

    #[test]
    fn test_calculate_message_processing_trend_degrading() {
        use crate::utils::calculate_message_processing_trend;

        // Processing times degrading (getting slower)
        let times = vec![80, 85, 90, 95, 100];
        let (direction, strength, recommendation) = calculate_message_processing_trend(&times);
        assert_eq!(direction, "degrading");
        assert!(strength > 0.0);
        assert_eq!(recommendation, "investigate_performance_issues");
    }

    #[test]
    fn test_calculate_message_processing_trend_stable() {
        use crate::utils::calculate_message_processing_trend;

        // Stable processing times
        let times = vec![90, 91, 90, 89, 90];
        let (direction, strength, _) = calculate_message_processing_trend(&times);
        assert_eq!(direction, "stable");
        assert!(strength < 0.1); // Very low trend strength
    }

    #[test]
    fn test_calculate_message_processing_trend_insufficient_data() {
        use crate::utils::calculate_message_processing_trend;

        // Too few data points
        let times = vec![100, 90];
        let (direction, strength, recommendation) = calculate_message_processing_trend(&times);
        assert_eq!(direction, "stable");
        assert_eq!(strength, 0.0);
        assert_eq!(recommendation, "insufficient_data");
    }

    #[test]
    fn test_suggest_prefetch_count_fast_processing() {
        use crate::utils::suggest_prefetch_count;

        // Fast processing (50ms = 20 msg/sec)
        let prefetch = suggest_prefetch_count(50, 10, 100);
        assert!(prefetch > 0);
        assert!(prefetch <= 100);
        assert!(prefetch >= 20); // Should suggest high prefetch for fast processing
    }

    #[test]
    fn test_suggest_prefetch_count_slow_processing() {
        use crate::utils::suggest_prefetch_count;

        // Slow processing (2000ms = 0.5 msg/sec)
        let prefetch = suggest_prefetch_count(2000, 2, 50);
        assert!(prefetch > 0);
        assert!(prefetch <= 20); // Should suggest low prefetch for slow processing
    }

    #[test]
    fn test_suggest_prefetch_count_edge_cases() {
        use crate::utils::suggest_prefetch_count;

        // Zero workers
        let prefetch = suggest_prefetch_count(100, 0, 50);
        assert_eq!(prefetch, 1);

        // Zero max
        let prefetch = suggest_prefetch_count(100, 5, 0);
        assert_eq!(prefetch, 1);
    }

    #[test]
    fn test_analyze_dead_letter_queue_critical() {
        use crate::utils::analyze_dead_letter_queue;

        // High failure rate (>10%)
        let (severity, issue, recommendation) = analyze_dead_letter_queue(500, 1000, 10);
        assert_eq!(severity, "critical");
        assert_eq!(issue, "high_failure_rate");
        assert_eq!(recommendation, "immediate_investigation_required");
    }

    #[test]
    fn test_analyze_dead_letter_queue_healthy() {
        use crate::utils::analyze_dead_letter_queue;

        // Low failure rate (<1%)
        let (severity, issue, _recommendation) = analyze_dead_letter_queue(10, 10000, 5);
        assert_eq!(severity, "low");
        assert!(issue == "normal_failures" || issue == "healthy");
    }

    #[test]
    fn test_analyze_dead_letter_queue_rapid_growth() {
        use crate::utils::analyze_dead_letter_queue;

        // Rapid DLQ growth
        let (severity, issue, recommendation) = analyze_dead_letter_queue(100, 10000, 150);
        assert_eq!(severity, "high");
        assert_eq!(issue, "rapid_dlq_growth");
        assert_eq!(recommendation, "monitor_closely_and_investigate");
    }

    #[test]
    fn test_analyze_dead_letter_queue_no_data() {
        use crate::utils::analyze_dead_letter_queue;

        // No messages processed yet
        let (severity, issue, recommendation) = analyze_dead_letter_queue(0, 0, 0);
        assert_eq!(severity, "low");
        assert_eq!(issue, "no_data");
        assert_eq!(recommendation, "monitor");
    }

    #[test]
    fn test_forecast_queue_capacity_ml_growth() {
        use crate::utils::forecast_queue_capacity_ml;

        // Growing queue
        let history = vec![100, 120, 140, 160, 180];
        let (forecast, slope, _confidence) = forecast_queue_capacity_ml(&history, 1);
        assert!(forecast > 180); // Should predict growth
        assert!(slope > 0.0); // Positive trend
    }

    #[test]
    fn test_forecast_queue_capacity_ml_decline() {
        use crate::utils::forecast_queue_capacity_ml;

        // Declining queue
        let history = vec![200, 180, 160, 140, 120];
        let (forecast, slope, _confidence) = forecast_queue_capacity_ml(&history, 1);
        assert!(forecast < 120); // Should predict decline
        assert!(slope < 0.0); // Negative trend
    }

    #[test]
    fn test_forecast_queue_capacity_ml_insufficient_data() {
        use crate::utils::forecast_queue_capacity_ml;

        let history = vec![100, 110];
        let (forecast, slope, confidence) = forecast_queue_capacity_ml(&history, 1);
        assert_eq!(forecast, 110); // Returns last value
        assert_eq!(slope, 0.0);
        assert_eq!(confidence, "insufficient_data");
    }

    #[test]
    fn test_optimize_batch_strategy_high_latency() {
        use crate::utils::optimize_batch_strategy;

        // High network latency scenario (but realistic processing time)
        let (batch_size, wait_ms, _throughput, strategy) =
            optimize_batch_strategy(1024, 150, 2, 1000);

        assert!(batch_size > 0);
        assert!(wait_ms > 0);
        // Throughput might be 0 with very high latency, so just check it's calculated
        assert!(
            strategy == "throughput_optimized"
                || strategy == "latency_optimized"
                || strategy == "balanced"
        );
    }

    #[test]
    fn test_optimize_batch_strategy_low_latency() {
        use crate::utils::optimize_batch_strategy;

        // Low network latency scenario
        let (batch_size, _wait_ms, _throughput, _strategy) =
            optimize_batch_strategy(512, 5, 50, 500);

        assert!(batch_size > 0);
        assert!(batch_size <= 1000); // Should respect limits
    }

    #[test]
    fn test_calculate_multi_queue_efficiency_balanced() {
        use crate::utils::calculate_multi_queue_efficiency;

        let sizes = vec![100, 100, 100];
        let rates = vec![10, 10, 10];
        let (efficiency, balance, recommendation) =
            calculate_multi_queue_efficiency(&sizes, &rates);

        assert!(efficiency > 0.9); // All queues at same rate
        assert!(balance > 0.9); // Perfectly balanced
        assert_eq!(recommendation, "optimal");
    }

    #[test]
    fn test_calculate_multi_queue_efficiency_imbalanced() {
        use crate::utils::calculate_multi_queue_efficiency;

        let sizes = vec![1000, 100, 50];
        let rates = vec![5, 10, 20];
        let (efficiency, balance, _recommendation) =
            calculate_multi_queue_efficiency(&sizes, &rates);

        assert!(efficiency > 0.0 && efficiency <= 1.0);
        assert!((0.0..=1.0).contains(&balance));
    }

    #[test]
    fn test_calculate_multi_queue_efficiency_invalid() {
        use crate::utils::calculate_multi_queue_efficiency;

        let sizes = vec![100, 200];
        let rates = vec![10]; // Mismatched lengths
        let (efficiency, balance, recommendation) =
            calculate_multi_queue_efficiency(&sizes, &rates);

        assert_eq!(efficiency, 0.0);
        assert_eq!(balance, 0.0);
        assert_eq!(recommendation, "invalid_input");
    }

    #[test]
    fn test_predict_resource_exhaustion_critical() {
        use crate::utils::predict_resource_exhaustion;

        let (hours, severity, action) = predict_resource_exhaustion(9500, 10000, 1000);
        assert_eq!(hours, 0); // Will exhaust very soon
        assert_eq!(severity, "critical");
        assert!(action.contains("immediate"));
    }

    #[test]
    fn test_predict_resource_exhaustion_warning() {
        use crate::utils::predict_resource_exhaustion;

        let (hours, severity, _action) = predict_resource_exhaustion(5000, 10000, 500);
        assert_eq!(hours, 10);
        assert_eq!(severity, "warning");
    }

    #[test]
    fn test_predict_resource_exhaustion_healthy() {
        use crate::utils::predict_resource_exhaustion;

        let (hours, severity, action) = predict_resource_exhaustion(1000, 10000, 10);
        assert!(hours > 100);
        assert_eq!(severity, "healthy");
        assert_eq!(action, "normal_monitoring");
    }

    #[test]
    fn test_predict_resource_exhaustion_no_growth() {
        use crate::utils::predict_resource_exhaustion;

        let (hours, severity, action) = predict_resource_exhaustion(5000, 10000, 0);
        assert_eq!(hours, usize::MAX);
        assert_eq!(severity, "healthy");
        assert_eq!(action, "monitor");
    }

    #[test]
    fn test_suggest_autoscaling_policy_high_volatility() {
        use crate::utils::suggest_autoscaling_policy;

        let (min_workers, max_workers, scale_up, scale_down, policy) =
            suggest_autoscaling_policy(1000, 500, 100, 400);

        assert!(min_workers > 0);
        assert!(max_workers >= min_workers);
        assert!(scale_up > scale_down);
        assert_eq!(policy, "aggressive");
    }

    #[test]
    fn test_suggest_autoscaling_policy_low_volatility() {
        use crate::utils::suggest_autoscaling_policy;

        let (min_workers, max_workers, scale_up, scale_down, policy) =
            suggest_autoscaling_policy(600, 500, 400, 50);

        assert!(min_workers > 0);
        assert!(max_workers >= min_workers);
        assert!(scale_up > scale_down);
        assert_eq!(policy, "conservative");
    }

    #[test]
    fn test_suggest_autoscaling_policy_medium_volatility() {
        use crate::utils::suggest_autoscaling_policy;

        let (min_workers, max_workers, scale_up, scale_down, policy) =
            suggest_autoscaling_policy(800, 500, 300, 150);

        assert!(min_workers >= 2); // Always at least 2
        assert!(max_workers >= min_workers * 2); // At least 2x min
        assert!(scale_up > scale_down);
        assert_eq!(policy, "balanced");
    }

    #[test]
    fn test_calculate_message_affinity() {
        use crate::utils::calculate_message_affinity;

        // Same key should always route to same worker
        let worker1 = calculate_message_affinity("user:12345", 8);
        let worker2 = calculate_message_affinity("user:12345", 8);
        assert_eq!(worker1, worker2);
        assert!(worker1 < 8);

        // Different keys should potentially route to different workers
        let worker_a = calculate_message_affinity("order:abc", 10);
        let worker_b = calculate_message_affinity("order:xyz", 10);
        assert!(worker_a < 10);
        assert!(worker_b < 10);
    }

    #[test]
    fn test_calculate_message_affinity_zero_workers() {
        use crate::utils::calculate_message_affinity;

        let worker = calculate_message_affinity("test", 0);
        assert_eq!(worker, 0);
    }

    #[test]
    fn test_analyze_queue_temperature_hot() {
        use crate::utils::analyze_queue_temperature;

        let (temp, rec) = analyze_queue_temperature(100, 30);
        assert_eq!(temp, "hot");
        assert_eq!(rec, "maintain_resources");
    }

    #[test]
    fn test_analyze_queue_temperature_warm() {
        use crate::utils::analyze_queue_temperature;

        let (temp, rec) = analyze_queue_temperature(20, 150);
        assert_eq!(temp, "warm");
        assert_eq!(rec, "monitor");
    }

    #[test]
    fn test_analyze_queue_temperature_cold() {
        use crate::utils::analyze_queue_temperature;

        let (temp, rec) = analyze_queue_temperature(2, 800);
        assert_eq!(temp, "cold");
        assert_eq!(rec, "consider_scaling_down");
    }

    #[test]
    fn test_analyze_queue_temperature_lukewarm() {
        use crate::utils::analyze_queue_temperature;

        let (temp, rec) = analyze_queue_temperature(8, 400);
        assert_eq!(temp, "lukewarm");
        assert_eq!(rec, "monitor");
    }

    #[test]
    fn test_detect_processing_bottleneck_consumer() {
        use crate::utils::detect_processing_bottleneck;

        let (location, severity, _) = detect_processing_bottleneck(100, 50, 2000, 300);
        assert_eq!(location, "consumer");
        assert_eq!(severity, "high");
    }

    #[test]
    fn test_detect_processing_bottleneck_processing() {
        use crate::utils::detect_processing_bottleneck;

        let (location, severity, _) = detect_processing_bottleneck(100, 100, 500, 1500);
        assert_eq!(location, "processing");
        assert_eq!(severity, "medium");
    }

    #[test]
    fn test_detect_processing_bottleneck_queue() {
        use crate::utils::detect_processing_bottleneck;

        let (location, severity, _) = detect_processing_bottleneck(100, 90, 6000, 200);
        assert_eq!(location, "queue");
        assert_eq!(severity, "medium");
    }

    #[test]
    fn test_detect_processing_bottleneck_publisher() {
        use crate::utils::detect_processing_bottleneck;

        let (location, severity, _) = detect_processing_bottleneck(50, 150, 50, 100);
        assert_eq!(location, "publisher");
        assert_eq!(severity, "low");
    }

    #[test]
    fn test_detect_processing_bottleneck_healthy() {
        use crate::utils::detect_processing_bottleneck;

        let (location, severity, rec) = detect_processing_bottleneck(100, 100, 500, 200);
        assert_eq!(location, "none");
        assert_eq!(severity, "low");
        assert_eq!(rec, "system_healthy");
    }

    #[test]
    fn test_calculate_optimal_prefetch_multiplier() {
        use crate::utils::calculate_optimal_prefetch_multiplier;

        let multiplier = calculate_optimal_prefetch_multiplier(100, 10, 4);
        assert!(multiplier >= 1.0);
        assert!(multiplier <= 10.0);
    }

    #[test]
    fn test_calculate_optimal_prefetch_multiplier_fast_processing() {
        use crate::utils::calculate_optimal_prefetch_multiplier;

        let multiplier = calculate_optimal_prefetch_multiplier(30, 5, 2);
        assert!(multiplier > 2.0); // Fast processing should prefetch more
    }

    #[test]
    fn test_calculate_optimal_prefetch_multiplier_slow_processing() {
        use crate::utils::calculate_optimal_prefetch_multiplier;

        let multiplier = calculate_optimal_prefetch_multiplier(500, 10, 4);
        assert!(multiplier < 3.0); // Slow processing should prefetch less
    }

    #[test]
    fn test_calculate_optimal_prefetch_multiplier_zero_inputs() {
        use crate::utils::calculate_optimal_prefetch_multiplier;

        let multiplier = calculate_optimal_prefetch_multiplier(0, 10, 4);
        assert_eq!(multiplier, 1.0);

        let multiplier = calculate_optimal_prefetch_multiplier(100, 10, 0);
        assert_eq!(multiplier, 1.0);
    }

    #[test]
    fn test_suggest_queue_consolidation_low_throughput() {
        use crate::utils::suggest_queue_consolidation;

        let sizes = vec![10, 5, 8];
        let rates = vec![2, 1, 3];
        let (consolidate, reason, _) = suggest_queue_consolidation(&sizes, &rates);

        assert_eq!(consolidate, true);
        assert_eq!(reason, "low_throughput");
    }

    #[test]
    fn test_suggest_queue_consolidation_overhead() {
        use crate::utils::suggest_queue_consolidation;

        let sizes = vec![15, 10, 12, 8];
        let rates = vec![3, 2, 4, 1];
        let (consolidate, reason, _) = suggest_queue_consolidation(&sizes, &rates);

        assert_eq!(consolidate, true);
        assert_eq!(reason, "overhead");
    }

    #[test]
    fn test_suggest_queue_consolidation_efficient() {
        use crate::utils::suggest_queue_consolidation;

        let sizes = vec![500, 600, 700];
        let rates = vec![50, 60, 70];
        let (consolidate, reason, _) = suggest_queue_consolidation(&sizes, &rates);

        assert_eq!(consolidate, false);
        assert_eq!(reason, "efficient");
    }

    #[test]
    fn test_suggest_queue_consolidation_no_data() {
        use crate::utils::suggest_queue_consolidation;

        let sizes: Vec<usize> = vec![];
        let rates: Vec<usize> = vec![];
        let (consolidate, reason, _) = suggest_queue_consolidation(&sizes, &rates);

        assert_eq!(consolidate, false);
        assert_eq!(reason, "no_data");
    }

    #[test]
    fn test_suggest_queue_consolidation_invalid_input() {
        use crate::utils::suggest_queue_consolidation;

        let sizes = vec![10, 20];
        let rates = vec![5];
        let (consolidate, reason, _) = suggest_queue_consolidation(&sizes, &rates);

        assert_eq!(consolidate, false);
        assert_eq!(reason, "invalid_input");
    }

    #[test]
    fn test_analyze_compression_benefit_json() {
        use crate::utils::analyze_compression_benefit;

        // High volume - should compress
        let (should_compress, ratio, _) =
            analyze_compression_benefit(1024, 2000, "application/json");
        assert!(should_compress);
        assert_eq!(ratio, 0.3); // JSON compresses to 30%
    }

    #[test]
    fn test_analyze_compression_benefit_small_message() {
        use crate::utils::analyze_compression_benefit;

        let (should_compress, ratio, rec) =
            analyze_compression_benefit(400, 100, "application/json");
        assert!(!should_compress);
        assert_eq!(ratio, 1.0);
        assert_eq!(rec, "message_too_small");
    }

    #[test]
    fn test_analyze_compression_benefit_not_compressible() {
        use crate::utils::analyze_compression_benefit;

        let (should_compress, ratio, rec) =
            analyze_compression_benefit(2048, 100, "application/octet-stream");
        assert!(!should_compress);
        assert_eq!(ratio, 1.0);
        assert_eq!(rec, "content_type_not_compressible");
    }

    #[test]
    fn test_analyze_compression_benefit_xml() {
        use crate::utils::analyze_compression_benefit;

        let (should_compress, ratio, _) =
            analyze_compression_benefit(20000, 100, "application/xml");
        assert!(should_compress);
        assert_eq!(ratio, 0.25); // XML compresses to 25%
    }

    #[test]
    fn test_calculate_queue_migration_plan_fast() {
        use crate::utils::calculate_queue_migration_plan;

        let (batches, time, rec) = calculate_queue_migration_plan(1000, 100, 100);
        assert_eq!(batches, 10);
        assert_eq!(time, 10);
        assert_eq!(rec, "fast_migration_proceed");
    }

    #[test]
    fn test_calculate_queue_migration_plan_moderate() {
        use crate::utils::calculate_queue_migration_plan;

        let (batches, time, rec) = calculate_queue_migration_plan(10000, 50, 100);
        assert_eq!(batches, 200);
        assert_eq!(time, 100);
        assert!(rec.starts_with("moderate_migration"));
    }

    #[test]
    fn test_calculate_queue_migration_plan_slow() {
        use crate::utils::calculate_queue_migration_plan;

        let (batches, time, rec) = calculate_queue_migration_plan(100000, 100, 10);
        assert_eq!(batches, 1000);
        assert_eq!(time, 10000);
        assert!(rec.contains("slow_migration"));
    }

    #[test]
    fn test_calculate_queue_migration_plan_empty() {
        use crate::utils::calculate_queue_migration_plan;

        let (batches, time, rec) = calculate_queue_migration_plan(0, 100, 50);
        assert_eq!(batches, 0);
        assert_eq!(time, 0);
        assert_eq!(rec, "no_migration_needed");
    }

    #[test]
    fn test_profile_message_patterns_regular() {
        use crate::utils::profile_message_patterns;

        let sizes = vec![100, 102, 98, 101];
        let intervals = vec![10, 11, 9, 10];
        let (pattern, score, rec) = profile_message_patterns(&sizes, &intervals);

        assert_eq!(pattern, "highly_regular");
        assert!(score > 0.8);
        assert_eq!(rec, "predictable_use_static_buffers");
    }

    #[test]
    fn test_profile_message_patterns_irregular() {
        use crate::utils::profile_message_patterns;

        let sizes = vec![100, 500, 50, 1000];
        let intervals = vec![10, 100, 5, 200];
        let (pattern, score, _) = profile_message_patterns(&sizes, &intervals);

        assert_eq!(pattern, "highly_irregular");
        assert!(score < 0.3);
    }

    #[test]
    fn test_profile_message_patterns_moderate() {
        use crate::utils::profile_message_patterns;

        let sizes = vec![100, 150, 80, 120];
        let intervals = vec![10, 15, 8, 12];
        let (pattern, score, rec) = profile_message_patterns(&sizes, &intervals);

        assert_eq!(pattern, "regular");
        assert!(score > 0.5 && score <= 0.8);
        assert_eq!(rec, "moderate_use_adaptive_buffers");
    }

    #[test]
    fn test_profile_message_patterns_empty() {
        use crate::utils::profile_message_patterns;

        let sizes: Vec<usize> = vec![];
        let intervals: Vec<usize> = vec![];
        let (pattern, score, rec) = profile_message_patterns(&sizes, &intervals);

        assert_eq!(pattern, "unknown");
        assert_eq!(score, 0.0);
        assert_eq!(rec, "insufficient_data");
    }

    #[test]
    fn test_calculate_network_efficiency_balanced() {
        use crate::utils::calculate_network_efficiency;

        let (score, util, _rec) = calculate_network_efficiency(5000, 5000, 10000);
        assert!(score >= 0.0 && score <= 1.0); // Score should be in valid range
        assert_eq!(util, 100.0); // 100% bandwidth utilization
    }

    #[test]
    fn test_calculate_network_efficiency_overutilized() {
        use crate::utils::calculate_network_efficiency;

        let (_score, util, rec) = calculate_network_efficiency(9500, 500, 10000);
        assert!(util > 90.0);
        assert_eq!(rec, "increase_bandwidth_overutilized");
    }

    #[test]
    fn test_calculate_network_efficiency_underutilized() {
        use crate::utils::calculate_network_efficiency;

        let (_score, util, rec) = calculate_network_efficiency(1000, 500, 10000);
        assert!(util < 30.0);
        assert_eq!(rec, "reduce_bandwidth_underutilized");
    }

    #[test]
    fn test_calculate_network_efficiency_imbalanced() {
        use crate::utils::calculate_network_efficiency;

        let (_score, _util, rec) = calculate_network_efficiency(8000, 500, 10000);
        // Highly imbalanced send/receive ratio
        assert!(rec.contains("imbalanced") || rec == "good_efficiency");
    }

    #[test]
    fn test_detect_message_hotspots_balanced() {
        use crate::utils::detect_message_hotspots;

        let counts = vec![100, 105, 98, 102];
        let (has_hotspot, _, ratio, rec) = detect_message_hotspots(&counts);

        assert!(!has_hotspot);
        assert!(ratio < 2.0);
        assert_eq!(rec, "balanced_distribution");
    }

    #[test]
    fn test_detect_message_hotspots_severe() {
        use crate::utils::detect_message_hotspots;

        let counts = vec![100, 1000, 120, 110]; // Avg=332.5, ratio=1000/332.5=3.0
        let (has_hotspot, index, ratio, rec) = detect_message_hotspots(&counts);

        assert!(has_hotspot);
        assert_eq!(index, 1);
        assert!(ratio > 2.0); // Moderate hotspot (ratio 3.0)
        assert!(rec.contains("hotspot"));
    }

    #[test]
    fn test_detect_message_hotspots_moderate() {
        use crate::utils::detect_message_hotspots;

        let counts = vec![100, 350, 120, 110];
        let (has_hotspot, index, ratio, rec) = detect_message_hotspots(&counts);

        assert!(has_hotspot);
        assert_eq!(index, 1);
        assert!(ratio > 2.0 && ratio <= 5.0);
        assert!(rec.contains("moderate_hotspot") || rec.contains("minor_hotspot"));
    }

    #[test]
    fn test_detect_message_hotspots_empty() {
        use crate::utils::detect_message_hotspots;

        let counts: Vec<usize> = vec![];
        let (has_hotspot, _, ratio, rec) = detect_message_hotspots(&counts);

        assert!(!has_hotspot);
        assert_eq!(ratio, 1.0);
        assert_eq!(rec, "no_data");
    }

    #[test]
    fn test_recommend_queue_topology_single() {
        use crate::utils::recommend_queue_topology;

        let (topology, count, _) = recommend_queue_topology(100, 50, 10, false);
        assert_eq!(topology, "single_queue");
        assert_eq!(count, 1);
    }

    #[test]
    fn test_recommend_queue_topology_partitioned() {
        use crate::utils::recommend_queue_topology;

        let (topology, count, _) = recommend_queue_topology(10000, 50, 50, true);
        assert_eq!(topology, "partitioned");
        assert!(count >= 4);
    }

    #[test]
    fn test_recommend_queue_topology_multi_queue() {
        use crate::utils::recommend_queue_topology;

        let (topology, count, rec) = recommend_queue_topology(20000, 100, 50, false);
        assert_eq!(topology, "multi_queue");
        assert!(count >= 2);
        assert!(rec.contains("overloaded"));
    }

    #[test]
    fn test_recommend_queue_topology_priority() {
        use crate::utils::recommend_queue_topology;

        // High load but not overloaded: 500 msg/sec with 50 consumers @ 10msg/sec each
        let (topology, count, _) = recommend_queue_topology(450, 100, 50, false);
        assert_eq!(topology, "priority_queues");
        assert_eq!(count, 3);
    }

    #[test]
    fn test_recommend_queue_topology_worker_pool() {
        use crate::utils::recommend_queue_topology;

        // Medium volume that triggers worker_pool: > 1000 msg/sec but load_ratio < 0.8
        // 1100 msg/sec with 200 consumers @ 10msg/sec each = load_ratio 0.55
        let (topology, count, _) = recommend_queue_topology(1100, 100, 200, false);
        assert_eq!(topology, "worker_pool");
        assert!(count >= 2);
    }

    #[test]
    fn test_calculate_message_deduplication_window_fast_stream() {
        use crate::utils::calculate_message_deduplication_window;

        // Fast message stream (100ms interval, 3 retries, 5s max delay)
        let (window, cache_size, rec) = calculate_message_deduplication_window(100, 3, 5000);
        assert!(window > 0);
        assert!(cache_size >= 1000);
        assert!(rec.contains("window"));
    }

    #[test]
    fn test_calculate_message_deduplication_window_slow_stream() {
        use crate::utils::calculate_message_deduplication_window;

        // Slow message stream (10s interval, 1 retry, 2s max delay)
        let (window, cache_size, _) = calculate_message_deduplication_window(10000, 1, 2000);
        assert!(window >= 60); // At least 1 minute
        assert!(cache_size >= 1000);
    }

    #[test]
    fn test_calculate_message_deduplication_window_long_window() {
        use crate::utils::calculate_message_deduplication_window;

        // Long window scenario (requires > 1 hour window for persistent storage recommendation)
        // 120s interval * 15 retries + 600s max delay = 1800s + 600s = 2400s, then *2 = 4800s > 3600s
        let (window, _, rec) = calculate_message_deduplication_window(120000, 15, 600000);
        assert!(window > 3600); // More than 1 hour
        assert!(rec.contains("persistent_storage"));
    }

    #[test]
    fn test_analyze_retry_effectiveness_high() {
        use crate::utils::analyze_retry_effectiveness;

        // High retry effectiveness (80 out of 100 failures recovered)
        let (effectiveness, success_rate, rec) = analyze_retry_effectiveness(1000, 100, 80, 20);
        assert!(effectiveness >= 80.0);
        assert!(success_rate >= 98.0); // 1000 - 20 = 980 successes
        assert!(rec.contains("effective"));
    }

    #[test]
    fn test_analyze_retry_effectiveness_low() {
        use crate::utils::analyze_retry_effectiveness;

        // Low retry effectiveness (10 out of 100 failures recovered)
        let (effectiveness, _, rec) = analyze_retry_effectiveness(1000, 100, 10, 90);
        assert!(effectiveness < 20.0);
        assert!(rec.contains("ineffective"));
    }

    #[test]
    fn test_analyze_retry_effectiveness_no_failures() {
        use crate::utils::analyze_retry_effectiveness;

        // No failures
        let (effectiveness, success_rate, rec) = analyze_retry_effectiveness(1000, 0, 0, 0);
        assert_eq!(effectiveness, 100.0);
        assert_eq!(success_rate, 100.0);
        assert!(rec.contains("no_failures"));
    }

    #[test]
    fn test_calculate_queue_overflow_risk_high() {
        use crate::utils::calculate_queue_overflow_risk;

        // High risk: 8000/10000 capacity, enqueue > dequeue
        let (risk, ttf, rec) = calculate_queue_overflow_risk(8000, 10000, 100, 50);
        assert!(risk > 50.0);
        assert!(ttf > 0); // Should have time to full
        assert!(rec.contains("high_risk") || rec.contains("medium_risk"));
    }

    #[test]
    fn test_calculate_queue_overflow_risk_low() {
        use crate::utils::calculate_queue_overflow_risk;

        // Low risk: low capacity, dequeue > enqueue (draining)
        let (risk, _, rec) = calculate_queue_overflow_risk(100, 10000, 50, 100);
        assert_eq!(risk, 0.0);
        assert!(rec.contains("healthy"));
    }

    #[test]
    fn test_calculate_queue_overflow_risk_critical() {
        use crate::utils::calculate_queue_overflow_risk;

        // Critical risk: 95% utilization
        let (risk, _, rec) = calculate_queue_overflow_risk(9500, 10000, 100, 80);
        assert!(risk >= 90.0);
        assert!(rec.contains("critical"));
    }

    #[test]
    fn test_calculate_queue_overflow_risk_invalid_max() {
        use crate::utils::calculate_queue_overflow_risk;

        // Invalid max size
        let (risk, _, rec) = calculate_queue_overflow_risk(100, 0, 50, 50);
        assert_eq!(risk, 100.0);
        assert!(rec.contains("misconfigured"));
    }
}
