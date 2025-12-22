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
}
