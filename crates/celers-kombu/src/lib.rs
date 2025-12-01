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
// Connection State Callbacks
// =============================================================================

/// Connection state
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

    /// Check if message has expired (based on expires_at)
    pub fn is_expired(&self, current_timestamp: u64) -> bool {
        self.expires_at.is_some_and(|exp| current_timestamp > exp)
    }

    /// Check if message should be delayed
    pub fn should_delay(&self) -> bool {
        self.delay.is_some()
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
}
