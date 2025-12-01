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

use async_trait::async_trait;
use celers_protocol::Message;
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
}
