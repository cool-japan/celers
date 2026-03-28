//! Core broker types and traits.

use async_trait::async_trait;
use celers_protocol::Message;
use std::collections::HashMap;
use std::time::Duration;

use crate::{QueueMode, Result};

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
