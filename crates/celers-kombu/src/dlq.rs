//! Dead letter queue and transaction types.

use async_trait::async_trait;
use celers_protocol::Message;
use std::collections::HashMap;
use std::time::Duration;

use crate::{Envelope, Result};

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
