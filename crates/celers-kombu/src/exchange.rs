//! Exchange and admin types.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::Result;

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
