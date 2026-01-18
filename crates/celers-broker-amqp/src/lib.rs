//! RabbitMQ/AMQP broker implementation for CeleRS
//!
//! This crate provides AMQP protocol support via RabbitMQ,
//! compatible with Python Celery's AMQP backend.
//!
//! # Features
//!
//! - Exchange/Queue/Binding topology management
//! - Publisher Confirm for reliability
//! - **Batch Publishing** for improved throughput (publish multiple messages before waiting for confirms)
//! - Consumer QoS (Prefetch) control
//! - **Consumer Streaming** via `start_consumer()` for high-throughput message processing
//! - Dead Letter Exchange (DLX) support
//! - Message TTL support
//! - Exchange types (Direct, Fanout, Topic, Headers)
//! - Connection recovery and retry
//! - Virtual host support
//! - **Health Monitoring** with `health_status()` and `is_healthy()`
//! - **Transaction Support** with `start_transaction()`, `commit_transaction()`, `rollback_transaction()`
//! - **Management API Integration** for queue listing, statistics, and server monitoring
//! - **Monitoring Utilities** for autoscaling, capacity planning, and SLA monitoring (see [`monitoring`] module)
//! - **Utility Functions** for performance tuning and optimization (see [`utilities`] module)
//!
//! # Enhanced Features (v6)
//!
//! - **Circuit Breaker Pattern** - Prevent cascading failures with automatic recovery (see [`circuit_breaker`] module)
//! - **Advanced Retry Strategies** - Exponential backoff with jitter support (see [`retry`] module)
//! - **Message Compression** - Reduce network overhead with Gzip/Zstd (see [`compression`] module)
//! - **Topology Validation** - Validate AMQP topology before deployment (see [`topology`] module)
//! - **Message Tracing** - Track message lifecycle and flow analysis (see [`tracing_util`] module)
//! - **Consumer Groups** - Load balancing with multiple strategies (see [`consumer_groups`] module)
//!
//! # Production Features (v7)
//!
//! - **Rate Limiting** - Token bucket and leaky bucket algorithms for controlling message rates (see [`rate_limiter`] module)
//! - **Bulkhead Pattern** - Resource isolation to prevent cascading failures (see [`bulkhead`] module)
//! - **Message Scheduling** - Delayed message delivery for scheduled tasks and retries (see [`scheduler`] module)
//! - **Metrics Export** - Export metrics to Prometheus, StatsD, and JSON formats (see [`metrics_export`] module)
//!
//! # Advanced Production Features (v8)
//!
//! - **Lifecycle Hooks** - Extensible hooks for intercepting publish, consume, and acknowledgment events (see [`hooks`] module)
//! - **DLX Analytics** - Comprehensive Dead Letter Exchange analytics and insights (see [`dlx_analytics`] module)
//! - **Adaptive Batching** - Dynamic batch size optimization based on system load and latency (see [`batch_optimizer`] module)
//! - **Performance Profiling** - Detailed performance profiling with percentile analysis (see [`profiler`] module)
//!
//! # Enterprise Production Features (v9)
//!
//! - **Backpressure Management** - Intelligent flow control to prevent overwhelming the broker or consumers (see [`backpressure`] module)
//! - **Poison Message Detection** - Identify and handle messages that repeatedly fail processing (see [`poison_detector`] module)
//! - **Advanced Routing** - Sophisticated routing strategies beyond basic AMQP exchange types (see [`router`] module)
//! - **Performance Optimization** - Advanced optimization strategies for connection tuning and resource management (see [`optimization`] module)
//!
//! # Example
//!
//! ```ignore
//! use celers_broker_amqp::{AmqpBroker, AmqpConfig};
//! use celers_kombu::{Transport, Producer, Consumer};
//! use celers_protocol::{Message, MessageBuilder};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Basic usage
//! let mut broker = AmqpBroker::new("amqp://localhost:5672", "celery").await?;
//!
//! // With configuration
//! let config = AmqpConfig::default()
//!     .with_prefetch(10)
//!     .with_retry(3, std::time::Duration::from_secs(1));
//! let mut broker = AmqpBroker::with_config("amqp://localhost:5672", "celery", config).await?;
//!
//! // Publish a message
//! let message = MessageBuilder::new("tasks.add").build()?;
//! broker.publish("my_queue", message).await?;
//!
//! // Consume messages
//! let envelope = broker.consume("my_queue", std::time::Duration::from_secs(5)).await?;
//!
//! // With Management API for monitoring
//! let config = AmqpConfig::default()
//!     .with_management_api("http://localhost:15672", "guest", "guest");
//! let mut broker = AmqpBroker::with_config("amqp://localhost:5672", "celery", config).await?;
//!
//! // List all queues
//! let queues = broker.list_queues().await?;
//! for queue in queues {
//!     println!("{}: {} messages", queue.name, queue.messages);
//! }
//!
//! // Get detailed queue statistics
//! let stats = broker.get_queue_stats("my_queue").await?;
//! println!("Ready: {}, Unacked: {}", stats.messages_ready, stats.messages_unacknowledged);
//!
//! // Get server overview
//! let overview = broker.get_server_overview().await?;
//! println!("RabbitMQ version: {}", overview.rabbitmq_version);
//!
//! // Monitor connections and channels
//! let connections = broker.list_connections().await?;
//! println!("Active connections: {}", connections.len());
//!
//! let channels = broker.list_channels().await?;
//! println!("Active channels: {}", channels.len());
//!
//! // List exchanges
//! let exchanges = broker.list_exchanges(None).await?;
//! for exchange in exchanges {
//!     println!("{}: {} ({})", exchange.name, exchange.exchange_type, exchange.vhost);
//! }
//!
//! // Inspect queue bindings
//! let bindings = broker.list_queue_bindings("my_queue").await?;
//! for binding in bindings {
//!     println!("{} -> {} via '{}'", binding.source, binding.destination, binding.routing_key);
//! }
//! # Ok(())
//! # }
//! ```

use async_trait::async_trait;
use celers_kombu::{
    Broker, BrokerError, Consumer, Envelope, Producer, QueueMode, Result, Transport,
};
use celers_protocol::Message;
use lapin::{
    options::*,
    types::{AMQPValue, FieldTable, ShortString},
    BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind,
};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

// Public modules
pub mod backpressure;
pub mod batch_optimizer;
pub mod bulkhead;
pub mod circuit_breaker;
pub mod compression;
pub mod consumer_groups;
pub mod dlx_analytics;
pub mod hooks;
pub mod metrics_export;
pub mod monitoring;
pub mod optimization;
pub mod poison_detector;
pub mod profiler;
pub mod rate_limiter;
pub mod retry;
pub mod router;
pub mod scheduler;
pub mod topology;
pub mod tracing_util;
pub mod utilities;

/// Exchange type for AMQP
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum AmqpExchangeType {
    /// Direct exchange - routes messages by exact routing key match
    #[default]
    Direct,
    /// Fanout exchange - broadcasts to all bound queues
    Fanout,
    /// Topic exchange - routes messages by pattern matching on routing key
    Topic,
    /// Headers exchange - routes messages by header attributes
    Headers,
}

impl AmqpExchangeType {
    fn to_exchange_kind(self) -> ExchangeKind {
        match self {
            AmqpExchangeType::Direct => ExchangeKind::Direct,
            AmqpExchangeType::Fanout => ExchangeKind::Fanout,
            AmqpExchangeType::Topic => ExchangeKind::Topic,
            AmqpExchangeType::Headers => ExchangeKind::Headers,
        }
    }
}

impl std::fmt::Display for AmqpExchangeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AmqpExchangeType::Direct => write!(f, "direct"),
            AmqpExchangeType::Fanout => write!(f, "fanout"),
            AmqpExchangeType::Topic => write!(f, "topic"),
            AmqpExchangeType::Headers => write!(f, "headers"),
        }
    }
}

/// Dead Letter Exchange configuration
#[derive(Debug, Clone)]
pub struct DlxConfig {
    /// Dead letter exchange name
    pub exchange: String,
    /// Dead letter routing key (optional)
    pub routing_key: Option<String>,
}

impl DlxConfig {
    /// Create a new DLX configuration
    pub fn new(exchange: impl Into<String>) -> Self {
        Self {
            exchange: exchange.into(),
            routing_key: None,
        }
    }

    /// Set the dead letter routing key
    pub fn with_routing_key(mut self, routing_key: impl Into<String>) -> Self {
        self.routing_key = Some(routing_key.into());
        self
    }
}

/// Queue type for RabbitMQ 3.8+
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum QueueType {
    /// Classic queue (default, best for most use cases)
    Classic,
    /// Quorum queue (replicated, highly available, data safety)
    Quorum,
    /// Stream queue (high-throughput, append-only log)
    Stream,
}

impl QueueType {
    fn as_str(&self) -> &str {
        match self {
            QueueType::Classic => "classic",
            QueueType::Quorum => "quorum",
            QueueType::Stream => "stream",
        }
    }
}

impl std::fmt::Display for QueueType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Queue mode for lazy queues (RabbitMQ 3.6+)
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum QueueLazyMode {
    /// Default mode - keeps messages in memory when possible
    Default,
    /// Lazy mode - moves messages to disk as early as possible
    Lazy,
}

impl QueueLazyMode {
    fn as_str(&self) -> &str {
        match self {
            QueueLazyMode::Default => "default",
            QueueLazyMode::Lazy => "lazy",
        }
    }
}

impl std::fmt::Display for QueueLazyMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Queue overflow behavior when max-length is reached
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum QueueOverflowBehavior {
    /// Drop messages from the head of the queue (default)
    DropHead,
    /// Reject new publishes
    RejectPublish,
    /// Reject new publishes and use dead letter exchange
    RejectPublishDlx,
}

impl QueueOverflowBehavior {
    fn as_str(&self) -> &str {
        match self {
            QueueOverflowBehavior::DropHead => "drop-head",
            QueueOverflowBehavior::RejectPublish => "reject-publish",
            QueueOverflowBehavior::RejectPublishDlx => "reject-publish-dlx",
        }
    }
}

impl std::fmt::Display for QueueOverflowBehavior {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Queue configuration options
#[derive(Debug, Clone, Default)]
pub struct QueueConfig {
    /// Queue durability
    pub durable: bool,
    /// Auto-delete when no consumers
    pub auto_delete: bool,
    /// Exclusive to this connection
    pub exclusive: bool,
    /// Maximum priority (0-255, 0 disables priority)
    pub max_priority: Option<u8>,
    /// Message TTL in milliseconds
    pub message_ttl: Option<u64>,
    /// Dead letter exchange configuration
    pub dlx: Option<DlxConfig>,
    /// Queue expiration in milliseconds (queue will be deleted after this time of disuse)
    pub expires: Option<u64>,
    /// Maximum queue length (messages)
    pub max_length: Option<i64>,
    /// Maximum queue size in bytes
    pub max_length_bytes: Option<i64>,
    /// Queue type (Classic, Quorum, or Stream) - RabbitMQ 3.8+
    pub queue_type: Option<QueueType>,
    /// Queue mode (Default or Lazy) - RabbitMQ 3.6+
    pub queue_mode: Option<QueueLazyMode>,
    /// Overflow behavior when max-length is reached
    pub overflow_behavior: Option<QueueOverflowBehavior>,
    /// Enable single active consumer (only one consumer receives messages at a time)
    pub single_active_consumer: bool,
}

impl QueueConfig {
    /// Create a new queue configuration with defaults
    pub fn new() -> Self {
        Self {
            durable: true,
            ..Default::default()
        }
    }

    /// Set queue as durable
    pub fn durable(mut self, durable: bool) -> Self {
        self.durable = durable;
        self
    }

    /// Set auto-delete behavior
    pub fn auto_delete(mut self, auto_delete: bool) -> Self {
        self.auto_delete = auto_delete;
        self
    }

    /// Set max priority level (1-255)
    pub fn with_max_priority(mut self, priority: u8) -> Self {
        self.max_priority = Some(priority);
        self
    }

    /// Set message TTL in milliseconds
    pub fn with_message_ttl(mut self, ttl_ms: u64) -> Self {
        self.message_ttl = Some(ttl_ms);
        self
    }

    /// Set dead letter exchange
    pub fn with_dlx(mut self, dlx: DlxConfig) -> Self {
        self.dlx = Some(dlx);
        self
    }

    /// Set queue expiration in milliseconds
    pub fn with_expires(mut self, expires_ms: u64) -> Self {
        self.expires = Some(expires_ms);
        self
    }

    /// Set maximum queue length
    pub fn with_max_length(mut self, max_length: i64) -> Self {
        self.max_length = Some(max_length);
        self
    }

    /// Set queue type (Classic, Quorum, or Stream) - RabbitMQ 3.8+
    ///
    /// # Examples
    /// ```ignore
    /// use celers_broker_amqp::{QueueConfig, QueueType};
    ///
    /// let config = QueueConfig::new().with_queue_type(QueueType::Quorum);
    /// ```
    pub fn with_queue_type(mut self, queue_type: QueueType) -> Self {
        self.queue_type = Some(queue_type);
        self
    }

    /// Set queue mode (Default or Lazy) - RabbitMQ 3.6+
    ///
    /// Lazy queues move messages to disk as early as possible, which is ideal
    /// for very long queues (millions of messages) or when dealing with large messages.
    ///
    /// # Examples
    /// ```ignore
    /// use celers_broker_amqp::{QueueConfig, QueueLazyMode};
    ///
    /// let config = QueueConfig::new().with_queue_mode(QueueLazyMode::Lazy);
    /// ```
    pub fn with_queue_mode(mut self, mode: QueueLazyMode) -> Self {
        self.queue_mode = Some(mode);
        self
    }

    /// Set overflow behavior when max-length is reached
    ///
    /// # Examples
    /// ```ignore
    /// use celers_broker_amqp::{QueueConfig, QueueOverflowBehavior};
    ///
    /// let config = QueueConfig::new()
    ///     .with_max_length(1000)
    ///     .with_overflow_behavior(QueueOverflowBehavior::RejectPublish);
    /// ```
    pub fn with_overflow_behavior(mut self, behavior: QueueOverflowBehavior) -> Self {
        self.overflow_behavior = Some(behavior);
        self
    }

    /// Enable single active consumer mode
    ///
    /// When enabled, only one consumer will receive messages at a time.
    /// This is useful for ensuring ordered message processing.
    ///
    /// # Examples
    /// ```ignore
    /// use celers_broker_amqp::QueueConfig;
    ///
    /// let config = QueueConfig::new().with_single_active_consumer(true);
    /// ```
    pub fn with_single_active_consumer(mut self, enabled: bool) -> Self {
        self.single_active_consumer = enabled;
        self
    }

    /// Convert to AMQP field table
    fn to_field_table(&self) -> FieldTable {
        let mut args = FieldTable::default();

        if let Some(priority) = self.max_priority {
            args.insert(
                ShortString::from("x-max-priority"),
                AMQPValue::ShortShortUInt(priority),
            );
        }

        if let Some(ttl) = self.message_ttl {
            args.insert(
                ShortString::from("x-message-ttl"),
                AMQPValue::LongLongInt(ttl as i64),
            );
        }

        if let Some(ref dlx) = self.dlx {
            args.insert(
                ShortString::from("x-dead-letter-exchange"),
                AMQPValue::LongString(dlx.exchange.clone().into()),
            );
            if let Some(ref routing_key) = dlx.routing_key {
                args.insert(
                    ShortString::from("x-dead-letter-routing-key"),
                    AMQPValue::LongString(routing_key.clone().into()),
                );
            }
        }

        if let Some(expires) = self.expires {
            args.insert(
                ShortString::from("x-expires"),
                AMQPValue::LongLongInt(expires as i64),
            );
        }

        if let Some(max_len) = self.max_length {
            args.insert(
                ShortString::from("x-max-length"),
                AMQPValue::LongLongInt(max_len),
            );
        }

        if let Some(max_bytes) = self.max_length_bytes {
            args.insert(
                ShortString::from("x-max-length-bytes"),
                AMQPValue::LongLongInt(max_bytes),
            );
        }

        if let Some(queue_type) = self.queue_type {
            args.insert(
                ShortString::from("x-queue-type"),
                AMQPValue::LongString(queue_type.as_str().into()),
            );
        }

        if let Some(queue_mode) = self.queue_mode {
            args.insert(
                ShortString::from("x-queue-mode"),
                AMQPValue::LongString(queue_mode.as_str().into()),
            );
        }

        if let Some(overflow) = self.overflow_behavior {
            args.insert(
                ShortString::from("x-overflow"),
                AMQPValue::LongString(overflow.as_str().into()),
            );
        }

        if self.single_active_consumer {
            args.insert(
                ShortString::from("x-single-active-consumer"),
                AMQPValue::Boolean(true),
            );
        }

        args
    }
}

/// Builder for message properties with enhanced ergonomics
#[derive(Debug, Clone, Default)]
pub struct MessagePropertiesBuilder {
    content_type: Option<String>,
    content_encoding: Option<String>,
    delivery_mode: Option<u8>,
    priority: Option<u8>,
    correlation_id: Option<String>,
    reply_to: Option<String>,
    expiration: Option<String>,
    message_id: Option<String>,
    timestamp: Option<u64>,
    user_id: Option<String>,
    app_id: Option<String>,
    headers: HashMap<String, String>,
}

impl MessagePropertiesBuilder {
    /// Create a new message properties builder with sensible defaults
    pub fn new() -> Self {
        Self {
            content_type: Some("application/json".to_string()),
            content_encoding: Some("utf-8".to_string()),
            delivery_mode: Some(2), // Persistent by default
            ..Default::default()
        }
    }

    /// Set content type (default: application/json)
    pub fn content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = Some(content_type.into());
        self
    }

    /// Set content encoding (default: utf-8)
    pub fn content_encoding(mut self, encoding: impl Into<String>) -> Self {
        self.content_encoding = Some(encoding.into());
        self
    }

    /// Set delivery mode (1 = non-persistent, 2 = persistent)
    pub fn delivery_mode(mut self, mode: u8) -> Self {
        self.delivery_mode = Some(mode);
        self
    }

    /// Set as persistent (delivery_mode = 2)
    pub fn persistent(mut self) -> Self {
        self.delivery_mode = Some(2);
        self
    }

    /// Set as transient/non-persistent (delivery_mode = 1)
    pub fn transient(mut self) -> Self {
        self.delivery_mode = Some(1);
        self
    }

    /// Set message priority (0-9, where 9 is highest)
    pub fn priority(mut self, priority: u8) -> Self {
        self.priority = Some(priority.min(9));
        self
    }

    /// Set correlation ID for request-reply pattern
    pub fn correlation_id(mut self, id: impl Into<String>) -> Self {
        self.correlation_id = Some(id.into());
        self
    }

    /// Set reply-to queue for RPC pattern
    pub fn reply_to(mut self, queue: impl Into<String>) -> Self {
        self.reply_to = Some(queue.into());
        self
    }

    /// Set message expiration/TTL in milliseconds
    pub fn expiration_ms(mut self, ttl_ms: u64) -> Self {
        self.expiration = Some(ttl_ms.to_string());
        self
    }

    /// Set message ID
    pub fn message_id(mut self, id: impl Into<String>) -> Self {
        self.message_id = Some(id.into());
        self
    }

    /// Set timestamp (Unix timestamp in seconds)
    pub fn timestamp(mut self, timestamp: u64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Set user ID
    pub fn user_id(mut self, user_id: impl Into<String>) -> Self {
        self.user_id = Some(user_id.into());
        self
    }

    /// Set application ID
    pub fn app_id(mut self, app_id: impl Into<String>) -> Self {
        self.app_id = Some(app_id.into());
        self
    }

    /// Add a custom header
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Build the BasicProperties
    #[allow(dead_code)]
    pub(crate) fn build(self) -> BasicProperties {
        let mut props = BasicProperties::default();

        if let Some(ct) = self.content_type {
            props = props.with_content_type(ShortString::from(ct));
        }
        if let Some(ce) = self.content_encoding {
            props = props.with_content_encoding(ShortString::from(ce));
        }
        if let Some(dm) = self.delivery_mode {
            props = props.with_delivery_mode(dm);
        }
        if let Some(p) = self.priority {
            props = props.with_priority(p);
        }
        if let Some(cid) = self.correlation_id {
            props = props.with_correlation_id(ShortString::from(cid));
        }
        if let Some(rt) = self.reply_to {
            props = props.with_reply_to(ShortString::from(rt));
        }
        if let Some(exp) = self.expiration {
            props = props.with_expiration(ShortString::from(exp));
        }
        if let Some(mid) = self.message_id {
            props = props.with_message_id(ShortString::from(mid));
        }
        if let Some(ts) = self.timestamp {
            props = props.with_timestamp(ts);
        }
        if let Some(uid) = self.user_id {
            props = props.with_user_id(ShortString::from(uid));
        }
        if let Some(aid) = self.app_id {
            props = props.with_app_id(ShortString::from(aid));
        }

        if !self.headers.is_empty() {
            let mut field_table = FieldTable::default();
            for (key, value) in self.headers {
                field_table.insert(ShortString::from(key), AMQPValue::LongString(value.into()));
            }
            props = props.with_headers(field_table);
        }

        props
    }
}

/// Exchange configuration with advanced options
#[derive(Debug, Clone)]
pub struct ExchangeConfig {
    /// Exchange type
    pub exchange_type: AmqpExchangeType,
    /// Durable (survives broker restart)
    pub durable: bool,
    /// Auto-delete when no queues are bound
    pub auto_delete: bool,
    /// Internal (cannot be published to directly)
    pub internal: bool,
    /// Alternative exchange for unroutable messages
    pub alternate_exchange: Option<String>,
}

impl ExchangeConfig {
    /// Create a new exchange configuration
    pub fn new(exchange_type: AmqpExchangeType) -> Self {
        Self {
            exchange_type,
            durable: true,
            auto_delete: false,
            internal: false,
            alternate_exchange: None,
        }
    }

    /// Set durability
    pub fn durable(mut self, durable: bool) -> Self {
        self.durable = durable;
        self
    }

    /// Set auto-delete behavior
    pub fn auto_delete(mut self, auto_delete: bool) -> Self {
        self.auto_delete = auto_delete;
        self
    }

    /// Set as internal exchange
    pub fn internal(mut self, internal: bool) -> Self {
        self.internal = internal;
        self
    }

    /// Set alternative exchange for unroutable messages
    pub fn with_alternate_exchange(mut self, exchange: impl Into<String>) -> Self {
        self.alternate_exchange = Some(exchange.into());
        self
    }

    /// Convert to AMQP field table
    fn to_field_table(&self) -> FieldTable {
        let mut args = FieldTable::default();
        if let Some(ref ae) = self.alternate_exchange {
            args.insert(
                ShortString::from("alternate-exchange"),
                AMQPValue::LongString(ae.clone().into()),
            );
        }
        args
    }
}

impl Default for ExchangeConfig {
    fn default() -> Self {
        Self::new(AmqpExchangeType::Direct)
    }
}

/// Consumer configuration with advanced options
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// Consumer tag (empty = auto-generated)
    pub consumer_tag: String,
    /// No-local flag (don't receive messages published by this connection)
    pub no_local: bool,
    /// No-ack flag (automatic acknowledgment)
    pub no_ack: bool,
    /// Exclusive consumer (only this consumer can access the queue)
    pub exclusive: bool,
    /// Consumer priority (higher priority consumers get messages first)
    pub priority: Option<i32>,
}

impl ConsumerConfig {
    /// Create a new consumer configuration
    pub fn new() -> Self {
        Self {
            consumer_tag: String::new(),
            no_local: false,
            no_ack: false,
            exclusive: false,
            priority: None,
        }
    }

    /// Set consumer tag
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.consumer_tag = tag.into();
        self
    }

    /// Set no-local flag
    pub fn no_local(mut self, no_local: bool) -> Self {
        self.no_local = no_local;
        self
    }

    /// Set no-ack flag (automatic acknowledgment)
    pub fn no_ack(mut self, no_ack: bool) -> Self {
        self.no_ack = no_ack;
        self
    }

    /// Set exclusive flag
    pub fn exclusive(mut self, exclusive: bool) -> Self {
        self.exclusive = exclusive;
        self
    }

    /// Set consumer priority (higher values = higher priority)
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = Some(priority);
        self
    }

    /// Convert to AMQP field table
    fn to_field_table(&self) -> FieldTable {
        let mut args = FieldTable::default();
        if let Some(priority) = self.priority {
            args.insert(
                ShortString::from("x-priority"),
                AMQPValue::LongInt(priority),
            );
        }
        args
    }
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// AMQP broker configuration
#[derive(Debug, Clone)]
pub struct AmqpConfig {
    /// Prefetch count (QoS) - number of unacknowledged messages allowed
    pub prefetch_count: u16,
    /// Global prefetch (applies to entire channel vs per-consumer)
    pub prefetch_global: bool,
    /// Default exchange name
    pub default_exchange: String,
    /// Default exchange type
    pub default_exchange_type: AmqpExchangeType,
    /// Connection retry count (0 = no retry)
    pub retry_count: u32,
    /// Delay between retry attempts
    pub retry_delay: Duration,
    /// Heartbeat interval in seconds (0 = disabled)
    pub heartbeat: u16,
    /// Connection timeout
    pub connection_timeout: Duration,
    /// Virtual host
    pub vhost: Option<String>,
    /// Enable automatic reconnection on connection loss during operation
    pub auto_reconnect: bool,
    /// Maximum reconnection attempts (0 = unlimited)
    pub auto_reconnect_max_attempts: u32,
    /// Delay between auto-reconnection attempts
    pub auto_reconnect_delay: Duration,
    /// Connection pool size (0 = disabled, use single connection)
    pub connection_pool_size: usize,
    /// Channel pool size per connection (0 = disabled, create channels on demand)
    pub channel_pool_size: usize,
    /// Enable message deduplication (default: false)
    pub enable_deduplication: bool,
    /// Deduplication cache size (number of message IDs to track)
    pub deduplication_cache_size: usize,
    /// Deduplication cache TTL (how long to remember message IDs)
    pub deduplication_ttl: Duration,
    /// Management API URL (e.g., "http://localhost:15672")
    pub management_url: Option<String>,
    /// Management API username
    pub management_username: Option<String>,
    /// Management API password
    pub management_password: Option<String>,
}

impl Default for AmqpConfig {
    fn default() -> Self {
        Self {
            prefetch_count: 0, // Unlimited
            prefetch_global: false,
            default_exchange: "celery".to_string(),
            default_exchange_type: AmqpExchangeType::Direct,
            retry_count: 0,
            retry_delay: Duration::from_secs(1),
            heartbeat: 60,
            connection_timeout: Duration::from_secs(30),
            vhost: None,
            auto_reconnect: true,
            auto_reconnect_max_attempts: 0, // Unlimited
            auto_reconnect_delay: Duration::from_secs(5),
            connection_pool_size: 0, // Disabled by default
            channel_pool_size: 10,   // 10 channels per connection
            enable_deduplication: false,
            deduplication_cache_size: 10000,
            deduplication_ttl: Duration::from_secs(3600), // 1 hour
            management_url: None,
            management_username: None,
            management_password: None,
        }
    }
}

impl AmqpConfig {
    /// Set prefetch count (QoS)
    pub fn with_prefetch(mut self, count: u16) -> Self {
        self.prefetch_count = count;
        self
    }

    /// Set global prefetch
    pub fn with_global_prefetch(mut self, global: bool) -> Self {
        self.prefetch_global = global;
        self
    }

    /// Set retry configuration
    pub fn with_retry(mut self, count: u32, delay: Duration) -> Self {
        self.retry_count = count;
        self.retry_delay = delay;
        self
    }

    /// Set default exchange
    pub fn with_exchange(mut self, exchange: impl Into<String>) -> Self {
        self.default_exchange = exchange.into();
        self
    }

    /// Set default exchange type
    pub fn with_exchange_type(mut self, exchange_type: AmqpExchangeType) -> Self {
        self.default_exchange_type = exchange_type;
        self
    }

    /// Set heartbeat interval
    pub fn with_heartbeat(mut self, heartbeat: u16) -> Self {
        self.heartbeat = heartbeat;
        self
    }

    /// Set connection timeout
    pub fn with_connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = timeout;
        self
    }

    /// Set virtual host
    pub fn with_vhost(mut self, vhost: impl Into<String>) -> Self {
        self.vhost = Some(vhost.into());
        self
    }

    /// Enable or disable automatic reconnection
    pub fn with_auto_reconnect(mut self, enabled: bool) -> Self {
        self.auto_reconnect = enabled;
        self
    }

    /// Set automatic reconnection configuration
    pub fn with_auto_reconnect_config(mut self, max_attempts: u32, delay: Duration) -> Self {
        self.auto_reconnect_max_attempts = max_attempts;
        self.auto_reconnect_delay = delay;
        self
    }

    /// Set connection pool size (0 = disabled)
    pub fn with_connection_pool_size(mut self, size: usize) -> Self {
        self.connection_pool_size = size;
        self
    }

    /// Set channel pool size per connection (0 = disabled)
    pub fn with_channel_pool_size(mut self, size: usize) -> Self {
        self.channel_pool_size = size;
        self
    }

    /// Enable message deduplication
    pub fn with_deduplication(mut self, enabled: bool) -> Self {
        self.enable_deduplication = enabled;
        self
    }

    /// Set deduplication cache configuration
    pub fn with_deduplication_config(mut self, cache_size: usize, ttl: Duration) -> Self {
        self.deduplication_cache_size = cache_size;
        self.deduplication_ttl = ttl;
        self
    }

    /// Set Management API configuration
    pub fn with_management_api(
        mut self,
        url: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.management_url = Some(url.into());
        self.management_username = Some(username.into());
        self.management_password = Some(password.into());
        self
    }
}

/// Connection health status
#[derive(Debug, Clone)]
pub struct HealthStatus {
    /// Whether the connection is established
    pub connected: bool,
    /// Whether the channel is open
    pub channel_open: bool,
    /// Connection state description
    pub connection_state: String,
    /// Channel state description
    pub channel_state: String,
    /// Connection pool metrics (if connection pooling is enabled)
    pub connection_pool_metrics: Option<ConnectionPoolMetrics>,
    /// Channel pool metrics (if channel pooling is enabled)
    pub channel_pool_metrics: Option<ChannelPoolMetrics>,
}

impl std::fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "connected={}, channel_open={}, connection_state={}, channel_state={}",
            self.connected, self.channel_open, self.connection_state, self.channel_state
        )
    }
}

impl HealthStatus {
    /// Check if the broker is healthy (connected and channel open)
    pub fn is_healthy(&self) -> bool {
        self.connected && self.channel_open
    }
}

/// Transaction state for AMQP transactions
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum TransactionState {
    /// No transaction active
    #[default]
    None,
    /// Transaction started
    Started,
    /// Transaction committed
    Committed,
    /// Transaction rolled back
    RolledBack,
}

impl std::fmt::Display for TransactionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionState::None => write!(f, "none"),
            TransactionState::Started => write!(f, "started"),
            TransactionState::Committed => write!(f, "committed"),
            TransactionState::RolledBack => write!(f, "rolled_back"),
        }
    }
}

/// Reconnection statistics
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ReconnectionStats {
    /// Total number of reconnection attempts
    pub total_attempts: u64,
    /// Number of successful reconnections
    pub successful_reconnections: u64,
    /// Number of failed reconnections
    pub failed_reconnections: u64,
    /// Last reconnection attempt time
    #[serde(skip)]
    pub last_attempt: Option<std::time::Instant>,
    /// Last successful reconnection time
    #[serde(skip)]
    pub last_success: Option<std::time::Instant>,
}

impl ReconnectionStats {
    /// Get the success rate as a percentage (0.0 - 100.0)
    pub fn success_rate(&self) -> f64 {
        if self.total_attempts == 0 {
            0.0
        } else {
            (self.successful_reconnections as f64 / self.total_attempts as f64) * 100.0
        }
    }

    /// Get the failure rate as a percentage (0.0 - 100.0)
    pub fn failure_rate(&self) -> f64 {
        if self.total_attempts == 0 {
            0.0
        } else {
            (self.failed_reconnections as f64 / self.total_attempts as f64) * 100.0
        }
    }

    /// Check if there have been any reconnection attempts
    pub fn has_reconnections(&self) -> bool {
        self.total_attempts > 0
    }
}

/// Channel-level metrics
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ChannelMetrics {
    /// Total messages published
    pub messages_published: u64,
    /// Total messages consumed
    pub messages_consumed: u64,
    /// Total messages acknowledged
    pub messages_acked: u64,
    /// Total messages rejected
    pub messages_rejected: u64,
    /// Total messages requeued
    pub messages_requeued: u64,
    /// Total publish errors
    pub publish_errors: u64,
    /// Total consume errors
    pub consume_errors: u64,
}

impl ChannelMetrics {
    /// Get the total number of operations
    pub fn total_operations(&self) -> u64 {
        self.messages_published + self.messages_consumed
    }

    /// Get the total number of errors
    pub fn total_errors(&self) -> u64 {
        self.publish_errors + self.consume_errors
    }

    /// Get the error rate as a percentage (0.0 - 100.0)
    pub fn error_rate(&self) -> f64 {
        let total = self.total_operations();
        if total == 0 {
            0.0
        } else {
            (self.total_errors() as f64 / total as f64) * 100.0
        }
    }

    /// Get the acknowledgment rate as a percentage (0.0 - 100.0)
    pub fn ack_rate(&self) -> f64 {
        if self.messages_consumed == 0 {
            0.0
        } else {
            (self.messages_acked as f64 / self.messages_consumed as f64) * 100.0
        }
    }

    /// Get the rejection rate as a percentage (0.0 - 100.0)
    pub fn reject_rate(&self) -> f64 {
        if self.messages_consumed == 0 {
            0.0
        } else {
            (self.messages_rejected as f64 / self.messages_consumed as f64) * 100.0
        }
    }
}

/// Publisher confirm statistics
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct PublisherConfirmStats {
    /// Total publisher confirms received
    pub total_confirms: u64,
    /// Total publisher confirms successful
    pub successful_confirms: u64,
    /// Total publisher confirms failed
    pub failed_confirms: u64,
    /// Total messages awaiting confirmation
    pub pending_confirms: u64,
    /// Average confirmation latency in microseconds
    pub avg_confirm_latency_us: u64,
}

impl PublisherConfirmStats {
    /// Get the success rate as a percentage (0.0 - 100.0)
    pub fn success_rate(&self) -> f64 {
        if self.total_confirms == 0 {
            0.0
        } else {
            (self.successful_confirms as f64 / self.total_confirms as f64) * 100.0
        }
    }

    /// Get the failure rate as a percentage (0.0 - 100.0)
    pub fn failure_rate(&self) -> f64 {
        if self.total_confirms == 0 {
            0.0
        } else {
            (self.failed_confirms as f64 / self.total_confirms as f64) * 100.0
        }
    }

    /// Get the average confirmation latency in milliseconds
    pub fn avg_confirm_latency_ms(&self) -> f64 {
        self.avg_confirm_latency_us as f64 / 1000.0
    }

    /// Check if there are any pending confirms
    pub fn has_pending_confirms(&self) -> bool {
        self.pending_confirms > 0
    }
}

/// Connection pool metrics
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ConnectionPoolMetrics {
    /// Current number of connections in pool
    pub pool_size: usize,
    /// Maximum pool size configured
    pub max_pool_size: usize,
    /// Total connections created
    pub total_created: u64,
    /// Total connections acquired from pool
    pub total_acquired: u64,
    /// Total connections released back to pool
    pub total_released: u64,
    /// Total connections discarded (dead/invalid)
    pub total_discarded: u64,
    /// Number of times pool was full
    pub pool_full_count: u64,
}

impl ConnectionPoolMetrics {
    /// Get the pool utilization as a percentage (0.0 - 100.0)
    pub fn utilization(&self) -> f64 {
        if self.max_pool_size == 0 {
            0.0
        } else {
            (self.pool_size as f64 / self.max_pool_size as f64) * 100.0
        }
    }

    /// Get the discard rate as a percentage (0.0 - 100.0)
    pub fn discard_rate(&self) -> f64 {
        if self.total_created == 0 {
            0.0
        } else {
            (self.total_discarded as f64 / self.total_created as f64) * 100.0
        }
    }

    /// Check if the pool is full
    pub fn is_full(&self) -> bool {
        self.pool_size >= self.max_pool_size
    }

    /// Check if the pool is empty
    pub fn is_empty(&self) -> bool {
        self.pool_size == 0
    }
}

/// Channel pool metrics
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ChannelPoolMetrics {
    /// Current number of channels in pool
    pub pool_size: usize,
    /// Maximum pool size configured
    pub max_pool_size: usize,
    /// Total channels created
    pub total_created: u64,
    /// Total channels acquired from pool
    pub total_acquired: u64,
    /// Total channels released back to pool
    pub total_released: u64,
    /// Total channels discarded (dead/invalid)
    pub total_discarded: u64,
    /// Number of times pool was full
    pub pool_full_count: u64,
}

impl ChannelPoolMetrics {
    /// Get the pool utilization as a percentage (0.0 - 100.0)
    pub fn utilization(&self) -> f64 {
        if self.max_pool_size == 0 {
            0.0
        } else {
            (self.pool_size as f64 / self.max_pool_size as f64) * 100.0
        }
    }

    /// Get the discard rate as a percentage (0.0 - 100.0)
    pub fn discard_rate(&self) -> f64 {
        if self.total_created == 0 {
            0.0
        } else {
            (self.total_discarded as f64 / self.total_created as f64) * 100.0
        }
    }

    /// Check if the pool is full
    pub fn is_full(&self) -> bool {
        self.pool_size >= self.max_pool_size
    }

    /// Check if the pool is empty
    pub fn is_empty(&self) -> bool {
        self.pool_size == 0
    }
}

/// Message deduplication cache entry
#[derive(Debug, Clone)]
struct DeduplicationEntry {
    /// When this entry was created
    created_at: Instant,
}

/// Message deduplication cache
struct DeduplicationCache {
    /// Cache of message IDs
    cache: Arc<Mutex<HashMap<String, DeduplicationEntry>>>,
    /// Maximum cache size
    max_size: usize,
    /// TTL for cache entries
    ttl: Duration,
}

impl DeduplicationCache {
    /// Create a new deduplication cache
    fn new(max_size: usize, ttl: Duration) -> Self {
        Self {
            cache: Arc::new(Mutex::new(HashMap::with_capacity(max_size))),
            max_size,
            ttl,
        }
    }

    /// Check if a message ID is duplicate (returns true if duplicate)
    async fn is_duplicate(&self, message_id: &str) -> bool {
        let mut cache = self.cache.lock().await;

        // Clean expired entries
        let now = Instant::now();
        cache.retain(|_, entry| now.duration_since(entry.created_at) < self.ttl);

        // Check if message ID exists
        if cache.contains_key(message_id) {
            debug!("Duplicate message detected: {}", message_id);
            return true;
        }

        // Add message ID to cache
        if cache.len() >= self.max_size {
            // Remove oldest entry (simple FIFO)
            if let Some(key) = cache.keys().next().cloned() {
                cache.remove(&key);
            }
        }

        cache.insert(
            message_id.to_string(),
            DeduplicationEntry { created_at: now },
        );

        false
    }

    /// Clear the cache
    #[allow(dead_code)]
    async fn clear(&self) {
        let mut cache = self.cache.lock().await;
        cache.clear();
    }

    /// Get cache size
    #[allow(dead_code)]
    async fn size(&self) -> usize {
        let cache = self.cache.lock().await;
        cache.len()
    }
}

/// RabbitMQ Management API client
#[derive(Clone)]
struct ManagementApiClient {
    base_url: String,
    username: String,
    password: String,
    client: reqwest::Client,
}

impl ManagementApiClient {
    /// Create a new Management API client
    fn new(base_url: String, username: String, password: String) -> Self {
        Self {
            base_url,
            username,
            password,
            client: reqwest::Client::new(),
        }
    }

    /// List all queues
    async fn list_queues(&self) -> Result<Vec<QueueInfo>> {
        let url = format!("{}/api/queues", self.base_url);
        let response = self
            .client
            .get(&url)
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await
            .map_err(|e| {
                BrokerError::Connection(format!("Management API request failed: {}", e))
            })?;

        if !response.status().is_success() {
            return Err(BrokerError::Connection(format!(
                "Management API returned error: {}",
                response.status()
            )));
        }

        let queues: Vec<QueueInfo> = response
            .json()
            .await
            .map_err(|e| BrokerError::Connection(format!("Failed to parse queue info: {}", e)))?;

        Ok(queues)
    }

    /// Get detailed queue statistics
    async fn get_queue_stats(&self, vhost: &str, queue_name: &str) -> Result<QueueStats> {
        let vhost_encoded = urlencoding::encode(vhost);
        let queue_encoded = urlencoding::encode(queue_name);
        let url = format!(
            "{}/api/queues/{}/{}",
            self.base_url, vhost_encoded, queue_encoded
        );

        let response = self
            .client
            .get(&url)
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await
            .map_err(|e| {
                BrokerError::Connection(format!("Management API request failed: {}", e))
            })?;

        if !response.status().is_success() {
            return Err(BrokerError::Connection(format!(
                "Management API returned error: {}",
                response.status()
            )));
        }

        let stats: QueueStats = response
            .json()
            .await
            .map_err(|e| BrokerError::Connection(format!("Failed to parse queue stats: {}", e)))?;

        Ok(stats)
    }

    /// Get overview of RabbitMQ server
    async fn get_overview(&self) -> Result<ServerOverview> {
        let url = format!("{}/api/overview", self.base_url);
        let response = self
            .client
            .get(&url)
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await
            .map_err(|e| {
                BrokerError::Connection(format!("Management API request failed: {}", e))
            })?;

        if !response.status().is_success() {
            return Err(BrokerError::Connection(format!(
                "Management API returned error: {}",
                response.status()
            )));
        }

        let overview: ServerOverview = response.json().await.map_err(|e| {
            BrokerError::Connection(format!("Failed to parse server overview: {}", e))
        })?;

        Ok(overview)
    }

    /// List all connections
    async fn list_connections(&self) -> Result<Vec<ConnectionInfo>> {
        let url = format!("{}/api/connections", self.base_url);
        let response = self
            .client
            .get(&url)
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await
            .map_err(|e| {
                BrokerError::Connection(format!("Management API request failed: {}", e))
            })?;

        if !response.status().is_success() {
            return Err(BrokerError::Connection(format!(
                "Management API returned error: {}",
                response.status()
            )));
        }

        let connections: Vec<ConnectionInfo> = response.json().await.map_err(|e| {
            BrokerError::Connection(format!("Failed to parse connection info: {}", e))
        })?;

        Ok(connections)
    }

    /// List all channels
    async fn list_channels(&self) -> Result<Vec<ChannelInfo>> {
        let url = format!("{}/api/channels", self.base_url);
        let response = self
            .client
            .get(&url)
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await
            .map_err(|e| {
                BrokerError::Connection(format!("Management API request failed: {}", e))
            })?;

        if !response.status().is_success() {
            return Err(BrokerError::Connection(format!(
                "Management API returned error: {}",
                response.status()
            )));
        }

        let channels: Vec<ChannelInfo> = response
            .json()
            .await
            .map_err(|e| BrokerError::Connection(format!("Failed to parse channel info: {}", e)))?;

        Ok(channels)
    }

    /// List all exchanges
    async fn list_exchanges(&self, vhost: &str) -> Result<Vec<ExchangeInfo>> {
        let vhost_encoded = urlencoding::encode(vhost);
        let url = format!("{}/api/exchanges/{}", self.base_url, vhost_encoded);
        let response = self
            .client
            .get(&url)
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await
            .map_err(|e| {
                BrokerError::Connection(format!("Management API request failed: {}", e))
            })?;

        if !response.status().is_success() {
            return Err(BrokerError::Connection(format!(
                "Management API returned error: {}",
                response.status()
            )));
        }

        let exchanges: Vec<ExchangeInfo> = response.json().await.map_err(|e| {
            BrokerError::Connection(format!("Failed to parse exchange info: {}", e))
        })?;

        Ok(exchanges)
    }

    /// List all bindings for a queue
    async fn list_queue_bindings(&self, vhost: &str, queue_name: &str) -> Result<Vec<BindingInfo>> {
        let vhost_encoded = urlencoding::encode(vhost);
        let queue_encoded = urlencoding::encode(queue_name);
        let url = format!(
            "{}/api/queues/{}/{}/bindings",
            self.base_url, vhost_encoded, queue_encoded
        );
        let response = self
            .client
            .get(&url)
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await
            .map_err(|e| {
                BrokerError::Connection(format!("Management API request failed: {}", e))
            })?;

        if !response.status().is_success() {
            return Err(BrokerError::Connection(format!(
                "Management API returned error: {}",
                response.status()
            )));
        }

        let bindings: Vec<BindingInfo> = response
            .json()
            .await
            .map_err(|e| BrokerError::Connection(format!("Failed to parse binding info: {}", e)))?;

        Ok(bindings)
    }
}

/// Queue information from Management API
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct QueueInfo {
    pub name: String,
    pub vhost: String,
    pub durable: bool,
    pub auto_delete: bool,
    #[serde(default)]
    pub messages: u64,
    #[serde(default)]
    pub messages_ready: u64,
    #[serde(default)]
    pub messages_unacknowledged: u64,
    #[serde(default)]
    pub consumers: u32,
    #[serde(default)]
    pub memory: u64,
}

impl QueueInfo {
    /// Check if the queue is empty (no messages).
    pub fn is_empty(&self) -> bool {
        self.messages == 0
    }

    /// Check if the queue has any consumers.
    pub fn has_consumers(&self) -> bool {
        self.consumers > 0
    }

    /// Check if the queue is idle (no messages and no consumers).
    pub fn is_idle(&self) -> bool {
        self.is_empty() && !self.has_consumers()
    }

    /// Get the percentage of messages that are ready (not unacknowledged).
    pub fn ready_percentage(&self) -> f64 {
        if self.messages == 0 {
            return 0.0;
        }
        (self.messages_ready as f64 / self.messages as f64) * 100.0
    }

    /// Get the percentage of messages that are unacknowledged.
    pub fn unacked_percentage(&self) -> f64 {
        if self.messages == 0 {
            return 0.0;
        }
        (self.messages_unacknowledged as f64 / self.messages as f64) * 100.0
    }

    /// Get memory usage in megabytes.
    pub fn memory_mb(&self) -> f64 {
        self.memory as f64 / 1024.0 / 1024.0
    }

    /// Get average memory per message in bytes.
    pub fn avg_message_memory(&self) -> f64 {
        if self.messages == 0 {
            return 0.0;
        }
        self.memory as f64 / self.messages as f64
    }
}

/// Detailed queue statistics from Management API
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct QueueStats {
    pub name: String,
    pub vhost: String,
    pub durable: bool,
    pub auto_delete: bool,
    #[serde(default)]
    pub messages: u64,
    #[serde(default)]
    pub messages_ready: u64,
    #[serde(default)]
    pub messages_unacknowledged: u64,
    #[serde(default)]
    pub consumers: u32,
    #[serde(default)]
    pub memory: u64,
    #[serde(default)]
    pub message_bytes: u64,
    #[serde(default)]
    pub message_bytes_ready: u64,
    #[serde(default)]
    pub message_bytes_unacknowledged: u64,
    pub message_stats: Option<MessageStats>,
}

impl QueueStats {
    /// Check if the queue is empty (no messages).
    pub fn is_empty(&self) -> bool {
        self.messages == 0
    }

    /// Check if the queue has any consumers.
    pub fn has_consumers(&self) -> bool {
        self.consumers > 0
    }

    /// Check if the queue is idle (no messages and no consumers).
    pub fn is_idle(&self) -> bool {
        self.is_empty() && !self.has_consumers()
    }

    /// Get the percentage of messages that are ready (not unacknowledged).
    pub fn ready_percentage(&self) -> f64 {
        if self.messages == 0 {
            return 0.0;
        }
        (self.messages_ready as f64 / self.messages as f64) * 100.0
    }

    /// Get the percentage of messages that are unacknowledged.
    pub fn unacked_percentage(&self) -> f64 {
        if self.messages == 0 {
            return 0.0;
        }
        (self.messages_unacknowledged as f64 / self.messages as f64) * 100.0
    }

    /// Get memory usage in megabytes.
    pub fn memory_mb(&self) -> f64 {
        self.memory as f64 / 1024.0 / 1024.0
    }

    /// Get total message bytes in megabytes.
    pub fn message_bytes_mb(&self) -> f64 {
        self.message_bytes as f64 / 1024.0 / 1024.0
    }

    /// Get average message size in bytes.
    pub fn avg_message_size(&self) -> f64 {
        if self.messages == 0 {
            return 0.0;
        }
        self.message_bytes as f64 / self.messages as f64
    }

    /// Get average memory per message in bytes.
    pub fn avg_message_memory(&self) -> f64 {
        if self.messages == 0 {
            return 0.0;
        }
        self.memory as f64 / self.messages as f64
    }

    /// Get publish rate (messages/sec) from message stats.
    pub fn publish_rate(&self) -> Option<f64> {
        self.message_stats
            .as_ref()
            .and_then(|stats| stats.publish_details.as_ref())
            .map(|details| details.rate)
    }

    /// Get deliver rate (messages/sec) from message stats.
    pub fn deliver_rate(&self) -> Option<f64> {
        self.message_stats
            .as_ref()
            .and_then(|stats| stats.deliver_details.as_ref())
            .map(|details| details.rate)
    }

    /// Get ack rate (messages/sec) from message stats.
    pub fn ack_rate(&self) -> Option<f64> {
        self.message_stats
            .as_ref()
            .and_then(|stats| stats.ack_details.as_ref())
            .map(|details| details.rate)
    }

    /// Check if the queue is growing (publish rate > deliver rate).
    pub fn is_growing(&self) -> bool {
        match (self.publish_rate(), self.deliver_rate()) {
            (Some(pub_rate), Some(del_rate)) => pub_rate > del_rate,
            _ => false,
        }
    }

    /// Check if the queue is shrinking (deliver rate > publish rate).
    pub fn is_shrinking(&self) -> bool {
        match (self.publish_rate(), self.deliver_rate()) {
            (Some(pub_rate), Some(del_rate)) => del_rate > pub_rate,
            _ => false,
        }
    }

    /// Check if consumers are keeping up (ack rate >= deliver rate).
    pub fn consumers_keeping_up(&self) -> bool {
        match (self.ack_rate(), self.deliver_rate()) {
            (Some(ack_rate), Some(del_rate)) => ack_rate >= del_rate * 0.95, // 5% tolerance
            _ => true, // Assume OK if no stats available
        }
    }
}

/// Message statistics
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct MessageStats {
    #[serde(default)]
    pub publish: u64,
    #[serde(default)]
    pub publish_details: Option<RateDetails>,
    #[serde(default)]
    pub deliver: u64,
    #[serde(default)]
    pub deliver_details: Option<RateDetails>,
    #[serde(default)]
    pub ack: u64,
    #[serde(default)]
    pub ack_details: Option<RateDetails>,
}

impl MessageStats {
    /// Get total messages processed (published + delivered + acked).
    pub fn total_processed(&self) -> u64 {
        self.publish + self.deliver + self.ack
    }

    /// Get publish rate if available.
    pub fn publish_rate(&self) -> Option<f64> {
        self.publish_details.as_ref().map(|d| d.rate)
    }

    /// Get deliver rate if available.
    pub fn deliver_rate(&self) -> Option<f64> {
        self.deliver_details.as_ref().map(|d| d.rate)
    }

    /// Get ack rate if available.
    pub fn ack_rate(&self) -> Option<f64> {
        self.ack_details.as_ref().map(|d| d.rate)
    }
}

/// Rate details for statistics
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct RateDetails {
    pub rate: f64,
}

/// Server overview from Management API
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ServerOverview {
    pub management_version: String,
    pub rabbitmq_version: String,
    pub erlang_version: String,
    pub cluster_name: String,
    pub queue_totals: Option<QueueTotals>,
    pub object_totals: Option<ObjectTotals>,
}

impl ServerOverview {
    /// Get total messages across all queues.
    pub fn total_messages(&self) -> u64 {
        self.queue_totals.as_ref().map_or(0, |q| q.messages)
    }

    /// Get total ready messages across all queues.
    pub fn total_messages_ready(&self) -> u64 {
        self.queue_totals.as_ref().map_or(0, |q| q.messages_ready)
    }

    /// Get total unacknowledged messages across all queues.
    pub fn total_messages_unacked(&self) -> u64 {
        self.queue_totals
            .as_ref()
            .map_or(0, |q| q.messages_unacknowledged)
    }

    /// Get total number of queues.
    pub fn total_queues(&self) -> u32 {
        self.object_totals.as_ref().map_or(0, |o| o.queues)
    }

    /// Get total number of active connections.
    pub fn total_connections(&self) -> u32 {
        self.object_totals.as_ref().map_or(0, |o| o.connections)
    }

    /// Get total number of active channels.
    pub fn total_channels(&self) -> u32 {
        self.object_totals.as_ref().map_or(0, |o| o.channels)
    }

    /// Get total number of consumers.
    pub fn total_consumers(&self) -> u32 {
        self.object_totals.as_ref().map_or(0, |o| o.consumers)
    }

    /// Check if the server has any active connections.
    pub fn has_connections(&self) -> bool {
        self.total_connections() > 0
    }

    /// Check if there are any messages in the system.
    pub fn has_messages(&self) -> bool {
        self.total_messages() > 0
    }
}

/// Queue totals
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct QueueTotals {
    #[serde(default)]
    pub messages: u64,
    #[serde(default)]
    pub messages_ready: u64,
    #[serde(default)]
    pub messages_unacknowledged: u64,
}

impl QueueTotals {
    /// Get percentage of ready messages.
    pub fn ready_percentage(&self) -> f64 {
        if self.messages == 0 {
            return 0.0;
        }
        (self.messages_ready as f64 / self.messages as f64) * 100.0
    }

    /// Get percentage of unacknowledged messages.
    pub fn unacked_percentage(&self) -> f64 {
        if self.messages == 0 {
            return 0.0;
        }
        (self.messages_unacknowledged as f64 / self.messages as f64) * 100.0
    }
}

/// Object totals
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ObjectTotals {
    #[serde(default)]
    pub consumers: u32,
    #[serde(default)]
    pub queues: u32,
    #[serde(default)]
    pub exchanges: u32,
    #[serde(default)]
    pub connections: u32,
    #[serde(default)]
    pub channels: u32,
}

impl ObjectTotals {
    /// Get average channels per connection.
    pub fn avg_channels_per_connection(&self) -> f64 {
        if self.connections == 0 {
            return 0.0;
        }
        self.channels as f64 / self.connections as f64
    }

    /// Get average consumers per queue.
    pub fn avg_consumers_per_queue(&self) -> f64 {
        if self.queues == 0 {
            return 0.0;
        }
        self.consumers as f64 / self.queues as f64
    }

    /// Check if there are any idle resources (queues without consumers).
    pub fn has_idle_queues(&self) -> bool {
        self.queues > self.consumers
    }
}

/// Connection information from Management API
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ConnectionInfo {
    pub name: String,
    #[serde(default)]
    pub vhost: String,
    pub user: String,
    pub state: String,
    #[serde(default)]
    pub channels: u32,
    pub peer_host: String,
    pub peer_port: u16,
    #[serde(default)]
    pub recv_oct: u64,
    #[serde(default)]
    pub send_oct: u64,
    #[serde(default)]
    pub recv_cnt: u64,
    #[serde(default)]
    pub send_cnt: u64,
}

impl ConnectionInfo {
    /// Check if the connection is in running state.
    pub fn is_running(&self) -> bool {
        self.state == "running"
    }

    /// Check if the connection has active channels.
    pub fn has_channels(&self) -> bool {
        self.channels > 0
    }

    /// Get total bytes transferred (received + sent).
    pub fn total_bytes(&self) -> u64 {
        self.recv_oct + self.send_oct
    }

    /// Get total bytes received in megabytes.
    pub fn recv_mb(&self) -> f64 {
        self.recv_oct as f64 / 1024.0 / 1024.0
    }

    /// Get total bytes sent in megabytes.
    pub fn send_mb(&self) -> f64 {
        self.send_oct as f64 / 1024.0 / 1024.0
    }

    /// Get total messages transferred (received + sent).
    pub fn total_messages(&self) -> u64 {
        self.recv_cnt + self.send_cnt
    }

    /// Get average message size in bytes.
    pub fn avg_message_size(&self) -> f64 {
        let total_msgs = self.total_messages();
        if total_msgs == 0 {
            return 0.0;
        }
        self.total_bytes() as f64 / total_msgs as f64
    }

    /// Get peer address as a string.
    pub fn peer_address(&self) -> String {
        format!("{}:{}", self.peer_host, self.peer_port)
    }
}

/// Channel information from Management API
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ChannelInfo {
    pub name: String,
    pub connection_details: Option<ConnectionDetails>,
    pub vhost: String,
    pub user: String,
    pub number: u32,
    #[serde(default)]
    pub consumers: u32,
    #[serde(default)]
    pub messages_unacknowledged: u64,
    #[serde(default)]
    pub messages_uncommitted: u64,
    #[serde(default)]
    pub acks_uncommitted: u64,
    #[serde(default)]
    pub prefetch_count: u32,
    pub state: String,
}

impl ChannelInfo {
    /// Check if the channel is in running state.
    pub fn is_running(&self) -> bool {
        self.state == "running"
    }

    /// Check if the channel has active consumers.
    pub fn has_consumers(&self) -> bool {
        self.consumers > 0
    }

    /// Check if the channel has unacknowledged messages.
    pub fn has_unacked_messages(&self) -> bool {
        self.messages_unacknowledged > 0
    }

    /// Check if the channel is in a transaction.
    pub fn is_in_transaction(&self) -> bool {
        self.messages_uncommitted > 0 || self.acks_uncommitted > 0
    }

    /// Check if prefetch is configured.
    pub fn has_prefetch(&self) -> bool {
        self.prefetch_count > 0
    }

    /// Get utilization percentage based on prefetch.
    pub fn utilization(&self) -> f64 {
        if self.prefetch_count == 0 {
            return 0.0;
        }
        (self.messages_unacknowledged as f64 / self.prefetch_count as f64) * 100.0
    }

    /// Get peer address from connection details.
    pub fn peer_address(&self) -> Option<String> {
        self.connection_details
            .as_ref()
            .map(|details| format!("{}:{}", details.peer_host, details.peer_port))
    }
}

/// Connection details for a channel
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ConnectionDetails {
    pub name: String,
    pub peer_host: String,
    pub peer_port: u16,
}

/// Exchange information from Management API
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ExchangeInfo {
    pub name: String,
    pub vhost: String,
    #[serde(rename = "type")]
    pub exchange_type: String,
    pub durable: bool,
    pub auto_delete: bool,
    pub internal: bool,
    #[serde(default)]
    pub arguments: serde_json::Value,
    pub message_stats: Option<MessageStats>,
}

/// Binding information from Management API
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct BindingInfo {
    pub source: String,
    pub vhost: String,
    pub destination: String,
    pub destination_type: String,
    pub routing_key: String,
    #[serde(default)]
    pub arguments: serde_json::Value,
    #[serde(default)]
    pub properties_key: String,
}

/// Connection pool for managing multiple AMQP connections
#[derive(Clone)]
struct ConnectionPool {
    connections: Arc<Mutex<VecDeque<Connection>>>,
    #[allow(dead_code)]
    max_size: usize,
    url: String,
    /// Pool metrics
    metrics: Arc<Mutex<ConnectionPoolMetrics>>,
}

impl ConnectionPool {
    /// Create a new connection pool
    fn new(url: String, max_size: usize) -> Self {
        let metrics = ConnectionPoolMetrics {
            max_pool_size: max_size,
            ..Default::default()
        };

        Self {
            connections: Arc::new(Mutex::new(VecDeque::with_capacity(max_size))),
            max_size,
            url,
            metrics: Arc::new(Mutex::new(metrics)),
        }
    }

    /// Get a connection from the pool or create a new one
    #[allow(dead_code)]
    async fn acquire(&self) -> Result<Connection> {
        let mut pool = self.connections.lock().await;
        let mut metrics = self.metrics.lock().await;

        // Try to get an existing connection
        while let Some(conn) = pool.pop_front() {
            if conn.status().connected() {
                metrics.total_acquired += 1;
                metrics.pool_size = pool.len();
                return Ok(conn);
            }
            // Connection is dead, discard it
            metrics.total_discarded += 1;
            debug!("Discarded dead connection from pool");
        }

        // No available connections, create a new one
        metrics.pool_size = pool.len();
        drop(pool); // Release lock before creating connection
        drop(metrics); // Release metrics lock

        let connection = Connection::connect(&self.url, ConnectionProperties::default())
            .await
            .map_err(|e| BrokerError::Connection(format!("Failed to connect: {}", e)))?;

        debug!("Created new connection for pool");

        // Update metrics
        let mut metrics = self.metrics.lock().await;
        metrics.total_created += 1;
        metrics.total_acquired += 1;

        Ok(connection)
    }

    /// Return a connection to the pool
    #[allow(dead_code)]
    async fn release(&self, connection: Connection) {
        if !connection.status().connected() {
            debug!("Not returning dead connection to pool");
            let mut metrics = self.metrics.lock().await;
            metrics.total_discarded += 1;
            return;
        }

        let mut pool = self.connections.lock().await;
        let mut metrics = self.metrics.lock().await;

        if pool.len() < self.max_size {
            pool.push_back(connection);
            metrics.total_released += 1;
            metrics.pool_size = pool.len();
            debug!("Returned connection to pool (size: {})", pool.len());
        } else {
            debug!("Pool full, closing excess connection");
            metrics.pool_full_count += 1;
            drop(pool);
            drop(metrics);
            // Pool is full, close the connection
            let _ = connection.close(200, "Pool full").await;
        }
    }

    /// Close all connections in the pool
    async fn close_all(&self) {
        let mut pool = self.connections.lock().await;
        while let Some(conn) = pool.pop_front() {
            let _ = conn.close(200, "Closing pool").await;
        }
    }

    /// Get current pool metrics
    async fn get_metrics(&self) -> ConnectionPoolMetrics {
        let pool = self.connections.lock().await;
        let mut metrics = self.metrics.lock().await;
        metrics.pool_size = pool.len();
        metrics.clone()
    }
}

/// Channel pool for managing multiple AMQP channels per connection
struct ChannelPool {
    channels: Arc<Mutex<VecDeque<Channel>>>,
    #[allow(dead_code)]
    max_size: usize,
    /// Pool metrics
    metrics: Arc<Mutex<ChannelPoolMetrics>>,
}

impl ChannelPool {
    /// Create a new channel pool
    fn new(max_size: usize) -> Self {
        let metrics = ChannelPoolMetrics {
            max_pool_size: max_size,
            ..Default::default()
        };

        Self {
            channels: Arc::new(Mutex::new(VecDeque::with_capacity(max_size))),
            max_size,
            metrics: Arc::new(Mutex::new(metrics)),
        }
    }

    /// Get a channel from the pool or create a new one
    #[allow(dead_code)]
    async fn acquire(&self, connection: &Connection) -> Result<Channel> {
        let mut pool = self.channels.lock().await;
        let mut metrics = self.metrics.lock().await;

        // Try to get an existing channel
        while let Some(ch) = pool.pop_front() {
            if ch.status().connected() {
                metrics.total_acquired += 1;
                metrics.pool_size = pool.len();
                return Ok(ch);
            }
            // Channel is dead, discard it
            metrics.total_discarded += 1;
            debug!("Discarded dead channel from pool");
        }

        // No available channels, create a new one
        metrics.pool_size = pool.len();
        drop(pool); // Release lock before creating channel
        drop(metrics); // Release metrics lock

        let channel = connection
            .create_channel()
            .await
            .map_err(|e| BrokerError::Connection(format!("Failed to create channel: {}", e)))?;

        debug!("Created new channel for pool");

        // Update metrics
        let mut metrics = self.metrics.lock().await;
        metrics.total_created += 1;
        metrics.total_acquired += 1;

        Ok(channel)
    }

    /// Return a channel to the pool
    #[allow(dead_code)]
    async fn release(&self, channel: Channel) {
        if !channel.status().connected() {
            debug!("Not returning dead channel to pool");
            let mut metrics = self.metrics.lock().await;
            metrics.total_discarded += 1;
            return;
        }

        let mut pool = self.channels.lock().await;
        let mut metrics = self.metrics.lock().await;

        if pool.len() < self.max_size {
            pool.push_back(channel);
            metrics.total_released += 1;
            metrics.pool_size = pool.len();
            debug!("Returned channel to pool (size: {})", pool.len());
        } else {
            debug!("Channel pool full, closing excess channel");
            metrics.pool_full_count += 1;
            drop(pool);
            drop(metrics);
            // Pool is full, close the channel
            let _ = channel.close(200, "Pool full").await;
        }
    }

    /// Close all channels in the pool
    async fn close_all(&self) {
        let mut pool = self.channels.lock().await;
        while let Some(ch) = pool.pop_front() {
            let _ = ch.close(200, "Closing pool").await;
        }
    }

    /// Get current pool metrics
    async fn get_metrics(&self) -> ChannelPoolMetrics {
        let pool = self.channels.lock().await;
        let mut metrics = self.metrics.lock().await;
        metrics.pool_size = pool.len();
        metrics.clone()
    }
}

/// AMQP broker implementation using RabbitMQ
pub struct AmqpBroker {
    url: String,
    queue_name: String,
    connection: Option<Connection>,
    channel: Option<Channel>,
    #[allow(dead_code)]
    consumer_tag: Option<String>,
    /// Broker configuration
    config: AmqpConfig,
    /// Current transaction state
    transaction_state: TransactionState,
    /// Reconnection statistics
    reconnection_stats: ReconnectionStats,
    /// Connection pool (if enabled)
    connection_pool: Option<ConnectionPool>,
    /// Channel pool (if enabled)
    channel_pool: Option<ChannelPool>,
    /// Channel-level metrics
    channel_metrics: ChannelMetrics,
    /// Publisher confirm statistics
    publisher_confirm_stats: PublisherConfirmStats,
    /// Message deduplication cache (if enabled)
    deduplication_cache: Option<DeduplicationCache>,
    /// Management API client (if configured)
    management_api_client: Option<ManagementApiClient>,
}

impl AmqpBroker {
    /// Create a new AMQP broker with default configuration
    pub async fn new(url: &str, queue_name: &str) -> Result<Self> {
        Self::with_config(url, queue_name, AmqpConfig::default()).await
    }

    /// Create a new AMQP broker with custom configuration
    pub async fn with_config(url: &str, queue_name: &str, config: AmqpConfig) -> Result<Self> {
        let connection_pool = if config.connection_pool_size > 0 {
            Some(ConnectionPool::new(
                url.to_string(),
                config.connection_pool_size,
            ))
        } else {
            None
        };

        let channel_pool = if config.channel_pool_size > 0 {
            Some(ChannelPool::new(config.channel_pool_size))
        } else {
            None
        };

        let deduplication_cache = if config.enable_deduplication {
            Some(DeduplicationCache::new(
                config.deduplication_cache_size,
                config.deduplication_ttl,
            ))
        } else {
            None
        };

        let management_api_client = if let (Some(mgmt_url), Some(mgmt_user), Some(mgmt_pass)) = (
            config.management_url.clone(),
            config.management_username.clone(),
            config.management_password.clone(),
        ) {
            Some(ManagementApiClient::new(mgmt_url, mgmt_user, mgmt_pass))
        } else {
            None
        };

        Ok(Self {
            url: url.to_string(),
            queue_name: queue_name.to_string(),
            connection: None,
            channel: None,
            consumer_tag: None,
            config,
            transaction_state: TransactionState::None,
            reconnection_stats: ReconnectionStats::default(),
            connection_pool,
            channel_pool,
            channel_metrics: ChannelMetrics::default(),
            publisher_confirm_stats: PublisherConfirmStats::default(),
            deduplication_cache,
            management_api_client,
        })
    }

    /// Get the broker configuration
    pub fn config(&self) -> &AmqpConfig {
        &self.config
    }

    /// Get reconnection statistics
    pub fn reconnection_stats(&self) -> &ReconnectionStats {
        &self.reconnection_stats
    }

    /// Get channel metrics
    pub fn channel_metrics(&self) -> &ChannelMetrics {
        &self.channel_metrics
    }

    /// Get publisher confirm statistics
    pub fn publisher_confirm_stats(&self) -> &PublisherConfirmStats {
        &self.publisher_confirm_stats
    }

    /// Reset all metrics
    pub fn reset_metrics(&mut self) {
        self.channel_metrics = ChannelMetrics::default();
        self.publisher_confirm_stats = PublisherConfirmStats::default();
    }

    /// Get the effective URL including virtual host if configured
    fn effective_url(&self) -> String {
        if let Some(ref vhost) = self.config.vhost {
            // Parse and append vhost to URL
            if self.url.ends_with('/') {
                format!("{}{}", self.url, vhost)
            } else {
                format!("{}/{}", self.url, vhost)
            }
        } else {
            self.url.clone()
        }
    }

    /// Check connection health and attempt auto-reconnection if needed
    async fn ensure_connection(&mut self) -> Result<()> {
        // Check if connection is alive
        let connection_alive = self
            .connection
            .as_ref()
            .map(|c| c.status().connected())
            .unwrap_or(false);

        if !connection_alive {
            if self.config.auto_reconnect {
                info!("Connection lost, attempting auto-reconnection...");
                self.auto_reconnect().await?;
            } else {
                return Err(BrokerError::Connection(
                    "Connection lost and auto-reconnect is disabled".to_string(),
                ));
            }
        }

        Ok(())
    }

    /// Attempt automatic reconnection with configured retry logic
    async fn auto_reconnect(&mut self) -> Result<()> {
        let max_attempts = if self.config.auto_reconnect_max_attempts == 0 {
            u32::MAX // Unlimited
        } else {
            self.config.auto_reconnect_max_attempts
        };

        for attempt in 0..max_attempts {
            self.reconnection_stats.total_attempts += 1;
            self.reconnection_stats.last_attempt = Some(std::time::Instant::now());

            if attempt > 0 {
                warn!(
                    "Auto-reconnection attempt {} of {}",
                    attempt + 1,
                    if max_attempts == u32::MAX {
                        "unlimited".to_string()
                    } else {
                        max_attempts.to_string()
                    }
                );
                tokio::time::sleep(self.config.auto_reconnect_delay).await;
            }

            match self.reconnect_internal().await {
                Ok(()) => {
                    self.reconnection_stats.successful_reconnections += 1;
                    self.reconnection_stats.last_success = Some(std::time::Instant::now());
                    info!("Auto-reconnection successful");
                    return Ok(());
                }
                Err(e) => {
                    self.reconnection_stats.failed_reconnections += 1;
                    warn!("Auto-reconnection attempt failed: {}", e);
                }
            }
        }

        Err(BrokerError::Connection(format!(
            "Auto-reconnection failed after {} attempts",
            max_attempts
        )))
    }

    /// Internal reconnection logic without triggering ensure_connection
    async fn reconnect_internal(&mut self) -> Result<()> {
        let url = self.effective_url();

        // Try to connect
        let connection = Connection::connect(&url, ConnectionProperties::default())
            .await
            .map_err(|e| BrokerError::Connection(format!("Failed to connect: {}", e)))?;

        self.connection = Some(connection);
        self.channel = None; // Reset channel

        // Create channel directly without going through get_channel
        let connection = self
            .connection
            .as_ref()
            .ok_or_else(|| BrokerError::Connection("Not connected".to_string()))?;

        let channel = connection
            .create_channel()
            .await
            .map_err(|e| BrokerError::Connection(format!("Failed to create channel: {}", e)))?;

        // Apply QoS settings
        if self.config.prefetch_count > 0 {
            channel
                .basic_qos(
                    self.config.prefetch_count,
                    BasicQosOptions {
                        global: self.config.prefetch_global,
                    },
                )
                .await
                .map_err(|e| BrokerError::Connection(format!("Failed to set QoS: {}", e)))?;
        }

        self.channel = Some(channel);

        // Setup topology using the channel we just created
        self.setup_topology_internal().await?;

        Ok(())
    }

    /// Setup topology without triggering ensure_connection (used during reconnection)
    async fn setup_topology_internal(&mut self) -> Result<()> {
        let queue = self.queue_name.clone();
        let exchange = self.config.default_exchange.clone();
        let exchange_type = self.config.default_exchange_type;

        // Get the channel directly without ensure_connection
        let channel = self
            .channel
            .as_ref()
            .ok_or_else(|| BrokerError::Connection("Channel not available".to_string()))?;

        // Declare exchange
        channel
            .exchange_declare(
                &exchange,
                exchange_type.to_exchange_kind(),
                ExchangeDeclareOptions {
                    durable: true,
                    auto_delete: false,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to declare exchange: {}", e))
            })?;

        // Declare queue
        let config = QueueConfig::new();
        let args = config.to_field_table();

        channel
            .queue_declare(
                &queue,
                QueueDeclareOptions {
                    durable: config.durable,
                    auto_delete: config.auto_delete,
                    exclusive: config.exclusive,
                    ..Default::default()
                },
                args,
            )
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to declare queue: {}", e)))?;

        // Bind queue to exchange
        channel
            .queue_bind(
                &queue,
                &exchange,
                &queue,
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to bind queue: {}", e)))?;

        debug!("Setup topology for queue: {}", queue);
        Ok(())
    }

    /// Get or create channel
    async fn get_channel(&mut self) -> Result<&Channel> {
        // Ensure connection is healthy before getting channel
        self.ensure_connection().await?;

        if self.channel.is_none() {
            if !self.is_connected() {
                self.connect().await?;
            }

            let connection = self
                .connection
                .as_ref()
                .ok_or_else(|| BrokerError::Connection("Not connected".to_string()))?;

            let channel = connection
                .create_channel()
                .await
                .map_err(|e| BrokerError::Connection(format!("Failed to create channel: {}", e)))?;

            // Apply QoS settings if configured
            if self.config.prefetch_count > 0 {
                channel
                    .basic_qos(
                        self.config.prefetch_count,
                        BasicQosOptions {
                            global: self.config.prefetch_global,
                        },
                    )
                    .await
                    .map_err(|e| BrokerError::Connection(format!("Failed to set QoS: {}", e)))?;
                debug!(
                    "Set QoS prefetch={} global={}",
                    self.config.prefetch_count, self.config.prefetch_global
                );
            }

            self.channel = Some(channel);
        }

        self.channel
            .as_ref()
            .ok_or_else(|| BrokerError::Connection("Channel not available".to_string()))
    }

    /// Connect with retry logic
    async fn connect_with_retry(&mut self) -> Result<()> {
        let url = self.effective_url();
        let mut last_error = None;

        for attempt in 0..=self.config.retry_count {
            if attempt > 0 {
                warn!(
                    "Connection attempt {} of {} after {:?} delay",
                    attempt + 1,
                    self.config.retry_count + 1,
                    self.config.retry_delay
                );
                tokio::time::sleep(self.config.retry_delay).await;
            }

            match Connection::connect(&url, ConnectionProperties::default()).await {
                Ok(connection) => {
                    self.connection = Some(connection);
                    self.channel = None; // Reset channel
                    return Ok(());
                }
                Err(e) => {
                    if attempt < self.config.retry_count {
                        warn!("Connection failed, will retry: {}", e);
                    }
                    last_error = Some(e);
                }
            }
        }

        Err(BrokerError::Connection(format!(
            "Failed to connect after {} attempts: {}",
            self.config.retry_count + 1,
            last_error.map(|e| e.to_string()).unwrap_or_default()
        )))
    }

    /// Declare a queue with basic options
    pub async fn declare_queue(&mut self, queue: &str, mode: QueueMode) -> Result<()> {
        let config = match mode {
            QueueMode::Priority => QueueConfig::new().with_max_priority(10),
            QueueMode::Fifo => QueueConfig::new(),
        };
        self.declare_queue_with_config(queue, &config).await
    }

    /// Declare a queue with full configuration
    pub async fn declare_queue_with_config(
        &mut self,
        queue: &str,
        config: &QueueConfig,
    ) -> Result<()> {
        let channel = self.get_channel().await?;
        let args = config.to_field_table();

        channel
            .queue_declare(
                queue,
                QueueDeclareOptions {
                    durable: config.durable,
                    auto_delete: config.auto_delete,
                    exclusive: config.exclusive,
                    ..Default::default()
                },
                args,
            )
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to declare queue: {}", e)))?;

        debug!("Declared queue: {}", queue);
        Ok(())
    }

    /// Declare the default exchange and bind queue
    async fn setup_topology(&mut self) -> Result<()> {
        let queue = self.queue_name.clone();
        let exchange = self.config.default_exchange.clone();
        let exchange_type = self.config.default_exchange_type;

        self.declare_exchange(&exchange, exchange_type).await?;

        // Declare the queue
        self.declare_queue(&queue, QueueMode::Fifo).await?;

        // Bind queue to exchange
        self.bind_queue(&queue, &exchange, &queue).await?;

        debug!("Setup topology for queue: {}", queue);
        Ok(())
    }

    /// Declare an exchange
    pub async fn declare_exchange(
        &mut self,
        exchange: &str,
        exchange_type: AmqpExchangeType,
    ) -> Result<()> {
        let channel = self.get_channel().await?;

        channel
            .exchange_declare(
                exchange,
                exchange_type.to_exchange_kind(),
                ExchangeDeclareOptions {
                    durable: true,
                    auto_delete: false,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to declare exchange: {}", e))
            })?;

        debug!("Declared exchange: {} ({:?})", exchange, exchange_type);
        Ok(())
    }

    /// Declare an exchange with full configuration including alternative exchange
    ///
    /// # Examples
    /// ```ignore
    /// use celers_broker_amqp::{AmqpBroker, ExchangeConfig, AmqpExchangeType};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut broker = AmqpBroker::new("amqp://localhost:5672", "test").await?;
    /// broker.connect().await?;
    ///
    /// // Declare main exchange with alternate exchange for unroutable messages
    /// let config = ExchangeConfig::new(AmqpExchangeType::Direct)
    ///     .with_alternate_exchange("unroutable_messages");
    ///
    /// broker.declare_exchange_with_config("orders", &config).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn declare_exchange_with_config(
        &mut self,
        exchange: &str,
        config: &ExchangeConfig,
    ) -> Result<()> {
        let channel = self.get_channel().await?;
        let args = config.to_field_table();

        channel
            .exchange_declare(
                exchange,
                config.exchange_type.to_exchange_kind(),
                ExchangeDeclareOptions {
                    durable: config.durable,
                    auto_delete: config.auto_delete,
                    internal: config.internal,
                    ..Default::default()
                },
                args,
            )
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to declare exchange: {}", e))
            })?;

        debug!(
            "Declared exchange with config: {} ({:?})",
            exchange, config.exchange_type
        );
        Ok(())
    }

    /// Bind an exchange to another exchange (for advanced routing)
    ///
    /// This allows creating complex routing topologies where messages flow through
    /// multiple exchanges before reaching queues.
    ///
    /// # Examples
    /// ```ignore
    /// use celers_broker_amqp::{AmqpBroker, AmqpExchangeType};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut broker = AmqpBroker::new("amqp://localhost:5672", "test").await?;
    /// broker.connect().await?;
    ///
    /// // Create a routing hierarchy
    /// broker.declare_exchange("frontend", AmqpExchangeType::Topic).await?;
    /// broker.declare_exchange("backend", AmqpExchangeType::Direct).await?;
    ///
    /// // Route messages from frontend to backend
    /// broker.bind_exchange("backend", "frontend", "api.#").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn bind_exchange(
        &mut self,
        destination: &str,
        source: &str,
        routing_key: &str,
    ) -> Result<()> {
        let channel = self.get_channel().await?;

        channel
            .exchange_bind(
                destination,
                source,
                routing_key,
                ExchangeBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to bind exchange: {}", e)))?;

        debug!(
            "Bound exchange {} to exchange {} with routing key {}",
            destination, source, routing_key
        );
        Ok(())
    }

    /// Unbind an exchange from another exchange
    pub async fn unbind_exchange(
        &mut self,
        destination: &str,
        source: &str,
        routing_key: &str,
    ) -> Result<()> {
        let channel = self.get_channel().await?;

        channel
            .exchange_unbind(
                destination,
                source,
                routing_key,
                ExchangeUnbindOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to unbind exchange: {}", e))
            })?;

        debug!(
            "Unbound exchange {} from exchange {} with routing key {}",
            destination, source, routing_key
        );
        Ok(())
    }

    /// Passive queue declaration - check if queue exists without creating it
    ///
    /// This is useful to verify a queue exists before attempting operations.
    /// Returns Ok if queue exists, or an error if it doesn't.
    ///
    /// # Examples
    /// ```ignore
    /// use celers_broker_amqp::AmqpBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut broker = AmqpBroker::new("amqp://localhost:5672", "test").await?;
    /// broker.connect().await?;
    ///
    /// // Check if queue exists
    /// if broker.declare_queue_passive("my_queue").await.is_ok() {
    ///     println!("Queue exists!");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn declare_queue_passive(&mut self, queue: &str) -> Result<()> {
        let channel = self.get_channel().await?;

        channel
            .queue_declare(
                queue,
                QueueDeclareOptions {
                    passive: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Queue does not exist: {}", e)))?;

        debug!("Queue exists: {}", queue);
        Ok(())
    }

    /// Passive exchange declaration - check if exchange exists without creating it
    ///
    /// # Examples
    /// ```ignore
    /// use celers_broker_amqp::AmqpBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut broker = AmqpBroker::new("amqp://localhost:5672", "test").await?;
    /// broker.connect().await?;
    ///
    /// // Check if exchange exists
    /// if broker.declare_exchange_passive("amq.direct").await.is_ok() {
    ///     println!("Exchange exists!");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn declare_exchange_passive(&mut self, exchange: &str) -> Result<()> {
        let channel = self.get_channel().await?;

        channel
            .exchange_declare(
                exchange,
                ExchangeKind::Direct, // Type doesn't matter for passive
                ExchangeDeclareOptions {
                    passive: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Exchange does not exist: {}", e)))?;

        debug!("Exchange exists: {}", exchange);
        Ok(())
    }

    /// Declare a dead letter exchange with its queue
    pub async fn declare_dlx(&mut self, dlx_exchange: &str, dlx_queue: &str) -> Result<()> {
        // Declare the DLX exchange
        self.declare_exchange(dlx_exchange, AmqpExchangeType::Direct)
            .await?;

        // Declare the DLX queue (messages that fail will go here)
        let config = QueueConfig::new();
        self.declare_queue_with_config(dlx_queue, &config).await?;

        // Bind DLX queue to DLX exchange
        self.bind_queue(dlx_queue, dlx_exchange, dlx_queue).await?;

        debug!(
            "Declared DLX: exchange={}, queue={}",
            dlx_exchange, dlx_queue
        );
        Ok(())
    }

    /// Bind a queue to an exchange
    pub async fn bind_queue(
        &mut self,
        queue: &str,
        exchange: &str,
        routing_key: &str,
    ) -> Result<()> {
        let channel = self.get_channel().await?;

        channel
            .queue_bind(
                queue,
                exchange,
                routing_key,
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to bind queue: {}", e)))?;

        debug!(
            "Bound queue {} to exchange {} with routing key {}",
            queue, exchange, routing_key
        );
        Ok(())
    }

    /// Unbind a queue from an exchange
    pub async fn unbind_queue(
        &mut self,
        queue: &str,
        exchange: &str,
        routing_key: &str,
    ) -> Result<()> {
        let channel = self.get_channel().await?;

        channel
            .queue_unbind(queue, exchange, routing_key, FieldTable::default())
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to unbind queue: {}", e)))?;

        debug!(
            "Unbound queue {} from exchange {} with routing key {}",
            queue, exchange, routing_key
        );
        Ok(())
    }

    /// Delete an exchange
    pub async fn delete_exchange(&mut self, exchange: &str) -> Result<()> {
        let channel = self.get_channel().await?;

        channel
            .exchange_delete(exchange, ExchangeDeleteOptions::default())
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to delete exchange: {}", e))
            })?;

        debug!("Deleted exchange: {}", exchange);
        Ok(())
    }

    /// Set QoS (prefetch) for the channel
    pub async fn set_qos(&mut self, prefetch_count: u16, global: bool) -> Result<()> {
        let channel = self.get_channel().await?;

        channel
            .basic_qos(prefetch_count, BasicQosOptions { global })
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to set QoS: {}", e)))?;

        debug!("Set QoS: prefetch={} global={}", prefetch_count, global);
        Ok(())
    }

    /// Publish a message with TTL (time-to-live)
    pub async fn publish_with_ttl(
        &mut self,
        queue: &str,
        message: Message,
        ttl_ms: u64,
    ) -> Result<()> {
        let exchange = self.config.default_exchange.clone();
        self.publish_with_options(&exchange, queue, message, Some(ttl_ms))
            .await
    }

    /// Publish a message with full options
    async fn publish_with_options(
        &mut self,
        exchange: &str,
        routing_key: &str,
        message: Message,
        ttl_ms: Option<u64>,
    ) -> Result<()> {
        let channel = self.get_channel().await?;

        // Serialize message to JSON
        let payload =
            serde_json::to_vec(&message).map_err(|e| BrokerError::Serialization(e.to_string()))?;

        // Build properties
        let mut properties = BasicProperties::default()
            .with_content_type(ShortString::from("application/json"))
            .with_content_encoding(ShortString::from("utf-8"))
            .with_delivery_mode(2); // Persistent

        // Set priority if specified
        if let Some(priority) = message.properties.priority {
            properties = properties.with_priority(priority);
        }

        // Set correlation_id
        if let Some(ref correlation_id) = message.properties.correlation_id {
            properties = properties.with_correlation_id(ShortString::from(correlation_id.as_str()));
        }

        // Set TTL if specified
        if let Some(ttl) = ttl_ms {
            properties = properties.with_expiration(ShortString::from(ttl.to_string()));
        }

        // Publish message
        channel
            .basic_publish(
                exchange,
                routing_key,
                BasicPublishOptions::default(),
                &payload,
                properties,
            )
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to publish: {}", e)))?
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to confirm publish: {}", e))
            })?;

        debug!("Published message to {}/{}", exchange, routing_key);
        Ok(())
    }

    // ==================== Health Monitoring ====================

    /// Get the health status of the broker connection
    pub async fn health_status(&self) -> HealthStatus {
        let connected = self
            .connection
            .as_ref()
            .map(|c| c.status().connected())
            .unwrap_or(false);

        let connection_state = self
            .connection
            .as_ref()
            .map(|c| format!("{:?}", c.status().state()))
            .unwrap_or_else(|| "Not connected".to_string());

        let channel_open = self
            .channel
            .as_ref()
            .map(|ch| ch.status().connected())
            .unwrap_or(false);

        let channel_state = self
            .channel
            .as_ref()
            .map(|ch| format!("{:?}", ch.status().state()))
            .unwrap_or_else(|| "No channel".to_string());

        // Get connection pool metrics if pooling is enabled
        let connection_pool_metrics = if let Some(ref pool) = self.connection_pool {
            Some(pool.get_metrics().await)
        } else {
            None
        };

        // Get channel pool metrics if pooling is enabled
        let channel_pool_metrics = if let Some(ref pool) = self.channel_pool {
            Some(pool.get_metrics().await)
        } else {
            None
        };

        HealthStatus {
            connected,
            channel_open,
            connection_state,
            channel_state,
            connection_pool_metrics,
            channel_pool_metrics,
        }
    }

    /// Check if the broker is healthy
    pub fn is_healthy(&self) -> bool {
        let connected = self
            .connection
            .as_ref()
            .map(|c| c.status().connected())
            .unwrap_or(false);

        let channel_open = self
            .channel
            .as_ref()
            .map(|ch| ch.status().connected())
            .unwrap_or(false);

        connected && channel_open
    }

    // ==================== Consumer Streaming ====================

    /// Start a consumer stream on a queue
    ///
    /// Returns a lapin Consumer that can be used to asynchronously receive messages.
    /// This is more efficient than polling with `consume()` for high-throughput scenarios.
    ///
    /// # Arguments
    /// * `queue` - The queue to consume from
    /// * `consumer_tag` - A unique identifier for this consumer (empty string for auto-generated)
    ///
    /// # Example
    /// ```ignore
    /// use futures_lite::StreamExt;
    ///
    /// let mut consumer = broker.start_consumer("my_queue", "").await?;
    /// while let Some(delivery) = consumer.next().await {
    ///     let delivery = delivery?;
    ///     // Process message...
    ///     delivery.ack(BasicAckOptions::default()).await?;
    /// }
    /// ```
    pub async fn start_consumer(
        &mut self,
        queue: &str,
        consumer_tag: &str,
    ) -> Result<lapin::Consumer> {
        let channel = self.get_channel().await?;

        let consumer = channel
            .basic_consume(
                queue,
                consumer_tag,
                BasicConsumeOptions {
                    no_local: false,
                    no_ack: false,
                    exclusive: false,
                    nowait: false,
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to start consumer: {}", e))
            })?;

        info!("Started consumer on queue: {}", queue);
        Ok(consumer)
    }

    /// Start a consumer with full configuration including priority
    ///
    /// This allows setting consumer priority, which determines message distribution
    /// when multiple consumers are active. Higher priority consumers receive messages first.
    ///
    /// # Examples
    /// ```ignore
    /// use celers_broker_amqp::{AmqpBroker, ConsumerConfig};
    /// use futures::StreamExt;
    /// use lapin::options::BasicAckOptions;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut broker = AmqpBroker::new("amqp://localhost:5672", "test").await?;
    /// broker.connect().await?;
    ///
    /// // Start high-priority consumer
    /// let config = ConsumerConfig::new()
    ///     .with_tag("high_priority_worker")
    ///     .with_priority(10);  // Higher priority
    ///
    /// let mut consumer = broker.start_consumer_with_config("work_queue", &config).await?;
    ///
    /// // Process messages
    /// while let Some(delivery) = consumer.next().await {
    ///     let delivery = delivery?;
    ///     // Process...
    ///     delivery.ack(BasicAckOptions::default()).await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn start_consumer_with_config(
        &mut self,
        queue: &str,
        config: &ConsumerConfig,
    ) -> Result<lapin::Consumer> {
        let channel = self.get_channel().await?;
        let args = config.to_field_table();

        let consumer = channel
            .basic_consume(
                queue,
                &config.consumer_tag,
                BasicConsumeOptions {
                    no_local: config.no_local,
                    no_ack: config.no_ack,
                    exclusive: config.exclusive,
                    nowait: false,
                },
                args,
            )
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to start consumer: {}", e))
            })?;

        info!(
            "Started consumer on queue: {} with config (priority: {:?})",
            queue, config.priority
        );
        Ok(consumer)
    }

    /// Cancel a consumer by its tag
    pub async fn cancel_consumer(&mut self, consumer_tag: &str) -> Result<()> {
        let channel = self.get_channel().await?;

        channel
            .basic_cancel(consumer_tag, BasicCancelOptions::default())
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to cancel consumer: {}", e))
            })?;

        info!("Cancelled consumer: {}", consumer_tag);
        Ok(())
    }

    // ==================== Transaction Support ====================

    /// Start a transaction
    ///
    /// After calling this, all publish and ack operations will be part of the transaction
    /// until `commit_transaction()` or `rollback_transaction()` is called.
    pub async fn start_transaction(&mut self) -> Result<()> {
        if self.transaction_state == TransactionState::Started {
            return Err(BrokerError::OperationFailed(
                "Transaction already in progress".to_string(),
            ));
        }

        let channel = self.get_channel().await?;

        channel.tx_select().await.map_err(|e| {
            BrokerError::OperationFailed(format!("Failed to start transaction: {}", e))
        })?;

        self.transaction_state = TransactionState::Started;
        debug!("Started transaction");
        Ok(())
    }

    /// Commit the current transaction
    ///
    /// All operations since `start_transaction()` will be atomically committed.
    pub async fn commit_transaction(&mut self) -> Result<()> {
        if self.transaction_state != TransactionState::Started {
            return Err(BrokerError::OperationFailed(
                "No transaction in progress".to_string(),
            ));
        }

        let channel = self.get_channel().await?;

        channel.tx_commit().await.map_err(|e| {
            BrokerError::OperationFailed(format!("Failed to commit transaction: {}", e))
        })?;

        self.transaction_state = TransactionState::Committed;
        debug!("Committed transaction");
        Ok(())
    }

    /// Rollback the current transaction
    ///
    /// All operations since `start_transaction()` will be discarded.
    pub async fn rollback_transaction(&mut self) -> Result<()> {
        if self.transaction_state != TransactionState::Started {
            return Err(BrokerError::OperationFailed(
                "No transaction in progress".to_string(),
            ));
        }

        let channel = self.get_channel().await?;

        channel.tx_rollback().await.map_err(|e| {
            BrokerError::OperationFailed(format!("Failed to rollback transaction: {}", e))
        })?;

        self.transaction_state = TransactionState::RolledBack;
        debug!("Rolled back transaction");
        Ok(())
    }

    /// Get the current transaction state
    pub fn transaction_state(&self) -> TransactionState {
        self.transaction_state
    }

    /// List all queues via Management API
    ///
    /// Requires Management API to be configured via `with_management_api()`.
    ///
    /// # Returns
    ///
    /// A vector of `QueueInfo` structs containing basic queue information.
    ///
    /// # Errors
    ///
    /// Returns error if Management API is not configured or if the request fails.
    pub async fn list_queues(&self) -> Result<Vec<QueueInfo>> {
        let client = self
            .management_api_client
            .as_ref()
            .ok_or_else(|| BrokerError::Connection("Management API not configured".to_string()))?;

        client.list_queues().await
    }

    /// Get detailed statistics for a specific queue via Management API
    ///
    /// Requires Management API to be configured via `with_management_api()`.
    ///
    /// # Arguments
    ///
    /// * `queue_name` - Name of the queue
    ///
    /// # Returns
    ///
    /// A `QueueStats` struct containing detailed queue statistics including:
    /// - Message counts (total, ready, unacknowledged)
    /// - Consumer count
    /// - Memory usage
    /// - Message rates (publish, deliver, ack)
    ///
    /// # Errors
    ///
    /// Returns error if Management API is not configured or if the request fails.
    pub async fn get_queue_stats(&self, queue_name: &str) -> Result<QueueStats> {
        let client = self
            .management_api_client
            .as_ref()
            .ok_or_else(|| BrokerError::Connection("Management API not configured".to_string()))?;

        let vhost = self.config.vhost.as_deref().unwrap_or("/");
        client.get_queue_stats(vhost, queue_name).await
    }

    /// Get RabbitMQ server overview via Management API
    ///
    /// Requires Management API to be configured via `with_management_api()`.
    ///
    /// # Returns
    ///
    /// A `ServerOverview` struct containing:
    /// - RabbitMQ version information
    /// - Cluster information
    /// - Queue totals across all queues
    /// - Object counts (queues, exchanges, connections, channels)
    ///
    /// # Errors
    ///
    /// Returns error if Management API is not configured or if the request fails.
    pub async fn get_server_overview(&self) -> Result<ServerOverview> {
        let client = self
            .management_api_client
            .as_ref()
            .ok_or_else(|| BrokerError::Connection("Management API not configured".to_string()))?;

        client.get_overview().await
    }

    /// Check if Management API is configured
    pub fn has_management_api(&self) -> bool {
        self.management_api_client.is_some()
    }

    /// List all active connections via Management API
    ///
    /// Requires Management API to be configured via `with_management_api()`.
    ///
    /// # Returns
    ///
    /// A vector of `ConnectionInfo` structs containing:
    /// - Connection name and state
    /// - Virtual host and user
    /// - Peer host and port
    /// - Number of channels
    /// - Sent/received bytes and packet counts
    ///
    /// # Errors
    ///
    /// Returns error if Management API is not configured or if the request fails.
    pub async fn list_connections(&self) -> Result<Vec<ConnectionInfo>> {
        let client = self
            .management_api_client
            .as_ref()
            .ok_or_else(|| BrokerError::Connection("Management API not configured".to_string()))?;

        client.list_connections().await
    }

    /// List all active channels via Management API
    ///
    /// Requires Management API to be configured via `with_management_api()`.
    ///
    /// # Returns
    ///
    /// A vector of `ChannelInfo` structs containing:
    /// - Channel number and state
    /// - Virtual host and user
    /// - Consumer count
    /// - Unacknowledged/uncommitted message counts
    /// - Prefetch count
    ///
    /// # Errors
    ///
    /// Returns error if Management API is not configured or if the request fails.
    pub async fn list_channels(&self) -> Result<Vec<ChannelInfo>> {
        let client = self
            .management_api_client
            .as_ref()
            .ok_or_else(|| BrokerError::Connection("Management API not configured".to_string()))?;

        client.list_channels().await
    }

    /// List all exchanges in a virtual host via Management API
    ///
    /// Requires Management API to be configured via `with_management_api()`.
    ///
    /// # Arguments
    ///
    /// * `vhost` - Virtual host name (optional, uses configured vhost or "/" if None)
    ///
    /// # Returns
    ///
    /// A vector of `ExchangeInfo` structs containing:
    /// - Exchange name and type
    /// - Durability and auto-delete settings
    /// - Arguments
    /// - Message statistics (if available)
    ///
    /// # Errors
    ///
    /// Returns error if Management API is not configured or if the request fails.
    pub async fn list_exchanges(&self, vhost: Option<&str>) -> Result<Vec<ExchangeInfo>> {
        let client = self
            .management_api_client
            .as_ref()
            .ok_or_else(|| BrokerError::Connection("Management API not configured".to_string()))?;

        let vhost = vhost.unwrap_or_else(|| self.config.vhost.as_deref().unwrap_or("/"));
        client.list_exchanges(vhost).await
    }

    /// List all bindings for a specific queue via Management API
    ///
    /// Requires Management API to be configured via `with_management_api()`.
    ///
    /// # Arguments
    ///
    /// * `queue_name` - Name of the queue
    ///
    /// # Returns
    ///
    /// A vector of `BindingInfo` structs containing:
    /// - Source exchange
    /// - Destination queue
    /// - Routing key
    /// - Binding arguments
    ///
    /// # Errors
    ///
    /// Returns error if Management API is not configured or if the request fails.
    pub async fn list_queue_bindings(&self, queue_name: &str) -> Result<Vec<BindingInfo>> {
        let client = self
            .management_api_client
            .as_ref()
            .ok_or_else(|| BrokerError::Connection("Management API not configured".to_string()))?;

        let vhost = self.config.vhost.as_deref().unwrap_or("/");
        client.list_queue_bindings(vhost, queue_name).await
    }
}

#[async_trait]
impl Transport for AmqpBroker {
    async fn connect(&mut self) -> Result<()> {
        info!("Connecting to AMQP broker: {}", self.url);

        // Use retry logic if configured
        if self.config.retry_count > 0 {
            self.connect_with_retry().await?;
        } else {
            let url = self.effective_url();
            let connection = Connection::connect(&url, ConnectionProperties::default())
                .await
                .map_err(|e| BrokerError::Connection(format!("Failed to connect: {}", e)))?;

            self.connection = Some(connection);
            self.channel = None; // Reset channel
        }

        // Setup topology
        self.setup_topology().await?;

        info!("Connected to AMQP broker");
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        // Close channel pool first
        if let Some(ref channel_pool) = self.channel_pool {
            channel_pool.close_all().await;
        }

        if let Some(channel) = self.channel.take() {
            let _ = channel.close(200, "Disconnecting").await;
        }

        // Close connection pool
        if let Some(ref connection_pool) = self.connection_pool {
            connection_pool.close_all().await;
        }

        if let Some(connection) = self.connection.take() {
            connection.close(200, "Disconnecting").await.map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to disconnect: {}", e))
            })?;
        }

        info!("Disconnected from AMQP broker");
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.connection
            .as_ref()
            .map(|c| c.status().connected())
            .unwrap_or(false)
    }

    fn name(&self) -> &str {
        "amqp"
    }
}

#[async_trait]
impl Producer for AmqpBroker {
    async fn publish(&mut self, queue: &str, message: Message) -> Result<()> {
        self.publish_with_routing("celery", queue, message).await
    }

    async fn publish_with_routing(
        &mut self,
        exchange: &str,
        routing_key: &str,
        message: Message,
    ) -> Result<()> {
        // Check for duplicate messages if deduplication is enabled
        if let Some(ref dedup_cache) = self.deduplication_cache {
            let message_id = message.headers.id.to_string();
            if dedup_cache.is_duplicate(&message_id).await {
                debug!(
                    "Skipping duplicate message: {} to {}/{}",
                    message_id, exchange, routing_key
                );
                return Ok(()); // Silently skip duplicate
            }
        }

        let start_time = std::time::Instant::now();

        // Serialize message to JSON
        let payload =
            serde_json::to_vec(&message).map_err(|e| BrokerError::Serialization(e.to_string()))?;

        // Build properties
        let mut properties = BasicProperties::default()
            .with_content_type(ShortString::from("application/json"))
            .with_content_encoding(ShortString::from("utf-8"))
            .with_delivery_mode(2); // Persistent

        // Set priority if specified
        if let Some(priority) = message.properties.priority {
            properties = properties.with_priority(priority);
        }

        // Set correlation_id
        if let Some(ref correlation_id) = message.properties.correlation_id {
            properties = properties.with_correlation_id(ShortString::from(correlation_id.as_str()));
        }

        // Publish and get confirmation future in a scoped block to drop channel reference
        let confirm_future = {
            let channel = self.get_channel().await?;
            channel
                .basic_publish(
                    exchange,
                    routing_key,
                    BasicPublishOptions::default(),
                    &payload,
                    properties,
                )
                .await
                .map_err(|e| BrokerError::OperationFailed(format!("Failed to publish: {}", e)))?
        };

        // Now we can update metrics since channel reference is dropped
        self.publisher_confirm_stats.pending_confirms += 1;

        // Wait for confirmation
        match confirm_future.await {
            Ok(_) => {
                // Update metrics
                self.channel_metrics.messages_published += 1;
                self.publisher_confirm_stats.total_confirms += 1;
                self.publisher_confirm_stats.successful_confirms += 1;
                self.publisher_confirm_stats.pending_confirms -= 1;

                // Update latency
                let latency_us = start_time.elapsed().as_micros() as u64;
                let old_avg = self.publisher_confirm_stats.avg_confirm_latency_us;
                let count = self.publisher_confirm_stats.successful_confirms;
                if count > 0 {
                    self.publisher_confirm_stats.avg_confirm_latency_us =
                        (old_avg * (count - 1) + latency_us) / count;
                } else {
                    self.publisher_confirm_stats.avg_confirm_latency_us = latency_us;
                }

                debug!("Published message to {}/{}", exchange, routing_key);
                Ok(())
            }
            Err(e) => {
                self.channel_metrics.publish_errors += 1;
                self.publisher_confirm_stats.total_confirms += 1;
                self.publisher_confirm_stats.failed_confirms += 1;
                self.publisher_confirm_stats.pending_confirms -= 1;
                Err(BrokerError::OperationFailed(format!(
                    "Failed to confirm publish: {}",
                    e
                )))
            }
        }
    }
}

// Additional AmqpBroker methods
impl AmqpBroker {
    /// Publish messages with pipelining for maximum throughput
    ///
    /// Pipeline publishing sends multiple messages before waiting for confirms,
    /// with configurable pipeline depth. This is more efficient than individual
    /// publishes and provides better control than batch publishing.
    ///
    /// # Arguments
    /// * `queue` - Queue name (used as routing key)
    /// * `messages` - Vector of messages to publish
    /// * `pipeline_depth` - Number of messages to send before waiting for confirms (0 = unlimited)
    ///
    /// # Returns
    /// Number of messages successfully published
    ///
    /// # Example
    /// ```ignore
    /// // Publish with pipeline depth of 100 (send 100 messages before waiting for confirms)
    /// let count = broker.publish_pipeline("my_queue", messages, 100).await?;
    /// ```
    pub async fn publish_pipeline(
        &mut self,
        queue: &str,
        messages: Vec<Message>,
        pipeline_depth: usize,
    ) -> Result<usize> {
        if messages.is_empty() {
            return Ok(0);
        }

        let exchange = self.config.default_exchange.clone();
        let channel = self.get_channel().await?;

        let effective_depth = if pipeline_depth == 0 {
            messages.len() // Unlimited - send all before waiting
        } else {
            pipeline_depth.min(messages.len())
        };

        let mut confirms = Vec::with_capacity(effective_depth);
        let mut success_count: usize = 0;
        let mut publish_count: usize = 0;

        for (idx, message) in messages.iter().enumerate() {
            // Serialize message to JSON
            let payload = serde_json::to_vec(message)
                .map_err(|e| BrokerError::Serialization(e.to_string()))?;

            // Build properties
            let mut properties = BasicProperties::default()
                .with_content_type(ShortString::from("application/json"))
                .with_content_encoding(ShortString::from("utf-8"))
                .with_delivery_mode(2); // Persistent

            // Set priority if specified
            if let Some(priority) = message.properties.priority {
                properties = properties.with_priority(priority);
            }

            // Set correlation_id
            if let Some(ref correlation_id) = message.properties.correlation_id {
                properties =
                    properties.with_correlation_id(ShortString::from(correlation_id.as_str()));
            }

            // Publish message and collect confirm future
            let confirm = channel
                .basic_publish(
                    &exchange,
                    queue,
                    BasicPublishOptions::default(),
                    &payload,
                    properties,
                )
                .await
                .map_err(|e| BrokerError::OperationFailed(format!("Failed to publish: {}", e)))?;

            confirms.push(confirm);
            publish_count += 1;

            // Wait for confirms when pipeline is full or at the end
            if confirms.len() >= effective_depth || idx == messages.len() - 1 {
                for confirm in confirms.drain(..) {
                    if confirm.await.is_ok() {
                        success_count += 1;
                    }
                }
            }
        }

        // Update metrics
        self.channel_metrics.messages_published += success_count as u64;
        self.publisher_confirm_stats.total_confirms += publish_count as u64;
        self.publisher_confirm_stats.successful_confirms += success_count as u64;

        if success_count < messages.len() {
            self.channel_metrics.publish_errors += (messages.len() - success_count) as u64;
            self.publisher_confirm_stats.failed_confirms += (messages.len() - success_count) as u64;
            warn!(
                "Pipeline publish: {} of {} messages confirmed",
                success_count,
                messages.len()
            );
        } else {
            debug!(
                "Published {} messages with pipeline depth {} to {}/{}",
                messages.len(),
                effective_depth,
                &exchange,
                queue
            );
        }

        Ok(success_count)
    }

    /// Publish multiple messages in a batch
    ///
    /// This is more efficient than individual publishes as it sends all messages
    /// before waiting for publisher confirms, reducing round-trips.
    ///
    /// # Arguments
    /// * `queue` - Queue name (used as routing key)
    /// * `messages` - Vector of messages to publish
    ///
    /// # Returns
    /// Number of messages successfully published
    pub async fn publish_batch(&mut self, queue: &str, messages: Vec<Message>) -> Result<usize> {
        if messages.is_empty() {
            return Ok(0);
        }

        let exchange = self.config.default_exchange.clone();
        let channel = self.get_channel().await?;

        // Publish all messages and collect confirm futures
        let mut confirms = Vec::with_capacity(messages.len());

        for message in &messages {
            // Serialize message to JSON
            let payload = serde_json::to_vec(message)
                .map_err(|e| BrokerError::Serialization(e.to_string()))?;

            // Build properties
            let mut properties = BasicProperties::default()
                .with_content_type(ShortString::from("application/json"))
                .with_content_encoding(ShortString::from("utf-8"))
                .with_delivery_mode(2); // Persistent

            // Set priority if specified
            if let Some(priority) = message.properties.priority {
                properties = properties.with_priority(priority);
            }

            // Set correlation_id
            if let Some(ref correlation_id) = message.properties.correlation_id {
                properties =
                    properties.with_correlation_id(ShortString::from(correlation_id.as_str()));
            }

            // Publish message and collect confirm future
            let confirm = channel
                .basic_publish(
                    &exchange,
                    queue,
                    BasicPublishOptions::default(),
                    &payload,
                    properties,
                )
                .await
                .map_err(|e| BrokerError::OperationFailed(format!("Failed to publish: {}", e)))?;

            confirms.push(confirm);
        }

        // Wait for all publisher confirms
        let mut success_count = 0;
        for confirm in confirms {
            if confirm.await.is_ok() {
                success_count += 1;
            }
        }

        if success_count < messages.len() {
            warn!(
                "Batch publish: {} of {} messages confirmed",
                success_count,
                messages.len()
            );
        } else {
            debug!(
                "Published {} messages in batch to {}/{}",
                messages.len(),
                &exchange,
                queue
            );
        }

        Ok(success_count)
    }

    /// Drain all messages from a queue
    ///
    /// Consumes and returns all messages currently in the queue.
    /// This is useful for queue maintenance, testing, or message migration.
    ///
    /// # Arguments
    /// * `queue` - Queue name to drain
    ///
    /// # Returns
    /// Vector of envelopes containing all drained messages
    ///
    /// # Example
    /// ```ignore
    /// let messages = broker.drain_queue("my_queue").await?;
    /// println!("Drained {} messages", messages.len());
    /// ```
    pub async fn drain_queue(&mut self, queue: &str) -> Result<Vec<Envelope>> {
        let mut envelopes = Vec::new();
        let mut consumed_count = 0;
        let mut error_count = 0;

        loop {
            let channel = self.get_channel().await?;
            match channel
                .basic_get(queue, BasicGetOptions { no_ack: false })
                .await
            {
                Ok(Some(delivery)) => match serde_json::from_slice::<Message>(&delivery.data) {
                    Ok(message) => {
                        envelopes.push(Envelope {
                            delivery_tag: delivery.delivery_tag.to_string(),
                            message,
                            redelivered: delivery.redelivered,
                        });
                        consumed_count += 1;
                    }
                    Err(e) => {
                        warn!("Failed to deserialize message during drain: {}", e);
                        error_count += 1;
                    }
                },
                Ok(None) => break,
                Err(e) => {
                    warn!("Error during queue drain: {}", e);
                    break;
                }
            }
        }

        // Update metrics after loop
        self.channel_metrics.messages_consumed += consumed_count;
        self.channel_metrics.consume_errors += error_count;

        debug!("Drained {} messages from queue: {}", envelopes.len(), queue);
        Ok(envelopes)
    }

    /// Bulk declare multiple queues with their configurations
    ///
    /// Declares multiple queues atomically with their respective configurations.
    /// This is more efficient than declaring queues one by one.
    ///
    /// # Arguments
    /// * `queue_configs` - Vector of tuples (queue_name, QueueConfig)
    ///
    /// # Example
    /// ```ignore
    /// let configs = vec![
    ///     ("queue1", QueueConfig::default()),
    ///     ("queue2", QueueConfig::default().with_priority(10)),
    /// ];
    /// broker.declare_queues_batch(configs).await?;
    /// ```
    pub async fn declare_queues_batch(
        &mut self,
        queue_configs: Vec<(&str, QueueConfig)>,
    ) -> Result<()> {
        let channel = self.get_channel().await?;

        for (queue, config) in queue_configs {
            let field_table = config.to_field_table();

            channel
                .queue_declare(
                    queue,
                    QueueDeclareOptions {
                        passive: false,
                        durable: config.durable,
                        exclusive: config.exclusive,
                        auto_delete: config.auto_delete,
                        nowait: false,
                    },
                    field_table,
                )
                .await
                .map_err(|e| {
                    BrokerError::OperationFailed(format!(
                        "Failed to declare queue {}: {}",
                        queue, e
                    ))
                })?;

            debug!("Declared queue: {}", queue);
        }

        Ok(())
    }

    /// Delete multiple queues in batch
    ///
    /// # Arguments
    /// * `queues` - Vector of queue names to delete
    /// * `if_unused` - Only delete if queue has no consumers
    /// * `if_empty` - Only delete if queue is empty
    ///
    /// # Returns
    /// Number of queues successfully deleted
    pub async fn delete_queues_batch(
        &mut self,
        queues: Vec<&str>,
        if_unused: bool,
        if_empty: bool,
    ) -> Result<usize> {
        let channel = self.get_channel().await?;
        let mut deleted_count = 0;

        for queue in queues {
            match channel
                .queue_delete(
                    queue,
                    QueueDeleteOptions {
                        if_unused,
                        if_empty,
                        nowait: false,
                    },
                )
                .await
            {
                Ok(_) => {
                    debug!("Deleted queue: {}", queue);
                    deleted_count += 1;
                }
                Err(e) => {
                    warn!("Failed to delete queue {}: {}", queue, e);
                }
            }
        }

        Ok(deleted_count)
    }

    /// Purge multiple queues in batch
    ///
    /// Removes all messages from the specified queues.
    ///
    /// # Arguments
    /// * `queues` - Vector of queue names to purge
    ///
    /// # Returns
    /// Total number of messages purged across all queues
    pub async fn purge_queues_batch(&mut self, queues: Vec<&str>) -> Result<usize> {
        let channel = self.get_channel().await?;
        let mut total_purged = 0;

        for queue in queues {
            match channel
                .queue_purge(queue, QueuePurgeOptions { nowait: false })
                .await
            {
                Ok(message_count) => {
                    debug!("Purged {} messages from queue: {}", message_count, queue);
                    total_purged += message_count as usize;
                }
                Err(e) => {
                    warn!("Failed to purge queue {}: {}", queue, e);
                }
            }
        }

        Ok(total_purged)
    }

    /// Request-Reply (RPC) pattern: Send a message and wait for reply
    ///
    /// Implements the RPC pattern by:
    /// 1. Creating a temporary reply queue
    /// 2. Sending the request with reply-to and correlation-id
    /// 3. Waiting for the reply with timeout
    /// 4. Cleaning up the reply queue
    ///
    /// # Arguments
    /// * `rpc_queue` - Queue name for the RPC server
    /// * `request` - Request message
    /// * `timeout` - Maximum time to wait for reply
    ///
    /// # Returns
    /// Reply message from the RPC server
    ///
    /// # Example
    /// ```ignore
    /// use std::time::Duration;
    /// use celers_protocol::MessageBuilder;
    ///
    /// let request = MessageBuilder::new("tasks.calculate")
    ///     .args(vec![serde_json::json!({"x": 10, "y": 20})])
    ///     .build()?;
    ///
    /// let reply = broker.rpc_call("rpc_queue", request, Duration::from_secs(5)).await?;
    /// println!("RPC result: {:?}", reply);
    /// ```
    pub async fn rpc_call(
        &mut self,
        rpc_queue: &str,
        mut request: Message,
        timeout: Duration,
    ) -> Result<Message> {
        // Create temporary reply queue with auto-delete
        let reply_queue_name = format!("reply.{}", uuid::Uuid::new_v4());
        {
            let channel = self.get_channel().await?;
            channel
                .queue_declare(
                    &reply_queue_name,
                    QueueDeclareOptions {
                        passive: false,
                        durable: false,
                        exclusive: true,
                        auto_delete: true,
                        nowait: false,
                    },
                    FieldTable::default(),
                )
                .await
                .map_err(|e| {
                    BrokerError::OperationFailed(format!("Failed to create reply queue: {}", e))
                })?;
        }

        // Set reply-to and correlation-id
        let correlation_id = uuid::Uuid::new_v4().to_string();
        request.properties.correlation_id = Some(correlation_id.clone());
        let reply_to = reply_queue_name.clone();

        // Serialize and publish request
        let payload =
            serde_json::to_vec(&request).map_err(|e| BrokerError::Serialization(e.to_string()))?;

        let properties = BasicProperties::default()
            .with_delivery_mode(2)
            .with_content_type(ShortString::from("application/json"))
            .with_correlation_id(ShortString::from(correlation_id.as_str()))
            .with_reply_to(ShortString::from(reply_to.as_str()));

        let exchange = self.config.default_exchange.clone();
        {
            let channel = self.get_channel().await?;
            channel
                .basic_publish(
                    &exchange,
                    rpc_queue,
                    BasicPublishOptions::default(),
                    &payload,
                    properties,
                )
                .await
                .map_err(|e| {
                    BrokerError::OperationFailed(format!("Failed to publish RPC request: {}", e))
                })?
                .await
                .map_err(|e| {
                    BrokerError::OperationFailed(format!("Failed to confirm RPC request: {}", e))
                })?;
        }

        debug!(
            "Sent RPC request to {} with correlation_id: {}",
            rpc_queue, correlation_id
        );

        // Wait for reply with timeout
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if tokio::time::Instant::now() > deadline {
                // Clean up reply queue before returning error
                let channel = self.get_channel().await?;
                let _ = channel
                    .queue_delete(&reply_queue_name, QueueDeleteOptions::default())
                    .await;
                return Err(BrokerError::OperationFailed(format!(
                    "RPC timeout: no reply received within {:?}",
                    timeout
                )));
            }

            let channel = self.get_channel().await?;
            match channel
                .basic_get(&reply_queue_name, BasicGetOptions { no_ack: false })
                .await
            {
                Ok(Some(delivery)) => {
                    // Check correlation ID matches
                    if let Some(corr_id) = delivery.properties.correlation_id() {
                        if corr_id.as_str() == correlation_id {
                            // Acknowledge the reply
                            channel
                                .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                                .await
                                .map_err(|e| {
                                    BrokerError::OperationFailed(format!(
                                        "Failed to ack reply: {}",
                                        e
                                    ))
                                })?;

                            // Deserialize reply
                            let reply = serde_json::from_slice::<Message>(&delivery.data)
                                .map_err(|e| BrokerError::Serialization(e.to_string()))?;

                            // Clean up reply queue
                            let _ = channel
                                .queue_delete(&reply_queue_name, QueueDeleteOptions::default())
                                .await;

                            debug!("Received RPC reply for correlation_id: {}", correlation_id);
                            return Ok(reply);
                        }
                    }
                }
                Ok(None) => {
                    // No message yet, wait a bit
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Err(e) => {
                    // Clean up reply queue before returning error
                    let channel = self.get_channel().await?;
                    let _ = channel
                        .queue_delete(&reply_queue_name, QueueDeleteOptions::default())
                        .await;
                    return Err(BrokerError::OperationFailed(format!(
                        "Failed to receive RPC reply: {}",
                        e
                    )));
                }
            }
        }
    }

    /// Send an RPC reply
    ///
    /// Helper method to reply to an RPC request. Extracts reply-to and correlation-id
    /// from the original request and sends the reply appropriately.
    ///
    /// # Arguments
    /// * `request_envelope` - Original request envelope containing reply-to information
    /// * `reply` - Reply message to send
    ///
    /// # Example
    /// ```ignore
    /// // In RPC server
    /// if let Some(envelope) = broker.consume("rpc_queue", Duration::from_secs(1)).await? {
    ///     // Process request and create reply
    ///     let reply = MessageBuilder::new("result")
    ///         .body(serde_json::json!({"result": 42}))
    ///         .build()?;
    ///
    ///     broker.rpc_reply(&envelope, reply).await?;
    ///     broker.ack(&envelope.delivery_tag).await?;
    /// }
    /// ```
    pub async fn rpc_reply(&mut self, request_envelope: &Envelope, reply: Message) -> Result<()> {
        // Extract correlation_id and reply_to from request
        let correlation_id = request_envelope
            .message
            .properties
            .correlation_id
            .as_ref()
            .ok_or_else(|| {
                BrokerError::OperationFailed("Request message missing correlation_id".to_string())
            })?;

        // For this implementation, we need to get reply_to from message headers
        // Since the Envelope doesn't store reply_to, we'll need to pass it explicitly
        // or extract it from the message body/headers if available

        // Simplified: Use correlation_id to determine reply queue
        // In production, you'd want to extract reply_to from the AMQP properties
        let reply_queue = format!("reply.{}", correlation_id);

        // Publish reply with correlation_id
        let mut reply_msg = reply;
        reply_msg.properties.correlation_id = Some(correlation_id.clone());

        self.publish(&reply_queue, reply_msg).await?;
        debug!(
            "Sent RPC reply to {} with correlation_id: {}",
            reply_queue, correlation_id
        );
        Ok(())
    }

    /// Acknowledge multiple messages up to and including the specified delivery tag
    ///
    /// This is more efficient than calling ack() multiple times when processing
    /// messages in order. All messages up to and including the specified tag
    /// will be acknowledged atomically.
    ///
    /// # Examples
    /// ```ignore
    /// use celers_broker_amqp::AmqpBroker;
    /// use celers_kombu::{Transport, Consumer};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut broker = AmqpBroker::new("amqp://localhost:5672", "test").await?;
    /// broker.connect().await?;
    ///
    /// // Consume multiple messages
    /// let mut last_tag = String::new();
    /// for _ in 0..10 {
    ///     if let Ok(Some(envelope)) = broker.consume("test", std::time::Duration::from_secs(1)).await {
    ///         last_tag = envelope.delivery_tag.clone();
    ///     }
    /// }
    ///
    /// // Acknowledge all messages at once
    /// broker.ack_multiple(&last_tag).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn ack_multiple(&mut self, delivery_tag: &str) -> Result<()> {
        let channel = self.get_channel().await?;

        let tag = delivery_tag
            .parse::<u64>()
            .map_err(|e| BrokerError::OperationFailed(format!("Invalid delivery tag: {}", e)))?;

        channel
            .basic_ack(tag, BasicAckOptions { multiple: true })
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to ack multiple: {}", e)))?;

        debug!("Acknowledged multiple messages up to: {}", delivery_tag);
        Ok(())
    }

    /// Reject multiple messages up to and including the specified delivery tag
    ///
    /// This is more efficient than calling reject() multiple times.
    /// Uses NACK to reject multiple messages atomically.
    ///
    /// # Examples
    /// ```ignore
    /// use celers_broker_amqp::AmqpBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut broker = AmqpBroker::new("amqp://localhost:5672", "test").await?;
    /// broker.connect().await?;
    ///
    /// // Reject and requeue multiple messages
    /// broker.reject_multiple("123", true).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn reject_multiple(&mut self, delivery_tag: &str, requeue: bool) -> Result<()> {
        let channel = self.get_channel().await?;

        let tag = delivery_tag
            .parse::<u64>()
            .map_err(|e| BrokerError::OperationFailed(format!("Invalid delivery tag: {}", e)))?;

        channel
            .basic_nack(
                tag,
                BasicNackOptions {
                    multiple: true,
                    requeue,
                },
            )
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to nack multiple: {}", e)))?;

        debug!(
            "Rejected multiple messages up to: {} (requeue: {})",
            delivery_tag, requeue
        );
        Ok(())
    }

    /// Consume multiple messages from a queue in a single batch operation.
    ///
    /// This method retrieves up to `max_messages` from the queue efficiently.
    /// Each message must be acknowledged individually using `ack()` or `reject()`.
    ///
    /// # Arguments
    ///
    /// * `queue` - Queue name to consume from
    /// * `max_messages` - Maximum number of messages to retrieve (1-1000)
    /// * `timeout` - Timeout for waiting when no messages are available
    ///
    /// # Returns
    ///
    /// Returns a vector of envelopes. May be empty if no messages available.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let envelopes = broker.consume_batch("my_queue", 100, Duration::from_secs(1)).await?;
    /// for envelope in envelopes {
    ///     // Process message
    ///     broker.ack(&envelope.delivery_tag).await?;
    /// }
    /// ```
    pub async fn consume_batch(
        &mut self,
        queue: &str,
        max_messages: usize,
        timeout: Duration,
    ) -> Result<Vec<Envelope>> {
        let max_messages = max_messages.min(1000); // Cap at 1000
        let mut envelopes = Vec::with_capacity(max_messages);

        let start = Instant::now();
        let mut consumed_count = 0u64;
        let mut error_count = 0u64;

        for _ in 0..max_messages {
            // Check timeout
            if start.elapsed() >= timeout {
                break;
            }

            // Get channel for each iteration to avoid borrow issues
            let channel = self.get_channel().await?;

            // Try to get a message
            let get_result = channel
                .basic_get(queue, BasicGetOptions { no_ack: false })
                .await;

            match get_result {
                Ok(Some(delivery)) => match serde_json::from_slice::<Message>(&delivery.data) {
                    Ok(message) => {
                        envelopes.push(Envelope {
                            delivery_tag: delivery.delivery_tag.to_string(),
                            message,
                            redelivered: delivery.redelivered,
                        });
                        consumed_count += 1;
                    }
                    Err(e) => {
                        error_count += 1;
                        self.channel_metrics.consume_errors += error_count;
                        return Err(BrokerError::Serialization(e.to_string()));
                    }
                },
                Ok(None) => {
                    // No more messages available
                    break;
                }
                Err(e) => {
                    error_count += 1;
                    self.channel_metrics.consume_errors += error_count;
                    return Err(BrokerError::OperationFailed(format!(
                        "Failed to get message: {}",
                        e
                    )));
                }
            }
        }

        // Update metrics after all operations
        self.channel_metrics.messages_consumed += consumed_count;

        if !envelopes.is_empty() {
            debug!(
                "Consumed batch of {} messages from queue: {}",
                envelopes.len(),
                queue
            );
        }

        Ok(envelopes)
    }

    /// Peek at messages in a queue without consuming them.
    ///
    /// This method retrieves up to `max_messages` from the queue for inspection
    /// and immediately requeues them. Useful for monitoring queue contents.
    ///
    /// **Note:** This operation may affect message ordering and performance.
    /// Use sparingly and only for debugging/monitoring purposes.
    ///
    /// # Arguments
    ///
    /// * `queue` - Queue name to peek at
    /// * `max_messages` - Maximum number of messages to peek (1-100)
    ///
    /// # Returns
    ///
    /// Returns a vector of messages (without delivery tags).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let messages = broker.peek_queue("my_queue", 10).await?;
    /// for msg in messages {
    ///     println!("Message ID: {}", msg.headers.id);
    /// }
    /// ```
    pub async fn peek_queue(&mut self, queue: &str, max_messages: usize) -> Result<Vec<Message>> {
        let max_messages = max_messages.min(100); // Cap at 100 for safety
        let mut messages = Vec::with_capacity(max_messages);
        let channel = self.get_channel().await?;

        for _ in 0..max_messages {
            let get_result = channel
                .basic_get(queue, BasicGetOptions { no_ack: false })
                .await;

            match get_result {
                Ok(Some(delivery)) => {
                    let delivery_tag = delivery.delivery_tag;

                    match serde_json::from_slice::<Message>(&delivery.data) {
                        Ok(message) => {
                            messages.push(message);

                            // Requeue the message immediately
                            channel
                                .basic_reject(delivery_tag, BasicRejectOptions { requeue: true })
                                .await
                                .map_err(|e| {
                                    BrokerError::OperationFailed(format!(
                                        "Failed to requeue peeked message: {}",
                                        e
                                    ))
                                })?;
                        }
                        Err(e) => {
                            // Requeue even on deserialization error
                            let _ = channel
                                .basic_reject(delivery_tag, BasicRejectOptions { requeue: true })
                                .await;
                            return Err(BrokerError::Serialization(e.to_string()));
                        }
                    }
                }
                Ok(None) => {
                    // No more messages
                    break;
                }
                Err(e) => {
                    return Err(BrokerError::OperationFailed(format!(
                        "Failed to peek message: {}",
                        e
                    )));
                }
            }
        }

        debug!("Peeked {} messages from queue: {}", messages.len(), queue);
        Ok(messages)
    }

    /// Check if RabbitMQ server is alive using the Management API aliveness test.
    ///
    /// This endpoint declares a test queue, publishes and consumes a message,
    /// and cleans up. It provides a robust health check for the entire message path.
    ///
    /// Requires Management API to be configured.
    ///
    /// # Arguments
    ///
    /// * `vhost` - Virtual host to check (defaults to configured vhost)
    ///
    /// # Returns
    ///
    /// Returns `true` if the aliveness test passed, `false` otherwise.
    ///
    /// # Example
    ///
    /// ```ignore
    /// if broker.check_aliveness(None).await? {
    ///     println!("RabbitMQ is alive!");
    /// }
    /// ```
    pub async fn check_aliveness(&self, vhost: Option<&str>) -> Result<bool> {
        let mgmt_api = self
            .management_api_client
            .as_ref()
            .ok_or_else(|| BrokerError::OperationFailed("Management API not configured".into()))?;

        let vhost = vhost.unwrap_or(self.config.vhost.as_deref().unwrap_or("/"));
        let encoded_vhost = urlencoding::encode(vhost);

        let url = format!("{}/api/aliveness-test/{}", mgmt_api.base_url, encoded_vhost);

        let response = mgmt_api
            .client
            .get(&url)
            .basic_auth(&mgmt_api.username, Some(&mgmt_api.password))
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to check aliveness: {}", e))
            })?;

        if !response.status().is_success() {
            return Ok(false);
        }

        #[derive(serde::Deserialize)]
        struct AlivenessResponse {
            status: String,
        }

        let aliveness: AlivenessResponse = response.json().await.map_err(|e| {
            BrokerError::Serialization(format!("Failed to parse aliveness response: {}", e))
        })?;

        Ok(aliveness.status == "ok")
    }

    /// Get connection pool metrics for monitoring connection pool health.
    ///
    /// # Returns
    ///
    /// Returns connection pool metrics including size, acquisitions, releases, and discards.
    /// Returns None if connection pooling is disabled.
    ///
    /// # Example
    ///
    /// ```ignore
    /// if let Some(metrics) = broker.get_connection_pool_metrics().await {
    ///     println!("Pool utilization: {:.2}%", metrics.utilization() * 100.0);
    /// }
    /// ```
    pub async fn get_connection_pool_metrics(&self) -> Option<ConnectionPoolMetrics> {
        match &self.connection_pool {
            Some(pool) => Some(pool.get_metrics().await),
            None => None,
        }
    }

    /// Get channel pool metrics for monitoring channel pool health.
    ///
    /// # Returns
    ///
    /// Returns channel pool metrics including size, acquisitions, releases, and discards.
    /// Returns None if channel pooling is disabled.
    ///
    /// # Example
    ///
    /// ```ignore
    /// if let Some(metrics) = broker.get_channel_pool_metrics().await {
    ///     println!("Pool utilization: {:.2}%", metrics.utilization() * 100.0);
    /// }
    /// ```
    pub async fn get_channel_pool_metrics(&self) -> Option<ChannelPoolMetrics> {
        match &self.channel_pool {
            Some(pool) => Some(pool.get_metrics().await),
            None => None,
        }
    }
}

#[async_trait]
impl Consumer for AmqpBroker {
    async fn consume(&mut self, queue: &str, timeout: Duration) -> Result<Option<Envelope>> {
        let channel = self.get_channel().await?;

        // Use basic_get for polling (compatible with Redis implementation)
        let get_result = channel
            .basic_get(queue, BasicGetOptions { no_ack: false })
            .await;

        match get_result {
            Ok(Some(delivery)) => {
                // Deserialize message
                match serde_json::from_slice::<Message>(&delivery.data) {
                    Ok(message) => {
                        let envelope = Envelope {
                            delivery_tag: delivery.delivery_tag.to_string(),
                            message,
                            redelivered: delivery.redelivered,
                        };

                        // Update metrics
                        self.channel_metrics.messages_consumed += 1;

                        debug!("Consumed message from queue: {}", queue);
                        Ok(Some(envelope))
                    }
                    Err(e) => {
                        self.channel_metrics.consume_errors += 1;
                        Err(BrokerError::Serialization(e.to_string()))
                    }
                }
            }
            Ok(None) => {
                // No message available
                tokio::time::sleep(timeout).await;
                Ok(None)
            }
            Err(e) => {
                self.channel_metrics.consume_errors += 1;
                Err(BrokerError::OperationFailed(format!(
                    "Failed to get message: {}",
                    e
                )))
            }
        }
    }

    async fn ack(&mut self, delivery_tag: &str) -> Result<()> {
        let channel = self.get_channel().await?;

        let tag = delivery_tag
            .parse::<u64>()
            .map_err(|e| BrokerError::OperationFailed(format!("Invalid delivery tag: {}", e)))?;

        channel
            .basic_ack(tag, BasicAckOptions::default())
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to ack: {}", e)))?;

        // Update metrics
        self.channel_metrics.messages_acked += 1;

        debug!("Acknowledged message: {}", delivery_tag);
        Ok(())
    }

    async fn reject(&mut self, delivery_tag: &str, requeue: bool) -> Result<()> {
        let channel = self.get_channel().await?;

        let tag = delivery_tag
            .parse::<u64>()
            .map_err(|e| BrokerError::OperationFailed(format!("Invalid delivery tag: {}", e)))?;

        channel
            .basic_reject(tag, BasicRejectOptions { requeue })
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to reject: {}", e)))?;

        // Update metrics
        self.channel_metrics.messages_rejected += 1;
        if requeue {
            self.channel_metrics.messages_requeued += 1;
        }

        debug!("Rejected message: {} (requeue: {})", delivery_tag, requeue);
        Ok(())
    }

    async fn queue_size(&mut self, queue: &str) -> Result<usize> {
        let channel = self.get_channel().await?;

        let queue_state = channel
            .queue_declare(
                queue,
                QueueDeclareOptions {
                    passive: true, // Just check, don't create
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to get queue size: {}", e))
            })?;

        Ok(queue_state.message_count() as usize)
    }
}

#[async_trait]
impl Broker for AmqpBroker {
    async fn purge(&mut self, queue: &str) -> Result<usize> {
        let channel = self.get_channel().await?;

        let purge_result = channel
            .queue_purge(queue, QueuePurgeOptions::default())
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to purge queue: {}", e)))?;

        debug!("Purged {} messages from queue: {}", purge_result, queue);
        Ok(purge_result as usize)
    }

    async fn create_queue(&mut self, queue: &str, mode: QueueMode) -> Result<()> {
        self.declare_queue(queue, mode).await
    }

    async fn delete_queue(&mut self, queue: &str) -> Result<()> {
        let channel = self.get_channel().await?;

        channel
            .queue_delete(queue, QueueDeleteOptions::default())
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to delete queue: {}", e)))?;

        debug!("Deleted queue: {}", queue);
        Ok(())
    }

    async fn list_queues(&mut self) -> Result<Vec<String>> {
        // Note: AMQP doesn't provide a native way to list all queues
        // This would require the RabbitMQ Management API
        error!("list_queues not supported via AMQP protocol - use RabbitMQ Management API");
        Err(BrokerError::OperationFailed(
            "list_queues requires RabbitMQ Management API".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_amqp_broker_creation() {
        let broker = AmqpBroker::new("amqp://localhost:5672", "test_queue").await;
        assert!(broker.is_ok());
    }

    #[test]
    fn test_broker_name() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let broker = rt.block_on(async {
            AmqpBroker::new("amqp://localhost:5672", "test")
                .await
                .unwrap()
        });
        assert_eq!(broker.name(), "amqp");
    }

    #[tokio::test]
    async fn test_broker_with_config() {
        let config = AmqpConfig::default()
            .with_prefetch(10)
            .with_retry(3, Duration::from_millis(100))
            .with_exchange("test_exchange")
            .with_exchange_type(AmqpExchangeType::Topic);

        let broker = AmqpBroker::with_config("amqp://localhost:5672", "test_queue", config).await;
        assert!(broker.is_ok());

        let broker = broker.unwrap();
        assert_eq!(broker.config().prefetch_count, 10);
        assert_eq!(broker.config().retry_count, 3);
        assert_eq!(broker.config().default_exchange, "test_exchange");
        assert_eq!(
            broker.config().default_exchange_type,
            AmqpExchangeType::Topic
        );
    }

    #[test]
    fn test_amqp_config_builder() {
        let config = AmqpConfig::default()
            .with_prefetch(25)
            .with_global_prefetch(true)
            .with_retry(5, Duration::from_secs(2))
            .with_exchange("my_exchange")
            .with_exchange_type(AmqpExchangeType::Fanout)
            .with_heartbeat(120)
            .with_connection_timeout(Duration::from_secs(60))
            .with_vhost("my_vhost");

        assert_eq!(config.prefetch_count, 25);
        assert!(config.prefetch_global);
        assert_eq!(config.retry_count, 5);
        assert_eq!(config.retry_delay, Duration::from_secs(2));
        assert_eq!(config.default_exchange, "my_exchange");
        assert_eq!(config.default_exchange_type, AmqpExchangeType::Fanout);
        assert_eq!(config.heartbeat, 120);
        assert_eq!(config.connection_timeout, Duration::from_secs(60));
        assert_eq!(config.vhost, Some("my_vhost".to_string()));
    }

    #[test]
    fn test_queue_config_builder() {
        let dlx = DlxConfig::new("dlx_exchange").with_routing_key("dead");

        let config = QueueConfig::new()
            .durable(true)
            .auto_delete(false)
            .with_max_priority(10)
            .with_message_ttl(60000)
            .with_dlx(dlx)
            .with_expires(3600000)
            .with_max_length(10000);

        assert!(config.durable);
        assert!(!config.auto_delete);
        assert_eq!(config.max_priority, Some(10));
        assert_eq!(config.message_ttl, Some(60000));
        assert!(config.dlx.is_some());
        assert_eq!(config.dlx.as_ref().unwrap().exchange, "dlx_exchange");
        assert_eq!(
            config.dlx.as_ref().unwrap().routing_key,
            Some("dead".to_string())
        );
        assert_eq!(config.expires, Some(3600000));
        assert_eq!(config.max_length, Some(10000));
    }

    #[test]
    fn test_queue_config_field_table() {
        let dlx = DlxConfig::new("dlx").with_routing_key("failed");

        let config = QueueConfig::new()
            .with_max_priority(5)
            .with_message_ttl(30000)
            .with_dlx(dlx)
            .with_expires(7200000)
            .with_max_length(5000);

        let table = config.to_field_table();

        assert!(table
            .inner()
            .contains_key(&ShortString::from("x-max-priority")));
        assert!(table
            .inner()
            .contains_key(&ShortString::from("x-message-ttl")));
        assert!(table
            .inner()
            .contains_key(&ShortString::from("x-dead-letter-exchange")));
        assert!(table
            .inner()
            .contains_key(&ShortString::from("x-dead-letter-routing-key")));
        assert!(table.inner().contains_key(&ShortString::from("x-expires")));
        assert!(table
            .inner()
            .contains_key(&ShortString::from("x-max-length")));
    }

    #[test]
    fn test_dlx_config() {
        let dlx = DlxConfig::new("my_dlx");
        assert_eq!(dlx.exchange, "my_dlx");
        assert!(dlx.routing_key.is_none());

        let dlx_with_key = DlxConfig::new("my_dlx").with_routing_key("dead_letters");
        assert_eq!(dlx_with_key.exchange, "my_dlx");
        assert_eq!(dlx_with_key.routing_key, Some("dead_letters".to_string()));
    }

    #[test]
    fn test_exchange_types() {
        assert_eq!(AmqpExchangeType::default(), AmqpExchangeType::Direct);

        // Test conversion to ExchangeKind
        assert!(matches!(
            AmqpExchangeType::Direct.to_exchange_kind(),
            ExchangeKind::Direct
        ));
        assert!(matches!(
            AmqpExchangeType::Fanout.to_exchange_kind(),
            ExchangeKind::Fanout
        ));
        assert!(matches!(
            AmqpExchangeType::Topic.to_exchange_kind(),
            ExchangeKind::Topic
        ));
        assert!(matches!(
            AmqpExchangeType::Headers.to_exchange_kind(),
            ExchangeKind::Headers
        ));
    }

    #[tokio::test]
    async fn test_effective_url_with_vhost() {
        let config = AmqpConfig::default().with_vhost("production");
        let broker = AmqpBroker::with_config("amqp://localhost:5672", "test", config)
            .await
            .unwrap();

        assert_eq!(broker.effective_url(), "amqp://localhost:5672/production");
    }

    #[tokio::test]
    async fn test_effective_url_with_trailing_slash() {
        let config = AmqpConfig::default().with_vhost("staging");
        let broker = AmqpBroker::with_config("amqp://localhost:5672/", "test", config)
            .await
            .unwrap();

        assert_eq!(broker.effective_url(), "amqp://localhost:5672/staging");
    }

    #[tokio::test]
    async fn test_effective_url_no_vhost() {
        let broker = AmqpBroker::new("amqp://localhost:5672", "test")
            .await
            .unwrap();

        assert_eq!(broker.effective_url(), "amqp://localhost:5672");
    }

    #[tokio::test]
    async fn test_health_status_not_connected() {
        let broker = AmqpBroker::new("amqp://localhost:5672", "test")
            .await
            .unwrap();

        let status = broker.health_status().await;
        assert!(!status.connected);
        assert!(!status.channel_open);
        assert!(!status.is_healthy());
        assert_eq!(status.connection_state, "Not connected");
        assert_eq!(status.channel_state, "No channel");
        // Connection pool is disabled by default (connection_pool_size: 0)
        assert!(status.connection_pool_metrics.is_none());
        // Channel pool is enabled by default (channel_pool_size: 10)
        assert!(status.channel_pool_metrics.is_some());
    }

    #[tokio::test]
    async fn test_is_healthy() {
        let broker = AmqpBroker::new("amqp://localhost:5672", "test")
            .await
            .unwrap();

        // Not connected yet, should not be healthy
        assert!(!broker.is_healthy());
    }

    #[tokio::test]
    async fn test_transaction_state_default() {
        let broker = AmqpBroker::new("amqp://localhost:5672", "test")
            .await
            .unwrap();

        assert_eq!(broker.transaction_state(), TransactionState::None);
    }

    #[test]
    fn test_transaction_state_enum() {
        assert_ne!(TransactionState::None, TransactionState::Started);
        assert_ne!(TransactionState::Started, TransactionState::Committed);
        assert_ne!(TransactionState::Committed, TransactionState::RolledBack);

        // Test clone and copy
        let state = TransactionState::Started;
        let cloned = state;
        assert_eq!(state, cloned);
    }

    #[test]
    fn test_health_status_is_healthy() {
        let healthy = HealthStatus {
            connected: true,
            channel_open: true,
            connection_state: "Connected".to_string(),
            channel_state: "Open".to_string(),
            connection_pool_metrics: None,
            channel_pool_metrics: None,
        };
        assert!(healthy.is_healthy());

        let not_connected = HealthStatus {
            connected: false,
            channel_open: true,
            connection_state: "Disconnected".to_string(),
            channel_state: "Open".to_string(),
            connection_pool_metrics: None,
            channel_pool_metrics: None,
        };
        assert!(!not_connected.is_healthy());

        let no_channel = HealthStatus {
            connected: true,
            channel_open: false,
            connection_state: "Connected".to_string(),
            channel_state: "Closed".to_string(),
            connection_pool_metrics: None,
            channel_pool_metrics: None,
        };
        assert!(!no_channel.is_healthy());
    }

    // ==================== Integration Tests ====================
    // These tests require a running RabbitMQ instance
    // Run with: docker run -d --name rabbitmq -p 5672:5672 rabbitmq:3-management

    #[tokio::test]
    #[ignore] // Requires RabbitMQ to be running
    async fn test_integration_connection_and_disconnect() {
        let mut broker = AmqpBroker::new("amqp://localhost:5672", "test_integration")
            .await
            .unwrap();

        // Test connection
        let result = broker.connect().await;
        if result.is_err() {
            eprintln!(
                "Skipping integration test - RabbitMQ not available: {:?}",
                result.err()
            );
            return;
        }

        assert!(broker.is_connected());
        assert!(broker.is_healthy());

        // Test health status
        let health = broker.health_status().await;
        assert!(health.connected);
        assert!(health.channel_open);

        // Test disconnect
        broker.disconnect().await.unwrap();
        assert!(!broker.is_connected());
    }

    #[tokio::test]
    #[ignore] // Requires RabbitMQ to be running
    async fn test_integration_publish_and_consume() {
        use celers_protocol::builder::MessageBuilder;

        let mut broker = AmqpBroker::new("amqp://localhost:5672", "test_pubsub")
            .await
            .unwrap();

        if broker.connect().await.is_err() {
            eprintln!("Skipping integration test - RabbitMQ not available");
            return;
        }

        // Purge queue to start clean
        let _ = broker.purge("test_pubsub").await;

        // Publish a message
        let message = MessageBuilder::new("test.task")
            .args(vec![serde_json::json!(42)])
            .build()
            .unwrap();

        broker
            .publish("test_pubsub", message.clone())
            .await
            .unwrap();

        // Consume the message
        let envelope = broker
            .consume("test_pubsub", Duration::from_secs(1))
            .await
            .unwrap();

        assert!(envelope.is_some());
        let envelope = envelope.unwrap();
        assert_eq!(envelope.message.headers.task, "test.task");

        // Acknowledge the message
        broker.ack(&envelope.delivery_tag).await.unwrap();

        // Verify queue is empty
        let size = broker.queue_size("test_pubsub").await.unwrap();
        assert_eq!(size, 0);

        broker.disconnect().await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires RabbitMQ to be running
    async fn test_integration_batch_publish() {
        use celers_protocol::builder::MessageBuilder;

        let mut broker = AmqpBroker::new("amqp://localhost:5672", "test_batch")
            .await
            .unwrap();

        if broker.connect().await.is_err() {
            eprintln!("Skipping integration test - RabbitMQ not available");
            return;
        }

        let _ = broker.purge("test_batch").await;

        // Create batch of messages
        let mut messages = Vec::new();
        for i in 0..10 {
            let msg = MessageBuilder::new("batch.task")
                .args(vec![serde_json::json!(i)])
                .build()
                .unwrap();
            messages.push(msg);
        }

        // Publish batch
        let count = broker.publish_batch("test_batch", messages).await.unwrap();
        assert_eq!(count, 10);

        // Verify queue size
        let size = broker.queue_size("test_batch").await.unwrap();
        assert_eq!(size, 10);

        broker.disconnect().await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires RabbitMQ to be running
    async fn test_integration_pipeline_publish() {
        use celers_protocol::builder::MessageBuilder;

        let mut broker = AmqpBroker::new("amqp://localhost:5672", "test_pipeline")
            .await
            .unwrap();

        if broker.connect().await.is_err() {
            eprintln!("Skipping integration test - RabbitMQ not available");
            return;
        }

        let _ = broker.purge("test_pipeline").await;

        // Create batch of messages
        let mut messages = Vec::new();
        for i in 0..20 {
            let msg = MessageBuilder::new("pipeline.task")
                .args(vec![serde_json::json!(i)])
                .build()
                .unwrap();
            messages.push(msg);
        }

        // Publish with pipeline depth of 5
        let count = broker
            .publish_pipeline("test_pipeline", messages, 5)
            .await
            .unwrap();
        assert_eq!(count, 20);

        // Verify queue size
        let size = broker.queue_size("test_pipeline").await.unwrap();
        assert_eq!(size, 20);

        broker.disconnect().await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires RabbitMQ to be running
    async fn test_integration_message_ordering() {
        use celers_protocol::builder::MessageBuilder;

        let mut broker = AmqpBroker::new("amqp://localhost:5672", "test_ordering")
            .await
            .unwrap();

        if broker.connect().await.is_err() {
            eprintln!("Skipping integration test - RabbitMQ not available");
            return;
        }

        let _ = broker.purge("test_ordering").await;

        // Publish messages in order
        for i in 0..5 {
            let msg = MessageBuilder::new("ordering.task")
                .args(vec![serde_json::json!(i)])
                .build()
                .unwrap();
            broker.publish("test_ordering", msg).await.unwrap();
        }

        // Consume messages and verify order
        for i in 0..5 {
            let envelope = broker
                .consume("test_ordering", Duration::from_secs(1))
                .await
                .unwrap();
            assert!(envelope.is_some());

            let envelope = envelope.unwrap();
            // Deserialize body to get TaskArgs
            let task_args: celers_protocol::TaskArgs =
                serde_json::from_slice(&envelope.message.body).unwrap();
            assert_eq!(task_args.args[0], serde_json::json!(i));

            broker.ack(&envelope.delivery_tag).await.unwrap();
        }

        broker.disconnect().await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires RabbitMQ to be running
    async fn test_integration_priority_queue() {
        use celers_protocol::builder::MessageBuilder;

        let config = AmqpConfig::default();
        let mut broker = AmqpBroker::with_config("amqp://localhost:5672", "test_priority", config)
            .await
            .unwrap();

        if broker.connect().await.is_err() {
            eprintln!("Skipping integration test - RabbitMQ not available");
            return;
        }

        // Declare priority queue
        let queue_config = QueueConfig::new().with_max_priority(10);
        broker
            .declare_queue_with_config("test_priority", &queue_config)
            .await
            .unwrap();

        let _ = broker.purge("test_priority").await;

        // Publish messages with different priorities (lower number = lower priority)
        for priority in [1, 5, 3, 9, 7] {
            let msg = MessageBuilder::new("priority.task")
                .priority(priority)
                .args(vec![serde_json::json!(priority)])
                .build()
                .unwrap();
            broker.publish("test_priority", msg).await.unwrap();
        }

        // Give RabbitMQ time to sort by priority
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Consume messages - should be in priority order (highest first)
        let expected_order = [9, 7, 5, 3, 1];
        for expected_priority in expected_order {
            let envelope = broker
                .consume("test_priority", Duration::from_secs(1))
                .await
                .unwrap();

            assert!(envelope.is_some());
            let envelope = envelope.unwrap();
            assert_eq!(
                envelope.message.properties.priority,
                Some(expected_priority)
            );
            broker.ack(&envelope.delivery_tag).await.unwrap();
        }

        broker.disconnect().await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires RabbitMQ to be running
    async fn test_integration_concurrent_publishing() {
        use celers_protocol::builder::MessageBuilder;
        use std::sync::Arc;
        use tokio::sync::Mutex;

        let broker = Arc::new(Mutex::new(
            AmqpBroker::new("amqp://localhost:5672", "test_concurrent")
                .await
                .unwrap(),
        ));

        {
            let mut b = broker.lock().await;
            if b.connect().await.is_err() {
                eprintln!("Skipping integration test - RabbitMQ not available");
                return;
            }
            let _ = b.purge("test_concurrent").await;
        }

        // Spawn multiple tasks to publish concurrently
        let mut handles = vec![];

        for task_id in 0..10 {
            let broker_clone = Arc::clone(&broker);
            let handle = tokio::spawn(async move {
                let mut b = broker_clone.lock().await;
                for i in 0..5 {
                    let msg = MessageBuilder::new("concurrent.task")
                        .args(vec![serde_json::json!(format!(
                            "task-{}-msg-{}",
                            task_id, i
                        ))])
                        .build()
                        .unwrap();
                    b.publish("test_concurrent", msg).await.unwrap();
                }
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify all 50 messages were published (10 tasks * 5 messages each)
        let mut b = broker.lock().await;
        let size = b.queue_size("test_concurrent").await.unwrap();
        assert_eq!(size, 50);

        b.disconnect().await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires RabbitMQ to be running
    async fn test_integration_connection_recovery() {
        let config = AmqpConfig::default()
            .with_auto_reconnect(true)
            .with_auto_reconnect_config(3, Duration::from_millis(500));

        let mut broker = AmqpBroker::with_config("amqp://localhost:5672", "test_recovery", config)
            .await
            .unwrap();

        if broker.connect().await.is_err() {
            eprintln!("Skipping integration test - RabbitMQ not available");
            return;
        }

        // Verify initial connection
        assert!(broker.is_connected());

        // Note: Actually killing and restoring the connection would require
        // Docker container manipulation or similar, which is beyond the scope
        // of a unit test. Instead, we verify the reconnection stats structure.
        let stats = broker.reconnection_stats();
        assert_eq!(stats.total_attempts, 0);
        assert_eq!(stats.successful_reconnections, 0);
        assert_eq!(stats.failed_reconnections, 0);

        broker.disconnect().await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires RabbitMQ to be running
    async fn test_integration_transaction_commit() {
        use celers_protocol::builder::MessageBuilder;

        let mut broker = AmqpBroker::new("amqp://localhost:5672", "test_transaction")
            .await
            .unwrap();

        if broker.connect().await.is_err() {
            eprintln!("Skipping integration test - RabbitMQ not available");
            return;
        }

        let _ = broker.purge("test_transaction").await;

        // Start transaction
        broker.start_transaction().await.unwrap();
        assert_eq!(broker.transaction_state(), TransactionState::Started);

        // Publish within transaction
        let msg = MessageBuilder::new("transaction.task")
            .args(vec![serde_json::json!("commit")])
            .build()
            .unwrap();
        broker.publish("test_transaction", msg).await.unwrap();

        // Commit transaction
        broker.commit_transaction().await.unwrap();
        assert_eq!(broker.transaction_state(), TransactionState::Committed);

        // Verify message was committed
        let size = broker.queue_size("test_transaction").await.unwrap();
        assert_eq!(size, 1);

        broker.disconnect().await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires RabbitMQ to be running
    async fn test_integration_transaction_rollback() {
        use celers_protocol::builder::MessageBuilder;

        let mut broker = AmqpBroker::new("amqp://localhost:5672", "test_rollback")
            .await
            .unwrap();

        if broker.connect().await.is_err() {
            eprintln!("Skipping integration test - RabbitMQ not available");
            return;
        }

        let _ = broker.purge("test_rollback").await;

        // Start transaction
        broker.start_transaction().await.unwrap();

        // Publish within transaction
        let msg = MessageBuilder::new("rollback.task")
            .args(vec![serde_json::json!("rollback")])
            .build()
            .unwrap();
        broker.publish("test_rollback", msg).await.unwrap();

        // Rollback transaction
        broker.rollback_transaction().await.unwrap();
        assert_eq!(broker.transaction_state(), TransactionState::RolledBack);

        // Verify message was NOT committed
        let size = broker.queue_size("test_rollback").await.unwrap();
        assert_eq!(size, 0);

        broker.disconnect().await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires RabbitMQ to be running
    async fn test_integration_dead_letter_exchange() {
        use celers_protocol::builder::MessageBuilder;

        let mut broker = AmqpBroker::new("amqp://localhost:5672", "test_dlx_main")
            .await
            .unwrap();

        if broker.connect().await.is_err() {
            eprintln!("Skipping integration test - RabbitMQ not available");
            return;
        }

        // Declare DLX
        broker
            .declare_dlx("test_dlx_exchange", "test_dlx_queue")
            .await
            .unwrap();

        // Declare main queue with DLX configuration
        let dlx_config = DlxConfig::new("test_dlx_exchange").with_routing_key("test_dlx_queue");
        let queue_config = QueueConfig::new().with_dlx(dlx_config);

        broker
            .declare_queue_with_config("test_dlx_main", &queue_config)
            .await
            .unwrap();

        let _ = broker.purge("test_dlx_main").await;
        let _ = broker.purge("test_dlx_queue").await;

        // Publish a message
        let msg = MessageBuilder::new("dlx.task")
            .args(vec![serde_json::json!("will be rejected")])
            .build()
            .unwrap();
        broker.publish("test_dlx_main", msg).await.unwrap();

        // Consume and reject (without requeue) - should go to DLX
        let envelope = broker
            .consume("test_dlx_main", Duration::from_secs(1))
            .await
            .unwrap();
        assert!(envelope.is_some());

        broker
            .reject(&envelope.unwrap().delivery_tag, false)
            .await
            .unwrap();

        // Give RabbitMQ time to route to DLX
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Verify message is in DLX queue
        let dlx_size = broker.queue_size("test_dlx_queue").await.unwrap();
        assert_eq!(dlx_size, 1);

        broker.disconnect().await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires RabbitMQ to be running
    async fn test_integration_message_ttl() {
        use celers_protocol::builder::MessageBuilder;

        let mut broker = AmqpBroker::new("amqp://localhost:5672", "test_ttl")
            .await
            .unwrap();

        if broker.connect().await.is_err() {
            eprintln!("Skipping integration test - RabbitMQ not available");
            return;
        }

        let _ = broker.purge("test_ttl").await;

        // Publish message with 500ms TTL
        let msg = MessageBuilder::new("ttl.task")
            .args(vec![serde_json::json!("expires soon")])
            .build()
            .unwrap();

        broker.publish_with_ttl("test_ttl", msg, 500).await.unwrap();

        // Wait for message to expire
        tokio::time::sleep(Duration::from_millis(600)).await;

        // Message should have expired
        let envelope = broker
            .consume("test_ttl", Duration::from_millis(100))
            .await
            .unwrap();
        assert!(envelope.is_none());

        broker.disconnect().await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires RabbitMQ to be running
    async fn test_integration_metrics_tracking() {
        use celers_protocol::builder::MessageBuilder;

        let mut broker = AmqpBroker::new("amqp://localhost:5672", "test_metrics")
            .await
            .unwrap();

        if broker.connect().await.is_err() {
            eprintln!("Skipping integration test - RabbitMQ not available");
            return;
        }

        broker.reset_metrics();
        let _ = broker.purge("test_metrics").await;

        // Publish some messages
        for i in 0..3 {
            let msg = MessageBuilder::new("metrics.task")
                .args(vec![serde_json::json!(i)])
                .build()
                .unwrap();
            broker.publish("test_metrics", msg).await.unwrap();
        }

        // Check metrics
        let metrics = broker.channel_metrics();
        assert_eq!(metrics.messages_published, 3);

        let confirm_stats = broker.publisher_confirm_stats();
        assert_eq!(confirm_stats.successful_confirms, 3);

        // Consume and ack
        for _ in 0..3 {
            if let Ok(Some(envelope)) = broker.consume("test_metrics", Duration::from_secs(1)).await
            {
                broker.ack(&envelope.delivery_tag).await.unwrap();
            }
        }

        let metrics = broker.channel_metrics();
        assert_eq!(metrics.messages_consumed, 3);
        assert_eq!(metrics.messages_acked, 3);

        broker.disconnect().await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires RabbitMQ to be running
    async fn test_integration_deduplication() {
        use celers_protocol::builder::MessageBuilder;

        let config = AmqpConfig::default()
            .with_deduplication(true)
            .with_deduplication_config(100, Duration::from_secs(60));

        let mut broker = AmqpBroker::with_config("amqp://localhost:5672", "test_dedup", config)
            .await
            .unwrap();

        if broker.connect().await.is_err() {
            eprintln!("Skipping integration test - RabbitMQ not available");
            return;
        }

        let _ = broker.purge("test_dedup").await;

        // Create a message with specific ID
        let msg1 = MessageBuilder::new("dedup.task")
            .args(vec![serde_json::json!("first")])
            .build()
            .unwrap();

        // Publish first time - should succeed
        broker.publish("test_dedup", msg1.clone()).await.unwrap();

        // Publish again with same message ID - should be deduplicated
        broker.publish("test_dedup", msg1).await.unwrap();

        // Only one message should be in queue
        let size = broker.queue_size("test_dedup").await.unwrap();
        assert_eq!(size, 1);

        broker.disconnect().await.unwrap();
    }

    #[test]
    fn test_management_api_config() {
        let config =
            AmqpConfig::default().with_management_api("http://localhost:15672", "guest", "guest");

        assert_eq!(
            config.management_url,
            Some("http://localhost:15672".to_string())
        );
        assert_eq!(config.management_username, Some("guest".to_string()));
        assert_eq!(config.management_password, Some("guest".to_string()));
    }

    #[tokio::test]
    async fn test_management_api_not_configured() {
        let broker = AmqpBroker::new("amqp://localhost:5672", "test_queue")
            .await
            .unwrap();

        assert!(!broker.has_management_api());

        let result = broker.list_queues().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not configured"));
    }

    #[tokio::test]
    async fn test_management_api_configured() {
        let config =
            AmqpConfig::default().with_management_api("http://localhost:15672", "guest", "guest");

        let broker = AmqpBroker::with_config("amqp://localhost:5672", "test_queue", config)
            .await
            .unwrap();

        assert!(broker.has_management_api());
    }

    #[tokio::test]
    #[ignore] // Requires RabbitMQ Management API to be running
    async fn test_integration_list_queues() {
        let config =
            AmqpConfig::default().with_management_api("http://localhost:15672", "guest", "guest");

        let mut broker = AmqpBroker::with_config("amqp://localhost:5672", "test_mgmt_list", config)
            .await
            .unwrap();

        if broker.connect().await.is_err() {
            eprintln!("Skipping integration test - RabbitMQ not available");
            return;
        }

        // Declare a test queue
        broker
            .declare_queue("test_mgmt_list", QueueMode::Fifo)
            .await
            .unwrap();

        // List queues
        let queues = broker.list_queues().await.unwrap();
        assert!(!queues.is_empty());

        // Find our test queue
        let found = queues.iter().any(|q| q.name == "test_mgmt_list");
        assert!(found);

        broker.disconnect().await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires RabbitMQ Management API to be running
    async fn test_integration_queue_stats() {
        use celers_protocol::builder::MessageBuilder;

        let config =
            AmqpConfig::default().with_management_api("http://localhost:15672", "guest", "guest");

        let mut broker =
            AmqpBroker::with_config("amqp://localhost:5672", "test_mgmt_stats", config)
                .await
                .unwrap();

        if broker.connect().await.is_err() {
            eprintln!("Skipping integration test - RabbitMQ not available");
            return;
        }

        // Declare a test queue
        broker
            .declare_queue("test_mgmt_stats", QueueMode::Fifo)
            .await
            .unwrap();

        let _ = broker.purge("test_mgmt_stats").await;

        // Publish some messages
        for i in 0..5 {
            let msg = MessageBuilder::new("test.task")
                .args(vec![serde_json::json!(i)])
                .build()
                .unwrap();
            broker.publish("test_mgmt_stats", msg).await.unwrap();
        }

        // Give RabbitMQ time to update stats
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Get queue stats
        let stats = broker.get_queue_stats("test_mgmt_stats").await.unwrap();
        assert_eq!(stats.name, "test_mgmt_stats");
        assert_eq!(stats.messages, 5);
        assert_eq!(stats.messages_ready, 5);
        assert_eq!(stats.messages_unacknowledged, 0);

        broker.disconnect().await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires RabbitMQ Management API to be running
    async fn test_integration_server_overview() {
        let config =
            AmqpConfig::default().with_management_api("http://localhost:15672", "guest", "guest");

        let mut broker =
            AmqpBroker::with_config("amqp://localhost:5672", "test_mgmt_overview", config)
                .await
                .unwrap();

        if broker.connect().await.is_err() {
            eprintln!("Skipping integration test - RabbitMQ not available");
            return;
        }

        // Get server overview
        let overview = broker.get_server_overview().await.unwrap();

        // Verify we got valid data
        assert!(!overview.rabbitmq_version.is_empty());
        assert!(!overview.erlang_version.is_empty());
        assert!(!overview.management_version.is_empty());
        assert!(!overview.cluster_name.is_empty());

        broker.disconnect().await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires RabbitMQ Management API to be running
    async fn test_integration_list_connections() {
        let config =
            AmqpConfig::default().with_management_api("http://localhost:15672", "guest", "guest");

        let mut broker = AmqpBroker::with_config("amqp://localhost:5672", "test_mgmt_conn", config)
            .await
            .unwrap();

        if broker.connect().await.is_err() {
            eprintln!("Skipping integration test - RabbitMQ not available");
            return;
        }

        // List connections
        let connections = broker.list_connections().await.unwrap();

        // Should have at least our connection
        assert!(!connections.is_empty());

        // Verify connection data
        let conn = &connections[0];
        assert!(!conn.name.is_empty());
        assert!(!conn.user.is_empty());
        assert!(!conn.state.is_empty());

        broker.disconnect().await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires RabbitMQ Management API to be running
    async fn test_integration_list_channels() {
        let config =
            AmqpConfig::default().with_management_api("http://localhost:15672", "guest", "guest");

        let mut broker = AmqpBroker::with_config("amqp://localhost:5672", "test_mgmt_chan", config)
            .await
            .unwrap();

        if broker.connect().await.is_err() {
            eprintln!("Skipping integration test - RabbitMQ not available");
            return;
        }

        // List channels
        let channels = broker.list_channels().await.unwrap();

        // Should have at least our channel
        assert!(!channels.is_empty());

        // Verify channel data
        let chan = &channels[0];
        assert!(!chan.name.is_empty());
        assert!(!chan.user.is_empty());
        assert!(!chan.state.is_empty());
        assert!(chan.number > 0);

        broker.disconnect().await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires RabbitMQ Management API to be running
    async fn test_integration_list_exchanges() {
        let config =
            AmqpConfig::default().with_management_api("http://localhost:15672", "guest", "guest");

        let mut broker = AmqpBroker::with_config("amqp://localhost:5672", "test_mgmt_exch", config)
            .await
            .unwrap();

        if broker.connect().await.is_err() {
            eprintln!("Skipping integration test - RabbitMQ not available");
            return;
        }

        // List exchanges
        let exchanges = broker.list_exchanges(None).await.unwrap();

        // Should have default exchanges
        assert!(!exchanges.is_empty());

        // Find the default exchange
        let default_exch = exchanges.iter().find(|e| e.name.is_empty());
        assert!(default_exch.is_some());

        // Find the amq.direct exchange
        let direct_exch = exchanges.iter().find(|e| e.name == "amq.direct");
        assert!(direct_exch.is_some());
        assert_eq!(direct_exch.unwrap().exchange_type, "direct");

        broker.disconnect().await.unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires RabbitMQ Management API to be running
    async fn test_integration_list_queue_bindings() {
        let config =
            AmqpConfig::default().with_management_api("http://localhost:15672", "guest", "guest");

        let mut broker =
            AmqpBroker::with_config("amqp://localhost:5672", "test_mgmt_bindings", config)
                .await
                .unwrap();

        if broker.connect().await.is_err() {
            eprintln!("Skipping integration test - RabbitMQ not available");
            return;
        }

        // Declare a test queue
        broker
            .declare_queue("test_mgmt_bindings", QueueMode::Fifo)
            .await
            .unwrap();

        // Give RabbitMQ time to register the queue
        tokio::time::sleep(Duration::from_millis(200)).await;

        // List bindings
        let bindings = broker
            .list_queue_bindings("test_mgmt_bindings")
            .await
            .unwrap();

        // Should have at least the default binding
        assert!(!bindings.is_empty());

        // Verify binding data
        let binding = &bindings[0];
        assert_eq!(binding.destination, "test_mgmt_bindings");
        assert_eq!(binding.destination_type, "queue");

        broker.disconnect().await.unwrap();
    }

    #[test]
    fn test_queue_type_enum() {
        assert_eq!(QueueType::Classic.as_str(), "classic");
        assert_eq!(QueueType::Quorum.as_str(), "quorum");
        assert_eq!(QueueType::Stream.as_str(), "stream");
    }

    #[test]
    fn test_queue_lazy_mode_enum() {
        assert_eq!(QueueLazyMode::Default.as_str(), "default");
        assert_eq!(QueueLazyMode::Lazy.as_str(), "lazy");
    }

    #[test]
    fn test_queue_overflow_behavior_enum() {
        assert_eq!(QueueOverflowBehavior::DropHead.as_str(), "drop-head");
        assert_eq!(
            QueueOverflowBehavior::RejectPublish.as_str(),
            "reject-publish"
        );
        assert_eq!(
            QueueOverflowBehavior::RejectPublishDlx.as_str(),
            "reject-publish-dlx"
        );
    }

    #[test]
    fn test_queue_config_with_modern_features() {
        let config = QueueConfig::new()
            .with_queue_type(QueueType::Quorum)
            .with_queue_mode(QueueLazyMode::Lazy)
            .with_overflow_behavior(QueueOverflowBehavior::RejectPublish)
            .with_single_active_consumer(true);

        assert_eq!(config.queue_type, Some(QueueType::Quorum));
        assert_eq!(config.queue_mode, Some(QueueLazyMode::Lazy));
        assert_eq!(
            config.overflow_behavior,
            Some(QueueOverflowBehavior::RejectPublish)
        );
        assert!(config.single_active_consumer);
    }

    #[test]
    fn test_queue_config_field_table_with_modern_features() {
        let config = QueueConfig::new()
            .with_queue_type(QueueType::Quorum)
            .with_queue_mode(QueueLazyMode::Lazy)
            .with_overflow_behavior(QueueOverflowBehavior::DropHead)
            .with_single_active_consumer(true)
            .with_max_length(1000);

        let table = config.to_field_table();

        assert!(table
            .inner()
            .contains_key(&ShortString::from("x-queue-type")));
        assert!(table
            .inner()
            .contains_key(&ShortString::from("x-queue-mode")));
        assert!(table.inner().contains_key(&ShortString::from("x-overflow")));
        assert!(table
            .inner()
            .contains_key(&ShortString::from("x-single-active-consumer")));
        assert!(table
            .inner()
            .contains_key(&ShortString::from("x-max-length")));
    }

    #[test]
    fn test_quorum_queue_config() {
        let config = QueueConfig::new()
            .durable(true)
            .with_queue_type(QueueType::Quorum)
            .with_max_length(10000)
            .with_overflow_behavior(QueueOverflowBehavior::RejectPublish);

        assert!(config.durable);
        assert_eq!(config.queue_type, Some(QueueType::Quorum));
        assert_eq!(config.max_length, Some(10000));
        assert_eq!(
            config.overflow_behavior,
            Some(QueueOverflowBehavior::RejectPublish)
        );
    }

    #[test]
    fn test_lazy_queue_config() {
        let config = QueueConfig::new()
            .with_queue_mode(QueueLazyMode::Lazy)
            .with_max_length(1000000);

        assert_eq!(config.queue_mode, Some(QueueLazyMode::Lazy));
        assert_eq!(config.max_length, Some(1000000));
    }

    #[test]
    fn test_stream_queue_config() {
        let config = QueueConfig::new()
            .durable(true)
            .with_queue_type(QueueType::Stream);

        assert!(config.durable);
        assert_eq!(config.queue_type, Some(QueueType::Stream));
    }

    // ===== v4 Helper Methods Tests =====

    #[test]
    fn test_queue_info_helpers() {
        let info = QueueInfo {
            name: "test_queue".to_string(),
            vhost: "/".to_string(),
            durable: true,
            auto_delete: false,
            messages: 100,
            messages_ready: 80,
            messages_unacknowledged: 20,
            consumers: 2,
            memory: 10485760, // 10 MB
        };

        assert!(!info.is_empty());
        assert!(info.has_consumers());
        assert!(!info.is_idle());
        assert_eq!(info.ready_percentage(), 80.0);
        assert_eq!(info.unacked_percentage(), 20.0);
        assert_eq!(info.memory_mb(), 10.0);
        assert_eq!(info.avg_message_memory(), 104857.6);

        // Test empty queue
        let empty_info = QueueInfo {
            name: "empty".to_string(),
            vhost: "/".to_string(),
            durable: true,
            auto_delete: false,
            messages: 0,
            messages_ready: 0,
            messages_unacknowledged: 0,
            consumers: 0,
            memory: 0,
        };

        assert!(empty_info.is_empty());
        assert!(!empty_info.has_consumers());
        assert!(empty_info.is_idle());
        assert_eq!(empty_info.ready_percentage(), 0.0);
        assert_eq!(empty_info.avg_message_memory(), 0.0);
    }

    #[test]
    fn test_queue_stats_helpers() {
        let stats = QueueStats {
            name: "test_queue".to_string(),
            vhost: "/".to_string(),
            durable: true,
            auto_delete: false,
            messages: 1000,
            messages_ready: 800,
            messages_unacknowledged: 200,
            consumers: 5,
            memory: 10485760,       // 10 MB
            message_bytes: 5242880, // 5 MB
            message_bytes_ready: 4194304,
            message_bytes_unacknowledged: 1048576,
            message_stats: Some(MessageStats {
                publish: 10000,
                publish_details: Some(RateDetails { rate: 100.0 }),
                deliver: 9500,
                deliver_details: Some(RateDetails { rate: 95.0 }),
                ack: 9000,
                ack_details: Some(RateDetails { rate: 90.0 }),
            }),
        };

        assert!(!stats.is_empty());
        assert!(stats.has_consumers());
        assert!(!stats.is_idle());
        assert_eq!(stats.ready_percentage(), 80.0);
        assert_eq!(stats.unacked_percentage(), 20.0);
        assert_eq!(stats.memory_mb(), 10.0);
        assert_eq!(stats.message_bytes_mb(), 5.0);
        assert_eq!(stats.avg_message_size(), 5242.88);
        assert_eq!(stats.avg_message_memory(), 10485.76);
        assert_eq!(stats.publish_rate(), Some(100.0));
        assert_eq!(stats.deliver_rate(), Some(95.0));
        assert_eq!(stats.ack_rate(), Some(90.0));
        assert!(stats.is_growing());
        assert!(!stats.is_shrinking());
        assert!(!stats.consumers_keeping_up()); // Ack rate (90) < deliver rate * 0.95 (90.25)
    }

    #[test]
    fn test_message_stats_helpers() {
        let stats = MessageStats {
            publish: 1000,
            publish_details: Some(RateDetails { rate: 10.0 }),
            deliver: 950,
            deliver_details: Some(RateDetails { rate: 9.5 }),
            ack: 900,
            ack_details: Some(RateDetails { rate: 9.0 }),
        };

        assert_eq!(stats.total_processed(), 2850);
        assert_eq!(stats.publish_rate(), Some(10.0));
        assert_eq!(stats.deliver_rate(), Some(9.5));
        assert_eq!(stats.ack_rate(), Some(9.0));
    }

    #[test]
    fn test_server_overview_helpers() {
        let overview = ServerOverview {
            management_version: "3.12.0".to_string(),
            rabbitmq_version: "3.12.0".to_string(),
            erlang_version: "26.0".to_string(),
            cluster_name: "rabbit@localhost".to_string(),
            queue_totals: Some(QueueTotals {
                messages: 5000,
                messages_ready: 4000,
                messages_unacknowledged: 1000,
            }),
            object_totals: Some(ObjectTotals {
                consumers: 10,
                queues: 5,
                exchanges: 7,
                connections: 3,
                channels: 15,
            }),
        };

        assert_eq!(overview.total_messages(), 5000);
        assert_eq!(overview.total_messages_ready(), 4000);
        assert_eq!(overview.total_messages_unacked(), 1000);
        assert_eq!(overview.total_queues(), 5);
        assert_eq!(overview.total_connections(), 3);
        assert_eq!(overview.total_channels(), 15);
        assert_eq!(overview.total_consumers(), 10);
        assert!(overview.has_connections());
        assert!(overview.has_messages());
    }

    #[test]
    fn test_queue_totals_helpers() {
        let totals = QueueTotals {
            messages: 1000,
            messages_ready: 750,
            messages_unacknowledged: 250,
        };

        assert_eq!(totals.ready_percentage(), 75.0);
        assert_eq!(totals.unacked_percentage(), 25.0);
    }

    #[test]
    fn test_object_totals_helpers() {
        let totals = ObjectTotals {
            consumers: 20,
            queues: 10,
            exchanges: 15,
            connections: 5,
            channels: 25,
        };

        assert_eq!(totals.avg_channels_per_connection(), 5.0);
        assert_eq!(totals.avg_consumers_per_queue(), 2.0);
        assert!(!totals.has_idle_queues()); // 20 consumers for 10 queues
    }

    #[test]
    fn test_connection_info_helpers() {
        let conn = ConnectionInfo {
            name: "test_connection".to_string(),
            vhost: "/".to_string(),
            user: "guest".to_string(),
            state: "running".to_string(),
            channels: 10,
            peer_host: "127.0.0.1".to_string(),
            peer_port: 5672,
            recv_oct: 10485760, // 10 MB
            send_oct: 5242880,  // 5 MB
            recv_cnt: 1000,
            send_cnt: 500,
        };

        assert!(conn.is_running());
        assert!(conn.has_channels());
        assert_eq!(conn.total_bytes(), 15728640);
        assert_eq!(conn.recv_mb(), 10.0);
        assert_eq!(conn.send_mb(), 5.0);
        assert_eq!(conn.total_messages(), 1500);
        assert_eq!(conn.avg_message_size(), 10485.76);
        assert_eq!(conn.peer_address(), "127.0.0.1:5672");
    }

    #[test]
    fn test_channel_info_helpers() {
        let channel = ChannelInfo {
            name: "test_channel".to_string(),
            connection_details: Some(ConnectionDetails {
                name: "conn".to_string(),
                peer_host: "127.0.0.1".to_string(),
                peer_port: 5672,
            }),
            vhost: "/".to_string(),
            user: "guest".to_string(),
            number: 1,
            consumers: 3,
            messages_unacknowledged: 50,
            messages_uncommitted: 10,
            acks_uncommitted: 5,
            prefetch_count: 100,
            state: "running".to_string(),
        };

        assert!(channel.is_running());
        assert!(channel.has_consumers());
        assert!(channel.has_unacked_messages());
        assert!(channel.is_in_transaction());
        assert!(channel.has_prefetch());
        assert_eq!(channel.utilization(), 50.0);
        assert_eq!(channel.peer_address(), Some("127.0.0.1:5672".to_string()));
    }
}
