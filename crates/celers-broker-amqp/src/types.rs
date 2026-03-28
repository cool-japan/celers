//! AMQP type definitions, configuration structs, and status types.

use lapin::{
    types::{AMQPValue, FieldTable, ShortString},
    BasicProperties, ExchangeKind,
};
use std::collections::HashMap;
use std::time::Duration;

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
    pub(crate) fn to_exchange_kind(self) -> ExchangeKind {
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
    pub(crate) fn as_str(&self) -> &str {
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
    pub(crate) fn as_str(&self) -> &str {
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
    pub(crate) fn as_str(&self) -> &str {
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
    pub(crate) fn to_field_table(&self) -> FieldTable {
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
    pub(crate) fn to_field_table(&self) -> FieldTable {
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
    pub(crate) fn to_field_table(&self) -> FieldTable {
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
