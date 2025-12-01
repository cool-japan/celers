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
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// Exchange type for AMQP
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
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

        args
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
}

impl HealthStatus {
    /// Check if the broker is healthy (connected and channel open)
    pub fn is_healthy(&self) -> bool {
        self.connected && self.channel_open
    }
}

/// Transaction state for AMQP transactions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    /// No transaction active
    None,
    /// Transaction started
    Started,
    /// Transaction committed
    Committed,
    /// Transaction rolled back
    RolledBack,
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
}

impl AmqpBroker {
    /// Create a new AMQP broker with default configuration
    pub async fn new(url: &str, queue_name: &str) -> Result<Self> {
        Self::with_config(url, queue_name, AmqpConfig::default()).await
    }

    /// Create a new AMQP broker with custom configuration
    pub async fn with_config(url: &str, queue_name: &str, config: AmqpConfig) -> Result<Self> {
        Ok(Self {
            url: url.to_string(),
            queue_name: queue_name.to_string(),
            connection: None,
            channel: None,
            consumer_tag: None,
            config,
            transaction_state: TransactionState::None,
        })
    }

    /// Get the broker configuration
    pub fn config(&self) -> &AmqpConfig {
        &self.config
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

    /// Get or create channel
    async fn get_channel(&mut self) -> Result<&Channel> {
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
                    last_error = Some(e);
                    if attempt < self.config.retry_count {
                        warn!(
                            "Connection failed, will retry: {}",
                            last_error.as_ref().unwrap()
                        );
                    }
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
    async fn declare_queue(&mut self, queue: &str, mode: QueueMode) -> Result<()> {
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
    pub fn health_status(&self) -> HealthStatus {
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

        HealthStatus {
            connected,
            channel_open,
            connection_state,
            channel_state,
        }
    }

    /// Check if the broker is healthy
    pub fn is_healthy(&self) -> bool {
        self.health_status().is_healthy()
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
        if let Some(channel) = self.channel.take() {
            let _ = channel.close(200, "Disconnecting").await;
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
}

// Additional AmqpBroker methods
impl AmqpBroker {
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
}

#[async_trait]
impl Consumer for AmqpBroker {
    async fn consume(&mut self, queue: &str, timeout: Duration) -> Result<Option<Envelope>> {
        let channel = self.get_channel().await?;

        // Use basic_get for polling (compatible with Redis implementation)
        let get_result = channel
            .basic_get(queue, BasicGetOptions { no_ack: false })
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to get message: {}", e)))?;

        match get_result {
            Some(delivery) => {
                // Deserialize message
                let message: Message = serde_json::from_slice(&delivery.data)
                    .map_err(|e| BrokerError::Serialization(e.to_string()))?;

                let envelope = Envelope {
                    delivery_tag: delivery.delivery_tag.to_string(),
                    message,
                    redelivered: delivery.redelivered,
                };

                debug!("Consumed message from queue: {}", queue);
                Ok(Some(envelope))
            }
            None => {
                // No message available
                tokio::time::sleep(timeout).await;
                Ok(None)
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

        let status = broker.health_status();
        assert!(!status.connected);
        assert!(!status.channel_open);
        assert!(!status.is_healthy());
        assert_eq!(status.connection_state, "Not connected");
        assert_eq!(status.channel_state, "No channel");
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
        };
        assert!(healthy.is_healthy());

        let not_connected = HealthStatus {
            connected: false,
            channel_open: true,
            connection_state: "Disconnected".to_string(),
            channel_state: "Open".to_string(),
        };
        assert!(!not_connected.is_healthy());

        let no_channel = HealthStatus {
            connected: true,
            channel_open: false,
            connection_state: "Connected".to_string(),
            channel_state: "Closed".to_string(),
        };
        assert!(!no_channel.is_healthy());
    }
}
