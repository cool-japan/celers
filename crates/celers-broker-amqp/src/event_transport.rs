//! AMQP event transport for real-time event publishing
//!
//! This module provides AMQP (RabbitMQ) based event transport for CeleRS events.
//! It implements the `EventEmitter` trait from `celers-core` and publishes
//! events to a RabbitMQ fanout exchange following Celery's event protocol.
//!
//! # Celery Event Exchange
//!
//! Events are published to a fanout exchange (default: `celeryev`) that broadcasts
//! to all bound queues. This mirrors Celery's `celery.events` exchange behavior.
//!
//! # Example
//!
//! ```no_run
//! use celers_broker_amqp::event_transport::{AmqpEventEmitter, AmqpEventConfig};
//! use celers_core::event::{Event, EventEmitter, WorkerEventBuilder};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = AmqpEventConfig::default();
//! let emitter = AmqpEventEmitter::new("amqp://localhost:5672", config).await?;
//!
//! // Emit a worker online event
//! let event = WorkerEventBuilder::new("worker-1").online();
//! emitter.emit(event).await?;
//! # Ok(())
//! # }
//! ```

use async_trait::async_trait;
use celers_core::event::{Event, EventEmitter};
use celers_core::{CelersError, Result};
use lapin::{
    options::*, types::FieldTable, BasicProperties, Channel, Connection, ConnectionProperties,
    ExchangeKind,
};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::{debug, error, warn};

/// Default exchange name for Celery events
const DEFAULT_EXCHANGE: &str = "celeryev";

/// Default exchange type for event broadcasting
const DEFAULT_EXCHANGE_TYPE: &str = "fanout";

/// Default batch size for batch publishing
const DEFAULT_BATCH_SIZE: usize = 100;

/// Configuration for AMQP event transport
#[derive(Debug, Clone)]
pub struct AmqpEventConfig {
    /// Exchange name for events (default: "celeryev")
    pub exchange: String,

    /// Exchange type (default: "fanout")
    pub exchange_type: String,

    /// Routing key for published messages (default: "")
    pub routing_key: String,

    /// Whether the exchange is durable (default: false)
    pub durable: bool,

    /// Whether the emitter is enabled (default: true)
    pub enabled: bool,

    /// Maximum batch size for batch publishing (default: 100)
    pub batch_size: usize,

    /// Serialization format (default: "json")
    pub serialization: String,
}

impl Default for AmqpEventConfig {
    fn default() -> Self {
        Self {
            exchange: DEFAULT_EXCHANGE.to_string(),
            exchange_type: DEFAULT_EXCHANGE_TYPE.to_string(),
            routing_key: String::new(),
            durable: false,
            enabled: true,
            batch_size: DEFAULT_BATCH_SIZE,
            serialization: "json".to_string(),
        }
    }
}

impl AmqpEventConfig {
    /// Create a new configuration with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the exchange name
    pub fn exchange(mut self, exchange: impl Into<String>) -> Self {
        self.exchange = exchange.into();
        self
    }

    /// Set the exchange type
    pub fn exchange_type(mut self, exchange_type: impl Into<String>) -> Self {
        self.exchange_type = exchange_type.into();
        self
    }

    /// Set the routing key
    pub fn routing_key(mut self, routing_key: impl Into<String>) -> Self {
        self.routing_key = routing_key.into();
        self
    }

    /// Set whether the exchange is durable
    pub fn durable(mut self, durable: bool) -> Self {
        self.durable = durable;
        self
    }

    /// Enable or disable the emitter
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Set the batch size
    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the serialization format
    pub fn serialization(mut self, serialization: impl Into<String>) -> Self {
        self.serialization = serialization.into();
        self
    }

    /// Parse the exchange type string into a lapin ExchangeKind
    fn exchange_kind(&self) -> ExchangeKind {
        match self.exchange_type.as_str() {
            "direct" => ExchangeKind::Direct,
            "fanout" => ExchangeKind::Fanout,
            "topic" => ExchangeKind::Topic,
            "headers" => ExchangeKind::Headers,
            other => ExchangeKind::Custom(other.to_string()),
        }
    }
}

/// Statistics for the AMQP event transport
#[derive(Debug, Clone, Default)]
pub struct EventTransportStats {
    /// Total number of events successfully published
    pub events_published: u64,

    /// Total number of events that failed to publish
    pub events_failed: u64,

    /// Total bytes sent over the wire
    pub bytes_sent: u64,

    /// Timestamp of the last successful publish
    pub last_publish_at: Option<Instant>,
}

impl EventTransportStats {
    /// Create new empty stats
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a successful publish
    fn record_success(&mut self, bytes: u64) {
        self.events_published += 1;
        self.bytes_sent += bytes;
        self.last_publish_at = Some(Instant::now());
    }

    /// Record a failed publish
    fn record_failure(&mut self) {
        self.events_failed += 1;
    }

    /// Get the total number of events attempted
    pub fn total_attempts(&self) -> u64 {
        self.events_published + self.events_failed
    }

    /// Get the success rate as a percentage (0.0 - 100.0)
    pub fn success_rate(&self) -> f64 {
        let total = self.total_attempts();
        if total == 0 {
            return 100.0;
        }
        (self.events_published as f64 / total as f64) * 100.0
    }
}

/// AMQP event emitter - publishes events to a RabbitMQ fanout exchange
///
/// This emitter connects to a RabbitMQ server and publishes serialized events
/// to a fanout exchange. All consumers bound to the exchange will receive
/// copies of the events, enabling real-time monitoring.
///
/// The exchange is declared idempotently on first use. If the exchange already
/// exists with compatible settings, the declaration is a no-op.
pub struct AmqpEventEmitter {
    /// AMQP channel (lazily initialized)
    channel: Arc<RwLock<Option<Channel>>>,

    /// AMQP connection URI
    uri: String,

    /// Event transport configuration
    config: AmqpEventConfig,

    /// Transport statistics
    stats: Arc<RwLock<EventTransportStats>>,

    /// Whether the exchange has been declared
    exchange_declared: Arc<RwLock<bool>>,
}

impl AmqpEventEmitter {
    /// Create a new AMQP event emitter
    ///
    /// Establishes a connection to the AMQP server and prepares for event publishing.
    /// The exchange is declared lazily on the first publish.
    ///
    /// # Arguments
    /// * `uri` - AMQP connection URI (e.g., "amqp://localhost:5672")
    /// * `config` - Event transport configuration
    pub async fn new(uri: &str, config: AmqpEventConfig) -> std::result::Result<Self, CelersError> {
        let conn = Connection::connect(uri, ConnectionProperties::default())
            .await
            .map_err(|e| CelersError::Other(format!("AMQP connection error: {}", e)))?;

        let channel = conn
            .create_channel()
            .await
            .map_err(|e| CelersError::Other(format!("AMQP channel creation error: {}", e)))?;

        Ok(Self {
            channel: Arc::new(RwLock::new(Some(channel))),
            uri: uri.to_string(),
            config,
            stats: Arc::new(RwLock::new(EventTransportStats::new())),
            exchange_declared: Arc::new(RwLock::new(false)),
        })
    }

    /// Create an emitter from an existing lapin Channel
    ///
    /// Useful when you already have a connection and want to reuse it.
    ///
    /// # Arguments
    /// * `channel` - An existing lapin Channel
    /// * `config` - Event transport configuration
    pub fn from_channel(channel: Channel, config: AmqpEventConfig) -> Self {
        Self {
            channel: Arc::new(RwLock::new(Some(channel))),
            uri: String::new(),
            config,
            stats: Arc::new(RwLock::new(EventTransportStats::new())),
            exchange_declared: Arc::new(RwLock::new(false)),
        }
    }

    /// Get a reference to the configuration
    pub fn config(&self) -> &AmqpEventConfig {
        &self.config
    }

    /// Check if the emitter is active
    pub fn is_active(&self) -> bool {
        self.config.enabled
    }

    /// Get the exchange name
    pub fn exchange(&self) -> &str {
        &self.config.exchange
    }

    /// Get a snapshot of the current transport statistics
    pub async fn stats(&self) -> EventTransportStats {
        self.stats.read().await.clone()
    }

    /// Ensure the exchange is declared
    async fn ensure_exchange(&self, channel: &Channel) -> std::result::Result<(), CelersError> {
        // Fast path: already declared
        {
            let declared = self.exchange_declared.read().await;
            if *declared {
                return Ok(());
            }
        }

        // Slow path: declare the exchange
        let opts = ExchangeDeclareOptions {
            passive: false,
            durable: self.config.durable,
            auto_delete: !self.config.durable,
            internal: false,
            nowait: false,
        };

        channel
            .exchange_declare(
                self.config.exchange.as_str().into(),
                self.config.exchange_kind(),
                opts,
                FieldTable::default(),
            )
            .await
            .map_err(|e| CelersError::Other(format!("AMQP exchange declare error: {}", e)))?;

        debug!(
            exchange = %self.config.exchange,
            exchange_type = %self.config.exchange_type,
            durable = self.config.durable,
            "Declared event exchange"
        );

        let mut declared = self.exchange_declared.write().await;
        *declared = true;

        Ok(())
    }

    /// Get the channel, reconnecting if necessary
    async fn get_channel(&self) -> std::result::Result<Channel, CelersError> {
        // Check if we have a valid channel
        {
            let guard = self.channel.read().await;
            if let Some(ref ch) = *guard {
                if ch.status().connected() {
                    return Ok(ch.clone());
                }
            }
        }

        // Reconnect if we have a URI
        if self.uri.is_empty() {
            return Err(CelersError::Other(
                "AMQP channel is not connected and no URI available for reconnection".to_string(),
            ));
        }

        let conn = Connection::connect(&self.uri, ConnectionProperties::default())
            .await
            .map_err(|e| CelersError::Other(format!("AMQP reconnection error: {}", e)))?;

        let new_channel = conn
            .create_channel()
            .await
            .map_err(|e| CelersError::Other(format!("AMQP channel recreation error: {}", e)))?;

        // Reset exchange declared flag since we have a new channel
        {
            let mut declared = self.exchange_declared.write().await;
            *declared = false;
        }

        let mut guard = self.channel.write().await;
        *guard = Some(new_channel.clone());

        Ok(new_channel)
    }

    /// Publish a serialized event payload to the exchange
    async fn publish_payload(&self, payload: &[u8]) -> std::result::Result<(), CelersError> {
        let channel = self.get_channel().await?;

        self.ensure_exchange(&channel).await?;

        let properties = BasicProperties::default()
            .with_content_type("application/json".into())
            .with_delivery_mode(if self.config.durable { 2 } else { 1 });

        channel
            .basic_publish(
                self.config.exchange.as_str().into(),
                self.config.routing_key.as_str().into(),
                BasicPublishOptions::default(),
                payload,
                properties,
            )
            .await
            .map_err(|e| CelersError::Other(format!("AMQP publish error: {}", e)))?
            .await
            .map_err(|e| CelersError::Other(format!("AMQP publish confirm error: {}", e)))?;

        Ok(())
    }
}

#[async_trait]
impl EventEmitter for AmqpEventEmitter {
    async fn emit(&self, event: Event) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        let event_json = serde_json::to_string(&event)
            .map_err(|e| CelersError::Other(format!("Event serialization error: {}", e)))?;

        let payload = event_json.as_bytes();
        let payload_len = payload.len() as u64;

        match self.publish_payload(payload).await {
            Ok(()) => {
                let mut stats = self.stats.write().await;
                stats.record_success(payload_len);
                debug!(
                    event_type = %event.event_type(),
                    exchange = %self.config.exchange,
                    bytes = payload_len,
                    "Published event to AMQP exchange"
                );
                Ok(())
            }
            Err(e) => {
                let mut stats = self.stats.write().await;
                stats.record_failure();
                error!(
                    event_type = %event.event_type(),
                    exchange = %self.config.exchange,
                    error = %e,
                    "Failed to publish event to AMQP exchange"
                );
                Err(e)
            }
        }
    }

    async fn emit_batch(&self, events: Vec<Event>) -> Result<()> {
        if !self.config.enabled || events.is_empty() {
            return Ok(());
        }

        let channel = self.get_channel().await?;
        self.ensure_exchange(&channel).await?;

        let properties = BasicProperties::default()
            .with_content_type("application/json".into())
            .with_delivery_mode(if self.config.durable { 2 } else { 1 });

        // Process events in configurable batch chunks
        for chunk in events.chunks(self.config.batch_size) {
            let mut confirms = Vec::with_capacity(chunk.len());

            for event in chunk {
                let event_json = serde_json::to_string(event)
                    .map_err(|e| CelersError::Other(format!("Event serialization error: {}", e)))?;

                let confirm = channel
                    .basic_publish(
                        self.config.exchange.as_str().into(),
                        self.config.routing_key.as_str().into(),
                        BasicPublishOptions::default(),
                        event_json.as_bytes(),
                        properties.clone(),
                    )
                    .await
                    .map_err(|e| CelersError::Other(format!("AMQP batch publish error: {}", e)))?;

                confirms.push((event_json.len() as u64, event.event_type(), confirm));
            }

            // Wait for all confirms in this chunk
            for (bytes, event_type, confirm) in confirms {
                match confirm.await {
                    Ok(_) => {
                        let mut stats = self.stats.write().await;
                        stats.record_success(bytes);
                        debug!(
                            event_type = %event_type,
                            exchange = %self.config.exchange,
                            bytes = bytes,
                            "Batch-published event to AMQP exchange"
                        );
                    }
                    Err(e) => {
                        let mut stats = self.stats.write().await;
                        stats.record_failure();
                        warn!(
                            event_type = %event_type,
                            exchange = %self.config.exchange,
                            error = %e,
                            "Failed to confirm batch-published event"
                        );
                    }
                }
            }
        }

        Ok(())
    }

    fn is_enabled(&self) -> bool {
        self.config.enabled
    }
}

impl std::fmt::Debug for AmqpEventEmitter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AmqpEventEmitter")
            .field("exchange", &self.config.exchange)
            .field("exchange_type", &self.config.exchange_type)
            .field("enabled", &self.config.enabled)
            .field("durable", &self.config.durable)
            .finish()
    }
}

/// AMQP event receiver - subscribes to events from a RabbitMQ exchange
///
/// Creates an exclusive, auto-delete queue bound to the event exchange,
/// then consumes messages from it. This mirrors Celery's event receiver
/// behavior where each consumer gets its own transient queue.
pub struct AmqpEventReceiver {
    /// Event transport configuration
    config: AmqpEventConfig,

    /// Queue name (auto-generated exclusive queue)
    queue_name: String,

    /// AMQP connection URI
    uri: String,
}

impl AmqpEventReceiver {
    /// Create a new event receiver with default exchange settings
    ///
    /// # Arguments
    /// * `uri` - AMQP connection URI (e.g., "amqp://localhost:5672")
    pub fn new(uri: &str) -> Self {
        Self {
            config: AmqpEventConfig::default(),
            queue_name: format!(
                "celeryev.{}.{}",
                std::process::id(),
                uuid::Uuid::new_v4().as_simple()
            ),
            uri: uri.to_string(),
        }
    }

    /// Create a new event receiver with custom configuration
    ///
    /// # Arguments
    /// * `uri` - AMQP connection URI
    /// * `config` - Event transport configuration
    pub fn with_config(uri: &str, config: AmqpEventConfig) -> Self {
        Self {
            config,
            queue_name: format!(
                "celeryev.{}.{}",
                std::process::id(),
                uuid::Uuid::new_v4().as_simple()
            ),
            uri: uri.to_string(),
        }
    }

    /// Create a new event receiver with a specific queue name
    ///
    /// # Arguments
    /// * `uri` - AMQP connection URI
    /// * `config` - Event transport configuration
    /// * `queue_name` - Explicit queue name to use
    pub fn with_queue_name(
        uri: &str,
        config: AmqpEventConfig,
        queue_name: impl Into<String>,
    ) -> Self {
        Self {
            config,
            queue_name: queue_name.into(),
            uri: uri.to_string(),
        }
    }

    /// Get the queue name that will be used
    pub fn queue_name(&self) -> &str {
        &self.queue_name
    }

    /// Get a reference to the configuration
    pub fn config(&self) -> &AmqpEventConfig {
        &self.config
    }

    /// Start receiving events, calling the handler for each event
    ///
    /// This method:
    /// 1. Connects to the AMQP server
    /// 2. Declares the event exchange (idempotent)
    /// 3. Declares an exclusive, auto-delete queue
    /// 4. Binds the queue to the exchange
    /// 5. Starts consuming and dispatching events to the handler
    ///
    /// # Arguments
    /// * `handler` - Async function to call for each received event
    ///
    /// # Example
    /// ```no_run
    /// use celers_broker_amqp::event_transport::AmqpEventReceiver;
    /// use celers_core::event::Event;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let receiver = AmqpEventReceiver::new("amqp://localhost:5672");
    ///
    /// receiver.receive(|event| async move {
    ///     println!("Received event: {:?}", event.event_type());
    ///     Ok(())
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn receive<F, Fut>(&self, mut handler: F) -> std::result::Result<(), CelersError>
    where
        F: FnMut(Event) -> Fut,
        Fut: std::future::Future<Output = std::result::Result<(), CelersError>>,
    {
        use futures_util::StreamExt;

        let conn = Connection::connect(&self.uri, ConnectionProperties::default())
            .await
            .map_err(|e| CelersError::Other(format!("AMQP connection error: {}", e)))?;

        let channel = conn
            .create_channel()
            .await
            .map_err(|e| CelersError::Other(format!("AMQP channel creation error: {}", e)))?;

        // Declare the exchange (idempotent)
        let exchange_kind = self.config.exchange_kind();
        let exchange_opts = ExchangeDeclareOptions {
            passive: false,
            durable: self.config.durable,
            auto_delete: !self.config.durable,
            internal: false,
            nowait: false,
        };

        channel
            .exchange_declare(
                self.config.exchange.as_str().into(),
                exchange_kind,
                exchange_opts,
                FieldTable::default(),
            )
            .await
            .map_err(|e| CelersError::Other(format!("AMQP exchange declare error: {}", e)))?;

        // Declare an exclusive, auto-delete queue
        let queue_opts = QueueDeclareOptions {
            passive: false,
            durable: false,
            exclusive: true,
            auto_delete: true,
            nowait: false,
        };

        let queue = channel
            .queue_declare(
                self.queue_name.as_str().into(),
                queue_opts,
                FieldTable::default(),
            )
            .await
            .map_err(|e| CelersError::Other(format!("AMQP queue declare error: {}", e)))?;

        debug!(
            queue = %queue.name(),
            exchange = %self.config.exchange,
            "Declared exclusive event queue"
        );

        // Bind queue to exchange
        channel
            .queue_bind(
                queue.name().as_str().into(),
                self.config.exchange.as_str().into(),
                self.config.routing_key.as_str().into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| CelersError::Other(format!("AMQP queue bind error: {}", e)))?;

        debug!(
            queue = %queue.name(),
            exchange = %self.config.exchange,
            routing_key = %self.config.routing_key,
            "Bound queue to event exchange"
        );

        // Start consuming
        let consumer_tag = format!("celers-event-consumer-{}", uuid::Uuid::new_v4().as_simple());

        let mut consumer = channel
            .basic_consume(
                queue.name().as_str().into(),
                consumer_tag.as_str().into(),
                BasicConsumeOptions {
                    no_local: false,
                    no_ack: true, // Events are fire-and-forget
                    exclusive: false,
                    nowait: false,
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| CelersError::Other(format!("AMQP consume error: {}", e)))?;

        debug!(
            queue = %queue.name(),
            "Started consuming events"
        );

        // Process deliveries
        while let Some(delivery_result) = consumer.next().await {
            match delivery_result {
                Ok(delivery) => {
                    let payload = std::str::from_utf8(&delivery.data).map_err(|e| {
                        CelersError::Other(format!("Invalid UTF-8 in event payload: {}", e))
                    })?;

                    match serde_json::from_str::<Event>(payload) {
                        Ok(event) => {
                            handler(event).await?;
                        }
                        Err(e) => {
                            warn!(
                                error = %e,
                                payload_len = delivery.data.len(),
                                "Failed to deserialize event from AMQP"
                            );
                        }
                    }
                }
                Err(e) => {
                    error!(error = %e, "AMQP delivery error");
                    return Err(CelersError::Other(format!("AMQP delivery error: {}", e)));
                }
            }
        }

        Ok(())
    }

    /// Subscribe and return a channel-based stream of events
    ///
    /// Returns a `tokio::sync::mpsc::Receiver` that yields deserialized events.
    /// The consumer runs in a background task.
    ///
    /// # Arguments
    /// * `buffer_size` - Size of the mpsc channel buffer
    ///
    /// # Returns
    /// A tuple of (receiver, join_handle) where the join_handle can be used
    /// to monitor or cancel the background consumer task.
    pub async fn subscribe(
        &self,
        buffer_size: usize,
    ) -> std::result::Result<
        (
            tokio::sync::mpsc::Receiver<Event>,
            tokio::task::JoinHandle<std::result::Result<(), CelersError>>,
        ),
        CelersError,
    > {
        use futures_util::StreamExt;

        let (tx, rx) = tokio::sync::mpsc::channel(buffer_size);

        let conn = Connection::connect(&self.uri, ConnectionProperties::default())
            .await
            .map_err(|e| CelersError::Other(format!("AMQP connection error: {}", e)))?;

        let channel = conn
            .create_channel()
            .await
            .map_err(|e| CelersError::Other(format!("AMQP channel creation error: {}", e)))?;

        // Declare exchange
        let exchange_kind = self.config.exchange_kind();
        let exchange_opts = ExchangeDeclareOptions {
            passive: false,
            durable: self.config.durable,
            auto_delete: !self.config.durable,
            internal: false,
            nowait: false,
        };

        channel
            .exchange_declare(
                self.config.exchange.as_str().into(),
                exchange_kind,
                exchange_opts,
                FieldTable::default(),
            )
            .await
            .map_err(|e| CelersError::Other(format!("AMQP exchange declare error: {}", e)))?;

        // Declare exclusive auto-delete queue
        let queue_opts = QueueDeclareOptions {
            passive: false,
            durable: false,
            exclusive: true,
            auto_delete: true,
            nowait: false,
        };

        let queue = channel
            .queue_declare(
                self.queue_name.as_str().into(),
                queue_opts,
                FieldTable::default(),
            )
            .await
            .map_err(|e| CelersError::Other(format!("AMQP queue declare error: {}", e)))?;

        // Bind queue to exchange
        channel
            .queue_bind(
                queue.name().as_str().into(),
                self.config.exchange.as_str().into(),
                self.config.routing_key.as_str().into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| CelersError::Other(format!("AMQP queue bind error: {}", e)))?;

        // Start consuming
        let consumer_tag = format!("celers-event-sub-{}", uuid::Uuid::new_v4().as_simple());

        let mut consumer = channel
            .basic_consume(
                queue.name().as_str().into(),
                consumer_tag.as_str().into(),
                BasicConsumeOptions {
                    no_local: false,
                    no_ack: true,
                    exclusive: false,
                    nowait: false,
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| CelersError::Other(format!("AMQP consume error: {}", e)))?;

        let queue_name = queue.name().as_str().to_string();

        // Spawn background consumer task
        let handle = tokio::spawn(async move {
            debug!(queue = %queue_name, "Event subscriber background task started");

            while let Some(delivery_result) = consumer.next().await {
                match delivery_result {
                    Ok(delivery) => {
                        let payload = std::str::from_utf8(&delivery.data).map_err(|e| {
                            CelersError::Other(format!("Invalid UTF-8 in event payload: {}", e))
                        })?;

                        match serde_json::from_str::<Event>(payload) {
                            Ok(event) => {
                                if tx.send(event).await.is_err() {
                                    debug!("Event receiver dropped, stopping consumer");
                                    break;
                                }
                            }
                            Err(e) => {
                                warn!(
                                    error = %e,
                                    "Failed to deserialize event from AMQP subscription"
                                );
                            }
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "AMQP subscription delivery error");
                        return Err(CelersError::Other(format!("AMQP delivery error: {}", e)));
                    }
                }
            }

            Ok(())
        });

        Ok((rx, handle))
    }
}

impl std::fmt::Debug for AmqpEventReceiver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AmqpEventReceiver")
            .field("exchange", &self.config.exchange)
            .field("queue_name", &self.queue_name)
            .field("uri", &self.uri)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use celers_core::event::{TaskEventBuilder, WorkerEventBuilder};
    use uuid::Uuid;

    #[test]
    fn test_amqp_event_config_default() {
        let config = AmqpEventConfig::default();
        assert_eq!(config.exchange, "celeryev");
        assert_eq!(config.exchange_type, "fanout");
        assert_eq!(config.routing_key, "");
        assert!(!config.durable);
        assert!(config.enabled);
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.serialization, "json");
    }

    #[test]
    fn test_amqp_event_config_builder() {
        let config = AmqpEventConfig::new()
            .exchange("my-events")
            .exchange_type("topic")
            .routing_key("events.task.*")
            .durable(true)
            .enabled(false)
            .batch_size(50)
            .serialization("msgpack");

        assert_eq!(config.exchange, "my-events");
        assert_eq!(config.exchange_type, "topic");
        assert_eq!(config.routing_key, "events.task.*");
        assert!(config.durable);
        assert!(!config.enabled);
        assert_eq!(config.batch_size, 50);
        assert_eq!(config.serialization, "msgpack");
    }

    #[test]
    fn test_exchange_kind_parsing() {
        let config = AmqpEventConfig::new().exchange_type("direct");
        assert!(matches!(config.exchange_kind(), ExchangeKind::Direct));

        let config = AmqpEventConfig::new().exchange_type("fanout");
        assert!(matches!(config.exchange_kind(), ExchangeKind::Fanout));

        let config = AmqpEventConfig::new().exchange_type("topic");
        assert!(matches!(config.exchange_kind(), ExchangeKind::Topic));

        let config = AmqpEventConfig::new().exchange_type("headers");
        assert!(matches!(config.exchange_kind(), ExchangeKind::Headers));

        let config = AmqpEventConfig::new().exchange_type("x-delayed-message");
        assert!(matches!(config.exchange_kind(), ExchangeKind::Custom(_)));
    }

    #[test]
    fn test_event_transport_stats() {
        let mut stats = EventTransportStats::new();
        assert_eq!(stats.events_published, 0);
        assert_eq!(stats.events_failed, 0);
        assert_eq!(stats.bytes_sent, 0);
        assert!(stats.last_publish_at.is_none());
        assert_eq!(stats.total_attempts(), 0);
        assert_eq!(stats.success_rate(), 100.0);

        stats.record_success(256);
        assert_eq!(stats.events_published, 1);
        assert_eq!(stats.bytes_sent, 256);
        assert!(stats.last_publish_at.is_some());
        assert_eq!(stats.success_rate(), 100.0);

        stats.record_failure();
        assert_eq!(stats.events_failed, 1);
        assert_eq!(stats.total_attempts(), 2);
        assert_eq!(stats.success_rate(), 50.0);

        stats.record_success(128);
        assert_eq!(stats.events_published, 2);
        assert_eq!(stats.bytes_sent, 384);
        assert_eq!(stats.total_attempts(), 3);
        // 2/3 * 100 = 66.666...
        assert!((stats.success_rate() - 66.666).abs() < 0.1);
    }

    #[test]
    fn test_event_serialization_roundtrip_task_event() {
        let task_id = Uuid::new_v4();
        let event = TaskEventBuilder::new(task_id, "tasks.add")
            .hostname("worker-1")
            .started();

        let json = serde_json::to_string(&event).expect("task event should serialize");
        let deserialized: Event =
            serde_json::from_str(&json).expect("task event should deserialize");

        assert_eq!(deserialized.event_type(), event.event_type());
        assert_eq!(deserialized.task_id(), event.task_id());
    }

    #[test]
    fn test_event_serialization_roundtrip_worker_event() {
        let event = WorkerEventBuilder::new("worker-1").online();

        let json = serde_json::to_string(&event).expect("worker event should serialize");
        let deserialized: Event =
            serde_json::from_str(&json).expect("worker event should deserialize");

        assert_eq!(deserialized.event_type(), event.event_type());
    }

    #[test]
    fn test_event_serialization_roundtrip_batch() {
        let events = vec![
            WorkerEventBuilder::new("worker-1").online(),
            TaskEventBuilder::new(Uuid::new_v4(), "tasks.add")
                .hostname("worker-1")
                .started(),
            WorkerEventBuilder::new("worker-1").heartbeat(4, 100, [0.5, 0.4, 0.3], 2.0),
        ];

        for event in &events {
            let json = serde_json::to_string(event).expect("event should serialize");
            let deserialized: Event =
                serde_json::from_str(&json).expect("event should deserialize");
            assert_eq!(deserialized.event_type(), event.event_type());
        }
    }

    #[test]
    fn test_amqp_event_receiver_queue_name_generation() {
        let receiver = AmqpEventReceiver::new("amqp://localhost:5672");
        assert!(receiver.queue_name().starts_with("celeryev."));
        assert!(!receiver.queue_name().is_empty());
    }

    #[test]
    fn test_amqp_event_receiver_custom_queue_name() {
        let config = AmqpEventConfig::default();
        let receiver =
            AmqpEventReceiver::with_queue_name("amqp://localhost:5672", config, "my-custom-queue");
        assert_eq!(receiver.queue_name(), "my-custom-queue");
    }

    #[test]
    fn test_amqp_event_receiver_config() {
        let config = AmqpEventConfig::new().exchange("my-exchange");
        let receiver = AmqpEventReceiver::with_config("amqp://localhost:5672", config);
        assert_eq!(receiver.config().exchange, "my-exchange");
    }

    #[test]
    fn test_emitter_debug_format() {
        let config = AmqpEventConfig::new()
            .exchange("test-exchange")
            .exchange_type("topic")
            .enabled(true)
            .durable(false);

        let emitter = AmqpEventEmitter {
            channel: Arc::new(RwLock::new(None)),
            uri: String::new(),
            config,
            stats: Arc::new(RwLock::new(EventTransportStats::new())),
            exchange_declared: Arc::new(RwLock::new(false)),
        };

        let debug_str = format!("{:?}", emitter);
        assert!(debug_str.contains("test-exchange"));
        assert!(debug_str.contains("topic"));
    }

    #[test]
    fn test_receiver_debug_format() {
        let receiver = AmqpEventReceiver::new("amqp://localhost:5672");
        let debug_str = format!("{:?}", receiver);
        assert!(debug_str.contains("celeryev"));
        assert!(debug_str.contains("amqp://localhost:5672"));
    }

    #[test]
    fn test_config_exchange_kind_default_is_fanout() {
        let config = AmqpEventConfig::default();
        assert!(matches!(config.exchange_kind(), ExchangeKind::Fanout));
    }
}
