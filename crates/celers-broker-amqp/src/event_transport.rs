//! AMQP event transport for real-time event publishing
//!
//! This module provides AMQP (RabbitMQ) based event transport for CeleRS events.
//! It implements the `EventEmitter` trait from `celers-core` and publishes
//! events to a RabbitMQ fanout exchange following Celery's event protocol.
//!
//! # Wire format
//!
//! Events travel in the **Celery wire shape**, not CeleRS' internal one: a
//! `type` string, a float Unix `timestamp`, Celery's `uuid`/`name` field names
//! and the `clock`/`utcoffset`/`pid`/`hostname` envelope. That is exactly what
//! `celers_protocol::event::EventMessage` describes, so `celery events`,
//! Flower and any other Celery monitor parse what this emitter publishes.
//! [`celers_core::event::Event::to_wire_json`] renders it and
//! [`celers_core::event::Event::from_wire_str`] parses it back, so
//! [`AmqpEventEmitter`] and [`AmqpEventReceiver`] stay a matched pair.
//!
//! [`AmqpEventReceiver`] also reads events published by a real Python Celery
//! worker on the same exchange, so a mixed cluster monitors as one stream.
//!
//! # Celery Event Exchange
//!
//! Events are published to a **topic** exchange (default: `celeryev`), matching
//! real Celery's `celery.events` exchange rather than a fanout: [`EventRoutingMode`]
//! (default [`EventRoutingMode::PerEventType`]) publishes each event under a key
//! derived from its own wire `type` (`task-started` becomes `task.started`,
//! `worker-heartbeat` becomes `worker.heartbeat`), the same translation
//! `celery.events.dispatcher.EventDispatcher` applies before publishing. A
//! consumer can then bind `task.#` for every task event, `worker.#` for every
//! worker event, or `#` for everything — [`AmqpEventReceiver`] does the latter
//! by default (see [`AmqpEventConfig::receiver_binding_key`]).
//!
//! The pre-topic-routing behaviour — every event under one fixed
//! [`AmqpEventConfig::routing_key`] — is kept as [`EventRoutingMode::Fixed`], an
//! explicit opt-in for a fanout exchange (where RabbitMQ ignores the routing key
//! outright) or a direct exchange bound on one key.
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
use celers_core::event::{Event, EventEmitter, EventEnvelope};
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
///
/// `"topic"`, matching the real `celery.events.dispatcher.EventDispatcher`'s
/// `celeryev` exchange — not `"fanout"`. A mixed cluster with a Python Celery
/// worker publishing onto the real topic exchange would otherwise collide with
/// this emitter declaring the same name as a different kind.
const DEFAULT_EXCHANGE_TYPE: &str = "topic";

/// Default batch size for batch publishing
const DEFAULT_BATCH_SIZE: usize = 100;

/// How the routing key is chosen for each event this emitter publishes.
///
/// See the module documentation for the topic-exchange rationale.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum EventRoutingMode {
    /// Derive the routing key from the event's own wire `type`
    /// ([`celers_core::event::Event::event_type`]): every `-` becomes a `.`,
    /// so `task-started` publishes under `task.started` and
    /// `worker-heartbeat` under `worker.heartbeat`. This is what
    /// [`AmqpEventConfig::publish_routing_key`] computes, matches what real
    /// Celery puts on the wire for the same exchange, and is the default.
    #[default]
    PerEventType,
    /// Publish every event under the fixed [`AmqpEventConfig::routing_key`],
    /// regardless of its type. The pre-topic-routing behaviour, kept as an
    /// explicit choice for a fanout exchange (where RabbitMQ ignores the
    /// routing key outright) or a direct exchange bound on one fixed key.
    Fixed,
}

/// Configuration for AMQP event transport
#[derive(Debug, Clone)]
pub struct AmqpEventConfig {
    /// Exchange name for events (default: "celeryev")
    pub exchange: String,

    /// Exchange type (default: "topic")
    pub exchange_type: String,

    /// How the routing key is chosen per event (default:
    /// [`EventRoutingMode::PerEventType`])
    pub routing_mode: EventRoutingMode,

    /// Routing key for published messages (default: "")
    ///
    /// Consulted in two places: as the fixed publish key when `routing_mode`
    /// is [`EventRoutingMode::Fixed`] (see
    /// [`AmqpEventConfig::publish_routing_key`]), and, unconditionally, as the
    /// literal binding pattern `AmqpEventReceiver` requests — unless it is left
    /// at this default *and* the exchange is a topic, in which case
    /// [`AmqpEventConfig::receiver_binding_key`] substitutes Celery's
    /// catch-all `"#"` rather than binding on a key nothing publishes under.
    pub routing_key: String,

    /// Whether the exchange is durable (default: false)
    pub durable: bool,

    /// Whether the emitter is enabled (default: true)
    pub enabled: bool,

    /// Maximum batch size for batch publishing (default: 100)
    pub batch_size: usize,

    /// Serialization format (default: "json")
    pub serialization: String,

    /// Hostname stamped onto events that do not carry one of their own
    ///
    /// `task-sent` is the only such event left — it is published by the
    /// *client*, which is not a worker and has no hostname of its own — so a
    /// monitor otherwise cannot tell which node published it. Leave `None` to
    /// omit the field entirely.
    pub hostname: Option<String>,
}

impl Default for AmqpEventConfig {
    fn default() -> Self {
        Self {
            exchange: DEFAULT_EXCHANGE.to_string(),
            exchange_type: DEFAULT_EXCHANGE_TYPE.to_string(),
            routing_mode: EventRoutingMode::default(),
            routing_key: String::new(),
            durable: false,
            enabled: true,
            batch_size: DEFAULT_BATCH_SIZE,
            serialization: "json".to_string(),
            hostname: None,
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

    /// Set how the routing key is chosen per event; see [`EventRoutingMode`].
    pub fn routing_mode(mut self, mode: EventRoutingMode) -> Self {
        self.routing_mode = mode;
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

    /// Set the fallback hostname stamped onto events that carry none
    pub fn hostname(mut self, hostname: impl Into<String>) -> Self {
        self.hostname = Some(hostname.into());
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

    /// The routing key an emitter publishes an event of `event_type` under.
    ///
    /// `event_type` is the wire `type` string
    /// ([`celers_core::event::Event::event_type`]), e.g. `"task-started"` or
    /// `"worker-heartbeat"`.
    ///
    /// Under [`EventRoutingMode::PerEventType`] (the default) every `-` in
    /// `event_type` becomes a `.` — `"task-started"` publishes as
    /// `"task.started"`, `"worker-heartbeat"` as `"worker.heartbeat"` — which
    /// is what a real Celery `EventDispatcher` puts on the wire for the same
    /// `celeryev` exchange, and is what lets a topic-exchange consumer bind
    /// `task.#` / `worker.#` / `#` usefully.
    ///
    /// Under [`EventRoutingMode::Fixed`], `event_type` is ignored and the
    /// configured [`AmqpEventConfig::routing_key`] is returned verbatim.
    pub fn publish_routing_key(&self, event_type: &str) -> String {
        match self.routing_mode {
            EventRoutingMode::PerEventType => event_type.replace('-', "."),
            EventRoutingMode::Fixed => self.routing_key.clone(),
        }
    }

    /// The binding pattern [`AmqpEventReceiver`] subscribes with.
    ///
    /// An explicit (non-empty) [`AmqpEventConfig::routing_key`] is always
    /// honoured verbatim, whatever the exchange type. Left at its default
    /// (empty) on a **topic** exchange this returns Celery's own catch-all
    /// pattern `"#"` instead: binding on the literal empty string would only
    /// match events an emitter running [`EventRoutingMode::Fixed`] with an
    /// equally-empty key deliberately publishes there, which is not how
    /// [`EventRoutingMode::PerEventType`] — the default — publishes anything.
    /// On any other exchange type the key is taken literally, empty or not:
    /// `#`/`*` wildcards are a topic-exchange concept and bind nothing extra
    /// on a fanout (which ignores the routing key regardless) or a direct
    /// exchange (exact match only).
    pub fn receiver_binding_key(&self) -> &str {
        if self.routing_key.is_empty() && self.exchange_type == "topic" {
            "#"
        } else {
            self.routing_key.as_str()
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

        enable_publisher_confirms(&channel).await?;

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

    /// Render one event in the Celery wire shape.
    ///
    /// Each call stamps a fresh envelope, so every published event gets its own
    /// logical clock tick — a monitor can order two events that share a
    /// wall-clock timestamp.
    fn render(&self, event: &Event) -> Result<String> {
        let mut envelope = EventEnvelope::stamp();
        if let Some(ref hostname) = self.config.hostname {
            envelope = envelope.with_hostname(hostname);
        }
        event.to_wire_json_with(&envelope)
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

        enable_publisher_confirms(&new_channel).await?;

        // Reset exchange declared flag since we have a new channel
        {
            let mut declared = self.exchange_declared.write().await;
            *declared = false;
        }

        let mut guard = self.channel.write().await;
        *guard = Some(new_channel.clone());

        Ok(new_channel)
    }

    /// Publish a serialized event payload to the exchange under the routing
    /// key `event_type` resolves to; see [`AmqpEventConfig::publish_routing_key`].
    async fn publish_payload(
        &self,
        event_type: &str,
        payload: &[u8],
    ) -> std::result::Result<(), CelersError> {
        let channel = self.get_channel().await?;

        self.ensure_exchange(&channel).await?;

        let properties = BasicProperties::default()
            .with_content_type("application/json".into())
            .with_delivery_mode(if self.config.durable { 2 } else { 1 });

        let routing_key = self.config.publish_routing_key(event_type);
        let confirmation = channel
            .basic_publish(
                self.config.exchange.as_str().into(),
                routing_key.as_str().into(),
                BasicPublishOptions::default(),
                payload,
                properties,
            )
            .await
            .map_err(|e| CelersError::Other(format!("AMQP publish error: {}", e)))?
            .await
            .map_err(|e| CelersError::Other(format!("AMQP publish confirm error: {}", e)))?;

        // A `Nack` or an unroutable return means the broker did not take the
        // event: it must not be reported as a successful publish.
        crate::confirm::classify_confirmation(confirmation, true)
            .map_err(|e| CelersError::Other(format!("AMQP publish not confirmed: {}", e)))?;

        Ok(())
    }
}

/// Enable publisher confirms on an event-publishing channel.
///
/// Without `confirm.select` every `PublisherConfirm` resolves immediately
/// with `Confirmation::NotRequested`, so "confirmed" statistics would be
/// fabricated.
async fn enable_publisher_confirms(channel: &Channel) -> std::result::Result<(), CelersError> {
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .map_err(|e| CelersError::Other(format!("AMQP confirm.select error: {}", e)))
}

#[async_trait]
impl EventEmitter for AmqpEventEmitter {
    async fn emit(&self, event: Event) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        // Render the Celery wire shape, not the internal model.
        let event_json = self.render(&event)?;

        let payload = event_json.as_bytes();
        let payload_len = payload.len() as u64;

        match self.publish_payload(event.event_type(), payload).await {
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
                // Render the Celery wire shape, not the internal model. Each
                // event gets its own envelope, so the logical clock still
                // orders them.
                let event_json = self.render(event)?;
                let routing_key = self.config.publish_routing_key(event.event_type());

                let confirm = channel
                    .basic_publish(
                        self.config.exchange.as_str().into(),
                        routing_key.as_str().into(),
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
                let outcome = match confirm.await {
                    Ok(confirmation) => crate::confirm::classify_confirmation(confirmation, true)
                        .map_err(|e| CelersError::Other(e.to_string())),
                    Err(e) => Err(CelersError::Other(format!(
                        "AMQP publish confirm error: {}",
                        e
                    ))),
                };
                match outcome {
                    Ok(()) => {
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
            .field("routing_mode", &self.config.routing_mode)
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
                self.config.receiver_binding_key().into(),
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

                    match Event::from_wire_str(payload) {
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
                self.config.receiver_binding_key().into(),
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

                        match Event::from_wire_str(payload) {
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
        assert_eq!(
            config.exchange_type, "topic",
            "must match the real celery.events.dispatcher exchange, not fanout"
        );
        assert_eq!(config.routing_mode, EventRoutingMode::PerEventType);
        assert_eq!(config.routing_key, "");
        assert!(!config.durable);
        assert!(config.enabled);
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.serialization, "json");
        assert!(config.hostname.is_none());
    }

    #[test]
    fn test_amqp_event_config_routing_mode_builder() {
        let config = AmqpEventConfig::new().routing_mode(EventRoutingMode::Fixed);
        assert_eq!(config.routing_mode, EventRoutingMode::Fixed);
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
            .serialization("msgpack")
            .hostname("celery@publisher");

        assert_eq!(config.exchange, "my-events");
        assert_eq!(config.exchange_type, "topic");
        assert_eq!(config.routing_key, "events.task.*");
        assert!(config.durable);
        assert!(!config.enabled);
        assert_eq!(config.batch_size, 50);
        assert_eq!(config.serialization, "msgpack");
        assert_eq!(config.hostname, Some("celery@publisher".to_string()));
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

    /// The emitter publishes the Celery wire shape and the receiver parses it
    /// back, so the two must round trip through *that* format — not through
    /// `Event`'s internal serde representation, which never reaches a broker.
    #[test]
    fn test_event_wire_roundtrip_task_event() {
        let task_id = Uuid::new_v4();
        let event = TaskEventBuilder::new(task_id, "tasks.add")
            .hostname("worker-1")
            .pid(4242)
            .started();

        let json = event.to_wire_json().expect("task event should render");

        // Celery's field names, not CeleRS' internal ones.
        assert!(json.contains(r#""type":"task-started""#), "{json}");
        assert!(json.contains(&format!(r#""uuid":"{task_id}""#)), "{json}");
        assert!(json.contains(r#""name":"tasks.add""#), "{json}");
        assert!(!json.contains("task_id"), "{json}");
        assert!(!json.contains("task_name"), "{json}");

        let deserialized = Event::from_wire_str(&json).expect("task event should parse back");
        assert_eq!(deserialized, event);
    }

    #[test]
    fn test_event_wire_roundtrip_worker_event() {
        let event = WorkerEventBuilder::new("worker-1").online();

        let json = event.to_wire_json().expect("worker event should render");
        assert!(json.contains(r#""type":"worker-online""#), "{json}");
        assert!(json.contains(r#""clock":"#), "{json}");

        let deserialized = Event::from_wire_str(&json).expect("worker event should parse back");
        assert_eq!(deserialized, event);
    }

    #[test]
    fn test_event_wire_roundtrip_batch() {
        let events = vec![
            WorkerEventBuilder::new("worker-1").online(),
            TaskEventBuilder::new(Uuid::new_v4(), "tasks.add")
                .hostname("worker-1")
                .started(),
            WorkerEventBuilder::new("worker-1").heartbeat(4, 100, [0.5, 0.4, 0.3], 2.0),
        ];

        for event in &events {
            let json = event.to_wire_json().expect("event should render");
            let deserialized = Event::from_wire_str(&json).expect("event should parse back");
            assert_eq!(&deserialized, event);
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
    fn test_config_exchange_kind_default_is_topic() {
        let config = AmqpEventConfig::default();
        assert!(matches!(config.exchange_kind(), ExchangeKind::Topic));
    }

    // --- Per-event-type routing keys (idx: topic-exchange routing) ---

    /// The default mode derives a dotted routing key from each event's own
    /// wire `type`, matching what a real Celery `EventDispatcher` publishes
    /// on the same exchange.
    #[test]
    fn test_publish_routing_key_derives_a_dotted_key_per_event_type() {
        let config = AmqpEventConfig::default();
        assert_eq!(config.routing_mode, EventRoutingMode::PerEventType);

        assert_eq!(config.publish_routing_key("task-started"), "task.started");
        assert_eq!(
            config.publish_routing_key("worker-heartbeat"),
            "worker.heartbeat"
        );
        assert_eq!(
            config.publish_routing_key("task-soft-time-limit-exceeded"),
            "task.soft.time.limit.exceeded"
        );
        // No hyphens: nothing to translate.
        assert_eq!(config.publish_routing_key("taskstarted"), "taskstarted");
    }

    /// The routing key an emitter would actually pass to `basic_publish` for
    /// every event type this crate emits, pinned so a typo in the
    /// hyphen-to-dot translation (or in `Event::event_type` itself) shows up
    /// here rather than only against a live broker.
    #[test]
    fn test_publish_routing_key_covers_every_known_event_type() {
        use celers_core::event::{Event, TaskEventBuilder, WorkerEventBuilder};

        let config = AmqpEventConfig::default();
        let task_id = Uuid::new_v4();
        let events: Vec<Event> = vec![
            TaskEventBuilder::new(task_id, "t").sent("celery"),
            TaskEventBuilder::new(task_id, "t").received(),
            TaskEventBuilder::new(task_id, "t").started(),
            WorkerEventBuilder::new("w").online(),
            WorkerEventBuilder::new("w").offline(),
            WorkerEventBuilder::new("w").heartbeat(0, 0, [0.0, 0.0, 0.0], 1.0),
        ];
        for event in events {
            let key = config.publish_routing_key(event.event_type());
            assert!(!key.contains('-'), "key must be fully dotted: {key}");
            assert_eq!(key, event.event_type().replace('-', "."));
        }
    }

    /// `Fixed` mode ignores the event type entirely -- the pre-topic-routing
    /// behaviour, preserved for a fanout exchange (which ignores the routing
    /// key outright) or a direct exchange bound on one key.
    #[test]
    fn test_publish_routing_key_fixed_mode_ignores_the_event_type() {
        let config = AmqpEventConfig::new()
            .routing_mode(EventRoutingMode::Fixed)
            .routing_key("celery");

        assert_eq!(config.publish_routing_key("task-started"), "celery");
        assert_eq!(config.publish_routing_key("worker-heartbeat"), "celery");
        assert_eq!(config.publish_routing_key(""), "celery");
    }

    /// On the default topic exchange, an unset routing key must not leave the
    /// receiver bound to a pattern nothing under `PerEventType` ever
    /// publishes to.
    #[test]
    fn test_receiver_binding_key_defaults_to_the_topic_catchall() {
        let config = AmqpEventConfig::default();
        assert_eq!(config.exchange_type, "topic");
        assert_eq!(config.receiver_binding_key(), "#");
    }

    /// An explicit routing key is always honoured verbatim as the binding
    /// pattern, on any exchange type.
    #[test]
    fn test_receiver_binding_key_honours_an_explicit_key() {
        let topic = AmqpEventConfig::new()
            .exchange_type("topic")
            .routing_key("task.#");
        assert_eq!(topic.receiver_binding_key(), "task.#");

        let fanout = AmqpEventConfig::new()
            .exchange_type("fanout")
            .routing_key("ignored-by-fanout");
        assert_eq!(fanout.receiver_binding_key(), "ignored-by-fanout");
    }

    /// The `"#"` substitution is specifically a topic-exchange concept: on a
    /// fanout or direct exchange an empty key is taken literally, because
    /// `#`/`*` wildcards mean nothing to either.
    #[test]
    fn test_receiver_binding_key_is_literal_off_topic() {
        let fanout = AmqpEventConfig::new().exchange_type("fanout");
        assert_eq!(fanout.receiver_binding_key(), "");

        let direct = AmqpEventConfig::new().exchange_type("direct");
        assert_eq!(direct.receiver_binding_key(), "");
    }
}
