//! Core AmqpBroker struct, construction, connection management, and trait implementations.

use async_trait::async_trait;
use celers_kombu::{
    Broker, BrokerError, Consumer, Envelope, Producer, QueueMode, Result, Transport,
};
use celers_protocol::Message;
use futures_util::StreamExt;
use lapin::{
    options::*,
    types::{FieldTable, ShortString},
    BasicProperties, Channel, Connection,
};
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use crate::confirm::classify_confirmation;
use crate::connect;
use crate::management::ManagementApiClient;
use crate::pool::{configure_channel, ChannelPool, ConnectionPool, DeduplicationCache};
use crate::topic_routing;
use crate::types::*;

/// AMQP broker implementation using RabbitMQ
pub struct AmqpBroker {
    pub(crate) url: String,
    pub(crate) queue_name: String,
    pub(crate) connection: Option<Connection>,
    pub(crate) channel: Option<Channel>,
    #[allow(dead_code)]
    pub(crate) consumer_tag: Option<String>,
    /// Broker configuration
    pub(crate) config: AmqpConfig,
    /// Current transaction state
    pub(crate) transaction_state: TransactionState,
    /// Reconnection statistics
    pub(crate) reconnection_stats: ReconnectionStats,
    /// Connection pool (if enabled)
    pub(crate) connection_pool: Option<ConnectionPool>,
    /// Channel pool (if enabled)
    pub(crate) channel_pool: Option<ChannelPool>,
    /// Channel-level metrics
    pub(crate) channel_metrics: ChannelMetrics,
    /// Publisher confirm statistics
    pub(crate) publisher_confirm_stats: PublisherConfirmStats,
    /// Message deduplication cache (if enabled)
    pub(crate) deduplication_cache: Option<DeduplicationCache>,
    /// Management API client (if configured)
    pub(crate) management_api_client: Option<ManagementApiClient>,
    /// Topic router for task-based routing key resolution (if configured)
    pub(crate) topic_router: Option<topic_routing::TopicRouter>,
    /// Whether the cached channel actually has publisher confirms enabled.
    ///
    /// This tracks the channel, not the configuration: a channel created for
    /// an AMQP transaction is in `tx` mode and can never be in confirm mode.
    pub(crate) channel_confirm_mode: bool,
    /// Live `basic.consume` subscriptions, keyed by queue name.
    ///
    /// Consumers are bound to the channel that created them, so this map is
    /// cleared whenever the channel is replaced.
    pub(crate) consumers: HashMap<String, lapin::Consumer>,
}

impl AmqpBroker {
    /// Create a new AMQP broker with default configuration
    pub async fn new(url: &str, queue_name: &str) -> Result<Self> {
        Self::with_config(url, queue_name, AmqpConfig::default()).await
    }

    /// Create a new AMQP broker with custom configuration
    pub async fn with_config(url: &str, queue_name: &str, config: AmqpConfig) -> Result<Self> {
        let connection_pool = if config.connection_pool_size > 0 {
            // Pooled connections must use exactly the same URI (vhost,
            // heartbeat, connection timeout) as the primary connection.
            let pool_url = match config.vhost {
                Some(ref vhost) if url.ends_with('/') => format!("{}{}", url, vhost),
                Some(ref vhost) => format!("{}/{}", url, vhost),
                None => url.to_string(),
            };
            let uri = connect::build_uri(&pool_url, config.heartbeat, config.connection_timeout)?;
            Some(ConnectionPool::new(
                uri,
                config.connection_timeout,
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
            Some(ManagementApiClient::new(mgmt_url, mgmt_user, mgmt_pass)?)
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
            topic_router: None,
            channel_confirm_mode: false,
            consumers: HashMap::new(),
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

    /// Configure topic-based routing for task messages.
    ///
    /// When a topic router is set, publish methods will resolve the routing key
    /// from the task name in the message headers using the router's rules.
    /// If no rule matches, the router's default routing key is used.
    /// The exchange is also overridden to the router's configured topic exchange.
    pub fn with_topic_router(mut self, router: topic_routing::TopicRouter) -> Self {
        self.topic_router = Some(router);
        self
    }

    /// Set the topic router on a mutable reference (for post-construction configuration).
    pub fn set_topic_router(&mut self, router: topic_routing::TopicRouter) {
        self.topic_router = Some(router);
    }

    /// Get a reference to the topic router if configured.
    pub fn topic_router(&self) -> Option<&topic_routing::TopicRouter> {
        self.topic_router.as_ref()
    }

    /// Get a mutable reference to the topic router.
    pub fn topic_router_mut(&mut self) -> Option<&mut topic_routing::TopicRouter> {
        self.topic_router.as_mut()
    }

    /// Set up AMQP topology for topic routing.
    ///
    /// Creates the topic exchange based on the router configuration.
    /// Queue bindings are typically done by consumers, not producers;
    /// this method just ensures the exchange exists.
    pub async fn setup_topic_routing(&mut self) -> Result<()> {
        let exchange_name = self
            .topic_router
            .as_ref()
            .map(|router| router.exchange_name().to_string());

        if let Some(exchange_name) = exchange_name {
            let channel = self.get_channel().await?;

            channel
                .exchange_declare(
                    exchange_name.as_str().into(),
                    lapin::ExchangeKind::Topic,
                    lapin::options::ExchangeDeclareOptions {
                        durable: true,
                        ..Default::default()
                    },
                    lapin::types::FieldTable::default(),
                )
                .await
                .map_err(|e| {
                    BrokerError::OperationFailed(format!(
                        "Failed to declare topic exchange '{}': {}",
                        exchange_name, e
                    ))
                })?;

            debug!(
                "Declared topic exchange '{}' for topic routing",
                exchange_name
            );
        }
        Ok(())
    }

    /// Get the effective URL including virtual host if configured
    pub(crate) fn effective_url(&self) -> String {
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

    /// Open a connection using the configured heartbeat, vhost and timeout.
    pub(crate) async fn open_connection(&self) -> Result<Connection> {
        let uri = connect::build_uri(
            &self.effective_url(),
            self.config.heartbeat,
            self.config.connection_timeout,
        )?;
        connect::open_connection(&uri, self.config.connection_timeout).await
    }

    /// Whether publisher confirms should be enabled on a newly created channel.
    ///
    /// `confirm.select` and `tx.select` are mutually exclusive on a channel,
    /// so confirms are suppressed while an AMQP transaction is in progress.
    pub(crate) fn confirms_enabled(&self) -> bool {
        self.config.publisher_confirms && self.transaction_state != TransactionState::Started
    }

    /// Create a channel with QoS and publisher confirms applied.
    pub(crate) async fn create_configured_channel(
        &self,
        connection: &Connection,
    ) -> Result<Channel> {
        let channel = connection
            .create_channel()
            .await
            .map_err(|e| BrokerError::Connection(format!("Failed to create channel: {}", e)))?;

        configure_channel(
            &channel,
            self.config.effective_prefetch_count(),
            self.config.prefetch_global,
            self.confirms_enabled(),
        )
        .await?;

        debug!(
            "Created channel (prefetch={}, global={}, confirms={})",
            self.config.effective_prefetch_count(),
            self.config.prefetch_global,
            self.confirms_enabled()
        );

        Ok(channel)
    }

    /// Whether the cached channel exists and is still usable.
    ///
    /// A channel dies permanently on any channel-level exception (a failed
    /// passive declare, an unroutable mandatory publish, an ack with an
    /// unknown delivery tag) while the TCP connection stays up, so channel
    /// health has to be checked independently of connection health.
    pub(crate) fn channel_alive(&self) -> bool {
        self.channel
            .as_ref()
            .map(|ch| ch.status().connected())
            .unwrap_or(false)
    }

    /// Drop the cached channel and every subscription bound to it.
    pub(crate) fn discard_channel(&mut self) {
        self.channel = None;
        self.channel_confirm_mode = false;
        // Consumers are channel-scoped: a consumer from a dead channel never
        // yields another delivery, so they must not survive the channel.
        self.consumers.clear();
    }

    /// Check connection and channel health, attempting auto-reconnection if needed
    pub(crate) async fn ensure_connection(&mut self) -> Result<()> {
        // Check if connection is alive
        let connection_alive = self
            .connection
            .as_ref()
            .map(|c| c.status().connected())
            .unwrap_or(false);

        if !connection_alive {
            self.discard_channel();
            if self.config.auto_reconnect {
                info!("Connection lost, attempting auto-reconnection...");
                self.auto_reconnect().await?;
            } else {
                return Err(BrokerError::Connection(
                    "Connection lost and auto-reconnect is disabled".to_string(),
                ));
            }
        } else if self.channel.is_some() && !self.channel_alive() {
            // The connection is fine but the channel died: drop it so the
            // next `get_channel()` transparently creates a fresh one.
            warn!("AMQP channel is closed, discarding it and creating a new one");
            self.discard_channel();
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
        // Try to connect
        let connection = self.open_connection().await?;

        self.connection = Some(connection);
        self.discard_channel(); // Reset channel and its subscriptions

        // Create channel directly without going through get_channel
        let connection = self
            .connection
            .as_ref()
            .ok_or_else(|| BrokerError::Connection("Not connected".to_string()))?;

        let channel = self.create_configured_channel(connection).await?;

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
                exchange.as_str().into(),
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
                queue.as_str().into(),
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
                queue.as_str().into(),
                exchange.as_str().into(),
                queue.as_str().into(),
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to bind queue: {}", e)))?;

        debug!("Setup topology for queue: {}", queue);
        Ok(())
    }

    /// Get or create channel
    ///
    /// Transparently replaces a channel that the broker closed (a channel
    /// dies on any channel-level exception even though the connection stays
    /// up), so a single failed passive declare cannot poison every later
    /// publish, consume and ack.
    pub(crate) async fn get_channel(&mut self) -> Result<&Channel> {
        // Ensure connection (and channel) health before getting the channel
        self.ensure_connection().await?;

        if !self.channel_alive() {
            self.discard_channel();

            if !self.is_connected() {
                self.connect().await?;
            }

            let connection = self
                .connection
                .as_ref()
                .ok_or_else(|| BrokerError::Connection("Not connected".to_string()))?;

            let channel = self.create_configured_channel(connection).await?;

            self.channel_confirm_mode = self.confirms_enabled();
            self.channel = Some(channel);
        }

        self.channel
            .as_ref()
            .ok_or_else(|| BrokerError::Connection("Channel not available".to_string()))
    }

    /// Acquire a channel to publish on.
    ///
    /// When channel pooling is enabled (and no AMQP transaction is in
    /// progress) this hands out a pooled channel - created from a pooled
    /// connection when connection pooling is enabled too - so that batch
    /// publishing does not serialise behind the broker's primary channel.
    /// The returned flag tells [`Self::release_publish_channel`] whether the
    /// channel belongs to the pool.
    ///
    /// Pooled channels are only ever used for publishing: delivery tags are
    /// channel-scoped, so consuming/acking always stays on the primary
    /// channel.
    pub(crate) async fn acquire_publish_channel(&mut self) -> Result<(Channel, bool)> {
        let pooling_usable =
            self.channel_pool.is_some() && self.transaction_state != TransactionState::Started;

        if !pooling_usable {
            return Ok((self.get_channel().await?.clone(), false));
        }

        // Make sure we have a live connection to create pooled channels from.
        self.get_channel().await?;

        let prefetch = self.config.effective_prefetch_count();
        let prefetch_global = self.config.prefetch_global;
        let confirms = self.confirms_enabled();

        let pooled_connection = match self.connection_pool {
            Some(ref pool) => Some(pool.acquire().await?),
            None => None,
        };

        let result = {
            let connection = match pooled_connection {
                Some(ref connection) => connection,
                None => self
                    .connection
                    .as_ref()
                    .ok_or_else(|| BrokerError::Connection("Not connected".to_string()))?,
            };

            let pool = self
                .channel_pool
                .as_ref()
                .ok_or_else(|| BrokerError::Connection("Channel pool not available".to_string()))?;

            pool.acquire(connection, prefetch, prefetch_global, confirms)
                .await
        };

        // The connection goes straight back to the pool: it stays open, so
        // the channel we just created on it remains valid.
        if let (Some(connection), Some(pool)) = (pooled_connection, self.connection_pool.as_ref()) {
            pool.release(connection).await;
        }

        match result {
            Ok(channel) => Ok((channel, true)),
            Err(e) => {
                warn!("Falling back to the primary channel: {}", e);
                Ok((self.get_channel().await?.clone(), false))
            }
        }
    }

    /// Return a channel obtained from [`Self::acquire_publish_channel`].
    pub(crate) async fn release_publish_channel(&self, channel: Channel, pooled: bool) {
        if !pooled {
            return;
        }
        if let Some(ref pool) = self.channel_pool {
            pool.release(channel).await;
        }
    }

    /// Make sure a `basic.consume` subscription exists for `queue`.
    async fn ensure_consumer(&mut self, queue: &str) -> Result<()> {
        // Refreshes connection/channel health first; a dead channel drops
        // every subscription bound to it.
        self.ensure_connection().await?;

        if self.consumers.contains_key(queue) {
            return Ok(());
        }

        let channel = self.get_channel().await?.clone();
        let consumer = channel
            .basic_consume(
                queue.into(),
                // Empty tag: the broker generates a unique consumer tag.
                "".into(),
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
                BrokerError::OperationFailed(format!(
                    "Failed to start consumer on queue '{}': {}",
                    queue, e
                ))
            })?;

        debug!(
            "Subscribed to queue '{}' with prefetch {}",
            queue,
            self.config.effective_prefetch_count()
        );
        self.consumers.insert(queue.to_string(), consumer);
        Ok(())
    }

    /// Consume through a long-lived `basic.consume` subscription.
    ///
    /// Messages are pushed by the broker (bounded by the configured
    /// prefetch), so a message that arrives right after a failed attempt is
    /// delivered immediately instead of after a full timeout.
    async fn consume_subscribed(
        &mut self,
        queue: &str,
        timeout: Duration,
    ) -> Result<Option<Envelope>> {
        self.ensure_consumer(queue).await?;

        let next = {
            let consumer = self.consumers.get_mut(queue).ok_or_else(|| {
                BrokerError::Connection(format!("No consumer for queue '{}'", queue))
            })?;

            match tokio::time::timeout(timeout, consumer.next()).await {
                Ok(next) => next,
                // Nothing arrived within the timeout.
                Err(_) => return Ok(None),
            }
        };

        match next {
            Some(Ok(delivery)) => match serde_json::from_slice::<Message>(&delivery.data) {
                Ok(message) => {
                    let envelope = Envelope {
                        delivery_tag: delivery.delivery_tag.to_string(),
                        message,
                        redelivered: delivery.redelivered,
                    };
                    self.channel_metrics.messages_consumed += 1;
                    debug!("Consumed message from queue: {}", queue);
                    Ok(Some(envelope))
                }
                Err(e) => {
                    // Never leave a poison message unacked: reject it without
                    // requeue so it goes to the dead-letter exchange (or is
                    // dropped) instead of blocking the prefetch window.
                    let _ = delivery
                        .acker
                        .nack(BasicNackOptions {
                            multiple: false,
                            requeue: false,
                        })
                        .await;
                    self.channel_metrics.consume_errors += 1;
                    self.channel_metrics.messages_rejected += 1;
                    Err(BrokerError::Serialization(e.to_string()))
                }
            },
            Some(Err(e)) => {
                self.channel_metrics.consume_errors += 1;
                self.consumers.remove(queue);
                Err(BrokerError::OperationFailed(format!(
                    "Failed to receive message: {}",
                    e
                )))
            }
            None => {
                // The subscription ended: the channel is gone. Drop it so the
                // next call transparently resubscribes on a fresh channel.
                warn!("Consumer for queue '{}' was cancelled by the broker", queue);
                self.discard_channel();
                Ok(None)
            }
        }
    }

    /// Consume with `basic.get` polling (opt-in via [`AmqpConfig::poll_mode`]).
    async fn consume_polling(
        &mut self,
        queue: &str,
        timeout: Duration,
    ) -> Result<Option<Envelope>> {
        let deadline = tokio::time::Instant::now() + timeout;
        let poll_interval = self.config.poll_interval;

        loop {
            let channel = self.get_channel().await?;
            let get_result = channel
                .basic_get(queue.into(), BasicGetOptions { no_ack: false })
                .await;

            match get_result {
                Ok(Some(delivery)) => {
                    return match serde_json::from_slice::<Message>(&delivery.data) {
                        Ok(message) => {
                            let envelope = Envelope {
                                delivery_tag: delivery.delivery_tag.to_string(),
                                message,
                                redelivered: delivery.redelivered,
                            };
                            self.channel_metrics.messages_consumed += 1;
                            debug!("Consumed message from queue: {}", queue);
                            Ok(Some(envelope))
                        }
                        Err(e) => {
                            let _ = delivery
                                .acker
                                .nack(BasicNackOptions {
                                    multiple: false,
                                    requeue: false,
                                })
                                .await;
                            self.channel_metrics.consume_errors += 1;
                            self.channel_metrics.messages_rejected += 1;
                            Err(BrokerError::Serialization(e.to_string()))
                        }
                    };
                }
                Ok(None) => {
                    let now = tokio::time::Instant::now();
                    if now >= deadline {
                        return Ok(None);
                    }
                    // Poll again shortly instead of burning the whole timeout.
                    tokio::time::sleep((deadline - now).min(poll_interval)).await;
                }
                Err(e) => {
                    self.channel_metrics.consume_errors += 1;
                    return Err(BrokerError::OperationFailed(format!(
                        "Failed to get message: {}",
                        e
                    )));
                }
            }
        }
    }

    /// Connect with retry logic
    pub(crate) async fn connect_with_retry(&mut self) -> Result<()> {
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

            match self.open_connection().await {
                Ok(connection) => {
                    self.connection = Some(connection);
                    self.discard_channel(); // Reset channel
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

    /// Declare the default exchange and bind queue
    pub(crate) async fn setup_topology(&mut self) -> Result<()> {
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
}

#[async_trait]
impl Transport for AmqpBroker {
    async fn connect(&mut self) -> Result<()> {
        info!("Connecting to AMQP broker: {}", self.url);

        // Use retry logic if configured
        if self.config.retry_count > 0 {
            self.connect_with_retry().await?;
        } else {
            let connection = self.open_connection().await?;

            self.connection = Some(connection);
            self.discard_channel(); // Reset channel
        }

        // Setup topology
        self.setup_topology().await?;

        info!("Connected to AMQP broker");
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        // Drop live subscriptions before tearing the channel down
        self.consumers.clear();

        // Close channel pool first
        if let Some(ref channel_pool) = self.channel_pool {
            channel_pool.close_all().await;
        }

        if let Some(channel) = self.channel.take() {
            let _ = channel.close(200, "Disconnecting".into()).await;
        }

        // Close connection pool
        if let Some(ref connection_pool) = self.connection_pool {
            connection_pool.close_all().await;
        }

        if let Some(connection) = self.connection.take() {
            connection
                .close(200, "Disconnecting".into())
                .await
                .map_err(|e| {
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

        // Resolve routing key and exchange from topic router if configured
        let (effective_exchange, effective_routing_key) =
            if let Some(ref router) = self.topic_router {
                let task_name = &message.headers.task;
                let resolved_key = router.resolve_routing_key(task_name).to_string();
                let resolved_exchange = router.exchange_name().to_string();
                debug!(
                    "Topic router resolved task '{}' -> exchange='{}', routing_key='{}'",
                    task_name, resolved_exchange, resolved_key
                );
                (resolved_exchange, resolved_key)
            } else {
                (exchange.to_string(), routing_key.to_string())
            };

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
        let (channel, pooled) = self.acquire_publish_channel().await?;
        let publish_result = channel
            .basic_publish(
                effective_exchange.as_str().into(),
                effective_routing_key.as_str().into(),
                BasicPublishOptions {
                    mandatory: self.config.mandatory_publish,
                    ..Default::default()
                },
                &payload,
                properties,
            )
            .await;

        let confirm_future = match publish_result {
            Ok(confirm_future) => confirm_future,
            Err(e) => {
                self.release_publish_channel(channel, pooled).await;
                self.channel_metrics.publish_errors += 1;
                return Err(BrokerError::OperationFailed(format!(
                    "Failed to publish: {}",
                    e
                )));
            }
        };

        // Now we can update metrics since channel reference is dropped
        self.publisher_confirm_stats.pending_confirms += 1;

        // Wait for the broker's acknowledgement. A `Nack`, a returned
        // (unroutable) message, or a channel that is not in confirm mode are
        // all publish failures - never "successful confirms".
        let confirmation = confirm_future
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to confirm publish: {}", e)));
        self.release_publish_channel(channel, pooled).await;

        let confirms_enabled = self.confirms_enabled();
        match confirmation.and_then(|c| classify_confirmation(c, confirms_enabled)) {
            Ok(()) => {
                // Update metrics
                self.channel_metrics.messages_published += 1;
                self.publisher_confirm_stats.total_confirms += 1;
                self.publisher_confirm_stats.successful_confirms += 1;
                self.publisher_confirm_stats.pending_confirms -= 1;

                // Update latency
                let latency_us = start_time.elapsed().as_micros() as u64;
                let old_avg = self.publisher_confirm_stats.avg_confirm_latency_us;
                let count = self.publisher_confirm_stats.successful_confirms;
                self.publisher_confirm_stats.avg_confirm_latency_us = (old_avg * (count - 1)
                    + latency_us)
                    .checked_div(count)
                    .unwrap_or(latency_us);

                debug!(
                    "Published message to {}/{}",
                    effective_exchange, effective_routing_key
                );
                Ok(())
            }
            Err(e) => {
                self.channel_metrics.publish_errors += 1;
                self.publisher_confirm_stats.total_confirms += 1;
                self.publisher_confirm_stats.failed_confirms += 1;
                self.publisher_confirm_stats.pending_confirms -= 1;
                warn!(
                    "Publish to {}/{} was not confirmed: {}",
                    effective_exchange, effective_routing_key, e
                );
                Err(e)
            }
        }
    }
}

#[async_trait]
impl Consumer for AmqpBroker {
    async fn consume(&mut self, queue: &str, timeout: Duration) -> Result<Option<Envelope>> {
        if self.config.poll_mode {
            return self.consume_polling(queue, timeout).await;
        }
        self.consume_subscribed(queue, timeout).await
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
                queue.into(),
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
            .queue_purge(queue.into(), QueuePurgeOptions::default())
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
            .queue_delete(queue.into(), QueueDeleteOptions::default())
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
