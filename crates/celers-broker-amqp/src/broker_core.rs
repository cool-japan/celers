//! Core AmqpBroker struct, construction, connection management, and trait implementations.

use async_trait::async_trait;
use celers_kombu::{
    Broker, BrokerError, Consumer, Envelope, Producer, QueueMode, Result, Transport,
};
use celers_protocol::Message;
use lapin::{
    options::*,
    types::{FieldTable, ShortString},
    BasicProperties, Channel, Connection, ConnectionProperties,
};
use std::time::Duration;
use tracing::{debug, error, info, warn};

use crate::management::ManagementApiClient;
use crate::pool::{ChannelPool, ConnectionPool, DeduplicationCache};
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
            topic_router: None,
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

    /// Check connection health and attempt auto-reconnection if needed
    pub(crate) async fn ensure_connection(&mut self) -> Result<()> {
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
    pub(crate) async fn get_channel(&mut self) -> Result<&Channel> {
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
    pub(crate) async fn connect_with_retry(&mut self) -> Result<()> {
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
        let confirm_future = {
            let channel = self.get_channel().await?;
            channel
                .basic_publish(
                    effective_exchange.as_str().into(),
                    effective_routing_key.as_str().into(),
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
                Err(BrokerError::OperationFailed(format!(
                    "Failed to confirm publish: {}",
                    e
                )))
            }
        }
    }
}

#[async_trait]
impl Consumer for AmqpBroker {
    async fn consume(&mut self, queue: &str, timeout: Duration) -> Result<Option<Envelope>> {
        let channel = self.get_channel().await?;

        // Use basic_get for polling (compatible with Redis implementation)
        let get_result = channel
            .basic_get(queue.into(), BasicGetOptions { no_ack: false })
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
