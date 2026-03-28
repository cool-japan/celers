//! Queue and exchange operations, consumer streaming, transactions, and management API methods.

use celers_kombu::{BrokerError, QueueMode, Result};
use celers_protocol::Message;
use lapin::{
    options::*,
    types::{FieldTable, ShortString},
    BasicProperties, ExchangeKind,
};
use tracing::{debug, info};

use crate::broker_core::AmqpBroker;
use crate::management::*;
use crate::types::*;

impl AmqpBroker {
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
                queue.into(),
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

    /// Declare an exchange
    pub async fn declare_exchange(
        &mut self,
        exchange: &str,
        exchange_type: AmqpExchangeType,
    ) -> Result<()> {
        let channel = self.get_channel().await?;

        channel
            .exchange_declare(
                exchange.into(),
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
                exchange.into(),
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
                destination.into(),
                source.into(),
                routing_key.into(),
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
                destination.into(),
                source.into(),
                routing_key.into(),
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
                queue.into(),
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
                exchange.into(),
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
                queue.into(),
                exchange.into(),
                routing_key.into(),
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
            .queue_unbind(
                queue.into(),
                exchange.into(),
                routing_key.into(),
                FieldTable::default(),
            )
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
            .exchange_delete(exchange.into(), ExchangeDeleteOptions::default())
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
        // Resolve routing key and exchange from topic router if configured
        let (effective_exchange, effective_routing_key) =
            if let Some(ref router) = self.topic_router {
                let task_name = &message.headers.task;
                (
                    router.exchange_name().to_string(),
                    router.resolve_routing_key(task_name).to_string(),
                )
            } else {
                (exchange.to_string(), routing_key.to_string())
            };

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
                effective_exchange.as_str().into(),
                effective_routing_key.as_str().into(),
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

        debug!(
            "Published message to {}/{}",
            effective_exchange, effective_routing_key
        );
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
            .map(|c| format!("{:?}", c.status()))
            .unwrap_or_else(|| "Not connected".to_string());

        let channel_open = self
            .channel
            .as_ref()
            .map(|ch| ch.status().connected())
            .unwrap_or(false);

        let channel_state = self
            .channel
            .as_ref()
            .map(|ch| format!("{:?}", ch.status()))
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
                queue.into(),
                consumer_tag.into(),
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
                queue.into(),
                config.consumer_tag.as_str().into(),
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
            .basic_cancel(consumer_tag.into(), BasicCancelOptions::default())
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
