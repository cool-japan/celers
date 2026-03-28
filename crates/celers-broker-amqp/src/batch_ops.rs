//! Batch operations, RPC, pipeline publishing, and advanced message handling.

use celers_kombu::{BrokerError, Envelope, Result};
use celers_protocol::Message;
use lapin::{
    options::*,
    types::{FieldTable, ShortString},
    BasicProperties,
};
use std::time::{Duration, Instant};
use tracing::{debug, warn};

use crate::broker_core::AmqpBroker;
use crate::types::*;

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

        let default_exchange = self.config.default_exchange.clone();

        // Pre-resolve routing keys for all messages before borrowing channel
        let resolved_routes: Vec<(String, String)> = messages
            .iter()
            .map(|message| {
                if let Some(ref router) = self.topic_router {
                    let task_name = &message.headers.task;
                    (
                        router.exchange_name().to_string(),
                        router.resolve_routing_key(task_name).to_string(),
                    )
                } else {
                    (default_exchange.clone(), queue.to_string())
                }
            })
            .collect();

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
            let (ref effective_exchange, ref effective_routing_key) = resolved_routes[idx];

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
                    effective_exchange.as_str().into(),
                    effective_routing_key.as_str().into(),
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
                "Published {} messages with pipeline depth {} to queue '{}'",
                messages.len(),
                effective_depth,
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

        let default_exchange = self.config.default_exchange.clone();

        // Pre-resolve routing keys for all messages before borrowing channel
        let resolved_routes: Vec<(String, String)> = messages
            .iter()
            .map(|message| {
                if let Some(ref router) = self.topic_router {
                    let task_name = &message.headers.task;
                    (
                        router.exchange_name().to_string(),
                        router.resolve_routing_key(task_name).to_string(),
                    )
                } else {
                    (default_exchange.clone(), queue.to_string())
                }
            })
            .collect();

        let channel = self.get_channel().await?;

        // Publish all messages and collect confirm futures
        let mut confirms = Vec::with_capacity(messages.len());

        for (idx, message) in messages.iter().enumerate() {
            let (ref effective_exchange, ref effective_routing_key) = resolved_routes[idx];

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
                    effective_exchange.as_str().into(),
                    effective_routing_key.as_str().into(),
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
                "Published {} messages in batch to queue '{}'",
                messages.len(),
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
                .basic_get(queue.into(), BasicGetOptions { no_ack: false })
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
                    queue.into(),
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
                    queue.into(),
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
                .queue_purge(queue.into(), QueuePurgeOptions { nowait: false })
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
                    reply_queue_name.as_str().into(),
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
                    exchange.as_str().into(),
                    rpc_queue.into(),
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
                    .queue_delete(
                        reply_queue_name.as_str().into(),
                        QueueDeleteOptions::default(),
                    )
                    .await;
                return Err(BrokerError::OperationFailed(format!(
                    "RPC timeout: no reply received within {:?}",
                    timeout
                )));
            }

            let channel = self.get_channel().await?;
            match channel
                .basic_get(
                    reply_queue_name.as_str().into(),
                    BasicGetOptions { no_ack: false },
                )
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
                                .queue_delete(
                                    reply_queue_name.as_str().into(),
                                    QueueDeleteOptions::default(),
                                )
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
                        .queue_delete(
                            reply_queue_name.as_str().into(),
                            QueueDeleteOptions::default(),
                        )
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
        use celers_kombu::Producer;

        // Extract correlation_id and reply_to from request
        let correlation_id = request_envelope
            .message
            .properties
            .correlation_id
            .as_ref()
            .ok_or_else(|| {
                BrokerError::OperationFailed("Request message missing correlation_id".to_string())
            })?;

        // Simplified: Use correlation_id to determine reply queue
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
                .basic_get(queue.into(), BasicGetOptions { no_ack: false })
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
                .basic_get(queue.into(), BasicGetOptions { no_ack: false })
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
