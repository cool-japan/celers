//! Batch operations, RPC, pipeline publishing, and advanced message handling.

use celers_kombu::{BrokerError, Envelope, Result};
use celers_protocol::Message;
use lapin::{
    options::*,
    types::{FieldTable, ShortString},
    BasicProperties,
};
use std::collections::HashSet;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

use crate::broker_core::AmqpBroker;
use crate::confirm::classify_confirmation;
use crate::types::*;

// Additional AmqpBroker methods
impl AmqpBroker {
    /// Upper bound on the number of messages [`AmqpBroker::drain_queue`]
    /// fetches before giving up, so a queue that is being written to
    /// concurrently cannot spin the drain loop forever.
    pub const DEFAULT_DRAIN_LIMIT: usize = 100_000;

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
    /// ```no_run
    /// # use celers_broker_amqp::AmqpBroker;
    /// # use celers_protocol::Message;
    /// # async fn example(mut broker: AmqpBroker, messages: Vec<Message>) -> Result<(), Box<dyn std::error::Error>> {
    /// // Publish with pipeline depth of 100 (send 100 messages before waiting for confirms)
    /// let _count = broker.publish_pipeline("my_queue", messages, 100).await?;
    /// # Ok(())
    /// # }
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

        let publish_options = BasicPublishOptions {
            mandatory: self.config.mandatory_publish,
            ..Default::default()
        };
        let publish_channel = self.acquire_publish_channel().await?;
        let confirms_enabled = publish_channel.confirms;
        let channel = publish_channel.channel.clone();

        let effective_depth = if pipeline_depth == 0 {
            messages.len() // Unlimited - send all before waiting
        } else {
            pipeline_depth.min(messages.len())
        };

        let mut confirms = Vec::with_capacity(effective_depth);
        let mut success_count: usize = 0;
        let mut publish_count: usize = 0;
        let mut first_error: Option<BrokerError> = None;

        let mut fatal_error: Option<BrokerError> = None;

        'publish: for (idx, message) in messages.iter().enumerate() {
            let (ref effective_exchange, ref effective_routing_key) = resolved_routes[idx];

            // Serialize message to JSON
            let payload = match serde_json::to_vec(message) {
                Ok(payload) => payload,
                Err(e) => {
                    fatal_error = Some(BrokerError::Serialization(e.to_string()));
                    break 'publish;
                }
            };

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
            let confirm = match channel
                .basic_publish(
                    effective_exchange.as_str().into(),
                    effective_routing_key.as_str().into(),
                    publish_options,
                    &payload,
                    properties,
                )
                .await
            {
                Ok(confirm) => confirm,
                Err(e) => {
                    fatal_error = Some(BrokerError::OperationFailed(format!(
                        "Failed to publish: {}",
                        e
                    )));
                    break 'publish;
                }
            };

            confirms.push(confirm);
            publish_count += 1;

            // Wait for confirms when pipeline is full or at the end
            if confirms.len() >= effective_depth || idx == messages.len() - 1 {
                for confirm in confirms.drain(..) {
                    let outcome = match confirm.await {
                        Ok(confirmation) => classify_confirmation(confirmation, confirms_enabled),
                        Err(e) => Err(BrokerError::OperationFailed(format!(
                            "Failed to confirm publish: {}",
                            e
                        ))),
                    };
                    match outcome {
                        Ok(()) => success_count += 1,
                        Err(e) => {
                            if first_error.is_none() {
                                first_error = Some(e);
                            }
                        }
                    }
                }
            }
        }

        // Never abandon confirms that were already issued.
        for confirm in confirms.drain(..) {
            let outcome = match confirm.await {
                Ok(confirmation) => classify_confirmation(confirmation, confirms_enabled),
                Err(e) => Err(BrokerError::OperationFailed(format!(
                    "Failed to confirm publish: {}",
                    e
                ))),
            };
            match outcome {
                Ok(()) => success_count += 1,
                Err(e) => {
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }

        drop(channel);
        self.release_publish_channel(publish_channel).await;

        if let Some(e) = fatal_error {
            self.channel_metrics.publish_errors += (messages.len() - success_count) as u64;
            self.publisher_confirm_stats.total_confirms += publish_count as u64;
            self.publisher_confirm_stats.successful_confirms += success_count as u64;
            self.publisher_confirm_stats.failed_confirms += (publish_count - success_count) as u64;
            return Err(e);
        }

        // Update metrics
        self.channel_metrics.messages_published += success_count as u64;
        self.publisher_confirm_stats.total_confirms += publish_count as u64;
        self.publisher_confirm_stats.successful_confirms += success_count as u64;

        if success_count < messages.len() {
            self.channel_metrics.publish_errors += (messages.len() - success_count) as u64;
            self.publisher_confirm_stats.failed_confirms += (messages.len() - success_count) as u64;
            warn!(
                "Pipeline publish: {} of {} messages confirmed{}",
                success_count,
                messages.len(),
                first_error
                    .map(|e| format!(" (first failure: {})", e))
                    .unwrap_or_default()
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

        let publish_options = BasicPublishOptions {
            mandatory: self.config.mandatory_publish,
            ..Default::default()
        };
        let publish_channel = self.acquire_publish_channel().await?;
        let confirms_enabled = publish_channel.confirms;
        let channel = publish_channel.channel.clone();

        // Publish all messages and collect confirm futures
        let mut confirms = Vec::with_capacity(messages.len());
        let mut fatal_error: Option<BrokerError> = None;

        'publish: for (idx, message) in messages.iter().enumerate() {
            let (ref effective_exchange, ref effective_routing_key) = resolved_routes[idx];

            // Serialize message to JSON
            let payload = match serde_json::to_vec(message) {
                Ok(payload) => payload,
                Err(e) => {
                    fatal_error = Some(BrokerError::Serialization(e.to_string()));
                    break 'publish;
                }
            };

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
            let confirm = match channel
                .basic_publish(
                    effective_exchange.as_str().into(),
                    effective_routing_key.as_str().into(),
                    publish_options,
                    &payload,
                    properties,
                )
                .await
            {
                Ok(confirm) => confirm,
                Err(e) => {
                    fatal_error = Some(BrokerError::OperationFailed(format!(
                        "Failed to publish: {}",
                        e
                    )));
                    break 'publish;
                }
            };

            confirms.push(confirm);
        }

        // Wait for all publisher confirms. A negative acknowledgement or an
        // unroutable (returned) message is a failure, not a success.
        let mut success_count = 0;
        let mut first_error: Option<BrokerError> = None;
        for confirm in confirms {
            let outcome = match confirm.await {
                Ok(confirmation) => classify_confirmation(confirmation, confirms_enabled),
                Err(e) => Err(BrokerError::OperationFailed(format!(
                    "Failed to confirm publish: {}",
                    e
                ))),
            };
            match outcome {
                Ok(()) => success_count += 1,
                Err(e) => {
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }

        drop(channel);
        self.release_publish_channel(publish_channel).await;

        self.channel_metrics.messages_published += success_count as u64;
        self.publisher_confirm_stats.total_confirms += messages.len() as u64;
        self.publisher_confirm_stats.successful_confirms += success_count as u64;
        if success_count < messages.len() {
            self.channel_metrics.publish_errors += (messages.len() - success_count) as u64;
            self.publisher_confirm_stats.failed_confirms += (messages.len() - success_count) as u64;
        }

        if let Some(e) = fatal_error {
            return Err(e);
        }

        if success_count < messages.len() {
            warn!(
                "Batch publish: {} of {} messages confirmed{}",
                success_count,
                messages.len(),
                first_error
                    .map(|e| format!(" (first failure: {})", e))
                    .unwrap_or_default()
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
    /// ```no_run
    /// # use celers_broker_amqp::AmqpBroker;
    /// # async fn example(mut broker: AmqpBroker) -> Result<(), Box<dyn std::error::Error>> {
    /// let messages = broker.drain_queue("my_queue").await?;
    /// println!("Drained {} messages", messages.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn drain_queue(&mut self, queue: &str) -> Result<Vec<Envelope>> {
        self.drain_queue_limited(queue, Self::DEFAULT_DRAIN_LIMIT)
            .await
    }

    /// Drain at most `max_messages` messages from a queue.
    ///
    /// Unlike [`Self::drain_queue`] the caller chooses the cap, which matters
    /// for a queue that is being written to concurrently: without a cap the
    /// drain can never finish.
    ///
    /// Messages that fail to deserialize are rejected without requeue (so
    /// they are dead-lettered rather than left unacknowledged forever) and
    /// counted in `consume_errors`.
    pub async fn drain_queue_limited(
        &mut self,
        queue: &str,
        max_messages: usize,
    ) -> Result<Vec<Envelope>> {
        let mut envelopes = Vec::new();
        let mut consumed_count = 0;
        let mut error_count = 0;

        for _ in 0..max_messages {
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
                        // Do not leave the poison message unacknowledged.
                        let _ = delivery
                            .acker
                            .nack(BasicNackOptions {
                                multiple: false,
                                requeue: false,
                            })
                            .await;
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
    /// ```no_run
    /// # use celers_broker_amqp::{AmqpBroker, QueueConfig};
    /// # async fn example(mut broker: AmqpBroker) -> Result<(), Box<dyn std::error::Error>> {
    /// let configs = vec![
    ///     ("queue1", QueueConfig::default()),
    ///     ("queue2", QueueConfig::default().with_max_priority(10)),
    /// ];
    /// broker.declare_queues_batch(configs).await?;
    /// # Ok(())
    /// # }
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

    /// Name of the temporary reply queue used for the RPC exchange identified
    /// by `correlation_id`.
    ///
    /// The reply queue name is derived from the correlation id so that the
    /// two can never disagree; the authoritative reply address is still
    /// carried by the request's `reply_to` property.
    pub(crate) fn reply_queue_for(correlation_id: &str) -> String {
        format!("reply.{}", correlation_id)
    }

    /// Resolve where an RPC reply must be published, from the request envelope.
    ///
    /// AMQP semantics: `reply_to` is the reply address and `correlation_id`
    /// is only the matching token, so the destination is read from
    /// `reply_to` and never reconstructed.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::OperationFailed`] when the request carries no
    /// `correlation_id` or no `reply_to`.
    pub(crate) fn rpc_reply_target(request_envelope: &Envelope) -> Result<(String, String)> {
        let correlation_id = request_envelope
            .message
            .properties
            .correlation_id
            .clone()
            .ok_or_else(|| {
                BrokerError::OperationFailed("Request message missing correlation_id".to_string())
            })?;

        let reply_to = request_envelope
            .message
            .properties
            .reply_to
            .clone()
            .ok_or_else(|| {
                BrokerError::OperationFailed(
                    "Request message missing reply_to; cannot route the RPC reply".to_string(),
                )
            })?;

        Ok((correlation_id, reply_to))
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
    /// ```no_run
    /// # use celers_broker_amqp::AmqpBroker;
    /// use std::time::Duration;
    /// use celers_protocol::builder::MessageBuilder;
    ///
    /// # async fn example(mut broker: AmqpBroker) -> Result<(), Box<dyn std::error::Error>> {
    /// let request = MessageBuilder::new("tasks.calculate")
    ///     .args(vec![serde_json::json!({"x": 10, "y": 20})])
    ///     .build()?;
    ///
    /// let reply = broker.rpc_call("rpc_queue", request, Duration::from_secs(5)).await?;
    /// println!("RPC result: {:?}", reply);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn rpc_call(
        &mut self,
        rpc_queue: &str,
        mut request: Message,
        timeout: Duration,
    ) -> Result<Message> {
        // The reply queue name and the correlation id must agree, so derive
        // one from the other instead of drawing two unrelated UUIDs.
        let correlation_id = uuid::Uuid::new_v4().to_string();
        let reply_queue_name = Self::reply_queue_for(&correlation_id);
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

        // Set reply-to and correlation-id, both on the AMQP properties and on
        // the serialized message body: the RPC server deserializes the body,
        // so a `reply_to` that only exists in the AMQP frame is invisible to it.
        request.properties.correlation_id = Some(correlation_id.clone());
        request.properties.reply_to = Some(reply_queue_name.clone());
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
            let channel = self.get_channel().await?.clone();
            let confirms_enabled = self.channel_confirm_mode;
            let confirmation = channel
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
            classify_confirmation(confirmation, confirms_enabled)?;
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
                    let matches = delivery
                        .properties
                        .correlation_id()
                        .as_ref()
                        .map(|corr_id| corr_id.as_str() == correlation_id)
                        .unwrap_or(false);

                    if !matches {
                        // A stale or foreign reply on our private queue: it can
                        // never become the answer we are waiting for, so drop it
                        // instead of leaving it unacknowledged forever. Requeuing
                        // it would put it straight back at the head of this queue
                        // and spin the loop.
                        warn!(
                            "Discarding RPC reply with unexpected correlation_id on queue '{}'",
                            reply_queue_name
                        );
                        let _ = delivery
                            .acker
                            .nack(BasicNackOptions {
                                multiple: false,
                                requeue: false,
                            })
                            .await;
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }

                    // Acknowledge the reply
                    channel
                        .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                        .await
                        .map_err(|e| {
                            BrokerError::OperationFailed(format!("Failed to ack reply: {}", e))
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
    /// ```no_run
    /// # use celers_broker_amqp::AmqpBroker;
    /// use std::time::Duration;
    /// use celers_kombu::Consumer;
    /// use celers_protocol::builder::MessageBuilder;
    ///
    /// # async fn example(mut broker: AmqpBroker) -> Result<(), Box<dyn std::error::Error>> {
    /// // In RPC server
    /// if let Some(envelope) = broker.consume("rpc_queue", Duration::from_secs(1)).await? {
    ///     // Process request and create reply
    ///     let reply = MessageBuilder::new("result")
    ///         .args(vec![serde_json::json!({"result": 42})])
    ///         .build()?;
    ///
    ///     broker.rpc_reply(&envelope, reply).await?;
    ///     broker.ack(&envelope.delivery_tag).await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn rpc_reply(&mut self, request_envelope: &Envelope, reply: Message) -> Result<()> {
        // The reply address is whatever the requester asked for; it is never
        // reconstructed from the correlation id.
        let (correlation_id, reply_queue) = Self::rpc_reply_target(request_envelope)?;

        // Publish reply with correlation_id
        let mut reply_msg = reply;
        reply_msg.properties.correlation_id = Some(correlation_id.clone());

        let payload = serde_json::to_vec(&reply_msg)
            .map_err(|e| BrokerError::Serialization(e.to_string()))?;

        let properties = BasicProperties::default()
            .with_delivery_mode(2)
            .with_content_type(ShortString::from("application/json"))
            .with_content_encoding(ShortString::from("utf-8"))
            .with_correlation_id(ShortString::from(correlation_id.as_str()));

        let channel = self.get_channel().await?.clone();
        let confirms_enabled = self.channel_confirm_mode;

        // The reply queue is a temporary queue with no binding, so the reply
        // goes to the default exchange with the queue name as routing key.
        let confirmation = channel
            .basic_publish(
                "".into(),
                reply_queue.as_str().into(),
                BasicPublishOptions::default(),
                &payload,
                properties,
            )
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to publish RPC reply: {}", e))
            })?
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to confirm RPC reply: {}", e))
            })?;

        classify_confirmation(confirmation, confirms_enabled)?;

        self.channel_metrics.messages_published += 1;

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
    /// ```no_run
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
    /// ```no_run
    /// use celers_broker_amqp::AmqpBroker;
    /// use celers_kombu::Transport;
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
    /// ```no_run
    /// # use celers_broker_amqp::AmqpBroker;
    /// use celers_kombu::Consumer;
    /// use std::time::Duration;
    ///
    /// # async fn example(mut broker: AmqpBroker) -> Result<(), Box<dyn std::error::Error>> {
    /// let envelopes = broker.consume_batch("my_queue", 100, Duration::from_secs(1)).await?;
    /// for envelope in envelopes {
    ///     // Process message
    ///     broker.ack(&envelope.delivery_tag).await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn consume_batch(
        &mut self,
        queue: &str,
        max_messages: usize,
        timeout: Duration,
    ) -> Result<Vec<Envelope>> {
        let max_messages = max_messages.min(1000); // Cap at 1000
        let mut envelopes = Vec::with_capacity(max_messages);
        // Every delivery fetched in this call, so none can be abandoned in the
        // unacknowledged state if the batch aborts halfway through.
        let mut in_flight_tags: Vec<u64> = Vec::with_capacity(max_messages);

        let start = Instant::now();
        let mut consumed_count = 0u64;

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
                        in_flight_tags.push(delivery.delivery_tag);
                        envelopes.push(Envelope {
                            delivery_tag: delivery.delivery_tag.to_string(),
                            message,
                            redelivered: delivery.redelivered,
                        });
                        consumed_count += 1;
                    }
                    Err(e) => {
                        // Requeue everything fetched so far, then reject the
                        // poison message without requeue so it is dead-lettered
                        // instead of poisoning the next batch too.
                        self.requeue_tags(&in_flight_tags).await;
                        let _ = delivery
                            .acker
                            .nack(BasicNackOptions {
                                multiple: false,
                                requeue: false,
                            })
                            .await;
                        self.channel_metrics.consume_errors += 1;
                        self.channel_metrics.messages_requeued += in_flight_tags.len() as u64;
                        self.channel_metrics.messages_rejected += 1;
                        return Err(BrokerError::Serialization(e.to_string()));
                    }
                },
                Ok(None) => {
                    // No more messages available
                    break;
                }
                Err(e) => {
                    self.requeue_tags(&in_flight_tags).await;
                    self.channel_metrics.consume_errors += 1;
                    self.channel_metrics.messages_requeued += in_flight_tags.len() as u64;
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

    /// Requeue the given deliveries, one by one.
    ///
    /// `BasicNackOptions { multiple: true, .. }` would nack *every*
    /// unacknowledged delivery up to that tag on the channel - including
    /// deliveries belonging to unrelated consumers sharing the broker's
    /// channel - so each tag is nacked individually.
    async fn requeue_tags(&mut self, tags: &[u64]) {
        if tags.is_empty() {
            return;
        }

        let channel = match self.get_channel().await {
            Ok(channel) => channel,
            Err(e) => {
                warn!("Cannot requeue {} deliveries: {}", tags.len(), e);
                return;
            }
        };

        for tag in tags {
            if let Err(e) = channel
                .basic_nack(
                    *tag,
                    BasicNackOptions {
                        multiple: false,
                        requeue: true,
                    },
                )
                .await
            {
                warn!("Failed to requeue delivery {}: {}", tag, e);
            }
        }
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
    /// ```no_run
    /// # use celers_broker_amqp::AmqpBroker;
    /// # async fn example(mut broker: AmqpBroker) -> Result<(), Box<dyn std::error::Error>> {
    /// let messages = broker.peek_queue("my_queue", 10).await?;
    /// for msg in messages {
    ///     println!("Message ID: {}", msg.headers.id);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn peek_queue(&mut self, queue: &str, max_messages: usize) -> Result<Vec<Message>> {
        let max_messages = max_messages.min(100); // Cap at 100 for safety
        let mut messages = Vec::with_capacity(max_messages);
        let mut seen_ids: HashSet<String> = HashSet::with_capacity(max_messages);
        // Every delivery is held unacknowledged until the whole peek is done,
        // so the broker cannot hand the same message back on the next
        // `basic_get`. Rejecting immediately would put the message straight
        // back at the head of the queue and the loop would keep re-reading it.
        let mut in_flight_tags: Vec<u64> = Vec::with_capacity(max_messages);
        let mut error: Option<BrokerError> = None;

        {
            let channel = self.get_channel().await?;

            'peek: for _ in 0..max_messages {
                let get_result = channel
                    .basic_get(queue.into(), BasicGetOptions { no_ack: false })
                    .await;

                match get_result {
                    Ok(Some(delivery)) => {
                        in_flight_tags.push(delivery.delivery_tag);

                        match serde_json::from_slice::<Message>(&delivery.data) {
                            Ok(message) => {
                                // Defensive de-duplication: if the broker does
                                // hand back a message we already hold, stop
                                // rather than reporting it twice.
                                if !seen_ids.insert(message.headers.id.to_string()) {
                                    break 'peek;
                                }
                                messages.push(message);
                            }
                            Err(e) => {
                                error = Some(BrokerError::Serialization(e.to_string()));
                                break 'peek;
                            }
                        }
                    }
                    Ok(None) => {
                        // No more messages
                        break 'peek;
                    }
                    Err(e) => {
                        error = Some(BrokerError::OperationFailed(format!(
                            "Failed to peek message: {}",
                            e
                        )));
                        break 'peek;
                    }
                }
            }
        }

        // Put everything back exactly once, now that nothing else will be read.
        let requeued = in_flight_tags.len() as u64;
        self.requeue_tags(&in_flight_tags).await;
        self.channel_metrics.messages_requeued += requeued;

        if let Some(e) = error {
            return Err(e);
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
    /// ```no_run
    /// # use celers_broker_amqp::AmqpBroker;
    /// # async fn example(broker: AmqpBroker) -> Result<(), Box<dyn std::error::Error>> {
    /// if broker.check_aliveness(None).await? {
    ///     println!("RabbitMQ is alive!");
    /// }
    /// # Ok(())
    /// # }
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
            .and_then(|b| b.basic_auth(&mgmt_api.username, Some(&mgmt_api.password)))
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to check aliveness: {}", e)))?
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

        let aliveness: AlivenessResponse = response.body_json().await.map_err(|e| {
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
    /// ```no_run
    /// # use celers_broker_amqp::AmqpBroker;
    /// # async fn example(broker: AmqpBroker) {
    /// if let Some(metrics) = broker.get_connection_pool_metrics().await {
    ///     println!("Pool utilization: {:.2}%", metrics.utilization() * 100.0);
    /// }
    /// # }
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
    /// ```no_run
    /// # use celers_broker_amqp::AmqpBroker;
    /// # async fn example(broker: AmqpBroker) {
    /// if let Some(metrics) = broker.get_channel_pool_metrics().await {
    ///     println!("Pool utilization: {:.2}%", metrics.utilization() * 100.0);
    /// }
    /// # }
    /// ```
    pub async fn get_channel_pool_metrics(&self) -> Option<ChannelPoolMetrics> {
        match &self.channel_pool {
            Some(pool) => Some(pool.get_metrics().await),
            None => None,
        }
    }
}
