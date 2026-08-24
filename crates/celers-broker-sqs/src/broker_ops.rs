//! Additional SQS broker operations and trait implementations
//!
//! Contains batch operations, FIFO queue support, DLQ management,
//! queue management utilities, and trait implementations for
//! Transport, Producer, Consumer, and Broker.

use async_trait::async_trait;
use aws_sdk_sqs::types::{MessageSystemAttributeName, QueueAttributeName};
use celers_kombu::{
    Broker, BrokerError, Consumer, Envelope, Producer, QueueMode, Result, Transport,
};
use celers_protocol::Message;
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, info, warn};

use crate::broker_core::SqsBroker;
use crate::delivery::{
    decode_delivery_tag, encode_delivery_tag, extract_receipt_metadata, resolve_max_messages,
    resolve_wait_time,
};
use crate::retry_policy::describe_error;
use crate::types::QueueStats;

impl SqsBroker {
    /// Receive messages from a physical queue and turn them into envelopes.
    ///
    /// This is the single entry point shared by `consume` and `consume_batch`
    /// so that both request the same attributes and produce identically shaped
    /// delivery tags.
    ///
    /// `MessageSystemAttributeNames` is requested explicitly: without it SQS
    /// omits `ApproximateReceiveCount` and `SentTimestamp` from the response,
    /// which is why `Envelope::redelivered` used to be permanently `false`.
    pub(crate) async fn receive_envelopes(
        &mut self,
        physical_queue: &str,
        max_messages: i32,
        wait_time: i32,
    ) -> Result<Vec<Envelope>> {
        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(physical_queue).await?;

        let result = client
            .receive_message()
            .queue_url(&queue_url)
            .max_number_of_messages(resolve_max_messages(max_messages))
            .visibility_timeout(self.visibility_timeout)
            .wait_time_seconds(wait_time.clamp(0, 20))
            .message_attribute_names("All")
            .message_system_attribute_names(MessageSystemAttributeName::All)
            .send()
            .await
            .map_err(|e| {
                self.mark_unhealthy();
                BrokerError::Connection(format!(
                    "Failed to receive messages from '{}': {}",
                    physical_queue,
                    describe_error(&e)
                ))
            })?;

        self.mark_healthy();

        let mut envelopes = Vec::new();

        for sqs_message in result.messages.unwrap_or_default() {
            let body = sqs_message
                .body()
                .ok_or_else(|| BrokerError::OperationFailed("Message has no body".to_string()))?;

            let receipt_handle = sqs_message
                .receipt_handle()
                .ok_or_else(|| {
                    BrokerError::OperationFailed("Message has no receipt handle".to_string())
                })?
                .to_string();

            let decompressed_body = self.decompress_message(body)?;
            let mut message: Message = serde_json::from_str(&decompressed_body)
                .map_err(|e| BrokerError::Serialization(e.to_string()))?;

            // Celery compatibility: recover headers that only exist as SQS
            // message attributes (a Python producer may not repeat them in the
            // body). Gaps are filled, never overwritten, so a CeleRS-produced
            // body stays authoritative.
            if let (Some(mapper), Some(attributes)) = (
                self.celery_mapper.as_ref(),
                sqs_message.message_attributes(),
            ) {
                match mapper.deserialize_attributes(attributes) {
                    Ok(headers) => mapper.apply_headers(&headers, &mut message),
                    Err(error) => {
                        warn!("Ignoring unreadable Celery attributes: {}", error);
                    }
                }
            }

            let metadata = extract_receipt_metadata(&sqs_message);
            let delivery_tag = encode_delivery_tag(physical_queue, &receipt_handle);

            self.maybe_start_heartbeat(&client, &queue_url, &delivery_tag, &receipt_handle);
            let redelivered = metadata.is_redelivered();
            self.remember_receipt_metadata(&delivery_tag, metadata);

            envelopes.push(Envelope {
                delivery_tag,
                message,
                redelivered,
            });
        }

        Ok(envelopes)
    }

    /// Consume multiple messages in a single batch (up to 10 messages)
    ///
    /// More efficient than polling one message at a time.
    ///
    /// Each returned [`Envelope::delivery_tag`] carries the queue the message
    /// was received from, so the envelopes can be acknowledged with
    /// [`ack`](celers_kombu::Consumer::ack) even when they came from a queue
    /// other than the broker's configured one.
    ///
    /// # Arguments
    /// * `queue` - Queue name to consume from
    /// * `max_messages` - Maximum number of messages to receive (max 10)
    /// * `timeout` - Long polling wait time (capped by `with_wait_time` and by
    ///   SQS's own 20 second maximum)
    ///
    /// # Returns
    /// Vector of envelopes
    pub async fn consume_batch(
        &mut self,
        queue: &str,
        max_messages: i32,
        timeout: Duration,
    ) -> Result<Vec<Envelope>> {
        let physical_queue = self.resolve_queue_name(queue);
        let wait_time = resolve_wait_time(timeout, self.wait_time_seconds);

        let envelopes = self
            .consume_batch_physical(&physical_queue, max_messages, wait_time)
            .await?;

        debug!(
            "Consumed {} messages in batch from SQS queue: {}",
            envelopes.len(),
            physical_queue
        );
        Ok(envelopes)
    }

    /// Batch consume from an already-resolved (physical) queue name.
    ///
    /// Anything left in the prefetch buffer for that queue is drained first:
    /// buffered messages are already in flight, so serving them here is what
    /// keeps a caller that mixes `consume` and `consume_batch` from letting
    /// them expire and be redelivered.
    pub(crate) async fn consume_batch_physical(
        &mut self,
        physical_queue: &str,
        max_messages: i32,
        wait_time: i32,
    ) -> Result<Vec<Envelope>> {
        let wanted = resolve_max_messages(max_messages) as usize;
        let mut envelopes = Vec::with_capacity(wanted);

        while envelopes.len() < wanted {
            match self.take_prefetched(physical_queue) {
                Some(envelope) => envelopes.push(envelope),
                None => break,
            }
        }

        if envelopes.len() >= wanted {
            return Ok(envelopes);
        }

        let still_wanted = (wanted - envelopes.len()) as i32;
        let fetched = self
            .receive_envelopes(physical_queue, still_wanted, wait_time)
            .await?;
        envelopes.extend(fetched);

        Ok(envelopes)
    }

    /// Publish a message to a FIFO queue
    ///
    /// FIFO queues require a message group ID for ordering guarantees.
    ///
    /// # Arguments
    /// * `queue` - Queue name (must end with ".fifo")
    /// * `message` - The message to publish
    /// * `message_group_id` - Required for FIFO ordering
    /// * `deduplication_id` - Optional; when omitted the task id is used unless
    ///   the queue has content-based deduplication enabled. A *stable*
    ///   deduplication id makes a retried send idempotent, which a freshly
    ///   generated UUID could never be.
    pub async fn publish_fifo(
        &mut self,
        queue: &str,
        message: Message,
        message_group_id: &str,
        deduplication_id: Option<&str>,
    ) -> Result<()> {
        let physical_queue = self.resolve_queue_name(queue);
        if !physical_queue.ends_with(".fifo") {
            return Err(BrokerError::OperationFailed(
                "FIFO queue name must end with '.fifo'".to_string(),
            ));
        }

        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(&physical_queue).await?;

        let body = self.encode_body(&message)?;
        let attributes = self.build_attributes(&message)?;
        let group_id = crate::fifo::sanitize_fifo_id(message_group_id);

        let content_based_dedup = self
            .fifo_config
            .as_ref()
            .is_some_and(|c| c.content_based_deduplication);
        let dedup_id =
            crate::fifo::derive_deduplication_id(content_based_dedup, deduplication_id, &message);

        let send_operation = || async {
            let mut request = client
                .send_message()
                .queue_url(&queue_url)
                .message_body(&body)
                .message_group_id(&group_id);

            if let Some(ref dedup) = dedup_id {
                request = request.message_deduplication_id(dedup);
            }

            if !attributes.is_empty() {
                request = request.set_message_attributes(Some(attributes.clone()));
            }

            request.send().await.map_err(|e| {
                BrokerError::OperationFailed(format!(
                    "Failed to send FIFO message: {}",
                    describe_error(&e)
                ))
            })
        };

        let result = self.retry_with_backoff(send_operation).await;
        self.record_call_result(&result);
        result?;

        debug!(
            "Published FIFO message to queue: {} (group: {})",
            physical_queue, group_id
        );
        Ok(())
    }

    /// Get detailed queue statistics and monitoring data
    ///
    /// Returns approximate counts for messages and other queue attributes.
    pub async fn get_queue_stats(&mut self, queue: &str) -> Result<QueueStats> {
        let physical_queue = self.resolve_queue_name(queue);
        self.queue_stats_for_physical(&physical_queue).await
    }

    /// Queue statistics for an already-resolved (physical) queue name.
    pub(crate) async fn queue_stats_for_physical(&mut self, queue: &str) -> Result<QueueStats> {
        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        let result = client
            .get_queue_attributes()
            .queue_url(&queue_url)
            .attribute_names(QueueAttributeName::All)
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to get queue attributes: {}", e))
            })?;

        let attrs = result.attributes();

        let parse_u64 = |name: QueueAttributeName| -> Option<u64> {
            attrs
                .and_then(|a| a.get(&name))
                .and_then(|v| v.parse().ok())
        };

        let parse_bool = |name: QueueAttributeName| -> bool {
            attrs
                .and_then(|a| a.get(&name))
                .map(|v| v == "true")
                .unwrap_or(false)
        };

        // Parse age of oldest message from raw attributes map
        // AWS SQS returns this but aws-sdk-sqs may not have the enum variant yet
        let age_of_oldest = attrs.and_then(|a| {
            a.iter()
                .find(|(k, _)| k.as_str() == "ApproximateAgeOfOldestMessage")
                .and_then(|(_, v)| v.parse::<u64>().ok())
        });

        Ok(QueueStats {
            approximate_message_count: parse_u64(QueueAttributeName::ApproximateNumberOfMessages)
                .unwrap_or(0),
            approximate_not_visible_count: parse_u64(
                QueueAttributeName::ApproximateNumberOfMessagesNotVisible,
            )
            .unwrap_or(0),
            approximate_delayed_count: parse_u64(
                QueueAttributeName::ApproximateNumberOfMessagesDelayed,
            )
            .unwrap_or(0),
            approximate_age_of_oldest_message: age_of_oldest,
            created_timestamp: parse_u64(QueueAttributeName::CreatedTimestamp),
            last_modified_timestamp: parse_u64(QueueAttributeName::LastModifiedTimestamp),
            message_retention_period: parse_u64(QueueAttributeName::MessageRetentionPeriod),
            visibility_timeout: parse_u64(QueueAttributeName::VisibilityTimeout),
            is_fifo: parse_bool(QueueAttributeName::FifoQueue),
        })
    }

    /// Extend visibility timeout for a message
    ///
    /// Use this when processing takes longer than expected to prevent
    /// the message from becoming visible to other consumers. For long running
    /// handlers prefer
    /// [`with_visibility_heartbeat`](Self::with_visibility_heartbeat), which
    /// does this automatically for the lifetime of the message.
    ///
    /// # Arguments
    /// * `delivery_tag` - Delivery tag of the message (the queue it was
    ///   received from is taken from the tag)
    /// * `timeout_seconds` - New visibility timeout (0-43200 seconds)
    pub async fn extend_visibility(
        &mut self,
        delivery_tag: &str,
        timeout_seconds: i32,
    ) -> Result<()> {
        let (source, receipt_handle) = decode_delivery_tag(delivery_tag);
        let queue_name = source
            .map(str::to_string)
            .unwrap_or_else(|| self.resolve_queue_name(&self.queue_name.clone()));

        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(&queue_name).await?;

        client
            .change_message_visibility()
            .queue_url(queue_url)
            .receipt_handle(receipt_handle)
            .visibility_timeout(timeout_seconds.clamp(0, 43200))
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!(
                    "Failed to extend visibility on '{}': {}",
                    queue_name,
                    describe_error(&e)
                ))
            })?;

        debug!(
            "Extended visibility timeout to {} seconds for message on {}",
            timeout_seconds, queue_name
        );
        Ok(())
    }

    /// Get the ARN of a queue
    pub async fn get_queue_arn(&mut self, queue: &str) -> Result<String> {
        let queue = &self.resolve_queue_name(queue);
        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        let result = client
            .get_queue_attributes()
            .queue_url(&queue_url)
            .attribute_names(QueueAttributeName::QueueArn)
            .send()
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to get queue ARN: {}", e)))?;

        result
            .attributes()
            .and_then(|a| a.get(&QueueAttributeName::QueueArn))
            .map(|s| s.to_string())
            .ok_or_else(|| BrokerError::OperationFailed("Queue ARN not found".to_string()))
    }

    /// Configure redrive policy (Dead Letter Queue) for an existing queue
    ///
    /// # Arguments
    /// * `queue` - The source queue name
    /// * `dlq_arn` - ARN of the dead letter queue
    /// * `max_receive_count` - Number of receives before moving to DLQ
    pub async fn set_redrive_policy(
        &mut self,
        queue: &str,
        dlq_arn: &str,
        max_receive_count: i32,
    ) -> Result<()> {
        let queue = &self.resolve_queue_name(queue);
        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        let redrive_policy = serde_json::json!({
            "deadLetterTargetArn": dlq_arn,
            "maxReceiveCount": max_receive_count.clamp(1, 1000).to_string()
        })
        .to_string();

        client
            .set_queue_attributes()
            .queue_url(&queue_url)
            .attributes(QueueAttributeName::RedrivePolicy, redrive_policy)
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to set redrive policy: {}", e))
            })?;

        info!(
            "Set redrive policy for queue {} -> DLQ {} (max receives: {})",
            queue, dlq_arn, max_receive_count
        );
        Ok(())
    }

    /// Publish a message with a custom delay
    ///
    /// The message will be invisible for the specified delay before becoming available.
    ///
    /// # Arguments
    /// * `queue` - Queue name
    /// * `message` - The message to publish
    /// * `delay_seconds` - Delay before message becomes visible (0-900 seconds)
    ///
    /// # Errors
    ///
    /// FIFO queues do not support per-message delays (only a queue-wide
    /// `DelaySeconds`), so this returns an error rather than letting SQS reject
    /// the request after the retry budget has been spent.
    pub async fn publish_with_delay(
        &mut self,
        queue: &str,
        message: Message,
        delay_seconds: i32,
    ) -> Result<()> {
        let physical_queue = self.resolve_publish_queue(queue, &message);

        if self.is_fifo_queue(&physical_queue) {
            return Err(BrokerError::OperationFailed(format!(
                "FIFO queue '{physical_queue}' does not support per-message delays; \
                 set DelaySeconds on the queue instead"
            )));
        }

        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(&physical_queue).await?;

        let body = self.encode_body(&message)?;
        let attributes = self.build_attributes(&message)?;

        let send_operation = || async {
            client
                .send_message()
                .queue_url(&queue_url)
                .message_body(&body)
                .delay_seconds(delay_seconds.clamp(0, 900))
                .set_message_attributes(if attributes.is_empty() {
                    None
                } else {
                    Some(attributes.clone())
                })
                .send()
                .await
                .map_err(|e| {
                    BrokerError::OperationFailed(format!(
                        "Failed to send delayed message: {}",
                        describe_error(&e)
                    ))
                })
        };

        let result = self.retry_with_backoff(send_operation).await;
        self.record_call_result(&result);
        result?;

        debug!(
            "Published message to SQS queue {} with {} second delay",
            physical_queue, delay_seconds
        );
        Ok(())
    }

    /// Update queue attributes
    ///
    /// # Arguments
    /// * `queue` - Queue name
    /// * `visibility_timeout` - Optional new visibility timeout (0-43200 seconds)
    /// * `message_retention` - Optional new message retention period (60-1209600 seconds)
    /// * `delay_seconds` - Optional new default delay (0-900 seconds)
    pub async fn update_queue_attributes(
        &mut self,
        queue: &str,
        visibility_timeout: Option<i32>,
        message_retention: Option<i32>,
        delay_seconds: Option<i32>,
    ) -> Result<()> {
        let queue = &self.resolve_queue_name(queue);
        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        let mut attributes = HashMap::new();

        if let Some(vt) = visibility_timeout {
            attributes.insert(
                QueueAttributeName::VisibilityTimeout,
                vt.clamp(0, 43200).to_string(),
            );
        }

        if let Some(mr) = message_retention {
            attributes.insert(
                QueueAttributeName::MessageRetentionPeriod,
                mr.clamp(60, 1209600).to_string(),
            );
        }

        if let Some(ds) = delay_seconds {
            attributes.insert(
                QueueAttributeName::DelaySeconds,
                ds.clamp(0, 900).to_string(),
            );
        }

        if attributes.is_empty() {
            return Ok(());
        }

        client
            .set_queue_attributes()
            .queue_url(&queue_url)
            .set_attributes(Some(attributes))
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to update queue attributes: {}", e))
            })?;

        debug!("Updated queue attributes for: {}", queue);
        Ok(())
    }

    /// Remove the redrive policy from a queue
    ///
    /// This disables the Dead Letter Queue for the specified queue.
    pub async fn remove_redrive_policy(&mut self, queue: &str) -> Result<()> {
        let queue = &self.resolve_queue_name(queue);
        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        // Setting an empty string removes the redrive policy
        client
            .set_queue_attributes()
            .queue_url(&queue_url)
            .attributes(QueueAttributeName::RedrivePolicy, "")
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to remove redrive policy: {}", e))
            })?;

        info!("Removed redrive policy from queue: {}", queue);
        Ok(())
    }

    /// Get the redrive policy for a queue
    ///
    /// Returns the DLQ ARN and max receive count if configured.
    pub async fn get_redrive_policy(&mut self, queue: &str) -> Result<Option<(String, i32)>> {
        let queue = &self.resolve_queue_name(queue);
        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        let result = client
            .get_queue_attributes()
            .queue_url(&queue_url)
            .attribute_names(QueueAttributeName::RedrivePolicy)
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to get redrive policy: {}", e))
            })?;

        if let Some(policy_str) = result
            .attributes()
            .and_then(|a| a.get(&QueueAttributeName::RedrivePolicy))
        {
            if policy_str.is_empty() {
                return Ok(None);
            }

            let policy: serde_json::Value = serde_json::from_str(policy_str)
                .map_err(|e| BrokerError::Serialization(e.to_string()))?;

            let dlq_arn = policy["deadLetterTargetArn"]
                .as_str()
                .unwrap_or("")
                .to_string();

            let max_receive_count = policy["maxReceiveCount"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .or_else(|| policy["maxReceiveCount"].as_i64().map(|n| n as i32))
                .unwrap_or(10);

            if dlq_arn.is_empty() {
                return Ok(None);
            }

            return Ok(Some((dlq_arn, max_receive_count)));
        }

        Ok(None)
    }

    /// Tag a queue with metadata
    ///
    /// # Arguments
    /// * `queue` - Queue name
    /// * `tags` - Map of tag key-value pairs
    pub async fn tag_queue(&mut self, queue: &str, tags: HashMap<String, String>) -> Result<()> {
        if tags.is_empty() {
            return Ok(());
        }

        let queue = &self.resolve_queue_name(queue);
        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        client
            .tag_queue()
            .queue_url(&queue_url)
            .set_tags(Some(tags))
            .send()
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to tag queue: {}", e)))?;

        debug!("Tagged queue: {}", queue);
        Ok(())
    }

    /// Get tags for a queue
    pub async fn get_queue_tags(&mut self, queue: &str) -> Result<HashMap<String, String>> {
        let queue = &self.resolve_queue_name(queue);
        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        let result = client
            .list_queue_tags()
            .queue_url(&queue_url)
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to get queue tags: {}", e))
            })?;

        Ok(result.tags().cloned().unwrap_or_default())
    }

    /// Remove tags from a queue
    pub async fn untag_queue(&mut self, queue: &str, tag_keys: Vec<String>) -> Result<()> {
        if tag_keys.is_empty() {
            return Ok(());
        }

        let queue = &self.resolve_queue_name(queue);
        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        client
            .untag_queue()
            .queue_url(&queue_url)
            .set_tag_keys(Some(tag_keys))
            .send()
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to untag queue: {}", e)))?;

        debug!("Removed tags from queue: {}", queue);
        Ok(())
    }

    /// Check if the broker is healthy and can access the queue
    ///
    /// This method verifies that:
    /// - AWS SDK client can be initialized
    /// - Queue URL can be retrieved
    /// - Queue attributes can be fetched
    ///
    /// Useful for Kubernetes readiness/liveness probes and monitoring systems.
    ///
    /// # Arguments
    /// * `queue` - Queue name to check
    ///
    /// # Returns
    /// - `Ok(true)` if queue is accessible and healthy
    /// - `Ok(false)` if the queue does not exist
    /// - `Err(_)` if there is a connection, credential or permission problem
    ///
    /// A missing queue and a missing IAM permission are different operational
    /// problems and are reported differently: only `QueueDoesNotExist` yields
    /// `Ok(false)`.
    pub async fn health_check(&mut self, queue: &str) -> Result<bool> {
        let physical_queue = self.resolve_queue_name(queue);

        let Some(queue_url) = self.try_get_queue_url(&physical_queue).await? else {
            warn!("Health check: queue {} does not exist", physical_queue);
            return Ok(false);
        };

        let client = self.get_client().await?;
        match client
            .get_queue_attributes()
            .queue_url(&queue_url)
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
            .send()
            .await
        {
            Ok(_) => {
                self.mark_healthy();
                debug!("Health check passed for queue: {}", physical_queue);
                Ok(true)
            }
            Err(e) => {
                self.mark_unhealthy();
                Err(BrokerError::Connection(format!(
                    "GetQueueAttributes for '{}' failed: {}",
                    physical_queue,
                    describe_error(&e)
                )))
            }
        }
    }

    /// Get messages from the Dead Letter Queue (DLQ)
    ///
    /// Retrieves messages that failed processing and were moved to the DLQ.
    /// Requires the DLQ ARN to be configured.
    ///
    /// # Arguments
    /// * `max_messages` - Maximum messages to retrieve (1-10)
    ///
    /// # Returns
    /// Vector of envelopes from the DLQ
    ///
    /// # Example
    /// ```ignore
    /// let dlq_messages = broker.get_dlq_messages(10).await?;
    /// for envelope in dlq_messages {
    ///     println!("Failed message: {:?}", envelope.message);
    /// }
    /// ```
    pub async fn get_dlq_messages(&mut self, max_messages: i32) -> Result<Vec<Envelope>> {
        let dlq_name = self.dlq_queue_name()?;
        let wait_time = resolve_wait_time(Duration::from_secs(20), self.wait_time_seconds);

        let envelopes = self
            .consume_batch_physical(&dlq_name, max_messages.clamp(1, 10), wait_time)
            .await?;

        debug!(
            "Received {} message(s) from DLQ {}",
            envelopes.len(),
            dlq_name
        );
        Ok(envelopes)
    }

    /// Physical queue name of the configured Dead Letter Queue.
    ///
    /// Derived from the DLQ ARN (`arn:aws:sqs:region:account:queue-name`).
    ///
    /// # Errors
    ///
    /// [`BrokerError::Configuration`] when no DLQ is configured, or when the
    /// ARN does not carry a queue name.
    pub fn dlq_queue_name(&self) -> Result<String> {
        let dlq_arn = &self
            .dlq_config
            .as_ref()
            .ok_or_else(|| {
                BrokerError::Configuration("DLQ not configured; call with_dlq() first".to_string())
            })?
            .dlq_arn;

        let name = dlq_arn.rsplit(':').next().unwrap_or("");
        if name.is_empty() {
            return Err(BrokerError::Configuration(format!(
                "Invalid DLQ ARN '{dlq_arn}': no queue name"
            )));
        }

        Ok(name.to_string())
    }

    /// Physical name of the broker's own (main) queue.
    pub fn main_queue_name(&self) -> String {
        self.resolve_queue_name(&self.queue_name)
    }

    /// Move a message from DLQ back to the main queue (redrive)
    ///
    /// Takes a message from the DLQ and republishes it to the main queue.
    ///
    /// # Arguments
    /// * `envelope` - Message envelope from DLQ
    ///
    /// # Example
    /// ```ignore
    /// let dlq_messages = broker.get_dlq_messages(10).await?;
    /// for envelope in dlq_messages {
    ///     // Inspect and potentially redrive the message
    ///     broker.redrive_dlq_message(&envelope).await?;
    /// }
    /// ```
    pub async fn redrive_dlq_message(&mut self, envelope: &Envelope) -> Result<()> {
        let dlq_name = self.dlq_queue_name()?;

        // Republish to the main queue first: if the delete afterwards fails,
        // the DLQ copy is still there and the redrive can be retried.
        self.publish(&self.queue_name.clone(), envelope.message.clone())
            .await?;

        // Delete from the DLQ. The receipt handle is only valid against the DLQ
        // itself, so the delete is addressed explicitly to `dlq_name` rather
        // than to the broker's configured main queue.
        self.ack_on(&dlq_name, &envelope.delivery_tag).await?;

        info!("Redriven message from DLQ {} to main queue", dlq_name);
        Ok(())
    }

    /// Get DLQ statistics
    ///
    /// Returns statistics about the Dead Letter Queue including message count
    /// and oldest message age.
    ///
    /// # Returns
    /// QueueStats for the DLQ
    ///
    /// # Example
    /// ```ignore
    /// let dlq_stats = broker.get_dlq_stats().await?;
    /// println!("DLQ has {} messages", dlq_stats.approximate_message_count);
    /// ```
    pub async fn get_dlq_stats(&mut self) -> Result<QueueStats> {
        let dlq_name = self.dlq_queue_name()?;
        self.queue_stats_for_physical(&dlq_name).await
    }

    /// Process messages in parallel with a handler function
    ///
    /// This method consumes messages in batches and processes them concurrently,
    /// improving throughput for I/O-bound or CPU-intensive tasks.
    ///
    /// # Arguments
    /// * `queue` - Queue name to consume from
    /// * `max_messages` - Maximum messages to process in parallel (1-10)
    /// * `timeout` - Long polling wait time
    /// * `handler` - Async function to process each message
    ///
    /// # Returns
    /// Number of messages successfully processed
    ///
    /// # Example
    /// ```ignore
    /// let processed = broker.consume_parallel(
    ///     "my-queue",
    ///     5,
    ///     Duration::from_secs(20),
    ///     |envelope| async move {
    ///         println!("Processing message: {:?}", envelope.message);
    ///         Ok(())
    ///     }
    /// ).await?;
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub async fn consume_parallel<F, Fut>(
        &mut self,
        queue: &str,
        max_messages: i32,
        timeout: Duration,
        handler: F,
    ) -> Result<usize>
    where
        F: Fn(Envelope) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send,
    {
        let envelopes = self.consume_batch(queue, max_messages, timeout).await?;

        if envelopes.is_empty() {
            return Ok(0);
        }

        let handler = std::sync::Arc::new(handler);
        let mut tasks = Vec::new();

        for envelope in envelopes {
            let delivery_tag = envelope.delivery_tag.clone();
            let handler = handler.clone();

            let task = tokio::spawn(async move {
                let result = handler(envelope).await;
                (delivery_tag, result)
            });

            tasks.push(task);
        }

        let mut successful = 0;
        let mut failed_tags = Vec::new();

        for task in tasks {
            match task.await {
                Ok((delivery_tag, Ok(()))) => {
                    // Handler succeeded, acknowledge message
                    if let Err(e) = self.ack(&delivery_tag).await {
                        warn!("Failed to acknowledge message {}: {}", delivery_tag, e);
                    } else {
                        successful += 1;
                    }
                }
                Ok((delivery_tag, Err(e))) => {
                    // Handler failed, requeue message
                    warn!("Handler failed for message {}: {}", delivery_tag, e);
                    failed_tags.push(delivery_tag);
                }
                Err(e) => {
                    // Task panicked
                    warn!("Task panicked: {}", e);
                }
            }
        }

        // Reject failed messages (requeue them)
        for tag in failed_tags {
            if let Err(e) = self.reject(&tag, true).await {
                warn!("Failed to requeue message {}: {}", tag, e);
            }
        }

        debug!(
            "Processed {} messages in parallel from queue: {}",
            successful, queue
        );
        Ok(successful)
    }

    /// Acknowledge (delete) a message against an explicit queue.
    ///
    /// [`Consumer::ack`] resolves the queue from the delivery tag; use this
    /// when holding a bare receipt handle whose origin is known out of band.
    pub async fn ack_on(&mut self, queue: &str, delivery_tag: &str) -> Result<()> {
        let (source, receipt_handle) = decode_delivery_tag(delivery_tag);
        let target_queue = source.unwrap_or(queue).to_string();

        self.stop_visibility_heartbeat(delivery_tag);

        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(&target_queue).await?;

        client
            .delete_message()
            .queue_url(&queue_url)
            .receipt_handle(receipt_handle)
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!(
                    "Failed to delete message from '{}': {}",
                    target_queue,
                    describe_error(&e)
                ))
            })?;

        self.forget_receipt_metadata(delivery_tag);
        self.mark_healthy();
        debug!("Acknowledged message on queue {}", target_queue);
        Ok(())
    }

    /// Reject a message against an explicit queue.
    ///
    /// With `requeue` the message's visibility timeout is reset to zero so it
    /// becomes immediately available again; without it the message is deleted.
    pub async fn reject_on(
        &mut self,
        queue: &str,
        delivery_tag: &str,
        requeue: bool,
    ) -> Result<()> {
        let (source, receipt_handle) = decode_delivery_tag(delivery_tag);
        let target_queue = source.unwrap_or(queue).to_string();

        self.stop_visibility_heartbeat(delivery_tag);

        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(&target_queue).await?;

        if requeue {
            client
                .change_message_visibility()
                .queue_url(&queue_url)
                .receipt_handle(receipt_handle)
                .visibility_timeout(0)
                .send()
                .await
                .map_err(|e| {
                    BrokerError::OperationFailed(format!(
                        "Failed to requeue message on '{}': {}",
                        target_queue,
                        describe_error(&e)
                    ))
                })?;

            debug!("Rejected and requeued message on queue {}", target_queue);
        } else {
            client
                .delete_message()
                .queue_url(&queue_url)
                .receipt_handle(receipt_handle)
                .send()
                .await
                .map_err(|e| {
                    BrokerError::OperationFailed(format!(
                        "Failed to delete message from '{}': {}",
                        target_queue,
                        describe_error(&e)
                    ))
                })?;

            debug!("Rejected and deleted message on queue {}", target_queue);
        }

        self.forget_receipt_metadata(delivery_tag);
        self.mark_healthy();
        Ok(())
    }
}

// --- Trait Implementations ---

#[async_trait]
impl Transport for SqsBroker {
    async fn connect(&mut self) -> Result<()> {
        info!("Connecting to AWS SQS: {}", self.queue_name);

        // Initialize client
        let _ = self.get_client().await?;

        // Get or create queue URL and cache it (clone to avoid a borrow conflict)
        let queue_name = self.resolve_queue_name(&self.queue_name.clone());
        let _ = self.get_queue_url(&queue_name).await?;

        self.mark_healthy();
        info!("Connected to SQS queue: {}", queue_name);
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        self.stop_all_visibility_heartbeats();
        self.clear_prefetch();
        self.receipt_metadata.clear();
        self.receipt_metadata_order.clear();
        self.client = None;
        self.queue_url_cache.clear();
        info!("Disconnected from SQS");
        Ok(())
    }

    /// Best-effort connection state.
    ///
    /// This is synchronous, so it cannot probe AWS. It reports the local client
    /// state AND-ed with the outcome of the most recent SDK call, so expired
    /// credentials, a revoked IAM policy or a network outage flip it to `false`
    /// as soon as one operation observes them. For a real readiness probe call
    /// [`health_check`](SqsBroker::health_check), which performs an actual
    /// `GetQueueAttributes` round trip.
    fn is_connected(&self) -> bool {
        self.client.is_some() && !self.queue_url_cache.is_empty() && self.is_connection_healthy()
    }

    fn name(&self) -> &str {
        "sqs"
    }
}

#[async_trait]
impl Producer for SqsBroker {
    /// Publish a message.
    ///
    /// FIFO queues are detected from the queue name and routed through the FIFO
    /// path with a derived `MessageGroupId` and `MessageDeduplicationId` — SQS
    /// rejects a FIFO `SendMessage` without a group id, which used to make
    /// every publish through this trait fail (and then be retried, pointlessly,
    /// `max_retries` times).
    ///
    /// With Celery compatibility enabled the queue name is translated by the
    /// configured naming strategy and, when priority queues are on, the message
    /// is routed to the queue for its priority.
    ///
    /// Delivery is at-least-once on standard queues: a send that times out
    /// after AWS accepted it is retried and may duplicate. FIFO publishes carry
    /// a stable deduplication id and are idempotent within SQS's 5-minute
    /// deduplication interval.
    async fn publish(&mut self, queue: &str, message: Message) -> Result<()> {
        let physical_queue = self.resolve_publish_queue(queue, &message);

        if self.is_fifo_queue(&physical_queue) {
            let group_id = self.derive_group_id(&physical_queue, &message);
            return self
                .publish_fifo(&physical_queue, message, &group_id, None)
                .await;
        }

        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(&physical_queue).await?;

        let body = self.encode_body(&message)?;
        let attributes = self.build_attributes(&message)?;

        let send_operation = || async {
            client
                .send_message()
                .queue_url(&queue_url)
                .message_body(&body)
                .set_message_attributes(if attributes.is_empty() {
                    None
                } else {
                    Some(attributes.clone())
                })
                .send()
                .await
                .map_err(|e| {
                    BrokerError::OperationFailed(format!(
                        "Failed to send message: {}",
                        describe_error(&e)
                    ))
                })
        };

        let result = self.retry_with_backoff(send_operation).await;
        self.record_call_result(&result);
        result?;

        debug!("Published message to SQS queue: {}", physical_queue);
        Ok(())
    }

    async fn publish_with_routing(
        &mut self,
        _exchange: &str,
        routing_key: &str,
        message: Message,
    ) -> Result<()> {
        // SQS doesn't have exchanges, route to queue directly
        debug!(
            "SQS has no exchanges; routing directly to queue: {}",
            routing_key
        );
        self.publish(routing_key, message).await
    }
}

#[async_trait]
impl Consumer for SqsBroker {
    /// Receive one message.
    ///
    /// When [`with_max_messages`](SqsBroker::with_max_messages) is greater than
    /// one, a single `ReceiveMessage` fetches up to that many messages; the
    /// first is returned and the rest are buffered *per queue* and served from
    /// memory by later calls. Buffered messages are already in flight, so their
    /// visibility timeout is running — see `with_max_messages` for the
    /// trade-off.
    ///
    /// With Celery priority queues enabled the priority queues are polled
    /// highest-first; only the last one uses the full long-polling wait so the
    /// scan costs one long poll rather than one per priority level.
    async fn consume(&mut self, queue: &str, timeout: Duration) -> Result<Option<Envelope>> {
        let wait_time = if let Some(ref mut adaptive) = self.adaptive_polling {
            adaptive.current_wait_time().clamp(0, 20)
        } else {
            resolve_wait_time(timeout, self.wait_time_seconds)
        };

        let mut candidates = self.priority_queues(queue);
        if candidates.is_empty() {
            candidates.push(self.resolve_queue_name(queue));
        }

        // Serve anything already prefetched before spending an API call.
        for candidate in &candidates {
            if let Some(envelope) = self.take_prefetched(candidate) {
                if let Some(ref mut adaptive) = self.adaptive_polling {
                    adaptive.adjust_wait_time(true);
                }
                return Ok(Some(envelope));
            }
        }

        let max_messages = self.max_messages;
        let last_index = candidates.len() - 1;
        let mut missing_queues = 0usize;
        let mut last_missing: Option<BrokerError> = None;

        for (index, candidate) in candidates.iter().enumerate() {
            // Only the lowest-priority (last) queue gets the long poll; the
            // higher-priority queues are checked with a short poll so a
            // priority scan does not multiply the wait.
            let candidate_wait = if index == last_index { wait_time } else { 0 };

            let mut envelopes = match self
                .receive_envelopes(candidate, max_messages, candidate_wait)
                .await
            {
                Ok(envelopes) => envelopes,
                // A priority sibling that was never created is not an error as
                // long as some other candidate exists: skip it and keep polling.
                Err(BrokerError::QueueNotFound(queue)) if candidates.len() > 1 => {
                    missing_queues += 1;
                    last_missing = Some(BrokerError::QueueNotFound(queue));
                    continue;
                }
                Err(error) => return Err(error),
            };

            if envelopes.is_empty() {
                continue;
            }

            let envelope = envelopes.remove(0);
            self.buffer_prefetched(candidate, envelopes);

            if let Some(ref mut adaptive) = self.adaptive_polling {
                adaptive.adjust_wait_time(true);
            }

            debug!("Consumed message from SQS queue: {}", candidate);
            return Ok(Some(envelope));
        }

        // Every candidate queue was missing: that is a real configuration
        // problem, not an empty queue, and must not be reported as "no work".
        if missing_queues == candidates.len() {
            if let Some(error) = last_missing {
                return Err(error);
            }
        }

        if let Some(ref mut adaptive) = self.adaptive_polling {
            adaptive.adjust_wait_time(false);
        }

        Ok(None)
    }

    /// Acknowledge a message against the queue it was received from.
    ///
    /// The queue is recovered from the delivery tag, so messages consumed from
    /// any queue — including a DLQ — are deleted correctly. Tags without queue
    /// information fall back to the broker's configured queue.
    async fn ack(&mut self, delivery_tag: &str) -> Result<()> {
        let fallback = self.resolve_queue_name(&self.queue_name.clone());
        self.ack_on(&fallback, delivery_tag).await
    }

    /// Reject a message against the queue it was received from.
    async fn reject(&mut self, delivery_tag: &str, requeue: bool) -> Result<()> {
        let fallback = self.resolve_queue_name(&self.queue_name.clone());
        self.reject_on(&fallback, delivery_tag, requeue).await
    }

    async fn queue_size(&mut self, queue: &str) -> Result<usize> {
        let physical_queue = self.resolve_queue_name(queue);
        self.queue_size_for_physical(&physical_queue).await
    }
}

impl SqsBroker {
    /// Approximate message count for an already-resolved (physical) queue name.
    pub(crate) async fn queue_size_for_physical(&mut self, queue: &str) -> Result<usize> {
        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        let result = client
            .get_queue_attributes()
            .queue_url(&queue_url)
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
            .send()
            .await
            .map_err(|e| {
                self.mark_unhealthy();
                BrokerError::Connection(format!(
                    "Failed to get queue attributes for '{}': {}",
                    queue,
                    describe_error(&e)
                ))
            })?;

        self.mark_healthy();

        let count = result
            .attributes()
            .and_then(|attrs| attrs.get(&QueueAttributeName::ApproximateNumberOfMessages))
            .and_then(|count_str| count_str.parse::<usize>().ok())
            .unwrap_or(0);

        Ok(count)
    }
}

#[async_trait]
impl Broker for SqsBroker {
    async fn purge(&mut self, queue: &str) -> Result<usize> {
        // Resolve once and use the physical name for both calls, so the count
        // and the purge can never address two different queues.
        let physical_queue = self.resolve_queue_name(queue);

        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(&physical_queue).await?;

        // Get current size before purge
        let size = self.queue_size_for_physical(&physical_queue).await?;

        client
            .purge_queue()
            .queue_url(&queue_url)
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!(
                    "Failed to purge queue '{}': {}",
                    physical_queue,
                    describe_error(&e)
                ))
            })?;

        // Anything buffered from this queue is gone as well.
        self.prefetch.remove(&physical_queue);

        debug!("Purged SQS queue: {}", physical_queue);
        Ok(size)
    }

    async fn create_queue(&mut self, queue: &str, mode: QueueMode) -> Result<()> {
        let physical_queue = self.resolve_queue_name(queue);
        self.create_queue_physical(&physical_queue, mode).await
    }
    async fn delete_queue(&mut self, queue: &str) -> Result<()> {
        let physical_queue = self.resolve_queue_name(queue);

        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(&physical_queue).await?;

        client
            .delete_queue()
            .queue_url(&queue_url)
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!(
                    "Failed to delete queue '{}': {}",
                    physical_queue,
                    describe_error(&e)
                ))
            })?;

        // Remove from cache
        self.queue_url_cache.remove(&physical_queue);
        self.prefetch.remove(&physical_queue);

        debug!("Deleted SQS queue: {}", physical_queue);
        Ok(())
    }

    async fn list_queues(&mut self) -> Result<Vec<String>> {
        let client = self.get_client().await?;

        let result =
            client.list_queues().send().await.map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to list queues: {}", e))
            })?;

        let queues = result
            .queue_urls()
            .iter()
            .filter_map(|url| {
                // Extract queue name from URL (last segment)
                url.rsplit('/').next().map(String::from)
            })
            .collect();

        Ok(queues)
    }
}

impl SqsBroker {
    /// Create a queue using an already-resolved (physical) name.
    ///
    /// `get_queue_url`'s auto-creation path calls this directly: the naming
    /// strategy has already been applied there and applying it twice would
    /// produce `celery_celery_tasks`.
    pub(crate) async fn create_queue_physical(
        &mut self,
        queue: &str,
        mode: QueueMode,
    ) -> Result<()> {
        // Clone values before borrowing
        let visibility_timeout = self.visibility_timeout;
        let wait_time_seconds = self.wait_time_seconds;
        let message_retention_seconds = self.message_retention_seconds;
        let delay_seconds = self.delay_seconds;
        let fifo_config = self.fifo_config.clone();
        let sse_config = self.sse_config.clone();
        let dlq_config = self.dlq_config.clone();

        let client = self.get_client().await?;

        let mut attributes = HashMap::new();

        // Set visibility timeout
        attributes.insert(
            QueueAttributeName::VisibilityTimeout,
            visibility_timeout.to_string(),
        );

        // Set receive message wait time for long polling
        attributes.insert(
            QueueAttributeName::ReceiveMessageWaitTimeSeconds,
            wait_time_seconds.to_string(),
        );

        // Set message retention period
        attributes.insert(
            QueueAttributeName::MessageRetentionPeriod,
            message_retention_seconds.to_string(),
        );

        // Set default delay seconds
        attributes.insert(QueueAttributeName::DelaySeconds, delay_seconds.to_string());

        // Configure for priority mode if requested
        if matches!(mode, QueueMode::Priority) {
            warn!("SQS doesn't natively support priority queues. Priority is handled via message attributes.");
        }

        // Configure FIFO queue settings
        let is_fifo = queue.ends_with(".fifo") || fifo_config.is_some();
        if is_fifo {
            if !queue.ends_with(".fifo") {
                return Err(BrokerError::OperationFailed(
                    "FIFO queue name must end with '.fifo'".to_string(),
                ));
            }

            attributes.insert(QueueAttributeName::FifoQueue, "true".to_string());

            if let Some(ref fifo) = fifo_config {
                if fifo.content_based_deduplication {
                    attributes.insert(
                        QueueAttributeName::ContentBasedDeduplication,
                        "true".to_string(),
                    );
                }

                if fifo.high_throughput {
                    attributes.insert(
                        QueueAttributeName::DeduplicationScope,
                        "messageGroup".to_string(),
                    );
                    attributes.insert(
                        QueueAttributeName::FifoThroughputLimit,
                        "perMessageGroupId".to_string(),
                    );
                }
            }

            info!("Creating FIFO queue: {}", queue);
        }

        // Configure Server-Side Encryption
        if let Some(ref sse) = sse_config {
            if sse.use_kms {
                if let Some(ref key_id) = sse.kms_key_id {
                    attributes.insert(QueueAttributeName::KmsMasterKeyId, key_id.clone());
                }
                if let Some(reuse_period) = sse.kms_data_key_reuse_period {
                    attributes.insert(
                        QueueAttributeName::KmsDataKeyReusePeriodSeconds,
                        reuse_period.to_string(),
                    );
                }
                info!("Queue {} configured with KMS encryption", queue);
            } else {
                attributes.insert(QueueAttributeName::SqsManagedSseEnabled, "true".to_string());
                info!("Queue {} configured with SQS-managed SSE", queue);
            }
        }

        // Configure Dead Letter Queue (redrive policy)
        if let Some(ref dlq) = dlq_config {
            let redrive_policy = serde_json::json!({
                "deadLetterTargetArn": dlq.dlq_arn,
                "maxReceiveCount": dlq.max_receive_count.to_string()
            })
            .to_string();

            attributes.insert(QueueAttributeName::RedrivePolicy, redrive_policy);
            info!(
                "Queue {} configured with DLQ: {} (max receives: {})",
                queue, dlq.dlq_arn, dlq.max_receive_count
            );
        }

        let result = client
            .create_queue()
            .queue_name(queue)
            .set_attributes(Some(attributes))
            .send()
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to create queue: {}", e)))?;

        if let Some(url) = result.queue_url() {
            // Cache the queue URL
            self.queue_url_cache
                .insert(queue.to_string(), url.to_string());
            debug!("Created SQS queue: {} ({})", queue, url);
        }

        Ok(())
    }
}
