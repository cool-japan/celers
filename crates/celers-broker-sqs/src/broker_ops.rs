//! Additional SQS broker operations and trait implementations
//!
//! Contains batch operations, FIFO queue support, DLQ management,
//! queue management utilities, and trait implementations for
//! Transport, Producer, Consumer, and Broker.

use async_trait::async_trait;
use aws_sdk_sqs::types::{MessageAttributeValue, QueueAttributeName};
use celers_kombu::{
    Broker, BrokerError, Consumer, Envelope, Producer, QueueMode, Result, Transport,
};
use celers_protocol::Message;
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, info, warn};

use crate::broker_core::SqsBroker;
use crate::types::QueueStats;

impl SqsBroker {
    /// Publish multiple messages in a single batch (up to 10 messages)
    ///
    /// This is significantly more efficient and cost-effective than individual publishes.
    /// AWS SQS charges per API request, so batch operations reduce costs by 10x.
    ///
    /// # Arguments
    /// * `queue` - Queue name to publish to
    /// * `messages` - Vector of messages to publish (max 10)
    ///
    /// # Returns
    /// Number of messages successfully published
    ///
    /// # Note
    /// If batch size exceeds 10, only the first 10 messages will be sent.
    pub async fn publish_batch(&mut self, queue: &str, messages: Vec<Message>) -> Result<usize> {
        if messages.is_empty() {
            return Ok(0);
        }

        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        // SQS batch limit is 10 messages
        let batch_size = messages.len().min(10);
        let batch_messages = &messages[..batch_size];

        // Build batch entries
        let mut entries = Vec::new();
        for (idx, message) in batch_messages.iter().enumerate() {
            let mut body = serde_json::to_string(message)
                .map_err(|e| BrokerError::Serialization(e.to_string()))?;

            // Apply compression if enabled and message exceeds threshold
            if let Some(threshold) = self.compression_threshold {
                let original_size = body.len();
                if original_size > threshold {
                    body = self.compress_message(&body)?;
                    debug!(
                        "Compressed batch message {} from {} to {} bytes",
                        idx,
                        original_size,
                        body.len()
                    );
                }
            }

            let mut entry = aws_sdk_sqs::types::SendMessageBatchRequestEntry::builder()
                .id(idx.to_string())
                .message_body(body);

            // Add message attributes
            let mut attributes = HashMap::new();

            if let Some(priority) = message.properties.priority {
                attributes.insert(
                    "priority".to_string(),
                    MessageAttributeValue::builder()
                        .data_type("Number")
                        .string_value(priority.to_string())
                        .build()
                        .map_err(|e| BrokerError::OperationFailed(e.to_string()))?,
                );
            }

            if let Some(ref correlation_id) = message.properties.correlation_id {
                attributes.insert(
                    "correlation_id".to_string(),
                    MessageAttributeValue::builder()
                        .data_type("String")
                        .string_value(correlation_id)
                        .build()
                        .map_err(|e| BrokerError::OperationFailed(e.to_string()))?,
                );
            }

            if !attributes.is_empty() {
                entry = entry.set_message_attributes(Some(attributes));
            }

            entries.push(
                entry
                    .build()
                    .map_err(|e| BrokerError::OperationFailed(e.to_string()))?,
            );
        }

        // Send batch
        let result = client
            .send_message_batch()
            .queue_url(&queue_url)
            .set_entries(Some(entries))
            .send()
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to send batch: {}", e)))?;

        let successful = result.successful().len();

        let failed = result.failed();
        if !failed.is_empty() {
            warn!(
                "Batch send had {} failures out of {}",
                failed.len(),
                batch_size
            );
        }

        debug!(
            "Published {} messages in batch to SQS queue: {}",
            successful, queue
        );
        Ok(successful)
    }

    /// Publish a large batch of messages with automatic chunking
    ///
    /// Automatically splits large batches into groups of 10 messages and sends them
    /// in multiple API calls. This is more efficient than calling publish() repeatedly.
    ///
    /// # Arguments
    /// * `queue` - Queue name to publish to
    /// * `messages` - Vector of messages to publish (any size)
    ///
    /// # Returns
    /// Number of messages successfully published
    ///
    /// # Example
    /// ```ignore
    /// // Send 100 messages in 10 batches of 10
    /// let messages = vec![/* ... 100 messages ... */];
    /// let count = broker.publish_batch_chunked("my-queue", messages).await?;
    /// println!("Published {} messages", count);
    /// ```
    pub async fn publish_batch_chunked(
        &mut self,
        queue: &str,
        messages: Vec<Message>,
    ) -> Result<usize> {
        if messages.is_empty() {
            return Ok(0);
        }

        let mut total_published = 0;

        // Process in chunks of 10
        for chunk in messages.chunks(10) {
            let chunk_messages = chunk.to_vec();
            let published = self.publish_batch(queue, chunk_messages).await?;
            total_published += published;
        }

        info!(
            "Published {} messages in {} chunks to SQS queue: {}",
            total_published,
            messages.len().div_ceil(10),
            queue
        );

        Ok(total_published)
    }

    /// Consume multiple messages in a single batch (up to 10 messages)
    ///
    /// More efficient than polling one message at a time.
    ///
    /// # Arguments
    /// * `queue` - Queue name to consume from
    /// * `max_messages` - Maximum number of messages to receive (max 10)
    /// * `timeout` - Long polling wait time (max 20 seconds)
    ///
    /// # Returns
    /// Vector of envelopes
    pub async fn consume_batch(
        &mut self,
        queue: &str,
        max_messages: i32,
        timeout: Duration,
    ) -> Result<Vec<Envelope>> {
        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        let wait_time = timeout.as_secs().min(20) as i32;
        let max_msgs = max_messages.min(10);

        let result = client
            .receive_message()
            .queue_url(&queue_url)
            .max_number_of_messages(max_msgs)
            .visibility_timeout(self.visibility_timeout)
            .wait_time_seconds(wait_time)
            .message_attribute_names("All")
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to receive messages: {}", e))
            })?;

        let mut envelopes = Vec::new();

        if let Some(messages) = result.messages {
            for sqs_message in messages {
                let body = sqs_message.body().ok_or_else(|| {
                    BrokerError::OperationFailed("Message has no body".to_string())
                })?;

                let receipt_handle = sqs_message
                    .receipt_handle()
                    .ok_or_else(|| {
                        BrokerError::OperationFailed("Message has no receipt handle".to_string())
                    })?
                    .to_string();

                // Decompress message if it was compressed
                let decompressed_body = self.decompress_message(body)?;

                let message: Message = serde_json::from_str(&decompressed_body)
                    .map_err(|e| BrokerError::Serialization(e.to_string()))?;

                let envelope = Envelope {
                    delivery_tag: receipt_handle,
                    message,
                    redelivered: sqs_message.attributes().is_some_and(|attrs| {
                        attrs
                            .get(&aws_sdk_sqs::types::MessageSystemAttributeName::ApproximateReceiveCount)
                            .and_then(|count| count.parse::<i32>().ok())
                            .map(|count| count > 1)
                            .unwrap_or(false)
                    }),
                };

                envelopes.push(envelope);
            }
        }

        debug!(
            "Consumed {} messages in batch from SQS queue: {}",
            envelopes.len(),
            queue
        );
        Ok(envelopes)
    }

    /// Acknowledge (delete) multiple messages in a single batch (up to 10 messages)
    ///
    /// This is significantly more efficient than individual acks.
    ///
    /// # Arguments
    /// * `queue` - Queue name
    /// * `receipt_handles` - Vector of receipt handles to delete (max 10)
    ///
    /// # Returns
    /// Number of messages successfully deleted
    pub async fn ack_batch(&mut self, queue: &str, receipt_handles: Vec<String>) -> Result<usize> {
        if receipt_handles.is_empty() {
            return Ok(0);
        }

        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        // SQS batch limit is 10 messages
        let batch_size = receipt_handles.len().min(10);
        let batch_handles = &receipt_handles[..batch_size];

        // Build batch entries
        let mut entries = Vec::new();
        for (idx, receipt_handle) in batch_handles.iter().enumerate() {
            entries.push(
                aws_sdk_sqs::types::DeleteMessageBatchRequestEntry::builder()
                    .id(idx.to_string())
                    .receipt_handle(receipt_handle)
                    .build()
                    .map_err(|e| BrokerError::OperationFailed(e.to_string()))?,
            );
        }

        // Delete batch
        let result = client
            .delete_message_batch()
            .queue_url(&queue_url)
            .set_entries(Some(entries))
            .send()
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to delete batch: {}", e)))?;

        let successful = result.successful().len();

        let failed = result.failed();
        if !failed.is_empty() {
            warn!(
                "Batch delete had {} failures out of {}",
                failed.len(),
                batch_size
            );
        }

        debug!(
            "Acknowledged {} messages in batch from SQS queue: {}",
            successful, queue
        );
        Ok(successful)
    }

    /// Publish a message to a FIFO queue
    ///
    /// FIFO queues require a message group ID for ordering guarantees.
    /// Optionally provide a deduplication ID for exactly-once delivery.
    ///
    /// # Arguments
    /// * `queue` - Queue name (must end with ".fifo")
    /// * `message` - The message to publish
    /// * `message_group_id` - Required for FIFO ordering
    /// * `deduplication_id` - Optional; if None and content-based deduplication is
    ///   disabled, a UUID will be generated
    pub async fn publish_fifo(
        &mut self,
        queue: &str,
        message: Message,
        message_group_id: &str,
        deduplication_id: Option<&str>,
    ) -> Result<()> {
        if !queue.ends_with(".fifo") {
            return Err(BrokerError::OperationFailed(
                "FIFO queue name must end with '.fifo'".to_string(),
            ));
        }

        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        // Serialize message to JSON
        let mut body = serde_json::to_string(&message)
            .map_err(|e| BrokerError::Serialization(e.to_string()))?;

        // Apply compression if enabled and message exceeds threshold
        if let Some(threshold) = self.compression_threshold {
            let original_size = body.len();
            if original_size > threshold {
                body = self.compress_message(&body)?;
                debug!(
                    "Compressed FIFO message from {} to {} bytes",
                    original_size,
                    body.len()
                );
            }
        }

        // Build message attributes
        let mut attributes = HashMap::new();

        if let Some(priority) = message.properties.priority {
            attributes.insert(
                "priority".to_string(),
                MessageAttributeValue::builder()
                    .data_type("Number")
                    .string_value(priority.to_string())
                    .build()
                    .map_err(|e| BrokerError::OperationFailed(e.to_string()))?,
            );
        }

        if let Some(ref correlation_id) = message.properties.correlation_id {
            attributes.insert(
                "correlation_id".to_string(),
                MessageAttributeValue::builder()
                    .data_type("String")
                    .string_value(correlation_id)
                    .build()
                    .map_err(|e| BrokerError::OperationFailed(e.to_string()))?,
            );
        }

        // Determine deduplication ID
        let dedup_id = deduplication_id.map(String::from).or_else(|| {
            // If content-based deduplication is enabled, SQS will handle it
            if self
                .fifo_config
                .as_ref()
                .is_some_and(|c| c.content_based_deduplication)
            {
                None
            } else {
                // Generate a UUID for deduplication
                Some(uuid::Uuid::new_v4().to_string())
            }
        });

        // Send message
        let mut request = client
            .send_message()
            .queue_url(&queue_url)
            .message_body(&body)
            .message_group_id(message_group_id);

        if let Some(ref dedup) = dedup_id {
            request = request.message_deduplication_id(dedup);
        }

        if !attributes.is_empty() {
            request = request.set_message_attributes(Some(attributes));
        }

        request.send().await.map_err(|e| {
            BrokerError::OperationFailed(format!("Failed to send FIFO message: {}", e))
        })?;

        debug!(
            "Published FIFO message to queue: {} (group: {})",
            queue, message_group_id
        );
        Ok(())
    }

    /// Publish multiple messages to a FIFO queue in a batch
    ///
    /// # Arguments
    /// * `queue` - Queue name (must end with ".fifo")
    /// * `messages` - Vector of (message, message_group_id, optional_deduplication_id)
    ///
    /// # Returns
    /// Number of messages successfully published
    pub async fn publish_fifo_batch(
        &mut self,
        queue: &str,
        messages: Vec<(Message, String, Option<String>)>,
    ) -> Result<usize> {
        if messages.is_empty() {
            return Ok(0);
        }

        if !queue.ends_with(".fifo") {
            return Err(BrokerError::OperationFailed(
                "FIFO queue name must end with '.fifo'".to_string(),
            ));
        }

        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        let batch_size = messages.len().min(10);
        let batch_messages = &messages[..batch_size];

        let content_based_dedup = self
            .fifo_config
            .as_ref()
            .is_some_and(|c| c.content_based_deduplication);

        let mut entries = Vec::new();
        for (idx, (message, group_id, dedup_id)) in batch_messages.iter().enumerate() {
            let mut body = serde_json::to_string(message)
                .map_err(|e| BrokerError::Serialization(e.to_string()))?;

            // Apply compression if enabled and message exceeds threshold
            if let Some(threshold) = self.compression_threshold {
                let original_size = body.len();
                if original_size > threshold {
                    body = self.compress_message(&body)?;
                    debug!(
                        "Compressed FIFO batch message {} from {} to {} bytes",
                        idx,
                        original_size,
                        body.len()
                    );
                }
            }

            let mut entry = aws_sdk_sqs::types::SendMessageBatchRequestEntry::builder()
                .id(idx.to_string())
                .message_body(body)
                .message_group_id(group_id);

            // Set deduplication ID
            let final_dedup_id = dedup_id.clone().or_else(|| {
                if content_based_dedup {
                    None
                } else {
                    Some(uuid::Uuid::new_v4().to_string())
                }
            });

            if let Some(ref dedup) = final_dedup_id {
                entry = entry.message_deduplication_id(dedup);
            }

            // Add message attributes
            let mut attributes = HashMap::new();
            if let Some(priority) = message.properties.priority {
                attributes.insert(
                    "priority".to_string(),
                    MessageAttributeValue::builder()
                        .data_type("Number")
                        .string_value(priority.to_string())
                        .build()
                        .map_err(|e| BrokerError::OperationFailed(e.to_string()))?,
                );
            }

            if let Some(ref correlation_id) = message.properties.correlation_id {
                attributes.insert(
                    "correlation_id".to_string(),
                    MessageAttributeValue::builder()
                        .data_type("String")
                        .string_value(correlation_id)
                        .build()
                        .map_err(|e| BrokerError::OperationFailed(e.to_string()))?,
                );
            }

            if !attributes.is_empty() {
                entry = entry.set_message_attributes(Some(attributes));
            }

            entries.push(
                entry
                    .build()
                    .map_err(|e| BrokerError::OperationFailed(e.to_string()))?,
            );
        }

        let result = client
            .send_message_batch()
            .queue_url(&queue_url)
            .set_entries(Some(entries))
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to send FIFO batch: {}", e))
            })?;

        let successful = result.successful().len();

        let failed = result.failed();
        if !failed.is_empty() {
            warn!(
                "FIFO batch send had {} failures out of {}",
                failed.len(),
                batch_size
            );
        }

        debug!(
            "Published {} FIFO messages in batch to queue: {}",
            successful, queue
        );
        Ok(successful)
    }

    /// Get detailed queue statistics and monitoring data
    ///
    /// Returns approximate counts for messages and other queue attributes.
    pub async fn get_queue_stats(&mut self, queue: &str) -> Result<QueueStats> {
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
    /// the message from becoming visible to other consumers.
    ///
    /// # Arguments
    /// * `delivery_tag` - The receipt handle of the message
    /// * `timeout_seconds` - New visibility timeout (0-43200 seconds)
    pub async fn extend_visibility(
        &mut self,
        delivery_tag: &str,
        timeout_seconds: i32,
    ) -> Result<()> {
        let client = self.get_client().await?;
        let queue_name = self.queue_name.clone();
        let queue_url = self.get_queue_url(&queue_name).await?;

        client
            .change_message_visibility()
            .queue_url(queue_url)
            .receipt_handle(delivery_tag)
            .visibility_timeout(timeout_seconds.clamp(0, 43200))
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to extend visibility: {}", e))
            })?;

        debug!(
            "Extended visibility timeout to {} seconds for message",
            timeout_seconds
        );
        Ok(())
    }

    /// Get the ARN of a queue
    pub async fn get_queue_arn(&mut self, queue: &str) -> Result<String> {
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

    /// Extend visibility timeout for multiple messages in a batch
    ///
    /// More efficient than extending visibility for individual messages.
    ///
    /// # Arguments
    /// * `queue` - Queue name
    /// * `entries` - Vector of (receipt_handle, timeout_seconds) tuples (max 10)
    ///
    /// # Returns
    /// Number of messages successfully updated
    pub async fn extend_visibility_batch(
        &mut self,
        queue: &str,
        entries: Vec<(String, i32)>,
    ) -> Result<usize> {
        if entries.is_empty() {
            return Ok(0);
        }

        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        let batch_size = entries.len().min(10);
        let batch_entries = &entries[..batch_size];

        let mut request_entries = Vec::new();
        for (idx, (receipt_handle, timeout)) in batch_entries.iter().enumerate() {
            request_entries.push(
                aws_sdk_sqs::types::ChangeMessageVisibilityBatchRequestEntry::builder()
                    .id(idx.to_string())
                    .receipt_handle(receipt_handle)
                    .visibility_timeout(timeout.clamp(&0, &43200).to_owned())
                    .build()
                    .map_err(|e| BrokerError::OperationFailed(e.to_string()))?,
            );
        }

        let result = client
            .change_message_visibility_batch()
            .queue_url(&queue_url)
            .set_entries(Some(request_entries))
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to extend visibility batch: {}", e))
            })?;

        let successful = result.successful().len();

        let failed = result.failed();
        if !failed.is_empty() {
            warn!(
                "Batch visibility extension had {} failures out of {}",
                failed.len(),
                batch_size
            );
        }

        debug!("Extended visibility for {} messages in batch", successful);
        Ok(successful)
    }

    /// Publish a message with a custom delay
    ///
    /// The message will be invisible for the specified delay before becoming available.
    ///
    /// # Arguments
    /// * `queue` - Queue name
    /// * `message` - The message to publish
    /// * `delay_seconds` - Delay before message becomes visible (0-900 seconds)
    pub async fn publish_with_delay(
        &mut self,
        queue: &str,
        message: Message,
        delay_seconds: i32,
    ) -> Result<()> {
        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        let body = serde_json::to_string(&message)
            .map_err(|e| BrokerError::Serialization(e.to_string()))?;

        let mut attributes = HashMap::new();

        if let Some(priority) = message.properties.priority {
            attributes.insert(
                "priority".to_string(),
                MessageAttributeValue::builder()
                    .data_type("Number")
                    .string_value(priority.to_string())
                    .build()
                    .map_err(|e| BrokerError::OperationFailed(e.to_string()))?,
            );
        }

        if let Some(ref correlation_id) = message.properties.correlation_id {
            attributes.insert(
                "correlation_id".to_string(),
                MessageAttributeValue::builder()
                    .data_type("String")
                    .string_value(correlation_id)
                    .build()
                    .map_err(|e| BrokerError::OperationFailed(e.to_string()))?,
            );
        }

        client
            .send_message()
            .queue_url(&queue_url)
            .message_body(&body)
            .delay_seconds(delay_seconds.clamp(0, 900))
            .set_message_attributes(if attributes.is_empty() {
                None
            } else {
                Some(attributes)
            })
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to send delayed message: {}", e))
            })?;

        debug!(
            "Published message to SQS queue {} with {} second delay",
            queue, delay_seconds
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
    /// - `Ok(false)` if queue doesn't exist or is inaccessible
    /// - `Err(_)` if there's a connection or authentication issue
    pub async fn health_check(&mut self, queue: &str) -> Result<bool> {
        match self.get_queue_url(queue).await {
            Ok(queue_url) => {
                // Try to get queue attributes to verify full access
                let client = self.get_client().await?;
                match client
                    .get_queue_attributes()
                    .queue_url(&queue_url)
                    .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
                    .send()
                    .await
                {
                    Ok(_) => {
                        debug!("Health check passed for queue: {}", queue);
                        Ok(true)
                    }
                    Err(e) => {
                        warn!("Health check failed for queue {}: {}", queue, e);
                        Ok(false)
                    }
                }
            }
            Err(e) => {
                warn!("Health check failed - queue not found: {}", e);
                Ok(false)
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
        let dlq_arn = self
            .dlq_config
            .as_ref()
            .ok_or_else(|| BrokerError::OperationFailed("DLQ not configured".to_string()))?
            .dlq_arn
            .clone();

        // Extract queue name from ARN (format: arn:aws:sqs:region:account:queue-name)
        let dlq_name = dlq_arn
            .split(':')
            .next_back()
            .ok_or_else(|| BrokerError::OperationFailed("Invalid DLQ ARN format".to_string()))?;

        self.consume_batch(dlq_name, max_messages.clamp(1, 10), Duration::from_secs(20))
            .await
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
        let dlq_arn = self
            .dlq_config
            .as_ref()
            .ok_or_else(|| BrokerError::OperationFailed("DLQ not configured".to_string()))?
            .dlq_arn
            .clone();

        let dlq_name = dlq_arn
            .split(':')
            .next_back()
            .ok_or_else(|| BrokerError::OperationFailed("Invalid DLQ ARN format".to_string()))?;

        // Republish to main queue
        self.publish(&self.queue_name.clone(), envelope.message.clone())
            .await?;

        // Delete from DLQ
        self.ack(&envelope.delivery_tag).await?;

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
        let dlq_arn = self
            .dlq_config
            .as_ref()
            .ok_or_else(|| BrokerError::OperationFailed("DLQ not configured".to_string()))?
            .dlq_arn
            .clone();

        let dlq_name = dlq_arn
            .split(':')
            .next_back()
            .ok_or_else(|| BrokerError::OperationFailed("Invalid DLQ ARN format".to_string()))?;

        self.get_queue_stats(dlq_name).await
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
}

// --- Trait Implementations ---

#[async_trait]
impl Transport for SqsBroker {
    async fn connect(&mut self) -> Result<()> {
        info!("Connecting to AWS SQS: {}", self.queue_name);

        // Initialize client
        let _ = self.get_client().await?;

        // Get or create queue URL and cache it (clone queue_name to avoid borrow conflict)
        let queue_name = self.queue_name.clone();
        let _ = self.get_queue_url(&queue_name).await?;

        info!("Connected to SQS queue: {}", queue_name);
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        self.client = None;
        self.queue_url_cache.clear();
        info!("Disconnected from SQS");
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.client.is_some() && !self.queue_url_cache.is_empty()
    }

    fn name(&self) -> &str {
        "sqs"
    }
}

#[async_trait]
impl Producer for SqsBroker {
    async fn publish(&mut self, queue: &str, message: Message) -> Result<()> {
        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        // Serialize message to JSON
        let mut body = serde_json::to_string(&message)
            .map_err(|e| BrokerError::Serialization(e.to_string()))?;

        // Apply compression if enabled and message exceeds threshold
        if let Some(threshold) = self.compression_threshold {
            let original_size = body.len();
            if original_size > threshold {
                body = self.compress_message(&body)?;
                debug!(
                    "Compressed message from {} to {} bytes",
                    original_size,
                    body.len()
                );
            }
        }

        // Build message attributes
        let attributes = if let Some(ref mapper) = self.celery_mapper {
            // Celery compatibility mode: map all Celery headers to SQS attributes
            mapper
                .serialize_message(&message)
                .map_err(|e| BrokerError::OperationFailed(e.to_string()))?
        } else {
            // Standard mode: only map priority and correlation_id
            let mut attrs = HashMap::new();

            // Add priority as message attribute
            if let Some(priority) = message.properties.priority {
                attrs.insert(
                    "priority".to_string(),
                    MessageAttributeValue::builder()
                        .data_type("Number")
                        .string_value(priority.to_string())
                        .build()
                        .map_err(|e| BrokerError::OperationFailed(e.to_string()))?,
                );
            }

            // Add correlation ID
            if let Some(ref correlation_id) = message.properties.correlation_id {
                attrs.insert(
                    "correlation_id".to_string(),
                    MessageAttributeValue::builder()
                        .data_type("String")
                        .string_value(correlation_id)
                        .build()
                        .map_err(|e| BrokerError::OperationFailed(e.to_string()))?,
                );
            }

            attrs
        };

        // Send message with retry logic
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
                .map_err(|e| BrokerError::OperationFailed(format!("Failed to send message: {}", e)))
        };

        self.retry_with_backoff(send_operation).await?;

        debug!("Published message to SQS queue: {}", queue);
        Ok(())
    }

    async fn publish_with_routing(
        &mut self,
        _exchange: &str,
        routing_key: &str,
        message: Message,
    ) -> Result<()> {
        // SQS doesn't have exchanges, route to queue directly
        warn!(
            "SQS doesn't support exchanges, routing to queue: {}",
            routing_key
        );
        self.publish(routing_key, message).await
    }
}

#[async_trait]
impl Consumer for SqsBroker {
    async fn consume(&mut self, queue: &str, timeout: Duration) -> Result<Option<Envelope>> {
        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        // Determine wait time based on adaptive polling configuration
        let wait_time = if let Some(ref mut adaptive) = self.adaptive_polling {
            adaptive.current_wait_time()
        } else {
            timeout.as_secs().min(20) as i32
        };

        // Receive message with long polling
        let result = client
            .receive_message()
            .queue_url(&queue_url)
            .max_number_of_messages(1)
            .visibility_timeout(self.visibility_timeout)
            .wait_time_seconds(wait_time)
            .message_attribute_names("All")
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to receive message: {}", e))
            })?;

        let received_messages = result
            .messages
            .as_ref()
            .map(|m| !m.is_empty())
            .unwrap_or(false);

        // Adjust adaptive polling based on result
        if let Some(ref mut adaptive) = self.adaptive_polling {
            adaptive.adjust_wait_time(received_messages);
        }

        if let Some(messages) = result.messages {
            if let Some(sqs_message) = messages.into_iter().next() {
                let body = sqs_message.body().ok_or_else(|| {
                    BrokerError::OperationFailed("Message has no body".to_string())
                })?;

                let receipt_handle = sqs_message
                    .receipt_handle()
                    .ok_or_else(|| {
                        BrokerError::OperationFailed("Message has no receipt handle".to_string())
                    })?
                    .to_string();

                // Decompress message if it was compressed
                let decompressed_body = self.decompress_message(body)?;

                // Deserialize message
                let message: Message = serde_json::from_str(&decompressed_body)
                    .map_err(|e| BrokerError::Serialization(e.to_string()))?;

                let envelope = Envelope {
                    delivery_tag: receipt_handle,
                    message,
                    redelivered: sqs_message.attributes().is_some_and(|attrs| {
                        attrs.get(&aws_sdk_sqs::types::MessageSystemAttributeName::ApproximateReceiveCount)
                            .and_then(|count| count.parse::<i32>().ok())
                            .map(|count| count > 1)
                            .unwrap_or(false)
                    }),
                };

                debug!("Consumed message from SQS queue: {}", queue);
                return Ok(Some(envelope));
            }
        }

        // No message received within timeout
        Ok(None)
    }

    async fn ack(&mut self, delivery_tag: &str) -> Result<()> {
        let client = self.get_client().await?;
        let queue_name = self.queue_name.clone();
        let queue_url = self.get_queue_url(&queue_name).await?;

        client
            .delete_message()
            .queue_url(&queue_url)
            .receipt_handle(delivery_tag)
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to delete message: {}", e))
            })?;

        debug!("Acknowledged message: {}", delivery_tag);
        Ok(())
    }

    async fn reject(&mut self, delivery_tag: &str, requeue: bool) -> Result<()> {
        let client = self.get_client().await?;
        let queue_name = self.queue_name.clone();
        let queue_url = self.get_queue_url(&queue_name).await?;

        if requeue {
            // Change visibility timeout to 0 to make message immediately available
            client
                .change_message_visibility()
                .queue_url(&queue_url)
                .receipt_handle(delivery_tag)
                .visibility_timeout(0)
                .send()
                .await
                .map_err(|e| {
                    BrokerError::OperationFailed(format!("Failed to requeue message: {}", e))
                })?;

            debug!("Rejected and requeued message: {}", delivery_tag);
        } else {
            // Delete message (don't requeue)
            client
                .delete_message()
                .queue_url(&queue_url)
                .receipt_handle(delivery_tag)
                .send()
                .await
                .map_err(|e| {
                    BrokerError::OperationFailed(format!("Failed to delete message: {}", e))
                })?;

            debug!("Rejected and deleted message: {}", delivery_tag);
        }

        Ok(())
    }

    async fn queue_size(&mut self, queue: &str) -> Result<usize> {
        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        let result = client
            .get_queue_attributes()
            .queue_url(&queue_url)
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to get queue attributes: {}", e))
            })?;

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
        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        // Get current size before purge
        let size = self.queue_size(queue).await?;

        client
            .purge_queue()
            .queue_url(&queue_url)
            .send()
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to purge queue: {}", e)))?;

        debug!("Purged SQS queue: {}", queue);
        Ok(size)
    }

    async fn create_queue(&mut self, queue: &str, mode: QueueMode) -> Result<()> {
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

    async fn delete_queue(&mut self, queue: &str) -> Result<()> {
        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        client
            .delete_queue()
            .queue_url(&queue_url)
            .send()
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to delete queue: {}", e)))?;

        // Remove from cache
        self.queue_url_cache.remove(queue);

        debug!("Deleted SQS queue: {}", queue);
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
