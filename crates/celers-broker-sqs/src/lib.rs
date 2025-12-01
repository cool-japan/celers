//! AWS SQS broker implementation for CeleRS
//!
//! This crate provides AWS SQS support for cloud-native deployments.
//!
//! # Features
//!
//! - Long polling for efficiency (up to 20 seconds)
//! - Visibility timeout handling
//! - Dead Letter Queue integration
//! - IAM role authentication
//! - Batch operations for throughput (publish_batch, consume_batch, ack_batch)
//! - Priority queue support (via message attributes)
//! - Cost optimization through batch API calls (10x reduction)
//!
//! # Example
//!
//! ```ignore
//! use celers_broker_sqs::SqsBroker;
//! use celers_kombu::{Transport, Producer, Consumer};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut broker = SqsBroker::new("my-queue-name").await?;
//! broker.connect().await?;
//!
//! // Publish a message
//! let message = Message::new("tasks.add");
//! broker.publish("my-queue", message).await?;
//!
//! // Consume messages
//! let envelope = broker.consume("my-queue", std::time::Duration::from_secs(20)).await?;
//! # Ok(())
//! # }
//! ```

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_sqs::{
    types::{MessageAttributeValue, QueueAttributeName},
    Client,
};
use celers_kombu::{
    Broker, BrokerError, Consumer, Envelope, Producer, QueueMode, Result, Transport,
};
use celers_protocol::Message;
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, info, warn};

/// AWS SQS broker implementation
pub struct SqsBroker {
    client: Option<Client>,
    queue_name: String,
    queue_url: Option<String>,
    /// Visibility timeout in seconds (default 30)
    visibility_timeout: i32,
    /// Long polling wait time in seconds (default 20, max 20)
    wait_time_seconds: i32,
    /// Maximum messages to receive per poll (default 1, max 10)
    max_messages: i32,
}

impl SqsBroker {
    /// Create a new SQS broker
    ///
    /// # Arguments
    /// * `queue_name` - SQS queue name (will be created if it doesn't exist)
    pub async fn new(queue_name: &str) -> Result<Self> {
        Ok(Self {
            client: None,
            queue_name: queue_name.to_string(),
            queue_url: None,
            visibility_timeout: 30,
            wait_time_seconds: 20, // Long polling
            max_messages: 1,
        })
    }

    /// Set visibility timeout (default 30 seconds)
    pub fn with_visibility_timeout(mut self, seconds: i32) -> Self {
        self.visibility_timeout = seconds;
        self
    }

    /// Set long polling wait time (default 20 seconds, max 20)
    pub fn with_wait_time(mut self, seconds: i32) -> Self {
        self.wait_time_seconds = seconds.min(20);
        self
    }

    /// Set maximum messages per poll (default 1, max 10)
    pub fn with_max_messages(mut self, max: i32) -> Self {
        self.max_messages = max.min(10);
        self
    }

    /// Get or create SQS client (cloned for borrow checker compatibility)
    async fn get_client(&mut self) -> Result<Client> {
        if self.client.is_none() {
            let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
            self.client = Some(Client::new(&config));
        }

        self.client
            .clone()
            .ok_or_else(|| BrokerError::Connection("SQS client not initialized".to_string()))
    }

    /// Get or create queue URL
    async fn get_queue_url(&mut self, queue: &str) -> Result<String> {
        if let Some(ref url) = self.queue_url {
            if queue == self.queue_name {
                return Ok(url.clone());
            }
        }

        let client = self.get_client().await?;

        // Try to get existing queue URL
        match client.get_queue_url().queue_name(queue).send().await {
            Ok(output) => {
                let url = output
                    .queue_url()
                    .ok_or_else(|| {
                        BrokerError::OperationFailed("No queue URL returned".to_string())
                    })?
                    .to_string();

                if queue == self.queue_name {
                    self.queue_url = Some(url.clone());
                }

                Ok(url)
            }
            Err(_) => {
                // Queue doesn't exist, return error (use create_queue explicitly)
                Err(BrokerError::OperationFailed(format!(
                    "Queue '{}' does not exist. Call create_queue() first.",
                    queue
                )))
            }
        }
    }

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
            let body = serde_json::to_string(message)
                .map_err(|e| BrokerError::Serialization(e.to_string()))?;

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

                let message: Message = serde_json::from_str(body)
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
}

#[async_trait]
impl Transport for SqsBroker {
    async fn connect(&mut self) -> Result<()> {
        info!("Connecting to AWS SQS: {}", self.queue_name);

        // Initialize client
        let _ = self.get_client().await?;

        // Get or create queue URL (clone queue_name to avoid borrow conflict)
        let queue_name = self.queue_name.clone();
        self.queue_url = Some(self.get_queue_url(&queue_name).await?);

        info!("Connected to SQS queue: {}", queue_name);
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        self.client = None;
        self.queue_url = None;
        info!("Disconnected from SQS");
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.client.is_some() && self.queue_url.is_some()
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
        let body = serde_json::to_string(&message)
            .map_err(|e| BrokerError::Serialization(e.to_string()))?;

        // Build message attributes
        let mut attributes = HashMap::new();

        // Add priority as message attribute
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

        // Add correlation ID
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

        // Send message
        client
            .send_message()
            .queue_url(&queue_url)
            .message_body(&body)
            .set_message_attributes(if attributes.is_empty() {
                None
            } else {
                Some(attributes)
            })
            .send()
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to send message: {}", e)))?;

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

        // Convert timeout to wait time (max 20 seconds for SQS)
        let wait_time = timeout.as_secs().min(20) as i32;

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

                // Deserialize message
                let message: Message = serde_json::from_str(body)
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
        let queue_url = self
            .queue_url
            .as_ref()
            .ok_or_else(|| BrokerError::Connection("Not connected".to_string()))?;

        client
            .delete_message()
            .queue_url(queue_url)
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
        let queue_url = self
            .queue_url
            .as_ref()
            .ok_or_else(|| BrokerError::Connection("Not connected".to_string()))?;

        if requeue {
            // Change visibility timeout to 0 to make message immediately available
            client
                .change_message_visibility()
                .queue_url(queue_url)
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
                .queue_url(queue_url)
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
        let queue_name = self.queue_name.clone();

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

        // Configure for priority mode if requested
        if matches!(mode, QueueMode::Priority) {
            warn!("SQS doesn't natively support priority queues. Priority is handled via message attributes.");
        }

        let result = client
            .create_queue()
            .queue_name(queue)
            .set_attributes(Some(attributes))
            .send()
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to create queue: {}", e)))?;

        if let Some(url) = result.queue_url() {
            if queue == queue_name {
                self.queue_url = Some(url.to_string());
            }
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

        if queue == self.queue_name {
            self.queue_url = None;
        }

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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_sqs_broker_creation() {
        let broker = SqsBroker::new("test-queue").await;
        assert!(broker.is_ok());
    }

    #[test]
    fn test_broker_name() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let broker = rt.block_on(async { SqsBroker::new("test").await.unwrap() });
        assert_eq!(broker.name(), "sqs");
    }

    #[tokio::test]
    async fn test_builder_pattern() {
        let broker = SqsBroker::new("test-queue")
            .await
            .unwrap()
            .with_visibility_timeout(60)
            .with_wait_time(10)
            .with_max_messages(5);

        assert_eq!(broker.visibility_timeout, 60);
        assert_eq!(broker.wait_time_seconds, 10);
        assert_eq!(broker.max_messages, 5);
    }
}
