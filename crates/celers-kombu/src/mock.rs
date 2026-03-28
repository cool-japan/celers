//! Mock broker implementation for testing.

use async_trait::async_trait;
use celers_protocol::Message;
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

use crate::{
    BatchConsumer, BatchProducer, BatchPublishResult, Broker, BrokerError, BrokerMetrics, Consumer,
    Envelope, HealthCheck, HealthCheckResponse, MetricsProvider, MiddlewareConsumer,
    MiddlewareProducer, Producer, QueueMode, Result, Transport,
};

// Mock Broker Implementation (for testing)
// =============================================================================

/// Mock broker for testing
#[derive(Debug)]
pub struct MockBroker {
    connected: bool,
    queues: HashMap<String, Vec<Envelope>>,
    pending_acks: HashMap<String, Message>,
    metrics: BrokerMetrics,
}

impl Default for MockBroker {
    fn default() -> Self {
        Self::new()
    }
}

impl MockBroker {
    /// Create a new mock broker
    pub fn new() -> Self {
        Self {
            connected: false,
            queues: HashMap::new(),
            pending_acks: HashMap::new(),
            metrics: BrokerMetrics::new(),
        }
    }

    /// Get number of messages in a queue
    pub fn queue_len(&self, queue: &str) -> usize {
        self.queues.get(queue).map(|q| q.len()).unwrap_or(0)
    }

    /// Get all queue names
    pub fn queue_names(&self) -> Vec<String> {
        self.queues.keys().cloned().collect()
    }
}

#[async_trait]
impl Transport for MockBroker {
    async fn connect(&mut self) -> Result<()> {
        self.metrics.inc_connection_attempt();
        self.connected = true;
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        self.connected = false;
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.connected
    }

    fn name(&self) -> &str {
        "mock"
    }
}

#[async_trait]
impl Producer for MockBroker {
    async fn publish(&mut self, queue: &str, message: Message) -> Result<()> {
        if !self.connected {
            self.metrics.inc_publish_error();
            return Err(BrokerError::Connection("Not connected".to_string()));
        }

        let tag = Uuid::new_v4().to_string();
        let envelope = Envelope::new(message, tag);

        self.queues
            .entry(queue.to_string())
            .or_default()
            .push(envelope);

        self.metrics.inc_published();
        Ok(())
    }

    async fn publish_with_routing(
        &mut self,
        _exchange: &str,
        routing_key: &str,
        message: Message,
    ) -> Result<()> {
        // For mock, just use routing_key as queue name
        self.publish(routing_key, message).await
    }
}

#[async_trait]
impl Consumer for MockBroker {
    async fn consume(&mut self, queue: &str, _timeout: Duration) -> Result<Option<Envelope>> {
        if !self.connected {
            self.metrics.inc_consume_error();
            return Err(BrokerError::Connection("Not connected".to_string()));
        }

        let envelope = self.queues.get_mut(queue).and_then(|q| {
            if q.is_empty() {
                None
            } else {
                Some(q.remove(0))
            }
        });

        if let Some(ref env) = envelope {
            self.pending_acks
                .insert(env.delivery_tag.clone(), env.message.clone());
            self.metrics.inc_consumed();
        }

        Ok(envelope)
    }

    async fn ack(&mut self, delivery_tag: &str) -> Result<()> {
        if self.pending_acks.remove(delivery_tag).is_some() {
            self.metrics.inc_acknowledged();
            Ok(())
        } else {
            Err(BrokerError::MessageNotFound(Uuid::nil()))
        }
    }

    async fn reject(&mut self, delivery_tag: &str, requeue: bool) -> Result<()> {
        if let Some(message) = self.pending_acks.remove(delivery_tag) {
            self.metrics.inc_rejected();
            if requeue {
                // Requeue to the default queue (we don't track which queue it came from)
                let tag = Uuid::new_v4().to_string();
                let mut envelope = Envelope::new(message, tag);
                envelope.redelivered = true;
                self.queues
                    .entry("celery".to_string())
                    .or_default()
                    .push(envelope);
            }
            Ok(())
        } else {
            Err(BrokerError::MessageNotFound(Uuid::nil()))
        }
    }

    async fn queue_size(&mut self, queue: &str) -> Result<usize> {
        Ok(self.queue_len(queue))
    }
}

#[async_trait]
impl Broker for MockBroker {
    async fn purge(&mut self, queue: &str) -> Result<usize> {
        let count = self.queue_len(queue);
        if let Some(q) = self.queues.get_mut(queue) {
            q.clear();
        }
        Ok(count)
    }

    async fn create_queue(&mut self, queue: &str, _mode: QueueMode) -> Result<()> {
        self.queues.entry(queue.to_string()).or_default();
        Ok(())
    }

    async fn delete_queue(&mut self, queue: &str) -> Result<()> {
        self.queues.remove(queue);
        Ok(())
    }

    async fn list_queues(&mut self) -> Result<Vec<String>> {
        Ok(self.queue_names())
    }
}

#[async_trait]
impl BatchProducer for MockBroker {
    async fn publish_batch(
        &mut self,
        queue: &str,
        messages: Vec<Message>,
    ) -> Result<BatchPublishResult> {
        let count = messages.len();
        for message in messages {
            self.publish(queue, message).await?;
        }
        Ok(BatchPublishResult::success(count))
    }

    async fn publish_batch_with_routing(
        &mut self,
        exchange: &str,
        routing_key: &str,
        messages: Vec<Message>,
    ) -> Result<BatchPublishResult> {
        let count = messages.len();
        for message in messages {
            self.publish_with_routing(exchange, routing_key, message)
                .await?;
        }
        Ok(BatchPublishResult::success(count))
    }
}

#[async_trait]
impl BatchConsumer for MockBroker {
    async fn consume_batch(
        &mut self,
        queue: &str,
        max_messages: usize,
        timeout: Duration,
    ) -> Result<Vec<Envelope>> {
        let mut results = Vec::new();
        for _ in 0..max_messages {
            if let Some(envelope) = self.consume(queue, timeout).await? {
                results.push(envelope);
            } else {
                break;
            }
        }
        Ok(results)
    }

    async fn ack_batch(&mut self, delivery_tags: &[String]) -> Result<()> {
        for tag in delivery_tags {
            self.ack(tag).await?;
        }
        Ok(())
    }

    async fn reject_batch(&mut self, delivery_tags: &[String], requeue: bool) -> Result<()> {
        for tag in delivery_tags {
            self.reject(tag, requeue).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl HealthCheck for MockBroker {
    async fn health_check(&self) -> HealthCheckResponse {
        if self.connected {
            HealthCheckResponse::healthy("mock", "mock://localhost")
        } else {
            HealthCheckResponse::unhealthy("mock", "mock://localhost", "Not connected")
        }
    }

    async fn ping(&self) -> bool {
        self.connected
    }
}

#[async_trait]
impl MetricsProvider for MockBroker {
    async fn get_metrics(&self) -> BrokerMetrics {
        self.metrics.clone()
    }

    async fn reset_metrics(&mut self) {
        self.metrics = BrokerMetrics::new();
    }
}

// Blanket implementations for middleware traits
impl<T: Producer> MiddlewareProducer for T {}
impl<T: Consumer> MiddlewareConsumer for T {}
