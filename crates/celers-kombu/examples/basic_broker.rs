//! Basic broker usage example
//!
//! This example demonstrates how to implement a simple broker and perform
//! basic publish/consume operations.
//!
//! Run with: cargo run --example basic_broker

use celers_kombu::*;
use celers_protocol::Message;
use std::time::Duration;
use uuid::Uuid;

// Simple in-memory broker implementation
struct SimpleBroker {
    connected: bool,
    messages: Vec<(String, Message)>,
}

impl SimpleBroker {
    fn new() -> Self {
        Self {
            connected: false,
            messages: Vec::new(),
        }
    }
}

#[async_trait::async_trait]
impl Transport for SimpleBroker {
    async fn connect(&mut self) -> Result<()> {
        println!("📡 Connecting to broker...");
        self.connected = true;
        println!("✅ Connected!");
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        println!("📡 Disconnecting from broker...");
        self.connected = false;
        println!("✅ Disconnected!");
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.connected
    }

    fn name(&self) -> &str {
        "simple-memory-broker"
    }
}

#[async_trait::async_trait]
impl Producer for SimpleBroker {
    async fn publish(&mut self, queue: &str, message: Message) -> Result<()> {
        if !self.connected {
            return Err(BrokerError::Connection("Not connected".to_string()));
        }
        println!("📤 Publishing message to queue '{}'", queue);
        println!("   Task: {}", message.task_name());
        println!("   ID: {}", message.task_id());
        self.messages.push((queue.to_string(), message));
        Ok(())
    }

    async fn publish_with_routing(
        &mut self,
        exchange: &str,
        routing_key: &str,
        message: Message,
    ) -> Result<()> {
        println!(
            "📤 Publishing to exchange '{}' with routing key '{}'",
            exchange, routing_key
        );
        self.publish(routing_key, message).await
    }
}

#[async_trait::async_trait]
impl Consumer for SimpleBroker {
    async fn consume(&mut self, queue: &str, _timeout: Duration) -> Result<Option<Envelope>> {
        if !self.connected {
            return Err(BrokerError::Connection("Not connected".to_string()));
        }

        if let Some(idx) = self.messages.iter().position(|(q, _)| q == queue) {
            let (_, message) = self.messages.remove(idx);
            println!("📥 Consumed message from queue '{}'", queue);
            println!("   Task: {}", message.task_name());
            println!("   ID: {}", message.task_id());
            Ok(Some(Envelope::new(message, format!("delivery-{}", idx))))
        } else {
            Ok(None)
        }
    }

    async fn ack(&mut self, delivery_tag: &str) -> Result<()> {
        println!("✅ Acknowledged message: {}", delivery_tag);
        Ok(())
    }

    async fn reject(&mut self, delivery_tag: &str, requeue: bool) -> Result<()> {
        println!(
            "❌ Rejected message: {} (requeue: {})",
            delivery_tag, requeue
        );
        Ok(())
    }

    async fn queue_size(&mut self, queue: &str) -> Result<usize> {
        let count = self.messages.iter().filter(|(q, _)| q == queue).count();
        Ok(count)
    }
}

#[async_trait::async_trait]
impl Broker for SimpleBroker {
    async fn purge(&mut self, queue: &str) -> Result<usize> {
        let before = self.messages.len();
        self.messages.retain(|(q, _)| q != queue);
        let removed = before - self.messages.len();
        println!("🗑️  Purged {} messages from queue '{}'", removed, queue);
        Ok(removed)
    }

    async fn create_queue(&mut self, queue: &str, mode: QueueMode) -> Result<()> {
        println!("➕ Created queue '{}' with mode: {}", queue, mode);
        Ok(())
    }

    async fn delete_queue(&mut self, queue: &str) -> Result<()> {
        self.purge(queue).await?;
        println!("❌ Deleted queue '{}'", queue);
        Ok(())
    }

    async fn list_queues(&mut self) -> Result<Vec<String>> {
        let mut queues: Vec<String> = self.messages.iter().map(|(q, _)| q.clone()).collect();
        queues.sort();
        queues.dedup();
        Ok(queues)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("🚀 Basic Broker Example\n");

    // Create and connect to broker
    let mut broker = SimpleBroker::new();
    broker.connect().await?;

    println!("\n📋 Queue Configuration");
    let config = QueueConfig::new("tasks".to_string())
        .with_mode(QueueMode::Fifo)
        .with_durable(true)
        .with_ttl(Duration::from_secs(3600));
    println!("   Queue: {}", config.name);
    println!("   Mode: {}", config.mode);
    println!("   Durable: {}", config.durable);
    println!("   TTL: {:?}", config.message_ttl);

    // Create queue
    broker.create_queue("tasks", QueueMode::Fifo).await?;

    println!("\n📤 Publishing Messages");
    // Publish some messages
    for i in 1..=3 {
        let task_id = Uuid::new_v4();
        let message = Message::new(
            format!("task_{}", i),
            task_id,
            format!("payload_{}", i).into_bytes(),
        );
        broker.publish("tasks", message).await?;
    }

    // Check queue size
    let size = broker.queue_size("tasks").await?;
    println!("\n📊 Queue size: {} messages", size);

    println!("\n📥 Consuming Messages");
    // Consume messages
    while let Some(envelope) = broker.consume("tasks", Duration::from_secs(1)).await? {
        println!("   Processing: {}", envelope.message.task_name());
        broker.ack(&envelope.delivery_tag).await?;
    }

    // Cleanup
    broker.delete_queue("tasks").await?;
    broker.disconnect().await?;

    println!("\n✨ Example completed successfully!");
    Ok(())
}
