//! Example demonstrating modern RabbitMQ queue features
//!
//! This example shows how to use:
//! - Quorum queues (highly available, replicated)
//! - Lazy queues (for large queues)
//! - Queue overflow behaviors
//! - Single active consumer mode
//!
//! Prerequisites:
//! - RabbitMQ 3.8+ running on localhost:5672
//! - RabbitMQ cluster for quorum queues (optional)
//!
//! Run with:
//! ```bash
//! cargo run --example modern_queue_features
//! ```

use celers_broker_amqp::{
    AmqpBroker, QueueConfig, QueueLazyMode, QueueOverflowBehavior, QueueType,
};
use celers_kombu::{Broker, Consumer, Producer, Transport};
use celers_protocol::builder::MessageBuilder;
use std::time::Duration;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    info!("=== Modern RabbitMQ Queue Features Demo ===\n");

    // Example 1: Quorum Queue (High Availability)
    demonstrate_quorum_queue().await?;

    // Example 2: Lazy Queue (Large Queues)
    demonstrate_lazy_queue().await?;

    // Example 3: Overflow Behavior
    demonstrate_overflow_behavior().await?;

    // Example 4: Single Active Consumer
    demonstrate_single_active_consumer().await?;

    info!("\n=== Demo Complete ===");
    Ok(())
}

/// Demonstrate quorum queue for high availability
async fn demonstrate_quorum_queue() -> Result<(), Box<dyn std::error::Error>> {
    info!("--- Example 1: Quorum Queue (High Availability) ---");

    let mut broker = AmqpBroker::new("amqp://localhost:5672", "quorum_queue_demo").await?;

    match broker.connect().await {
        Ok(_) => {
            info!("✓ Connected to RabbitMQ");

            // Configure a quorum queue
            let config = QueueConfig::new()
                .durable(true)
                .with_queue_type(QueueType::Quorum)
                .with_max_length(10000)
                .with_overflow_behavior(QueueOverflowBehavior::RejectPublish);

            info!("✓ Configuring quorum queue:");
            info!("  - Replicated across cluster nodes");
            info!("  - Max length: 10,000 messages");
            info!("  - Overflow: Reject new publishes");

            broker
                .declare_queue_with_config("quorum_queue_demo", &config)
                .await?;

            info!("✓ Quorum queue declared successfully");

            // Publish a test message
            let msg = MessageBuilder::new("quorum.task")
                .args(vec![serde_json::json!({"data": "high availability"})])
                .build()?;

            broker.publish("quorum_queue_demo", msg).await?;
            info!("✓ Message published to quorum queue");

            // Cleanup
            let _ = broker.purge("quorum_queue_demo").await;
            broker.disconnect().await?;
            info!("");
        }
        Err(e) => {
            error!("✗ Failed to connect: {}", e);
            info!("  Note: Quorum queues require RabbitMQ 3.8+\n");
        }
    }

    Ok(())
}

/// Demonstrate lazy queue for handling large queues
async fn demonstrate_lazy_queue() -> Result<(), Box<dyn std::error::Error>> {
    info!("--- Example 2: Lazy Queue (Large Queues) ---");

    let mut broker = AmqpBroker::new("amqp://localhost:5672", "lazy_queue_demo").await?;

    match broker.connect().await {
        Ok(_) => {
            info!("✓ Connected to RabbitMQ");

            // Configure a lazy queue
            let config = QueueConfig::new()
                .durable(true)
                .with_queue_mode(QueueLazyMode::Lazy)
                .with_max_length(1_000_000);

            info!("✓ Configuring lazy queue:");
            info!("  - Messages moved to disk early");
            info!("  - Ideal for millions of messages");
            info!("  - Max length: 1,000,000 messages");

            broker
                .declare_queue_with_config("lazy_queue_demo", &config)
                .await?;

            info!("✓ Lazy queue declared successfully");

            // Publish test messages
            for i in 0..5 {
                let msg = MessageBuilder::new("lazy.task")
                    .args(vec![serde_json::json!({"batch": i})])
                    .build()?;
                broker.publish("lazy_queue_demo", msg).await?;
            }

            info!("✓ Published 5 messages to lazy queue");

            // Cleanup
            let _ = broker.purge("lazy_queue_demo").await;
            broker.disconnect().await?;
            info!("");
        }
        Err(e) => {
            error!("✗ Failed to connect: {}", e);
            info!("  Note: Lazy queues require RabbitMQ 3.6+\n");
        }
    }

    Ok(())
}

/// Demonstrate queue overflow behavior
async fn demonstrate_overflow_behavior() -> Result<(), Box<dyn std::error::Error>> {
    info!("--- Example 3: Queue Overflow Behavior ---");

    let mut broker = AmqpBroker::new("amqp://localhost:5672", "overflow_demo").await?;

    match broker.connect().await {
        Ok(_) => {
            info!("✓ Connected to RabbitMQ");

            // Configure queue with overflow behavior
            let config = QueueConfig::new()
                .with_max_length(3)
                .with_overflow_behavior(QueueOverflowBehavior::DropHead);

            info!("✓ Configuring queue with overflow:");
            info!("  - Max length: 3 messages");
            info!("  - Overflow: Drop oldest messages (drop-head)");

            broker
                .declare_queue_with_config("overflow_demo", &config)
                .await?;

            // Publish 5 messages (more than max)
            for i in 0..5 {
                let msg = MessageBuilder::new("overflow.task")
                    .args(vec![serde_json::json!({"number": i})])
                    .build()?;
                broker.publish("overflow_demo", msg).await?;
            }

            info!("✓ Published 5 messages (max is 3)");

            // Check queue size
            tokio::time::sleep(Duration::from_millis(100)).await;
            let size = broker.queue_size("overflow_demo").await?;
            info!("✓ Queue size after overflow: {} (oldest dropped)", size);

            // Cleanup
            let _ = broker.purge("overflow_demo").await;
            broker.disconnect().await?;
            info!("");
        }
        Err(e) => {
            error!("✗ Failed to connect: {}", e);
        }
    }

    Ok(())
}

/// Demonstrate single active consumer mode
async fn demonstrate_single_active_consumer() -> Result<(), Box<dyn std::error::Error>> {
    info!("--- Example 4: Single Active Consumer (Ordered Processing) ---");

    let mut broker = AmqpBroker::new("amqp://localhost:5672", "sac_demo").await?;

    match broker.connect().await {
        Ok(_) => {
            info!("✓ Connected to RabbitMQ");

            // Configure queue with single active consumer
            let config = QueueConfig::new()
                .with_single_active_consumer(true)
                .with_max_priority(10);

            info!("✓ Configuring single active consumer queue:");
            info!("  - Only one consumer receives messages at a time");
            info!("  - Ensures ordered message processing");
            info!("  - With priority support");

            broker
                .declare_queue_with_config("sac_demo", &config)
                .await?;

            info!("✓ Single active consumer queue declared");

            // Publish ordered messages
            for i in 0..5 {
                let msg = MessageBuilder::new("sac.task")
                    .args(vec![serde_json::json!({"order": i})])
                    .priority((5 - i) as u8) // Higher priority for earlier messages
                    .build()?;
                broker.publish("sac_demo", msg).await?;
            }

            info!("✓ Published 5 ordered messages with priorities");
            info!("  Note: With multiple consumers, only one will be active");

            // Cleanup
            let _ = broker.purge("sac_demo").await;
            broker.disconnect().await?;
            info!("");
        }
        Err(e) => {
            error!("✗ Failed to connect: {}", e);
            info!("  Note: Single active consumer requires RabbitMQ 3.8+\n");
        }
    }

    Ok(())
}
