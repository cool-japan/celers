//! Priority queue example
//!
//! This example demonstrates:
//! - Creating a priority queue
//! - Publishing messages with different priorities
//! - Messages are consumed in priority order
//!
//! To run: cargo run --example priority_queue
//!
//! Prerequisites:
//! - RabbitMQ 3.5.0+ running on localhost:5672

use celers_broker_amqp::{AmqpBroker, AmqpConfig, QueueConfig};
use celers_kombu::{Consumer, Producer, Transport};
use celers_protocol::builder::MessageBuilder;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("=== Priority Queue Example ===\n");

    let config = AmqpConfig::default();
    let mut broker =
        AmqpBroker::with_config("amqp://localhost:5672", "example_priority", config).await?;

    println!("Connecting to RabbitMQ...");
    broker.connect().await?;
    println!("Connected!\n");

    // Create a priority queue with max priority 10
    let queue_name = "example_priority_queue";
    let queue_config = QueueConfig::new().with_max_priority(10);

    println!(
        "Declaring priority queue: {} (max priority: 10)",
        queue_name
    );
    broker
        .declare_queue_with_config(queue_name, &queue_config)
        .await?;
    println!("Priority queue declared!\n");

    // Publish messages with different priorities
    println!("Publishing messages with different priorities...");
    let priorities = vec![1, 5, 10, 3, 7];

    for priority in &priorities {
        let message = MessageBuilder::new(format!("tasks.priority_{}", priority))
            .priority(*priority)
            .build()?;

        broker.publish(queue_name, message).await?;
        println!("  Published message with priority {}", priority);
    }
    println!();

    // Wait for messages to settle
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Consume messages (should come out in priority order)
    println!("Consuming messages (expected order: 10, 7, 5, 3, 1):");
    let mut count = 0;
    while count < priorities.len() {
        if let Some(envelope) = broker.consume(queue_name, Duration::from_secs(1)).await? {
            let priority = envelope.message.properties.priority.unwrap_or(0);
            println!("  Received message with priority: {}", priority);
            broker.ack(&envelope.delivery_tag).await?;
            count += 1;
        } else {
            break;
        }
    }
    println!();

    // Cleanup
    println!("Disconnecting...");
    broker.disconnect().await?;
    println!("Disconnected!");

    Ok(())
}
