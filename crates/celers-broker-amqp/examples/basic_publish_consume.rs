//! Basic publish and consume example
//!
//! This example demonstrates:
//! - Connecting to RabbitMQ
//! - Publishing messages to a queue
//! - Consuming messages from the queue
//! - Acknowledging messages
//!
//! To run: cargo run --example basic_publish_consume
//!
//! Prerequisites:
//! - RabbitMQ running on localhost:5672

use celers_broker_amqp::{AmqpBroker, AmqpConfig};
use celers_kombu::{Consumer, Producer, QueueMode, Transport};
use celers_protocol::builder::MessageBuilder;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for logging
    tracing_subscriber::fmt::init();

    println!("=== Basic Publish/Consume Example ===\n");

    // Create broker with configuration
    let config = AmqpConfig::default()
        .with_prefetch(10)
        .with_retry(3, Duration::from_secs(1));

    let mut broker =
        AmqpBroker::with_config("amqp://localhost:5672", "example_basic", config).await?;

    println!("Connecting to RabbitMQ...");
    broker.connect().await?;
    println!("Connected!\n");

    // Declare a queue
    let queue_name = "example_basic_queue";
    println!("Declaring queue: {}", queue_name);
    broker.declare_queue(queue_name, QueueMode::Fifo).await?;
    println!("Queue declared!\n");

    // Publish a message
    println!("Publishing message...");
    let message = MessageBuilder::new("tasks.example")
        .args(vec!["arg1".into(), "arg2".into()])
        .build()?;

    broker.publish(queue_name, message).await?;
    println!("Message published!\n");

    // Wait a moment for the message to be available
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Consume the message
    println!("Consuming message...");
    if let Some(envelope) = broker.consume(queue_name, Duration::from_secs(5)).await? {
        println!("Received message:");
        println!("  Task ID: {}", envelope.message.headers.id);
        println!("  Task Name: {}", envelope.message.headers.task);

        // Acknowledge the message
        broker.ack(&envelope.delivery_tag).await?;
        println!("Message acknowledged!\n");
    } else {
        println!("No message received\n");
    }

    // Cleanup
    println!("Disconnecting...");
    broker.disconnect().await?;
    println!("Disconnected!");

    Ok(())
}
