//! Dead Letter Exchange (DLX) example
//!
//! This example demonstrates:
//! - Setting up a Dead Letter Exchange
//! - Rejecting messages to send them to DLX
//! - Processing dead-lettered messages
//!
//! To run: cargo run --example dead_letter_exchange
//!
//! Prerequisites:
//! - RabbitMQ running on localhost:5672

use celers_broker_amqp::{AmqpBroker, AmqpConfig, AmqpExchangeType, DlxConfig, QueueConfig};
use celers_kombu::{Consumer, Producer, QueueMode, Transport};
use celers_protocol::builder::MessageBuilder;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("=== Dead Letter Exchange Example ===\n");

    let config = AmqpConfig::default();
    let mut broker =
        AmqpBroker::with_config("amqp://localhost:5672", "example_dlx", config).await?;

    println!("Connecting to RabbitMQ...");
    broker.connect().await?;
    println!("Connected!\n");

    // Setup topology
    let main_queue = "example_main_queue";
    let dlx_exchange = "example_dlx_exchange";
    let dlx_queue = "example_dlx_queue";

    // 1. Declare DLX exchange
    println!("Setting up Dead Letter Exchange topology...");
    broker
        .declare_exchange(dlx_exchange, AmqpExchangeType::Direct)
        .await?;
    println!("  Created DLX exchange: {}", dlx_exchange);

    // 2. Declare DLX queue
    broker.declare_queue(dlx_queue, QueueMode::Fifo).await?;
    println!("  Created DLX queue: {}", dlx_queue);

    // 3. Bind DLX queue to DLX exchange
    broker
        .bind_queue(dlx_queue, dlx_exchange, dlx_queue)
        .await?;
    println!("  Bound DLX queue to exchange");

    // 4. Declare main queue with DLX configuration
    let queue_config =
        QueueConfig::new().with_dlx(DlxConfig::new(dlx_exchange).with_routing_key(dlx_queue));

    broker
        .declare_queue_with_config(main_queue, &queue_config)
        .await?;
    println!("  Created main queue: {} (with DLX)", main_queue);
    println!();

    // Publish a message to the main queue
    println!("Publishing message to main queue...");
    let message = MessageBuilder::new("tasks.example")
        .args(vec!["test".into()])
        .build()?;

    broker.publish(main_queue, message).await?;
    println!("Message published!\n");

    // Wait for message to be available
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Consume from main queue and reject it (send to DLX)
    println!("Consuming from main queue and rejecting...");
    if let Some(envelope) = broker.consume(main_queue, Duration::from_secs(1)).await? {
        println!("  Received message: {}", envelope.message.headers.id);
        println!("  Rejecting message (will go to DLX)...");
        broker.reject(&envelope.delivery_tag, false).await?;
        println!("  Message rejected!\n");
    }

    // Wait for message to be dead-lettered
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Consume from DLX queue
    println!("Consuming from DLX queue...");
    if let Some(envelope) = broker.consume(dlx_queue, Duration::from_secs(1)).await? {
        println!(
            "  Received dead-lettered message: {}",
            envelope.message.headers.id
        );
        println!("  Task: {}", envelope.message.headers.task);
        broker.ack(&envelope.delivery_tag).await?;
        println!("  Message acknowledged from DLX!\n");
    } else {
        println!("  No message in DLX queue\n");
    }

    // Cleanup
    println!("Disconnecting...");
    broker.disconnect().await?;
    println!("Disconnected!");

    Ok(())
}
