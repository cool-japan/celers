//! Batch publishing example
//!
//! This example demonstrates:
//! - Publishing multiple messages in a single batch
//! - Improved throughput using batch operations
//! - Publisher confirms
//!
//! To run: cargo run --example batch_publish
//!
//! Prerequisites:
//! - RabbitMQ running on localhost:5672

use celers_broker_amqp::{AmqpBroker, AmqpConfig};
use celers_kombu::{Consumer, QueueMode, Transport};
use celers_protocol::builder::MessageBuilder;
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("=== Batch Publishing Example ===\n");

    let config = AmqpConfig::default().with_prefetch(100);

    let mut broker =
        AmqpBroker::with_config("amqp://localhost:5672", "example_batch", config).await?;

    println!("Connecting to RabbitMQ...");
    broker.connect().await?;
    println!("Connected!\n");

    let queue_name = "example_batch_queue";
    println!("Declaring queue: {}", queue_name);
    broker.declare_queue(queue_name, QueueMode::Fifo).await?;

    // Create a batch of messages
    let batch_size = 100;
    println!("\nCreating {} messages...", batch_size);
    let mut messages = Vec::new();
    for i in 0..batch_size {
        let message = MessageBuilder::new(format!("tasks.batch_{}", i))
            .args(vec![i.into()])
            .build()?;
        messages.push(message);
    }

    // Publish using batch operation
    println!("Publishing batch of {} messages...", batch_size);
    let start = Instant::now();
    let published_count = broker.publish_batch(queue_name, messages).await?;
    let duration = start.elapsed();

    println!("Published {} messages in {:?}", published_count, duration);
    println!(
        "Throughput: {:.2} msg/sec\n",
        published_count as f64 / duration.as_secs_f64()
    );

    // Consume and count messages
    println!("Consuming messages...");
    let mut consumed_count = 0;
    let start = Instant::now();

    while let Some(envelope) = broker
        .consume(queue_name, Duration::from_millis(100))
        .await?
    {
        broker.ack(&envelope.delivery_tag).await?;
        consumed_count += 1;
        if consumed_count >= batch_size {
            break;
        }
    }

    let duration = start.elapsed();
    println!("Consumed {} messages in {:?}", consumed_count, duration);
    println!(
        "Throughput: {:.2} msg/sec\n",
        consumed_count as f64 / duration.as_secs_f64()
    );

    // Cleanup
    println!("Disconnecting...");
    broker.disconnect().await?;
    println!("Disconnected!");

    Ok(())
}
