//! Streaming consumer example
//!
//! This example demonstrates:
//! - Starting a streaming consumer
//! - Processing messages asynchronously
//! - Canceling a consumer
//!
//! To run: cargo run --example streaming_consumer
//!
//! Prerequisites:
//! - RabbitMQ running on localhost:5672

use celers_broker_amqp::{AmqpBroker, AmqpConfig};
use celers_kombu::{Producer, QueueMode, Transport};
use celers_protocol::builder::MessageBuilder;
use futures::StreamExt;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("=== Streaming Consumer Example ===\n");

    let config = AmqpConfig::default().with_prefetch(10);

    let mut broker =
        AmqpBroker::with_config("amqp://localhost:5672", "example_stream", config).await?;

    println!("Connecting to RabbitMQ...");
    broker.connect().await?;
    println!("Connected!\n");

    let queue_name = "example_stream_queue";
    println!("Declaring queue: {}", queue_name);
    broker.declare_queue(queue_name, QueueMode::Fifo).await?;
    println!();

    // Publish some messages
    println!("Publishing 10 messages...");
    for i in 1..=10 {
        let message = MessageBuilder::new(format!("tasks.stream_{}", i))
            .args(vec![i.into()])
            .build()?;
        broker.publish(queue_name, message).await?;
    }
    println!("Messages published!\n");

    // Wait for messages to be available
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Start streaming consumer
    println!("Starting streaming consumer with tag 'example_consumer'...");
    let consumer_tag = "example_consumer";
    let mut consumer = broker.start_consumer(queue_name, consumer_tag).await?;
    println!("Consumer started!\n");

    println!("Processing messages from stream:");
    let mut count = 0;
    let max_messages = 10;

    while let Some(delivery_result) = consumer.next().await {
        match delivery_result {
            Ok(delivery) => {
                // Deserialize message
                match serde_json::from_slice::<celers_protocol::Message>(&delivery.data) {
                    Ok(message) => {
                        count += 1;
                        println!(
                            "  [{}] Received: {} (task: {})",
                            count, message.headers.id, message.headers.task
                        );

                        // Acknowledge the message
                        delivery.ack(Default::default()).await?;

                        // Stop after processing all messages
                        if count >= max_messages {
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to deserialize message: {}", e);
                        delivery.nack(Default::default()).await?;
                    }
                }
            }
            Err(e) => {
                eprintln!("Error receiving message: {}", e);
                break;
            }
        }
    }

    println!("\nProcessed {} messages", count);

    // Cancel the consumer
    println!("\nCanceling consumer...");
    broker.cancel_consumer(consumer_tag).await?;
    println!("Consumer canceled!");

    // Cleanup
    println!("\nDisconnecting...");
    broker.disconnect().await?;
    println!("Disconnected!");

    Ok(())
}
