//! Transaction example
//!
//! This example demonstrates:
//! - Starting AMQP transactions
//! - Publishing messages within a transaction
//! - Committing transactions
//! - Rolling back transactions
//!
//! To run: cargo run --example transaction
//!
//! Prerequisites:
//! - RabbitMQ running on localhost:5672

use celers_broker_amqp::{AmqpBroker, AmqpConfig};
use celers_kombu::{Consumer, Producer, QueueMode, Transport};
use celers_protocol::builder::MessageBuilder;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("=== Transaction Example ===\n");

    let config = AmqpConfig::default();
    let mut broker =
        AmqpBroker::with_config("amqp://localhost:5672", "example_transaction", config).await?;

    println!("Connecting to RabbitMQ...");
    broker.connect().await?;
    println!("Connected!\n");

    let queue_name = "example_transaction_queue";
    println!("Declaring queue: {}", queue_name);
    broker.declare_queue(queue_name, QueueMode::Fifo).await?;
    println!();

    // Example 1: Commit transaction
    println!("=== Example 1: Commit Transaction ===");
    println!("Starting transaction...");
    broker.start_transaction().await?;
    println!("Transaction state: {:?}", broker.transaction_state());

    println!("Publishing message within transaction...");
    let message = MessageBuilder::new("tasks.commit")
        .args(vec!["data1".into()])
        .build()?;
    broker.publish(queue_name, message).await?;

    println!("Committing transaction...");
    broker.commit_transaction().await?;
    println!("Transaction committed!");
    println!("Transaction state: {:?}\n", broker.transaction_state());

    // Verify message was committed
    tokio::time::sleep(Duration::from_millis(100)).await;
    if let Some(envelope) = broker.consume(queue_name, Duration::from_secs(1)).await? {
        println!("Message found in queue: {}", envelope.message.headers.task);
        broker.ack(&envelope.delivery_tag).await?;
        println!("(Transaction was successfully committed)\n");
    }

    // Example 2: Rollback transaction
    println!("=== Example 2: Rollback Transaction ===");
    println!("Starting transaction...");
    broker.start_transaction().await?;
    println!("Transaction state: {:?}", broker.transaction_state());

    println!("Publishing message within transaction...");
    let message = MessageBuilder::new("tasks.rollback")
        .args(vec!["data2".into()])
        .build()?;
    broker.publish(queue_name, message).await?;

    println!("Rolling back transaction...");
    broker.rollback_transaction().await?;
    println!("Transaction rolled back!");
    println!("Transaction state: {:?}\n", broker.transaction_state());

    // Verify message was NOT committed
    tokio::time::sleep(Duration::from_millis(100)).await;
    if let Some(envelope) = broker
        .consume(queue_name, Duration::from_millis(500))
        .await?
    {
        println!(
            "Unexpected message found: {}",
            envelope.message.headers.task
        );
        broker.ack(&envelope.delivery_tag).await?;
    } else {
        println!("No message in queue (as expected - transaction was rolled back)\n");
    }

    // Example 3: Multiple operations in transaction
    println!("=== Example 3: Multiple Operations in Transaction ===");
    println!("Starting transaction...");
    broker.start_transaction().await?;

    println!("Publishing 3 messages within transaction...");
    for i in 1..=3 {
        let message = MessageBuilder::new(format!("tasks.multi_{}", i)).build()?;
        broker.publish(queue_name, message).await?;
        println!("  Published message {}", i);
    }

    println!("Committing transaction...");
    broker.commit_transaction().await?;
    println!("All 3 messages committed!\n");

    // Verify all messages were committed
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut count = 0;
    while let Some(envelope) = broker
        .consume(queue_name, Duration::from_millis(500))
        .await?
    {
        count += 1;
        println!(
            "Received message {}: {}",
            count, envelope.message.headers.task
        );
        broker.ack(&envelope.delivery_tag).await?;
    }
    println!("Total messages received: {}\n", count);

    // Cleanup
    println!("Disconnecting...");
    broker.disconnect().await?;
    println!("Disconnected!");

    Ok(())
}
