//! RabbitMQ Management API example
//!
//! This example demonstrates:
//! - Configuring Management API access
//! - Listing queues and their statistics
//! - Getting server overview
//! - Monitoring connections and channels
//!
//! To run: cargo run --example management_api
//!
//! Prerequisites:
//! - RabbitMQ running on localhost:5672
//! - RabbitMQ Management Plugin enabled (default)
//! - Management API accessible on localhost:15672

use celers_broker_amqp::{AmqpBroker, AmqpConfig};
use celers_kombu::{Producer, QueueMode, Transport};
use celers_protocol::builder::MessageBuilder;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("=== RabbitMQ Management API Example ===\n");

    // Configure broker with Management API access
    let config =
        AmqpConfig::default().with_management_api("http://localhost:15672", "guest", "guest");

    let mut broker =
        AmqpBroker::with_config("amqp://localhost:5672", "example_mgmt", config).await?;

    println!("Connecting to RabbitMQ...");
    broker.connect().await?;
    println!("Connected!\n");

    // Check if Management API is configured
    if !broker.has_management_api() {
        eprintln!("Management API not configured!");
        return Ok(());
    }

    // Create some test queues
    let queue_name = "example_mgmt_queue";
    println!("Creating test queue: {}", queue_name);
    broker.declare_queue(queue_name, QueueMode::Fifo).await?;

    // Publish some messages
    println!("Publishing test messages...");
    for i in 0..5 {
        let message = MessageBuilder::new(format!("tasks.mgmt_{}", i)).build()?;
        broker.publish(queue_name, message).await?;
    }
    println!("Published 5 messages\n");

    // Wait for statistics to update
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Get server overview
    println!("=== Server Overview ===");
    match broker.get_server_overview().await {
        Ok(overview) => {
            println!("RabbitMQ Version: {}", overview.rabbitmq_version);
            println!("Erlang Version: {}", overview.erlang_version);
            println!("Cluster Name: {}", overview.cluster_name);
            if let Some(totals) = overview.queue_totals {
                println!("Total Messages: {}", totals.messages);
                println!("Messages Ready: {}", totals.messages_ready);
            }
        }
        Err(e) => println!("Error getting server overview: {}", e),
    }
    println!();

    // List all queues
    println!("=== Queue List ===");
    match broker.list_queues().await {
        Ok(queues) => {
            println!("Found {} queues:", queues.len());
            for queue in queues.iter().take(10) {
                println!(
                    "  - {} ({} messages, {} consumers)",
                    queue.name, queue.messages, queue.consumers
                );
            }
            if queues.len() > 10 {
                println!("  ... and {} more", queues.len() - 10);
            }
        }
        Err(e) => println!("Error listing queues: {}", e),
    }
    println!();

    // Get detailed queue statistics
    println!("=== Queue Statistics for '{}' ===", queue_name);
    match broker.get_queue_stats(queue_name).await {
        Ok(stats) => {
            println!("Messages: {}", stats.messages);
            println!("Messages Ready: {}", stats.messages_ready);
            println!("Messages Unacknowledged: {}", stats.messages_unacknowledged);
            println!("Durable: {}", stats.durable);
            if let Some(msg_stats) = stats.message_stats.as_ref() {
                println!("Total Published: {}", msg_stats.publish);
            }
        }
        Err(e) => println!("Error getting queue stats: {}", e),
    }
    println!();

    // List connections
    println!("=== Active Connections ===");
    match broker.list_connections().await {
        Ok(connections) => {
            println!("Found {} connections:", connections.len());
            for conn in connections.iter().take(5) {
                println!(
                    "  - {} (state: {}, {} channels)",
                    conn.name, conn.state, conn.channels
                );
            }
        }
        Err(e) => println!("Error listing connections: {}", e),
    }
    println!();

    // List channels
    println!("=== Active Channels ===");
    match broker.list_channels().await {
        Ok(channels) => {
            println!("Found {} channels:", channels.len());
            for ch in channels.iter().take(5) {
                println!("  - {} (number: {})", ch.name, ch.number);
            }
        }
        Err(e) => println!("Error listing channels: {}", e),
    }
    println!();

    // List exchanges
    println!("=== Exchanges ===");
    match broker.list_exchanges(None).await {
        Ok(exchanges) => {
            println!("Found {} exchanges:", exchanges.len());
            for ex in exchanges.iter().take(10) {
                println!(
                    "  - {} (type: {}, durable: {})",
                    ex.name, ex.exchange_type, ex.durable
                );
            }
        }
        Err(e) => println!("Error listing exchanges: {}", e),
    }
    println!();

    // List queue bindings
    println!("=== Queue Bindings for '{}' ===", queue_name);
    match broker.list_queue_bindings(queue_name).await {
        Ok(bindings) => {
            println!("Found {} bindings:", bindings.len());
            for binding in &bindings {
                println!(
                    "  - {} -> {} (routing key: '{}')",
                    binding.source, binding.destination, binding.routing_key
                );
            }
        }
        Err(e) => println!("Error listing bindings: {}", e),
    }
    println!();

    // Cleanup
    println!("Disconnecting...");
    broker.disconnect().await?;
    println!("Disconnected!");

    Ok(())
}
