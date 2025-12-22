//! Advanced monitoring and batch consumption features (v4)
//!
//! This example demonstrates the new v4 features:
//! - Batch message consumption
//! - Queue peeking (non-destructive inspection)
//! - RabbitMQ aliveness checks
//! - Helper methods for queue statistics and monitoring
//!
//! Prerequisites:
//! - RabbitMQ running on localhost:5672
//! - Management plugin enabled (enabled by default)
//!
//! Run with:
//! ```bash
//! cargo run --example advanced_monitoring
//! ```

use celers_broker_amqp::{AmqpBroker, AmqpConfig};
use celers_kombu::{Broker, Consumer, Producer, QueueMode, Transport};
use celers_protocol::builder::MessageBuilder;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("=== Advanced Monitoring & Batch Consumption Demo ===\n");

    // Configure broker with Management API
    let config = AmqpConfig::default()
        .with_prefetch(50)
        .with_channel_pool_size(10)
        .with_management_api("http://localhost:15672", "guest", "guest");

    let mut broker = AmqpBroker::with_config("amqp://localhost:5672", "celery", config).await?;
    broker.connect().await?;

    println!("✓ Connected to RabbitMQ\n");

    // Declare a test queue
    let queue = "test_monitoring_queue";
    broker.declare_queue(queue, QueueMode::Fifo).await?;

    println!("=== 1. Aliveness Check ===");
    match broker.check_aliveness(None).await {
        Ok(true) => println!("✓ RabbitMQ is alive and healthy"),
        Ok(false) => println!("✗ RabbitMQ aliveness test failed"),
        Err(e) => println!("✗ Error checking aliveness: {}", e),
    }
    println!();

    // Publish test messages
    println!("=== 2. Publishing Test Messages ===");
    let message_count = 25;
    for i in 1..=message_count {
        let msg = MessageBuilder::new("task.test")
            .args(vec![
                serde_json::json!({"id": i, "data": format!("Message {}", i)}),
            ])
            .build()?;
        broker.publish(queue, msg).await?;
    }
    println!("✓ Published {} messages\n", message_count);

    // Wait for messages to be ready
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Peek at messages without consuming
    println!("=== 3. Peeking at Messages (Non-destructive) ===");
    let peeked = broker.peek_queue(queue, 5).await?;
    println!("✓ Peeked at {} messages:", peeked.len());
    for (i, msg) in peeked.iter().enumerate() {
        println!("  Message {}: task={}", i + 1, msg.headers.task);
    }
    println!();

    // Get queue statistics using Management API
    println!("=== 4. Queue Statistics (Before Consumption) ===");
    if let Ok(stats) = broker.get_queue_stats(queue).await {
        println!("Queue: {}", stats.name);
        println!("  Total messages: {}", stats.messages);
        println!("  Ready messages: {}", stats.messages_ready);
        println!("  Unacknowledged: {}", stats.messages_unacknowledged);
        println!("  Consumers: {}", stats.consumers);
        println!();

        // Use helper methods
        println!("=== 5. Helper Methods Demo ===");
        println!("Queue Analysis:");
        println!("  Is empty: {}", stats.is_empty());
        println!("  Has consumers: {}", stats.has_consumers());
        println!("  Is idle: {}", stats.is_idle());
        println!("  Ready percentage: {:.1}%", stats.ready_percentage());
        println!("  Unacked percentage: {:.1}%", stats.unacked_percentage());
        println!("  Memory usage: {:.2} MB", stats.memory_mb());
        println!("  Avg message size: {:.0} bytes", stats.avg_message_size());

        if let Some(rate) = stats.publish_rate() {
            println!("  Publish rate: {:.2} msg/s", rate);
        }
        if let Some(rate) = stats.deliver_rate() {
            println!("  Deliver rate: {:.2} msg/s", rate);
        }

        println!("  Queue is growing: {}", stats.is_growing());
        println!("  Queue is shrinking: {}", stats.is_shrinking());
        println!("  Consumers keeping up: {}", stats.consumers_keeping_up());
        println!();
    }

    // Batch consumption
    println!("=== 6. Batch Consumption ===");
    let batch_size = 10;
    let timeout = Duration::from_secs(2);

    let envelopes = broker.consume_batch(queue, batch_size, timeout).await?;
    println!("✓ Consumed batch of {} messages", envelopes.len());

    // Process and acknowledge all messages in batch
    for envelope in envelopes {
        broker.ack(&envelope.delivery_tag).await?;
    }
    println!("✓ Acknowledged {} messages\n", batch_size);

    // Get updated queue statistics
    println!("=== 7. Queue Statistics (After Batch Consumption) ===");
    if let Ok(stats) = broker.get_queue_stats(queue).await {
        println!("Queue: {}", stats.name);
        println!("  Total messages: {}", stats.messages);
        println!("  Ready messages: {}", stats.messages_ready);
        println!("  Messages remaining: {}", stats.messages);
        println!();
    }

    // Consume remaining messages in multiple batches
    println!("=== 8. Multiple Batch Consumption ===");
    let mut total_consumed = batch_size;
    loop {
        let batch = broker
            .consume_batch(queue, 5, Duration::from_secs(1))
            .await?;
        if batch.is_empty() {
            break;
        }

        for envelope in batch.iter() {
            broker.ack(&envelope.delivery_tag).await?;
        }

        total_consumed += batch.len();
        println!(
            "✓ Consumed and acked {} messages (total: {})",
            batch.len(),
            total_consumed
        );
    }
    println!();

    // Server overview with helper methods
    println!("=== 9. Server Overview ===");
    if let Ok(overview) = broker.get_server_overview().await {
        println!("RabbitMQ Server:");
        println!("  Version: {}", overview.rabbitmq_version);
        println!("  Erlang: {}", overview.erlang_version);
        println!("  Cluster: {}", overview.cluster_name);
        println!();

        // Use helper methods
        println!("System Totals (using helper methods):");
        println!("  Total messages: {}", overview.total_messages());
        println!("  Total ready: {}", overview.total_messages_ready());
        println!("  Total unacked: {}", overview.total_messages_unacked());
        println!("  Total queues: {}", overview.total_queues());
        println!("  Total connections: {}", overview.total_connections());
        println!("  Total channels: {}", overview.total_channels());
        println!("  Total consumers: {}", overview.total_consumers());
        println!("  Has active connections: {}", overview.has_connections());
        println!("  Has messages in system: {}", overview.has_messages());
        println!();
    }

    // Connection and channel statistics
    println!("=== 10. Connection & Channel Statistics ===");
    if let Ok(connections) = broker.list_connections().await {
        println!("Active Connections: {}", connections.len());
        for (i, conn) in connections.iter().take(3).enumerate() {
            println!("  Connection {}:", i + 1);
            println!("    User: {}", conn.user);
            println!("    State: {}", conn.state);
            println!("    Is running: {}", conn.is_running());
            println!("    Has channels: {}", conn.has_channels());
            println!("    Channels: {}", conn.channels);
            println!("    Peer: {}", conn.peer_address());
            println!(
                "    Total bytes: {} ({:.2} MB)",
                conn.total_bytes(),
                conn.recv_mb() + conn.send_mb()
            );
            println!("    Total messages: {}", conn.total_messages());
            if conn.total_messages() > 0 {
                println!("    Avg message size: {:.0} bytes", conn.avg_message_size());
            }
        }
        println!();
    }

    if let Ok(channels) = broker.list_channels().await {
        println!("Active Channels: {}", channels.len());
        for (i, ch) in channels.iter().take(3).enumerate() {
            println!("  Channel {}:", i + 1);
            println!("    Number: {}", ch.number);
            println!("    User: {}", ch.user);
            println!("    State: {}", ch.state);
            println!("    Is running: {}", ch.is_running());
            println!("    Has consumers: {}", ch.has_consumers());
            println!("    Consumers: {}", ch.consumers);
            println!("    Has unacked messages: {}", ch.has_unacked_messages());
            println!("    Unacked: {}", ch.messages_unacknowledged);
            println!("    Is in transaction: {}", ch.is_in_transaction());
            println!("    Has prefetch: {}", ch.has_prefetch());
            if ch.has_prefetch() {
                println!("    Prefetch: {}", ch.prefetch_count);
                println!("    Utilization: {:.1}%", ch.utilization());
            }
            if let Some(addr) = ch.peer_address() {
                println!("    Peer: {}", addr);
            }
        }
        println!();
    }

    // Pool metrics
    println!("=== 11. Pool Metrics ===");
    if let Some(metrics) = broker.get_channel_pool_metrics().await {
        println!("Channel Pool:");
        println!("  Pool size: {}", metrics.pool_size);
        println!("  Max size: {}", metrics.max_pool_size);
        println!("  Utilization: {:.1}%", metrics.utilization() * 100.0);
        println!("  Acquisitions: {}", metrics.total_acquired);
        println!("  Releases: {}", metrics.total_released);
        println!("  Discards: {}", metrics.total_discarded);
        println!("  Discard rate: {:.2}%", metrics.discard_rate() * 100.0);
        println!("  Is full: {}", metrics.is_full());
        println!("  Is empty: {}", metrics.is_empty());
    }

    if let Some(metrics) = broker.get_connection_pool_metrics().await {
        println!("\nConnection Pool:");
        println!("  Pool size: {}", metrics.pool_size);
        println!("  Max size: {}", metrics.max_pool_size);
        println!("  Utilization: {:.1}%", metrics.utilization() * 100.0);
    }
    println!();

    // Cleanup
    println!("=== Cleanup ===");
    broker.purge(queue).await?;
    broker.disconnect().await?;
    println!("✓ Cleaned up and disconnected\n");

    println!("=== Demo Complete ===");
    println!("All v4 features demonstrated successfully!");

    Ok(())
}
