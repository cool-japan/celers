//! Basic AWS SQS broker usage examples
//!
//! This example demonstrates:
//! - Creating and connecting to an SQS broker
//! - Publishing messages
//! - Consuming messages with long polling
//! - Acknowledging and rejecting messages
//! - Queue management operations
//!
//! # Running this example
//!
//! ```bash
//! # Set AWS credentials (or use IAM role)
//! export AWS_ACCESS_KEY_ID=your_key_id
//! export AWS_SECRET_ACCESS_KEY=your_secret_key
//! export AWS_REGION=us-east-1
//!
//! # Run the example
//! cargo run --example basic_usage
//! ```

use celers_broker_sqs::SqsBroker;
use celers_kombu::{Broker, Consumer, Producer, Transport};
use celers_protocol::builder::MessageBuilder;
use std::time::Duration;

#[tokio::main]
#[allow(dead_code)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing for logging
    tracing_subscriber::fmt::init();

    println!("=== AWS SQS Basic Usage Example ===\n");

    // Example 1: Create broker with default settings
    example_1_create_broker().await?;

    // Example 2: Publish and consume messages
    example_2_publish_consume().await?;

    // Example 3: Batch operations
    example_3_batch_operations().await?;

    // Example 4: Queue management
    example_4_queue_management().await?;

    // Example 5: Message rejection and requeue
    example_5_reject_requeue().await?;

    println!("\n=== All examples completed successfully ===");
    Ok(())
}

/// Example 1: Create and configure an SQS broker
async fn example_1_create_broker() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Example 1: Create Broker ---");

    // Create broker with default configuration
    let mut broker = SqsBroker::new("celers-example-queue").await?;
    broker.connect().await?;
    println!("✓ Broker created and connected");

    // Create broker with custom configuration
    let mut custom_broker = SqsBroker::new("celers-custom-queue")
        .await?
        .with_visibility_timeout(60) // 60 seconds visibility timeout
        .with_wait_time(20) // 20 seconds long polling
        .with_max_messages(10); // Receive up to 10 messages at once

    custom_broker.connect().await?;
    println!("✓ Custom broker created with visibility_timeout=60s, wait_time=20s");

    broker.disconnect().await?;
    custom_broker.disconnect().await?;

    Ok(())
}

/// Example 2: Publish and consume messages
async fn example_2_publish_consume() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Example 2: Publish and Consume ---");

    let mut broker = SqsBroker::new("celers-pubsub-queue").await?;
    broker.connect().await?;

    // Publish a simple message
    let message = MessageBuilder::new("tasks.process_data")
        .kwarg("user_id", serde_json::json!(123))
        .kwarg("action", serde_json::json!("update"))
        .kwarg("priority", serde_json::json!("high"))
        .build()?;

    broker.publish("celers-pubsub-queue", message).await?;
    println!("✓ Message published");

    // Consume the message with 20-second long polling
    if let Some(envelope) = broker
        .consume("celers-pubsub-queue", Duration::from_secs(20))
        .await?
    {
        println!("✓ Message received: {:?}", envelope.message.task_name());
        println!("  Delivery tag: {}", envelope.delivery_tag);
        println!("  Redelivered: {}", envelope.redelivered);

        // Acknowledge the message
        broker.ack(&envelope.delivery_tag).await?;
        println!("✓ Message acknowledged");
    } else {
        println!("⚠ No message received (queue might be empty)");
    }

    broker.disconnect().await?;

    Ok(())
}

/// Example 3: Batch operations for cost optimization
async fn example_3_batch_operations() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Example 3: Batch Operations ---");

    let mut broker = SqsBroker::new("celers-batch-queue").await?;
    broker.connect().await?;

    // Publish multiple messages in a single API call (10x cost reduction)
    let messages: Vec<_> = (1..=5)
        .map(|i| {
            MessageBuilder::new("tasks.process_batch")
                .kwarg("item_id", serde_json::json!(i))
                .build()
        })
        .collect::<Result<Vec<_>, _>>()?;

    broker.publish_batch("celers-batch-queue", messages).await?;
    println!("✓ Published 5 messages in a single batch API call");

    // Consume multiple messages at once
    let envelopes = broker
        .consume_batch("celers-batch-queue", 10, Duration::from_secs(20))
        .await?;
    println!("✓ Received {} messages in batch", envelopes.len());

    // Acknowledge all messages in a single API call
    if !envelopes.is_empty() {
        let tags: Vec<String> = envelopes.iter().map(|e| e.delivery_tag.clone()).collect();
        broker.ack_batch("celers-batch-queue", tags).await?;
        println!(
            "✓ Acknowledged {} messages in a single batch API call",
            envelopes.len()
        );
    }

    broker.disconnect().await?;

    Ok(())
}

/// Example 4: Queue management operations
async fn example_4_queue_management() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Example 4: Queue Management ---");

    let mut broker = SqsBroker::new("celers-mgmt-queue").await?;
    broker.connect().await?;

    // Get queue size
    let size = broker.queue_size("celers-mgmt-queue").await?;
    println!("✓ Queue size: {} messages", size);

    // Get queue statistics
    let stats = broker.get_queue_stats("celers-mgmt-queue").await?;
    println!("✓ Queue statistics:");
    println!(
        "  - Approximate messages: {}",
        stats.approximate_message_count
    );
    println!(
        "  - Messages in flight: {}",
        stats.approximate_not_visible_count
    );
    println!("  - Delayed messages: {}", stats.approximate_delayed_count);
    if let Some(age) = stats.approximate_age_of_oldest_message {
        println!("  - Oldest message age: {}s", age);
    }

    // Get queue ARN
    let arn = broker.get_queue_arn("celers-mgmt-queue").await?;
    println!("✓ Queue ARN: {}", arn);

    // List all queues
    let queues = broker.list_queues().await?;
    println!("✓ Available queues: {}", queues.len());
    for queue in queues.iter().take(5) {
        println!("  - {}", queue);
    }

    // Health check
    let is_healthy = broker.health_check("celers-mgmt-queue").await?;
    println!(
        "✓ Queue health check: {}",
        if is_healthy { "HEALTHY" } else { "UNHEALTHY" }
    );

    broker.disconnect().await?;

    Ok(())
}

/// Example 5: Message rejection and requeue
async fn example_5_reject_requeue() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Example 5: Reject and Requeue ---");

    let mut broker = SqsBroker::new("celers-reject-queue").await?;
    broker.connect().await?;

    // Publish a test message
    let message = MessageBuilder::new("tasks.may_fail")
        .kwarg("attempt", serde_json::json!(1))
        .build()?;
    broker.publish("celers-reject-queue", message).await?;
    println!("✓ Test message published");

    // Consume and reject (requeue=true means message goes back to queue)
    if let Some(envelope) = broker
        .consume("celers-reject-queue", Duration::from_secs(20))
        .await?
    {
        println!("✓ Message received, simulating processing failure");

        // Reject and requeue (message will become visible again after visibility timeout)
        broker.reject(&envelope.delivery_tag, true).await?;
        println!("✓ Message rejected and requeued");
    }

    // Consume again after a short delay
    tokio::time::sleep(Duration::from_secs(2)).await;

    if let Some(envelope) = broker
        .consume("celers-reject-queue", Duration::from_secs(20))
        .await?
    {
        println!(
            "✓ Message received again (redelivered flag: {})",
            envelope.redelivered
        );

        // This time reject without requeue (message is deleted)
        broker.reject(&envelope.delivery_tag, false).await?;
        println!("✓ Message rejected without requeue (deleted)");
    }

    broker.disconnect().await?;

    Ok(())
}
