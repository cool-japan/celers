//! Advanced AWS SQS broker features
//!
//! This example demonstrates:
//! - FIFO queues with message deduplication
//! - Dead Letter Queue (DLQ) configuration
//! - Server-side encryption (SSE)
//! - Message compression for large payloads
//! - CloudWatch metrics integration
//! - Adaptive polling strategies
//! - Parallel message processing
//!
//! # Running this example
//!
//! ```bash
//! export AWS_ACCESS_KEY_ID=your_key_id
//! export AWS_SECRET_ACCESS_KEY=your_secret_key
//! export AWS_REGION=us-east-1
//!
//! cargo run --example advanced_features
//! ```

use celers_broker_sqs::{
    AdaptivePollingConfig, AlarmConfig, CloudWatchConfig, FifoConfig, PollingStrategy, SqsBroker,
    SseConfig,
};
use celers_kombu::{Broker, Consumer, Producer, Transport};
use celers_protocol::builder::MessageBuilder;
use std::time::Duration;

#[tokio::main]
#[allow(dead_code)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("=== AWS SQS Advanced Features Example ===\n");

    // Example 1: FIFO queues
    example_1_fifo_queues().await?;

    // Example 2: Dead Letter Queue (DLQ)
    example_2_dead_letter_queue().await?;

    // Example 3: Server-side encryption
    example_3_encryption().await?;

    // Example 4: Message compression
    example_4_compression().await?;

    // Example 5: CloudWatch metrics
    example_5_cloudwatch_metrics().await?;

    // Example 6: CloudWatch alarms
    example_6_cloudwatch_alarms().await?;

    // Example 7: Adaptive polling
    example_7_adaptive_polling().await?;

    // Example 8: Parallel processing
    example_8_parallel_processing().await?;

    println!("\n=== All advanced examples completed ===");
    Ok(())
}

/// Example 1: FIFO queues with guaranteed ordering and deduplication
async fn example_1_fifo_queues() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Example 1: FIFO Queues ---");

    // FIFO queue configuration
    let fifo_config = FifoConfig::new()
        .with_content_based_deduplication(true) // Automatic deduplication using SHA-256
        .with_high_throughput(true) // 3000 TPS per message group
        .with_default_message_group_id("default-group");

    let mut broker = SqsBroker::new("celers-fifo.fifo")
        .await?
        .with_fifo(fifo_config);

    broker.connect().await?;
    println!("✓ FIFO broker created with content-based deduplication");

    // Publish to FIFO queue with message group ID
    let message = MessageBuilder::new("tasks.ordered_processing")
        .kwarg("sequence", serde_json::json!(1))
        .build()?;

    broker
        .publish_fifo("celers-fifo.fifo", message, "group-1", None)
        .await?;
    println!("✓ Message published to FIFO queue with group ID 'group-1'");

    // Publish batch to FIFO queue
    let messages: Vec<_> = (1..=3)
        .map(|i| {
            let msg = MessageBuilder::new("tasks.batch_ordered")
                .kwarg("seq", serde_json::json!(i))
                .build()?;
            Ok::<_, Box<dyn std::error::Error>>((msg, "group-2".to_string(), None))
        })
        .collect::<Result<Vec<_>, _>>()?;

    broker
        .publish_fifo_batch("celers-fifo.fifo", messages)
        .await?;
    println!("✓ Batch of 3 messages published to FIFO queue with group ID 'group-2'");

    broker.disconnect().await?;

    Ok(())
}

/// Example 2: Dead Letter Queue for handling failed messages
async fn example_2_dead_letter_queue() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Example 2: Dead Letter Queue ---");

    let mut broker = SqsBroker::new("celers-dlq-main").await?;
    broker.connect().await?;

    // Create DLQ first
    use celers_kombu::QueueMode;
    broker
        .create_queue("celers-dlq-dead-letter", QueueMode::Priority)
        .await?;
    println!("✓ Dead letter queue created");

    // Get DLQ ARN
    let dlq_arn = broker.get_queue_arn("celers-dlq-dead-letter").await?;

    // Configure DLQ for main queue
    broker
        .set_redrive_policy("celers-dlq-main", &dlq_arn, 3)
        .await?;
    println!("✓ DLQ configured for main queue (max_receive_count=3)");

    // Get current redrive policy
    if let Some(policy) = broker.get_redrive_policy("celers-dlq-main").await? {
        println!("✓ Current redrive policy: {:?}", policy);
    }

    // Monitor DLQ (note: get_dlq_stats uses the configured DLQ)
    // For this example, we'll skip calling get_dlq_stats since it requires
    // a DLQ to be configured on the broker itself
    println!("✓ DLQ monitoring available via get_dlq_stats()");

    broker.disconnect().await?;

    Ok(())
}

/// Example 3: Server-side encryption for message security
async fn example_3_encryption() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Example 3: Server-Side Encryption ---");

    // Option 1: SQS-managed encryption (easiest)
    let sse_config = SseConfig::sqs_managed();

    let mut broker = SqsBroker::new("celers-encrypted-sqs")
        .await?
        .with_sse(sse_config);

    broker.connect().await?;
    println!("✓ Broker with SQS-managed encryption created");

    // Option 2: KMS encryption (more control)
    // Uncomment if you have a KMS key:
    // let kms_config = SseConfig::kms("alias/my-key")
    //     .with_data_key_reuse_period(300); // 5 minutes
    //
    // let mut kms_broker = SqsBroker::new("celers-encrypted-kms")
    //     .await?
    //     .with_sse(kms_config);
    //
    // println!("✓ Broker with KMS encryption created");

    broker.disconnect().await?;

    Ok(())
}

/// Example 4: Message compression for large payloads
async fn example_4_compression() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Example 4: Message Compression ---");

    // Enable compression for messages larger than 10 KB
    let mut broker = SqsBroker::new("celers-compressed")
        .await?
        .with_compression(10 * 1024); // 10 KB threshold

    broker.connect().await?;
    println!("✓ Broker with compression enabled (threshold: 10 KB)");

    // Publish a large message (will be automatically compressed)
    let large_payload = "x".repeat(50_000); // 50 KB payload
    let message = MessageBuilder::new("tasks.process_large_data")
        .kwarg("data", serde_json::json!(large_payload))
        .build()?;

    broker.publish("celers-compressed", message).await?;
    println!("✓ Large message published (automatically compressed)");

    // Message will be automatically decompressed on consume
    if let Some(envelope) = broker
        .consume("celers-compressed", Duration::from_secs(20))
        .await?
    {
        println!("✓ Message received and decompressed");
        broker.ack(&envelope.delivery_tag).await?;
    }

    broker.disconnect().await?;

    Ok(())
}

/// Example 5: CloudWatch metrics integration
async fn example_5_cloudwatch_metrics() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Example 5: CloudWatch Metrics ---");

    // Configure CloudWatch metrics
    let cw_config = CloudWatchConfig::new("CeleRS/SQS")
        .with_dimension("Environment", "production")
        .with_dimension("Application", "example-app");

    let mut broker = SqsBroker::new("celers-monitored")
        .await?
        .with_cloudwatch(cw_config);

    broker.connect().await?;
    println!("✓ Broker with CloudWatch metrics created");

    // Publish metrics manually
    broker.publish_metrics("celers-monitored").await?;
    println!("✓ Metrics published to CloudWatch:");
    println!("  - ApproximateNumberOfMessages");
    println!("  - ApproximateNumberOfMessagesNotVisible");
    println!("  - ApproximateNumberOfMessagesDelayed");
    println!("  - ApproximateAgeOfOldestMessage");

    broker.disconnect().await?;

    Ok(())
}

/// Example 6: CloudWatch alarms for monitoring
async fn example_6_cloudwatch_alarms() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Example 6: CloudWatch Alarms ---");

    let mut broker = SqsBroker::new("celers-alarms").await?;
    broker.connect().await?;

    // Create alarm for high queue depth
    let queue_depth_alarm = AlarmConfig::queue_depth_alarm(
        "HighQueueDepth",
        "celers-alarms",
        1000.0, // Alert when > 1000 messages
    )
    .with_alarm_action("arn:aws:sns:us-east-1:123456789012:alerts");

    broker.create_alarm(queue_depth_alarm).await?;
    println!("✓ Queue depth alarm created (threshold: 1000 messages)");

    // Create alarm for old messages (backlog detection)
    let message_age_alarm = AlarmConfig::message_age_alarm(
        "OldMessages",
        "celers-alarms",
        600.0, // Alert when oldest message > 10 minutes
    )
    .with_alarm_action("arn:aws:sns:us-east-1:123456789012:alerts");

    broker.create_alarm(message_age_alarm).await?;
    println!("✓ Message age alarm created (threshold: 600 seconds)");

    // List alarms
    let alarms = broker.list_alarms("celers-alarms").await?;
    println!("✓ Total alarms configured: {}", alarms.len());

    broker.disconnect().await?;

    Ok(())
}

/// Example 7: Adaptive polling strategies for cost optimization
async fn example_7_adaptive_polling() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Example 7: Adaptive Polling ---");

    // Exponential backoff strategy
    let backoff_config = AdaptivePollingConfig::new(PollingStrategy::ExponentialBackoff)
        .with_min_wait_time(1)
        .with_max_wait_time(20)
        .with_backoff_multiplier(2.0);

    let mut broker = SqsBroker::new("celers-adaptive")
        .await?
        .with_adaptive_polling(backoff_config);

    broker.connect().await?;
    println!("✓ Broker with exponential backoff polling created");
    println!("  - Min wait time: 1s");
    println!("  - Max wait time: 20s");
    println!("  - Backoff multiplier: 2.0");
    println!("  - Polling adapts automatically based on queue activity");

    // Adaptive strategy
    let adaptive_config = AdaptivePollingConfig::new(PollingStrategy::Adaptive)
        .with_min_wait_time(5)
        .with_max_wait_time(20);

    let mut adaptive_broker = SqsBroker::new("celers-smart")
        .await?
        .with_adaptive_polling(adaptive_config);

    adaptive_broker.connect().await?;
    println!("✓ Broker with adaptive polling created");
    println!("  - Decreases wait time when messages are flowing");
    println!("  - Increases wait time when queue is idle");

    broker.disconnect().await?;
    adaptive_broker.disconnect().await?;

    Ok(())
}

/// Example 8: Parallel message processing for higher throughput
async fn example_8_parallel_processing() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Example 8: Parallel Processing ---");

    let mut broker = SqsBroker::new("celers-parallel").await?;
    broker.connect().await?;

    // Publish test messages
    let messages: Vec<_> = (1..=10)
        .map(|i| {
            MessageBuilder::new("tasks.parallel_task")
                .kwarg("id", serde_json::json!(i))
                .build()
        })
        .collect::<Result<Vec<_>, _>>()?;

    broker.publish_batch("celers-parallel", messages).await?;
    println!("✓ Published 10 test messages");

    // Process up to 10 messages in parallel
    let processed = broker
        .consume_parallel(
            "celers-parallel",
            10,
            Duration::from_secs(20),
            |envelope| async move {
                // Simulate async processing
                println!("  Processing message: {:?}", envelope.message.task_name());
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok(())
            },
        )
        .await?;

    println!(
        "✓ Successfully processed {} messages in parallel",
        processed
    );

    broker.disconnect().await?;

    Ok(())
}
