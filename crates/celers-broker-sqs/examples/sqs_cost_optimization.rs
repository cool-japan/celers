//! Cost optimization strategies for AWS SQS
//!
//! This example demonstrates various techniques to reduce AWS SQS costs by up to 90%:
//! - Configuration presets (production, development, cost_optimized)
//! - Batch operations (10x cost reduction)
//! - Long polling (reduces empty receives)
//! - Adaptive polling (automatic cost optimization)
//! - Message compression (fits more data in 256 KB limit)
//!
//! # Cost Breakdown
//!
//! **Without optimization** (10M messages/month):
//! - 10M SendMessage = $4.00
//! - 10M ReceiveMessage = $4.00
//! - 10M DeleteMessage = $4.00
//! - **Total: $12.00/month**
//!
//! **With optimization** (batching + long polling):
//! - 1M SendMessageBatch = $0.40
//! - 1M ReceiveMessage (long polling) = $0.40
//! - 1M DeleteMessageBatch = $0.40
//! - **Total: $1.20/month (90% savings!)**
//!
//! # Running this example
//!
//! ```bash
//! export AWS_ACCESS_KEY_ID=your_key_id
//! export AWS_SECRET_ACCESS_KEY=your_secret_key
//! export AWS_REGION=us-east-1
//!
//! cargo run --example cost_optimization
//! ```

use celers_broker_sqs::{AdaptivePollingConfig, PollingStrategy, SqsBroker};
use celers_kombu::{Producer, Transport};
use celers_protocol::builder::MessageBuilder;
use std::time::Instant;

#[tokio::main]
#[allow(dead_code)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("=== AWS SQS Cost Optimization Examples ===\n");

    // Example 1: Configuration presets
    example_1_configuration_presets().await?;

    // Example 2: Batch operations
    example_2_batch_operations().await?;

    // Example 3: Long polling
    example_3_long_polling().await?;

    // Example 4: Adaptive polling
    example_4_adaptive_polling().await?;

    // Example 5: Message compression
    example_5_compression().await?;

    // Example 6: Cost comparison
    example_6_cost_comparison().await?;

    println!("\n=== Cost optimization examples completed ===");
    Ok(())
}

/// Example 1: Use configuration presets for quick setup
async fn example_1_configuration_presets() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Example 1: Configuration Presets ---");

    // Production preset: optimized for reliability and performance
    let _prod_broker = SqsBroker::production("celers-prod-queue").await?;
    println!("✓ Production preset:");
    println!("  - Long polling: 20s");
    println!("  - Max messages: 10 (batch receiving)");
    println!("  - Message retention: 14 days");
    println!("  - Visibility timeout: 30s");

    // Development preset: quick feedback, short retention
    let _dev_broker = SqsBroker::development("celers-dev-queue").await?;
    println!("✓ Development preset:");
    println!("  - Wait time: 5s");
    println!("  - Message retention: 1 hour");
    println!("  - Visibility timeout: 30s");

    // Cost-optimized preset: 90% cost reduction
    let _cost_broker = SqsBroker::cost_optimized("celers-cost-queue").await?;
    println!("✓ Cost-optimized preset:");
    println!("  - Adaptive polling (exponential backoff)");
    println!("  - Max messages: 10 (batch receiving)");
    println!("  - Long polling: 20s");
    println!("  - Estimated savings: 90% for high-volume workloads");

    println!("\nRecommendation: Use SqsBroker::cost_optimized() for production!");

    Ok(())
}

/// Example 2: Batch operations reduce API calls by 10x
async fn example_2_batch_operations() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Example 2: Batch Operations ---");

    let mut broker = SqsBroker::new("celers-batch-cost").await?;
    broker.connect().await?;

    // Scenario: Send 1000 messages

    // EXPENSIVE: Individual sends (1000 API calls = $0.40)
    println!("Individual operations (NOT RECOMMENDED):");
    let start = Instant::now();
    for i in 1..=10 {
        // Using 10 for demo
        let msg = MessageBuilder::new("task")
            .kwarg("id", serde_json::json!(i))
            .build()?;
        broker.publish("celers-batch-cost", msg).await?;
    }
    let individual_time = start.elapsed();
    println!("  - 10 individual publishes: {:?}", individual_time);
    println!("  - For 1000 messages: ~1000 API calls = $0.40");

    // CHEAP: Batch sends (100 API calls = $0.04, saves $0.36)
    println!("\nBatch operations (RECOMMENDED):");
    let messages: Vec<_> = (1..=10)
        .map(|i| {
            MessageBuilder::new("task")
                .kwarg("id", serde_json::json!(i))
                .build()
        })
        .collect::<Result<Vec<_>, _>>()?;

    let start = Instant::now();
    broker.publish_batch("celers-batch-cost", messages).await?;
    let batch_time = start.elapsed();
    println!("  - 1 batch publish (10 messages): {:?}", batch_time);
    println!("  - For 1000 messages: ~100 API calls = $0.04");
    println!("  - Savings: $0.36 (90% reduction)");

    // Auto-chunking for large batches
    let large_batch: Vec<_> = (1..=25)
        .map(|i| {
            MessageBuilder::new("task")
                .kwarg("id", serde_json::json!(i))
                .build()
        })
        .collect::<Result<Vec<_>, _>>()?;

    broker
        .publish_batch_chunked("celers-batch-cost", large_batch)
        .await?;
    println!("\n✓ Auto-chunked 25 messages into 3 batches (10+10+5)");

    broker.disconnect().await?;

    Ok(())
}

/// Example 3: Long polling reduces empty receives
async fn example_3_long_polling() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Example 3: Long Polling ---");

    // WITHOUT long polling (expensive)
    let mut short_poll_broker = SqsBroker::new("celers-short-poll").await?.with_wait_time(0); // No long polling

    short_poll_broker.connect().await?;
    println!("Short polling (NOT RECOMMENDED):");
    println!("  - Wait time: 0s");
    println!("  - Result: Many empty receives = wasted API calls");
    println!("  - Example: 1000 checks/hour = $0.40/hour");

    // WITH long polling (cheap)
    let mut long_poll_broker = SqsBroker::new("celers-long-poll").await?.with_wait_time(20); // Maximum long polling

    long_poll_broker.connect().await?;
    println!("\nLong polling (RECOMMENDED):");
    println!("  - Wait time: 20s (maximum)");
    println!("  - Result: Fewer empty receives = fewer API calls");
    println!("  - Example: 180 checks/hour = $0.07/hour");
    println!("  - Savings: $0.33/hour (83% reduction)");

    println!("\nRecommendation: Always use 20s long polling!");

    short_poll_broker.disconnect().await?;
    long_poll_broker.disconnect().await?;

    Ok(())
}

/// Example 4: Adaptive polling automatically optimizes costs
async fn example_4_adaptive_polling() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Example 4: Adaptive Polling ---");

    // Exponential backoff: increases wait time when queue is empty
    let backoff_config = AdaptivePollingConfig::new(PollingStrategy::ExponentialBackoff)
        .with_min_wait_time(1)
        .with_max_wait_time(20)
        .with_backoff_multiplier(2.0);

    let mut broker = SqsBroker::new("celers-adaptive-cost")
        .await?
        .with_adaptive_polling(backoff_config);

    broker.connect().await?;

    println!("Exponential backoff polling:");
    println!("  - When queue has messages: 1s wait (responsive)");
    println!("  - After 1 empty receive: 2s wait");
    println!("  - After 2 empty receives: 4s wait");
    println!("  - After 3 empty receives: 8s wait");
    println!("  - After 4+ empty receives: 20s wait (cost-optimized)");
    println!("  - Resets to 1s when message received");

    println!("\nBenefit:");
    println!("  - Low latency when busy (1s response time)");
    println!("  - Low cost when idle (20s polling)");
    println!("  - Automatic adaptation = best of both worlds!");

    broker.disconnect().await?;

    Ok(())
}

/// Example 5: Compression reduces message count
async fn example_5_compression() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Example 5: Message Compression ---");

    let mut broker = SqsBroker::new("celers-compression-cost")
        .await?
        .with_compression(10 * 1024); // Compress messages > 10 KB

    broker.connect().await?;

    println!("Compression benefits:");
    println!("  - SQS limit: 256 KB per message");
    println!("  - Without compression: 50 KB payload = 1 message");
    println!("  - With compression: 50 KB payload compressed to ~5 KB");
    println!("  - Result: Fit 10x more data per message = 10x fewer messages");

    println!("\nExample:");
    println!("  - Send 1 GB of data");
    println!("  - Without compression: ~4000 messages = $1.60");
    println!("  - With compression (10:1 ratio): ~400 messages = $0.16");
    println!("  - Savings: $1.44 (90% reduction)");

    println!("\nNote: Compression works best for text data (JSON, XML, logs)");

    broker.disconnect().await?;

    Ok(())
}

/// Example 6: Compare costs of different strategies
async fn example_6_cost_comparison() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Example 6: Cost Comparison ---");

    println!("Processing 10 million messages per month:\n");

    // Strategy 1: No optimization
    println!("Strategy 1: No Optimization");
    println!("  - Individual SendMessage: 10M × $0.40 = $4.00");
    println!("  - Short polling ReceiveMessage (5 attempts per message): 50M × $0.40 = $20.00");
    println!("  - Individual DeleteMessage: 10M × $0.40 = $4.00");
    println!("  - Total: $28.00/month");

    // Strategy 2: Basic optimization (long polling)
    println!("\nStrategy 2: Long Polling");
    println!("  - Individual SendMessage: 10M × $0.40 = $4.00");
    println!("  - Long polling ReceiveMessage (1.5 attempts avg): 15M × $0.40 = $6.00");
    println!("  - Individual DeleteMessage: 10M × $0.40 = $4.00");
    println!("  - Total: $14.00/month");
    println!("  - Savings: $14.00 (50% reduction)");

    // Strategy 3: Advanced optimization (batching + long polling)
    println!("\nStrategy 3: Batching + Long Polling");
    println!("  - Batch SendMessage (10 per batch): 1M × $0.40 = $0.40");
    println!("  - Long polling batch ReceiveMessage: 1.5M × $0.40 = $0.60");
    println!("  - Batch DeleteMessage: 1M × $0.40 = $0.40");
    println!("  - Total: $1.40/month");
    println!("  - Savings: $26.60 (95% reduction!)");

    // Strategy 4: Maximum optimization (all features)
    println!("\nStrategy 4: Maximum Optimization (RECOMMENDED)");
    println!("  - Batch SendMessage: 1M × $0.40 = $0.40");
    println!("  - Adaptive polling batch ReceiveMessage: 1.2M × $0.40 = $0.48");
    println!("  - Batch DeleteMessage: 1M × $0.40 = $0.40");
    println!("  - Total: $1.28/month");
    println!("  - Savings: $26.72 (95.4% reduction!)");

    println!("\n=== Recommendation ===");
    println!("Use SqsBroker::cost_optimized() to automatically get:");
    println!("  ✓ Batch operations");
    println!("  ✓ Long polling (20s)");
    println!("  ✓ Adaptive polling");
    println!("  ✓ ~95% cost reduction!");

    Ok(())
}
