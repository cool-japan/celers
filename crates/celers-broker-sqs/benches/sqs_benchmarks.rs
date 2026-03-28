//! AWS SQS Broker Benchmarks
//!
//! These benchmarks measure the performance of various SQS operations:
//! - Individual vs batch publish operations
//! - Individual vs batch consume operations
//! - Individual vs batch acknowledge operations
//! - Compression overhead for different payload sizes
//! - Polling strategies performance
//! - Parallel vs sequential processing
//!
//! # Running benchmarks
//!
//! ```bash
//! # Set AWS credentials
//! export AWS_ACCESS_KEY_ID=your_key_id
//! export AWS_SECRET_ACCESS_KEY=your_secret_key
//! export AWS_REGION=us-east-1
//!
//! # Run all benchmarks
//! cargo bench --bench sqs_benchmarks
//!
//! # Run specific benchmark
//! cargo bench --bench sqs_benchmarks -- publish
//! ```
//!
//! # Prerequisites
//!
//! - Valid AWS credentials configured
//! - SQS queue access permissions
//! - Stable network connection to AWS

use celers_broker_sqs::{AdaptivePollingConfig, PollingStrategy, SqsBroker};
use celers_kombu::{Consumer, Producer, Transport};
use celers_protocol::builder::MessageBuilder;
use std::time::{Duration, Instant};

/// Helper function to create a test message
fn create_test_message(
    id: usize,
    size_kb: usize,
) -> Result<celers_protocol::Message, Box<dyn std::error::Error>> {
    let payload = "x".repeat(size_kb * 1024);
    Ok(MessageBuilder::new("tasks.benchmark")
        .kwarg("id", serde_json::json!(id))
        .kwarg("data", serde_json::json!(payload))
        .build()?)
}

/// Benchmark: Individual publish vs batch publish
async fn benchmark_publish_operations() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Benchmark: Publish Operations ===");

    let mut broker = SqsBroker::new("celers-bench-publish").await?;
    broker.connect().await?;

    // Individual publish
    let messages: Vec<_> = (0..10)
        .map(|i| create_test_message(i, 1))
        .collect::<Result<Vec<_>, _>>()?;

    let start = Instant::now();
    for msg in &messages {
        broker.publish("celers-bench-publish", msg.clone()).await?;
    }
    let individual_duration = start.elapsed();
    let individual_per_msg = individual_duration.as_millis() / 10;

    println!("\nIndividual Publish (10 messages):");
    println!("  Total time: {:?}", individual_duration);
    println!("  Per message: {} ms", individual_per_msg);
    println!(
        "  Throughput: {:.2} msg/s",
        10000.0 / individual_duration.as_millis() as f64
    );

    // Batch publish
    let batch_messages: Vec<_> = (0..10)
        .map(|i| create_test_message(i + 100, 1))
        .collect::<Result<Vec<_>, _>>()?;

    let start = Instant::now();
    broker
        .publish_batch("celers-bench-publish", batch_messages)
        .await?;
    let batch_duration = start.elapsed();
    let batch_per_msg = batch_duration.as_millis() / 10;

    println!("\nBatch Publish (10 messages):");
    println!("  Total time: {:?}", batch_duration);
    println!("  Per message: {} ms", batch_per_msg);
    println!(
        "  Throughput: {:.2} msg/s",
        10000.0 / batch_duration.as_millis() as f64
    );

    let speedup = individual_duration.as_millis() as f64 / batch_duration.as_millis() as f64;
    println!("\nSpeedup: {:.2}x faster", speedup);
    println!("Cost reduction: {:.1}%", (1.0 - 1.0 / speedup) * 100.0);

    broker.disconnect().await?;

    Ok(())
}

/// Benchmark: Individual consume vs batch consume
async fn benchmark_consume_operations() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Benchmark: Consume Operations ===");

    let mut broker = SqsBroker::new("celers-bench-consume").await?;
    broker.connect().await?;

    // Publish test messages
    let messages: Vec<_> = (0..20)
        .map(|i| create_test_message(i, 1))
        .collect::<Result<Vec<_>, _>>()?;
    broker
        .publish_batch("celers-bench-consume", messages)
        .await?;

    // Individual consume
    let start = Instant::now();
    let mut count = 0;
    for _ in 0..10 {
        if broker
            .consume("celers-bench-consume", Duration::from_secs(5))
            .await?
            .is_some()
        {
            count += 1;
        }
    }
    let individual_duration = start.elapsed();

    println!("\nIndividual Consume ({} messages):", count);
    println!("  Total time: {:?}", individual_duration);
    if let Some(per_msg) = individual_duration.as_millis().checked_div(count) {
        println!("  Per message: {} ms", per_msg);
        println!(
            "  Throughput: {:.2} msg/s",
            count as f64 / individual_duration.as_secs_f64()
        );
    }

    // Batch consume
    let start = Instant::now();
    let envelopes = broker
        .consume_batch("celers-bench-consume", 10, Duration::from_secs(5))
        .await?;
    let batch_duration = start.elapsed();

    println!("\nBatch Consume ({} messages):", envelopes.len());
    println!("  Total time: {:?}", batch_duration);
    if !envelopes.is_empty() {
        println!(
            "  Per message: {} ms",
            batch_duration.as_millis() / envelopes.len() as u128
        );
        println!(
            "  Throughput: {:.2} msg/s",
            envelopes.len() as f64 / batch_duration.as_secs_f64()
        );
    }

    if count > 0 && !envelopes.is_empty() {
        let speedup = individual_duration.as_secs_f64() / batch_duration.as_secs_f64();
        println!("\nSpeedup: {:.2}x faster", speedup);
    }

    broker.disconnect().await?;

    Ok(())
}

/// Benchmark: Compression overhead for different payload sizes
async fn benchmark_compression() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Benchmark: Compression Overhead ===");

    let sizes = vec![1, 10, 50, 100]; // KB

    for size_kb in sizes {
        let mut broker_no_compression = SqsBroker::new("celers-bench-compress")
            .await?
            .with_compression(usize::MAX); // Disable compression

        let mut broker_with_compression = SqsBroker::new("celers-bench-compress")
            .await?
            .with_compression(5 * 1024); // Compress > 5 KB

        broker_no_compression.connect().await?;
        broker_with_compression.connect().await?;

        let message = create_test_message(0, size_kb)?;

        // Without compression
        let start = Instant::now();
        broker_no_compression
            .publish("celers-bench-compress", message.clone())
            .await?;
        let no_compress_duration = start.elapsed();

        // With compression
        let start = Instant::now();
        broker_with_compression
            .publish("celers-bench-compress", message)
            .await?;
        let compress_duration = start.elapsed();

        println!("\nPayload size: {} KB", size_kb);
        println!("  Without compression: {:?}", no_compress_duration);
        println!("  With compression: {:?}", compress_duration);
        println!(
            "  Overhead: {:.1}%",
            ((compress_duration.as_micros() as f64 / no_compress_duration.as_micros() as f64)
                - 1.0)
                * 100.0
        );

        broker_no_compression.disconnect().await?;
        broker_with_compression.disconnect().await?;
    }

    Ok(())
}

/// Benchmark: Different polling strategies
async fn benchmark_polling_strategies() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Benchmark: Polling Strategies ===");

    // Fixed polling
    let mut fixed_broker = SqsBroker::new("celers-bench-poll").await?.with_wait_time(5);
    fixed_broker.connect().await?;

    let start = Instant::now();
    let _ = fixed_broker
        .consume("celers-bench-poll", Duration::from_secs(5))
        .await?;
    let fixed_duration = start.elapsed();

    println!("\nFixed Polling (5s wait):");
    println!("  Time: {:?}", fixed_duration);

    // Exponential backoff
    let backoff_config = AdaptivePollingConfig::new(PollingStrategy::ExponentialBackoff)
        .with_min_wait_time(1)
        .with_max_wait_time(10)
        .with_backoff_multiplier(2.0);

    let mut backoff_broker = SqsBroker::new("celers-bench-poll")
        .await?
        .with_adaptive_polling(backoff_config);
    backoff_broker.connect().await?;

    let start = Instant::now();
    let _ = backoff_broker
        .consume("celers-bench-poll", Duration::from_secs(5))
        .await?;
    let backoff_duration = start.elapsed();

    println!("\nExponential Backoff:");
    println!("  Time: {:?}", backoff_duration);

    // Adaptive polling
    let adaptive_config = AdaptivePollingConfig::new(PollingStrategy::Adaptive)
        .with_min_wait_time(1)
        .with_max_wait_time(10);

    let mut adaptive_broker = SqsBroker::new("celers-bench-poll")
        .await?
        .with_adaptive_polling(adaptive_config);
    adaptive_broker.connect().await?;

    let start = Instant::now();
    let _ = adaptive_broker
        .consume("celers-bench-poll", Duration::from_secs(5))
        .await?;
    let adaptive_duration = start.elapsed();

    println!("\nAdaptive Polling:");
    println!("  Time: {:?}", adaptive_duration);

    fixed_broker.disconnect().await?;
    backoff_broker.disconnect().await?;
    adaptive_broker.disconnect().await?;

    Ok(())
}

/// Benchmark: Parallel vs sequential processing
async fn benchmark_parallel_processing() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Benchmark: Parallel Processing ===");

    let mut broker = SqsBroker::new("celers-bench-parallel").await?;
    broker.connect().await?;

    // Publish test messages
    let messages: Vec<_> = (0..20)
        .map(|i| create_test_message(i, 1))
        .collect::<Result<Vec<_>, _>>()?;
    broker
        .publish_batch("celers-bench-parallel", messages)
        .await?;

    // Sequential processing
    let start = Instant::now();
    let mut seq_count = 0;
    for _ in 0..10 {
        if let Some(envelope) = broker
            .consume("celers-bench-parallel", Duration::from_secs(5))
            .await?
        {
            // Simulate work
            tokio::time::sleep(Duration::from_millis(50)).await;
            broker.ack(&envelope.delivery_tag).await?;
            seq_count += 1;
        }
    }
    let sequential_duration = start.elapsed();

    println!("\nSequential Processing ({} messages):", seq_count);
    println!("  Total time: {:?}", sequential_duration);
    if let Some(per_msg) = sequential_duration.as_millis().checked_div(seq_count) {
        println!("  Per message: {} ms", per_msg);
    }

    // Parallel processing
    let start = Instant::now();
    let parallel_count = broker
        .consume_parallel(
            "celers-bench-parallel",
            10,
            Duration::from_secs(5),
            |_envelope| async move {
                // Simulate work
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok(())
            },
        )
        .await?;
    let parallel_duration = start.elapsed();

    println!("\nParallel Processing ({} messages):", parallel_count);
    println!("  Total time: {:?}", parallel_duration);
    if parallel_count > 0 {
        println!(
            "  Per message: {} ms",
            parallel_duration.as_millis() / parallel_count as u128
        );
    }

    if seq_count > 0 && parallel_count > 0 {
        let speedup = sequential_duration.as_secs_f64() / parallel_duration.as_secs_f64();
        println!("\nSpeedup: {:.2}x faster", speedup);
    }

    broker.disconnect().await?;

    Ok(())
}

/// Benchmark: Batch acknowledge operations
async fn benchmark_ack_operations() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Benchmark: Acknowledge Operations ===");

    let mut broker = SqsBroker::new("celers-bench-ack").await?;
    broker.connect().await?;

    // Publish and consume messages for individual ack test
    let messages: Vec<_> = (0..10)
        .map(|i| create_test_message(i, 1))
        .collect::<Result<Vec<_>, _>>()?;
    broker.publish_batch("celers-bench-ack", messages).await?;

    let mut tags = Vec::new();
    for _ in 0..10 {
        if let Some(envelope) = broker
            .consume("celers-bench-ack", Duration::from_secs(5))
            .await?
        {
            tags.push(envelope.delivery_tag);
        }
    }

    // Individual ack
    let start = Instant::now();
    for tag in &tags {
        broker.ack(tag).await?;
    }
    let individual_duration = start.elapsed();

    println!("\nIndividual Ack ({} messages):", tags.len());
    println!("  Total time: {:?}", individual_duration);
    if !tags.is_empty() {
        println!(
            "  Per message: {} ms",
            individual_duration.as_millis() / tags.len() as u128
        );
    }

    // Batch ack
    let messages: Vec<_> = (0..10)
        .map(|i| create_test_message(i + 100, 1))
        .collect::<Result<Vec<_>, _>>()?;
    broker.publish_batch("celers-bench-ack", messages).await?;

    let envelopes = broker
        .consume_batch("celers-bench-ack", 10, Duration::from_secs(5))
        .await?;
    let batch_tags: Vec<String> = envelopes.iter().map(|e| e.delivery_tag.clone()).collect();
    let batch_count = batch_tags.len();

    let start = Instant::now();
    broker.ack_batch("celers-bench-ack", batch_tags).await?;
    let batch_duration = start.elapsed();

    println!("\nBatch Ack ({} messages):", batch_count);
    println!("  Total time: {:?}", batch_duration);
    if batch_count > 0 {
        println!(
            "  Per message: {} ms",
            batch_duration.as_millis() / batch_count as u128
        );
    }

    if !tags.is_empty() && batch_count > 0 {
        let speedup = individual_duration.as_secs_f64() / batch_duration.as_secs_f64();
        println!("\nSpeedup: {:.2}x faster", speedup);
        println!("Cost reduction: {:.1}%", (1.0 - 1.0 / speedup) * 100.0);
    }

    broker.disconnect().await?;

    Ok(())
}

#[tokio::main]
#[allow(dead_code)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("=== AWS SQS Broker Benchmarks ===");
    println!("Note: These benchmarks require valid AWS credentials and network access\n");

    // Run all benchmarks
    benchmark_publish_operations().await?;
    benchmark_consume_operations().await?;
    benchmark_ack_operations().await?;
    benchmark_compression().await?;
    benchmark_polling_strategies().await?;
    benchmark_parallel_processing().await?;

    println!("\n=== All benchmarks completed ===");

    Ok(())
}
