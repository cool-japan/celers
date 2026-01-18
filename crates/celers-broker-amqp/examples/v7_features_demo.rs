//! V7 Features Demo - Production Resilience & Observability
//!
//! This example demonstrates the v7 enhancements for production-ready systems:
//! - Rate limiting with token bucket and leaky bucket strategies
//! - Bulkhead isolation for resource protection
//! - Message scheduling for delayed delivery
//! - Metrics export to Prometheus, StatsD, and JSON
//!
//! These features enable robust, observable, and resilient message processing.
//!
//! Run with: cargo run --example v7_features_demo

use celers_broker_amqp::{
    bulkhead::{Bulkhead, BulkheadConfig},
    metrics_export::{JsonExporter, MetricsCollector, PrometheusExporter, StatsDExporter},
    rate_limiter::{LeakyBucket, RateLimiter, TokenBucket},
    scheduler::MessageScheduler,
};
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 V7 Features Demo - Production Resilience & Observability\n");

    // Phase 1: Rate Limiting
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("⏱️  Phase 1: Rate Limiting");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    demonstrate_rate_limiting().await?;
    println!();

    // Phase 2: Bulkhead Isolation
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("🛡️  Phase 2: Bulkhead Isolation");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    demonstrate_bulkhead_isolation().await?;
    println!();

    // Phase 3: Message Scheduling
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📅 Phase 3: Message Scheduling");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    demonstrate_message_scheduling().await?;
    println!();

    // Phase 4: Metrics Export
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📊 Phase 4: Metrics Export");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    demonstrate_metrics_export().await?;
    println!();

    // Phase 5: Integration Pattern
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("🔗 Phase 5: Integration Pattern");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    demonstrate_integration_pattern().await?;
    println!();

    println!("✅ All v7 features demonstrated successfully!");
    println!("\n💡 Key Takeaways:");
    println!("  1. Use rate limiting to prevent overwhelming downstream systems");
    println!("  2. Apply bulkhead isolation to prevent cascading failures");
    println!("  3. Schedule messages for delayed processing and batch operations");
    println!("  4. Export metrics to monitoring systems for observability");
    println!("  5. Combine all patterns for production-ready resilience");

    Ok(())
}

/// Demonstrate rate limiting with token bucket and leaky bucket
async fn demonstrate_rate_limiting() -> Result<(), Box<dyn std::error::Error>> {
    println!("1️⃣  Token Bucket Strategy (allows bursts)");
    println!("   Capacity: 10 tokens, Refill: 5 tokens/sec");

    // Token bucket allows bursts up to capacity
    let token_bucket = TokenBucket::new(5.0, 10);

    println!("   Testing burst capability...");
    let start = Instant::now();
    for _i in 1..=10 {
        if token_bucket.try_acquire(1).await {
            print!(".");
        } else {
            print!("X");
        }
    }
    println!(" ({:.2}ms)", start.elapsed().as_secs_f64() * 1000.0);

    let stats = token_bucket.statistics().await;
    println!(
        "   Stats: {:.1}% utilization, {} tokens available",
        stats.utilization * 100.0,
        stats.available_tokens
    );

    println!("\n2️⃣  Leaky Bucket Strategy (smooth rate limiting)");
    println!("   Bucket Size: 5, Leak Rate: 2 items/sec");

    // Leaky bucket provides smooth, constant rate processing
    let leaky_bucket = LeakyBucket::new(2.0, 5);

    println!("   Testing smooth rate limiting...");
    let start = Instant::now();
    for _i in 1..=5 {
        if leaky_bucket.try_add(1).await {
            print!(".");
        } else {
            print!("X");
        }
    }
    println!(" ({:.2}ms)", start.elapsed().as_secs_f64() * 1000.0);

    let stats = leaky_bucket.statistics().await;
    println!(
        "   Stats: {:.1}% utilization, {} items in queue",
        stats.utilization * 100.0,
        stats.queue_size
    );

    println!("\n3️⃣  Generic RateLimiter (strategy selection)");

    // RateLimiter enum allows runtime strategy selection
    let rate_limiter = RateLimiter::TokenBucket(TokenBucket::new(50.0, 100));

    let acquired = if rate_limiter.try_acquire(10).await {
        "✓ Acquired"
    } else {
        "✗ Rejected"
    };
    println!("   Batch request (10 tokens): {}", acquired);

    Ok(())
}

/// Demonstrate bulkhead isolation for resource protection
async fn demonstrate_bulkhead_isolation() -> Result<(), Box<dyn std::error::Error>> {
    println!("1️⃣  Creating Bulkhead (max 3 concurrent operations)");

    let config = BulkheadConfig {
        max_concurrent: 3,
        max_wait_time: Some(Duration::from_secs(1)),
    };
    let bulkhead = Bulkhead::with_config(config);

    println!("   Testing concurrent operations...");

    // Spawn tasks that will compete for bulkhead permits
    let mut handles = vec![];

    for i in 1..=5 {
        let bulkhead_clone = bulkhead.clone();
        let handle = tokio::spawn(async move {
            let operation = async {
                // Simulate work
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok::<_, String>(format!("Task {} completed", i))
            };

            match bulkhead_clone.try_call(operation).await {
                Ok(result) => println!("   ✓ {}", result),
                Err(e) => println!("   ⏸️  Task {} rejected: {:?}", i, e),
            }
        });
        handles.push(handle);
    }

    // Wait for all tasks
    for handle in handles {
        handle.await?;
    }

    let stats = bulkhead.statistics().await;
    println!("\n   Stats:");
    println!("     Success: {}", stats.successful_operations);
    println!("     Rejected: {}", stats.rejected_operations);
    println!("     Failed: {}", stats.failed_operations);
    println!(
        "     Rejection rate: {:.1}%",
        stats.rejection_rate() * 100.0
    );

    Ok(())
}

/// Demonstrate message scheduling for delayed delivery
async fn demonstrate_message_scheduling() -> Result<(), Box<dyn std::error::Error>> {
    println!("1️⃣  Creating Message Scheduler");

    let scheduler = MessageScheduler::new();

    println!("   Scheduling messages with different delays...");

    // Schedule messages with various delays
    let now = Instant::now();

    scheduler
        .schedule_after(
            "queue1",
            b"Message 1 - 200ms delay".to_vec(),
            Duration::from_millis(200),
        )
        .await;
    println!("   ✓ Scheduled: Message 1 (200ms delay)");

    scheduler
        .schedule_after(
            "queue1",
            b"Message 2 - 100ms delay".to_vec(),
            Duration::from_millis(100),
        )
        .await;
    println!("   ✓ Scheduled: Message 2 (100ms delay)");

    scheduler
        .schedule_after(
            "queue1",
            b"Message 3 - 150ms delay".to_vec(),
            Duration::from_millis(150),
        )
        .await;
    println!("   ✓ Scheduled: Message 3 (150ms delay)");

    println!("\n2️⃣  Polling for ready messages...");

    // Poll for messages as they become ready
    let mut received = 0;
    while received < 3 {
        if let Some(msg) = scheduler.poll().await {
            let elapsed = now.elapsed().as_millis();
            let payload_str = String::from_utf8_lossy(&msg.payload);
            println!("   📨 Received: {} (after {}ms)", payload_str, elapsed);
            received += 1;
        } else {
            // Wait a bit before next poll
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    let stats = scheduler.statistics().await;
    println!("\n   Stats:");
    println!("     Scheduled: {}", stats.total_scheduled);
    println!("     Delivered: {}", stats.delivered_messages);
    println!("     Cancelled: {}", stats.cancelled_messages);
    println!("     Pending: {}", stats.pending_messages);

    Ok(())
}

/// Demonstrate metrics export to various formats
async fn demonstrate_metrics_export() -> Result<(), Box<dyn std::error::Error>> {
    println!("1️⃣  Collecting Metrics");

    let collector = MetricsCollector::new();

    // Record some metrics
    collector.record_message_published("high_priority", 150);
    collector.record_message_published("normal", 500);
    collector.record_message_consumed("high_priority", 145);
    collector.record_message_consumed("normal", 480);
    collector.record_message_acknowledged("high_priority", 143);
    collector.record_message_acknowledged("normal", 475);
    collector.record_message_rejected("high_priority", 2);
    collector.record_message_rejected("normal", 5);
    collector.set_connections(5);
    collector.set_channels(20);
    collector.record_error("connection_timeout");
    collector.record_error("channel_closed");

    // Wait for async updates
    tokio::time::sleep(Duration::from_millis(50)).await;

    let snapshot = collector.snapshot().await;
    println!("   Total published: {}", snapshot.total_published());
    println!("   Total consumed: {}", snapshot.total_consumed());
    println!("   Total acknowledged: {}", snapshot.total_acknowledged());
    println!("   Total rejected: {}", snapshot.total_rejected());
    println!("   Total errors: {}", snapshot.total_errors());

    println!("\n2️⃣  Prometheus Export");
    let prom_exporter = PrometheusExporter::new("amqp_broker");
    let prom_text = prom_exporter.export(&collector).await;

    // Show first few lines
    let lines: Vec<&str> = prom_text.lines().take(6).collect();
    for line in lines {
        println!("   {}", line);
    }
    println!("   ... ({} total lines)", prom_text.lines().count());

    println!("\n3️⃣  StatsD Export");
    let statsd_exporter = StatsDExporter::new("amqp_broker", "localhost:8125");
    let statsd_metrics = statsd_exporter.format(&collector).await;

    // Show first few metrics
    for metric in statsd_metrics.iter().take(4) {
        println!("   {}", metric);
    }
    println!("   ... ({} total metrics)", statsd_metrics.len());

    println!("\n4️⃣  JSON Export");
    let json = JsonExporter::export(&collector).await?;

    // Show formatted JSON (first few lines)
    let lines: Vec<&str> = json.lines().take(8).collect();
    for line in lines {
        println!("   {}", line);
    }
    println!("   ... }}");

    Ok(())
}

/// Demonstrate integration of all v7 features
async fn demonstrate_integration_pattern() -> Result<(), Box<dyn std::error::Error>> {
    println!("Simulating production message processing pipeline...\n");

    // Setup components
    let rate_limiter = TokenBucket::new(5.0, 10);
    let bulkhead = Bulkhead::with_config(BulkheadConfig {
        max_concurrent: 3,
        max_wait_time: Some(Duration::from_secs(1)),
    });
    let scheduler = MessageScheduler::new();
    let metrics = MetricsCollector::new();

    println!("1️⃣  Processing incoming messages (rate limited)");

    // Simulate processing 15 messages
    for i in 1..=15 {
        // Rate limiting
        if rate_limiter.try_acquire(1).await {
            // Record metric
            metrics.record_message_published("incoming", 1);

            // Schedule with different delays
            let delay = if i % 3 == 0 {
                Duration::from_millis(50)
            } else {
                Duration::from_millis(100)
            };

            scheduler
                .schedule_after(
                    "processing_queue",
                    format!("Message {}", i).as_bytes().to_vec(),
                    delay,
                )
                .await;

            print!(".");
        } else {
            metrics.record_message_rejected("incoming", 1);
            print!("X");
        }
    }
    println!(" (15 messages processed)\n");

    tokio::time::sleep(Duration::from_millis(50)).await;

    println!("2️⃣  Processing scheduled messages (bulkhead protected)");

    let mut processed = 0;
    while processed < 10 {
        if let Some(_msg) = scheduler.poll().await {
            let bulkhead_clone = bulkhead.clone();
            let metrics_clone = metrics.clone();

            tokio::spawn(async move {
                let operation = async {
                    // Simulate processing
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    Ok::<_, String>(())
                };

                match bulkhead_clone.try_call(operation).await {
                    Ok(()) => {
                        metrics_clone.record_message_consumed("processing", 1);
                        metrics_clone.record_message_acknowledged("processing", 1);
                    }
                    Err(_) => {
                        metrics_clone.record_message_rejected("processing", 1);
                    }
                }
            });

            processed += 1;
            print!(".");
        } else {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    println!(" (10 messages processed)\n");

    tokio::time::sleep(Duration::from_millis(100)).await;

    println!("3️⃣  Final Metrics");
    let snapshot = metrics.snapshot().await;
    println!("   Messages published: {}", snapshot.total_published());
    println!("   Messages consumed: {}", snapshot.total_consumed());
    println!(
        "   Messages acknowledged: {}",
        snapshot.total_acknowledged()
    );
    println!("   Messages rejected: {}", snapshot.total_rejected());

    let rate_stats = rate_limiter.statistics().await;
    println!(
        "   Rate limiter utilization: {:.1}%",
        rate_stats.utilization * 100.0
    );

    let bulkhead_stats = bulkhead.statistics().await;
    println!(
        "   Bulkhead rejection rate: {:.1}%",
        bulkhead_stats.rejection_rate() * 100.0
    );

    Ok(())
}
