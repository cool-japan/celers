//! Monitoring utilities demonstration
//!
//! This example demonstrates the monitoring and utility functions for
//! capacity planning, autoscaling decisions, and performance optimization.
//!
//! Prerequisites:
//! - RabbitMQ running on localhost:5672
//! - Management plugin enabled (enabled by default)
//!
//! Run with:
//! ```bash
//! cargo run --example monitoring_utilities
//! ```

use celers_broker_amqp::monitoring::*;
use celers_broker_amqp::utilities::*;
use celers_broker_amqp::{AmqpBroker, AmqpConfig, QueueType};
use celers_kombu::{Broker, Producer, QueueMode, Transport};
use celers_protocol::builder::MessageBuilder;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("=== AMQP Monitoring & Utility Functions Demo ===\n");

    // Configure broker with Management API
    let config = AmqpConfig::default()
        .with_prefetch(10)
        .with_channel_pool_size(10)
        .with_management_api("http://localhost:15672", "guest", "guest");

    let mut broker = AmqpBroker::with_config("amqp://localhost:5672", "celery", config).await?;
    broker.connect().await?;

    println!("✓ Connected to RabbitMQ\n");

    // Declare a test queue
    let queue = "test_monitoring_utilities";
    broker.declare_queue(queue, QueueMode::Fifo).await?;

    // Publish test messages
    println!("=== 1. Publishing Test Messages ===");
    let message_count = 100;
    for i in 1..=message_count {
        let msg = MessageBuilder::new("task.test")
            .args(vec![
                serde_json::json!({"id": i, "data": format!("Message {}", i)}),
            ])
            .build()?;
        broker.publish(queue, msg).await?;
    }
    println!("✓ Published {} messages\n", message_count);

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Get queue stats
    println!("=== 2. Queue Statistics ===");
    if let Ok(stats) = broker.get_queue_stats(queue).await {
        println!("Queue: {}", stats.name);
        println!("  Messages: {}", stats.messages);
        println!("  Ready: {}", stats.messages_ready);
        println!("  Unacknowledged: {}", stats.messages_unacknowledged);
        println!("  Consumers: {}", stats.consumers);
        println!("  Memory: {:.2} MB", stats.memory_mb());
        println!();

        // Consumer lag analysis
        println!("=== 3. Consumer Lag Analysis ===");
        let processing_rate = 10.0; // Assume 10 msg/sec
        let target_lag = 60; // Target: drain queue in 60 seconds

        let lag_analysis =
            analyze_amqp_consumer_lag(stats.messages_ready as usize, processing_rate, target_lag);

        println!("Current queue size: {}", lag_analysis.queue_size);
        println!(
            "Processing rate: {:.1} msg/sec",
            lag_analysis.processing_rate
        );
        println!("Calculated lag: {:.1} seconds", lag_analysis.lag_seconds);
        println!("Target lag: {} seconds", lag_analysis.target_lag_seconds);
        println!("Is lagging: {}", lag_analysis.is_lagging);
        println!("Recommendation: {:?}", lag_analysis.recommendation);
        println!();

        // Worker scaling suggestion
        println!("=== 4. Worker Scaling Suggestion ===");
        let current_workers = 2;
        let avg_rate_per_worker = 5.0;

        let scaling = suggest_amqp_worker_scaling(
            stats.messages_ready as usize,
            current_workers,
            avg_rate_per_worker,
            target_lag,
        );

        println!("Current workers: {}", scaling.current_workers);
        println!("Recommended workers: {}", scaling.recommended_workers);
        println!("Scaling action: {:?}", scaling.action);
        println!();

        // Processing capacity estimation
        println!("=== 5. Processing Capacity Estimation ===");
        let capacity = estimate_amqp_processing_capacity(
            scaling.recommended_workers,
            avg_rate_per_worker,
            Some(stats.messages_ready as usize),
        );

        println!("Workers: {}", capacity.workers);
        println!("Rate per worker: {:.1} msg/sec", capacity.rate_per_worker);
        println!(
            "Total capacity: {:.1} msg/sec",
            capacity.total_capacity_per_sec
        );
        println!(
            "Total capacity: {:.1} msg/min",
            capacity.total_capacity_per_min
        );
        println!(
            "Total capacity: {:.1} msg/hour",
            capacity.total_capacity_per_hour
        );
        if let Some(drain_time) = capacity.time_to_drain_secs {
            println!("Time to drain queue: {:.1} seconds", drain_time);
        }
        println!();

        // Queue health assessment
        println!("=== 6. Queue Health Assessment ===");
        let health = assess_amqp_queue_health(
            queue,
            stats.messages as usize,
            stats.consumers as usize,
            processing_rate,
            stats.memory as usize,
        );

        println!("Queue: {}", health.queue_name);
        println!("Health status: {:?}", health.health);
        if !health.issues.is_empty() {
            println!("\nIssues:");
            for issue in &health.issues {
                println!("  - {}", issue);
            }
        }
        if !health.recommendations.is_empty() {
            println!("\nRecommendations:");
            for rec in &health.recommendations {
                println!("  - {}", rec);
            }
        }
        println!();
    }

    // Utility functions demonstration
    println!("=== 7. Utility Functions ===");

    // Optimal batch size
    let batch_size = calculate_optimal_amqp_batch_size(
        message_count,
        1024, // 1KB average message
        100,  // 100ms target latency
    );
    println!("Optimal batch size: {}", batch_size);

    // Memory estimation
    let estimated_memory = estimate_amqp_queue_memory(message_count, 1024, QueueType::Classic);
    println!(
        "Estimated memory: {:.2} MB",
        estimated_memory as f64 / 1_000_000.0
    );

    // Channel pool size
    let channel_pool_size = calculate_optimal_amqp_channel_pool_size(
        50,  // Expected concurrency
        100, // 100ms avg operation duration
    );
    println!("Recommended channel pool size: {}", channel_pool_size);

    // Prefetch calculation
    let prefetch = calculate_optimal_amqp_prefetch(
        5,   // 5 consumers
        200, // 200ms avg processing time
        10,  // 10ms network latency
    );
    println!("Recommended prefetch: {}", prefetch);

    // Drain time estimation
    let drain_time = estimate_amqp_drain_time(
        message_count,
        5,    // 5 consumers
        10.0, // 10 msg/sec per consumer
    );
    println!("Estimated drain time: {} seconds", drain_time.as_secs());

    // Pipeline depth
    let pipeline_depth = calculate_amqp_pipeline_depth(
        100, // 100 messages
        10,  // 10ms network latency
    );
    println!("Recommended pipeline depth: {}", pipeline_depth);

    // Confirm latency estimation
    let confirm_latency = estimate_amqp_confirm_latency(
        QueueType::Classic,
        10, // 10ms network latency
    );
    println!("Estimated confirm latency: {} ms", confirm_latency);

    // Maximum throughput
    let max_throughput = calculate_amqp_max_throughput(
        QueueType::Classic,
        1024, // 1KB messages
        1000, // 1 Gbps network
    );
    println!("Estimated max throughput: {:.0} msg/sec", max_throughput);

    // Lazy mode recommendation
    let use_lazy = should_use_amqp_lazy_mode(
        100_000,       // 100K expected messages
        10_240,        // 10KB average message
        1_000_000_000, // 1GB available memory
    );
    println!("Should use lazy mode: {}", use_lazy);

    println!();

    // Message velocity tracking
    println!("=== 8. Message Velocity Tracking ===");
    println!("Simulating queue growth over time...");

    let initial_size = message_count;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Publish more messages
    for i in 1..=50 {
        let msg = MessageBuilder::new("task.test")
            .args(vec![serde_json::json!({"id": message_count + i})])
            .build()?;
        broker.publish(queue, msg).await?;
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    if let Ok(stats) = broker.get_queue_stats(queue).await {
        let velocity = calculate_amqp_message_velocity(
            initial_size,
            stats.messages_ready as usize,
            2.5, // 2.5 second window
        );

        println!("Previous size: {}", velocity.previous_size);
        println!("Current size: {}", velocity.current_size);
        println!("Velocity: {:.2} msg/sec", velocity.velocity);
        println!("Trend: {:?}", velocity.trend);
        println!();
    }

    // Message age distribution
    println!("=== 9. Message Age Distribution (Simulated) ===");
    // In a real scenario, you would extract timestamps from messages
    let simulated_ages = vec![
        5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 70.0, 80.0, 90.0,
        100.0, 120.0, 150.0, 180.0, 200.0,
    ];

    let age_dist = calculate_amqp_message_age_distribution(simulated_ages, 60.0);

    println!("Total messages analyzed: {}", age_dist.total_messages);
    println!("Min age: {:.1} sec", age_dist.min_age_secs);
    println!("Max age: {:.1} sec", age_dist.max_age_secs);
    println!("Avg age: {:.1} sec", age_dist.avg_age_secs);
    println!("P50 (median): {:.1} sec", age_dist.p50_age_secs);
    println!("P95: {:.1} sec", age_dist.p95_age_secs);
    println!("P99: {:.1} sec", age_dist.p99_age_secs);
    println!(
        "Messages exceeding SLA (>60s): {}",
        age_dist.messages_exceeding_sla
    );
    println!();

    // Cleanup
    println!("=== Cleanup ===");
    broker.delete_queue(queue).await?;
    broker.disconnect().await?;
    println!("✓ Cleaned up and disconnected");

    Ok(())
}
