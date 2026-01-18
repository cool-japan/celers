//! Comprehensive example of SQS monitoring and utilities
//!
//! This example demonstrates:
//! - Consumer lag analysis and autoscaling recommendations
//! - Message velocity tracking and trend detection
//! - Worker scaling suggestions
//! - Queue health assessment
//! - Cost estimation
//! - Batch size optimization
//! - Throughput capacity estimation
//!
//! Run with:
//! ```bash
//! cargo run --example monitoring_utilities
//! ```

use celers_broker_sqs::monitoring::*;
use celers_broker_sqs::utilities::*;

fn main() {
    println!("=== AWS SQS Monitoring & Utilities Demo ===\n");

    // Scenario: Production queue with some backlog
    let queue_size = 5000;
    let current_workers = 10;
    let avg_processing_rate_per_worker = 25.0; // messages/sec
    let target_lag_seconds = 300; // 5 minutes

    println!("Current Queue State:");
    println!("  - Queue Size: {} messages", queue_size);
    println!("  - Workers: {}", current_workers);
    println!(
        "  - Processing Rate: {} msg/sec per worker\n",
        avg_processing_rate_per_worker
    );

    // 1. Analyze Consumer Lag
    println!("--- Consumer Lag Analysis ---");
    let total_rate = current_workers as f64 * avg_processing_rate_per_worker;
    let lag = analyze_sqs_consumer_lag(queue_size, total_rate, target_lag_seconds);
    println!("  Lag: {:.1} seconds", lag.lag_seconds);
    println!("  Target Lag: {} seconds", lag.target_lag_seconds);
    println!("  Is Lagging: {}", lag.is_lagging);
    println!("  Recommendation: {:?}\n", lag.recommendation);

    // 2. Worker Scaling Suggestion
    println!("--- Worker Scaling Suggestion ---");
    let scaling = suggest_sqs_worker_scaling(
        queue_size,
        current_workers,
        avg_processing_rate_per_worker,
        target_lag_seconds,
    );
    println!("  Current Workers: {}", scaling.current_workers);
    println!("  Recommended Workers: {}", scaling.recommended_workers);
    println!("  Action: {:?}\n", scaling.action);

    // 3. Message Velocity Tracking
    println!("--- Message Velocity Tracking ---");
    let previous_size = 4000;
    let time_window = 60.0; // 1 minute
    let velocity = calculate_sqs_message_velocity(previous_size, queue_size, time_window);
    println!(
        "  Queue grew from {} to {} in {:.0} seconds",
        velocity.previous_size, velocity.current_size, velocity.time_window_secs
    );
    println!("  Velocity: {:.2} msg/sec", velocity.velocity);
    println!("  Trend: {:?}\n", velocity.trend);

    // 4. Processing Capacity Estimation
    println!("--- Processing Capacity ---");
    let capacity = estimate_sqs_processing_capacity(
        current_workers,
        avg_processing_rate_per_worker,
        Some(queue_size),
    );
    println!("  Workers: {}", capacity.workers);
    println!(
        "  Total Capacity: {:.0} msg/sec",
        capacity.total_capacity_per_sec
    );
    println!(
        "  Total Capacity: {:.0} msg/min",
        capacity.total_capacity_per_min
    );
    println!(
        "  Total Capacity: {:.0} msg/hour",
        capacity.total_capacity_per_hour
    );
    if let Some(drain_time) = capacity.time_to_drain_secs {
        println!(
            "  Time to Drain Queue: {:.1} seconds ({:.1} minutes)\n",
            drain_time,
            drain_time / 60.0
        );
    }

    // 5. Queue Health Assessment
    println!("--- Queue Health Assessment ---");
    let in_flight = 200;
    let delayed = 50;
    let oldest_message_age = Some(180.0); // 3 minutes
    let health = assess_sqs_queue_health(
        "production-queue",
        queue_size,
        in_flight,
        delayed,
        total_rate,
        oldest_message_age,
    );
    println!("  Queue: {}", health.queue_name);
    println!("  Total Messages: {}", health.total_messages);
    println!("  In-Flight Messages: {}", health.in_flight_messages);
    println!("  Delayed Messages: {}", health.delayed_messages);
    println!("  Processing Rate: {:.1} msg/sec", health.processing_rate);
    if let Some(age) = health.oldest_message_age_secs {
        println!("  Oldest Message Age: {:.1} seconds", age);
    }
    println!("  Health Status: {:?}", health.health);
    if !health.issues.is_empty() {
        println!("  Issues:");
        for issue in &health.issues {
            println!("    - {}", issue);
        }
    }
    if !health.recommendations.is_empty() {
        println!("  Recommendations:");
        for rec in &health.recommendations {
            println!("    - {}", rec);
        }
    }
    println!();

    // 6. Message Age Distribution
    println!("--- Message Age Distribution ---");
    let message_ages = vec![
        10.0, 20.0, 30.0, 45.0, 60.0, 75.0, 90.0, 120.0, 150.0, 180.0,
    ];
    let sla_threshold = 300.0; // 5 minutes
    let age_dist = calculate_sqs_message_age_distribution(message_ages, sla_threshold);
    println!("  Total Messages Analyzed: {}", age_dist.total_messages);
    println!("  Min Age: {:.1} seconds", age_dist.min_age_secs);
    println!("  Max Age: {:.1} seconds", age_dist.max_age_secs);
    println!("  Avg Age: {:.1} seconds", age_dist.avg_age_secs);
    println!("  P50 Age: {:.1} seconds", age_dist.p50_age_secs);
    println!("  P95 Age: {:.1} seconds", age_dist.p95_age_secs);
    println!("  P99 Age: {:.1} seconds", age_dist.p99_age_secs);
    println!(
        "  Messages Exceeding SLA ({:.0}s): {}\n",
        sla_threshold, age_dist.messages_exceeding_sla
    );

    // 7. Cost Estimation
    println!("--- Cost Estimation ---");
    let messages_per_day = 1_000_000;
    let cost_standard_no_batch = estimate_sqs_monthly_cost(messages_per_day, false, false);
    let cost_standard_batch = estimate_sqs_monthly_cost(messages_per_day, false, true);
    let cost_fifo_no_batch = estimate_sqs_monthly_cost(messages_per_day, true, false);
    let cost_fifo_batch = estimate_sqs_monthly_cost(messages_per_day, true, true);

    println!(
        "  Messages per Day: {} ({} per month)",
        messages_per_day,
        messages_per_day * 30
    );
    println!(
        "  Standard Queue (no batching): ${:.2}/month",
        cost_standard_no_batch
    );
    println!(
        "  Standard Queue (with batching): ${:.2}/month",
        cost_standard_batch
    );
    println!(
        "  FIFO Queue (no batching): ${:.2}/month",
        cost_fifo_no_batch
    );
    println!(
        "  FIFO Queue (with batching): ${:.2}/month",
        cost_fifo_batch
    );
    println!(
        "  Savings with Batching: {:.1}%\n",
        (1.0 - cost_standard_batch / cost_standard_no_batch) * 100.0
    );

    // 8. Batch Size Optimization
    println!("--- Batch Size Optimization ---");
    let avg_message_size = 5_000; // 5KB
    let target_latency_ms = 100;
    let optimal_batch =
        calculate_optimal_sqs_batch_size(queue_size, avg_message_size, target_latency_ms);
    println!("  Avg Message Size: {} bytes", avg_message_size);
    println!("  Target Latency: {} ms", target_latency_ms);
    println!("  Optimal Batch Size: {} messages\n", optimal_batch);

    // 9. Long Polling Wait Time
    println!("--- Long Polling Configuration ---");
    let msg_arrival_rate = 15.0; // messages per second
    let wait_time_cost_optimized = calculate_optimal_sqs_wait_time(msg_arrival_rate, 0.0);
    let wait_time_balanced = calculate_optimal_sqs_wait_time(msg_arrival_rate, 0.5);
    let wait_time_latency_optimized = calculate_optimal_sqs_wait_time(msg_arrival_rate, 1.0);
    println!("  Message Arrival Rate: {:.1} msg/sec", msg_arrival_rate);
    println!(
        "  Cost Optimized Wait Time: {} seconds",
        wait_time_cost_optimized
    );
    println!("  Balanced Wait Time: {} seconds", wait_time_balanced);
    println!(
        "  Latency Optimized Wait Time: {} seconds\n",
        wait_time_latency_optimized
    );

    // 10. Visibility Timeout Calculation
    println!("--- Visibility Timeout Configuration ---");
    let avg_processing_time = 30.0; // seconds
    let variance = 0.3; // 30% variance
    let optimal_timeout = calculate_optimal_sqs_visibility_timeout(avg_processing_time, variance);
    println!("  Avg Processing Time: {:.1} seconds", avg_processing_time);
    println!("  Processing Variance: {:.0}%", variance * 100.0);
    println!(
        "  Optimal Visibility Timeout: {} seconds\n",
        optimal_timeout
    );

    // 11. Throughput Capacity
    println!("--- Throughput Capacity ---");
    let avg_proc_time = 2.0; // 2 seconds per message
    let capacity_no_batch = estimate_sqs_throughput_capacity(current_workers, avg_proc_time, false);
    let capacity_batch = estimate_sqs_throughput_capacity(current_workers, avg_proc_time, true);
    println!("  Workers: {}", current_workers);
    println!("  Avg Processing Time: {:.1} seconds", avg_proc_time);
    println!("  Capacity (no batching): {:.1} msg/sec", capacity_no_batch);
    println!("  Capacity (with batching): {:.1} msg/sec", capacity_batch);
    println!(
        "  Throughput Improvement: {:.1}%\n",
        ((capacity_batch - capacity_no_batch) / capacity_no_batch) * 100.0
    );

    // 12. DLQ Configuration
    println!("--- Dead Letter Queue Configuration ---");
    let failure_rate = 0.05; // 5% failure rate
    let transient_error_prob = 0.3; // 30% of errors are transient
    let dlq_threshold = calculate_sqs_dlq_threshold(failure_rate, transient_error_prob);
    println!("  Failure Rate: {:.0}%", failure_rate * 100.0);
    println!(
        "  Transient Error Probability: {:.0}%",
        transient_error_prob * 100.0
    );
    println!("  Recommended Max Receive Count: {}\n", dlq_threshold);

    // 13. FIFO vs Standard Decision
    println!("--- FIFO vs Standard Queue Decision ---");
    let requires_ordering = true;
    let requires_dedup = false;
    let expected_throughput = 500.0; // msg/sec
    let should_use_fifo =
        should_use_sqs_fifo(requires_ordering, requires_dedup, expected_throughput);
    println!("  Requires Ordering: {}", requires_ordering);
    println!("  Requires Deduplication: {}", requires_dedup);
    println!("  Expected Throughput: {:.0} msg/sec", expected_throughput);
    println!(
        "  Recommendation: Use {} queue\n",
        if should_use_fifo { "FIFO" } else { "Standard" }
    );

    println!("=== Demo Complete ===");
}
