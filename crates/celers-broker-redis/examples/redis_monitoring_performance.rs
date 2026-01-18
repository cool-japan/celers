//! Comprehensive Redis broker monitoring and performance tuning example
//!
//! This example demonstrates:
//! - Consumer lag analysis and autoscaling decisions
//! - Message velocity tracking and queue growth trends
//! - Worker scaling recommendations
//! - Message age distribution for SLA monitoring
//! - Processing capacity estimation
//! - Queue health score calculation
//! - Utility functions for performance optimization

use celers_broker_redis::monitoring::*;
use celers_broker_redis::utilities::*;
use celers_broker_redis::QueueMode;
use std::collections::HashMap;

#[allow(dead_code)]
fn main() {
    println!("=== Redis Broker Monitoring & Performance Example ===\n");

    // ===================================================
    // Section 1: Consumer Lag Analysis and Autoscaling
    // ===================================================
    println!("1. Consumer Lag Analysis");
    println!("   --------------------");

    let queue_size = 5000;
    let processing_rate = 45.0; // msg/sec
    let target_lag = 60; // seconds

    let lag_analysis = analyze_redis_consumer_lag(queue_size, processing_rate, target_lag);

    println!("   Queue size: {} messages", lag_analysis.queue_size);
    println!(
        "   Processing rate: {:.1} msg/sec",
        lag_analysis.processing_rate
    );
    println!(
        "   Current lag: {:.1} seconds (target: {} sec)",
        lag_analysis.lag_seconds, lag_analysis.target_lag_seconds
    );
    println!("   Is lagging: {}", lag_analysis.is_lagging);
    println!("   Recommendation: {:?}", lag_analysis.recommendation);

    match lag_analysis.recommendation {
        ScalingRecommendation::ScaleUp { additional_workers } => {
            println!(
                "   ACTION: Scale up by {} workers to meet target lag",
                additional_workers
            );
        }
        ScalingRecommendation::ScaleDown { workers_to_remove } => {
            println!("   ACTION: Can scale down by {} workers", workers_to_remove);
        }
        ScalingRecommendation::Optimal => {
            println!("   ACTION: Current worker count is optimal");
        }
    }
    println!();

    // ===================================================
    // Section 2: Message Velocity and Queue Growth Trends
    // ===================================================
    println!("2. Message Velocity and Queue Growth");
    println!("   ----------------------------------");

    let previous_size = 1000;
    let current_size = 1800;
    let time_window = 60.0; // seconds

    let velocity = calculate_redis_message_velocity(previous_size, current_size, time_window);

    println!("   Previous size: {} messages", velocity.previous_size);
    println!("   Current size: {} messages", velocity.current_size);
    println!("   Time window: {:.0} seconds", velocity.time_window_secs);
    println!("   Velocity: {:.2} msg/sec", velocity.velocity);
    println!("   Trend: {:?}", velocity.trend);

    match velocity.trend {
        QueueTrend::RapidGrowth => println!("   WARNING: Queue growing rapidly!"),
        QueueTrend::SlowGrowth => println!("   INFO: Queue growing slowly"),
        QueueTrend::Stable => println!("   INFO: Queue size is stable"),
        QueueTrend::SlowShrink => println!("   INFO: Queue shrinking slowly"),
        QueueTrend::RapidShrink => println!("   INFO: Queue shrinking rapidly"),
    }
    println!();

    // ===================================================
    // Section 3: Worker Scaling Recommendations
    // ===================================================
    println!("3. Worker Scaling Recommendations");
    println!("   -------------------------------");

    let current_workers = 8;
    let avg_worker_rate = 45.0; // msg/sec per worker
    let target_lag_secs = 100;

    let scaling = suggest_redis_worker_scaling(
        queue_size,
        current_workers,
        avg_worker_rate,
        target_lag_secs,
    );

    println!("   Current queue size: {}", scaling.queue_size);
    println!("   Current workers: {}", scaling.current_workers);
    println!(
        "   Avg processing rate: {:.1} msg/sec/worker",
        scaling.avg_processing_rate
    );
    println!("   Target lag: {} seconds", scaling.target_lag_seconds);
    println!("   Recommended workers: {}", scaling.recommended_workers);
    println!("   Action: {:?}", scaling.action);

    if scaling.recommended_workers != scaling.current_workers {
        let diff = scaling.recommended_workers as i32 - scaling.current_workers as i32;
        if diff > 0 {
            println!("   ACTION: Add {} workers", diff);
        } else {
            println!("   ACTION: Remove {} workers", -diff);
        }
    }
    println!();

    // ===================================================
    // Section 4: Message Age Distribution (SLA Monitoring)
    // ===================================================
    println!("4. Message Age Distribution (SLA Monitoring)");
    println!("   -----------------------------------------");

    let message_ages: Vec<f64> = vec![
        5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0,
        80.0, 85.0, 90.0, 95.0, 100.0,
    ];
    let sla_threshold = 60.0; // seconds

    let age_dist = calculate_redis_message_age_distribution(&message_ages, sla_threshold);

    println!("   Total messages analyzed: {}", age_dist.total_messages);
    println!(
        "   Min/Avg/Max age: {:.1}/{:.1}/{:.1} seconds",
        age_dist.min_age_secs, age_dist.avg_age_secs, age_dist.max_age_secs
    );
    println!(
        "   Age percentiles (P50/P95/P99): {:.1}/{:.1}/{:.1} seconds",
        age_dist.p50_age_secs, age_dist.p95_age_secs, age_dist.p99_age_secs
    );
    println!("   SLA threshold: {:.0} seconds", sla_threshold);
    println!(
        "   Messages exceeding SLA: {} ({:.1}%)",
        age_dist.messages_exceeding_sla,
        (age_dist.messages_exceeding_sla as f64 / age_dist.total_messages as f64) * 100.0
    );

    if age_dist.messages_exceeding_sla > 0 {
        println!("   WARNING: Some messages are exceeding SLA threshold!");
    } else {
        println!("   INFO: All messages are within SLA threshold");
    }
    println!();

    // ===================================================
    // Section 5: Processing Capacity Estimation
    // ===================================================
    println!("5. Processing Capacity Estimation");
    println!("   -------------------------------");

    let workers = 10;
    let rate_per_worker = 50.0; // msg/sec
    let backlog = 5000;

    let capacity = estimate_redis_processing_capacity(workers, rate_per_worker, backlog);

    println!("   Workers: {}", capacity.workers);
    println!(
        "   Rate per worker: {:.1} msg/sec",
        capacity.rate_per_worker
    );
    println!(
        "   Total capacity: {:.1} msg/sec",
        capacity.total_capacity_per_sec
    );
    println!(
        "   Total capacity: {:.0} msg/min",
        capacity.total_capacity_per_min
    );
    println!(
        "   Total capacity: {:.0} msg/hour",
        capacity.total_capacity_per_hour
    );
    println!(
        "   Time to clear backlog: {:.1} seconds ({:.1} minutes)",
        capacity.time_to_clear_backlog_secs,
        capacity.time_to_clear_backlog_secs / 60.0
    );
    println!();

    // ===================================================
    // Section 6: Queue Health Score
    // ===================================================
    println!("6. Queue Health Score");
    println!("   ------------------");

    let max_acceptable = 10000;
    let target_rate = 100.0;

    let health_score = calculate_redis_queue_health_score(
        queue_size,
        processing_rate,
        max_acceptable,
        target_rate,
    );

    println!("   Current queue size: {}", queue_size);
    println!("   Max acceptable size: {}", max_acceptable);
    println!("   Current processing rate: {:.1} msg/sec", processing_rate);
    println!("   Target processing rate: {:.1} msg/sec", target_rate);
    println!(
        "   Health score: {:.2} (0.0 = unhealthy, 1.0 = healthy)",
        health_score
    );

    if health_score > 0.8 {
        println!("   STATUS: Excellent health");
    } else if health_score > 0.6 {
        println!("   STATUS: Good health");
    } else if health_score > 0.4 {
        println!("   STATUS: Fair health - monitoring recommended");
    } else {
        println!("   STATUS: Poor health - action required!");
    }
    println!();

    // ===================================================
    // Section 7: Performance Utilities
    // ===================================================
    println!("7. Performance Optimization Utilities");
    println!("   -----------------------------------");

    // Optimal batch size
    let batch_size = calculate_optimal_redis_batch_size(queue_size, 2048, 100);
    println!("   Optimal batch size: {}", batch_size);

    // Memory estimation
    let memory_fifo = estimate_redis_queue_memory(queue_size, 2048, QueueMode::Fifo);
    let memory_priority = estimate_redis_queue_memory(queue_size, 2048, QueueMode::Priority);
    println!(
        "   Estimated memory (FIFO): {} bytes ({:.2} MB)",
        memory_fifo,
        memory_fifo as f64 / 1024.0 / 1024.0
    );
    println!(
        "   Estimated memory (Priority): {} bytes ({:.2} MB)",
        memory_priority,
        memory_priority as f64 / 1024.0 / 1024.0
    );

    // Pool size
    let pool_size = calculate_optimal_redis_pool_size(100, 50);
    println!("   Optimal connection pool size: {}", pool_size);

    // Pipeline size
    let pipeline_size = calculate_redis_pipeline_size(500, 10);
    println!("   Optimal pipeline size: {}", pipeline_size);

    // Drain time
    let drain_time = estimate_redis_queue_drain_time(queue_size, processing_rate);
    println!(
        "   Estimated drain time: {:.1} seconds ({:.1} minutes)",
        drain_time,
        drain_time / 60.0
    );

    // Pipeline strategy
    let strategy = suggest_redis_pipeline_strategy(500, "write");
    println!("   Pipeline strategy: {}", strategy);

    // TTL by priority
    let low_ttl = calculate_redis_key_ttl_by_priority(50, 3600);
    let high_ttl = calculate_redis_key_ttl_by_priority(200, 3600);
    println!("   TTL for low priority (50): {} seconds", low_ttl);
    println!("   TTL for high priority (200): {} seconds", high_ttl);

    // Timeout values
    let (conn_timeout, op_timeout) = calculate_redis_timeout_values(50.0, 200.0);
    println!("   Connection timeout: {} ms", conn_timeout);
    println!("   Operation timeout: {} ms", op_timeout);
    println!();

    // ===================================================
    // Section 8: Command Performance Analysis
    // ===================================================
    println!("8. Command Performance Analysis");
    println!("   ----------------------------");

    let mut command_latencies = HashMap::new();
    command_latencies.insert("GET".to_string(), 2.5);
    command_latencies.insert("SET".to_string(), 3.0);
    command_latencies.insert("LPUSH".to_string(), 2.8);
    command_latencies.insert("RPOPLPUSH".to_string(), 4.5);
    command_latencies.insert("ZADD".to_string(), 5.2);
    command_latencies.insert("ZPOPMIN".to_string(), 4.8);

    let analysis = analyze_redis_command_performance(&command_latencies);

    println!(
        "   Slowest command: {}",
        analysis.get("slowest_command").unwrap()
    );
    println!(
        "   Max latency: {} ms",
        analysis.get("max_latency_ms").unwrap()
    );
    println!(
        "   Avg latency: {} ms",
        analysis.get("avg_latency_ms").unwrap()
    );
    println!(
        "   Overall status: {}",
        analysis.get("overall_status").unwrap()
    );
    println!();

    // ===================================================
    // Section 9: Persistence Strategy Recommendations
    // ===================================================
    println!("9. Persistence Strategy Recommendations");
    println!("   -------------------------------------");

    let throughputs = vec![100.0, 500.0, 2000.0];
    let durability_levels = vec!["high", "medium", "low"];

    for throughput in &throughputs {
        for level in &durability_levels {
            let strategy = suggest_redis_persistence_strategy(*throughput, level);
            println!(
                "   Throughput: {:.0} msg/sec, Durability: {} => {}",
                throughput, level, strategy
            );
        }
    }
    println!();

    println!("=== Monitoring Example Complete ===");
}
