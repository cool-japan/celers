//! Example demonstrating MySQL broker monitoring and utilities
//!
//! This example shows how to use the monitoring and utilities modules
//! for production-grade queue monitoring, autoscaling decisions, and
//! performance tuning.
//!
//! Usage:
//! ```bash
//! cargo run --example monitoring_utilities
//! ```

use celers_broker_sql::monitoring::*;
use celers_broker_sql::utilities::*;
use std::collections::HashMap;

fn main() {
    println!("=== MySQL Broker Monitoring & Utilities Example ===\n");

    // Monitoring Examples
    println!("--- Monitoring Examples ---\n");

    // 1. Consumer Lag Analysis
    println!("1. Consumer Lag Analysis:");
    let queue_size = 5000;
    let processing_rate = 50.0; // messages per second
    let target_lag_seconds = 60;

    let lag = analyze_mysql_consumer_lag(queue_size, processing_rate, target_lag_seconds);
    println!("   Queue size: {}", lag.queue_size);
    println!("   Processing rate: {} msg/sec", lag.processing_rate);
    println!("   Current lag: {:.2} seconds", lag.lag_seconds);
    println!("   Is lagging: {}", lag.is_lagging);
    println!("   Recommendation: {:?}", lag.recommendation);
    println!();

    // 2. Message Velocity and Trend
    println!("2. Message Velocity Analysis:");
    let previous_size = 1000;
    let current_size = 1500;
    let time_window_secs = 60.0;

    let velocity = calculate_mysql_message_velocity(previous_size, current_size, time_window_secs);
    println!("   Previous size: {}", velocity.previous_size);
    println!("   Current size: {}", velocity.current_size);
    println!("   Time window: {} seconds", velocity.time_window_secs);
    println!("   Velocity: {:.2} msg/sec", velocity.velocity);
    println!("   Trend: {:?}", velocity.trend);
    println!();

    // 3. Worker Scaling Suggestion
    println!("3. Worker Scaling Recommendation:");
    let current_workers = 5;
    let avg_processing_rate = 40.0; // per worker

    let scaling = suggest_mysql_worker_scaling(
        queue_size,
        current_workers,
        avg_processing_rate,
        target_lag_seconds,
    );
    println!("   Current workers: {}", scaling.current_workers);
    println!(
        "   Avg rate per worker: {} msg/sec",
        scaling.avg_processing_rate
    );
    println!("   Recommended workers: {}", scaling.recommended_workers);
    println!("   Action: {:?}", scaling.action);
    println!();

    // 4. Message Age Distribution (SLA Monitoring)
    println!("4. Message Age Distribution (SLA Monitoring):");
    let message_ages = vec![
        5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 45.0, 60.0, 75.0, 90.0, 120.0, 150.0,
    ];
    let sla_threshold_secs = 60.0;

    let age_dist = calculate_mysql_message_age_distribution(&message_ages, sla_threshold_secs);
    println!("   Total messages: {}", age_dist.total_messages);
    println!("   Min age: {:.2}s", age_dist.min_age_secs);
    println!("   Max age: {:.2}s", age_dist.max_age_secs);
    println!("   Avg age: {:.2}s", age_dist.avg_age_secs);
    println!("   P50 age: {:.2}s", age_dist.p50_age_secs);
    println!("   P95 age: {:.2}s", age_dist.p95_age_secs);
    println!("   P99 age: {:.2}s", age_dist.p99_age_secs);
    println!(
        "   Messages exceeding SLA ({:.0}s): {}",
        sla_threshold_secs, age_dist.messages_exceeding_sla
    );
    println!();

    // 5. Processing Capacity Estimation
    println!("5. Processing Capacity Estimation:");
    let workers = 10;
    let rate_per_worker = 50.0;
    let current_backlog = 5000;

    let capacity = estimate_mysql_processing_capacity(workers, rate_per_worker, current_backlog);
    println!("   Workers: {}", capacity.workers);
    println!("   Rate per worker: {} msg/sec", capacity.rate_per_worker);
    println!(
        "   Total capacity: {} msg/sec",
        capacity.total_capacity_per_sec
    );
    println!(
        "   Total capacity: {} msg/min",
        capacity.total_capacity_per_min
    );
    println!(
        "   Total capacity: {} msg/hour",
        capacity.total_capacity_per_hour
    );
    println!(
        "   Time to clear backlog: {:.2} seconds ({:.2} minutes)",
        capacity.time_to_clear_backlog_secs,
        capacity.time_to_clear_backlog_secs / 60.0
    );
    println!();

    // 6. Queue Health Score
    println!("6. Queue Health Score:");
    let max_acceptable_size = 10000;
    let target_processing_rate = 60.0;

    let health_score = calculate_mysql_queue_health_score(
        queue_size,
        processing_rate,
        max_acceptable_size,
        target_processing_rate,
    );
    println!(
        "   Health score: {:.2} (0.0 = unhealthy, 1.0 = healthy)",
        health_score
    );
    let status = if health_score > 0.8 {
        "Excellent"
    } else if health_score > 0.6 {
        "Good"
    } else if health_score > 0.4 {
        "Fair"
    } else {
        "Poor"
    };
    println!("   Status: {}", status);
    println!();

    // 7. Broker Performance Analysis
    println!("7. Broker Performance Analysis:");
    let mut metrics = HashMap::new();
    metrics.insert("avg_latency_ms".to_string(), 25.0);
    metrics.insert("throughput_msg_per_sec".to_string(), 500.0);
    metrics.insert("error_rate_percent".to_string(), 0.5);

    let analysis = analyze_mysql_broker_performance(&metrics);
    println!(
        "   Latency status: {}",
        analysis.get("latency_status").unwrap()
    );
    println!(
        "   Throughput status: {}",
        analysis.get("throughput_status").unwrap()
    );
    println!(
        "   Error rate status: {}",
        analysis.get("error_rate_status").unwrap()
    );
    println!();

    // Utilities Examples
    println!("\n--- Utilities Examples ---\n");

    // 1. Optimal Batch Size Calculation
    println!("1. Optimal Batch Size:");
    let queue_size = 10000;
    let avg_message_size = 2048; // 2KB
    let target_latency_ms = 100;

    let batch_size =
        calculate_optimal_mysql_batch_size(queue_size, avg_message_size, target_latency_ms);
    println!("   Queue size: {}", queue_size);
    println!("   Avg message size: {} bytes", avg_message_size);
    println!("   Target latency: {}ms", target_latency_ms);
    println!("   Recommended batch size: {}", batch_size);
    println!();

    // 2. Memory Usage Estimation
    println!("2. Queue Memory Estimation:");
    let memory = estimate_mysql_queue_memory(queue_size, avg_message_size);
    println!("   Queue size: {}", queue_size);
    println!("   Avg message size: {} bytes", avg_message_size);
    println!(
        "   Estimated memory: {} bytes ({:.2} MB)",
        memory,
        memory as f64 / 1024.0 / 1024.0
    );
    println!();

    // 3. Optimal Pool Size
    println!("3. Optimal Connection Pool Size:");
    let expected_concurrency = 100;
    let avg_operation_duration_ms = 50;

    let pool_size =
        calculate_optimal_mysql_pool_size(expected_concurrency, avg_operation_duration_ms);
    println!("   Expected concurrency: {}", expected_concurrency);
    println!("   Avg operation duration: {}ms", avg_operation_duration_ms);
    println!("   Recommended pool size: {}", pool_size);
    println!();

    // 4. Queue Drain Time
    println!("4. Queue Drain Time Estimation:");
    let drain_time = estimate_mysql_queue_drain_time(queue_size, processing_rate);
    println!("   Queue size: {}", queue_size);
    println!("   Processing rate: {} msg/sec", processing_rate);
    println!(
        "   Time to drain: {:.2} seconds ({:.2} minutes)",
        drain_time,
        drain_time / 60.0
    );
    println!();

    // 5. Query Strategy Recommendation
    println!("5. Query Strategy Recommendation:");
    for &operation_count in &[5, 50, 500] {
        let strategy = suggest_mysql_query_strategy(operation_count, "write");
        println!("   {} operations: {}", operation_count, strategy);
    }
    println!();

    // 6. OPTIMIZE TABLE Strategy
    println!("6. OPTIMIZE TABLE Strategy:");
    let table_fragmentation = 35.0;
    let table_size_mb = 500.0;

    let optimize_strategy = suggest_mysql_optimize_strategy(table_fragmentation, table_size_mb);
    println!("   Table fragmentation: {}%", table_fragmentation);
    println!("   Table size: {} MB", table_size_mb);
    println!("   Recommendation: {}", optimize_strategy);
    println!();

    // 7. Index Strategy Recommendation
    println!("7. Index Strategy Recommendation:");
    let index_scan_count = 1000;
    let full_scan_count = 5000;
    let table_rows = 1_000_000;

    let index_strategy =
        suggest_mysql_index_strategy(index_scan_count, full_scan_count, table_rows);
    println!("   Index scans: {}", index_scan_count);
    println!("   Full table scans: {}", full_scan_count);
    println!("   Table rows: {}", table_rows);
    println!("   Recommendation: {}", index_strategy);
    println!();

    // 8. Query Performance Analysis
    println!("8. Query Performance Analysis:");
    let mut query_latencies = HashMap::new();
    query_latencies.insert("enqueue".to_string(), 5.0);
    query_latencies.insert("dequeue".to_string(), 15.0);
    query_latencies.insert("ack".to_string(), 3.0);
    query_latencies.insert("reject".to_string(), 8.0);

    let query_analysis = analyze_mysql_query_performance(&query_latencies);
    println!(
        "   Slowest query: {}",
        query_analysis.get("slowest_query").unwrap()
    );
    println!(
        "   Max latency: {}ms",
        query_analysis.get("max_latency_ms").unwrap()
    );
    println!(
        "   Avg latency: {}ms",
        query_analysis.get("avg_latency_ms").unwrap()
    );
    println!(
        "   Overall status: {}",
        query_analysis.get("overall_status").unwrap()
    );
    println!();

    // 9. InnoDB Tuning Recommendation
    println!("9. InnoDB Buffer Pool Tuning:");
    let throughput_msg_per_sec = 500.0;
    let table_size_gb = 20.0;

    let innodb_config = suggest_mysql_innodb_tuning(throughput_msg_per_sec, table_size_gb);
    println!("   Throughput: {} msg/sec", throughput_msg_per_sec);
    println!("   Table size: {} GB", table_size_gb);
    println!("   Recommendation: {}", innodb_config);
    println!();

    // 10. Timeout Values
    println!("10. MySQL Timeout Values:");
    let avg_op_ms = 50.0;
    let p99_op_ms = 200.0;

    let (connect_timeout, wait_timeout) = calculate_mysql_timeout_values(avg_op_ms, p99_op_ms);
    println!("   Avg operation: {}ms", avg_op_ms);
    println!("   P99 operation: {}ms", p99_op_ms);
    println!("   connect_timeout: {}s", connect_timeout);
    println!("   wait_timeout: {}s", wait_timeout);
    println!();

    // 11. Sort Buffer Size
    println!("11. Sort Buffer Size:");
    let avg_sort_size_mb = 5.0;
    let concurrent_workers = 20;
    let total_ram_gb = 32.0;

    let sort_buffer =
        suggest_mysql_sort_buffer_size(avg_sort_size_mb, concurrent_workers, total_ram_gb);
    println!("   Avg sort size: {} MB", avg_sort_size_mb);
    println!("   Concurrent workers: {}", concurrent_workers);
    println!("   Total RAM: {} GB", total_ram_gb);
    println!("   Recommended sort_buffer_size: {} MB", sort_buffer);
    println!();

    // 12. InnoDB Buffer Pool Size
    println!("12. InnoDB Buffer Pool Size:");
    let database_size_gb = 25.0;

    let buffer_pool = suggest_mysql_innodb_buffer_pool_size(total_ram_gb, database_size_gb);
    println!("   Total RAM: {} GB", total_ram_gb);
    println!("   Database size: {} GB", database_size_gb);
    println!(
        "   Recommended innodb_buffer_pool_size: {} MB ({:.2} GB)",
        buffer_pool,
        buffer_pool as f64 / 1024.0
    );
    println!();

    // 13. Max Allowed Packet
    println!("13. Max Allowed Packet:");
    let max_message_size_mb = 8.0;

    let max_packet = suggest_mysql_max_allowed_packet(max_message_size_mb);
    println!("   Max message size: {} MB", max_message_size_mb);
    println!("   Recommended max_allowed_packet: {} MB", max_packet);
    println!();

    println!("=== Example Complete ===");
}
