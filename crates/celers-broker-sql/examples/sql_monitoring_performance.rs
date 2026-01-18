//! Production Monitoring and Performance Analysis Example
//!
//! This example demonstrates how to use the monitoring and utilities modules
//! for production-grade queue monitoring, performance analysis, and optimization.
//!
//! Run with: cargo run --example monitoring_performance

use celers_broker_sql::{monitoring::*, utilities::*};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== MySQL Broker Monitoring & Performance Example ===\n");

    // Example 1: Consumer Lag Analysis
    println!("1. Consumer Lag Analysis");
    println!("--------------------------");

    let queue_size = 1500;
    let processing_rate = 25.0; // messages per second
    let target_lag = 60; // target: 60 seconds max lag

    let lag_analysis = analyze_mysql_consumer_lag(queue_size, processing_rate, target_lag);

    println!("Queue Size: {}", lag_analysis.queue_size);
    println!(
        "Processing Rate: {:.2} msg/sec",
        lag_analysis.processing_rate
    );
    println!("Current Lag: {:.2} seconds", lag_analysis.lag_seconds);
    println!("Is Lagging: {}", lag_analysis.is_lagging);

    match lag_analysis.recommendation {
        ScalingRecommendation::ScaleUp { additional_workers } => {
            println!(
                "⚠️  Recommendation: Scale UP by {} workers",
                additional_workers
            );
        }
        ScalingRecommendation::ScaleDown { workers_to_remove } => {
            println!(
                "✓  Recommendation: Scale DOWN by {} workers",
                workers_to_remove
            );
        }
        ScalingRecommendation::Optimal => {
            println!("✓  Recommendation: Current capacity is optimal");
        }
    }
    println!();

    // Example 2: Message Velocity Analysis
    println!("2. Message Velocity & Growth Trends");
    println!("-------------------------------------");

    let previous_size = 1000;
    let current_size = 1500;
    let time_window = 60.0; // 60 seconds

    let velocity = calculate_mysql_message_velocity(previous_size, current_size, time_window);

    println!("Previous Size: {}", velocity.previous_size);
    println!("Current Size: {}", velocity.current_size);
    println!("Velocity: {:.2} msg/sec", velocity.velocity);
    println!("Trend: {:?}", velocity.trend);

    match velocity.trend {
        QueueTrend::RapidGrowth => println!("⚠️  Queue is growing rapidly!"),
        QueueTrend::SlowGrowth => println!("ℹ️  Queue is growing slowly"),
        QueueTrend::Stable => println!("✓  Queue is stable"),
        QueueTrend::SlowShrink => println!("✓  Queue is shrinking slowly"),
        QueueTrend::RapidShrink => println!("✓  Queue is shrinking rapidly"),
    }
    println!();

    // Example 3: Worker Scaling Recommendations
    println!("3. Worker Scaling Recommendations");
    println!("-----------------------------------");

    let current_workers = 5;
    let avg_processing_rate = 30.0; // per worker

    let scaling =
        suggest_mysql_worker_scaling(queue_size, current_workers, avg_processing_rate, target_lag);

    println!("Current Workers: {}", scaling.current_workers);
    println!(
        "Current Total Rate: {:.2} msg/sec",
        current_workers as f64 * avg_processing_rate
    );
    println!("Recommended Workers: {}", scaling.recommended_workers);

    match scaling.action {
        ScalingRecommendation::ScaleUp { additional_workers } => {
            println!("Action: Add {} more workers", additional_workers);
        }
        ScalingRecommendation::ScaleDown { workers_to_remove } => {
            println!("Action: Remove {} workers", workers_to_remove);
        }
        ScalingRecommendation::Optimal => {
            println!("Action: No scaling needed");
        }
    }
    println!();

    // Example 4: Message Age Distribution (SLA Monitoring)
    println!("4. Message Age Distribution (SLA Monitoring)");
    println!("---------------------------------------------");

    // Simulate message ages (in seconds)
    let message_ages = vec![
        5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0,
        80.0, 85.0, 90.0, 95.0, 100.0, 120.0, 150.0, 180.0, 200.0, 250.0,
    ];
    let sla_threshold = 120.0; // 2 minutes SLA

    let age_dist = calculate_mysql_message_age_distribution(&message_ages, sla_threshold);

    println!("Total Messages: {}", age_dist.total_messages);
    println!("Min Age: {:.2}s", age_dist.min_age_secs);
    println!("Max Age: {:.2}s", age_dist.max_age_secs);
    println!("Avg Age: {:.2}s", age_dist.avg_age_secs);
    println!("P50 (Median): {:.2}s", age_dist.p50_age_secs);
    println!("P95: {:.2}s", age_dist.p95_age_secs);
    println!("P99: {:.2}s", age_dist.p99_age_secs);
    println!(
        "Messages exceeding SLA ({:.0}s): {}",
        sla_threshold, age_dist.messages_exceeding_sla
    );

    let sla_compliance =
        100.0 * (1.0 - age_dist.messages_exceeding_sla as f64 / age_dist.total_messages as f64);
    println!("SLA Compliance: {:.2}%", sla_compliance);
    println!();

    // Example 5: Processing Capacity Estimation
    println!("5. Processing Capacity Estimation");
    println!("----------------------------------");

    let workers = 10;
    let rate_per_worker = 50.0;
    let backlog = 5000;

    let capacity = estimate_mysql_processing_capacity(workers, rate_per_worker, backlog);

    println!("Workers: {}", capacity.workers);
    println!("Rate per Worker: {:.2} msg/sec", capacity.rate_per_worker);
    println!(
        "Total Capacity: {:.2} msg/sec",
        capacity.total_capacity_per_sec
    );
    println!(
        "               {:.2} msg/min",
        capacity.total_capacity_per_min
    );
    println!(
        "               {:.2} msg/hour",
        capacity.total_capacity_per_hour
    );
    println!(
        "Time to Clear Backlog: {:.2} seconds ({:.2} minutes)",
        capacity.time_to_clear_backlog_secs,
        capacity.time_to_clear_backlog_secs / 60.0
    );
    println!();

    // Example 6: Queue Health Score
    println!("6. Queue Health Score");
    println!("----------------------");

    let max_acceptable_size = 10000;
    let target_processing_rate = 100.0;

    let health_score = calculate_mysql_queue_health_score(
        queue_size,
        processing_rate,
        max_acceptable_size,
        target_processing_rate,
    );

    println!("Queue Size: {} / {} max", queue_size, max_acceptable_size);
    println!(
        "Processing Rate: {:.2} / {:.2} target",
        processing_rate, target_processing_rate
    );
    println!(
        "Health Score: {:.2} (0.0 = unhealthy, 1.0 = healthy)",
        health_score
    );

    if health_score > 0.8 {
        println!("✓  Queue is healthy");
    } else if health_score > 0.5 {
        println!("⚠️  Queue health is moderate");
    } else {
        println!("❌ Queue health is poor");
    }
    println!();

    // Example 7: Broker Performance Analysis
    println!("7. Broker Performance Analysis");
    println!("-------------------------------");

    let mut metrics = HashMap::new();
    metrics.insert("avg_latency_ms".to_string(), 35.0);
    metrics.insert("throughput_msg_per_sec".to_string(), 450.0);
    metrics.insert("error_rate_percent".to_string(), 0.8);

    let analysis = analyze_mysql_broker_performance(&metrics);

    println!("Metrics:");
    println!("  Avg Latency: {:.2}ms", metrics["avg_latency_ms"]);
    println!(
        "  Throughput: {:.2} msg/sec",
        metrics["throughput_msg_per_sec"]
    );
    println!("  Error Rate: {:.2}%", metrics["error_rate_percent"]);
    println!("\nAnalysis:");
    println!(
        "  Latency Status: {}",
        analysis
            .get("latency_status")
            .unwrap_or(&"unknown".to_string())
    );
    println!(
        "  Throughput Status: {}",
        analysis
            .get("throughput_status")
            .unwrap_or(&"unknown".to_string())
    );
    println!(
        "  Error Rate Status: {}",
        analysis
            .get("error_rate_status")
            .unwrap_or(&"unknown".to_string())
    );
    println!();

    // Example 8: Batch Size Optimization
    println!("8. Performance Utilities - Batch Size Optimization");
    println!("---------------------------------------------------");

    let avg_message_size = 2048; // 2KB
    let target_latency_ms = 100;

    let optimal_batch =
        calculate_optimal_mysql_batch_size(queue_size, avg_message_size, target_latency_ms);

    println!("Queue Size: {}", queue_size);
    println!("Avg Message Size: {} bytes", avg_message_size);
    println!("Target Latency: {}ms", target_latency_ms);
    println!("Recommended Batch Size: {}", optimal_batch);
    println!();

    // Example 9: Memory Estimation
    println!("9. Queue Memory Estimation");
    println!("---------------------------");

    let estimated_memory = estimate_mysql_queue_memory(queue_size, avg_message_size);

    println!("Queue Size: {}", queue_size);
    println!("Avg Message Size: {} bytes", avg_message_size);
    println!(
        "Estimated Memory: {} bytes ({:.2} MB)",
        estimated_memory,
        estimated_memory as f64 / 1_048_576.0
    );
    println!();

    // Example 10: Connection Pool Sizing
    println!("10. Connection Pool Sizing");
    println!("---------------------------");

    let expected_concurrency = 50;
    let avg_operation_ms = 75;

    let pool_size = calculate_optimal_mysql_pool_size(expected_concurrency, avg_operation_ms);

    println!("Expected Concurrency: {}", expected_concurrency);
    println!("Avg Operation Duration: {}ms", avg_operation_ms);
    println!("Recommended Pool Size: {}", pool_size);
    println!();

    // Example 11: Query Strategy Recommendations
    println!("11. Query Strategy Recommendations");
    println!("-----------------------------------");

    let small_ops = 5;
    let medium_ops = 50;
    let large_ops = 1000;

    println!("For {} write operations:", small_ops);
    println!("  {}", suggest_mysql_query_strategy(small_ops, "write"));

    println!("For {} write operations:", medium_ops);
    println!("  {}", suggest_mysql_query_strategy(medium_ops, "write"));

    println!("For {} write operations:", large_ops);
    println!("  {}", suggest_mysql_query_strategy(large_ops, "write"));
    println!();

    // Example 12: OPTIMIZE TABLE Strategy
    println!("12. OPTIMIZE TABLE Strategy Recommendations");
    println!("---------------------------------------------");

    let scenarios = vec![
        (60.0, 500.0),  // High fragmentation, medium table
        (25.0, 2000.0), // Moderate fragmentation, large table
        (10.0, 100.0),  // Low fragmentation, small table
        (5.0, 50.0),    // Minimal fragmentation, tiny table
    ];

    for (fragmentation_percent, table_size_mb) in scenarios {
        println!(
            "Table: {:.0}MB, Fragmentation: {:.1}%",
            table_size_mb, fragmentation_percent
        );
        println!(
            "  {}",
            suggest_mysql_optimize_strategy(fragmentation_percent, table_size_mb)
        );
    }
    println!();

    // Example 13: Timeout Calculations
    println!("13. Optimal Timeout Values");
    println!("---------------------------");

    let avg_op_ms = 50.0;
    let p99_op_ms = 200.0;

    let (connect_timeout, wait_timeout) = calculate_mysql_timeout_values(avg_op_ms, p99_op_ms);

    println!("Avg Operation: {:.0}ms", avg_op_ms);
    println!("P99 Operation: {:.0}ms", p99_op_ms);
    println!("Recommended connect_timeout: {}s", connect_timeout);
    println!("Recommended wait_timeout: {}s", wait_timeout);
    println!();

    // Example 14: MySQL Configuration Recommendations
    println!("14. MySQL Configuration Recommendations");
    println!("-----------------------------------------");

    let total_ram_gb = 32.0;
    let database_size_gb = 15.0;
    let avg_sort_size_mb = 8.0;
    let concurrent_workers = 20;

    let buffer_pool = suggest_mysql_innodb_buffer_pool_size(total_ram_gb, database_size_gb);
    let sort_buffer =
        suggest_mysql_sort_buffer_size(avg_sort_size_mb, concurrent_workers, total_ram_gb);

    println!("System RAM: {:.0}GB", total_ram_gb);
    println!("Database Size: {:.0}GB", database_size_gb);
    println!("\nRecommended Configuration:");
    println!(
        "  innodb_buffer_pool_size = {}MB ({:.2}GB)",
        buffer_pool,
        buffer_pool as f64 / 1024.0
    );
    println!("  sort_buffer_size = {}MB", sort_buffer);
    println!();

    // Example 15: InnoDB Tuning Recommendations
    println!("15. InnoDB Tuning for Different Workloads");
    println!("-------------------------------------------");

    let workloads = vec![
        (50.0, 5.0, "Light"),
        (500.0, 15.0, "Medium"),
        (1500.0, 60.0, "Heavy"),
    ];

    for (throughput, table_size, label) in workloads {
        println!(
            "{} Workload ({}msg/sec, {:.0}GB):",
            label, throughput, table_size
        );
        let config = suggest_mysql_innodb_tuning(throughput, table_size);
        println!("  {}", config);
    }
    println!();

    // Example 16: Index Strategy Analysis
    println!("16. Index Strategy Analysis");
    println!("----------------------------");

    let index_scenarios = vec![
        (10000, 100, 1_000_000, "Good index usage"),
        (100, 10000, 1_000_000, "High full table scans"),
        (5000, 5000, 500_000, "Balanced usage"),
    ];

    for (index_scans, full_scans, rows, label) in index_scenarios {
        println!(
            "{} ({} index, {} full, {} rows):",
            label, index_scans, full_scans, rows
        );
        let recommendation = suggest_mysql_index_strategy(index_scans, full_scans, rows);
        println!("  {}", recommendation);
    }
    println!();

    // Example 17: Query Performance Analysis
    println!("17. Query Performance Analysis");
    println!("-------------------------------");

    let mut query_latencies = HashMap::new();
    query_latencies.insert("enqueue".to_string(), 4.5);
    query_latencies.insert("dequeue".to_string(), 12.0);
    query_latencies.insert("ack".to_string(), 3.2);
    query_latencies.insert("reject".to_string(), 7.5);
    query_latencies.insert("get_statistics".to_string(), 25.0);

    let query_analysis = analyze_mysql_query_performance(&query_latencies);

    println!("Query Latencies:");
    for (query, latency) in &query_latencies {
        println!("  {}: {:.2}ms", query, latency);
    }
    println!("\nAnalysis:");
    println!(
        "  Slowest Query: {}",
        query_analysis.get("slowest_query").unwrap()
    );
    println!(
        "  Max Latency: {}ms",
        query_analysis.get("max_latency_ms").unwrap()
    );
    println!(
        "  Avg Latency: {}ms",
        query_analysis.get("avg_latency_ms").unwrap()
    );
    println!(
        "  Overall Status: {}",
        query_analysis.get("overall_status").unwrap()
    );
    println!();

    // Example 18: Max Allowed Packet Sizing
    println!("18. max_allowed_packet Configuration");
    println!("--------------------------------------");

    let message_size_scenarios = vec![
        (0.5, "Small messages"),
        (5.0, "Medium messages"),
        (16.0, "Large messages"),
        (50.0, "Very large messages"),
    ];

    for (max_msg_size_mb, label) in message_size_scenarios {
        println!("{} ({:.1}MB max):", label, max_msg_size_mb);
        let max_packet = suggest_mysql_max_allowed_packet(max_msg_size_mb);
        println!("  Recommended max_allowed_packet: {}MB", max_packet);
    }
    println!();

    println!("=== Example Complete ===");
    println!("\nThese utilities provide production-ready monitoring and optimization");
    println!("capabilities for MySQL-based task queues. Integrate them into your");
    println!("monitoring dashboards and alerting systems for optimal queue performance.");
    println!("\nKey MySQL-Specific Recommendations:");
    println!("  - Use InnoDB buffer pool size = 70-80% of RAM for dedicated servers");
    println!("  - Configure max_allowed_packet based on your largest message size");
    println!("  - Run OPTIMIZE TABLE regularly on high-churn tables");
    println!("  - Monitor index usage with EXPLAIN and adjust as needed");
    println!("  - Set appropriate timeouts to prevent connection buildup");

    Ok(())
}
