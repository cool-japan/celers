//! Production-ready SQS optimization example
//!
//! This example demonstrates how to use the optimization module to
//! automatically configure and tune an SQS broker for production workloads.
//!
//! Run with:
//! ```bash
//! cargo run --example production_optimization
//! ```

use celers_broker_sqs::optimization::*;

fn main() {
    println!("=== Production SQS Optimization Demo ===\n");

    // Scenario 1: High Throughput E-commerce Backend
    println!("--- Scenario 1: High Throughput E-commerce ---");
    println!("Requirements:");
    println!("  - 500 orders/sec during peak hours");
    println!("  - Average order size: 10 KB");
    println!("  - 20 worker instances\n");

    let ecommerce_config = optimize_for_workload(WorkloadProfile::HighThroughput {
        messages_per_second: 500.0,
        avg_message_size_bytes: 10_000,
        workers: 20,
    });

    print_config("E-commerce Backend", &ecommerce_config);

    // Scenario 2: Low Latency Real-time Notifications
    println!("\n--- Scenario 2: Low Latency Notifications ---");
    println!("Requirements:");
    println!("  - < 50ms notification delivery");
    println!("  - Average notification size: 2 KB");
    println!("  - 10 worker instances\n");

    let notifications_config = optimize_for_workload(WorkloadProfile::LowLatency {
        target_latency_ms: 50,
        avg_message_size_bytes: 2_000,
        workers: 10,
    });

    print_config("Notification Service", &notifications_config);

    // Scenario 3: Cost-Optimized Batch Processing
    println!("\n--- Scenario 3: Cost-Optimized Batch Processing ---");
    println!("Requirements:");
    println!("  - 1M messages/day (background processing)");
    println!("  - Average data record: 5 KB");
    println!("  - Acceptable latency: 5 minutes\n");

    let batch_config = optimize_for_workload(WorkloadProfile::CostOptimized {
        messages_per_day: 1_000_000,
        avg_message_size_bytes: 5_000,
        acceptable_latency_secs: 300,
    });

    print_config("Batch Processor", &batch_config);

    // Scenario 4: Balanced Microservice Communication
    println!("\n--- Scenario 4: Balanced Microservice Queue ---");
    println!("Requirements:");
    println!("  - 100 messages/sec average load");
    println!("  - Average message size: 8 KB");
    println!("  - 10 worker instances\n");

    let microservice_config = optimize_for_workload(WorkloadProfile::Balanced {
        messages_per_second: 100.0,
        avg_message_size_bytes: 8_000,
        workers: 10,
    });

    print_config("Microservice Queue", &microservice_config);

    // Auto-scaling demonstration
    println!("\n=== Auto-Scaling Recommendations ===\n");

    // Case 1: Queue backlog building up
    println!("--- Case 1: Queue Backlog ---");
    println!("Current state:");
    println!("  - Queue size: 10,000 messages");
    println!("  - In-flight: 500 messages");
    println!("  - Workers: 10");
    println!("  - Processing rate: 25 msg/sec per worker");
    println!("  - Target lag: 5 minutes\n");

    let (recommended_workers, action, notes) = auto_scale_recommendation(
        10000, // queue_size
        500,   // in_flight
        10,    // current_workers
        25.0,  // avg_processing_rate
        300,   // target_lag_seconds (5 min)
    );

    println!("Recommendation:");
    println!("  - Recommended workers: {}", recommended_workers);
    println!("  - Action: {:?}", action);
    println!("  - Analysis:");
    for note in &notes {
        println!("    • {}", note);
    }

    // Case 2: Queue nearly empty
    println!("\n--- Case 2: Low Load ---");
    println!("Current state:");
    println!("  - Queue size: 50 messages");
    println!("  - In-flight: 10 messages");
    println!("  - Workers: 20");
    println!("  - Processing rate: 50 msg/sec per worker");
    println!("  - Target lag: 1 minute\n");

    let (recommended_workers2, action2, notes2) = auto_scale_recommendation(
        50,   // queue_size
        10,   // in_flight
        20,   // current_workers
        50.0, // avg_processing_rate
        60,   // target_lag_seconds (1 min)
    );

    println!("Recommendation:");
    println!("  - Recommended workers: {}", recommended_workers2);
    println!("  - Action: {:?}", action2);
    println!("  - Analysis:");
    for note in &notes2 {
        println!("    • {}", note);
    }

    // Queue health analysis
    println!("\n=== Queue Health Analysis ===\n");

    println!("--- Healthy Queue ---");
    let healthy = analyze_queue_health_with_recommendations(
        "orders-queue",
        1000,       // total_messages
        50,         // in_flight
        10,         // delayed
        100.0,      // processing_rate
        Some(30.0), // oldest_message_age_secs
    );

    print_health_assessment(&healthy);

    println!("\n--- Queue with Issues ---");
    let unhealthy = analyze_queue_health_with_recommendations(
        "problematic-queue",
        15000,        // total_messages (large backlog)
        5000,         // in_flight (many stuck)
        200,          // delayed
        5.0,          // processing_rate (very slow)
        Some(2400.0), // oldest_message_age_secs (40 minutes!)
    );

    print_health_assessment(&unhealthy);

    // Cost comparison
    println!("\n=== Cost Comparison ===\n");

    println!("Workload: 1 million messages/day");
    println!();
    println!("Configuration                  | Monthly Cost | Savings");
    println!("-------------------------------|--------------|--------");
    println!(
        "Standard (no optimization)     | ${:>10.2} |   0.0%",
        ecommerce_config.estimated_monthly_cost_usd * 10.0
    );
    println!(
        "Balanced configuration         | ${:>10.2} |  60.0%",
        microservice_config.estimated_monthly_cost_usd
    );
    println!(
        "Cost optimized                 | ${:>10.2} |  85.0%",
        batch_config.estimated_monthly_cost_usd
    );

    println!("\n=== Demo Complete ===");
}

fn print_config(name: &str, config: &OptimizedConfig) {
    println!("Optimized Configuration for {}:", name);
    println!("  Queue Settings:");
    println!("    - Batch size: {} messages", config.batch_size);
    println!(
        "    - Visibility timeout: {}s",
        config.visibility_timeout_secs
    );
    println!("    - Long polling wait time: {}s", config.wait_time_secs);
    println!(
        "    - Message retention: {}s ({:.1} days)",
        config.message_retention_secs,
        config.message_retention_secs as f64 / 86400.0
    );
    println!(
        "    - DLQ max receive count: {}",
        config.dlq_max_receive_count
    );

    println!("  Queue Type:");
    println!(
        "    - {}",
        if config.use_fifo {
            "FIFO Queue"
        } else {
            "Standard Queue"
        }
    );

    println!("  Optimization Features:");
    println!(
        "    - Batching: {}",
        if config.enable_batching {
            "enabled"
        } else {
            "disabled"
        }
    );
    println!(
        "    - Compression: {}",
        if config.enable_compression {
            "enabled"
        } else {
            "disabled"
        }
    );
    if config.enable_compression {
        println!(
            "      (threshold: {} bytes)",
            config.compression_threshold_bytes
        );
    }
    println!("    - Concurrent receives: {}", config.concurrent_receives);

    println!("  Performance Estimates:");
    println!(
        "    - Throughput capacity: {:.0} msg/sec",
        config.estimated_throughput_capacity
    );
    println!(
        "    - Estimated monthly cost: ${:.2}",
        config.estimated_monthly_cost_usd
    );

    println!("  Optimization Notes:");
    for note in &config.notes {
        println!("    • {}", note);
    }
}

fn print_health_assessment(health: &celers_broker_sqs::monitoring::QueueHealthAssessment) {
    println!("Queue: {}", health.queue_name);
    println!("  Metrics:");
    println!("    - Total messages: {}", health.total_messages);
    println!("    - In-flight messages: {}", health.in_flight_messages);
    println!("    - Delayed messages: {}", health.delayed_messages);
    println!(
        "    - Processing rate: {:.1} msg/sec",
        health.processing_rate
    );
    if let Some(age) = health.oldest_message_age_secs {
        println!("    - Oldest message: {:.1} seconds", age);
    }

    println!("  Health Status: {:?}", health.health);

    if !health.issues.is_empty() {
        println!("  Issues:");
        for issue in &health.issues {
            println!("    ⚠ {}", issue);
        }
    }

    if !health.recommendations.is_empty() {
        println!("  Recommendations:");
        for rec in &health.recommendations {
            println!("    → {}", rec);
        }
    }
}
