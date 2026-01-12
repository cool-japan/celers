//! Cost Optimization and Monitoring Example
//!
//! Demonstrates the v0.1.4 features for cost estimation, alert thresholds,
//! health reporting, and performance trend analysis.

use celers_broker_redis::monitoring::{
    analyze_performance_trend, estimate_redis_monthly_cost, generate_queue_health_report,
    recommend_alert_thresholds,
};

fn main() {
    println!("=== Cost Optimization and Monitoring Example ===\n");

    // Example 1: Cost Estimation
    println!("1. Cost Estimation");
    println!("-----------------");

    // Small instance
    let small_cost = estimate_redis_monthly_cost(512, 100.0, "aws");
    println!("Small Instance (512MB, 100 ops/sec):");
    println!(
        "  - Total Cost: ${:.2}/month",
        small_cost.total_monthly_cost
    );
    println!(
        "  - Memory Cost: ${:.2}/month",
        small_cost.memory_cost_monthly
    );
    println!(
        "  - Operations Cost: ${:.2}/month",
        small_cost.operations_cost_monthly
    );
    println!("  - Optimization: {}", small_cost.optimization_potential);
    println!();

    // Medium instance
    let medium_cost = estimate_redis_monthly_cost(2048, 5000.0, "aws");
    println!("Medium Instance (2GB, 5000 ops/sec):");
    println!(
        "  - Total Cost: ${:.2}/month",
        medium_cost.total_monthly_cost
    );
    println!("  - Optimization: {}", medium_cost.optimization_potential);
    println!();

    // Large instance
    let large_cost = estimate_redis_monthly_cost(8192, 20000.0, "gcp");
    println!("Large Instance (8GB, 20000 ops/sec, GCP):");
    println!(
        "  - Total Cost: ${:.2}/month",
        large_cost.total_monthly_cost
    );
    println!("  - Provider: {}", large_cost.provider);
    println!();

    // Over-provisioned instance
    let overprovisioned = estimate_redis_monthly_cost(16384, 50.0, "aws");
    println!("Over-provisioned Instance (16GB, 50 ops/sec):");
    println!(
        "  - Total Cost: ${:.2}/month",
        overprovisioned.total_monthly_cost
    );
    println!(
        "  - ⚠️  Optimization: {}",
        overprovisioned.optimization_potential
    );
    println!();

    // Example 2: Alert Threshold Recommendations
    println!("\n2. Alert Threshold Recommendations");
    println!("----------------------------------");

    let thresholds = recommend_alert_thresholds(
        1000, // average queue size
        5000, // peak queue size
        60,   // target SLA (60 seconds)
    );

    println!("Recommended Thresholds for Production:");
    println!("  Queue Size:");
    println!("    - Warning: {} messages", thresholds.queue_size_warning);
    println!(
        "    - Critical: {} messages",
        thresholds.queue_size_critical
    );
    println!("  Processing Lag:");
    println!("    - Warning: {} seconds", thresholds.lag_warning_seconds);
    println!(
        "    - Critical: {} seconds",
        thresholds.lag_critical_seconds
    );
    println!("  DLQ Size:");
    println!("    - Warning: {} messages", thresholds.dlq_size_warning);
    println!("    - Critical: {} messages", thresholds.dlq_size_critical);
    println!("  Error Rate:");
    println!(
        "    - Warning: {:.1}%",
        thresholds.error_rate_warning * 100.0
    );
    println!(
        "    - Critical: {:.1}%",
        thresholds.error_rate_critical * 100.0
    );
    println!("  Memory Usage:");
    println!(
        "    - Warning: {:.0}%",
        thresholds.memory_usage_warning * 100.0
    );
    println!(
        "    - Critical: {:.0}%",
        thresholds.memory_usage_critical * 100.0
    );
    println!();

    // Example 3: Comprehensive Health Report
    println!("\n3. Comprehensive Health Report");
    println!("------------------------------");

    // Healthy system
    let healthy_report = generate_queue_health_report(
        150,  // queue_size
        5,    // processing_queue_size
        2,    // dlq_size
        50.0, // processing_rate (msg/sec)
        3,    // error_count
        1000, // total_processed
        512,  // memory_used_mb
        2048, // memory_max_mb
    );

    println!("Healthy System Report:");
    println!("  Overall Status: {} ✅", healthy_report.overall_status);
    println!("  Queue Health: {}", healthy_report.queue_health);
    println!("  DLQ Health: {}", healthy_report.dlq_health);
    println!("  Error Rate: {:.2}%", healthy_report.error_rate * 100.0);
    println!(
        "  Memory Usage: {:.1}%",
        healthy_report.memory_usage * 100.0
    );
    println!(
        "  Processing Rate: {:.1} msg/sec",
        healthy_report.processing_rate
    );
    if healthy_report.recommendations.is_empty() {
        println!("  Recommendations: None - system is healthy");
    }
    println!();

    // Warning system
    let warning_report = generate_queue_health_report(
        1500, // queue_size (warning level)
        10,   // processing_queue_size
        15,   // dlq_size (warning level)
        50.0, // processing_rate
        50,   // error_count
        1000, // total_processed
        1600, // memory_used_mb (78% usage)
        2048, // memory_max_mb
    );

    println!("System Under Load Report:");
    println!("  Overall Status: {} ⚠️", warning_report.overall_status);
    println!("  Queue Health: {}", warning_report.queue_health);
    println!("  DLQ Health: {}", warning_report.dlq_health);
    println!("  Error Rate: {:.2}%", warning_report.error_rate * 100.0);
    println!(
        "  Memory Usage: {:.1}%",
        warning_report.memory_usage * 100.0
    );
    println!("  Recommendations:");
    for rec in &warning_report.recommendations {
        println!("    - {}", rec);
    }
    println!();

    // Critical system
    let critical_report = generate_queue_health_report(
        12000, // queue_size (critical)
        20,    // processing_queue_size
        150,   // dlq_size (critical)
        10.0,  // processing_rate (low!)
        100,   // error_count
        1000,  // total_processed
        1900,  // memory_used_mb (93% usage)
        2048,  // memory_max_mb
    );

    println!("Critical System Report:");
    println!("  Overall Status: {} 🚨", critical_report.overall_status);
    println!("  Queue Health: {}", critical_report.queue_health);
    println!("  DLQ Health: {}", critical_report.dlq_health);
    println!("  Error Health: {}", critical_report.error_health);
    println!("  Memory Health: {}", critical_report.memory_health);
    println!("  Error Rate: {:.2}%", critical_report.error_rate * 100.0);
    println!(
        "  Memory Usage: {:.1}%",
        critical_report.memory_usage * 100.0
    );
    println!("  🚨 URGENT Recommendations:");
    for rec in &critical_report.recommendations {
        println!("    - {}", rec);
    }
    println!();

    // Example 4: Performance Trend Analysis
    println!("\n4. Performance Trend Analysis");
    println!("----------------------------");

    // Stable system
    let stable_metrics = vec![
        (0.0, 100, 50.0),
        (60.0, 105, 51.0),
        (120.0, 98, 49.0),
        (180.0, 102, 50.5),
        (240.0, 100, 50.0),
    ];

    let stable_trend = analyze_performance_trend(&stable_metrics);
    println!("Stable System Trend:");
    println!("  Data Points: {}", stable_trend.data_points);
    println!("  Time Span: {:.0} seconds", stable_trend.time_span_seconds);
    println!("  Queue Size Trend: {}", stable_trend.queue_size_trend);
    println!(
        "  Processing Rate Trend: {}",
        stable_trend.processing_rate_trend
    );
    println!(
        "  Predicted Queue (1h): {} messages",
        stable_trend.predicted_queue_size_1h
    );
    println!(
        "  Predicted Rate (1h): {:.1} msg/sec",
        stable_trend.predicted_processing_rate_1h
    );
    println!("  Anomalies: {}", stable_trend.anomalies_detected);
    println!();

    // Growing queue, degrading performance
    let degrading_metrics = vec![
        (0.0, 100, 100.0),
        (300.0, 500, 80.0),
        (600.0, 1200, 60.0),
        (900.0, 2000, 40.0),
        (1200.0, 3000, 25.0),
    ];

    let degrading_trend = analyze_performance_trend(&degrading_metrics);
    println!("Degrading System Trend:");
    println!(
        "  Queue Size Trend: {} 📈",
        degrading_trend.queue_size_trend
    );
    println!(
        "  Processing Rate Trend: {} 📉",
        degrading_trend.processing_rate_trend
    );
    println!(
        "  Predicted Queue (1h): {} messages ⚠️",
        degrading_trend.predicted_queue_size_1h
    );
    println!(
        "  Predicted Rate (1h): {:.1} msg/sec",
        degrading_trend.predicted_processing_rate_1h
    );
    println!("  Anomalies: {}", degrading_trend.anomalies_detected);
    println!("  🚨 Critical Recommendations:");
    for rec in &degrading_trend.recommendations {
        println!("    - {}", rec);
    }
    println!();

    // Improving system (queue shrinking, rate improving)
    let improving_metrics = vec![
        (0.0, 5000, 20.0),
        (300.0, 4000, 30.0),
        (600.0, 3000, 40.0),
        (900.0, 2000, 50.0),
        (1200.0, 1000, 60.0),
    ];

    let improving_trend = analyze_performance_trend(&improving_metrics);
    println!("Improving System Trend:");
    println!(
        "  Queue Size Trend: {} 📉",
        improving_trend.queue_size_trend
    );
    println!(
        "  Processing Rate Trend: {} 📈",
        improving_trend.processing_rate_trend
    );
    println!(
        "  Predicted Queue (1h): {} messages",
        improving_trend.predicted_queue_size_1h
    );
    println!(
        "  Predicted Rate (1h): {:.1} msg/sec",
        improving_trend.predicted_processing_rate_1h
    );
    println!("  Status: System recovering ✅");
    println!();

    // Example 5: Combined Analysis for Decision Making
    println!("\n5. Combined Analysis for Decision Making");
    println!("---------------------------------------");

    println!("Current System State:");
    println!(
        "  - Queue: {} messages ({})",
        warning_report.queue_size, warning_report.queue_health
    );
    println!(
        "  - Processing: {:.1} msg/sec",
        warning_report.processing_rate
    );
    println!(
        "  - Memory: {:.1}% ({} MB / {} MB)",
        warning_report.memory_usage * 100.0,
        1600,
        2048
    );
    println!();

    println!("Cost Analysis:");
    println!(
        "  - Current monthly cost: ${:.2}",
        medium_cost.total_monthly_cost
    );
    println!();

    println!("Performance Forecast:");
    println!(
        "  - Queue will grow to ~{} messages in 1 hour",
        degrading_trend.predicted_queue_size_1h
    );
    println!("  - Processing rate declining");
    println!();

    println!("Recommended Actions:");
    println!("  1. Scale up workers immediately (queue growing)");
    println!("  2. Monitor memory usage (approaching 80% threshold)");
    println!(
        "  3. Investigate DLQ - {} failures need attention",
        warning_report.dlq_size
    );
    println!(
        "  4. Review error logs - {:.1}% error rate",
        warning_report.error_rate * 100.0
    );
    println!("  5. Consider upgrading instance if trend continues");
    println!();

    println!("Alert Configuration:");
    println!("  Set alerts at:");
    println!(
        "    - Queue: {} (warning), {} (critical)",
        thresholds.queue_size_warning, thresholds.queue_size_critical
    );
    println!(
        "    - Memory: {:.0}% (warning), {:.0}% (critical)",
        thresholds.memory_usage_warning * 100.0,
        thresholds.memory_usage_critical * 100.0
    );
    println!();

    println!("=== End of Example ===");
}
