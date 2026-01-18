//! Advanced PostgreSQL Broker Utilities Example
//!
//! This example demonstrates the latest advanced utility functions including:
//! - Task result compression analysis
//! - Query performance regression detection
//! - Task execution metrics tracking
//! - DLQ retry policies with intelligent backoff
//!
//! Run with:
//! ```bash
//! cargo run --example advanced_utilities
//! ```

use celers_broker_postgres::utilities::*;

fn main() {
    println!("=== Advanced PostgreSQL Broker Utilities Demo ===\n");

    // 1. Compression Analysis
    println!("1. Task Result Compression Analysis");
    println!("====================================");

    // Analyze compression for large JSON payload
    let json_rec = calculate_compression_recommendation(50_000, "json", 1000);
    println!("Large JSON payload (50KB, 1000 tasks/hour):");
    println!("  Should compress: {}", json_rec.should_compress);
    println!(
        "  Estimated ratio: {:.1}%",
        json_rec.estimated_ratio * 100.0
    );
    println!(
        "  Recommended algorithm: {}",
        json_rec.recommended_algorithm
    );
    println!("  Reason: {}", json_rec.reason);
    println!();

    // Analyze compression for binary data
    let binary_rec = calculate_compression_recommendation(50_000, "binary", 1000);
    println!("Binary payload (50KB, 1000 tasks/hour):");
    println!("  Should compress: {}", binary_rec.should_compress);
    println!("  Reason: {}", binary_rec.reason);
    println!();

    // 2. Query Performance Regression Detection
    println!("2. Query Performance Regression Detection");
    println!("=========================================");

    // Simulate query performance tracking
    let queries = vec![
        ("dequeue", 50.0, 45.0),  // Slight improvement
        ("enqueue", 120.0, 80.0), // Moderate regression
        ("ack", 300.0, 100.0),    // Severe regression
    ];

    for (query_name, current_ms, baseline_ms) in queries {
        let analysis = detect_query_performance_regression(
            query_name,
            current_ms,
            baseline_ms,
            20.0, // 20% threshold
        );

        println!("Query: {}", query_name);
        println!(
            "  Current: {:.1}ms, Baseline: {:.1}ms",
            analysis.current_time_ms, analysis.baseline_time_ms
        );
        println!("  Regression: {:.1}%", analysis.regression_percent);
        println!("  Severity: {}", analysis.severity);

        if analysis.is_regression {
            println!("  Recommendations:");
            for rec in &analysis.recommendations {
                println!("    - {}", rec);
            }
        }
        println!();
    }

    // 3. Task Execution Metrics
    println!("3. Task Execution Metrics Tracking");
    println!("==================================");

    // Simulate different task types with varying resource usage
    let task_types = vec![
        ("lightweight_task", 500.0, 5_000_000, 100), // 5ms avg, 50KB
        ("normal_task", 5_000.0, 50_000_000, 50),    // 100ms avg, 1MB
        ("heavy_task", 100_000.0, 500_000_000, 20),  // 5s avg, 25MB
    ];

    for (task_name, total_cpu_ms, total_mem, executions) in task_types {
        let metrics =
            calculate_task_execution_metrics(task_name, total_cpu_ms, total_mem, executions);

        println!("Task: {}", task_name);
        println!("  Avg CPU time: {:.1}ms", metrics.avg_cpu_time_ms);
        println!(
            "  Avg memory: {:.2}MB",
            metrics.avg_memory_bytes as f64 / 1_000_000.0
        );
        println!("  Total executions: {}", metrics.total_executions);
        println!("  Resource intensive: {}", metrics.is_resource_intensive);

        if !metrics.suggestions.is_empty() {
            println!("  Suggestions:");
            for suggestion in &metrics.suggestions {
                println!("    - {}", suggestion);
            }
        }
        println!();
    }

    // 4. DLQ Retry Policies
    println!("4. DLQ Automatic Retry Policies");
    println!("================================");

    // Test default policy
    let default_policy = DlqRetryPolicy::default();
    println!("Default DLQ Policy:");
    println!("  Max retries: {}", default_policy.max_retries);
    println!("  Initial delay: {}s", default_policy.initial_delay_secs);
    println!("  Max delay: {}s", default_policy.max_delay_secs);
    println!("  Use jitter: {}", default_policy.use_jitter);
    println!();

    // Calculate retry delays
    println!("Retry delay progression:");
    for retry in 0..4 {
        let delay = calculate_dlq_retry_delay(retry, &default_policy);
        if delay > 0 {
            println!(
                "  Retry {}: {}s ({:.1} minutes)",
                retry,
                delay,
                delay as f64 / 60.0
            );
        } else {
            println!("  Retry {}: No more retries", retry);
        }
    }
    println!();

    // Get intelligent policy suggestions for different task types
    let task_scenarios = vec![
        ("api_request", 200.0, 0.05),      // Fast, reliable
        ("data_processing", 3000.0, 0.15), // Moderate, some failures
        ("external_api", 10000.0, 0.4),    // Slow, unreliable
    ];

    println!("Intelligent Policy Suggestions:");
    for (task_type, avg_time_ms, failure_rate) in task_scenarios {
        let policy = suggest_dlq_retry_policy(task_type, avg_time_ms, failure_rate);
        println!(
            "  {} (avg: {:.0}ms, failure rate: {:.1}%):",
            task_type,
            avg_time_ms,
            failure_rate * 100.0
        );
        println!("    Max retries: {}", policy.max_retries);
        println!("    Initial delay: {}s", policy.initial_delay_secs);
        println!(
            "    Max delay: {}s ({} hours)",
            policy.max_delay_secs,
            policy.max_delay_secs / 3600
        );
        println!();
    }

    // 5. Real-world Workflow Example
    println!("5. Real-World Workflow Example");
    println!("==============================");
    println!("Scenario: High-volume image processing queue");
    println!();

    // Analyze compression for image metadata
    let metadata_compression = calculate_compression_recommendation(
        2048, // 2KB metadata per image
        "json", 10_000, // 10k images/hour
    );
    println!("Image metadata compression:");
    println!("  {}", metadata_compression.reason);
    println!();

    // Check for query regression in processing pipeline
    let dequeue_regression = detect_query_performance_regression(
        "image_dequeue",
        75.0, // Current: 75ms
        50.0, // Baseline: 50ms
        20.0, // 20% threshold
    );
    if dequeue_regression.is_regression {
        println!(
            "⚠️  Dequeue performance degraded by {:.1}%",
            dequeue_regression.regression_percent
        );
        println!(
            "   Action needed: {}",
            dequeue_regression.recommendations[0]
        );
    }
    println!();

    // Analyze resource usage for image processing
    let processing_metrics = calculate_task_execution_metrics(
        "image_resize",
        120_000.0,     // 120s total CPU
        2_000_000_000, // 2GB total memory
        1000,          // 1000 images processed
    );
    println!("Image processing performance:");
    println!(
        "  Avg processing time: {:.1}ms per image",
        processing_metrics.avg_cpu_time_ms
    );
    println!(
        "  Avg memory: {:.1}MB per image",
        processing_metrics.avg_memory_bytes as f64 / 1_000_000.0
    );

    if processing_metrics.is_resource_intensive {
        println!("  ⚠️  Resource intensive - consider optimization");
    }
    println!();

    // Configure DLQ retry for failed image processing
    let image_policy = suggest_dlq_retry_policy("image_resize", 120.0, 0.02);
    println!("DLQ retry policy for failed images:");
    println!(
        "  Retry up to {} times with {}-{}s delays",
        image_policy.max_retries, image_policy.initial_delay_secs, image_policy.max_delay_secs
    );

    println!("\n=== Demo Complete ===");
}
