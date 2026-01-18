//! Simple metrics usage example
//!
//! Demonstrates basic metrics tracking and reporting.
//!
//! Run with: cargo run --example simple_metrics

use celers_metrics::*;

fn main() {
    println!("=== CeleRS Simple Metrics Demo ===\n");

    // Simulate some task activity
    println!("Simulating task processing...");

    //Enqueue tasks
    TASKS_ENQUEUED_TOTAL.inc_by(100.0);
    TASKS_ENQUEUED_BY_TYPE
        .with_label_values(&["send_email"])
        .inc_by(60.0);
    TASKS_ENQUEUED_BY_TYPE
        .with_label_values(&["process_data"])
        .inc_by(40.0);

    // Complete tasks
    TASKS_COMPLETED_TOTAL.inc_by(95.0);
    TASKS_COMPLETED_BY_TYPE
        .with_label_values(&["send_email"])
        .inc_by(58.0);
    TASKS_COMPLETED_BY_TYPE
        .with_label_values(&["process_data"])
        .inc_by(37.0);

    // Some failures
    TASKS_FAILED_TOTAL.inc_by(5.0);
    TASKS_FAILED_BY_TYPE
        .with_label_values(&["send_email"])
        .inc_by(2.0);
    TASKS_FAILED_BY_TYPE
        .with_label_values(&["process_data"])
        .inc_by(3.0);

    // Update queue sizes
    QUEUE_SIZE.set(50.0);
    PROCESSING_QUEUE_SIZE.set(10.0);
    DLQ_SIZE.set(2.0);
    ACTIVE_WORKERS.set(5.0);

    // Record execution times
    TASK_EXECUTION_TIME.observe(0.5);
    TASK_EXECUTION_TIME.observe(1.2);
    TASK_EXECUTION_TIME.observe(0.8);
    TASK_EXECUTION_TIME_BY_TYPE
        .with_label_values(&["send_email"])
        .observe(0.3);
    TASK_EXECUTION_TIME_BY_TYPE
        .with_label_values(&["process_data"])
        .observe(2.5);

    println!("Done!\n");

    // Capture current metrics
    let metrics = CurrentMetrics::capture();

    println!("=== Current Metrics ===");
    println!("Tasks enqueued: {}", metrics.tasks_enqueued);
    println!("Tasks completed: {}", metrics.tasks_completed);
    println!("Tasks failed: {}", metrics.tasks_failed);
    println!("Queue size: {}", metrics.queue_size);
    println!("Processing: {}", metrics.processing_queue_size);
    println!("DLQ size: {}", metrics.dlq_size);
    println!("Active workers: {}\n", metrics.active_workers);

    // Calculate derived metrics
    println!("=== Derived Metrics ===");
    println!("Success rate: {:.2}%", metrics.success_rate() * 100.0);
    println!("Error rate: {:.2}%", metrics.error_rate() * 100.0);
    println!("Total processed: {}\n", metrics.total_processed());

    // Generate summary report
    println!("{}", generate_metric_summary());

    // Export metrics in Prometheus format
    println!("\n=== Prometheus Format (first 500 chars) ===");
    let prom_metrics = gather_metrics();
    let preview: String = prom_metrics.chars().take(500).collect();
    println!("{}", preview);
    println!("...\n(Total {} bytes)", prom_metrics.len());
}
