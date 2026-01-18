//! Basic Prometheus metrics endpoint example
//!
//! This example demonstrates how to expose CeleRS metrics via an HTTP endpoint
//! that Prometheus can scrape.
//!
//! Run with: cargo run --example basic_prometheus_endpoint
//! Then visit http://localhost:9090/metrics

use celers_metrics::*;
use std::io::Write;
use std::net::TcpListener;

fn main() -> std::io::Result<()> {
    println!("Starting metrics endpoint on http://localhost:9090/metrics");

    // Simulate some task activity
    simulate_task_activity();

    // Start HTTP server
    let listener = TcpListener::bind("127.0.0.1:9090")?;

    for stream in listener.incoming() {
        let mut stream = stream?;

        // Read the HTTP request (we don't parse it, just consume it)
        let mut buffer = [0u8; 1024];
        use std::io::Read;
        let _ = stream.read(&mut buffer)?;

        // Gather metrics
        let metrics = gather_metrics();

        // Send HTTP response
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\n\r\n{}",
            metrics.len(),
            metrics
        );

        stream.write_all(response.as_bytes())?;
        stream.flush()?;

        println!("Served metrics to client");
    }

    Ok(())
}

fn simulate_task_activity() {
    // Simulate some tasks being processed
    TASKS_ENQUEUED_TOTAL.inc_by(100.0);
    TASKS_COMPLETED_TOTAL.inc_by(95.0);
    TASKS_FAILED_TOTAL.inc_by(5.0);
    TASKS_RETRIED_TOTAL.inc_by(3.0);

    QUEUE_SIZE.set(25.0);
    PROCESSING_QUEUE_SIZE.set(5.0);
    ACTIVE_WORKERS.set(3.0);

    // Simulate some execution times
    TASK_EXECUTION_TIME.observe(0.5);
    TASK_EXECUTION_TIME.observe(1.2);
    TASK_EXECUTION_TIME.observe(0.8);

    // Per-task-type metrics
    TASKS_ENQUEUED_BY_TYPE
        .with_label_values(&["send_email"])
        .inc_by(50.0);
    TASKS_ENQUEUED_BY_TYPE
        .with_label_values(&["process_data"])
        .inc_by(30.0);
    TASKS_COMPLETED_BY_TYPE
        .with_label_values(&["send_email"])
        .inc_by(48.0);
    TASKS_COMPLETED_BY_TYPE
        .with_label_values(&["process_data"])
        .inc_by(28.0);

    println!("Simulated task activity");
}
