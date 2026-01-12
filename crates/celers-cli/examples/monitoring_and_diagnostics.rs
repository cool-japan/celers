//! Advanced monitoring and diagnostics example.
//!
//! Demonstrates how to use celers-cli for production monitoring,
//! including metrics collection, health checks, and failure analysis.
//!
//! # Running this example
//!
//! ```bash
//! cargo run --example monitoring_and_diagnostics
//! ```
//!
//! # Prerequisites
//!
//! - Redis server running
//! - Active workers processing tasks

use celers_cli::commands;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("=== CeleRS Monitoring & Diagnostics Example ===\n");

    let broker_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
    let queue_name = std::env::var("QUEUE_NAME").unwrap_or_else(|_| "celers".to_string());

    // 1. Run comprehensive diagnostics
    println!("1. Running automatic problem detection (doctor)...");
    println!(
        "   This will identify configuration issues, connectivity problems, and bottlenecks.\n"
    );
    if let Err(e) = commands::doctor(&broker_url, &queue_name).await {
        eprintln!("Doctor check failed: {e}");
    }

    // 2. Analyze performance bottlenecks
    println!("\n2. Analyzing performance bottlenecks...");
    println!("   Identifying queue depth issues and worker capacity problems.\n");
    if let Err(e) = commands::analyze_bottlenecks(&broker_url, &queue_name).await {
        eprintln!("Bottleneck analysis failed: {e}");
    }

    // 3. Analyze failure patterns
    println!("\n3. Analyzing failure patterns...");
    println!("   Examining DLQ tasks to identify common failure modes.\n");
    if let Err(e) = commands::analyze_failures(&broker_url, &queue_name).await {
        eprintln!("Failure analysis failed: {e}");
    }

    // 4. Generate daily report
    println!("\n4. Generating daily execution report...");
    println!("   Summary of today's task execution metrics.\n");
    if let Err(e) = commands::report_daily(&broker_url, &queue_name).await {
        eprintln!("Daily report failed: {e}");
    }

    // 5. List active workers
    println!("\n5. Listing active workers...");
    if let Err(e) = commands::list_workers(&broker_url).await {
        eprintln!("Failed to list workers: {e}");
    }

    // 6. Queue statistics
    println!("\n6. Detailed queue statistics...");
    if let Err(e) = commands::queue_stats(&broker_url, &queue_name).await {
        eprintln!("Failed to get queue stats: {e}");
    }

    println!("\n=== Monitoring Complete ===");
    println!("\nProduction Monitoring Tips:");
    println!("  • Schedule 'doctor' checks every 5-10 minutes");
    println!("  • Export metrics to Prometheus for historical tracking");
    println!("  • Set up alerts for high DLQ size (> 50 tasks)");
    println!("  • Monitor queue depth and scale workers accordingly");
    println!("  • Review daily/weekly reports for trends");
    println!("\nExport metrics:");
    println!("  celers metrics --format prometheus --output /var/metrics/celers.prom");
    println!("\nSet up alerts:");
    println!("  celers alert start --broker {broker_url} --queue {queue_name}");

    Ok(())
}
