//! Basic workflow example demonstrating common celers-cli usage patterns.
//!
//! This example shows how to use the celers CLI programmatically for common
//! operational workflows like starting workers, monitoring queues, and managing tasks.
//!
//! # Running this example
//!
//! ```bash
//! cargo run --example basic_workflow
//! ```
//!
//! # Prerequisites
//!
//! - Redis server running on localhost:6379
//! - Tasks registered in your application

use celers_cli::commands;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("=== CeleRS CLI Basic Workflow Example ===\n");

    let broker_url = "redis://localhost:6379";
    let queue_name = "example_queue";

    // Step 1: Check queue status
    println!("Step 1: Checking queue status...");
    if let Err(e) = commands::show_status(broker_url, queue_name).await {
        eprintln!("Failed to get queue status: {e}");
        eprintln!("Make sure Redis is running on localhost:6379");
        return Ok(());
    }

    // Step 2: List all queues
    println!("\nStep 2: Listing all queues...");
    if let Err(e) = commands::list_queues(broker_url).await {
        eprintln!("Failed to list queues: {e}");
    }

    // Step 3: Show metrics
    println!("\nStep 3: Displaying metrics...");
    if let Err(e) = commands::show_metrics("text", None, None, None).await {
        eprintln!("Failed to get metrics: {e}");
    }

    // Step 4: Check system health
    println!("\nStep 4: Running health check...");
    if let Err(e) = commands::health_check(broker_url, queue_name).await {
        eprintln!("Health check failed: {e}");
    }

    println!("\n=== Workflow Complete ===");
    println!("\nNext steps:");
    println!("  • Start a worker: celers worker --broker {broker_url} --queue {queue_name}");
    println!("  • Inspect DLQ: celers dlq inspect --broker {broker_url} --queue {queue_name}");
    println!("  • Run diagnostics: celers doctor --broker {broker_url} --queue {queue_name}");

    Ok(())
}
