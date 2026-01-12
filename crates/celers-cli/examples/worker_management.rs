//! Worker management operations example.
//!
//! Demonstrates how to manage workers programmatically, including:
//! - Listing active workers
//! - Getting worker statistics
//! - Pausing and resuming workers
//! - Scaling worker count
//! - Draining workers for graceful shutdown
//!
//! # Running this example
//!
//! ```bash
//! cargo run --example worker_management
//! ```
//!
//! # Prerequisites
//!
//! - Redis server running
//! - At least one active worker

use celers_cli::commands;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("=== CeleRS Worker Management Example ===\n");

    let broker_url = "redis://localhost:6379";

    println!("This example demonstrates various worker management operations.");
    println!("Note: Workers must be running for most operations to work.\n");

    // 1. List all active workers
    println!("1. Listing all active workers...");
    if let Err(e) = commands::list_workers(broker_url).await {
        eprintln!("Failed to list workers: {e}");
        eprintln!("Make sure at least one worker is running:");
        eprintln!("  celers worker --broker {broker_url} --queue my_queue\n");
        return Ok(());
    }

    // Get first worker ID for demonstration
    let worker_id = "worker-example-123";

    // 2. Show worker statistics
    println!("\n2. Getting statistics for worker '{worker_id}'...");
    println!("   This shows tasks processed, failures, uptime, and heartbeat status.");
    if let Err(e) = commands::worker_stats(broker_url, worker_id).await {
        eprintln!("Failed to get worker stats: {e}");
        eprintln!("Worker ID may not exist. Use 'celers worker-mgmt list' to see active workers.");
    }

    // 3. Pause worker
    println!("\n3. Pause worker example:");
    println!("   To temporarily stop a worker from processing new tasks:");
    println!("   celers worker-mgmt pause {worker_id}");
    println!("   The worker remains active but won't fetch new tasks.");

    // 4. Resume worker
    println!("\n4. Resume worker example:");
    println!("   To resume a paused worker:");
    println!("   celers worker-mgmt resume {worker_id}");

    // 5. Drain worker
    println!("\n5. Drain worker example:");
    println!("   To gracefully shutdown a worker (finish current tasks, no new ones):");
    println!("   celers worker-mgmt drain {worker_id}");
    println!("   Useful before maintenance or deployment.");

    // 6. Stop worker
    println!("\n6. Stop worker example:");
    println!("   Graceful stop (wait for current tasks to finish):");
    println!("   celers worker-mgmt stop {worker_id} --graceful");
    println!("   Immediate stop:");
    println!("   celers worker-mgmt stop {worker_id}");

    // 7. Scale workers
    println!("\n7. Scale workers example:");
    println!("   To scale to 5 workers:");
    println!("   celers worker-mgmt scale 5");
    println!(
        "   Note: This provides instructions, actual scaling requires starting/stopping processes."
    );

    // 8. View worker logs
    println!("\n8. View worker logs example:");
    println!("   Stream worker logs in real-time:");
    println!("   celers worker-mgmt logs {worker_id} --follow");
    println!("   Filter by log level:");
    println!("   celers worker-mgmt logs {worker_id} --level error");

    // 9. Debug worker
    println!("\n9. Debug worker example:");
    println!("   Run comprehensive diagnostics on a worker:");
    println!("   celers debug worker {worker_id}");
    println!("   Shows heartbeat, stats, pause/drain status, and recent logs.");

    println!("\n=== Worker Management Patterns ===");
    println!("\nRolling Deployment:");
    println!("  1. celers worker-mgmt drain worker-1  # Stop accepting new tasks");
    println!("  2. Wait for current tasks to complete");
    println!("  3. celers worker-mgmt stop worker-1 --graceful");
    println!("  4. Deploy new version and start worker-1");
    println!("  5. Repeat for other workers");

    println!("\nLoad-based Scaling:");
    println!("  1. celers queue stats --queue my_queue  # Check queue depth");
    println!("  2. If queue_size > 1000: scale up");
    println!("  3. celers worker-mgmt scale 10");
    println!("  4. If queue_size < 100: scale down");
    println!("  5. celers worker-mgmt scale 3");

    println!("\nAuto-scaling:");
    println!("  celers autoscale start  # Start auto-scaling service");
    println!("  Configure min_workers, max_workers, and thresholds in celers.toml");

    println!("\nMaintenance Window:");
    println!("  1. celers queue pause --queue my_queue  # Stop processing");
    println!("  2. celers worker-mgmt list  # Verify all idle");
    println!("  3. <perform maintenance>");
    println!("  4. celers queue resume --queue my_queue  # Resume processing");

    println!("\n=== Monitoring Tips ===");
    println!("• Monitor worker heartbeats to detect crashes");
    println!("• Track worker statistics for performance analysis");
    println!("• Use drain before shutdown to prevent task loss");
    println!("• Keep worker logs for debugging failures");
    println!("• Set up alerts for worker health issues");

    Ok(())
}
