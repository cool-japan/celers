//! Queue management operations example.
//!
//! Demonstrates how to manage queues programmatically, including:
//! - Moving tasks between queues
//! - Exporting and importing queues
//! - Pausing and resuming queue processing
//! - Purging queues
//!
//! # Running this example
//!
//! ```bash
//! cargo run --example queue_management
//! ```
//!
//! # Prerequisites
//!
//! - Redis server running
//! - Test queues with some tasks

use celers_cli::commands;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("=== CeleRS Queue Management Example ===\n");

    let broker_url = "redis://localhost:6379";
    let source_queue = "source_queue";
    let dest_queue = "dest_queue";
    let export_file = "/tmp/queue_backup.json";

    println!("This example demonstrates various queue management operations.");
    println!("Note: Some operations require confirmation flags to prevent accidental data loss.\n");

    // 1. List all queues
    println!("1. Listing all available queues...");
    if let Err(e) = commands::list_queues(broker_url).await {
        eprintln!("Failed to list queues: {e}");
        return Ok(());
    }

    // 2. Show detailed queue statistics
    println!("\n2. Getting detailed statistics for queue '{source_queue}'...");
    if let Err(e) = commands::queue_stats(broker_url, source_queue).await {
        eprintln!("Failed to get queue stats: {e}");
    }

    // 3. Pause queue processing
    println!("\n3. Pausing queue processing for '{source_queue}'...");
    println!("   Workers will stop consuming tasks from this queue.");
    if let Err(e) = commands::pause_queue(broker_url, source_queue).await {
        eprintln!("Failed to pause queue: {e}");
    }

    // 4. Resume queue processing
    println!("\n4. Resuming queue processing for '{source_queue}'...");
    if let Err(e) = commands::resume_queue(broker_url, source_queue).await {
        eprintln!("Failed to resume queue: {e}");
    }

    // 5. Export queue to file
    println!("\n5. Exporting queue '{source_queue}' to {export_file}...");
    println!("   This creates a backup of all tasks in JSON format.");
    if let Err(e) = commands::export_queue(broker_url, source_queue, export_file).await {
        eprintln!("Failed to export queue: {e}");
    }

    // 6. Import queue from file (requires confirmation in CLI)
    println!("\n6. Import example:");
    println!("   To import the backup:");
    println!("   celers queue import --input {export_file} --queue {dest_queue} --confirm");

    // 7. Move tasks between queues (requires confirmation in CLI)
    println!("\n7. Move tasks example:");
    println!("   To move all tasks from one queue to another:");
    println!("   celers queue move --from {source_queue} --to {dest_queue} --confirm");

    // 8. Purge queue (requires confirmation in CLI)
    println!("\n8. Purge queue example:");
    println!("   To remove all tasks from a queue:");
    println!("   celers queue purge --queue {source_queue} --confirm");

    println!("\n=== Queue Management Tips ===");
    println!("• Always export queues before destructive operations");
    println!("• Use queue pause/resume for maintenance windows");
    println!("• Move tasks to specialized queues for priority processing");
    println!("• Regularly backup critical queues");
    println!("• Monitor queue depth to prevent backlogs");

    println!("\n=== Common Workflows ===");
    println!("Backup before maintenance:");
    println!(
        "  1. celers queue export --queue production --output /backup/prod_$(date +%Y%m%d).json"
    );
    println!("  2. celers queue pause --queue production");
    println!("  3. <perform maintenance>");
    println!("  4. celers queue resume --queue production");
    println!("\nMigrate to priority queue:");
    println!("  celers queue move --from default --to high_priority --confirm");
    println!("\nRestore from backup:");
    println!(
        "  celers queue import --input /backup/prod_20251230.json --queue production --confirm"
    );

    Ok(())
}
