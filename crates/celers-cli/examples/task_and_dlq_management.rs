//! Task and Dead Letter Queue (DLQ) management example.
//!
//! Demonstrates how to manage individual tasks and handle failed tasks in the DLQ:
//! - Inspecting task details
//! - Canceling running tasks
//! - Retrying failed tasks
//! - Managing DLQ tasks
//! - Task logs and debugging
//!
//! # Running this example
//!
//! ```bash
//! cargo run --example task_and_dlq_management
//! ```
//!
//! # Prerequisites
//!
//! - Redis server running
//! - Some tasks in the queue or DLQ

use celers_cli::commands;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("=== CeleRS Task & DLQ Management Example ===\n");

    let broker_url = "redis://localhost:6379";
    let queue_name = "celers";
    let task_id = "550e8400-e29b-41d4-a716-446655440000"; // Example UUID

    println!("This example demonstrates task and DLQ management operations.");
    println!("Note: Task IDs must be valid UUIDs.\n");

    // === TASK MANAGEMENT ===
    println!("=== Task Management ===\n");

    // 1. Inspect task details
    println!("1. Inspecting task details...");
    println!("   celers task inspect {task_id}");
    println!("   Shows task metadata, status, arguments, and location (queue/DLQ/delayed).");
    if let Err(e) = commands::inspect_task(broker_url, queue_name, task_id).await {
        eprintln!("   Task not found: {e}");
    }

    // 2. Cancel running task
    println!("\n2. Cancel running task:");
    println!("   celers task cancel {task_id}");
    println!("   Sends cancellation signal to worker processing this task.");

    // 3. Retry failed task
    println!("\n3. Retry failed task:");
    println!("   celers task retry {task_id}");
    println!("   Moves task from DLQ back to main queue for reprocessing.");

    // 4. View task logs
    println!("\n4. View task execution logs:");
    println!("   celers task logs {task_id}");
    println!("   Shows last 50 log entries with color-coded levels (ERROR/WARN/INFO/DEBUG).");
    println!("   Limit output:");
    println!("   celers task logs {task_id} --limit 100");

    // 5. Show task result
    println!("\n5. Show task result:");
    println!("   celers task result {task_id} --backend redis://localhost:6379");
    println!("   Retrieves task execution result from backend storage.");

    // 6. Move task to different queue
    println!("\n6. Move task to different queue:");
    println!("   celers task requeue {task_id} --from low_priority --to high_priority");
    println!("   Useful for reprioritizing tasks.");

    // 7. Debug task
    println!("\n7. Debug task execution:");
    println!("   celers debug task {task_id}");
    println!("   Shows comprehensive debugging info: logs, metadata, queue state.");

    // === DLQ MANAGEMENT ===
    println!("\n=== Dead Letter Queue (DLQ) Management ===\n");

    // 1. Inspect DLQ
    println!("1. Inspecting failed tasks in DLQ...");
    if let Err(e) = commands::inspect_dlq(broker_url, queue_name, 10).await {
        eprintln!("Failed to inspect DLQ: {e}");
    }
    println!("   Shows up to 10 failed tasks with error messages.");
    println!("   Adjust limit:");
    println!("   celers dlq inspect --limit 50");

    // 2. Replay specific task from DLQ
    println!("\n2. Replay specific task from DLQ:");
    println!("   celers dlq replay {task_id}");
    println!("   Retries a single failed task from DLQ.");

    // 3. Clear DLQ (requires confirmation)
    println!("\n3. Clear all DLQ tasks:");
    println!("   celers dlq clear --confirm");
    println!("   ⚠️  WARNING: This permanently removes all failed tasks!");
    println!("   Consider exporting DLQ first:");
    println!("   celers queue export --queue celers --output /tmp/dlq_backup.json");

    // === COMMON WORKFLOWS ===
    println!("\n=== Common Task Management Workflows ===\n");

    println!("Handle High DLQ Size:");
    println!("  1. celers dlq inspect --limit 50");
    println!("  2. celers analyze failures  # Identify patterns");
    println!("  3. Fix underlying issues in task code");
    println!("  4. Replay failed tasks:");
    println!("     for task_id in $(celers dlq inspect | grep UUID); do");
    println!("       celers dlq replay $task_id");
    println!("     done");

    println!("\nDebug Failing Task:");
    println!("  1. celers task logs {task_id}  # Check execution logs");
    println!("  2. celers task inspect {task_id}  # Review task parameters");
    println!("  3. celers debug task {task_id}  # Full diagnostic");
    println!("  4. Fix code and retry:");
    println!("     celers task retry {task_id}");

    println!("\nCancel Runaway Tasks:");
    println!("  1. celers queue stats --queue my_queue  # Find stuck tasks");
    println!("  2. celers task inspect <task_id>  # Verify it's stuck");
    println!("  3. celers task cancel <task_id>  # Cancel execution");

    println!("\nReprioritize Tasks:");
    println!("  1. Export low priority queue:");
    println!("     celers queue export --queue low_priority --output /tmp/tasks.json");
    println!("  2. Import to high priority queue:");
    println!("     celers queue import --queue high_priority --input /tmp/tasks.json --confirm");
    println!("  3. Clear original queue:");
    println!("     celers queue purge --queue low_priority --confirm");

    println!("\n=== Best Practices ===");
    println!("• Always check DLQ size in health checks");
    println!("• Set up alerts for DLQ threshold (e.g., > 50 tasks)");
    println!("• Export DLQ before clearing for audit trail");
    println!("• Analyze failure patterns to prevent recurrence");
    println!("• Use task logs for debugging (enable logging in task code)");
    println!("• Retry transient failures, fix code for persistent failures");

    println!("\n=== Monitoring Commands ===");
    println!("Monitor DLQ size:");
    println!("  watch -n 10 'celers status | grep DLQ'");
    println!("\nSet up automated alerts:");
    println!("  celers alert start  # Configure in celers.toml");
    println!("\nGenerate failure report:");
    println!("  celers analyze failures");

    Ok(())
}
