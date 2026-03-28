//! Dead Letter Queue (DLQ) command implementations.

use celers_broker_redis::RedisBroker;
use colored::Colorize;

/// Inspect failed tasks in the Dead Letter Queue (DLQ).
///
/// Displays a list of failed tasks with their IDs, task names, and failure metadata.
/// Useful for debugging and understanding why tasks are failing.
///
/// # Arguments
///
/// * `broker_url` - Redis connection URL
/// * `queue` - Queue name
/// * `limit` - Maximum number of tasks to display (default: 10)
///
/// # Returns
///
/// Returns `Ok(())` on success, or an error if connection fails.
///
/// # Examples
///
/// ```no_run
/// # use celers_cli::commands::inspect_dlq;
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// // Show last 20 failed tasks
/// inspect_dlq("redis://localhost:6379", "my_queue", 20).await?;
/// # Ok(())
/// # }
/// ```
pub async fn inspect_dlq(broker_url: &str, queue: &str, limit: usize) -> anyhow::Result<()> {
    let broker = RedisBroker::new(broker_url, queue)?;

    println!("{}", "=== Dead Letter Queue ===".bold().red());
    println!();

    let dlq_size = broker.dlq_size().await?;
    println!("Total failed tasks: {}", dlq_size.to_string().yellow());

    if dlq_size == 0 {
        println!("{}", "✓ DLQ is empty".green());
        return Ok(());
    }

    println!();
    let tasks = broker.inspect_dlq(limit as isize).await?;

    for (idx, task) in tasks.iter().enumerate() {
        println!("{}", format!("Task #{}", idx + 1).bold());
        println!("  ID: {}", task.metadata.id.to_string().cyan());
        println!("  Name: {}", task.metadata.name.yellow());
        println!("  State: {:?}", task.metadata.state);
        println!("  Max Retries: {}", task.metadata.max_retries);
        println!("  Created: {}", task.metadata.created_at);
        println!();
    }

    println!(
        "Showing {} of {} tasks",
        tasks.len().to_string().yellow(),
        dlq_size
    );

    Ok(())
}

/// Clear Dead Letter Queue
pub async fn clear_dlq(broker_url: &str, queue: &str, confirm: bool) -> anyhow::Result<()> {
    let broker = RedisBroker::new(broker_url, queue)?;

    let dlq_size = broker.dlq_size().await?;

    if dlq_size == 0 {
        println!("{}", "✓ DLQ is already empty".green());
        return Ok(());
    }

    if !confirm {
        println!(
            "{}",
            format!("⚠️  This will delete {dlq_size} tasks from DLQ")
                .yellow()
                .bold()
        );
        println!("   Add --confirm to proceed");
        return Ok(());
    }

    let count = broker.clear_dlq().await?;
    println!("{}", format!("✓ Cleared {count} tasks from DLQ").green());

    Ok(())
}

/// Replay a task from DLQ
pub async fn replay_task(broker_url: &str, queue: &str, task_id_str: &str) -> anyhow::Result<()> {
    let broker = RedisBroker::new(broker_url, queue)?;

    let task_id = task_id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| anyhow::anyhow!("Invalid task ID format"))?;

    println!("{}", "=== Replaying Task from DLQ ===".bold().cyan());
    println!("Task ID: {}", task_id.to_string().yellow());

    let replayed = broker.replay_from_dlq(&task_id).await?;

    if replayed {
        println!("{}", "✓ Task replayed successfully".green().bold());
        println!("  The task will be processed again by workers");
    } else {
        println!("{}", "✗ Task not found in DLQ".red());
    }

    Ok(())
}
