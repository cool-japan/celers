//! Task Producer Example for CeleRS MySQL Broker
//!
//! This example demonstrates how to enqueue tasks to the MySQL broker.
//! It shows single task enqueue, batch enqueue, and scheduled task enqueue.
//!
//! Run with:
//! ```bash
//! export DATABASE_URL="mysql://root:password@localhost/celers_dev"
//! cargo run --example task_producer
//! ```

use celers_broker_sql::{DbTaskState, MysqlBroker};
use celers_core::{Broker, SerializedTask};
use serde::{Deserialize, Serialize};
use tracing::info;

/// Example email task
#[derive(Debug, Serialize, Deserialize)]
struct EmailTask {
    to: String,
    subject: String,
    body: String,
}

/// Example processing task
#[derive(Debug, Serialize, Deserialize)]
struct ProcessingTask {
    id: u64,
    data: Vec<u8>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_target(false)
        .with_level(true)
        .init();

    info!("Starting Task Producer Example");

    // Connect to MySQL
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://root:password@localhost/celers_dev".to_string());

    let broker = MysqlBroker::new(&database_url).await?;
    info!("Connected to MySQL broker");

    // Run migrations
    broker.migrate().await?;
    info!("Migrations complete");

    // Example 1: Enqueue a single email task
    info!("\n=== Example 1: Single Task Enqueue ===");
    let email_task = EmailTask {
        to: "user@example.com".to_string(),
        subject: "Welcome!".to_string(),
        body: "Welcome to our service!".to_string(),
    };

    let payload = serde_json::to_vec(&email_task)?;
    let task = SerializedTask::new("send_email".to_string(), payload);
    let task_id = broker.enqueue(task).await?;
    info!("Enqueued email task: {}", task_id);

    // Example 2: Enqueue tasks in batch
    info!("\n=== Example 2: Batch Task Enqueue ===");
    let mut batch_tasks = Vec::new();

    for i in 0..10 {
        let processing_task = ProcessingTask {
            id: i,
            data: vec![0u8; 1024], // 1KB of data
        };
        let payload = serde_json::to_vec(&processing_task)?;
        let task = SerializedTask::new("process_data".to_string(), payload);
        batch_tasks.push(task);
    }

    let task_ids = broker.enqueue_batch(batch_tasks).await?;
    info!("Enqueued {} processing tasks in batch", task_ids.len());

    // Example 3: Schedule tasks for future execution
    info!("\n=== Example 3: Scheduled Task Enqueue ===");

    // Schedule a task to run in 60 seconds
    let email_task = EmailTask {
        to: "delayed@example.com".to_string(),
        subject: "Delayed Message".to_string(),
        body: "This message was scheduled for delivery".to_string(),
    };
    let payload = serde_json::to_vec(&email_task)?;
    let task = SerializedTask::new("send_email".to_string(), payload);
    let task_id = broker.enqueue_after(task, 60).await?;
    info!("Scheduled task for 60 seconds from now: {}", task_id);

    // Example 4: Priority tasks
    info!("\n=== Example 4: Priority Tasks ===");
    for priority in [1, 5, 10] {
        let email_task = EmailTask {
            to: format!("priority{}@example.com", priority),
            subject: format!("Priority {} Message", priority),
            body: format!("This is a priority {} task", priority),
        };
        let payload = serde_json::to_vec(&email_task)?;
        let task = SerializedTask::new("send_email".to_string(), payload).with_priority(priority);
        let task_id = broker.enqueue(task).await?;
        info!("Enqueued priority {} task: {}", priority, task_id);
    }

    // Example 5: Task with retry configuration
    info!("\n=== Example 5: Task with Custom Retry ===");
    let email_task = EmailTask {
        to: "retry@example.com".to_string(),
        subject: "Retryable Task".to_string(),
        body: "This task will retry up to 5 times on failure".to_string(),
    };
    let payload = serde_json::to_vec(&email_task)?;
    let task = SerializedTask::new("send_email".to_string(), payload).with_max_retries(5);
    let task_id = broker.enqueue(task).await?;
    info!("Enqueued task with max 5 retries: {}", task_id);

    // Show queue statistics
    info!("\n=== Queue Statistics ===");
    let stats = broker.get_statistics().await?;
    info!("Pending tasks: {}", stats.pending);
    info!("Processing tasks: {}", stats.processing);
    info!("Completed tasks: {}", stats.completed);
    info!("Failed tasks: {}", stats.failed);
    info!("DLQ tasks: {}", stats.dlq);

    // Show tasks by name
    info!("\n=== Tasks by Name ===");
    let task_counts = broker.count_by_task_name().await?;
    for count in task_counts {
        info!(
            "{}: total={}, pending={}, processing={}, completed={}, failed={}",
            count.task_name,
            count.total,
            count.pending,
            count.processing,
            count.completed,
            count.failed
        );
    }

    // List pending tasks
    info!("\n=== Pending Tasks ===");
    let pending_tasks = broker.list_tasks(Some(DbTaskState::Pending), 10, 0).await?;
    for task in &pending_tasks {
        info!(
            "Task {} ({}): priority={}, retries={}/{}",
            task.id, task.task_name, task.priority, task.retry_count, task.max_retries
        );
    }

    // Show scheduled tasks
    info!("\n=== Scheduled Tasks ===");
    let scheduled = broker.list_scheduled_tasks(10, 0).await?;
    for task in &scheduled {
        info!(
            "Task {} ({}): scheduled for {}",
            task.id, task.task_name, task.scheduled_at
        );
    }

    info!("\n=== Task Producer Complete ===");
    info!(
        "Total tasks enqueued: {} pending in queue",
        stats.pending + pending_tasks.len() as i64
    );
    info!("Start the worker_pool example to process these tasks:");
    info!("  cargo run --example worker_pool");

    Ok(())
}
