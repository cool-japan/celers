//! Basic Usage Example for PostgreSQL Broker
//!
//! This example demonstrates the fundamental operations of the PostgreSQL broker:
//! - Creating and configuring a broker
//! - Enqueuing tasks
//! - Dequeuing and processing tasks
//! - Batch operations
//! - Delayed execution
//! - Queue management
//!
//! Run with: cargo run --example basic_usage
//!
//! Note: This example requires a PostgreSQL database. Set the DATABASE_URL
//! environment variable or modify the connection string below.

use celers_broker_postgres::PostgresBroker;
use celers_core::{Broker, SerializedTask};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== PostgreSQL Broker Basic Usage Example ===\n");

    // Get database URL from environment or use default
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://localhost/celers_example".to_string());

    println!("Connecting to PostgreSQL...");
    println!("Database: {}\n", database_url);

    // Create the broker
    let broker = match PostgresBroker::new(&database_url).await {
        Ok(b) => {
            println!("✓ Connected successfully\n");
            b
        }
        Err(e) => {
            eprintln!("❌ Failed to connect to PostgreSQL: {}", e);
            eprintln!("\nPlease ensure:");
            eprintln!("1. PostgreSQL is running");
            eprintln!("2. Database exists: celers_example");
            eprintln!("3. Or set DATABASE_URL environment variable");
            eprintln!("\nExample:");
            eprintln!("  createdb celers_example");
            eprintln!("  export DATABASE_URL=postgres://user:pass@localhost/celers_example");
            std::process::exit(1);
        }
    };

    // Run migrations to set up tables
    println!("Running migrations...");
    broker.migrate().await?;
    println!("✓ Migrations complete\n");

    // Example 1: Basic Enqueue and Dequeue
    println!("1. Basic Enqueue and Dequeue");
    println!("-----------------------------");

    let task1 = SerializedTask::new("process_order".to_string(), vec![1, 2, 3, 4]);
    let task_id = broker.enqueue(task1).await?;
    println!("✓ Enqueued task with ID: {}", task_id);

    // Dequeue the task
    if let Some(message) = broker.dequeue().await? {
        println!("✓ Dequeued task: {}", message.task.metadata.name);
        println!("  Task ID: {}", message.task.metadata.id);
        println!("  Payload size: {} bytes", message.task.payload.len());

        // Acknowledge successful processing
        broker
            .ack(&message.task.metadata.id, message.receipt_handle.as_deref())
            .await?;
        println!("✓ Task acknowledged\n");
    }

    // Example 2: Batch Operations
    println!("2. Batch Operations");
    println!("--------------------");

    let tasks = vec![
        SerializedTask::new("send_email".to_string(), b"user1@example.com".to_vec()),
        SerializedTask::new("send_email".to_string(), b"user2@example.com".to_vec()),
        SerializedTask::new("send_email".to_string(), b"user3@example.com".to_vec()),
        SerializedTask::new("send_sms".to_string(), b"+1234567890".to_vec()),
        SerializedTask::new("send_sms".to_string(), b"+9876543210".to_vec()),
    ];

    println!("Enqueuing {} tasks in batch...", tasks.len());
    let task_ids = broker.enqueue_batch(tasks).await?;
    println!("✓ Enqueued {} tasks", task_ids.len());

    // Batch dequeue
    println!("Dequeuing up to 3 tasks...");
    let messages = broker.dequeue_batch(3).await?;
    println!("✓ Dequeued {} tasks", messages.len());

    for msg in &messages {
        println!(
            "  - Task: {}, ID: {}",
            msg.task.metadata.name, msg.task.metadata.id
        );
    }

    // Batch acknowledge
    let acks: Vec<_> = messages
        .iter()
        .map(|m| (m.task.metadata.id, m.receipt_handle.clone()))
        .collect();
    broker.ack_batch(&acks).await?;
    println!("✓ Acknowledged {} tasks\n", acks.len());

    // Example 3: Priority Tasks
    println!("3. Priority Tasks");
    println!("------------------");

    let low_priority = SerializedTask::new("backup".to_string(), vec![]).with_priority(10);
    let high_priority =
        SerializedTask::new("critical_alert".to_string(), vec![]).with_priority(200);
    let medium_priority =
        SerializedTask::new("report_generation".to_string(), vec![]).with_priority(100);

    broker.enqueue(low_priority).await?;
    broker.enqueue(high_priority).await?;
    broker.enqueue(medium_priority).await?;
    println!("✓ Enqueued tasks with different priorities");

    // Dequeue will return highest priority first
    if let Some(msg) = broker.dequeue().await? {
        println!(
            "✓ Dequeued highest priority: {} (priority: {})",
            msg.task.metadata.name, msg.task.metadata.priority
        );
        broker
            .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
            .await?;
    }
    println!();

    // Example 4: Delayed Execution
    println!("4. Delayed Execution");
    println!("---------------------");

    // Schedule for 5 seconds from now
    let delayed_task = SerializedTask::new("scheduled_cleanup".to_string(), vec![]);
    let delay_secs = 5;

    broker.enqueue_after(delayed_task, delay_secs).await?;
    println!("✓ Scheduled task to run in {} seconds", delay_secs);

    println!("Attempting immediate dequeue (should be empty)...");
    if (broker.dequeue().await?).is_some() {
        println!("  Unexpectedly got a task");
    } else {
        println!("  ✓ No tasks available (task is delayed)");
    }

    println!("Waiting {} seconds...", delay_secs);
    sleep(Duration::from_secs(delay_secs)).await;

    println!("Attempting dequeue after delay...");
    if let Some(msg) = broker.dequeue().await? {
        println!(
            "  ✓ Successfully dequeued delayed task: {}",
            msg.task.metadata.name
        );
        broker
            .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
            .await?;
    }
    println!();

    // Example 5: Queue Statistics
    println!("5. Queue Statistics");
    println!("--------------------");

    // Enqueue a few more tasks for stats
    for i in 0..5 {
        let task = SerializedTask::new(format!("task_{}", i), vec![i as u8]);
        broker.enqueue(task).await?;
    }

    let stats = broker.get_statistics().await?;
    println!("Queue Statistics:");
    println!("  Pending: {}", stats.pending);
    println!("  Processing: {}", stats.processing);
    println!("  Completed: {}", stats.completed);
    println!("  Failed: {}", stats.failed);
    println!("  Cancelled: {}", stats.cancelled);
    println!("  Total: {}", stats.total);
    println!();

    // Example 6: Queue Control (Pause/Resume)
    println!("6. Queue Control");
    println!("-----------------");

    println!("Pausing queue...");
    broker.pause();
    println!("✓ Queue paused");

    let is_paused = broker.is_paused();
    println!("  Is paused: {}", is_paused);

    println!("Attempting to dequeue while paused...");
    match broker.dequeue().await {
        Ok(None) => println!("  ✓ No tasks dequeued (queue is paused)"),
        Ok(Some(_)) => println!("  Unexpected: got a task while paused"),
        Err(e) => println!("  Error: {}", e),
    }

    println!("Resuming queue...");
    broker.resume();
    println!("✓ Queue resumed");

    let is_paused = broker.is_paused();
    println!("  Is paused: {}\n", is_paused);

    // Example 7: Task Rejection and Retries
    println!("7. Task Rejection and Retries");
    println!("-------------------------------");

    let retry_task = SerializedTask::new("risky_operation".to_string(), vec![]).with_max_retries(3);
    broker.enqueue(retry_task).await?;
    println!("✓ Enqueued task with 3 retries allowed");

    if let Some(msg) = broker.dequeue().await? {
        println!("✓ Dequeued task for processing");

        // Simulate a failure - reject and requeue
        println!("Simulating task failure...");
        broker
            .reject(
                &msg.task.metadata.id,
                msg.receipt_handle.as_deref(),
                true, // requeue: true to retry
            )
            .await?;
        println!("✓ Task rejected (will retry)");

        // Check if task is back in queue for retry
        sleep(Duration::from_millis(100)).await;

        if let Some(retry_msg) = broker.dequeue().await? {
            println!("✓ Task automatically requeued for retry");
            println!("  Task ID: {}", retry_msg.task.metadata.id);
            broker
                .ack(
                    &retry_msg.task.metadata.id,
                    retry_msg.receipt_handle.as_deref(),
                )
                .await?;
        }
    }
    println!();

    // Example 8: Cleanup
    println!("8. Cleanup");
    println!("-----------");

    let queue_size = broker.queue_size().await?;
    println!("Current queue size: {}", queue_size);

    if queue_size > 0 {
        println!("Dequeuing and acknowledging remaining tasks...");
        while let Some(msg) = broker.dequeue().await? {
            broker
                .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
                .await?;
        }
        println!("✓ Queue cleared");
    }

    let final_stats = broker.get_statistics().await?;
    println!("\nFinal Statistics:");
    println!("  Completed: {}", final_stats.completed);
    println!("  Failed: {}", final_stats.failed);

    println!("\n=== Example Complete ===");
    println!("\nNext steps:");
    println!("  - Explore advanced features with monitoring_performance example");
    println!("  - Review the README for production best practices");
    println!("  - Configure PostgreSQL for optimal performance");

    Ok(())
}
