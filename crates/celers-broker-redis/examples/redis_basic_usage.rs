//! Basic Redis broker usage example
//!
//! Demonstrates:
//! - Creating a Redis broker
//! - Enqueuing tasks
//! - Dequeuing tasks
//! - Acknowledging tasks
//! - Using FIFO vs Priority queues
//!
//! Run with: cargo run --example basic_usage
//! Requires: Redis running on localhost:6379

use celers_broker_redis::{QueueMode, RedisBroker};
use celers_core::{Broker, SerializedTask, TaskMetadata};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("=== Basic Redis Broker Usage Example ===\n");

    // Example 1: FIFO Queue
    println!("1. FIFO Queue Example");
    fifo_example().await?;

    // Example 2: Priority Queue
    println!("\n2. Priority Queue Example");
    priority_example().await?;

    // Example 3: Batch Operations
    println!("\n3. Batch Operations Example");
    batch_example().await?;

    println!("\n=== All examples completed successfully! ===");
    Ok(())
}

async fn fifo_example() -> Result<(), Box<dyn std::error::Error>> {
    let broker = RedisBroker::new("redis://localhost:6379", "fifo_example")?;

    // Create some tasks
    let task1 = create_task("process_image", 5, vec![1, 2, 3]);
    let task2 = create_task("send_email", 3, vec![4, 5, 6]);
    let task3 = create_task("generate_report", 8, vec![7, 8, 9]);

    // Enqueue tasks
    let id1 = broker.enqueue(task1.clone()).await?;
    let id2 = broker.enqueue(task2.clone()).await?;
    let id3 = broker.enqueue(task3.clone()).await?;

    println!("  Enqueued tasks: {}, {}, {}", id1, id2, id3);
    println!("  Queue size: {}", broker.queue_size().await?);

    // Dequeue tasks (FIFO order)
    if let Some(msg) = broker.dequeue().await? {
        println!(
            "  Dequeued: {} (priority: {})",
            msg.task.metadata.name, msg.task.metadata.priority
        );
        broker
            .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
            .await?;
    }

    if let Some(msg) = broker.dequeue().await? {
        println!(
            "  Dequeued: {} (priority: {})",
            msg.task.metadata.name, msg.task.metadata.priority
        );
        broker
            .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
            .await?;
    }

    println!("  Remaining queue size: {}", broker.queue_size().await?);

    // Cleanup
    broker.purge_all_queues().await?;

    Ok(())
}

async fn priority_example() -> Result<(), Box<dyn std::error::Error>> {
    let broker = RedisBroker::with_mode(
        "redis://localhost:6379",
        "priority_example",
        QueueMode::Priority,
    )?;

    // Create tasks with different priorities
    let low_priority = create_task("backup_logs", 1, vec![1]);
    let medium_priority = create_task("process_order", 5, vec![2]);
    let high_priority = create_task("urgent_notification", 9, vec![3]);

    // Enqueue in random order
    broker.enqueue(low_priority).await?;
    broker.enqueue(high_priority).await?;
    broker.enqueue(medium_priority).await?;

    println!("  Enqueued 3 tasks with priorities: 1, 9, 5");
    println!("  Queue size: {}", broker.queue_size().await?);

    // Dequeue tasks (priority order: 9, 5, 1)
    println!("  Dequeuing in priority order:");
    while let Some(msg) = broker.dequeue().await? {
        println!(
            "    - {} (priority: {})",
            msg.task.metadata.name, msg.task.metadata.priority
        );
        broker
            .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
            .await?;
    }

    // Cleanup
    broker.purge_all_queues().await?;

    Ok(())
}

async fn batch_example() -> Result<(), Box<dyn std::error::Error>> {
    let broker = RedisBroker::new("redis://localhost:6379", "batch_example")?;

    // Create batch of tasks
    let tasks: Vec<SerializedTask> = (0..10)
        .map(|i| create_task(&format!("task_{}", i), 5, vec![i as u8]))
        .collect();

    println!("  Enqueuing batch of {} tasks", tasks.len());
    let task_ids = broker.enqueue_batch(tasks).await?;
    println!("  Enqueued {} tasks", task_ids.len());
    println!("  Queue size: {}", broker.queue_size().await?);

    // Dequeue batch
    println!("  Dequeuing batch of 5 tasks");
    let messages = broker.dequeue_batch(5).await?;
    println!("  Dequeued {} tasks", messages.len());

    // Acknowledge batch
    let acks: Vec<_> = messages
        .iter()
        .map(|msg| (msg.task.metadata.id, msg.receipt_handle.clone()))
        .collect();
    broker.ack_batch(&acks).await?;
    println!("  Acknowledged {} tasks", acks.len());

    println!("  Remaining queue size: {}", broker.queue_size().await?);

    // Cleanup
    broker.purge_all_queues().await?;

    Ok(())
}

fn create_task(name: &str, priority: i32, payload: Vec<u8>) -> SerializedTask {
    let mut metadata = TaskMetadata::new(name.to_string());
    metadata.priority = priority;
    SerializedTask { metadata, payload }
}
