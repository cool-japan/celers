//! Advanced Redis broker features example
//!
//! Demonstrates:
//! - Dead Letter Queue (DLQ) handling
//! - Task replay from DLQ
//! - Delayed task execution
//! - Task cancellation
//! - Health checks
//! - Queue control (pause/resume/drain)
//!
//! Run with: cargo run --example advanced_features
//! Requires: Redis running on localhost:6379

use celers_broker_redis::RedisBroker;
use celers_core::{Broker, SerializedTask, TaskMetadata};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("=== Advanced Redis Broker Features Example ===\n");

    // Example 1: DLQ Handling
    println!("1. Dead Letter Queue (DLQ) Example");
    dlq_example().await?;

    // Example 2: Delayed Tasks
    println!("\n2. Delayed Task Execution Example");
    delayed_task_example().await?;

    // Example 3: Task Cancellation
    println!("\n3. Task Cancellation Example");
    cancellation_example().await?;

    // Example 4: Health Checks
    println!("\n4. Health Check Example");
    health_check_example().await?;

    // Example 5: Queue Control
    println!("\n5. Queue Control Example");
    queue_control_example().await?;

    println!("\n=== All advanced examples completed successfully! ===");
    Ok(())
}

async fn dlq_example() -> Result<(), Box<dyn std::error::Error>> {
    let broker = RedisBroker::new("redis://localhost:6379", "dlq_example")?;

    // Create and enqueue a task
    let task = create_task("failing_task", 5, vec![1, 2, 3]);
    let task_id = broker.enqueue(task.clone()).await?;
    println!("  Enqueued task: {}", task_id);

    // Dequeue and reject (simulating failure)
    if let Some(msg) = broker.dequeue().await? {
        println!("  Dequeued task, simulating failure...");
        // Reject without requeue -> moves to DLQ
        broker
            .reject(&msg.task.metadata.id, msg.receipt_handle.as_deref(), false)
            .await?;
        println!("  Task moved to DLQ");
    }

    // Check DLQ size
    let dlq_size = broker.dlq_size().await?;
    println!("  DLQ size: {}", dlq_size);

    // Inspect DLQ
    let dlq_tasks = broker.inspect_dlq(10).await?;
    println!("  Tasks in DLQ: {}", dlq_tasks.len());
    for task in &dlq_tasks {
        println!("    - {}: {}", task.metadata.id, task.metadata.name);
    }

    // Replay task from DLQ
    if broker.replay_from_dlq(&task_id).await? {
        println!("  Successfully replayed task from DLQ");
        println!("  Queue size after replay: {}", broker.queue_size().await?);
    }

    // Cleanup
    broker.purge_all_queues().await?;
    broker.clear_dlq().await?;

    Ok(())
}

async fn delayed_task_example() -> Result<(), Box<dyn std::error::Error>> {
    let broker = RedisBroker::new("redis://localhost:6379", "delayed_example")?;

    println!("  Scheduling tasks for future execution...");

    // Schedule task for 2 seconds from now
    let task1 = create_task("delayed_task_2s", 5, vec![1]);
    broker.enqueue_after(task1, 2).await?;
    println!("    - Task scheduled for 2 seconds from now");

    // Schedule task for 5 seconds from now
    let task2 = create_task("delayed_task_5s", 7, vec![2]);
    broker.enqueue_after(task2, 5).await?;
    println!("    - Task scheduled for 5 seconds from now");

    // Check main queue (should be empty)
    println!("  Main queue size: {}", broker.queue_size().await?);

    // Wait for first task to become ready
    println!("  Waiting 3 seconds...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Try dequeuing (should get the first task)
    if let Some(msg) = broker.dequeue().await? {
        println!("  Dequeued delayed task: {}", msg.task.metadata.name);
        broker
            .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
            .await?;
    }

    // Cleanup
    broker.purge_all_queues().await?;

    Ok(())
}

async fn cancellation_example() -> Result<(), Box<dyn std::error::Error>> {
    let broker = RedisBroker::new("redis://localhost:6379", "cancel_example")?;

    // Create and enqueue tasks
    let task1 = create_task("long_running_task", 5, vec![1]);
    let task2 = create_task("another_task", 3, vec![2]);
    let task_id1 = broker.enqueue(task1).await?;
    let task_id2 = broker.enqueue(task2).await?;

    println!("  Enqueued tasks: {}, {}", task_id1, task_id2);

    // Cancel the first task
    println!("  Cancelling task: {}", task_id1);
    let cancelled = broker.cancel(&task_id1).await?;

    if cancelled {
        println!("    Cancellation signal sent successfully");
    } else {
        println!("    No subscribers listening (would work in real worker scenario)");
    }

    // Get cancel channel name
    println!("  Cancel channel: {}", broker.cancel_channel());

    // Cleanup
    broker.purge_all_queues().await?;

    Ok(())
}

async fn health_check_example() -> Result<(), Box<dyn std::error::Error>> {
    let broker = RedisBroker::new("redis://localhost:6379", "health_example")?;

    // Ping Redis
    let latency = broker.ping().await?;
    println!("  Redis ping latency: {} ms", latency);

    // Get comprehensive health status
    let health = broker.check_health().await;
    println!("  Health Status:");
    println!("    Healthy: {}", health.is_healthy);
    println!("    Latency: {} ms", health.latency_ms);
    if let (Some(used), Some(max)) = (health.used_memory, health.max_memory) {
        println!(
            "    Memory: {} bytes / {} bytes ({:.1}%)",
            used,
            max,
            health.memory_usage_percent.unwrap_or(0.0)
        );
    }
    if let Some(clients) = health.connected_clients {
        println!("    Connected clients: {}", clients);
    }
    println!(
        "    Redis version: {}",
        health
            .redis_version
            .unwrap_or_else(|| "unknown".to_string())
    );
    println!(
        "    Role: {}",
        health.role.unwrap_or_else(|| "unknown".to_string())
    );

    // Get queue statistics
    let stats = broker.get_queue_stats().await?;
    println!("  Queue Statistics:");
    println!("    Pending: {}", stats.pending);
    println!("    Processing: {}", stats.processing);
    println!("    DLQ: {}", stats.dlq);
    println!("    Delayed: {}", stats.delayed);

    // Cleanup
    broker.purge_all_queues().await?;

    Ok(())
}

async fn queue_control_example() -> Result<(), Box<dyn std::error::Error>> {
    let broker = RedisBroker::new("redis://localhost:6379", "control_example")?;
    let controller = broker.queue_controller();

    // Enqueue some tasks
    for i in 0..5 {
        let task = create_task(&format!("task_{}", i), 5, vec![i]);
        broker.enqueue(task).await?;
    }
    println!("  Enqueued 5 tasks");
    println!("  Queue size: {}", broker.queue_size().await?);

    // Pause the queue
    println!("  Pausing queue...");
    controller.pause().await?;
    let state = controller.get_state().await?;
    println!("    Queue state: {:?}", state);
    println!("    Can enqueue: {}", controller.can_enqueue().await?);
    println!("    Can dequeue: {}", controller.can_dequeue().await?);

    // Resume the queue
    println!("  Resuming queue...");
    controller.resume().await?;
    let state = controller.get_state().await?;
    println!("    Queue state: {:?}", state);

    // Drain mode (no new enqueues, but can dequeue)
    println!("  Setting drain mode...");
    controller.drain().await?;
    let state = controller.get_state().await?;
    println!("    Queue state: {:?}", state);
    println!("    Can enqueue: {}", controller.can_enqueue().await?);
    println!("    Can dequeue: {}", controller.can_dequeue().await?);

    // Resume normal operation
    controller.resume().await?;

    // Cleanup
    broker.purge_all_queues().await?;

    Ok(())
}

fn create_task(name: &str, priority: i32, payload: Vec<u8>) -> SerializedTask {
    let mut metadata = TaskMetadata::new(name.to_string());
    metadata.priority = priority;
    SerializedTask { metadata, payload }
}
