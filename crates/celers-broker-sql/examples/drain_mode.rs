//! Queue Drain Mode Example
//!
//! This example demonstrates how to use drain mode for graceful shutdowns
//! and zero-downtime deployments.
//!
//! Features demonstrated:
//! - Enabling drain mode to stop accepting new tasks
//! - Processing existing tasks while in drain mode
//! - Checking drain mode status
//! - Disabling drain mode to resume operations
//! - Graceful shutdown pattern
//!
//! Run with:
//! ```bash
//! cargo run --example drain_mode
//! ```

use celers_broker_sql::MysqlBroker;
use celers_core::{Broker, SerializedTask};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("=== Queue Drain Mode Example ===\n");

    // Database connection
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://root:password@localhost/celers".to_string());

    println!("Connecting to database...");
    let broker = MysqlBroker::new(&database_url).await?;
    println!("Connected successfully!\n");

    // Example 1: Basic drain mode usage
    println!("--- Example 1: Basic Drain Mode ---");

    // Enqueue some tasks before drain mode
    println!("Enqueueing 5 tasks...");
    for i in 1..=5 {
        let task = SerializedTask::new(format!("task_{}", i), vec![]);
        broker.enqueue(task).await?;
    }

    let queue_size = broker.queue_size().await?;
    println!("Queue size before drain mode: {}\n", queue_size);

    // Enable drain mode
    println!("Enabling drain mode...");
    broker.enable_drain_mode().await?;

    let is_draining = broker.is_drain_mode().await?;
    println!("Drain mode enabled: {}", is_draining);
    assert!(is_draining);

    // Try to enqueue during drain mode (note: actual behavior depends on integration)
    println!("\nAttempting to enqueue tasks during drain mode...");
    println!("(In production, API layer should check drain mode and reject new tasks)\n");

    // Check queue size during drain
    let queue_size = broker.queue_size().await?;
    println!("Queue size during drain mode: {}", queue_size);

    // Disable drain mode
    println!("\nDisabling drain mode...");
    broker.disable_drain_mode().await?;

    let is_draining = broker.is_drain_mode().await?;
    println!("Drain mode disabled: {}", !is_draining);
    assert!(!is_draining);

    // Example 2: Graceful shutdown simulation
    println!("\n--- Example 2: Graceful Shutdown Simulation ---");

    // Enqueue more tasks
    println!("Enqueueing 10 tasks for processing...");
    for i in 1..=10 {
        let task = SerializedTask::new(format!("shutdown_task_{}", i), vec![]);
        broker.enqueue(task).await?;
    }

    println!("Initial queue size: {}", broker.queue_size().await?);

    // Simulate shutdown signal
    println!("\n[SHUTDOWN SIGNAL RECEIVED]");
    println!("Step 1: Enable drain mode to stop accepting new tasks");
    broker.enable_drain_mode().await?;

    println!("Step 2: Wait for workers to finish existing tasks");
    println!("(Simulating worker processing...)");

    // Simulate workers processing tasks
    let mut iterations = 0;
    while broker.queue_size().await? > 0 && iterations < 10 {
        sleep(Duration::from_millis(500)).await;
        let remaining = broker.queue_size().await?;
        println!("  Tasks remaining: {}", remaining);

        // Simulate processing a task
        if let Some(msg) = broker.dequeue().await? {
            broker
                .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
                .await?;
            println!("  Processed task: {}", msg.task.metadata.id);
        }

        iterations += 1;
    }

    println!("Step 3: All tasks processed, safe to shutdown");
    println!("Final queue size: {}\n", broker.queue_size().await?);

    // Example 3: Rolling deployment pattern
    println!("--- Example 3: Rolling Deployment Pattern ---");

    println!("Scenario: Deploying new version of workers\n");

    // Enqueue some tasks
    for i in 1..=5 {
        let task = SerializedTask::new(format!("deployment_task_{}", i), vec![]);
        broker.enqueue(task).await?;
    }

    println!("Old worker version: 1.0");
    println!("  Tasks in queue: {}", broker.queue_size().await?);

    println!("\nStep 1: Enable drain mode on old workers");
    broker.enable_drain_mode().await?;
    println!("  Drain mode: {}", broker.is_drain_mode().await?);

    println!("\nStep 2: Start new workers (version 2.0)");
    println!("  New workers can process tasks immediately");

    println!("\nStep 3: Old workers finish their tasks");
    // Process a couple tasks to simulate old workers finishing
    for _ in 0..2 {
        if let Some(msg) = broker.dequeue().await? {
            broker
                .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
                .await?;
            println!("  Old worker processed: {}", msg.task.metadata.id);
        }
        sleep(Duration::from_millis(200)).await;
    }

    println!("\nStep 4: Old workers shutdown gracefully");
    println!("  Remaining tasks: {}", broker.queue_size().await?);

    println!("\nStep 5: New workers take over completely");
    broker.disable_drain_mode().await?;
    println!("  Drain mode: {}", broker.is_drain_mode().await?);

    println!("\nDeployment complete! Zero downtime achieved.");

    // Example 4: Maintenance window pattern
    println!("\n--- Example 4: Maintenance Window ---");

    println!("Scenario: Performing database maintenance\n");

    // Add some tasks
    for i in 1..=3 {
        let task = SerializedTask::new(format!("maintenance_task_{}", i), vec![]);
        broker.enqueue(task).await?;
    }

    println!("Current queue size: {}", broker.queue_size().await?);

    println!("\nStarting maintenance window:");
    println!("  1. Enable drain mode");
    broker.enable_drain_mode().await?;

    println!("  2. Wait for queue to drain");
    let mut wait_iterations = 0;
    while broker.queue_size().await? > 0 && wait_iterations < 10 {
        if let Some(msg) = broker.dequeue().await? {
            broker
                .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
                .await?;
        }
        sleep(Duration::from_millis(100)).await;
        wait_iterations += 1;
    }
    println!(
        "  Queue drained: {} tasks remaining",
        broker.queue_size().await?
    );

    println!("  3. Perform maintenance (simulated)");
    sleep(Duration::from_millis(500)).await;
    println!("  Maintenance complete");

    println!("  4. Resume normal operations");
    broker.disable_drain_mode().await?;
    println!("  System ready for new tasks");

    // Example 5: Multiple queue drain coordination
    println!("\n--- Example 5: Multi-Queue Drain ---");

    let queue1 = MysqlBroker::with_queue(&database_url, "queue1").await?;
    let queue2 = MysqlBroker::with_queue(&database_url, "queue2").await?;

    println!("Managing multiple queues:");
    println!("  Queue 1 drain mode: {}", queue1.is_drain_mode().await?);
    println!("  Queue 2 drain mode: {}", queue2.is_drain_mode().await?);

    println!("\nEnabling drain mode on both queues...");
    queue1.enable_drain_mode().await?;
    queue2.enable_drain_mode().await?;

    println!("  Queue 1 drain mode: {}", queue1.is_drain_mode().await?);
    println!("  Queue 2 drain mode: {}", queue2.is_drain_mode().await?);

    println!("\nDisabling drain mode on both queues...");
    queue1.disable_drain_mode().await?;
    queue2.disable_drain_mode().await?;

    println!("  Queue 1 drain mode: {}", queue1.is_drain_mode().await?);
    println!("  Queue 2 drain mode: {}", queue2.is_drain_mode().await?);

    println!("\n=== Example Complete ===");
    println!("\nBest Practices:");
    println!("  - Always check drain mode status before accepting new tasks");
    println!("  - Set appropriate timeouts for queue draining");
    println!("  - Monitor queue size during drain operations");
    println!("  - Use drain mode for zero-downtime deployments");
    println!("  - Coordinate drain mode across all queue instances");

    Ok(())
}
