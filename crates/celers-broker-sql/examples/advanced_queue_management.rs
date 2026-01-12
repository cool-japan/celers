//! Advanced Queue Management Example
//!
//! This example demonstrates production-critical queue management features:
//!
//! - **Transactional Operations**: Execute multiple operations atomically
//! - **Metadata Queries**: Search tasks by JSON metadata fields
//! - **Capacity Management**: Implement backpressure and queue limits
//! - **Task Expiration**: Automatically expire stale pending tasks
//! - **Batch State Updates**: Update multiple task states efficiently
//! - **Date Range Queries**: Search tasks within time windows
//!
//! These features are essential for:
//! - Maintaining queue health and preventing overload
//! - Implementing complex task dependencies
//! - Cleaning up stale or abandoned tasks
//! - Advanced monitoring and analytics
//! - Ensuring data consistency across operations
//!
//! # Prerequisites
//!
//! 1. MySQL 8.0+ running
//! 2. Database created: `CREATE DATABASE celers_test;`
//! 3. Migrations applied (will run automatically)
//!
//! # Running this example
//!
//! ```bash
//! cargo run --example advanced_queue_management
//! ```

use celers_broker_sql::{DbTaskState, MysqlBroker};
use celers_core::{Broker, SerializedTask};
use chrono::{Duration as ChronoDuration, Utc};
use serde_json::json;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
#[allow(dead_code)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("=== CeleRS MySQL Broker: Advanced Queue Management ===\n");

    // Connect to broker
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://root:password@localhost:3306/celers_test".to_string());

    let broker = MysqlBroker::new(&database_url).await?;
    println!("✓ Connected to MySQL broker\n");

    broker.migrate().await?;
    println!("✓ Migrations applied\n");

    // Clean up
    broker.purge_all().await?;

    // Demo 1: Transactional operations
    println!("--- Demo 1: Transactional Operations ---");
    demo_transactions(&broker).await?;

    // Demo 2: Metadata-based queries
    println!("\n--- Demo 2: Metadata-Based Queries ---");
    demo_metadata_queries(&broker).await?;

    // Demo 3: Capacity management
    println!("\n--- Demo 3: Capacity Management (Backpressure) ---");
    demo_capacity_management(&broker).await?;

    // Demo 4: Task expiration
    println!("\n--- Demo 4: Task Expiration (TTL) ---");
    demo_task_expiration(&broker).await?;

    // Demo 5: Batch state updates
    println!("\n--- Demo 5: Batch State Updates ---");
    demo_batch_state_updates(&broker).await?;

    // Demo 6: Date range queries
    println!("\n--- Demo 6: Date Range Queries ---");
    demo_date_range_queries(&broker).await?;

    println!("\n=== Demo Complete ===");

    Ok(())
}

/// Demo 1: Transactional Operations
///
/// Demonstrates atomic multi-step operations using transactions.
/// All operations succeed together or fail together, ensuring data consistency.
async fn demo_transactions(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    println!("  1. Creating tasks using batch enqueue (atomic operation)...");

    // Batch enqueue is transactional - all tasks are created atomically
    let tasks = vec![
        SerializedTask::new(
            "step1".to_string(),
            serde_json::to_vec(&json!({"step": 1}))?,
        ),
        SerializedTask::new(
            "step2".to_string(),
            serde_json::to_vec(&json!({"step": 2}))?,
        ),
        SerializedTask::new(
            "step3".to_string(),
            serde_json::to_vec(&json!({"step": 3}))?,
        ),
    ];

    let task_ids = broker.enqueue_batch(tasks).await?;

    println!("     ✓ Batch transaction committed successfully");
    println!(
        "     ✓ Created {} tasks atomically: {}",
        task_ids.len(),
        task_ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );

    let stats = broker.get_statistics().await?;
    println!("     ✓ Queue now has {} pending tasks", stats.pending);

    println!("\n  2. Updating metadata atomically...");
    if !task_ids.is_empty() {
        broker
            .update_task_metadata(&task_ids[0], "$.workflow_id", "\"wf-12345\"")
            .await?;
        println!("     ✓ Metadata updated on task {}", task_ids[0]);
    }

    println!("\n  💡 Transaction features:");
    println!("     - Batch operations are automatically transactional");
    println!("     - Use with_transaction() for custom SQL operations");
    println!("     - All-or-nothing semantics ensure data consistency");

    Ok(())
}

/// Demo 2: Metadata-Based Queries
///
/// Demonstrates searching tasks by JSON metadata fields.
/// Useful for filtering tasks by business attributes.
async fn demo_metadata_queries(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    println!("  1. Creating tasks and updating with metadata...");

    // Create tasks for different customers
    let mut task_ids = Vec::new();
    for i in 0..3 {
        let payload = json!({"order_id": format!("order-{}", i)});
        let task = SerializedTask::new("process_order".to_string(), serde_json::to_vec(&payload)?);

        let task_id = broker.enqueue(task).await?;
        task_ids.push(task_id);

        // Update metadata after creation
        let customer_id = format!("\"cust-{}\"", i % 2);
        broker
            .update_task_metadata(&task_id, "$.customer_id", &customer_id)
            .await?;

        if i % 2 == 0 {
            broker
                .update_task_metadata(&task_id, "$.priority_customer", "true")
                .await?;
        }
    }

    println!("     ✓ Created 3 tasks with customer metadata");

    // Query tasks for specific customer
    println!("\n  2. Querying tasks for customer 'cust-0'...");
    let customer_tasks = broker
        .query_tasks_by_metadata("$.customer_id", "\"cust-0\"", 10, 0)
        .await?;

    println!(
        "     ✓ Found {} tasks for customer 'cust-0'",
        customer_tasks.len()
    );
    for task in &customer_tasks {
        println!("       - Task {}: {}", task.id, task.task_name);
    }

    // Query priority customers
    println!("\n  3. Querying priority customer tasks...");
    let priority_tasks = broker
        .query_tasks_by_metadata("$.priority_customer", "true", 10, 0)
        .await?;

    println!(
        "     ✓ Found {} priority customer tasks",
        priority_tasks.len()
    );

    println!("\n  💡 Metadata query use cases:");
    println!("     - Filter tasks by customer, tenant, or organization");
    println!("     - Find tasks with specific business attributes");
    println!("     - Implement priority queues based on metadata");
    println!("     - Track and query tasks by workflow or campaign ID");

    Ok(())
}

/// Demo 3: Capacity Management
///
/// Demonstrates queue capacity limits and backpressure control.
/// Prevents queue overload by rejecting tasks when queue is full.
async fn demo_capacity_management(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    let max_capacity = 5;

    println!("  1. Checking queue capacity (max: {})...", max_capacity);
    let has_capacity = broker.has_capacity(max_capacity).await?;
    println!("     ✓ Queue has capacity: {}", has_capacity);

    println!("\n  2. Enqueuing tasks with capacity check...");
    let mut enqueued_count = 0;

    for i in 0..8 {
        let payload = json!({"item": i});
        let task = SerializedTask::new("process_item".to_string(), serde_json::to_vec(&payload)?);

        match broker.enqueue_with_capacity(task, max_capacity).await {
            Ok(task_id) => {
                enqueued_count += 1;
                println!("     ✓ Task {} enqueued: {}", i, task_id);
            }
            Err(e) => {
                println!("     ✗ Task {} rejected (queue full): {}", i, e);
            }
        }
    }

    println!(
        "\n     ✓ Successfully enqueued {} tasks (max capacity: {})",
        enqueued_count, max_capacity
    );

    let stats = broker.get_statistics().await?;
    println!("     ✓ Current queue size: {}", stats.pending);

    // Clean up some tasks to make room
    println!("\n  3. Processing 3 tasks to free capacity...");
    for _ in 0..3 {
        if let Some(msg) = broker.dequeue().await? {
            broker
                .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
                .await?;
        }
    }

    let has_capacity_now = broker.has_capacity(max_capacity).await?;
    println!("     ✓ Queue now has capacity: {}", has_capacity_now);

    Ok(())
}

/// Demo 4: Task Expiration
///
/// Demonstrates automatic expiration of stale pending tasks.
/// Useful for cleaning up abandoned or outdated tasks.
async fn demo_task_expiration(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    println!("  1. Creating tasks with short TTL...");

    // Create some tasks
    for i in 0..5 {
        let payload = json!({"job": i});
        let task = SerializedTask::new(
            "short_lived_task".to_string(),
            serde_json::to_vec(&payload)?,
        );
        broker.enqueue(task).await?;
    }

    println!("     ✓ Created 5 tasks");

    let stats_before = broker.get_statistics().await?;
    println!("     ✓ Pending tasks: {}", stats_before.pending);

    println!("\n  2. Waiting 2 seconds for tasks to age...");
    sleep(Duration::from_secs(2)).await;

    // Expire tasks older than 1 second
    println!("  3. Expiring tasks older than 1 second...");
    let expired_count = broker.expire_pending_tasks(Duration::from_secs(1)).await?;

    println!("     ✓ Expired {} stale tasks", expired_count);

    let stats_after = broker.get_statistics().await?;
    println!("     ✓ Pending tasks now: {}", stats_after.pending);
    println!("     ✓ Cancelled tasks: {}", stats_after.cancelled);

    println!("\n  💡 Tip: Run expire_pending_tasks() periodically to clean up");
    println!("     abandoned tasks (e.g., from crashed clients or cancelled operations)");

    Ok(())
}

/// Demo 5: Batch State Updates
///
/// Demonstrates efficient batch updates of task states.
/// More efficient than individual updates for bulk operations.
async fn demo_batch_state_updates(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    println!("  1. Creating 10 test tasks...");

    let mut task_ids = Vec::new();
    for i in 0..10 {
        let payload = json!({"batch_item": i});
        let task = SerializedTask::new("batch_task".to_string(), serde_json::to_vec(&payload)?);
        let task_id = broker.enqueue(task).await?;
        task_ids.push(task_id);
    }

    println!("     ✓ Created {} tasks", task_ids.len());

    // Update half of them to processing state
    println!("\n  2. Updating 5 tasks to 'processing' state in batch...");
    let processing_ids = &task_ids[0..5];
    let updated = broker
        .update_batch_state(processing_ids, DbTaskState::Processing)
        .await?;

    println!("     ✓ Updated {} tasks to processing", updated);

    // Verify state distribution
    let stats = broker.get_statistics().await?;
    println!("\n  Queue State Distribution:");
    println!("    - Pending: {}", stats.pending);
    println!("    - Processing: {}", stats.processing);

    // Update remaining to cancelled
    println!("\n  3. Cancelling remaining 5 tasks in batch...");
    let cancelled_ids = &task_ids[5..10];
    let cancelled = broker
        .update_batch_state(cancelled_ids, DbTaskState::Cancelled)
        .await?;

    println!("     ✓ Cancelled {} tasks", cancelled);

    let final_stats = broker.get_statistics().await?;
    println!("\n  Final State Distribution:");
    println!("    - Pending: {}", final_stats.pending);
    println!("    - Processing: {}", final_stats.processing);
    println!("    - Cancelled: {}", final_stats.cancelled);

    Ok(())
}

/// Demo 6: Date Range Queries
///
/// Demonstrates searching tasks within specific time windows.
/// Useful for analytics, auditing, and time-based cleanup.
async fn demo_date_range_queries(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    println!("  1. Creating tasks at different times...");

    let now = Utc::now();

    // Create task "in the past" (simulate by creating and noting time)
    let past_task = SerializedTask::new(
        "past_task".to_string(),
        serde_json::to_vec(&json!({"time": "past"}))?,
    );
    broker.enqueue(past_task).await?;

    // Wait a bit
    sleep(Duration::from_millis(100)).await;
    let middle_time = Utc::now();

    // Create more tasks
    for i in 0..3 {
        let task = SerializedTask::new(
            "current_task".to_string(),
            serde_json::to_vec(&json!({"time": "current", "id": i}))?,
        );
        broker.enqueue(task).await?;
    }

    sleep(Duration::from_millis(100)).await;
    let end_time = Utc::now();

    println!("     ✓ Created tasks across a time range");

    // Query all tasks
    println!("\n  2. Querying all tasks (entire time range)...");
    let all_tasks = broker
        .search_tasks_by_date_range(
            now - ChronoDuration::seconds(10),
            end_time + ChronoDuration::seconds(10),
            None,
            100,
            0,
        )
        .await?;

    println!("     ✓ Found {} total tasks", all_tasks.len());

    // Query tasks created after middle time
    println!("\n  3. Querying tasks created in last time window...");
    let recent_tasks = broker
        .search_tasks_by_date_range(middle_time, end_time, None, 100, 0)
        .await?;

    println!("     ✓ Found {} recent tasks", recent_tasks.len());
    for task in &recent_tasks {
        println!(
            "       - Task {}: {} (created: {})",
            task.id, task.task_name, task.created_at
        );
    }

    // Query by state and date range
    println!("\n  4. Querying pending tasks in date range...");
    let pending_in_range = broker
        .search_tasks_by_date_range(
            now - ChronoDuration::seconds(10),
            end_time,
            Some(DbTaskState::Pending),
            100,
            0,
        )
        .await?;

    println!(
        "     ✓ Found {} pending tasks in range",
        pending_in_range.len()
    );

    println!("\n  💡 Use cases for date range queries:");
    println!("     - Generate time-based reports and analytics");
    println!("     - Audit task processing within specific periods");
    println!("     - Clean up tasks older than retention period");
    println!("     - Monitor SLA compliance (tasks created vs completed time)");

    Ok(())
}
