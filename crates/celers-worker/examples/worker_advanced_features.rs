//! Advanced Features Example
//!
//! This example demonstrates advanced features of celers-worker including:
//! - Multi-level priority queues with priority inheritance
//! - Sliding window rate limiting
//! - Lock-free task queues
//! - Task scheduling with resource awareness
//!
//! Run with: cargo run --example advanced_features

use celers_worker::{
    LockFreeQueue, ScheduledTask, SchedulerConfig, SlidingWindowConfig, SlidingWindowLimiter,
    TaskPriority, TaskRequirements, TaskScheduler,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    println!("=== Advanced Features Demo ===\n");

    // Demo 1: Multi-level Priority Queues
    demo_priority_queues().await;

    // Demo 2: Priority Inheritance
    demo_priority_inheritance().await;

    // Demo 3: Sliding Window Rate Limiting
    demo_sliding_window_rate_limiting().await;

    // Demo 4: Lock-Free Queues
    demo_lockfree_queues().await;

    println!("\n=== Demo Complete ===");
}

/// Demo 1: Multi-level priority queues ensure high-priority tasks are processed first
async fn demo_priority_queues() {
    println!("--- Demo 1: Multi-Level Priority Queues ---");

    let config = SchedulerConfig::default().resource_aware(false);
    let scheduler = TaskScheduler::new(config);

    // Add tasks with different priorities
    let tasks = vec![
        (
            "critical-backup",
            TaskPriority::Highest,
            "Critical system backup",
        ),
        ("normal-email", TaskPriority::Normal, "Send email"),
        ("low-cleanup", TaskPriority::Low, "Cleanup old logs"),
        ("high-alert", TaskPriority::High, "Send alert"),
    ];

    for (id, priority, desc) in tasks {
        let task = ScheduledTask::new(
            id.to_string(),
            desc.to_string(),
            priority,
            TaskRequirements::default(),
        );
        scheduler.enqueue(task).await.unwrap();
        println!("  Enqueued: {} with priority {:?}", desc, priority);
    }

    // Get priority statistics
    let stats = scheduler.priority_stats().await;
    println!("\nPriority Distribution:");
    println!("  Lowest:  {}", stats[0]);
    println!("  Low:     {}", stats[1]);
    println!("  Normal:  {}", stats[2]);
    println!("  High:    {}", stats[3]);
    println!("  Highest: {}", stats[4]);

    // Dequeue tasks - they'll come out in priority order
    println!("\nProcessing order:");
    while let Some(task) = scheduler.dequeue().await {
        println!(
            "  Processing: {} (priority: {:?})",
            task.task_name, task.priority
        );
    }
    println!();
}

/// Demo 2: Priority inheritance prevents priority inversion
async fn demo_priority_inheritance() {
    println!("--- Demo 2: Priority Inheritance ---");

    let config = SchedulerConfig::default().resource_aware(false);
    let scheduler = TaskScheduler::new(config);

    // Add a low-priority task that holds a resource
    let low_task = ScheduledTask::new(
        "database-lock".to_string(),
        "Hold database lock".to_string(),
        TaskPriority::Low,
        TaskRequirements::default(),
    );
    scheduler.enqueue(low_task).await.unwrap();
    println!("  Enqueued: Low-priority task holding database lock");

    // A high-priority task needs the same resource
    println!("  High-priority task needs the lock - donating priority...");

    // Donate priority to prevent priority inversion
    scheduler
        .donate_priority("high-task", TaskPriority::High, "database-lock")
        .await
        .unwrap();

    // Check that the low-priority task now has inherited priority
    if let Some(task) = scheduler.dequeue().await {
        println!(
            "  Task '{}' now has effective priority: {}",
            task.task_id,
            task.effective_priority()
        );
        println!("  Base priority: {:?}", task.priority);
        println!("  Inherited: {}", task.has_inherited_priority());
    }
    println!();
}

/// Demo 3: Sliding window rate limiting provides accurate rate control
async fn demo_sliding_window_rate_limiting() {
    println!("--- Demo 3: Sliding Window Rate Limiting ---");

    let limiter = SlidingWindowLimiter::new();

    // Configure: Allow 5 API calls per 2 seconds
    let config = SlidingWindowConfig::new(5, Duration::from_secs(2));
    limiter.set_limit("api_call", config).await;
    println!("  Configured: 5 requests per 2 seconds");

    // Make 5 requests - all should succeed
    println!("\n  Making 5 requests...");
    for i in 1..=5 {
        let allowed = limiter.try_acquire("api_call").await;
        println!(
            "    Request {}: {}",
            i,
            if allowed { "✓ Allowed" } else { "✗ Denied" }
        );
    }

    // 6th request should be denied
    let allowed = limiter.try_acquire("api_call").await;
    println!(
        "    Request 6: {} (rate limit exceeded)",
        if allowed { "✓ Allowed" } else { "✗ Denied" }
    );

    // Check current count
    if let Some(count) = limiter.current_count("api_call").await {
        println!("\n  Current requests in window: {}/5", count);
    }

    // Wait for window to slide
    println!("  Waiting for window to slide (2 seconds)...");
    sleep(Duration::from_secs(2)).await;

    // Should be able to make requests again
    let allowed = limiter.try_acquire("api_call").await;
    println!(
        "  After window expiry: {}",
        if allowed { "✓ Allowed" } else { "✗ Denied" }
    );
    println!();
}

/// Demo 4: Lock-free queues provide high-performance concurrent access
async fn demo_lockfree_queues() {
    println!("--- Demo 4: Lock-Free Queues ---");

    let queue = Arc::new(LockFreeQueue::new());
    println!("  Created lock-free queue");

    // Spawn multiple producers
    println!("\n  Spawning 4 producer threads...");
    let mut handles = vec![];

    for i in 0..4 {
        let q = Arc::clone(&queue);
        handles.push(tokio::spawn(async move {
            for j in 0..5 {
                let task_id = format!("task-{}-{}", i, j);
                q.push(task_id);
            }
            println!("    Producer {} finished", i);
        }));
    }

    // Wait for producers
    for handle in handles {
        handle.await.unwrap();
    }

    println!("\n  Queue size: {}", queue.len());

    // Batch dequeue
    println!("  Batch dequeuing 10 tasks...");
    let batch = queue.pop_batch(10);
    println!("    Dequeued {} tasks", batch.len());

    // Process remaining
    let mut count = 0;
    while queue.pop().is_some() {
        count += 1;
    }
    println!("    Processed {} more tasks individually", count);
    println!("    Queue is now empty: {}", queue.is_empty());
    println!();
}
