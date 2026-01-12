//! Task Lifecycle Hooks Example
//!
//! Demonstrates various lifecycle hook patterns for extending
//! task processing behavior without modifying the broker.
//!
//! This example shows:
//! - Validation hooks to reject invalid tasks
//! - Logging hooks for observability
//! - Metrics collection hooks
//! - Enrichment hooks to add metadata
//! - Authorization hooks
//! - Integration hooks for external systems
//!
//! Run with:
//! ```bash
//! cargo run --example lifecycle_hooks
//! ```

use celers_broker_sql::{MysqlBroker, TaskHook};
use celers_core::{Broker, SerializedTask};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing subscriber for logging
    tracing_subscriber::fmt::init();

    println!("=== Task Lifecycle Hooks Example ===\n");

    // Connect to MySQL
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://root:password@localhost/celers".to_string());

    let broker = MysqlBroker::new(&database_url).await?;
    broker.migrate().await?;

    println!("✓ Connected to MySQL and ran migrations\n");

    // Demo 1: Validation hooks
    demo_validation_hooks(&broker).await?;

    // Demo 2: Logging hooks
    demo_logging_hooks(&broker).await?;

    // Demo 3: Metrics collection hooks
    demo_metrics_hooks(&broker).await?;

    // Demo 4: Task enrichment hooks
    demo_enrichment_hooks(&broker).await?;

    // Demo 5: Multiple hooks execution
    demo_multiple_hooks(&broker).await?;

    // Demo 6: Hook clearing
    demo_clear_hooks(&broker).await?;

    println!("\n=== All Demos Completed Successfully ===");
    Ok(())
}

/// Demo 1: Validation hooks to reject invalid tasks
async fn demo_validation_hooks(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    println!("Demo 1: Validation Hooks");
    println!("------------------------");

    // Add validation hook: reject tasks with empty payload
    broker
        .add_hook(TaskHook::BeforeEnqueue(Arc::new(|_ctx, task| {
            let is_empty = task.payload.is_empty();
            Box::pin(async move {
                if is_empty {
                    return Err(celers_core::CelersError::Other(
                        "Validation failed: Task payload cannot be empty".to_string(),
                    ));
                }
                Ok(())
            })
        })))
        .await;

    println!("✓ Added validation hook");

    // Try to enqueue valid task
    let valid_task = SerializedTask::new("valid_task".to_string(), vec![1, 2, 3]);
    match broker.enqueue(valid_task).await {
        Ok(task_id) => println!("✓ Valid task accepted: {}", task_id),
        Err(e) => println!("✗ Valid task rejected: {}", e),
    }

    // Try to enqueue invalid task (empty payload)
    let invalid_task = SerializedTask::new("invalid_task".to_string(), vec![]);
    match broker.enqueue(invalid_task).await {
        Ok(task_id) => println!(
            "✗ Invalid task accepted (should have been rejected): {}",
            task_id
        ),
        Err(e) => println!("✓ Invalid task rejected as expected: {}", e),
    }

    broker.clear_hooks().await;
    println!();
    Ok(())
}

/// Demo 2: Logging hooks for observability
async fn demo_logging_hooks(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    println!("Demo 2: Logging Hooks");
    println!("---------------------");

    // Add logging hook for enqueue
    broker
        .add_hook(TaskHook::BeforeEnqueue(Arc::new(|ctx, task| {
            let task_name = task.metadata.name.clone();
            let queue_name = ctx.queue_name.clone();
            Box::pin(async move {
                println!(
                    "  [LOG] Enqueueing task '{}' to queue '{}'",
                    task_name, queue_name
                );
                Ok(())
            })
        })))
        .await;

    broker
        .add_hook(TaskHook::AfterEnqueue(Arc::new(|ctx, task| {
            let task_name = task.metadata.name.clone();
            let task_id = ctx.task_id;
            Box::pin(async move {
                println!(
                    "  [LOG] Task '{}' enqueued successfully: {:?}",
                    task_name, task_id
                );
                Ok(())
            })
        })))
        .await;

    println!("✓ Added logging hooks");

    // Enqueue tasks
    let task1 = SerializedTask::new("send_email".to_string(), b"email_data".to_vec());
    broker.enqueue(task1).await?;

    let task2 = SerializedTask::new("process_image".to_string(), b"image_data".to_vec());
    broker.enqueue(task2).await?;

    broker.clear_hooks().await;
    println!();
    Ok(())
}

/// Demo 3: Metrics collection hooks
async fn demo_metrics_hooks(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    println!("Demo 3: Metrics Collection Hooks");
    println!("---------------------------------");

    // Shared metrics counters
    let enqueue_counter = Arc::new(AtomicU64::new(0));
    let success_counter = Arc::new(AtomicU64::new(0));

    // Add metrics collection hooks
    let enqueue_counter_clone = enqueue_counter.clone();
    broker
        .add_hook(TaskHook::BeforeEnqueue(Arc::new(move |_ctx, _task| {
            let counter = enqueue_counter_clone.clone();
            Box::pin(async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        })))
        .await;

    let success_counter_clone = success_counter.clone();
    broker
        .add_hook(TaskHook::AfterEnqueue(Arc::new(move |_ctx, _task| {
            let counter = success_counter_clone.clone();
            Box::pin(async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
        })))
        .await;

    println!("✓ Added metrics collection hooks");

    // Enqueue multiple tasks
    for i in 1..=5 {
        let task = SerializedTask::new(format!("task_{}", i), vec![i as u8]);
        broker.enqueue(task).await?;
    }

    println!("\nMetrics collected:");
    println!(
        "  Tasks enqueue attempts: {}",
        enqueue_counter.load(Ordering::SeqCst)
    );
    println!(
        "  Tasks enqueued successfully: {}",
        success_counter.load(Ordering::SeqCst)
    );

    broker.clear_hooks().await;
    println!();
    Ok(())
}

/// Demo 4: Task enrichment hooks
async fn demo_enrichment_hooks(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    println!("Demo 4: Task Enrichment Hooks");
    println!("------------------------------");

    // Add enrichment hook: add timestamp and source
    broker
        .add_hook(TaskHook::BeforeEnqueue(Arc::new(|ctx, _task| {
            let timestamp = ctx.timestamp;
            Box::pin(async move {
                println!("  [ENRICH] Adding metadata: timestamp={}", timestamp);
                // In a real scenario, you could modify task metadata here
                // For this demo, we just log the enrichment
                Ok(())
            })
        })))
        .await;

    println!("✓ Added enrichment hook");

    // Enqueue task
    let task = SerializedTask::new("enriched_task".to_string(), b"data".to_vec());
    let task_id = broker.enqueue(task).await?;
    println!("✓ Task enqueued with enrichment: {}", task_id);

    broker.clear_hooks().await;
    println!();
    Ok(())
}

/// Demo 5: Multiple hooks execution order
async fn demo_multiple_hooks(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    println!("Demo 5: Multiple Hooks Execution Order");
    println!("---------------------------------------");

    // Add multiple hooks - they execute in registration order
    broker
        .add_hook(TaskHook::BeforeEnqueue(Arc::new(|_ctx, task| {
            let task_name = task.metadata.name.clone();
            Box::pin(async move {
                println!("  [HOOK 1] First hook: {}", task_name);
                Ok(())
            })
        })))
        .await;

    broker
        .add_hook(TaskHook::BeforeEnqueue(Arc::new(|_ctx, task| {
            let task_name = task.metadata.name.clone();
            Box::pin(async move {
                println!("  [HOOK 2] Second hook: {}", task_name);
                Ok(())
            })
        })))
        .await;

    broker
        .add_hook(TaskHook::BeforeEnqueue(Arc::new(|_ctx, task| {
            let task_name = task.metadata.name.clone();
            Box::pin(async move {
                println!("  [HOOK 3] Third hook: {}", task_name);
                Ok(())
            })
        })))
        .await;

    println!("✓ Added 3 BeforeEnqueue hooks");
    println!("\nEnqueueing task to trigger hooks:");

    let task = SerializedTask::new("multi_hook_task".to_string(), vec![1, 2, 3]);
    broker.enqueue(task).await?;

    println!("✓ All hooks executed in order");

    broker.clear_hooks().await;
    println!();
    Ok(())
}

/// Demo 6: Clearing hooks
async fn demo_clear_hooks(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    println!("Demo 6: Clear Hooks");
    println!("-------------------");

    // Add a hook
    broker
        .add_hook(TaskHook::BeforeEnqueue(Arc::new(|_ctx, _task| {
            Box::pin(async move {
                println!("  [HOOK] This hook will be cleared");
                Ok(())
            })
        })))
        .await;

    println!("✓ Added hook");

    // Enqueue task (hook will execute)
    let task1 = SerializedTask::new("task_before_clear".to_string(), vec![1]);
    println!("\nEnqueueing task before clearing hooks:");
    broker.enqueue(task1).await?;

    // Clear all hooks
    broker.clear_hooks().await;
    println!("\n✓ Cleared all hooks");

    // Enqueue task (no hook will execute)
    let task2 = SerializedTask::new("task_after_clear".to_string(), vec![2]);
    println!("\nEnqueueing task after clearing hooks:");
    broker.enqueue(task2).await?;
    println!("  (No hook output - hooks were cleared)");

    println!();
    Ok(())
}

/// Example: Production-ready hook patterns
#[allow(dead_code)]
async fn production_hook_patterns() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Production Hook Patterns ===\n");

    let broker = MysqlBroker::new("mysql://localhost/celers").await?;

    // Pattern 1: Authorization hook
    println!("Pattern 1: Authorization Hook");
    broker
        .add_hook(TaskHook::BeforeEnqueue(Arc::new(|_ctx, task| {
            let task_name = task.metadata.name.clone();
            Box::pin(async move {
                // Check if task is authorized
                if task_name.starts_with("admin_") {
                    // In production, check user permissions here
                    return Err(celers_core::CelersError::Other(
                        "Unauthorized: admin tasks require special permissions".to_string(),
                    ));
                }
                Ok(())
            })
        })))
        .await;

    // Pattern 2: Rate limiting hook
    println!("Pattern 2: Rate Limiting Hook");
    let rate_limiter = Arc::new(AtomicU64::new(0));
    let rate_limiter_clone = rate_limiter.clone();
    broker
        .add_hook(TaskHook::BeforeEnqueue(Arc::new(move |_ctx, _task| {
            let limiter = rate_limiter_clone.clone();
            Box::pin(async move {
                let current = limiter.fetch_add(1, Ordering::SeqCst);
                if current > 100 {
                    return Err(celers_core::CelersError::Other(
                        "Rate limit exceeded: max 100 tasks per window".to_string(),
                    ));
                }
                Ok(())
            })
        })))
        .await;

    // Pattern 3: External system integration hook
    println!("Pattern 3: External Integration Hook");
    broker
        .add_hook(TaskHook::AfterEnqueue(Arc::new(|ctx, task| {
            let task_id = ctx.task_id;
            let task_name = task.metadata.name.clone();
            Box::pin(async move {
                // Send webhook notification
                println!(
                    "  [WEBHOOK] Notifying external system: task={}, id={:?}",
                    task_name, task_id
                );
                // In production: reqwest::post(...).await?
                Ok(())
            })
        })))
        .await;

    // Pattern 4: Audit logging hook
    println!("Pattern 4: Audit Logging Hook");
    broker
        .add_hook(TaskHook::BeforeEnqueue(Arc::new(|ctx, task| {
            let queue_name = ctx.queue_name.clone();
            let task_name = task.metadata.name.clone();
            let timestamp = ctx.timestamp;
            Box::pin(async move {
                println!(
                    "  [AUDIT] Task enqueue: queue={}, task={}, timestamp={}",
                    queue_name, task_name, timestamp
                );
                // In production: write to audit database
                Ok(())
            })
        })))
        .await;

    // Pattern 5: Error aggregation hook
    println!("Pattern 5: Error Aggregation Hook");
    broker
        .add_hook(TaskHook::BeforeEnqueue(Arc::new(|_ctx, task| {
            let task_name = task.metadata.name.clone();
            Box::pin(async move {
                // Track errors for specific task types
                if task_name == "flaky_task" {
                    println!(
                        "  [ERROR_TRACKING] Known flaky task detected: {}",
                        task_name
                    );
                    // In production: increment error counter in metrics system
                }
                Ok(())
            })
        })))
        .await;

    println!("\n✓ All production hook patterns configured");
    Ok(())
}
