/// Convenience Methods Example
///
/// Demonstrates the new convenience methods that simplify common operations:
/// - Store with TTL
/// - Batch store with TTL
/// - Wait for result
/// - Mark success/failed/revoked
/// - Get or create
/// - Task status checks
use celers_backend_redis::{batch_size, ttl, RedisResultBackend, ResultBackend, TaskMeta};
use std::time::Duration;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Convenience Methods Example ===\n");

    let mut backend = RedisResultBackend::new("redis://127.0.0.1:6379")?;
    println!("✓ Connected to Redis\n");

    // Example 1: Store with TTL (atomic operation)
    println!("=== Example 1: Store with TTL ===");
    let task_id = Uuid::new_v4();
    let meta = TaskMeta::new(task_id, "example.task".to_string());

    // Old way: two operations
    // backend.store_result(task_id, &meta).await?;
    // backend.set_expiration(task_id, ttl::LONG).await?;

    // New way: atomic single operation
    backend
        .store_result_with_ttl(task_id, &meta, ttl::LONG)
        .await?;
    println!("✓ Stored task with TTL in one operation\n");

    // Example 2: Batch store with TTL
    println!("=== Example 2: Batch Store with TTL ===");
    let tasks: Vec<(Uuid, TaskMeta)> = (0..batch_size::SMALL)
        .map(|i| {
            let id = Uuid::new_v4();
            let meta = TaskMeta::new(id, format!("batch.task_{}", i));
            (id, meta)
        })
        .collect();

    // Store all tasks with same TTL
    backend.store_results_with_ttl(&tasks, ttl::MEDIUM).await?;
    println!("✓ Stored {} tasks with TTL in batch\n", batch_size::SMALL);

    // Example 3: Mark task states (convenience methods)
    println!("=== Example 3: Mark Task States ===");

    // Mark as started first
    let work_task_id = Uuid::new_v4();
    let work_meta = TaskMeta::new(work_task_id, "work.process".to_string());
    backend
        .store_result_with_ttl(work_task_id, &work_meta, ttl::LONG)
        .await?;
    backend
        .mark_started(work_task_id, Some("worker-1".to_string()))
        .await?;
    println!("✓ Marked task as started");

    // Mark as success
    backend
        .mark_success(work_task_id, serde_json::json!({"processed": 100}))
        .await?;
    println!("✓ Marked task as successful");

    // Mark failed task
    let failed_task_id = Uuid::new_v4();
    let failed_meta = TaskMeta::new(failed_task_id, "work.risky".to_string());
    backend
        .store_result_with_ttl(failed_task_id, &failed_meta, ttl::LONG)
        .await?;
    backend
        .mark_failed(failed_task_id, "Connection timeout".to_string())
        .await?;
    println!("✓ Marked task as failed");

    // Mark revoked task
    let revoked_task_id = Uuid::new_v4();
    let revoked_meta = TaskMeta::new(revoked_task_id, "work.cancelled".to_string());
    backend
        .store_result_with_ttl(revoked_task_id, &revoked_meta, ttl::LONG)
        .await?;
    backend.mark_revoked(revoked_task_id).await?;
    println!("✓ Marked task as revoked\n");

    // Example 4: Check task completion status
    println!("=== Example 4: Check Task Completion ===");
    if backend.is_task_complete(work_task_id).await? {
        println!("✓ Work task is complete");
    }

    if backend.is_task_complete(failed_task_id).await? {
        println!("✓ Failed task is complete (terminal state)");
    }

    if backend.is_task_complete(revoked_task_id).await? {
        println!("✓ Revoked task is complete (terminal state)\n");
    }

    // Example 5: Get task age
    println!("=== Example 5: Get Task Age ===");
    if let Some(age) = backend.get_task_age(work_task_id).await? {
        println!("✓ Work task age: {} milliseconds\n", age.num_milliseconds());
    }

    // Example 6: Get or create
    println!("=== Example 6: Get or Create ===");
    let get_or_create_id = Uuid::new_v4();

    // First call: creates the task
    let meta1 = backend
        .get_or_create(
            get_or_create_id,
            TaskMeta::new(get_or_create_id, "getorcreate.task".to_string()),
        )
        .await?;
    println!("✓ Created new task: {}", meta1.task_name);

    // Second call: returns existing task
    let meta2 = backend
        .get_or_create(
            get_or_create_id,
            TaskMeta::new(get_or_create_id, "different.name".to_string()),
        )
        .await?;
    println!("✓ Got existing task: {} (not changed)\n", meta2.task_name);

    // Example 7: Wait for result with timeout
    println!("=== Example 7: Wait for Result ===");
    let async_task_id = Uuid::new_v4();
    let async_meta = TaskMeta::new(async_task_id, "async.task".to_string());
    backend
        .store_result_with_ttl(async_task_id, &async_meta, ttl::LONG)
        .await?;

    // Simulate async task completing in background
    let mut backend_clone = backend.clone();
    let task_id_clone = async_task_id;
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let _ = backend_clone
            .mark_success(task_id_clone, serde_json::json!({"done": true}))
            .await;
    });

    // Wait for task to complete (with timeout)
    match backend
        .wait_for_result(
            async_task_id,
            Duration::from_secs(5),
            Duration::from_millis(100),
        )
        .await?
    {
        Some(completed) => {
            println!("✓ Task completed: {:?}", completed.result);
        }
        None => {
            println!("✗ Task timed out");
        }
    }

    // Example 8: Bulk TTL updates
    println!("\n=== Example 8: Bulk TTL Updates ===");
    let task_ids: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();

    // Create tasks
    for (i, &id) in task_ids.iter().enumerate() {
        let meta = TaskMeta::new(id, format!("bulk.task_{}", i));
        backend.store_result(id, &meta).await?;
    }

    // Set TTL for all at once (pipelined)
    backend
        .set_multiple_expirations(&task_ids, ttl::FAILURE)
        .await?;
    println!("✓ Set TTL for {} tasks in batch\n", task_ids.len());

    // Cleanup
    println!("=== Cleanup ===");
    backend.delete_result(task_id).await?;
    backend.delete_result(work_task_id).await?;
    backend.delete_result(failed_task_id).await?;
    backend.delete_result(revoked_task_id).await?;
    backend.delete_result(get_or_create_id).await?;
    backend.delete_result(async_task_id).await?;

    for id in task_ids {
        backend.delete_result(id).await?;
    }
    println!("✓ Cleaned up test data\n");

    println!("=== Summary ===");
    println!("Convenience methods demonstrated:");
    println!("  ✓ store_result_with_ttl() - Atomic store + TTL");
    println!("  ✓ store_results_with_ttl() - Batch store + TTL");
    println!("  ✓ mark_success() - Mark task successful");
    println!("  ✓ mark_failed() - Mark task failed");
    println!("  ✓ mark_revoked() - Mark task cancelled");
    println!("  ✓ is_task_complete() - Check completion status");
    println!("  ✓ get_task_age() - Get task age");
    println!("  ✓ get_or_create() - Idempotent create");
    println!("  ✓ wait_for_result() - Poll with timeout");
    println!("  ✓ set_multiple_expirations() - Bulk TTL updates");

    Ok(())
}
