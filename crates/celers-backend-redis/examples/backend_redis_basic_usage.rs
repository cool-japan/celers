/// Basic Redis Result Backend Usage Example
///
/// Demonstrates:
/// - Creating a Redis backend
/// - Storing and retrieving task results
/// - Different task states (Pending, Started, Success, Failure)
/// - Batch operations
use celers_backend_redis::{RedisResultBackend, ResultBackend, TaskMeta, TaskResult};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Redis Result Backend: Basic Usage ===\n");

    // Connect to Redis
    let mut backend = RedisResultBackend::new("redis://127.0.0.1:6379")?;
    println!("✓ Connected to Redis\n");

    // Example 1: Store a pending task
    println!("=== Example 1: Pending Task ===");
    let task_id = Uuid::new_v4();
    let mut meta = TaskMeta::new(task_id, "example.add".to_string());
    meta.result = TaskResult::Pending;

    backend.store_result(task_id, &meta).await?;
    println!("✓ Stored pending task: {}", task_id);

    // Retrieve it
    if let Some(retrieved) = backend.get_result(task_id).await? {
        println!("✓ Retrieved task: {}", retrieved);
    }

    // Example 2: Update to started state
    println!("\n=== Example 2: Started Task ===");
    meta.result = TaskResult::Started;
    meta.started_at = Some(chrono::Utc::now());
    meta.worker = Some("worker-1".to_string());

    backend.store_result(task_id, &meta).await?;
    println!("✓ Updated task to STARTED");

    if let Some(retrieved) = backend.get_result(task_id).await? {
        println!("✓ Task status: {:?}", retrieved.result);
    }

    // Example 3: Store success result
    println!("\n=== Example 3: Success Result ===");
    meta.result = TaskResult::Success(serde_json::json!({
        "sum": 42,
        "message": "Task completed successfully"
    }));
    meta.completed_at = Some(chrono::Utc::now());

    backend.store_result(task_id, &meta).await?;
    println!("✓ Stored success result");

    if let Some(retrieved) = backend.get_result(task_id).await? {
        println!("✓ Final result: {}", retrieved);
    }

    // Example 4: Failed task
    println!("\n=== Example 4: Failed Task ===");
    let failed_task_id = Uuid::new_v4();
    let mut failed_meta = TaskMeta::new(failed_task_id, "example.divide".to_string());
    failed_meta.result = TaskResult::Failure("Division by zero".to_string());
    failed_meta.started_at = Some(chrono::Utc::now());
    failed_meta.completed_at = Some(chrono::Utc::now());
    failed_meta.worker = Some("worker-2".to_string());

    backend.store_result(failed_task_id, &failed_meta).await?;
    println!("✓ Stored failed task");

    if let Some(retrieved) = backend.get_result(failed_task_id).await? {
        println!("✓ Error: {:?}", retrieved.result);
    }

    // Example 5: Retry task
    println!("\n=== Example 5: Retry Task ===");
    let retry_task_id = Uuid::new_v4();
    let mut retry_meta = TaskMeta::new(retry_task_id, "example.flaky".to_string());
    retry_meta.result = TaskResult::Retry(2); // Retry attempt 2
    retry_meta.started_at = Some(chrono::Utc::now());
    retry_meta.worker = Some("worker-3".to_string());

    backend.store_result(retry_task_id, &retry_meta).await?;
    println!("✓ Stored retry task");

    if let Some(retrieved) = backend.get_result(retry_task_id).await? {
        println!("✓ Retry state: {:?}", retrieved.result);
    }

    // Example 6: Batch operations
    println!("\n=== Example 6: Batch Operations ===");
    let task_ids: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();
    let results: Vec<(Uuid, TaskMeta)> = task_ids
        .iter()
        .enumerate()
        .map(|(i, &id)| {
            let mut meta = TaskMeta::new(id, format!("batch.task_{}", i));
            meta.result = TaskResult::Success(serde_json::json!({"index": i}));
            meta.completed_at = Some(chrono::Utc::now());
            (id, meta)
        })
        .collect();

    backend.store_results_batch(&results).await?;
    println!("✓ Stored {} tasks in batch", results.len());

    let batch_retrieved = backend.get_results_batch(&task_ids).await?;
    let found_count = batch_retrieved.iter().filter(|r| r.is_some()).count();
    println!("✓ Retrieved {} tasks in batch", found_count);

    // Example 7: Delete result
    println!("\n=== Example 7: Delete Result ===");
    backend.delete_result(task_id).await?;
    println!("✓ Deleted task: {}", task_id);

    // Verify deletion
    match backend.get_result(task_id).await? {
        None => println!("✓ Confirmed task was deleted"),
        Some(_) => println!("⚠ Task still exists"),
    }

    // Batch delete
    backend.delete_results_batch(&task_ids).await?;
    println!("✓ Batch deleted {} tasks", task_ids.len());

    println!("\n✅ All basic operations completed successfully!");
    Ok(())
}
