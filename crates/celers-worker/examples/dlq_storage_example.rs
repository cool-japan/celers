//! Example demonstrating DLQ storage backends
//!
//! This example shows how to use different storage backends for the Dead Letter Queue:
//! - Memory storage (default, non-persistent)
//! - Redis storage (persistent, with TTL support)
//! - PostgreSQL storage (persistent, with advanced querying)

use celers_core::{SerializedTask, TaskMetadata};
use celers_worker::dlq::DlqEntry;
use celers_worker::dlq_storage::{DlqStorage, MemoryDlqStorage};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("=== DLQ Storage Backend Examples ===\n");

    // Example 1: Memory storage (default)
    println!("1. Memory Storage Example");
    memory_storage_example().await?;

    // Example 2: Redis storage (requires redis feature)
    #[cfg(feature = "redis")]
    {
        println!("\n2. Redis Storage Example");
        redis_storage_example().await?;
    }

    // Example 3: PostgreSQL storage (requires postgres feature)
    #[cfg(feature = "postgres")]
    {
        println!("\n3. PostgreSQL Storage Example");
        postgres_storage_example().await?;
    }

    println!("\n=== Examples completed ===");
    Ok(())
}

async fn memory_storage_example() -> Result<(), Box<dyn std::error::Error>> {
    // Create memory storage (non-persistent)
    let storage = MemoryDlqStorage::new();

    // Create a test entry
    let entry = create_test_entry("memory_test_task");

    // Add entry
    storage.add(entry.clone()).await?;
    println!("  Added entry to memory storage");

    // Get entry
    let retrieved = storage.get(&entry.task_id).await?;
    println!("  Retrieved entry: {:?}", retrieved.is_some());

    // Get all entries
    let all_entries = storage.get_all().await?;
    println!("  Total entries: {}", all_entries.len());

    // Get statistics
    let stats = storage.get_stats_by_task_name().await?;
    println!("  Task statistics: {:?}", stats);

    // Health check
    let healthy = storage.health_check().await?;
    println!("  Storage healthy: {}", healthy);

    // Clean up
    let cleared = storage.clear().await?;
    println!("  Cleared {} entries", cleared);

    Ok(())
}

#[cfg(feature = "redis")]
async fn redis_storage_example() -> Result<(), Box<dyn std::error::Error>> {
    use celers_worker::dlq_storage::RedisDlqStorage;

    // Connect to Redis (requires Redis server running)
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());

    match RedisDlqStorage::new(&redis_url, Some("test:dlq".to_string())) {
        Ok(storage) => {
            println!("  Connected to Redis at {}", redis_url);

            // Create a test entry
            let entry = create_test_entry("redis_test_task");

            // Add entry
            storage.add(entry.clone()).await?;
            println!("  Added entry to Redis storage");

            // Get entry
            let retrieved = storage.get(&entry.task_id).await?;
            println!("  Retrieved entry: {:?}", retrieved.is_some());

            // Get all entries
            let all_entries = storage.get_all().await?;
            println!("  Total entries: {}", all_entries.len());

            // Get entries by task name
            let task_entries = storage.get_by_task_name("redis_test_task").await?;
            println!("  Entries for 'redis_test_task': {}", task_entries.len());

            // Get statistics
            let stats = storage.get_stats_by_task_name().await?;
            println!("  Task statistics: {:?}", stats);

            // Health check
            let healthy = storage.health_check().await?;
            println!("  Storage healthy: {}", healthy);

            // Clean up
            let cleared = storage.clear().await?;
            println!("  Cleared {} entries", cleared);
        }
        Err(e) => {
            println!("  Warning: Could not connect to Redis: {}", e);
            println!("  Skipping Redis example. Start Redis server to run this example.");
            println!("  Example: docker run -d -p 6379:6379 redis:latest");
        }
    }

    Ok(())
}

#[cfg(feature = "postgres")]
async fn postgres_storage_example() -> Result<(), Box<dyn std::error::Error>> {
    use celers_worker::dlq_storage::PostgresDlqStorage;

    // Connect to PostgreSQL (requires PostgreSQL server running)
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/celery_test".to_string());

    match PostgresDlqStorage::new(&database_url, Some("test_dlq".to_string())).await {
        Ok(storage) => {
            println!("  Connected to PostgreSQL");

            // Create a test entry
            let entry = create_test_entry("postgres_test_task");

            // Add entry
            storage.add(entry.clone()).await?;
            println!("  Added entry to PostgreSQL storage");

            // Get entry
            let retrieved = storage.get(&entry.task_id).await?;
            println!("  Retrieved entry: {:?}", retrieved.is_some());

            // Get all entries
            let all_entries = storage.get_all().await?;
            println!("  Total entries: {}", all_entries.len());

            // Get entries by task name
            let task_entries = storage.get_by_task_name("postgres_test_task").await?;
            println!("  Entries for 'postgres_test_task': {}", task_entries.len());

            // Get entries older than 1 second (should be empty for new entries)
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            let old_entries = storage.get_older_than(1).await?;
            println!("  Entries older than 1 second: {}", old_entries.len());

            // Get statistics
            let stats = storage.get_stats_by_task_name().await?;
            println!("  Task statistics: {:?}", stats);

            // Health check
            let healthy = storage.health_check().await?;
            println!("  Storage healthy: {}", healthy);

            // Clean up
            let cleared = storage.clear().await?;
            println!("  Cleared {} entries", cleared);
        }
        Err(e) => {
            println!("  Warning: Could not connect to PostgreSQL: {}", e);
            println!("  Skipping PostgreSQL example. Start PostgreSQL server to run this example.");
            println!("  Example: docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:latest");
        }
    }

    Ok(())
}

fn create_test_entry(task_name: &str) -> DlqEntry {
    let task = SerializedTask {
        metadata: TaskMetadata::new(task_name.to_string()),
        payload: vec![1, 2, 3, 4],
    };

    let task_id = task.metadata.id;

    DlqEntry::new(
        task,
        task_id,
        3,
        "Example error for testing".to_string(),
        hostname::get()
            .unwrap_or_else(|_| std::ffi::OsString::from("unknown"))
            .to_string_lossy()
            .to_string(),
    )
    .with_metadata("test", "true")
    .with_metadata("example", "storage_backend")
}
