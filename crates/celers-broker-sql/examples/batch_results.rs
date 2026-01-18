//! Batch Result Operations Example
//!
//! This example demonstrates how to use batch result operations for efficient
//! storage and retrieval of task results in high-throughput scenarios.
//!
//! Features demonstrated:
//! - Storing multiple task results in a single transaction
//! - Retrieving multiple task results efficiently
//! - Performance comparison: batch vs individual operations
//!
//! Run with:
//! ```bash
//! cargo run --example batch_results
//! ```

use celers_broker_sql::{BatchResultInput, MysqlBroker, TaskResultStatus};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("=== Batch Result Operations Example ===\n");

    // Database connection
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://root:password@localhost/celers".to_string());

    println!("Connecting to database...");
    let broker = MysqlBroker::new(&database_url).await?;
    println!("Connected successfully!\n");

    // Example 1: Store multiple results in a batch
    println!("--- Example 1: Batch Result Storage ---");
    let task_ids: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();

    let batch_results = vec![
        BatchResultInput {
            task_id: task_ids[0],
            task_name: "data_processing".to_string(),
            status: TaskResultStatus::Success,
            result: Some(serde_json::json!({"records_processed": 1000, "duration_ms": 150})),
            error: None,
            traceback: None,
            runtime_ms: Some(150),
        },
        BatchResultInput {
            task_id: task_ids[1],
            task_name: "email_sending".to_string(),
            status: TaskResultStatus::Success,
            result: Some(serde_json::json!({"emails_sent": 50})),
            error: None,
            traceback: None,
            runtime_ms: Some(200),
        },
        BatchResultInput {
            task_id: task_ids[2],
            task_name: "report_generation".to_string(),
            status: TaskResultStatus::Failure,
            result: None,
            error: Some("Template not found".to_string()),
            traceback: Some("File: report.rs:42".to_string()),
            runtime_ms: Some(50),
        },
        BatchResultInput {
            task_id: task_ids[3],
            task_name: "data_export".to_string(),
            status: TaskResultStatus::Success,
            result: Some(serde_json::json!({"exported_rows": 5000, "file_size_mb": 12.5})),
            error: None,
            traceback: None,
            runtime_ms: Some(800),
        },
        BatchResultInput {
            task_id: task_ids[4],
            task_name: "cache_warming".to_string(),
            status: TaskResultStatus::Success,
            result: Some(serde_json::json!({"cache_entries": 250})),
            error: None,
            traceback: None,
            runtime_ms: Some(100),
        },
    ];

    let start = std::time::Instant::now();
    let stored = broker.store_result_batch(&batch_results).await?;
    let duration = start.elapsed();

    println!("Stored {} results in {:?}", stored, duration);
    println!("Average time per result: {:?}\n", duration / stored as u32);

    // Example 2: Retrieve multiple results in a batch
    println!("--- Example 2: Batch Result Retrieval ---");
    let start = std::time::Instant::now();
    let results = broker.get_result_batch(&task_ids).await?;
    let duration = start.elapsed();

    println!("Retrieved {} results in {:?}", results.len(), duration);
    println!(
        "Average time per result: {:?}\n",
        duration / results.len() as u32
    );

    for result in &results {
        println!("Task: {}", result.task_name);
        println!("  Status: {}", result.status);
        if let Some(result_data) = &result.result {
            println!("  Result: {}", result_data);
        }
        if let Some(error) = &result.error {
            println!("  Error: {}", error);
        }
        if let Some(runtime) = result.runtime_ms {
            println!("  Runtime: {}ms", runtime);
        }
        println!();
    }

    // Example 3: Performance comparison - batch vs individual
    println!("--- Example 3: Performance Comparison ---");
    let test_task_ids: Vec<Uuid> = (0..10).map(|_| Uuid::new_v4()).collect();

    // Individual operations
    println!("Individual operations:");
    let individual_results: Vec<BatchResultInput> = test_task_ids
        .iter()
        .map(|&task_id| BatchResultInput {
            task_id,
            task_name: "test_task".to_string(),
            status: TaskResultStatus::Success,
            result: Some(serde_json::json!({"value": 42})),
            error: None,
            traceback: None,
            runtime_ms: Some(100),
        })
        .collect();

    let start = std::time::Instant::now();
    for result in &individual_results {
        broker
            .store_result(
                &result.task_id,
                &result.task_name,
                result.status.clone(),
                result.result.clone(),
                result.error.clone().as_deref(),
                result.traceback.clone().as_deref(),
                result.runtime_ms,
            )
            .await?;
    }
    let individual_duration = start.elapsed();
    println!(
        "  Time: {:?} ({:?} per result)",
        individual_duration,
        individual_duration / 10
    );

    // Batch operations
    println!("Batch operations:");
    let start = std::time::Instant::now();
    broker.store_result_batch(&individual_results).await?;
    let batch_duration = start.elapsed();
    println!(
        "  Time: {:?} ({:?} per result)",
        batch_duration,
        batch_duration / 10
    );

    let speedup = individual_duration.as_micros() as f64 / batch_duration.as_micros() as f64;
    println!("\nBatch operations are {:.2}x faster!", speedup);

    // Example 4: Update existing results in batch
    println!("\n--- Example 4: Batch Result Updates ---");
    let updated_results = vec![
        BatchResultInput {
            task_id: task_ids[0],
            task_name: "data_processing".to_string(),
            status: TaskResultStatus::Success,
            result: Some(serde_json::json!({"records_processed": 2000, "duration_ms": 180})),
            error: None,
            traceback: None,
            runtime_ms: Some(180),
        },
        BatchResultInput {
            task_id: task_ids[2],
            task_name: "report_generation".to_string(),
            status: TaskResultStatus::Retry,
            result: None,
            error: Some("Retrying with fallback template".to_string()),
            traceback: None,
            runtime_ms: Some(75),
        },
    ];

    let updated = broker.store_result_batch(&updated_results).await?;
    println!("Updated {} results", updated);

    // Verify updates
    let updated_task_ids = vec![task_ids[0], task_ids[2]];
    let verified = broker.get_result_batch(&updated_task_ids).await?;

    for result in verified {
        println!("\nTask: {}", result.task_name);
        println!("  Status: {}", result.status);
        if let Some(result_data) = &result.result {
            println!("  Result: {}", result_data);
        }
        if let Some(runtime) = result.runtime_ms {
            println!("  Runtime: {}ms", runtime);
        }
    }

    // Example 5: Handling large batches
    println!("\n--- Example 5: Large Batch Processing ---");
    let large_batch_size = 100;
    let large_batch: Vec<BatchResultInput> = (0..large_batch_size)
        .map(|i| BatchResultInput {
            task_id: Uuid::new_v4(),
            task_name: format!("batch_task_{}", i),
            status: TaskResultStatus::Success,
            result: Some(serde_json::json!({"iteration": i})),
            error: None,
            traceback: None,
            runtime_ms: Some(50 + i as i64),
        })
        .collect();

    let start = std::time::Instant::now();
    let stored = broker.store_result_batch(&large_batch).await?;
    let duration = start.elapsed();

    println!("Stored {} results in {:?}", stored, duration);
    println!(
        "Throughput: {:.0} results/second",
        stored as f64 / duration.as_secs_f64()
    );

    println!("\n=== Example Complete ===");
    Ok(())
}
