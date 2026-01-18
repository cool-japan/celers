//! Task Group Operations Example
//!
//! This example demonstrates how to use task groups for batch task tracking
//! and monitoring related tasks as a single unit.
//!
//! Features demonstrated:
//! - Enqueueing related tasks as a group
//! - Tracking group status and progress
//! - Monitoring group completion
//! - Group-based workflows and batch processing
//!
//! Run with:
//! ```bash
//! cargo run --example task_groups
//! ```

use celers_broker_sql::MysqlBroker;
use celers_core::{Broker, SerializedTask};
use serde_json::json;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("=== Task Group Operations Example ===\n");

    // Database connection
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://root:password@localhost/celers".to_string());

    println!("Connecting to database...");
    let broker = MysqlBroker::new(&database_url).await?;
    println!("Connected successfully!\n");

    // Example 1: Basic task group creation
    println!("--- Example 1: Basic Task Group ---");

    let tasks = vec![
        SerializedTask::new("process_image_1".to_string(), vec![]).with_priority(1),
        SerializedTask::new("process_image_2".to_string(), vec![]).with_priority(1),
        SerializedTask::new("process_image_3".to_string(), vec![]).with_priority(1),
    ];

    let group_id = "image-batch-001";
    let metadata = json!({
        "batch_type": "image_processing",
        "user_id": "user123",
        "created_by": "api_server_1"
    });

    println!(
        "Creating task group '{}' with {} tasks...",
        group_id,
        tasks.len()
    );
    let task_ids = broker
        .enqueue_group(group_id, tasks, Some(metadata))
        .await?;

    println!("✓ Group created with {} tasks", task_ids.len());
    for (i, task_id) in task_ids.iter().enumerate() {
        println!("  Task {}: {}", i + 1, task_id);
    }

    // Check group status
    let status = broker.get_group_status(group_id).await?;
    println!("\nGroup Status:");
    println!("  Total tasks: {}", status.total_tasks);
    println!("  Pending: {}", status.pending_tasks);
    println!("  Processing: {}", status.processing_tasks);
    println!("  Completed: {}", status.completed_tasks);
    println!("  Failed: {}", status.failed_tasks);

    // Example 2: Data processing pipeline
    println!("\n--- Example 2: Data Processing Pipeline ---");

    let pipeline_group = "data-pipeline-20260118";

    // Create tasks for each stage of the pipeline
    let pipeline_tasks = vec![
        SerializedTask::new("extract_data".to_string(), vec![])
            .with_priority(3)
            .with_max_retries(3),
        SerializedTask::new("transform_data".to_string(), vec![])
            .with_priority(2)
            .with_max_retries(3),
        SerializedTask::new("validate_data".to_string(), vec![])
            .with_priority(2)
            .with_max_retries(2),
        SerializedTask::new("load_data".to_string(), vec![])
            .with_priority(1)
            .with_max_retries(3),
    ];

    let pipeline_metadata = json!({
        "pipeline": "etl",
        "source": "database_a",
        "destination": "warehouse_b",
        "scheduled_by": "cron_job",
        "timestamp": chrono::Utc::now()
    });

    println!("Creating ETL pipeline group '{}'...", pipeline_group);
    let pipeline_task_ids = broker
        .enqueue_group(pipeline_group, pipeline_tasks, Some(pipeline_metadata))
        .await?;

    println!("✓ Pipeline created with {} stages", pipeline_task_ids.len());

    // Monitor pipeline progress
    let status = broker.get_group_status(pipeline_group).await?;
    println!("\nPipeline Status:");
    print_progress_bar(
        "Progress",
        status.completed_tasks as usize,
        status.total_tasks as usize,
    );

    // Example 3: Batch document processing
    println!("\n--- Example 3: Batch Document Processing ---");

    let document_group = "document-batch-789";
    let num_documents = 50;

    println!("Processing {} documents in batch...", num_documents);

    let document_tasks: Vec<SerializedTask> = (1..=num_documents)
        .map(|i| {
            SerializedTask::new(format!("process_document_{}", i), vec![])
                .with_priority(1)
                .with_max_retries(2)
        })
        .collect();

    let doc_metadata = json!({
        "batch_type": "document_processing",
        "document_count": num_documents,
        "category": "invoices",
        "priority": "normal"
    });

    let start = std::time::Instant::now();
    let task_ids = broker
        .enqueue_group(document_group, document_tasks, Some(doc_metadata))
        .await?;
    let duration = start.elapsed();

    println!("✓ Enqueued {} documents in {:?}", task_ids.len(), duration);
    println!(
        "  Throughput: {:.0} tasks/second",
        task_ids.len() as f64 / duration.as_secs_f64()
    );

    // Example 4: Monitoring group progress
    println!("\n--- Example 4: Group Progress Monitoring ---");

    let monitoring_group = "monitor-demo";

    // Create tasks
    let tasks = vec![
        SerializedTask::new("task_1".to_string(), vec![]),
        SerializedTask::new("task_2".to_string(), vec![]),
        SerializedTask::new("task_3".to_string(), vec![]),
        SerializedTask::new("task_4".to_string(), vec![]),
        SerializedTask::new("task_5".to_string(), vec![]),
    ];

    broker.enqueue_group(monitoring_group, tasks, None).await?;

    println!("Monitoring group progress (simulated):\n");

    // Simulate processing tasks and monitoring
    for _ in 0..6 {
        let status = broker.get_group_status(monitoring_group).await?;

        println!("Group: {}", monitoring_group);
        println!("├─ Total:      {}", status.total_tasks);
        println!("├─ Pending:    {} 📋", status.pending_tasks);
        println!("├─ Processing: {} ⚙️", status.processing_tasks);
        println!("├─ Completed:  {} ✅", status.completed_tasks);
        println!("├─ Failed:     {} ❌", status.failed_tasks);
        println!(
            "└─ Progress:   {}%",
            (status.completed_tasks * 100) / status.total_tasks.max(1)
        );

        print_progress_bar(
            "Status",
            status.completed_tasks as usize,
            status.total_tasks as usize,
        );

        // Simulate processing a task
        if let Some(msg) = broker.dequeue().await? {
            if msg.task.metadata.name.starts_with("task_") {
                sleep(Duration::from_millis(500)).await;
                broker
                    .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
                    .await?;
            }
        }

        println!();
        sleep(Duration::from_millis(300)).await;
    }

    // Example 5: Multiple concurrent groups
    println!("--- Example 5: Multiple Concurrent Groups ---");

    let groups = vec![
        ("email-campaign-001", 10, "Email Campaign"),
        ("report-generation-002", 5, "Report Generation"),
        ("data-sync-003", 15, "Data Synchronization"),
    ];

    println!("Creating multiple task groups:\n");

    for (group_id, task_count, description) in &groups {
        let tasks: Vec<SerializedTask> = (1..=*task_count)
            .map(|i| SerializedTask::new(format!("{}_{}", group_id, i), vec![]))
            .collect();

        let metadata = json!({
            "description": description,
            "task_count": task_count
        });

        broker
            .enqueue_group(group_id, tasks, Some(metadata))
            .await?;
        println!("✓ Created '{}' with {} tasks", group_id, task_count);
    }

    println!("\nGroup Overview:");
    println!("┌────────────────────────────┬───────┬─────────┬────────────┬───────────┐");
    println!("│ Group ID                   │ Total │ Pending │ Processing │ Completed │");
    println!("├────────────────────────────┼───────┼─────────┼────────────┼───────────┤");

    for (group_id, _, _) in &groups {
        let status = broker.get_group_status(group_id).await?;
        println!(
            "│ {:<26} │ {:>5} │ {:>7} │ {:>10} │ {:>9} │",
            group_id,
            status.total_tasks,
            status.pending_tasks,
            status.processing_tasks,
            status.completed_tasks
        );
    }
    println!("└────────────────────────────┴───────┴─────────┴────────────┴───────────┘");

    // Example 6: Group-based reporting
    println!("\n--- Example 6: Group-Based Reporting ---");

    let report_group = "monthly-report-jan-2026";

    let report_tasks = vec![
        SerializedTask::new("generate_sales_report".to_string(), vec![]),
        SerializedTask::new("generate_inventory_report".to_string(), vec![]),
        SerializedTask::new("generate_financial_report".to_string(), vec![]),
        SerializedTask::new("generate_customer_report".to_string(), vec![]),
        SerializedTask::new("consolidate_reports".to_string(), vec![]),
    ];

    let report_metadata = json!({
        "report_type": "monthly_summary",
        "period": "2026-01",
        "requester": "management",
        "priority": "high"
    });

    broker
        .enqueue_group(report_group, report_tasks, Some(report_metadata))
        .await?;

    let status = broker.get_group_status(report_group).await?;

    println!("Report Generation Summary:");
    println!("  Group: {}", report_group);
    println!("  Total Reports: {}", status.total_tasks);
    println!("  Status Breakdown:");
    println!("    ├─ Queued:     {}", status.pending_tasks);
    println!("    ├─ In Progress: {}", status.processing_tasks);
    println!("    ├─ Completed:   {}", status.completed_tasks);
    println!("    └─ Failed:      {}", status.failed_tasks);

    let completion_percentage = (status.completed_tasks * 100) / status.total_tasks.max(1);
    println!("  Overall Progress: {}%", completion_percentage);

    println!("\n=== Example Complete ===");
    println!("\nBest Practices:");
    println!("  - Use meaningful group IDs (e.g., timestamp, batch ID)");
    println!("  - Include metadata for tracking and debugging");
    println!("  - Monitor group status for batch job completion");
    println!("  - Use groups for related tasks that form a logical unit");
    println!("  - Track group progress for user-facing batch operations");
    println!("  - Consider group-based retry and cancellation strategies");

    Ok(())
}

fn print_progress_bar(label: &str, current: usize, total: usize) {
    let width = 40;
    let filled = if total > 0 {
        (current * width) / total
    } else {
        0
    };
    let empty = width - filled;

    print!("  {}: [", label);
    for _ in 0..filled {
        print!("█");
    }
    for _ in 0..empty {
        print!("░");
    }
    println!("] {}/{}", current, total);
}
