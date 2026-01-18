//! Bulk Import/Export Example
//!
//! Demonstrates data migration and backup utilities using JSON format.
//! This is useful for:
//! - Backing up tasks before maintenance
//! - Migrating tasks between environments
//! - Analyzing DLQ entries offline
//! - Disaster recovery scenarios

use celers_broker_sql::{DbTaskState, MysqlBroker, PoolConfig};
use celers_core::{Broker, SerializedTask};
use std::fs;
use std::path::PathBuf;

fn temp_dir() -> PathBuf {
    std::env::temp_dir()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("=== Bulk Import/Export Example ===\n");

    // Database URL from environment or default
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://root:password@localhost/celers_dev".to_string());

    println!("Connecting to MySQL: {}", database_url);

    // Configure connection pool
    let pool_config = PoolConfig {
        max_connections: 10,
        min_connections: 2,
        acquire_timeout_secs: 5,
        max_lifetime_secs: Some(1800),
        idle_timeout_secs: Some(600),
    };

    // Create broker
    let broker = MysqlBroker::with_config(&database_url, "import_export_demo", pool_config).await?;

    println!("Running migrations...");
    broker.migrate().await?;

    // Clean up any existing tasks
    println!("Cleaning up existing tasks...");
    broker.purge_all().await?;

    // Example 1: Create sample tasks for export
    println!("\n=== Example 1: Creating Sample Tasks ===");
    create_sample_tasks(&broker).await?;

    // Example 2: Export all tasks
    println!("\n=== Example 2: Export All Tasks ===");
    export_all_tasks(&broker).await?;

    // Example 3: Export tasks by state
    println!("\n=== Example 3: Export Pending Tasks Only ===");
    export_tasks_by_state(&broker, DbTaskState::Pending).await?;

    // Example 4: Import tasks
    println!("\n=== Example 4: Import Tasks ===");
    import_tasks_example(&broker).await?;

    // Example 5: Export DLQ
    println!("\n=== Example 5: Export Dead Letter Queue ===");
    export_dlq_example(&broker).await?;

    // Example 6: Backup and restore workflow
    println!("\n=== Example 6: Backup and Restore Workflow ===");
    backup_restore_workflow(&broker).await?;

    println!("\n=== Bulk Import/Export Example Complete ===");
    Ok(())
}

async fn create_sample_tasks(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    println!("Creating 20 sample tasks...");

    // Create pending tasks
    for i in 0..10 {
        let task = SerializedTask::new(
            "process_order".to_string(),
            serde_json::to_vec(&serde_json::json!({
                "order_id": i,
                "customer": format!("customer_{}", i),
                "amount": 100.0 + (i as f64 * 10.0)
            }))?,
        )
        .with_priority(i % 3)
        .with_max_retries(3);

        broker.enqueue(task).await?;
    }

    // Create completed tasks
    for i in 10..15 {
        let task = SerializedTask::new(
            "send_email".to_string(),
            serde_json::to_vec(&serde_json::json!({
                "email_id": i,
                "to": format!("user{}@example.com", i),
                "subject": "Order Confirmation"
            }))?,
        )
        .with_priority(5)
        .with_max_retries(3);

        broker.enqueue(task).await?;
    }

    // Create failed tasks
    for i in 15..20 {
        let task = SerializedTask::new(
            "generate_report".to_string(),
            serde_json::to_vec(&serde_json::json!({
                "report_id": i,
                "type": "monthly",
                "month": i % 12 + 1
            }))?,
        )
        .with_priority(8)
        .with_max_retries(3);

        broker.enqueue(task).await?;
    }

    let stats = broker.get_statistics().await?;
    println!(
        "Created tasks - Pending: {}, Total: {}",
        stats.pending, stats.total
    );

    Ok(())
}

async fn export_all_tasks(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    // Export all tasks to JSON
    let json_data = broker.export_tasks(None, None).await?;

    // Save to file
    let backup_file = temp_dir().join("celers_tasks_backup.json");
    fs::write(&backup_file, &json_data)?;

    println!("Exported all tasks to: {}", backup_file.display());
    println!("File size: {} bytes", json_data.len());

    // Parse and display summary
    let tasks: Vec<serde_json::Value> = serde_json::from_str(&json_data)?;
    println!("Total tasks exported: {}", tasks.len());

    Ok(())
}

async fn export_tasks_by_state(
    broker: &MysqlBroker,
    state: DbTaskState,
) -> Result<(), Box<dyn std::error::Error>> {
    // Export only pending tasks
    let json_data = broker.export_tasks(Some(state.clone()), None).await?;

    // Save to file
    let backup_file = temp_dir().join(format!("celers_tasks_{}.json", state));
    fs::write(&backup_file, &json_data)?;

    println!("Exported {} tasks to: {}", state, backup_file.display());

    // Parse and display
    let tasks: Vec<serde_json::Value> = serde_json::from_str(&json_data)?;
    println!("Tasks exported: {}", tasks.len());

    Ok(())
}

async fn import_tasks_example(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    // Read from backup file
    let backup_file = temp_dir().join("celers_tasks_backup.json");

    if !backup_file.exists() {
        println!("Backup file not found, skipping import example");
        return Ok(());
    }

    let json_data = fs::read_to_string(&backup_file)?;

    println!("Importing tasks from: {}", backup_file.display());

    // Import with skip_existing=true (don't import duplicates)
    let imported_count = broker.import_tasks(&json_data, true).await?;

    println!("Imported {} tasks successfully", imported_count);

    // Verify
    let stats = broker.get_statistics().await?;
    println!("Current queue status:");
    println!("  Pending: {}", stats.pending);
    println!("  Processing: {}", stats.processing);
    println!("  Completed: {}", stats.completed);
    println!("  Failed: {}", stats.failed);
    println!("  Total: {}", stats.total);

    Ok(())
}

async fn export_dlq_example(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    // Export DLQ entries for analysis
    let json_data = broker.export_dlq(Some(100)).await?;

    let dlq_file = temp_dir().join("celers_dlq_export.json");
    fs::write(&dlq_file, &json_data)?;

    println!("Exported DLQ to: {}", dlq_file.display());
    println!("File size: {} bytes", json_data.len());

    // Parse and analyze
    let dlq_entries: Vec<serde_json::Value> = serde_json::from_str(&json_data)?;
    println!("DLQ entries exported: {}", dlq_entries.len());

    if !dlq_entries.is_empty() {
        println!("\nSample DLQ entry:");
        println!("{}", serde_json::to_string_pretty(&dlq_entries[0])?);
    }

    Ok(())
}

async fn backup_restore_workflow(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    println!("Demonstrating complete backup and restore workflow...\n");

    // Step 1: Backup current state
    println!("Step 1: Creating backup...");
    let backup_dir = temp_dir().join("celers_backup");
    fs::create_dir_all(&backup_dir)?;

    // Export all tasks
    let all_tasks = broker.export_tasks(None, None).await?;
    fs::write(backup_dir.join("all_tasks.json"), &all_tasks)?;

    // Export by state
    let pending_tasks = broker
        .export_tasks(Some(DbTaskState::Pending), None)
        .await?;
    fs::write(backup_dir.join("pending_tasks.json"), &pending_tasks)?;

    // Export DLQ
    let dlq_data = broker.export_dlq(None).await?;
    fs::write(backup_dir.join("dlq.json"), &dlq_data)?;

    println!("Backup created in: {}", backup_dir.display());

    // Step 2: Simulate disaster (purge all)
    println!("\nStep 2: Simulating disaster (purging all tasks)...");
    let purged = broker.purge_all().await?;
    println!("Purged {} tasks", purged);

    let stats_after_purge = broker.get_statistics().await?;
    println!("Tasks remaining: {}", stats_after_purge.total);

    // Step 3: Restore from backup
    println!("\nStep 3: Restoring from backup...");
    let all_tasks_json = fs::read_to_string(backup_dir.join("all_tasks.json"))?;
    let restored = broker.import_tasks(&all_tasks_json, false).await?;
    println!("Restored {} tasks", restored);

    // Step 4: Verify restoration
    println!("\nStep 4: Verifying restoration...");
    let stats_after_restore = broker.get_statistics().await?;
    println!("Queue status after restore:");
    println!("  Pending: {}", stats_after_restore.pending);
    println!("  Processing: {}", stats_after_restore.processing);
    println!("  Completed: {}", stats_after_restore.completed);
    println!("  Failed: {}", stats_after_restore.failed);
    println!("  Total: {}", stats_after_restore.total);

    println!("\n✓ Backup and restore workflow completed successfully!");

    Ok(())
}
