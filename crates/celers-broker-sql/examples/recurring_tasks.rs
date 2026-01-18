//! Recurring Tasks Example
//!
//! Demonstrates scheduled periodic task execution (cron-like functionality).
//! This is useful for:
//! - Regular maintenance tasks
//! - Scheduled reports
//! - Periodic data synchronization
//! - Time-based automation

use celers_broker_sql::{MysqlBroker, PoolConfig, RecurringSchedule, RecurringTaskConfig};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("=== Recurring Tasks Example ===\n");

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
    let broker = MysqlBroker::with_config(&database_url, "recurring_demo", pool_config).await?;

    println!("Running migrations...");
    broker.migrate().await?;

    // Example 1: Register recurring task - every N seconds
    println!("\n=== Example 1: Task Every 30 Seconds ===");
    register_every_seconds_task(&broker).await?;

    // Example 2: Register recurring task - every N minutes
    println!("\n=== Example 2: Task Every 5 Minutes ===");
    register_every_minutes_task(&broker).await?;

    // Example 3: Register recurring task - every N hours
    println!("\n=== Example 3: Task Every Hour ===");
    register_every_hours_task(&broker).await?;

    // Example 4: Register recurring task - daily at specific time
    println!("\n=== Example 4: Daily Task at 3:00 AM ===");
    register_daily_task(&broker).await?;

    // Example 5: Register recurring task - weekly
    println!("\n=== Example 5: Weekly Task (Every Monday) ===");
    register_weekly_task(&broker).await?;

    // Example 6: Register recurring task - monthly
    println!("\n=== Example 6: Monthly Task (1st of month) ===");
    register_monthly_task(&broker).await?;

    // Example 7: List all recurring tasks
    println!("\n=== Example 7: List All Recurring Tasks ===");
    list_all_recurring_tasks(&broker).await?;

    // Example 8: Process recurring tasks (scheduler simulation)
    println!("\n=== Example 8: Process Recurring Tasks ===");
    simulate_scheduler(&broker).await?;

    // Example 9: Delete recurring task
    println!("\n=== Example 9: Delete Recurring Task ===");
    delete_recurring_task_example(&broker).await?;

    println!("\n=== Recurring Tasks Example Complete ===");
    println!("\nNote: In production, you would run process_recurring_tasks()");
    println!("on a regular interval (e.g., every 10-60 seconds) using a");
    println!("background scheduler or cron job.");

    Ok(())
}

async fn register_every_seconds_task(
    broker: &MysqlBroker,
) -> Result<(), Box<dyn std::error::Error>> {
    let payload = serde_json::to_vec(&serde_json::json!({
        "check_type": "database",
        "interval": 30
    }))?;

    let config = RecurringTaskConfig {
        task_name: "health_check".to_string(),
        schedule: RecurringSchedule::EverySeconds(30),
        payload,
        priority: 10,
        enabled: true,
        last_run: None,
        next_run: chrono::Utc::now(),
    };

    let config_id = broker.register_recurring_task(config).await?;
    println!(
        "Registered health check task (every 30 seconds): {}",
        config_id
    );
    println!("  Next run: ~30 seconds from now");

    Ok(())
}

async fn register_every_minutes_task(
    broker: &MysqlBroker,
) -> Result<(), Box<dyn std::error::Error>> {
    let payload = serde_json::to_vec(&serde_json::json!({
        "cache_type": "user_stats",
        "interval_minutes": 5
    }))?;

    let config = RecurringTaskConfig {
        task_name: "update_cache".to_string(),
        schedule: RecurringSchedule::EveryMinutes(5),
        payload,
        priority: 8,
        enabled: true,
        last_run: None,
        next_run: chrono::Utc::now(),
    };

    let config_id = broker.register_recurring_task(config).await?;
    println!(
        "Registered cache update task (every 5 minutes): {}",
        config_id
    );
    println!("  Next run: ~5 minutes from now");

    Ok(())
}

async fn register_every_hours_task(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    let payload = serde_json::to_vec(&serde_json::json!({
        "directory": "/tmp",
        "max_age_hours": 24
    }))?;

    let config = RecurringTaskConfig {
        task_name: "cleanup_temp_files".to_string(),
        schedule: RecurringSchedule::EveryHours(1),
        payload,
        priority: 5,
        enabled: true,
        last_run: None,
        next_run: chrono::Utc::now(),
    };

    let config_id = broker.register_recurring_task(config).await?;
    println!("Registered cleanup task (every hour): {}", config_id);
    println!("  Next run: ~1 hour from now");

    Ok(())
}

async fn register_daily_task(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    let payload = serde_json::to_vec(&serde_json::json!({
        "report_type": "daily_summary",
        "recipients": ["admin@example.com"]
    }))?;

    let config = RecurringTaskConfig {
        task_name: "generate_daily_report".to_string(),
        schedule: RecurringSchedule::EveryDays(1, 3, 0), // every 1 day at 3:00
        payload,
        priority: 9,
        enabled: true,
        last_run: None,
        next_run: chrono::Utc::now(),
    };

    let config_id = broker.register_recurring_task(config).await?;
    println!(
        "Registered daily report task (every day at 3:00 AM): {}",
        config_id
    );
    println!("  Next run: tomorrow at 3:00 AM");

    Ok(())
}

async fn register_weekly_task(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    let payload = serde_json::to_vec(&serde_json::json!({
        "backup_type": "full",
        "destination": "s3://backups/weekly/"
    }))?;

    let config = RecurringTaskConfig {
        task_name: "weekly_backup".to_string(),
        schedule: RecurringSchedule::Weekly(1, 2, 0), // Monday at 2:00 AM
        payload,
        priority: 10,
        enabled: true,
        last_run: None,
        next_run: chrono::Utc::now(),
    };

    let config_id = broker.register_recurring_task(config).await?;
    println!(
        "Registered weekly backup task (every Monday at 2:00 AM): {}",
        config_id
    );
    println!("  Next run: next Monday at 2:00 AM");

    Ok(())
}

async fn register_monthly_task(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    let payload = serde_json::to_vec(&serde_json::json!({
        "billing_type": "subscription",
        "process": "invoices"
    }))?;

    let config = RecurringTaskConfig {
        task_name: "monthly_billing".to_string(),
        schedule: RecurringSchedule::Monthly(1, 0, 0), // 1st of month at midnight
        payload,
        priority: 10,
        enabled: true,
        last_run: None,
        next_run: chrono::Utc::now(),
    };

    let config_id = broker.register_recurring_task(config).await?;
    println!(
        "Registered monthly billing task (1st of every month at midnight): {}",
        config_id
    );
    println!("  Next run: 1st of next month at midnight");

    Ok(())
}

async fn list_all_recurring_tasks(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    let recurring_tasks = broker.list_recurring_tasks().await?;

    println!(
        "Total recurring tasks registered: {}",
        recurring_tasks.len()
    );
    println!();

    for (id, config) in recurring_tasks {
        println!("ID: {}", id);
        println!("  Task Name: {}", config.task_name);
        println!("  Schedule: {:?}", config.schedule);
        println!("  Enabled: {}", config.enabled);
        println!("  Priority: {}", config.priority);
        println!();
    }

    Ok(())
}

async fn simulate_scheduler(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    println!("Simulating scheduler processing (checking for due tasks)...\n");

    // Process recurring tasks multiple times to demonstrate
    for i in 1..=3 {
        println!("Scheduler tick {}/3", i);

        let enqueued = broker.process_recurring_tasks().await?;

        if enqueued > 0 {
            println!("  ✓ Enqueued {} recurring task(s)", enqueued);

            // Show queue stats
            let stats = broker.get_statistics().await?;
            println!(
                "  Queue status: {} pending, {} total",
                stats.pending, stats.total
            );
        } else {
            println!("  No tasks due for execution");
        }

        // Wait a bit between checks
        if i < 3 {
            sleep(Duration::from_secs(2)).await;
        }
    }

    println!("\nNote: In production, process_recurring_tasks() would be called");
    println!("every 10-60 seconds by a background scheduler.");

    Ok(())
}

async fn delete_recurring_task_example(
    broker: &MysqlBroker,
) -> Result<(), Box<dyn std::error::Error>> {
    // Get all recurring tasks
    let recurring_tasks = broker.list_recurring_tasks().await?;

    if let Some((id, config)) = recurring_tasks.first() {
        println!("Deleting recurring task:");
        println!("  ID: {}", id);
        println!("  Task Name: {}", config.task_name);

        let deleted = broker.delete_recurring_task(id).await?;

        if deleted {
            println!("  ✓ Task deleted successfully");
        } else {
            println!("  ✗ Task not found or already deleted");
        }

        // Verify deletion
        let remaining = broker.list_recurring_tasks().await?;
        println!("Remaining recurring tasks: {}", remaining.len());
    } else {
        println!("No recurring tasks to delete");
    }

    Ok(())
}
