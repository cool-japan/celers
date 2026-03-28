//! Worker management command implementations.

use celers_broker_redis::{QueueMode, RedisBroker};
use celers_worker::{wait_for_signal, Worker, WorkerConfig};
use chrono::Utc;
use colored::Colorize;
use tabled::{settings::Style, Table, Tabled};

/// Start a worker with the given configuration.
///
/// Creates and runs a worker that processes tasks from the specified queue.
/// The worker will run until it receives a shutdown signal (Ctrl+C).
///
/// # Arguments
///
/// * `broker_url` - Redis connection URL (e.g., `redis://localhost:6379`)
/// * `queue` - Queue name to process tasks from
/// * `mode` - Queue mode: "fifo" for FIFO, "priority" for priority-based
/// * `concurrency` - Maximum number of concurrent tasks to process
/// * `max_retries` - Maximum retry attempts for failed tasks
/// * `timeout` - Task execution timeout in seconds
///
/// # Returns
///
/// Returns `Ok(())` on successful shutdown, or an error if worker fails to start.
///
/// # Examples
///
/// ```no_run
/// # use celers_cli::commands::start_worker;
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// // Start a FIFO worker with 4 concurrent tasks
/// start_worker(
///     "redis://localhost:6379",
///     "my_queue",
///     "fifo",
///     4,
///     3,
///     300
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn start_worker(
    broker_url: &str,
    queue: &str,
    mode: &str,
    concurrency: usize,
    max_retries: u32,
    timeout: u64,
) -> anyhow::Result<()> {
    println!("{}", "=== CeleRS Worker ===".bold().green());
    println!();

    // Parse queue mode
    let queue_mode = match mode.to_lowercase().as_str() {
        "priority" => QueueMode::Priority,
        _ => QueueMode::Fifo,
    };

    // Create broker
    let broker = RedisBroker::with_mode(broker_url, queue, queue_mode)?;
    println!("✓ Connected to Redis: {}", broker_url.cyan());
    println!("✓ Queue: {} (mode: {})", queue.cyan(), mode.cyan());

    // Create empty task registry (users would register their tasks)
    let registry = celers_core::TaskRegistry::new();
    println!("⚠️  No tasks registered. Register tasks in your application code.");

    // Configure worker
    let config = WorkerConfig {
        concurrency,
        poll_interval_ms: 1000,
        max_retries,
        default_timeout_secs: timeout,
        ..Default::default()
    };

    println!();
    println!("Worker configuration:");
    println!("  Concurrency: {}", concurrency.to_string().yellow());
    println!("  Max retries: {}", max_retries.to_string().yellow());
    println!("  Timeout: {}s", timeout.to_string().yellow());
    println!();

    // Create worker
    let worker = Worker::new(broker, registry, config);
    println!("{}", "✓ Worker started successfully".green().bold());
    println!("{}", "  Press Ctrl+C to stop gracefully".dimmed());
    println!();

    // Set up signal handler
    let worker_task = tokio::spawn(async move {
        if let Err(e) = worker.run().await {
            eprintln!("Worker error: {e}");
        }
    });

    // Wait for shutdown signal
    wait_for_signal().await;

    println!();
    println!("{}", "Shutting down gracefully...".yellow());
    worker_task.abort();

    println!("{}", "✓ Worker stopped".green());

    Ok(())
}

/// List all running workers
pub async fn list_workers(broker_url: &str) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    println!("{}", "=== Active Workers ===".bold().cyan());
    println!();

    // Workers register themselves with a heartbeat key
    let worker_pattern = "celers:worker:*:heartbeat";
    let mut cursor = 0;
    let mut worker_keys: Vec<String> = Vec::new();

    loop {
        let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(worker_pattern)
            .arg("COUNT")
            .arg(100)
            .query_async(&mut conn)
            .await?;

        worker_keys.extend(keys);
        cursor = new_cursor;

        if cursor == 0 {
            break;
        }
    }

    if worker_keys.is_empty() {
        println!("{}", "No active workers found".yellow());
        println!();
        println!("Workers register themselves when they start processing tasks.");
        return Ok(());
    }

    #[derive(Tabled)]
    struct WorkerInfo {
        #[tabled(rename = "Worker ID")]
        id: String,
        #[tabled(rename = "Status")]
        status: String,
        #[tabled(rename = "Last Heartbeat")]
        last_heartbeat: String,
    }

    let mut workers = Vec::new();
    let worker_count = worker_keys.len();

    for key in &worker_keys {
        // Extract worker ID from key: celers:worker:<id>:heartbeat
        let parts: Vec<&str> = key.split(':').collect();
        if parts.len() >= 3 {
            let worker_id = parts[2].to_string();

            // Get heartbeat timestamp
            let heartbeat: Option<String> =
                redis::cmd("GET").arg(key).query_async(&mut conn).await?;

            let status = if heartbeat.is_some() {
                "Active".to_string()
            } else {
                "Unknown".to_string()
            };

            let last_heartbeat = heartbeat.unwrap_or_else(|| "N/A".to_string());

            workers.push(WorkerInfo {
                id: worker_id,
                status,
                last_heartbeat,
            });
        }
    }

    let table = Table::new(workers).with(Style::rounded()).to_string();
    println!("{table}");
    println!();
    println!(
        "{}",
        format!("Total active workers: {worker_count}")
            .cyan()
            .bold()
    );

    Ok(())
}

/// Show detailed statistics for a worker
pub async fn worker_stats(broker_url: &str, worker_id: &str) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    println!(
        "{}",
        format!("=== Worker Statistics: {worker_id} ===")
            .bold()
            .cyan()
    );
    println!();

    // Check if worker exists
    let heartbeat_key = format!("celers:worker:{worker_id}:heartbeat");
    let heartbeat: Option<String> = redis::cmd("GET")
        .arg(&heartbeat_key)
        .query_async(&mut conn)
        .await?;

    if heartbeat.is_none() {
        println!("{}", format!("✗ Worker '{worker_id}' not found").red());
        println!();
        println!("Possible reasons:");
        println!("  • Worker is not running");
        println!("  • Worker ID is incorrect");
        println!("  • Worker hasn't sent a heartbeat yet");
        return Ok(());
    }

    #[derive(Tabled)]
    struct StatRow {
        #[tabled(rename = "Metric")]
        metric: String,
        #[tabled(rename = "Value")]
        value: String,
    }

    // Gather worker statistics
    let stats_key = format!("celers:worker:{worker_id}:stats");
    let stats: Option<String> = redis::cmd("GET")
        .arg(&stats_key)
        .query_async(&mut conn)
        .await?;

    let mut stat_rows = vec![
        StatRow {
            metric: "Worker ID".to_string(),
            value: worker_id.to_string(),
        },
        StatRow {
            metric: "Status".to_string(),
            value: "Active".to_string(),
        },
        StatRow {
            metric: "Last Heartbeat".to_string(),
            value: heartbeat.unwrap_or_else(|| "N/A".to_string()),
        },
    ];

    if let Some(stats_json) = stats {
        if let Ok(stats_data) = serde_json::from_str::<serde_json::Value>(&stats_json) {
            if let Some(tasks_processed) = stats_data.get("tasks_processed") {
                stat_rows.push(StatRow {
                    metric: "Tasks Processed".to_string(),
                    value: tasks_processed.to_string(),
                });
            }
            if let Some(tasks_failed) = stats_data.get("tasks_failed") {
                stat_rows.push(StatRow {
                    metric: "Tasks Failed".to_string(),
                    value: tasks_failed.to_string(),
                });
            }
            if let Some(uptime) = stats_data.get("uptime_seconds") {
                stat_rows.push(StatRow {
                    metric: "Uptime".to_string(),
                    value: format!("{uptime} seconds"),
                });
            }
        }
    }

    let table = Table::new(stat_rows).with(Style::rounded()).to_string();
    println!("{table}");

    Ok(())
}

/// Stop a specific worker
pub async fn stop_worker(broker_url: &str, worker_id: &str, graceful: bool) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    println!(
        "{}",
        format!("=== Stop Worker: {worker_id} ===").bold().yellow()
    );
    println!();

    // Check if worker exists
    let heartbeat_key = format!("celers:worker:{worker_id}:heartbeat");
    let heartbeat: Option<String> = redis::cmd("GET")
        .arg(&heartbeat_key)
        .query_async(&mut conn)
        .await?;

    if heartbeat.is_none() {
        println!("{}", format!("✗ Worker '{worker_id}' not found").red());
        return Ok(());
    }

    // Publish stop command via Redis Pub/Sub
    let channel = if graceful {
        format!("celers:worker:{worker_id}:shutdown_graceful")
    } else {
        format!("celers:worker:{worker_id}:shutdown")
    };

    let subscribers: usize = redis::cmd("PUBLISH")
        .arg(&channel)
        .arg("STOP")
        .query_async(&mut conn)
        .await?;

    if subscribers > 0 {
        println!(
            "{}",
            format!(
                "✓ Stop signal sent to worker '{}' (mode: {})",
                worker_id,
                if graceful { "graceful" } else { "immediate" }
            )
            .green()
            .bold()
        );
        println!();
        if graceful {
            println!("The worker will:");
            println!("  • Finish processing current tasks");
            println!("  • Stop accepting new tasks");
            println!("  • Shut down gracefully");
        } else {
            println!("The worker will:");
            println!("  • Stop immediately");
            println!("  • Cancel running tasks");
        }
    } else {
        println!(
            "{}",
            format!("⚠ No subscribers for worker '{worker_id}'")
                .yellow()
                .bold()
        );
        println!();
        println!("The worker may not be listening for stop commands.");
    }

    Ok(())
}

/// Pause task processing for a worker
pub async fn pause_worker(broker_url: &str, worker_id: &str) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    let pause_key = format!("celers:worker:{worker_id}:paused");
    let timestamp = chrono::Utc::now().to_rfc3339();

    let _: () = redis::cmd("SET")
        .arg(&pause_key)
        .arg(&timestamp)
        .query_async(&mut conn)
        .await?;

    println!(
        "{}",
        format!("✓ Worker '{worker_id}' has been paused")
            .green()
            .bold()
    );
    println!();
    println!("{}", "Note:".yellow().bold());
    println!("  • Worker will stop accepting new tasks");
    println!("  • Current tasks will continue to completion");
    println!("  • Use 'celers worker-mgmt resume' to resume");
    println!();
    println!("  Paused at: {}", timestamp.cyan());

    Ok(())
}

/// Resume task processing for a worker
pub async fn resume_worker(broker_url: &str, worker_id: &str) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    let pause_key = format!("celers:worker:{worker_id}:paused");

    // Check if worker is paused
    let paused: Option<String> = redis::cmd("GET")
        .arg(&pause_key)
        .query_async(&mut conn)
        .await?;

    if paused.is_none() {
        println!(
            "{}",
            format!("✓ Worker '{worker_id}' is not paused").yellow()
        );
        return Ok(());
    }

    // Remove pause flag
    let _: () = redis::cmd("DEL")
        .arg(&pause_key)
        .query_async(&mut conn)
        .await?;

    println!(
        "{}",
        format!("✓ Worker '{worker_id}' has been resumed")
            .green()
            .bold()
    );
    println!();
    println!("{}", "Note:".yellow().bold());
    println!("  • Worker will now accept new tasks");
    if let Some(paused_at) = paused {
        println!("  • Was paused at: {}", paused_at.dimmed());
    }

    Ok(())
}

/// Scale workers to N instances
pub async fn scale_workers(broker_url: &str, target_count: usize) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    println!(
        "{}",
        format!("=== Scale Workers to {target_count} ===")
            .bold()
            .cyan()
    );
    println!();

    // Get current worker count
    let pattern = "celers:worker:*:heartbeat";
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(pattern)
        .query_async(&mut conn)
        .await?;

    let current_count = keys.len();

    println!("Current workers: {}", current_count.to_string().yellow());
    println!("Target workers: {}", target_count.to_string().green());
    println!();

    if current_count == target_count {
        println!("{}", "✓ Already at target worker count".green().bold());
        return Ok(());
    }

    if current_count < target_count {
        let needed = target_count - current_count;
        println!(
            "{}",
            format!("⚠ Need to start {needed} more workers")
                .yellow()
                .bold()
        );
        println!();
        println!("To scale up, start additional worker instances:");
        println!("  celers worker --broker {broker_url}");
        println!();
        println!("Or run them in parallel:");
        for i in 1..=needed {
            println!("  celers worker --broker {broker_url} & # Worker {i}");
        }
    } else {
        let excess = current_count - target_count;
        println!(
            "{}",
            format!("⚠ Need to stop {excess} workers").yellow().bold()
        );
        println!();
        println!("To scale down, stop workers gracefully:");
        println!("  celers worker-mgmt list");
        println!("  celers worker-mgmt stop <worker-id> --graceful");
    }

    Ok(())
}

/// Drain worker (stop accepting new tasks)
pub async fn drain_worker(broker_url: &str, worker_id: &str) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    println!(
        "{}",
        format!("=== Drain Worker: {worker_id} ===").bold().cyan()
    );
    println!();

    // Check if worker exists
    let heartbeat_key = format!("celers:worker:{worker_id}:heartbeat");
    let exists: bool = redis::cmd("EXISTS")
        .arg(&heartbeat_key)
        .query_async(&mut conn)
        .await?;

    if !exists {
        println!("{}", format!("✗ Worker '{worker_id}' not found").red());
        return Ok(());
    }

    // Set drain flag
    let drain_key = format!("celers:worker:{worker_id}:draining");
    let timestamp = Utc::now().to_rfc3339();
    redis::cmd("SET")
        .arg(&drain_key)
        .arg(&timestamp)
        .query_async::<()>(&mut conn)
        .await?;

    // Set TTL to 24 hours
    redis::cmd("EXPIRE")
        .arg(&drain_key)
        .arg(86400)
        .query_async::<()>(&mut conn)
        .await?;

    println!(
        "{}",
        format!("✓ Worker '{worker_id}' is now draining")
            .green()
            .bold()
    );
    println!();
    println!("The worker will:");
    println!("  • Stop accepting new tasks");
    println!("  • Complete currently running tasks");
    println!("  • Shut down automatically when all tasks complete");
    println!();
    println!("To resume normal operation:");
    println!("  celers worker-mgmt resume {worker_id}");

    Ok(())
}
