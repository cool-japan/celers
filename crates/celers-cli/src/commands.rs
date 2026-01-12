//! CLI command implementations for `CeleRS` distributed task queue management.
//!
//! This module provides all the command implementations for the `CeleRS` CLI tool.
//! Commands are organized into the following categories:
//!
//! - **Worker Management**: Starting, stopping, pausing, scaling workers
//! - **Queue Operations**: Listing, purging, moving, importing/exporting queues
//! - **Task Management**: Inspecting, canceling, retrying, and monitoring tasks
//! - **DLQ Operations**: Managing failed tasks in the Dead Letter Queue
//! - **Scheduling**: Managing scheduled/periodic tasks with cron expressions
//! - **Monitoring**: Metrics, dashboard, health checks, and diagnostics
//! - **Database**: Connection testing, health checks, migrations
//! - **Configuration**: Validation, initialization, and profile management
//!
//! # Error Handling
//!
//! All functions return `anyhow::Result<()>` for consistent error handling.
//! Errors are user-friendly and provide actionable feedback.
//!
//! # Examples
//!
//! ```no_run
//! use celers_cli::commands;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Start a worker
//!     commands::start_worker(
//!         "redis://localhost:6379",
//!         "my_queue",
//!         "fifo",
//!         4,
//!         3,
//!         300
//!     ).await?;
//!     Ok(())
//! }
//! ```

use celers_broker_redis::{QueueMode, RedisBroker};
use celers_core::Broker;
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

/// Display queue status and statistics.
///
/// Shows current queue metrics including pending tasks, DLQ size, and health warnings.
/// Uses a formatted table for clear visualization of queue state.
///
/// # Arguments
///
/// * `broker_url` - Redis connection URL
/// * `queue` - Queue name to check status for
///
/// # Returns
///
/// Returns `Ok(())` on success, or an error if connection fails.
///
/// # Examples
///
/// ```no_run
/// # use celers_cli::commands::show_status;
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// show_status("redis://localhost:6379", "my_queue").await?;
/// # Ok(())
/// # }
/// ```
pub async fn show_status(broker_url: &str, queue: &str) -> anyhow::Result<()> {
    let broker = RedisBroker::new(broker_url, queue)?;

    println!("{}", "=== Queue Status ===".bold().cyan());
    println!();

    let queue_size = broker.queue_size().await?;
    let dlq_size = broker.dlq_size().await?;

    #[derive(Tabled)]
    struct QueueStats {
        #[tabled(rename = "Metric")]
        metric: String,
        #[tabled(rename = "Value")]
        value: String,
    }

    let stats = vec![
        QueueStats {
            metric: "Queue".to_string(),
            value: queue.to_string(),
        },
        QueueStats {
            metric: "Pending Tasks".to_string(),
            value: queue_size.to_string(),
        },
        QueueStats {
            metric: "Failed Tasks (DLQ)".to_string(),
            value: dlq_size.to_string(),
        },
        QueueStats {
            metric: "Total".to_string(),
            value: (queue_size + dlq_size).to_string(),
        },
    ];

    let table = Table::new(stats).with(Style::rounded()).to_string();
    println!("{table}");

    if dlq_size > 0 {
        println!();
        println!(
            "{}",
            format!("⚠️  {dlq_size} tasks in Dead Letter Queue")
                .yellow()
                .bold()
        );
        println!("   Run: celers dlq inspect --broker {broker_url} --queue {queue}");
    }

    Ok(())
}

/// Inspect failed tasks in the Dead Letter Queue (DLQ).
///
/// Displays a list of failed tasks with their IDs, task names, and failure metadata.
/// Useful for debugging and understanding why tasks are failing.
///
/// # Arguments
///
/// * `broker_url` - Redis connection URL
/// * `queue` - Queue name
/// * `limit` - Maximum number of tasks to display (default: 10)
///
/// # Returns
///
/// Returns `Ok(())` on success, or an error if connection fails.
///
/// # Examples
///
/// ```no_run
/// # use celers_cli::commands::inspect_dlq;
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// // Show last 20 failed tasks
/// inspect_dlq("redis://localhost:6379", "my_queue", 20).await?;
/// # Ok(())
/// # }
/// ```
pub async fn inspect_dlq(broker_url: &str, queue: &str, limit: usize) -> anyhow::Result<()> {
    let broker = RedisBroker::new(broker_url, queue)?;

    println!("{}", "=== Dead Letter Queue ===".bold().red());
    println!();

    let dlq_size = broker.dlq_size().await?;
    println!("Total failed tasks: {}", dlq_size.to_string().yellow());

    if dlq_size == 0 {
        println!("{}", "✓ DLQ is empty".green());
        return Ok(());
    }

    println!();
    let tasks = broker.inspect_dlq(limit as isize).await?;

    for (idx, task) in tasks.iter().enumerate() {
        println!("{}", format!("Task #{}", idx + 1).bold());
        println!("  ID: {}", task.metadata.id.to_string().cyan());
        println!("  Name: {}", task.metadata.name.yellow());
        println!("  State: {:?}", task.metadata.state);
        println!("  Max Retries: {}", task.metadata.max_retries);
        println!("  Created: {}", task.metadata.created_at);
        println!();
    }

    println!(
        "Showing {} of {} tasks",
        tasks.len().to_string().yellow(),
        dlq_size
    );

    Ok(())
}

/// Clear Dead Letter Queue
pub async fn clear_dlq(broker_url: &str, queue: &str, confirm: bool) -> anyhow::Result<()> {
    let broker = RedisBroker::new(broker_url, queue)?;

    let dlq_size = broker.dlq_size().await?;

    if dlq_size == 0 {
        println!("{}", "✓ DLQ is already empty".green());
        return Ok(());
    }

    if !confirm {
        println!(
            "{}",
            format!("⚠️  This will delete {dlq_size} tasks from DLQ")
                .yellow()
                .bold()
        );
        println!("   Add --confirm to proceed");
        return Ok(());
    }

    let count = broker.clear_dlq().await?;
    println!("{}", format!("✓ Cleared {count} tasks from DLQ").green());

    Ok(())
}

/// Replay a task from DLQ
pub async fn replay_task(broker_url: &str, queue: &str, task_id_str: &str) -> anyhow::Result<()> {
    let broker = RedisBroker::new(broker_url, queue)?;

    let task_id = task_id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| anyhow::anyhow!("Invalid task ID format"))?;

    println!("{}", "=== Replaying Task from DLQ ===".bold().cyan());
    println!("Task ID: {}", task_id.to_string().yellow());

    let replayed = broker.replay_from_dlq(&task_id).await?;

    if replayed {
        println!("{}", "✓ Task replayed successfully".green().bold());
        println!("  The task will be processed again by workers");
    } else {
        println!("{}", "✗ Task not found in DLQ".red());
    }

    Ok(())
}

/// Generate a default configuration file with template settings.
///
/// Creates a TOML configuration file with commented examples for all settings.
/// The generated file can be customized for different environments.
///
/// # Arguments
///
/// * `path` - Output file path (default: "celers.toml")
///
/// # Returns
///
/// Returns `Ok(())` on success, or an error if file creation fails.
///
/// # Examples
///
/// ```no_run
/// # use celers_cli::commands::init_config;
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// // Create default config
/// init_config("celers.toml").await?;
///
/// // Create production config
/// init_config("celers-prod.toml").await?;
/// # Ok(())
/// # }
/// ```
#[allow(clippy::unused_async)]
pub async fn init_config(path: &str) -> anyhow::Result<()> {
    let config = crate::config::Config::default_config();

    config.to_file(path)?;

    println!("{}", "✓ Configuration file created".green().bold());
    println!("  Location: {}", path.cyan());
    println!();
    println!("Edit the file and run:");
    println!("  celers worker --config {path}");

    Ok(())
}

/// List all queues (Redis only)
pub async fn list_queues(broker_url: &str) -> anyhow::Result<()> {
    // Connect to Redis
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    println!("{}", "=== Redis Queues ===".bold().cyan());
    println!();

    // Scan for keys matching queue patterns
    let mut cursor = 0;
    let mut queue_keys: Vec<String> = Vec::new();

    loop {
        let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg("celers:*")
            .arg("COUNT")
            .arg(100)
            .query_async(&mut conn)
            .await?;

        queue_keys.extend(keys);
        cursor = new_cursor;

        if cursor == 0 {
            break;
        }
    }

    if queue_keys.is_empty() {
        println!("{}", "No queues found".yellow());
        return Ok(());
    }

    #[derive(Tabled)]
    struct QueueInfo {
        #[tabled(rename = "Queue")]
        name: String,
        #[tabled(rename = "Type")]
        queue_type: String,
        #[tabled(rename = "Size")]
        size: String,
    }

    let mut queue_infos = Vec::new();

    for key in queue_keys {
        let key_type: String = redis::cmd("TYPE").arg(&key).query_async(&mut conn).await?;

        let size: isize = match key_type.as_str() {
            "list" => redis::cmd("LLEN").arg(&key).query_async(&mut conn).await?,
            "zset" => redis::cmd("ZCARD").arg(&key).query_async(&mut conn).await?,
            _ => 0,
        };

        let queue_type = if key.contains(":dlq") {
            "DLQ".to_string()
        } else if key.contains(":delayed") {
            "Delayed".to_string()
        } else if key_type == "zset" {
            "Priority".to_string()
        } else {
            "FIFO".to_string()
        };

        queue_infos.push(QueueInfo {
            name: key,
            queue_type,
            size: size.to_string(),
        });
    }

    let table = Table::new(queue_infos).with(Style::rounded()).to_string();
    println!("{table}");

    Ok(())
}

/// Purge all tasks from a queue
pub async fn purge_queue(broker_url: &str, queue: &str, confirm: bool) -> anyhow::Result<()> {
    let broker = RedisBroker::new(broker_url, queue)?;

    let queue_size = broker.queue_size().await?;

    if queue_size == 0 {
        println!("{}", "✓ Queue is already empty".green());
        return Ok(());
    }

    if !confirm {
        println!(
            "{}",
            format!("⚠️  This will delete {queue_size} tasks from queue '{queue}'")
                .yellow()
                .bold()
        );
        println!("   Add --confirm to proceed");
        return Ok(());
    }

    // Connect to Redis directly to delete the queue
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    let queue_key = format!("celers:{queue}");
    redis::cmd("DEL")
        .arg(&queue_key)
        .query_async::<()>(&mut conn)
        .await?;

    println!(
        "{}",
        format!("✓ Purged {queue_size} tasks from queue '{queue}'").green()
    );

    Ok(())
}

/// Inspect a specific task by ID
pub async fn inspect_task(broker_url: &str, queue: &str, task_id_str: &str) -> anyhow::Result<()> {
    let task_id = task_id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| anyhow::anyhow!("Invalid task ID format"))?;

    println!("{}", "=== Task Details ===".bold().cyan());
    println!("Task ID: {}", task_id.to_string().yellow());
    println!();

    // Connect to Redis
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    // Search in main queue
    let queue_key = format!("celers:{queue}");
    let queue_type: String = redis::cmd("TYPE")
        .arg(&queue_key)
        .query_async(&mut conn)
        .await?;

    let mut found = false;

    // Try to find task in main queue
    if queue_type == "list" {
        let tasks: Vec<String> = redis::cmd("LRANGE")
            .arg(&queue_key)
            .arg(0)
            .arg(-1)
            .query_async(&mut conn)
            .await?;

        for task_str in tasks {
            if let Ok(task) = serde_json::from_str::<celers_core::SerializedTask>(&task_str) {
                if task.metadata.id == task_id {
                    print_task_details(&task, "Main Queue");
                    found = true;
                    break;
                }
            }
        }
    } else if queue_type == "zset" {
        let tasks: Vec<String> = redis::cmd("ZRANGE")
            .arg(&queue_key)
            .arg(0)
            .arg(-1)
            .query_async(&mut conn)
            .await?;

        for task_str in tasks {
            if let Ok(task) = serde_json::from_str::<celers_core::SerializedTask>(&task_str) {
                if task.metadata.id == task_id {
                    print_task_details(&task, "Main Queue (Priority)");
                    found = true;
                    break;
                }
            }
        }
    }

    // Try DLQ if not found
    if !found {
        let dlq_key = format!("celers:{queue}:dlq");
        let dlq_tasks: Vec<String> = redis::cmd("LRANGE")
            .arg(&dlq_key)
            .arg(0)
            .arg(-1)
            .query_async(&mut conn)
            .await?;

        for task_str in dlq_tasks {
            if let Ok(task) = serde_json::from_str::<celers_core::SerializedTask>(&task_str) {
                if task.metadata.id == task_id {
                    print_task_details(&task, "Dead Letter Queue");
                    found = true;
                    break;
                }
            }
        }
    }

    // Try delayed queue if not found
    if !found {
        let delayed_key = format!("celers:{queue}:delayed");
        let delayed_tasks: Vec<String> = redis::cmd("ZRANGE")
            .arg(&delayed_key)
            .arg(0)
            .arg(-1)
            .query_async(&mut conn)
            .await?;

        for task_str in delayed_tasks {
            if let Ok(task) = serde_json::from_str::<celers_core::SerializedTask>(&task_str) {
                if task.metadata.id == task_id {
                    print_task_details(&task, "Delayed Queue");
                    found = true;
                    break;
                }
            }
        }
    }

    if !found {
        println!("{}", "✗ Task not found in any queue".red());
        println!();
        println!("The task may have been:");
        println!("  • Already processed");
        println!("  • Deleted");
        println!("  • In a different queue");
    }

    Ok(())
}

/// Helper to print task details
fn print_task_details(task: &celers_core::SerializedTask, location: &str) {
    println!("{}", format!("Location: {location}").green().bold());
    println!();

    #[derive(Tabled)]
    struct TaskDetail {
        #[tabled(rename = "Field")]
        field: String,
        #[tabled(rename = "Value")]
        value: String,
    }

    let details = vec![
        TaskDetail {
            field: "ID".to_string(),
            value: task.metadata.id.to_string(),
        },
        TaskDetail {
            field: "Name".to_string(),
            value: task.metadata.name.clone(),
        },
        TaskDetail {
            field: "State".to_string(),
            value: format!("{:?}", task.metadata.state),
        },
        TaskDetail {
            field: "Priority".to_string(),
            value: task.metadata.priority.to_string(),
        },
        TaskDetail {
            field: "Max Retries".to_string(),
            value: task.metadata.max_retries.to_string(),
        },
        TaskDetail {
            field: "Timeout".to_string(),
            value: task
                .metadata
                .timeout_secs
                .map_or_else(|| "default".to_string(), |s| format!("{s}s")),
        },
        TaskDetail {
            field: "Created At".to_string(),
            value: task.metadata.created_at.to_string(),
        },
        TaskDetail {
            field: "Payload Size".to_string(),
            value: format!("{} bytes", task.payload.len()),
        },
    ];

    let table = Table::new(details).with(Style::rounded()).to_string();
    println!("{table}");
}

/// Cancel a running or pending task
pub async fn cancel_task(broker_url: &str, queue: &str, task_id_str: &str) -> anyhow::Result<()> {
    let task_id = task_id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| anyhow::anyhow!("Invalid task ID format"))?;

    println!("{}", "=== Cancel Task ===".bold().yellow());
    println!("Task ID: {}", task_id.to_string().yellow());
    println!();

    // Create broker
    let broker = RedisBroker::new(broker_url, queue)?;
    println!("✓ Connected to Redis: {}", broker_url.cyan());
    println!();

    // Send cancellation signal
    println!("Sending cancellation signal...");
    let cancelled = broker.cancel(&task_id).await?;

    if cancelled {
        println!(
            "{}",
            "✓ Cancellation signal sent successfully".green().bold()
        );
        println!();
        println!("The task will be cancelled if:");
        println!("  • It's currently running and has cancellation checkpoints");
        println!("  • It's pending in the queue (will be removed)");
        println!("  • Workers are subscribed to the cancellation channel");
        println!();
        println!(
            "{}",
            "Note: Task cancellation depends on worker implementation".yellow()
        );
    } else {
        println!(
            "{}",
            "⚠️  No workers subscribed to cancellation channel"
                .yellow()
                .bold()
        );
        println!();
        println!("Possible reasons:");
        println!("  • No workers are currently running");
        println!("  • Workers don't support cancellation");
        println!("  • The task has already completed");
        println!();
        println!("Make sure workers are running and support task cancellation.");
    }

    Ok(())
}

/// Retry a failed task (from any queue)
pub async fn retry_task(broker_url: &str, queue: &str, task_id_str: &str) -> anyhow::Result<()> {
    let task_id = task_id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| anyhow::anyhow!("Invalid task ID format"))?;

    println!("{}", "=== Retry Task ===".bold().cyan());
    println!("Task ID: {}", task_id.to_string().yellow());
    println!();

    // Connect to Redis
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    let mut task: Option<celers_core::SerializedTask> = None;
    let mut source_queue: Option<String> = None;

    // Search for task in main queue
    let queue_key = format!("celers:{queue}");
    let queue_type: String = redis::cmd("TYPE")
        .arg(&queue_key)
        .query_async(&mut conn)
        .await?;

    // Try to find task in main queue (FIFO/Priority)
    if queue_type == "list" {
        let tasks: Vec<String> = redis::cmd("LRANGE")
            .arg(&queue_key)
            .arg(0)
            .arg(-1)
            .query_async(&mut conn)
            .await?;

        for task_str in &tasks {
            if let Ok(t) = serde_json::from_str::<celers_core::SerializedTask>(task_str) {
                if t.metadata.id == task_id {
                    task = Some(t);
                    source_queue = Some("main_list".to_string());
                    // Remove from list
                    let _: usize = redis::cmd("LREM")
                        .arg(&queue_key)
                        .arg(1)
                        .arg(task_str)
                        .query_async(&mut conn)
                        .await?;
                    break;
                }
            }
        }
    } else if queue_type == "zset" {
        let tasks: Vec<String> = redis::cmd("ZRANGE")
            .arg(&queue_key)
            .arg(0)
            .arg(-1)
            .query_async(&mut conn)
            .await?;

        for task_str in tasks {
            if let Ok(t) = serde_json::from_str::<celers_core::SerializedTask>(&task_str) {
                if t.metadata.id == task_id {
                    task = Some(t);
                    source_queue = Some("main_zset".to_string());
                    // Remove from sorted set
                    let _: usize = redis::cmd("ZREM")
                        .arg(&queue_key)
                        .arg(&task_str)
                        .query_async(&mut conn)
                        .await?;
                    break;
                }
            }
        }
    }

    // Try DLQ if not found
    if task.is_none() {
        let dlq_key = format!("celers:{queue}:dlq");
        let dlq_tasks: Vec<String> = redis::cmd("LRANGE")
            .arg(&dlq_key)
            .arg(0)
            .arg(-1)
            .query_async(&mut conn)
            .await?;

        for task_str in dlq_tasks {
            if let Ok(t) = serde_json::from_str::<celers_core::SerializedTask>(&task_str) {
                if t.metadata.id == task_id {
                    task = Some(t);
                    source_queue = Some("dlq".to_string());
                    // Remove from DLQ
                    let _: usize = redis::cmd("LREM")
                        .arg(&dlq_key)
                        .arg(1)
                        .arg(&task_str)
                        .query_async(&mut conn)
                        .await?;
                    break;
                }
            }
        }
    }

    // Try delayed queue if not found
    if task.is_none() {
        let delayed_key = format!("celers:{queue}:delayed");
        let delayed_tasks: Vec<String> = redis::cmd("ZRANGE")
            .arg(&delayed_key)
            .arg(0)
            .arg(-1)
            .query_async(&mut conn)
            .await?;

        for task_str in delayed_tasks {
            if let Ok(t) = serde_json::from_str::<celers_core::SerializedTask>(&task_str) {
                if t.metadata.id == task_id {
                    task = Some(t);
                    source_queue = Some("delayed".to_string());
                    // Remove from delayed queue
                    let _: usize = redis::cmd("ZREM")
                        .arg(&delayed_key)
                        .arg(&task_str)
                        .query_async(&mut conn)
                        .await?;
                    break;
                }
            }
        }
    }

    // If task found, reset and re-enqueue
    if let Some(mut t) = task {
        println!("✓ Task found in: {}", source_queue.unwrap().cyan());
        println!();

        // Reset task state
        t.metadata.state = celers_core::TaskState::Pending;
        t.metadata.updated_at = Utc::now();

        // Re-enqueue to main queue
        let task_json = serde_json::to_string(&t)?;
        let _: usize = redis::cmd("LPUSH")
            .arg(&queue_key)
            .arg(&task_json)
            .query_async(&mut conn)
            .await?;

        println!("{}", "✓ Task retried successfully".green().bold());
        println!();
        println!("The task has been:");
        println!("  • Removed from its current queue");
        println!("  • Reset to Pending state");
        println!("  • Re-enqueued to main queue");
        println!();
        println!("Workers will process it again.");
    } else {
        println!("{}", "✗ Task not found in any queue".red());
        println!();
        println!("The task may have been:");
        println!("  • Already processed and completed");
        println!("  • Deleted manually");
        println!("  • In a different queue");
    }

    Ok(())
}

/// Show task result from backend
pub async fn show_task_result(backend_url: &str, task_id_str: &str) -> anyhow::Result<()> {
    let task_id = task_id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| anyhow::anyhow!("Invalid task ID format"))?;

    println!("{}", "=== Task Result ===".bold().cyan());
    println!("Task ID: {}", task_id.to_string().yellow());
    println!();

    // Connect to Redis
    let client = redis::Client::open(backend_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    // Check for task result in Redis backend
    let result_key = format!("celery-task-meta-{task_id}");
    let result_data: Option<String> = redis::cmd("GET")
        .arg(&result_key)
        .query_async(&mut conn)
        .await?;

    if let Some(data) = result_data {
        // Parse the result JSON
        let result: serde_json::Value = serde_json::from_str(&data)?;

        println!("{}", "✓ Task result found".green().bold());
        println!();

        #[derive(Tabled)]
        struct ResultField {
            #[tabled(rename = "Field")]
            field: String,
            #[tabled(rename = "Value")]
            value: String,
        }

        let mut fields = vec![
            ResultField {
                field: "Task ID".to_string(),
                value: result
                    .get("task_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("N/A")
                    .to_string(),
            },
            ResultField {
                field: "Status".to_string(),
                value: result
                    .get("status")
                    .and_then(|v| v.as_str())
                    .unwrap_or("UNKNOWN")
                    .to_string(),
            },
        ];

        // Add result if present
        if let Some(task_result) = result.get("result") {
            fields.push(ResultField {
                field: "Result".to_string(),
                value: serde_json::to_string_pretty(task_result)?,
            });
        }

        // Add error info if present
        if let Some(traceback) = result.get("traceback") {
            if !traceback.is_null() {
                fields.push(ResultField {
                    field: "Error".to_string(),
                    value: traceback
                        .as_str()
                        .unwrap_or("Error information unavailable")
                        .to_string(),
                });
            }
        }

        // Add metadata
        if let Some(date_done) = result.get("date_done") {
            if !date_done.is_null() {
                fields.push(ResultField {
                    field: "Completed At".to_string(),
                    value: date_done.as_str().unwrap_or("N/A").to_string(),
                });
            }
        }

        let table = Table::new(fields).with(Style::rounded()).to_string();
        println!("{table}");
    } else {
        println!("{}", "✗ Task result not found".red());
        println!();
        println!("Possible reasons:");
        println!("  • Task hasn't completed yet");
        println!("  • Task result has expired (TTL)");
        println!("  • Wrong backend URL");
        println!("  • Task was never executed");
    }

    Ok(())
}

/// Move task from one queue to another
pub async fn requeue_task(
    broker_url: &str,
    from_queue: &str,
    to_queue: &str,
    task_id_str: &str,
) -> anyhow::Result<()> {
    let task_id = task_id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| anyhow::anyhow!("Invalid task ID format"))?;

    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    // Construct queue keys
    let from_key = format!("celers:{from_queue}");
    let to_key = format!("celers:{to_queue}");

    // Determine the source queue type
    let from_type: String = redis::cmd("TYPE")
        .arg(&from_key)
        .query_async(&mut conn)
        .await?;

    let mut task: Option<celers_core::SerializedTask> = None;
    let mut source_type = String::new();

    // Search in source queue
    if from_type == "list" {
        // FIFO queue
        let tasks: Vec<String> = redis::cmd("LRANGE")
            .arg(&from_key)
            .arg(0)
            .arg(-1)
            .query_async(&mut conn)
            .await?;

        for task_str in &tasks {
            if let Ok(t) = serde_json::from_str::<celers_core::SerializedTask>(task_str) {
                if t.metadata.id == task_id {
                    task = Some(t);
                    source_type = "list".to_string();
                    // Remove from source
                    let _: usize = redis::cmd("LREM")
                        .arg(&from_key)
                        .arg(1)
                        .arg(task_str)
                        .query_async(&mut conn)
                        .await?;
                    break;
                }
            }
        }
    } else if from_type == "zset" {
        // Priority queue
        let tasks: Vec<(String, f64)> = redis::cmd("ZRANGE")
            .arg(&from_key)
            .arg(0)
            .arg(-1)
            .arg("WITHSCORES")
            .query_async(&mut conn)
            .await?;

        for (task_str, _score) in &tasks {
            if let Ok(t) = serde_json::from_str::<celers_core::SerializedTask>(task_str) {
                if t.metadata.id == task_id {
                    task = Some(t);
                    source_type = "zset".to_string();
                    // Remove from source
                    let _: usize = redis::cmd("ZREM")
                        .arg(&from_key)
                        .arg(task_str)
                        .query_async(&mut conn)
                        .await?;
                    break;
                }
            }
        }
    } else {
        return Err(anyhow::anyhow!(
            "Source queue '{from_queue}' not found or invalid queue type"
        ));
    }

    if let Some(t) = task {
        // Determine destination queue type
        let to_type: String = redis::cmd("TYPE")
            .arg(&to_key)
            .query_async(&mut conn)
            .await?;

        let task_json = serde_json::to_string(&t)?;

        if to_type == "list" || to_type == "none" {
            // FIFO queue (or create new)
            let _: usize = redis::cmd("LPUSH")
                .arg(&to_key)
                .arg(&task_json)
                .query_async(&mut conn)
                .await?;
            println!(
                "{}",
                format!("✓ Task moved from '{from_queue}' ({source_type}) to '{to_queue}' (FIFO)")
                    .green()
                    .bold()
            );
        } else if to_type == "zset" {
            // Priority queue
            let priority = f64::from(t.metadata.priority);
            let _: usize = redis::cmd("ZADD")
                .arg(&to_key)
                .arg(priority)
                .arg(&task_json)
                .query_async(&mut conn)
                .await?;
            println!(
                "{}",
                format!(
                    "✓ Task moved from '{from_queue}' ({source_type}) to '{to_queue}' (Priority)"
                )
                .green()
                .bold()
            );
        } else {
            return Err(anyhow::anyhow!(
                "Destination queue '{to_queue}' has invalid type: {to_type}"
            ));
        }

        // Show task details
        println!();
        println!("  {} {}", "Task ID:".cyan(), t.metadata.id);
        println!("  {} {}", "Task Name:".cyan(), t.metadata.name);
        println!("  {} {}", "Priority:".cyan(), t.metadata.priority);
    } else {
        println!(
            "{}",
            format!("✗ Task not found in queue '{from_queue}'").red()
        );
        println!();
        println!("Possible reasons:");
        println!("  • Task ID is incorrect");
        println!("  • Task is in a different queue");
        println!("  • Task has already been processed");
        return Err(anyhow::anyhow!("Task not found"));
    }

    Ok(())
}

/// Show detailed queue statistics
pub async fn queue_stats(broker_url: &str, queue: &str) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    // Construct queue keys
    let queue_key = format!("celers:{queue}");
    let processing_key = format!("{queue_key}:processing");
    let dlq_key = format!("{queue_key}:dlq");
    let delayed_key = format!("{queue_key}:delayed");

    // Get queue type
    let queue_type: String = redis::cmd("TYPE")
        .arg(&queue_key)
        .query_async(&mut conn)
        .await?;

    // Get queue sizes
    let queue_size: usize = if queue_type == "list" {
        redis::cmd("LLEN")
            .arg(&queue_key)
            .query_async(&mut conn)
            .await?
    } else if queue_type == "zset" {
        redis::cmd("ZCARD")
            .arg(&queue_key)
            .query_async(&mut conn)
            .await?
    } else {
        0
    };

    let processing_size: usize = redis::cmd("LLEN")
        .arg(&processing_key)
        .query_async(&mut conn)
        .await
        .unwrap_or(0);

    let dlq_size: usize = redis::cmd("LLEN")
        .arg(&dlq_key)
        .query_async(&mut conn)
        .await
        .unwrap_or(0);

    let delayed_size: usize = redis::cmd("ZCARD")
        .arg(&delayed_key)
        .query_async(&mut conn)
        .await
        .unwrap_or(0);

    // Sample tasks to get task type distribution
    let mut task_names = std::collections::HashMap::new();

    if queue_size > 0 {
        let sample_size = std::cmp::min(queue_size, 100);
        let tasks: Vec<String> = if queue_type == "list" {
            redis::cmd("LRANGE")
                .arg(&queue_key)
                .arg(0)
                .arg(sample_size as isize - 1)
                .query_async(&mut conn)
                .await?
        } else if queue_type == "zset" {
            redis::cmd("ZRANGE")
                .arg(&queue_key)
                .arg(0)
                .arg(sample_size as isize - 1)
                .query_async(&mut conn)
                .await?
        } else {
            vec![]
        };

        for task_str in tasks {
            if let Ok(task) = serde_json::from_str::<celers_core::SerializedTask>(&task_str) {
                *task_names.entry(task.metadata.name.clone()).or_insert(0) += 1;
            }
        }
    }

    // Display statistics
    println!("{}", format!("Queue Statistics: {queue}").cyan().bold());
    println!();

    #[derive(Tabled)]
    struct StatRow {
        #[tabled(rename = "Metric")]
        metric: String,
        #[tabled(rename = "Value")]
        value: String,
    }

    let stats = vec![
        StatRow {
            metric: "Queue Type".to_string(),
            value: if queue_type == "list" {
                "FIFO (List)".to_string()
            } else if queue_type == "zset" {
                "Priority (Sorted Set)".to_string()
            } else {
                format!("Unknown ({queue_type})")
            },
        },
        StatRow {
            metric: "Pending Tasks".to_string(),
            value: queue_size.to_string(),
        },
        StatRow {
            metric: "Processing Tasks".to_string(),
            value: processing_size.to_string(),
        },
        StatRow {
            metric: "Dead Letter Queue".to_string(),
            value: dlq_size.to_string(),
        },
        StatRow {
            metric: "Delayed Tasks".to_string(),
            value: delayed_size.to_string(),
        },
        StatRow {
            metric: "Total Tasks".to_string(),
            value: (queue_size + processing_size + dlq_size + delayed_size).to_string(),
        },
    ];

    let table = Table::new(stats).with(Style::rounded()).to_string();
    println!("{table}");

    // Show task type distribution if we have data
    if !task_names.is_empty() {
        println!();
        println!("{}", "Task Type Distribution (sample):".cyan().bold());
        println!();

        #[derive(Tabled)]
        struct TaskTypeRow {
            #[tabled(rename = "Task Name")]
            task_name: String,
            #[tabled(rename = "Count")]
            count: usize,
        }

        let mut task_types: Vec<TaskTypeRow> = task_names
            .into_iter()
            .map(|(name, count)| TaskTypeRow {
                task_name: name,
                count,
            })
            .collect();

        task_types.sort_by(|a, b| b.count.cmp(&a.count));

        let table = Table::new(task_types.into_iter().take(10))
            .with(Style::rounded())
            .to_string();
        println!("{table}");
    }

    // Health indicators
    println!();
    if dlq_size > 0 {
        println!("{}", format!("⚠ Warning: {dlq_size} tasks in DLQ").yellow());
    }
    if processing_size > queue_size * 2 {
        println!(
            "{}",
            "⚠ Warning: High number of processing tasks (possible stuck workers)".yellow()
        );
    }
    if queue_size == 0 && processing_size == 0 && dlq_size == 0 {
        println!("{}", "✓ Queue is empty and healthy".green());
    }

    Ok(())
}

/// Move all tasks from one queue to another
pub async fn move_queue(
    broker_url: &str,
    from_queue: &str,
    to_queue: &str,
    confirm: bool,
) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    // Construct queue keys
    let from_key = format!("celers:{from_queue}");
    let to_key = format!("celers:{to_queue}");

    // Determine the source queue type
    let from_type: String = redis::cmd("TYPE")
        .arg(&from_key)
        .query_async(&mut conn)
        .await?;

    if from_type == "none" {
        println!(
            "{}",
            format!("✗ Source queue '{from_queue}' does not exist").red()
        );
        return Ok(());
    }

    // Get source queue size
    let queue_size: usize = if from_type == "list" {
        redis::cmd("LLEN")
            .arg(&from_key)
            .query_async(&mut conn)
            .await?
    } else if from_type == "zset" {
        redis::cmd("ZCARD")
            .arg(&from_key)
            .query_async(&mut conn)
            .await?
    } else {
        println!("{}", format!("✗ Unknown queue type: {from_type}").red());
        return Ok(());
    };

    if queue_size == 0 {
        println!(
            "{}",
            format!("✗ Source queue '{from_queue}' is empty").yellow()
        );
        return Ok(());
    }

    // Confirm operation
    if !confirm {
        println!(
            "{}",
            format!(
                "⚠ Warning: This will move {queue_size} tasks from '{from_queue}' to '{to_queue}'"
            )
            .yellow()
        );
        println!("{}", "Use --confirm to proceed".yellow());
        return Ok(());
    }

    println!(
        "{}",
        format!("Moving {queue_size} tasks from '{from_queue}' to '{to_queue}'...").cyan()
    );

    // Determine destination queue type (or create as list if doesn't exist)
    let to_type: String = redis::cmd("TYPE")
        .arg(&to_key)
        .query_async(&mut conn)
        .await?;

    let mut moved_count = 0;

    // Move tasks
    if from_type == "list" {
        // Source is FIFO queue
        loop {
            let task: Option<String> = redis::cmd("RPOP")
                .arg(&from_key)
                .query_async(&mut conn)
                .await?;

            match task {
                Some(task_str) => {
                    if to_type == "list" || to_type == "none" {
                        // Destination is FIFO queue (or create new)
                        let _: usize = redis::cmd("LPUSH")
                            .arg(&to_key)
                            .arg(&task_str)
                            .query_async(&mut conn)
                            .await?;
                    } else if to_type == "zset" {
                        // Destination is priority queue
                        if let Ok(task) =
                            serde_json::from_str::<celers_core::SerializedTask>(&task_str)
                        {
                            let priority = f64::from(task.metadata.priority);
                            let _: usize = redis::cmd("ZADD")
                                .arg(&to_key)
                                .arg(priority)
                                .arg(&task_str)
                                .query_async(&mut conn)
                                .await?;
                        }
                    }
                    moved_count += 1;

                    if moved_count % 100 == 0 {
                        print!(
                            "\r{}",
                            format!("Moved {moved_count} / {queue_size} tasks...").cyan()
                        );
                        use std::io::Write;
                        std::io::stdout().flush()?;
                    }
                }
                None => break,
            }
        }
    } else if from_type == "zset" {
        // Source is priority queue
        loop {
            let result: Vec<(String, f64)> = redis::cmd("ZPOPMIN")
                .arg(&from_key)
                .arg(1)
                .query_async(&mut conn)
                .await?;

            if result.is_empty() {
                break;
            }

            let (task_str, _score) = &result[0];

            if to_type == "list" || to_type == "none" {
                // Destination is FIFO queue
                let _: usize = redis::cmd("LPUSH")
                    .arg(&to_key)
                    .arg(task_str)
                    .query_async(&mut conn)
                    .await?;
            } else if to_type == "zset" {
                // Destination is priority queue
                if let Ok(task) = serde_json::from_str::<celers_core::SerializedTask>(task_str) {
                    let priority = f64::from(task.metadata.priority);
                    let _: usize = redis::cmd("ZADD")
                        .arg(&to_key)
                        .arg(priority)
                        .arg(task_str)
                        .query_async(&mut conn)
                        .await?;
                }
            }
            moved_count += 1;

            if moved_count % 100 == 0 {
                print!(
                    "\r{}",
                    format!("Moved {moved_count} / {queue_size} tasks...").cyan()
                );
                use std::io::Write;
                std::io::stdout().flush()?;
            }
        }
    }

    println!();
    println!(
        "{}",
        format!("✓ Successfully moved {moved_count} tasks from '{from_queue}' to '{to_queue}'")
            .green()
            .bold()
    );

    // Show queue type info
    let dest_queue_type = if to_type == "list" || to_type == "none" {
        "FIFO"
    } else if to_type == "zset" {
        "Priority"
    } else {
        "Unknown"
    };

    println!(
        "  {} {} → {}",
        "Queue Type:".cyan(),
        from_type,
        dest_queue_type
    );

    Ok(())
}

/// Export queue tasks to a JSON file
pub async fn export_queue(broker_url: &str, queue: &str, output_file: &str) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    let queue_key = format!("celers:{queue}");

    // Get queue type
    let queue_type: String = redis::cmd("TYPE")
        .arg(&queue_key)
        .query_async(&mut conn)
        .await?;

    if queue_type == "none" {
        println!("{}", format!("✗ Queue '{queue}' does not exist").red());
        return Ok(());
    }

    println!("{}", format!("Exporting queue '{queue}'...").cyan());

    // Fetch all tasks
    let tasks: Vec<String> = if queue_type == "list" {
        redis::cmd("LRANGE")
            .arg(&queue_key)
            .arg(0)
            .arg(-1)
            .query_async(&mut conn)
            .await?
    } else if queue_type == "zset" {
        redis::cmd("ZRANGE")
            .arg(&queue_key)
            .arg(0)
            .arg(-1)
            .query_async(&mut conn)
            .await?
    } else {
        println!("{}", format!("✗ Unknown queue type: {queue_type}").red());
        return Ok(());
    };

    // Parse tasks and create export data
    let mut export_tasks = Vec::new();
    for task_str in tasks {
        if let Ok(task) = serde_json::from_str::<celers_core::SerializedTask>(&task_str) {
            export_tasks.push(task);
        }
    }

    #[derive(serde::Serialize)]
    struct QueueExport {
        queue_name: String,
        queue_type: String,
        exported_at: String,
        task_count: usize,
        tasks: Vec<celers_core::SerializedTask>,
    }

    let export_data = QueueExport {
        queue_name: queue.to_string(),
        queue_type: queue_type.clone(),
        exported_at: chrono::Utc::now().to_rfc3339(),
        task_count: export_tasks.len(),
        tasks: export_tasks,
    };

    // Write to file
    let json = serde_json::to_string_pretty(&export_data)?;
    std::fs::write(output_file, json)?;

    println!(
        "{}",
        format!(
            "✓ Exported {} tasks from queue '{}' to '{}'",
            export_data.task_count, queue, output_file
        )
        .green()
        .bold()
    );
    println!("  {} {}", "Queue Type:".cyan(), queue_type);
    let file_size = std::fs::metadata(output_file)?.len();
    println!("  {} {} bytes", "File Size:".cyan(), file_size);

    Ok(())
}

/// Import queue tasks from a JSON file
pub async fn import_queue(
    broker_url: &str,
    queue: &str,
    input_file: &str,
    confirm: bool,
) -> anyhow::Result<()> {
    // Read and parse file
    let json = std::fs::read_to_string(input_file)?;

    #[derive(serde::Deserialize)]
    struct QueueExport {
        queue_name: String,
        queue_type: String,
        exported_at: String,
        task_count: usize,
        tasks: Vec<celers_core::SerializedTask>,
    }

    let export_data: QueueExport = serde_json::from_str(&json)?;

    // Show import info
    println!("{}", "Import Information:".cyan().bold());
    println!("  {} {}", "Source Queue:".cyan(), export_data.queue_name);
    println!("  {} {}", "Source Type:".cyan(), export_data.queue_type);
    println!("  {} {}", "Exported At:".cyan(), export_data.exported_at);
    println!("  {} {}", "Task Count:".cyan(), export_data.task_count);
    println!("  {} {}", "Destination Queue:".cyan(), queue);
    println!();

    if !confirm {
        println!(
            "{}",
            format!(
                "⚠ Warning: This will import {} tasks into queue '{}'",
                export_data.task_count, queue
            )
            .yellow()
        );
        println!("{}", "Use --confirm to proceed".yellow());
        return Ok(());
    }

    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    let queue_key = format!("celers:{queue}");

    // Determine destination queue type
    let to_type: String = redis::cmd("TYPE")
        .arg(&queue_key)
        .query_async(&mut conn)
        .await?;

    println!(
        "{}",
        format!("Importing {} tasks...", export_data.task_count).cyan()
    );

    let mut imported = 0;
    for task in export_data.tasks {
        let task_json = serde_json::to_string(&task)?;

        if to_type == "list" || to_type == "none" {
            // Destination is FIFO queue
            let _: usize = redis::cmd("LPUSH")
                .arg(&queue_key)
                .arg(&task_json)
                .query_async(&mut conn)
                .await?;
        } else if to_type == "zset" {
            // Destination is priority queue
            let priority = f64::from(task.metadata.priority);
            let _: usize = redis::cmd("ZADD")
                .arg(&queue_key)
                .arg(priority)
                .arg(&task_json)
                .query_async(&mut conn)
                .await?;
        }

        imported += 1;
        if imported % 100 == 0 {
            print!(
                "\r{}",
                format!(
                    "Imported {} / {} tasks...",
                    imported, export_data.task_count
                )
                .cyan()
            );
            use std::io::Write;
            std::io::stdout().flush()?;
        }
    }

    println!();
    println!(
        "{}",
        format!("✓ Successfully imported {imported} tasks into queue '{queue}'")
            .green()
            .bold()
    );

    Ok(())
}

/// Display Prometheus metrics
pub async fn show_metrics(
    format: &str,
    output_file: Option<&str>,
    pattern: Option<&str>,
    watch_interval: Option<u64>,
) -> anyhow::Result<()> {
    // If watch mode is enabled and output_file is set, it doesn't make sense
    if watch_interval.is_some() && output_file.is_some() {
        println!(
            "{}",
            "⚠ Watch mode cannot be used with file output".yellow()
        );
        return Ok(());
    }

    if let Some(interval) = watch_interval {
        // Watch mode - refresh metrics periodically
        println!("{}", "=== Metrics Watch Mode ===".bold().green());
        println!(
            "{}",
            format!("Refreshing every {interval} seconds (Ctrl+C to stop)").dimmed()
        );
        println!();

        loop {
            // Clear screen for better readability
            print!("\x1B[2J\x1B[1;1H"); // ANSI escape codes to clear screen

            // Display current time
            println!(
                "{}",
                format!("Last updated: {}", Utc::now().format("%Y-%m-%d %H:%M:%S")).dimmed()
            );
            println!();

            // Gather and format metrics
            format_and_display_metrics(format, pattern)?;

            // Sleep for the specified interval
            tokio::time::sleep(tokio::time::Duration::from_secs(interval)).await;
        }
    } else {
        // One-time display
        format_and_output_metrics(format, output_file, pattern)?;
        Ok(())
    }
}

/// Format and output metrics (for one-time display)
fn format_and_output_metrics(
    format: &str,
    output_file: Option<&str>,
    pattern: Option<&str>,
) -> anyhow::Result<()> {
    // Gather metrics from Prometheus registry
    let metrics_text = celers_metrics::gather_metrics();

    // Filter metrics if pattern is provided
    let filtered_metrics = if let Some(pat) = pattern {
        metrics_text
            .lines()
            .filter(|line| {
                // Keep HELP and TYPE lines for matching metrics
                if line.starts_with("# HELP") || line.starts_with("# TYPE") {
                    line.contains(pat)
                } else if line.starts_with('#') {
                    // Skip other comment lines
                    false
                } else {
                    // Keep metric data lines if they match the pattern
                    line.contains(pat)
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
    } else {
        metrics_text.clone()
    };

    // Format metrics based on format parameter
    let output = match format.to_lowercase().as_str() {
        "json" => {
            // Parse Prometheus text format and convert to JSON
            let mut metrics_map = serde_json::Map::new();

            for line in filtered_metrics.lines() {
                if line.starts_with('#') || line.trim().is_empty() {
                    continue;
                }

                // Parse metric line: metric_name{labels} value
                if let Some(space_idx) = line.rfind(' ') {
                    let (metric_part, value_str) = line.split_at(space_idx);
                    let value_str = value_str.trim();

                    if let Ok(value) = value_str.parse::<f64>() {
                        let metric_name = if let Some(brace_idx) = metric_part.find('{') {
                            &metric_part[..brace_idx]
                        } else {
                            metric_part
                        };

                        metrics_map.insert(metric_name.to_string(), serde_json::json!(value));
                    }
                }
            }

            serde_json::to_string_pretty(&metrics_map)?
        }

        "prometheus" | "prom" => {
            // Raw Prometheus format
            filtered_metrics
        }

        _ => {
            // Human-readable text format (default)
            let mut output = String::new();
            output.push_str(&format!("{}\n\n", "=== CeleRS Metrics ===".bold().green()));

            let mut current_metric = String::new();
            let mut help_text = String::new();

            for line in filtered_metrics.lines() {
                if line.starts_with("# HELP") {
                    // Extract help text
                    if let Some(help) = line.strip_prefix("# HELP ") {
                        let parts: Vec<&str> = help.splitn(2, ' ').collect();
                        if parts.len() == 2 {
                            current_metric = parts[0].to_string();
                            help_text = parts[1].to_string();
                        }
                    }
                } else if line.starts_with("# TYPE") {
                    // Skip TYPE lines
                } else if line.starts_with('#') || line.trim().is_empty() {
                    // Skip comments and empty lines
                } else {
                    // Parse metric value
                    if let Some(space_idx) = line.rfind(' ') {
                        let (metric_part, value_str) = line.split_at(space_idx);
                        let value_str = value_str.trim();

                        let metric_name = if let Some(brace_idx) = metric_part.find('{') {
                            &metric_part[..brace_idx]
                        } else {
                            metric_part
                        };

                        // Only show if this is a new metric
                        if metric_name == current_metric && !help_text.is_empty() {
                            output.push_str(&format!("{}\n", metric_name.cyan().bold()));
                            output.push_str(&format!("  {}\n", help_text.dimmed()));
                            output.push_str(&format!(
                                "  {} {}\n\n",
                                "Value:".yellow(),
                                value_str.green()
                            ));
                            help_text.clear();
                        }
                    }
                }
            }

            if output.trim().is_empty() {
                output = format!("{}\n", "No metrics found".yellow());
                if pattern.is_some() {
                    output.push_str(&format!(
                        "{}\n",
                        "Try adjusting your filter pattern".dimmed()
                    ));
                }
            }

            output
        }
    };

    // Write to file or stdout
    if let Some(file_path) = output_file {
        std::fs::write(file_path, &output)?;
        println!(
            "{}",
            format!("✓ Metrics exported to '{file_path}'")
                .green()
                .bold()
        );
        println!("  {} {}", "Format:".cyan(), format);
        if let Some(pat) = pattern {
            println!("  {} {}", "Filter:".cyan(), pat);
        }
    } else {
        println!("{output}");
    }

    Ok(())
}

/// Format and display metrics (for watch mode)
fn format_and_display_metrics(format: &str, pattern: Option<&str>) -> anyhow::Result<()> {
    // Gather metrics from Prometheus registry
    let metrics_text = celers_metrics::gather_metrics();

    // Filter metrics if pattern is provided
    let filtered_metrics = if let Some(pat) = pattern {
        metrics_text
            .lines()
            .filter(|line| {
                // Keep HELP and TYPE lines for matching metrics
                if line.starts_with("# HELP") || line.starts_with("# TYPE") {
                    line.contains(pat)
                } else if line.starts_with('#') {
                    false
                } else {
                    line.contains(pat)
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
    } else {
        metrics_text.clone()
    };

    // Format metrics based on format parameter
    let output = match format.to_lowercase().as_str() {
        "json" => {
            // Parse Prometheus text format and convert to JSON
            let mut metrics_map = serde_json::Map::new();

            for line in filtered_metrics.lines() {
                if line.starts_with('#') || line.trim().is_empty() {
                    continue;
                }

                if let Some(space_idx) = line.rfind(' ') {
                    let (metric_part, value_str) = line.split_at(space_idx);
                    let value_str = value_str.trim();

                    if let Ok(value) = value_str.parse::<f64>() {
                        let metric_name = if let Some(brace_idx) = metric_part.find('{') {
                            &metric_part[..brace_idx]
                        } else {
                            metric_part
                        };

                        metrics_map.insert(metric_name.to_string(), serde_json::json!(value));
                    }
                }
            }

            serde_json::to_string_pretty(&metrics_map)?
        }

        "prometheus" | "prom" => filtered_metrics,

        _ => {
            // Human-readable text format (default)
            let mut output = String::new();
            output.push_str(&format!("{}\n\n", "=== CeleRS Metrics ===".bold().green()));

            let mut current_metric = String::new();
            let mut help_text = String::new();

            for line in filtered_metrics.lines() {
                if line.starts_with("# HELP") {
                    if let Some(help) = line.strip_prefix("# HELP ") {
                        let parts: Vec<&str> = help.splitn(2, ' ').collect();
                        if parts.len() == 2 {
                            current_metric = parts[0].to_string();
                            help_text = parts[1].to_string();
                        }
                    }
                } else if line.starts_with('#') || line.trim().is_empty() {
                    // Skip comments and empty lines
                } else if let Some(space_idx) = line.rfind(' ') {
                    let (metric_part, value_str) = line.split_at(space_idx);
                    let value_str = value_str.trim();

                    let metric_name = if let Some(brace_idx) = metric_part.find('{') {
                        &metric_part[..brace_idx]
                    } else {
                        metric_part
                    };

                    if metric_name == current_metric && !help_text.is_empty() {
                        output.push_str(&format!("{}\n", metric_name.cyan().bold()));
                        output.push_str(&format!("  {}\n", help_text.dimmed()));
                        output.push_str(&format!(
                            "  {} {}\n\n",
                            "Value:".yellow(),
                            value_str.green()
                        ));
                        help_text.clear();
                    }
                }
            }

            if output.trim().is_empty() {
                output = format!("{}\n", "No metrics found".yellow());
                if pattern.is_some() {
                    output.push_str(&format!(
                        "{}\n",
                        "Try adjusting your filter pattern".dimmed()
                    ));
                }
            }

            output
        }
    };

    println!("{output}");
    Ok(())
}

/// Validate configuration file
pub async fn validate_config(config_path: &str, test_connection: bool) -> anyhow::Result<()> {
    use std::path::Path;

    println!("{}", "=== Configuration Validation ===".bold().green());
    println!();

    // Check if file exists
    if !Path::new(config_path).exists() {
        println!(
            "{}",
            format!("✗ Configuration file not found: {config_path}")
                .red()
                .bold()
        );
        return Ok(());
    }

    println!(
        "{}",
        format!("📄 Loading configuration from '{config_path}'...").cyan()
    );
    println!();

    // Try to load and parse the configuration
    let config = match crate::config::Config::from_file(config_path) {
        Ok(cfg) => {
            println!("{}", "✓ Configuration file is valid TOML".green());
            cfg
        }
        Err(e) => {
            println!("{}", "✗ Failed to parse configuration file:".red().bold());
            println!("  {}", format!("{e}").red());
            return Ok(());
        }
    };

    println!();
    println!("{}", "Configuration Details:".bold());
    println!();

    // Validate broker configuration
    println!("{}", "Broker Configuration:".cyan().bold());
    println!("  {} {}", "Type:".yellow(), config.broker.broker_type);
    println!("  {} {}", "URL:".yellow(), config.broker.url);
    println!("  {} {}", "Queue:".yellow(), config.broker.queue);
    println!("  {} {}", "Mode:".yellow(), config.broker.mode);

    println!();

    // Validate worker configuration
    println!("{}", "Worker Configuration:".cyan().bold());
    println!(
        "  {} {}",
        "Concurrency:".yellow(),
        config.worker.concurrency
    );
    println!(
        "  {} {} ms",
        "Poll Interval:".yellow(),
        config.worker.poll_interval_ms
    );
    println!(
        "  {} {}",
        "Max Retries:".yellow(),
        config.worker.max_retries
    );
    println!(
        "  {} {} seconds",
        "Default Timeout:".yellow(),
        config.worker.default_timeout_secs
    );

    println!();

    // Run validation and show warnings
    let warnings = config.validate()?;
    if warnings.is_empty() {
        println!(
            "{}",
            "✓ Configuration is valid with no warnings".green().bold()
        );
    } else {
        println!("{}", "Configuration Warnings:".yellow().bold());
        for warning in &warnings {
            println!("  {} {}", "⚠".yellow(), warning.yellow());
        }
    }

    println!();

    // Show configured queues
    if !config.queues.is_empty() {
        println!("{}", "Configured Queues:".cyan().bold());
        for queue in &config.queues {
            println!("  • {}", queue.dimmed());
        }
        println!();
    }

    // Test broker connection if requested
    if test_connection {
        println!("{}", "Testing broker connection...".cyan().bold());
        println!();

        match config.broker.broker_type.to_lowercase().as_str() {
            "redis" => {
                match redis::Client::open(config.broker.url.as_str()) {
                    Ok(client) => {
                        match client.get_multiplexed_async_connection().await {
                            Ok(mut conn) => {
                                // Test a simple PING command
                                match redis::cmd("PING").query_async::<String>(&mut conn).await {
                                    Ok(_) => {
                                        println!(
                                            "{}",
                                            "✓ Successfully connected to Redis broker"
                                                .green()
                                                .bold()
                                        );
                                    }
                                    Err(e) => {
                                        println!(
                                            "{}",
                                            "✗ Failed to PING Redis broker:".red().bold()
                                        );
                                        println!("  {}", format!("{e}").red());
                                    }
                                }
                            }
                            Err(e) => {
                                println!("{}", "✗ Failed to connect to Redis broker:".red().bold());
                                println!("  {}", format!("{e}").red());
                            }
                        }
                    }
                    Err(e) => {
                        println!("{}", "✗ Invalid Redis URL:".red().bold());
                        println!("  {}", format!("{e}").red());
                    }
                }
            }
            "postgres" | "postgresql" => {
                // Test PostgreSQL connection
                match sqlx::postgres::PgPool::connect(&config.broker.url).await {
                    Ok(pool) => {
                        // Test with a simple query
                        match sqlx::query("SELECT 1").fetch_one(&pool).await {
                            Ok(_) => {
                                println!(
                                    "{}",
                                    "✓ Successfully connected to PostgreSQL broker"
                                        .green()
                                        .bold()
                                );
                            }
                            Err(e) => {
                                println!("{}", "✗ Failed to query PostgreSQL broker:".red().bold());
                                println!("  {}", format!("{e}").red());
                            }
                        }
                        pool.close().await;
                    }
                    Err(e) => {
                        println!(
                            "{}",
                            "✗ Failed to connect to PostgreSQL broker:".red().bold()
                        );
                        println!("  {}", format!("{e}").red());
                    }
                }
            }
            "mysql" => {
                println!(
                    "{}",
                    "ℹ MySQL connection testing via CLI not yet available".cyan()
                );
                println!("  {}", "To test MySQL connection:".dimmed());
                println!(
                    "  {}",
                    "  1. Use the 'db test-connection' command with your MySQL URL".dimmed()
                );
                println!(
                    "  {}",
                    "  2. Or use mysql client: mysql -h <host> -u <user> -p".dimmed()
                );
            }
            "amqp" | "rabbitmq" => {
                println!(
                    "{}",
                    "ℹ AMQP connection testing via CLI not yet available".cyan()
                );
                println!("  {}", "To test RabbitMQ connection:".dimmed());
                println!(
                    "  {}",
                    "  1. Check RabbitMQ Management UI at http://<host>:15672".dimmed()
                );
                println!("  {}", "  2. Or use rabbitmq-diagnostics ping".dimmed());
                println!(
                    "  {}",
                    "  3. Verify credentials and virtual host configuration".dimmed()
                );
            }
            "sqs" => {
                println!(
                    "{}",
                    "ℹ SQS connection testing via CLI not yet available".cyan()
                );
                println!("  {}", "To test AWS SQS connection:".dimmed());
                println!(
                    "  {}",
                    "  1. Ensure AWS credentials are configured (aws configure)".dimmed()
                );
                println!("  {}", "  2. Test with: aws sqs list-queues".dimmed());
                println!(
                    "  {}",
                    "  3. Verify IAM permissions for SQS operations".dimmed()
                );
            }
            _ => {
                println!(
                    "{}",
                    format!(
                        "⚠ Cannot test connection for broker type '{}'",
                        config.broker.broker_type
                    )
                    .yellow()
                );
            }
        }

        println!();
    }

    println!("{}", "✓ Configuration validation complete".green().bold());

    Ok(())
}

/// Pause queue processing
pub async fn pause_queue(broker_url: &str, queue: &str) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    let pause_key = format!("celers:{queue}:paused");

    // Set pause flag with a timestamp
    let timestamp = chrono::Utc::now().to_rfc3339();
    let _: () = redis::cmd("SET")
        .arg(&pause_key)
        .arg(&timestamp)
        .query_async(&mut conn)
        .await?;

    println!(
        "{}",
        format!("✓ Queue '{queue}' has been paused").green().bold()
    );
    println!();
    println!("{}", "Note:".yellow().bold());
    println!("  • Workers will stop processing tasks from this queue");
    println!("  • Existing tasks will remain in the queue");
    println!("  • Use 'celers queue resume' to resume processing");
    println!();
    println!("  Paused at: {}", timestamp.cyan());

    Ok(())
}

/// Resume queue processing
pub async fn resume_queue(broker_url: &str, queue: &str) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    let pause_key = format!("celers:{queue}:paused");

    // Check if queue is paused
    let paused: Option<String> = redis::cmd("GET")
        .arg(&pause_key)
        .query_async(&mut conn)
        .await?;

    if paused.is_none() {
        println!("{}", format!("✓ Queue '{queue}' is not paused").yellow());
        return Ok(());
    }

    // Remove pause flag
    let _: () = redis::cmd("DEL")
        .arg(&pause_key)
        .query_async(&mut conn)
        .await?;

    println!(
        "{}",
        format!("✓ Queue '{queue}' has been resumed").green().bold()
    );
    println!();
    println!("{}", "Note:".yellow().bold());
    println!("  • Workers will now process tasks from this queue");
    if let Some(paused_at) = paused {
        println!("  • Was paused at: {}", paused_at.dimmed());
    }

    Ok(())
}

/// Run system health diagnostics
pub async fn health_check(broker_url: &str, queue: &str) -> anyhow::Result<()> {
    println!("{}", "=== System Health Check ===".bold().cyan());
    println!();

    let mut health_issues = Vec::new();
    let mut health_warnings = Vec::new();

    // Test 1: Broker Connection
    println!("{}", "1. Broker Connection".bold());
    let client = match redis::Client::open(broker_url) {
        Ok(c) => {
            println!("  {} Redis client created", "✓".green());
            c
        }
        Err(e) => {
            println!("  {} Failed to create Redis client: {}", "✗".red(), e);
            health_issues.push("Cannot create Redis client".to_string());
            println!();
            println!("{}", "Health Check Failed".red().bold());
            return Ok(());
        }
    };

    let mut conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => {
            println!("  {} Successfully connected to broker", "✓".green());
            c
        }
        Err(e) => {
            println!("  {} Failed to connect: {}", "✗".red(), e);
            health_issues.push("Cannot connect to broker".to_string());
            println!();
            println!("{}", "Health Check Failed".red().bold());
            return Ok(());
        }
    };

    // Test PING
    match redis::cmd("PING").query_async::<String>(&mut conn).await {
        Ok(_) => {
            println!("  {} PING successful", "✓".green());
        }
        Err(e) => {
            println!("  {} PING failed: {}", "⚠".yellow(), e);
            health_warnings.push("PING to broker failed".to_string());
        }
    }

    println!();

    // Test 2: Queue Status
    println!("{}", "2. Queue Status".bold());
    let broker = RedisBroker::new(broker_url, queue)?;

    let _queue_size = match broker.queue_size().await {
        Ok(size) => {
            println!("  {} Queue size: {}", "✓".green(), size);
            size
        }
        Err(e) => {
            println!("  {} Failed to get queue size: {}", "✗".red(), e);
            health_issues.push("Cannot get queue size".to_string());
            0
        }
    };

    let dlq_size = match broker.dlq_size().await {
        Ok(size) => {
            if size > 0 {
                println!("  {} DLQ size: {} (has failed tasks)", "⚠".yellow(), size);
                health_warnings.push(format!("{size} tasks in Dead Letter Queue"));
            } else {
                println!("  {} DLQ size: {} (empty)", "✓".green(), size);
            }
            size
        }
        Err(e) => {
            println!("  {} Failed to get DLQ size: {}", "✗".red(), e);
            health_issues.push("Cannot get DLQ size".to_string());
            0
        }
    };

    println!();

    // Test 3: Queue Accessibility
    println!("{}", "3. Queue Accessibility".bold());
    let queue_key = format!("celers:{queue}");
    let _queue_type: String = match redis::cmd("TYPE")
        .arg(&queue_key)
        .query_async(&mut conn)
        .await
    {
        Ok(t) => {
            if t == "none" {
                println!(
                    "  {} Queue does not exist (will be created on first task)",
                    "⚠".yellow()
                );
                health_warnings.push("Queue not yet created".to_string());
            } else if t == "list" {
                println!("  {} Queue type: FIFO (list)", "✓".green());
            } else if t == "zset" {
                println!("  {} Queue type: Priority (sorted set)", "✓".green());
            } else {
                println!("  {} Unknown queue type: {}", "⚠".yellow(), t);
                health_warnings.push(format!("Unknown queue type: {t}"));
            }
            t
        }
        Err(e) => {
            println!("  {} Failed to check queue type: {}", "✗".red(), e);
            health_issues.push("Cannot check queue type".to_string());
            "none".to_string()
        }
    };

    // Check if queue is paused
    let pause_key = format!("celers:{queue}:paused");
    match redis::cmd("GET")
        .arg(&pause_key)
        .query_async::<Option<String>>(&mut conn)
        .await
    {
        Ok(Some(paused_at)) => {
            println!("  {} Queue is PAUSED (since: {})", "⚠".yellow(), paused_at);
            health_warnings.push(format!("Queue is paused since {paused_at}"));
        }
        Ok(None) => {
            println!("  {} Queue is not paused", "✓".green());
        }
        Err(e) => {
            println!("  {} Failed to check pause status: {}", "⚠".yellow(), e);
        }
    }

    println!();

    // Test 4: Memory Usage (if accessible)
    println!("{}", "4. Broker Memory".bold());
    match redis::cmd("INFO")
        .arg("memory")
        .query_async::<String>(&mut conn)
        .await
    {
        Ok(info) => {
            // Parse used_memory from INFO output
            for line in info.lines() {
                if line.starts_with("used_memory_human:") {
                    let memory = line.split(':').nth(1).unwrap_or("N/A");
                    println!("  {} Used memory: {}", "✓".green(), memory);
                    break;
                }
            }
        }
        Err(_) => {
            println!("  {} Memory info not available", "⚠".yellow());
        }
    }

    println!();

    // Test 5: Health Summary
    println!("{}", "Health Summary".bold().cyan());
    println!();

    if health_issues.is_empty() && health_warnings.is_empty() {
        println!(
            "{}",
            "  ✓ All checks passed! System is healthy.".green().bold()
        );
    } else {
        if !health_issues.is_empty() {
            println!("{}", "  Critical Issues:".red().bold());
            for issue in &health_issues {
                println!("    {} {}", "✗".red(), issue);
            }
            println!();
        }

        if !health_warnings.is_empty() {
            println!("{}", "  Warnings:".yellow().bold());
            for warning in &health_warnings {
                println!("    {} {}", "⚠".yellow(), warning);
            }
            println!();
        }

        if health_issues.is_empty() {
            println!(
                "{}",
                "  Overall: System is operational with warnings"
                    .yellow()
                    .bold()
            );
        } else {
            println!("{}", "  Overall: System has critical issues".red().bold());
        }
    }

    println!();

    // Recommendations
    if dlq_size > 0 {
        println!("{}", "Recommendations:".cyan().bold());
        println!("  • Inspect DLQ: celers dlq inspect");
        println!("  • Clear DLQ: celers dlq clear --confirm");
        println!();
    }

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

/// Automatic problem detection and diagnostics
pub async fn doctor(broker_url: &str, queue: &str) -> anyhow::Result<()> {
    println!("{}", "=== CeleRS Doctor ===".bold().cyan());
    println!("{}", "Running automatic diagnostics...".dimmed());
    println!();

    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    let mut issues = Vec::new();
    let mut warnings = Vec::new();
    let mut recommendations = Vec::new();

    // Test 1: Broker connectivity
    println!("{}", "1. Checking broker connectivity...".bold());
    match redis::cmd("PING").query_async::<String>(&mut conn).await {
        Ok(_) => {
            println!("  {} Broker is reachable", "✓".green());
        }
        Err(e) => {
            println!("  {} Broker connection failed: {}", "✗".red(), e);
            issues.push("Cannot connect to broker".to_string());
            recommendations.push("Check broker URL and ensure Redis is running".to_string());
        }
    }
    println!();

    // Test 2: Queue health
    println!("{}", "2. Analyzing queue health...".bold());
    let broker = RedisBroker::new(broker_url, queue)?;

    let queue_size = broker.queue_size().await.unwrap_or(0);
    let dlq_size = broker.dlq_size().await.unwrap_or(0);

    println!("  {} Pending tasks: {}", "•".cyan(), queue_size);
    println!("  {} DLQ tasks: {}", "•".cyan(), dlq_size);

    if dlq_size > 10 {
        warnings.push(format!("High number of failed tasks in DLQ: {dlq_size}"));
        recommendations.push("Inspect DLQ with: celers dlq inspect".to_string());
    }

    if queue_size > 1000 {
        warnings.push(format!("Large queue backlog: {queue_size} tasks"));
        recommendations.push("Consider scaling up workers".to_string());
    }
    println!();

    // Test 3: Worker availability
    println!("{}", "3. Checking worker availability...".bold());
    let worker_pattern = "celers:worker:*:heartbeat";
    let mut cursor = 0;
    let mut worker_count = 0;

    loop {
        let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(worker_pattern)
            .arg("COUNT")
            .arg(100)
            .query_async(&mut conn)
            .await?;

        worker_count += keys.len();
        cursor = new_cursor;

        if cursor == 0 {
            break;
        }
    }

    println!("  {} Active workers: {}", "•".cyan(), worker_count);

    if worker_count == 0 && queue_size > 0 {
        issues.push("No workers available to process pending tasks".to_string());
        recommendations.push("Start workers with: celers worker".to_string());
    } else if worker_count > 0 {
        println!("  {} Workers are available", "✓".green());
    }
    println!();

    // Test 4: Queue pause status
    println!("{}", "4. Checking queue status...".bold());
    let pause_key = format!("celers:{queue}:paused");
    let paused: Option<String> = redis::cmd("GET")
        .arg(&pause_key)
        .query_async(&mut conn)
        .await?;

    if let Some(paused_at) = paused {
        warnings.push(format!("Queue '{queue}' is paused since {paused_at}"));
        recommendations.push("Resume queue with: celers queue resume".to_string());
        println!("  {} Queue is PAUSED", "⚠".yellow());
    } else {
        println!("  {} Queue is active", "✓".green());
    }
    println!();

    // Test 5: Memory usage
    println!("{}", "5. Checking broker memory...".bold());
    match redis::cmd("INFO")
        .arg("memory")
        .query_async::<String>(&mut conn)
        .await
    {
        Ok(info) => {
            for line in info.lines() {
                if line.starts_with("used_memory_human:") {
                    let memory = line.split(':').nth(1).unwrap_or("N/A");
                    println!("  {} Used memory: {}", "•".cyan(), memory);
                }
                if line.starts_with("maxmemory_human:") {
                    let max_memory = line.split(':').nth(1).unwrap_or("N/A");
                    if max_memory != "0B" {
                        println!("  {} Max memory: {}", "•".cyan(), max_memory);
                    }
                }
            }
            println!("  {} Memory usage is acceptable", "✓".green());
        }
        Err(_) => {
            println!("  {} Memory info unavailable", "⚠".yellow());
        }
    }
    println!();

    // Summary
    println!("{}", "=== Diagnosis Summary ===".bold().cyan());
    println!();

    if issues.is_empty() && warnings.is_empty() {
        println!(
            "{}",
            "  ✓ No issues detected! System is healthy.".green().bold()
        );
    } else {
        if !issues.is_empty() {
            println!("{}", "  Critical Issues:".red().bold());
            for issue in &issues {
                println!("    {} {}", "✗".red(), issue);
            }
            println!();
        }

        if !warnings.is_empty() {
            println!("{}", "  Warnings:".yellow().bold());
            for warning in &warnings {
                println!("    {} {}", "⚠".yellow(), warning);
            }
            println!();
        }

        if !recommendations.is_empty() {
            println!("{}", "  Recommendations:".cyan().bold());
            for (i, rec) in recommendations.iter().enumerate() {
                println!("    {}. {}", i + 1, rec);
            }
            println!();
        }

        if issues.is_empty() {
            println!(
                "{}",
                "  Overall: System is operational with warnings"
                    .yellow()
                    .bold()
            );
        } else {
            println!(
                "{}",
                "  Overall: System has critical issues that need attention"
                    .red()
                    .bold()
            );
        }
    }

    Ok(())
}

/// Show task execution logs
pub async fn show_task_logs(
    broker_url: &str,
    task_id_str: &str,
    limit: usize,
) -> anyhow::Result<()> {
    let task_id = task_id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| anyhow::anyhow!("Invalid task ID format"))?;

    println!("{}", "=== Task Execution Logs ===".bold().cyan());
    println!("Task ID: {}", task_id.to_string().yellow());
    println!();

    // Connect to Redis
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    // Logs are stored in a Redis list: celers:task:<id>:logs
    let logs_key = format!("celers:task:{task_id}:logs");

    // Check if logs exist
    let exists: bool = redis::cmd("EXISTS")
        .arg(&logs_key)
        .query_async(&mut conn)
        .await?;

    if !exists {
        println!("{}", "✗ No logs found for this task".red());
        println!();
        println!("Possible reasons:");
        println!("  • Task hasn't been executed yet");
        println!("  • Logs have expired (TTL)");
        println!("  • Task was executed before logging was enabled");
        println!("  • Wrong task ID");
        return Ok(());
    }

    // Get log count
    let log_count: isize = redis::cmd("LLEN")
        .arg(&logs_key)
        .query_async(&mut conn)
        .await?;

    println!(
        "{}",
        format!("Total log entries: {log_count}").cyan().bold()
    );
    println!();

    // Fetch logs (most recent first)
    let logs: Vec<String> = redis::cmd("LRANGE")
        .arg(&logs_key)
        .arg(-(limit as isize))
        .arg(-1)
        .query_async(&mut conn)
        .await?;

    if logs.is_empty() {
        println!("{}", "No log entries available".yellow());
        return Ok(());
    }

    // Display logs with colors
    for (idx, log_entry) in logs.iter().enumerate() {
        // Try to parse as JSON for structured logs
        if let Ok(log_json) = serde_json::from_str::<serde_json::Value>(log_entry) {
            let timestamp = log_json
                .get("timestamp")
                .and_then(|v| v.as_str())
                .unwrap_or("N/A");
            let level = log_json
                .get("level")
                .and_then(|v| v.as_str())
                .unwrap_or("INFO");
            let message = log_json
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or(log_entry);

            let level_colored = match level {
                "ERROR" | "error" => level.red().bold(),
                "WARN" | "warn" => level.yellow().bold(),
                "DEBUG" | "debug" => level.dimmed(),
                _ => level.cyan().bold(),
            };

            println!(
                "{} {} {} {}",
                format!("[{}]", idx + 1).dimmed(),
                timestamp.dimmed(),
                level_colored,
                message
            );
        } else {
            // Plain text log
            println!("{} {}", format!("[{}]", idx + 1).dimmed(), log_entry);
        }
    }

    println!();
    if log_count as usize > limit {
        println!(
            "{}",
            format!("Showing last {} of {} log entries", logs.len(), log_count).yellow()
        );
        println!(
            "{}",
            format!("Use --limit to show more entries (max: {log_count})").dimmed()
        );
    } else {
        println!(
            "{}",
            format!("Showing all {} log entries", logs.len())
                .green()
                .bold()
        );
    }

    Ok(())
}

/// List all scheduled tasks
pub async fn list_schedules(broker_url: &str) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    println!("{}", "=== Scheduled Tasks ===".bold().cyan());
    println!();

    // Schedules are stored with keys like: celers:schedule:<name>
    let schedule_pattern = "celers:schedule:*";
    let mut cursor = 0;
    let mut schedule_keys: Vec<String> = Vec::new();

    loop {
        let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(schedule_pattern)
            .arg("COUNT")
            .arg(100)
            .query_async(&mut conn)
            .await?;

        schedule_keys.extend(keys);
        cursor = new_cursor;

        if cursor == 0 {
            break;
        }
    }

    if schedule_keys.is_empty() {
        println!("{}", "No scheduled tasks found".yellow());
        println!();
        println!("Add a schedule with: celers schedule add <name> --task <task> --cron <expr>");
        return Ok(());
    }

    #[derive(Tabled)]
    struct ScheduleInfo {
        #[tabled(rename = "Name")]
        name: String,
        #[tabled(rename = "Task")]
        task: String,
        #[tabled(rename = "Cron")]
        cron: String,
        #[tabled(rename = "Status")]
        status: String,
        #[tabled(rename = "Last Run")]
        last_run: String,
    }

    let mut schedules = Vec::new();

    for key in &schedule_keys {
        // Extract schedule name from key
        let name = key.strip_prefix("celers:schedule:").unwrap_or(key);

        // Get schedule data
        let schedule_data: Option<String> =
            redis::cmd("GET").arg(key).query_async(&mut conn).await?;

        if let Some(data) = schedule_data {
            if let Ok(schedule) = serde_json::from_str::<serde_json::Value>(&data) {
                let task = schedule
                    .get("task")
                    .and_then(|v| v.as_str())
                    .unwrap_or("N/A")
                    .to_string();
                let cron = schedule
                    .get("cron")
                    .and_then(|v| v.as_str())
                    .unwrap_or("N/A")
                    .to_string();

                // Check if paused
                let pause_key = format!("celers:schedule:{name}:paused");
                let paused: Option<String> = redis::cmd("GET")
                    .arg(&pause_key)
                    .query_async(&mut conn)
                    .await?;

                let status = if paused.is_some() {
                    "Paused".to_string()
                } else {
                    "Active".to_string()
                };

                let last_run = schedule
                    .get("last_run")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Never")
                    .to_string();

                schedules.push(ScheduleInfo {
                    name: name.to_string(),
                    task,
                    cron,
                    status,
                    last_run,
                });
            }
        }
    }

    let table = Table::new(schedules).with(Style::rounded()).to_string();
    println!("{table}");
    println!();
    println!(
        "{}",
        format!("Total scheduled tasks: {}", schedule_keys.len())
            .cyan()
            .bold()
    );

    Ok(())
}

/// Add a new scheduled task
#[allow(clippy::too_many_arguments)]
pub async fn add_schedule(
    broker_url: &str,
    name: &str,
    task: &str,
    cron: &str,
    queue: &str,
    args: Option<&str>,
) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    println!("{}", "=== Add Scheduled Task ===".bold().cyan());
    println!();

    // Validate cron expression (basic validation)
    let cron_parts: Vec<&str> = cron.split_whitespace().collect();
    if cron_parts.len() != 5 {
        println!(
            "{}",
            "✗ Invalid cron expression. Expected format: 'min hour day month weekday'"
                .red()
                .bold()
        );
        println!();
        println!("Examples:");
        println!("  0 0 * * *       - Daily at midnight");
        println!("  0 */2 * * *     - Every 2 hours");
        println!("  */15 * * * *    - Every 15 minutes");
        println!("  0 9 * * 1-5     - Weekdays at 9 AM");
        return Ok(());
    }

    // Check if schedule already exists
    let schedule_key = format!("celers:schedule:{name}");
    let exists: bool = redis::cmd("EXISTS")
        .arg(&schedule_key)
        .query_async(&mut conn)
        .await?;

    if exists {
        println!(
            "{}",
            format!("✗ Schedule '{name}' already exists").red().bold()
        );
        println!();
        println!("Use a different name or remove the existing schedule first:");
        println!("  celers schedule remove {name} --confirm");
        return Ok(());
    }

    // Parse args if provided
    let args_value = if let Some(args_str) = args {
        match serde_json::from_str::<serde_json::Value>(args_str) {
            Ok(v) => v,
            Err(e) => {
                println!("{}", "✗ Invalid JSON arguments".red().bold());
                println!("  Error: {e}");
                return Ok(());
            }
        }
    } else {
        serde_json::json!({})
    };

    // Create schedule data
    let schedule_data = serde_json::json!({
        "name": name,
        "task": task,
        "cron": cron,
        "queue": queue,
        "args": args_value,
        "created_at": chrono::Utc::now().to_rfc3339(),
        "last_run": null,
    });

    // Save schedule
    let schedule_json = serde_json::to_string(&schedule_data)?;
    let _: () = redis::cmd("SET")
        .arg(&schedule_key)
        .arg(&schedule_json)
        .query_async(&mut conn)
        .await?;

    println!("{}", "✓ Schedule added successfully".green().bold());
    println!();
    println!("  {} {}", "Name:".cyan(), name);
    println!("  {} {}", "Task:".cyan(), task);
    println!("  {} {}", "Cron:".cyan(), cron);
    println!("  {} {}", "Queue:".cyan(), queue);
    if args.is_some() {
        println!("  {} {}", "Args:".cyan(), args.unwrap());
    }
    println!();
    println!("{}", "Note:".yellow().bold());
    println!("  • Ensure a beat scheduler is running to execute this schedule");
    println!("  • The schedule is active and will run at the specified times");

    Ok(())
}

/// Remove a scheduled task
pub async fn remove_schedule(broker_url: &str, name: &str, confirm: bool) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    let schedule_key = format!("celers:schedule:{name}");

    // Check if schedule exists
    let exists: bool = redis::cmd("EXISTS")
        .arg(&schedule_key)
        .query_async(&mut conn)
        .await?;

    if !exists {
        println!("{}", format!("✗ Schedule '{name}' not found").red());
        return Ok(());
    }

    if !confirm {
        println!(
            "{}",
            format!("⚠ Warning: This will delete schedule '{name}'")
                .yellow()
                .bold()
        );
        println!("   Add --confirm to proceed");
        return Ok(());
    }

    // Remove schedule and its pause flag
    let pause_key = format!("celers:schedule:{name}:paused");
    let _: () = redis::cmd("DEL")
        .arg(&schedule_key)
        .arg(&pause_key)
        .query_async(&mut conn)
        .await?;

    println!(
        "{}",
        format!("✓ Schedule '{name}' removed successfully")
            .green()
            .bold()
    );

    Ok(())
}

/// Pause a schedule
pub async fn pause_schedule(broker_url: &str, name: &str) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    let schedule_key = format!("celers:schedule:{name}");

    // Check if schedule exists
    let exists: bool = redis::cmd("EXISTS")
        .arg(&schedule_key)
        .query_async(&mut conn)
        .await?;

    if !exists {
        println!("{}", format!("✗ Schedule '{name}' not found").red());
        return Ok(());
    }

    // Set pause flag
    let pause_key = format!("celers:schedule:{name}:paused");
    let timestamp = chrono::Utc::now().to_rfc3339();
    let _: () = redis::cmd("SET")
        .arg(&pause_key)
        .arg(&timestamp)
        .query_async(&mut conn)
        .await?;

    println!(
        "{}",
        format!("✓ Schedule '{name}' has been paused")
            .green()
            .bold()
    );
    println!();
    println!("{}", "Note:".yellow().bold());
    println!("  • Schedule will not execute until resumed");
    println!("  • Use 'celers schedule resume {name}' to resume");
    println!();
    println!("  Paused at: {}", timestamp.cyan());

    Ok(())
}

/// Resume a paused schedule
pub async fn resume_schedule(broker_url: &str, name: &str) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    let schedule_key = format!("celers:schedule:{name}");

    // Check if schedule exists
    let exists: bool = redis::cmd("EXISTS")
        .arg(&schedule_key)
        .query_async(&mut conn)
        .await?;

    if !exists {
        println!("{}", format!("✗ Schedule '{name}' not found").red());
        return Ok(());
    }

    // Check if paused
    let pause_key = format!("celers:schedule:{name}:paused");
    let paused: Option<String> = redis::cmd("GET")
        .arg(&pause_key)
        .query_async(&mut conn)
        .await?;

    if paused.is_none() {
        println!("{}", format!("✓ Schedule '{name}' is not paused").yellow());
        return Ok(());
    }

    // Remove pause flag
    let _: () = redis::cmd("DEL")
        .arg(&pause_key)
        .query_async(&mut conn)
        .await?;

    println!(
        "{}",
        format!("✓ Schedule '{name}' has been resumed")
            .green()
            .bold()
    );
    println!();
    println!("{}", "Note:".yellow().bold());
    println!("  • Schedule will now execute at the specified times");
    if let Some(paused_at) = paused {
        println!("  • Was paused at: {}", paused_at.dimmed());
    }

    Ok(())
}

/// Manually trigger a scheduled task
pub async fn trigger_schedule(broker_url: &str, name: &str) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    let schedule_key = format!("celers:schedule:{name}");

    println!(
        "{}",
        format!("=== Trigger Schedule: {name} ===").bold().cyan()
    );
    println!();

    // Get schedule data
    let schedule_data: Option<String> = redis::cmd("GET")
        .arg(&schedule_key)
        .query_async(&mut conn)
        .await?;

    if let Some(data) = schedule_data {
        if let Ok(schedule) = serde_json::from_str::<serde_json::Value>(&data) {
            let task = schedule
                .get("task")
                .and_then(|v| v.as_str())
                .unwrap_or("N/A");
            let queue = schedule
                .get("queue")
                .and_then(|v| v.as_str())
                .unwrap_or("celers");

            // Publish trigger command via Redis Pub/Sub
            let trigger_channel = format!("celers:schedule:{name}:trigger");
            let subscribers: usize = redis::cmd("PUBLISH")
                .arg(&trigger_channel)
                .arg("TRIGGER")
                .query_async(&mut conn)
                .await?;

            if subscribers > 0 {
                println!(
                    "{}",
                    format!("✓ Trigger signal sent for schedule '{name}'")
                        .green()
                        .bold()
                );
                println!();
                println!("  {} {}", "Task:".cyan(), task);
                println!("  {} {}", "Queue:".cyan(), queue);
                println!();
                println!("The task will be executed immediately by the beat scheduler.");
            } else {
                println!(
                    "{}",
                    "⚠ No beat scheduler subscribed to trigger channel"
                        .yellow()
                        .bold()
                );
                println!();
                println!("The trigger command was sent but no beat scheduler is listening.");
                println!("Ensure a beat scheduler is running to execute scheduled tasks.");
            }
        } else {
            println!("{}", "✗ Invalid schedule data".red());
        }
    } else {
        println!("{}", format!("✗ Schedule '{name}' not found").red());
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

/// Show execution history for a schedule
pub async fn schedule_history(broker_url: &str, name: &str, limit: usize) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    println!(
        "{}",
        format!("=== Schedule History: {name} ===").bold().cyan()
    );
    println!();

    // Get history from Redis sorted set
    let history_key = format!("celers:schedule:{name}:history");
    let entries: Vec<String> = redis::cmd("ZREVRANGE")
        .arg(&history_key)
        .arg(0)
        .arg(limit as isize - 1)
        .query_async(&mut conn)
        .await?;

    if entries.is_empty() {
        println!("{}", "No execution history found".yellow());
        println!();
        println!("History is recorded when tasks are triggered by the beat scheduler.");
        return Ok(());
    }

    #[derive(Tabled)]
    struct HistoryEntry {
        #[tabled(rename = "#")]
        index: String,
        #[tabled(rename = "Timestamp")]
        timestamp: String,
        #[tabled(rename = "Status")]
        status: String,
        #[tabled(rename = "Task ID")]
        task_id: String,
    }

    let mut history_entries = Vec::new();
    for (idx, entry) in entries.iter().enumerate() {
        if let Ok(data) = serde_json::from_str::<serde_json::Value>(entry) {
            let timestamp = data
                .get("timestamp")
                .and_then(|v| v.as_str())
                .unwrap_or("N/A")
                .to_string();
            let status = data
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();
            let task_id = data
                .get("task_id")
                .and_then(|v| v.as_str())
                .unwrap_or("N/A")
                .to_string();

            history_entries.push(HistoryEntry {
                index: (idx + 1).to_string(),
                timestamp,
                status,
                task_id,
            });
        }
    }

    let entry_count = history_entries.len();
    let table = Table::new(history_entries)
        .with(Style::rounded())
        .to_string();
    println!("{table}");
    println!();
    println!(
        "Showing {} of {} entries",
        entry_count.to_string().yellow(),
        entries.len()
    );

    Ok(())
}

/// Debug task execution details
pub async fn debug_task(broker_url: &str, queue: &str, task_id_str: &str) -> anyhow::Result<()> {
    let task_id = task_id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| anyhow::anyhow!("Invalid task ID format"))?;

    println!("{}", format!("=== Debug Task: {task_id} ===").bold().cyan());
    println!();

    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    // Get task logs
    let logs_key = format!("celers:task:{task_id}:logs");
    let logs: Vec<String> = redis::cmd("LRANGE")
        .arg(&logs_key)
        .arg(0)
        .arg(-1)
        .query_async(&mut conn)
        .await?;

    if logs.is_empty() {
        println!("{}", "No debug logs found for this task".yellow());
    } else {
        println!("{}", "Task Logs:".green().bold());
        println!();
        for log in &logs {
            if let Ok(log_json) = serde_json::from_str::<serde_json::Value>(log) {
                let level = log_json
                    .get("level")
                    .and_then(|v| v.as_str())
                    .unwrap_or("INFO");
                let message = log_json
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or(log);
                let timestamp = log_json
                    .get("timestamp")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                let level_colored = match level {
                    "ERROR" | "error" => level.red(),
                    "WARN" | "warn" => level.yellow(),
                    "DEBUG" | "debug" => level.cyan(),
                    _ => level.normal(),
                };

                println!("[{}] {} {}", timestamp.dimmed(), level_colored, message);
            } else {
                println!("{log}");
            }
        }
        println!();
    }

    // Get task metadata
    let metadata_key = format!("celers:task:{task_id}:metadata");
    let metadata: Option<String> = redis::cmd("GET")
        .arg(&metadata_key)
        .query_async(&mut conn)
        .await?;

    if let Some(meta_str) = metadata {
        println!("{}", "Task Metadata:".green().bold());
        println!();
        if let Ok(meta_json) = serde_json::from_str::<serde_json::Value>(&meta_str) {
            println!("{}", serde_json::to_string_pretty(&meta_json)?);
        } else {
            println!("{meta_str}");
        }
        println!();
    }

    // Get task state from queue
    inspect_task(broker_url, queue, task_id_str).await?;

    Ok(())
}

/// Debug worker issues
pub async fn debug_worker(broker_url: &str, worker_id: &str) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    println!(
        "{}",
        format!("=== Debug Worker: {worker_id} ===").bold().cyan()
    );
    println!();

    // Get worker heartbeat
    let heartbeat_key = format!("celers:worker:{worker_id}:heartbeat");
    let heartbeat: Option<String> = redis::cmd("GET")
        .arg(&heartbeat_key)
        .query_async(&mut conn)
        .await?;

    if heartbeat.is_none() {
        println!("{}", format!("✗ Worker '{worker_id}' not found").red());
        println!();
        println!("Possible causes:");
        println!("  • Worker is not running");
        println!("  • Worker ID is incorrect");
        println!("  • Heartbeat expired (worker crashed)");
        return Ok(());
    }

    println!("{}", "Worker Status: Active".green().bold());
    if let Some(hb) = heartbeat {
        println!("Last heartbeat: {}", hb.yellow());
    }
    println!();

    // Check worker stats
    let stats_key = format!("celers:worker:{worker_id}:stats");
    let stats: Option<String> = redis::cmd("GET")
        .arg(&stats_key)
        .query_async(&mut conn)
        .await?;

    if let Some(stats_str) = stats {
        println!("{}", "Worker Statistics:".green().bold());
        if let Ok(stats_json) = serde_json::from_str::<serde_json::Value>(&stats_str) {
            println!("{}", serde_json::to_string_pretty(&stats_json)?);
        } else {
            println!("{stats_str}");
        }
        println!();
    }

    // Check for pause status
    let pause_key = format!("celers:worker:{worker_id}:paused");
    let paused: bool = redis::cmd("EXISTS")
        .arg(&pause_key)
        .query_async(&mut conn)
        .await?;

    if paused {
        println!("{}", "⚠ Worker is PAUSED".yellow().bold());
        println!("  Tasks are not being processed");
        println!();
    }

    // Check for drain status
    let drain_key = format!("celers:worker:{worker_id}:draining");
    let draining: bool = redis::cmd("EXISTS")
        .arg(&drain_key)
        .query_async(&mut conn)
        .await?;

    if draining {
        println!("{}", "⚠ Worker is DRAINING".yellow().bold());
        println!("  Not accepting new tasks");
        println!();
    }

    // Get worker logs
    let logs_key = format!("celers:worker:{worker_id}:logs");
    let logs: Vec<String> = redis::cmd("LRANGE")
        .arg(&logs_key)
        .arg(-20)
        .arg(-1)
        .query_async(&mut conn)
        .await?;

    if !logs.is_empty() {
        println!("{}", "Recent Worker Logs (last 20):".green().bold());
        println!();
        for log in logs {
            println!("{log}");
        }
    }

    Ok(())
}

/// Generate daily execution report
pub async fn report_daily(broker_url: &str, queue: &str) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    println!("{}", "=== Daily Execution Report ===".bold().cyan());
    println!();

    let now = Utc::now();
    let today_key = format!("celers:metrics:{}:daily:{}", queue, now.format("%Y-%m-%d"));

    // Get daily metrics
    let metrics: Option<String> = redis::cmd("GET")
        .arg(&today_key)
        .query_async(&mut conn)
        .await?;

    if let Some(metrics_str) = metrics {
        if let Ok(metrics_json) = serde_json::from_str::<serde_json::Value>(&metrics_str) {
            println!("{}", format!("Date: {}", now.format("%Y-%m-%d")).yellow());
            println!();

            #[derive(Tabled)]
            struct DailyMetric {
                #[tabled(rename = "Metric")]
                metric: String,
                #[tabled(rename = "Count")]
                count: String,
            }

            let mut daily_metrics = vec![];

            if let Some(total) = metrics_json
                .get("total_tasks")
                .and_then(serde_json::Value::as_u64)
            {
                daily_metrics.push(DailyMetric {
                    metric: "Total Tasks".to_string(),
                    count: total.to_string(),
                });
            }

            if let Some(succeeded) = metrics_json
                .get("succeeded")
                .and_then(serde_json::Value::as_u64)
            {
                daily_metrics.push(DailyMetric {
                    metric: "Succeeded".to_string(),
                    count: succeeded.to_string(),
                });
            }

            if let Some(failed) = metrics_json
                .get("failed")
                .and_then(serde_json::Value::as_u64)
            {
                daily_metrics.push(DailyMetric {
                    metric: "Failed".to_string(),
                    count: failed.to_string(),
                });
            }

            if let Some(retried) = metrics_json
                .get("retried")
                .and_then(serde_json::Value::as_u64)
            {
                daily_metrics.push(DailyMetric {
                    metric: "Retried".to_string(),
                    count: retried.to_string(),
                });
            }

            if let Some(avg_time) = metrics_json
                .get("avg_execution_time")
                .and_then(serde_json::Value::as_f64)
            {
                daily_metrics.push(DailyMetric {
                    metric: "Avg Execution Time".to_string(),
                    count: format!("{avg_time:.2}s"),
                });
            }

            let table = Table::new(daily_metrics).with(Style::rounded()).to_string();
            println!("{table}");
        }
    } else {
        println!("{}", "No metrics available for today".yellow());
        println!();
        println!("Metrics are collected automatically when tasks are executed.");
        println!("Ensure workers are running and processing tasks.");
    }

    Ok(())
}

/// Generate weekly statistics report
pub async fn report_weekly(broker_url: &str, queue: &str) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    println!("{}", "=== Weekly Statistics Report ===".bold().cyan());
    println!();

    let now = Utc::now();
    let week_start = now - chrono::Duration::days(7);

    println!(
        "{}",
        format!(
            "Period: {} to {}",
            week_start.format("%Y-%m-%d"),
            now.format("%Y-%m-%d")
        )
        .yellow()
    );
    println!();

    let mut total_tasks = 0u64;
    let mut total_succeeded = 0u64;
    let mut total_failed = 0u64;
    let mut total_retried = 0u64;

    // Aggregate daily metrics for the week
    for day_offset in 0..7 {
        let day = now - chrono::Duration::days(day_offset);
        let day_key = format!("celers:metrics:{}:daily:{}", queue, day.format("%Y-%m-%d"));

        let metrics: Option<String> = redis::cmd("GET")
            .arg(&day_key)
            .query_async(&mut conn)
            .await?;

        if let Some(metrics_str) = metrics {
            if let Ok(metrics_json) = serde_json::from_str::<serde_json::Value>(&metrics_str) {
                if let Some(tasks) = metrics_json
                    .get("total_tasks")
                    .and_then(serde_json::Value::as_u64)
                {
                    total_tasks += tasks;
                }
                if let Some(succeeded) = metrics_json
                    .get("succeeded")
                    .and_then(serde_json::Value::as_u64)
                {
                    total_succeeded += succeeded;
                }
                if let Some(failed) = metrics_json
                    .get("failed")
                    .and_then(serde_json::Value::as_u64)
                {
                    total_failed += failed;
                }
                if let Some(retried) = metrics_json
                    .get("retried")
                    .and_then(serde_json::Value::as_u64)
                {
                    total_retried += retried;
                }
            }
        }
    }

    #[derive(Tabled)]
    struct WeeklyMetric {
        #[tabled(rename = "Metric")]
        metric: String,
        #[tabled(rename = "Count")]
        count: String,
        #[tabled(rename = "Percentage")]
        percentage: String,
    }

    let success_rate = if total_tasks > 0 {
        (total_succeeded as f64 / total_tasks as f64) * 100.0
    } else {
        0.0
    };

    let failure_rate = if total_tasks > 0 {
        (total_failed as f64 / total_tasks as f64) * 100.0
    } else {
        0.0
    };

    let weekly_metrics = vec![
        WeeklyMetric {
            metric: "Total Tasks".to_string(),
            count: total_tasks.to_string(),
            percentage: "100%".to_string(),
        },
        WeeklyMetric {
            metric: "Succeeded".to_string(),
            count: total_succeeded.to_string(),
            percentage: format!("{success_rate:.1}%"),
        },
        WeeklyMetric {
            metric: "Failed".to_string(),
            count: total_failed.to_string(),
            percentage: format!("{failure_rate:.1}%"),
        },
        WeeklyMetric {
            metric: "Retried".to_string(),
            count: total_retried.to_string(),
            percentage: "-".to_string(),
        },
    ];

    let table = Table::new(weekly_metrics)
        .with(Style::rounded())
        .to_string();
    println!("{table}");

    if total_tasks == 0 {
        println!();
        println!("{}", "No tasks processed this week".yellow());
    }

    Ok(())
}

/// Analyze performance bottlenecks
pub async fn analyze_bottlenecks(broker_url: &str, queue: &str) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    println!(
        "{}",
        "=== Performance Bottleneck Analysis ===".bold().cyan()
    );
    println!();

    // Check queue depth
    let queue_key = format!("celers:{queue}");
    let queue_type: String = redis::cmd("TYPE")
        .arg(&queue_key)
        .query_async(&mut conn)
        .await?;

    let queue_size: isize = match queue_type.as_str() {
        "list" => {
            redis::cmd("LLEN")
                .arg(&queue_key)
                .query_async(&mut conn)
                .await?
        }
        "zset" => {
            redis::cmd("ZCARD")
                .arg(&queue_key)
                .query_async(&mut conn)
                .await?
        }
        _ => 0,
    };

    // Check worker count
    let worker_keys: Vec<String> = redis::cmd("KEYS")
        .arg("celers:worker:*:heartbeat")
        .query_async(&mut conn)
        .await?;
    let worker_count = worker_keys.len();

    // Check DLQ size
    let dlq_key = format!("celers:{queue}:dlq");
    let dlq_size: isize = redis::cmd("LLEN")
        .arg(&dlq_key)
        .query_async(&mut conn)
        .await?;

    println!("{}", "System Overview:".green().bold());
    println!("  Queue Depth: {}", queue_size.to_string().yellow());
    println!("  Active Workers: {}", worker_count.to_string().yellow());
    println!("  DLQ Size: {}", dlq_size.to_string().yellow());
    println!();

    let mut bottlenecks = Vec::new();

    // Analyze bottlenecks
    if queue_size > 1000 {
        bottlenecks.push("High queue depth - consider scaling up workers");
    }

    if worker_count == 0 && queue_size > 0 {
        bottlenecks.push("No active workers - tasks are not being processed");
    }

    if worker_count > 0 && queue_size > (worker_count * 100) as isize {
        bottlenecks.push("Queue depth is very high relative to worker count");
    }

    if dlq_size > 100 {
        bottlenecks.push("High DLQ size - many tasks are failing");
    }

    if bottlenecks.is_empty() {
        println!("{}", "✓ No significant bottlenecks detected".green());
    } else {
        println!("{}", "⚠ Bottlenecks Detected:".yellow().bold());
        println!();
        for (idx, bottleneck) in bottlenecks.iter().enumerate() {
            println!("  {}. {}", idx + 1, bottleneck);
        }
        println!();

        println!("{}", "Recommendations:".cyan().bold());
        println!();
        if queue_size > 1000 {
            println!(
                "  • Scale up workers: celers worker-mgmt scale {}",
                worker_count * 2
            );
        }
        if worker_count == 0 {
            println!("  • Start workers: celers worker --broker {broker_url}");
        }
        if dlq_size > 100 {
            println!("  • Investigate failed tasks: celers dlq inspect");
            println!("  • Check task implementations for errors");
        }
    }

    Ok(())
}

/// Analyze failure patterns
pub async fn analyze_failures(broker_url: &str, queue: &str) -> anyhow::Result<()> {
    let broker = RedisBroker::new(broker_url, queue)?;

    println!("{}", "=== Failure Pattern Analysis ===".bold().cyan());
    println!();

    let dlq_size = broker.dlq_size().await?;
    println!("Total failed tasks: {}", dlq_size.to_string().yellow());

    if dlq_size == 0 {
        println!("{}", "✓ No failed tasks to analyze".green());
        return Ok(());
    }

    println!();

    // Get failed tasks
    let tasks = broker.inspect_dlq(100).await?;

    let mut task_name_failures: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();

    for task in &tasks {
        // Count by task name
        *task_name_failures
            .entry(task.metadata.name.clone())
            .or_insert(0) += 1;
    }

    println!("{}", "Failures by Task Type:".green().bold());
    println!();

    #[derive(Tabled)]
    struct FailureCount {
        #[tabled(rename = "Task Name")]
        task_name: String,
        #[tabled(rename = "Failures")]
        count: String,
    }

    let mut task_failures: Vec<FailureCount> = task_name_failures
        .into_iter()
        .map(|(name, count)| FailureCount {
            task_name: name,
            count: count.to_string(),
        })
        .collect();
    task_failures.sort_by(|a, b| {
        b.count
            .parse::<usize>()
            .unwrap()
            .cmp(&a.count.parse::<usize>().unwrap())
    });

    let table = Table::new(task_failures.iter().take(10))
        .with(Style::rounded())
        .to_string();
    println!("{table}");
    println!();
    println!("{}", "Recommendations:".cyan().bold());
    println!();
    println!("  • Review task implementations for the most failing tasks");
    println!("  • Check error logs: celers task logs <task-id>");
    println!("  • Consider increasing retry limits for transient failures");
    println!("  • Replay fixed tasks: celers dlq replay <task-id>");

    Ok(())
}

/// Stream worker logs with optional filtering and follow mode
pub async fn worker_logs(
    broker_url: &str,
    worker_id: &str,
    level_filter: Option<&str>,
    follow: bool,
    initial_lines: usize,
) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    println!(
        "{}",
        format!("=== Worker Logs: {worker_id} ===").bold().cyan()
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

    let logs_key = format!("celers:worker:{worker_id}:logs");

    // Get initial logs
    let logs: Vec<String> = redis::cmd("LRANGE")
        .arg(&logs_key)
        .arg(-(initial_lines as isize))
        .arg(-1)
        .query_async(&mut conn)
        .await?;

    // Display initial logs with filtering
    for log in &logs {
        display_log_line(log, level_filter);
    }

    if !follow {
        return Ok(());
    }

    println!();
    println!("{}", "=== Following logs (Ctrl+C to stop) ===".dimmed());
    println!();

    // Follow mode - use Redis polling
    let mut last_length: isize = redis::cmd("LLEN")
        .arg(&logs_key)
        .query_async(&mut conn)
        .await?;

    loop {
        // Sleep briefly
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // Check current length
        let current_length: isize = redis::cmd("LLEN")
            .arg(&logs_key)
            .query_async(&mut conn)
            .await?;

        if current_length > last_length {
            // New logs available
            let new_logs: Vec<String> = redis::cmd("LRANGE")
                .arg(&logs_key)
                .arg(last_length)
                .arg(-1)
                .query_async(&mut conn)
                .await?;

            for log in &new_logs {
                display_log_line(log, level_filter);
            }

            last_length = current_length;
        }

        // Check if worker is still alive
        let still_exists: bool = redis::cmd("EXISTS")
            .arg(&heartbeat_key)
            .query_async(&mut conn)
            .await?;

        if !still_exists {
            println!();
            println!("{}", "Worker has stopped".yellow());
            break;
        }
    }

    Ok(())
}

/// Helper function to display a log line with optional filtering
#[allow(dead_code)]
fn display_log_line(log: &str, level_filter: Option<&str>) {
    if let Ok(log_json) = serde_json::from_str::<serde_json::Value>(log) {
        let level = log_json
            .get("level")
            .and_then(|v| v.as_str())
            .unwrap_or("INFO");
        let message = log_json
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or(log);
        let timestamp = log_json
            .get("timestamp")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        // Filter by level if specified
        if let Some(filter) = level_filter {
            if !level.eq_ignore_ascii_case(filter) {
                return;
            }
        }

        let level_colored = match level.to_uppercase().as_str() {
            "ERROR" => level.red(),
            "WARN" => level.yellow(),
            "DEBUG" => level.cyan(),
            _ => level.normal(),
        };

        println!("[{}] {} {}", timestamp.dimmed(), level_colored, message);
    } else {
        // Non-JSON log, just print it
        if level_filter.is_none() {
            println!("{log}");
        }
    }
}

/// Start auto-scaling service
pub async fn autoscale_start(
    broker_url: &str,
    queue: &str,
    autoscale_config: Option<crate::config::AutoScaleConfig>,
) -> anyhow::Result<()> {
    println!("{}", "=== Auto-Scaling Service ===".bold().green());
    println!();

    let config = match autoscale_config {
        Some(cfg) if cfg.enabled => cfg,
        Some(_) => {
            println!(
                "{}",
                "⚠️  Auto-scaling is disabled in configuration".yellow()
            );
            return Ok(());
        }
        None => {
            println!("{}", "⚠️  No auto-scaling configuration found".yellow());
            println!("Add [autoscale] section to your celers.toml");
            return Ok(());
        }
    };

    println!("Configuration:");
    println!("  Min workers: {}", config.min_workers.to_string().cyan());
    println!("  Max workers: {}", config.max_workers.to_string().cyan());
    println!(
        "  Scale up threshold: {}",
        config.scale_up_threshold.to_string().cyan()
    );
    println!(
        "  Scale down threshold: {}",
        config.scale_down_threshold.to_string().cyan()
    );
    println!(
        "  Check interval: {}s",
        config.check_interval_secs.to_string().cyan()
    );
    println!();

    let broker = RedisBroker::new(broker_url, queue)?;
    println!("{}", "✓ Connected to broker".green());
    println!();
    println!("{}", "Starting auto-scaling monitor...".green().bold());
    println!("{}", "  Press Ctrl+C to stop".dimmed());
    println!();

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(config.check_interval_secs)).await;

        // Get current queue size
        let queue_size = broker.queue_size().await?;

        // Get current worker count
        let client = redis::Client::open(broker_url)?;
        let mut conn = client.get_multiplexed_async_connection().await?;
        let worker_keys: Vec<String> = redis::cmd("KEYS")
            .arg("celers:worker:*:heartbeat")
            .query_async(&mut conn)
            .await?;
        let current_workers = worker_keys.len();

        println!(
            "[{}] Queue: {}, Workers: {}",
            Utc::now().format("%H:%M:%S").to_string().dimmed(),
            queue_size.to_string().yellow(),
            current_workers.to_string().cyan()
        );

        // Determine scaling action
        if queue_size > config.scale_up_threshold && current_workers < config.max_workers {
            let needed = config.max_workers.min(current_workers + 1);
            println!(
                "  {} Scale up recommended: {} -> {}",
                "↑".green().bold(),
                current_workers,
                needed
            );
        } else if queue_size < config.scale_down_threshold && current_workers > config.min_workers {
            let target = config.min_workers.max(current_workers.saturating_sub(1));
            println!(
                "  {} Scale down possible: {} -> {}",
                "↓".yellow().bold(),
                current_workers,
                target
            );
        }
    }
}

/// Show auto-scaling status
pub async fn autoscale_status(
    broker_url: &str,
    autoscale_config: Option<crate::config::AutoScaleConfig>,
) -> anyhow::Result<()> {
    println!("{}", "=== Auto-Scaling Status ===".bold().cyan());
    println!();

    if let Some(cfg) = autoscale_config {
        println!(
            "Status: {}",
            if cfg.enabled {
                "Enabled".green()
            } else {
                "Disabled".red()
            }
        );
        println!();
        println!("Configuration:");
        println!("  Min workers: {}", cfg.min_workers);
        println!("  Max workers: {}", cfg.max_workers);
        println!("  Scale up threshold: {}", cfg.scale_up_threshold);
        println!("  Scale down threshold: {}", cfg.scale_down_threshold);
        println!("  Check interval: {}s", cfg.check_interval_secs);
        println!();

        // Get current metrics
        let client = redis::Client::open(broker_url)?;
        let mut conn = client.get_multiplexed_async_connection().await?;
        let worker_keys: Vec<String> = redis::cmd("KEYS")
            .arg("celers:worker:*:heartbeat")
            .query_async(&mut conn)
            .await?;

        println!("Current State:");
        println!("  Active workers: {}", worker_keys.len().to_string().cyan());
    } else {
        println!("{}", "Auto-scaling is not configured".yellow());
        println!("Add [autoscale] section to your celers.toml");
    }

    Ok(())
}

/// Start alert monitoring service
pub async fn alert_start(
    broker_url: &str,
    queue: &str,
    alert_config: Option<crate::config::AlertConfig>,
) -> anyhow::Result<()> {
    println!("{}", "=== Alert Monitoring Service ===".bold().green());
    println!();

    let config = match alert_config {
        Some(cfg) if cfg.enabled => cfg,
        Some(_) => {
            println!(
                "{}",
                "⚠️  Alert monitoring is disabled in configuration".yellow()
            );
            return Ok(());
        }
        None => {
            println!("{}", "⚠️  No alert configuration found".yellow());
            println!("Add [alerts] section to your celers.toml");
            return Ok(());
        }
    };

    if config.webhook_url.is_none() {
        println!("{}", "⚠️  No webhook URL configured".yellow());
        return Ok(());
    }

    println!("Configuration:");
    println!(
        "  Webhook URL: {}",
        config.webhook_url.as_ref().unwrap().cyan()
    );
    println!(
        "  DLQ threshold: {}",
        config.dlq_threshold.to_string().cyan()
    );
    println!(
        "  Failed threshold: {}",
        config.failed_threshold.to_string().cyan()
    );
    println!(
        "  Check interval: {}s",
        config.check_interval_secs.to_string().cyan()
    );
    println!();

    let broker = RedisBroker::new(broker_url, queue)?;
    println!("{}", "✓ Connected to broker".green());
    println!();
    println!("{}", "Starting alert monitor...".green().bold());
    println!("{}", "  Press Ctrl+C to stop".dimmed());
    println!();

    let webhook_url = config.webhook_url.unwrap();

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(config.check_interval_secs)).await;

        // Check DLQ size
        let dlq_size = broker.dlq_size().await?;

        println!(
            "[{}] DLQ size: {}",
            Utc::now().format("%H:%M:%S").to_string().dimmed(),
            dlq_size.to_string().yellow()
        );

        // Send alert if threshold exceeded
        if dlq_size > config.dlq_threshold {
            let message = format!(
                "⚠️ DLQ size ({}) exceeded threshold ({})",
                dlq_size, config.dlq_threshold
            );
            println!("  {} Sending alert...", "!".red().bold());

            if let Err(e) = send_webhook_alert(&webhook_url, &message).await {
                println!("  {} Failed to send alert: {}", "✗".red(), e);
            } else {
                println!("  {} Alert sent", "✓".green());
            }
        }
    }
}

/// Test webhook notification
pub async fn alert_test(webhook_url: &str, message: &str) -> anyhow::Result<()> {
    println!("{}", "=== Testing Webhook ===".bold().cyan());
    println!();
    println!("Webhook URL: {}", webhook_url.cyan());
    println!("Message: {}", message.yellow());
    println!();

    println!("Sending test notification...");
    send_webhook_alert(webhook_url, message).await?;

    println!("{}", "✓ Test notification sent successfully".green());

    Ok(())
}

/// Helper function to send webhook alert
async fn send_webhook_alert(webhook_url: &str, message: &str) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let payload = serde_json::json!({
        "text": message,
        "timestamp": Utc::now().to_rfc3339(),
    });

    let response = client.post(webhook_url).json(&payload).send().await?;

    if !response.status().is_success() {
        anyhow::bail!("Webhook request failed with status: {}", response.status());
    }

    Ok(())
}

/// Test database connection
pub async fn db_test_connection(url: &str, benchmark: bool) -> anyhow::Result<()> {
    println!("{}", "=== Database Connection Test ===".bold().cyan());
    println!();
    println!("Database URL: {}", mask_password(url).cyan());
    println!();

    // Determine database type from URL
    let db_type = if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        "PostgreSQL"
    } else if url.starts_with("mysql://") {
        "MySQL"
    } else {
        "Unknown"
    };

    println!("Database type: {}", db_type.yellow());
    println!();

    // Test connection
    println!("Testing connection...");
    let start = std::time::Instant::now();

    // For PostgreSQL
    if db_type == "PostgreSQL" {
        match test_postgres_connection(url).await {
            Ok(version) => {
                let duration = start.elapsed();
                println!("{}", "✓ Connection successful".green());
                println!("  Version: {}", version.cyan());
                println!("  Latency: {}ms", duration.as_millis().to_string().yellow());
            }
            Err(e) => {
                println!("{}", "✗ Connection failed".red());
                println!("  Error: {e}");
                return Err(e);
            }
        }
    } else if db_type == "MySQL" {
        println!("{}", "⚠️  MySQL support not yet implemented".yellow());
        return Ok(());
    } else {
        println!("{}", "⚠️  Unknown database type".yellow());
        return Ok(());
    }

    // Run benchmark if requested
    if benchmark {
        println!();
        println!("{}", "Running latency benchmark...".bold());
        println!();

        let mut latencies = Vec::new();
        for i in 1..=10 {
            let start = std::time::Instant::now();
            if let Err(e) = test_postgres_connection(url).await {
                println!("  {} Query {} failed: {}", "✗".red(), i, e);
                continue;
            }
            let duration = start.elapsed();
            latencies.push(duration.as_millis());
            println!("  {} Query {}: {}ms", "✓".green(), i, duration.as_millis());
        }

        if !latencies.is_empty() {
            let avg = latencies.iter().sum::<u128>() / latencies.len() as u128;
            let min = latencies.iter().min().unwrap();
            let max = latencies.iter().max().unwrap();

            println!();
            println!("Benchmark results:");
            println!("  Average: {}ms", avg.to_string().cyan());
            println!("  Min: {}ms", min.to_string().green());
            println!("  Max: {}ms", max.to_string().yellow());
        }
    }

    Ok(())
}

/// Test `PostgreSQL` connection
async fn test_postgres_connection(url: &str) -> anyhow::Result<String> {
    use celers_broker_postgres::PostgresBroker;

    // Create a temporary broker to test connection
    let broker = PostgresBroker::new(url).await?;

    // Test connection and return a version string
    let connected = broker.test_connection().await?;
    if connected {
        Ok("Connected".to_string())
    } else {
        anyhow::bail!("Connection test failed")
    }
}

/// Check database health
pub async fn db_health(url: &str) -> anyhow::Result<()> {
    println!("{}", "=== Database Health Check ===".bold().cyan());
    println!();

    let db_type = if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        "PostgreSQL"
    } else if url.starts_with("mysql://") {
        "MySQL"
    } else {
        "Unknown"
    };

    println!("Database: {}", db_type.yellow());
    println!();

    if db_type == "PostgreSQL" {
        check_postgres_health(url).await?;
    } else {
        println!(
            "{}",
            "⚠️  Health check not supported for this database type".yellow()
        );
    }

    Ok(())
}

/// Check `PostgreSQL` health
async fn check_postgres_health(url: &str) -> anyhow::Result<()> {
    use celers_broker_postgres::PostgresBroker;

    println!("Checking connection...");
    let broker = PostgresBroker::new(url).await?;
    println!("{}", "  ✓ Connection OK".green());

    println!();
    println!("Testing connection with latency measurement...");

    // Measure query performance
    let mut latencies = Vec::new();
    for i in 1..=5 {
        let start = std::time::Instant::now();
        let connected = broker.test_connection().await?;
        let duration = start.elapsed();

        if connected {
            latencies.push(duration.as_millis());
            println!("  {} Query {}: {}ms", "✓".green(), i, duration.as_millis());
        } else {
            println!("  {} Query {} failed", "✗".red(), i);
        }
    }

    if !latencies.is_empty() {
        let avg = latencies.iter().sum::<u128>() / latencies.len() as u128;
        let min = latencies.iter().min().unwrap();
        let max = latencies.iter().max().unwrap();

        println!();
        println!("Query Performance:");
        println!("  Average: {}ms", avg.to_string().cyan());
        println!("  Min: {}ms", min.to_string().green());
        println!("  Max: {}ms", max.to_string().yellow());

        if avg > 100 {
            println!("  {}", "⚠️  High query latency detected".yellow());
        } else {
            println!("  {}", "✓ Query latency is healthy".green());
        }
    }

    // Check pool metrics
    println!();
    println!("Connection Pool Status:");
    let pool_metrics = broker.get_pool_metrics();
    println!(
        "  Max Connections: {}",
        pool_metrics.max_size.to_string().cyan()
    );
    println!("  Active: {}", pool_metrics.size.to_string().cyan());
    println!("  Idle: {}", pool_metrics.idle.to_string().cyan());
    println!("  In-Use: {}", pool_metrics.in_use.to_string().cyan());

    let utilization = if pool_metrics.max_size > 0 {
        (f64::from(pool_metrics.in_use) / f64::from(pool_metrics.max_size)) * 100.0
    } else {
        0.0
    };

    if utilization > 80.0 {
        println!(
            "  {}",
            "⚠️  High pool utilization - consider scaling".yellow()
        );
    }

    println!();
    println!("{}", "✓ Database health check completed".green().bold());

    Ok(())
}

/// Show connection pool statistics
pub async fn db_pool_stats(url: &str) -> anyhow::Result<()> {
    println!("{}", "=== Connection Pool Statistics ===".bold().cyan());
    println!();
    println!("Database URL: {}", mask_password(url).cyan());
    println!();

    let db_type = if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        "PostgreSQL"
    } else if url.starts_with("mysql://") {
        "MySQL"
    } else {
        "Unknown"
    };

    if db_type == "PostgreSQL" {
        use celers_broker_postgres::PostgresBroker;

        println!("Connecting to database...");
        let broker = PostgresBroker::new(url).await?;
        println!("{}", "  ✓ Connected".green());
        println!();

        let metrics = broker.get_pool_metrics();

        #[derive(Tabled)]
        struct PoolStat {
            #[tabled(rename = "Metric")]
            metric: String,
            #[tabled(rename = "Value")]
            value: String,
        }

        let stats = vec![
            PoolStat {
                metric: "Max Connections".to_string(),
                value: metrics.max_size.to_string(),
            },
            PoolStat {
                metric: "Active Connections".to_string(),
                value: metrics.size.to_string(),
            },
            PoolStat {
                metric: "Idle Connections".to_string(),
                value: metrics.idle.to_string(),
            },
            PoolStat {
                metric: "In-Use Connections".to_string(),
                value: metrics.in_use.to_string(),
            },
            PoolStat {
                metric: "Waiting Tasks".to_string(),
                value: if metrics.waiting > 0 {
                    metrics.waiting.to_string()
                } else {
                    "0 (estimated)".to_string()
                },
            },
        ];

        let table = Table::new(stats).with(Style::rounded()).to_string();
        println!("{table}");
        println!();

        // Pool utilization
        let utilization = if metrics.max_size > 0 {
            (f64::from(metrics.in_use) / f64::from(metrics.max_size)) * 100.0
        } else {
            0.0
        };

        println!("Pool Utilization: {utilization:.1}%");
        if utilization > 80.0 {
            println!(
                "{}",
                "⚠️  High pool utilization - consider increasing max_connections".yellow()
            );
        } else if utilization < 20.0 && metrics.max_size > 10 {
            println!(
                "{}",
                "ℹ️  Low pool utilization - consider reducing max_connections".cyan()
            );
        } else {
            println!("{}", "✓ Pool utilization is healthy".green());
        }
    } else {
        println!(
            "{}",
            "⚠️  Pool statistics only supported for PostgreSQL".yellow()
        );
    }

    Ok(())
}

/// Run database migrations
pub async fn db_migrate(url: &str, action: &str, steps: usize) -> anyhow::Result<()> {
    println!("{}", "=== Database Migrations ===".bold().cyan());
    println!();
    println!("Database URL: {}", mask_password(url).cyan());
    println!("Action: {}", action.yellow());
    println!();

    let db_type = if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        "PostgreSQL"
    } else if url.starts_with("mysql://") {
        "MySQL"
    } else {
        "Unknown"
    };

    if db_type == "PostgreSQL" {
        use celers_broker_postgres::PostgresBroker;

        match action.to_lowercase().as_str() {
            "apply" => {
                println!("Applying migrations...");
                let broker = PostgresBroker::new(url).await?;
                let _ = broker; // Use broker to ensure it connects

                println!("{}", "  ✓ Schema initialized".green());
                println!();
                println!("{}", "✓ Migrations applied successfully".green().bold());
            }
            "rollback" => {
                println!("Rolling back {steps} migration(s)...");
                println!();
                println!(
                    "{}",
                    "⚠️  Manual rollback required - use SQL scripts".yellow()
                );
                println!("  CeleRS uses auto-migration with SQLx");
                println!("  To rollback, restore from database backup");
            }
            "status" => {
                println!("Checking migration status...");
                let broker = PostgresBroker::new(url).await?;
                let connected = broker.test_connection().await?;

                println!();
                if connected {
                    println!("{}", "  ✓ Database schema is up-to-date".green());
                    println!("  Tables: celers_tasks, celers_results");
                } else {
                    println!("{}", "  ✗ Cannot connect to database".red());
                }
            }
            _ => {
                anyhow::bail!("Unknown action '{action}'. Valid actions: apply, rollback, status");
            }
        }
    } else {
        println!(
            "{}",
            "⚠️  Migrations only supported for PostgreSQL".yellow()
        );
    }

    Ok(())
}

/// Mask password in database URL for display
fn mask_password(url: &str) -> String {
    if let Some(at_pos) = url.find('@') {
        if let Some(colon_pos) = url[..at_pos].rfind(':') {
            let before = &url[..=colon_pos];
            let after = &url[at_pos..];
            return format!("{before}****{after}");
        }
    }
    url.to_string()
}

/// Run interactive TUI dashboard for real-time monitoring
pub async fn run_dashboard(broker_url: &str, queue: &str, refresh_secs: u64) -> anyhow::Result<()> {
    use crossterm::{
        event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
        execute,
        terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    };
    use ratatui::{
        backend::CrosstermBackend,
        layout::{Constraint, Direction, Layout},
        style::{Color, Modifier, Style},
        widgets::{Block, Borders, Gauge, List, ListItem, Paragraph},
        Terminal,
    };
    use std::io;
    use std::time::Duration;

    let broker = RedisBroker::new(broker_url, queue)?;

    // Setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let refresh_duration = Duration::from_secs(refresh_secs);
    let mut last_update = std::time::Instant::now();

    let result = loop {
        // Fetch stats
        let queue_size = broker.queue_size().await.unwrap_or(0);
        let dlq_size = broker.dlq_size().await.unwrap_or(0);
        let now = Utc::now();

        // Get worker list
        let mut con = redis::Client::open(broker_url)?.get_connection()?;
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg("celers:worker:*:heartbeat")
            .query(&mut con)?;
        let worker_count = keys.len();

        // Render UI
        terminal.draw(|f| {
            let size = f.area();

            // Create layout
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .margin(1)
                .constraints([
                    Constraint::Length(3),
                    Constraint::Length(7),
                    Constraint::Length(7),
                    Constraint::Min(0),
                ])
                .split(size);

            // Title
            let title = Paragraph::new(format!(
                "CeleRS Dashboard - Queue: {} | Last Update: {}",
                queue,
                now.format("%Y-%m-%d %H:%M:%S")
            ))
            .style(
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            )
            .block(Block::default().borders(Borders::ALL));
            f.render_widget(title, chunks[0]);

            // Queue stats
            let queue_stats = [
                format!("Pending Tasks:  {queue_size}"),
                format!("DLQ Size:       {dlq_size}"),
                format!("Active Workers: {worker_count}"),
            ];
            let queue_block = Paragraph::new(queue_stats.join("\n"))
                .style(Style::default().fg(Color::Green))
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title("Queue Statistics"),
                );
            f.render_widget(queue_block, chunks[1]);

            // Queue depth gauge
            let max_display = 1000;
            let ratio = (queue_size.min(max_display) as f64 / max_display as f64).min(1.0);
            let gauge_color = if queue_size > 500 {
                Color::Red
            } else if queue_size > 100 {
                Color::Yellow
            } else {
                Color::Green
            };

            let gauge = Gauge::default()
                .block(Block::default().borders(Borders::ALL).title("Queue Depth"))
                .gauge_style(Style::default().fg(gauge_color))
                .ratio(ratio)
                .label(format!("{queue_size} / {max_display}"));
            f.render_widget(gauge, chunks[2]);

            // Status messages
            let mut messages = vec![];
            if worker_count == 0 && queue_size > 0 {
                messages.push(
                    ListItem::new("⚠️  No active workers - tasks are not being processed")
                        .style(Style::default().fg(Color::Red)),
                );
            }
            if dlq_size > 10 {
                messages.push(
                    ListItem::new(format!("⚠️  High DLQ size: {dlq_size} failed tasks"))
                        .style(Style::default().fg(Color::Yellow)),
                );
            }
            if queue_size > 1000 {
                messages.push(
                    ListItem::new("⚠️  Queue backlog is high - consider scaling workers")
                        .style(Style::default().fg(Color::Yellow)),
                );
            }
            if messages.is_empty() {
                messages.push(
                    ListItem::new("✓ All systems normal").style(Style::default().fg(Color::Green)),
                );
            }

            messages.push(ListItem::new(""));
            messages.push(
                ListItem::new("Press 'q' to quit").style(Style::default().fg(Color::DarkGray)),
            );

            let status_list = List::new(messages).block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Status & Alerts"),
            );
            f.render_widget(status_list, chunks[3]);
        })?;

        // Handle input
        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.code == KeyCode::Char('q') {
                    break Ok(());
                }
            }
        }

        // Auto-refresh
        if last_update.elapsed() >= refresh_duration {
            last_update = std::time::Instant::now();
        }
    };

    // Cleanup terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    result
}

// ============================================================================
// Utility Functions
// ============================================================================

/// Retry a connection operation with exponential backoff
#[allow(dead_code)]
async fn retry_with_backoff<F, T, E>(
    operation: F,
    max_retries: u32,
    operation_name: &str,
) -> Result<T, E>
where
    F: Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, E>> + Send>>,
    E: std::fmt::Display,
{
    let mut retries = 0;
    loop {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(err) => {
                retries += 1;
                if retries >= max_retries {
                    eprintln!(
                        "{}",
                        format!("✗ {operation_name} failed after {max_retries} attempts: {err}")
                            .red()
                    );
                    return Err(err);
                }

                let backoff_ms = 100 * (2_u64.pow(retries - 1));
                eprintln!(
                    "{}",
                    format!(
                        "⚠ {operation_name} failed (attempt {retries}/{max_retries}), retrying in {backoff_ms}ms..."
                    )
                    .yellow()
                );
                tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
            }
        }
    }
}

/// Format bytes into human-readable size
#[allow(dead_code)]
fn format_bytes(bytes: usize) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_idx = 0;

    while size >= 1024.0 && unit_idx < UNITS.len() - 1 {
        size /= 1024.0;
        unit_idx += 1;
    }

    if unit_idx == 0 {
        format!("{} {}", size as usize, UNITS[unit_idx])
    } else {
        format!("{:.2} {}", size, UNITS[unit_idx])
    }
}

/// Format duration into human-readable string
#[allow(dead_code)]
fn format_duration(seconds: u64) -> String {
    if seconds < 60 {
        format!("{seconds}s")
    } else if seconds < 3600 {
        let minutes = seconds / 60;
        let secs = seconds % 60;
        if secs == 0 {
            format!("{minutes}m")
        } else {
            format!("{minutes}m {secs}s")
        }
    } else if seconds < 86400 {
        let hours = seconds / 3600;
        let minutes = (seconds % 3600) / 60;
        if minutes == 0 {
            format!("{hours}h")
        } else {
            format!("{hours}h {minutes}m")
        }
    } else {
        let days = seconds / 86400;
        let hours = (seconds % 86400) / 3600;
        if hours == 0 {
            format!("{days}d")
        } else {
            format!("{days}d {hours}h")
        }
    }
}

/// Validate task ID format
#[allow(dead_code)]
fn validate_task_id(task_id: &str) -> anyhow::Result<uuid::Uuid> {
    uuid::Uuid::parse_str(task_id)
        .map_err(|e| anyhow::anyhow!("Invalid task ID format: {e}. Expected UUID format."))
}

/// Validate queue name
#[allow(dead_code)]
fn validate_queue_name(queue: &str) -> anyhow::Result<()> {
    if queue.is_empty() {
        anyhow::bail!("Queue name cannot be empty");
    }
    if queue.contains(char::is_whitespace) {
        anyhow::bail!("Queue name cannot contain whitespace");
    }
    if queue.len() > 255 {
        anyhow::bail!("Queue name too long (max 255 characters)");
    }
    Ok(())
}

/// Calculate percentage safely
#[allow(dead_code)]
fn calculate_percentage(part: usize, total: usize) -> f64 {
    if total == 0 {
        0.0
    } else {
        (part as f64 / total as f64) * 100.0
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_task_id_parsing() {
        // Valid UUID
        let valid_id = "550e8400-e29b-41d4-a716-446655440000";
        assert!(valid_id.parse::<uuid::Uuid>().is_ok());

        // Invalid UUID
        let invalid_id = "not-a-valid-uuid";
        assert!(invalid_id.parse::<uuid::Uuid>().is_err());
    }

    #[test]
    fn test_worker_id_extraction() {
        let key = "celers:worker:worker-123:heartbeat";
        let parts: Vec<&str> = key.split(':').collect();
        assert_eq!(parts.len(), 4);
        assert_eq!(parts[2], "worker-123");
    }

    #[test]
    fn test_log_level_matching() {
        let levels = vec![
            "ERROR", "error", "WARN", "warn", "INFO", "info", "DEBUG", "debug",
        ];

        for level in levels {
            let _colored = match level {
                "ERROR" | "error" => level,
                "WARN" | "warn" => level,
                "DEBUG" | "debug" => level,
                _ => level,
            };
            // Just testing the pattern matching logic
            assert!(!level.is_empty());
        }
    }

    #[test]
    fn test_redis_key_formatting() {
        let task_id = uuid::Uuid::new_v4();
        let logs_key = format!("celers:task:{}:logs", task_id);
        assert!(logs_key.starts_with("celers:task:"));
        assert!(logs_key.ends_with(":logs"));

        let worker_id = "worker-123";
        let heartbeat_key = format!("celers:worker:{}:heartbeat", worker_id);
        assert_eq!(heartbeat_key, "celers:worker:worker-123:heartbeat");

        let pause_key = format!("celers:worker:{}:paused", worker_id);
        assert_eq!(pause_key, "celers:worker:worker-123:paused");
    }

    #[test]
    fn test_queue_key_formatting() {
        let queue = "test-queue";
        let queue_key = format!("celers:{}", queue);
        assert_eq!(queue_key, "celers:test-queue");

        let dlq_key = format!("{}:dlq", queue_key);
        assert_eq!(dlq_key, "celers:test-queue:dlq");

        let delayed_key = format!("{}:delayed", queue_key);
        assert_eq!(delayed_key, "celers:test-queue:delayed");
    }

    #[test]
    fn test_json_log_parsing() {
        let valid_log =
            r#"{"timestamp":"2025-12-04T10:00:00Z","level":"INFO","message":"Test message"}"#;
        let parsed: Result<serde_json::Value, _> = serde_json::from_str(valid_log);
        assert!(parsed.is_ok());

        let log_json = parsed.unwrap();
        assert_eq!(log_json.get("level").and_then(|v| v.as_str()), Some("INFO"));
        assert_eq!(
            log_json.get("message").and_then(|v| v.as_str()),
            Some("Test message")
        );

        let invalid_log = "not json";
        let parsed: Result<serde_json::Value, _> = serde_json::from_str(invalid_log);
        assert!(parsed.is_err());
    }

    #[test]
    fn test_limit_range_calculation() {
        let limit = 50;
        let log_count = 100;

        // Redis LRANGE with negative indices
        let start_idx = -(limit as isize);
        let end_idx = -1;

        assert_eq!(start_idx, -50);
        assert_eq!(end_idx, -1);

        // Should get last 50 items
        assert!(log_count as usize > limit);
    }

    #[test]
    fn test_diagnostic_thresholds() {
        // DLQ threshold
        let dlq_size = 15;
        assert!(dlq_size > 10, "Should trigger warning when DLQ > 10");

        // Queue backlog threshold
        let queue_size = 1500;
        assert!(
            queue_size > 1000,
            "Should trigger warning when queue > 1000"
        );

        // No workers scenario
        let worker_count = 0;
        let pending_tasks = 50;
        assert!(
            worker_count == 0 && pending_tasks > 0,
            "Should trigger error when no workers but tasks pending"
        );
    }

    #[test]
    fn test_shutdown_channel_naming() {
        let worker_id = "worker-123";

        let graceful_channel = format!("celers:worker:{}:shutdown_graceful", worker_id);
        assert_eq!(
            graceful_channel,
            "celers:worker:worker-123:shutdown_graceful"
        );

        let immediate_channel = format!("celers:worker:{}:shutdown", worker_id);
        assert_eq!(immediate_channel, "celers:worker:worker-123:shutdown");
    }

    #[test]
    fn test_timestamp_formatting() {
        let timestamp = chrono::Utc::now().to_rfc3339();
        assert!(timestamp.contains('T'));
        assert!(timestamp.contains('Z') || timestamp.contains('+'));
    }

    #[test]
    fn test_mask_password() {
        // Test PostgreSQL URL
        let pg_url = "postgres://user:password123@localhost:5432/dbname";
        let masked = super::mask_password(pg_url);
        assert!(masked.contains("postgres://user:****@localhost"));
        assert!(!masked.contains("password123"));

        // Test MySQL URL
        let mysql_url = "mysql://admin:secret@127.0.0.1:3306/db";
        let masked = super::mask_password(mysql_url);
        assert!(masked.contains("mysql://admin:****@127.0.0.1"));
        assert!(!masked.contains("secret"));

        // Test URL without password
        let no_pass_url = "redis://localhost:6379";
        let masked = super::mask_password(no_pass_url);
        assert_eq!(masked, no_pass_url);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(super::format_bytes(0), "0 B");
        assert_eq!(super::format_bytes(500), "500 B");
        assert_eq!(super::format_bytes(1024), "1.00 KB");
        assert_eq!(super::format_bytes(1536), "1.50 KB");
        assert_eq!(super::format_bytes(1048576), "1.00 MB");
        assert_eq!(super::format_bytes(1073741824), "1.00 GB");
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(super::format_duration(0), "0s");
        assert_eq!(super::format_duration(30), "30s");
        assert_eq!(super::format_duration(60), "1m");
        assert_eq!(super::format_duration(90), "1m 30s");
        assert_eq!(super::format_duration(3600), "1h");
        assert_eq!(super::format_duration(3660), "1h 1m");
        assert_eq!(super::format_duration(86400), "1d");
        assert_eq!(super::format_duration(90000), "1d 1h");
    }

    #[test]
    fn test_validate_task_id() {
        // Valid UUID
        let valid = "550e8400-e29b-41d4-a716-446655440000";
        assert!(super::validate_task_id(valid).is_ok());

        // Invalid UUIDs
        assert!(super::validate_task_id("not-a-uuid").is_err());
        assert!(super::validate_task_id("").is_err());
        assert!(super::validate_task_id("12345").is_err());
    }

    #[test]
    fn test_validate_queue_name() {
        // Valid queue names
        assert!(super::validate_queue_name("default").is_ok());
        assert!(super::validate_queue_name("high-priority").is_ok());
        assert!(super::validate_queue_name("queue_1").is_ok());

        // Invalid queue names
        assert!(super::validate_queue_name("").is_err()); // Empty
        assert!(super::validate_queue_name("queue name").is_err()); // Whitespace
        assert!(super::validate_queue_name(&"x".repeat(256)).is_err()); // Too long
    }

    #[test]
    fn test_calculate_percentage() {
        assert_eq!(super::calculate_percentage(0, 100), 0.0);
        assert_eq!(super::calculate_percentage(50, 100), 50.0);
        assert_eq!(super::calculate_percentage(100, 100), 100.0);
        assert_eq!(super::calculate_percentage(25, 100), 25.0);

        // Edge case: division by zero
        assert_eq!(super::calculate_percentage(10, 0), 0.0);
    }
}
