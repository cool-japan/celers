//! CLI command implementations

use celers_broker_redis::{QueueMode, RedisBroker};
use celers_core::Broker;
use celers_worker::{wait_for_signal, Worker, WorkerConfig};
use chrono::Utc;
use colored::Colorize;
use tabled::{settings::Style, Table, Tabled};

/// Start a worker with the given configuration
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
            eprintln!("Worker error: {}", e);
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

/// Display queue status
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
    println!("{}", table);

    if dlq_size > 0 {
        println!();
        println!(
            "{}",
            format!("⚠️  {} tasks in Dead Letter Queue", dlq_size)
                .yellow()
                .bold()
        );
        println!(
            "   Run: celers dlq inspect --broker {} --queue {}",
            broker_url, queue
        );
    }

    Ok(())
}

/// Inspect Dead Letter Queue
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
            format!("⚠️  This will delete {} tasks from DLQ", dlq_size)
                .yellow()
                .bold()
        );
        println!("   Add --confirm to proceed");
        return Ok(());
    }

    let count = broker.clear_dlq().await?;
    println!("{}", format!("✓ Cleared {} tasks from DLQ", count).green());

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

/// Generate a default configuration file
pub async fn init_config(path: &str) -> anyhow::Result<()> {
    let config = crate::config::Config::default_config();

    config.to_file(path)?;

    println!("{}", "✓ Configuration file created".green().bold());
    println!("  Location: {}", path.cyan());
    println!();
    println!("Edit the file and run:");
    println!("  celers worker --config {}", path);

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
    println!("{}", table);

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
            format!(
                "⚠️  This will delete {} tasks from queue '{}'",
                queue_size, queue
            )
            .yellow()
            .bold()
        );
        println!("   Add --confirm to proceed");
        return Ok(());
    }

    // Connect to Redis directly to delete the queue
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    let queue_key = format!("celers:{}", queue);
    redis::cmd("DEL")
        .arg(&queue_key)
        .query_async::<()>(&mut conn)
        .await?;

    println!(
        "{}",
        format!("✓ Purged {} tasks from queue '{}'", queue_size, queue).green()
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
    let queue_key = format!("celers:{}", queue);
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
        let dlq_key = format!("celers:{}:dlq", queue);
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
        let delayed_key = format!("celers:{}:delayed", queue);
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
    println!("{}", format!("Location: {}", location).green().bold());
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
                .map(|s| format!("{}s", s))
                .unwrap_or_else(|| "default".to_string()),
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
    println!("{}", table);
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
    let queue_key = format!("celers:{}", queue);
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

        for task_str in tasks.iter() {
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
        let dlq_key = format!("celers:{}:dlq", queue);
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
        let delayed_key = format!("celers:{}:delayed", queue);
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
    let result_key = format!("celery-task-meta-{}", task_id);
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
        println!("{}", table);
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
    let from_key = format!("celers:{}", from_queue);
    let to_key = format!("celers:{}", to_queue);

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

        for task_str in tasks.iter() {
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

        for (task_str, _score) in tasks.iter() {
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
            "Source queue '{}' not found or invalid queue type",
            from_queue
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
                format!(
                    "✓ Task moved from '{}' ({}) to '{}' (FIFO)",
                    from_queue, source_type, to_queue
                )
                .green()
                .bold()
            );
        } else if to_type == "zset" {
            // Priority queue
            let priority = t.metadata.priority as f64;
            let _: usize = redis::cmd("ZADD")
                .arg(&to_key)
                .arg(priority)
                .arg(&task_json)
                .query_async(&mut conn)
                .await?;
            println!(
                "{}",
                format!(
                    "✓ Task moved from '{}' ({}) to '{}' (Priority)",
                    from_queue, source_type, to_queue
                )
                .green()
                .bold()
            );
        } else {
            return Err(anyhow::anyhow!(
                "Destination queue '{}' has invalid type: {}",
                to_queue,
                to_type
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
            format!("✗ Task not found in queue '{}'", from_queue).red()
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
    let queue_key = format!("celers:{}", queue);
    let processing_key = format!("{}:processing", queue_key);
    let dlq_key = format!("{}:dlq", queue_key);
    let delayed_key = format!("{}:delayed", queue_key);

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
    println!("{}", format!("Queue Statistics: {}", queue).cyan().bold());
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
                format!("Unknown ({})", queue_type)
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
    println!("{}", table);

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
        println!("{}", table);
    }

    // Health indicators
    println!();
    if dlq_size > 0 {
        println!(
            "{}",
            format!("⚠ Warning: {} tasks in DLQ", dlq_size).yellow()
        );
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
    let from_key = format!("celers:{}", from_queue);
    let to_key = format!("celers:{}", to_queue);

    // Determine the source queue type
    let from_type: String = redis::cmd("TYPE")
        .arg(&from_key)
        .query_async(&mut conn)
        .await?;

    if from_type == "none" {
        println!(
            "{}",
            format!("✗ Source queue '{}' does not exist", from_queue).red()
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
        println!("{}", format!("✗ Unknown queue type: {}", from_type).red());
        return Ok(());
    };

    if queue_size == 0 {
        println!(
            "{}",
            format!("✗ Source queue '{}' is empty", from_queue).yellow()
        );
        return Ok(());
    }

    // Confirm operation
    if !confirm {
        println!(
            "{}",
            format!(
                "⚠ Warning: This will move {} tasks from '{}' to '{}'",
                queue_size, from_queue, to_queue
            )
            .yellow()
        );
        println!("{}", "Use --confirm to proceed".yellow());
        return Ok(());
    }

    println!(
        "{}",
        format!(
            "Moving {} tasks from '{}' to '{}'...",
            queue_size, from_queue, to_queue
        )
        .cyan()
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
                            let priority = task.metadata.priority as f64;
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
                            format!("Moved {} / {} tasks...", moved_count, queue_size).cyan()
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
                    let priority = task.metadata.priority as f64;
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
                    format!("Moved {} / {} tasks...", moved_count, queue_size).cyan()
                );
                use std::io::Write;
                std::io::stdout().flush()?;
            }
        }
    }

    println!();
    println!(
        "{}",
        format!(
            "✓ Successfully moved {} tasks from '{}' to '{}'",
            moved_count, from_queue, to_queue
        )
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

    let queue_key = format!("celers:{}", queue);

    // Get queue type
    let queue_type: String = redis::cmd("TYPE")
        .arg(&queue_key)
        .query_async(&mut conn)
        .await?;

    if queue_type == "none" {
        println!("{}", format!("✗ Queue '{}' does not exist", queue).red());
        return Ok(());
    }

    println!("{}", format!("Exporting queue '{}'...", queue).cyan());

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
        println!("{}", format!("✗ Unknown queue type: {}", queue_type).red());
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

    let queue_key = format!("celers:{}", queue);

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
            let priority = task.metadata.priority as f64;
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
        format!(
            "✓ Successfully imported {} tasks into queue '{}'",
            imported, queue
        )
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
                    continue;
                } else if line.starts_with('#') || line.trim().is_empty() {
                    continue;
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
            format!("✓ Metrics exported to '{}'", file_path)
                .green()
                .bold()
        );
        println!("  {} {}", "Format:".cyan(), format);
        if let Some(pat) = pattern {
            println!("  {} {}", "Filter:".cyan(), pat);
        }
    } else {
        println!("{}", output);
    }

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
            format!("✗ Configuration file not found: {}", config_path)
                .red()
                .bold()
        );
        return Ok(());
    }

    println!(
        "{}",
        format!("📄 Loading configuration from '{}'...", config_path).cyan()
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
            println!("  {}", format!("{}", e).red());
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

    // Validate broker type
    let valid_broker_types = [
        "redis",
        "postgres",
        "postgresql",
        "mysql",
        "amqp",
        "rabbitmq",
        "sqs",
    ];
    if !valid_broker_types.contains(&config.broker.broker_type.to_lowercase().as_str()) {
        println!(
            "  {}",
            format!(
                "⚠ Warning: Unknown broker type '{}'",
                config.broker.broker_type
            )
            .yellow()
        );
        println!(
            "  {}",
            format!("Supported types: {}", valid_broker_types.join(", ")).dimmed()
        );
    } else {
        println!("  {}", "✓ Broker type is valid".green());
    }

    // Validate queue mode
    if config.broker.mode != "fifo" && config.broker.mode != "priority" {
        println!(
            "  {}",
            format!(
                "⚠ Warning: Unknown queue mode '{}' (expected 'fifo' or 'priority')",
                config.broker.mode
            )
            .yellow()
        );
    } else {
        println!(
            "  {}",
            format!("✓ Queue mode '{}' is valid", config.broker.mode).green()
        );
    }

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

    if config.worker.concurrency == 0 {
        println!(
            "  {}",
            "⚠ Warning: Concurrency is 0 - worker will not process any tasks".yellow()
        );
    } else if config.worker.concurrency > 100 {
        println!(
            "  {}",
            format!(
                "⚠ Warning: High concurrency ({}) may cause resource exhaustion",
                config.worker.concurrency
            )
            .yellow()
        );
    } else {
        println!("  {}", "✓ Worker configuration looks reasonable".green());
    }

    if config.worker.poll_interval_ms < 100 {
        println!(
            "  {}",
            "⚠ Warning: Very low poll interval may cause excessive CPU usage".yellow()
        );
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
                                        println!("  {}", format!("{}", e).red());
                                    }
                                }
                            }
                            Err(e) => {
                                println!("{}", "✗ Failed to connect to Redis broker:".red().bold());
                                println!("  {}", format!("{}", e).red());
                            }
                        }
                    }
                    Err(e) => {
                        println!("{}", "✗ Invalid Redis URL:".red().bold());
                        println!("  {}", format!("{}", e).red());
                    }
                }
            }
            "postgres" | "postgresql" => {
                println!(
                    "{}",
                    "✓ PostgreSQL connection testing not yet implemented".yellow()
                );
                println!("  {}", "Manual connection test recommended".dimmed());
            }
            "mysql" => {
                println!(
                    "{}",
                    "✓ MySQL connection testing not yet implemented".yellow()
                );
                println!("  {}", "Manual connection test recommended".dimmed());
            }
            "amqp" | "rabbitmq" => {
                println!(
                    "{}",
                    "✓ AMQP connection testing not yet implemented".yellow()
                );
                println!("  {}", "Manual connection test recommended".dimmed());
            }
            "sqs" => {
                println!(
                    "{}",
                    "✓ SQS connection testing not yet implemented".yellow()
                );
                println!("  {}", "Manual connection test recommended".dimmed());
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
