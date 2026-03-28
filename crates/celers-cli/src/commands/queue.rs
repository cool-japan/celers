//! Queue operations command implementations.

use celers_broker_redis::RedisBroker;
use celers_core::Broker;
use colored::Colorize;
use tabled::{settings::Style, Table, Tabled};

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

        task_types.sort_by_key(|t| std::cmp::Reverse(t.count));

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
