//! Task management command implementations.

use crate::pool::pooled_redis_connection;
use celers_broker_redis::RedisBroker;
use celers_core::Broker;
use chrono::Utc;
use colored::Colorize;
use tabled::{settings::Style, Table, Tabled};

/// Which Redis command a queue-like key needs for a full-range read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RangeKind {
    /// `LRANGE key 0 -1` (FIFO queues, the DLQ).
    List,
    /// `ZRANGE key 0 -1` (priority queues, the delayed queue).
    SortedSet,
}

/// Outcome of searching one candidate location for a task.
struct TaskLocation {
    task: Option<celers_core::SerializedTask>,
    label: &'static str,
}

/// Inspect a specific task by ID
pub async fn inspect_task(broker_url: &str, queue: &str, task_id_str: &str) -> anyhow::Result<()> {
    let task_id = task_id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| anyhow::anyhow!("Invalid task ID format"))?;

    println!("{}", "=== Task Details ===".bold().cyan());
    println!("Task ID: {}", task_id.to_string().yellow());
    println!();

    let conn = pooled_redis_connection(broker_url).await?;

    let queue_key = format!("celers:{queue}");
    let dlq_key = format!("celers:{queue}:dlq");
    let delayed_key = format!("celers:{queue}:delayed");

    let mut main_conn = conn.clone();
    let mut dlq_conn = conn.clone();
    let mut delayed_conn = conn.clone();

    // The three candidate locations (main queue, DLQ, delayed queue) are
    // independent of each other, so they are searched concurrently via
    // `tokio::join!` instead of the original "try main, then DLQ, then
    // delayed" sequential fallback chain. Locating within the main queue
    // still requires its own two-step chain internally, since its Redis type
    // must be known before choosing `LRANGE` vs `ZRANGE`.
    let main = async move {
        let queue_type: String = redis::cmd("TYPE")
            .arg(&queue_key)
            .query_async(&mut main_conn)
            .await?;

        let (task, label) = if queue_type == "list" {
            (
                find_task_in_range(&mut main_conn, &queue_key, RangeKind::List, task_id).await?,
                "Main Queue",
            )
        } else if queue_type == "zset" {
            (
                find_task_in_range(&mut main_conn, &queue_key, RangeKind::SortedSet, task_id)
                    .await?,
                "Main Queue (Priority)",
            )
        } else {
            (None, "Main Queue")
        };

        Ok::<_, anyhow::Error>(TaskLocation { task, label })
    };

    let dlq = async move {
        let task = find_task_in_range(&mut dlq_conn, &dlq_key, RangeKind::List, task_id).await?;
        Ok::<_, anyhow::Error>(TaskLocation {
            task,
            label: "Dead Letter Queue",
        })
    };

    let delayed = async move {
        let task = find_task_in_range(
            &mut delayed_conn,
            &delayed_key,
            RangeKind::SortedSet,
            task_id,
        )
        .await?;
        Ok::<_, anyhow::Error>(TaskLocation {
            task,
            label: "Delayed Queue",
        })
    };

    let (main_result, dlq_result, delayed_result) = tokio::join!(main, dlq, delayed);
    // Order matters: preserves the original main -> DLQ -> delayed
    // precedence when (in principle) the same task ID were somehow present
    // in more than one location.
    let candidates = [main_result?, dlq_result?, delayed_result?];

    match candidates
        .into_iter()
        .find_map(|c| c.task.map(|t| (t, c.label)))
    {
        Some((task, label)) => print_task_details(&task, label),
        None => {
            println!("{}", "✗ Task not found in any queue".red());
            println!();
            println!("The task may have been:");
            println!("  • Already processed");
            println!("  • Deleted");
            println!("  • In a different queue");
        }
    }

    Ok(())
}

/// Fetch every entry from `key` (via `LRANGE 0 -1` or `ZRANGE 0 -1`
/// depending on `kind`) and return the first one whose `metadata.id`
/// matches `task_id`, if any.
async fn find_task_in_range(
    conn: &mut redis::aio::MultiplexedConnection,
    key: &str,
    kind: RangeKind,
    task_id: uuid::Uuid,
) -> anyhow::Result<Option<celers_core::SerializedTask>> {
    let raw_tasks: Vec<String> = match kind {
        RangeKind::List => {
            redis::cmd("LRANGE")
                .arg(key)
                .arg(0)
                .arg(-1)
                .query_async(conn)
                .await?
        }
        RangeKind::SortedSet => {
            redis::cmd("ZRANGE")
                .arg(key)
                .arg(0)
                .arg(-1)
                .query_async(conn)
                .await?
        }
    };

    Ok(find_task_in_raw(&raw_tasks, task_id))
}

/// Search `raw_tasks` (as returned by `LRANGE`/`ZRANGE`) for the first entry
/// whose `metadata.id` matches `task_id`.
///
/// Pure and synchronous — unlike [`find_task_in_range`], it needs no live
/// broker — so it can be unit tested directly with mock/stub JSON strings.
fn find_task_in_raw(
    raw_tasks: &[String],
    task_id: uuid::Uuid,
) -> Option<celers_core::SerializedTask> {
    raw_tasks.iter().find_map(|task_str| {
        let task = serde_json::from_str::<celers_core::SerializedTask>(task_str).ok()?;
        (task.metadata.id == task_id).then_some(task)
    })
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
    let mut conn = pooled_redis_connection(broker_url).await?;

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
        println!(
            "✓ Task found in: {}",
            source_queue
                .expect("source_queue set when task is found")
                .cyan()
        );
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
    let mut conn = pooled_redis_connection(backend_url).await?;

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

    let mut conn = pooled_redis_connection(broker_url).await?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use celers_core::SerializedTask;

    /// Build a raw JSON string exactly as `LRANGE`/`ZRANGE` would return it,
    /// for a task with a specific, known ID.
    fn raw_task(id: uuid::Uuid, name: &str) -> String {
        let mut task = SerializedTask::new(name.to_string(), Vec::new());
        task.metadata.id = id;
        serde_json::to_string(&task).expect("SerializedTask always serializes")
    }

    #[test]
    fn find_task_in_raw_locates_matching_id() {
        let target = uuid::Uuid::new_v4();
        let other = uuid::Uuid::new_v4();
        let raw = vec![raw_task(other, "noise"), raw_task(target, "wanted")];

        let found = find_task_in_raw(&raw, target).expect("target id is present");
        assert_eq!(found.metadata.id, target);
        assert_eq!(found.metadata.name, "wanted");
    }

    #[test]
    fn find_task_in_raw_returns_none_when_absent() {
        let target = uuid::Uuid::new_v4();
        let raw = vec![
            raw_task(uuid::Uuid::new_v4(), "a"),
            raw_task(uuid::Uuid::new_v4(), "b"),
        ];

        assert!(find_task_in_raw(&raw, target).is_none());
    }

    #[test]
    fn find_task_in_raw_skips_unparseable_entries() {
        let target = uuid::Uuid::new_v4();
        let raw = vec!["not json".to_string(), raw_task(target, "wanted")];

        let found = find_task_in_raw(&raw, target).expect("valid entry after garbage is found");
        assert_eq!(found.metadata.id, target);
    }

    /// `inspect_task` searches its three candidate locations (main queue,
    /// DLQ, delayed queue) concurrently via `tokio::join!` rather than the
    /// original sequential "try main, then DLQ, then delayed" fallback
    /// chain. This test proves the parallel search selects the exact same
    /// task (and respects the same main > DLQ > delayed precedence) as the
    /// equivalent serial loop would, using in-memory stand-ins for
    /// `LRANGE`/`ZRANGE` results — no live broker involved.
    #[tokio::test]
    async fn parallel_location_search_matches_serial_precedence_fallback() {
        let target = uuid::Uuid::new_v4();
        let main_raw = vec![raw_task(uuid::Uuid::new_v4(), "main-noise")];
        let dlq_raw = vec![raw_task(target, "in-dlq")];
        let delayed_raw = vec![raw_task(target, "would-also-match-in-delayed")];

        // Serial: the pre-parallelization "try main, then DLQ, then
        // delayed" fallback chain.
        let serial = find_task_in_raw(&main_raw, target)
            .or_else(|| find_task_in_raw(&dlq_raw, target))
            .or_else(|| find_task_in_raw(&delayed_raw, target));

        // Parallel: all three run concurrently, then precedence is applied
        // to the results afterward (mirrors `inspect_task`'s `tokio::join!`
        // + `candidates.into_iter().find_map(..)`).
        let (main_res, dlq_res, delayed_res) = tokio::join!(
            async { find_task_in_raw(&main_raw, target) },
            async { find_task_in_raw(&dlq_raw, target) },
            async { find_task_in_raw(&delayed_raw, target) },
        );
        let parallel = [main_res, dlq_res, delayed_res]
            .into_iter()
            .find_map(|candidate| candidate);

        let serial = serial.expect("serial fallback chain must find the task in the DLQ");
        let parallel = parallel.expect("parallel search must find the same task");
        assert_eq!(parallel.metadata.id, serial.metadata.id);
        assert_eq!(
            parallel.metadata.name, "in-dlq",
            "DLQ must win over delayed even though both contain a matching id, \
             matching the original main > DLQ > delayed precedence"
        );
        assert_eq!(serial.metadata.name, parallel.metadata.name);
    }

    #[tokio::test]
    async fn parallel_location_search_returns_none_when_absent_everywhere() {
        let target = uuid::Uuid::new_v4();
        let main_raw = vec![raw_task(uuid::Uuid::new_v4(), "a")];
        let dlq_raw: Vec<String> = vec![];
        let delayed_raw = vec![raw_task(uuid::Uuid::new_v4(), "b")];

        let (main_res, dlq_res, delayed_res) = tokio::join!(
            async { find_task_in_raw(&main_raw, target) },
            async { find_task_in_raw(&dlq_raw, target) },
            async { find_task_in_raw(&delayed_raw, target) },
        );
        let parallel = [main_res, dlq_res, delayed_res]
            .into_iter()
            .find_map(|candidate| candidate);

        assert!(parallel.is_none());
    }
}
