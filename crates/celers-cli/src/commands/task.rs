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

/// The exact raw-Redis removal command needed to take a task out of the
/// location [`retry_task`]/[`requeue_task`] found it in.
///
/// Both commands used to remove a task from its source the moment they
/// found it, then separately push it to its destination afterward -- so a
/// failure (wrong destination type, a dropped connection, a crash) between
/// those two steps lost the task permanently (idx 322, idx 323). Capturing
/// *what to remove* here instead lets the caller defer the actual removal
/// until it is bundled into one atomic pipeline with the matching insert.
enum RemovalOp {
    /// `LREM key 1 raw`.
    Lrem { key: String, raw: String },
    /// `ZREM key raw`.
    Zrem { key: String, raw: String },
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

    let queue_key = crate::keys::main(queue);
    let dlq_key = crate::keys::dlq(queue);
    let delayed_key = crate::keys::delayed(queue);

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

/// Retry a failed task (from any queue).
///
/// Re-enqueues onto the main queue using whichever op matches its *actual*
/// Redis type (`LPUSH`... `RPUSH`, in FIFO mode; `ZADD` in Priority mode)
/// instead of unconditionally issuing `LPUSH` -- against a Priority-mode
/// (zset) main queue, an unconditional `LPUSH` fails with `WRONGTYPE` after
/// the task has already been removed from its source, permanently losing it
/// (idx 322). The removal and the re-insert are also bundled into one
/// `redis::pipe().atomic()` (`MULTI`/`EXEC`) so a crash or dropped
/// connection between them can no longer lose the task either.
pub async fn retry_task(broker_url: &str, queue: &str, task_id_str: &str) -> anyhow::Result<()> {
    let task_id = task_id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| anyhow::anyhow!("Invalid task ID format"))?;

    println!("{}", "=== Retry Task ===".bold().cyan());
    println!("Task ID: {}", task_id.to_string().yellow());
    println!();

    // Connect to Redis
    let mut conn = pooled_redis_connection(broker_url).await?;

    let queue_key = crate::keys::main(queue);
    let dlq_key = crate::keys::dlq(queue);
    let delayed_key = crate::keys::delayed(queue);

    // The re-enqueue destination is always the main queue, so its Redis
    // type must be known up front regardless of *where* the task is
    // currently found; this doubles as "how do I search the main queue"
    // (LRANGE vs ZRANGE).
    let queue_type: String = redis::cmd("TYPE")
        .arg(&queue_key)
        .query_async(&mut conn)
        .await?;

    // A single `Option` over everything a match needs (the task, its
    // display label, and how to remove it) rather than three separately
    // `Option`-typed variables that must be kept in sync by convention --
    // this makes "found a task without also knowing its label/removal op"
    // unrepresentable, so using the result below never needs an
    // `.expect()`/`.unwrap()` to bridge that gap.
    let mut found: Option<(celers_core::SerializedTask, &'static str, RemovalOp)> = None;

    // Search the main queue (FIFO or Priority) first -- this only locates
    // the task; nothing is removed yet.
    if queue_type == "list" {
        let tasks: Vec<String> = redis::cmd("LRANGE")
            .arg(&queue_key)
            .arg(0)
            .arg(-1)
            .query_async(&mut conn)
            .await?;

        for task_str in tasks {
            if let Ok(t) = serde_json::from_str::<celers_core::SerializedTask>(&task_str) {
                if t.metadata.id == task_id {
                    let removal = RemovalOp::Lrem {
                        key: queue_key.clone(),
                        raw: task_str,
                    };
                    found = Some((t, "main queue (FIFO)", removal));
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
                    let removal = RemovalOp::Zrem {
                        key: queue_key.clone(),
                        raw: task_str,
                    };
                    found = Some((t, "main queue (Priority)", removal));
                    break;
                }
            }
        }
    }

    // Try DLQ if not found
    if found.is_none() {
        let dlq_tasks: Vec<String> = redis::cmd("LRANGE")
            .arg(&dlq_key)
            .arg(0)
            .arg(-1)
            .query_async(&mut conn)
            .await?;

        for task_str in dlq_tasks {
            if let Ok(t) = serde_json::from_str::<celers_core::SerializedTask>(&task_str) {
                if t.metadata.id == task_id {
                    let removal = RemovalOp::Lrem {
                        key: dlq_key.clone(),
                        raw: task_str,
                    };
                    found = Some((t, "Dead Letter Queue", removal));
                    break;
                }
            }
        }
    }

    // Try delayed queue if not found
    if found.is_none() {
        let delayed_tasks: Vec<String> = redis::cmd("ZRANGE")
            .arg(&delayed_key)
            .arg(0)
            .arg(-1)
            .query_async(&mut conn)
            .await?;

        for task_str in delayed_tasks {
            if let Ok(t) = serde_json::from_str::<celers_core::SerializedTask>(&task_str) {
                if t.metadata.id == task_id {
                    let removal = RemovalOp::Zrem {
                        key: delayed_key.clone(),
                        raw: task_str,
                    };
                    found = Some((t, "Delayed Queue", removal));
                    break;
                }
            }
        }
    }

    // If task found, reset and atomically remove+re-enqueue.
    if let Some((mut t, source_label, removal)) = found {
        println!("✓ Task found in: {}", source_label.cyan());
        println!();

        // Reset task state
        t.metadata.state = celers_core::TaskState::Pending;
        t.metadata.updated_at = Utc::now();
        let task_json = serde_json::to_string(&t)?;

        let mut pipe = redis::pipe();
        pipe.atomic();
        match removal {
            RemovalOp::Lrem { key, raw } => {
                pipe.lrem(&key, 1, &raw);
            }
            RemovalOp::Zrem { key, raw } => {
                pipe.zrem(&key, &raw);
            }
        }
        if queue_type == "zset" {
            // Negated priority, matching `RedisBroker::enqueue`'s
            // `-priority as f64` scoring convention (ZPOPMIN pops the
            // lowest score first, so higher `priority` values must sort
            // lower).
            let score = -(t.metadata.priority as f64);
            pipe.zadd(&queue_key, &task_json, score);
        } else {
            // List (or "none", i.e. the main queue does not exist yet --
            // create it as a FIFO list). `RPUSH` to match
            // `RedisBroker::enqueue`'s own push direction.
            pipe.rpush(&queue_key, &task_json);
        }
        pipe.query_async::<redis::Value>(&mut conn).await?;

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

/// Move task from one queue to another.
///
/// The destination's Redis type is validated *before* the source queue is
/// touched at all: previously this only inspected the destination after
/// already `LREM`/`ZREM`-ing the task out of `from_queue`, so a destination
/// key that happened to exist with a non-queue type (`string`/`hash`/`set`/
/// `stream` -- a name collision, or an operator typo) lost the task
/// permanently while returning an error (idx 323). The removal and the
/// destination insert are also bundled into one `redis::pipe().atomic()`
/// (`MULTI`/`EXEC`) so the pair can no longer be split by a crash or a
/// dropped connection.
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
    let from_key = crate::keys::main(from_queue);
    let to_key = crate::keys::main(to_queue);

    let from_type: String = redis::cmd("TYPE")
        .arg(&from_key)
        .query_async(&mut conn)
        .await?;
    if from_type != "list" && from_type != "zset" {
        return Err(anyhow::anyhow!(
            "Source queue '{from_queue}' not found or invalid queue type"
        ));
    }

    // Validate the destination BEFORE touching the source: a destination
    // key that exists with anything other than list/zset/"none" can never
    // be written to, so reject the whole operation up front rather than
    // discovering that after the source has already lost the task.
    let to_type: String = redis::cmd("TYPE")
        .arg(&to_key)
        .query_async(&mut conn)
        .await?;
    if to_type != "list" && to_type != "zset" && to_type != "none" {
        return Err(anyhow::anyhow!(
            "Destination queue '{to_queue}' has invalid type: {to_type}"
        ));
    }

    // A single `Option` over everything a match needs, rather than three
    // separately `Option`-typed variables kept in sync by convention -- see
    // the identical rationale on `retry_task`'s `found` variable.
    let mut found: Option<(celers_core::SerializedTask, &'static str, RemovalOp)> = None;

    // Search in source queue (locate only -- no removal yet).
    if from_type == "list" {
        let tasks: Vec<String> = redis::cmd("LRANGE")
            .arg(&from_key)
            .arg(0)
            .arg(-1)
            .query_async(&mut conn)
            .await?;

        for task_str in tasks {
            if let Ok(t) = serde_json::from_str::<celers_core::SerializedTask>(&task_str) {
                if t.metadata.id == task_id {
                    let removal = RemovalOp::Lrem {
                        key: from_key.clone(),
                        raw: task_str,
                    };
                    found = Some((t, "list", removal));
                    break;
                }
            }
        }
    } else {
        // from_type == "zset" (the only other value that passed the guard
        // above).
        let tasks: Vec<(String, f64)> = redis::cmd("ZRANGE")
            .arg(&from_key)
            .arg(0)
            .arg(-1)
            .arg("WITHSCORES")
            .query_async(&mut conn)
            .await?;

        for (task_str, _score) in tasks {
            if let Ok(t) = serde_json::from_str::<celers_core::SerializedTask>(&task_str) {
                if t.metadata.id == task_id {
                    let removal = RemovalOp::Zrem {
                        key: from_key.clone(),
                        raw: task_str,
                    };
                    found = Some((t, "zset", removal));
                    break;
                }
            }
        }
    }

    let Some((t, source_type, removal)) = found else {
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
    };

    let task_json = serde_json::to_string(&t)?;
    let mut pipe = redis::pipe();
    pipe.atomic();
    match &removal {
        RemovalOp::Lrem { key, raw } => {
            pipe.lrem(key, 1, raw);
        }
        RemovalOp::Zrem { key, raw } => {
            pipe.zrem(key, raw);
        }
    }
    if to_type == "zset" {
        // Negated priority, matching `RedisBroker::enqueue`'s scoring
        // convention (see `retry_task`/`move_queue` for the same rationale).
        let score = -f64::from(t.metadata.priority);
        pipe.zadd(&to_key, &task_json, score);
        println!(
            "{}",
            format!("✓ Task moved from '{from_queue}' ({source_type}) to '{to_queue}' (Priority)")
                .green()
                .bold()
        );
    } else {
        // "list" or "none" (create as FIFO). `RPUSH` to match
        // `RedisBroker::enqueue`'s push direction.
        pipe.rpush(&to_key, &task_json);
        println!(
            "{}",
            format!("✓ Task moved from '{from_queue}' ({source_type}) to '{to_queue}' (FIFO)")
                .green()
                .bold()
        );
    }
    pipe.query_async::<redis::Value>(&mut conn).await?;

    // Show task details
    println!();
    println!("  {} {}", "Task ID:".cyan(), t.metadata.id);
    println!("  {} {}", "Task Name:".cyan(), t.metadata.name);
    println!("  {} {}", "Priority:".cyan(), t.metadata.priority);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use celers_core::SerializedTask;

    /// Local Redis used by this module's live-broker regression tests.
    /// Every test scopes its own queue name with a fresh UUID so concurrent
    /// test runs never collide on the same key.
    const TEST_BROKER_URL: &str = "redis://127.0.0.1:6379";

    /// Build a raw JSON string exactly as `LRANGE`/`ZRANGE` would return it,
    /// for a task with a specific, known ID.
    fn raw_task(id: uuid::Uuid, name: &str) -> String {
        let mut task = SerializedTask::new(name.to_string(), Vec::new());
        task.metadata.id = id;
        serde_json::to_string(&task).expect("SerializedTask always serializes")
    }

    /// Regression test for idx 322: `retry_task` used to unconditionally
    /// `LPUSH` the retried task back onto the main queue regardless of that
    /// queue's actual Redis type. Against a Priority-mode (ZSET) main
    /// queue, that `LPUSH` fails with `WRONGTYPE` -- but only *after* the
    /// task had already been removed from wherever it was found (here: the
    /// DLQ), permanently losing it. This proves a retry into a real
    /// Priority-mode main queue now succeeds and lands the task in that
    /// ZSET instead.
    #[tokio::test]
    async fn retry_task_into_priority_main_queue_uses_zadd_not_lpush() {
        let queue_name = format!("test-retry-priority-{}", uuid::Uuid::new_v4());

        let broker = celers_broker_redis::RedisBroker::with_mode(
            TEST_BROKER_URL,
            &queue_name,
            celers_broker_redis::QueueMode::Priority,
        )
        .expect("broker");
        // Seed the main queue as a real ZSET (an empty/never-created key
        // reports Redis type "none", not "zset") before `retry_task` runs.
        broker
            .enqueue(SerializedTask::new("seed".to_string(), Vec::new()))
            .await
            .expect("seed main queue");

        let target_id = uuid::Uuid::new_v4();
        let mut target = SerializedTask::new("retry-me".to_string(), Vec::new());
        target.metadata.id = target_id;
        target.metadata.priority = 7;
        let target_json = serde_json::to_string(&target).expect("serialize target");

        let client = redis::Client::open(TEST_BROKER_URL).expect("client");
        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .expect("conn");
        let dlq_key = crate::keys::dlq(&queue_name);
        let _: usize = redis::cmd("LPUSH")
            .arg(&dlq_key)
            .arg(&target_json)
            .query_async(&mut conn)
            .await
            .expect("seed dlq");

        retry_task(TEST_BROKER_URL, &queue_name, &target_id.to_string())
            .await
            .expect(
                "retry_task must succeed against a Priority-mode main queue \
                 instead of WRONGTYPE-failing after already removing the task from the DLQ",
            );

        let dlq_len: usize = redis::cmd("LLEN")
            .arg(&dlq_key)
            .query_async(&mut conn)
            .await
            .expect("llen dlq");
        assert_eq!(dlq_len, 0, "the task must be removed from the DLQ");

        let main_key = crate::keys::main(&queue_name);
        let members: Vec<String> = redis::cmd("ZRANGE")
            .arg(&main_key)
            .arg(0)
            .arg(-1)
            .query_async(&mut conn)
            .await
            .expect("zrange main");
        let retried = members
            .iter()
            .find_map(|m| serde_json::from_str::<SerializedTask>(m).ok())
            .filter(|t| t.metadata.id == target_id)
            .expect("the retried task must be present in the main ZSET, not lost");
        assert_eq!(retried.metadata.state, celers_core::TaskState::Pending);
    }

    /// Regression test for idx 323: `requeue_task` used to remove the task
    /// from its source queue *before* validating the destination's Redis
    /// type, so a destination that happened to exist with a non-queue type
    /// (here: a plain string) lost the task permanently while still
    /// returning an error. This proves the source queue is left untouched
    /// when the destination is rejected.
    #[tokio::test]
    async fn requeue_task_does_not_remove_source_when_destination_type_is_invalid() {
        let from_queue = format!("test-requeue-src-{}", uuid::Uuid::new_v4());
        let to_queue = format!("test-requeue-badtype-{}", uuid::Uuid::new_v4());

        let broker =
            celers_broker_redis::RedisBroker::new(TEST_BROKER_URL, &from_queue).expect("broker");
        let target_id = uuid::Uuid::new_v4();
        let mut target = SerializedTask::new("keep-me".to_string(), Vec::new());
        target.metadata.id = target_id;
        broker.enqueue(target).await.expect("enqueue source");

        let client = redis::Client::open(TEST_BROKER_URL).expect("client");
        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .expect("conn");
        let to_key = crate::keys::main(&to_queue);
        let _: () = redis::cmd("SET")
            .arg(&to_key)
            .arg("not-a-queue")
            .query_async(&mut conn)
            .await
            .expect("seed invalid-type destination");

        let result = requeue_task(
            TEST_BROKER_URL,
            &from_queue,
            &to_queue,
            &target_id.to_string(),
        )
        .await;
        assert!(
            result.is_err(),
            "requeue into an invalid-type destination must fail"
        );

        assert_eq!(
            broker.queue_size().await.expect("source size"),
            1,
            "the task must remain in the source queue when the destination is rejected \
             instead of being removed before the destination was ever validated"
        );

        let _: () = redis::cmd("DEL")
            .arg(&to_key)
            .query_async(&mut conn)
            .await
            .unwrap_or(());
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
