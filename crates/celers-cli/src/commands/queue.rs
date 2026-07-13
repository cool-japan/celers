//! Queue operations command implementations.

use crate::cache::{CacheStats, TtlCache};
use crate::config::CacheConfig;
use crate::pool::pooled_redis_connection;
use celers_broker_redis::RedisBroker;
use celers_core::Broker;
use colored::Colorize;
use std::collections::HashMap;
use std::sync::OnceLock;
use tabled::{settings::Style, Table, Tabled};

/// A single row of [`list_queues`]'s output; cached as plain data (as
/// opposed to a `Tabled` display type) so a cache hit can be rendered
/// without importing any presentation concerns into [`TtlCache`].
#[derive(Debug, Clone, PartialEq, Eq)]
struct QueueListEntry {
    name: String,
    queue_type: String,
    size: String,
}

/// Cached snapshot of [`queue_stats`]'s computed metrics for one queue.
#[derive(Debug, Clone, PartialEq, Eq)]
struct QueueStatsSnapshot {
    queue_type: String,
    queue_size: usize,
    processing_size: usize,
    dlq_size: usize,
    delayed_size: usize,
    task_names: HashMap<String, usize>,
}

/// Process-wide TTL cache of [`list_queues`] results, keyed by broker URL.
fn queue_list_cache() -> &'static TtlCache<String, Vec<QueueListEntry>> {
    static CACHE: OnceLock<TtlCache<String, Vec<QueueListEntry>>> = OnceLock::new();
    CACHE.get_or_init(|| TtlCache::new(CacheConfig::from_env_or_default().ttl()))
}

/// Process-wide TTL cache of [`queue_stats`] results, keyed by
/// `(broker_url, queue)`.
fn queue_stats_cache() -> &'static TtlCache<(String, String), QueueStatsSnapshot> {
    static CACHE: OnceLock<TtlCache<(String, String), QueueStatsSnapshot>> = OnceLock::new();
    CACHE.get_or_init(|| TtlCache::new(CacheConfig::from_env_or_default().ttl()))
}

/// Live hit/reuse statistics for [`queue_list_cache`] and
/// [`queue_stats_cache`], in that order.
///
/// `pub(crate)` (not bare private) so `crate::interactive`'s REPL `stats`
/// command can read them: a normal one-shot `celers <command>` invocation
/// runs a single command and exits long before these process-wide
/// [`OnceLock`] counters could accumulate anything meaningful, so the REPL
/// (which keeps one process alive across many commands) is the one place
/// they are worth surfacing live. The top-level `celers cache-stats`
/// snapshot command intentionally does not call this: it only reports
/// configured capacity/TTL, never live ratios.
#[must_use]
pub(crate) fn queue_cache_stats() -> (CacheStats, CacheStats) {
    (queue_list_cache().stats(), queue_stats_cache().stats())
}

/// Re-export of [`crate::commands::worker::worker_cache_stats`] so it is
/// reachable from outside the `commands` module tree.
///
/// `commands::worker`'s module declaration in `commands/mod.rs` is a bare
/// private `mod worker;`, so `commands::worker::*` is visible only within
/// the `commands` module tree (module-privacy in Rust extends to the
/// current module and its descendants, and `crate::interactive` is a
/// sibling of `commands`, not a descendant of it) -- unlike this module,
/// which was made `pub(crate) mod queue;` in an earlier cleanup pass
/// specifically so `queue_names` could be called from `crate::interactive`.
/// Rather than touch `commands/mod.rs` a second time, this already-
/// `pub(crate)` module re-exposes the one function the REPL's `stats`
/// command needs from `commands::worker`.
pub(crate) use crate::commands::worker::worker_cache_stats;

/// Drop any cached [`queue_stats`]/[`list_queues`] entries touching `queue`
/// on `broker_url`.
///
/// Called after a command mutates queue state (purge, move, pause, resume,
/// import) so the next read reflects the change instead of a stale cached
/// snapshot, per the read/invalidate contract documented on
/// [`crate::cache::TtlCache`].
fn invalidate_queue_caches(broker_url: &str, queue: &str) {
    queue_stats_cache().invalidate(&(broker_url.to_string(), queue.to_string()));
    queue_list_cache().invalidate(&broker_url.to_string());
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

    // `queue_size`/`dlq_size` are independent broker round trips; running
    // them concurrently instead of one after the other halves the wait on
    // networks where each call has non-trivial latency.
    let (queue_size, dlq_size) = tokio::join!(broker.queue_size(), broker.dlq_size());
    let queue_size = queue_size?;
    let dlq_size = dlq_size?;

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
    println!("{}", "=== Redis Queues ===".bold().cyan());
    println!();

    let cache_cfg = CacheConfig::from_env_or_default();
    let cache_key = broker_url.to_string();

    let (entries, served_from_cache) = if cache_cfg.enabled {
        if let Some(cached) = queue_list_cache().get(&cache_key) {
            (cached, true)
        } else {
            let fetched = fetch_queue_list(broker_url).await?;
            queue_list_cache().insert(cache_key, fetched.clone());
            (fetched, false)
        }
    } else {
        (fetch_queue_list(broker_url).await?, false)
    };

    if entries.is_empty() {
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

    let queue_infos: Vec<QueueInfo> = entries
        .into_iter()
        .map(|e| QueueInfo {
            name: e.name,
            queue_type: e.queue_type,
            size: e.size,
        })
        .collect();

    let table = Table::new(queue_infos).with(Style::rounded()).to_string();
    println!("{table}");
    if served_from_cache {
        println!();
        println!(
            "{}",
            format!("(cached; ttl {}s)", cache_cfg.ttl_secs).dimmed()
        );
    }

    Ok(())
}

/// Fetch the live queue list from Redis.
///
/// Discovering the candidate keys via `SCAN` is inherently sequential (each
/// page depends on the previous page's cursor), but once every key is known,
/// looking up each key's `TYPE` and size is completely independent across
/// keys — those lookups run concurrently via [`futures::future::join_all`]
/// over cloned handles from the shared connection pool, rather than one
/// round trip at a time.
async fn fetch_queue_list(broker_url: &str) -> anyhow::Result<Vec<QueueListEntry>> {
    let mut conn = pooled_redis_connection(broker_url).await?;

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

    let fetches = queue_keys.into_iter().map(|key| {
        let mut task_conn = conn.clone();
        async move { fetch_queue_list_entry(&mut task_conn, key).await }
    });

    futures::future::join_all(fetches)
        .await
        .into_iter()
        .collect()
}

/// Fetch the `TYPE` and size of a single queue-like key.
async fn fetch_queue_list_entry(
    conn: &mut redis::aio::MultiplexedConnection,
    key: String,
) -> anyhow::Result<QueueListEntry> {
    let key_type: String = redis::cmd("TYPE").arg(&key).query_async(conn).await?;

    let size: isize = match key_type.as_str() {
        "list" => redis::cmd("LLEN").arg(&key).query_async(conn).await?,
        "zset" => redis::cmd("ZCARD").arg(&key).query_async(conn).await?,
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

    Ok(QueueListEntry {
        name: key,
        queue_type,
        size: size.to_string(),
    })
}

/// Discover the primary queue names currently known to the broker.
///
/// Scans `celers:*` keys the same way [`fetch_queue_list`] does, then
/// filters them down to primary queue keys via
/// [`crate::commands::monitoring::report::base_queue_name`] — the exact
/// same filter [`crate::commands::monitoring::report::report_queues`] uses
/// to enumerate queues for its metrics report — so this reuses that single
/// source of truth for "what counts as a queue" rather than duplicating the
/// key-scan/filter logic a third time.
///
/// Returns a sorted, deduplicated list of queue names (no type/size
/// information, unlike [`list_queues`]/[`fetch_queue_list`]). Used by the
/// interactive REPL's `use <queue>` command to offer a "did you mean"
/// suggestion when the requested queue doesn't already exist.
pub async fn queue_names(broker_url: &str) -> anyhow::Result<Vec<String>> {
    let mut conn = pooled_redis_connection(broker_url).await?;

    let mut cursor = 0u64;
    let mut keys: Vec<String> = Vec::new();

    loop {
        let (new_cursor, batch): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg("celers:*")
            .arg("COUNT")
            .arg(100)
            .query_async(&mut conn)
            .await?;

        keys.extend(batch);
        cursor = new_cursor;

        if cursor == 0 {
            break;
        }
    }

    let mut names: Vec<String> = keys
        .iter()
        .filter_map(|key| crate::commands::monitoring::report::base_queue_name(key))
        .collect();
    names.sort();
    names.dedup();

    Ok(names)
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
    invalidate_queue_caches(broker_url, queue);

    println!(
        "{}",
        format!("✓ Purged {queue_size} tasks from queue '{queue}'").green()
    );

    Ok(())
}

/// Show detailed queue statistics
pub async fn queue_stats(broker_url: &str, queue: &str) -> anyhow::Result<()> {
    let cache_cfg = CacheConfig::from_env_or_default();
    let cache_key = (broker_url.to_string(), queue.to_string());

    let (snapshot, served_from_cache) = if cache_cfg.enabled {
        if let Some(cached) = queue_stats_cache().get(&cache_key) {
            (cached, true)
        } else {
            let fetched = fetch_queue_stats(broker_url, queue).await?;
            queue_stats_cache().insert(cache_key, fetched.clone());
            (fetched, false)
        }
    } else {
        (fetch_queue_stats(broker_url, queue).await?, false)
    };

    render_queue_stats(queue, &snapshot);
    if served_from_cache {
        println!();
        println!(
            "{}",
            format!("(cached; ttl {}s)", cache_cfg.ttl_secs).dimmed()
        );
    }

    Ok(())
}

/// Fetch the live statistics for `queue` from Redis.
///
/// `processing_size`, `dlq_size`, and `delayed_size` are independent of each
/// other and of the main queue's type/size/sample lookup, so all four run
/// concurrently via `tokio::join!` over cloned handles from the shared
/// connection pool instead of four round trips in sequence. The main queue's
/// type must still be resolved before its size (a list uses `LLEN`, a sorted
/// set uses `ZCARD`) and, in turn, before the task-name sample, so that chain
/// stays sequential internally.
async fn fetch_queue_stats(broker_url: &str, queue: &str) -> anyhow::Result<QueueStatsSnapshot> {
    let conn = pooled_redis_connection(broker_url).await?;

    let queue_key = format!("celers:{queue}");
    let processing_key = format!("{queue_key}:processing");
    let dlq_key = format!("{queue_key}:dlq");
    let delayed_key = format!("{queue_key}:delayed");

    let mut main_conn = conn.clone();
    let mut processing_conn = conn.clone();
    let mut dlq_conn = conn.clone();
    let mut delayed_conn = conn.clone();

    let main = async move {
        let queue_type: String = redis::cmd("TYPE")
            .arg(&queue_key)
            .query_async(&mut main_conn)
            .await?;

        let queue_size: usize = if queue_type == "list" {
            redis::cmd("LLEN")
                .arg(&queue_key)
                .query_async(&mut main_conn)
                .await?
        } else if queue_type == "zset" {
            redis::cmd("ZCARD")
                .arg(&queue_key)
                .query_async(&mut main_conn)
                .await?
        } else {
            0
        };

        let mut task_names: HashMap<String, usize> = HashMap::new();
        if queue_size > 0 {
            let sample_size = std::cmp::min(queue_size, 100);
            let tasks: Vec<String> = if queue_type == "list" {
                redis::cmd("LRANGE")
                    .arg(&queue_key)
                    .arg(0)
                    .arg(sample_size as isize - 1)
                    .query_async(&mut main_conn)
                    .await?
            } else if queue_type == "zset" {
                redis::cmd("ZRANGE")
                    .arg(&queue_key)
                    .arg(0)
                    .arg(sample_size as isize - 1)
                    .query_async(&mut main_conn)
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

        Ok::<_, anyhow::Error>((queue_type, queue_size, task_names))
    };

    let processing = async move {
        redis::cmd("LLEN")
            .arg(&processing_key)
            .query_async::<usize>(&mut processing_conn)
            .await
            .unwrap_or(0)
    };
    let dlq = async move {
        redis::cmd("LLEN")
            .arg(&dlq_key)
            .query_async::<usize>(&mut dlq_conn)
            .await
            .unwrap_or(0)
    };
    let delayed = async move {
        redis::cmd("ZCARD")
            .arg(&delayed_key)
            .query_async::<usize>(&mut delayed_conn)
            .await
            .unwrap_or(0)
    };

    let (main_result, processing_size, dlq_size, delayed_size) =
        tokio::join!(main, processing, dlq, delayed);
    let (queue_type, queue_size, task_names) = main_result?;

    Ok(QueueStatsSnapshot {
        queue_type,
        queue_size,
        processing_size,
        dlq_size,
        delayed_size,
        task_names,
    })
}

/// Render a [`QueueStatsSnapshot`] as the `queue_stats` table/health report.
fn render_queue_stats(queue: &str, snapshot: &QueueStatsSnapshot) {
    let QueueStatsSnapshot {
        queue_type,
        queue_size,
        processing_size,
        dlq_size,
        delayed_size,
        task_names,
    } = snapshot;
    let (queue_size, processing_size, dlq_size, delayed_size) =
        (*queue_size, *processing_size, *dlq_size, *delayed_size);

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
            .iter()
            .map(|(name, count)| TaskTypeRow {
                task_name: name.clone(),
                count: *count,
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

    invalidate_queue_caches(broker_url, from_queue);
    invalidate_queue_caches(broker_url, to_queue);

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

    invalidate_queue_caches(broker_url, queue);

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
    invalidate_queue_caches(broker_url, queue);

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
    invalidate_queue_caches(broker_url, queue);

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

#[cfg(test)]
mod tests {
    use super::*;

    /// Stand-in for a per-key broker round trip (e.g. `TYPE` + `LLEN`):
    /// deterministic and independent of every other call, exactly the shape
    /// `fetch_queue_list_entry` has for real Redis keys.
    async fn stub_fetch(x: i32) -> i32 {
        x * 2 + 1
    }

    /// The parallel-fetch pattern used throughout this module's read paths:
    /// run an async operation for every item in a `Vec` via
    /// `futures::future::join_all` instead of a `for` loop that awaits one
    /// item at a time. This proves that pattern returns the same,
    /// order-preserving result set as the equivalent serial loop, using a
    /// stub async closure so no live broker is involved — matching how
    /// `fetch_queue_list`'s per-key `TYPE`/`LLEN`/`ZCARD` lookups are
    /// parallelized in production.
    #[tokio::test]
    async fn parallel_join_all_matches_equivalent_serial_loop() {
        let items: Vec<i32> = (0..25).collect();

        let mut serial = Vec::with_capacity(items.len());
        for item in items.clone() {
            serial.push(stub_fetch(item).await);
        }

        let parallel: Vec<i32> = futures::future::join_all(items.into_iter().map(stub_fetch)).await;

        assert_eq!(
            parallel, serial,
            "join_all must preserve input order and match the serial result set"
        );
    }

    #[test]
    fn queue_stats_cache_hit_after_insert_then_miss_after_invalidate() {
        let key = (
            "test://queue-rs-cache-1".to_string(),
            "q-cache-1".to_string(),
        );
        let snapshot = QueueStatsSnapshot {
            queue_type: "list".to_string(),
            queue_size: 3,
            processing_size: 1,
            dlq_size: 0,
            delayed_size: 0,
            task_names: HashMap::new(),
        };

        queue_stats_cache().insert(key.clone(), snapshot.clone());
        assert_eq!(queue_stats_cache().get(&key), Some(snapshot));

        queue_stats_cache().invalidate(&key);
        assert_eq!(queue_stats_cache().get(&key), None);
    }

    #[test]
    fn queue_list_cache_hit_after_insert_then_miss_after_invalidate() {
        let key = "test://queue-rs-cache-2".to_string();
        let entries = vec![QueueListEntry {
            name: "celers:q-cache-2".to_string(),
            queue_type: "FIFO".to_string(),
            size: "5".to_string(),
        }];

        queue_list_cache().insert(key.clone(), entries.clone());
        assert_eq!(queue_list_cache().get(&key), Some(entries));

        queue_list_cache().invalidate(&key);
        assert_eq!(queue_list_cache().get(&key), None);
    }

    #[test]
    fn invalidate_queue_caches_clears_both_caches() {
        let broker = "test://queue-rs-cache-3";
        let queue = "q-cache-3";

        queue_stats_cache().insert(
            (broker.to_string(), queue.to_string()),
            QueueStatsSnapshot {
                queue_type: "list".to_string(),
                queue_size: 0,
                processing_size: 0,
                dlq_size: 0,
                delayed_size: 0,
                task_names: HashMap::new(),
            },
        );
        queue_list_cache().insert(broker.to_string(), vec![]);

        invalidate_queue_caches(broker, queue);

        assert_eq!(
            queue_stats_cache().get(&(broker.to_string(), queue.to_string())),
            None
        );
        assert_eq!(queue_list_cache().get(&broker.to_string()), None);
    }

    #[test]
    fn queue_cache_stats_reflects_underlying_cache_activity() {
        let key = "test://queue-rs-cache-stats-accessor".to_string();

        let (list_before, stats_before) = queue_cache_stats();

        queue_list_cache().insert(key.clone(), vec![]);
        queue_list_cache().get(&key); // guaranteed hit

        let (list_after, stats_after) = queue_cache_stats();

        assert!(
            list_after.len >= list_before.len,
            "list-cache entry count must never decrease from an insert alone"
        );
        assert!(
            list_after.hits > list_before.hits,
            "queue_cache_stats must observe the hit just recorded on queue_list_cache"
        );
        // The stats-cache side is untouched by this test; other tests may
        // run concurrently against it (this module's tests share one
        // process-wide static), but its counters only ever increase.
        assert!(stats_after.hits >= stats_before.hits);
        assert!(stats_after.misses >= stats_before.misses);
    }

    #[test]
    fn worker_cache_stats_is_reachable_via_queue_module() {
        // `worker_cache_stats` is re-exported here (see its doc comment)
        // specifically so `crate::interactive` can reach it despite
        // `commands::worker` being a private submodule of `commands`; this
        // proves the re-export actually compiles and forwards correctly.
        let (list_stats, stats_stats) = worker_cache_stats();
        assert!(list_stats.hit_ratio() <= 1.0);
        assert!(stats_stats.hit_ratio() <= 1.0);
    }
}
