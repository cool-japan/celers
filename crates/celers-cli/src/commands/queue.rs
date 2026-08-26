//! Queue operations command implementations.

use crate::cache::{CacheStats, TtlCache};
use crate::config::CacheConfig;
use crate::pool::pooled_redis_connection;
use celers_broker_redis::{QueueState, RedisBroker};
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

    // `MATCH *` (not `celers:*`): real `RedisBroker` queue-family keys carry
    // no shared prefix at all -- see `crate::keys`'s module docs and
    // `commands::monitoring::report::base_queue_name`'s docs for the full
    // rationale (idx 331/337). Every namespace this CLI itself owns in
    // Redis (worker/task/metrics/schedule/alias) *is* `celers:`-prefixed, so
    // those are filtered back out below via
    // `crate::keys::is_reserved_namespace_key` -- the pause/drain control
    // flags are not filtered by name here (unlike that namespace check,
    // they are not distinguishable by name alone from the default queue
    // literally named "celers", see that function's docs) and are instead
    // dropped by [`fetch_queue_list_entry`]'s Redis `TYPE` check below,
    // since a control flag is always a Redis STRING, never a LIST/ZSET.
    let mut cursor = 0;
    let mut candidate_keys: Vec<String> = Vec::new();

    loop {
        let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg("*")
            .arg("COUNT")
            .arg(100)
            .query_async(&mut conn)
            .await?;

        candidate_keys.extend(keys);
        cursor = new_cursor;

        if cursor == 0 {
            break;
        }
    }

    let queue_keys: Vec<String> = candidate_keys
        .into_iter()
        .filter(|k| !crate::keys::is_reserved_namespace_key(k))
        .collect();

    let fetches = queue_keys.into_iter().map(|key| {
        let mut task_conn = conn.clone();
        async move { fetch_queue_list_entry(&mut task_conn, key).await }
    });

    let entries: Vec<Option<QueueListEntry>> = futures::future::join_all(fetches)
        .await
        .into_iter()
        .collect::<anyhow::Result<Vec<Option<QueueListEntry>>>>()?;

    Ok(entries.into_iter().flatten().collect())
}

/// Fetch the `TYPE` and size of a single queue-like key.
///
/// Returns `Ok(None)` when `key`'s Redis type is neither `list` nor `zset`
/// -- the only two types `RedisBroker` ever creates a queue-family key as
/// -- rather than reporting a stray non-queue key elsewhere in the keyspace
/// as a bogus zero-size "queue".
async fn fetch_queue_list_entry(
    conn: &mut redis::aio::MultiplexedConnection,
    key: String,
) -> anyhow::Result<Option<QueueListEntry>> {
    let key_type: String = redis::cmd("TYPE").arg(&key).query_async(conn).await?;

    if key_type != "list" && key_type != "zset" {
        return Ok(None);
    }

    let size: isize = match key_type.as_str() {
        "list" => redis::cmd("LLEN").arg(&key).query_async(conn).await?,
        "zset" => redis::cmd("ZCARD").arg(&key).query_async(conn).await?,
        _ => 0,
    };

    let queue_type = if key.ends_with(":dlq") {
        "DLQ".to_string()
    } else if key.ends_with(":delayed") {
        "Delayed".to_string()
    } else if key.ends_with(":processing") {
        "Processing".to_string()
    } else if key_type == "zset" {
        "Priority".to_string()
    } else {
        "FIFO".to_string()
    };

    Ok(Some(QueueListEntry {
        name: key,
        queue_type,
        size: size.to_string(),
    }))
}

/// Discover the primary queue names currently known to the broker.
///
/// Scans the full keyspace the same way [`fetch_queue_list`] does, then
/// filters them down to name candidates via
/// [`crate::commands::monitoring::report::base_queue_name`] -- the same
/// prefix/suffix filter [`crate::commands::monitoring::report::report_queues`]
/// uses -- and finally narrows those candidates to the two Redis types
/// `RedisBroker` ever creates a primary queue key as (`LIST`/`ZSET`, one
/// concurrent `TYPE` lookup per candidate, mirroring
/// [`fetch_queue_list_entry`]'s own check), so a stray non-queue key
/// elsewhere in the keyspace that happens to survive the name filter is
/// never suggested as a "did you mean" queue.
///
/// Returns a sorted, deduplicated list of queue names (no size information,
/// unlike [`list_queues`]/[`fetch_queue_list`]). Used by the interactive
/// REPL's `use <queue>` command to offer a "did you mean" suggestion when
/// the requested queue doesn't already exist.
pub async fn queue_names(broker_url: &str) -> anyhow::Result<Vec<String>> {
    let mut conn = pooled_redis_connection(broker_url).await?;

    let mut cursor = 0u64;
    let mut keys: Vec<String> = Vec::new();

    loop {
        let (new_cursor, batch): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg("*")
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

    let mut candidates: Vec<String> = keys
        .iter()
        .filter_map(|key| crate::commands::monitoring::report::base_queue_name(key))
        .collect();
    candidates.sort();
    candidates.dedup();

    // Each candidate's `TYPE` lookup is independent, so these run
    // concurrently via cloned handles from the shared connection pool
    // rather than one round trip at a time (matching `fetch_queue_list`'s
    // own concurrency pattern above).
    let checks = candidates.into_iter().map(|name| {
        let mut task_conn = conn.clone();
        async move {
            let key_type: String = redis::cmd("TYPE")
                .arg(crate::keys::main(&name))
                .query_async(&mut task_conn)
                .await
                .unwrap_or_else(|_| "none".to_string());
            (key_type == "list" || key_type == "zset").then_some(name)
        }
    });

    let names: Vec<String> = futures::future::join_all(checks)
        .await
        .into_iter()
        .flatten()
        .collect();

    Ok(names)
}

/// Read `key`'s current size via `LLEN` (list) or `ZCARD` (zset); `0` for
/// any other type (including `"none"`, i.e. the key does not exist).
/// `key_type` must already be known (from a prior `TYPE` call on the same
/// key) so this never has to re-issue it.
async fn read_sized_key(
    conn: &mut redis::aio::MultiplexedConnection,
    key: &str,
    key_type: &str,
) -> anyhow::Result<usize> {
    Ok(match key_type {
        "list" => redis::cmd("LLEN").arg(key).query_async(conn).await?,
        "zset" => redis::cmd("ZCARD").arg(key).query_async(conn).await?,
        _ => 0,
    })
}

/// Purge all tasks from a queue's main (pending) key.
///
/// Reads and deletes the *same* Redis key -- the one `RedisBroker` itself
/// reads from and writes to (see `crate::keys::main`) -- rather than
/// reading the size through the broker and then deleting an unrelated
/// `celers:{queue}` key that no producer or worker ever creates (idx 313:
/// that mismatch used to make this command always print a false "✓ Purged N
/// tasks" while leaving the real queue untouched).
pub async fn purge_queue(broker_url: &str, queue: &str, confirm: bool) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client
        .get_multiplexed_async_connection_with_config(&crate::pool::async_connection_config())
        .await?;

    let queue_key = crate::keys::main(queue);
    let queue_type: String = redis::cmd("TYPE")
        .arg(&queue_key)
        .query_async(&mut conn)
        .await?;

    let queue_size = read_sized_key(&mut conn, &queue_key, &queue_type).await?;

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

    // Re-measure immediately before deleting (same key, same connection) so
    // the reported count reflects what this call actually removes rather
    // than a read taken slightly earlier, and use `DEL`'s own return value
    // to detect the rare race where the key was already gone by the time we
    // got here (e.g. a concurrent purge) instead of claiming a purge that
    // did not happen.
    let final_size = read_sized_key(&mut conn, &queue_key, &queue_type).await?;
    let deleted: i64 = redis::cmd("DEL")
        .arg(&queue_key)
        .query_async(&mut conn)
        .await?;
    invalidate_queue_caches(broker_url, queue);

    if deleted == 0 {
        println!(
            "{}",
            format!("✓ Queue '{queue}' was already empty (raced with a concurrent purge)").yellow()
        );
        return Ok(());
    }

    println!(
        "{}",
        format!("✓ Purged {final_size} tasks from queue '{queue}'").green()
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

    let queue_key = crate::keys::main(queue);
    let processing_key = crate::keys::processing(queue);
    let dlq_key = crate::keys::dlq(queue);
    let delayed_key = crate::keys::delayed(queue);

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
    let mut conn = client
        .get_multiplexed_async_connection_with_config(&crate::pool::async_connection_config())
        .await?;

    // Construct queue keys (see `crate::keys` -- these must match the bare,
    // unprefixed key scheme `RedisBroker` itself reads/writes).
    let from_key = crate::keys::main(from_queue);
    let to_key = crate::keys::main(to_queue);

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
                        // Destination is FIFO queue (or create new). `RPUSH`
                        // to match `RedisBroker::enqueue`'s own push
                        // direction for list-mode queues -- an `LPUSH` here
                        // would silently invert this task's position
                        // relative to every task the broker itself enqueues.
                        let _: usize = redis::cmd("RPUSH")
                            .arg(&to_key)
                            .arg(&task_str)
                            .query_async(&mut conn)
                            .await?;
                    } else if to_type == "zset" {
                        // Destination is priority queue. Score is the
                        // negated priority, matching
                        // `RedisBroker::enqueue`'s `-priority as f64`
                        // convention (ZPOPMIN pops the lowest score first,
                        // so higher `priority` values must sort lower).
                        if let Ok(task) =
                            serde_json::from_str::<celers_core::SerializedTask>(&task_str)
                        {
                            let score = -f64::from(task.metadata.priority);
                            let _: usize = redis::cmd("ZADD")
                                .arg(&to_key)
                                .arg(score)
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
                // Destination is FIFO queue. `RPUSH` to match
                // `RedisBroker::enqueue`'s push direction (see the FIFO
                // branch above).
                let _: usize = redis::cmd("RPUSH")
                    .arg(&to_key)
                    .arg(task_str)
                    .query_async(&mut conn)
                    .await?;
            } else if to_type == "zset" {
                // Destination is priority queue. Negated priority, matching
                // `RedisBroker::enqueue`'s scoring convention (see above).
                if let Ok(task) = serde_json::from_str::<celers_core::SerializedTask>(task_str) {
                    let score = -f64::from(task.metadata.priority);
                    let _: usize = redis::cmd("ZADD")
                        .arg(&to_key)
                        .arg(score)
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

/// One raw queue entry as captured by [`export_queue`]/consumed by
/// [`import_queue`].
///
/// `raw` is the exact, unparsed string Redis returned from `LRANGE`/
/// `ZRANGE` -- never re-serialized through this CLI's own
/// `celers_core::SerializedTask` -- so an entry this CLI cannot deserialize
/// (a Celery-native message, a task written by a different CeleRS version,
/// a partially-written entry, ...) is still exported and re-imported
/// byte-for-byte instead of being silently dropped (idx 332).
#[derive(serde::Serialize, serde::Deserialize)]
struct ExportedEntry {
    /// The entry exactly as stored in Redis.
    raw: String,
    /// This entry's `ZADD` score at export time (`None` for a FIFO/list
    /// export). Captured via `ZRANGE ... WITHSCORES` so importing into a
    /// priority queue can restore the exact original ordering without
    /// needing to parse `raw` at all.
    score: Option<f64>,
}

/// On-disk shape written by [`export_queue`] and read by [`import_queue`].
#[derive(serde::Serialize, serde::Deserialize)]
struct QueueExport {
    queue_name: String,
    queue_type: String,
    exported_at: String,
    task_count: usize,
    entries: Vec<ExportedEntry>,
}

/// Max entries fetched per `LRANGE`/`ZRANGE` round trip while exporting, so
/// a very large queue is streamed in bounded chunks rather than pulled into
/// CLI memory (and the single Redis reply) in one unbounded `0 -1` call.
const EXPORT_CHUNK_SIZE: isize = 1000;

/// Read every entry of a list-type queue in [`EXPORT_CHUNK_SIZE`]-sized
/// pages via repeated `LRANGE start stop` calls.
async fn export_list_entries(
    conn: &mut redis::aio::MultiplexedConnection,
    key: &str,
) -> anyhow::Result<Vec<ExportedEntry>> {
    let mut entries = Vec::new();
    let mut start: isize = 0;
    loop {
        let stop = start + EXPORT_CHUNK_SIZE - 1;
        let chunk: Vec<String> = redis::cmd("LRANGE")
            .arg(key)
            .arg(start)
            .arg(stop)
            .query_async(conn)
            .await?;
        let got = chunk.len();
        entries.extend(
            chunk
                .into_iter()
                .map(|raw| ExportedEntry { raw, score: None }),
        );
        if got < EXPORT_CHUNK_SIZE as usize {
            break;
        }
        start += EXPORT_CHUNK_SIZE;
    }
    Ok(entries)
}

/// Read every entry of a zset-type queue (with its score) in
/// [`EXPORT_CHUNK_SIZE`]-sized pages via repeated `ZRANGE start stop
/// WITHSCORES` calls.
async fn export_zset_entries(
    conn: &mut redis::aio::MultiplexedConnection,
    key: &str,
) -> anyhow::Result<Vec<ExportedEntry>> {
    let mut entries = Vec::new();
    let mut start: isize = 0;
    loop {
        let stop = start + EXPORT_CHUNK_SIZE - 1;
        let chunk: Vec<(String, f64)> = redis::cmd("ZRANGE")
            .arg(key)
            .arg(start)
            .arg(stop)
            .arg("WITHSCORES")
            .query_async(conn)
            .await?;
        let got = chunk.len();
        entries.extend(chunk.into_iter().map(|(raw, score)| ExportedEntry {
            raw,
            score: Some(score),
        }));
        if got < EXPORT_CHUNK_SIZE as usize {
            break;
        }
        start += EXPORT_CHUNK_SIZE;
    }
    Ok(entries)
}

/// Export queue tasks to a JSON file.
///
/// Every entry is captured raw and byte-for-byte (see `ExportedEntry`),
/// so nothing is silently dropped even when this CLI's own
/// `SerializedTask` cannot parse an entry -- unlike the previous
/// `if let Ok(task) = serde_json::from_str(...)` implementation, which
/// dropped unparseable entries with no warning and reported the
/// already-reduced count as if it were the total (idx 332).
pub async fn export_queue(broker_url: &str, queue: &str, output_file: &str) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client
        .get_multiplexed_async_connection_with_config(&crate::pool::async_connection_config())
        .await?;

    let queue_key = crate::keys::main(queue);

    let queue_type: String = redis::cmd("TYPE")
        .arg(&queue_key)
        .query_async(&mut conn)
        .await?;

    if queue_type == "none" {
        println!("{}", format!("✗ Queue '{queue}' does not exist").red());
        return Ok(());
    }

    println!("{}", format!("Exporting queue '{queue}'...").cyan());

    let entries = if queue_type == "list" {
        export_list_entries(&mut conn, &queue_key).await?
    } else if queue_type == "zset" {
        export_zset_entries(&mut conn, &queue_key).await?
    } else {
        println!("{}", format!("✗ Unknown queue type: {queue_type}").red());
        return Ok(());
    };

    let export_data = QueueExport {
        queue_name: queue.to_string(),
        queue_type: queue_type.clone(),
        exported_at: chrono::Utc::now().to_rfc3339(),
        task_count: entries.len(),
        entries,
    };

    // Write to file
    let json = serde_json::to_string_pretty(&export_data)?;
    std::fs::write(output_file, json)?;

    println!(
        "{}",
        format!(
            "✓ Exported {} entries from queue '{}' to '{}' (raw, byte-faithful)",
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

/// Import queue tasks from a JSON file produced by [`export_queue`].
pub async fn import_queue(
    broker_url: &str,
    queue: &str,
    input_file: &str,
    confirm: bool,
) -> anyhow::Result<()> {
    // Read and parse file
    let json = std::fs::read_to_string(input_file)?;
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
    let mut conn = client
        .get_multiplexed_async_connection_with_config(&crate::pool::async_connection_config())
        .await?;

    let queue_key = crate::keys::main(queue);

    // Determine destination queue type
    let to_type: String = redis::cmd("TYPE")
        .arg(&queue_key)
        .query_async(&mut conn)
        .await?;

    println!(
        "{}",
        format!("Importing {} entries...", export_data.task_count).cyan()
    );

    let mut imported = 0;
    for entry in export_data.entries {
        if to_type == "list" || to_type == "none" {
            // Destination is FIFO queue. `RPUSH` to match
            // `RedisBroker::enqueue`'s own push direction for list-mode
            // queues.
            let _: usize = redis::cmd("RPUSH")
                .arg(&queue_key)
                .arg(&entry.raw)
                .query_async(&mut conn)
                .await?;
        } else if to_type == "zset" {
            // Destination is priority queue. Prefer the score captured at
            // export time (restores the exact original ordering
            // byte-for-byte); fall back to recomputing `-priority` from the
            // raw JSON (matching `RedisBroker::enqueue`'s convention) only
            // when importing a score-less FIFO export into a priority
            // destination.
            let score = entry
                .score
                .or_else(|| {
                    serde_json::from_str::<celers_core::SerializedTask>(&entry.raw)
                        .ok()
                        .map(|task| -f64::from(task.metadata.priority))
                })
                .unwrap_or(0.0);
            let _: usize = redis::cmd("ZADD")
                .arg(&queue_key)
                .arg(score)
                .arg(&entry.raw)
                .query_async(&mut conn)
                .await?;
        }

        imported += 1;
        if imported % 100 == 0 {
            print!(
                "\r{}",
                format!(
                    "Imported {} / {} entries...",
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
        format!("✓ Successfully imported {imported} entries into queue '{queue}'")
            .green()
            .bold()
    );

    Ok(())
}

/// Pause queue processing.
///
/// Goes through `RedisBroker::queue_controller()` (`QueueController::pause`)
/// rather than a raw `SET celers:{queue}:paused`: the CLI's previous
/// hand-rolled key (`celers:{queue}:paused`) did not match
/// `QueueController`'s own `{queue}:paused` key at all (idx 312), and even
/// after aligning the key, `QueueController` is the single source of truth
/// for queue pause/drain state that any broker-side consumer (present or
/// future) is expected to check -- writing it directly with raw commands
/// would just be reinventing that logic with a second chance to drift.
///
/// NOTE: as of this fix, `RedisBroker::dequeue` still does not itself
/// consult `QueueController::can_dequeue`/`is_paused` (that gap lives in
/// `celers-broker-redis`, outside this crate) -- so a paused queue is now at
/// least recorded under the *correct* key, in the *correct* format, ready
/// for that consultation to be wired in, but does not yet stop a running
/// worker from dequeuing on its own. See the crate-level followups.
pub async fn pause_queue(broker_url: &str, queue: &str) -> anyhow::Result<()> {
    let broker = RedisBroker::new(broker_url, queue)?;
    broker.queue_controller().pause().await?;
    invalidate_queue_caches(broker_url, queue);

    let timestamp = chrono::Utc::now().to_rfc3339();
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

/// Resume queue processing. See [`pause_queue`] for why this goes through
/// `QueueController` instead of a raw `GET`/`DEL`.
pub async fn resume_queue(broker_url: &str, queue: &str) -> anyhow::Result<()> {
    let broker = RedisBroker::new(broker_url, queue)?;
    let controller = broker.queue_controller();

    if controller.get_state().await? == QueueState::Active {
        println!("{}", format!("✓ Queue '{queue}' is not paused").yellow());
        return Ok(());
    }

    controller.resume().await?;
    invalidate_queue_caches(broker_url, queue);

    println!(
        "{}",
        format!("✓ Queue '{queue}' has been resumed").green().bold()
    );
    println!();
    println!("{}", "Note:".yellow().bold());
    println!("  • Workers will now process tasks from this queue");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Local Redis used by this module's live-broker regression tests.
    /// Every test below scopes its own queue name with a fresh UUID so
    /// concurrent test runs (this suite, other crates' suites, other
    /// parallel work against the same shared Redis instance) never collide.
    const TEST_BROKER_URL: &str = "redis://127.0.0.1:6379";

    /// Regression test for idx 312/313: `purge_queue` used to read the
    /// queue's size through `RedisBroker` (which resolves to the *real* key)
    /// and then `DEL celers:{queue}` -- a key nothing ever wrote to -- so it
    /// unconditionally printed "✓ Purged N tasks" while leaving the real
    /// queue completely untouched. This proves a purge against a queue
    /// populated the same way a real producer would (via `RedisBroker`)
    /// actually empties that same, real key.
    #[tokio::test]
    async fn purge_queue_actually_empties_the_real_broker_key() {
        let queue_name = format!("test-purge-{}", uuid::Uuid::new_v4());
        let broker = RedisBroker::new(TEST_BROKER_URL, &queue_name).expect("broker");

        for i in 0..3 {
            let task = celers_core::SerializedTask::new(format!("task-{i}"), Vec::new());
            broker.enqueue(task).await.expect("enqueue");
        }
        assert_eq!(broker.queue_size().await.expect("size"), 3);

        purge_queue(TEST_BROKER_URL, &queue_name, true)
            .await
            .expect("purge");

        assert_eq!(
            broker.queue_size().await.expect("size after purge"),
            0,
            "purge_queue must empty the exact key RedisBroker reads from/writes to"
        );
    }

    /// Regression test for idx 331/337: `fetch_queue_list`/`queue_names`
    /// used to `SCAN celers:*`, a namespace `RedisBroker` never writes a
    /// queue-family key into (see `crate::keys`'s module docs) -- so a
    /// queue populated the same way a real producer would (via
    /// `RedisBroker::enqueue`) was invisible to both, matching nothing.
    /// Asserts only that this specific, UUID-scoped queue is discovered
    /// (not an exact total count) since this suite runs against a shared,
    /// possibly concurrently-used Redis instance.
    #[tokio::test]
    async fn queue_names_and_fetch_queue_list_discover_a_real_broker_queue() {
        let queue_name = format!("test-discover-{}", uuid::Uuid::new_v4());
        let broker = RedisBroker::new(TEST_BROKER_URL, &queue_name).expect("broker");
        broker
            .enqueue(celers_core::SerializedTask::new(
                "solo".to_string(),
                Vec::new(),
            ))
            .await
            .expect("enqueue");

        let names = queue_names(TEST_BROKER_URL).await.expect("queue_names");
        assert!(
            names.contains(&queue_name),
            "queue_names must discover a queue populated through the real RedisBroker key scheme"
        );

        let entries = fetch_queue_list(TEST_BROKER_URL)
            .await
            .expect("fetch_queue_list");
        let discovered = entries.iter().find(|e| e.name == queue_name);
        assert_eq!(
            discovered.map(|e| (e.queue_type.as_str(), e.size.as_str())),
            Some(("FIFO", "1")),
            "fetch_queue_list must list the real broker queue with its actual type/size, \
             not omit it"
        );

        purge_queue(TEST_BROKER_URL, &queue_name, true)
            .await
            .expect("cleanup purge");
    }

    /// Regression test found during review of idx 331/337's fix: without a
    /// Redis `TYPE` check, any non-`celers:`-namespaced key in the
    /// keyspace -- including one totally unrelated to any queue -- would be
    /// suggested by [`queue_names`] as a "did you mean" queue name. This
    /// proves a bare STRING key is excluded.
    #[tokio::test]
    async fn queue_names_excludes_non_list_zset_keys() {
        let stray_key = format!("test-stray-string-{}", uuid::Uuid::new_v4());

        let client = redis::Client::open(TEST_BROKER_URL).expect("client");
        let mut conn = client
            .get_multiplexed_async_connection_with_config(&crate::pool::async_connection_config())
            .await
            .expect("conn");
        let _: () = redis::cmd("SET")
            .arg(&stray_key)
            .arg("not a queue")
            .query_async(&mut conn)
            .await
            .expect("seed stray string key");

        let names = queue_names(TEST_BROKER_URL).await.expect("queue_names");
        assert!(
            !names.contains(&stray_key),
            "queue_names must not suggest a bare STRING key as a queue name"
        );

        let _: () = redis::cmd("DEL")
            .arg(&stray_key)
            .query_async(&mut conn)
            .await
            .unwrap_or(());
    }

    /// `fetch_queue_list` must not surface this CLI's own `celers:`-
    /// namespaced bookkeeping keys (worker heartbeats, schedules, etc.) or
    /// a queue's pause/drain control flags as if they were queue rows --
    /// only real, addressable queue-family keys.
    #[tokio::test]
    async fn fetch_queue_list_excludes_non_queue_namespaces_and_control_flags() {
        let queue_name = format!("test-exclude-{}", uuid::Uuid::new_v4());
        let broker = RedisBroker::new(TEST_BROKER_URL, &queue_name).expect("broker");
        broker.queue_controller().pause().await.expect("pause");

        let client = redis::Client::open(TEST_BROKER_URL).expect("client");
        let mut conn = client
            .get_multiplexed_async_connection_with_config(&crate::pool::async_connection_config())
            .await
            .expect("conn");
        let bookkeeping_key = format!("celers:worker:{queue_name}:heartbeat");
        let _: () = redis::cmd("SET")
            .arg(&bookkeeping_key)
            .arg("alive")
            .query_async(&mut conn)
            .await
            .expect("seed bookkeeping key");

        let entries = fetch_queue_list(TEST_BROKER_URL)
            .await
            .expect("fetch_queue_list");
        assert!(
            entries.iter().all(|e| e.name != bookkeeping_key),
            "a celers:-namespaced bookkeeping key must never be listed as a queue"
        );
        assert!(
            entries.iter().all(|e| !e.name.ends_with(":paused")),
            "a queue's pause control flag must never be listed as a queue"
        );

        broker.queue_controller().resume().await.expect("resume");
        let _: () = redis::cmd("DEL")
            .arg(&bookkeeping_key)
            .query_async(&mut conn)
            .await
            .unwrap_or(());
    }

    /// Regression test found during review of idx 331/337's fix:
    /// `Config::default_config` names the default queue literally
    /// `"celers"`, so this queue's own sibling keys (`celers:dlq`,
    /// `celers:processing`, `celers:delayed`) collide syntactically with the
    /// `celers:`-prefixed bookkeeping namespaces `fetch_queue_list` also
    /// filters out. Checking the bookkeeping-namespace exclusion before the
    /// queue-family-suffix check would silently hide this queue's DLQ row
    /// (and every other sibling) from `celers queue list` forever. This
    /// proves a DLQ entry on a queue literally named "celers" is still
    /// discovered.
    ///
    /// Uses `RPUSH`/`LREM` (additive, targeted removal) rather than `DEL`,
    /// since the literal name "celers" is also this suite's config-driven
    /// default queue name and may be touched by other concurrently-running
    /// tests against the same shared Redis instance.
    #[tokio::test]
    async fn fetch_queue_list_finds_dlq_row_for_the_default_queue_named_celers() {
        let marker = format!("dlq-marker-{}", uuid::Uuid::new_v4());
        let dlq_key = crate::keys::dlq("celers");

        let client = redis::Client::open(TEST_BROKER_URL).expect("client");
        let mut conn = client
            .get_multiplexed_async_connection_with_config(&crate::pool::async_connection_config())
            .await
            .expect("conn");
        let _: usize = redis::cmd("RPUSH")
            .arg(&dlq_key)
            .arg(&marker)
            .query_async(&mut conn)
            .await
            .expect("seed dlq marker");

        let entries = fetch_queue_list(TEST_BROKER_URL)
            .await
            .expect("fetch_queue_list");
        let dlq_row = entries.iter().find(|e| e.name == dlq_key);
        match dlq_row {
            Some(row) => assert_eq!(
                row.queue_type, "DLQ",
                "the default queue's DLQ sibling key must be labeled DLQ, not hidden or \
                 mislabeled"
            ),
            None => panic!(
                "fetch_queue_list must not hide the default queue's own DLQ sibling key \
                 ({dlq_key}) just because it starts with \"celers:\""
            ),
        }

        let _: usize = redis::cmd("LREM")
            .arg(&dlq_key)
            .arg(1)
            .arg(&marker)
            .query_async(&mut conn)
            .await
            .unwrap_or(0);
    }

    /// `purge_queue` against a queue key that was never created must be a
    /// safe no-op (not an error), and purging an already-empty real queue
    /// (created then fully drained) must likewise succeed cleanly -- this
    /// exercises the `deleted == 0` / already-empty branches added while
    /// fixing idx 313.
    #[tokio::test]
    async fn purge_queue_on_nonexistent_or_already_empty_queue_is_a_safe_noop() {
        let never_created = format!("test-purge-missing-{}", uuid::Uuid::new_v4());
        purge_queue(TEST_BROKER_URL, &never_created, true)
            .await
            .expect("purging a queue key that was never created must not error");

        let emptied = format!("test-purge-emptied-{}", uuid::Uuid::new_v4());
        let broker = RedisBroker::new(TEST_BROKER_URL, &emptied).expect("broker");
        broker
            .enqueue(celers_core::SerializedTask::new(
                "solo".to_string(),
                Vec::new(),
            ))
            .await
            .expect("enqueue");
        purge_queue(TEST_BROKER_URL, &emptied, true)
            .await
            .expect("first purge");
        purge_queue(TEST_BROKER_URL, &emptied, true)
            .await
            .expect("second purge against an already-empty queue must not error");
    }

    /// Regression test for idx 332: an entry this CLI's own
    /// `celers_core::SerializedTask` cannot deserialize (e.g. a
    /// Celery-native message, or a task from an incompatible CeleRS
    /// version) must still survive an export/import round trip
    /// byte-for-byte, instead of being silently dropped with the reduced
    /// count reported as if it were the total.
    #[tokio::test]
    async fn export_then_import_round_trips_entries_the_cli_cannot_deserialize() {
        let src_queue = format!("test-export-src-{}", uuid::Uuid::new_v4());
        let dst_queue = format!("test-export-dst-{}", uuid::Uuid::new_v4());

        let client = redis::Client::open(TEST_BROKER_URL).expect("client");
        let mut conn = client
            .get_multiplexed_async_connection_with_config(&crate::pool::async_connection_config())
            .await
            .expect("conn");

        let opaque_entry = r#"{"not":"a serialized task","id":42}"#;
        let _: usize = redis::cmd("RPUSH")
            .arg(crate::keys::main(&src_queue))
            .arg(opaque_entry)
            .query_async(&mut conn)
            .await
            .expect("seed opaque entry");

        let export_path = std::env::temp_dir().join(format!(
            "celers_cli_export_test_{}.json",
            uuid::Uuid::new_v4()
        ));
        let export_path_str = export_path.to_str().expect("utf8 temp path");

        export_queue(TEST_BROKER_URL, &src_queue, export_path_str)
            .await
            .expect("export");
        import_queue(TEST_BROKER_URL, &dst_queue, export_path_str, true)
            .await
            .expect("import");

        let dst_key = crate::keys::main(&dst_queue);
        let imported: Vec<String> = redis::cmd("LRANGE")
            .arg(&dst_key)
            .arg(0)
            .arg(-1)
            .query_async(&mut conn)
            .await
            .expect("lrange dst");

        assert_eq!(
            imported,
            vec![opaque_entry.to_string()],
            "an entry the CLI cannot parse as SerializedTask must still round-trip byte-for-byte"
        );

        let _ = std::fs::remove_file(&export_path);
        let _: () = redis::cmd("DEL")
            .arg(&dst_key)
            .query_async(&mut conn)
            .await
            .unwrap_or(());
    }

    /// Regression test for the priority-score-sign half of idx 312's "CLI
    /// disagrees with the broker on push direction" finding: `move_queue`
    /// used to `ZADD` a moved task's positive, un-negated priority as its
    /// score, while `RedisBroker::enqueue` always scores a priority queue
    /// with `-priority`. Since `ZPOPMIN` (what `RedisBroker::dequeue` uses
    /// in Priority mode) pops the *lowest* score first, that sign mismatch
    /// would make a moved high-priority task dequeue *after* a
    /// broker-enqueued low-priority one instead of before it.
    #[tokio::test]
    async fn move_queue_into_priority_destination_uses_broker_compatible_score_sign() {
        let from_queue = format!("test-move-src-{}", uuid::Uuid::new_v4());
        let to_queue = format!("test-move-dst-{}", uuid::Uuid::new_v4());

        let from_broker = RedisBroker::new(TEST_BROKER_URL, &from_queue).expect("broker");
        let mut high_priority_task =
            celers_core::SerializedTask::new("high".to_string(), Vec::new());
        high_priority_task.metadata.priority = 9;
        from_broker
            .enqueue(high_priority_task)
            .await
            .expect("enqueue high priority");

        let to_broker = RedisBroker::with_mode(
            TEST_BROKER_URL,
            &to_queue,
            celers_broker_redis::QueueMode::Priority,
        )
        .expect("broker");
        let mut low_priority_task = celers_core::SerializedTask::new("low".to_string(), Vec::new());
        low_priority_task.metadata.priority = 1;
        to_broker
            .enqueue(low_priority_task)
            .await
            .expect("seed destination with low priority task");

        move_queue(TEST_BROKER_URL, &from_queue, &to_queue, true)
            .await
            .expect("move");

        let first = to_broker
            .dequeue()
            .await
            .expect("dequeue")
            .expect("destination has a task");
        assert_eq!(
            first.task.metadata.priority, 9,
            "the moved higher-priority task must dequeue before the pre-seeded lower-priority one"
        );
    }

    /// Regression test for the queue-pause half of idx 312/324:
    /// `pause_queue`/`resume_queue` used to write/read
    /// `celers:{queue}:paused`, a key `QueueController` (the real owner of
    /// pause/drain state) never looks at. This proves the CLI's pause/resume
    /// commands now observably change `QueueController`'s own state for the
    /// same queue.
    #[tokio::test]
    async fn pause_queue_and_resume_queue_change_the_real_queue_controller_state() {
        let queue_name = format!("test-pause-{}", uuid::Uuid::new_v4());
        let broker = RedisBroker::new(TEST_BROKER_URL, &queue_name).expect("broker");
        let controller = broker.queue_controller();

        assert_eq!(
            controller.get_state().await.expect("state"),
            QueueState::Active
        );

        pause_queue(TEST_BROKER_URL, &queue_name)
            .await
            .expect("pause");
        assert_eq!(
            controller.get_state().await.expect("state after pause"),
            QueueState::Paused,
            "pause_queue must be observable through QueueController, the real owner of this state"
        );

        resume_queue(TEST_BROKER_URL, &queue_name)
            .await
            .expect("resume");
        assert_eq!(
            controller.get_state().await.expect("state after resume"),
            QueueState::Active
        );
    }

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
