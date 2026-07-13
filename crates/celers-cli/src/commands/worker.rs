//! Worker management command implementations.

use crate::cache::{CacheStats, TtlCache};
use crate::config::CacheConfig;
use crate::pool::pooled_redis_connection;
use celers_broker_redis::{QueueMode, RedisBroker};
use celers_worker::{wait_for_signal, Worker, WorkerConfig};
use chrono::Utc;
use colored::Colorize;
use std::sync::OnceLock;
use tabled::{settings::Style, Table, Tabled};

/// A single row of [`list_workers`]'s output; cached as plain data (as
/// opposed to a `Tabled` display type) so a cache hit can be rendered
/// without importing any presentation concerns into [`TtlCache`].
#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkerListEntry {
    id: String,
    status: String,
    last_heartbeat: String,
}

/// Cached snapshot of [`worker_stats`]'s computed metrics for one worker.
/// Fields already carry their rendered `to_string()` form (matching the
/// original inline rendering exactly), so caching never re-interprets a
/// `serde_json::Value`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkerStatsSnapshot {
    heartbeat: String,
    tasks_processed: Option<String>,
    tasks_failed: Option<String>,
    uptime_seconds: Option<String>,
}

/// Process-wide TTL cache of [`list_workers`] results, keyed by broker URL.
fn worker_list_cache() -> &'static TtlCache<String, Vec<WorkerListEntry>> {
    static CACHE: OnceLock<TtlCache<String, Vec<WorkerListEntry>>> = OnceLock::new();
    CACHE.get_or_init(|| TtlCache::new(CacheConfig::from_env_or_default().ttl()))
}

/// Process-wide TTL cache of [`worker_stats`] results, keyed by
/// `(broker_url, worker_id)`.
fn worker_stats_cache() -> &'static TtlCache<(String, String), WorkerStatsSnapshot> {
    static CACHE: OnceLock<TtlCache<(String, String), WorkerStatsSnapshot>> = OnceLock::new();
    CACHE.get_or_init(|| TtlCache::new(CacheConfig::from_env_or_default().ttl()))
}

/// Live hit/reuse statistics for [`worker_list_cache`] and
/// [`worker_stats_cache`], in that order.
///
/// `pub(crate)` (not bare private) so this is reachable from outside the
/// `commands` module tree. `commands::worker`'s own module declaration in
/// `commands/mod.rs` stays a bare private `mod worker;` (out of scope for
/// this change), so `crate::interactive` cannot name
/// `crate::commands::worker::worker_cache_stats` directly; it instead goes
/// through a re-export at `crate::commands::queue::worker_cache_stats` (see
/// that re-export's doc comment for the full rationale).
///
/// As with `commands::queue`'s equivalent accessor, a normal one-shot
/// `celers <command>` invocation exits long before these process-wide
/// [`OnceLock`] counters could accumulate anything meaningful; the REPL's
/// `stats` command (the one place a process stays alive across many
/// commands) is where they are worth surfacing live.
#[must_use]
pub(crate) fn worker_cache_stats() -> (CacheStats, CacheStats) {
    (worker_list_cache().stats(), worker_stats_cache().stats())
}

/// Drop any cached [`worker_stats`]/[`list_workers`] entries touching
/// `worker_id` on `broker_url`.
///
/// Called after a command mutates worker state (stop, pause, resume, scale,
/// drain) so the next read reflects the change instead of a stale cached
/// snapshot, per the read/invalidate contract documented on
/// [`crate::cache::TtlCache`].
fn invalidate_worker_caches(broker_url: &str, worker_id: &str) {
    worker_stats_cache().invalidate(&(broker_url.to_string(), worker_id.to_string()));
    worker_list_cache().invalidate(&broker_url.to_string());
}

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
    println!("{}", "=== Active Workers ===".bold().cyan());
    println!();

    let cache_cfg = CacheConfig::from_env_or_default();
    let cache_key = broker_url.to_string();

    let (entries, served_from_cache) = if cache_cfg.enabled {
        if let Some(cached) = worker_list_cache().get(&cache_key) {
            (cached, true)
        } else {
            let fetched = fetch_worker_list(broker_url).await?;
            worker_list_cache().insert(cache_key, fetched.clone());
            (fetched, false)
        }
    } else {
        (fetch_worker_list(broker_url).await?, false)
    };

    if entries.is_empty() {
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

    let worker_count = entries.len();
    let workers: Vec<WorkerInfo> = entries
        .into_iter()
        .map(|e| WorkerInfo {
            id: e.id,
            status: e.status,
            last_heartbeat: e.last_heartbeat,
        })
        .collect();

    let table = Table::new(workers).with(Style::rounded()).to_string();
    println!("{table}");
    println!();
    println!(
        "{}",
        format!("Total active workers: {worker_count}")
            .cyan()
            .bold()
    );
    if served_from_cache {
        println!(
            "{}",
            format!("(cached; ttl {}s)", cache_cfg.ttl_secs).dimmed()
        );
    }

    Ok(())
}

/// Fetch the live worker list from Redis.
///
/// Discovering candidate keys via `SCAN` is inherently sequential (each page
/// depends on the previous page's cursor), but each worker's heartbeat
/// lookup is completely independent of every other worker's — those lookups
/// run concurrently via [`futures::future::join_all`] over cloned handles
/// from the shared connection pool, rather than one round trip at a time.
async fn fetch_worker_list(broker_url: &str) -> anyhow::Result<Vec<WorkerListEntry>> {
    let mut conn = pooled_redis_connection(broker_url).await?;

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

    let fetches = worker_keys.into_iter().filter_map(|key| {
        // Extract worker ID from key: celers:worker:<id>:heartbeat
        let worker_id = key.split(':').nth(2)?.to_string();
        let mut task_conn = conn.clone();
        Some(async move { fetch_worker_list_entry(&mut task_conn, key, worker_id).await })
    });

    futures::future::join_all(fetches)
        .await
        .into_iter()
        .collect()
}

/// Fetch a single worker's heartbeat and derive its list-row entry.
async fn fetch_worker_list_entry(
    conn: &mut redis::aio::MultiplexedConnection,
    key: String,
    worker_id: String,
) -> anyhow::Result<WorkerListEntry> {
    let heartbeat: Option<String> = redis::cmd("GET").arg(&key).query_async(conn).await?;

    let status = if heartbeat.is_some() {
        "Active".to_string()
    } else {
        "Unknown".to_string()
    };
    let last_heartbeat = heartbeat.unwrap_or_else(|| "N/A".to_string());

    Ok(WorkerListEntry {
        id: worker_id,
        status,
        last_heartbeat,
    })
}

/// Show detailed statistics for a worker
pub async fn worker_stats(broker_url: &str, worker_id: &str) -> anyhow::Result<()> {
    println!(
        "{}",
        format!("=== Worker Statistics: {worker_id} ===")
            .bold()
            .cyan()
    );
    println!();

    let cache_cfg = CacheConfig::from_env_or_default();
    let cache_key = (broker_url.to_string(), worker_id.to_string());

    let (snapshot, served_from_cache) = if cache_cfg.enabled {
        if let Some(cached) = worker_stats_cache().get(&cache_key) {
            (Some(cached), true)
        } else {
            let fetched = fetch_worker_stats(broker_url, worker_id).await?;
            if let Some(ref snapshot) = fetched {
                worker_stats_cache().insert(cache_key, snapshot.clone());
            }
            (fetched, false)
        }
    } else {
        (fetch_worker_stats(broker_url, worker_id).await?, false)
    };

    let Some(snapshot) = snapshot else {
        println!("{}", format!("✗ Worker '{worker_id}' not found").red());
        println!();
        println!("Possible reasons:");
        println!("  • Worker is not running");
        println!("  • Worker ID is incorrect");
        println!("  • Worker hasn't sent a heartbeat yet");
        return Ok(());
    };

    render_worker_stats(worker_id, &snapshot);
    if served_from_cache {
        println!(
            "{}",
            format!("(cached; ttl {}s)", cache_cfg.ttl_secs).dimmed()
        );
    }

    Ok(())
}

/// Fetch the live heartbeat + stats blob for `worker_id` from Redis, or
/// `None` if the worker has never sent a heartbeat.
///
/// The heartbeat existence check and the stats blob lookup are independent
/// reads, so both run concurrently via `tokio::join!` over cloned handles
/// from the shared connection pool instead of the stats lookup waiting on
/// the heartbeat check to finish first.
async fn fetch_worker_stats(
    broker_url: &str,
    worker_id: &str,
) -> anyhow::Result<Option<WorkerStatsSnapshot>> {
    let conn = pooled_redis_connection(broker_url).await?;

    let heartbeat_key = format!("celers:worker:{worker_id}:heartbeat");
    let stats_key = format!("celers:worker:{worker_id}:stats");

    let mut heartbeat_conn = conn.clone();
    let mut stats_conn = conn.clone();

    let heartbeat_fut = async move {
        redis::cmd("GET")
            .arg(&heartbeat_key)
            .query_async::<Option<String>>(&mut heartbeat_conn)
            .await
    };
    let stats_fut = async move {
        redis::cmd("GET")
            .arg(&stats_key)
            .query_async::<Option<String>>(&mut stats_conn)
            .await
    };

    let (heartbeat, stats) = tokio::join!(heartbeat_fut, stats_fut);
    let Some(heartbeat) = heartbeat? else {
        return Ok(None);
    };
    let stats = stats?;

    let mut tasks_processed = None;
    let mut tasks_failed = None;
    let mut uptime_seconds = None;

    if let Some(stats_json) = stats {
        if let Ok(stats_data) = serde_json::from_str::<serde_json::Value>(&stats_json) {
            tasks_processed = stats_data.get("tasks_processed").map(ToString::to_string);
            tasks_failed = stats_data.get("tasks_failed").map(ToString::to_string);
            uptime_seconds = stats_data.get("uptime_seconds").map(ToString::to_string);
        }
    }

    Ok(Some(WorkerStatsSnapshot {
        heartbeat,
        tasks_processed,
        tasks_failed,
        uptime_seconds,
    }))
}

/// Render a [`WorkerStatsSnapshot`] as the `worker_stats` table.
fn render_worker_stats(worker_id: &str, snapshot: &WorkerStatsSnapshot) {
    #[derive(Tabled)]
    struct StatRow {
        #[tabled(rename = "Metric")]
        metric: String,
        #[tabled(rename = "Value")]
        value: String,
    }

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
            value: snapshot.heartbeat.clone(),
        },
    ];

    if let Some(ref tasks_processed) = snapshot.tasks_processed {
        stat_rows.push(StatRow {
            metric: "Tasks Processed".to_string(),
            value: tasks_processed.clone(),
        });
    }
    if let Some(ref tasks_failed) = snapshot.tasks_failed {
        stat_rows.push(StatRow {
            metric: "Tasks Failed".to_string(),
            value: tasks_failed.clone(),
        });
    }
    if let Some(ref uptime) = snapshot.uptime_seconds {
        stat_rows.push(StatRow {
            metric: "Uptime".to_string(),
            value: format!("{uptime} seconds"),
        });
    }

    let table = Table::new(stat_rows).with(Style::rounded()).to_string();
    println!("{table}");
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
    invalidate_worker_caches(broker_url, worker_id);

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
    invalidate_worker_caches(broker_url, worker_id);

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
    invalidate_worker_caches(broker_url, worker_id);

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

    worker_list_cache().invalidate(&broker_url.to_string());

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
    invalidate_worker_caches(broker_url, worker_id);

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

#[cfg(test)]
mod tests {
    use super::*;

    /// Stand-in for a per-worker broker round trip (e.g. heartbeat `GET`):
    /// deterministic and independent of every other call, exactly the shape
    /// `fetch_worker_list_entry` has for real Redis keys.
    async fn stub_fetch(worker_index: usize) -> String {
        format!("worker-{worker_index}")
    }

    /// The parallel-fetch pattern used throughout this module's read paths:
    /// run an async operation for every item in a `Vec` via
    /// `futures::future::join_all` instead of a `for` loop that awaits one
    /// item at a time. This proves that pattern returns the same,
    /// order-preserving result set as the equivalent serial loop, using a
    /// stub async closure so no live broker is involved — matching how
    /// `fetch_worker_list`'s per-worker heartbeat lookups are parallelized in
    /// production.
    #[tokio::test]
    async fn parallel_join_all_matches_equivalent_serial_loop() {
        let indices: Vec<usize> = (0..30).collect();

        let mut serial = Vec::with_capacity(indices.len());
        for i in indices.clone() {
            serial.push(stub_fetch(i).await);
        }

        let parallel: Vec<String> =
            futures::future::join_all(indices.into_iter().map(stub_fetch)).await;

        assert_eq!(
            parallel, serial,
            "join_all must preserve input order and match the serial result set"
        );
    }

    #[test]
    fn worker_stats_cache_hit_after_insert_then_miss_after_invalidate() {
        let key = (
            "test://worker-rs-cache-1".to_string(),
            "w-cache-1".to_string(),
        );
        let snapshot = WorkerStatsSnapshot {
            heartbeat: "2026-01-01T00:00:00Z".to_string(),
            tasks_processed: Some("10".to_string()),
            tasks_failed: Some("1".to_string()),
            uptime_seconds: Some("3600".to_string()),
        };

        worker_stats_cache().insert(key.clone(), snapshot.clone());
        assert_eq!(worker_stats_cache().get(&key), Some(snapshot));

        worker_stats_cache().invalidate(&key);
        assert_eq!(worker_stats_cache().get(&key), None);
    }

    #[test]
    fn worker_list_cache_hit_after_insert_then_miss_after_invalidate() {
        let key = "test://worker-rs-cache-2".to_string();
        let entries = vec![WorkerListEntry {
            id: "w-cache-2".to_string(),
            status: "Active".to_string(),
            last_heartbeat: "2026-01-01T00:00:00Z".to_string(),
        }];

        worker_list_cache().insert(key.clone(), entries.clone());
        assert_eq!(worker_list_cache().get(&key), Some(entries));

        worker_list_cache().invalidate(&key);
        assert_eq!(worker_list_cache().get(&key), None);
    }

    #[test]
    fn invalidate_worker_caches_clears_both_caches() {
        let broker = "test://worker-rs-cache-3";
        let worker_id = "w-cache-3";

        worker_stats_cache().insert(
            (broker.to_string(), worker_id.to_string()),
            WorkerStatsSnapshot {
                heartbeat: "2026-01-01T00:00:00Z".to_string(),
                tasks_processed: None,
                tasks_failed: None,
                uptime_seconds: None,
            },
        );
        worker_list_cache().insert(broker.to_string(), vec![]);

        invalidate_worker_caches(broker, worker_id);

        assert_eq!(
            worker_stats_cache().get(&(broker.to_string(), worker_id.to_string())),
            None
        );
        assert_eq!(worker_list_cache().get(&broker.to_string()), None);
    }

    #[test]
    fn worker_cache_stats_reflects_underlying_cache_activity() {
        let key = "test://worker-rs-cache-stats-accessor".to_string();

        let (list_before, stats_before) = worker_cache_stats();

        worker_list_cache().insert(key.clone(), vec![]);
        worker_list_cache().get(&key); // guaranteed hit

        let (list_after, stats_after) = worker_cache_stats();

        assert!(
            list_after.len >= list_before.len,
            "list-cache entry count must never decrease from an insert alone"
        );
        assert!(
            list_after.hits > list_before.hits,
            "worker_cache_stats must observe the hit just recorded on worker_list_cache"
        );
        // The stats-cache side is untouched by this test; other tests may
        // run concurrently against it (this module's tests share one
        // process-wide static), but its counters only ever increase.
        assert!(stats_after.hits >= stats_before.hits);
        assert!(stats_after.misses >= stats_before.misses);
    }
}
