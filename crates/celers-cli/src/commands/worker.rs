//! Worker management command implementations.

use crate::cache::{CacheStats, TtlCache};
use crate::config::CacheConfig;
use crate::pool::pooled_redis_connection;
use celers_broker_redis::{QueueMode, RedisBroker, RedisControlTransport};
use celers_core::time_limit::WorkerTimeLimits;
use celers_core::ControlTransport;
use celers_worker::{wait_for_signal, RevocationWatcher, Worker, WorkerConfig};
use chrono::Utc;
use colored::Colorize;
use std::sync::{Arc, OnceLock};
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

/// Grace period [`start_worker`] waits for in-flight tasks to finish during
/// shutdown before giving up and exiting anyway.
///
/// Precedence: `override_secs` (threaded from `Commands::Worker`'s
/// `--shutdown-timeout` flag via `cli::dispatch`) wins when set to a
/// positive value; otherwise the `CELERS_WORKER_SHUTDOWN_TIMEOUT_SECS`
/// environment variable is consulted; otherwise this falls back to 30
/// seconds. A `0` from either source is treated as "not set" -- a
/// zero-length timeout would make shutdown behave exactly like the old
/// unconditional `abort()` again.
fn worker_shutdown_timeout(override_secs: Option<u64>) -> std::time::Duration {
    let secs = override_secs
        .filter(|&secs| secs > 0)
        .or_else(|| {
            std::env::var("CELERS_WORKER_SHUTDOWN_TIMEOUT_SECS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|&secs| secs > 0)
        })
        .unwrap_or(30);
    std::time::Duration::from_secs(secs)
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
/// * `shutdown_timeout_secs` - Optional override for the graceful-shutdown
///   grace period (see [`worker_shutdown_timeout`]); `None` falls back to
///   `CELERS_WORKER_SHUTDOWN_TIMEOUT_SECS`, then a 30s default.
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
///     300,
///     None,
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
    shutdown_timeout_secs: Option<u64>,
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
    let visibility_timeout_secs = broker.visibility_timeout();
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
        queue_name: queue.to_string(),
        ..Default::default()
    };
    let hostname = config.hostname.clone();

    // Join the remote control channel so `celers inspect` / `celers control`
    // can reach this worker. Without it the worker runs fine but is invisible
    // to those commands, which is the sort of thing an operator only discovers
    // during an incident.
    let control_transport = RedisControlTransport::new(broker_url)?;
    let control_channel = control_transport.channel().to_string();

    println!();
    println!("Worker configuration:");
    println!("  Hostname: {}", hostname.yellow());
    println!("  Concurrency: {}", concurrency.to_string().yellow());
    println!("  Max retries: {}", max_retries.to_string().yellow());
    println!("  Timeout: {}s", timeout.to_string().yellow());
    println!("  Control channel: {}", control_channel.yellow());
    println!();

    // Create worker and start it with a real shutdown handshake:
    // `run_with_shutdown` returns a `WorkerHandle` whose `drain()` stops the
    // worker from dequeuing anything new and waits for every already
    // in-flight task to finish before the run loop exits. This replaces the
    // previous "abort()` the moment Ctrl+C is pressed" shutdown, which
    // printed "Shutting down gracefully..." and then dropped every
    // in-flight task mid-execution with no ack/nack/requeue -- recovery
    // depended entirely on the broker's visibility timeout expiring (idx
    // 336).
    let worker = Worker::new(broker, registry, config)
        .with_control_transport(Arc::new(control_transport) as Arc<dyn ControlTransport>)
        .with_broker_url(broker_url)
        // Both of these are no-ops until an operator uses them, and both are
        // what makes `celers control time-limit` / `celers control revoke
        // --terminate` do something instead of reporting "not configured":
        // the limits manager starts empty, and the revocation watcher only
        // trips tasks it is told to.
        .with_time_limits(WorkerTimeLimits::new())
        .with_revocation_watcher(RevocationWatcher::new())
        // And this is what feeds that watcher from outside the process: the
        // worker subscribes to the queue's revocation channel and checks the
        // queue's durable revoked set before running anything it dequeues, so
        // `celers control revoke` works even for a task revoked while this
        // worker was starting up.
        .with_broker_revocation();
    let handle = worker.run_with_shutdown().await?;
    println!("{}", "✓ Worker started successfully".green().bold());
    println!(
        "  Reachable as {} via `celers inspect ping --broker {}`",
        hostname.cyan(),
        broker_url.cyan()
    );
    println!("{}", "  Press Ctrl+C to stop gracefully".dimmed());
    println!();

    // Wait for shutdown signal
    wait_for_signal().await;

    let shutdown_timeout = worker_shutdown_timeout(shutdown_timeout_secs);
    println!();
    println!(
        "{}",
        format!(
            "Shutting down gracefully (waiting up to {}s for in-flight tasks to finish)...",
            shutdown_timeout.as_secs()
        )
        .yellow()
    );

    match tokio::time::timeout(shutdown_timeout, handle.drain()).await {
        Ok(Ok(())) => {
            println!(
                "{}",
                "✓ Worker stopped (all in-flight tasks completed)".green()
            );
        }
        Ok(Err(e)) => {
            eprintln!(
                "{} {e}",
                "⚠ Worker reported an error while draining:".yellow().bold()
            );
        }
        Err(_) => {
            let still_active = handle.stats().active();
            eprintln!(
                "{}",
                format!(
                    "⚠ Shutdown timed out after {}s with {still_active} task(s) still \
                     in flight; exiting anyway. Those tasks were NOT acked, nacked, or \
                     requeued by this command -- they remain claimed in the broker's \
                     processing queue until its visibility timeout ({visibility_timeout_secs}s) \
                     recovers them. Increase --shutdown-timeout (or the \
                     CELERS_WORKER_SHUTDOWN_TIMEOUT_SECS env var) if this happens routinely.",
                    shutdown_timeout.as_secs(),
                )
                .red()
                .bold()
            );
            // Best-effort: ask the (still-running, now-detached) worker loop
            // to stop at its next check, even though this command can no
            // longer wait for that to happen.
            let _ = handle.shutdown().await;
        }
    }

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
    let mut conn = client
        .get_multiplexed_async_connection_with_config(&crate::pool::async_connection_config())
        .await?;

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

/// Pause task processing for a worker.
///
/// Records the pause flag in Redis and, mirroring [`stop_worker`]'s
/// shutdown channels, publishes a notification and reports the *actual*
/// subscriber count instead of an unconditional "✓". No component in
/// `celers-worker` currently subscribes to a pause channel or reads
/// `pause_key` at all -- see the crate-level followups -- so, until that
/// lands, an honest "no worker is listening" is the only truthful thing
/// this command can report (idx 324).
pub async fn pause_worker(broker_url: &str, worker_id: &str) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client
        .get_multiplexed_async_connection_with_config(&crate::pool::async_connection_config())
        .await?;

    let pause_key = format!("celers:worker:{worker_id}:paused");
    let timestamp = chrono::Utc::now().to_rfc3339();

    let _: () = redis::cmd("SET")
        .arg(&pause_key)
        .arg(&timestamp)
        .query_async(&mut conn)
        .await?;

    let channel = format!("celers:worker:{worker_id}:pause");
    let subscribers: usize = redis::cmd("PUBLISH")
        .arg(&channel)
        .arg("PAUSE")
        .query_async(&mut conn)
        .await?;
    invalidate_worker_caches(broker_url, worker_id);

    if subscribers > 0 {
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
    } else {
        println!(
            "{}",
            format!("⚠ Pause flag recorded for worker '{worker_id}', but no worker is listening")
                .yellow()
                .bold()
        );
        println!();
        println!("The pause flag was written to Redis (key: {pause_key}), but no running");
        println!("worker process subscribed to its pause channel -- celers-worker does not");
        println!("yet act on it, so this has no effect on task processing.");
    }

    Ok(())
}

/// Resume task processing for a worker. See [`pause_worker`] for why this
/// reports the real subscriber count instead of an unconditional "✓".
pub async fn resume_worker(broker_url: &str, worker_id: &str) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client
        .get_multiplexed_async_connection_with_config(&crate::pool::async_connection_config())
        .await?;

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

    let channel = format!("celers:worker:{worker_id}:resume");
    let subscribers: usize = redis::cmd("PUBLISH")
        .arg(&channel)
        .arg("RESUME")
        .query_async(&mut conn)
        .await?;
    invalidate_worker_caches(broker_url, worker_id);

    if subscribers > 0 {
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
    } else {
        println!(
            "{}",
            format!("⚠ Pause flag cleared for worker '{worker_id}', but no worker is listening")
                .yellow()
                .bold()
        );
        println!("(no running worker process subscribed to its resume channel)");
    }

    Ok(())
}

/// Scale workers to N instances
pub async fn scale_workers(broker_url: &str, target_count: usize) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client
        .get_multiplexed_async_connection_with_config(&crate::pool::async_connection_config())
        .await?;

    println!(
        "{}",
        format!("=== Scale Workers to {target_count} ===")
            .bold()
            .cyan()
    );
    println!();

    // Get current worker count. `SCAN` (cursor-based, bounded per call cost)
    // rather than a blocking `KEYS celers:worker:*:heartbeat`, which locks
    // up the entire single-threaded Redis server for the duration of the
    // call on a large keyspace -- the same fix already applied to the other
    // 5 sites that used to duplicate this exact pattern (idx 331).
    let current_count = crate::commands::monitoring::report::scan_worker_heartbeat_keys(&mut conn)
        .await?
        .len();

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
    let mut conn = client
        .get_multiplexed_async_connection_with_config(&crate::pool::async_connection_config())
        .await?;

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

    // Mirror `pause_worker`/`stop_worker`: publish and report the real
    // subscriber count rather than an unconditional "✓" (idx 324). No
    // component in `celers-worker` currently subscribes to a drain channel.
    let channel = format!("celers:worker:{worker_id}:drain");
    let subscribers: usize = redis::cmd("PUBLISH")
        .arg(&channel)
        .arg("DRAIN")
        .query_async(&mut conn)
        .await?;
    invalidate_worker_caches(broker_url, worker_id);

    if subscribers > 0 {
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
    } else {
        println!(
            "{}",
            format!("⚠ Drain flag recorded for worker '{worker_id}', but no worker is listening")
                .yellow()
                .bold()
        );
        println!();
        println!("The drain flag was written to Redis (key: {drain_key}), but no running");
        println!("worker process subscribed to its drain channel -- celers-worker does not");
        println!("yet act on it, so this has no effect on task processing.");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Local Redis used by this module's live-broker regression tests.
    const TEST_BROKER_URL: &str = "redis://127.0.0.1:6379";

    /// Serializes tests that mutate the process-wide
    /// `CELERS_WORKER_SHUTDOWN_TIMEOUT_SECS` environment variable. Only
    /// matters for the plain `cargo test` fallback runner (nextest, this
    /// crate's primary runner, isolates each test in its own process).
    fn shutdown_timeout_env_guard() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Regression test for the 6th (and last) idx-331 site: `scale_workers`
    /// used to count workers via a blocking `KEYS celers:worker:*:heartbeat`
    /// (locks up the single-threaded Redis server for the call's duration on
    /// a large keyspace), unlike the other 5 sites already swapped to the
    /// cursor-based `scan_worker_heartbeat_keys`. This proves the swapped
    /// call still completes cleanly end-to-end against live Redis with a
    /// real heartbeat key present.
    #[tokio::test]
    async fn scale_workers_counts_via_scan_not_blocking_keys() {
        let worker_id = format!("test-scale-{}", uuid::Uuid::new_v4());
        let heartbeat_key = format!("celers:worker:{worker_id}:heartbeat");

        let client = redis::Client::open(TEST_BROKER_URL).expect("client");
        let mut conn = client
            .get_multiplexed_async_connection_with_config(&crate::pool::async_connection_config())
            .await
            .expect("conn");
        let _: () = redis::cmd("SET")
            .arg(&heartbeat_key)
            .arg("alive")
            .query_async(&mut conn)
            .await
            .expect("seed heartbeat key");

        let current_count =
            crate::commands::monitoring::report::scan_worker_heartbeat_keys(&mut conn)
                .await
                .expect("scan_worker_heartbeat_keys")
                .len();
        assert!(
            current_count >= 1,
            "the just-seeded heartbeat key must be counted"
        );

        scale_workers(TEST_BROKER_URL, current_count + 1)
            .await
            .expect("scale_workers must complete without error against live Redis");

        let _: () = redis::cmd("DEL")
            .arg(&heartbeat_key)
            .query_async(&mut conn)
            .await
            .unwrap_or(());
    }

    /// Regression test for the `--shutdown-timeout` half of idx 336:
    /// `worker_shutdown_timeout` must honor a valid env-var override, fall
    /// back to a sane default (30s) when unset, and never panic or silently
    /// produce a zero-length timeout (which would make shutdown behave
    /// exactly like the old unconditional `abort()` again) on a malformed
    /// value.
    #[test]
    fn worker_shutdown_timeout_reads_env_with_sane_fallback() {
        let _guard = shutdown_timeout_env_guard();

        std::env::remove_var("CELERS_WORKER_SHUTDOWN_TIMEOUT_SECS");
        assert_eq!(
            worker_shutdown_timeout(None),
            std::time::Duration::from_secs(30)
        );

        std::env::set_var("CELERS_WORKER_SHUTDOWN_TIMEOUT_SECS", "5");
        assert_eq!(
            worker_shutdown_timeout(None),
            std::time::Duration::from_secs(5)
        );

        std::env::set_var("CELERS_WORKER_SHUTDOWN_TIMEOUT_SECS", "not-a-number");
        assert_eq!(
            worker_shutdown_timeout(None),
            std::time::Duration::from_secs(30),
            "an unparseable override must fall back to the default"
        );

        std::env::set_var("CELERS_WORKER_SHUTDOWN_TIMEOUT_SECS", "0");
        assert_eq!(
            worker_shutdown_timeout(None),
            std::time::Duration::from_secs(30),
            "a zero timeout would make every shutdown behave like an immediate hard-abort again"
        );

        std::env::remove_var("CELERS_WORKER_SHUTDOWN_TIMEOUT_SECS");
    }

    /// Regression test for the `--shutdown-timeout` CLI flag itself (the
    /// second half of idx 336, wired up once `Commands::Worker`/
    /// `cli::dispatch` were in scope): an explicit `override_secs` must win
    /// over the environment variable, and a `0` override must fall through
    /// to the env var (or default) exactly like an unset env var does,
    /// never producing a zero-length timeout.
    #[test]
    fn worker_shutdown_timeout_explicit_override_wins_over_env_var() {
        let _guard = shutdown_timeout_env_guard();

        std::env::remove_var("CELERS_WORKER_SHUTDOWN_TIMEOUT_SECS");
        assert_eq!(
            worker_shutdown_timeout(Some(15)),
            std::time::Duration::from_secs(15),
            "an explicit override must be honored with no env var set"
        );

        std::env::set_var("CELERS_WORKER_SHUTDOWN_TIMEOUT_SECS", "5");
        assert_eq!(
            worker_shutdown_timeout(Some(15)),
            std::time::Duration::from_secs(15),
            "an explicit CLI-flag override must win over the environment variable"
        );
        assert_eq!(
            worker_shutdown_timeout(Some(0)),
            std::time::Duration::from_secs(5),
            "a zero override must fall through to the env var, not zero out the timeout"
        );
        assert_eq!(
            worker_shutdown_timeout(None),
            std::time::Duration::from_secs(5),
            "no override at all must still fall through to the env var"
        );

        std::env::remove_var("CELERS_WORKER_SHUTDOWN_TIMEOUT_SECS");
        assert_eq!(
            worker_shutdown_timeout(Some(0)),
            std::time::Duration::from_secs(30),
            "a zero override with no env var must fall through to the default"
        );
    }

    /// Regression test for idx 336's core mechanism: `start_worker` now
    /// shuts down via `WorkerHandle::drain()` (set draining, wait for
    /// `stats.active() == 0`) wrapped in a bounded `tokio::time::timeout`,
    /// instead of an unconditional `worker_task.abort()`. This proves that
    /// exact sequence -- `run_with_shutdown` -> `drain()` under a timeout --
    /// completes promptly (well inside the bound) and leaves `active() ==
    /// 0` when there is no in-flight work, i.e. the happy path a graceful
    /// shutdown should always hit does not hang or spuriously time out.
    #[tokio::test]
    async fn worker_handle_drain_completes_promptly_with_no_in_flight_tasks() {
        let queue_name = format!("test-worker-drain-{}", uuid::Uuid::new_v4());
        let broker = RedisBroker::new(TEST_BROKER_URL, &queue_name).expect("broker");
        let registry = celers_core::TaskRegistry::new();
        let config = WorkerConfig::default();

        let worker = Worker::new(broker, registry, config);
        let handle = worker
            .run_with_shutdown()
            .await
            .expect("run_with_shutdown must hand back a WorkerHandle");

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), handle.drain()).await;
        assert!(
            result.is_ok(),
            "drain() must complete well within the shutdown timeout when nothing is in flight"
        );
        assert_eq!(handle.stats().active(), 0);
    }

    /// Regression test for idx 324 (worker pause/drain honesty): proves
    /// `pause_worker` actually `PUBLISH`es to a real, discoverable channel
    /// (`celers:worker:{id}:pause`) rather than only ever writing a key
    /// nothing reads -- the same mechanism `stop_worker` already used, now
    /// extended to pause/resume/drain so a future `celers-worker`
    /// subscriber has something real to listen for, and so this command's
    /// "no worker is listening" branch is backed by a genuine subscriber
    /// count rather than a hardcoded guess.
    #[tokio::test]
    async fn pause_worker_publishes_to_a_discoverable_pause_channel() {
        use futures::StreamExt;

        let worker_id = format!("test-worker-pause-{}", uuid::Uuid::new_v4());
        let client = redis::Client::open(TEST_BROKER_URL).expect("client");

        let mut pubsub = client.get_async_pubsub().await.expect("pubsub");
        let channel = format!("celers:worker:{worker_id}:pause");
        pubsub.subscribe(&channel).await.expect("subscribe");
        let mut messages = pubsub.on_message();

        pause_worker(TEST_BROKER_URL, &worker_id)
            .await
            .expect("pause_worker");

        let msg = tokio::time::timeout(std::time::Duration::from_secs(5), messages.next())
            .await
            .expect("must receive the PUBLISH within the timeout")
            .expect("subscribed channel must yield a message");
        let payload: String = msg.get_payload().expect("payload is a UTF-8 string");
        assert_eq!(payload, "PAUSE");
    }

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
