//! Worker management command implementations.

use crate::cache::{CacheStats, TtlCache};
use crate::commands::control::{self, ControlOptions};
use crate::config::CacheConfig;
use crate::pool::pooled_redis_connection;
use celers_broker_redis::{QueueMode, RedisBroker, RedisControlTransport};
use celers_core::control::ControlCommand;
use celers_core::time_limit::WorkerTimeLimits;
use celers_core::{ControlTransport, TaskRegistry};
use celers_worker::{wait_for_signal, RevocationWatcher, Worker, WorkerConfig};
use chrono::Utc;
use colored::Colorize;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
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

/// Total wall-clock budget [`probe_broker_connectivity`] retries within
/// before giving up.
///
/// Mirrors [`worker_shutdown_timeout`]'s precedence and "0 means unset"
/// convention: `override_secs` (from
/// [`WorkerStartupOptions::connect_timeout_secs`], threaded from
/// `Commands::Worker`'s `--broker-connect-timeout` flag) wins when set to a
/// positive value; otherwise the `CELERS_BROKER_CONNECT_TIMEOUT_SECS`
/// environment variable is consulted; otherwise this falls back to 30
/// seconds.
fn broker_connect_timeout(override_secs: Option<u64>) -> std::time::Duration {
    let secs = override_secs
        .filter(|&secs| secs > 0)
        .or_else(|| {
            std::env::var("CELERS_BROKER_CONNECT_TIMEOUT_SECS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|&secs| secs > 0)
        })
        .unwrap_or(30);
    std::time::Duration::from_secs(secs)
}

/// Initial backoff between [`probe_broker_connectivity`] retry attempts.
const CONNECT_PROBE_INITIAL_BACKOFF: Duration = Duration::from_millis(500);

/// Backoff ceiling: doubles after each failed attempt, capped here.
const CONNECT_PROBE_MAX_BACKOFF: Duration = Duration::from_secs(8);

/// Probe `broker`'s real reachability with a bounded exponential-backoff
/// retry loop (idx prod-gaps-4).
///
/// `RedisBroker::with_mode` performs no network I/O -- it only parses the
/// URL -- so without this, `start_worker` used to print a green "Connected"
/// line on zero evidence, and a stopped broker, a typo'd host, or a
/// firewalled port only surfaced later as dequeue errors inside the run
/// loop. This issues a real `PING` -- the cheapest round trip that still
/// proves the server is reachable and answering -- retrying within `budget`
/// with backoff starting at [`CONNECT_PROBE_INITIAL_BACKOFF`] and doubling
/// (capped at [`CONNECT_PROBE_MAX_BACKOFF`]) between attempts, so a
/// container started just before its broker converges does not immediately
/// fail. Every failed attempt is logged via [`tracing::warn`].
///
/// Returns the final successful `PING`'s latency in milliseconds.
///
/// # Errors
///
/// Returns the last `PING` error, with context, once `budget` is exhausted.
async fn probe_broker_connectivity(broker: &RedisBroker, budget: Duration) -> anyhow::Result<u64> {
    let deadline = Instant::now() + budget;
    let mut backoff = CONNECT_PROBE_INITIAL_BACKOFF;
    let mut attempt: u32 = 0;

    loop {
        attempt += 1;
        match broker.ping().await {
            Ok(latency_ms) => return Ok(latency_ms),
            Err(e) => {
                let now = Instant::now();
                if now >= deadline {
                    anyhow::bail!(
                        "could not reach the broker after {attempt} attempt(s) over {:.1}s: \
                         {e}. Pass --no-connect-check to skip this probe and start anyway.",
                        budget.as_secs_f64(),
                    );
                }
                tracing::warn!(
                    attempt,
                    error = %e,
                    "broker connectivity probe failed; retrying"
                );
                let remaining = deadline - now;
                let sleep_for = backoff.min(remaining);
                tokio::time::sleep(sleep_for).await;
                backoff = (backoff * 2).min(CONNECT_PROBE_MAX_BACKOFF);
            }
        }
    }
}

/// Bound for the control-channel probe below: a single attempt, not a
/// retrying budget like [`probe_broker_connectivity`]'s. By the time this
/// runs, the broker probe has already proven the network path is up, so a
/// further failure here is very likely a permission problem (a Redis 6 ACL
/// can grant `PING`/`LPUSH` while denying `SUBSCRIBE`) rather than a
/// transient one, and retrying will not fix a permission problem.
const CONTROL_CHANNEL_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// Confirm the worker can actually subscribe to its control channel (idx
/// prod-gaps-4's "apply the same probe to `RedisControlTransport`").
///
/// [`RedisControlTransport::new`] performs no network I/O -- like
/// `RedisBroker::with_mode`, it only parses the URL -- so a worker that
/// cannot reach the channel previously looked identical to one that could,
/// right up until `celers inspect`/`celers control` silently found nobody
/// home. `RedisControlTransport` has no probe method of its own, so this
/// opens a throwaway `SUBSCRIBE` directly against the same Redis URL as the
/// cheapest available proof, without adding a dependency on
/// `celers-broker-redis` beyond what this crate already has.
///
/// # Errors
///
/// Returns an error if the connection or subscription fails or times out.
async fn probe_control_channel(broker_url: &str, channel: &str) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)
        .map_err(|e| anyhow::anyhow!("invalid broker url '{broker_url}': {e}"))?;
    let subscribe = async {
        let mut pubsub = client.get_async_pubsub().await?;
        pubsub.subscribe(channel).await
    };
    match tokio::time::timeout(CONTROL_CHANNEL_PROBE_TIMEOUT, subscribe).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => anyhow::bail!(
            "could not subscribe to control channel '{channel}': {e} (a Redis ACL can permit \
             PING/LPUSH while denying SUBSCRIBE -- check the user's channel permissions, or \
             pass --no-connect-check to skip this probe)"
        ),
        Err(_) => anyhow::bail!(
            "timed out after {:.0}s subscribing to control channel '{channel}'. Pass \
             --no-connect-check to skip this probe and start anyway.",
            CONTROL_CHANNEL_PROBE_TIMEOUT.as_secs_f64()
        ),
    }
}

/// Extra, less-frequently-tuned knobs for [`start_worker`], grouped into one
/// struct so its parameter count does not grow unbounded as new startup
/// behaviors are added (mirrors `loadtest_cmds::LoadTestConfig`'s "core args
/// stay positional, everything else bundles into a struct" convention).
#[derive(Debug, Clone, Default)]
pub struct WorkerStartupOptions {
    /// Grace period `start_worker` waits for in-flight tasks to finish
    /// during shutdown; see [`worker_shutdown_timeout`]. `None` falls back
    /// to `CELERS_WORKER_SHUTDOWN_TIMEOUT_SECS`, then a 30s default.
    pub shutdown_timeout_secs: Option<u64>,

    /// Skip the startup connectivity probe (both the broker `PING` and the
    /// control-channel `SUBSCRIBE` check) entirely and go straight to
    /// `Worker::run_with_shutdown` -- the escape hatch for a broker only
    /// reachable after the worker itself establishes a tunnel, or an
    /// operator who has already verified connectivity another way and does
    /// not want to pay the probe's latency.
    pub no_connect_check: bool,

    /// Total wall-clock budget for the connectivity probe's bounded
    /// exponential-backoff retry loop; see [`broker_connect_timeout`].
    /// Ignored when `no_connect_check` is set.
    pub connect_timeout_secs: Option<u64>,

    /// Register the built-in demo tasks (see
    /// `commands::worker_demo_tasks::register_demo_tasks`) instead of
    /// starting with a genuinely empty registry, so a freshly deployed
    /// worker can be smoke-tested before any real task code exists.
    pub demo_tasks: bool,
}

/// Start a worker with the given configuration.
///
/// Creates and runs a worker that processes tasks from the specified queue.
/// The worker will run until it receives a shutdown signal (Ctrl+C).
///
/// # Arguments
///
/// * `broker_url` - Redis connection URL (e.g., `redis://localhost:6379`).
///   `celers worker` supports Redis only; a well-formed URL for a different
///   broker (e.g. `postgres://...`) is rejected with a clear error rather
///   than attempted.
/// * `queue` - Queue name to process tasks from
/// * `mode` - Queue mode: "fifo" for FIFO, "priority" for priority-based
/// * `concurrency` - Maximum number of concurrent tasks to process
/// * `max_retries` - Maximum retry attempts for failed tasks
/// * `timeout` - Task execution timeout in seconds
/// * `options` - Less-frequently-tuned startup knobs; see
///   [`WorkerStartupOptions`].
///
/// # Returns
///
/// Returns `Ok(())` on successful shutdown, or an error if the broker URL is
/// invalid, the startup connectivity probe fails (see
/// [`WorkerStartupOptions::no_connect_check`]), or the worker fails to
/// start.
///
/// # Examples
///
/// ```no_run
/// # use celers_cli::commands::start_worker;
/// # use celers_cli::commands::WorkerStartupOptions;
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
///     &WorkerStartupOptions::default(),
/// ).await?;
/// # Ok(())
/// # }
/// ```
// Long mainly because of the registry-setup warning's many `println!` lines
// (see `commands::control::print_inspect` for the same allow on the same
// kind of function).
#[allow(clippy::too_many_lines)]
pub async fn start_worker(
    broker_url: &str,
    queue: &str,
    mode: &str,
    concurrency: usize,
    max_retries: u32,
    timeout: u64,
    options: &WorkerStartupOptions,
) -> anyhow::Result<()> {
    println!("{}", "=== CeleRS Worker ===".bold().green());
    println!();

    // Syntactic validation (idx prod-gaps-11): reject a broker URL that
    // cannot possibly work before attempting anything against it, with
    // wording `errors::classify_anyhow` recognizes as E_BAD_BROKER_URL.
    crate::config_validation::require_well_formed(broker_url).map_err(|e| anyhow::anyhow!(e))?;
    let scheme = crate::config_validation::scheme_of(broker_url);
    if !matches!(scheme, "redis" | "rediss") {
        anyhow::bail!(
            "unsupported broker scheme '{scheme}': `celers worker` only supports Redis broker \
             URLs (redis:// or rediss://) today. Got: {broker_url}"
        );
    }

    // Parse queue mode
    let queue_mode = match mode.to_lowercase().as_str() {
        "priority" => QueueMode::Priority,
        _ => QueueMode::Fifo,
    };

    // Create broker. This alone performs no network I/O -- it only parses
    // the URL -- which is exactly why the probe below exists (idx
    // prod-gaps-4).
    let broker = RedisBroker::with_mode(broker_url, queue, queue_mode)?;
    let visibility_timeout_secs = broker.visibility_timeout();

    // Join the remote control channel so `celers inspect` / `celers control`
    // can reach this worker. Without it the worker runs fine but is invisible
    // to those commands, which is the sort of thing an operator only discovers
    // during an incident. Constructing this, like the broker above, performs
    // no network I/O by itself.
    let control_transport = RedisControlTransport::new(broker_url)?;
    let control_channel = control_transport.channel().to_string();

    if options.no_connect_check {
        println!(
            "{}",
            format!(
                "⚠ Broker connectivity check skipped (--no-connect-check); proceeding \
                 without verifying {broker_url} is reachable"
            )
            .yellow()
        );
    } else {
        let connect_timeout = broker_connect_timeout(options.connect_timeout_secs);
        let latency_ms = probe_broker_connectivity(&broker, connect_timeout).await?;
        println!(
            "✓ Connected to Redis: {} ({latency_ms}ms)",
            broker_url.cyan()
        );
        probe_control_channel(broker_url, &control_channel).await?;
        println!("✓ Control channel reachable: {}", control_channel.cyan());
    }
    println!("✓ Queue: {} (mode: {})", queue.cyan(), mode.cyan());

    // Create task registry: either genuinely empty (tasks are Rust types
    // registered at build time, not something this command can conjure) or,
    // with --demo-tasks, seeded with a handful of harmless built-ins so a
    // fresh deployment can be smoke-tested before any real task code
    // exists. See `commands::worker_demo_tasks`'s module comment for the
    // full rationale.
    let registry = TaskRegistry::new();
    if options.demo_tasks {
        super::worker_demo_tasks::register_demo_tasks(&registry).await;
        println!(
            "{}",
            format!(
                "✓ Registered {} demo task(s): {}",
                super::worker_demo_tasks::DEMO_TASK_NAMES.len(),
                super::worker_demo_tasks::DEMO_TASK_NAMES.join(", ")
            )
            .green()
        );
        println!(
            "  {}",
            "(each accepts any JSON payload; demo.fail always fails on purpose -- useful".dimmed()
        );
        println!(
            "  {}",
            "for exercising retry/DLQ handling against this deployment)".dimmed()
        );
    } else {
        println!(
            "{}",
            "⚠ No tasks registered -- this worker cannot execute anything on its own."
                .yellow()
                .bold()
        );
        println!(
            "  {}",
            "`celers worker` has no way to compile in application task code: tasks are Rust"
                .dimmed()
        );
        println!(
            "  {}",
            "types registered with a TaskRegistry at build time, not something a CLI flag".dimmed()
        );
        println!(
            "  {}",
            "or config file can express. Embed celers-worker (or the celers facade) in your"
                .dimmed()
        );
        println!(
            "  {}",
            "own binary and register your tasks there -- see README.md (\"Define a Task\" /"
                .dimmed()
        );
        println!(
            "  {}",
            "\"Start a Worker\") and `cargo run -p celers-examples --example macro_tasks`".dimmed()
        );
        println!("  {}", "for a complete, runnable pattern.".dimmed());
        println!(
            "  {}",
            "Pass --demo-tasks to register a couple of harmless built-ins instead, so you".dimmed()
        );
        println!("  {}", "can smoke-test this deployment right now.".dimmed());
    }

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

    let shutdown_timeout = worker_shutdown_timeout(options.shutdown_timeout_secs);
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

/// Redis key [`pause_worker`]/[`resume_worker`] use to record, in Redis,
/// that a pause was requested through this command. Read by `celers
/// report`/`celers doctor`.
fn pause_flag_key(worker_id: &str) -> String {
    format!("celers:worker:{worker_id}:paused")
}

/// Pause task processing for a worker by suspending its consumption of
/// `queue` over the real-time control channel (`celers control
/// cancel-consumer`, targeted at this one worker's hostname).
///
/// This reaches an actually-running worker: the previous implementation
/// only ever published to `celers:worker:{id}:pause`, a channel nothing in
/// `celers-worker` ever subscribed to, so it could report only "no worker
/// is listening" (idx 324) no matter what. Its (unconditional) reply is now
/// the worker's own, real acknowledgement.
///
/// `queue` must be the queue this worker was actually started on (mirroring
/// `celers control cancel-consumer`'s own requirement) -- a worker replies
/// with an error, not a silent no-op, when it does not match, which this
/// prints in full since a mismatch is otherwise easy to misread as a
/// successful pause.
///
/// This also records an *advisory* timestamp under
/// `celers:worker:{worker_id}:paused` in Redis, which `celers report` and
/// `celers doctor` read: it records only that a pause was *requested*
/// through this command, not the worker's actual live mode -- the
/// control-channel reply above (printed first) is the authoritative signal
/// for whether it actually happened. A worker that never answers still gets
/// this key written, on the theory that "a pause was asked for" remains
/// true even when nobody was listening to hear it.
///
/// # Errors
///
/// Returns an error if the control channel or the advisory Redis write
/// cannot be reached.
pub async fn pause_worker(broker_url: &str, worker_id: &str, queue: &str) -> anyhow::Result<()> {
    println!(
        "{}",
        format!("=== Pause Worker: {worker_id} (queue: {queue}) ===")
            .bold()
            .yellow()
    );
    println!(
        "  {}",
        "queue must be the queue this worker was started on, or it answers with an error".dimmed()
    );
    println!();

    let mut options = ControlOptions::new(broker_url);
    options.destination = vec![worker_id.to_string()];
    options.timeout_secs = 1.0;
    control::run_control(&options, ControlCommand::cancel_consumer(queue)).await?;

    let client = redis::Client::open(broker_url)?;
    let mut conn = client
        .get_multiplexed_async_connection_with_config(&crate::pool::async_connection_config())
        .await?;
    let timestamp = Utc::now().to_rfc3339();
    let _: () = redis::cmd("SET")
        .arg(pause_flag_key(worker_id))
        .arg(&timestamp)
        .query_async(&mut conn)
        .await?;
    invalidate_worker_caches(broker_url, worker_id);

    println!();
    println!(
        "  {}",
        format!(
            "(recorded a pause request for '{worker_id}' at {timestamp}; \
                  see the reply above for whether it actually took effect)"
        )
        .dimmed()
    );

    Ok(())
}

/// Resume task processing for a worker by re-enabling consumption of
/// `queue` over the control channel (`celers control add-consumer`,
/// targeted at this one worker's hostname). See [`pause_worker`] for why
/// `queue` must match the worker's own queue and why this reaches a real
/// worker instead of only reporting subscriber counts.
///
/// Unlike the previous implementation, this always asks -- it does not
/// first check the advisory `celers:worker:{worker_id}:paused` Redis key
/// and skip the request when absent. `add-consumer` is idempotent (a
/// worker that was never paused simply re-acknowledges "already
/// consuming"), and the advisory key can be stale (written by a request
/// that reached a worker which has since restarted, or written for a
/// worker that was never actually reached at all), so trusting it to decide
/// whether to *ask* would just relocate the same "green light on no
/// evidence" problem [`pause_worker`] exists to fix.
///
/// # Errors
///
/// Returns an error if the control channel or the advisory Redis write
/// cannot be reached.
pub async fn resume_worker(broker_url: &str, worker_id: &str, queue: &str) -> anyhow::Result<()> {
    println!(
        "{}",
        format!("=== Resume Worker: {worker_id} (queue: {queue}) ===")
            .bold()
            .cyan()
    );
    println!(
        "  {}",
        "queue must be the queue this worker was started on, or it answers with an error".dimmed()
    );
    println!();

    let mut options = ControlOptions::new(broker_url);
    options.destination = vec![worker_id.to_string()];
    options.timeout_secs = 1.0;
    control::run_control(&options, ControlCommand::add_consumer(queue)).await?;

    let client = redis::Client::open(broker_url)?;
    let mut conn = client
        .get_multiplexed_async_connection_with_config(&crate::pool::async_connection_config())
        .await?;
    let _: () = redis::cmd("DEL")
        .arg(pause_flag_key(worker_id))
        .query_async(&mut conn)
        .await?;
    invalidate_worker_caches(broker_url, worker_id);

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

/// Drain a worker: ask it, over the control channel, to stop consuming new
/// tasks, finish whatever it is already running, then **exit**.
///
/// This is `celers control shutdown` (with an optional `grace_secs` grace
/// period) targeted at this one worker's hostname. Unlike [`pause_worker`],
/// this is one-way and terminal: a successfully-drained worker does not come
/// back on its own, so unlike the previous implementation, this prints no
/// "resume with `celers worker-mgmt resume`" instruction -- that command
/// only re-enables consumption on a worker that is still running, which a
/// drained one, by design, no longer is. Start a new worker process (`celers
/// worker`) once it has exited.
///
/// The previous implementation also wrote a `celers:worker:{worker_id}:draining`
/// Redis key with a 24-hour TTL for `celers report`/`celers doctor` to read.
/// That key is not written here: under the old "still running, just not
/// consuming" semantics a long-lived flag made sense, but now that drain is
/// terminal, a flag that outlives the worker it describes (which has
/// already exited) would be actively misleading rather than merely stale.
///
/// # Errors
///
/// Returns an error if the control channel cannot be reached.
pub async fn drain_worker(
    broker_url: &str,
    worker_id: &str,
    grace_secs: Option<u64>,
) -> anyhow::Result<()> {
    println!(
        "{}",
        format!("=== Drain Worker: {worker_id} ===").bold().cyan()
    );
    println!();
    println!("This asks the worker to stop consuming, finish any in-flight task(s), then");
    println!("EXIT -- unlike pause, a successfully-drained worker does not come back on");
    println!("its own; start a new one (`celers worker`) once it has exited.");
    println!();

    let mut options = ControlOptions::new(broker_url);
    options.destination = vec![worker_id.to_string()];
    options.timeout_secs = 1.0;
    control::run_control(&options, ControlCommand::shutdown(grace_secs)).await?;

    invalidate_worker_caches(broker_url, worker_id);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use celers_core::{Broker, Task};

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

    /// Serializes tests that mutate the process-wide
    /// `CELERS_BROKER_CONNECT_TIMEOUT_SECS` environment variable. Mirrors
    /// [`shutdown_timeout_env_guard`]'s rationale for a different variable.
    fn connect_timeout_env_guard() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Regression test for idx prod-gaps-4's `--broker-connect-timeout` /
    /// `CELERS_BROKER_CONNECT_TIMEOUT_SECS` knob: mirrors
    /// `worker_shutdown_timeout_reads_env_with_sane_fallback` for the new
    /// connect-timeout equivalent -- same precedence, same "0 means unset"
    /// convention.
    #[test]
    fn broker_connect_timeout_reads_env_with_sane_fallback() {
        let _guard = connect_timeout_env_guard();

        std::env::remove_var("CELERS_BROKER_CONNECT_TIMEOUT_SECS");
        assert_eq!(
            broker_connect_timeout(None),
            std::time::Duration::from_secs(30)
        );

        std::env::set_var("CELERS_BROKER_CONNECT_TIMEOUT_SECS", "5");
        assert_eq!(
            broker_connect_timeout(None),
            std::time::Duration::from_secs(5)
        );

        std::env::set_var("CELERS_BROKER_CONNECT_TIMEOUT_SECS", "not-a-number");
        assert_eq!(
            broker_connect_timeout(None),
            std::time::Duration::from_secs(30),
            "an unparseable override must fall back to the default"
        );

        std::env::set_var("CELERS_BROKER_CONNECT_TIMEOUT_SECS", "0");
        assert_eq!(
            broker_connect_timeout(None),
            std::time::Duration::from_secs(30),
            "a zero timeout would make the probe give up before a single retry"
        );

        std::env::remove_var("CELERS_BROKER_CONNECT_TIMEOUT_SECS");
    }

    /// See [`worker_shutdown_timeout_explicit_override_wins_over_env_var`]
    /// for the equivalent test on the shutdown-timeout sibling.
    #[test]
    fn broker_connect_timeout_explicit_override_wins_over_env_var() {
        let _guard = connect_timeout_env_guard();

        std::env::remove_var("CELERS_BROKER_CONNECT_TIMEOUT_SECS");
        assert_eq!(
            broker_connect_timeout(Some(15)),
            std::time::Duration::from_secs(15)
        );

        std::env::set_var("CELERS_BROKER_CONNECT_TIMEOUT_SECS", "5");
        assert_eq!(
            broker_connect_timeout(Some(15)),
            std::time::Duration::from_secs(15),
            "an explicit CLI-flag override must win over the environment variable"
        );
        assert_eq!(
            broker_connect_timeout(Some(0)),
            std::time::Duration::from_secs(5),
            "a zero override must fall through to the env var, not zero out the budget"
        );

        std::env::remove_var("CELERS_BROKER_CONNECT_TIMEOUT_SECS");
    }

    /// Regression test for idx prod-gaps-4: `probe_broker_connectivity`
    /// must succeed quickly against a real, reachable Redis, and its
    /// returned latency must be a real (small) measurement.
    #[tokio::test]
    async fn probe_broker_connectivity_succeeds_against_a_live_broker() {
        let queue_name = format!("test-probe-ok-{}", uuid::Uuid::new_v4());
        let broker = RedisBroker::new(TEST_BROKER_URL, &queue_name).expect("broker");

        let latency_ms = probe_broker_connectivity(&broker, std::time::Duration::from_secs(5))
            .await
            .expect("a reachable broker must be probed successfully");
        assert!(
            latency_ms < 5000,
            "a local PING must complete well under the 5s budget, got {latency_ms}ms"
        );
    }

    /// Regression test for idx prod-gaps-4's core claim: against an
    /// unreachable broker, the probe must retry (not fail on the first
    /// attempt) and still respect its budget rather than hanging forever.
    /// `redis://127.0.0.1:1` (rather than a black-holed address) refuses the
    /// connection immediately, keeping this test fast.
    #[tokio::test]
    async fn probe_broker_connectivity_retries_then_gives_up_within_budget() {
        let broker = RedisBroker::new("redis://127.0.0.1:1", "test-probe-fail")
            .expect("with_mode performs no I/O, so an unreachable port still parses");

        let budget = std::time::Duration::from_millis(1200);
        let start = std::time::Instant::now();
        let err = probe_broker_connectivity(&broker, budget)
            .await
            .expect_err("an unreachable broker must fail the probe");
        let elapsed = start.elapsed();

        assert!(
            err.to_string().contains("--no-connect-check"),
            "the failure must point at the escape hatch: {err}"
        );
        assert!(
            !err.to_string()
                .to_lowercase()
                .contains("invalid broker url"),
            "a probe timeout is a connectivity failure, not a malformed URL -- it must not \
             misclassify via `errors::classify_anyhow`'s E_BAD_BROKER_URL bucket: {err}"
        );
        assert!(
            elapsed >= budget,
            "must not give up before the budget elapses: {elapsed:?} < {budget:?}"
        );
        assert!(
            elapsed < budget + std::time::Duration::from_secs(5),
            "must not overrun the budget by more than one extra backoff step: {elapsed:?}"
        );
    }

    /// Regression test for idx prod-gaps-4's control-channel half: a
    /// reachable Redis must let a `SUBSCRIBE` probe against the default
    /// control channel succeed.
    #[tokio::test]
    async fn probe_control_channel_succeeds_against_a_live_broker() {
        probe_control_channel(
            TEST_BROKER_URL,
            celers_core::control_transport::DEFAULT_CONTROL_CHANNEL,
        )
        .await
        .expect("a reachable broker must accept the SUBSCRIBE probe");
    }

    /// Regression test for idx prod-gaps-11 + prod-gaps-4: `start_worker`
    /// must reject a broker URL of the wrong scheme immediately, before
    /// ever touching the network or reaching `wait_for_signal` (which would
    /// otherwise hang this test forever).
    #[tokio::test]
    async fn start_worker_rejects_a_non_redis_broker_scheme_before_any_network_io() {
        let err = start_worker(
            "postgres://localhost:5432/db",
            "q",
            "fifo",
            1,
            1,
            30,
            &WorkerStartupOptions::default(),
        )
        .await
        .expect_err("a postgres:// url must be rejected by celers worker");
        assert!(err.to_string().contains("unsupported broker scheme"));
    }

    /// Regression test for idx prod-gaps-4: `start_worker` must fail fast
    /// (within roughly its configured budget) rather than printing a false
    /// "✓ Connected" line when the broker is unreachable, and must never
    /// reach `wait_for_signal` (which would otherwise hang this test
    /// forever).
    #[tokio::test]
    async fn start_worker_fails_fast_when_the_broker_is_unreachable() {
        let options = WorkerStartupOptions {
            connect_timeout_secs: Some(1),
            ..Default::default()
        };
        let start = std::time::Instant::now();
        let err = start_worker("redis://127.0.0.1:1", "q", "fifo", 1, 1, 30, &options)
            .await
            .expect_err("an unreachable broker must fail startup, not hang at wait_for_signal");
        assert!(
            start.elapsed() < std::time::Duration::from_secs(10),
            "must fail within roughly its configured budget, not hang"
        );
        assert!(err.to_string().contains("--no-connect-check"));
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

    /// A minimal task used by this module's live control-channel tests:
    /// completes immediately and counts how many times it ran, so a test can
    /// tell "consumed" from "not consumed" without inspecting worker
    /// internals.
    #[derive(serde::Serialize, serde::Deserialize)]
    struct Empty {}

    struct CountingTask {
        runs: Arc<std::sync::atomic::AtomicUsize>,
        name: String,
    }

    #[async_trait::async_trait]
    impl Task for CountingTask {
        type Input = Empty;
        type Output = Empty;

        async fn execute(&self, _input: Self::Input) -> celers_core::Result<Self::Output> {
            self.runs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(Empty {})
        }

        fn name(&self) -> &str {
            &self.name
        }
    }

    /// Start a real worker on a run-unique queue, subscribed to the
    /// *default* control channel -- the one [`pause_worker`]/
    /// [`resume_worker`]/[`drain_worker`] address, since none of them take a
    /// channel override -- and block until it answers a targeted ping, so a
    /// caller never races the `SUBSCRIBE` setup. Returns `(queue, hostname,
    /// handle)`.
    async fn start_live_control_worker(
        label: &str,
        registry: TaskRegistry,
    ) -> (String, String, celers_worker::WorkerHandle) {
        let run_id = uuid::Uuid::new_v4();
        let queue = format!("test-worker-mgmt-{label}-{run_id}");
        let hostname = format!("test-worker-mgmt-{label}-{run_id}");

        let broker =
            RedisBroker::with_mode(TEST_BROKER_URL, &queue, QueueMode::Fifo).expect("broker");
        let control_transport =
            RedisControlTransport::new(TEST_BROKER_URL).expect("control transport");

        let config = WorkerConfig {
            concurrency: 2,
            poll_interval_ms: 20,
            queue_name: queue.clone(),
            hostname: hostname.clone(),
            ..Default::default()
        };

        let worker = Worker::new(broker, registry, config)
            .with_control_transport(Arc::new(control_transport) as Arc<dyn ControlTransport>)
            .with_broker_url(TEST_BROKER_URL);
        let handle = worker.run_with_shutdown().await.expect("run_with_shutdown");

        let probe_transport =
            Arc::new(RedisControlTransport::new(TEST_BROKER_URL).expect("probe transport"));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let replies = celers_core::control_transport::ControlClient::new(Arc::clone(
                &probe_transport,
            )
                as Arc<dyn ControlTransport>)
            .with_timeout(std::time::Duration::from_millis(300))
            .with_destination(vec![hostname.clone()])
            .ping()
            .await
            .expect("ping");
            if !replies.is_empty() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "worker {hostname} never joined the control channel"
            );
        }

        (queue, hostname, handle)
    }

    /// Delete the Redis keys a [`start_live_control_worker`] queue used.
    async fn cleanup_test_queue(broker: &RedisBroker) {
        if let Ok(client) = redis::Client::open(TEST_BROKER_URL) {
            if let Ok(mut conn) = client
                .get_multiplexed_async_connection_with_config(
                    &crate::pool::async_connection_config(),
                )
                .await
            {
                let _: i64 = redis::cmd("DEL")
                    .arg(broker.queue_names())
                    .query_async(&mut conn)
                    .await
                    .unwrap_or(0);
            }
        }
    }

    /// Regression test for the worker-mgmt/control-client reimplementation
    /// (backlog: "worker-mgmt pause/resume/drain publish to channels
    /// nothing subscribes to"): proves `pause_worker`/`resume_worker` reach
    /// a real, running worker over the control channel and actually change
    /// whether it consumes its queue -- not merely report a subscriber
    /// count on a dead channel nothing ever listened to.
    ///
    /// Also covers the queue-mismatch behavior flagged during review:
    /// `pause_worker` addressed at the *wrong* queue must have no effect at
    /// all (the worker's real queue keeps consuming normally), proving the
    /// worker's own "this worker consumes only '{actual}', not '{given}'"
    /// rejection is genuinely enforced end-to-end, not merely documented.
    #[tokio::test]
    async fn pause_worker_and_resume_worker_control_a_live_worker() {
        let runs = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let registry = TaskRegistry::new();
        registry
            .register(CountingTask {
                runs: Arc::clone(&runs),
                name: "test.worker-mgmt.pause-resume".to_string(),
            })
            .await;

        let (queue, hostname, handle) = start_live_control_worker("pause", registry).await;
        let broker = RedisBroker::new(TEST_BROKER_URL, &queue).expect("broker");
        let payload = serde_json::to_vec(&serde_json::json!({})).expect("payload");

        // A pause addressed at the wrong queue must be a no-op: the real
        // queue keeps consuming.
        pause_worker(TEST_BROKER_URL, &hostname, "not-this-workers-queue")
            .await
            .expect("pause_worker must succeed at the transport level even on a queue mismatch");
        broker
            .enqueue(celers_core::SerializedTask::new(
                "test.worker-mgmt.pause-resume".to_string(),
                payload.clone(),
            ))
            .await
            .expect("enqueue after a mismatched pause");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if runs.load(std::sync::atomic::Ordering::SeqCst) == 1 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "a pause addressed at the wrong queue must not stop the real queue from consuming"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        // A pause on the *real* queue must actually suspend consumption.
        pause_worker(TEST_BROKER_URL, &hostname, &queue)
            .await
            .expect("pause_worker must reach the live worker");
        broker
            .enqueue(celers_core::SerializedTask::new(
                "test.worker-mgmt.pause-resume".to_string(),
                payload.clone(),
            ))
            .await
            .expect("enqueue while paused");
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert_eq!(
            runs.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "a paused worker must not consume a task enqueued while paused \
             (still just the one run from before the pause)"
        );

        // Resuming must lift the suspension.
        resume_worker(TEST_BROKER_URL, &hostname, &queue)
            .await
            .expect("resume_worker must reach the live worker");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if runs.load(std::sync::atomic::Ordering::SeqCst) == 2 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the second task must run once consumption resumes"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        let _ = handle.shutdown().await;
        cleanup_test_queue(&broker).await;
    }

    /// Regression test for the worker-mgmt/control-client reimplementation:
    /// proves `drain_worker` actually stops a live worker (via `celers
    /// control shutdown` underneath) rather than merely writing a
    /// 24-hour-TTL Redis flag nothing ever read.
    #[tokio::test]
    async fn drain_worker_stops_a_live_worker() {
        let runs = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let registry = TaskRegistry::new();
        registry
            .register(CountingTask {
                runs: Arc::clone(&runs),
                name: "test.worker-mgmt.drain".to_string(),
            })
            .await;

        let (queue, hostname, handle) = start_live_control_worker("drain", registry).await;
        let broker = RedisBroker::new(TEST_BROKER_URL, &queue).expect("broker");

        drain_worker(TEST_BROKER_URL, &hostname, Some(2))
            .await
            .expect("drain_worker must reach the live worker");

        let payload = serde_json::to_vec(&serde_json::json!({})).expect("payload");
        broker
            .enqueue(celers_core::SerializedTask::new(
                "test.worker-mgmt.drain".to_string(),
                payload,
            ))
            .await
            .expect("enqueue after drain");

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        assert_eq!(
            runs.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "a drained (shut down) worker must not consume a task enqueued after drain"
        );

        // Best-effort: the worker has almost certainly already exited on its
        // own by now; this just ensures the run loop task is not left
        // dangling if it somehow has not.
        let _ = handle.shutdown().await;
        cleanup_test_queue(&broker).await;
    }

    /// `pause_worker` must succeed (and still record its advisory Redis
    /// flag) even when no worker answers -- mirroring
    /// `celers_core::control_transport`'s "no replies is a normal outcome,
    /// not an error" contract, and matching how a pause request that never
    /// reaches anyone is still true information about what was asked.
    #[tokio::test]
    async fn pause_worker_records_an_advisory_flag_even_with_no_worker_listening() {
        let worker_id = format!("test-worker-pause-no-listener-{}", uuid::Uuid::new_v4());

        pause_worker(TEST_BROKER_URL, &worker_id, "no-such-queue")
            .await
            .expect("pause_worker must succeed even when no worker answers");

        let client = redis::Client::open(TEST_BROKER_URL).expect("client");
        let mut conn = client
            .get_multiplexed_async_connection_with_config(&crate::pool::async_connection_config())
            .await
            .expect("conn");
        let exists: bool = redis::cmd("EXISTS")
            .arg(pause_flag_key(&worker_id))
            .query_async(&mut conn)
            .await
            .expect("exists check");
        assert!(
            exists,
            "the advisory pause flag must be recorded even with nobody listening"
        );

        let _: i64 = redis::cmd("DEL")
            .arg(pause_flag_key(&worker_id))
            .query_async(&mut conn)
            .await
            .unwrap_or(0);
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
