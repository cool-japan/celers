//! Reusable client-connection pooling for the `CeleRS` CLI.
//!
//! `celers-cli` issues many short-lived broker connections over the lifetime
//! of a single process: every `queue`/`worker`/`task` subcommand (and, once
//! read paths batch independent lookups concurrently, each branch of that
//! batch) would otherwise open its own `redis::Client` and negotiate a fresh
//! connection. [`ClientPool`] keeps a small, size-bounded set of already
//! connected handles keyed by connection identity (typically a broker URL) so
//! repeat lookups within the same process reuse an existing connection
//! instead of reconnecting.
//!
//! The pool is generic over the pooled handle type so its bookkeeping logic
//! (hit/miss tracking, capacity eviction, [`PoolStats`] reporting) can be unit
//! tested with cheap stand-in values and no live broker. Production call
//! sites in [`crate::commands`]' `queue`/`worker`/`task` read paths use
//! [`pooled_redis_connection`], which instantiates the shared
//! [`redis_connection_pool`] with `redis::aio::MultiplexedConnection` — a
//! handle that is cheap to `Clone` and explicitly designed for concurrent use
//! (it multiplexes many in-flight requests over one underlying socket).
//!
//! # Examples
//!
//! ```
//! use celers_cli::pool::ClientPool;
//!
//! # #[tokio::main]
//! # async fn main() -> anyhow::Result<()> {
//! let pool: ClientPool<u32> = ClientPool::new(4);
//!
//! // First call for a key is a miss: `connect` runs and the value is cached.
//! let a = pool
//!     .get_or_connect("primary", || async { Ok::<_, anyhow::Error>(1_u32) })
//!     .await?;
//! // A second call for the same key is a hit: `connect` is not invoked again.
//! let b = pool
//!     .get_or_connect("primary", || async { Ok::<_, anyhow::Error>(2_u32) })
//!     .await?;
//!
//! assert_eq!((a, b), (1, 1));
//! assert_eq!(pool.stats().reuse_count, 1);
//! # Ok(())
//! # }
//! ```

use colored::Colorize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock, PoisonError};

/// Minimum usable pool size. A pool of `0` could never cache anything, which
/// would silently defeat reuse tracking, so construction floors it to `1`.
const MIN_POOL_SIZE: usize = 1;

/// A small process-local pool of reusable client handles, keyed by an
/// arbitrary identity string (e.g. a broker URL).
///
/// Handles are cloned out to callers rather than borrowed, so `T` must be
/// cheap to clone. Connection types such as
/// `redis::aio::MultiplexedConnection` are designed for exactly this: cloning
/// shares the same underlying socket rather than opening a new one.
pub struct ClientPool<T: Clone> {
    max_size: usize,
    reuse_count: AtomicU64,
    created_count: AtomicU64,
    entries: Mutex<HashMap<String, T>>,
}

/// A point-in-time snapshot of [`ClientPool`] utilization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolStats {
    /// Number of distinct keys currently pooled.
    pub size: usize,
    /// Configured maximum number of distinct keys the pool will retain.
    pub max_size: usize,
    /// Cumulative number of [`ClientPool::get_or_connect`] calls that were
    /// satisfied from the pool instead of invoking the connect closure.
    pub reuse_count: u64,
    /// Cumulative number of handles created (successful connect-closure
    /// invocations).
    pub created_count: u64,
}

impl PoolStats {
    /// Pool utilization as a percentage of `max_size` (`0.0` when `max_size`
    /// is `0`).
    #[must_use]
    pub fn utilization_pct(&self) -> f64 {
        if self.max_size == 0 {
            0.0
        } else {
            (self.size as f64 / self.max_size as f64) * 100.0
        }
    }

    /// Fraction of all [`ClientPool::get_or_connect`] calls that were served
    /// from the pool rather than reconnecting (`0.0` when there have been no
    /// calls yet).
    #[must_use]
    pub fn reuse_ratio(&self) -> f64 {
        let total = self.reuse_count + self.created_count;
        if total == 0 {
            0.0
        } else {
            self.reuse_count as f64 / total as f64
        }
    }
}

impl<T: Clone> ClientPool<T> {
    /// Create a pool that retains at most `max_size` distinct entries
    /// (floored to `MIN_POOL_SIZE`).
    #[must_use]
    pub fn new(max_size: usize) -> Self {
        Self {
            max_size: max_size.max(MIN_POOL_SIZE),
            reuse_count: AtomicU64::new(0),
            created_count: AtomicU64::new(0),
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Return the pooled handle for `key`, creating one via `connect` on a
    /// miss.
    ///
    /// `connect` is invoked only when no live entry exists for `key`; a hit
    /// increments [`PoolStats::reuse_count`] and returns a clone of the
    /// pooled handle without calling `connect` at all. When the pool is at
    /// capacity and `key` is not already present, an arbitrary existing entry
    /// is evicted to make room — this pool is sized for the small number of
    /// distinct broker URLs a single CLI invocation touches, not intended as
    /// a general-purpose LRU cache.
    ///
    /// # Errors
    ///
    /// Returns whatever error `connect` returns; the pool is left unchanged
    /// on failure.
    pub async fn get_or_connect<F, Fut, E>(&self, key: &str, connect: F) -> Result<T, E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        if let Some(hit) = self.get(key) {
            return Ok(hit);
        }

        let value = connect().await?;
        self.insert(key, value.clone());
        Ok(value)
    }

    /// Look up `key` without creating a new entry on a miss.
    fn get(&self, key: &str) -> Option<T> {
        let guard = lock(&self.entries);
        let hit = guard.get(key).cloned();
        drop(guard);
        if hit.is_some() {
            self.reuse_count.fetch_add(1, Ordering::Relaxed);
        }
        hit
    }

    /// Insert `value` for `key`, evicting an arbitrary entry first if the
    /// pool is full and `key` is not already present.
    fn insert(&self, key: &str, value: T) {
        let mut guard = lock(&self.entries);
        if !guard.contains_key(key) && guard.len() >= self.max_size {
            if let Some(evict_key) = guard.keys().next().cloned() {
                guard.remove(&evict_key);
            }
        }
        guard.insert(key.to_string(), value);
        drop(guard);
        self.created_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Remove `key` from the pool unconditionally, forcing the next
    /// [`ClientPool::get_or_connect`] call for it to reconnect. Used when a
    /// caller knows a pooled handle is no longer valid.
    pub fn invalidate(&self, key: &str) {
        let mut guard = lock(&self.entries);
        guard.remove(key);
    }

    /// Cheaply check whether `key` currently has a pooled entry, without
    /// creating one on a miss or affecting hit/miss counters.
    ///
    /// Lets a caller distinguish "the handle [`ClientPool::get_or_connect`]
    /// is about to hand back is freshly connected" (known-good by
    /// construction) from "it is a reused, potentially stale entry" (may
    /// have gone bad since it was cached) without duplicating this pool's
    /// locking -- see [`pooled_redis_connection`]'s liveness probe.
    #[must_use]
    pub fn contains(&self, key: &str) -> bool {
        lock(&self.entries).contains_key(key)
    }

    /// Current utilization/reuse snapshot.
    #[must_use]
    pub fn stats(&self) -> PoolStats {
        let guard = lock(&self.entries);
        PoolStats {
            size: guard.len(),
            max_size: self.max_size,
            reuse_count: self.reuse_count.load(Ordering::Relaxed),
            created_count: self.created_count.load(Ordering::Relaxed),
        }
    }
}

/// Lock `mutex`, recovering the guard from a poisoned lock instead of
/// panicking. A panic in one pooled-connection caller should not permanently
/// wedge every subsequent command in the same process.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Effective maximum pool size for the shared [`redis_connection_pool`]:
/// `CELERS_POOL_MAX_SIZE` if set and valid, otherwise
/// [`crate::config::PoolConfig`]'s default.
fn configured_max_size() -> usize {
    crate::config::PoolConfig::from_env_or_default().max_size
}

/// The process-wide pool of reusable Redis multiplexed connections shared by
/// the `queue`/`worker`/`task` read paths.
///
/// A single shared pool (rather than one per command module) means a
/// connection opened while listing queues is reused moments later, in the
/// same process, by worker or task commands talking to the same broker URL.
/// This matters most for programmatic/library use (see the crate's
/// `examples/`), since a single CLI invocation normally only runs one
/// command.
pub fn redis_connection_pool() -> &'static ClientPool<redis::aio::MultiplexedConnection> {
    static POOL: OnceLock<ClientPool<redis::aio::MultiplexedConnection>> = OnceLock::new();
    POOL.get_or_init(|| ClientPool::new(configured_max_size()))
}

/// Whether a `redis::RedisError` observed on a pooled connection means that
/// connection is no longer usable and should be replaced, as opposed to an
/// application-level error (a bad command, `WRONGTYPE`, ...) that says
/// nothing about the connection's own health.
fn is_dead_pooled_connection(e: &redis::RedisError) -> bool {
    e.is_connection_dropped() || e.is_io_error() || e.is_timeout() || e.is_connection_refusal()
}

/// Default response timeout applied to every async Redis connection this CLI
/// opens, overridable via `CELERS_REDIS_RESPONSE_TIMEOUT_SECS`.
const DEFAULT_REDIS_RESPONSE_TIMEOUT_SECS: u64 = 30;

/// Default connection (dial) timeout applied to every async Redis connection
/// this CLI opens, overridable via `CELERS_REDIS_CONNECTION_TIMEOUT_SECS`.
const DEFAULT_REDIS_CONNECTION_TIMEOUT_SECS: u64 = 10;

/// The [`redis::AsyncConnectionConfig`] every async Redis connection this CLI
/// opens (pooled, via [`connect_and_cache`]/[`pooled_redis_connection`], or
/// direct, via `redis::Client::get_multiplexed_async_connection_with_config`
/// at the many one-shot call sites throughout `commands::*`) must be built
/// with.
///
/// The `redis` crate's own default (`AsyncConnectionConfig::default()`, used
/// by the bare, config-less `get_multiplexed_async_connection()`) is a
/// **500ms response timeout and a 1s connection timeout** -- tuned for a
/// healthy, idle server, not for an operator CLI that may run alongside
/// other CPU/IO-heavy work on the same host. Under load elsewhere (e.g. a
/// concurrent full-workspace test run hammering the same broker), even a
/// trivial command like `LPUSH` can legitimately take longer than 500ms to
/// round-trip; the client reports that as a hard connection failure ("timed
/// out") rather than a slow-but-alive server. This crate issues only plain
/// request/response commands -- no `BRPOP`/`BLPOP`/`BRPOPLPUSH`/blocking
/// `XREAD` anywhere in `commands::*` -- so there is no blocking-command
/// lower bound to respect here; a generous, uniform timeout is strictly an
/// improvement over the upstream default.
///
/// `response_timeout` bounds a single command's round trip, not a
/// connection's total lifetime, so this same generous value is safe for the
/// long-lived polling loops in `commands::monitoring::autoscale_alert` too.
#[must_use]
pub(crate) fn async_connection_config() -> redis::AsyncConnectionConfig {
    let response_timeout = first_env_secs(
        "CELERS_REDIS_RESPONSE_TIMEOUT_SECS",
        DEFAULT_REDIS_RESPONSE_TIMEOUT_SECS,
    );
    let connection_timeout = first_env_secs(
        "CELERS_REDIS_CONNECTION_TIMEOUT_SECS",
        DEFAULT_REDIS_CONNECTION_TIMEOUT_SECS,
    );
    redis::AsyncConnectionConfig::new()
        .set_response_timeout(Some(std::time::Duration::from_secs(response_timeout)))
        .set_connection_timeout(Some(std::time::Duration::from_secs(connection_timeout)))
}

/// Read `key` from the environment as a `u64` second count, falling back to
/// `default` when unset, empty, or unparsable. Mirrors the override pattern
/// `PoolConfig`/`CacheConfig::from_env_or_default` already use for their own
/// environment overrides, kept local to this module rather than reusing
/// `config`'s private `first_env_parsed` (no need to widen that helper's
/// visibility for a single extra caller).
///
/// Floored to `1`: a `0`-second timeout is not "no timeout" (that is
/// `set_response_timeout(None)`/`set_connection_timeout(None)`, which this
/// module never uses) but a request that fails *every* command instantly --
/// the opposite of what someone setting `CELERS_REDIS_RESPONSE_TIMEOUT_SECS=0`
/// almost certainly intends, and exactly the footgun this whole helper
/// exists to move this crate away from. Mirrors the same floor
/// `TtlCache::with_capacity_and_clock`'s `max_entries.max(1)` and
/// `ClientPool::new`'s `MIN_POOL_SIZE` apply to their own degenerate-zero
/// case.
fn first_env_secs(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
        .max(1)
}

/// Connect (or reconnect) `broker_url` and cache the result in `pool`.
async fn connect_and_cache(
    pool: &ClientPool<redis::aio::MultiplexedConnection>,
    broker_url: &str,
) -> anyhow::Result<redis::aio::MultiplexedConnection> {
    pool.get_or_connect(broker_url, || async {
        let client = redis::Client::open(broker_url)?;
        client
            .get_multiplexed_async_connection_with_config(&async_connection_config())
            .await
    })
    .await
    .map_err(anyhow::Error::from)
}

/// Obtain a pooled multiplexed connection for `broker_url`, connecting (and
/// caching the result) on a miss.
///
/// This is the pooled replacement for the direct-connect pattern (open a
/// `redis::Client`, then `get_multiplexed_async_connection_with_config(&
/// async_connection_config())`) most one-shot read paths throughout
/// `commands::*` use instead -- callers that issue many independent lookups
/// against the same broker within one process (`commands::queue`/
/// `commands::worker`'s read paths) use this pooled path so they reuse one
/// connection rather than negotiating a fresh one per lookup. When
/// [`crate::config::PoolConfig::reuse_enabled`] is `false`, the shared
/// pool's cached entry for `broker_url` (if any) is dropped first so every
/// call reconnects, matching the "pooling disabled" contract.
///
/// A *reused* (pool-hit) handle is additionally validated with a cheap
/// `PING` before being handed back: in a long-lived process (`celers
/// interactive`, or library use of this crate), nothing previously
/// invalidated a pooled handle on failure, so a Redis restart or a dropped
/// TCP connection left the dead handle cached and every subsequent command
/// in that process failed until the process itself restarted (idx 339). A
/// freshly-connected handle (a pool miss) skips this probe -- it is
/// known-good by construction, so probing it would only add a redundant
/// round trip.
///
/// # Errors
///
/// Returns an error if `broker_url` cannot be parsed or the connection
/// attempt fails.
pub async fn pooled_redis_connection(
    broker_url: &str,
) -> anyhow::Result<redis::aio::MultiplexedConnection> {
    let pool = redis_connection_pool();
    if !crate::config::PoolConfig::from_env_or_default().reuse_enabled {
        pool.invalidate(broker_url);
    }

    let reused = pool.contains(broker_url);
    let mut conn = connect_and_cache(pool, broker_url).await?;

    if reused {
        if let Err(e) = redis::cmd("PING").query_async::<String>(&mut conn).await {
            if is_dead_pooled_connection(&e) {
                tracing::warn!(
                    "Pooled Redis connection for {} appears dead ({e}); reconnecting",
                    crate::commands::utils::mask_password(broker_url)
                );
                pool.invalidate(broker_url);
                conn = connect_and_cache(pool, broker_url).await?;
            }
        }
    }

    Ok(conn)
}

/// Print the shared Redis connection pool's configured capacity and the
/// `queue`/`worker` read-path caches' configured TTL and current
/// in-process entry counts -- the `celers cache-stats` top-level command.
///
/// This deliberately excludes live hit/reuse ratios (see [`PoolStats`],
/// [`crate::cache::CacheStats`]): a one-shot `celers <command>` process
/// runs a single command and exits before those counters could accumulate
/// anything meaningful, so printing them here would misrepresent near-empty
/// startup noise as a steady-state figure. Entry counts are shown anyway
/// despite normally reading 0/near-zero on a fresh process, since "empty"
/// is itself an accurate, non-misleading answer. For live ratios
/// accumulated across many commands sharing one process, run `celers
/// interactive` and use its `stats` command instead.
///
/// `pub` (not `pub(crate)`), matching [`crate::errors::print_error_code_reference`]
/// (the `celers error-codes` command's equivalent handler): `celers-cli`
/// builds as both a library target (`lib.rs`, which does not declare a
/// `cli` module at all) and a binary target (`main.rs`, which redeclares
/// the same source files as an independent crate). This function's only
/// call site (`cli::dispatch`'s `Commands::CacheStats` arm) exists solely
/// in the binary's module tree, so from the library compilation's
/// perspective it has no in-crate caller at all; a bare `pub(crate)` would
/// make it (correctly) dead code there. Full `pub` visibility marks it as
/// public API instead, exempting it from that lint exactly as
/// `print_error_code_reference` already is.
pub fn print_cache_pool_snapshot() {
    println!(
        "{}",
        "=== Cache & Connection Pool Configuration ==="
            .bold()
            .cyan()
    );
    println!();
    println!(
        "{}",
        "(configured capacity/TTL only -- run `celers interactive` and use \
         its `stats` command for live hit/reuse ratios)"
            .dimmed()
    );
    println!();

    let pool_cfg = crate::config::PoolConfig::from_env_or_default();
    println!("{}", "Redis connection pool".cyan().bold());
    println!("  Configured max size: {}", configured_max_size());
    println!("  Reuse enabled:       {}", pool_cfg.reuse_enabled);
    println!();

    let cache_cfg = crate::config::CacheConfig::from_env_or_default();
    let (queue_list, queue_stats) = crate::commands::queue::queue_cache_stats();
    let (worker_list, worker_stats) = crate::commands::queue::worker_cache_stats();

    println!("{}", "Read-path caches".cyan().bold());
    println!("  Caching enabled:     {}", cache_cfg.enabled);
    println!("  Configured TTL:      {}s", cache_cfg.ttl_secs);
    println!(
        "{}",
        format_cache_entry_line("queue list cache:", queue_list.len)
    );
    println!(
        "{}",
        format_cache_entry_line("queue stats cache:", queue_stats.len)
    );
    println!(
        "{}",
        format_cache_entry_line("worker list cache:", worker_list.len)
    );
    println!(
        "{}",
        format_cache_entry_line("worker stats cache:", worker_stats.len)
    );
}

/// Format one cache's current entry count as a `  <label> N entries` line,
/// used by [`print_cache_pool_snapshot`] for each of the four
/// `queue`/`worker` read-path caches.
///
/// Pulled out as a pure, unit-testable formatter rather than a `println!`
/// baked directly into [`print_cache_pool_snapshot`].
fn format_cache_entry_line(label: &str, entries: usize) -> String {
    format!("  {label:<20} {entries} entries")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression test for idx 339: `is_dead_pooled_connection` must
    /// classify a transport-level failure (here: an `ErrorKind::Io` error,
    /// exactly the class a dropped socket or a Redis restart produces) as
    /// dead, so `pooled_redis_connection` invalidates and reconnects
    /// instead of continuing to hand out a socket that will fail on every
    /// use until the whole process restarts.
    #[test]
    fn is_dead_pooled_connection_classifies_transport_errors_as_dead() {
        let io_err: redis::RedisError = (redis::ErrorKind::Io, "simulated io failure").into();
        assert!(is_dead_pooled_connection(&io_err));
    }

    /// An application-level error (a bad command, a type mismatch, ...)
    /// says nothing about the connection's own health and must NOT trigger
    /// an invalidate+reconnect -- doing so on every ordinary command error
    /// would defeat pooling for no reason.
    #[test]
    fn is_dead_pooled_connection_does_not_treat_application_errors_as_dead() {
        let app_err: redis::RedisError =
            (redis::ErrorKind::UnexpectedReturnType, "wrong type").into();
        assert!(!is_dead_pooled_connection(&app_err));
    }

    /// Regression test for idx 339's happy path: a *reused* (pool-hit)
    /// connection that is, in fact, perfectly healthy must still come back
    /// from `pooled_redis_connection` as a working connection -- the new
    /// PING liveness probe must not itself break ordinary reuse.
    #[tokio::test]
    async fn pooled_redis_connection_reuses_and_validates_a_live_cached_connection() {
        let broker_url = "redis://127.0.0.1:6379";
        let pool = redis_connection_pool();
        pool.invalidate(broker_url);
        assert!(!pool.contains(broker_url));

        let _first = pooled_redis_connection(broker_url)
            .await
            .expect("first (miss) connect");
        assert!(
            pool.contains(broker_url),
            "a successful connect must be cached"
        );

        let mut second = pooled_redis_connection(broker_url)
            .await
            .expect("second (hit) connect must still succeed");
        let pong: String = redis::cmd("PING")
            .query_async(&mut second)
            .await
            .expect("the reused, PING-validated connection must still work");
        assert_eq!(pong, "PONG");
    }

    #[tokio::test]
    async fn get_or_connect_reuses_existing_entry() {
        let pool: ClientPool<u32> = ClientPool::new(4);

        let a = pool
            .get_or_connect("k", || async { Ok::<_, anyhow::Error>(1_u32) })
            .await
            .expect("first connect succeeds");
        let b = pool
            .get_or_connect("k", || async { Ok::<_, anyhow::Error>(999_u32) })
            .await
            .expect("second call hits the pool instead of reconnecting");

        assert_eq!(a, 1);
        assert_eq!(
            b, 1,
            "pooled value must win over the second connect closure"
        );

        let stats = pool.stats();
        assert_eq!(stats.size, 1);
        assert_eq!(stats.reuse_count, 1);
        assert_eq!(stats.created_count, 1);
    }

    #[tokio::test]
    async fn get_or_connect_distinct_keys_do_not_reuse() {
        let pool: ClientPool<u32> = ClientPool::new(4);

        pool.get_or_connect("a", || async { Ok::<_, anyhow::Error>(1_u32) })
            .await
            .expect("connect a");
        pool.get_or_connect("b", || async { Ok::<_, anyhow::Error>(2_u32) })
            .await
            .expect("connect b");

        let stats = pool.stats();
        assert_eq!(stats.size, 2);
        assert_eq!(stats.reuse_count, 0, "distinct keys are never reused");
        assert_eq!(stats.created_count, 2);
    }

    #[tokio::test]
    async fn eviction_makes_room_when_pool_is_full() {
        let pool: ClientPool<u32> = ClientPool::new(1);

        pool.get_or_connect("a", || async { Ok::<_, anyhow::Error>(1_u32) })
            .await
            .expect("connect a");
        pool.get_or_connect("b", || async { Ok::<_, anyhow::Error>(2_u32) })
            .await
            .expect("connect b evicts a");

        let stats = pool.stats();
        assert_eq!(stats.size, 1, "pool never exceeds max_size");
        assert_eq!(stats.max_size, 1);
        assert_eq!(stats.created_count, 2);
    }

    #[tokio::test]
    async fn new_floors_zero_to_min_pool_size() {
        let pool: ClientPool<u32> = ClientPool::new(0);
        assert_eq!(pool.stats().max_size, MIN_POOL_SIZE);
    }

    #[tokio::test]
    async fn invalidate_forces_reconnect() {
        let pool: ClientPool<u32> = ClientPool::new(4);

        pool.get_or_connect("k", || async { Ok::<_, anyhow::Error>(1_u32) })
            .await
            .expect("connect");
        pool.invalidate("k");

        let after = pool
            .get_or_connect("k", || async { Ok::<_, anyhow::Error>(2_u32) })
            .await
            .expect("reconnect after invalidate");

        assert_eq!(after, 2, "invalidated entry must be recreated, not reused");
        assert_eq!(pool.stats().created_count, 2);
        assert_eq!(pool.stats().reuse_count, 0);
    }

    #[tokio::test]
    async fn get_or_connect_propagates_connect_errors_without_caching() {
        let pool: ClientPool<u32> = ClientPool::new(4);

        let err = pool
            .get_or_connect("k", || async {
                Err::<u32, anyhow::Error>(anyhow::anyhow!("boom"))
            })
            .await;
        assert!(err.is_err());
        assert_eq!(pool.stats().size, 0, "a failed connect must not be cached");
        assert_eq!(pool.stats().created_count, 0);
    }

    #[test]
    fn pool_stats_utilization_and_reuse_ratio() {
        let full = PoolStats {
            size: 8,
            max_size: 16,
            reuse_count: 3,
            created_count: 1,
        };
        assert!((full.utilization_pct() - 50.0).abs() < f64::EPSILON);
        assert!((full.reuse_ratio() - 0.75).abs() < f64::EPSILON);

        let empty = PoolStats {
            size: 0,
            max_size: 0,
            reuse_count: 0,
            created_count: 0,
        };
        assert_eq!(empty.utilization_pct(), 0.0);
        assert_eq!(empty.reuse_ratio(), 0.0);
    }

    #[test]
    fn format_cache_entry_line_formats_label_and_count() {
        let line = format_cache_entry_line("queue list cache:", 5);
        assert!(line.contains("queue list cache:"));
        assert!(line.contains("5 entries"));
    }

    /// Serializes tests in this module that mutate the process-wide
    /// `CELERS_REDIS_*_TIMEOUT_SECS` environment variables, mirroring the
    /// pattern used by `config_layer::tests::env_guard` (see
    /// `smart_defaults::tests::env_guard`'s docs for why each module keeps
    /// its own lock rather than sharing one).
    fn timeout_env_guard() -> MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn clear_timeout_env() {
        std::env::remove_var("CELERS_REDIS_RESPONSE_TIMEOUT_SECS");
        std::env::remove_var("CELERS_REDIS_CONNECTION_TIMEOUT_SECS");
    }

    /// `redis::AsyncConnectionConfig`'s `response_timeout`/`connection_timeout`
    /// fields are `pub(crate)` to the `redis` crate (builder-only from the
    /// outside, see `AsyncConnectionConfig::set_response_timeout`/
    /// `set_connection_timeout` -- no getters), so this module's own default
    /// constants are asserted directly against the documented `redis`-crate
    /// default (500ms/1s) instead of introspecting the built config; the
    /// live-connection test below then proves `async_connection_config`'s
    /// output is actually accepted and used by a real connection.
    #[test]
    fn timeout_defaults_are_more_generous_than_the_redis_crate_default() {
        // The `redis` crate's own undocumented default
        // (`AsyncConnectionConfig::default()`, used by the bare, config-less
        // `get_multiplexed_async_connection()`) is 500ms response / 1s
        // connection timeout -- both of this crate's defaults must be
        // strictly more generous.
        assert!(
            std::time::Duration::from_secs(DEFAULT_REDIS_RESPONSE_TIMEOUT_SECS)
                > std::time::Duration::from_millis(500)
        );
        assert!(
            std::time::Duration::from_secs(DEFAULT_REDIS_CONNECTION_TIMEOUT_SECS)
                > std::time::Duration::from_secs(1)
        );
    }

    /// Regression test for the redis-timeout root cause: a connection opened
    /// via [`async_connection_config`] must actually work end-to-end against
    /// a live broker (not just construct without panicking), proving the
    /// config `async_connection_config` builds -- whether from defaults or
    /// from an environment override -- is one `redis` itself accepts and
    /// honors, not just a value this module happens to compute.
    #[tokio::test]
    async fn async_connection_config_produces_a_working_connection() {
        // The env-var mutation + `async_connection_config()` read is the
        // only part that needs serializing against other tests in this
        // module; `config` is a concrete, already-resolved value by the
        // time the guard scope ends, so the connect/PING below (the actual
        // `await` points) run with no lock held -- avoids
        // `clippy::await_holding_lock` and, more importantly, avoids
        // blocking every other test in this file for the duration of a
        // live network round trip.
        let config = {
            let _guard = timeout_env_guard();
            clear_timeout_env();
            std::env::set_var("CELERS_REDIS_RESPONSE_TIMEOUT_SECS", "7");
            std::env::set_var("CELERS_REDIS_CONNECTION_TIMEOUT_SECS", "3");
            let config = async_connection_config();
            clear_timeout_env();
            config
        };

        let client =
            redis::Client::open("redis://127.0.0.1:6379").expect("valid broker url literal");
        let mut conn = client
            .get_multiplexed_async_connection_with_config(&config)
            .await
            .expect("connect with the overridden timeout config");
        let pong: String = redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .expect("PING over a connection built from async_connection_config");
        assert_eq!(pong, "PONG");
    }

    #[test]
    fn first_env_secs_falls_back_on_unset_or_unparsable() {
        let _guard = timeout_env_guard();
        std::env::remove_var("CELERS_TEST_TIMEOUT_PROBE");
        assert_eq!(first_env_secs("CELERS_TEST_TIMEOUT_PROBE", 42), 42);

        std::env::set_var("CELERS_TEST_TIMEOUT_PROBE", "not-a-number");
        assert_eq!(first_env_secs("CELERS_TEST_TIMEOUT_PROBE", 42), 42);

        std::env::set_var("CELERS_TEST_TIMEOUT_PROBE", "99");
        assert_eq!(first_env_secs("CELERS_TEST_TIMEOUT_PROBE", 42), 99);

        std::env::remove_var("CELERS_TEST_TIMEOUT_PROBE");
    }

    /// A `0`-second timeout is not "no timeout" (that is `None`, which this
    /// module never passes) -- it fails every command instantly. Both an
    /// explicit `0` override and a `0` `default` argument must be floored to
    /// `1`, mirroring the same `.max(1)` floor `TtlCache`/`ClientPool` apply
    /// to their own degenerate-zero-capacity case.
    #[test]
    fn first_env_secs_floors_a_zero_override_or_default_to_one() {
        let _guard = timeout_env_guard();
        std::env::set_var("CELERS_TEST_TIMEOUT_PROBE", "0");
        assert_eq!(first_env_secs("CELERS_TEST_TIMEOUT_PROBE", 42), 1);

        std::env::remove_var("CELERS_TEST_TIMEOUT_PROBE");
        assert_eq!(first_env_secs("CELERS_TEST_TIMEOUT_PROBE", 0), 1);

        std::env::remove_var("CELERS_TEST_TIMEOUT_PROBE");
    }
}
