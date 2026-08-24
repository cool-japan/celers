//! Distributed rate limiting across workers.
//!
//! This module provides a *pluggable backend* abstraction for enforcing rate
//! limits cluster-wide rather than per-worker. The key idea is a shared
//! token / counter store, keyed by limiter name, that performs the
//! token-bucket and sliding-window arithmetic **atomically** so that
//! concurrent workers cannot collectively exceed the configured rate.
//!
//! It is designed to complement (not replace) the in-process limiters in
//! [`crate::rate_limit`]:
//!
//! - [`DistributedRateLimitBackend`] is the storage trait. The in-memory
//!   implementation here uses a [`tokio::sync::Mutex`]-guarded map so that the
//!   whole acquire/refill cycle for a key is a single critical section, which
//!   makes it fully testable in-process. A Redis backend can implement the
//!   same trait by translating each method into an atomic Lua script / `MULTI`
//!   transaction (the scripts already shipped in [`crate::rate_limit`] map
//!   directly onto these operations).
//! - [`DistributedRateLimiter`] is a thin, ergonomic handle that pairs a
//!   backend with a limiter key and a [`RateLimitConfig`]. It reuses the
//!   existing [`RateLimitConfig`] type and the same token-bucket /
//!   sliding-window *algorithms* defined in [`crate::rate_limit`].
//!
//! # Example
//!
//! ```rust
//! use celers_core::rate_limit::RateLimitConfig;
//! use celers_core::rate_limit_distributed::{
//!     DistributedRateLimiter, InMemoryDistributedBackend,
//! };
//! use std::sync::Arc;
//!
//! # async fn example() -> celers_core::Result<()> {
//! // A token bucket allowing a burst of 5, refilling at 10/sec, shared cluster-wide.
//! let backend = Arc::new(InMemoryDistributedBackend::new());
//! let config = RateLimitConfig::new(10.0).with_burst(5);
//! let limiter = DistributedRateLimiter::new(backend, "send_email", config);
//!
//! // First 5 acquisitions succeed (burst), the 6th is denied.
//! for _ in 0..5 {
//!     assert!(limiter.try_acquire().await?);
//! }
//! assert!(!limiter.try_acquire().await?);
//! # Ok(())
//! # }
//! ```

use crate::rate_limit::RateLimitConfig;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;

/// The rate-limiting algorithm a backend must apply for a key.
///
/// This mirrors the algorithm selection in [`RateLimitConfig`] but is expressed
/// as an explicit, `Copy` value so backends do not need to depend on the full
/// configuration struct when performing atomic arithmetic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistributedAlgorithm {
    /// Token bucket: tokens refill continuously at `rate` up to `burst`.
    TokenBucket,
    /// Sliding window: at most `rate * window` events within `window` seconds.
    SlidingWindow,
}

/// Immutable parameters describing how a key should be rate limited.
///
/// Derived from a [`RateLimitConfig`] via [`RateLimitParams::from_config`].
/// Backends use these to perform the refill / windowing math atomically.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RateLimitParams {
    /// Algorithm to apply.
    pub algorithm: DistributedAlgorithm,
    /// Refill rate in tokens (or events) per second.
    pub rate: f64,
    /// Maximum burst capacity (token bucket) — tokens never exceed this.
    pub burst: f64,
    /// Window size in seconds (sliding window).
    pub window_secs: u64,
}

impl RateLimitParams {
    /// Build parameters from a [`RateLimitConfig`], reusing its algorithm choice
    /// and effective burst computation so behaviour matches the in-process
    /// limiters exactly.
    #[must_use]
    pub fn from_config(config: &RateLimitConfig) -> Self {
        if config.sliding_window {
            Self {
                algorithm: DistributedAlgorithm::SlidingWindow,
                rate: config.rate,
                burst: f64::from(config.effective_burst()),
                window_secs: config.window_size.max(1),
            }
        } else {
            Self {
                algorithm: DistributedAlgorithm::TokenBucket,
                rate: config.rate,
                burst: f64::from(config.effective_burst()),
                window_secs: config.window_size.max(1),
            }
        }
    }

    /// Maximum number of events permitted within the sliding window.
    #[inline]
    #[must_use]
    pub fn max_window_events(&self) -> u64 {
        if !self.rate.is_finite() || self.rate <= 0.0 {
            return 0;
        }
        // Window sizes beyond `u32::MAX` seconds (~136 years) are clamped: the
        // product would be meaningless anyway and the clamp keeps the widening
        // conversion exact.
        let window = u32::try_from(self.window_secs).map_or(f64::from(u32::MAX), f64::from);
        cost_to_permits(self.rate * window)
    }
}

/// `2^64` as an `f64`, used as the saturation boundary for `f64 -> u64`.
const U64_MAX_AS_F64: f64 = 18_446_744_073_709_551_616.0;

/// Round a permit cost up to a whole number of permits, saturating at `u64::MAX`.
///
/// Non-finite and non-positive costs yield `0`.
// Justification for the lossy cast: the value is checked for finiteness, clamped
// at zero and compared against `2^64` immediately before the conversion, so
// neither truncation nor sign loss can occur.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
#[inline]
fn cost_to_permits(cost: f64) -> u64 {
    let ceiled = cost.ceil();
    if !ceiled.is_finite() || ceiled <= 0.0 {
        0
    } else if ceiled >= U64_MAX_AS_F64 {
        u64::MAX
    } else {
        ceiled as u64
    }
}

/// Widen a permit count for reporting in [`AcquireOutcome::remaining`].
// Justification: permit counts are far below `2^53` in any realistic
// configuration, and `remaining` is a diagnostic estimate, not an exact ledger.
#[allow(clippy::cast_precision_loss)]
#[inline]
fn permits_as_f64(count: u64) -> f64 {
    count as f64
}

/// Convert a `Duration` from seconds without ever panicking.
///
/// `Duration::from_secs_f64` panics on negative, NaN, or overflowing inputs;
/// those all mean "effectively never" here, so they saturate to [`Duration::MAX`].
#[inline]
fn duration_from_secs_saturating(secs: f64) -> Duration {
    Duration::try_from_secs_f64(secs).unwrap_or(Duration::MAX)
}

/// Outcome of an atomic acquire against a distributed backend.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AcquireOutcome {
    /// Whether the requested cost was granted.
    pub allowed: bool,
    /// Approximate permits remaining after this operation.
    pub remaining: f64,
    /// If denied, the estimated wait until the cost could succeed.
    pub retry_after: Duration,
}

impl AcquireOutcome {
    /// Construct an "allowed" outcome.
    #[inline]
    #[must_use]
    pub fn allowed(remaining: f64) -> Self {
        Self {
            allowed: true,
            remaining,
            retry_after: Duration::ZERO,
        }
    }

    /// Construct a "denied" outcome with an estimated retry delay.
    #[inline]
    #[must_use]
    pub fn denied(remaining: f64, retry_after: Duration) -> Self {
        Self {
            allowed: false,
            remaining,
            retry_after,
        }
    }
}

/// Shared token / counter store for distributed rate limiting.
///
/// Implementations must guarantee that [`acquire`](DistributedRateLimitBackend::acquire)
/// is **atomic** per `key`: the read, refill, and conditional decrement must
/// happen without interleaving from other callers acting on the same key.
/// This is what makes cluster-wide enforcement correct under concurrency.
///
/// A Redis-backed implementation can satisfy this by executing the equivalent
/// Lua scripts (see [`crate::rate_limit::DistributedTokenBucketSpec`] and
/// [`crate::rate_limit::DistributedSlidingWindowSpec`]) which Redis runs
/// atomically.
#[async_trait]
pub trait DistributedRateLimitBackend: Send + Sync {
    /// Atomically attempt to acquire `cost` permits for `key`.
    ///
    /// The backend applies refill / windowing using `params` before deciding.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying store is unavailable.
    async fn acquire(
        &self,
        key: &str,
        cost: f64,
        params: RateLimitParams,
    ) -> crate::Result<AcquireOutcome>;

    /// Report the currently available permits for `key` (after refill).
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying store is unavailable.
    async fn available(&self, key: &str, params: RateLimitParams) -> crate::Result<f64>;

    /// Estimate the time until at least `cost` permits are available for `key`.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying store is unavailable.
    async fn time_until_available(
        &self,
        key: &str,
        cost: f64,
        params: RateLimitParams,
    ) -> crate::Result<Duration>;

    /// Clear all stored state for `key`.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying store is unavailable.
    async fn reset(&self, key: &str) -> crate::Result<()>;

    /// Evict every key that has not been touched for at least `idle_for`,
    /// returning the number of keys removed.
    ///
    /// Limiter keys are commonly derived from task names or tenant identifiers,
    /// which are attacker-influenced in multi-tenant deployments, so a store
    /// without expiry grows without bound. Backends with native expiry (Redis
    /// `EX`, for instance) already reclaim keys themselves and can keep the
    /// default no-op.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying store is unavailable.
    async fn sweep_idle(&self, _idle_for: Duration) -> crate::Result<usize> {
        Ok(0)
    }

    /// Backend name, for diagnostics.
    fn backend_name(&self) -> &str;
}

/// Internal per-key state for the in-memory backend.
#[derive(Debug, Clone)]
enum KeyState {
    /// Token bucket state: current tokens and last refill instant.
    Bucket { tokens: f64, last_refill: Instant },
    /// Sliding window state: timestamps of recent acquisitions.
    Window { stamps: Vec<Instant> },
}

/// A tracked key plus the bookkeeping needed to reclaim it.
#[derive(Debug, Clone)]
struct KeyEntry {
    state: KeyState,
    /// Last time this key was read or written, used for idle eviction.
    last_touched: Instant,
    /// Monotonic touch counter, used for capacity eviction.
    ///
    /// Ordering by `Instant` alone is unreliable: two touches can land on the
    /// same clock tick, making the eviction victim arbitrary. A counter gives a
    /// total order regardless of clock resolution.
    touch_seq: u64,
}

/// Default upper bound on the number of distinct limiter keys retained.
///
/// Mirrors the order of magnitude Celery uses for its revoked-task set: large
/// enough that legitimate deployments never notice, small enough that a stream
/// of attacker-chosen keys cannot exhaust memory.
pub const DEFAULT_MAX_TRACKED_KEYS: usize = 50_000;

/// In-process distributed rate-limit backend.
///
/// Uses a single [`tokio::sync::Mutex`] guarding a `HashMap` so that the entire
/// acquire/refill cycle for any key is one atomic critical section. This makes
/// it correct for concurrent in-process workers and a faithful local stand-in
/// for a Redis backend during tests.
///
/// Although it shares one mutex across all keys (simple and contention-safe for
/// tests and single-process deployments), the public contract only promises
/// per-key atomicity, so a future sharded implementation remains compatible.
///
/// The key map is bounded: it holds at most [`DEFAULT_MAX_TRACKED_KEYS`] entries
/// (configurable via [`InMemoryDistributedBackend::with_max_keys`]), evicting the
/// least recently touched key when full, and
/// [`sweep_idle`](DistributedRateLimitBackend::sweep_idle) reclaims idle keys on
/// demand. Evicting a key resets its limiter to a full allowance, which is why
/// the bound is deliberately generous.
#[derive(Debug)]
pub struct InMemoryDistributedBackend {
    state: Mutex<HashMap<String, KeyEntry>>,
    max_keys: usize,
    /// Source of the monotonic touch counter used for capacity eviction.
    touch_counter: AtomicU64,
}

impl Default for InMemoryDistributedBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryDistributedBackend {
    /// Create a new empty in-memory backend with the default key bound.
    #[must_use]
    pub fn new() -> Self {
        Self::with_max_keys(DEFAULT_MAX_TRACKED_KEYS)
    }

    /// Create a backend tracking at most `max_keys` distinct limiter keys.
    ///
    /// A value of `0` is treated as `1`: the map always holds the key currently
    /// being operated on.
    #[must_use]
    pub fn with_max_keys(max_keys: usize) -> Self {
        Self {
            state: Mutex::new(HashMap::new()),
            max_keys: max_keys.max(1),
            touch_counter: AtomicU64::new(0),
        }
    }

    /// Next value of the monotonic touch counter.
    #[inline]
    fn next_touch_seq(&self) -> u64 {
        self.touch_counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Number of keys currently tracked (primarily for tests/diagnostics).
    pub async fn key_count(&self) -> usize {
        self.state.lock().await.len()
    }

    /// Maximum number of keys this backend retains.
    #[inline]
    #[must_use]
    pub fn max_keys(&self) -> usize {
        self.max_keys
    }

    /// Evict the least recently touched entries until `map` has room for one more
    /// key beyond those already present.
    fn enforce_capacity(map: &mut HashMap<String, KeyEntry>, max_keys: usize, incoming: &str) {
        if map.contains_key(incoming) {
            return;
        }
        while map.len() >= max_keys {
            let Some(victim) = map
                .iter()
                .min_by_key(|(_, entry)| entry.touch_seq)
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            map.remove(&victim);
        }
    }

    /// Refill a token-bucket value in place, returning the updated tokens.
    #[inline]
    fn refill_bucket(
        tokens: f64,
        last_refill: Instant,
        now: Instant,
        params: RateLimitParams,
    ) -> f64 {
        let elapsed = now.saturating_duration_since(last_refill).as_secs_f64();
        (tokens + elapsed * params.rate).min(params.burst)
    }

    /// Drop window timestamps older than the configured window.
    #[inline]
    fn prune_window(stamps: &mut Vec<Instant>, now: Instant, params: RateLimitParams) {
        let window = Duration::from_secs(params.window_secs);
        if let Some(cutoff) = now.checked_sub(window) {
            stamps.retain(|&t| t > cutoff);
        }
    }
}

#[async_trait]
impl DistributedRateLimitBackend for InMemoryDistributedBackend {
    async fn acquire(
        &self,
        key: &str,
        cost: f64,
        params: RateLimitParams,
    ) -> crate::Result<AcquireOutcome> {
        if cost < 0.0 {
            return Err(crate::CelersError::Configuration(
                "rate limit acquire cost must be non-negative".to_string(),
            ));
        }
        let now = Instant::now();
        let touch_seq = self.next_touch_seq();
        let mut guard = self.state.lock().await;
        Self::enforce_capacity(&mut guard, self.max_keys, key);

        match params.algorithm {
            DistributedAlgorithm::TokenBucket => {
                let entry = guard.entry(key.to_string()).or_insert_with(|| KeyEntry {
                    state: KeyState::Bucket {
                        tokens: params.burst,
                        last_refill: now,
                    },
                    last_touched: now,
                    touch_seq,
                });
                entry.last_touched = now;
                entry.touch_seq = touch_seq;
                // If a key was previously used with a different algorithm, reset it.
                if !matches!(entry.state, KeyState::Bucket { .. }) {
                    entry.state = KeyState::Bucket {
                        tokens: params.burst,
                        last_refill: now,
                    };
                }
                let KeyState::Bucket {
                    tokens,
                    last_refill,
                } = &mut entry.state
                else {
                    unreachable!("entry coerced to Bucket above")
                };
                *tokens = Self::refill_bucket(*tokens, *last_refill, now, params);
                *last_refill = now;
                if *tokens + f64::EPSILON >= cost {
                    *tokens -= cost;
                    Ok(AcquireOutcome::allowed(*tokens))
                } else {
                    let deficit = cost - *tokens;
                    let retry_after = if params.rate > 0.0 {
                        duration_from_secs_saturating(deficit / params.rate)
                    } else {
                        Duration::MAX
                    };
                    Ok(AcquireOutcome::denied(*tokens, retry_after))
                }
            }
            DistributedAlgorithm::SlidingWindow => {
                let entry = guard.entry(key.to_string()).or_insert_with(|| KeyEntry {
                    state: KeyState::Window { stamps: Vec::new() },
                    last_touched: now,
                    touch_seq,
                });
                entry.last_touched = now;
                entry.touch_seq = touch_seq;
                if !matches!(entry.state, KeyState::Window { .. }) {
                    entry.state = KeyState::Window { stamps: Vec::new() };
                }
                let KeyState::Window { stamps } = &mut entry.state else {
                    unreachable!("entry coerced to Window above")
                };
                Self::prune_window(stamps, now, params);
                let max = params.max_window_events();
                let needed = cost_to_permits(cost);
                let used = u64::try_from(stamps.len()).unwrap_or(u64::MAX);
                if used.saturating_add(needed) <= max {
                    for _ in 0..needed {
                        stamps.push(now);
                    }
                    let remaining = max.saturating_sub(used.saturating_add(needed));
                    Ok(AcquireOutcome::allowed(permits_as_f64(remaining)))
                } else {
                    let remaining = max.saturating_sub(used);
                    let retry_after = stamps.first().map_or(Duration::ZERO, |&oldest| {
                        let expires = oldest + Duration::from_secs(params.window_secs);
                        expires.saturating_duration_since(now)
                    });
                    Ok(AcquireOutcome::denied(
                        permits_as_f64(remaining),
                        retry_after,
                    ))
                }
            }
        }
    }

    async fn available(&self, key: &str, params: RateLimitParams) -> crate::Result<f64> {
        let now = Instant::now();
        let mut guard = self.state.lock().await;
        match params.algorithm {
            DistributedAlgorithm::TokenBucket => match guard.get_mut(key) {
                Some(KeyEntry {
                    state:
                        KeyState::Bucket {
                            tokens,
                            last_refill,
                        },
                    last_touched,
                    ..
                }) => {
                    *tokens = Self::refill_bucket(*tokens, *last_refill, now, params);
                    *last_refill = now;
                    *last_touched = now;
                    Ok(*tokens)
                }
                _ => Ok(params.burst),
            },
            DistributedAlgorithm::SlidingWindow => {
                let max = permits_as_f64(params.max_window_events());
                match guard.get_mut(key) {
                    Some(KeyEntry {
                        state: KeyState::Window { stamps },
                        last_touched,
                        ..
                    }) => {
                        Self::prune_window(stamps, now, params);
                        *last_touched = now;
                        Ok(max - permits_as_f64(u64::try_from(stamps.len()).unwrap_or(u64::MAX)))
                    }
                    _ => Ok(max),
                }
            }
        }
    }

    async fn sweep_idle(&self, idle_for: Duration) -> crate::Result<usize> {
        let now = Instant::now();
        let mut guard = self.state.lock().await;
        let before = guard.len();
        guard.retain(|_, entry| now.saturating_duration_since(entry.last_touched) < idle_for);
        Ok(before - guard.len())
    }

    async fn time_until_available(
        &self,
        key: &str,
        cost: f64,
        params: RateLimitParams,
    ) -> crate::Result<Duration> {
        let now = Instant::now();
        let mut guard = self.state.lock().await;
        match params.algorithm {
            DistributedAlgorithm::TokenBucket => {
                let tokens = match guard.get_mut(key) {
                    Some(KeyEntry {
                        state:
                            KeyState::Bucket {
                                tokens,
                                last_refill,
                            },
                        last_touched,
                        ..
                    }) => {
                        *tokens = Self::refill_bucket(*tokens, *last_refill, now, params);
                        *last_refill = now;
                        *last_touched = now;
                        *tokens
                    }
                    _ => params.burst,
                };
                if tokens + f64::EPSILON >= cost {
                    Ok(Duration::ZERO)
                } else if params.rate > 0.0 {
                    Ok(duration_from_secs_saturating((cost - tokens) / params.rate))
                } else {
                    Ok(Duration::MAX)
                }
            }
            DistributedAlgorithm::SlidingWindow => {
                let max = params.max_window_events();
                let needed = cost_to_permits(cost);
                match guard.get_mut(key) {
                    Some(KeyEntry {
                        state: KeyState::Window { stamps },
                        last_touched,
                        ..
                    }) => {
                        Self::prune_window(stamps, now, params);
                        *last_touched = now;
                        let used = u64::try_from(stamps.len()).unwrap_or(u64::MAX);
                        if used.saturating_add(needed) <= max {
                            Ok(Duration::ZERO)
                        } else {
                            Ok(stamps.first().map_or(Duration::ZERO, |&oldest| {
                                let expires = oldest + Duration::from_secs(params.window_secs);
                                expires.saturating_duration_since(now)
                            }))
                        }
                    }
                    _ => Ok(Duration::ZERO),
                }
            }
        }
    }

    async fn reset(&self, key: &str) -> crate::Result<()> {
        self.state.lock().await.remove(key);
        Ok(())
    }

    fn backend_name(&self) -> &str {
        "in-memory"
    }
}

/// Ergonomic handle for cluster-wide rate limiting against a backend.
///
/// Pairs a [`DistributedRateLimitBackend`] with a limiter `key` and a
/// [`RateLimitConfig`]. The algorithm (token bucket vs sliding window) is taken
/// from the config, exactly as with the in-process limiters in
/// [`crate::rate_limit`].
///
/// Cloning is cheap: the backend is held behind an [`Arc`] and shared.
#[derive(Clone)]
pub struct DistributedRateLimiter {
    backend: Arc<dyn DistributedRateLimitBackend>,
    key: String,
    config: RateLimitConfig,
    params: RateLimitParams,
}

impl std::fmt::Debug for DistributedRateLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributedRateLimiter")
            .field("key", &self.key)
            .field("backend", &self.backend.backend_name())
            .field("config", &self.config)
            .finish()
    }
}

impl DistributedRateLimiter {
    /// Create a new distributed rate limiter.
    ///
    /// # Arguments
    ///
    /// * `backend` - Shared store implementing [`DistributedRateLimitBackend`].
    /// * `key` - Logical limiter name (e.g. a task name or queue).
    /// * `config` - Rate limit configuration (selects the algorithm).
    pub fn new(
        backend: Arc<dyn DistributedRateLimitBackend>,
        key: impl Into<String>,
        config: RateLimitConfig,
    ) -> Self {
        let params = RateLimitParams::from_config(&config);
        Self {
            backend,
            key: key.into(),
            config,
            params,
        }
    }

    /// The limiter key.
    #[inline]
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// The active configuration.
    #[inline]
    #[must_use]
    pub fn config(&self) -> &RateLimitConfig {
        &self.config
    }

    /// The active algorithm.
    #[inline]
    #[must_use]
    pub fn algorithm(&self) -> DistributedAlgorithm {
        self.params.algorithm
    }

    /// Try to acquire a single permit cluster-wide.
    ///
    /// Returns `Ok(true)` if granted, `Ok(false)` if rate limited.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend store is unavailable.
    pub async fn try_acquire(&self) -> crate::Result<bool> {
        Ok(self
            .backend
            .acquire(&self.key, 1.0, self.params)
            .await?
            .allowed)
    }

    /// Try to acquire `cost` permits atomically.
    ///
    /// Returns the full [`AcquireOutcome`] so callers can inspect the remaining
    /// permits and suggested retry delay.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend store is unavailable.
    pub async fn try_acquire_n(&self, cost: f64) -> crate::Result<AcquireOutcome> {
        self.backend.acquire(&self.key, cost, self.params).await
    }

    /// Whether a *denied* acquisition could ever be granted by waiting.
    ///
    /// A token bucket with a non-positive refill rate never regains tokens once
    /// exhausted, and a sliding window admitting zero events never opens up: in
    /// both cases waiting is futile and the caller must be told so rather than
    /// left spinning.
    #[must_use]
    fn can_recover_after_denial(&self) -> bool {
        match self.params.algorithm {
            DistributedAlgorithm::TokenBucket => {
                self.params.rate.is_finite() && self.params.rate > 0.0
            }
            DistributedAlgorithm::SlidingWindow => self.params.max_window_events() >= 1,
        }
    }

    /// Acquire a single permit, awaiting (with bounded sleeps) until granted.
    ///
    /// Returns the total time waited. Each retry sleeps for the backend's
    /// suggested `retry_after`, clamped to `max_sleep` to remain responsive
    /// under contention from other workers.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend store is unavailable, or if the limiter is
    /// configured such that a permit can never become available (see
    /// [`Self::acquire_with_deadline`]).
    pub async fn acquire(&self, max_sleep: Duration) -> crate::Result<Duration> {
        // With no deadline the loop only ever exits via `Some` or an error.
        Ok(self
            .acquire_with_deadline(max_sleep, None)
            .await?
            .unwrap_or_default())
    }

    /// Acquire a single permit, giving up after `deadline` has elapsed.
    ///
    /// Returns `Ok(Some(waited))` when a permit was granted and `Ok(None)` when
    /// the deadline expired first. Passing `None` waits indefinitely, exactly like
    /// [`Self::acquire`].
    ///
    /// # Errors
    ///
    /// Returns an error if the backend store is unavailable, or if the limiter can
    /// never grant a permit no matter how long the caller waits (a token bucket
    /// with a non-positive rate, or a sliding window admitting zero events) — that
    /// is a configuration mistake, and reporting it beats spinning forever.
    pub async fn acquire_with_deadline(
        &self,
        max_sleep: Duration,
        deadline: Option<Duration>,
    ) -> crate::Result<Option<Duration>> {
        let start = Instant::now();
        loop {
            let outcome = self.backend.acquire(&self.key, 1.0, self.params).await?;
            if outcome.allowed {
                return Ok(Some(start.elapsed()));
            }
            if !self.can_recover_after_denial() {
                return Err(crate::CelersError::Configuration(format!(
                    "rate limiter '{}' can never grant a permit (rate={}, window={}s): waiting would block forever",
                    self.key, self.params.rate, self.params.window_secs
                )));
            }
            let elapsed = start.elapsed();
            if let Some(limit) = deadline {
                if elapsed >= limit {
                    return Ok(None);
                }
            }
            let mut sleep_for = outcome
                .retry_after
                .min(max_sleep)
                .max(Duration::from_millis(1));
            if let Some(limit) = deadline {
                sleep_for = sleep_for.min(limit - elapsed);
            }
            tokio::time::sleep(sleep_for).await;
        }
    }

    /// Available permits for this limiter (after refill).
    ///
    /// # Errors
    ///
    /// Returns an error if the backend store is unavailable.
    pub async fn available(&self) -> crate::Result<f64> {
        self.backend.available(&self.key, self.params).await
    }

    /// Estimated time until a single permit is available.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend store is unavailable.
    pub async fn time_until_available(&self) -> crate::Result<Duration> {
        self.backend
            .time_until_available(&self.key, 1.0, self.params)
            .await
    }

    /// Reset the limiter state for this key in the backend.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend store is unavailable.
    pub async fn reset(&self) -> crate::Result<()> {
        self.backend.reset(&self.key).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_token_bucket_burst_then_deny() {
        let backend = Arc::new(InMemoryDistributedBackend::new());
        let config = RateLimitConfig::new(10.0).with_burst(5);
        let limiter = DistributedRateLimiter::new(backend, "task", config);

        for _ in 0..5 {
            assert!(limiter.try_acquire().await.unwrap());
        }
        assert!(!limiter.try_acquire().await.unwrap());
        assert_eq!(limiter.algorithm(), DistributedAlgorithm::TokenBucket);
    }

    #[tokio::test]
    async fn test_token_bucket_refill() {
        let backend = Arc::new(InMemoryDistributedBackend::new());
        // 100/sec, burst 10 -> exhaust then refill after a short sleep.
        let config = RateLimitConfig::new(100.0).with_burst(10);
        let limiter = DistributedRateLimiter::new(backend, "task", config);

        for _ in 0..10 {
            assert!(limiter.try_acquire().await.unwrap());
        }
        assert!(!limiter.try_acquire().await.unwrap());

        tokio::time::sleep(Duration::from_millis(30)).await;
        // ~3 tokens should have refilled at 100/sec over 30ms.
        assert!(limiter.try_acquire().await.unwrap());
    }

    #[tokio::test]
    async fn test_sliding_window_basic_and_deny() {
        let backend = Arc::new(InMemoryDistributedBackend::new());
        let config = RateLimitConfig::new(5.0).with_sliding_window(1);
        let limiter = DistributedRateLimiter::new(backend, "task", config);
        assert_eq!(limiter.algorithm(), DistributedAlgorithm::SlidingWindow);

        for _ in 0..5 {
            assert!(limiter.try_acquire().await.unwrap());
        }
        assert!(!limiter.try_acquire().await.unwrap());
        let wait = limiter.time_until_available().await.unwrap();
        assert!(wait > Duration::ZERO);
    }

    #[tokio::test]
    async fn test_cost_n_acquire() {
        let backend = Arc::new(InMemoryDistributedBackend::new());
        let config = RateLimitConfig::new(10.0).with_burst(10);
        let limiter = DistributedRateLimiter::new(backend, "task", config);

        let outcome = limiter.try_acquire_n(4.0).await.unwrap();
        assert!(outcome.allowed);
        assert!((outcome.remaining - 6.0).abs() < 1e-6);

        // 6 left, request 7 -> denied with retry hint.
        let denied = limiter.try_acquire_n(7.0).await.unwrap();
        assert!(!denied.allowed);
        assert!(denied.retry_after > Duration::ZERO);
    }

    #[tokio::test]
    async fn test_available_and_reset() {
        // Use rate 0.0 so there is no refill between operations, making the
        // available-permit assertions deterministic.
        let backend = Arc::new(InMemoryDistributedBackend::new());
        let config = RateLimitConfig::new(0.0).with_burst(8);
        let limiter = DistributedRateLimiter::new(backend.clone(), "task", config);

        assert!((limiter.available().await.unwrap() - 8.0).abs() < 1e-6);
        for _ in 0..3 {
            assert!(limiter.try_acquire().await.unwrap());
        }
        let avail = limiter.available().await.unwrap();
        assert!((avail - 5.0).abs() < 1e-6, "expected 5 tokens, got {avail}");

        limiter.reset().await.unwrap();
        assert!((limiter.available().await.unwrap() - 8.0).abs() < 1e-6);
    }

    #[tokio::test]
    async fn test_concurrent_acquire_does_not_exceed_burst() {
        // Use rate 0 so no refill happens mid-test; only the initial burst is grantable.
        let backend = Arc::new(InMemoryDistributedBackend::new());
        let config = RateLimitConfig::new(0.0).with_burst(20);
        let limiter = DistributedRateLimiter::new(backend, "task", config);

        let mut handles = Vec::new();
        for _ in 0..8 {
            let l = limiter.clone();
            handles.push(tokio::spawn(async move {
                let mut count = 0u32;
                for _ in 0..10 {
                    if l.try_acquire().await.unwrap_or(false) {
                        count += 1;
                    }
                }
                count
            }));
        }

        let mut total = 0u32;
        for h in handles {
            total += h.await.unwrap();
        }
        // 8 workers * 10 attempts = 80 attempts, but only 20 tokens exist.
        assert_eq!(total, 20, "exactly the burst capacity should be granted");
    }

    #[tokio::test]
    async fn test_concurrent_sliding_window_cap() {
        let backend = Arc::new(InMemoryDistributedBackend::new());
        // 100/sec over a 10s window -> 1000 max events; cap the concurrent grants.
        let config = RateLimitConfig::new(3.0).with_sliding_window(100);
        let limiter = DistributedRateLimiter::new(backend, "task", config);
        let max = limiter.params.max_window_events();

        let mut handles = Vec::new();
        for _ in 0..6 {
            let l = limiter.clone();
            handles.push(tokio::spawn(async move {
                let mut count = 0u32;
                for _ in 0..200 {
                    if l.try_acquire().await.unwrap_or(false) {
                        count += 1;
                    }
                }
                count
            }));
        }
        let mut total = 0u64;
        for h in handles {
            total += u64::from(h.await.unwrap());
        }
        assert_eq!(total, max, "sliding window must not exceed max events");
    }

    #[tokio::test]
    async fn test_acquire_waits_then_succeeds() {
        let backend = Arc::new(InMemoryDistributedBackend::new());
        let config = RateLimitConfig::new(50.0).with_burst(1);
        let limiter = DistributedRateLimiter::new(backend, "task", config);

        assert!(limiter.try_acquire().await.unwrap());
        // Now empty; acquire() should sleep until a token refills (~20ms at 50/s).
        let waited = limiter.acquire(Duration::from_millis(100)).await.unwrap();
        assert!(waited > Duration::ZERO);
    }

    #[tokio::test]
    async fn test_shared_backend_across_keys() {
        let backend = Arc::new(InMemoryDistributedBackend::new());
        let a = DistributedRateLimiter::new(
            backend.clone(),
            "a",
            RateLimitConfig::new(0.0).with_burst(2),
        );
        let b = DistributedRateLimiter::new(
            backend.clone(),
            "b",
            RateLimitConfig::new(0.0).with_burst(2),
        );

        assert!(a.try_acquire().await.unwrap());
        assert!(a.try_acquire().await.unwrap());
        assert!(!a.try_acquire().await.unwrap());
        // b is an independent key, unaffected by a's exhaustion.
        assert!(b.try_acquire().await.unwrap());
        assert!(b.try_acquire().await.unwrap());
        assert!(!b.try_acquire().await.unwrap());
        assert_eq!(backend.key_count().await, 2);
    }

    #[tokio::test]
    async fn test_negative_cost_rejected() {
        let backend = Arc::new(InMemoryDistributedBackend::new());
        let config = RateLimitConfig::new(10.0).with_burst(5);
        let limiter = DistributedRateLimiter::new(backend, "task", config);
        assert!(limiter.try_acquire_n(-1.0).await.is_err());
    }

    /// Regression: the in-memory backend retained one map entry per limiter key
    /// for the process lifetime, with no expiry and no capacity bound.
    #[tokio::test]
    async fn test_sweep_idle_reclaims_untouched_keys() {
        let backend = InMemoryDistributedBackend::new();
        let params = RateLimitParams::from_config(&RateLimitConfig::new(10.0).with_burst(5));

        for i in 0..10 {
            backend
                .acquire(&format!("key-{i}"), 1.0, params)
                .await
                .expect("acquire should succeed");
        }
        assert_eq!(backend.key_count().await, 10);

        // A generous idle window keeps everything: nothing has aged out yet.
        let swept = backend
            .sweep_idle(Duration::from_secs(3600))
            .await
            .expect("sweep should succeed");
        assert_eq!(swept, 0);
        assert_eq!(backend.key_count().await, 10);

        // Back-date half the entries so they fall outside a 30s window, without
        // depending on wall-clock progress.
        {
            let mut guard = backend.state.lock().await;
            let now = Instant::now();
            for i in 0..5 {
                if let Some(entry) = guard.get_mut(&format!("key-{i}")) {
                    entry.last_touched = now
                        .checked_sub(Duration::from_secs(60))
                        .unwrap_or(entry.last_touched);
                }
            }
        }

        let swept = backend
            .sweep_idle(Duration::from_secs(30))
            .await
            .expect("sweep should succeed");
        assert_eq!(swept, 5, "aged-out keys must be reclaimed");
        assert_eq!(backend.key_count().await, 5);

        // A zero window reclaims everything.
        let swept = backend
            .sweep_idle(Duration::ZERO)
            .await
            .expect("sweep should succeed");
        assert_eq!(swept, 5);
        assert_eq!(backend.key_count().await, 0);
    }

    #[tokio::test]
    async fn test_key_map_is_capacity_bounded_with_lru_eviction() {
        let backend = InMemoryDistributedBackend::with_max_keys(3);
        assert_eq!(backend.max_keys(), 3);
        let params = RateLimitParams::from_config(&RateLimitConfig::new(0.0).with_burst(5));

        for i in 0..3 {
            backend
                .acquire(&format!("key-{i}"), 1.0, params)
                .await
                .expect("acquire should succeed");
        }
        assert_eq!(backend.key_count().await, 3);

        // Touch key-0 so key-1 becomes the least recently used.
        backend
            .acquire("key-0", 1.0, params)
            .await
            .expect("acquire should succeed");

        backend
            .acquire("key-3", 1.0, params)
            .await
            .expect("acquire should succeed");
        assert_eq!(backend.key_count().await, 3);

        // key-1 was evicted, so it starts from a full burst again; key-0 and
        // key-2 retain their consumed tokens (rate 0 means no refill).
        let evicted = backend
            .available("key-1", params)
            .await
            .expect("available should succeed");
        assert!(
            (evicted - 5.0).abs() < 1e-6,
            "key-1 should have been evicted, saw {evicted} tokens"
        );
        let retained = backend
            .available("key-2", params)
            .await
            .expect("available should succeed");
        assert!(
            (retained - 4.0).abs() < 1e-6,
            "key-2 should have been retained, saw {retained} tokens"
        );
    }

    #[tokio::test]
    async fn test_max_keys_zero_is_clamped_to_one() {
        let backend = InMemoryDistributedBackend::with_max_keys(0);
        assert_eq!(backend.max_keys(), 1);
        let params = RateLimitParams::from_config(&RateLimitConfig::new(10.0).with_burst(5));
        backend
            .acquire("a", 1.0, params)
            .await
            .expect("acquire should succeed");
        backend
            .acquire("b", 1.0, params)
            .await
            .expect("acquire should succeed");
        assert_eq!(backend.key_count().await, 1);
    }

    /// Regression: `acquire` looped forever with a rate of 0, because the backend
    /// reports `retry_after = Duration::MAX` which was silently clamped to
    /// `max_sleep`, so the caller spun at `max_sleep` cadence with no way out.
    #[tokio::test]
    async fn test_acquire_errors_instead_of_spinning_at_zero_rate() {
        let backend = Arc::new(InMemoryDistributedBackend::new());
        let config = RateLimitConfig::new(0.0).with_burst(1);
        let limiter = DistributedRateLimiter::new(backend, "task", config);

        assert!(limiter.try_acquire().await.expect("first acquire"));
        // Returns immediately, without a single sleep.
        let err = limiter
            .acquire(Duration::from_millis(10))
            .await
            .expect_err("a zero-rate limiter can never grant another permit");
        assert!(
            err.to_string().contains("can never grant a permit"),
            "unexpected error: {err}"
        );

        // The same holds for a sliding window that admits nothing.
        let backend = Arc::new(InMemoryDistributedBackend::new());
        let window = DistributedRateLimiter::new(
            backend,
            "task",
            RateLimitConfig::new(0.0).with_sliding_window(10),
        );
        assert!(window.acquire(Duration::from_millis(10)).await.is_err());
    }

    #[tokio::test]
    async fn test_acquire_with_deadline_gives_up() {
        let backend = Arc::new(InMemoryDistributedBackend::new());
        // 1 permit per 100 seconds: no refill can arrive within the deadline.
        let config = RateLimitConfig::new(0.01).with_burst(1);
        let limiter = DistributedRateLimiter::new(backend, "task", config);

        assert!(limiter.try_acquire().await.expect("first acquire"));
        // A zero deadline gives up on the first denial, without sleeping.
        let outcome = limiter
            .acquire_with_deadline(Duration::from_millis(50), Some(Duration::ZERO))
            .await
            .expect("acquire_with_deadline should not error");
        assert!(outcome.is_none(), "deadline should have expired");

        // A short but non-zero deadline also gives up rather than hanging.
        let outcome = limiter
            .acquire_with_deadline(Duration::from_millis(1), Some(Duration::from_millis(2)))
            .await
            .expect("acquire_with_deadline should not error");
        assert!(outcome.is_none(), "deadline should have expired");
    }

    #[tokio::test]
    async fn test_acquire_with_deadline_succeeds_when_permits_are_available() {
        let backend = Arc::new(InMemoryDistributedBackend::new());
        let config = RateLimitConfig::new(50.0).with_burst(2);
        let limiter = DistributedRateLimiter::new(backend, "task", config);

        let outcome = limiter
            .acquire_with_deadline(Duration::from_millis(50), Some(Duration::from_secs(5)))
            .await
            .expect("acquire_with_deadline should not error");
        assert!(outcome.is_some(), "a permit was available immediately");
    }

    #[tokio::test]
    async fn test_params_from_config() {
        let bucket = RateLimitParams::from_config(&RateLimitConfig::new(10.0).with_burst(20));
        assert_eq!(bucket.algorithm, DistributedAlgorithm::TokenBucket);
        assert!((bucket.burst - 20.0).abs() < 1e-6);

        let window =
            RateLimitParams::from_config(&RateLimitConfig::new(4.0).with_sliding_window(5));
        assert_eq!(window.algorithm, DistributedAlgorithm::SlidingWindow);
        assert_eq!(window.window_secs, 5);
        assert_eq!(window.max_window_events(), 20);
    }
}
