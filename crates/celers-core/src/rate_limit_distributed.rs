#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    clippy::cast_possible_wrap
)]
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
        (self.rate * self.window_secs as f64).ceil().max(0.0) as u64
    }
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
#[derive(Debug, Default)]
pub struct InMemoryDistributedBackend {
    state: Mutex<HashMap<String, KeyState>>,
}

impl InMemoryDistributedBackend {
    /// Create a new empty in-memory backend.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Mutex::new(HashMap::new()),
        }
    }

    /// Number of keys currently tracked (primarily for tests/diagnostics).
    pub async fn key_count(&self) -> usize {
        self.state.lock().await.len()
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
        let mut guard = self.state.lock().await;

        match params.algorithm {
            DistributedAlgorithm::TokenBucket => {
                let entry = guard
                    .entry(key.to_string())
                    .or_insert_with(|| KeyState::Bucket {
                        tokens: params.burst,
                        last_refill: now,
                    });
                // If a key was previously used with a different algorithm, reset it.
                if !matches!(entry, KeyState::Bucket { .. }) {
                    *entry = KeyState::Bucket {
                        tokens: params.burst,
                        last_refill: now,
                    };
                }
                let KeyState::Bucket {
                    tokens,
                    last_refill,
                } = entry
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
                        Duration::from_secs_f64(deficit / params.rate)
                    } else {
                        Duration::MAX
                    };
                    Ok(AcquireOutcome::denied(*tokens, retry_after))
                }
            }
            DistributedAlgorithm::SlidingWindow => {
                let entry = guard
                    .entry(key.to_string())
                    .or_insert_with(|| KeyState::Window { stamps: Vec::new() });
                if !matches!(entry, KeyState::Window { .. }) {
                    *entry = KeyState::Window { stamps: Vec::new() };
                }
                let KeyState::Window { stamps } = entry else {
                    unreachable!("entry coerced to Window above")
                };
                Self::prune_window(stamps, now, params);
                let max = params.max_window_events();
                let needed = cost.ceil() as u64;
                let used = stamps.len() as u64;
                if used + needed <= max {
                    for _ in 0..needed {
                        stamps.push(now);
                    }
                    let remaining = max.saturating_sub(used + needed) as f64;
                    Ok(AcquireOutcome::allowed(remaining))
                } else {
                    let remaining = max.saturating_sub(used) as f64;
                    let retry_after = stamps.first().map_or(Duration::ZERO, |&oldest| {
                        let expires = oldest + Duration::from_secs(params.window_secs);
                        expires.saturating_duration_since(now)
                    });
                    Ok(AcquireOutcome::denied(remaining, retry_after))
                }
            }
        }
    }

    async fn available(&self, key: &str, params: RateLimitParams) -> crate::Result<f64> {
        let now = Instant::now();
        let mut guard = self.state.lock().await;
        match params.algorithm {
            DistributedAlgorithm::TokenBucket => match guard.get_mut(key) {
                Some(KeyState::Bucket {
                    tokens,
                    last_refill,
                }) => {
                    *tokens = Self::refill_bucket(*tokens, *last_refill, now, params);
                    *last_refill = now;
                    Ok(*tokens)
                }
                _ => Ok(params.burst),
            },
            DistributedAlgorithm::SlidingWindow => {
                let max = params.max_window_events() as f64;
                match guard.get_mut(key) {
                    Some(KeyState::Window { stamps }) => {
                        Self::prune_window(stamps, now, params);
                        Ok(max - stamps.len() as f64)
                    }
                    _ => Ok(max),
                }
            }
        }
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
                    Some(KeyState::Bucket {
                        tokens,
                        last_refill,
                    }) => {
                        *tokens = Self::refill_bucket(*tokens, *last_refill, now, params);
                        *last_refill = now;
                        *tokens
                    }
                    _ => params.burst,
                };
                if tokens + f64::EPSILON >= cost {
                    Ok(Duration::ZERO)
                } else if params.rate > 0.0 {
                    Ok(Duration::from_secs_f64((cost - tokens) / params.rate))
                } else {
                    Ok(Duration::MAX)
                }
            }
            DistributedAlgorithm::SlidingWindow => {
                let max = params.max_window_events();
                let needed = cost.ceil() as u64;
                match guard.get_mut(key) {
                    Some(KeyState::Window { stamps }) => {
                        Self::prune_window(stamps, now, params);
                        if stamps.len() as u64 + needed <= max {
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

    /// Acquire a single permit, awaiting (with bounded sleeps) until granted.
    ///
    /// Returns the total time waited. Each retry sleeps for the backend's
    /// suggested `retry_after`, clamped to `max_sleep` to remain responsive
    /// under contention from other workers.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend store is unavailable.
    pub async fn acquire(&self, max_sleep: Duration) -> crate::Result<Duration> {
        let start = Instant::now();
        loop {
            let outcome = self.backend.acquire(&self.key, 1.0, self.params).await?;
            if outcome.allowed {
                return Ok(start.elapsed());
            }
            let sleep_for = outcome
                .retry_after
                .min(max_sleep)
                .max(Duration::from_millis(1));
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
