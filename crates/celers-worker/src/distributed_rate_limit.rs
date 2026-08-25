//! Distributed rate limiting coordination across multiple workers
//!
//! This module provides distributed rate limiting capabilities that work across
//! multiple worker instances, ensuring that rate limits are enforced globally
//! rather than per-worker.
//!
//! # Features
//!
//! - **Redis-based coordination**: Use Redis for distributed token bucket tracking
//! - **Atomic accounting**: the refill-and-consume step runs as a single Lua
//!   script inside Redis, so concurrent workers cannot over-grant permits
//! - **Automatic token refill**: Tokens are refilled based on configured rates
//! - **Multi-worker coordination**: Rate limits apply across all workers
//!
//! # Why a Lua script
//!
//! A token bucket implemented as `GET` → compute in the client → `SET` is not
//! a rate limiter under contention: two workers that both read `available = 1`
//! both pass the check and both write `0`, so a one-permit budget grants two
//! permits — and the failure mode appears precisely when the limiter is
//! needed. [`DistributedRateLimiter`] therefore performs the whole
//! refill/compare/decrement sequence inside Redis via [`redis::Script`], where
//! it is serialised against every other worker by Redis's single-threaded
//! command execution. [`InMemoryDistributedRateLimiter`] applies the same
//! algorithm under a single write lock.
//!
//! # Example
//!
//! ```no_run
//! # #[cfg(feature = "redis")]
//! # async fn example() -> celers_core::Result<()> {
//! use celers_worker::distributed_rate_limit::{
//!     DistributedRateLimitConfig, DistributedRateLimiter, DistributedRateLimiterTrait,
//! };
//!
//! let config = DistributedRateLimitConfig {
//!     redis_url: "redis://127.0.0.1:6379".to_string(),
//!     key_prefix: "celery:rate_limit".to_string(),
//!     capacity: 100,
//!     refill_rate: 10.0, // 10 tokens per second
//!     window_size_secs: 60,
//! };
//!
//! let limiter = DistributedRateLimiter::new(config).await?;
//!
//! // Check if we can process a task
//! if limiter.try_acquire("my_task_type", 1).await? {
//!     println!("Task allowed");
//! } else {
//!     println!("Rate limit exceeded");
//! }
//! # Ok(())
//! # }
//! ```

use async_trait::async_trait;
#[allow(unused_imports)]
use celers_core::{CelersError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
#[allow(unused_imports)]
use tracing::{debug, info};

/// Configuration for distributed rate limiting
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedRateLimitConfig {
    /// Redis connection URL
    pub redis_url: String,

    /// Key prefix for Redis keys
    pub key_prefix: String,

    /// Maximum number of tokens in the bucket
    pub capacity: u64,

    /// Token refill rate (tokens per second)
    pub refill_rate: f64,

    /// Time window size for sliding window rate limiting (seconds)
    pub window_size_secs: u64,
}

impl Default for DistributedRateLimitConfig {
    fn default() -> Self {
        Self {
            redis_url: "redis://127.0.0.1:6379".to_string(),
            key_prefix: "celery:rate_limit".to_string(),
            capacity: 100,
            refill_rate: 10.0,
            window_size_secs: 60,
        }
    }
}

impl DistributedRateLimitConfig {
    /// Create a new configuration
    pub fn new(redis_url: impl Into<String>) -> Self {
        Self {
            redis_url: redis_url.into(),
            ..Default::default()
        }
    }

    /// Set the capacity
    pub fn with_capacity(mut self, capacity: u64) -> Self {
        self.capacity = capacity;
        self
    }

    /// Set the refill rate
    pub fn with_refill_rate(mut self, refill_rate: f64) -> Self {
        self.refill_rate = refill_rate;
        self
    }

    /// Set the window size
    pub fn with_window_size(mut self, window_size_secs: u64) -> Self {
        self.window_size_secs = window_size_secs;
        self
    }

    /// Set the key prefix
    pub fn with_key_prefix(mut self, key_prefix: impl Into<String>) -> Self {
        self.key_prefix = key_prefix.into();
        self
    }

    /// Validate the configuration
    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.redis_url.is_empty() {
            return Err("Redis URL cannot be empty".to_string());
        }
        if self.capacity == 0 {
            return Err("Capacity must be greater than 0".to_string());
        }
        if self.refill_rate <= 0.0 {
            return Err("Refill rate must be greater than 0".to_string());
        }
        if self.window_size_secs == 0 {
            return Err("Window size must be greater than 0".to_string());
        }
        Ok(())
    }
}

/// Trait for distributed rate limiters
#[async_trait]
pub trait DistributedRateLimiterTrait: Send + Sync {
    /// Try to acquire tokens for a task type
    async fn try_acquire(&self, task_type: &str, tokens: u64) -> Result<bool>;

    /// Get the number of available tokens for a task type
    async fn available_tokens(&self, task_type: &str) -> Result<u64>;

    /// Get the time until the next token is available
    async fn time_until_next_token(&self, task_type: &str) -> Result<Duration>;

    /// Reset the rate limiter for a task type
    async fn reset(&self, task_type: &str) -> Result<()>;

    /// Get rate limiting statistics
    async fn get_stats(&self, task_type: &str) -> Result<RateLimitStats>;
}

/// Statistics for rate limiting
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitStats {
    /// Total number of requests
    pub total_requests: u64,

    /// Number of allowed requests
    pub allowed_requests: u64,

    /// Number of rejected requests
    pub rejected_requests: u64,

    /// Current available tokens
    pub available_tokens: u64,

    /// Maximum capacity
    pub capacity: u64,

    /// Rejection rate (0.0 to 1.0)
    pub rejection_rate: f64,
}

impl RateLimitStats {
    /// Create new statistics
    pub fn new(capacity: u64) -> Self {
        Self {
            total_requests: 0,
            allowed_requests: 0,
            rejected_requests: 0,
            available_tokens: capacity,
            capacity,
            rejection_rate: 0.0,
        }
    }

    /// Calculate rejection rate
    pub fn calculate_rejection_rate(&mut self) {
        if self.total_requests > 0 {
            self.rejection_rate = self.rejected_requests as f64 / self.total_requests as f64;
        }
    }
}

/// Current wall-clock time as fractional seconds since the Unix epoch.
///
/// A clock that predates the epoch yields `0.0` rather than panicking; the
/// bucket treats non-positive elapsed time as "no refill", so a nonsensical
/// clock can never mint tokens.
fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

/// How long it takes to accumulate `wanted` tokens starting from `available`.
///
/// Returns [`Duration::ZERO`] when the tokens are already there and
/// [`Duration::MAX`] when no refill rate is configured (they never arrive).
fn time_to_accumulate(wanted: u64, available: u64, refill_rate: f64) -> Duration {
    let deficit = wanted.saturating_sub(available);
    if deficit == 0 {
        return Duration::ZERO;
    }
    if refill_rate > 0.0 {
        Duration::from_secs_f64(deficit as f64 / refill_rate)
    } else {
        Duration::MAX
    }
}

/// Result of a single token-bucket acquisition attempt.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AcquireOutcome {
    /// Whether the requested tokens were granted.
    pub allowed: bool,
    /// Whole tokens left in the bucket afterwards.
    pub remaining: u64,
    /// How long to wait before a retry could succeed. Zero when allowed.
    pub retry_after: Duration,
}

/// A continuous token bucket.
///
/// Tokens are tracked as a `f64` on purpose: flooring the refill to whole
/// tokens and advancing the timestamp anyway would starve a bucket polled
/// more often than one token per interval (each poll credits `floor(small
/// elapsed * rate) == 0` and throws the elapsed time away). This is the exact
/// algorithm the Redis Lua script implements, kept here so it can be tested
/// without a server.
#[derive(Debug, Clone, Copy)]
struct TokenBucket {
    tokens: f64,
    last_refill: f64,
}

impl TokenBucket {
    fn new(capacity: u64, now: f64) -> Self {
        Self {
            tokens: capacity as f64,
            last_refill: now,
        }
    }

    /// Credit elapsed time, capped at `capacity`.
    fn refill(&mut self, capacity: u64, refill_rate: f64, now: f64) {
        // Guard against clock skew between workers: never credit negative time.
        let elapsed = (now - self.last_refill).max(0.0);
        if elapsed > 0.0 {
            self.tokens = (self.tokens + elapsed * refill_rate).min(capacity as f64);
            self.last_refill = now;
        }
    }

    /// Refill and, if the budget allows, consume `cost` tokens.
    fn try_consume(
        &mut self,
        capacity: u64,
        refill_rate: f64,
        now: f64,
        cost: u64,
    ) -> AcquireOutcome {
        self.refill(capacity, refill_rate, now);

        let cost_f = cost as f64;
        if self.tokens >= cost_f {
            self.tokens -= cost_f;
            AcquireOutcome {
                allowed: true,
                remaining: self.whole_tokens(),
                retry_after: Duration::ZERO,
            }
        } else {
            let deficit = cost_f - self.tokens;
            let retry_after = if refill_rate > 0.0 {
                Duration::from_secs_f64(deficit / refill_rate)
            } else {
                Duration::MAX
            };
            AcquireOutcome {
                allowed: false,
                remaining: self.whole_tokens(),
                retry_after,
            }
        }
    }

    fn whole_tokens(&self) -> u64 {
        if self.tokens <= 0.0 {
            0
        } else {
            self.tokens as u64
        }
    }
}

/// Atomic token-bucket refill-and-consume, executed server-side by Redis.
///
/// `KEYS[1]` is the token count, `KEYS[2]` the last-refill timestamp.
/// `ARGV` is `capacity, refill_rate, now, cost, ttl_seconds`. Returns
/// `{allowed, remaining_whole_tokens, retry_after_ms}` where `retry_after_ms`
/// is `-1` when no refill rate is configured (i.e. never).
///
/// Redis executes a script atomically with respect to every other client, so
/// the read, the comparison and the write cannot interleave with another
/// worker's — which is the entire point of the exercise.
#[cfg(feature = "redis")]
const TOKEN_BUCKET_SCRIPT: &str = r"
local capacity = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])

local tokens = tonumber(redis.call('GET', KEYS[1]))
local last = tonumber(redis.call('GET', KEYS[2]))
if tokens == nil then tokens = capacity end
if last == nil then last = now end

-- Never credit negative elapsed time: worker clocks can disagree.
local elapsed = now - last
if elapsed < 0 then elapsed = 0 end
if elapsed > 0 then
  tokens = math.min(capacity, tokens + elapsed * rate)
  last = now
end

local allowed = 0
local retry_after_ms = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
elseif rate > 0 then
  retry_after_ms = math.ceil(((cost - tokens) / rate) * 1000)
else
  retry_after_ms = -1
end

redis.call('SET', KEYS[1], tokens, 'EX', ttl)
redis.call('SET', KEYS[2], last, 'EX', ttl)

local remaining = math.floor(tokens)
if remaining < 0 then remaining = 0 end
return {allowed, remaining, retry_after_ms}
";

#[cfg(feature = "redis")]
/// Redis-based distributed rate limiter
pub struct DistributedRateLimiter {
    config: DistributedRateLimitConfig,
    client: redis::Client,
    stats: Arc<RwLock<HashMap<String, RateLimitStats>>>,
}

#[cfg(feature = "redis")]
impl DistributedRateLimiter {
    /// Create a new distributed rate limiter
    pub async fn new(config: DistributedRateLimitConfig) -> Result<Self> {
        config.validate().map_err(CelersError::Other)?;

        let client = redis::Client::open(config.redis_url.as_str())
            .map_err(|e| CelersError::Other(format!("Redis connection error: {}", e)))?;

        // Test connection
        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Other(format!("Redis connection error: {}", e)))?;

        redis::cmd("PING")
            .query_async::<String>(&mut conn)
            .await
            .map_err(|e| CelersError::Other(format!("Redis ping failed: {}", e)))?;

        info!("Connected to Redis for distributed rate limiting");

        Ok(Self {
            config,
            client,
            stats: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Get the Redis key for a task type's token bucket
    fn bucket_key(&self, task_type: &str) -> String {
        format!("{}:bucket:{}", self.config.key_prefix, task_type)
    }

    /// Get the Redis key for a task type's last refill timestamp
    fn refill_key(&self, task_type: &str) -> String {
        format!("{}:refill:{}", self.config.key_prefix, task_type)
    }

    /// Get the Redis key for a task type's sliding window
    fn window_key(&self, task_type: &str) -> String {
        format!("{}:window:{}", self.config.key_prefix, task_type)
    }

    /// Get the Redis key for a task type's statistics
    fn stats_key(&self, task_type: &str) -> String {
        format!("{}:stats:{}", self.config.key_prefix, task_type)
    }

    /// Run the atomic token-bucket script for `task_type`.
    ///
    /// A `cost` of zero performs the refill and reports the balance without
    /// consuming anything, which is how [`Self::available_tokens`] observes
    /// the bucket without perturbing it.
    async fn run_bucket_script(&self, task_type: &str, cost: u64) -> Result<AcquireOutcome> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Other(format!("Redis connection error: {}", e)))?;

        let script = redis::Script::new(TOKEN_BUCKET_SCRIPT);
        let (allowed, remaining, retry_after_ms): (i64, i64, i64) = script
            .key(self.bucket_key(task_type))
            .key(self.refill_key(task_type))
            .arg(self.config.capacity)
            .arg(self.config.refill_rate)
            .arg(now_secs())
            .arg(cost)
            .arg(self.ttl_secs())
            .invoke_async(&mut conn)
            .await
            .map_err(|e| CelersError::Other(format!("Redis script error: {}", e)))?;

        Ok(AcquireOutcome {
            allowed: allowed == 1,
            remaining: remaining.max(0) as u64,
            retry_after: if retry_after_ms < 0 {
                Duration::MAX
            } else {
                Duration::from_millis(retry_after_ms as u64)
            },
        })
    }

    /// Key expiry for the bucket, in seconds.
    ///
    /// The bucket must survive long enough for an empty bucket to refill
    /// completely, otherwise expiry silently resets it to full capacity and
    /// hands out a free burst. Take the larger of the configured window and
    /// the full-refill time.
    fn ttl_secs(&self) -> u64 {
        let full_refill = if self.config.refill_rate > 0.0 {
            (self.config.capacity as f64 / self.config.refill_rate).ceil() as u64
        } else {
            self.config.window_size_secs
        };
        self.config.window_size_secs.max(full_refill).max(1)
    }

    /// Update local statistics
    async fn update_stats(&self, task_type: &str, allowed: bool, available_tokens: u64) {
        let mut stats = self.stats.write().await;
        let stat = stats
            .entry(task_type.to_string())
            .or_insert_with(|| RateLimitStats::new(self.config.capacity));

        stat.total_requests += 1;
        if allowed {
            stat.allowed_requests += 1;
        } else {
            stat.rejected_requests += 1;
        }
        stat.available_tokens = available_tokens;
        stat.calculate_rejection_rate();
    }
}

#[cfg(feature = "redis")]
#[async_trait]
impl DistributedRateLimiterTrait for DistributedRateLimiter {
    async fn try_acquire(&self, task_type: &str, tokens: u64) -> Result<bool> {
        if tokens == 0 {
            return Ok(true);
        }

        // One round trip; the refill, the comparison and the decrement all
        // happen inside Redis so concurrent workers cannot over-grant.
        let outcome = self.run_bucket_script(task_type, tokens).await?;

        self.update_stats(task_type, outcome.allowed, outcome.remaining)
            .await;

        if outcome.allowed {
            debug!(
                "Acquired {} tokens for task type '{}' (remaining: {})",
                tokens, task_type, outcome.remaining
            );
        } else {
            debug!(
                "Rate limit exceeded for task type '{}' (available: {}, requested: {}, retry in {:?})",
                task_type, outcome.remaining, tokens, outcome.retry_after
            );
        }

        Ok(outcome.allowed)
    }

    async fn available_tokens(&self, task_type: &str) -> Result<u64> {
        Ok(self.run_bucket_script(task_type, 0).await?.remaining)
    }

    async fn time_until_next_token(&self, task_type: &str) -> Result<Duration> {
        // Ask the bucket what a one-token request would have to wait for,
        // without consuming anything.
        let outcome = self.run_bucket_script(task_type, 0).await?;
        if outcome.remaining >= 1 {
            return Ok(Duration::ZERO);
        }
        Ok(time_to_accumulate(
            1,
            outcome.remaining,
            self.config.refill_rate,
        ))
    }

    async fn reset(&self, task_type: &str) -> Result<()> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Other(format!("Redis connection error: {}", e)))?;

        let bucket_key = self.bucket_key(task_type);
        let refill_key = self.refill_key(task_type);
        let window_key = self.window_key(task_type);
        let stats_key = self.stats_key(task_type);

        redis::pipe()
            .del(&bucket_key)
            .del(&refill_key)
            .del(&window_key)
            .del(&stats_key)
            .query_async::<()>(&mut conn)
            .await
            .map_err(|e| CelersError::Other(format!("Redis pipeline error: {}", e)))?;

        // Clear local stats
        let mut stats = self.stats.write().await;
        stats.remove(task_type);

        info!("Reset rate limiter for task type '{}'", task_type);

        Ok(())
    }

    async fn get_stats(&self, task_type: &str) -> Result<RateLimitStats> {
        let stats = self.stats.read().await;
        Ok(stats
            .get(task_type)
            .cloned()
            .unwrap_or_else(|| RateLimitStats::new(self.config.capacity)))
    }
}

/// In-memory distributed rate limiter (single-process; also used to exercise
/// the bucket algorithm without a Redis server)
pub struct InMemoryDistributedRateLimiter {
    config: DistributedRateLimitConfig,
    buckets: Arc<RwLock<HashMap<String, TokenBucket>>>,
    stats: Arc<RwLock<HashMap<String, RateLimitStats>>>,
}

impl InMemoryDistributedRateLimiter {
    /// Create a new in-memory distributed rate limiter
    pub fn new(config: DistributedRateLimitConfig) -> Self {
        Self {
            config,
            buckets: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Refill and (when `cost > 0`) consume, under a single write lock.
    ///
    /// Holding the lock across the whole read-modify-write is what makes this
    /// a limiter: releasing it between the refill and the decrement would let
    /// two callers that each observed one available token both consume it —
    /// over-granting, and underflowing the balance.
    async fn acquire(&self, task_type: &str, cost: u64) -> AcquireOutcome {
        let now = now_secs();
        let capacity = self.config.capacity;
        let rate = self.config.refill_rate;

        let mut buckets = self.buckets.write().await;
        let bucket = buckets
            .entry(task_type.to_string())
            .or_insert_with(|| TokenBucket::new(capacity, now));
        bucket.try_consume(capacity, rate, now, cost)
    }

    /// Update statistics
    async fn update_stats(&self, task_type: &str, allowed: bool, available_tokens: u64) {
        let mut stats = self.stats.write().await;
        let stat = stats
            .entry(task_type.to_string())
            .or_insert_with(|| RateLimitStats::new(self.config.capacity));

        stat.total_requests += 1;
        if allowed {
            stat.allowed_requests += 1;
        } else {
            stat.rejected_requests += 1;
        }
        stat.available_tokens = available_tokens;
        stat.calculate_rejection_rate();
    }
}

#[async_trait]
impl DistributedRateLimiterTrait for InMemoryDistributedRateLimiter {
    async fn try_acquire(&self, task_type: &str, tokens: u64) -> Result<bool> {
        if tokens == 0 {
            return Ok(true);
        }

        let outcome = self.acquire(task_type, tokens).await;
        self.update_stats(task_type, outcome.allowed, outcome.remaining)
            .await;
        Ok(outcome.allowed)
    }

    async fn available_tokens(&self, task_type: &str) -> Result<u64> {
        Ok(self.acquire(task_type, 0).await.remaining)
    }

    async fn time_until_next_token(&self, task_type: &str) -> Result<Duration> {
        let available = self.acquire(task_type, 0).await.remaining;
        Ok(time_to_accumulate(1, available, self.config.refill_rate))
    }

    async fn reset(&self, task_type: &str) -> Result<()> {
        let mut buckets = self.buckets.write().await;
        buckets.remove(task_type);

        let mut stats = self.stats.write().await;
        stats.remove(task_type);

        Ok(())
    }

    async fn get_stats(&self, task_type: &str) -> Result<RateLimitStats> {
        let stats = self.stats.read().await;
        Ok(stats
            .get(task_type)
            .cloned()
            .unwrap_or_else(|| RateLimitStats::new(self.config.capacity)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_config_validation() {
        let config = DistributedRateLimitConfig::default();
        assert!(config.validate().is_ok());

        let config = DistributedRateLimitConfig::default().with_capacity(0);
        assert!(config.validate().is_err());

        let config = DistributedRateLimitConfig::default().with_refill_rate(0.0);
        assert!(config.validate().is_err());
    }

    #[tokio::test]
    async fn test_in_memory_limiter() {
        let config = DistributedRateLimitConfig::default()
            .with_capacity(10)
            .with_refill_rate(1.0);

        let limiter = InMemoryDistributedRateLimiter::new(config);

        // Should allow up to capacity
        for _ in 0..10 {
            assert!(limiter.try_acquire("test_task", 1).await.unwrap());
        }

        // Should reject when capacity exhausted
        assert!(!limiter.try_acquire("test_task", 1).await.unwrap());

        // Check stats
        let stats = limiter.get_stats("test_task").await.unwrap();
        assert_eq!(stats.total_requests, 11);
        assert_eq!(stats.allowed_requests, 10);
        assert_eq!(stats.rejected_requests, 1);
    }

    #[tokio::test]
    async fn test_token_refill() {
        let config = DistributedRateLimitConfig::default()
            .with_capacity(10)
            .with_refill_rate(10.0); // 10 tokens per second

        let limiter = InMemoryDistributedRateLimiter::new(config);

        // Consume all tokens
        for _ in 0..10 {
            assert!(limiter.try_acquire("test_task", 1).await.unwrap());
        }

        // Wait for refill
        tokio::time::sleep(Duration::from_millis(1100)).await;

        // Should have at least 10 tokens refilled
        let available = limiter.available_tokens("test_task").await.unwrap();
        assert!(available >= 10);
    }

    #[tokio::test]
    async fn test_reset() {
        let config = DistributedRateLimitConfig::default().with_capacity(10);
        let limiter = InMemoryDistributedRateLimiter::new(config);

        // Consume some tokens
        assert!(limiter.try_acquire("test_task", 5).await.unwrap());

        // Reset
        limiter.reset("test_task").await.unwrap();

        // Should have full capacity again
        let available = limiter.available_tokens("test_task").await.unwrap();
        assert_eq!(available, 10);
    }

    // --- Regression tests for the non-atomic bucket (idx 164) ---

    /// The audit's headline scenario: a burst budget of exactly one permit
    /// contended by many concurrent callers must grant exactly one.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_burst_of_one_grants_exactly_one_permit() {
        let config = DistributedRateLimitConfig::default()
            .with_capacity(1)
            // Slow enough that no token can be credited while the test runs.
            .with_refill_rate(1e-9);
        let limiter = Arc::new(InMemoryDistributedRateLimiter::new(config));

        let mut handles = Vec::new();
        for _ in 0..64 {
            let limiter = Arc::clone(&limiter);
            handles.push(tokio::spawn(async move {
                limiter
                    .try_acquire("burst", 1)
                    .await
                    .expect("in-memory limiter never fails")
            }));
        }

        let mut granted = 0;
        for handle in handles {
            if handle.await.expect("task should not panic") {
                granted += 1;
            }
        }

        assert_eq!(
            granted, 1,
            "a one-permit bucket must grant exactly one permit under contention"
        );
        assert_eq!(limiter.available_tokens("burst").await.unwrap(), 0);

        let stats = limiter.get_stats("burst").await.unwrap();
        assert_eq!(stats.total_requests, 64);
        assert_eq!(stats.allowed_requests, 1);
        assert_eq!(stats.rejected_requests, 63);
    }

    /// Before the fix the consume step subtracted without re-checking the
    /// balance, so contention could underflow the `u64` token count.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_contention_never_underflows_the_balance() {
        let config = DistributedRateLimitConfig::default()
            .with_capacity(4)
            .with_refill_rate(1e-9);
        let limiter = Arc::new(InMemoryDistributedRateLimiter::new(config));

        let mut handles = Vec::new();
        for _ in 0..32 {
            let limiter = Arc::clone(&limiter);
            handles.push(tokio::spawn(async move {
                limiter.try_acquire("underflow", 2).await.unwrap_or(false)
            }));
        }

        let mut granted = 0;
        for handle in handles {
            if handle.await.expect("task should not panic") {
                granted += 1;
            }
        }

        assert_eq!(granted, 2, "capacity 4 with cost 2 must grant twice");
        assert_eq!(limiter.available_tokens("underflow").await.unwrap(), 0);
    }

    #[test]
    fn test_bucket_does_not_lose_fractional_refill() {
        // 1 token/second polled every 100ms: flooring each poll to whole
        // tokens *and* advancing the timestamp would credit nothing, ever.
        let mut bucket = TokenBucket {
            tokens: 0.0,
            last_refill: 0.0,
        };

        for step in 1..=10 {
            bucket.refill(10, 1.0, step as f64 * 0.1);
        }

        assert!(
            (bucket.tokens - 1.0).abs() < 1e-9,
            "expected ~1 token after 1s at 1 token/s, got {}",
            bucket.tokens
        );
    }

    #[test]
    fn test_bucket_caps_at_capacity_and_ignores_backwards_clock() {
        let mut bucket = TokenBucket {
            tokens: 0.0,
            last_refill: 100.0,
        };

        // A worker whose clock is behind must not mint tokens.
        bucket.refill(5, 10.0, 90.0);
        assert_eq!(bucket.tokens, 0.0);
        assert_eq!(bucket.last_refill, 100.0);

        // A long idle period saturates at capacity, never beyond.
        bucket.refill(5, 10.0, 1_000.0);
        assert_eq!(bucket.tokens, 5.0);
    }

    #[test]
    fn test_bucket_reports_retry_after_when_empty() {
        let mut bucket = TokenBucket {
            tokens: 0.0,
            last_refill: 0.0,
        };

        let outcome = bucket.try_consume(10, 2.0, 0.0, 1);
        assert!(!outcome.allowed);
        assert_eq!(outcome.remaining, 0);
        // 1 token at 2 tokens/second == 500ms.
        assert_eq!(outcome.retry_after, Duration::from_millis(500));

        // Zero refill rate means "never".
        let mut stalled = TokenBucket {
            tokens: 0.0,
            last_refill: 0.0,
        };
        assert_eq!(
            stalled.try_consume(10, 0.0, 0.0, 1).retry_after,
            Duration::MAX
        );
    }

    #[test]
    fn test_time_to_accumulate() {
        assert_eq!(time_to_accumulate(1, 5, 1.0), Duration::ZERO);
        assert_eq!(time_to_accumulate(4, 2, 1.0), Duration::from_secs(2));
        assert_eq!(time_to_accumulate(1, 0, 0.0), Duration::MAX);
    }

    /// Exercises the Lua script against a real server when one is offered
    /// through `CELERS_TEST_REDIS_URL`; skipped otherwise so the suite stays
    /// hermetic.
    ///
    /// The gate is the environment variable and *only* the environment
    /// variable. Once an operator has named a server, a connection failure is
    /// a real failure: treating it as a second skip condition is how this test
    /// used to report green against a Redis that was down, misconfigured or
    /// too old for the script — while the atomicity of the token-bucket Lua is
    /// the one thing it exists to prove.
    #[cfg(feature = "redis")]
    #[tokio::test]
    async fn test_redis_script_is_atomic_when_a_server_is_available() {
        let Ok(url) = std::env::var("CELERS_TEST_REDIS_URL") else {
            eprintln!(
                "SKIPPED: test_redis_script_is_atomic_when_a_server_is_available \
                 (set CELERS_TEST_REDIS_URL to run)"
            );
            return;
        };

        let prefix = format!("celers:test:rate_limit:{}", uuid::Uuid::new_v4());
        let config = DistributedRateLimitConfig::new(url)
            .with_capacity(1)
            .with_refill_rate(1e-9)
            .with_key_prefix(prefix);

        let limiter = DistributedRateLimiter::new(config)
            .await
            .expect("CELERS_TEST_REDIS_URL is set, so connecting to it must succeed");
        let limiter = Arc::new(limiter);

        let mut handles = Vec::new();
        for _ in 0..32 {
            let limiter = Arc::clone(&limiter);
            handles.push(tokio::spawn(async move {
                limiter.try_acquire("burst", 1).await.unwrap_or(false)
            }));
        }

        let mut granted = 0;
        for handle in handles {
            if handle.await.expect("task should not panic") {
                granted += 1;
            }
        }
        assert_eq!(granted, 1);

        limiter.reset("burst").await.expect("reset should succeed");
    }
}
