//! Queue Rate Limiting
//!
//! Provides rate limiting for Redis queue operations to prevent overwhelming
//! Redis or downstream systems.
//!
//! Supports:
//! - **Token Bucket**: Local rate limiting with burst capacity
//! - **Sliding Window**: Track actual operations within time windows
//! - **Distributed**: Redis-backed rate limiting across multiple workers
//!
//! # Example
//!
//! ```rust,ignore
//! use celers_broker_redis::rate_limit::{QueueRateLimiter, QueueRateLimitConfig};
//! use std::time::Duration;
//!
//! // Create a rate limiter allowing 100 tasks per second
//! let config = QueueRateLimitConfig::new(100.0)
//!     .with_burst(150);
//!
//! let limiter = QueueRateLimiter::new(config);
//!
//! // Before enqueuing a task
//! if limiter.try_acquire() {
//!     broker.enqueue(task).await?;
//! } else {
//!     // Rate limited, wait or reject
//! }
//! ```

use celers_core::{CelersError, Result};
use redis::{aio::ConnectionManager, AsyncCommands};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};
use tokio::sync::OnceCell;

/// Acquire a read guard, recovering it even if the lock is poisoned.
///
/// A rate limiter exists to keep a system from falling over; turning it into
/// a landmine that panics every caller because some *unrelated* thread
/// panicked while holding this lock defeats the point. Every write through
/// this module is a plain assignment of a fully-formed value (`*guard =
/// ...`), never a multi-step mutation that could be observed half-applied,
/// so a value recovered from a poisoned lock is always structurally valid.
fn read_recover<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Write-lock counterpart of [`read_recover`]; see its documentation.
fn write_recover<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Configuration for queue rate limiting
#[derive(Debug, Clone)]
pub struct QueueRateLimitConfig {
    /// Maximum operations per second
    pub rate: f64,
    /// Burst capacity (max tokens in bucket)
    pub burst: u32,
    /// Whether to use distributed (Redis-backed) rate limiting
    pub distributed: bool,
    /// Key prefix for distributed rate limiting
    pub key_prefix: String,
    /// Window size for sliding window (in milliseconds)
    pub window_ms: u64,
}

impl QueueRateLimitConfig {
    /// Create a new configuration with the given rate
    pub fn new(rate: f64) -> Self {
        Self {
            rate,
            burst: rate.ceil() as u32,
            distributed: false,
            key_prefix: "ratelimit".to_string(),
            window_ms: 1000,
        }
    }

    /// Set the burst capacity
    pub fn with_burst(mut self, burst: u32) -> Self {
        self.burst = burst;
        self
    }

    /// Enable distributed rate limiting
    pub fn with_distributed(mut self, key_prefix: &str) -> Self {
        self.distributed = true;
        self.key_prefix = key_prefix.to_string();
        self
    }

    /// Set the sliding window size
    pub fn with_window(mut self, window: Duration) -> Self {
        self.window_ms = window.as_millis() as u64;
        self
    }
}

impl Default for QueueRateLimitConfig {
    fn default() -> Self {
        Self::new(100.0)
    }
}

/// Token bucket rate limiter for local (in-process) rate limiting
pub struct TokenBucketLimiter {
    config: QueueRateLimitConfig,
    tokens: RwLock<f64>,
    last_refill: RwLock<Instant>,
}

impl TokenBucketLimiter {
    /// Create a new token bucket limiter
    pub fn new(config: QueueRateLimitConfig) -> Self {
        Self {
            tokens: RwLock::new(config.burst as f64),
            last_refill: RwLock::new(Instant::now()),
            config,
        }
    }

    fn refill(&self) {
        let now = Instant::now();
        let mut last_refill = write_recover(&self.last_refill);
        let elapsed = now.duration_since(*last_refill);
        *last_refill = now;

        let tokens_to_add = elapsed.as_secs_f64() * self.config.rate;
        let mut tokens = write_recover(&self.tokens);
        *tokens = (*tokens + tokens_to_add).min(self.config.burst as f64);
    }

    /// Try to acquire a permit
    pub fn try_acquire(&self) -> bool {
        self.refill();

        let mut tokens = write_recover(&self.tokens);
        if *tokens >= 1.0 {
            *tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// Try to acquire multiple permits
    pub fn try_acquire_n(&self, n: u32) -> bool {
        self.refill();

        let mut tokens = write_recover(&self.tokens);
        if *tokens >= n as f64 {
            *tokens -= n as f64;
            true
        } else {
            false
        }
    }

    /// Get time until a permit will be available
    pub fn time_until_available(&self) -> Duration {
        self.refill();

        let tokens = read_recover(&self.tokens);
        if *tokens >= 1.0 {
            Duration::ZERO
        } else {
            let needed = 1.0 - *tokens;
            Duration::from_secs_f64(needed / self.config.rate)
        }
    }

    /// Get the current number of available permits
    pub fn available_permits(&self) -> u32 {
        self.refill();
        *read_recover(&self.tokens) as u32
    }

    /// Reset the limiter to full capacity
    pub fn reset(&self) {
        *write_recover(&self.tokens) = self.config.burst as f64;
        *write_recover(&self.last_refill) = Instant::now();
    }

    /// Get the configuration
    pub fn config(&self) -> &QueueRateLimitConfig {
        &self.config
    }
}

/// Distributed rate limiter using Redis
///
/// Uses a sliding window algorithm stored in Redis to provide
/// rate limiting across multiple workers.
pub struct DistributedRateLimiter {
    client: redis::Client,
    /// Long-lived multiplexed connection, shared by every call.
    conn: OnceCell<ConnectionManager>,
    config: QueueRateLimitConfig,
    queue_name: String,
}

impl DistributedRateLimiter {
    /// Create a new distributed rate limiter
    pub fn new(client: redis::Client, queue_name: &str, config: QueueRateLimitConfig) -> Self {
        Self {
            client,
            conn: OnceCell::new(),
            config,
            queue_name: queue_name.to_string(),
        }
    }

    /// Reuse an existing connection manager instead of opening one lazily.
    pub fn with_connection_manager(self, manager: ConnectionManager) -> Self {
        let conn = OnceCell::new();
        // Only fails if the cell is already initialised, which it is not.
        let _ = conn.set(manager);
        Self { conn, ..self }
    }

    /// Get the shared connection, establishing it on first use.
    async fn connection(&self) -> Result<ConnectionManager> {
        self.conn
            .get_or_try_init(|| async {
                self.client
                    .get_connection_manager()
                    .await
                    .map_err(|e| CelersError::Broker(format!("Failed to connect: {}", e)))
            })
            .await
            .cloned()
    }

    fn rate_limit_key(&self) -> String {
        format!("{}:{}:rate", self.config.key_prefix, self.queue_name)
    }

    /// The counter key backing unique sliding-window members.
    fn sequence_key(&self) -> String {
        format!("{}:seq", self.rate_limit_key())
    }

    /// Try to acquire a permit using Redis
    pub async fn try_acquire(&self) -> Result<bool> {
        let mut conn = self.connection().await?;

        let key = self.rate_limit_key();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| CelersError::Other(format!("Time error: {}", e)))?
            .as_millis() as i64;

        let window_start = now_ms - self.config.window_ms as i64;

        // Use a Lua script for atomic rate limiting
        let script = redis::Script::new(SLIDING_WINDOW_SCRIPT);

        let result: i64 = script
            .key(&key)
            .key(self.sequence_key())
            .arg(window_start)
            .arg(now_ms)
            .arg(self.config.rate as i64)
            .arg(self.config.window_ms as i64 / 1000) // TTL in seconds
            .invoke_async(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Rate limit check failed: {}", e)))?;

        Ok(result == 1)
    }

    /// Get the current request count in the window
    pub async fn current_count(&self) -> Result<u64> {
        let mut conn = self.connection().await?;

        let key = self.rate_limit_key();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| CelersError::Other(format!("Time error: {}", e)))?
            .as_millis() as i64;

        let window_start = now_ms - self.config.window_ms as i64;

        // Remove old entries and count
        redis::cmd("ZREMRANGEBYSCORE")
            .arg(&key)
            .arg("-inf")
            .arg(window_start)
            .query_async::<()>(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to clean window: {}", e)))?;

        let count: u64 = conn
            .zcard(&key)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get count: {}", e)))?;

        Ok(count)
    }

    /// Reset the rate limiter
    pub async fn reset(&self) -> Result<()> {
        let mut conn = self.connection().await?;

        conn.del::<_, ()>(&[self.rate_limit_key(), self.sequence_key()])
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to reset: {}", e)))?;

        Ok(())
    }

    /// Get the configuration
    pub fn config(&self) -> &QueueRateLimitConfig {
        &self.config
    }
}

/// Lua script for atomic sliding window rate limiting
///
/// Each admitted request is recorded as a distinct sorted-set member. The
/// member must be *provably* unique: `ZADD` treats a repeated member as an
/// update rather than an insertion, so two admissions that collapse onto one
/// member make `ZCARD` undercount and the limiter admits more than `limit` for
/// that window. A millisecond timestamp plus `math.random` is only
/// probabilistically unique — and Redis's Lua PRNG seeding is a
/// version-dependent implementation detail — so the discriminator here is an
/// `INCR` counter, which is exact under any seeding behaviour and across every
/// client. The counter shares the window's TTL so it is reclaimed with it.
///
/// `KEYS[1]`: sliding window sorted set
/// `KEYS[2]`: sequence counter key
/// `ARGV[1]`: window start (epoch ms)
/// `ARGV[2]`: now (epoch ms)
/// `ARGV[3]`: limit
/// `ARGV[4]`: TTL in seconds
const SLIDING_WINDOW_SCRIPT: &str = r#"
local key = KEYS[1]
local seq_key = KEYS[2]
local window_start = tonumber(ARGV[1])
local now = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

-- Remove old entries outside the window
redis.call('ZREMRANGEBYSCORE', key, '-inf', window_start)

-- Count current entries
local count = redis.call('ZCARD', key)

if count < limit then
    -- Add the new request under a provably unique member
    local seq = redis.call('INCR', seq_key)
    redis.call('ZADD', key, now, now .. ':' .. seq)
    -- Set TTL to auto-cleanup
    redis.call('EXPIRE', key, ttl + 1)
    redis.call('EXPIRE', seq_key, ttl + 1)
    return 1
else
    return 0
end
"#;

/// Queue rate limiter that can use local or distributed limiting
pub enum QueueRateLimiter {
    /// Local token bucket limiter
    Local(TokenBucketLimiter),
    /// Redis-backed distributed limiter.
    ///
    /// Boxed because [`DistributedRateLimiter`] owns a [`redis::Client`], whose
    /// `ConnectionAddr::TcpTls` variant carries the rustls trust store and
    /// client-certificate chain now that the `tls-rustls` feature is enabled.
    /// That makes it roughly three times the size of the `Local` variant, so an
    /// unboxed enum would pay 352 bytes for every purely local limiter.
    Distributed(Box<DistributedRateLimiter>),
}

impl QueueRateLimiter {
    /// Create a local (in-process) rate limiter
    pub fn local(config: QueueRateLimitConfig) -> Self {
        QueueRateLimiter::Local(TokenBucketLimiter::new(config))
    }

    /// Create a distributed (Redis-backed) rate limiter
    pub fn distributed(
        client: redis::Client,
        queue_name: &str,
        config: QueueRateLimitConfig,
    ) -> Self {
        QueueRateLimiter::Distributed(Box::new(DistributedRateLimiter::new(
            client, queue_name, config,
        )))
    }

    /// Try to acquire a permit (sync for local, async stub for distributed)
    pub fn try_acquire_local(&self) -> Option<bool> {
        match self {
            QueueRateLimiter::Local(limiter) => Some(limiter.try_acquire()),
            QueueRateLimiter::Distributed(_) => None, // Use try_acquire_async instead
        }
    }

    /// Try to acquire a permit (async, works for both)
    pub async fn try_acquire_async(&self) -> Result<bool> {
        match self {
            QueueRateLimiter::Local(limiter) => Ok(limiter.try_acquire()),
            QueueRateLimiter::Distributed(limiter) => limiter.try_acquire().await,
        }
    }

    /// Check if this is a distributed limiter
    pub fn is_distributed(&self) -> bool {
        matches!(self, QueueRateLimiter::Distributed(_))
    }
}

/// Rate limiter statistics
#[derive(Debug, Clone, Default)]
pub struct RateLimiterStats {
    /// Total requests allowed
    pub allowed: u64,
    /// Total requests rejected
    pub rejected: u64,
    /// Current available permits (local only)
    pub available_permits: Option<u32>,
}

/// Wrapper for tracking rate limiter statistics
pub struct TrackedRateLimiter {
    limiter: TokenBucketLimiter,
    allowed: AtomicU64,
    rejected: AtomicU64,
}

impl TrackedRateLimiter {
    /// Create a new tracked rate limiter
    pub fn new(config: QueueRateLimitConfig) -> Self {
        Self {
            limiter: TokenBucketLimiter::new(config),
            allowed: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
        }
    }

    /// Try to acquire a permit
    pub fn try_acquire(&self) -> bool {
        if self.limiter.try_acquire() {
            self.allowed.fetch_add(1, Ordering::SeqCst);
            true
        } else {
            self.rejected.fetch_add(1, Ordering::SeqCst);
            false
        }
    }

    /// Get statistics
    pub fn stats(&self) -> RateLimiterStats {
        RateLimiterStats {
            allowed: self.allowed.load(Ordering::SeqCst),
            rejected: self.rejected.load(Ordering::SeqCst),
            available_permits: Some(self.limiter.available_permits()),
        }
    }

    /// Reset the limiter and statistics
    pub fn reset(&self) {
        self.limiter.reset();
        self.allowed.store(0, Ordering::SeqCst);
        self.rejected.store(0, Ordering::SeqCst);
    }

    /// Get the configuration
    pub fn config(&self) -> &QueueRateLimitConfig {
        self.limiter.config()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every admitted request must land on its own sorted-set member. If two
    /// admissions collapse onto one member (as they can when the member is
    /// `timestamp:random`), `ZCARD` undercounts and the window admits more
    /// than `limit` requests.
    #[tokio::test]
    async fn test_sliding_window_members_are_unique_within_a_millisecond() {
        let client = redis::Client::open("redis://127.0.0.1:6379").expect("client");
        let queue = format!("test-rate-{}", uuid::Uuid::new_v4());
        let limit = 50u32;
        let config = QueueRateLimitConfig::new(f64::from(limit))
            .with_distributed("test-ratelimit")
            .with_window(Duration::from_secs(30));
        let limiter = DistributedRateLimiter::new(client, &queue, config);

        // These run back to back, so many share a millisecond timestamp.
        let mut admitted = 0u32;
        for _ in 0..limit {
            if limiter.try_acquire().await.expect("acquire") {
                admitted += 1;
            }
        }
        assert_eq!(admitted, limit, "the whole budget must be admitted");

        assert_eq!(
            limiter.current_count().await.expect("count"),
            u64::from(limit),
            "each admitted request must occupy its own window entry"
        );

        assert!(
            !limiter.try_acquire().await.expect("acquire past limit"),
            "the window budget is exhausted"
        );

        limiter.reset().await.expect("reset");
        assert_eq!(limiter.current_count().await.expect("count"), 0);
    }

    #[test]
    fn test_config_builder() {
        let config = QueueRateLimitConfig::new(50.0)
            .with_burst(100)
            .with_window(Duration::from_secs(5));

        assert_eq!(config.rate, 50.0);
        assert_eq!(config.burst, 100);
        assert_eq!(config.window_ms, 5000);
    }

    #[test]
    fn test_token_bucket_allows_burst() {
        let config = QueueRateLimitConfig::new(10.0).with_burst(5);
        let limiter = TokenBucketLimiter::new(config);

        // Should allow up to burst capacity
        for _ in 0..5 {
            assert!(limiter.try_acquire());
        }

        // Should be rate limited now
        assert!(!limiter.try_acquire());
    }

    #[test]
    fn test_token_bucket_refills() {
        let config = QueueRateLimitConfig::new(1000.0).with_burst(10);
        let limiter = TokenBucketLimiter::new(config);

        // Drain the bucket
        for _ in 0..10 {
            limiter.try_acquire();
        }
        assert!(!limiter.try_acquire());

        // Wait for refill (1000/sec = 1ms per token)
        std::thread::sleep(Duration::from_millis(15));

        // Should have some tokens now
        assert!(limiter.try_acquire());
    }

    #[test]
    fn test_token_bucket_available_permits() {
        let config = QueueRateLimitConfig::new(10.0).with_burst(5);
        let limiter = TokenBucketLimiter::new(config);

        assert_eq!(limiter.available_permits(), 5);

        limiter.try_acquire();
        limiter.try_acquire();

        assert_eq!(limiter.available_permits(), 3);
    }

    #[test]
    fn test_token_bucket_reset() {
        let config = QueueRateLimitConfig::new(10.0).with_burst(5);
        let limiter = TokenBucketLimiter::new(config);

        // Drain
        for _ in 0..5 {
            limiter.try_acquire();
        }
        assert_eq!(limiter.available_permits(), 0);

        // Reset
        limiter.reset();
        assert_eq!(limiter.available_permits(), 5);
    }

    #[test]
    fn test_token_bucket_time_until_available() {
        let config = QueueRateLimitConfig::new(10.0).with_burst(1);
        let limiter = TokenBucketLimiter::new(config);

        limiter.try_acquire();

        let wait_time = limiter.time_until_available();
        // Should be roughly 100ms (1 token / 10 per second)
        assert!(wait_time.as_millis() > 0);
        assert!(wait_time.as_millis() <= 150);
    }

    #[test]
    fn test_tracked_rate_limiter() {
        let config = QueueRateLimitConfig::new(10.0).with_burst(3);
        let limiter = TrackedRateLimiter::new(config);

        assert!(limiter.try_acquire());
        assert!(limiter.try_acquire());
        assert!(limiter.try_acquire());
        assert!(!limiter.try_acquire());

        let stats = limiter.stats();
        assert_eq!(stats.allowed, 3);
        assert_eq!(stats.rejected, 1);
    }

    #[test]
    fn test_queue_rate_limiter_local() {
        let config = QueueRateLimitConfig::new(10.0).with_burst(2);
        let limiter = QueueRateLimiter::local(config);

        assert!(!limiter.is_distributed());
        assert_eq!(limiter.try_acquire_local(), Some(true));
        assert_eq!(limiter.try_acquire_local(), Some(true));
        assert_eq!(limiter.try_acquire_local(), Some(false));
    }

    #[test]
    fn test_try_acquire_n() {
        let config = QueueRateLimitConfig::new(10.0).with_burst(5);
        let limiter = TokenBucketLimiter::new(config);

        assert!(limiter.try_acquire_n(3));
        assert!(limiter.try_acquire_n(2));
        assert!(!limiter.try_acquire_n(1));
    }
}
