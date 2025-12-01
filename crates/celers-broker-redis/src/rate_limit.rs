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
use redis::AsyncCommands;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant};

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
        let mut last_refill = self.last_refill.write().unwrap();
        let elapsed = now.duration_since(*last_refill);
        *last_refill = now;

        let tokens_to_add = elapsed.as_secs_f64() * self.config.rate;
        let mut tokens = self.tokens.write().unwrap();
        *tokens = (*tokens + tokens_to_add).min(self.config.burst as f64);
    }

    /// Try to acquire a permit
    pub fn try_acquire(&self) -> bool {
        self.refill();

        let mut tokens = self.tokens.write().unwrap();
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

        let mut tokens = self.tokens.write().unwrap();
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

        let tokens = self.tokens.read().unwrap();
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
        *self.tokens.read().unwrap() as u32
    }

    /// Reset the limiter to full capacity
    pub fn reset(&self) {
        *self.tokens.write().unwrap() = self.config.burst as f64;
        *self.last_refill.write().unwrap() = Instant::now();
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
    config: QueueRateLimitConfig,
    queue_name: String,
}

impl DistributedRateLimiter {
    /// Create a new distributed rate limiter
    pub fn new(client: redis::Client, queue_name: &str, config: QueueRateLimitConfig) -> Self {
        Self {
            client,
            config,
            queue_name: queue_name.to_string(),
        }
    }

    fn rate_limit_key(&self) -> String {
        format!("{}:{}:rate", self.config.key_prefix, self.queue_name)
    }

    /// Try to acquire a permit using Redis
    pub async fn try_acquire(&self) -> Result<bool> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to connect: {}", e)))?;

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
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to connect: {}", e)))?;

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
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to connect: {}", e)))?;

        conn.del::<_, ()>(self.rate_limit_key())
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
const SLIDING_WINDOW_SCRIPT: &str = r#"
local key = KEYS[1]
local window_start = tonumber(ARGV[1])
local now = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])

-- Remove old entries outside the window
redis.call('ZREMRANGEBYSCORE', key, '-inf', window_start)

-- Count current entries
local count = redis.call('ZCARD', key)

if count < limit then
    -- Add the new request
    redis.call('ZADD', key, now, now .. ':' .. math.random(1000000))
    -- Set TTL to auto-cleanup
    redis.call('EXPIRE', key, ttl + 1)
    return 1
else
    return 0
end
"#;

/// Queue rate limiter that can use local or distributed limiting
pub enum QueueRateLimiter {
    /// Local token bucket limiter
    Local(TokenBucketLimiter),
    /// Redis-backed distributed limiter
    Distributed(DistributedRateLimiter),
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
        QueueRateLimiter::Distributed(DistributedRateLimiter::new(client, queue_name, config))
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
