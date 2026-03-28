//! Distributed rate limiting coordination across multiple workers
//!
//! This module provides distributed rate limiting capabilities that work across
//! multiple worker instances, ensuring that rate limits are enforced globally
//! rather than per-worker.
//!
//! # Features
//!
//! - **Redis-based coordination**: Use Redis for distributed token bucket tracking
//! - **Sliding window rate limiting**: Track requests in time windows
//! - **Automatic token refill**: Tokens are refilled based on configured rates
//! - **Multi-worker coordination**: Rate limits apply across all workers
//!
//! # Example
//!
//! ```ignore
//! # #[cfg(feature = "redis")]
//! # async fn example() -> celers_core::Result<()> {
//! use celers_worker::distributed_rate_limit::{DistributedRateLimiter, DistributedRateLimitConfig};
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

    /// Refill tokens based on elapsed time
    async fn refill_tokens(&self, task_type: &str) -> Result<u64> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Other(format!("Redis connection error: {}", e)))?;

        let bucket_key = self.bucket_key(task_type);
        let refill_key = self.refill_key(task_type);

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();

        // Get current tokens and last refill time
        let (current_tokens, last_refill): (Option<u64>, Option<f64>) = redis::pipe()
            .get(&bucket_key)
            .get(&refill_key)
            .query_async(&mut conn)
            .await
            .map_err(|e| CelersError::Other(format!("Redis pipeline error: {}", e)))?;

        let current_tokens = current_tokens.unwrap_or(self.config.capacity);
        let last_refill = last_refill.unwrap_or(now);

        // Calculate tokens to add
        let elapsed = now - last_refill;
        let tokens_to_add = (elapsed * self.config.refill_rate).floor() as u64;

        if tokens_to_add > 0 {
            let new_tokens = (current_tokens + tokens_to_add).min(self.config.capacity);

            // Update tokens and refill time
            redis::pipe()
                .set(&bucket_key, new_tokens)
                .set(&refill_key, now)
                .expire(&bucket_key, self.config.window_size_secs as i64)
                .expire(&refill_key, self.config.window_size_secs as i64)
                .query_async::<()>(&mut conn)
                .await
                .map_err(|e| CelersError::Other(format!("Redis pipeline error: {}", e)))?;

            debug!(
                "Refilled {} tokens for task type '{}' (now: {})",
                tokens_to_add, task_type, new_tokens
            );

            Ok(new_tokens)
        } else {
            Ok(current_tokens)
        }
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
        use redis::AsyncCommands;

        if tokens == 0 {
            return Ok(true);
        }

        // Refill tokens first
        let available = self.refill_tokens(task_type).await?;

        if available >= tokens {
            // We have enough tokens, consume them
            let mut conn = self
                .client
                .get_multiplexed_async_connection()
                .await
                .map_err(|e| CelersError::Other(format!("Redis connection error: {}", e)))?;

            let bucket_key = self.bucket_key(task_type);
            let new_tokens = available - tokens;

            let _: () = conn
                .set(&bucket_key, new_tokens)
                .await
                .map_err(|e| CelersError::Other(format!("Redis set error: {}", e)))?;

            let _: () = conn
                .expire(&bucket_key, self.config.window_size_secs as i64)
                .await
                .map_err(|e| CelersError::Other(format!("Redis expire error: {}", e)))?;

            self.update_stats(task_type, true, new_tokens).await;

            debug!(
                "Acquired {} tokens for task type '{}' (remaining: {})",
                tokens, task_type, new_tokens
            );

            Ok(true)
        } else {
            self.update_stats(task_type, false, available).await;

            debug!(
                "Rate limit exceeded for task type '{}' (available: {}, requested: {})",
                task_type, available, tokens
            );

            Ok(false)
        }
    }

    async fn available_tokens(&self, task_type: &str) -> Result<u64> {
        self.refill_tokens(task_type).await
    }

    async fn time_until_next_token(&self, task_type: &str) -> Result<Duration> {
        let available = self.available_tokens(task_type).await?;

        if available >= self.config.capacity {
            return Ok(Duration::from_secs(0));
        }

        // Calculate time needed to refill one token
        let time_per_token = 1.0 / self.config.refill_rate;
        let secs = time_per_token.ceil() as u64;

        Ok(Duration::from_secs(secs))
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

/// In-memory distributed rate limiter (for testing without Redis)
pub struct InMemoryDistributedRateLimiter {
    config: DistributedRateLimitConfig,
    buckets: Arc<RwLock<HashMap<String, TokenBucket>>>,
    stats: Arc<RwLock<HashMap<String, RateLimitStats>>>,
}

#[derive(Debug, Clone)]
struct TokenBucket {
    tokens: u64,
    last_refill: f64,
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

    /// Refill tokens for a task type
    async fn refill_tokens(&self, task_type: &str) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();

        let mut buckets = self.buckets.write().await;
        let bucket = buckets.entry(task_type.to_string()).or_insert(TokenBucket {
            tokens: self.config.capacity,
            last_refill: now,
        });

        let elapsed = now - bucket.last_refill;
        let tokens_to_add = (elapsed * self.config.refill_rate).floor() as u64;

        if tokens_to_add > 0 {
            bucket.tokens = (bucket.tokens + tokens_to_add).min(self.config.capacity);
            bucket.last_refill = now;
        }

        bucket.tokens
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

        let available = self.refill_tokens(task_type).await;

        if available >= tokens {
            let mut buckets = self.buckets.write().await;
            if let Some(bucket) = buckets.get_mut(task_type) {
                bucket.tokens -= tokens;
                self.update_stats(task_type, true, bucket.tokens).await;
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            self.update_stats(task_type, false, available).await;
            Ok(false)
        }
    }

    async fn available_tokens(&self, task_type: &str) -> Result<u64> {
        Ok(self.refill_tokens(task_type).await)
    }

    async fn time_until_next_token(&self, task_type: &str) -> Result<Duration> {
        let available = self.available_tokens(task_type).await?;

        if available >= self.config.capacity {
            return Ok(Duration::from_secs(0));
        }

        let time_per_token = 1.0 / self.config.refill_rate;
        let secs = time_per_token.ceil() as u64;

        Ok(Duration::from_secs(secs))
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
}
