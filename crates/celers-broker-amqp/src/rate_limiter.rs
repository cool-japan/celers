//! Rate limiting for AMQP message publishing
//!
//! This module provides rate limiting mechanisms to control the rate of message
//! publishing, preventing broker overload and ensuring fair resource usage.
//!
//! # Rate Limiting Algorithms
//!
//! - **Token Bucket**: Allows bursts up to bucket capacity, refills at fixed rate
//! - **Leaky Bucket**: Smooths traffic by processing at a constant rate
//!
//! # Examples
//!
//! ```
//! use celers_broker_amqp::rate_limiter::{RateLimiter, TokenBucket};
//! use std::time::Duration;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create a token bucket rate limiter: 100 tokens/sec, max 200 tokens
//! let limiter = RateLimiter::token_bucket(100.0, 200);
//!
//! // Try to acquire tokens before publishing
//! if limiter.try_acquire(10).await {
//!     println!("Tokens acquired, proceed with publishing");
//! } else {
//!     println!("Rate limit exceeded, need to wait");
//! }
//!
//! // Wait for tokens to be available
//! limiter.acquire(5).await;
//! println!("Acquired 5 tokens");
//! # Ok(())
//! # }
//! ```

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Rate limiting strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RateLimitStrategy {
    /// Token bucket algorithm - allows bursts
    TokenBucket,
    /// Leaky bucket algorithm - smooth rate
    LeakyBucket,
}

impl std::fmt::Display for RateLimitStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RateLimitStrategy::TokenBucket => write!(f, "token_bucket"),
            RateLimitStrategy::LeakyBucket => write!(f, "leaky_bucket"),
        }
    }
}

/// Token bucket rate limiter configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenBucketConfig {
    /// Tokens generated per second
    pub rate: f64,
    /// Maximum bucket capacity
    pub capacity: usize,
    /// Initial tokens in bucket
    pub initial_tokens: usize,
}

impl Default for TokenBucketConfig {
    fn default() -> Self {
        Self {
            rate: 100.0,
            capacity: 100,
            initial_tokens: 100,
        }
    }
}

/// Token bucket rate limiter
///
/// Allows bursts up to capacity, refills at a fixed rate.
/// Good for handling bursty traffic while maintaining average rate.
#[derive(Debug, Clone)]
pub struct TokenBucket {
    config: TokenBucketConfig,
    state: Arc<Mutex<TokenBucketState>>,
}

#[derive(Debug)]
struct TokenBucketState {
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    /// Create a new token bucket rate limiter
    ///
    /// # Arguments
    ///
    /// * `rate` - Tokens per second
    /// * `capacity` - Maximum bucket capacity
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_broker_amqp::rate_limiter::TokenBucket;
    ///
    /// // 100 tokens/sec, max 200 tokens (allows 2 second burst)
    /// let limiter = TokenBucket::new(100.0, 200);
    /// ```
    pub fn new(rate: f64, capacity: usize) -> Self {
        Self::with_config(TokenBucketConfig {
            rate,
            capacity,
            initial_tokens: capacity,
        })
    }

    /// Create with custom configuration
    pub fn with_config(config: TokenBucketConfig) -> Self {
        let initial_tokens = config.initial_tokens.min(config.capacity) as f64;
        Self {
            config,
            state: Arc::new(Mutex::new(TokenBucketState {
                tokens: initial_tokens,
                last_refill: Instant::now(),
            })),
        }
    }

    /// Try to acquire tokens without blocking
    ///
    /// Returns `true` if tokens were acquired, `false` if insufficient tokens
    pub async fn try_acquire(&self, tokens: usize) -> bool {
        let mut state = self.state.lock().await;
        self.refill(&mut state);

        if state.tokens >= tokens as f64 {
            state.tokens -= tokens as f64;
            true
        } else {
            false
        }
    }

    /// Acquire tokens, waiting if necessary
    ///
    /// Blocks until sufficient tokens are available
    pub async fn acquire(&self, tokens: usize) {
        loop {
            let wait_time = {
                let mut state = self.state.lock().await;
                self.refill(&mut state);

                if state.tokens >= tokens as f64 {
                    state.tokens -= tokens as f64;
                    return;
                }

                // Calculate wait time for needed tokens
                let needed = tokens as f64 - state.tokens;
                let wait_seconds = needed / self.config.rate;
                Duration::from_secs_f64(wait_seconds)
            };

            tokio::time::sleep(wait_time).await;
        }
    }

    /// Get current token count
    pub async fn available_tokens(&self) -> usize {
        let mut state = self.state.lock().await;
        self.refill(&mut state);
        state.tokens.floor() as usize
    }

    /// Refill tokens based on elapsed time
    fn refill(&self, state: &mut TokenBucketState) {
        let now = Instant::now();
        let elapsed = now.duration_since(state.last_refill).as_secs_f64();
        let new_tokens = elapsed * self.config.rate;

        state.tokens = (state.tokens + new_tokens).min(self.config.capacity as f64);
        state.last_refill = now;
    }

    /// Reset the bucket to initial state
    pub async fn reset(&self) {
        let mut state = self.state.lock().await;
        state.tokens = self.config.initial_tokens as f64;
        state.last_refill = Instant::now();
    }

    /// Get rate limiter statistics
    pub async fn statistics(&self) -> TokenBucketStats {
        let state = self.state.lock().await;
        TokenBucketStats {
            available_tokens: state.tokens.floor() as usize,
            capacity: self.config.capacity,
            rate: self.config.rate,
            utilization: 1.0 - (state.tokens / self.config.capacity as f64),
        }
    }
}

/// Token bucket statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenBucketStats {
    /// Current available tokens
    pub available_tokens: usize,
    /// Maximum capacity
    pub capacity: usize,
    /// Refill rate (tokens/sec)
    pub rate: f64,
    /// Bucket utilization (0.0-1.0)
    pub utilization: f64,
}

/// Leaky bucket rate limiter configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeakyBucketConfig {
    /// Leak rate (items per second)
    pub rate: f64,
    /// Maximum queue size
    pub capacity: usize,
}

impl Default for LeakyBucketConfig {
    fn default() -> Self {
        Self {
            rate: 100.0,
            capacity: 1000,
        }
    }
}

/// Leaky bucket rate limiter
///
/// Processes items at a constant rate, queuing excess items.
/// Good for smoothing traffic and preventing bursts.
#[derive(Debug, Clone)]
pub struct LeakyBucket {
    config: LeakyBucketConfig,
    state: Arc<Mutex<LeakyBucketState>>,
}

#[derive(Debug)]
struct LeakyBucketState {
    queue_size: usize,
    last_leak: Instant,
}

impl LeakyBucket {
    /// Create a new leaky bucket rate limiter
    ///
    /// # Arguments
    ///
    /// * `rate` - Items processed per second
    /// * `capacity` - Maximum queue size
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_broker_amqp::rate_limiter::LeakyBucket;
    ///
    /// // Process 50 items/sec, max queue 500 items
    /// let limiter = LeakyBucket::new(50.0, 500);
    /// ```
    pub fn new(rate: f64, capacity: usize) -> Self {
        Self::with_config(LeakyBucketConfig { rate, capacity })
    }

    /// Create with custom configuration
    pub fn with_config(config: LeakyBucketConfig) -> Self {
        Self {
            config,
            state: Arc::new(Mutex::new(LeakyBucketState {
                queue_size: 0,
                last_leak: Instant::now(),
            })),
        }
    }

    /// Try to add items to the bucket without blocking
    ///
    /// Returns `true` if items were added, `false` if bucket is full
    pub async fn try_add(&self, count: usize) -> bool {
        let mut state = self.state.lock().await;
        self.leak(&mut state);

        if state.queue_size + count <= self.config.capacity {
            state.queue_size += count;
            true
        } else {
            false
        }
    }

    /// Add items to the bucket, waiting if necessary
    ///
    /// Blocks until items can be added to the queue
    pub async fn add(&self, count: usize) {
        loop {
            let wait_time = {
                let mut state = self.state.lock().await;
                self.leak(&mut state);

                if state.queue_size + count <= self.config.capacity {
                    state.queue_size += count;
                    return;
                }

                // Calculate wait time for space to become available
                let needed_space = (state.queue_size + count) - self.config.capacity;
                let wait_seconds = needed_space as f64 / self.config.rate;
                Duration::from_secs_f64(wait_seconds)
            };

            tokio::time::sleep(wait_time).await;
        }
    }

    /// Get current queue size
    pub async fn queue_size(&self) -> usize {
        let mut state = self.state.lock().await;
        self.leak(&mut state);
        state.queue_size
    }

    /// Leak items based on elapsed time
    fn leak(&self, state: &mut LeakyBucketState) {
        let now = Instant::now();
        let elapsed = now.duration_since(state.last_leak).as_secs_f64();
        let leaked = (elapsed * self.config.rate).floor() as usize;

        state.queue_size = state.queue_size.saturating_sub(leaked);
        state.last_leak = now;
    }

    /// Reset the bucket to empty state
    pub async fn reset(&self) {
        let mut state = self.state.lock().await;
        state.queue_size = 0;
        state.last_leak = Instant::now();
    }

    /// Get rate limiter statistics
    pub async fn statistics(&self) -> LeakyBucketStats {
        let state = self.state.lock().await;
        LeakyBucketStats {
            queue_size: state.queue_size,
            capacity: self.config.capacity,
            rate: self.config.rate,
            utilization: state.queue_size as f64 / self.config.capacity as f64,
        }
    }
}

/// Leaky bucket statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeakyBucketStats {
    /// Current queue size
    pub queue_size: usize,
    /// Maximum capacity
    pub capacity: usize,
    /// Leak rate (items/sec)
    pub rate: f64,
    /// Queue utilization (0.0-1.0)
    pub utilization: f64,
}

/// Generic rate limiter that can use different strategies
#[derive(Debug, Clone)]
pub enum RateLimiter {
    /// Token bucket strategy
    TokenBucket(TokenBucket),
    /// Leaky bucket strategy
    LeakyBucket(LeakyBucket),
}

impl RateLimiter {
    /// Create a token bucket rate limiter
    pub fn token_bucket(rate: f64, capacity: usize) -> Self {
        RateLimiter::TokenBucket(TokenBucket::new(rate, capacity))
    }

    /// Create a leaky bucket rate limiter
    pub fn leaky_bucket(rate: f64, capacity: usize) -> Self {
        RateLimiter::LeakyBucket(LeakyBucket::new(rate, capacity))
    }

    /// Try to acquire/add items without blocking
    pub async fn try_acquire(&self, count: usize) -> bool {
        match self {
            RateLimiter::TokenBucket(bucket) => bucket.try_acquire(count).await,
            RateLimiter::LeakyBucket(bucket) => bucket.try_add(count).await,
        }
    }

    /// Acquire/add items, waiting if necessary
    pub async fn acquire(&self, count: usize) {
        match self {
            RateLimiter::TokenBucket(bucket) => bucket.acquire(count).await,
            RateLimiter::LeakyBucket(bucket) => bucket.add(count).await,
        }
    }

    /// Get rate limiter strategy
    pub fn strategy(&self) -> RateLimitStrategy {
        match self {
            RateLimiter::TokenBucket(_) => RateLimitStrategy::TokenBucket,
            RateLimiter::LeakyBucket(_) => RateLimitStrategy::LeakyBucket,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_token_bucket_try_acquire() {
        let bucket = TokenBucket::new(100.0, 100);

        // Should succeed with available tokens
        assert!(bucket.try_acquire(50).await);
        assert_eq!(bucket.available_tokens().await, 50);

        // Should succeed with remaining tokens
        assert!(bucket.try_acquire(50).await);
        assert_eq!(bucket.available_tokens().await, 0);

        // Should fail with no tokens
        assert!(!bucket.try_acquire(1).await);
    }

    #[tokio::test]
    async fn test_token_bucket_refill() {
        let bucket = TokenBucket::new(100.0, 100);

        // Consume all tokens
        assert!(bucket.try_acquire(100).await);
        assert_eq!(bucket.available_tokens().await, 0);

        // Wait for refill
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Should have ~50 tokens after 0.5 seconds at 100/sec
        let tokens = bucket.available_tokens().await;
        assert!(
            (45..=55).contains(&tokens),
            "Expected ~50 tokens, got {}",
            tokens
        );
    }

    #[tokio::test]
    async fn test_token_bucket_acquire_blocking() {
        let bucket = TokenBucket::new(100.0, 100);

        // Consume all tokens
        bucket.acquire(100).await;

        // This should wait for tokens to refill
        let start = Instant::now();
        bucket.acquire(50).await;
        let elapsed = start.elapsed();

        // Should wait approximately 0.5 seconds for 50 tokens at 100/sec
        assert!(elapsed >= Duration::from_millis(400));
        assert!(elapsed <= Duration::from_millis(700));
    }

    #[tokio::test]
    async fn test_token_bucket_statistics() {
        let bucket = TokenBucket::new(100.0, 200);
        bucket.acquire(100).await;

        let stats = bucket.statistics().await;
        assert_eq!(stats.capacity, 200);
        assert_eq!(stats.rate, 100.0);
        assert!(stats.available_tokens >= 95 && stats.available_tokens <= 105);
        assert!(stats.utilization >= 0.45 && stats.utilization <= 0.55);
    }

    #[tokio::test]
    async fn test_leaky_bucket_try_add() {
        let bucket = LeakyBucket::new(100.0, 100);

        // Should succeed with space available
        assert!(bucket.try_add(50).await);
        assert_eq!(bucket.queue_size().await, 50);

        // Should succeed with remaining space
        assert!(bucket.try_add(50).await);
        assert_eq!(bucket.queue_size().await, 100);

        // Should fail when full
        assert!(!bucket.try_add(1).await);
    }

    #[tokio::test]
    async fn test_leaky_bucket_leak() {
        let bucket = LeakyBucket::new(100.0, 100);

        // Fill the bucket
        assert!(bucket.try_add(100).await);
        assert_eq!(bucket.queue_size().await, 100);

        // Wait for leak
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Should have ~50 items after 0.5 seconds at 100/sec leak rate
        let size = bucket.queue_size().await;
        assert!(
            (45..=55).contains(&size),
            "Expected ~50 items, got {}",
            size
        );
    }

    #[tokio::test]
    async fn test_leaky_bucket_add_blocking() {
        let bucket = LeakyBucket::new(100.0, 100);

        // Fill the bucket
        bucket.add(100).await;

        // This should wait for space to become available
        let start = Instant::now();
        bucket.add(50).await;
        let elapsed = start.elapsed();

        // Should wait approximately 0.5 seconds for 50 items to leak at 100/sec
        assert!(elapsed >= Duration::from_millis(400));
        assert!(elapsed <= Duration::from_millis(700));
    }

    #[tokio::test]
    async fn test_leaky_bucket_statistics() {
        let bucket = LeakyBucket::new(100.0, 200);
        bucket.add(100).await;

        let stats = bucket.statistics().await;
        assert_eq!(stats.capacity, 200);
        assert_eq!(stats.rate, 100.0);
        assert!(stats.queue_size >= 95 && stats.queue_size <= 105);
        assert!(stats.utilization >= 0.45 && stats.utilization <= 0.55);
    }

    #[tokio::test]
    async fn test_rate_limiter_token_bucket() {
        let limiter = RateLimiter::token_bucket(100.0, 100);
        assert_eq!(limiter.strategy(), RateLimitStrategy::TokenBucket);

        assert!(limiter.try_acquire(50).await);
        assert!(limiter.try_acquire(50).await);
        assert!(!limiter.try_acquire(1).await);
    }

    #[tokio::test]
    async fn test_rate_limiter_leaky_bucket() {
        let limiter = RateLimiter::leaky_bucket(100.0, 100);
        assert_eq!(limiter.strategy(), RateLimitStrategy::LeakyBucket);

        assert!(limiter.try_acquire(50).await);
        assert!(limiter.try_acquire(50).await);
        assert!(!limiter.try_acquire(1).await);
    }

    #[tokio::test]
    async fn test_token_bucket_reset() {
        let bucket = TokenBucket::new(100.0, 100);
        bucket.acquire(100).await;
        assert_eq!(bucket.available_tokens().await, 0);

        bucket.reset().await;
        assert_eq!(bucket.available_tokens().await, 100);
    }

    #[tokio::test]
    async fn test_leaky_bucket_reset() {
        let bucket = LeakyBucket::new(100.0, 100);
        bucket.add(100).await;
        assert_eq!(bucket.queue_size().await, 100);

        bucket.reset().await;
        assert_eq!(bucket.queue_size().await, 0);
    }
}
