//! Rate limiting for task execution
//!
//! Provides per-task-type rate limiting using the token bucket algorithm.
//! This prevents task types from overwhelming the system or external resources.
//!
//! # Token Bucket Algorithm
//!
//! Each task type has its own bucket with:
//! - **Capacity**: Maximum burst size (tokens that can accumulate)
//! - **Refill rate**: Tokens added per second
//! - **Current tokens**: Available tokens (consumed on task execution)
//!
//! When a task arrives:
//! 1. Check if bucket has ≥1 token
//! 2. If yes: consume token and execute task
//! 3. If no: reject or delay task
//!
//! # Example
//!
//! ```rust
//! use celers_worker::rate_limit::{RateLimiter, RateLimitConfig};
//! use std::time::Duration;
//!
//! # async fn example() {
//! let config = RateLimitConfig {
//!     max_tokens: 10.0,
//!     refill_rate: 2.0, // 2 tokens per second
//! };
//!
//! let limiter = RateLimiter::new();
//! limiter.set_limit("api_call", config).await;
//!
//! // Try to acquire a token
//! if limiter.try_acquire("api_call", 1.0).await {
//!     // Execute task
//! } else {
//!     // Rate limited, delay or reject
//! }
//! # }
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Rate limit configuration for a task type
#[derive(Debug, Clone, Copy)]
pub struct RateLimitConfig {
    /// Maximum tokens in the bucket (burst capacity)
    pub max_tokens: f64,

    /// Tokens refilled per second
    pub refill_rate: f64,
}

impl RateLimitConfig {
    /// Create a new rate limit configuration
    ///
    /// # Arguments
    ///
    /// * `max_tokens` - Maximum burst size
    /// * `refill_rate` - Tokens per second
    pub fn new(max_tokens: f64, refill_rate: f64) -> Self {
        Self {
            max_tokens,
            refill_rate,
        }
    }

    /// Validate configuration
    pub fn is_valid(&self) -> bool {
        self.max_tokens > 0.0 && self.refill_rate > 0.0
    }

    /// Create a strict configuration (low rate)
    pub fn strict() -> Self {
        Self {
            max_tokens: 5.0,
            refill_rate: 1.0,
        }
    }

    /// Create a moderate configuration
    pub fn moderate() -> Self {
        Self {
            max_tokens: 20.0,
            refill_rate: 10.0,
        }
    }

    /// Create a lenient configuration (high rate)
    pub fn lenient() -> Self {
        Self {
            max_tokens: 100.0,
            refill_rate: 50.0,
        }
    }
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            max_tokens: 10.0,
            refill_rate: 5.0,
        }
    }
}

/// Token bucket for rate limiting
#[derive(Debug)]
struct TokenBucket {
    /// Current number of tokens
    tokens: f64,

    /// Maximum tokens
    max_tokens: f64,

    /// Refill rate (tokens per second)
    refill_rate: f64,

    /// Last refill timestamp
    last_refill: Instant,
}

impl TokenBucket {
    fn new(config: RateLimitConfig) -> Self {
        Self {
            tokens: config.max_tokens,
            max_tokens: config.max_tokens,
            refill_rate: config.refill_rate,
            last_refill: Instant::now(),
        }
    }

    /// Refill tokens based on elapsed time
    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();

        if elapsed > 0.0 {
            let new_tokens = elapsed * self.refill_rate;
            self.tokens = (self.tokens + new_tokens).min(self.max_tokens);
            self.last_refill = now;

            debug!(
                "Refilled bucket: added {:.2} tokens, current: {:.2}/{:.2}",
                new_tokens, self.tokens, self.max_tokens
            );
        }
    }

    /// Try to acquire tokens
    ///
    /// Returns true if tokens were acquired, false if insufficient
    fn try_acquire(&mut self, tokens: f64) -> bool {
        self.refill();

        if self.tokens >= tokens {
            self.tokens -= tokens;
            debug!(
                "Acquired {:.2} tokens, remaining: {:.2}/{:.2}",
                tokens, self.tokens, self.max_tokens
            );
            true
        } else {
            debug!(
                "Rate limit exceeded: requested {:.2}, available {:.2}/{:.2}",
                tokens, self.tokens, self.max_tokens
            );
            false
        }
    }

    /// Get current token count
    fn current_tokens(&mut self) -> f64 {
        self.refill();
        self.tokens
    }

    /// Get time until next token is available
    fn time_until_next_token(&mut self) -> Duration {
        self.refill();

        if self.tokens >= 1.0 {
            Duration::ZERO
        } else {
            let tokens_needed = 1.0 - self.tokens;
            let seconds = tokens_needed / self.refill_rate;
            Duration::from_secs_f64(seconds)
        }
    }
}

/// Rate limiter managing multiple task types
pub struct RateLimiter {
    buckets: Arc<RwLock<HashMap<String, TokenBucket>>>,
}

impl RateLimiter {
    /// Create a new rate limiter
    pub fn new() -> Self {
        Self {
            buckets: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Set rate limit for a task type
    ///
    /// # Arguments
    ///
    /// * `task_name` - Name of the task type
    /// * `config` - Rate limit configuration
    pub async fn set_limit(&self, task_name: impl Into<String>, config: RateLimitConfig) {
        if !config.is_valid() {
            warn!("Invalid rate limit config: {:?}", config);
            return;
        }

        let task_name = task_name.into();
        let mut buckets = self.buckets.write().await;
        buckets.insert(task_name.clone(), TokenBucket::new(config));
        debug!("Set rate limit for '{}': {:?}", task_name, config);
    }

    /// Remove rate limit for a task type
    pub async fn remove_limit(&self, task_name: &str) {
        let mut buckets = self.buckets.write().await;
        buckets.remove(task_name);
        debug!("Removed rate limit for '{}'", task_name);
    }

    /// Try to acquire tokens for task execution
    ///
    /// Returns true if tokens were acquired, false if rate limited
    pub async fn try_acquire(&self, task_name: &str, tokens: f64) -> bool {
        let mut buckets = self.buckets.write().await;

        if let Some(bucket) = buckets.get_mut(task_name) {
            bucket.try_acquire(tokens)
        } else {
            // No rate limit configured, allow execution
            true
        }
    }

    /// Get current token count for a task type
    pub async fn current_tokens(&self, task_name: &str) -> Option<f64> {
        let mut buckets = self.buckets.write().await;
        buckets.get_mut(task_name).map(|b| b.current_tokens())
    }

    /// Get time until next token is available
    pub async fn time_until_available(&self, task_name: &str) -> Option<Duration> {
        let mut buckets = self.buckets.write().await;
        buckets
            .get_mut(task_name)
            .map(|b| b.time_until_next_token())
    }

    /// Check if a task type has rate limiting configured
    pub async fn has_limit(&self, task_name: &str) -> bool {
        let buckets = self.buckets.read().await;
        buckets.contains_key(task_name)
    }

    /// Get all configured rate limits
    pub async fn get_all_limits(&self) -> HashMap<String, RateLimitConfig> {
        let buckets = self.buckets.read().await;
        buckets
            .iter()
            .map(|(name, bucket)| {
                (
                    name.clone(),
                    RateLimitConfig {
                        max_tokens: bucket.max_tokens,
                        refill_rate: bucket.refill_rate,
                    },
                )
            })
            .collect()
    }

    /// Clear all rate limits
    pub async fn clear_all(&self) {
        let mut buckets = self.buckets.write().await;
        buckets.clear();
        debug!("Cleared all rate limits");
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for RateLimiter {
    fn clone(&self) -> Self {
        Self {
            buckets: Arc::clone(&self.buckets),
        }
    }
}

/// Sliding window rate limiter configuration
#[derive(Debug, Clone, Copy)]
pub struct SlidingWindowConfig {
    /// Maximum requests allowed in the time window
    pub max_requests: usize,
    /// Time window duration
    pub window_duration: Duration,
}

impl SlidingWindowConfig {
    /// Create a new sliding window configuration
    pub fn new(max_requests: usize, window_duration: Duration) -> Self {
        Self {
            max_requests,
            window_duration,
        }
    }

    /// Validate configuration
    pub fn is_valid(&self) -> bool {
        self.max_requests > 0 && !self.window_duration.is_zero()
    }

    /// Strict: 10 requests per minute
    pub fn strict() -> Self {
        Self {
            max_requests: 10,
            window_duration: Duration::from_secs(60),
        }
    }

    /// Moderate: 100 requests per minute
    pub fn moderate() -> Self {
        Self {
            max_requests: 100,
            window_duration: Duration::from_secs(60),
        }
    }

    /// Lenient: 1000 requests per minute
    pub fn lenient() -> Self {
        Self {
            max_requests: 1000,
            window_duration: Duration::from_secs(60),
        }
    }
}

impl Default for SlidingWindowConfig {
    fn default() -> Self {
        Self {
            max_requests: 100,
            window_duration: Duration::from_secs(60),
        }
    }
}

/// Sliding window for tracking requests
#[derive(Debug)]
struct SlidingWindow {
    /// Request timestamps
    requests: Vec<Instant>,
    /// Maximum requests allowed
    max_requests: usize,
    /// Window duration
    window_duration: Duration,
}

impl SlidingWindow {
    fn new(config: SlidingWindowConfig) -> Self {
        Self {
            requests: Vec::with_capacity(config.max_requests),
            max_requests: config.max_requests,
            window_duration: config.window_duration,
        }
    }

    /// Remove expired requests outside the current window
    fn cleanup(&mut self) {
        let now = Instant::now();
        let cutoff = now.checked_sub(self.window_duration).unwrap_or(now);
        self.requests.retain(|&timestamp| timestamp > cutoff);
    }

    /// Try to record a new request
    ///
    /// Returns true if request is allowed, false if rate limited
    fn try_record(&mut self) -> bool {
        self.cleanup();

        if self.requests.len() < self.max_requests {
            self.requests.push(Instant::now());
            debug!(
                "Request allowed: {}/{} in window",
                self.requests.len(),
                self.max_requests
            );
            true
        } else {
            debug!(
                "Rate limit exceeded: {}/{} requests in window",
                self.requests.len(),
                self.max_requests
            );
            false
        }
    }

    /// Get current request count in the window
    fn current_count(&mut self) -> usize {
        self.cleanup();
        self.requests.len()
    }

    /// Get time until the oldest request expires
    fn time_until_available(&mut self) -> Duration {
        self.cleanup();

        if self.requests.len() < self.max_requests {
            Duration::ZERO
        } else if let Some(&oldest) = self.requests.first() {
            let now = Instant::now();
            let expiry = oldest + self.window_duration;
            expiry.saturating_duration_since(now)
        } else {
            Duration::ZERO
        }
    }

    /// Reset the window
    fn reset(&mut self) {
        self.requests.clear();
    }
}

/// Sliding window rate limiter for more accurate rate limiting
pub struct SlidingWindowLimiter {
    windows: Arc<RwLock<HashMap<String, SlidingWindow>>>,
}

impl SlidingWindowLimiter {
    /// Create a new sliding window rate limiter
    pub fn new() -> Self {
        Self {
            windows: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Set rate limit for a task type
    pub async fn set_limit(&self, task_name: impl Into<String>, config: SlidingWindowConfig) {
        if !config.is_valid() {
            warn!("Invalid sliding window config: {:?}", config);
            return;
        }

        let task_name = task_name.into();
        let mut windows = self.windows.write().await;
        windows.insert(task_name.clone(), SlidingWindow::new(config));
        debug!("Set sliding window limit for '{}': {:?}", task_name, config);
    }

    /// Remove rate limit for a task type
    pub async fn remove_limit(&self, task_name: &str) {
        let mut windows = self.windows.write().await;
        windows.remove(task_name);
        debug!("Removed sliding window limit for '{}'", task_name);
    }

    /// Try to record a request
    ///
    /// Returns true if request is allowed, false if rate limited
    pub async fn try_acquire(&self, task_name: &str) -> bool {
        let mut windows = self.windows.write().await;

        if let Some(window) = windows.get_mut(task_name) {
            window.try_record()
        } else {
            // No rate limit configured, allow execution
            true
        }
    }

    /// Get current request count in the window
    pub async fn current_count(&self, task_name: &str) -> Option<usize> {
        let mut windows = self.windows.write().await;
        windows.get_mut(task_name).map(|w| w.current_count())
    }

    /// Get time until next request is available
    pub async fn time_until_available(&self, task_name: &str) -> Option<Duration> {
        let mut windows = self.windows.write().await;
        windows.get_mut(task_name).map(|w| w.time_until_available())
    }

    /// Reset the window for a task type
    pub async fn reset(&self, task_name: &str) {
        let mut windows = self.windows.write().await;
        if let Some(window) = windows.get_mut(task_name) {
            window.reset();
            debug!("Reset sliding window for '{}'", task_name);
        }
    }

    /// Check if a task type has rate limiting configured
    pub async fn has_limit(&self, task_name: &str) -> bool {
        let windows = self.windows.read().await;
        windows.contains_key(task_name)
    }

    /// Clear all rate limits
    pub async fn clear_all(&self) {
        let mut windows = self.windows.write().await;
        windows.clear();
        debug!("Cleared all sliding window limits");
    }
}

impl Default for SlidingWindowLimiter {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for SlidingWindowLimiter {
    fn clone(&self) -> Self {
        Self {
            windows: Arc::clone(&self.windows),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_rate_limit_config() {
        let config = RateLimitConfig::new(10.0, 5.0);
        assert_eq!(config.max_tokens, 10.0);
        assert_eq!(config.refill_rate, 5.0);
        assert!(config.is_valid());

        let invalid = RateLimitConfig::new(0.0, 5.0);
        assert!(!invalid.is_valid());
    }

    #[tokio::test]
    async fn test_rate_limit_presets() {
        let strict = RateLimitConfig::strict();
        assert_eq!(strict.max_tokens, 5.0);
        assert_eq!(strict.refill_rate, 1.0);

        let moderate = RateLimitConfig::moderate();
        assert_eq!(moderate.max_tokens, 20.0);
        assert_eq!(moderate.refill_rate, 10.0);

        let lenient = RateLimitConfig::lenient();
        assert_eq!(lenient.max_tokens, 100.0);
        assert_eq!(lenient.refill_rate, 50.0);
    }

    #[test]
    fn test_token_bucket_new() {
        let config = RateLimitConfig::new(10.0, 5.0);
        let mut bucket = TokenBucket::new(config);
        assert_eq!(bucket.current_tokens(), 10.0);
    }

    #[test]
    fn test_token_bucket_acquire() {
        let config = RateLimitConfig::new(10.0, 5.0);
        let mut bucket = TokenBucket::new(config);

        // Should succeed
        assert!(bucket.try_acquire(3.0));
        assert!(bucket.current_tokens() < 8.0); // Should be around 7

        // Acquire more
        assert!(bucket.try_acquire(5.0));
        assert!(bucket.current_tokens() < 3.0);

        // Should fail (not enough tokens)
        assert!(!bucket.try_acquire(5.0));
    }

    #[tokio::test]
    async fn test_token_bucket_refill() {
        let config = RateLimitConfig::new(10.0, 5.0);
        let mut bucket = TokenBucket::new(config);

        // Consume all tokens
        bucket.try_acquire(10.0);
        assert!(bucket.current_tokens() < 1.0);

        // Wait for refill (1 second should add 5 tokens)
        sleep(Duration::from_secs(1)).await;
        let tokens = bucket.current_tokens();
        assert!((4.0..=6.0).contains(&tokens)); // Allow some timing variance
    }

    #[tokio::test]
    async fn test_rate_limiter_set_limit() {
        let limiter = RateLimiter::new();
        let config = RateLimitConfig::new(10.0, 5.0);

        limiter.set_limit("test_task", config).await;
        assert!(limiter.has_limit("test_task").await);
        assert!(!limiter.has_limit("other_task").await);
    }

    #[tokio::test]
    async fn test_rate_limiter_try_acquire() {
        let limiter = RateLimiter::new();
        let config = RateLimitConfig::new(5.0, 2.0);
        limiter.set_limit("test_task", config).await;

        // Should succeed initially
        assert!(limiter.try_acquire("test_task", 2.0).await);
        assert!(limiter.try_acquire("test_task", 2.0).await);

        // Should fail (not enough tokens)
        assert!(!limiter.try_acquire("test_task", 2.0).await);

        // Task without limit should always succeed
        assert!(limiter.try_acquire("unlimited_task", 100.0).await);
    }

    #[tokio::test]
    async fn test_rate_limiter_current_tokens() {
        let limiter = RateLimiter::new();
        let config = RateLimitConfig::new(10.0, 5.0);
        limiter.set_limit("test_task", config).await;

        let tokens = limiter.current_tokens("test_task").await;
        assert!(tokens.is_some());
        assert_eq!(tokens.unwrap(), 10.0);

        // Unknown task
        assert!(limiter.current_tokens("unknown").await.is_none());
    }

    #[tokio::test]
    async fn test_rate_limiter_remove_limit() {
        let limiter = RateLimiter::new();
        let config = RateLimitConfig::new(10.0, 5.0);
        limiter.set_limit("test_task", config).await;

        assert!(limiter.has_limit("test_task").await);
        limiter.remove_limit("test_task").await;
        assert!(!limiter.has_limit("test_task").await);
    }

    #[tokio::test]
    async fn test_rate_limiter_get_all_limits() {
        let limiter = RateLimiter::new();
        limiter.set_limit("task1", RateLimitConfig::strict()).await;
        limiter
            .set_limit("task2", RateLimitConfig::moderate())
            .await;

        let limits = limiter.get_all_limits().await;
        assert_eq!(limits.len(), 2);
        assert!(limits.contains_key("task1"));
        assert!(limits.contains_key("task2"));
    }

    #[tokio::test]
    async fn test_rate_limiter_clear_all() {
        let limiter = RateLimiter::new();
        limiter.set_limit("task1", RateLimitConfig::default()).await;
        limiter.set_limit("task2", RateLimitConfig::default()).await;

        limiter.clear_all().await;
        let limits = limiter.get_all_limits().await;
        assert_eq!(limits.len(), 0);
    }

    #[tokio::test]
    async fn test_time_until_available() {
        let limiter = RateLimiter::new();
        let config = RateLimitConfig::new(5.0, 2.0);
        limiter.set_limit("test_task", config).await;

        // Consume all tokens
        limiter.try_acquire("test_task", 5.0).await;

        // Should need time for refill
        let wait_time = limiter.time_until_available("test_task").await;
        assert!(wait_time.is_some());
        assert!(wait_time.unwrap() > Duration::ZERO);
    }

    // Sliding window tests
    #[tokio::test]
    async fn test_sliding_window_config() {
        let config = SlidingWindowConfig::new(100, Duration::from_secs(60));
        assert_eq!(config.max_requests, 100);
        assert_eq!(config.window_duration, Duration::from_secs(60));
        assert!(config.is_valid());

        let invalid = SlidingWindowConfig::new(0, Duration::from_secs(60));
        assert!(!invalid.is_valid());
    }

    #[tokio::test]
    async fn test_sliding_window_basic() {
        let limiter = SlidingWindowLimiter::new();
        let config = SlidingWindowConfig::new(5, Duration::from_secs(1));
        limiter.set_limit("test_task", config).await;

        // Should succeed for first 5 requests
        for _ in 0..5 {
            assert!(limiter.try_acquire("test_task").await);
        }

        // 6th request should fail
        assert!(!limiter.try_acquire("test_task").await);
    }

    #[tokio::test]
    async fn test_sliding_window_expiry() {
        let limiter = SlidingWindowLimiter::new();
        let config = SlidingWindowConfig::new(3, Duration::from_millis(100));
        limiter.set_limit("test_task", config).await;

        // Use up all 3 requests
        for _ in 0..3 {
            assert!(limiter.try_acquire("test_task").await);
        }
        assert!(!limiter.try_acquire("test_task").await);

        // Wait for window to expire
        sleep(Duration::from_millis(150)).await;

        // Should succeed again
        assert!(limiter.try_acquire("test_task").await);
    }

    #[tokio::test]
    async fn test_sliding_window_current_count() {
        let limiter = SlidingWindowLimiter::new();
        let config = SlidingWindowConfig::new(10, Duration::from_secs(1));
        limiter.set_limit("test_task", config).await;

        assert_eq!(limiter.current_count("test_task").await, Some(0));

        limiter.try_acquire("test_task").await;
        limiter.try_acquire("test_task").await;

        assert_eq!(limiter.current_count("test_task").await, Some(2));
    }

    #[tokio::test]
    async fn test_sliding_window_reset() {
        let limiter = SlidingWindowLimiter::new();
        let config = SlidingWindowConfig::new(5, Duration::from_secs(10));
        limiter.set_limit("test_task", config).await;

        // Use up all requests
        for _ in 0..5 {
            limiter.try_acquire("test_task").await;
        }
        assert_eq!(limiter.current_count("test_task").await, Some(5));

        // Reset window
        limiter.reset("test_task").await;
        assert_eq!(limiter.current_count("test_task").await, Some(0));

        // Should be able to make requests again
        assert!(limiter.try_acquire("test_task").await);
    }

    #[tokio::test]
    async fn test_sliding_window_multiple_tasks() {
        let limiter = SlidingWindowLimiter::new();
        limiter
            .set_limit("task1", SlidingWindowConfig::new(2, Duration::from_secs(1)))
            .await;
        limiter
            .set_limit("task2", SlidingWindowConfig::new(3, Duration::from_secs(1)))
            .await;

        // task1 should allow 2 requests
        assert!(limiter.try_acquire("task1").await);
        assert!(limiter.try_acquire("task1").await);
        assert!(!limiter.try_acquire("task1").await);

        // task2 should allow 3 requests
        assert!(limiter.try_acquire("task2").await);
        assert!(limiter.try_acquire("task2").await);
        assert!(limiter.try_acquire("task2").await);
        assert!(!limiter.try_acquire("task2").await);
    }

    #[tokio::test]
    async fn test_sliding_window_remove_limit() {
        let limiter = SlidingWindowLimiter::new();
        limiter
            .set_limit("test_task", SlidingWindowConfig::default())
            .await;

        assert!(limiter.has_limit("test_task").await);
        limiter.remove_limit("test_task").await;
        assert!(!limiter.has_limit("test_task").await);
    }

    #[tokio::test]
    async fn test_sliding_window_time_until_available() {
        let limiter = SlidingWindowLimiter::new();
        let config = SlidingWindowConfig::new(2, Duration::from_millis(100));
        limiter.set_limit("test_task", config).await;

        // Use up all requests
        limiter.try_acquire("test_task").await;
        limiter.try_acquire("test_task").await;

        // Should need to wait
        let wait_time = limiter.time_until_available("test_task").await;
        assert!(wait_time.is_some());
        assert!(wait_time.unwrap() > Duration::ZERO);
    }
}
