//! Rate Limiting for Task Execution
//!
//! This module provides rate limiting capabilities for controlling task execution rates.
//! It supports multiple algorithms:
//!
//! - **Token Bucket**: Classic algorithm that allows bursts up to bucket capacity
//! - **Sliding Window**: Tracks actual execution counts within a time window
//!
//! # Example
//!
//! ```rust
//! use celers_core::rate_limit::{RateLimiter, TokenBucket, RateLimitConfig};
//! use std::time::Duration;
//!
//! // Create a rate limiter allowing 10 tasks per second with burst capacity of 20
//! let config = RateLimitConfig::new(10.0).with_burst(20);
//! let mut limiter = TokenBucket::new(config);
//!
//! // Check if we can execute a task
//! if limiter.try_acquire() {
//!     println!("Task can execute");
//! } else {
//!     println!("Rate limited, wait {:?}", limiter.time_until_available());
//! }
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Configuration for rate limiting
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// Maximum tasks per second
    pub rate: f64,
    /// Burst capacity (max tokens in bucket)
    /// If None, defaults to rate
    pub burst: Option<u32>,
    /// Whether to use sliding window algorithm instead of token bucket
    pub sliding_window: bool,
    /// Window size for sliding window algorithm (in seconds)
    pub window_size: u64,
}

impl RateLimitConfig {
    /// Create a new rate limit configuration
    ///
    /// # Arguments
    ///
    /// * `rate` - Maximum tasks per second
    pub fn new(rate: f64) -> Self {
        Self {
            rate,
            burst: None,
            sliding_window: false,
            window_size: 1,
        }
    }

    /// Set the burst capacity (max tokens in bucket)
    pub fn with_burst(mut self, burst: u32) -> Self {
        self.burst = Some(burst);
        self
    }

    /// Use sliding window algorithm instead of token bucket
    pub fn with_sliding_window(mut self, window_size: u64) -> Self {
        self.sliding_window = true;
        self.window_size = window_size;
        self
    }

    /// Get the effective burst capacity
    pub fn effective_burst(&self) -> u32 {
        self.burst.unwrap_or(self.rate.ceil() as u32)
    }
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            rate: 100.0, // 100 tasks per second default
            burst: None,
            sliding_window: false,
            window_size: 1,
        }
    }
}

/// Trait for rate limiter implementations
pub trait RateLimiter: Send + Sync {
    /// Try to acquire a permit to execute a task
    ///
    /// Returns `true` if the task can execute immediately, `false` if rate limited
    fn try_acquire(&mut self) -> bool;

    /// Acquire a permit, blocking until available
    ///
    /// Returns the time waited
    fn acquire(&mut self) -> Duration;

    /// Get the time until a permit will be available
    fn time_until_available(&self) -> Duration;

    /// Get the current number of available permits
    fn available_permits(&self) -> u32;

    /// Reset the rate limiter to its initial state
    fn reset(&mut self);

    /// Update the rate limit configuration
    fn set_rate(&mut self, rate: f64);

    /// Get current configuration
    fn config(&self) -> &RateLimitConfig;
}

/// Token bucket rate limiter
///
/// The token bucket algorithm works by:
/// - Adding tokens at a fixed rate (rate per second)
/// - Consuming one token per task execution
/// - Allowing bursts up to the bucket capacity
///
/// This is the default and recommended rate limiter for most use cases.
#[derive(Debug)]
pub struct TokenBucket {
    config: RateLimitConfig,
    /// Current number of tokens in the bucket
    tokens: f64,
    /// Last time tokens were refilled
    last_refill: Instant,
}

impl TokenBucket {
    /// Create a new token bucket rate limiter
    pub fn new(config: RateLimitConfig) -> Self {
        let tokens = config.effective_burst() as f64;
        Self {
            config,
            tokens,
            last_refill: Instant::now(),
        }
    }

    /// Create a token bucket with default configuration
    pub fn with_rate(rate: f64) -> Self {
        Self::new(RateLimitConfig::new(rate))
    }

    /// Refill tokens based on elapsed time
    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill);
        let new_tokens = elapsed.as_secs_f64() * self.config.rate;
        let max_tokens = self.config.effective_burst() as f64;
        self.tokens = (self.tokens + new_tokens).min(max_tokens);
        self.last_refill = now;
    }
}

impl RateLimiter for TokenBucket {
    fn try_acquire(&mut self) -> bool {
        self.refill();
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    fn acquire(&mut self) -> Duration {
        let start = Instant::now();
        while !self.try_acquire() {
            let wait_time = self.time_until_available();
            if wait_time > Duration::ZERO {
                std::thread::sleep(wait_time);
            }
        }
        start.elapsed()
    }

    fn time_until_available(&self) -> Duration {
        if self.tokens >= 1.0 {
            Duration::ZERO
        } else {
            let tokens_needed = 1.0 - self.tokens;
            let seconds = tokens_needed / self.config.rate;
            Duration::from_secs_f64(seconds)
        }
    }

    fn available_permits(&self) -> u32 {
        self.tokens.floor() as u32
    }

    fn reset(&mut self) {
        self.tokens = self.config.effective_burst() as f64;
        self.last_refill = Instant::now();
    }

    fn set_rate(&mut self, rate: f64) {
        self.config.rate = rate;
    }

    fn config(&self) -> &RateLimitConfig {
        &self.config
    }
}

/// Sliding window rate limiter
///
/// Tracks actual execution timestamps and counts executions within a sliding window.
/// More accurate than token bucket but uses more memory.
#[derive(Debug)]
pub struct SlidingWindow {
    config: RateLimitConfig,
    /// Timestamps of recent executions
    timestamps: Vec<Instant>,
}

impl SlidingWindow {
    /// Create a new sliding window rate limiter
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            config,
            timestamps: Vec::new(),
        }
    }

    /// Create a sliding window limiter with default configuration
    pub fn with_rate(rate: f64, window_size: u64) -> Self {
        let config = RateLimitConfig::new(rate).with_sliding_window(window_size);
        Self::new(config)
    }

    /// Clean up old timestamps outside the window
    fn cleanup(&mut self) {
        let window = Duration::from_secs(self.config.window_size);
        let cutoff = Instant::now() - window;
        self.timestamps.retain(|&t| t > cutoff);
    }

    /// Get the maximum allowed executions in the window
    fn max_executions(&self) -> usize {
        (self.config.rate * self.config.window_size as f64).ceil() as usize
    }
}

impl RateLimiter for SlidingWindow {
    fn try_acquire(&mut self) -> bool {
        self.cleanup();
        if self.timestamps.len() < self.max_executions() {
            self.timestamps.push(Instant::now());
            true
        } else {
            false
        }
    }

    fn acquire(&mut self) -> Duration {
        let start = Instant::now();
        while !self.try_acquire() {
            let wait_time = self.time_until_available();
            if wait_time > Duration::ZERO {
                std::thread::sleep(wait_time);
            }
        }
        start.elapsed()
    }

    fn time_until_available(&self) -> Duration {
        if self.timestamps.len() < self.max_executions() {
            Duration::ZERO
        } else if let Some(&oldest) = self.timestamps.first() {
            let window = Duration::from_secs(self.config.window_size);
            let expires = oldest + window;
            let now = Instant::now();
            if expires > now {
                expires - now
            } else {
                Duration::ZERO
            }
        } else {
            Duration::ZERO
        }
    }

    fn available_permits(&self) -> u32 {
        let max = self.max_executions();
        let current = self.timestamps.len();
        (max.saturating_sub(current)) as u32
    }

    fn reset(&mut self) {
        self.timestamps.clear();
    }

    fn set_rate(&mut self, rate: f64) {
        self.config.rate = rate;
    }

    fn config(&self) -> &RateLimitConfig {
        &self.config
    }
}

/// Per-task rate limiter manager
///
/// Manages rate limiters for multiple task types, allowing different
/// rate limits per task name.
#[derive(Debug)]
pub struct TaskRateLimiter {
    /// Per-task rate limiters (task_name -> limiter)
    limiters: HashMap<String, TokenBucket>,
    /// Default rate limit for tasks without specific configuration
    default_config: Option<RateLimitConfig>,
}

impl TaskRateLimiter {
    /// Create a new task rate limiter manager
    pub fn new() -> Self {
        Self {
            limiters: HashMap::new(),
            default_config: None,
        }
    }

    /// Create with a default rate limit for all tasks
    pub fn with_default(config: RateLimitConfig) -> Self {
        Self {
            limiters: HashMap::new(),
            default_config: Some(config),
        }
    }

    /// Set rate limit for a specific task type
    pub fn set_task_rate(&mut self, task_name: impl Into<String>, config: RateLimitConfig) {
        let name = task_name.into();
        self.limiters.insert(name, TokenBucket::new(config));
    }

    /// Remove rate limit for a specific task type
    pub fn remove_task_rate(&mut self, task_name: &str) {
        self.limiters.remove(task_name);
    }

    /// Try to acquire a permit for a specific task
    ///
    /// Returns `true` if the task can execute, `false` if rate limited
    pub fn try_acquire(&mut self, task_name: &str) -> bool {
        if let Some(limiter) = self.limiters.get_mut(task_name) {
            limiter.try_acquire()
        } else if let Some(ref config) = self.default_config {
            // Create a limiter for this task using default config
            let mut limiter = TokenBucket::new(config.clone());
            let result = limiter.try_acquire();
            self.limiters.insert(task_name.to_string(), limiter);
            result
        } else {
            // No rate limit configured
            true
        }
    }

    /// Get time until a task can be executed
    pub fn time_until_available(&self, task_name: &str) -> Duration {
        if let Some(limiter) = self.limiters.get(task_name) {
            limiter.time_until_available()
        } else {
            Duration::ZERO
        }
    }

    /// Check if a task type has a rate limit configured
    pub fn has_rate_limit(&self, task_name: &str) -> bool {
        self.limiters.contains_key(task_name) || self.default_config.is_some()
    }

    /// Get the rate limit configuration for a task
    pub fn get_rate_limit(&self, task_name: &str) -> Option<&RateLimitConfig> {
        self.limiters
            .get(task_name)
            .map(|l| l.config())
            .or(self.default_config.as_ref())
    }

    /// Reset all rate limiters
    pub fn reset_all(&mut self) {
        for limiter in self.limiters.values_mut() {
            limiter.reset();
        }
    }
}

impl Default for TaskRateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

/// Thread-safe per-worker rate limiter
///
/// Wraps a rate limiter for safe concurrent access from multiple worker threads.
#[derive(Debug, Clone)]
pub struct WorkerRateLimiter {
    inner: Arc<RwLock<TaskRateLimiter>>,
}

impl WorkerRateLimiter {
    /// Create a new worker rate limiter
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(TaskRateLimiter::new())),
        }
    }

    /// Create with a default rate limit
    pub fn with_default(config: RateLimitConfig) -> Self {
        Self {
            inner: Arc::new(RwLock::new(TaskRateLimiter::with_default(config))),
        }
    }

    /// Set rate limit for a specific task type
    pub fn set_task_rate(&self, task_name: impl Into<String>, config: RateLimitConfig) {
        if let Ok(mut guard) = self.inner.write() {
            guard.set_task_rate(task_name, config);
        }
    }

    /// Remove rate limit for a specific task type
    pub fn remove_task_rate(&self, task_name: &str) {
        if let Ok(mut guard) = self.inner.write() {
            guard.remove_task_rate(task_name);
        }
    }

    /// Try to acquire a permit for a specific task
    pub fn try_acquire(&self, task_name: &str) -> bool {
        if let Ok(mut guard) = self.inner.write() {
            guard.try_acquire(task_name)
        } else {
            // If lock is poisoned, allow execution
            true
        }
    }

    /// Get time until a task can be executed
    pub fn time_until_available(&self, task_name: &str) -> Duration {
        if let Ok(guard) = self.inner.read() {
            guard.time_until_available(task_name)
        } else {
            Duration::ZERO
        }
    }

    /// Check if a task type has a rate limit configured
    pub fn has_rate_limit(&self, task_name: &str) -> bool {
        if let Ok(guard) = self.inner.read() {
            guard.has_rate_limit(task_name)
        } else {
            false
        }
    }

    /// Reset all rate limiters
    pub fn reset_all(&self) {
        if let Ok(mut guard) = self.inner.write() {
            guard.reset_all();
        }
    }
}

impl Default for WorkerRateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

/// Create a rate limiter from configuration
pub fn create_rate_limiter(config: RateLimitConfig) -> Box<dyn RateLimiter> {
    if config.sliding_window {
        Box::new(SlidingWindow::new(config))
    } else {
        Box::new(TokenBucket::new(config))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_token_bucket_basic() {
        let config = RateLimitConfig::new(10.0).with_burst(5);
        let mut limiter = TokenBucket::new(config);

        // Should be able to acquire up to burst capacity immediately
        for _ in 0..5 {
            assert!(limiter.try_acquire());
        }

        // Next acquisition should fail
        assert!(!limiter.try_acquire());
    }

    #[test]
    fn test_token_bucket_refill() {
        let config = RateLimitConfig::new(100.0).with_burst(10);
        let mut limiter = TokenBucket::new(config);

        // Exhaust all tokens
        for _ in 0..10 {
            assert!(limiter.try_acquire());
        }
        assert!(!limiter.try_acquire());

        // Wait for refill (10ms should give us ~1 token at 100/sec)
        thread::sleep(Duration::from_millis(15));

        // Should have at least 1 token now
        assert!(limiter.try_acquire());
    }

    #[test]
    fn test_sliding_window_basic() {
        let config = RateLimitConfig::new(5.0).with_sliding_window(1);
        let mut limiter = SlidingWindow::new(config);

        // Should be able to execute 5 tasks in 1 second window
        for _ in 0..5 {
            assert!(limiter.try_acquire());
        }

        // Next acquisition should fail
        assert!(!limiter.try_acquire());
    }

    #[test]
    fn test_task_rate_limiter() {
        let mut manager = TaskRateLimiter::new();

        // Set rate limit for task_a
        manager.set_task_rate("task_a", RateLimitConfig::new(10.0).with_burst(2));

        // task_a should be rate limited
        assert!(manager.try_acquire("task_a"));
        assert!(manager.try_acquire("task_a"));
        assert!(!manager.try_acquire("task_a"));

        // task_b has no rate limit, should always pass
        assert!(manager.try_acquire("task_b"));
        assert!(manager.try_acquire("task_b"));
        assert!(manager.try_acquire("task_b"));
    }

    #[test]
    fn test_task_rate_limiter_default() {
        let mut manager = TaskRateLimiter::with_default(RateLimitConfig::new(10.0).with_burst(2));

        // All tasks should use default rate limit
        assert!(manager.try_acquire("task_a"));
        assert!(manager.try_acquire("task_a"));
        assert!(!manager.try_acquire("task_a"));

        assert!(manager.try_acquire("task_b"));
        assert!(manager.try_acquire("task_b"));
        assert!(!manager.try_acquire("task_b"));
    }

    #[test]
    fn test_worker_rate_limiter_thread_safe() {
        let limiter = WorkerRateLimiter::new();
        limiter.set_task_rate("task_a", RateLimitConfig::new(100.0).with_burst(10));

        let limiter_clone = limiter.clone();

        // Spawn multiple threads to test thread safety
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let l = limiter_clone.clone();
                thread::spawn(move || {
                    let mut count = 0;
                    for _ in 0..5 {
                        if l.try_acquire("task_a") {
                            count += 1;
                        }
                    }
                    count
                })
            })
            .collect();

        let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();

        // Total acquisitions should not exceed burst capacity
        assert!(total <= 10);
    }

    #[test]
    fn test_rate_limit_config_serialization() {
        let config = RateLimitConfig::new(50.0)
            .with_burst(100)
            .with_sliding_window(10);

        let json = serde_json::to_string(&config).unwrap();
        let parsed: RateLimitConfig = serde_json::from_str(&json).unwrap();

        assert!((parsed.rate - 50.0).abs() < f64::EPSILON);
        assert_eq!(parsed.burst, Some(100));
        assert!(parsed.sliding_window);
        assert_eq!(parsed.window_size, 10);
    }

    #[test]
    fn test_time_until_available() {
        let config = RateLimitConfig::new(10.0).with_burst(1);
        let mut limiter = TokenBucket::new(config);

        // Exhaust the token
        assert!(limiter.try_acquire());

        // Time until available should be around 100ms (1 token at 10/sec)
        let wait_time = limiter.time_until_available();
        assert!(wait_time > Duration::ZERO);
        assert!(wait_time <= Duration::from_millis(150));
    }

    #[test]
    fn test_reset() {
        let config = RateLimitConfig::new(10.0).with_burst(5);
        let mut limiter = TokenBucket::new(config);

        // Exhaust all tokens
        for _ in 0..5 {
            limiter.try_acquire();
        }
        assert!(!limiter.try_acquire());

        // Reset should restore tokens
        limiter.reset();
        assert!(limiter.try_acquire());
    }

    #[test]
    fn test_set_rate() {
        let config = RateLimitConfig::new(10.0).with_burst(10);
        let mut limiter = TokenBucket::new(config);

        // Update rate
        limiter.set_rate(100.0);
        assert!((limiter.config().rate - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_create_rate_limiter() {
        // Token bucket
        let config = RateLimitConfig::new(10.0);
        let mut limiter = create_rate_limiter(config);
        assert!(limiter.try_acquire());

        // Sliding window
        let config = RateLimitConfig::new(10.0).with_sliding_window(1);
        let mut limiter = create_rate_limiter(config);
        assert!(limiter.try_acquire());
    }
}
