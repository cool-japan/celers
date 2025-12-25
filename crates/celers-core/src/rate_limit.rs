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

/// Distributed rate limiting coordination
///
/// This module provides distributed rate limiting across multiple workers,
/// allowing rate limits to be enforced cluster-wide rather than per-worker.
///
/// # Features
///
/// - **Cluster-wide rate limiting**: Coordinate rate limits across all workers
/// - **Redis backend**: Use Redis for distributed state storage
/// - **Token bucket algorithm**: Distributed token bucket with atomic operations
/// - **Sliding window algorithm**: Distributed sliding window using sorted sets
/// - **Fallback support**: Graceful degradation to local rate limiting on failure
/// - **TTL support**: Automatic cleanup of stale data
///
/// # Example
///
/// ```rust,ignore
/// use celers_core::rate_limit::{DistributedRateLimiter, RateLimitConfig};
///
/// // Create a distributed rate limiter backed by Redis
/// let config = RateLimitConfig::new(100.0).with_burst(200);
/// let limiter = DistributedRateLimiter::redis(
///     "redis://localhost:6379",
///     "my_task",
///     config,
/// ).await?;
///
/// // Try to acquire a permit across all workers
/// if limiter.try_acquire().await? {
///     println!("Task can execute");
/// } else {
///     println!("Rate limited cluster-wide");
/// }
/// ```
use async_trait::async_trait;

/// Trait for distributed rate limiter backends
///
/// Implementations should provide atomic operations for rate limiting
/// across multiple processes/workers.
#[async_trait]
pub trait DistributedRateLimiter: Send + Sync {
    /// Try to acquire a permit atomically
    ///
    /// Returns `Ok(true)` if acquired, `Ok(false)` if rate limited,
    /// or an error if the backend is unavailable.
    async fn try_acquire(&self) -> crate::Result<bool>;

    /// Get the time until a permit will be available
    ///
    /// Returns `Ok(Duration)` with the wait time, or an error if unavailable.
    async fn time_until_available(&self) -> crate::Result<Duration>;

    /// Get the current number of available permits
    ///
    /// Returns `Ok(count)` or an error if unavailable.
    async fn available_permits(&self) -> crate::Result<u32>;

    /// Reset the distributed rate limiter
    ///
    /// Clears all state in the distributed backend.
    async fn reset(&self) -> crate::Result<()>;

    /// Update the rate limit configuration
    ///
    /// Note: This updates the local configuration. Distributed backends
    /// may need additional coordination to sync configuration changes.
    async fn set_rate(&self, rate: f64) -> crate::Result<()>;

    /// Get current configuration
    fn config(&self) -> &RateLimitConfig;

    /// Get the backend name (for diagnostics)
    fn backend_name(&self) -> &str;
}

/// Distributed rate limiter state
///
/// Stores rate limiting state in a distributed backend (e.g., Redis)
/// for coordination across multiple workers.
#[derive(Debug, Clone)]
pub struct DistributedRateLimiterState {
    /// Redis key for storing rate limit state
    pub key: String,
    /// Rate limit configuration
    pub config: RateLimitConfig,
    /// Local fallback limiter (used if distributed backend is unavailable)
    pub fallback: Arc<RwLock<TokenBucket>>,
}

impl DistributedRateLimiterState {
    /// Create a new distributed rate limiter state
    ///
    /// # Arguments
    ///
    /// * `key` - Redis key for storing rate limit state
    /// * `config` - Rate limit configuration
    pub fn new(key: String, config: RateLimitConfig) -> Self {
        let fallback = Arc::new(RwLock::new(TokenBucket::new(config.clone())));
        Self {
            key,
            config,
            fallback,
        }
    }

    /// Get the Redis key for token count
    ///
    /// Used by backend implementations to store token count.
    pub fn token_key(&self) -> String {
        format!("{}:tokens", self.key)
    }

    /// Get the Redis key for last refill timestamp
    ///
    /// Used by backend implementations to store last refill time.
    pub fn refill_key(&self) -> String {
        format!("{}:refill", self.key)
    }

    /// Get the Redis key for sliding window
    ///
    /// Used by backend implementations to store sliding window data.
    pub fn window_key(&self) -> String {
        format!("{}:window", self.key)
    }

    /// Try to acquire using local fallback
    fn try_acquire_fallback(&self) -> bool {
        if let Ok(mut guard) = self.fallback.write() {
            guard.try_acquire()
        } else {
            // If lock is poisoned, allow execution
            true
        }
    }
}

/// Distributed token bucket implementation
///
/// Uses atomic operations in a distributed backend (e.g., Redis Lua scripts)
/// to implement token bucket algorithm across multiple workers.
///
/// # Redis Implementation
///
/// The token bucket is implemented using Redis with two keys:
/// - `{key}:tokens` - Current token count (float)
/// - `{key}:refill` - Last refill timestamp (integer, milliseconds since epoch)
///
/// A Lua script performs atomic token refill and acquisition:
/// 1. Calculate elapsed time since last refill
/// 2. Add tokens based on elapsed time and rate
/// 3. Cap tokens at burst capacity
/// 4. Attempt to consume 1 token
/// 5. Update last refill timestamp
///
/// # Example Lua Script
///
/// ```lua
/// local tokens_key = KEYS[1]
/// local refill_key = KEYS[2]
/// local rate = tonumber(ARGV[1])
/// local burst = tonumber(ARGV[2])
/// local now = tonumber(ARGV[3])
///
/// local last_refill = redis.call('GET', refill_key)
/// local tokens = redis.call('GET', tokens_key)
///
/// if not tokens then
///     tokens = burst
/// else
///     tokens = tonumber(tokens)
/// end
///
/// if last_refill then
///     local elapsed = (now - tonumber(last_refill)) / 1000.0
///     tokens = math.min(tokens + elapsed * rate, burst)
/// end
///
/// if tokens >= 1.0 then
///     tokens = tokens - 1.0
///     redis.call('SET', tokens_key, tostring(tokens))
///     redis.call('SET', refill_key, tostring(now))
///     return 1
/// else
///     redis.call('SET', tokens_key, tostring(tokens))
///     redis.call('SET', refill_key, tostring(now))
///     return 0
/// end
/// ```
#[derive(Debug, Clone)]
pub struct DistributedTokenBucketSpec {
    state: DistributedRateLimiterState,
}

impl DistributedTokenBucketSpec {
    /// Create a new distributed token bucket specification
    ///
    /// This creates the specification for a distributed token bucket.
    /// Actual implementation requires a backend (e.g., Redis client).
    pub fn new(key: String, config: RateLimitConfig) -> Self {
        Self {
            state: DistributedRateLimiterState::new(key, config),
        }
    }

    /// Get the Lua script for atomic token acquisition
    ///
    /// This script should be loaded into Redis using SCRIPT LOAD
    /// and executed with EVALSHA for better performance.
    pub fn lua_acquire_script() -> &'static str {
        r#"
        local tokens_key = KEYS[1]
        local refill_key = KEYS[2]
        local rate = tonumber(ARGV[1])
        local burst = tonumber(ARGV[2])
        local now = tonumber(ARGV[3])
        local ttl = tonumber(ARGV[4])

        local last_refill = redis.call('GET', refill_key)
        local tokens = redis.call('GET', tokens_key)

        if not tokens then
            tokens = burst
        else
            tokens = tonumber(tokens)
        end

        if last_refill then
            local elapsed = (now - tonumber(last_refill)) / 1000.0
            tokens = math.min(tokens + elapsed * rate, burst)
        end

        if tokens >= 1.0 then
            tokens = tokens - 1.0
            redis.call('SET', tokens_key, tostring(tokens), 'EX', ttl)
            redis.call('SET', refill_key, tostring(now), 'EX', ttl)
            return {1, tokens}
        else
            redis.call('SET', tokens_key, tostring(tokens), 'EX', ttl)
            redis.call('SET', refill_key, tostring(now), 'EX', ttl)
            return {0, tokens}
        end
        "#
    }

    /// Get the Lua script for querying available permits
    pub fn lua_available_script() -> &'static str {
        r#"
        local tokens_key = KEYS[1]
        local refill_key = KEYS[2]
        local rate = tonumber(ARGV[1])
        local burst = tonumber(ARGV[2])
        local now = tonumber(ARGV[3])

        local last_refill = redis.call('GET', refill_key)
        local tokens = redis.call('GET', tokens_key)

        if not tokens then
            return burst
        else
            tokens = tonumber(tokens)
        end

        if last_refill then
            local elapsed = (now - tonumber(last_refill)) / 1000.0
            tokens = math.min(tokens + elapsed * rate, burst)
        end

        return math.floor(tokens)
        "#
    }

    /// Get the state for implementing the distributed backend
    pub fn state(&self) -> &DistributedRateLimiterState {
        &self.state
    }

    /// Try to acquire using local fallback
    pub fn try_acquire_fallback(&self) -> bool {
        self.state.try_acquire_fallback()
    }
}

/// Distributed sliding window implementation
///
/// Uses sorted sets in a distributed backend (e.g., Redis ZSET)
/// to implement sliding window algorithm across multiple workers.
///
/// # Redis Implementation
///
/// The sliding window is implemented using Redis sorted set:
/// - `{key}:window` - Sorted set of timestamps (score = timestamp, member = UUID)
///
/// Operations:
/// 1. **Acquire**: Add current timestamp to sorted set if count < limit
/// 2. **Cleanup**: Remove timestamps outside the window using ZREMRANGEBYSCORE
/// 3. **Count**: Count timestamps within window using ZCOUNT
///
/// # Example Lua Script
///
/// ```lua
/// local window_key = KEYS[1]
/// local now = tonumber(ARGV[1])
/// local window_size = tonumber(ARGV[2])
/// local max_count = tonumber(ARGV[3])
/// local uuid = ARGV[4]
///
/// local cutoff = now - window_size * 1000
/// redis.call('ZREMRANGEBYSCORE', window_key, '-inf', cutoff)
///
/// local count = redis.call('ZCARD', window_key)
/// if count < max_count then
///     redis.call('ZADD', window_key, now, uuid)
///     redis.call('EXPIRE', window_key, window_size * 2)
///     return 1
/// else
///     return 0
/// end
/// ```
#[derive(Debug, Clone)]
pub struct DistributedSlidingWindowSpec {
    state: DistributedRateLimiterState,
}

impl DistributedSlidingWindowSpec {
    /// Create a new distributed sliding window specification
    pub fn new(key: String, config: RateLimitConfig) -> Self {
        Self {
            state: DistributedRateLimiterState::new(key, config),
        }
    }

    /// Get the Lua script for atomic window acquisition
    pub fn lua_acquire_script() -> &'static str {
        r#"
        local window_key = KEYS[1]
        local now = tonumber(ARGV[1])
        local window_size = tonumber(ARGV[2])
        local max_count = tonumber(ARGV[3])
        local uuid = ARGV[4]

        local cutoff = now - window_size * 1000
        redis.call('ZREMRANGEBYSCORE', window_key, '-inf', cutoff)

        local count = redis.call('ZCARD', window_key)
        if count < max_count then
            redis.call('ZADD', window_key, now, uuid)
            redis.call('EXPIRE', window_key, window_size * 2)
            return {1, max_count - count - 1}
        else
            return {0, 0}
        end
        "#
    }

    /// Get the Lua script for querying available permits
    pub fn lua_available_script() -> &'static str {
        r#"
        local window_key = KEYS[1]
        local now = tonumber(ARGV[1])
        local window_size = tonumber(ARGV[2])
        local max_count = tonumber(ARGV[3])

        local cutoff = now - window_size * 1000
        redis.call('ZREMRANGEBYSCORE', window_key, '-inf', cutoff)

        local count = redis.call('ZCARD', window_key)
        return math.max(0, max_count - count)
        "#
    }

    /// Get the Lua script for querying time until available
    pub fn lua_time_until_script() -> &'static str {
        r#"
        local window_key = KEYS[1]
        local now = tonumber(ARGV[1])
        local window_size = tonumber(ARGV[2])
        local max_count = tonumber(ARGV[3])

        local cutoff = now - window_size * 1000
        redis.call('ZREMRANGEBYSCORE', window_key, '-inf', cutoff)

        local count = redis.call('ZCARD', window_key)
        if count < max_count then
            return 0
        else
            local oldest = redis.call('ZRANGE', window_key, 0, 0, 'WITHSCORES')
            if #oldest >= 2 then
                local oldest_timestamp = tonumber(oldest[2])
                local expires = oldest_timestamp + window_size * 1000
                return math.max(0, expires - now)
            else
                return 0
            end
        end
        "#
    }

    /// Get the maximum number of executions allowed in the window
    pub fn max_executions(&self) -> usize {
        (self.state.config.rate * self.state.config.window_size as f64).ceil() as usize
    }

    /// Get the state for implementing the distributed backend
    pub fn state(&self) -> &DistributedRateLimiterState {
        &self.state
    }

    /// Try to acquire using local fallback
    pub fn try_acquire_fallback(&self) -> bool {
        self.state.try_acquire_fallback()
    }
}

/// Distributed rate limiter coordinator
///
/// Manages distributed rate limiters for multiple task types,
/// allowing cluster-wide rate limit enforcement.
///
/// # Features
///
/// - **Per-task rate limits**: Different rate limits for each task type
/// - **Cluster-wide coordination**: Rate limits enforced across all workers
/// - **Automatic fallback**: Gracefully degrade to local rate limiting on backend failure
/// - **Configuration management**: Dynamic rate limit updates
///
/// # Example
///
/// ```rust,ignore
/// use celers_core::rate_limit::{DistributedRateLimiterCoordinator, RateLimitConfig};
///
/// let coordinator = DistributedRateLimiterCoordinator::new("myapp");
///
/// // Set cluster-wide rate limit for a task
/// coordinator.set_task_rate(
///     "send_email",
///     RateLimitConfig::new(100.0).with_burst(200),
/// );
///
/// // Try to acquire across the cluster
/// if coordinator.try_acquire("send_email").await? {
///     send_email().await?;
/// }
/// ```
#[derive(Debug, Clone)]
pub struct DistributedRateLimiterCoordinator {
    /// Application namespace for Redis keys
    namespace: String,
    /// Per-task token bucket specs
    token_buckets: Arc<RwLock<HashMap<String, DistributedTokenBucketSpec>>>,
    /// Per-task sliding window specs
    sliding_windows: Arc<RwLock<HashMap<String, DistributedSlidingWindowSpec>>>,
    /// Default configuration for tasks without specific limits
    default_config: Option<RateLimitConfig>,
}

impl DistributedRateLimiterCoordinator {
    /// Create a new distributed rate limiter coordinator
    ///
    /// # Arguments
    ///
    /// * `namespace` - Application namespace for Redis keys (e.g., "myapp")
    pub fn new(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            token_buckets: Arc::new(RwLock::new(HashMap::new())),
            sliding_windows: Arc::new(RwLock::new(HashMap::new())),
            default_config: None,
        }
    }

    /// Create with a default rate limit for all tasks
    pub fn with_default(namespace: impl Into<String>, config: RateLimitConfig) -> Self {
        Self {
            namespace: namespace.into(),
            token_buckets: Arc::new(RwLock::new(HashMap::new())),
            sliding_windows: Arc::new(RwLock::new(HashMap::new())),
            default_config: Some(config),
        }
    }

    /// Set distributed rate limit for a specific task type
    ///
    /// Creates a distributed rate limiter spec that can be used by
    /// backend implementations (e.g., Redis).
    pub fn set_task_rate(&self, task_name: impl Into<String>, config: RateLimitConfig) {
        let name = task_name.into();
        let key = format!("{}:ratelimit:{}", self.namespace, name);

        if config.sliding_window {
            if let Ok(mut guard) = self.sliding_windows.write() {
                guard.insert(name.clone(), DistributedSlidingWindowSpec::new(key, config));
            }
        } else if let Ok(mut guard) = self.token_buckets.write() {
            guard.insert(name.clone(), DistributedTokenBucketSpec::new(key, config));
        }
    }

    /// Remove rate limit for a specific task type
    pub fn remove_task_rate(&self, task_name: &str) {
        if let Ok(mut guard) = self.token_buckets.write() {
            guard.remove(task_name);
        }
        if let Ok(mut guard) = self.sliding_windows.write() {
            guard.remove(task_name);
        }
    }

    /// Get the token bucket spec for a task (if using token bucket)
    pub fn get_token_bucket_spec(&self, task_name: &str) -> Option<DistributedTokenBucketSpec> {
        if let Ok(guard) = self.token_buckets.read() {
            guard.get(task_name).cloned()
        } else {
            None
        }
    }

    /// Get the sliding window spec for a task (if using sliding window)
    pub fn get_sliding_window_spec(&self, task_name: &str) -> Option<DistributedSlidingWindowSpec> {
        if let Ok(guard) = self.sliding_windows.read() {
            guard.get(task_name).cloned()
        } else {
            None
        }
    }

    /// Check if a task has a distributed rate limit configured
    pub fn has_rate_limit(&self, task_name: &str) -> bool {
        let has_bucket = if let Ok(guard) = self.token_buckets.read() {
            guard.contains_key(task_name)
        } else {
            false
        };

        let has_window = if let Ok(guard) = self.sliding_windows.read() {
            guard.contains_key(task_name)
        } else {
            false
        };

        has_bucket || has_window || self.default_config.is_some()
    }

    /// Try to acquire using local fallback for a task
    ///
    /// This method is useful when the distributed backend is unavailable.
    pub fn try_acquire_fallback(&self, task_name: &str) -> bool {
        // Try token bucket first
        if let Some(spec) = self.get_token_bucket_spec(task_name) {
            return spec.try_acquire_fallback();
        }

        // Try sliding window
        if let Some(spec) = self.get_sliding_window_spec(task_name) {
            return spec.try_acquire_fallback();
        }

        // Use default config if available
        if let Some(ref config) = self.default_config {
            let key = format!("{}:ratelimit:{}", self.namespace, task_name);
            let spec = DistributedTokenBucketSpec::new(key, config.clone());
            return spec.try_acquire_fallback();
        }

        // No rate limit configured
        true
    }

    /// Get the Redis key for a task's rate limiter
    pub fn redis_key(&self, task_name: &str) -> String {
        format!("{}:ratelimit:{}", self.namespace, task_name)
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
