//! Retry Mechanism for Redis Operations
//!
//! Provides automatic retry with configurable backoff strategies for Redis
//! operations that may fail transiently.
//!
//! # Supported Backoff Strategies
//!
//! - **Fixed**: Wait a constant duration between retries
//! - **Linear**: Increase wait time linearly with each attempt
//! - **Exponential**: Double wait time with each attempt (with optional cap)
//! - **ExponentialWithJitter**: Exponential backoff with random jitter
//!
//! # Example
//!
//! ```rust,ignore
//! use celers_broker_redis::retry::{RetryConfig, RetryExecutor, BackoffStrategy};
//! use std::time::Duration;
//!
//! let config = RetryConfig::new()
//!     .with_max_attempts(5)
//!     .with_backoff(BackoffStrategy::exponential_with_jitter(
//!         Duration::from_millis(100),
//!         Duration::from_secs(30),
//!     ));
//!
//! let executor = RetryExecutor::new(config);
//!
//! let result = executor.execute(|| async {
//!     redis_operation().await
//! }).await;
//! ```

use std::future::Future;
use std::time::Duration;

/// Backoff strategy for retries
#[derive(Debug, Clone)]
pub enum BackoffStrategy {
    /// Fixed delay between retries
    Fixed(Duration),
    /// Linear backoff: delay = base * attempt
    Linear { base: Duration, max: Duration },
    /// Exponential backoff: delay = base * 2^attempt
    Exponential { base: Duration, max: Duration },
    /// Exponential backoff with random jitter
    ExponentialWithJitter {
        base: Duration,
        max: Duration,
        /// Jitter factor (0.0 to 1.0) - portion of delay to randomize
        jitter_factor: f64,
    },
    /// Decorrelated jitter (AWS recommended)
    DecorrelatedJitter { base: Duration, max: Duration },
}

impl BackoffStrategy {
    /// Create a fixed backoff strategy
    pub fn fixed(delay: Duration) -> Self {
        BackoffStrategy::Fixed(delay)
    }

    /// Create a linear backoff strategy
    pub fn linear(base: Duration, max: Duration) -> Self {
        BackoffStrategy::Linear { base, max }
    }

    /// Create an exponential backoff strategy
    pub fn exponential(base: Duration, max: Duration) -> Self {
        BackoffStrategy::Exponential { base, max }
    }

    /// Create an exponential backoff with jitter
    pub fn exponential_with_jitter(base: Duration, max: Duration) -> Self {
        BackoffStrategy::ExponentialWithJitter {
            base,
            max,
            jitter_factor: 0.5,
        }
    }

    /// Create decorrelated jitter backoff (AWS recommended)
    pub fn decorrelated_jitter(base: Duration, max: Duration) -> Self {
        BackoffStrategy::DecorrelatedJitter { base, max }
    }

    /// Calculate the delay for a given attempt number (1-indexed)
    pub fn delay_for_attempt(&self, attempt: u32, last_delay: Option<Duration>) -> Duration {
        match self {
            BackoffStrategy::Fixed(delay) => *delay,
            BackoffStrategy::Linear { base, max } => {
                let delay = base.saturating_mul(attempt);
                delay.min(*max)
            }
            BackoffStrategy::Exponential { base, max } => {
                let multiplier = 2u64.saturating_pow(attempt.saturating_sub(1));
                let delay = base.saturating_mul(multiplier as u32);
                delay.min(*max)
            }
            BackoffStrategy::ExponentialWithJitter {
                base,
                max,
                jitter_factor,
            } => {
                let multiplier = 2u64.saturating_pow(attempt.saturating_sub(1));
                let base_delay = base.saturating_mul(multiplier as u32);
                let capped_delay = base_delay.min(*max);

                // Add jitter
                let jitter_range = (capped_delay.as_millis() as f64 * jitter_factor) as u64;
                let jitter = if jitter_range > 0 {
                    random_u64() % jitter_range
                } else {
                    0
                };

                Duration::from_millis(capped_delay.as_millis() as u64 + jitter)
            }
            BackoffStrategy::DecorrelatedJitter { base, max } => {
                // AWS recommended: sleep = min(cap, random_between(base, sleep * 3))
                let last = last_delay.unwrap_or(*base);
                let range_max = last.as_millis() as u64 * 3;
                let range_min = base.as_millis() as u64;

                let sleep = if range_max > range_min {
                    range_min + (random_u64() % (range_max - range_min))
                } else {
                    range_min
                };

                Duration::from_millis(sleep).min(*max)
            }
        }
    }
}

impl Default for BackoffStrategy {
    fn default() -> Self {
        BackoffStrategy::exponential_with_jitter(
            Duration::from_millis(100),
            Duration::from_secs(30),
        )
    }
}

/// Configuration for retry behavior
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts (0 = no retries)
    pub max_attempts: u32,
    /// Backoff strategy
    pub backoff: BackoffStrategy,
    /// Whether to retry on timeout errors
    pub retry_on_timeout: bool,
    /// Whether to retry on connection errors
    pub retry_on_connection_error: bool,
}

impl RetryConfig {
    /// Create a new retry configuration with defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the maximum number of attempts
    pub fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Set the backoff strategy
    pub fn with_backoff(mut self, backoff: BackoffStrategy) -> Self {
        self.backoff = backoff;
        self
    }

    /// Set whether to retry on timeout errors
    pub fn with_retry_on_timeout(mut self, retry: bool) -> Self {
        self.retry_on_timeout = retry;
        self
    }

    /// Set whether to retry on connection errors
    pub fn with_retry_on_connection_error(mut self, retry: bool) -> Self {
        self.retry_on_connection_error = retry;
        self
    }

    /// Create a configuration with no retries
    pub fn no_retry() -> Self {
        Self {
            max_attempts: 0,
            ..Default::default()
        }
    }

    /// Create a configuration for aggressive retries
    pub fn aggressive() -> Self {
        Self {
            max_attempts: 10,
            backoff: BackoffStrategy::exponential_with_jitter(
                Duration::from_millis(50),
                Duration::from_secs(10),
            ),
            retry_on_timeout: true,
            retry_on_connection_error: true,
        }
    }

    /// Create a configuration for conservative retries
    pub fn conservative() -> Self {
        Self {
            max_attempts: 3,
            backoff: BackoffStrategy::exponential(
                Duration::from_millis(500),
                Duration::from_secs(30),
            ),
            retry_on_timeout: true,
            retry_on_connection_error: true,
        }
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            backoff: BackoffStrategy::default(),
            retry_on_timeout: true,
            retry_on_connection_error: true,
        }
    }
}

/// Error classification for retry decisions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Connection error (likely transient)
    Connection,
    /// Timeout error (may be transient)
    Timeout,
    /// Command error (usually not transient)
    Command,
    /// Authentication error (not transient)
    Authentication,
    /// Unknown error type
    Unknown,
}

impl ErrorKind {
    /// Classify a Redis error
    pub fn from_redis_error(err: &redis::RedisError) -> Self {
        use redis::ErrorKind as RedisErrorKind;

        match err.kind() {
            RedisErrorKind::Io => ErrorKind::Connection,
            RedisErrorKind::ClusterConnectionNotFound => ErrorKind::Connection,
            RedisErrorKind::AuthenticationFailed => ErrorKind::Authentication,
            RedisErrorKind::UnexpectedReturnType => ErrorKind::Command,
            RedisErrorKind::InvalidClientConfig => ErrorKind::Command,
            RedisErrorKind::Client => ErrorKind::Command,
            RedisErrorKind::Server(server_err) => {
                use redis::ServerErrorKind;
                match server_err {
                    ServerErrorKind::BusyLoading
                    | ServerErrorKind::TryAgain
                    | ServerErrorKind::ClusterDown
                    | ServerErrorKind::MasterDown => ErrorKind::Connection,
                    ServerErrorKind::ResponseError
                    | ServerErrorKind::ExecAbort
                    | ServerErrorKind::NoScript
                    | ServerErrorKind::CrossSlot
                    | ServerErrorKind::ReadOnly
                    | ServerErrorKind::NoPerm => ErrorKind::Command,
                    _ => ErrorKind::Unknown,
                }
            }
            _ => ErrorKind::Unknown,
        }
    }

    /// Check if this error kind should be retried based on config
    pub fn should_retry(&self, config: &RetryConfig) -> bool {
        match self {
            ErrorKind::Connection => config.retry_on_connection_error,
            ErrorKind::Timeout => config.retry_on_timeout,
            ErrorKind::Command => false,
            ErrorKind::Authentication => false,
            ErrorKind::Unknown => config.retry_on_connection_error,
        }
    }
}

/// Result of a retry operation
#[derive(Debug)]
pub struct RetryResult<T, E> {
    /// The result (success or final error)
    pub result: Result<T, E>,
    /// Number of attempts made
    pub attempts: u32,
    /// Total time spent retrying
    pub total_duration: Duration,
}

impl<T, E> RetryResult<T, E> {
    /// Check if the operation succeeded
    pub fn is_success(&self) -> bool {
        self.result.is_ok()
    }

    /// Get the result, discarding retry metadata
    pub fn into_result(self) -> Result<T, E> {
        self.result
    }
}

/// Retry executor for async operations
pub struct RetryExecutor {
    config: RetryConfig,
}

impl RetryExecutor {
    /// Create a new retry executor
    pub fn new(config: RetryConfig) -> Self {
        Self { config }
    }

    /// Execute an async operation with retries
    pub async fn execute<F, Fut, T, E>(&self, operation: F) -> RetryResult<T, E>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T, E>>,
        E: std::fmt::Debug,
    {
        let start = std::time::Instant::now();
        let mut attempts = 0u32;
        let mut last_delay = None;

        loop {
            attempts += 1;

            match operation().await {
                Ok(value) => {
                    return RetryResult {
                        result: Ok(value),
                        attempts,
                        total_duration: start.elapsed(),
                    };
                }
                Err(err) => {
                    if attempts > self.config.max_attempts {
                        return RetryResult {
                            result: Err(err),
                            attempts,
                            total_duration: start.elapsed(),
                        };
                    }

                    // Calculate delay and wait
                    let delay = self.config.backoff.delay_for_attempt(attempts, last_delay);
                    last_delay = Some(delay);
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    /// Execute an async operation with retries, using error classification
    pub async fn execute_with_classification<F, Fut, T>(
        &self,
        operation: F,
    ) -> RetryResult<T, redis::RedisError>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<T, redis::RedisError>>,
    {
        let start = std::time::Instant::now();
        let mut attempts = 0u32;
        let mut last_delay = None;

        loop {
            attempts += 1;

            match operation().await {
                Ok(value) => {
                    return RetryResult {
                        result: Ok(value),
                        attempts,
                        total_duration: start.elapsed(),
                    };
                }
                Err(err) => {
                    let error_kind = ErrorKind::from_redis_error(&err);

                    if attempts > self.config.max_attempts || !error_kind.should_retry(&self.config)
                    {
                        return RetryResult {
                            result: Err(err),
                            attempts,
                            total_duration: start.elapsed(),
                        };
                    }

                    // Calculate delay and wait
                    let delay = self.config.backoff.delay_for_attempt(attempts, last_delay);
                    last_delay = Some(delay);
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    /// Get the configuration
    pub fn config(&self) -> &RetryConfig {
        &self.config
    }
}

impl Default for RetryExecutor {
    fn default() -> Self {
        Self::new(RetryConfig::default())
    }
}

/// Simple pseudo-random number generator (not cryptographic)
fn random_u64() -> u64 {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};

    let state = RandomState::new();
    let mut hasher = state.build_hasher();
    hasher.write_u64(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64,
    );
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fixed_backoff() {
        let backoff = BackoffStrategy::fixed(Duration::from_millis(100));

        assert_eq!(
            backoff.delay_for_attempt(1, None),
            Duration::from_millis(100)
        );
        assert_eq!(
            backoff.delay_for_attempt(5, None),
            Duration::from_millis(100)
        );
    }

    #[test]
    fn test_linear_backoff() {
        let backoff = BackoffStrategy::linear(Duration::from_millis(100), Duration::from_secs(1));

        assert_eq!(
            backoff.delay_for_attempt(1, None),
            Duration::from_millis(100)
        );
        assert_eq!(
            backoff.delay_for_attempt(2, None),
            Duration::from_millis(200)
        );
        assert_eq!(
            backoff.delay_for_attempt(3, None),
            Duration::from_millis(300)
        );
        // Should cap at max
        assert_eq!(backoff.delay_for_attempt(20, None), Duration::from_secs(1));
    }

    #[test]
    fn test_exponential_backoff() {
        let backoff =
            BackoffStrategy::exponential(Duration::from_millis(100), Duration::from_secs(10));

        assert_eq!(
            backoff.delay_for_attempt(1, None),
            Duration::from_millis(100)
        );
        assert_eq!(
            backoff.delay_for_attempt(2, None),
            Duration::from_millis(200)
        );
        assert_eq!(
            backoff.delay_for_attempt(3, None),
            Duration::from_millis(400)
        );
        assert_eq!(
            backoff.delay_for_attempt(4, None),
            Duration::from_millis(800)
        );
    }

    #[test]
    fn test_exponential_backoff_caps_at_max() {
        let backoff =
            BackoffStrategy::exponential(Duration::from_millis(100), Duration::from_millis(500));

        assert_eq!(
            backoff.delay_for_attempt(10, None),
            Duration::from_millis(500)
        );
    }

    #[test]
    fn test_retry_config_builder() {
        let config = RetryConfig::new()
            .with_max_attempts(5)
            .with_backoff(BackoffStrategy::fixed(Duration::from_millis(50)))
            .with_retry_on_timeout(false);

        assert_eq!(config.max_attempts, 5);
        assert!(!config.retry_on_timeout);
    }

    #[test]
    fn test_no_retry_config() {
        let config = RetryConfig::no_retry();
        assert_eq!(config.max_attempts, 0);
    }

    #[test]
    fn test_aggressive_config() {
        let config = RetryConfig::aggressive();
        assert_eq!(config.max_attempts, 10);
        assert!(config.retry_on_timeout);
        assert!(config.retry_on_connection_error);
    }

    #[test]
    fn test_conservative_config() {
        let config = RetryConfig::conservative();
        assert_eq!(config.max_attempts, 3);
    }

    #[test]
    fn test_error_kind_retry_decision() {
        let config = RetryConfig::default();

        assert!(ErrorKind::Connection.should_retry(&config));
        assert!(ErrorKind::Timeout.should_retry(&config));
        assert!(!ErrorKind::Command.should_retry(&config));
        assert!(!ErrorKind::Authentication.should_retry(&config));
    }

    #[test]
    fn test_error_kind_with_disabled_retries() {
        let config = RetryConfig::new()
            .with_retry_on_timeout(false)
            .with_retry_on_connection_error(false);

        assert!(!ErrorKind::Connection.should_retry(&config));
        assert!(!ErrorKind::Timeout.should_retry(&config));
    }

    #[tokio::test]
    async fn test_retry_executor_success_first_try() {
        let config = RetryConfig::new().with_max_attempts(3);
        let executor = RetryExecutor::new(config);

        let result = executor
            .execute(|| async { Ok::<_, &str>("success") })
            .await;

        assert!(result.is_success());
        assert_eq!(result.attempts, 1);
        assert_eq!(result.into_result().unwrap(), "success");
    }

    #[tokio::test]
    async fn test_retry_executor_eventual_success() {
        let config = RetryConfig::new()
            .with_max_attempts(5)
            .with_backoff(BackoffStrategy::fixed(Duration::from_millis(1)));
        let executor = RetryExecutor::new(config);

        let counter = std::sync::atomic::AtomicU32::new(0);

        let result = executor
            .execute(|| async {
                let count = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if count < 2 {
                    Err("fail")
                } else {
                    Ok("success")
                }
            })
            .await;

        assert!(result.is_success());
        assert_eq!(result.attempts, 3);
    }

    #[tokio::test]
    async fn test_retry_executor_exhausted() {
        let config = RetryConfig::new()
            .with_max_attempts(2)
            .with_backoff(BackoffStrategy::fixed(Duration::from_millis(1)));
        let executor = RetryExecutor::new(config);

        let result = executor
            .execute(|| async { Err::<(), _>("always fail") })
            .await;

        assert!(!result.is_success());
        assert_eq!(result.attempts, 3); // 1 initial + 2 retries
    }
}
