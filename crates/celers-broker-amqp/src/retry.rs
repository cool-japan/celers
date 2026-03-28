//! Advanced retry strategies for AMQP operations
//!
//! This module provides sophisticated retry strategies including exponential backoff
//! with jitter to prevent thundering herd problems and improve resilience.
//!
//! # Retry Strategies
//!
//! - **Fixed Delay**: Constant delay between retries
//! - **Exponential Backoff**: Exponentially increasing delays
//! - **Exponential Backoff with Jitter**: Exponential backoff with randomization
//!
//! # Examples
//!
//! ```
//! use celers_broker_amqp::retry::{RetryStrategy, ExponentialBackoff, Jitter};
//! use std::time::Duration;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create exponential backoff with full jitter
//! let strategy = ExponentialBackoff::new(Duration::from_millis(100))
//!     .with_max_delay(Duration::from_secs(30))
//!     .with_max_retries(5)
//!     .with_jitter(Jitter::Full);
//!
//! // Use the strategy
//! for attempt in 0..strategy.max_retries() {
//!     match perform_operation().await {
//!         Ok(_) => break,
//!         Err(_) if attempt < strategy.max_retries() - 1 => {
//!             let delay = strategy.next_delay(attempt);
//!             tokio::time::sleep(delay).await;
//!         }
//!         Err(e) => return Err(e),
//!     }
//! }
//! # Ok(())
//! # }
//! # async fn perform_operation() -> Result<(), Box<dyn std::error::Error>> { Ok(()) }
//! ```

use rand::RngExt;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Jitter strategy for exponential backoff
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Jitter {
    /// No jitter, pure exponential backoff
    None,
    /// Full jitter: randomize between 0 and calculated delay
    Full,
    /// Equal jitter: half fixed delay + half randomized
    Equal,
    /// Decorrelated jitter: randomize between base and previous delay * 3
    Decorrelated,
}

impl std::fmt::Display for Jitter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Jitter::None => write!(f, "none"),
            Jitter::Full => write!(f, "full"),
            Jitter::Equal => write!(f, "equal"),
            Jitter::Decorrelated => write!(f, "decorrelated"),
        }
    }
}

/// Retry strategy trait
pub trait RetryStrategy: Send + Sync {
    /// Calculate next retry delay based on attempt number
    fn next_delay(&self, attempt: usize) -> Duration;

    /// Get maximum number of retries
    fn max_retries(&self) -> usize;

    /// Get maximum delay
    fn max_delay(&self) -> Duration;
}

/// Fixed delay retry strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixedDelay {
    delay: Duration,
    max_retries: usize,
}

impl FixedDelay {
    /// Create a new fixed delay strategy
    pub fn new(delay: Duration, max_retries: usize) -> Self {
        Self { delay, max_retries }
    }
}

impl RetryStrategy for FixedDelay {
    fn next_delay(&self, _attempt: usize) -> Duration {
        self.delay
    }

    fn max_retries(&self) -> usize {
        self.max_retries
    }

    fn max_delay(&self) -> Duration {
        self.delay
    }
}

/// Exponential backoff retry strategy with optional jitter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExponentialBackoff {
    base_delay: Duration,
    max_delay: Duration,
    max_retries: usize,
    multiplier: f64,
    jitter: Jitter,
    #[serde(skip)]
    last_delay: Option<Duration>,
}

impl ExponentialBackoff {
    /// Create a new exponential backoff strategy
    pub fn new(base_delay: Duration) -> Self {
        Self {
            base_delay,
            max_delay: Duration::from_secs(60),
            max_retries: 10,
            multiplier: 2.0,
            jitter: Jitter::None,
            last_delay: None,
        }
    }

    /// Set maximum delay between retries
    pub fn with_max_delay(mut self, max_delay: Duration) -> Self {
        self.max_delay = max_delay;
        self
    }

    /// Set maximum number of retries
    pub fn with_max_retries(mut self, max_retries: usize) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Set backoff multiplier (default: 2.0)
    pub fn with_multiplier(mut self, multiplier: f64) -> Self {
        self.multiplier = multiplier;
        self
    }

    /// Set jitter strategy
    pub fn with_jitter(mut self, jitter: Jitter) -> Self {
        self.jitter = jitter;
        self
    }

    /// Calculate exponential delay without jitter
    fn calculate_exponential_delay(&self, attempt: usize) -> Duration {
        let delay_ms = self.base_delay.as_millis() as f64 * self.multiplier.powi(attempt as i32);
        let delay = Duration::from_millis(delay_ms as u64);
        delay.min(self.max_delay)
    }

    /// Apply jitter to delay
    fn apply_jitter(&self, delay: Duration, attempt: usize) -> Duration {
        match self.jitter {
            Jitter::None => delay,
            Jitter::Full => {
                // Random value between 0 and delay
                let mut rng = rand::rng();
                let ms = delay.as_millis() as u64;
                Duration::from_millis(rng.random_range(0..=ms))
            }
            Jitter::Equal => {
                // Half fixed + half random
                let mut rng = rand::rng();
                let half_ms = delay.as_millis() as u64 / 2;
                let random_ms = rng.random_range(0..=half_ms);
                Duration::from_millis(half_ms + random_ms)
            }
            Jitter::Decorrelated => {
                // Random between base and min(max_delay, last_delay * 3)
                let mut rng = rand::rng();
                let base_ms = self.base_delay.as_millis() as u64;

                let upper_bound = if attempt == 0 {
                    (base_ms * 3).min(self.max_delay.as_millis() as u64)
                } else {
                    let prev_delay = self.last_delay.unwrap_or(self.base_delay);
                    (prev_delay.as_millis() as u64 * 3).min(self.max_delay.as_millis() as u64)
                };

                Duration::from_millis(rng.random_range(base_ms..=upper_bound))
            }
        }
    }
}

impl RetryStrategy for ExponentialBackoff {
    fn next_delay(&self, attempt: usize) -> Duration {
        let delay = self.calculate_exponential_delay(attempt);
        self.apply_jitter(delay, attempt)
    }

    fn max_retries(&self) -> usize {
        self.max_retries
    }

    fn max_delay(&self) -> Duration {
        self.max_delay
    }
}

/// Retry executor that applies a retry strategy
pub struct RetryExecutor<S: RetryStrategy> {
    strategy: S,
}

impl<S: RetryStrategy> RetryExecutor<S> {
    /// Create a new retry executor with the given strategy
    pub fn new(strategy: S) -> Self {
        Self { strategy }
    }

    /// Execute an operation with retry logic
    pub async fn execute<F, T, E>(&self, mut operation: F) -> Result<T, E>
    where
        F: FnMut() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, E>> + Send>>,
    {
        let mut last_error = None;

        for attempt in 0..self.strategy.max_retries() {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(error) => {
                    last_error = Some(error);

                    // Don't sleep after the last attempt
                    if attempt < self.strategy.max_retries() - 1 {
                        let delay = self.strategy.next_delay(attempt);
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }

        Err(last_error.expect("retry executor should have recorded at least one error"))
    }
}

/// Calculate optimal retry delay based on queue metrics
///
/// # Arguments
///
/// * `failure_count` - Number of consecutive failures
/// * `base_delay_ms` - Base delay in milliseconds
/// * `max_delay_ms` - Maximum delay in milliseconds
///
/// # Returns
///
/// Optimal retry delay with jitter
///
/// # Examples
///
/// ```
/// use celers_broker_amqp::retry::calculate_adaptive_retry_delay;
///
/// let delay = calculate_adaptive_retry_delay(3, 100, 30000);
/// // With Full jitter, delay can be anywhere from 0 to max calculated delay
/// assert!(delay.as_millis() <= 30000);
/// ```
pub fn calculate_adaptive_retry_delay(
    failure_count: usize,
    base_delay_ms: u64,
    max_delay_ms: u64,
) -> Duration {
    let strategy = ExponentialBackoff::new(Duration::from_millis(base_delay_ms))
        .with_max_delay(Duration::from_millis(max_delay_ms))
        .with_jitter(Jitter::Full);

    strategy.next_delay(failure_count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fixed_delay_strategy() {
        let strategy = FixedDelay::new(Duration::from_millis(100), 5);

        assert_eq!(strategy.max_retries(), 5);
        assert_eq!(strategy.next_delay(0), Duration::from_millis(100));
        assert_eq!(strategy.next_delay(3), Duration::from_millis(100));
    }

    #[test]
    fn test_exponential_backoff_no_jitter() {
        let strategy = ExponentialBackoff::new(Duration::from_millis(100))
            .with_max_delay(Duration::from_secs(10))
            .with_max_retries(5);

        assert_eq!(strategy.max_retries(), 5);

        // Delays should be: 100, 200, 400, 800, 1600
        assert_eq!(strategy.next_delay(0), Duration::from_millis(100));
        assert_eq!(strategy.next_delay(1), Duration::from_millis(200));
        assert_eq!(strategy.next_delay(2), Duration::from_millis(400));
        assert_eq!(strategy.next_delay(3), Duration::from_millis(800));
        assert_eq!(strategy.next_delay(4), Duration::from_millis(1600));
    }

    #[test]
    fn test_exponential_backoff_max_delay() {
        let strategy = ExponentialBackoff::new(Duration::from_millis(100))
            .with_max_delay(Duration::from_millis(500));

        // Should cap at 500ms
        assert_eq!(strategy.next_delay(10), Duration::from_millis(500));
    }

    #[test]
    fn test_exponential_backoff_with_full_jitter() {
        let strategy = ExponentialBackoff::new(Duration::from_millis(100))
            .with_max_delay(Duration::from_secs(10))
            .with_jitter(Jitter::Full);

        // With full jitter, delay should be between 0 and calculated delay
        let delay = strategy.next_delay(2);
        assert!(delay <= Duration::from_millis(400));
    }

    #[test]
    fn test_exponential_backoff_with_equal_jitter() {
        let strategy = ExponentialBackoff::new(Duration::from_millis(100))
            .with_max_delay(Duration::from_secs(10))
            .with_jitter(Jitter::Equal);

        // With equal jitter, delay should be between half and full calculated delay
        let delay = strategy.next_delay(2); // Base would be 400ms
        assert!(delay >= Duration::from_millis(200)); // At least half
        assert!(delay <= Duration::from_millis(400));
    }

    #[tokio::test]
    async fn test_retry_executor_success() {
        let strategy = FixedDelay::new(Duration::from_millis(10), 3);
        let executor = RetryExecutor::new(strategy);

        let result = executor
            .execute(|| Box::pin(async { Ok::<i32, String>(42) }))
            .await;

        assert_eq!(result, Ok(42));
    }

    #[tokio::test]
    async fn test_retry_executor_eventual_success() {
        let strategy = FixedDelay::new(Duration::from_millis(10), 3);
        let executor = RetryExecutor::new(strategy);

        let mut attempts = 0;
        let result = executor
            .execute(|| {
                attempts += 1;
                Box::pin(async move {
                    if attempts < 2 {
                        Err::<i32, String>("error".to_string())
                    } else {
                        Ok(42)
                    }
                })
            })
            .await;

        assert_eq!(result, Ok(42));
    }

    #[tokio::test]
    async fn test_retry_executor_all_failures() {
        let strategy = FixedDelay::new(Duration::from_millis(10), 3);
        let executor = RetryExecutor::new(strategy);

        let result = executor
            .execute(|| Box::pin(async { Err::<i32, String>("persistent error".to_string()) }))
            .await;

        assert!(result.is_err());
    }

    #[test]
    fn test_calculate_adaptive_retry_delay() {
        let delay = calculate_adaptive_retry_delay(0, 100, 30000);
        assert!(delay.as_millis() <= 100);

        let delay = calculate_adaptive_retry_delay(5, 100, 30000);
        assert!(delay.as_millis() <= 30000);
    }

    #[test]
    fn test_jitter_display() {
        assert_eq!(Jitter::None.to_string(), "none");
        assert_eq!(Jitter::Full.to_string(), "full");
        assert_eq!(Jitter::Equal.to_string(), "equal");
        assert_eq!(Jitter::Decorrelated.to_string(), "decorrelated");
    }
}
