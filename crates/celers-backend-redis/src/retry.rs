//! Connection retry and exponential backoff utilities
//!
//! This module provides retry logic with exponential backoff for handling
//! transient Redis connection failures and network issues.

use crate::BackendError;
use std::time::Duration;
use tokio::time::sleep;

/// Retry strategy configuration
#[derive(Debug, Clone)]
pub struct RetryStrategy {
    /// Maximum number of retry attempts
    pub max_attempts: u32,
    /// Initial backoff duration
    pub initial_backoff: Duration,
    /// Maximum backoff duration
    pub max_backoff: Duration,
    /// Backoff multiplier (exponential factor)
    pub multiplier: f64,
    /// Whether to add jitter to backoff
    pub jitter: bool,
}

impl Default for RetryStrategy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(30),
            multiplier: 2.0,
            jitter: true,
        }
    }
}

impl RetryStrategy {
    /// Create a new retry strategy with default settings
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the maximum number of attempts
    pub fn with_max_attempts(mut self, attempts: u32) -> Self {
        self.max_attempts = attempts;
        self
    }

    /// Set the initial backoff duration
    pub fn with_initial_backoff(mut self, duration: Duration) -> Self {
        self.initial_backoff = duration;
        self
    }

    /// Set the maximum backoff duration
    pub fn with_max_backoff(mut self, duration: Duration) -> Self {
        self.max_backoff = duration;
        self
    }

    /// Set the backoff multiplier
    pub fn with_multiplier(mut self, multiplier: f64) -> Self {
        self.multiplier = multiplier;
        self
    }

    /// Enable or disable jitter
    pub fn with_jitter(mut self, jitter: bool) -> Self {
        self.jitter = jitter;
        self
    }

    /// Calculate backoff duration for a given attempt number
    pub fn backoff_duration(&self, attempt: u32) -> Duration {
        let base_ms = self.initial_backoff.as_millis() as f64;
        let exp_backoff = base_ms * self.multiplier.powi(attempt as i32);
        let capped_ms = exp_backoff.min(self.max_backoff.as_millis() as f64);

        let final_ms = if self.jitter {
            // Add jitter: random value between 0.5x and 1.5x
            use std::collections::hash_map::RandomState;
            use std::hash::{BuildHasher, Hash, Hasher};
            let mut hasher = RandomState::new().build_hasher();
            attempt.hash(&mut hasher);
            std::time::SystemTime::now().hash(&mut hasher);
            let hash = hasher.finish();
            let jitter_factor = 0.5 + (hash % 100) as f64 / 100.0;
            capped_ms * jitter_factor
        } else {
            capped_ms
        };

        Duration::from_millis(final_ms as u64)
    }

    /// Check if an error is retryable
    pub fn is_retryable(&self, error: &BackendError) -> bool {
        match error {
            BackendError::Redis(redis_err) => {
                // Retry on connection errors, I/O errors, timeouts
                redis_err.is_connection_dropped()
                    || redis_err.is_connection_refusal()
                    || redis_err.is_timeout()
                    || redis_err.is_io_error()
            }
            BackendError::Connection(_) => true,
            BackendError::NotFound(_) => false,
            BackendError::Serialization(_) => false,
        }
    }
}

/// Retry executor for async operations
pub struct RetryExecutor {
    strategy: RetryStrategy,
}

impl RetryExecutor {
    /// Create a new retry executor with the given strategy
    pub fn new(strategy: RetryStrategy) -> Self {
        Self { strategy }
    }

    /// Create a retry executor with default strategy
    pub fn with_defaults() -> Self {
        Self::new(RetryStrategy::default())
    }

    /// Execute an async operation with retry logic
    ///
    /// # Example
    /// ```no_run
    /// use celers_backend_redis::retry::{RetryExecutor, RetryStrategy};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let executor = RetryExecutor::with_defaults();
    ///
    /// let result = executor.execute(|| async {
    ///     // Your operation here
    ///     Ok::<_, celers_backend_redis::BackendError>(42)
    /// }).await?;
    ///
    /// assert_eq!(result, 42);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute<F, Fut, T>(&self, mut operation: F) -> Result<T, BackendError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, BackendError>>,
    {
        let mut attempt = 0;

        loop {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(error) => {
                    attempt += 1;

                    // Check if we should retry
                    if attempt >= self.strategy.max_attempts || !self.strategy.is_retryable(&error)
                    {
                        return Err(error);
                    }

                    // Calculate backoff and wait
                    let backoff = self.strategy.backoff_duration(attempt - 1);
                    tracing::debug!(
                        attempt = attempt,
                        backoff_ms = backoff.as_millis(),
                        error = %error,
                        "Retrying operation after backoff"
                    );

                    sleep(backoff).await;
                }
            }
        }
    }

    /// Execute with a custom retry predicate
    pub async fn execute_with_predicate<F, Fut, T, P>(
        &self,
        mut operation: F,
        should_retry: P,
    ) -> Result<T, BackendError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, BackendError>>,
        P: Fn(&BackendError) -> bool,
    {
        let mut attempt = 0;

        loop {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(error) => {
                    attempt += 1;

                    if attempt >= self.strategy.max_attempts || !should_retry(&error) {
                        return Err(error);
                    }

                    let backoff = self.strategy.backoff_duration(attempt - 1);
                    tracing::debug!(
                        attempt = attempt,
                        backoff_ms = backoff.as_millis(),
                        "Retrying with custom predicate"
                    );

                    sleep(backoff).await;
                }
            }
        }
    }

    /// Get the retry strategy
    pub fn strategy(&self) -> &RetryStrategy {
        &self.strategy
    }
}

/// Retry a connection operation with exponential backoff
///
/// This is a convenience function for retrying Redis connection operations.
pub async fn retry_connection<F, Fut, T>(max_attempts: u32, operation: F) -> Result<T, BackendError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, BackendError>>,
{
    let strategy = RetryStrategy::new().with_max_attempts(max_attempts);
    let executor = RetryExecutor::new(strategy);

    executor.execute(operation).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_strategy_default() {
        let strategy = RetryStrategy::default();
        assert_eq!(strategy.max_attempts, 3);
        assert_eq!(strategy.initial_backoff, Duration::from_millis(100));
        assert_eq!(strategy.max_backoff, Duration::from_secs(30));
        assert_eq!(strategy.multiplier, 2.0);
        assert!(strategy.jitter);
    }

    #[test]
    fn test_retry_strategy_builder() {
        let strategy = RetryStrategy::new()
            .with_max_attempts(5)
            .with_initial_backoff(Duration::from_millis(200))
            .with_max_backoff(Duration::from_secs(60))
            .with_multiplier(3.0)
            .with_jitter(false);

        assert_eq!(strategy.max_attempts, 5);
        assert_eq!(strategy.initial_backoff, Duration::from_millis(200));
        assert_eq!(strategy.max_backoff, Duration::from_secs(60));
        assert_eq!(strategy.multiplier, 3.0);
        assert!(!strategy.jitter);
    }

    #[test]
    fn test_backoff_duration_calculation() {
        let strategy = RetryStrategy::new()
            .with_initial_backoff(Duration::from_millis(100))
            .with_multiplier(2.0)
            .with_jitter(false);

        let duration0 = strategy.backoff_duration(0);
        assert_eq!(duration0, Duration::from_millis(100));

        let duration1 = strategy.backoff_duration(1);
        assert_eq!(duration1, Duration::from_millis(200));

        let duration2 = strategy.backoff_duration(2);
        assert_eq!(duration2, Duration::from_millis(400));
    }

    #[test]
    fn test_backoff_duration_capped() {
        let strategy = RetryStrategy::new()
            .with_initial_backoff(Duration::from_millis(100))
            .with_max_backoff(Duration::from_millis(500))
            .with_multiplier(2.0)
            .with_jitter(false);

        let duration5 = strategy.backoff_duration(5);
        // Would be 3200ms without cap, should be capped at 500ms
        assert_eq!(duration5, Duration::from_millis(500));
    }

    #[test]
    fn test_is_retryable() {
        let strategy = RetryStrategy::default();

        // Connection errors are retryable
        let conn_error = BackendError::Connection("test".to_string());
        assert!(strategy.is_retryable(&conn_error));

        // NotFound errors are not retryable
        let not_found = BackendError::NotFound(uuid::Uuid::new_v4());
        assert!(!strategy.is_retryable(&not_found));

        // Serialization errors are not retryable
        let ser_error = BackendError::Serialization("test".to_string());
        assert!(!strategy.is_retryable(&ser_error));
    }

    #[tokio::test]
    async fn test_retry_executor_success() {
        use std::sync::Arc;
        use tokio::sync::Mutex;

        let executor = RetryExecutor::with_defaults();
        let call_count = Arc::new(Mutex::new(0));

        let count_clone = call_count.clone();
        let result = executor
            .execute(|| {
                let count = count_clone.clone();
                async move {
                    *count.lock().await += 1;
                    Ok::<i32, BackendError>(42)
                }
            })
            .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(*call_count.lock().await, 1);
    }

    #[tokio::test]
    async fn test_retry_executor_eventual_success() {
        use std::sync::Arc;
        use tokio::sync::Mutex;

        let strategy = RetryStrategy::new()
            .with_max_attempts(3)
            .with_initial_backoff(Duration::from_millis(1))
            .with_jitter(false);
        let executor = RetryExecutor::new(strategy);
        let call_count = Arc::new(Mutex::new(0));

        let count_clone = call_count.clone();
        let result = executor
            .execute(|| {
                let count = count_clone.clone();
                async move {
                    let mut c = count.lock().await;
                    *c += 1;
                    let current = *c;
                    drop(c);
                    if current < 2 {
                        Err(BackendError::Connection("retry".to_string()))
                    } else {
                        Ok::<i32, BackendError>(42)
                    }
                }
            })
            .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(*call_count.lock().await, 2);
    }

    #[tokio::test]
    async fn test_retry_executor_max_attempts() {
        use std::sync::Arc;
        use tokio::sync::Mutex;

        let strategy = RetryStrategy::new()
            .with_max_attempts(2)
            .with_initial_backoff(Duration::from_millis(1))
            .with_jitter(false);
        let executor = RetryExecutor::new(strategy);
        let call_count = Arc::new(Mutex::new(0));

        let count_clone = call_count.clone();
        let result = executor
            .execute(|| {
                let count = count_clone.clone();
                async move {
                    *count.lock().await += 1;
                    Err::<i32, BackendError>(BackendError::Connection("fail".to_string()))
                }
            })
            .await;

        assert!(result.is_err());
        assert_eq!(*call_count.lock().await, 2);
    }

    #[tokio::test]
    async fn test_retry_executor_non_retryable() {
        use std::sync::Arc;
        use tokio::sync::Mutex;

        let executor = RetryExecutor::with_defaults();
        let call_count = Arc::new(Mutex::new(0));

        let count_clone = call_count.clone();
        let result = executor
            .execute(|| {
                let count = count_clone.clone();
                async move {
                    *count.lock().await += 1;
                    Err::<i32, BackendError>(BackendError::NotFound(uuid::Uuid::new_v4()))
                }
            })
            .await;

        assert!(result.is_err());
        assert_eq!(*call_count.lock().await, 1); // Should not retry
    }

    #[tokio::test]
    async fn test_retry_executor_with_custom_predicate() {
        use std::sync::Arc;
        use tokio::sync::Mutex;

        let strategy = RetryStrategy::new()
            .with_max_attempts(3)
            .with_initial_backoff(Duration::from_millis(1))
            .with_jitter(false);
        let executor = RetryExecutor::new(strategy);
        let call_count = Arc::new(Mutex::new(0));

        let count_clone = call_count.clone();
        let result = executor
            .execute_with_predicate(
                || {
                    let count = count_clone.clone();
                    async move {
                        let mut c = count.lock().await;
                        *c += 1;
                        let current = *c;
                        drop(c);
                        if current < 2 {
                            Err(BackendError::Serialization("custom".to_string()))
                        } else {
                            Ok::<i32, BackendError>(42)
                        }
                    }
                },
                |_| true, // Always retry
            )
            .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(*call_count.lock().await, 2);
    }
}
