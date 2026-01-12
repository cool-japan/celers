//! Circuit breaker pattern for AWS SQS API resilience
//!
//! This module provides a circuit breaker implementation to protect against
//! cascading failures when AWS SQS API calls fail repeatedly. It helps prevent
//! overwhelming the AWS API with requests when it's experiencing issues.
//!
//! # Circuit Breaker States
//!
//! - **Closed**: Normal operation, requests pass through
//! - **Open**: Too many failures, requests fail fast without calling AWS API
//! - **HalfOpen**: Testing if AWS API has recovered, allows limited requests
//!
//! # Examples
//!
//! ```
//! use celers_broker_sqs::circuit_breaker::*;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create circuit breaker with 5 failures threshold and 30 second timeout
//! let mut breaker = CircuitBreaker::new(5, 30);
//!
//! // Execute operation through circuit breaker
//! let result = breaker.call(|| async {
//!     // Your AWS SQS operation here
//!     Ok::<_, String>("success")
//! }).await;
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{debug, warn};

/// Circuit breaker state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Circuit is closed - normal operation
    Closed,
    /// Circuit is open - failing fast
    Open,
    /// Circuit is half-open - testing recovery
    HalfOpen,
}

/// Circuit breaker statistics
#[derive(Debug, Clone, Default)]
pub struct CircuitBreakerStats {
    /// Total successful requests
    pub success_count: u64,
    /// Total failed requests
    pub failure_count: u64,
    /// Total rejected requests (circuit open)
    pub rejected_count: u64,
    /// Current consecutive failures
    pub consecutive_failures: usize,
    /// Last state change timestamp
    pub last_state_change: Option<Instant>,
}

/// Circuit breaker for protecting against cascading AWS API failures
#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    state: Arc<Mutex<CircuitState>>,
    stats: Arc<Mutex<CircuitBreakerStats>>,
    /// Number of consecutive failures before opening circuit
    failure_threshold: usize,
    /// Timeout in seconds before attempting recovery (half-open)
    timeout_secs: u64,
}

impl CircuitBreaker {
    /// Create a new circuit breaker
    ///
    /// # Arguments
    /// * `failure_threshold` - Number of failures before opening circuit (default: 5)
    /// * `timeout_secs` - Seconds to wait before attempting recovery (default: 30)
    pub fn new(failure_threshold: usize, timeout_secs: u64) -> Self {
        Self {
            state: Arc::new(Mutex::new(CircuitState::Closed)),
            stats: Arc::new(Mutex::new(CircuitBreakerStats::default())),
            failure_threshold: failure_threshold.max(1),
            timeout_secs: timeout_secs.max(1),
        }
    }

    /// Get current circuit state
    pub async fn state(&self) -> CircuitState {
        *self.state.lock().await
    }

    /// Get circuit breaker statistics
    pub async fn stats(&self) -> CircuitBreakerStats {
        self.stats.lock().await.clone()
    }

    /// Reset the circuit breaker to closed state
    pub async fn reset(&self) {
        let mut state = self.state.lock().await;
        let mut stats = self.stats.lock().await;

        *state = CircuitState::Closed;
        stats.consecutive_failures = 0;
        stats.last_state_change = Some(Instant::now());

        debug!("Circuit breaker reset to closed state");
    }

    /// Execute an operation through the circuit breaker
    ///
    /// # Type Parameters
    /// * `F` - Async function that returns a Result
    /// * `T` - Success type
    /// * `E` - Error type
    pub async fn call<F, Fut, T, E>(&self, operation: F) -> Result<T, CircuitBreakerError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        // Check if we should allow the request
        self.check_and_update_state().await?;

        // Execute the operation
        match operation().await {
            Ok(result) => {
                self.on_success().await;
                Ok(result)
            }
            Err(err) => {
                self.on_failure().await;
                Err(CircuitBreakerError::OperationFailed(err))
            }
        }
    }

    /// Check circuit state and update if necessary
    async fn check_and_update_state<E>(&self) -> Result<(), CircuitBreakerError<E>> {
        let mut state = self.state.lock().await;
        let stats = self.stats.lock().await;

        match *state {
            CircuitState::Closed => Ok(()),
            CircuitState::Open => {
                // Check if timeout has elapsed
                if let Some(last_change) = stats.last_state_change {
                    let elapsed = last_change.elapsed();
                    if elapsed >= Duration::from_secs(self.timeout_secs) {
                        // Try half-open state
                        *state = CircuitState::HalfOpen;
                        drop(state);
                        drop(stats);

                        let mut stats = self.stats.lock().await;
                        stats.last_state_change = Some(Instant::now());

                        debug!("Circuit breaker transitioning to half-open state");
                        Ok(())
                    } else {
                        // Still in timeout period
                        drop(state);
                        drop(stats);

                        let mut stats = self.stats.lock().await;
                        stats.rejected_count += 1;

                        Err(CircuitBreakerError::CircuitOpen)
                    }
                } else {
                    Err(CircuitBreakerError::CircuitOpen)
                }
            }
            CircuitState::HalfOpen => {
                // Allow request in half-open state
                Ok(())
            }
        }
    }

    /// Record a successful operation (for testing/monitoring)
    pub async fn record_success(&self) {
        self.on_success().await;
    }

    /// Record a failed operation (for testing/monitoring)
    pub async fn record_failure(&self) {
        self.on_failure().await;
    }

    /// Handle successful operation
    async fn on_success(&self) {
        let mut state = self.state.lock().await;
        let mut stats = self.stats.lock().await;

        stats.success_count += 1;
        stats.consecutive_failures = 0;

        // Close circuit if it was half-open
        if *state == CircuitState::HalfOpen {
            *state = CircuitState::Closed;
            stats.last_state_change = Some(Instant::now());
            debug!("Circuit breaker closed after successful recovery");
        }
    }

    /// Handle failed operation
    async fn on_failure(&self) {
        let mut state = self.state.lock().await;
        let mut stats = self.stats.lock().await;

        stats.failure_count += 1;
        stats.consecutive_failures += 1;

        // Open circuit if threshold exceeded
        if stats.consecutive_failures >= self.failure_threshold && *state != CircuitState::Open {
            *state = CircuitState::Open;
            stats.last_state_change = Some(Instant::now());
            warn!(
                "Circuit breaker opened after {} consecutive failures",
                stats.consecutive_failures
            );
        }

        // If in half-open and failed, go back to open
        if *state == CircuitState::HalfOpen {
            *state = CircuitState::Open;
            stats.last_state_change = Some(Instant::now());
            warn!("Circuit breaker reopened after failure in half-open state");
        }
    }
}

/// Circuit breaker error types
#[derive(Debug, thiserror::Error)]
pub enum CircuitBreakerError<E> {
    /// Circuit is open, request rejected
    #[error("Circuit breaker is open")]
    CircuitOpen,
    /// Operation failed
    #[error("Operation failed: {0}")]
    OperationFailed(E),
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self::new(5, 30)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_circuit_breaker_creation() {
        let breaker = CircuitBreaker::new(5, 30);
        assert_eq!(breaker.state().await, CircuitState::Closed);

        let stats = breaker.stats().await;
        assert_eq!(stats.success_count, 0);
        assert_eq!(stats.failure_count, 0);
    }

    #[tokio::test]
    async fn test_successful_operation() {
        let breaker = CircuitBreaker::new(5, 30);

        let result = breaker.call(|| async { Ok::<_, String>("success") }).await;
        assert!(result.is_ok());

        let stats = breaker.stats().await;
        assert_eq!(stats.success_count, 1);
        assert_eq!(stats.failure_count, 0);
    }

    #[tokio::test]
    async fn test_failed_operation() {
        let breaker = CircuitBreaker::new(5, 30);

        let result = breaker.call(|| async { Err::<String, _>("error") }).await;
        assert!(result.is_err());

        let stats = breaker.stats().await;
        assert_eq!(stats.success_count, 0);
        assert_eq!(stats.failure_count, 1);
        assert_eq!(stats.consecutive_failures, 1);
    }

    #[tokio::test]
    async fn test_circuit_opens_after_threshold() {
        let breaker = CircuitBreaker::new(3, 30);

        // Fail 3 times
        for _ in 0..3 {
            let _ = breaker.call(|| async { Err::<String, _>("error") }).await;
        }

        // Circuit should be open now
        assert_eq!(breaker.state().await, CircuitState::Open);

        // Next call should be rejected
        let result = breaker.call(|| async { Ok::<_, String>("success") }).await;
        assert!(matches!(result, Err(CircuitBreakerError::CircuitOpen)));

        let stats = breaker.stats().await;
        assert_eq!(stats.rejected_count, 1);
    }

    #[tokio::test]
    async fn test_circuit_recovery() {
        let breaker = CircuitBreaker::new(2, 1); // 1 second timeout for faster test

        // Fail twice to open circuit
        for _ in 0..2 {
            let _ = breaker.call(|| async { Err::<String, _>("error") }).await;
        }

        assert_eq!(breaker.state().await, CircuitState::Open);

        // Wait for timeout
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Should be able to call again (half-open)
        let result = breaker.call(|| async { Ok::<_, String>("success") }).await;
        assert!(result.is_ok());

        // Circuit should be closed after successful call in half-open
        assert_eq!(breaker.state().await, CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_reset() {
        let breaker = CircuitBreaker::new(2, 30);

        // Fail twice to open circuit
        for _ in 0..2 {
            let _ = breaker.call(|| async { Err::<String, _>("error") }).await;
        }

        assert_eq!(breaker.state().await, CircuitState::Open);

        // Reset circuit
        breaker.reset().await;

        assert_eq!(breaker.state().await, CircuitState::Closed);
        let stats = breaker.stats().await;
        assert_eq!(stats.consecutive_failures, 0);
    }

    #[tokio::test]
    async fn test_half_open_failure() {
        let breaker = CircuitBreaker::new(2, 1);

        // Fail twice to open circuit
        for _ in 0..2 {
            let _ = breaker.call(|| async { Err::<String, _>("error") }).await;
        }

        // Wait for timeout
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Fail in half-open state
        let _ = breaker.call(|| async { Err::<String, _>("error") }).await;

        // Should be open again
        assert_eq!(breaker.state().await, CircuitState::Open);
    }
}
