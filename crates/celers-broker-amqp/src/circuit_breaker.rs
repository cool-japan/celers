//! Circuit Breaker pattern for AMQP connection resilience
//!
//! This module provides a circuit breaker implementation to prevent cascading failures
//! when the AMQP broker becomes unavailable or experiences issues.
//!
//! # Circuit Breaker States
//!
//! - **Closed**: Normal operation, requests pass through
//! - **Open**: Failures exceeded threshold, requests fail fast
//! - **HalfOpen**: Testing if service recovered, limited requests allowed
//!
//! # Examples
//!
//! ```
//! use celers_broker_amqp::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
//! use std::time::Duration;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = CircuitBreakerConfig {
//!     failure_threshold: 5,
//!     success_threshold: 2,
//!     timeout: Duration::from_secs(60),
//!     half_open_max_calls: 3,
//! };
//!
//! let mut breaker = CircuitBreaker::new(config);
//!
//! // Execute operation through circuit breaker
//! match breaker.call(async { Ok::<(), String>(()) }).await {
//!     Ok(_) => println!("Operation succeeded"),
//!     Err(e) => println!("Operation failed: {:?}", e),
//! }
//! # Ok(())
//! # }
//! ```

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Circuit breaker state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CircuitState {
    /// Circuit is closed, requests pass through normally
    Closed,
    /// Circuit is open, requests fail fast
    Open,
    /// Circuit is half-open, testing recovery
    HalfOpen,
}

impl std::fmt::Display for CircuitState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CircuitState::Closed => write!(f, "closed"),
            CircuitState::Open => write!(f, "open"),
            CircuitState::HalfOpen => write!(f, "half-open"),
        }
    }
}

/// Circuit breaker configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    /// Number of failures before opening circuit
    pub failure_threshold: usize,
    /// Number of successes needed to close circuit from half-open state
    pub success_threshold: usize,
    /// Time to wait before transitioning from open to half-open
    pub timeout: Duration,
    /// Maximum number of calls allowed in half-open state
    pub half_open_max_calls: usize,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 2,
            timeout: Duration::from_secs(60),
            half_open_max_calls: 3,
        }
    }
}

/// Circuit breaker error
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CircuitBreakerError<E> {
    /// Circuit is open, request rejected
    CircuitOpen,
    /// Operation failed
    OperationFailed(E),
    /// Half-open limit reached
    HalfOpenLimitReached,
}

impl<E: std::fmt::Display> std::fmt::Display for CircuitBreakerError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CircuitBreakerError::CircuitOpen => write!(f, "Circuit breaker is open"),
            CircuitBreakerError::OperationFailed(e) => write!(f, "Operation failed: {}", e),
            CircuitBreakerError::HalfOpenLimitReached => {
                write!(f, "Half-open call limit reached")
            }
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for CircuitBreakerError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CircuitBreakerError::OperationFailed(e) => Some(e),
            _ => None,
        }
    }
}

/// Internal state of circuit breaker
#[derive(Debug)]
struct CircuitBreakerState {
    state: CircuitState,
    failure_count: usize,
    success_count: usize,
    last_failure_time: Option<Instant>,
    half_open_calls: usize,
}

/// Circuit breaker for fault tolerance
#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    state: Arc<Mutex<CircuitBreakerState>>,
}

impl CircuitBreaker {
    /// Create a new circuit breaker with the given configuration
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            state: Arc::new(Mutex::new(CircuitBreakerState {
                state: CircuitState::Closed,
                failure_count: 0,
                success_count: 0,
                last_failure_time: None,
                half_open_calls: 0,
            })),
        }
    }

    /// Get current circuit state
    pub async fn get_state(&self) -> CircuitState {
        let state = self.state.lock().await;
        state.state
    }

    /// Get circuit breaker metrics
    pub async fn get_metrics(&self) -> CircuitBreakerMetrics {
        let state = self.state.lock().await;
        CircuitBreakerMetrics {
            state: state.state,
            failure_count: state.failure_count,
            success_count: state.success_count,
            half_open_calls: state.half_open_calls,
        }
    }

    /// Execute operation through circuit breaker
    pub async fn call<F, T, E>(&self, operation: F) -> Result<T, CircuitBreakerError<E>>
    where
        F: std::future::Future<Output = Result<T, E>>,
    {
        // Check if circuit should transition from open to half-open
        self.check_and_transition_to_half_open().await;

        // Check current state
        let current_state = {
            let state = self.state.lock().await;
            if state.state == CircuitState::Open {
                return Err(CircuitBreakerError::CircuitOpen);
            }
            if state.state == CircuitState::HalfOpen
                && state.half_open_calls >= self.config.half_open_max_calls
            {
                return Err(CircuitBreakerError::HalfOpenLimitReached);
            }
            state.state
        };

        // Increment half-open calls if in half-open state
        if current_state == CircuitState::HalfOpen {
            let mut state = self.state.lock().await;
            state.half_open_calls += 1;
        }

        // Execute operation
        match operation.await {
            Ok(result) => {
                self.on_success().await;
                Ok(result)
            }
            Err(error) => {
                self.on_failure().await;
                Err(CircuitBreakerError::OperationFailed(error))
            }
        }
    }

    /// Handle successful operation
    async fn on_success(&self) {
        let mut state = self.state.lock().await;

        match state.state {
            CircuitState::Closed => {
                // Reset failure count
                state.failure_count = 0;
            }
            CircuitState::HalfOpen => {
                state.success_count += 1;
                if state.success_count >= self.config.success_threshold {
                    // Transition to closed
                    state.state = CircuitState::Closed;
                    state.failure_count = 0;
                    state.success_count = 0;
                    state.half_open_calls = 0;
                }
            }
            CircuitState::Open => {
                // Should not happen, but reset if it does
                state.state = CircuitState::Closed;
                state.failure_count = 0;
                state.success_count = 0;
            }
        }
    }

    /// Handle failed operation
    async fn on_failure(&self) {
        let mut state = self.state.lock().await;

        state.failure_count += 1;
        state.last_failure_time = Some(Instant::now());

        match state.state {
            CircuitState::Closed => {
                if state.failure_count >= self.config.failure_threshold {
                    // Transition to open
                    state.state = CircuitState::Open;
                    state.success_count = 0;
                }
            }
            CircuitState::HalfOpen => {
                // Transition back to open on any failure
                state.state = CircuitState::Open;
                state.success_count = 0;
                state.half_open_calls = 0;
            }
            CircuitState::Open => {
                // Already open, nothing to do
            }
        }
    }

    /// Check if circuit should transition from open to half-open
    async fn check_and_transition_to_half_open(&self) {
        let mut state = self.state.lock().await;

        if state.state == CircuitState::Open {
            if let Some(last_failure) = state.last_failure_time {
                if last_failure.elapsed() >= self.config.timeout {
                    // Transition to half-open
                    state.state = CircuitState::HalfOpen;
                    state.half_open_calls = 0;
                    state.success_count = 0;
                }
            }
        }
    }

    /// Reset circuit breaker to closed state
    pub async fn reset(&self) {
        let mut state = self.state.lock().await;
        state.state = CircuitState::Closed;
        state.failure_count = 0;
        state.success_count = 0;
        state.half_open_calls = 0;
        state.last_failure_time = None;
    }
}

/// Circuit breaker metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerMetrics {
    /// Current circuit state
    pub state: CircuitState,
    /// Current failure count
    pub failure_count: usize,
    /// Current success count (in half-open state)
    pub success_count: usize,
    /// Current half-open calls count
    pub half_open_calls: usize,
}

impl CircuitBreakerMetrics {
    /// Check if circuit is healthy (closed state)
    pub fn is_healthy(&self) -> bool {
        self.state == CircuitState::Closed
    }

    /// Check if circuit is open
    pub fn is_open(&self) -> bool {
        self.state == CircuitState::Open
    }

    /// Check if circuit is half-open
    pub fn is_half_open(&self) -> bool {
        self.state == CircuitState::HalfOpen
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_circuit_breaker_closed_to_open() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            success_threshold: 2,
            timeout: Duration::from_secs(1),
            half_open_max_calls: 2,
        };

        let breaker = CircuitBreaker::new(config);

        // Initial state should be closed
        assert_eq!(breaker.get_state().await, CircuitState::Closed);

        // Simulate failures
        for _ in 0..3 {
            let _result: Result<(), CircuitBreakerError<String>> = breaker
                .call(async { Err::<(), String>("error".to_string()) })
                .await;
        }

        // Circuit should now be open
        assert_eq!(breaker.get_state().await, CircuitState::Open);
    }

    #[tokio::test]
    async fn test_circuit_breaker_open_rejects_calls() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            success_threshold: 2,
            timeout: Duration::from_secs(60),
            half_open_max_calls: 2,
        };

        let breaker = CircuitBreaker::new(config);

        // Cause failures to open circuit
        for _ in 0..2 {
            let _result: Result<(), CircuitBreakerError<String>> = breaker
                .call(async { Err::<(), String>("error".to_string()) })
                .await;
        }

        // Circuit should be open
        assert_eq!(breaker.get_state().await, CircuitState::Open);

        // Next call should be rejected
        let result: Result<(), CircuitBreakerError<String>> =
            breaker.call(async { Ok::<(), String>(()) }).await;
        assert!(matches!(result, Err(CircuitBreakerError::CircuitOpen)));
    }

    #[tokio::test]
    async fn test_circuit_breaker_half_open_transition() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            success_threshold: 2,
            timeout: Duration::from_millis(100),
            half_open_max_calls: 3,
        };

        let breaker = CircuitBreaker::new(config);

        // Open the circuit
        for _ in 0..2 {
            let _result: Result<(), CircuitBreakerError<String>> = breaker
                .call(async { Err::<(), String>("error".to_string()) })
                .await;
        }

        assert_eq!(breaker.get_state().await, CircuitState::Open);

        // Wait for timeout
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Next call should transition to half-open
        let _result: Result<(), CircuitBreakerError<String>> =
            breaker.call(async { Ok::<(), String>(()) }).await;

        let metrics = breaker.get_metrics().await;
        assert_eq!(metrics.state, CircuitState::HalfOpen);
    }

    #[tokio::test]
    async fn test_circuit_breaker_recovery() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            success_threshold: 2,
            timeout: Duration::from_millis(100),
            half_open_max_calls: 3,
        };

        let breaker = CircuitBreaker::new(config);

        // Open the circuit
        for _ in 0..2 {
            let _result: Result<(), CircuitBreakerError<String>> = breaker
                .call(async { Err::<(), String>("error".to_string()) })
                .await;
        }

        // Wait for timeout
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Successful calls in half-open state
        for _ in 0..2 {
            let _result: Result<(), CircuitBreakerError<String>> =
                breaker.call(async { Ok::<(), String>(()) }).await;
        }

        // Circuit should be closed
        assert_eq!(breaker.get_state().await, CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_circuit_breaker_metrics() {
        let config = CircuitBreakerConfig::default();
        let breaker = CircuitBreaker::new(config);

        let metrics = breaker.get_metrics().await;
        assert!(metrics.is_healthy());
        assert!(!metrics.is_open());
        assert!(!metrics.is_half_open());
    }
}
