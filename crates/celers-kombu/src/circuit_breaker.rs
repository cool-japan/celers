//! Circuit breaker pattern types.

use serde::{Deserialize, Serialize};
use std::time::Duration;

// =============================================================================
// Circuit Breaker
// =============================================================================

/// Circuit breaker state
///
/// # Examples
///
/// ```
/// use celers_kombu::CircuitState;
///
/// let closed = CircuitState::Closed;
/// assert_eq!(closed.to_string(), "closed");
///
/// let open = CircuitState::Open;
/// assert_eq!(open.to_string(), "open");
///
/// let half_open = CircuitState::HalfOpen;
/// assert_eq!(half_open.to_string(), "half-open");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CircuitState {
    /// Circuit is closed, requests flow normally
    Closed,
    /// Circuit is open, requests are rejected
    Open,
    /// Circuit is half-open, testing if service recovered
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
///
/// # Examples
///
/// ```
/// use celers_kombu::CircuitBreakerConfig;
/// use std::time::Duration;
///
/// let config = CircuitBreakerConfig::new()
///     .with_failure_threshold(3)
///     .with_success_threshold(2)
///     .with_open_duration(Duration::from_secs(30))
///     .with_failure_window(Duration::from_secs(60));
///
/// assert_eq!(config.failure_threshold, 3);
/// assert_eq!(config.success_threshold, 2);
/// assert_eq!(config.open_duration, Duration::from_secs(30));
/// assert_eq!(config.failure_window, Duration::from_secs(60));
/// ```
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of failures before opening the circuit
    pub failure_threshold: u32,
    /// Number of successes in half-open state to close circuit
    pub success_threshold: u32,
    /// Duration to keep circuit open before trying again
    pub open_duration: Duration,
    /// Time window for counting failures
    pub failure_window: Duration,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 2,
            open_duration: Duration::from_secs(30),
            failure_window: Duration::from_secs(60),
        }
    }
}

impl CircuitBreakerConfig {
    /// Create a new circuit breaker config
    pub fn new() -> Self {
        Self::default()
    }

    /// Set failure threshold
    pub fn with_failure_threshold(mut self, threshold: u32) -> Self {
        self.failure_threshold = threshold;
        self
    }

    /// Set success threshold for half-open state
    pub fn with_success_threshold(mut self, threshold: u32) -> Self {
        self.success_threshold = threshold;
        self
    }

    /// Set open duration
    pub fn with_open_duration(mut self, duration: Duration) -> Self {
        self.open_duration = duration;
        self
    }

    /// Set failure window
    pub fn with_failure_window(mut self, window: Duration) -> Self {
        self.failure_window = window;
        self
    }
}

/// Circuit breaker statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CircuitBreakerStats {
    /// Current state
    pub state: String,
    /// Total requests
    pub total_requests: u64,
    /// Successful requests
    pub successful_requests: u64,
    /// Failed requests
    pub failed_requests: u64,
    /// Rejected requests (when open)
    pub rejected_requests: u64,
    /// Number of times circuit opened
    pub times_opened: u64,
    /// Consecutive failures
    pub consecutive_failures: u32,
    /// Consecutive successes (in half-open)
    pub consecutive_successes: u32,
}

impl CircuitBreakerStats {
    /// Calculate success rate
    pub fn success_rate(&self) -> f64 {
        if self.total_requests == 0 {
            1.0
        } else {
            self.successful_requests as f64 / self.total_requests as f64
        }
    }
}

/// Circuit breaker trait
pub trait CircuitBreaker: Send + Sync {
    /// Get current state
    fn state(&self) -> CircuitState;

    /// Get configuration
    fn config(&self) -> &CircuitBreakerConfig;

    /// Get statistics
    fn stats(&self) -> CircuitBreakerStats;

    /// Record a success
    fn record_success(&mut self);

    /// Record a failure
    fn record_failure(&mut self);

    /// Check if request is allowed
    fn is_allowed(&self) -> bool;

    /// Reset the circuit breaker
    fn reset(&mut self);
}
