//! Circuit Breaker for Redis Operations
//!
//! Implements the circuit breaker pattern to protect against cascading failures
//! when Redis becomes unavailable or slow.
//!
//! The circuit breaker has three states:
//! - **Closed**: Normal operation, requests flow through
//! - **Open**: Failures exceeded threshold, requests are rejected immediately
//! - **HalfOpen**: Testing if Redis has recovered, limited requests allowed
//!
//! # Example
//!
//! ```rust,ignore
//! use celers_broker_redis::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
//! use std::time::Duration;
//!
//! let config = CircuitBreakerConfig::new()
//!     .with_failure_threshold(5)
//!     .with_recovery_timeout(Duration::from_secs(30));
//!
//! let breaker = CircuitBreaker::new(config);
//!
//! // Before making a Redis call
//! if breaker.allow_request() {
//!     match redis_operation().await {
//!         Ok(_) => breaker.record_success(),
//!         Err(_) => breaker.record_failure(),
//!     }
//! } else {
//!     // Circuit is open, skip Redis call
//! }
//! ```

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant};

/// Circuit breaker state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal operation - requests flow through
    Closed,
    /// Failure threshold exceeded - requests are rejected
    Open,
    /// Testing recovery - limited requests allowed
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

/// Configuration for the circuit breaker
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of consecutive failures before opening the circuit
    pub failure_threshold: u32,
    /// Duration to wait before transitioning from Open to HalfOpen
    pub recovery_timeout: Duration,
    /// Number of successful requests in HalfOpen state before closing
    pub success_threshold: u32,
    /// Duration after which failure count resets in Closed state
    pub failure_reset_timeout: Duration,
    /// Maximum number of requests allowed in HalfOpen state
    pub half_open_max_requests: u32,
}

impl CircuitBreakerConfig {
    /// Create a new configuration with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the failure threshold
    pub fn with_failure_threshold(mut self, threshold: u32) -> Self {
        self.failure_threshold = threshold;
        self
    }

    /// Set the recovery timeout
    pub fn with_recovery_timeout(mut self, timeout: Duration) -> Self {
        self.recovery_timeout = timeout;
        self
    }

    /// Set the success threshold for closing from HalfOpen
    pub fn with_success_threshold(mut self, threshold: u32) -> Self {
        self.success_threshold = threshold;
        self
    }

    /// Set the failure reset timeout
    pub fn with_failure_reset_timeout(mut self, timeout: Duration) -> Self {
        self.failure_reset_timeout = timeout;
        self
    }

    /// Set the maximum requests allowed in HalfOpen state
    pub fn with_half_open_max_requests(mut self, max_requests: u32) -> Self {
        self.half_open_max_requests = max_requests;
        self
    }
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            recovery_timeout: Duration::from_secs(30),
            success_threshold: 3,
            failure_reset_timeout: Duration::from_secs(60),
            half_open_max_requests: 3,
        }
    }
}

/// Circuit breaker statistics
#[derive(Debug, Clone, Default)]
pub struct CircuitBreakerStats {
    /// Total number of successful requests
    pub total_successes: u64,
    /// Total number of failed requests
    pub total_failures: u64,
    /// Total number of rejected requests (circuit open)
    pub total_rejected: u64,
    /// Current consecutive failure count
    pub consecutive_failures: u32,
    /// Current consecutive success count (in HalfOpen)
    pub consecutive_successes: u32,
    /// Number of times circuit has opened
    pub times_opened: u64,
    /// Number of times circuit has closed
    pub times_closed: u64,
}

/// Thread-safe circuit breaker implementation
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    state: RwLock<CircuitState>,
    consecutive_failures: AtomicU32,
    consecutive_successes: AtomicU32,
    half_open_requests: AtomicU32,
    last_failure_time: RwLock<Option<Instant>>,
    opened_at: RwLock<Option<Instant>>,
    // Statistics
    total_successes: AtomicU64,
    total_failures: AtomicU64,
    total_rejected: AtomicU64,
    times_opened: AtomicU64,
    times_closed: AtomicU64,
}

impl CircuitBreaker {
    /// Create a new circuit breaker with the given configuration
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            state: RwLock::new(CircuitState::Closed),
            consecutive_failures: AtomicU32::new(0),
            consecutive_successes: AtomicU32::new(0),
            half_open_requests: AtomicU32::new(0),
            last_failure_time: RwLock::new(None),
            opened_at: RwLock::new(None),
            total_successes: AtomicU64::new(0),
            total_failures: AtomicU64::new(0),
            total_rejected: AtomicU64::new(0),
            times_opened: AtomicU64::new(0),
            times_closed: AtomicU64::new(0),
        }
    }

    /// Create a circuit breaker with default configuration
    pub fn with_defaults() -> Self {
        Self::new(CircuitBreakerConfig::default())
    }

    /// Get the current state of the circuit breaker
    pub fn state(&self) -> CircuitState {
        *self.state.read().expect("lock should not be poisoned")
    }

    /// Check if recovery timeout has passed (for Open -> HalfOpen transition)
    fn should_transition_to_half_open(&self) -> bool {
        let state = *self.state.read().expect("lock should not be poisoned");
        if state == CircuitState::Open {
            if let Some(opened_at) = *self.opened_at.read().expect("lock should not be poisoned") {
                return opened_at.elapsed() >= self.config.recovery_timeout;
            }
        }
        false
    }

    /// Check if a request should be allowed
    pub fn allow_request(&self) -> bool {
        // Check if we should transition from Open to HalfOpen first
        if self.should_transition_to_half_open() {
            self.transition_to_half_open();
        }

        let current_state = self.state();

        match current_state {
            CircuitState::Closed => {
                // Check if failure count should be reset
                if let Some(last_failure) = *self
                    .last_failure_time
                    .read()
                    .expect("lock should not be poisoned")
                {
                    if last_failure.elapsed() >= self.config.failure_reset_timeout {
                        self.consecutive_failures.store(0, Ordering::SeqCst);
                    }
                }
                true
            }
            CircuitState::Open => {
                self.total_rejected.fetch_add(1, Ordering::SeqCst);
                false
            }
            CircuitState::HalfOpen => {
                // Allow limited requests in HalfOpen state
                let current = self.half_open_requests.fetch_add(1, Ordering::SeqCst);
                if current < self.config.half_open_max_requests {
                    true
                } else {
                    self.half_open_requests.fetch_sub(1, Ordering::SeqCst);
                    self.total_rejected.fetch_add(1, Ordering::SeqCst);
                    false
                }
            }
        }
    }

    /// Record a successful operation
    pub fn record_success(&self) {
        self.total_successes.fetch_add(1, Ordering::SeqCst);

        let current_state = self.state();

        match current_state {
            CircuitState::Closed => {
                // Reset consecutive failures on success
                self.consecutive_failures.store(0, Ordering::SeqCst);
            }
            CircuitState::HalfOpen => {
                let successes = self.consecutive_successes.fetch_add(1, Ordering::SeqCst) + 1;
                if successes >= self.config.success_threshold {
                    self.transition_to_closed();
                }
            }
            CircuitState::Open => {
                // Should not happen, but ignore
            }
        }
    }

    /// Record a failed operation
    pub fn record_failure(&self) {
        self.total_failures.fetch_add(1, Ordering::SeqCst);
        *self
            .last_failure_time
            .write()
            .expect("lock should not be poisoned") = Some(Instant::now());

        let current_state = self.state();

        match current_state {
            CircuitState::Closed => {
                let failures = self.consecutive_failures.fetch_add(1, Ordering::SeqCst) + 1;
                if failures >= self.config.failure_threshold {
                    self.transition_to_open();
                }
            }
            CircuitState::HalfOpen => {
                // Any failure in HalfOpen immediately opens the circuit
                self.transition_to_open();
            }
            CircuitState::Open => {
                // Should not happen, but ignore
            }
        }
    }

    /// Force the circuit to open
    pub fn force_open(&self) {
        self.transition_to_open();
    }

    /// Force the circuit to close
    pub fn force_close(&self) {
        self.transition_to_closed();
    }

    /// Reset the circuit breaker to initial state
    pub fn reset(&self) {
        *self.state.write().expect("lock should not be poisoned") = CircuitState::Closed;
        self.consecutive_failures.store(0, Ordering::SeqCst);
        self.consecutive_successes.store(0, Ordering::SeqCst);
        self.half_open_requests.store(0, Ordering::SeqCst);
        *self
            .last_failure_time
            .write()
            .expect("lock should not be poisoned") = None;
        *self.opened_at.write().expect("lock should not be poisoned") = None;
    }

    /// Get statistics about the circuit breaker
    pub fn stats(&self) -> CircuitBreakerStats {
        CircuitBreakerStats {
            total_successes: self.total_successes.load(Ordering::SeqCst),
            total_failures: self.total_failures.load(Ordering::SeqCst),
            total_rejected: self.total_rejected.load(Ordering::SeqCst),
            consecutive_failures: self.consecutive_failures.load(Ordering::SeqCst),
            consecutive_successes: self.consecutive_successes.load(Ordering::SeqCst),
            times_opened: self.times_opened.load(Ordering::SeqCst),
            times_closed: self.times_closed.load(Ordering::SeqCst),
        }
    }

    /// Get the configuration
    pub fn config(&self) -> &CircuitBreakerConfig {
        &self.config
    }

    /// Get time remaining until transition from Open to HalfOpen
    pub fn time_until_half_open(&self) -> Option<Duration> {
        let state = self.state();
        if state != CircuitState::Open {
            return None;
        }

        if let Some(opened_at) = *self.opened_at.read().expect("lock should not be poisoned") {
            let elapsed = opened_at.elapsed();
            if elapsed < self.config.recovery_timeout {
                return Some(self.config.recovery_timeout - elapsed);
            }
            return Some(Duration::ZERO);
        }

        None
    }

    fn transition_to_open(&self) {
        let mut state = self.state.write().expect("lock should not be poisoned");

        // Always update opened_at when transitioning to Open
        *self.opened_at.write().expect("lock should not be poisoned") = Some(Instant::now());

        if *state != CircuitState::Open {
            *state = CircuitState::Open;
            self.times_opened.fetch_add(1, Ordering::SeqCst);
        }

        self.consecutive_successes.store(0, Ordering::SeqCst);
        self.half_open_requests.store(0, Ordering::SeqCst);
    }

    fn transition_to_half_open(&self) {
        let mut state = self.state.write().expect("lock should not be poisoned");
        if *state == CircuitState::Open {
            *state = CircuitState::HalfOpen;
            self.consecutive_successes.store(0, Ordering::SeqCst);
            self.half_open_requests.store(0, Ordering::SeqCst);
        }
    }

    fn transition_to_closed(&self) {
        let mut state = self.state.write().expect("lock should not be poisoned");
        if *state != CircuitState::Closed {
            *state = CircuitState::Closed;
            self.times_closed.fetch_add(1, Ordering::SeqCst);
            self.consecutive_failures.store(0, Ordering::SeqCst);
            self.consecutive_successes.store(0, Ordering::SeqCst);
            self.half_open_requests.store(0, Ordering::SeqCst);
            *self.opened_at.write().expect("lock should not be poisoned") = None;
        }
    }
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_circuit_state_display() {
        assert_eq!(CircuitState::Closed.to_string(), "closed");
        assert_eq!(CircuitState::Open.to_string(), "open");
        assert_eq!(CircuitState::HalfOpen.to_string(), "half-open");
    }

    #[test]
    fn test_default_config() {
        let config = CircuitBreakerConfig::default();
        assert_eq!(config.failure_threshold, 5);
        assert_eq!(config.recovery_timeout, Duration::from_secs(30));
        assert_eq!(config.success_threshold, 3);
    }

    #[test]
    fn test_config_builder() {
        let config = CircuitBreakerConfig::new()
            .with_failure_threshold(10)
            .with_recovery_timeout(Duration::from_secs(60))
            .with_success_threshold(5)
            .with_half_open_max_requests(10);

        assert_eq!(config.failure_threshold, 10);
        assert_eq!(config.recovery_timeout, Duration::from_secs(60));
        assert_eq!(config.success_threshold, 5);
        assert_eq!(config.half_open_max_requests, 10);
    }

    #[test]
    fn test_initial_state_is_closed() {
        let breaker = CircuitBreaker::with_defaults();
        assert_eq!(breaker.state(), CircuitState::Closed);
    }

    #[test]
    fn test_allows_requests_when_closed() {
        let breaker = CircuitBreaker::with_defaults();
        assert!(breaker.allow_request());
    }

    #[test]
    fn test_opens_after_failure_threshold() {
        let config = CircuitBreakerConfig::new().with_failure_threshold(3);
        let breaker = CircuitBreaker::new(config);

        // Record failures up to threshold
        for _ in 0..3 {
            assert!(breaker.allow_request());
            breaker.record_failure();
        }

        // Circuit should be open now
        assert_eq!(breaker.state(), CircuitState::Open);
    }

    #[test]
    fn test_rejects_requests_when_open() {
        let config = CircuitBreakerConfig::new()
            .with_failure_threshold(1)
            .with_recovery_timeout(Duration::from_secs(3600)); // Long timeout

        let breaker = CircuitBreaker::new(config);

        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Open);
        assert!(!breaker.allow_request());
    }

    #[test]
    fn test_success_resets_failure_count() {
        let config = CircuitBreakerConfig::new().with_failure_threshold(3);
        let breaker = CircuitBreaker::new(config);

        // Record some failures
        breaker.record_failure();
        breaker.record_failure();

        // Record success - should reset failures
        breaker.record_success();

        // Now we need 3 more failures to open
        breaker.record_failure();
        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Closed);

        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Open);
    }

    #[test]
    fn test_force_open() {
        let breaker = CircuitBreaker::with_defaults();
        breaker.force_open();
        assert_eq!(breaker.state(), CircuitState::Open);
    }

    #[test]
    fn test_force_close() {
        let config = CircuitBreakerConfig::new().with_failure_threshold(1);
        let breaker = CircuitBreaker::new(config);

        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Open);

        breaker.force_close();
        assert_eq!(breaker.state(), CircuitState::Closed);
    }

    #[test]
    fn test_reset() {
        let config = CircuitBreakerConfig::new().with_failure_threshold(1);
        let breaker = CircuitBreaker::new(config);

        breaker.record_failure();
        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Open);

        breaker.reset();
        assert_eq!(breaker.state(), CircuitState::Closed);
        assert_eq!(breaker.stats().consecutive_failures, 0);
    }

    #[test]
    fn test_stats() {
        let breaker = CircuitBreaker::with_defaults();

        breaker.record_success();
        breaker.record_success();
        breaker.record_failure();

        let stats = breaker.stats();
        assert_eq!(stats.total_successes, 2);
        assert_eq!(stats.total_failures, 1);
    }

    #[test]
    fn test_half_open_to_closed_on_success() {
        let config = CircuitBreakerConfig::new()
            .with_failure_threshold(1)
            .with_success_threshold(2)
            .with_recovery_timeout(Duration::from_millis(1));

        let breaker = CircuitBreaker::new(config);

        // Open the circuit
        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Open);

        // Wait for recovery timeout
        std::thread::sleep(Duration::from_millis(5));

        // Should transition to HalfOpen
        assert!(breaker.allow_request());
        assert_eq!(breaker.state(), CircuitState::HalfOpen);

        // Record successes to close
        breaker.record_success();
        breaker.record_success();

        assert_eq!(breaker.state(), CircuitState::Closed);
    }

    #[test]
    fn test_half_open_to_open_on_failure() {
        let config = CircuitBreakerConfig::new()
            .with_failure_threshold(1)
            .with_recovery_timeout(Duration::from_millis(1));

        let breaker = CircuitBreaker::new(config);

        // Open the circuit
        breaker.record_failure();

        // Wait for recovery timeout
        std::thread::sleep(Duration::from_millis(5));

        // Allow request (transitions to HalfOpen)
        assert!(breaker.allow_request());

        // Failure in HalfOpen opens circuit again
        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Open);
    }
}
