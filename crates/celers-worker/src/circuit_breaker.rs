//! Circuit breaker implementation for task execution resilience
//!
//! Prevents cascading failures by tracking task failures and temporarily
//! blocking execution when failure thresholds are exceeded.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Circuit breaker state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Circuit is closed, tasks execute normally
    Closed,
    /// Circuit is open, tasks are rejected immediately
    Open,
    /// Circuit is half-open, testing if service recovered
    HalfOpen,
}

impl CircuitState {
    /// Check if the circuit is closed
    pub fn is_closed(&self) -> bool {
        matches!(self, CircuitState::Closed)
    }

    /// Check if the circuit is open
    pub fn is_open(&self) -> bool {
        matches!(self, CircuitState::Open)
    }

    /// Check if the circuit is half-open
    pub fn is_half_open(&self) -> bool {
        matches!(self, CircuitState::HalfOpen)
    }

    /// Check if tasks can be executed (closed or half-open)
    pub fn can_execute(&self) -> bool {
        !self.is_open()
    }
}

impl std::fmt::Display for CircuitState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CircuitState::Closed => write!(f, "Closed"),
            CircuitState::Open => write!(f, "Open"),
            CircuitState::HalfOpen => write!(f, "Half-Open"),
        }
    }
}

/// Circuit breaker configuration
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of failures within window to trip circuit
    pub failure_threshold: u32,

    /// Number of consecutive successes to close circuit (from half-open)
    pub success_threshold: u32,

    /// Time to wait before attempting recovery (half-open)
    pub timeout_secs: u64,

    /// Time window for counting failures (seconds)
    pub window_secs: u64,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 2,
            timeout_secs: 60,
            window_secs: 60,
        }
    }
}

impl CircuitBreakerConfig {
    /// Check if configuration is valid
    pub fn is_valid(&self) -> bool {
        self.failure_threshold > 0
            && self.success_threshold > 0
            && self.timeout_secs > 0
            && self.window_secs > 0
    }

    /// Check if this is a lenient configuration (high thresholds)
    pub fn is_lenient(&self) -> bool {
        self.failure_threshold >= 10
    }

    /// Check if this is a strict configuration (low thresholds)
    pub fn is_strict(&self) -> bool {
        self.failure_threshold <= 3
    }
}

impl std::fmt::Display for CircuitBreakerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CircuitBreakerConfig[failures={}, successes={}, timeout={}s, window={}s]",
            self.failure_threshold, self.success_threshold, self.timeout_secs, self.window_secs
        )
    }
}

/// Statistics for a single circuit
#[derive(Debug)]
struct CircuitStats {
    state: CircuitState,
    failure_count: u32,
    success_count: u32,
    last_failure_time: Option<Instant>,
    opened_at: Option<Instant>,
}

impl CircuitStats {
    fn new() -> Self {
        Self {
            state: CircuitState::Closed,
            failure_count: 0,
            success_count: 0,
            last_failure_time: None,
            opened_at: None,
        }
    }

    fn reset_counts(&mut self) {
        self.failure_count = 0;
        self.success_count = 0;
    }
}

/// Circuit breaker for task execution
///
/// Tracks failures per task type and temporarily blocks execution
/// when failure thresholds are exceeded.
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    circuits: Arc<RwLock<HashMap<String, CircuitStats>>>,
}

impl CircuitBreaker {
    /// Create a new circuit breaker with default configuration
    pub fn new() -> Self {
        Self::with_config(CircuitBreakerConfig::default())
    }

    /// Create a new circuit breaker with custom configuration
    pub fn with_config(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            circuits: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Check if a task should be allowed to execute
    ///
    /// Returns true if the circuit is closed or half-open, false if open.
    pub async fn should_allow(&self, task_name: &str) -> bool {
        let mut circuits = self.circuits.write().await;
        let stats = circuits
            .entry(task_name.to_string())
            .or_insert_with(CircuitStats::new);

        match stats.state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                // Check if timeout has elapsed
                if let Some(opened_at) = stats.opened_at {
                    let elapsed = opened_at.elapsed();
                    if elapsed >= Duration::from_secs(self.config.timeout_secs) {
                        // Transition to half-open
                        stats.state = CircuitState::HalfOpen;
                        stats.reset_counts();
                        info!(
                            "Circuit breaker for '{}' transitioning to HALF-OPEN after {} seconds",
                            task_name,
                            elapsed.as_secs()
                        );
                        true
                    } else {
                        debug!(
                            "Circuit breaker for '{}' is OPEN, rejecting task",
                            task_name
                        );
                        false
                    }
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => true,
        }
    }

    /// Record a successful task execution
    pub async fn record_success(&self, task_name: &str) {
        let mut circuits = self.circuits.write().await;
        let stats = circuits
            .entry(task_name.to_string())
            .or_insert_with(CircuitStats::new);

        match stats.state {
            CircuitState::Closed => {
                // Reset failure count on success
                stats.failure_count = 0;
                stats.last_failure_time = None;
            }
            CircuitState::HalfOpen => {
                stats.success_count += 1;
                debug!(
                    "Circuit breaker for '{}' recorded success ({}/{})",
                    task_name, stats.success_count, self.config.success_threshold
                );

                if stats.success_count >= self.config.success_threshold {
                    // Close the circuit
                    stats.state = CircuitState::Closed;
                    stats.reset_counts();
                    stats.opened_at = None;
                    info!(
                        "Circuit breaker for '{}' transitioned to CLOSED after {} successes",
                        task_name, self.config.success_threshold
                    );
                }
            }
            CircuitState::Open => {
                // Shouldn't happen, but reset if it does
                warn!(
                    "Recorded success for '{}' while circuit is OPEN (unexpected)",
                    task_name
                );
            }
        }
    }

    /// Record a failed task execution
    pub async fn record_failure(&self, task_name: &str) {
        let mut circuits = self.circuits.write().await;
        let stats = circuits
            .entry(task_name.to_string())
            .or_insert_with(CircuitStats::new);

        let now = Instant::now();

        match stats.state {
            CircuitState::Closed => {
                // Check if we need to reset the window
                if let Some(last_failure) = stats.last_failure_time {
                    if now.duration_since(last_failure)
                        > Duration::from_secs(self.config.window_secs)
                    {
                        // Window expired, reset count
                        stats.failure_count = 0;
                    }
                }

                stats.failure_count += 1;
                stats.last_failure_time = Some(now);

                debug!(
                    "Circuit breaker for '{}' recorded failure ({}/{})",
                    task_name, stats.failure_count, self.config.failure_threshold
                );

                if stats.failure_count >= self.config.failure_threshold {
                    // Trip the circuit
                    stats.state = CircuitState::Open;
                    stats.opened_at = Some(now);
                    warn!(
                        "Circuit breaker for '{}' TRIPPED (OPEN) after {} failures within {} seconds",
                        task_name, stats.failure_count, self.config.window_secs
                    );
                }
            }
            CircuitState::HalfOpen => {
                // Failure in half-open state, immediately reopen
                stats.state = CircuitState::Open;
                stats.opened_at = Some(now);
                stats.reset_counts();
                warn!(
                    "Circuit breaker for '{}' reopened after failure in HALF-OPEN state",
                    task_name
                );
            }
            CircuitState::Open => {
                // Already open, update timestamp
                stats.last_failure_time = Some(now);
            }
        }
    }

    /// Get the current state of a circuit
    pub async fn get_state(&self, task_name: &str) -> CircuitState {
        let circuits = self.circuits.read().await;
        circuits
            .get(task_name)
            .map(|stats| stats.state)
            .unwrap_or(CircuitState::Closed)
    }

    /// Get statistics for all circuits
    pub async fn get_all_states(&self) -> HashMap<String, CircuitState> {
        let circuits = self.circuits.read().await;
        circuits
            .iter()
            .map(|(name, stats)| (name.clone(), stats.state))
            .collect()
    }

    /// Reset a specific circuit to closed state
    pub async fn reset(&self, task_name: &str) {
        let mut circuits = self.circuits.write().await;
        if let Some(stats) = circuits.get_mut(task_name) {
            stats.state = CircuitState::Closed;
            stats.reset_counts();
            stats.opened_at = None;
            stats.last_failure_time = None;
            info!(
                "Circuit breaker for '{}' manually reset to CLOSED",
                task_name
            );
        }
    }

    /// Reset all circuits to closed state
    pub async fn reset_all(&self) {
        let mut circuits = self.circuits.write().await;
        for (name, stats) in circuits.iter_mut() {
            stats.state = CircuitState::Closed;
            stats.reset_counts();
            stats.opened_at = None;
            stats.last_failure_time = None;
            info!("Circuit breaker for '{}' reset to CLOSED", name);
        }
    }
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_circuit_breaker_trip() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            success_threshold: 2,
            timeout_secs: 5,
            window_secs: 10,
        };
        let cb = CircuitBreaker::with_config(config);

        // Initially closed
        assert!(cb.should_allow("test_task").await);
        assert_eq!(cb.get_state("test_task").await, CircuitState::Closed);

        // Record failures
        cb.record_failure("test_task").await;
        assert_eq!(cb.get_state("test_task").await, CircuitState::Closed);
        cb.record_failure("test_task").await;
        assert_eq!(cb.get_state("test_task").await, CircuitState::Closed);
        cb.record_failure("test_task").await;

        // Circuit should be open
        assert_eq!(cb.get_state("test_task").await, CircuitState::Open);
        assert!(!cb.should_allow("test_task").await);
    }

    #[tokio::test]
    async fn test_circuit_breaker_recovery() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            success_threshold: 2,
            timeout_secs: 1,
            window_secs: 10,
        };
        let cb = CircuitBreaker::with_config(config);

        // Trip the circuit
        cb.record_failure("test_task").await;
        cb.record_failure("test_task").await;
        assert_eq!(cb.get_state("test_task").await, CircuitState::Open);

        // Wait for timeout
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Should transition to half-open
        assert!(cb.should_allow("test_task").await);
        assert_eq!(cb.get_state("test_task").await, CircuitState::HalfOpen);

        // Record successes
        cb.record_success("test_task").await;
        assert_eq!(cb.get_state("test_task").await, CircuitState::HalfOpen);
        cb.record_success("test_task").await;

        // Circuit should be closed
        assert_eq!(cb.get_state("test_task").await, CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_circuit_breaker_window_reset() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            success_threshold: 2,
            timeout_secs: 5,
            window_secs: 1,
        };
        let cb = CircuitBreaker::with_config(config);

        // Record 2 failures
        cb.record_failure("test_task").await;
        cb.record_failure("test_task").await;

        // Wait for window to expire
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Next failure should start new window
        cb.record_failure("test_task").await;

        // Circuit should still be closed (count reset)
        assert_eq!(cb.get_state("test_task").await, CircuitState::Closed);
    }
}
