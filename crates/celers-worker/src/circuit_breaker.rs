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

    /// Maximum number of *simultaneous* trial executions allowed while the
    /// circuit is half-open.
    ///
    /// Half-open exists to send a small amount of traffic at a service that may
    /// have recovered. Admitting every caller during that probe defeats the
    /// point: a worker running `concurrency` tasks would hand the still-broken
    /// dependency a full-width burst the instant the recovery timeout elapsed.
    /// The default of `1` matches the textbook single-probe behaviour; raising
    /// it trades a bigger recovery burst for a faster verdict.
    pub half_open_max_concurrent: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 2,
            timeout_secs: 60,
            window_secs: 60,
            half_open_max_concurrent: 1,
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
            && self.half_open_max_concurrent > 0
    }

    /// Set the maximum number of concurrent half-open probes.
    ///
    /// Values below `1` are clamped to `1`: a zero would make the half-open
    /// state admit nothing and the circuit could never close again.
    #[must_use]
    pub fn with_half_open_max_concurrent(mut self, max: u32) -> Self {
        self.half_open_max_concurrent = max.max(1);
        self
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
            "CircuitBreakerConfig[failures={}, successes={}, timeout={}s, window={}s, \
             half_open_probes={}]",
            self.failure_threshold,
            self.success_threshold,
            self.timeout_secs,
            self.window_secs,
            self.half_open_max_concurrent
        )
    }
}

/// Statistics for a single circuit
#[derive(Debug)]
struct CircuitStats {
    state: CircuitState,
    failure_count: u32,
    success_count: u32,
    /// When the *current* failure-counting window opened.
    ///
    /// The window used to be rolled forward from the **last** failure, which
    /// meant a steady trickle of failures spaced just under `window_secs` apart
    /// accumulated forever and eventually tripped the breaker while claiming
    /// "N failures within `window_secs` seconds" — a statement that could be off
    /// by hours. Anchoring the window at its start makes the trip message true.
    window_start: Option<Instant>,
    last_failure_time: Option<Instant>,
    opened_at: Option<Instant>,
    /// Trial executions currently admitted while half-open.
    in_flight_probes: u32,
}

impl CircuitStats {
    fn new() -> Self {
        Self {
            state: CircuitState::Closed,
            failure_count: 0,
            success_count: 0,
            window_start: None,
            last_failure_time: None,
            opened_at: None,
            in_flight_probes: 0,
        }
    }

    fn reset_counts(&mut self) {
        self.failure_count = 0;
        self.success_count = 0;
        self.window_start = None;
        self.in_flight_probes = 0;
    }

    /// Release a half-open probe slot (no-op when none is held).
    fn release_probe(&mut self) {
        self.in_flight_probes = self.in_flight_probes.saturating_sub(1);
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
    /// Returns true if the circuit is closed, or if it is half-open and a trial
    /// slot is free. Half-open admission is capped by
    /// [`CircuitBreakerConfig::half_open_max_concurrent`]: each admitted task
    /// takes a probe slot, released by the matching
    /// [`record_success`](Self::record_success) /
    /// [`record_failure`](Self::record_failure).
    pub async fn should_allow(&self, task_name: &str) -> bool {
        let max_probes = self.config.half_open_max_concurrent.max(1);
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
                        // Transition to half-open and take the first probe slot.
                        stats.state = CircuitState::HalfOpen;
                        stats.reset_counts();
                        stats.in_flight_probes = 1;
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
            CircuitState::HalfOpen => {
                if stats.in_flight_probes >= max_probes {
                    debug!(
                        "Circuit breaker for '{}' is HALF-OPEN with {} probe(s) in flight \
                         (max {}), rejecting task",
                        task_name, stats.in_flight_probes, max_probes
                    );
                    false
                } else {
                    stats.in_flight_probes += 1;
                    true
                }
            }
        }
    }

    /// Record a successful task execution
    pub async fn record_success(&self, task_name: &str) {
        let mut circuits = self.circuits.write().await;
        let stats = circuits
            .entry(task_name.to_string())
            .or_insert_with(CircuitStats::new);

        // A task admitted while half-open holds a probe slot; give it back
        // whatever the circuit's state is now (another probe may have reopened
        // or closed it in the meantime). Saturating, so a success recorded from
        // the closed state is a no-op.
        stats.release_probe();

        match stats.state {
            CircuitState::Closed => {
                // Reset failure count on success
                stats.failure_count = 0;
                stats.window_start = None;
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
        let window = Duration::from_secs(self.config.window_secs);

        // Release any half-open probe slot this task held (see `record_success`).
        stats.release_probe();

        match stats.state {
            CircuitState::Closed => {
                // Roll the counting window forward from its *start*, not from
                // the last failure: sliding the anchor on every failure let a
                // trickle spaced just under `window_secs` accumulate without
                // limit, so the trip message's "within N seconds" was a lie.
                match stats.window_start {
                    Some(start) if now.duration_since(start) > window => {
                        stats.window_start = Some(now);
                        stats.failure_count = 0;
                    }
                    None => stats.window_start = Some(now),
                    Some(_) => {}
                }

                stats.failure_count += 1;
                stats.last_failure_time = Some(now);

                debug!(
                    "Circuit breaker for '{}' recorded failure ({}/{})",
                    task_name, stats.failure_count, self.config.failure_threshold
                );

                if stats.failure_count >= self.config.failure_threshold {
                    // Trip the circuit
                    let spanned = stats
                        .window_start
                        .map(|start| now.duration_since(start))
                        .unwrap_or_default();
                    stats.state = CircuitState::Open;
                    stats.opened_at = Some(now);
                    warn!(
                        "Circuit breaker for '{}' TRIPPED (OPEN) after {} failures within {:?} \
                         (window {}s)",
                        task_name, stats.failure_count, spanned, self.config.window_secs
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

    /// Return a half-open trial slot taken by a task that was admitted but
    /// never actually executed (deferred by a later admission gate, or revoked
    /// mid-flight).
    ///
    /// Without this, [`should_allow`](Self::should_allow) would hand out the
    /// configured number of probe slots and never get them back, leaving the
    /// circuit stuck half-open and rejecting every task forever. It is a no-op
    /// when no slot is held.
    pub async fn release_probe(&self, task_name: &str) {
        let mut circuits = self.circuits.write().await;
        if let Some(stats) = circuits.get_mut(task_name) {
            stats.release_probe();
        }
    }

    /// Number of half-open trial executions currently in flight for a task type.
    ///
    /// Zero unless the circuit is (or has just been) half-open.
    pub async fn in_flight_probes(&self, task_name: &str) -> u32 {
        let circuits = self.circuits.read().await;
        circuits
            .get(task_name)
            .map(|stats| stats.in_flight_probes)
            .unwrap_or(0)
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
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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

    // ----------------------------------------------------------------------
    // Regression tests (idx 172)
    // ----------------------------------------------------------------------

    /// `should_allow` used to return `true` unconditionally while half-open, so
    /// the instant the recovery timeout elapsed every concurrent task was
    /// handed straight to the still-suspect dependency.
    #[tokio::test]
    async fn test_half_open_admits_only_the_configured_number_of_probes() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            success_threshold: 2,
            timeout_secs: 0,
            window_secs: 10,
            half_open_max_concurrent: 1,
        };
        let cb = CircuitBreaker::with_config(config);

        cb.record_failure("probe_task").await;
        assert_eq!(cb.get_state("probe_task").await, CircuitState::Open);

        // `timeout_secs: 0` means the recovery window is already over.
        assert!(cb.should_allow("probe_task").await, "first probe admitted");
        assert_eq!(cb.get_state("probe_task").await, CircuitState::HalfOpen);
        assert_eq!(cb.in_flight_probes("probe_task").await, 1);

        assert!(
            !cb.should_allow("probe_task").await,
            "a second concurrent probe must be rejected"
        );
        assert!(!cb.should_allow("probe_task").await);
        assert_eq!(cb.in_flight_probes("probe_task").await, 1);

        // Finishing the probe frees the slot for the next one.
        cb.record_success("probe_task").await;
        assert_eq!(cb.in_flight_probes("probe_task").await, 0);
        assert!(cb.should_allow("probe_task").await);
    }

    /// A larger budget admits exactly that many trials, and no more.
    #[tokio::test]
    async fn test_half_open_probe_budget_is_configurable() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            success_threshold: 5,
            timeout_secs: 0,
            window_secs: 10,
            half_open_max_concurrent: 3,
        };
        let cb = CircuitBreaker::with_config(config);
        cb.record_failure("probe_task").await;

        assert!(cb.should_allow("probe_task").await);
        assert!(cb.should_allow("probe_task").await);
        assert!(cb.should_allow("probe_task").await);
        assert_eq!(cb.in_flight_probes("probe_task").await, 3);
        assert!(!cb.should_allow("probe_task").await);
    }

    /// A task admitted on a probe slot but never executed (deferred by a later
    /// admission gate, or revoked mid-flight) must give the slot back, or the
    /// circuit stays half-open and rejects everything forever.
    #[tokio::test]
    async fn test_released_probe_slot_is_reusable() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            success_threshold: 2,
            timeout_secs: 0,
            window_secs: 10,
            half_open_max_concurrent: 1,
        };
        let cb = CircuitBreaker::with_config(config);
        cb.record_failure("probe_task").await;

        assert!(cb.should_allow("probe_task").await);
        assert!(!cb.should_allow("probe_task").await);

        cb.release_probe("probe_task").await;
        assert_eq!(cb.in_flight_probes("probe_task").await, 0);
        assert!(
            cb.should_allow("probe_task").await,
            "the freed slot must be reusable"
        );

        // Releasing a slot nobody holds is a harmless no-op.
        cb.release_probe("probe_task").await;
        cb.release_probe("probe_task").await;
        assert_eq!(cb.in_flight_probes("probe_task").await, 0);
        cb.release_probe("never_seen_task").await;
    }

    /// The failure window is anchored at its *start*. Rolling it forward from
    /// the last failure (the old behaviour) let a slow trickle accumulate
    /// without limit, so the trip log's "N failures within `window_secs`" could
    /// be off by hours.
    #[tokio::test]
    async fn test_failure_window_is_anchored_at_its_start() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            success_threshold: 2,
            timeout_secs: 60,
            // Sub-second so the test does not sleep for long.
            window_secs: 1,
            half_open_max_concurrent: 1,
        };
        let cb = CircuitBreaker::with_config(config);

        // Two failures spaced ~600ms apart: each is inside the *previous*
        // failure's 1s window, but together they span more than one window, so
        // the window rolls and the count restarts.
        cb.record_failure("trickle_task").await;
        tokio::time::sleep(Duration::from_millis(600)).await;
        cb.record_failure("trickle_task").await;
        tokio::time::sleep(Duration::from_millis(600)).await;
        cb.record_failure("trickle_task").await;
        tokio::time::sleep(Duration::from_millis(600)).await;
        cb.record_failure("trickle_task").await;

        assert_eq!(
            cb.get_state("trickle_task").await,
            CircuitState::Closed,
            "failures spread across several windows must not trip a 3-in-1s breaker"
        );

        // Three failures genuinely inside one window still trip it.
        cb.record_failure("burst_task").await;
        cb.record_failure("burst_task").await;
        cb.record_failure("burst_task").await;
        assert_eq!(cb.get_state("burst_task").await, CircuitState::Open);
    }

    #[tokio::test]
    async fn test_success_clears_the_failure_window() {
        let cb = CircuitBreaker::with_config(CircuitBreakerConfig {
            failure_threshold: 2,
            ..Default::default()
        });

        cb.record_failure("mixed_task").await;
        cb.record_success("mixed_task").await;
        cb.record_failure("mixed_task").await;

        assert_eq!(
            cb.get_state("mixed_task").await,
            CircuitState::Closed,
            "an intervening success resets the window, so this is failure 1 of 2"
        );
    }

    #[test]
    fn test_config_validation_rejects_a_zero_probe_budget() {
        let mut config = CircuitBreakerConfig::default();
        assert!(config.is_valid());
        config.half_open_max_concurrent = 0;
        assert!(
            !config.is_valid(),
            "zero probes would leave a half-open circuit unable to ever close"
        );

        assert_eq!(
            CircuitBreakerConfig::default()
                .with_half_open_max_concurrent(0)
                .half_open_max_concurrent,
            1,
            "the builder clamps to a usable value"
        );
        assert_eq!(
            CircuitBreakerConfig::default()
                .with_half_open_max_concurrent(4)
                .half_open_max_concurrent,
            4
        );
    }
}
