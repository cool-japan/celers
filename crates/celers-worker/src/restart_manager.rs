//! Automatic worker restart on critical errors
//!
//! This module provides self-healing capabilities for workers by detecting
//! critical errors and automatically restarting the worker process.
//!
//! # Features
//!
//! - Critical error detection
//! - Configurable restart policies
//! - Exponential backoff for repeated failures
//! - Maximum restart limits
//! - Crash reporting and logging
//! - Graceful shutdown before restart
//!
//! # Example
//!
//! ```
//! use celers_worker::{RestartManager, RestartPolicy};
//! use std::time::Duration;
//!
//! # async fn example() {
//! let policy = RestartPolicy::exponential_backoff()
//!     .with_max_restarts(5)
//!     .with_base_delay(Duration::from_secs(1));
//!
//! let mut manager = RestartManager::new(policy);
//!
//! // Check if we should restart after an error
//! if manager.should_restart("critical error").await {
//!     println!("Restarting worker...");
//!     manager.record_restart().await;
//!     // Perform restart...
//! }
//! # }
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Error severity level
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ErrorSeverity {
    /// Low severity - recoverable error
    Low = 0,
    /// Medium severity - may affect performance
    Medium = 1,
    /// High severity - affects functionality
    High = 2,
    /// Critical severity - worker cannot continue
    Critical = 3,
}

impl ErrorSeverity {
    /// Check if this severity requires a restart
    pub fn requires_restart(&self) -> bool {
        matches!(self, ErrorSeverity::Critical)
    }

    /// Check if this is a critical error
    pub fn is_critical(&self) -> bool {
        matches!(self, ErrorSeverity::Critical)
    }
}

impl std::fmt::Display for ErrorSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorSeverity::Low => write!(f, "Low"),
            ErrorSeverity::Medium => write!(f, "Medium"),
            ErrorSeverity::High => write!(f, "High"),
            ErrorSeverity::Critical => write!(f, "Critical"),
        }
    }
}

/// Restart policy strategy
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RestartStrategy {
    /// Always restart immediately
    Always,
    /// Never restart automatically
    Never,
    /// Restart with exponential backoff
    #[default]
    ExponentialBackoff,
    /// Restart with linear backoff
    LinearBackoff,
}

/// Restart policy configuration
#[derive(Clone, Debug)]
pub struct RestartPolicy {
    /// Restart strategy
    pub strategy: RestartStrategy,
    /// Maximum number of restarts allowed
    pub max_restarts: Option<usize>,
    /// Time window for counting restarts
    pub restart_window: Duration,
    /// Base delay for backoff strategies
    pub base_delay: Duration,
    /// Maximum delay for backoff strategies
    pub max_delay: Duration,
    /// Minimum severity level to trigger restart
    pub min_severity: ErrorSeverity,
    /// Enable restart functionality
    pub enabled: bool,
}

impl RestartPolicy {
    /// Create a new restart policy
    pub fn new(strategy: RestartStrategy) -> Self {
        Self {
            strategy,
            max_restarts: Some(5),
            restart_window: Duration::from_secs(300), // 5 minutes
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            min_severity: ErrorSeverity::Critical,
            enabled: true,
        }
    }

    /// Create an always-restart policy
    pub fn always() -> Self {
        Self::new(RestartStrategy::Always)
    }

    /// Create a never-restart policy
    pub fn never() -> Self {
        Self::new(RestartStrategy::Never).enabled(false)
    }

    /// Create an exponential backoff policy
    pub fn exponential_backoff() -> Self {
        Self::new(RestartStrategy::ExponentialBackoff)
    }

    /// Create a linear backoff policy
    pub fn linear_backoff() -> Self {
        Self::new(RestartStrategy::LinearBackoff)
    }

    /// Set the maximum number of restarts
    pub fn with_max_restarts(mut self, max: usize) -> Self {
        self.max_restarts = Some(max);
        self
    }

    /// Disable restart limit
    pub fn unlimited_restarts(mut self) -> Self {
        self.max_restarts = None;
        self
    }

    /// Set the restart time window
    pub fn with_restart_window(mut self, window: Duration) -> Self {
        self.restart_window = window;
        self
    }

    /// Set the base delay for backoff
    pub fn with_base_delay(mut self, delay: Duration) -> Self {
        self.base_delay = delay;
        self
    }

    /// Set the maximum delay for backoff
    pub fn with_max_delay(mut self, delay: Duration) -> Self {
        self.max_delay = delay;
        self
    }

    /// Set the minimum severity level
    pub fn with_min_severity(mut self, severity: ErrorSeverity) -> Self {
        self.min_severity = severity;
        self
    }

    /// Enable or disable restart
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Validate the policy
    pub fn validate(&self) -> Result<(), String> {
        if self.base_delay > self.max_delay {
            return Err("Base delay cannot be greater than max delay".to_string());
        }
        Ok(())
    }
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self::exponential_backoff()
    }
}

/// Restart record
#[allow(dead_code)]
#[derive(Debug, Clone)]
struct RestartRecord {
    /// When the restart occurred
    timestamp: Instant,
    /// Reason for restart
    reason: String,
    /// Error severity
    severity: ErrorSeverity,
}

impl RestartRecord {
    fn new(reason: String, severity: ErrorSeverity) -> Self {
        Self {
            timestamp: Instant::now(),
            reason,
            severity,
        }
    }

    /// Get the age of this restart record
    fn age(&self) -> Duration {
        self.timestamp.elapsed()
    }

    /// Check if this record is within the given window
    fn is_within(&self, window: Duration) -> bool {
        self.age() <= window
    }
}

/// Restart statistics
#[derive(Debug, Clone, Default)]
pub struct RestartStats {
    /// Total number of restarts
    pub total_restarts: usize,
    /// Number of restarts in current window
    pub recent_restarts: usize,
    /// Last restart time
    pub last_restart: Option<Instant>,
    /// Time until next allowed restart (if in backoff)
    pub next_restart_delay: Option<Duration>,
}

impl RestartStats {
    /// Check if restart limit has been reached
    pub fn has_reached_limit(&self, max_restarts: Option<usize>) -> bool {
        if let Some(max) = max_restarts {
            self.recent_restarts >= max
        } else {
            false
        }
    }
}

/// Automatic restart manager
pub struct RestartManager {
    /// Restart policy
    policy: RestartPolicy,
    /// Restart history
    history: Arc<RwLock<Vec<RestartRecord>>>,
    /// Last restart attempt
    last_restart: Arc<RwLock<Option<Instant>>>,
}

impl RestartManager {
    /// Create a new restart manager
    pub fn new(policy: RestartPolicy) -> Self {
        Self {
            policy,
            history: Arc::new(RwLock::new(Vec::new())),
            last_restart: Arc::new(RwLock::new(None)),
        }
    }

    /// Check if the worker should restart based on the error
    pub async fn should_restart(&self, _error: impl AsRef<str>) -> bool {
        self.should_restart_with_severity(_error, ErrorSeverity::Critical)
            .await
    }

    /// Check if the worker should restart based on error and severity
    pub async fn should_restart_with_severity(
        &self,
        _error: impl AsRef<str>,
        severity: ErrorSeverity,
    ) -> bool {
        if !self.policy.enabled {
            return false;
        }

        // Check severity threshold
        if severity < self.policy.min_severity {
            return false;
        }

        // Check strategy
        match self.policy.strategy {
            RestartStrategy::Never => false,
            RestartStrategy::Always => {
                // Check restart count limit even for Always strategy
                if let Some(max) = self.policy.max_restarts {
                    let stats = self.get_stats().await;
                    if stats.has_reached_limit(Some(max)) {
                        warn!(
                            "Restart limit reached ({} restarts in {:?}), not restarting",
                            stats.recent_restarts, self.policy.restart_window
                        );
                        return false;
                    }
                }
                true
            }
            RestartStrategy::ExponentialBackoff | RestartStrategy::LinearBackoff => {
                // Check restart count limit
                let stats = self.get_stats().await;
                if stats.has_reached_limit(self.policy.max_restarts) {
                    warn!(
                        "Restart limit reached ({} restarts in {:?}), not restarting",
                        stats.recent_restarts, self.policy.restart_window
                    );
                    return false;
                }

                // Check backoff delay
                if let Some(last) = stats.last_restart {
                    let delay = self.calculate_backoff_delay(stats.recent_restarts);
                    if last.elapsed() < delay {
                        let remaining = delay.saturating_sub(last.elapsed());
                        warn!("In backoff period, waiting {:?} before restart", remaining);
                        return false;
                    }
                }

                true
            }
        }
    }

    /// Calculate backoff delay based on restart count
    fn calculate_backoff_delay(&self, restart_count: usize) -> Duration {
        match self.policy.strategy {
            RestartStrategy::ExponentialBackoff => {
                let delay_ms =
                    self.policy.base_delay.as_millis() as u64 * 2_u64.pow(restart_count as u32);
                let delay = Duration::from_millis(delay_ms);
                delay.min(self.policy.max_delay)
            }
            RestartStrategy::LinearBackoff => {
                let delay_ms =
                    self.policy.base_delay.as_millis() as u64 * (restart_count as u64 + 1);
                let delay = Duration::from_millis(delay_ms);
                delay.min(self.policy.max_delay)
            }
            _ => Duration::ZERO,
        }
    }

    /// Record a restart
    pub async fn record_restart(&self) {
        self.record_restart_with_severity("Worker restart", ErrorSeverity::Critical)
            .await;
    }

    /// Record a restart with specific reason and severity
    pub async fn record_restart_with_severity(
        &self,
        reason: impl Into<String>,
        severity: ErrorSeverity,
    ) {
        let reason = reason.into();
        let record = RestartRecord::new(reason.clone(), severity);

        {
            let mut history = self.history.write().await;
            history.push(record);
        }

        {
            let mut last_restart = self.last_restart.write().await;
            *last_restart = Some(Instant::now());
        }

        info!("Recorded restart: {} (severity: {})", reason, severity);

        // Clean up old records outside the window
        self.cleanup_old_records().await;
    }

    /// Clean up restart records outside the time window
    async fn cleanup_old_records(&self) {
        let mut history = self.history.write().await;
        history.retain(|r| r.is_within(self.policy.restart_window));
    }

    /// Get restart statistics
    pub async fn get_stats(&self) -> RestartStats {
        self.cleanup_old_records().await;

        let history = self.history.read().await;
        let last_restart = *self.last_restart.read().await;

        let recent_restarts = history
            .iter()
            .filter(|r| r.is_within(self.policy.restart_window))
            .count();

        let next_restart_delay = if let Some(last) = last_restart {
            let delay = self.calculate_backoff_delay(recent_restarts);
            let elapsed = last.elapsed();
            if elapsed < delay {
                Some(delay - elapsed)
            } else {
                None
            }
        } else {
            None
        };

        RestartStats {
            total_restarts: history.len(),
            recent_restarts,
            last_restart,
            next_restart_delay,
        }
    }

    /// Clear restart history
    pub async fn clear_history(&self) {
        self.history.write().await.clear();
        *self.last_restart.write().await = None;
    }

    /// Get the restart policy
    pub fn policy(&self) -> &RestartPolicy {
        &self.policy
    }
}

// ---------------------------------------------------------------------------
// Self-healing supervisor
// ---------------------------------------------------------------------------

/// Why a worker needs to be restarted.
///
/// These map onto the unhealthy conditions a self-healing supervisor reacts to:
/// a crashed task, an exceeded memory budget, or missed heartbeats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RestartTrigger {
    /// The worker panicked or its task loop crashed.
    Panic,
    /// The worker exceeded its configured memory budget.
    MemoryExceeded,
    /// The worker missed heartbeats and was declared dead.
    MissedHeartbeat,
    /// A generic unhealthy condition was reported by the health subsystem.
    Unhealthy,
    /// An explicit, operator-requested restart.
    Manual,
}

impl RestartTrigger {
    /// Map this trigger onto an [`ErrorSeverity`].
    pub fn severity(&self) -> ErrorSeverity {
        match self {
            RestartTrigger::Panic | RestartTrigger::MemoryExceeded => ErrorSeverity::Critical,
            RestartTrigger::MissedHeartbeat | RestartTrigger::Unhealthy => ErrorSeverity::High,
            RestartTrigger::Manual => ErrorSeverity::Medium,
        }
    }

    /// Short, stable human-readable label.
    pub fn as_str(&self) -> &'static str {
        match self {
            RestartTrigger::Panic => "panic",
            RestartTrigger::MemoryExceeded => "memory_exceeded",
            RestartTrigger::MissedHeartbeat => "missed_heartbeat",
            RestartTrigger::Unhealthy => "unhealthy",
            RestartTrigger::Manual => "manual",
        }
    }
}

impl std::fmt::Display for RestartTrigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Lifecycle state of a [`SelfHealingSupervisor`].
///
/// The supervisor behaves like a circuit breaker around the restart loop:
/// while the breaker is closed it heals the worker, and once too many restarts
/// pile up within the window it trips into a terminal state and stops trying.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SupervisorState {
    /// The worker is believed healthy; no restart is in progress.
    #[default]
    Healthy,
    /// A restart is currently being backed off / executed.
    Restarting,
    /// The supervisor is waiting out a backoff delay before the next restart.
    BackingOff,
    /// The restart circuit breaker has tripped: too many restarts within the
    /// window. The supervisor will not restart again until reset. This is the
    /// terminal state the worker pool should surface to operators.
    Terminal,
}

impl SupervisorState {
    /// Whether this is the terminal (give-up) state.
    pub fn is_terminal(&self) -> bool {
        matches!(self, SupervisorState::Terminal)
    }

    /// Whether the worker is considered healthy.
    pub fn is_healthy(&self) -> bool {
        matches!(self, SupervisorState::Healthy)
    }
}

impl std::fmt::Display for SupervisorState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SupervisorState::Healthy => write!(f, "Healthy"),
            SupervisorState::Restarting => write!(f, "Restarting"),
            SupervisorState::BackingOff => write!(f, "BackingOff"),
            SupervisorState::Terminal => write!(f, "Terminal"),
        }
    }
}

/// The decision returned by [`SelfHealingSupervisor::on_unhealthy`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestartDecision {
    /// Restart now (no backoff remaining). `attempt` is the 1-based restart
    /// attempt number this represents.
    RestartNow {
        /// The 1-based attempt number for this restart.
        attempt: usize,
    },
    /// Wait the given duration before restarting (still within backoff).
    Backoff {
        /// How long the caller should wait before the next restart attempt.
        delay: Duration,
    },
    /// The circuit breaker has tripped; do not restart. The worker has entered
    /// the terminal state.
    GiveUp {
        /// Number of restarts recorded within the window when the breaker
        /// tripped.
        restarts_in_window: usize,
    },
    /// Restart is disabled by policy.
    Disabled,
}

impl RestartDecision {
    /// Whether the caller should restart now.
    pub fn should_restart_now(&self) -> bool {
        matches!(self, RestartDecision::RestartNow { .. })
    }

    /// Whether the supervisor has given up (terminal).
    pub fn is_give_up(&self) -> bool {
        matches!(self, RestartDecision::GiveUp { .. })
    }
}

/// A timestamped restart attempt against the supervisor.
#[derive(Debug, Clone)]
struct RestartAttempt {
    timestamp: Instant,
    trigger: RestartTrigger,
}

impl RestartAttempt {
    fn is_within(&self, window: Duration) -> bool {
        self.timestamp.elapsed() <= window
    }
}

/// Aggregate statistics for a [`SelfHealingSupervisor`].
#[derive(Debug, Clone, Default)]
pub struct SupervisorStats {
    /// Current supervisor state.
    pub state: SupervisorState,
    /// Total restarts performed since creation.
    pub total_restarts: usize,
    /// Restarts within the current window.
    pub restarts_in_window: usize,
    /// Total times the breaker tripped into the terminal state.
    pub trips: usize,
    /// The last trigger that caused a restart, if any.
    pub last_trigger: Option<RestartTrigger>,
    /// Time of the last restart attempt.
    pub last_restart: Option<Instant>,
    /// Per-trigger breakdown of restarts within the current window. Useful for
    /// diagnosing *why* a worker is flapping (e.g. all `MemoryExceeded`).
    pub triggers_in_window: Vec<(RestartTrigger, usize)>,
}

/// Internal mutable state for the supervisor.
struct SupervisorInner {
    state: SupervisorState,
    /// Timestamped restart history (pruned to the policy window).
    history: Vec<RestartAttempt>,
    /// Cumulative restart count.
    total_restarts: usize,
    /// Number of times the breaker tripped.
    trips: usize,
    /// Last restart trigger.
    last_trigger: Option<RestartTrigger>,
    /// Timestamp of last restart attempt (for backoff calculation).
    last_restart: Option<Instant>,
}

impl SupervisorInner {
    fn new() -> Self {
        Self {
            state: SupervisorState::Healthy,
            history: Vec::new(),
            total_restarts: 0,
            trips: 0,
            last_trigger: None,
            last_restart: None,
        }
    }

    fn prune(&mut self, window: Duration) {
        self.history.retain(|a| a.is_within(window));
    }

    fn recent_count(&self, window: Duration) -> usize {
        self.history.iter().filter(|a| a.is_within(window)).count()
    }

    /// Per-trigger breakdown of restarts still within the window.
    fn recent_triggers(&self, window: Duration) -> Vec<(RestartTrigger, usize)> {
        let mut counts: Vec<(RestartTrigger, usize)> = Vec::new();
        for attempt in self.history.iter().filter(|a| a.is_within(window)) {
            if let Some(entry) = counts.iter_mut().find(|(t, _)| *t == attempt.trigger) {
                entry.1 += 1;
            } else {
                counts.push((attempt.trigger, 1));
            }
        }
        counts
    }
}

/// Self-healing supervisor: drives automatic restart of an unhealthy worker
/// (panic, exceeded memory, missed heartbeats) with exponential backoff and a
/// max-restart circuit breaker.
///
/// The supervisor wraps a [`RestartPolicy`] for its backoff/window/limit
/// configuration and adds:
///
/// - a [`SupervisorState`] state machine (Healthy → Restarting/BackingOff →
///   Terminal),
/// - per-trigger accounting via [`RestartTrigger`],
/// - a circuit breaker that opens (enters [`SupervisorState::Terminal`]) once
///   the number of restarts within [`RestartPolicy::restart_window`] reaches
///   [`RestartPolicy::max_restarts`], so a crash-looping worker is given up on
///   instead of being restarted forever.
///
/// # Example
///
/// ```
/// use celers_worker::{RestartPolicy, RestartTrigger, SelfHealingSupervisor};
/// use std::time::Duration;
///
/// # async fn example() {
/// let policy = RestartPolicy::exponential_backoff()
///     .with_max_restarts(3)
///     .with_base_delay(Duration::from_millis(10))
///     .with_restart_window(Duration::from_secs(60));
/// let supervisor = SelfHealingSupervisor::new(policy);
///
/// let decision = supervisor.on_unhealthy(RestartTrigger::Panic).await;
/// if decision.should_restart_now() {
///     // perform the actual restart, then:
///     supervisor.confirm_restart(RestartTrigger::Panic).await;
/// }
/// # }
/// ```
#[derive(Clone)]
pub struct SelfHealingSupervisor {
    policy: RestartPolicy,
    inner: Arc<RwLock<SupervisorInner>>,
}

impl SelfHealingSupervisor {
    /// Create a new supervisor from a restart policy.
    pub fn new(policy: RestartPolicy) -> Self {
        Self {
            policy,
            inner: Arc::new(RwLock::new(SupervisorInner::new())),
        }
    }

    /// Borrow the underlying restart policy.
    pub fn policy(&self) -> &RestartPolicy {
        &self.policy
    }

    /// Current supervisor state.
    pub async fn state(&self) -> SupervisorState {
        self.inner.read().await.state
    }

    /// Whether the supervisor has reached the terminal state.
    pub async fn is_terminal(&self) -> bool {
        self.inner.read().await.state.is_terminal()
    }

    /// Compute the backoff delay for the Nth restart (0-based count of prior
    /// restarts within the window) using the policy strategy.
    fn backoff_for(&self, prior_restarts: usize) -> Duration {
        match self.policy.strategy {
            RestartStrategy::ExponentialBackoff => {
                let factor = 2_u64.checked_pow(prior_restarts as u32);
                let delay_ms = factor
                    .and_then(|f| (self.policy.base_delay.as_millis() as u64).checked_mul(f))
                    .unwrap_or(u64::MAX);
                Duration::from_millis(delay_ms).min(self.policy.max_delay)
            }
            RestartStrategy::LinearBackoff => {
                let delay_ms = (self.policy.base_delay.as_millis() as u64)
                    .saturating_mul(prior_restarts as u64 + 1);
                Duration::from_millis(delay_ms).min(self.policy.max_delay)
            }
            RestartStrategy::Always | RestartStrategy::Never => Duration::ZERO,
        }
    }

    /// React to an unhealthy worker condition and decide whether to restart.
    ///
    /// This does **not** itself record a restart; the caller should perform the
    /// actual restart when [`RestartDecision::should_restart_now`] is true and
    /// then call [`SelfHealingSupervisor::confirm_restart`]. Splitting the
    /// decision from the confirmation lets the caller honour backoff without the
    /// supervisor assuming a restart always succeeds.
    pub async fn on_unhealthy(&self, trigger: RestartTrigger) -> RestartDecision {
        if !self.policy.enabled || self.policy.strategy == RestartStrategy::Never {
            return RestartDecision::Disabled;
        }
        if trigger.severity() < self.policy.min_severity {
            return RestartDecision::Disabled;
        }

        let mut inner = self.inner.write().await;
        inner.prune(self.policy.restart_window);

        // Already given up.
        if inner.state == SupervisorState::Terminal {
            return RestartDecision::GiveUp {
                restarts_in_window: inner.recent_count(self.policy.restart_window),
            };
        }

        let recent = inner.recent_count(self.policy.restart_window);

        // Circuit breaker: trip if we've already used up the allowance.
        if let Some(max) = self.policy.max_restarts {
            if recent >= max {
                inner.state = SupervisorState::Terminal;
                inner.trips += 1;
                warn!(
                    "Self-healing circuit breaker tripped: {} restarts within {:?}; entering terminal state",
                    recent, self.policy.restart_window
                );
                return RestartDecision::GiveUp {
                    restarts_in_window: recent,
                };
            }
        }

        // Backoff: are we still inside the delay since the last restart?
        if let Some(last) = inner.last_restart {
            let delay = self.backoff_for(recent);
            let elapsed = last.elapsed();
            if elapsed < delay {
                inner.state = SupervisorState::BackingOff;
                let remaining = delay.saturating_sub(elapsed);
                debug!(
                    "Self-healing supervisor backing off; {:?} remaining before next restart",
                    remaining
                );
                return RestartDecision::Backoff { delay: remaining };
            }
        }

        inner.state = SupervisorState::Restarting;
        RestartDecision::RestartNow {
            attempt: recent + 1,
        }
    }

    /// Record that a restart was actually performed for the given trigger.
    ///
    /// This advances the circuit-breaker accounting. After confirming, the
    /// supervisor returns to [`SupervisorState::Healthy`] unless the restart
    /// just exhausted the allowance, in which case the *next*
    /// [`SelfHealingSupervisor::on_unhealthy`] call will trip the breaker.
    pub async fn confirm_restart(&self, trigger: RestartTrigger) {
        let mut inner = self.inner.write().await;
        let now = Instant::now();
        inner.history.push(RestartAttempt {
            timestamp: now,
            trigger,
        });
        inner.total_restarts += 1;
        inner.last_trigger = Some(trigger);
        inner.last_restart = Some(now);
        inner.prune(self.policy.restart_window);

        if inner.state != SupervisorState::Terminal {
            inner.state = SupervisorState::Healthy;
        }
        info!(
            "Self-healing supervisor confirmed restart (trigger: {}, total: {})",
            trigger, inner.total_restarts
        );
    }

    /// Convenience: decide and, if a restart is warranted *now*, immediately
    /// confirm it. Returns the decision. Backoff / give-up decisions are
    /// returned without recording a restart.
    pub async fn try_restart(&self, trigger: RestartTrigger) -> RestartDecision {
        let decision = self.on_unhealthy(trigger).await;
        if decision.should_restart_now() {
            self.confirm_restart(trigger).await;
        }
        decision
    }

    /// Report that the worker recovered and is healthy again. This clears the
    /// backoff timer so a future failure restarts promptly, but it does **not**
    /// clear the trip history (a flapping worker should still trip the breaker).
    /// To fully reset the breaker use [`SelfHealingSupervisor::reset`].
    pub async fn on_recovered(&self) {
        let mut inner = self.inner.write().await;
        if inner.state != SupervisorState::Terminal {
            inner.state = SupervisorState::Healthy;
            inner.last_restart = None;
        }
    }

    /// Manually reset the supervisor, clearing the terminal state and all
    /// restart history. Use after an operator has remediated the root cause.
    pub async fn reset(&self) {
        let mut inner = self.inner.write().await;
        inner.state = SupervisorState::Healthy;
        inner.history.clear();
        inner.last_restart = None;
        info!("Self-healing supervisor manually reset");
    }

    /// Snapshot of supervisor statistics.
    pub async fn stats(&self) -> SupervisorStats {
        let mut inner = self.inner.write().await;
        inner.prune(self.policy.restart_window);
        SupervisorStats {
            state: inner.state,
            total_restarts: inner.total_restarts,
            restarts_in_window: inner.recent_count(self.policy.restart_window),
            trips: inner.trips,
            last_trigger: inner.last_trigger,
            last_restart: inner.last_restart,
            triggers_in_window: inner.recent_triggers(self.policy.restart_window),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[test]
    fn test_error_severity_requires_restart() {
        assert!(!ErrorSeverity::Low.requires_restart());
        assert!(!ErrorSeverity::Medium.requires_restart());
        assert!(!ErrorSeverity::High.requires_restart());
        assert!(ErrorSeverity::Critical.requires_restart());
    }

    #[test]
    fn test_error_severity_is_critical() {
        assert!(!ErrorSeverity::Low.is_critical());
        assert!(!ErrorSeverity::Medium.is_critical());
        assert!(!ErrorSeverity::High.is_critical());
        assert!(ErrorSeverity::Critical.is_critical());
    }

    #[test]
    fn test_restart_policy_default() {
        let policy = RestartPolicy::default();
        assert_eq!(policy.strategy, RestartStrategy::ExponentialBackoff);
        assert_eq!(policy.max_restarts, Some(5));
        assert_eq!(policy.base_delay, Duration::from_secs(1));
        assert_eq!(policy.min_severity, ErrorSeverity::Critical);
        assert!(policy.enabled);
    }

    #[test]
    fn test_restart_policy_always() {
        let policy = RestartPolicy::always();
        assert_eq!(policy.strategy, RestartStrategy::Always);
    }

    #[test]
    fn test_restart_policy_never() {
        let policy = RestartPolicy::never();
        assert_eq!(policy.strategy, RestartStrategy::Never);
        assert!(!policy.enabled);
    }

    #[test]
    fn test_restart_policy_builder() {
        let policy = RestartPolicy::exponential_backoff()
            .with_max_restarts(10)
            .with_base_delay(Duration::from_secs(2))
            .with_max_delay(Duration::from_secs(120))
            .with_min_severity(ErrorSeverity::High);

        assert_eq!(policy.max_restarts, Some(10));
        assert_eq!(policy.base_delay, Duration::from_secs(2));
        assert_eq!(policy.max_delay, Duration::from_secs(120));
        assert_eq!(policy.min_severity, ErrorSeverity::High);
    }

    #[test]
    fn test_restart_policy_validation() {
        let policy = RestartPolicy::default()
            .with_base_delay(Duration::from_secs(100))
            .with_max_delay(Duration::from_secs(10));
        assert!(policy.validate().is_err());

        let policy = RestartPolicy::default();
        assert!(policy.validate().is_ok());
    }

    #[tokio::test]
    async fn test_restart_manager_should_restart_disabled() {
        let policy = RestartPolicy::never();
        let manager = RestartManager::new(policy);

        assert!(!manager.should_restart("critical error").await);
    }

    #[tokio::test]
    async fn test_restart_manager_should_restart_always() {
        let policy = RestartPolicy::always();
        let manager = RestartManager::new(policy);

        assert!(manager.should_restart("critical error").await);
        assert!(manager.should_restart("another error").await);
    }

    #[tokio::test]
    async fn test_restart_manager_severity_threshold() {
        let policy = RestartPolicy::always().with_min_severity(ErrorSeverity::High);
        let manager = RestartManager::new(policy);

        assert!(
            !manager
                .should_restart_with_severity("low error", ErrorSeverity::Low)
                .await
        );
        assert!(
            !manager
                .should_restart_with_severity("medium error", ErrorSeverity::Medium)
                .await
        );
        assert!(
            manager
                .should_restart_with_severity("high error", ErrorSeverity::High)
                .await
        );
        assert!(
            manager
                .should_restart_with_severity("critical error", ErrorSeverity::Critical)
                .await
        );
    }

    #[tokio::test]
    async fn test_restart_manager_max_restarts() {
        let policy = RestartPolicy::always().with_max_restarts(2);
        let manager = RestartManager::new(policy);

        // First two restarts should be allowed
        assert!(manager.should_restart("error 1").await);
        manager.record_restart().await;

        assert!(manager.should_restart("error 2").await);
        manager.record_restart().await;

        // Third restart should be blocked
        assert!(!manager.should_restart("error 3").await);

        let stats = manager.get_stats().await;
        assert_eq!(stats.recent_restarts, 2);
        assert!(stats.has_reached_limit(Some(2)));
    }

    #[tokio::test]
    async fn test_restart_manager_backoff_delay() {
        let policy = RestartPolicy::exponential_backoff()
            .with_base_delay(Duration::from_millis(100))
            .with_max_delay(Duration::from_secs(10));
        let manager = RestartManager::new(policy);

        // First restart - should be allowed
        assert!(manager.should_restart("error 1").await);
        manager.record_restart().await;

        // Second restart immediately - should be blocked (in backoff)
        assert!(!manager.should_restart("error 2").await);

        // Wait for backoff period
        sleep(Duration::from_millis(250)).await;

        // Now should be allowed
        assert!(manager.should_restart("error 3").await);
    }

    #[tokio::test]
    async fn test_restart_manager_stats() {
        let policy = RestartPolicy::default();
        let manager = RestartManager::new(policy);

        let stats = manager.get_stats().await;
        assert_eq!(stats.total_restarts, 0);
        assert_eq!(stats.recent_restarts, 0);
        assert!(stats.last_restart.is_none());

        manager.record_restart().await;

        let stats = manager.get_stats().await;
        assert_eq!(stats.total_restarts, 1);
        assert_eq!(stats.recent_restarts, 1);
        assert!(stats.last_restart.is_some());
    }

    #[tokio::test]
    async fn test_restart_manager_clear_history() {
        let policy = RestartPolicy::default();
        let manager = RestartManager::new(policy);

        manager.record_restart().await;
        let stats = manager.get_stats().await;
        assert_eq!(stats.total_restarts, 1);

        manager.clear_history().await;
        let stats = manager.get_stats().await;
        assert_eq!(stats.total_restarts, 0);
        assert!(stats.last_restart.is_none());
    }

    #[test]
    fn test_restart_manager_calculate_backoff_exponential() {
        let policy = RestartPolicy::exponential_backoff()
            .with_base_delay(Duration::from_secs(1))
            .with_max_delay(Duration::from_secs(60));
        let manager = RestartManager::new(policy);

        assert_eq!(manager.calculate_backoff_delay(0), Duration::from_secs(1));
        assert_eq!(manager.calculate_backoff_delay(1), Duration::from_secs(2));
        assert_eq!(manager.calculate_backoff_delay(2), Duration::from_secs(4));
        assert_eq!(manager.calculate_backoff_delay(3), Duration::from_secs(8));

        // Should cap at max_delay
        assert_eq!(manager.calculate_backoff_delay(10), Duration::from_secs(60));
    }

    #[test]
    fn test_restart_manager_calculate_backoff_linear() {
        let policy = RestartPolicy::linear_backoff()
            .with_base_delay(Duration::from_secs(1))
            .with_max_delay(Duration::from_secs(10));
        let manager = RestartManager::new(policy);

        assert_eq!(manager.calculate_backoff_delay(0), Duration::from_secs(1));
        assert_eq!(manager.calculate_backoff_delay(1), Duration::from_secs(2));
        assert_eq!(manager.calculate_backoff_delay(2), Duration::from_secs(3));
        assert_eq!(manager.calculate_backoff_delay(3), Duration::from_secs(4));

        // Should cap at max_delay
        assert_eq!(manager.calculate_backoff_delay(20), Duration::from_secs(10));
    }

    // -----------------------------------------------------------------------
    // SelfHealingSupervisor tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_restart_trigger_severity_and_str() {
        assert_eq!(RestartTrigger::Panic.severity(), ErrorSeverity::Critical);
        assert_eq!(
            RestartTrigger::MemoryExceeded.severity(),
            ErrorSeverity::Critical
        );
        assert_eq!(
            RestartTrigger::MissedHeartbeat.severity(),
            ErrorSeverity::High
        );
        assert_eq!(RestartTrigger::Unhealthy.severity(), ErrorSeverity::High);
        assert_eq!(RestartTrigger::Manual.severity(), ErrorSeverity::Medium);

        assert_eq!(RestartTrigger::Panic.as_str(), "panic");
        assert_eq!(
            RestartTrigger::MemoryExceeded.to_string(),
            "memory_exceeded"
        );
    }

    #[test]
    fn test_supervisor_state_helpers() {
        assert!(SupervisorState::Terminal.is_terminal());
        assert!(!SupervisorState::Healthy.is_terminal());
        assert!(SupervisorState::Healthy.is_healthy());
        assert!(!SupervisorState::Restarting.is_healthy());
        assert_eq!(SupervisorState::default(), SupervisorState::Healthy);
    }

    #[test]
    fn test_restart_decision_helpers() {
        assert!(RestartDecision::RestartNow { attempt: 1 }.should_restart_now());
        assert!(!RestartDecision::Backoff {
            delay: Duration::from_secs(1)
        }
        .should_restart_now());
        assert!(RestartDecision::GiveUp {
            restarts_in_window: 3
        }
        .is_give_up());
        assert!(!RestartDecision::Disabled.is_give_up());
    }

    #[tokio::test]
    async fn test_supervisor_disabled_when_policy_disabled() {
        let supervisor = SelfHealingSupervisor::new(RestartPolicy::never());
        let decision = supervisor.on_unhealthy(RestartTrigger::Panic).await;
        assert_eq!(decision, RestartDecision::Disabled);
        assert!(supervisor.state().await.is_healthy());
    }

    #[tokio::test]
    async fn test_supervisor_severity_threshold_blocks_low_trigger() {
        // Require Critical: a Manual (Medium) trigger should be disabled.
        let policy =
            RestartPolicy::exponential_backoff().with_min_severity(ErrorSeverity::Critical);
        let supervisor = SelfHealingSupervisor::new(policy);
        assert_eq!(
            supervisor.on_unhealthy(RestartTrigger::Manual).await,
            RestartDecision::Disabled
        );
        // A Panic (Critical) is allowed.
        assert!(supervisor
            .on_unhealthy(RestartTrigger::Panic)
            .await
            .should_restart_now());
    }

    #[tokio::test]
    async fn test_supervisor_first_restart_immediate() {
        let policy = RestartPolicy::exponential_backoff()
            .with_base_delay(Duration::from_millis(50))
            .with_max_restarts(5);
        let supervisor = SelfHealingSupervisor::new(policy);

        let decision = supervisor.on_unhealthy(RestartTrigger::Panic).await;
        assert_eq!(decision, RestartDecision::RestartNow { attempt: 1 });
        assert_eq!(supervisor.state().await, SupervisorState::Restarting);
    }

    #[tokio::test]
    async fn test_supervisor_backoff_sequence() {
        let policy = RestartPolicy::exponential_backoff()
            .with_base_delay(Duration::from_millis(100))
            .with_max_delay(Duration::from_secs(10))
            .with_max_restarts(10);
        let supervisor = SelfHealingSupervisor::new(policy);

        // First failure: restart immediately, then confirm.
        assert!(supervisor
            .try_restart(RestartTrigger::Panic)
            .await
            .should_restart_now());

        // Immediately after, we are within the backoff window for attempt 2.
        // prior_restarts == 1 -> delay = base * 2^1 = 200ms.
        match supervisor.on_unhealthy(RestartTrigger::Panic).await {
            RestartDecision::Backoff { delay } => {
                assert!(delay <= Duration::from_millis(200));
                assert!(delay > Duration::ZERO);
            }
            other => panic!("expected backoff, got {:?}", other),
        }
        assert_eq!(supervisor.state().await, SupervisorState::BackingOff);

        // Wait out the backoff; now a restart is permitted.
        sleep(Duration::from_millis(220)).await;
        assert!(supervisor
            .on_unhealthy(RestartTrigger::Panic)
            .await
            .should_restart_now());
    }

    #[tokio::test]
    async fn test_supervisor_circuit_breaker_opens_after_n() {
        // Zero backoff so the breaker logic is exercised independently of timing.
        let policy = RestartPolicy::always().with_max_restarts(3);
        let supervisor = SelfHealingSupervisor::new(policy);

        // Three restarts are allowed.
        for attempt in 1..=3 {
            let decision = supervisor.try_restart(RestartTrigger::Panic).await;
            assert_eq!(decision, RestartDecision::RestartNow { attempt });
            assert!(!supervisor.is_terminal().await);
        }

        // The 4th attempt trips the breaker into the terminal state.
        let decision = supervisor.on_unhealthy(RestartTrigger::Panic).await;
        assert_eq!(
            decision,
            RestartDecision::GiveUp {
                restarts_in_window: 3
            }
        );
        assert!(supervisor.is_terminal().await);
        assert_eq!(supervisor.state().await, SupervisorState::Terminal);

        // Once terminal, further unhealthy reports keep giving up.
        assert!(supervisor
            .on_unhealthy(RestartTrigger::MemoryExceeded)
            .await
            .is_give_up());

        let stats = supervisor.stats().await;
        assert_eq!(stats.total_restarts, 3);
        assert_eq!(stats.trips, 1);
        assert!(stats.state.is_terminal());
        assert_eq!(stats.last_trigger, Some(RestartTrigger::Panic));
        // All three restarts were panics.
        assert_eq!(stats.triggers_in_window, vec![(RestartTrigger::Panic, 3)]);
    }

    #[tokio::test]
    async fn test_supervisor_window_expiry_allows_more_restarts() {
        let policy = RestartPolicy::always()
            .with_max_restarts(2)
            .with_min_severity(ErrorSeverity::High)
            .with_restart_window(Duration::from_millis(100));
        let supervisor = SelfHealingSupervisor::new(policy);

        assert!(supervisor
            .try_restart(RestartTrigger::Unhealthy)
            .await
            .should_restart_now());
        assert!(supervisor
            .try_restart(RestartTrigger::Unhealthy)
            .await
            .should_restart_now());

        // Third within the window trips the breaker.
        assert!(supervisor
            .on_unhealthy(RestartTrigger::Unhealthy)
            .await
            .is_give_up());

        // Reset out of terminal, let the window expire, and verify restarts are
        // permitted again because the old attempts have aged out.
        supervisor.reset().await;
        sleep(Duration::from_millis(130)).await;
        assert!(supervisor
            .on_unhealthy(RestartTrigger::Unhealthy)
            .await
            .should_restart_now());
    }

    #[tokio::test]
    async fn test_supervisor_reset_clears_terminal() {
        let policy = RestartPolicy::always().with_max_restarts(1);
        let supervisor = SelfHealingSupervisor::new(policy);

        assert!(supervisor
            .try_restart(RestartTrigger::Panic)
            .await
            .should_restart_now());
        assert!(supervisor
            .on_unhealthy(RestartTrigger::Panic)
            .await
            .is_give_up());
        assert!(supervisor.is_terminal().await);

        supervisor.reset().await;
        assert!(!supervisor.is_terminal().await);
        assert!(supervisor.state().await.is_healthy());
        // After reset the allowance is restored.
        assert!(supervisor
            .on_unhealthy(RestartTrigger::Panic)
            .await
            .should_restart_now());
    }

    #[tokio::test]
    async fn test_supervisor_on_recovered_clears_backoff() {
        let policy = RestartPolicy::exponential_backoff()
            .with_base_delay(Duration::from_secs(10))
            .with_max_restarts(5);
        let supervisor = SelfHealingSupervisor::new(policy);

        assert!(supervisor
            .try_restart(RestartTrigger::Panic)
            .await
            .should_restart_now());

        // Long backoff is in effect.
        assert!(matches!(
            supervisor.on_unhealthy(RestartTrigger::Panic).await,
            RestartDecision::Backoff { .. }
        ));

        // Recovery clears the backoff timer, so the next failure restarts now.
        supervisor.on_recovered().await;
        assert!(supervisor.state().await.is_healthy());
        assert!(supervisor
            .on_unhealthy(RestartTrigger::Panic)
            .await
            .should_restart_now());
    }

    #[tokio::test]
    async fn test_supervisor_linear_backoff() {
        let policy = RestartPolicy::linear_backoff()
            .with_base_delay(Duration::from_millis(50))
            .with_max_delay(Duration::from_secs(10))
            .with_max_restarts(10);
        let supervisor = SelfHealingSupervisor::new(policy);

        // backoff_for is internal; verify via behaviour: confirm one restart,
        // then the next decision should back off ~ base * (1+1) = 100ms.
        supervisor.confirm_restart(RestartTrigger::Panic).await;
        match supervisor.on_unhealthy(RestartTrigger::Panic).await {
            RestartDecision::Backoff { delay } => {
                assert!(delay <= Duration::from_millis(100));
            }
            other => panic!("expected backoff, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_supervisor_clone_shares_state() {
        let policy = RestartPolicy::always().with_max_restarts(1);
        let supervisor = SelfHealingSupervisor::new(policy);
        let clone = supervisor.clone();

        assert!(supervisor
            .try_restart(RestartTrigger::Panic)
            .await
            .should_restart_now());
        // The clone observes the trip triggered through the original.
        assert!(clone.on_unhealthy(RestartTrigger::Panic).await.is_give_up());
        assert!(supervisor.is_terminal().await);
    }
}
