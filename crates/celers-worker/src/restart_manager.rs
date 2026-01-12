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
use tracing::{info, warn};

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestartStrategy {
    /// Always restart immediately
    Always,
    /// Never restart automatically
    Never,
    /// Restart with exponential backoff
    ExponentialBackoff,
    /// Restart with linear backoff
    LinearBackoff,
}

impl Default for RestartStrategy {
    fn default() -> Self {
        Self::ExponentialBackoff
    }
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
            RestartStrategy::Always => true,
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

        let mut history = self.history.write().await;
        history.push(record);

        let mut last_restart = self.last_restart.write().await;
        *last_restart = Some(Instant::now());

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
}
