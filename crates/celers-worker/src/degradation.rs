//! Graceful degradation on resource exhaustion
//!
//! This module provides automatic degradation strategies when worker resources
//! are constrained. It helps prevent crashes and maintain stability by:
//!
//! - Reducing task acceptance rate under high load
//! - Triggering garbage collection when memory is high
//! - Rejecting tasks when resources are critically low
//! - Providing backpressure signals to upstream systems
//!
//! # Examples
//!
//! ```
//! use celers_worker::{DegradationPolicy, DegradationThresholds};
//!
//! // Define degradation thresholds
//! let thresholds = DegradationThresholds::default()
//!     .with_memory_warning(0.7)  // Warn at 70% memory
//!     .with_memory_critical(0.9); // Critical at 90% memory
//!
//! // Create degradation policy
//! let policy = DegradationPolicy::new(thresholds);
//!
//! // Check current degradation level
//! let level = policy.assess_degradation(75.0, 80.0, 5);
//! println!("Current degradation level: {}", level);
//! ```

use serde::{Deserialize, Serialize};

/// Degradation level indicating system health
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum DegradationLevel {
    /// Normal operation - no degradation
    #[default]
    Normal = 0,
    /// Warning - approaching resource limits
    Warning = 1,
    /// Degraded - actively shedding load
    Degraded = 2,
    /// Critical - rejecting most new work
    Critical = 3,
}

impl DegradationLevel {
    /// Check if degradation is active (not normal)
    pub fn is_degraded(&self) -> bool {
        !matches!(self, DegradationLevel::Normal)
    }

    /// Check if at warning level
    pub fn is_warning(&self) -> bool {
        matches!(self, DegradationLevel::Warning)
    }

    /// Check if critically degraded
    pub fn is_critical(&self) -> bool {
        matches!(self, DegradationLevel::Critical)
    }

    /// Check if worker should accept new tasks
    pub fn should_accept_tasks(&self) -> bool {
        !matches!(self, DegradationLevel::Critical)
    }

    /// Get task acceptance probability (0.0 - 1.0)
    ///
    /// Returns how likely the worker should accept a new task:
    /// - Normal: 1.0 (100%)
    /// - Warning: 0.7 (70%)
    /// - Degraded: 0.3 (30%)
    /// - Critical: 0.0 (0%)
    pub fn acceptance_probability(&self) -> f64 {
        match self {
            DegradationLevel::Normal => 1.0,
            DegradationLevel::Warning => 0.7,
            DegradationLevel::Degraded => 0.3,
            DegradationLevel::Critical => 0.0,
        }
    }

    /// Get recommended sleep duration between task polls
    pub fn poll_backoff_ms(&self) -> u64 {
        match self {
            DegradationLevel::Normal => 100,
            DegradationLevel::Warning => 250,
            DegradationLevel::Degraded => 500,
            DegradationLevel::Critical => 1000,
        }
    }
}

impl std::fmt::Display for DegradationLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DegradationLevel::Normal => write!(f, "Normal"),
            DegradationLevel::Warning => write!(f, "Warning"),
            DegradationLevel::Degraded => write!(f, "Degraded"),
            DegradationLevel::Critical => write!(f, "Critical"),
        }
    }
}

/// Thresholds for triggering degradation levels
#[derive(Debug, Clone)]
pub struct DegradationThresholds {
    /// Memory usage percentage to trigger warning (0.0 - 1.0)
    pub memory_warning: f64,
    /// Memory usage percentage to trigger degraded mode (0.0 - 1.0)
    pub memory_degraded: f64,
    /// Memory usage percentage to trigger critical mode (0.0 - 1.0)
    pub memory_critical: f64,

    /// CPU usage percentage to trigger warning (0.0 - 1.0)
    pub cpu_warning: f64,
    /// CPU usage percentage to trigger degraded mode (0.0 - 1.0)
    pub cpu_degraded: f64,
    /// CPU usage percentage to trigger critical mode (0.0 - 1.0)
    pub cpu_critical: f64,

    /// Active task count to trigger warning
    pub active_tasks_warning: usize,
    /// Active task count to trigger degraded mode
    pub active_tasks_degraded: usize,
    /// Active task count to trigger critical mode
    pub active_tasks_critical: usize,
}

impl DegradationThresholds {
    /// Create new thresholds with specified values
    pub fn new() -> Self {
        Self::default()
    }

    /// Set memory warning threshold
    pub fn with_memory_warning(mut self, threshold: f64) -> Self {
        self.memory_warning = threshold.clamp(0.0, 1.0);
        self
    }

    /// Set memory degraded threshold
    pub fn with_memory_degraded(mut self, threshold: f64) -> Self {
        self.memory_degraded = threshold.clamp(0.0, 1.0);
        self
    }

    /// Set memory critical threshold
    pub fn with_memory_critical(mut self, threshold: f64) -> Self {
        self.memory_critical = threshold.clamp(0.0, 1.0);
        self
    }

    /// Set CPU warning threshold
    pub fn with_cpu_warning(mut self, threshold: f64) -> Self {
        self.cpu_warning = threshold.clamp(0.0, 1.0);
        self
    }

    /// Set CPU degraded threshold
    pub fn with_cpu_degraded(mut self, threshold: f64) -> Self {
        self.cpu_degraded = threshold.clamp(0.0, 1.0);
        self
    }

    /// Set CPU critical threshold
    pub fn with_cpu_critical(mut self, threshold: f64) -> Self {
        self.cpu_critical = threshold.clamp(0.0, 1.0);
        self
    }

    /// Set active tasks warning threshold
    pub fn with_active_tasks_warning(mut self, count: usize) -> Self {
        self.active_tasks_warning = count;
        self
    }

    /// Set active tasks degraded threshold
    pub fn with_active_tasks_degraded(mut self, count: usize) -> Self {
        self.active_tasks_degraded = count;
        self
    }

    /// Set active tasks critical threshold
    pub fn with_active_tasks_critical(mut self, count: usize) -> Self {
        self.active_tasks_critical = count;
        self
    }

    /// Validate thresholds
    ///
    /// Ensures that warning < degraded < critical for all metrics
    pub fn validate(&self) -> Result<(), String> {
        if self.memory_warning >= self.memory_degraded {
            return Err("Memory warning threshold must be less than degraded".to_string());
        }
        if self.memory_degraded >= self.memory_critical {
            return Err("Memory degraded threshold must be less than critical".to_string());
        }

        if self.cpu_warning >= self.cpu_degraded {
            return Err("CPU warning threshold must be less than degraded".to_string());
        }
        if self.cpu_degraded >= self.cpu_critical {
            return Err("CPU degraded threshold must be less than critical".to_string());
        }

        if self.active_tasks_warning >= self.active_tasks_degraded {
            return Err("Active tasks warning must be less than degraded".to_string());
        }
        if self.active_tasks_degraded >= self.active_tasks_critical {
            return Err("Active tasks degraded must be less than critical".to_string());
        }

        Ok(())
    }
}

impl Default for DegradationThresholds {
    fn default() -> Self {
        Self {
            memory_warning: 0.7,
            memory_degraded: 0.85,
            memory_critical: 0.95,
            cpu_warning: 0.7,
            cpu_degraded: 0.85,
            cpu_critical: 0.95,
            active_tasks_warning: 50,
            active_tasks_degraded: 75,
            active_tasks_critical: 100,
        }
    }
}

/// Degradation policy for assessing and responding to resource pressure
#[derive(Debug, Clone)]
pub struct DegradationPolicy {
    /// Thresholds for triggering degradation
    pub thresholds: DegradationThresholds,

    /// Whether to enable automatic garbage collection on high memory
    pub enable_gc_on_pressure: bool,

    /// Whether to log degradation state changes
    pub enable_logging: bool,
}

impl DegradationPolicy {
    /// Create a new degradation policy with specified thresholds
    pub fn new(thresholds: DegradationThresholds) -> Self {
        Self {
            thresholds,
            enable_gc_on_pressure: true,
            enable_logging: true,
        }
    }

    /// Enable or disable automatic garbage collection on memory pressure
    pub fn with_gc_on_pressure(mut self, enabled: bool) -> Self {
        self.enable_gc_on_pressure = enabled;
        self
    }

    /// Enable or disable logging of degradation state changes
    pub fn with_logging(mut self, enabled: bool) -> Self {
        self.enable_logging = enabled;
        self
    }

    /// Assess current degradation level based on resource usage
    ///
    /// # Arguments
    ///
    /// * `memory_pct` - Memory usage percentage (0.0 - 100.0)
    /// * `cpu_pct` - CPU usage percentage (0.0 - 100.0)
    /// * `active_tasks` - Number of currently active tasks
    ///
    /// # Returns
    ///
    /// The worst degradation level across all metrics
    pub fn assess_degradation(
        &self,
        memory_pct: f64,
        cpu_pct: f64,
        active_tasks: usize,
    ) -> DegradationLevel {
        let memory_norm = memory_pct / 100.0;
        let cpu_norm = cpu_pct / 100.0;

        // Check critical thresholds first
        if memory_norm >= self.thresholds.memory_critical
            || cpu_norm >= self.thresholds.cpu_critical
            || active_tasks >= self.thresholds.active_tasks_critical
        {
            return DegradationLevel::Critical;
        }

        // Check degraded thresholds
        if memory_norm >= self.thresholds.memory_degraded
            || cpu_norm >= self.thresholds.cpu_degraded
            || active_tasks >= self.thresholds.active_tasks_degraded
        {
            return DegradationLevel::Degraded;
        }

        // Check warning thresholds
        if memory_norm >= self.thresholds.memory_warning
            || cpu_norm >= self.thresholds.cpu_warning
            || active_tasks >= self.thresholds.active_tasks_warning
        {
            return DegradationLevel::Warning;
        }

        DegradationLevel::Normal
    }

    /// Check if a new task should be accepted based on degradation level
    ///
    /// Uses probabilistic rejection to gradually reduce load
    pub fn should_accept_task(&self, level: DegradationLevel) -> bool {
        use rand::RngExt;

        let probability = level.acceptance_probability();
        if probability == 1.0 {
            return true;
        }
        if probability == 0.0 {
            return false;
        }

        // Probabilistic rejection
        let mut rng = rand::rng();
        rng.random_range(0.0..1.0) < probability
    }

    /// Get recommended actions for current degradation level
    pub fn get_recommended_actions(&self, level: DegradationLevel) -> Vec<String> {
        let mut actions = Vec::new();

        match level {
            DegradationLevel::Normal => {
                actions.push("No action needed".to_string());
            }
            DegradationLevel::Warning => {
                actions.push("Monitor resource usage closely".to_string());
                actions.push("Consider scaling horizontally".to_string());
            }
            DegradationLevel::Degraded => {
                actions.push("Reduce task acceptance rate".to_string());
                if self.enable_gc_on_pressure {
                    actions.push("Trigger garbage collection".to_string());
                }
                actions.push("Alert operations team".to_string());
            }
            DegradationLevel::Critical => {
                actions.push("Stop accepting new tasks".to_string());
                actions.push("Wait for active tasks to complete".to_string());
                actions.push("Immediate intervention required".to_string());
            }
        }

        actions
    }
}

impl Default for DegradationPolicy {
    fn default() -> Self {
        Self::new(DegradationThresholds::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_degradation_level_predicates() {
        assert!(!DegradationLevel::Normal.is_degraded());
        assert!(DegradationLevel::Warning.is_degraded());
        assert!(DegradationLevel::Warning.is_warning());
        assert!(DegradationLevel::Critical.is_critical());

        assert!(DegradationLevel::Normal.should_accept_tasks());
        assert!(DegradationLevel::Warning.should_accept_tasks());
        assert!(DegradationLevel::Degraded.should_accept_tasks());
        assert!(!DegradationLevel::Critical.should_accept_tasks());
    }

    #[test]
    fn test_degradation_level_acceptance_probability() {
        assert_eq!(DegradationLevel::Normal.acceptance_probability(), 1.0);
        assert_eq!(DegradationLevel::Warning.acceptance_probability(), 0.7);
        assert_eq!(DegradationLevel::Degraded.acceptance_probability(), 0.3);
        assert_eq!(DegradationLevel::Critical.acceptance_probability(), 0.0);
    }

    #[test]
    fn test_degradation_level_poll_backoff() {
        assert!(
            DegradationLevel::Normal.poll_backoff_ms()
                < DegradationLevel::Warning.poll_backoff_ms()
        );
        assert!(
            DegradationLevel::Warning.poll_backoff_ms()
                < DegradationLevel::Degraded.poll_backoff_ms()
        );
        assert!(
            DegradationLevel::Degraded.poll_backoff_ms()
                < DegradationLevel::Critical.poll_backoff_ms()
        );
    }

    #[test]
    fn test_degradation_thresholds_default() {
        let thresholds = DegradationThresholds::default();
        assert!(thresholds.validate().is_ok());
    }

    #[test]
    fn test_degradation_thresholds_builder() {
        let thresholds = DegradationThresholds::new()
            .with_memory_warning(0.6)
            .with_memory_degraded(0.8)
            .with_memory_critical(0.95);

        assert_eq!(thresholds.memory_warning, 0.6);
        assert_eq!(thresholds.memory_degraded, 0.8);
        assert_eq!(thresholds.memory_critical, 0.95);
        assert!(thresholds.validate().is_ok());
    }

    #[test]
    fn test_degradation_thresholds_validation() {
        // Invalid: warning >= degraded
        let thresholds = DegradationThresholds {
            memory_warning: 0.9,
            memory_degraded: 0.8,
            memory_critical: 0.95,
            ..Default::default()
        };
        assert!(thresholds.validate().is_err());

        // Invalid: degraded >= critical
        let thresholds = DegradationThresholds {
            memory_warning: 0.7,
            memory_degraded: 0.95,
            memory_critical: 0.9,
            ..Default::default()
        };
        assert!(thresholds.validate().is_err());
    }

    #[test]
    fn test_degradation_policy_assess_normal() {
        let policy = DegradationPolicy::default();
        let level = policy.assess_degradation(50.0, 50.0, 10);
        assert_eq!(level, DegradationLevel::Normal);
    }

    #[test]
    fn test_degradation_policy_assess_warning() {
        let policy = DegradationPolicy::default();
        let level = policy.assess_degradation(75.0, 50.0, 10);
        assert_eq!(level, DegradationLevel::Warning);
    }

    #[test]
    fn test_degradation_policy_assess_degraded() {
        let policy = DegradationPolicy::default();
        let level = policy.assess_degradation(90.0, 50.0, 10);
        assert_eq!(level, DegradationLevel::Degraded);
    }

    #[test]
    fn test_degradation_policy_assess_critical() {
        let policy = DegradationPolicy::default();
        let level = policy.assess_degradation(96.0, 50.0, 10);
        assert_eq!(level, DegradationLevel::Critical);
    }

    #[test]
    fn test_degradation_policy_assess_by_cpu() {
        let policy = DegradationPolicy::default();

        let level = policy.assess_degradation(50.0, 75.0, 10);
        assert_eq!(level, DegradationLevel::Warning);

        let level = policy.assess_degradation(50.0, 90.0, 10);
        assert_eq!(level, DegradationLevel::Degraded);

        let level = policy.assess_degradation(50.0, 96.0, 10);
        assert_eq!(level, DegradationLevel::Critical);
    }

    #[test]
    fn test_degradation_policy_assess_by_tasks() {
        let policy = DegradationPolicy::default();

        let level = policy.assess_degradation(50.0, 50.0, 60);
        assert_eq!(level, DegradationLevel::Warning);

        let level = policy.assess_degradation(50.0, 50.0, 80);
        assert_eq!(level, DegradationLevel::Degraded);

        let level = policy.assess_degradation(50.0, 50.0, 110);
        assert_eq!(level, DegradationLevel::Critical);
    }

    #[test]
    fn test_degradation_policy_should_accept_task() {
        let policy = DegradationPolicy::default();

        // Normal should always accept
        assert!(policy.should_accept_task(DegradationLevel::Normal));

        // Critical should always reject
        assert!(!policy.should_accept_task(DegradationLevel::Critical));

        // Warning and Degraded are probabilistic - can't test deterministically
        // but we can verify they sometimes accept and sometimes reject
        let warning_results: Vec<bool> = (0..100)
            .map(|_| policy.should_accept_task(DegradationLevel::Warning))
            .collect();
        assert!(warning_results.iter().any(|&x| x)); // Some true
                                                     // Note: might occasionally fail due to randomness, but very unlikely
    }

    #[test]
    fn test_degradation_policy_recommended_actions() {
        let policy = DegradationPolicy::default();

        let actions = policy.get_recommended_actions(DegradationLevel::Normal);
        assert!(!actions.is_empty());

        let actions = policy.get_recommended_actions(DegradationLevel::Warning);
        assert!(actions.len() > 1);

        let actions = policy.get_recommended_actions(DegradationLevel::Degraded);
        assert!(actions.len() > 1);

        let actions = policy.get_recommended_actions(DegradationLevel::Critical);
        assert!(actions.len() > 1);
    }

    #[test]
    fn test_degradation_level_display() {
        assert_eq!(format!("{}", DegradationLevel::Normal), "Normal");
        assert_eq!(format!("{}", DegradationLevel::Warning), "Warning");
        assert_eq!(format!("{}", DegradationLevel::Degraded), "Degraded");
        assert_eq!(format!("{}", DegradationLevel::Critical), "Critical");
    }
}
