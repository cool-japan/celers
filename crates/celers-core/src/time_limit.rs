//! Time Limits for Task Execution
//!
//! This module provides time limit enforcement for task execution:
//!
//! - **Soft Time Limit**: Warning before the task is killed, allowing graceful cleanup
//! - **Hard Time Limit**: Force kill after this duration
//!
//! # Example
//!
//! ```rust
//! use celers_core::time_limit::{TimeLimit, TimeLimitConfig, TimeLimitExceeded};
//! use std::time::Duration;
//!
//! // Create a time limit config
//! let config = TimeLimitConfig::new()
//!     .with_soft_limit(Duration::from_secs(30))
//!     .with_hard_limit(Duration::from_secs(60));
//!
//! assert_eq!(config.soft_limit(), Some(Duration::from_secs(30)));
//! assert_eq!(config.hard_limit(), Some(Duration::from_secs(60)));
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Time limit configuration for a task
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TimeLimitConfig {
    /// Soft time limit in seconds (warning before kill)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub soft_seconds: Option<u64>,
    /// Hard time limit in seconds (force kill)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hard_seconds: Option<u64>,
}

impl TimeLimitConfig {
    /// Create a new time limit configuration with no limits
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the soft time limit
    pub fn with_soft_limit(mut self, duration: Duration) -> Self {
        self.soft_seconds = Some(duration.as_secs());
        self
    }

    /// Set the hard time limit
    pub fn with_hard_limit(mut self, duration: Duration) -> Self {
        self.hard_seconds = Some(duration.as_secs());
        self
    }

    /// Set both soft and hard limits
    pub fn with_limits(mut self, soft: Duration, hard: Duration) -> Self {
        self.soft_seconds = Some(soft.as_secs());
        self.hard_seconds = Some(hard.as_secs());
        self
    }

    /// Get the soft limit as Duration
    pub fn soft_limit(&self) -> Option<Duration> {
        self.soft_seconds.map(Duration::from_secs)
    }

    /// Get the hard limit as Duration
    pub fn hard_limit(&self) -> Option<Duration> {
        self.hard_seconds.map(Duration::from_secs)
    }

    /// Check if any time limit is configured
    pub fn has_limits(&self) -> bool {
        self.soft_seconds.is_some() || self.hard_seconds.is_some()
    }

    /// Merge with another config, taking non-None values from the other
    pub fn merge(&self, other: &TimeLimitConfig) -> TimeLimitConfig {
        TimeLimitConfig {
            soft_seconds: other.soft_seconds.or(self.soft_seconds),
            hard_seconds: other.hard_seconds.or(self.hard_seconds),
        }
    }
}

/// Error type for time limit exceeded
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TimeLimitExceeded {
    /// Soft time limit exceeded (warning)
    SoftLimitExceeded {
        /// Task ID
        task_id: String,
        /// Elapsed time in seconds
        elapsed_seconds: u64,
        /// Configured soft limit in seconds
        limit_seconds: u64,
    },
    /// Hard time limit exceeded (force kill)
    HardLimitExceeded {
        /// Task ID
        task_id: String,
        /// Elapsed time in seconds
        elapsed_seconds: u64,
        /// Configured hard limit in seconds
        limit_seconds: u64,
    },
}

impl std::fmt::Display for TimeLimitExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SoftLimitExceeded {
                task_id,
                elapsed_seconds,
                limit_seconds,
            } => {
                write!(
                    f,
                    "Soft time limit exceeded for task {}: {}s elapsed (limit: {}s)",
                    task_id, elapsed_seconds, limit_seconds
                )
            }
            Self::HardLimitExceeded {
                task_id,
                elapsed_seconds,
                limit_seconds,
            } => {
                write!(
                    f,
                    "Hard time limit exceeded for task {}: {}s elapsed (limit: {}s)",
                    task_id, elapsed_seconds, limit_seconds
                )
            }
        }
    }
}

impl std::error::Error for TimeLimitExceeded {}

/// Status of time limit check
#[derive(Debug, Clone, PartialEq)]
pub enum TimeLimitStatus {
    /// No limit configured or within limits
    Ok,
    /// Soft limit exceeded (warning)
    SoftLimitExceeded,
    /// Hard limit exceeded (force kill)
    HardLimitExceeded,
}

/// Time limit tracker for a running task
#[derive(Debug, Clone)]
pub struct TimeLimit {
    /// Task ID being tracked
    task_id: String,
    /// Time limit configuration
    config: TimeLimitConfig,
    /// When the task started
    started_at: Instant,
    /// Whether soft limit warning has been emitted
    soft_limit_warned: bool,
}

impl TimeLimit {
    /// Create a new time limit tracker
    pub fn new(task_id: impl Into<String>, config: TimeLimitConfig) -> Self {
        Self {
            task_id: task_id.into(),
            config,
            started_at: Instant::now(),
            soft_limit_warned: false,
        }
    }

    /// Create with specific start time (for testing)
    pub fn with_start_time(
        task_id: impl Into<String>,
        config: TimeLimitConfig,
        started_at: Instant,
    ) -> Self {
        Self {
            task_id: task_id.into(),
            config,
            started_at,
            soft_limit_warned: false,
        }
    }

    /// Get elapsed time since task started
    pub fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }

    /// Get elapsed time in seconds
    pub fn elapsed_seconds(&self) -> u64 {
        self.elapsed().as_secs()
    }

    /// Check current time limit status
    pub fn check(&self) -> TimeLimitStatus {
        let elapsed = self.elapsed();

        // Check hard limit first
        if let Some(hard_limit) = self.config.hard_limit() {
            if elapsed >= hard_limit {
                return TimeLimitStatus::HardLimitExceeded;
            }
        }

        // Check soft limit
        if let Some(soft_limit) = self.config.soft_limit() {
            if elapsed >= soft_limit {
                return TimeLimitStatus::SoftLimitExceeded;
            }
        }

        TimeLimitStatus::Ok
    }

    /// Check and return error if limit exceeded
    pub fn check_exceeded(&self) -> Option<TimeLimitExceeded> {
        let elapsed_seconds = self.elapsed_seconds();

        // Check hard limit first
        if let Some(limit_seconds) = self.config.hard_seconds {
            if elapsed_seconds >= limit_seconds {
                return Some(TimeLimitExceeded::HardLimitExceeded {
                    task_id: self.task_id.clone(),
                    elapsed_seconds,
                    limit_seconds,
                });
            }
        }

        // Check soft limit
        if let Some(limit_seconds) = self.config.soft_seconds {
            if elapsed_seconds >= limit_seconds {
                return Some(TimeLimitExceeded::SoftLimitExceeded {
                    task_id: self.task_id.clone(),
                    elapsed_seconds,
                    limit_seconds,
                });
            }
        }

        None
    }

    /// Check if soft limit was already warned
    pub fn soft_limit_warned(&self) -> bool {
        self.soft_limit_warned
    }

    /// Mark soft limit as warned
    pub fn mark_soft_limit_warned(&mut self) {
        self.soft_limit_warned = true;
    }

    /// Get remaining time until soft limit
    pub fn time_until_soft_limit(&self) -> Option<Duration> {
        self.config.soft_limit().and_then(|limit| {
            let elapsed = self.elapsed();
            if elapsed < limit {
                Some(limit - elapsed)
            } else {
                None
            }
        })
    }

    /// Get remaining time until hard limit
    pub fn time_until_hard_limit(&self) -> Option<Duration> {
        self.config.hard_limit().and_then(|limit| {
            let elapsed = self.elapsed();
            if elapsed < limit {
                Some(limit - elapsed)
            } else {
                None
            }
        })
    }

    /// Get the task ID
    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    /// Get the configuration
    pub fn config(&self) -> &TimeLimitConfig {
        &self.config
    }
}

/// Per-task time limit manager
///
/// Manages time limits for multiple task types, allowing different
/// limits per task name.
#[derive(Debug, Default)]
pub struct TaskTimeLimits {
    /// Per-task time limits (task_name -> config)
    limits: HashMap<String, TimeLimitConfig>,
    /// Default time limit for tasks without specific configuration
    default_config: Option<TimeLimitConfig>,
}

impl TaskTimeLimits {
    /// Create a new task time limits manager
    pub fn new() -> Self {
        Self::default()
    }

    /// Create with a default time limit for all tasks
    pub fn with_default(config: TimeLimitConfig) -> Self {
        Self {
            limits: HashMap::new(),
            default_config: Some(config),
        }
    }

    /// Set time limit for a specific task type
    pub fn set_task_limit(&mut self, task_name: impl Into<String>, config: TimeLimitConfig) {
        self.limits.insert(task_name.into(), config);
    }

    /// Remove time limit for a specific task type
    pub fn remove_task_limit(&mut self, task_name: &str) {
        self.limits.remove(task_name);
    }

    /// Get time limit configuration for a task
    pub fn get_limit(&self, task_name: &str) -> Option<&TimeLimitConfig> {
        self.limits.get(task_name).or(self.default_config.as_ref())
    }

    /// Check if a task type has time limits configured
    pub fn has_limit(&self, task_name: &str) -> bool {
        self.limits.contains_key(task_name) || self.default_config.is_some()
    }

    /// Create a time limit tracker for a task
    pub fn create_tracker(&self, task_id: &str, task_name: &str) -> Option<TimeLimit> {
        self.get_limit(task_name)
            .filter(|c| c.has_limits())
            .map(|config| TimeLimit::new(task_id, config.clone()))
    }

    /// Set the default time limit configuration
    pub fn set_default(&mut self, config: TimeLimitConfig) {
        self.default_config = Some(config);
    }

    /// Clear all configurations
    pub fn clear(&mut self) {
        self.limits.clear();
        self.default_config = None;
    }
}

/// Thread-safe per-worker time limits manager
#[derive(Debug, Clone, Default)]
pub struct WorkerTimeLimits {
    inner: Arc<RwLock<TaskTimeLimits>>,
}

impl WorkerTimeLimits {
    /// Create a new worker time limits manager
    pub fn new() -> Self {
        Self::default()
    }

    /// Create with a default time limit
    pub fn with_default(config: TimeLimitConfig) -> Self {
        Self {
            inner: Arc::new(RwLock::new(TaskTimeLimits::with_default(config))),
        }
    }

    /// Set time limit for a specific task type
    pub fn set_task_limit(&self, task_name: impl Into<String>, config: TimeLimitConfig) {
        if let Ok(mut guard) = self.inner.write() {
            guard.set_task_limit(task_name, config);
        }
    }

    /// Remove time limit for a specific task type
    pub fn remove_task_limit(&self, task_name: &str) {
        if let Ok(mut guard) = self.inner.write() {
            guard.remove_task_limit(task_name);
        }
    }

    /// Create a time limit tracker for a task
    pub fn create_tracker(&self, task_id: &str, task_name: &str) -> Option<TimeLimit> {
        if let Ok(guard) = self.inner.read() {
            guard.create_tracker(task_id, task_name)
        } else {
            None
        }
    }

    /// Check if a task type has time limits configured
    pub fn has_limit(&self, task_name: &str) -> bool {
        if let Ok(guard) = self.inner.read() {
            guard.has_limit(task_name)
        } else {
            false
        }
    }

    /// Set the default time limit configuration
    pub fn set_default(&self, config: TimeLimitConfig) {
        if let Ok(mut guard) = self.inner.write() {
            guard.set_default(config);
        }
    }
}

/// Serializable time limit configuration for config files
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TimeLimitSettings {
    /// Default soft time limit in seconds
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_soft_limit: Option<u64>,
    /// Default hard time limit in seconds
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_hard_limit: Option<u64>,
    /// Per-task time limits (task_name -> config)
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub task_limits: HashMap<String, TimeLimitConfig>,
}

impl TimeLimitSettings {
    /// Create a new empty settings
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a TaskTimeLimits from settings
    pub fn into_task_time_limits(self) -> TaskTimeLimits {
        let default_config =
            if self.default_soft_limit.is_some() || self.default_hard_limit.is_some() {
                Some(TimeLimitConfig {
                    soft_seconds: self.default_soft_limit,
                    hard_seconds: self.default_hard_limit,
                })
            } else {
                None
            };

        TaskTimeLimits {
            limits: self.task_limits,
            default_config,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_time_limit_config() {
        let config = TimeLimitConfig::new()
            .with_soft_limit(Duration::from_secs(30))
            .with_hard_limit(Duration::from_secs(60));

        assert_eq!(config.soft_limit(), Some(Duration::from_secs(30)));
        assert_eq!(config.hard_limit(), Some(Duration::from_secs(60)));
        assert!(config.has_limits());
    }

    #[test]
    fn test_time_limit_config_no_limits() {
        let config = TimeLimitConfig::new();
        assert!(!config.has_limits());
        assert_eq!(config.soft_limit(), None);
        assert_eq!(config.hard_limit(), None);
    }

    #[test]
    fn test_time_limit_tracker() {
        let config = TimeLimitConfig::new()
            .with_soft_limit(Duration::from_secs(5))
            .with_hard_limit(Duration::from_secs(10));

        let tracker = TimeLimit::new("task-123", config);
        assert_eq!(tracker.task_id(), "task-123");
        assert_eq!(tracker.check(), TimeLimitStatus::Ok);
    }

    #[test]
    fn test_time_limit_soft_exceeded() {
        let config = TimeLimitConfig::new().with_soft_limit(Duration::from_millis(10));

        let tracker = TimeLimit::new("task-123", config);

        // Wait for soft limit to be exceeded
        thread::sleep(Duration::from_millis(15));

        assert_eq!(tracker.check(), TimeLimitStatus::SoftLimitExceeded);
    }

    #[test]
    fn test_time_limit_hard_exceeded() {
        let config = TimeLimitConfig::new()
            .with_soft_limit(Duration::from_millis(5))
            .with_hard_limit(Duration::from_millis(10));

        let tracker = TimeLimit::new("task-123", config);

        // Wait for hard limit to be exceeded
        thread::sleep(Duration::from_millis(15));

        assert_eq!(tracker.check(), TimeLimitStatus::HardLimitExceeded);
    }

    #[test]
    fn test_time_limit_exceeded_error() {
        let config = TimeLimitConfig::new().with_soft_limit(Duration::from_millis(10));

        let tracker = TimeLimit::new("task-123", config);
        thread::sleep(Duration::from_millis(15));

        let error = tracker.check_exceeded();
        assert!(error.is_some());
        assert!(matches!(
            error,
            Some(TimeLimitExceeded::SoftLimitExceeded { .. })
        ));
    }

    #[test]
    fn test_task_time_limits() {
        let mut limits = TaskTimeLimits::new();

        limits.set_task_limit(
            "slow.task",
            TimeLimitConfig::new()
                .with_soft_limit(Duration::from_secs(60))
                .with_hard_limit(Duration::from_secs(120)),
        );

        limits.set_task_limit(
            "fast.task",
            TimeLimitConfig::new().with_hard_limit(Duration::from_secs(10)),
        );

        assert!(limits.has_limit("slow.task"));
        assert!(limits.has_limit("fast.task"));
        assert!(!limits.has_limit("unknown.task"));

        let slow_config = limits.get_limit("slow.task").unwrap();
        assert_eq!(slow_config.soft_seconds, Some(60));
        assert_eq!(slow_config.hard_seconds, Some(120));
    }

    #[test]
    fn test_task_time_limits_default() {
        let limits = TaskTimeLimits::with_default(
            TimeLimitConfig::new().with_hard_limit(Duration::from_secs(300)),
        );

        // Unknown task should get default limit
        assert!(limits.has_limit("any.task"));
        let config = limits.get_limit("any.task").unwrap();
        assert_eq!(config.hard_seconds, Some(300));
    }

    #[test]
    fn test_create_tracker() {
        let mut limits = TaskTimeLimits::new();
        limits.set_task_limit(
            "my.task",
            TimeLimitConfig::new().with_hard_limit(Duration::from_secs(60)),
        );

        let tracker = limits.create_tracker("task-id-123", "my.task");
        assert!(tracker.is_some());

        let tracker = limits.create_tracker("task-id-456", "unknown.task");
        assert!(tracker.is_none());
    }

    #[test]
    fn test_time_remaining() {
        let config = TimeLimitConfig::new()
            .with_soft_limit(Duration::from_secs(30))
            .with_hard_limit(Duration::from_secs(60));

        let tracker = TimeLimit::new("task-123", config);

        let soft_remaining = tracker.time_until_soft_limit();
        assert!(soft_remaining.is_some());
        assert!(soft_remaining.unwrap() <= Duration::from_secs(30));

        let hard_remaining = tracker.time_until_hard_limit();
        assert!(hard_remaining.is_some());
        assert!(hard_remaining.unwrap() <= Duration::from_secs(60));
    }

    #[test]
    fn test_config_merge() {
        let base = TimeLimitConfig::new()
            .with_soft_limit(Duration::from_secs(30))
            .with_hard_limit(Duration::from_secs(60));

        let override_config = TimeLimitConfig {
            soft_seconds: Some(15),
            hard_seconds: None,
        };

        let merged = base.merge(&override_config);
        assert_eq!(merged.soft_seconds, Some(15)); // Overridden
        assert_eq!(merged.hard_seconds, Some(60)); // From base
    }

    #[test]
    fn test_soft_limit_warned() {
        let config = TimeLimitConfig::new().with_soft_limit(Duration::from_secs(30));

        let mut tracker = TimeLimit::new("task-123", config);
        assert!(!tracker.soft_limit_warned());

        tracker.mark_soft_limit_warned();
        assert!(tracker.soft_limit_warned());
    }

    #[test]
    fn test_time_limit_settings_serialization() {
        let mut settings = TimeLimitSettings::new();
        settings.default_soft_limit = Some(30);
        settings.default_hard_limit = Some(60);
        settings.task_limits.insert(
            "slow.task".to_string(),
            TimeLimitConfig {
                soft_seconds: Some(120),
                hard_seconds: Some(300),
            },
        );

        let json = serde_json::to_string(&settings).unwrap();
        let parsed: TimeLimitSettings = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.default_soft_limit, Some(30));
        assert_eq!(parsed.default_hard_limit, Some(60));
        assert!(parsed.task_limits.contains_key("slow.task"));
    }

    #[test]
    fn test_worker_time_limits_thread_safe() {
        let limits = WorkerTimeLimits::new();
        limits.set_task_limit(
            "my.task",
            TimeLimitConfig::new().with_hard_limit(Duration::from_secs(60)),
        );

        let limits_clone = limits.clone();

        // Spawn multiple threads to test thread safety
        let handles: Vec<_> = (0..4)
            .map(|i| {
                let l = limits_clone.clone();
                thread::spawn(move || {
                    for _ in 0..10 {
                        l.has_limit("my.task");
                        l.create_tracker(&format!("task-{i}"), "my.task");
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        assert!(limits.has_limit("my.task"));
    }

    #[test]
    fn test_into_task_time_limits() {
        let mut settings = TimeLimitSettings::new();
        settings.default_soft_limit = Some(30);
        settings.default_hard_limit = Some(60);
        settings.task_limits.insert(
            "custom.task".to_string(),
            TimeLimitConfig {
                soft_seconds: Some(10),
                hard_seconds: Some(20),
            },
        );

        let limits = settings.into_task_time_limits();

        // Default should be applied
        let default = limits.get_limit("any.task").unwrap();
        assert_eq!(default.soft_seconds, Some(30));
        assert_eq!(default.hard_seconds, Some(60));

        // Custom should override
        let custom = limits.get_limit("custom.task").unwrap();
        assert_eq!(custom.soft_seconds, Some(10));
        assert_eq!(custom.hard_seconds, Some(20));
    }
}
