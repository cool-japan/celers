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

/// Convert fractional seconds to whole milliseconds, saturating and clamping at
/// zero.
///
/// Celery accepts float time limits, so a config file may legitimately carry
/// `0.5`. Negative and non-finite values are meaningless as limits and collapse
/// to `0`.
// Justification for the lossy cast: the value is checked for finiteness, clamped
// at zero and compared against `2^64` immediately before the conversion.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
#[must_use]
pub fn seconds_to_millis(seconds: f64) -> u64 {
    const U64_MAX_AS_F64: f64 = 18_446_744_073_709_551_616.0;
    if seconds.is_nan() || seconds <= 0.0 {
        return 0;
    }
    if seconds.is_infinite() {
        // An infinite limit is "effectively never"; saturate rather than
        // collapsing to zero, which `check()` would read as "already exceeded".
        return u64::MAX;
    }
    let millis = (seconds * 1000.0).round();
    if millis <= 0.0 {
        0
    } else if millis >= U64_MAX_AS_F64 {
        u64::MAX
    } else {
        millis as u64
    }
}

/// Convert whole milliseconds back to fractional seconds for serialization.
// Justification: limits are bounded by any realistic deployment far below 2^53.
#[allow(clippy::cast_precision_loss)]
#[must_use]
fn millis_to_seconds(millis: u64) -> f64 {
    millis as f64 / 1000.0
}

/// Serde adapter storing a limit as fractional seconds on the wire (matching
/// Celery's `time_limit` / `soft_time_limit`) while keeping millisecond
/// precision in memory.
mod serde_optional_seconds {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(millis: &Option<u64>, ser: S) -> Result<S::Ok, S::Error> {
        match millis {
            Some(value) => ser.serialize_some(&super::millis_to_seconds(*value)),
            None => ser.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<Option<u64>, D::Error> {
        let seconds = Option::<f64>::deserialize(de)?;
        // A configured limit of zero (or a negative/NaN one) is meaningless:
        // `check()` compares `elapsed >= limit`, so it would fire before the task
        // ran a single instruction. Treat it as "unset", matching
        // `TimeLimitConfig::with_soft_limit`.
        Ok(seconds
            .map(super::seconds_to_millis)
            .filter(|millis| *millis > 0))
    }
}

/// Time limit configuration for a task
///
/// Limits are stored in **milliseconds**. Storing whole seconds silently
/// truncated any sub-second configuration to zero, and `check()` treats a zero
/// limit as "already exceeded", so a 500 ms limit meant "every task is instantly
/// over its limit" rather than "over its limit after 500 ms". Celery accepts
/// float time limits, so sub-second values are a legitimate configuration.
///
/// On the wire the fields are still named `soft_seconds` / `hard_seconds` and
/// carry fractional seconds, so existing configuration files keep working.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct TimeLimitConfig {
    /// Soft time limit in milliseconds (warning before kill)
    #[serde(
        rename = "soft_seconds",
        default,
        skip_serializing_if = "Option::is_none",
        with = "serde_optional_seconds"
    )]
    pub soft_millis: Option<u64>,
    /// Hard time limit in milliseconds (force kill)
    #[serde(
        rename = "hard_seconds",
        default,
        skip_serializing_if = "Option::is_none",
        with = "serde_optional_seconds"
    )]
    pub hard_millis: Option<u64>,
}

impl TimeLimitConfig {
    /// Create a new time limit configuration with no limits
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the soft time limit
    ///
    /// Sub-second durations are preserved. `Duration::ZERO` is rejected as
    /// meaningless (it would mark every task as instantly over its limit) and
    /// leaves the limit unset.
    #[must_use]
    pub fn with_soft_limit(mut self, duration: Duration) -> Self {
        self.soft_millis = duration_to_millis(duration);
        self
    }

    /// Set the hard time limit
    ///
    /// Sub-second durations are preserved; `Duration::ZERO` leaves the limit
    /// unset (see [`Self::with_soft_limit`]).
    #[must_use]
    pub fn with_hard_limit(mut self, duration: Duration) -> Self {
        self.hard_millis = duration_to_millis(duration);
        self
    }

    /// Set both soft and hard limits
    #[must_use]
    pub fn with_limits(mut self, soft: Duration, hard: Duration) -> Self {
        self.soft_millis = duration_to_millis(soft);
        self.hard_millis = duration_to_millis(hard);
        self
    }

    /// Get the soft limit as Duration
    #[must_use]
    pub fn soft_limit(&self) -> Option<Duration> {
        self.soft_millis.map(Duration::from_millis)
    }

    /// Get the hard limit as Duration
    #[must_use]
    pub fn hard_limit(&self) -> Option<Duration> {
        self.hard_millis.map(Duration::from_millis)
    }

    /// Check if any time limit is configured
    #[inline]
    #[must_use]
    pub const fn has_limits(&self) -> bool {
        self.soft_millis.is_some() || self.hard_millis.is_some()
    }

    /// Merge with another config, taking non-None values from the other
    #[must_use]
    pub fn merge(&self, other: &TimeLimitConfig) -> TimeLimitConfig {
        TimeLimitConfig {
            soft_millis: other.soft_millis.or(self.soft_millis),
            hard_millis: other.hard_millis.or(self.hard_millis),
        }
    }
}

/// Convert a limit duration to milliseconds, rejecting a zero limit.
///
/// A zero limit is never a useful configuration: `check()` compares
/// `elapsed >= limit`, so it would fire before the task ran a single
/// instruction. Treating it as "unset" is the only sane interpretation.
fn duration_to_millis(duration: Duration) -> Option<u64> {
    let millis = u64::try_from(duration.as_millis()).unwrap_or(u64::MAX);
    if millis == 0 {
        None
    } else {
        Some(millis)
    }
}

/// Error type for time limit exceeded
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TimeLimitExceeded {
    /// Soft time limit exceeded (warning)
    SoftLimitExceeded {
        /// Task ID
        task_id: String,
        /// Elapsed time in milliseconds
        elapsed_millis: u64,
        /// Configured soft limit in milliseconds
        limit_millis: u64,
    },
    /// Hard time limit exceeded (force kill)
    HardLimitExceeded {
        /// Task ID
        task_id: String,
        /// Elapsed time in milliseconds
        elapsed_millis: u64,
        /// Configured hard limit in milliseconds
        limit_millis: u64,
    },
}

impl TimeLimitExceeded {
    /// The task this violation belongs to.
    #[inline]
    #[must_use]
    pub fn task_id(&self) -> &str {
        match self {
            Self::SoftLimitExceeded { task_id, .. } | Self::HardLimitExceeded { task_id, .. } => {
                task_id
            }
        }
    }

    /// How long the task had been running when the limit fired.
    #[inline]
    #[must_use]
    pub const fn elapsed(&self) -> Duration {
        match self {
            Self::SoftLimitExceeded { elapsed_millis, .. }
            | Self::HardLimitExceeded { elapsed_millis, .. } => {
                Duration::from_millis(*elapsed_millis)
            }
        }
    }

    /// The limit that was exceeded.
    #[inline]
    #[must_use]
    pub const fn limit(&self) -> Duration {
        match self {
            Self::SoftLimitExceeded { limit_millis, .. }
            | Self::HardLimitExceeded { limit_millis, .. } => Duration::from_millis(*limit_millis),
        }
    }

    /// Whether this is the hard (force-kill) limit rather than the soft one.
    #[inline]
    #[must_use]
    pub const fn is_hard(&self) -> bool {
        matches!(self, Self::HardLimitExceeded { .. })
    }
}

impl std::fmt::Display for TimeLimitExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SoftLimitExceeded {
                task_id,
                elapsed_millis,
                limit_millis,
            } => {
                let elapsed = millis_to_seconds(*elapsed_millis);
                let limit = millis_to_seconds(*limit_millis);
                write!(
                    f,
                    "Soft time limit exceeded for task {task_id}: {elapsed}s elapsed (limit: {limit}s)"
                )
            }
            Self::HardLimitExceeded {
                task_id,
                elapsed_millis,
                limit_millis,
            } => {
                let elapsed = millis_to_seconds(*elapsed_millis);
                let limit = millis_to_seconds(*limit_millis);
                write!(
                    f,
                    "Hard time limit exceeded for task {task_id}: {elapsed}s elapsed (limit: {limit}s)"
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
    #[must_use]
    pub fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }

    /// Get elapsed time in seconds
    #[inline]
    #[must_use]
    pub fn elapsed_seconds(&self) -> u64 {
        self.elapsed().as_secs()
    }

    /// Check current time limit status
    #[must_use]
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

    /// Get elapsed time in whole milliseconds
    #[inline]
    #[must_use]
    pub fn elapsed_millis(&self) -> u64 {
        u64::try_from(self.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    /// Check and return error if limit exceeded
    ///
    /// Comparisons use the full `Duration`, so a sub-second limit fires at the
    /// configured moment rather than immediately.
    #[must_use]
    pub fn check_exceeded(&self) -> Option<TimeLimitExceeded> {
        let elapsed = self.elapsed();
        let elapsed_millis = self.elapsed_millis();

        // Check hard limit first
        if let Some(limit) = self.config.hard_limit() {
            if elapsed >= limit {
                return Some(TimeLimitExceeded::HardLimitExceeded {
                    task_id: self.task_id.clone(),
                    elapsed_millis,
                    limit_millis: self.config.hard_millis.unwrap_or_default(),
                });
            }
        }

        // Check soft limit
        if let Some(limit) = self.config.soft_limit() {
            if elapsed >= limit {
                return Some(TimeLimitExceeded::SoftLimitExceeded {
                    task_id: self.task_id.clone(),
                    elapsed_millis,
                    limit_millis: self.config.soft_millis.unwrap_or_default(),
                });
            }
        }

        None
    }

    /// Check if soft limit was already warned
    #[inline]
    #[must_use]
    pub const fn soft_limit_warned(&self) -> bool {
        self.soft_limit_warned
    }

    /// Mark soft limit as warned
    pub fn mark_soft_limit_warned(&mut self) {
        self.soft_limit_warned = true;
    }

    /// Get remaining time until soft limit
    #[must_use]
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
    #[must_use]
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
    #[inline]
    #[must_use]
    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    /// Get the configuration
    #[inline]
    #[must_use]
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
    /// Per-task time limits (`task_name` -> config)
    limits: HashMap<String, TimeLimitConfig>,
    /// Default time limit for tasks without specific configuration
    default_config: Option<TimeLimitConfig>,
}

impl TaskTimeLimits {
    /// Create a new task time limits manager
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create with a default time limit for all tasks
    #[must_use]
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

    /// Get the effective time limit configuration for a task
    ///
    /// The per-task configuration is **merged onto** the default rather than
    /// replacing it, field by field. Choosing one or the other silently dropped
    /// the global soft limit whenever a task overrode only the hard limit — the
    /// natural way to say "this task may run longer" — leaving it with no soft
    /// limit at all.
    #[must_use]
    pub fn get_limit(&self, task_name: &str) -> Option<TimeLimitConfig> {
        match (self.limits.get(task_name), self.default_config.as_ref()) {
            (Some(task), Some(default)) => Some(default.merge(task)),
            (Some(task), None) => Some(task.clone()),
            (None, Some(default)) => Some(default.clone()),
            (None, None) => None,
        }
    }

    /// Check if a task type has time limits configured
    #[must_use]
    pub fn has_limit(&self, task_name: &str) -> bool {
        self.limits.contains_key(task_name) || self.default_config.is_some()
    }

    /// Create a time limit tracker for a task
    #[must_use]
    pub fn create_tracker(&self, task_id: &str, task_name: &str) -> Option<TimeLimit> {
        self.get_limit(task_name)
            .filter(TimeLimitConfig::has_limits)
            .map(|config| TimeLimit::new(task_id, config))
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
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create with a default time limit
    #[must_use]
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
    #[must_use]
    pub fn create_tracker(&self, task_id: &str, task_name: &str) -> Option<TimeLimit> {
        if let Ok(guard) = self.inner.read() {
            guard.create_tracker(task_id, task_name)
        } else {
            None
        }
    }

    /// Check if a task type has time limits configured
    #[must_use]
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
    /// Per-task time limits (`task_name` -> config)
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub task_limits: HashMap<String, TimeLimitConfig>,
}

impl TimeLimitSettings {
    /// Create a new empty settings
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a `TaskTimeLimits` from settings
    #[must_use]
    pub fn into_task_time_limits(self) -> TaskTimeLimits {
        let default_config =
            if self.default_soft_limit.is_some() || self.default_hard_limit.is_some() {
                Some(TimeLimitConfig {
                    soft_millis: self.default_soft_limit.map(|s| s.saturating_mul(1000)),
                    hard_millis: self.default_hard_limit.map(|s| s.saturating_mul(1000)),
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

    /// Back-date a tracker's start so limits can be asserted without sleeping.
    ///
    /// Returns `None` when the process has not been running long enough for the
    /// subtraction to be representable, which only happens on a machine that
    /// just booted; callers treat that as "skip".
    fn tracker_started_ago(
        task_id: &str,
        config: TimeLimitConfig,
        ago: Duration,
    ) -> Option<TimeLimit> {
        Instant::now()
            .checked_sub(ago)
            .map(|started| TimeLimit::with_start_time(task_id, config, started))
    }

    #[test]
    fn test_time_limit_soft_exceeded() {
        let config = TimeLimitConfig::new().with_soft_limit(Duration::from_millis(10));

        let Some(tracker) = tracker_started_ago("task-123", config, Duration::from_millis(15))
        else {
            return;
        };
        assert_eq!(tracker.check(), TimeLimitStatus::SoftLimitExceeded);
    }

    #[test]
    fn test_time_limit_hard_exceeded() {
        let config = TimeLimitConfig::new()
            .with_soft_limit(Duration::from_millis(5))
            .with_hard_limit(Duration::from_millis(10));

        let Some(tracker) = tracker_started_ago("task-123", config, Duration::from_millis(15))
        else {
            return;
        };
        assert_eq!(tracker.check(), TimeLimitStatus::HardLimitExceeded);
    }

    #[test]
    fn test_time_limit_exceeded_error() {
        let config = TimeLimitConfig::new().with_soft_limit(Duration::from_millis(10));

        let Some(tracker) = tracker_started_ago("task-123", config, Duration::from_millis(15))
        else {
            return;
        };

        let error = tracker.check_exceeded().expect("soft limit should be hit");
        assert!(matches!(error, TimeLimitExceeded::SoftLimitExceeded { .. }));
        assert_eq!(error.task_id(), "task-123");
        assert_eq!(error.limit(), Duration::from_millis(10));
        assert!(error.elapsed() >= Duration::from_millis(15));
        assert!(!error.is_hard());
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

        let slow_config = limits.get_limit("slow.task").expect("limit configured");
        assert_eq!(slow_config.soft_limit(), Some(Duration::from_secs(60)));
        assert_eq!(slow_config.hard_limit(), Some(Duration::from_secs(120)));
    }

    #[test]
    fn test_task_time_limits_default() {
        let limits = TaskTimeLimits::with_default(
            TimeLimitConfig::new().with_hard_limit(Duration::from_secs(300)),
        );

        // Unknown task should get default limit
        assert!(limits.has_limit("any.task"));
        let config = limits.get_limit("any.task").expect("default configured");
        assert_eq!(config.hard_limit(), Some(Duration::from_secs(300)));
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

        let override_config = TimeLimitConfig::new().with_soft_limit(Duration::from_secs(15));

        let merged = base.merge(&override_config);
        assert_eq!(merged.soft_limit(), Some(Duration::from_secs(15))); // Overridden
        assert_eq!(merged.hard_limit(), Some(Duration::from_secs(60))); // From base
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
            TimeLimitConfig::new()
                .with_soft_limit(Duration::from_secs(120))
                .with_hard_limit(Duration::from_secs(300)),
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
                        let _ = l.has_limit("my.task");
                        let _ = l.create_tracker(&format!("task-{i}"), "my.task");
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
            TimeLimitConfig::new()
                .with_soft_limit(Duration::from_secs(10))
                .with_hard_limit(Duration::from_secs(20)),
        );

        let limits = settings.into_task_time_limits();

        // Default should be applied
        let default = limits.get_limit("any.task").expect("default configured");
        assert_eq!(default.soft_limit(), Some(Duration::from_secs(30)));
        assert_eq!(default.hard_limit(), Some(Duration::from_secs(60)));

        // Custom should override
        let custom = limits.get_limit("custom.task").expect("custom configured");
        assert_eq!(custom.soft_limit(), Some(Duration::from_secs(10)));
        assert_eq!(custom.hard_limit(), Some(Duration::from_secs(20)));
    }

    // ------------------------------------------------------------------
    // Regression tests
    // ------------------------------------------------------------------

    /// Regression: limits were stored as whole seconds, so a sub-second limit
    /// truncated to zero and `check()` reported "exceeded" before the task ran.
    #[test]
    fn test_sub_second_limits_are_preserved() {
        let config = TimeLimitConfig::new()
            .with_soft_limit(Duration::from_millis(500))
            .with_hard_limit(Duration::from_millis(1_500));

        assert_eq!(config.soft_limit(), Some(Duration::from_millis(500)));
        assert_eq!(config.hard_limit(), Some(Duration::from_millis(1_500)));

        // At t=0 nothing is exceeded.
        let tracker = TimeLimit::with_start_time("task-1", config.clone(), Instant::now());
        assert_eq!(tracker.check(), TimeLimitStatus::Ok);
        assert!(tracker.check_exceeded().is_none());

        // At t=600ms the soft limit — and only the soft limit — has fired.
        if let Some(tracker) =
            tracker_started_ago("task-1", config.clone(), Duration::from_millis(600))
        {
            assert_eq!(tracker.check(), TimeLimitStatus::SoftLimitExceeded);
        }

        // At t=1.6s the hard limit has fired.
        if let Some(tracker) = tracker_started_ago("task-1", config, Duration::from_millis(1_600)) {
            assert_eq!(tracker.check(), TimeLimitStatus::HardLimitExceeded);
        }
    }

    #[test]
    fn test_zero_limit_is_treated_as_unset() {
        let config = TimeLimitConfig::new()
            .with_soft_limit(Duration::ZERO)
            .with_hard_limit(Duration::ZERO);
        assert!(!config.has_limits());

        let tracker = TimeLimit::with_start_time("task-1", config, Instant::now());
        assert_eq!(tracker.check(), TimeLimitStatus::Ok);
    }

    #[test]
    fn test_config_serializes_as_fractional_seconds() {
        let config = TimeLimitConfig::new()
            .with_soft_limit(Duration::from_millis(500))
            .with_hard_limit(Duration::from_secs(60));

        let json = serde_json::to_string(&config).expect("serialize");
        assert_eq!(json, r#"{"soft_seconds":0.5,"hard_seconds":60.0}"#);

        let parsed: TimeLimitConfig = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed, config);

        // Integer-seconds configuration files (the historical shape) still load.
        let legacy: TimeLimitConfig =
            serde_json::from_str(r#"{"soft_seconds":30,"hard_seconds":60}"#)
                .expect("legacy config should deserialize");
        assert_eq!(legacy.soft_limit(), Some(Duration::from_secs(30)));
        assert_eq!(legacy.hard_limit(), Some(Duration::from_secs(60)));

        // As does an empty config.
        let empty: TimeLimitConfig = serde_json::from_str("{}").expect("empty config");
        assert!(!empty.has_limits());
    }

    #[test]
    fn test_seconds_to_millis_is_saturating() {
        assert_eq!(seconds_to_millis(0.5), 500);
        assert_eq!(seconds_to_millis(1.25), 1_250);
        assert_eq!(seconds_to_millis(0.0), 0);
        assert_eq!(seconds_to_millis(-1.0), 0);
        assert_eq!(seconds_to_millis(f64::NAN), 0);
        assert_eq!(seconds_to_millis(f64::NEG_INFINITY), 0);
        assert_eq!(seconds_to_millis(f64::INFINITY), u64::MAX);
        assert_eq!(seconds_to_millis(1e30), u64::MAX);
    }

    #[test]
    fn test_zero_limits_in_config_files_deserialize_as_unset() {
        let config: TimeLimitConfig =
            serde_json::from_str(r#"{"soft_seconds":0,"hard_seconds":-3}"#).expect("deserialize");
        assert!(
            !config.has_limits(),
            "a zero limit would mark every task instantly over its limit"
        );
    }

    /// Regression: `get_limit` returned the per-task config *or* the default,
    /// so overriding only the hard limit silently dropped the global soft limit.
    #[test]
    fn test_per_task_override_merges_with_the_default() {
        let mut limits = TaskTimeLimits::with_default(
            TimeLimitConfig::new()
                .with_soft_limit(Duration::from_secs(30))
                .with_hard_limit(Duration::from_secs(60)),
        );
        limits.set_task_limit(
            "slow.task",
            TimeLimitConfig::new().with_hard_limit(Duration::from_secs(600)),
        );

        let effective = limits.get_limit("slow.task").expect("limit configured");
        assert_eq!(
            effective.soft_limit(),
            Some(Duration::from_secs(30)),
            "the global soft limit must survive a hard-limit-only override"
        );
        assert_eq!(effective.hard_limit(), Some(Duration::from_secs(600)));

        // A tracker built from the manager sees the merged configuration too.
        let tracker = limits
            .create_tracker("task-1", "slow.task")
            .expect("tracker should be created");
        assert_eq!(tracker.config().soft_limit(), Some(Duration::from_secs(30)));
        assert_eq!(
            tracker.config().hard_limit(),
            Some(Duration::from_secs(600))
        );

        // Tasks with no override still get the plain default.
        let default = limits.get_limit("other.task").expect("default configured");
        assert_eq!(default.soft_limit(), Some(Duration::from_secs(30)));
        assert_eq!(default.hard_limit(), Some(Duration::from_secs(60)));
    }

    #[test]
    fn test_settings_merge_default_with_per_task_override() {
        let mut settings = TimeLimitSettings::new();
        settings.default_soft_limit = Some(30);
        settings.default_hard_limit = Some(60);
        settings.task_limits.insert(
            "slow.task".to_string(),
            TimeLimitConfig::new().with_hard_limit(Duration::from_secs(600)),
        );

        let limits = settings.into_task_time_limits();
        let effective = limits.get_limit("slow.task").expect("limit configured");
        assert_eq!(effective.soft_limit(), Some(Duration::from_secs(30)));
        assert_eq!(effective.hard_limit(), Some(Duration::from_secs(600)));
    }
}
