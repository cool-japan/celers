//! Execution history and health tracking
//!
//! Contains types for tracking task execution history, health status,
//! retry policies, catch-up policies, jitter configuration, schedule
//! versioning, and dependency tracking.

use crate::schedule::Schedule;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

/// Schedule health status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ScheduleHealth {
    /// Schedule is healthy and functioning normally
    Healthy,
    /// Schedule has warnings but is still functional
    Warning { issues: Vec<String> },
    /// Schedule is unhealthy and may not execute properly
    Unhealthy { issues: Vec<String> },
}

impl ScheduleHealth {
    /// Check if the schedule is healthy
    pub fn is_healthy(&self) -> bool {
        matches!(self, ScheduleHealth::Healthy)
    }

    /// Check if the schedule has warnings
    pub fn has_warnings(&self) -> bool {
        matches!(self, ScheduleHealth::Warning { .. })
    }

    /// Check if the schedule is unhealthy
    pub fn is_unhealthy(&self) -> bool {
        matches!(self, ScheduleHealth::Unhealthy { .. })
    }

    /// Get all issues (warnings or errors)
    pub fn get_issues(&self) -> Vec<String> {
        match self {
            ScheduleHealth::Healthy => Vec::new(),
            ScheduleHealth::Warning { issues } | ScheduleHealth::Unhealthy { issues } => {
                issues.clone()
            }
        }
    }
}

/// Health check result for a scheduled task
#[derive(Debug, Clone)]
pub struct HealthCheckResult {
    /// Task name
    pub task_name: String,
    /// Overall health status
    pub health: ScheduleHealth,
    /// Next scheduled run time (if calculable)
    pub next_run: Option<DateTime<Utc>>,
    /// Time since last execution (if applicable)
    pub time_since_last_run: Option<chrono::Duration>,
}

impl HealthCheckResult {
    /// Create a new health check result
    pub fn new(task_name: String, health: ScheduleHealth) -> Self {
        Self {
            task_name,
            health,
            next_run: None,
            time_since_last_run: None,
        }
    }

    /// Create a healthy result
    pub fn healthy(task_name: String) -> Self {
        Self::new(task_name, ScheduleHealth::Healthy)
    }

    /// Create a warning result
    pub fn warning(task_name: String, issues: Vec<String>) -> Self {
        Self::new(task_name, ScheduleHealth::Warning { issues })
    }

    /// Create an unhealthy result
    pub fn unhealthy(task_name: String, issues: Vec<String>) -> Self {
        Self::new(task_name, ScheduleHealth::Unhealthy { issues })
    }

    /// Set next run time
    pub fn with_next_run(mut self, next_run: DateTime<Utc>) -> Self {
        self.next_run = Some(next_run);
        self
    }

    /// Set time since last run
    pub fn with_time_since_last_run(mut self, duration: chrono::Duration) -> Self {
        self.time_since_last_run = Some(duration);
        self
    }
}

/// Execution result status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ExecutionResult {
    /// Execution completed successfully
    Success,
    /// Execution failed with error message
    Failure { error: String },
    /// Execution timed out
    Timeout,
    /// Execution was interrupted (e.g., scheduler crash)
    Interrupted,
}

/// Execution state for crash recovery
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub enum ExecutionState {
    /// No execution in progress
    #[default]
    Idle,
    /// Execution is currently running
    Running {
        /// When execution started
        started_at: DateTime<Utc>,
        /// Expected timeout (for detection)
        timeout_after: Option<DateTime<Utc>>,
    },
}

impl ExecutionState {
    /// Check if execution is running
    pub fn is_running(&self) -> bool {
        matches!(self, ExecutionState::Running { .. })
    }

    /// Check if execution is idle
    pub fn is_idle(&self) -> bool {
        matches!(self, ExecutionState::Idle)
    }

    /// Check if a running execution has timed out
    pub fn has_timed_out(&self) -> bool {
        match self {
            ExecutionState::Running {
                timeout_after: Some(timeout),
                ..
            } => Utc::now() > *timeout,
            _ => false,
        }
    }

    /// Get duration since execution started (if running)
    pub fn running_duration(&self) -> Option<Duration> {
        match self {
            ExecutionState::Running { started_at, .. } => Some(Utc::now() - *started_at),
            ExecutionState::Idle => None,
        }
    }
}

/// Record of a single task execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionRecord {
    /// Execution start timestamp
    pub started_at: DateTime<Utc>,
    /// Execution end timestamp (if completed)
    pub completed_at: Option<DateTime<Utc>>,
    /// Execution result
    pub result: ExecutionResult,
    /// Execution duration in milliseconds
    pub duration_ms: Option<u64>,
}

impl ExecutionRecord {
    /// Create a new execution record
    pub fn new(started_at: DateTime<Utc>) -> Self {
        Self {
            started_at,
            completed_at: None,
            result: ExecutionResult::Success,
            duration_ms: None,
        }
    }

    /// Create a completed execution record
    pub fn completed(started_at: DateTime<Utc>, result: ExecutionResult) -> Self {
        let now = Utc::now();
        let duration_ms = now
            .signed_duration_since(started_at)
            .num_milliseconds()
            .max(0) as u64;

        Self {
            started_at,
            completed_at: Some(now),
            result,
            duration_ms: Some(duration_ms),
        }
    }

    /// Check if execution was successful
    pub fn is_success(&self) -> bool {
        matches!(self.result, ExecutionResult::Success)
    }

    /// Check if execution failed
    pub fn is_failure(&self) -> bool {
        matches!(self.result, ExecutionResult::Failure { .. })
    }

    /// Check if execution timed out
    pub fn is_timeout(&self) -> bool {
        matches!(self.result, ExecutionResult::Timeout)
    }

    /// Check if execution is completed
    pub fn is_completed(&self) -> bool {
        self.completed_at.is_some()
    }

    /// Check if execution was interrupted
    pub fn is_interrupted(&self) -> bool {
        matches!(self.result, ExecutionResult::Interrupted)
    }
}

/// Retry policy for failed task executions
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum RetryPolicy {
    /// Don't retry failed tasks
    #[default]
    NoRetry,
    /// Retry with fixed delay between attempts
    FixedDelay {
        /// Delay in seconds between retries
        delay_seconds: u64,
        /// Maximum number of retry attempts
        max_retries: u32,
    },
    /// Retry with exponential backoff
    ExponentialBackoff {
        /// Initial delay in seconds
        initial_delay_seconds: u64,
        /// Multiplier for each retry (e.g., 2.0 doubles the delay)
        multiplier: f64,
        /// Maximum delay in seconds
        max_delay_seconds: u64,
        /// Maximum number of retry attempts
        max_retries: u32,
    },
}

impl RetryPolicy {
    /// Calculate the next retry delay based on the number of attempts
    pub fn next_retry_delay(&self, attempt: u32) -> Option<u64> {
        match self {
            RetryPolicy::NoRetry => None,
            RetryPolicy::FixedDelay {
                delay_seconds,
                max_retries,
            } => {
                if attempt < *max_retries {
                    Some(*delay_seconds)
                } else {
                    None
                }
            }
            RetryPolicy::ExponentialBackoff {
                initial_delay_seconds,
                multiplier,
                max_delay_seconds,
                max_retries,
            } => {
                if attempt < *max_retries {
                    let delay = (*initial_delay_seconds as f64) * multiplier.powi(attempt as i32);
                    let delay = delay.min(*max_delay_seconds as f64) as u64;
                    Some(delay)
                } else {
                    None
                }
            }
        }
    }

    /// Check if retry is allowed for the given attempt
    pub fn should_retry(&self, attempt: u32) -> bool {
        self.next_retry_delay(attempt).is_some()
    }
}

/// Catch-up policy for handling missed schedules
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum CatchupPolicy {
    /// Skip all missed runs
    #[default]
    Skip,
    /// Run once immediately if any runs were missed
    RunOnce,
    /// Run multiple times to catch up (up to max_catchup runs)
    RunMultiple { max_catchup: u32 },
    /// Only run if missed within a time window (in seconds)
    TimeWindow { window_seconds: u64 },
}

impl CatchupPolicy {
    /// Check if we should execute a catch-up run
    pub fn should_catchup(
        &self,
        last_run_at: Option<DateTime<Utc>>,
        next_scheduled_run: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> bool {
        match self {
            CatchupPolicy::Skip => false,
            CatchupPolicy::RunOnce => last_run_at.is_some() && now > next_scheduled_run,
            CatchupPolicy::RunMultiple { .. } => last_run_at.is_some() && now > next_scheduled_run,
            CatchupPolicy::TimeWindow { window_seconds } => {
                if let Some(_last_run) = last_run_at {
                    let missed_duration = now.signed_duration_since(next_scheduled_run);
                    missed_duration.num_seconds() > 0
                        && missed_duration.num_seconds() <= *window_seconds as i64
                } else {
                    false
                }
            }
        }
    }

    /// Calculate number of catch-up runs to execute
    pub fn catchup_count(
        &self,
        last_run_at: Option<DateTime<Utc>>,
        interval_seconds: u64,
        now: DateTime<Utc>,
    ) -> u32 {
        match self {
            CatchupPolicy::Skip => 0,
            CatchupPolicy::RunOnce => {
                if last_run_at.is_some() {
                    1
                } else {
                    0
                }
            }
            CatchupPolicy::RunMultiple { max_catchup } => {
                if let Some(last_run) = last_run_at {
                    let elapsed = now.signed_duration_since(last_run).num_seconds() as u64;
                    let missed_runs = elapsed / interval_seconds;
                    std::cmp::min(missed_runs.saturating_sub(1) as u32, *max_catchup)
                } else {
                    0
                }
            }
            CatchupPolicy::TimeWindow { .. } => {
                if let Some(last_run) = last_run_at {
                    let next_run = last_run + Duration::seconds(interval_seconds as i64);
                    if self.should_catchup(last_run_at, next_run, now) {
                        1
                    } else {
                        0
                    }
                } else {
                    0
                }
            }
        }
    }
}

/// Jitter configuration for schedule randomization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Jitter {
    /// Minimum jitter offset in seconds (can be negative)
    pub min_seconds: i64,
    /// Maximum jitter offset in seconds
    pub max_seconds: i64,
}

impl Jitter {
    /// Create a new jitter configuration
    pub fn new(min_seconds: i64, max_seconds: i64) -> Self {
        Self {
            min_seconds,
            max_seconds,
        }
    }

    /// Create jitter with only positive offset (0 to max_seconds)
    pub fn positive(max_seconds: i64) -> Self {
        Self {
            min_seconds: 0,
            max_seconds,
        }
    }

    /// Create symmetric jitter (-seconds to +seconds)
    pub fn symmetric(seconds: i64) -> Self {
        Self {
            min_seconds: -seconds,
            max_seconds: seconds,
        }
    }

    /// Apply jitter to a datetime using hash-based deterministic randomization
    pub fn apply(&self, dt: DateTime<Utc>, task_name: &str) -> DateTime<Utc> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        // Use task name and datetime to generate deterministic random offset
        let mut hasher = DefaultHasher::new();
        task_name.hash(&mut hasher);
        dt.timestamp().hash(&mut hasher);
        let hash = hasher.finish();

        // Map hash to jitter range
        let range = (self.max_seconds - self.min_seconds) as u64;
        let offset = if range > 0 {
            (hash % range) as i64 + self.min_seconds
        } else {
            self.min_seconds
        };

        dt + Duration::seconds(offset)
    }
}

/// Schedule version record for tracking changes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleVersion {
    /// Version number (starts at 1)
    pub version: u32,
    /// The schedule at this version
    pub schedule: Schedule,
    /// Timestamp when this version was created
    pub created_at: DateTime<Utc>,
    /// Optional reason for the change
    #[serde(skip_serializing_if = "Option::is_none")]
    pub change_reason: Option<String>,
    /// Task configuration at this version (enabled, jitter, etc.)
    #[serde(default)]
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jitter: Option<Jitter>,
    #[serde(default)]
    pub catchup_policy: CatchupPolicy,
}

/// Task dependency status
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DependencyStatus {
    /// All dependencies satisfied
    Satisfied,
    /// Waiting for dependencies to complete
    Waiting { pending: Vec<String> },
    /// One or more dependencies failed
    Failed { failed: Vec<String> },
}

impl DependencyStatus {
    /// Check if dependencies are satisfied
    pub fn is_satisfied(&self) -> bool {
        matches!(self, DependencyStatus::Satisfied)
    }

    /// Check if any dependency failed
    pub fn has_failures(&self) -> bool {
        matches!(self, DependencyStatus::Failed { .. })
    }

    /// Get list of pending dependencies
    pub fn pending_tasks(&self) -> Vec<String> {
        match self {
            DependencyStatus::Waiting { pending } => pending.clone(),
            _ => Vec::new(),
        }
    }

    /// Get list of failed dependencies
    pub fn failed_tasks(&self) -> Vec<String> {
        match self {
            DependencyStatus::Failed { failed } => failed.clone(),
            _ => Vec::new(),
        }
    }
}
