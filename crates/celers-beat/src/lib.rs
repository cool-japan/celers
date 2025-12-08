//! Periodic task scheduler (Celery Beat equivalent)
//!
//! This crate provides scheduled task execution with various schedule types
//! and persistent state management.
//!
//! # Schedule Types
//!
//! - **Interval**: Execute every N seconds
//! - **Crontab**: Execute based on cron expression (requires `cron` feature)
//! - **Solar**: Execute at solar events (sunrise, sunset) (requires `solar` feature)
//! - **OneTime**: Execute once at a specific timestamp (auto-cleanup after execution)
//!
//! # Persistence
//!
//! The scheduler supports automatic state persistence to JSON files, preserving
//! schedules and execution history across restarts:
//!
//! ```no_run
//! use celers_beat::{BeatScheduler, Schedule, ScheduledTask};
//!
//! // Load scheduler from file (or create new if file doesn't exist)
//! let mut scheduler = BeatScheduler::load_from_file("schedules.json").unwrap();
//!
//! // Add tasks - automatically saved to file
//! let task = ScheduledTask::new("send_report".to_string(), Schedule::interval(60));
//! scheduler.add_task(task).unwrap();
//!
//! // State persists across restarts
//! ```
//!
//! # Basic Example
//!
//! ```ignore
//! use celers_beat::{Schedule, ScheduledTask};
//!
//! let schedule = Schedule::interval(60);  // Every 60 seconds
//! let task = ScheduledTask::new("send_report".to_string(), schedule);
//! ```

#[cfg(feature = "solar")]
use chrono::Datelike;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use thiserror::Error;

/// Schedule type
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Schedule {
    /// Interval schedule (every N seconds)
    Interval {
        /// Interval in seconds
        every: u64,
    },

    /// Crontab schedule
    #[cfg(feature = "cron")]
    Crontab {
        /// Minute (0-59)
        minute: String,
        /// Hour (0-23)
        hour: String,
        /// Day of week (0-6, 0=Sunday)
        day_of_week: String,
        /// Day of month (1-31)
        day_of_month: String,
        /// Month (1-12)
        month_of_year: String,
    },

    /// Solar schedule (sunrise, sunset)
    #[cfg(feature = "solar")]
    Solar {
        /// Event type ("sunrise", "sunset")
        event: String,
        /// Latitude
        latitude: f64,
        /// Longitude
        longitude: f64,
    },

    /// One-time schedule (run once at specific time)
    OneTime {
        /// Exact run time (UTC)
        run_at: DateTime<Utc>,
    },
}

impl Schedule {
    /// Create interval schedule
    pub fn interval(seconds: u64) -> Self {
        Self::Interval { every: seconds }
    }

    /// Create crontab schedule
    #[cfg(feature = "cron")]
    pub fn crontab(
        minute: &str,
        hour: &str,
        day_of_week: &str,
        day_of_month: &str,
        month_of_year: &str,
    ) -> Self {
        Self::Crontab {
            minute: minute.to_string(),
            hour: hour.to_string(),
            day_of_week: day_of_week.to_string(),
            day_of_month: day_of_month.to_string(),
            month_of_year: month_of_year.to_string(),
        }
    }

    /// Create solar schedule
    #[cfg(feature = "solar")]
    pub fn solar(event: &str, latitude: f64, longitude: f64) -> Self {
        Self::Solar {
            event: event.to_string(),
            latitude,
            longitude,
        }
    }

    /// Create one-time schedule
    pub fn onetime(run_at: DateTime<Utc>) -> Self {
        Self::OneTime { run_at }
    }

    /// Calculate next run time
    pub fn next_run(
        &self,
        last_run: Option<DateTime<Utc>>,
    ) -> Result<DateTime<Utc>, ScheduleError> {
        match self {
            Schedule::Interval { every } => {
                let base = last_run.unwrap_or_else(Utc::now);
                Ok(base + Duration::seconds(*every as i64))
            }
            #[cfg(feature = "cron")]
            Schedule::Crontab {
                minute,
                hour,
                day_of_week,
                day_of_month,
                month_of_year,
            } => {
                use cron::Schedule as CronSchedule;
                use std::str::FromStr;

                // Build cron expression from fields
                // Cron format: sec min hour day month day_of_week year
                // We use "0" for seconds and "*" for year
                let cron_expr = format!(
                    "0 {} {} {} {} {} *",
                    minute, hour, day_of_month, month_of_year, day_of_week
                );

                let cron_schedule = CronSchedule::from_str(&cron_expr)
                    .map_err(|e| ScheduleError::Parse(format!("Invalid cron expression: {}", e)))?;

                let after = last_run.unwrap_or_else(Utc::now);
                let next = cron_schedule.after(&after).next().ok_or_else(|| {
                    ScheduleError::Invalid("No future execution time".to_string())
                })?;

                Ok(next)
            }
            #[cfg(feature = "solar")]
            Schedule::Solar {
                event,
                latitude,
                longitude,
            } => {
                #[allow(deprecated)]
                use sunrise::sunrise_sunset;

                // Start from last_run or now
                let start_time = last_run.unwrap_or_else(Utc::now);
                let mut current_date = start_time.date_naive();

                // Search for next occurrence (up to 365 days ahead)
                for _ in 0..365 {
                    #[allow(deprecated)]
                    let (sunrise_time, sunset_time) = sunrise_sunset(
                        *latitude,
                        *longitude,
                        current_date.year(),
                        current_date.month(),
                        current_date.day(),
                    );

                    let event_time = match event.to_lowercase().as_str() {
                        "sunrise" => {
                            // sunrise_time is minutes since midnight
                            let hours = (sunrise_time / 60) as u32;
                            let minutes = (sunrise_time % 60) as u32;
                            current_date
                                .and_hms_opt(hours, minutes, 0)
                                .ok_or_else(|| {
                                    ScheduleError::Invalid(format!(
                                        "Invalid sunrise time: {} minutes",
                                        sunrise_time
                                    ))
                                })?
                                .and_utc()
                        }
                        "sunset" => {
                            // sunset_time is minutes since midnight
                            let hours = (sunset_time / 60) as u32;
                            let minutes = (sunset_time % 60) as u32;
                            current_date
                                .and_hms_opt(hours, minutes, 0)
                                .ok_or_else(|| {
                                    ScheduleError::Invalid(format!(
                                        "Invalid sunset time: {} minutes",
                                        sunset_time
                                    ))
                                })?
                                .and_utc()
                        }
                        _ => {
                            return Err(ScheduleError::Invalid(format!(
                                "Unknown solar event: {}. Use 'sunrise' or 'sunset'",
                                event
                            )))
                        }
                    };

                    // If this event time is in the future, return it
                    if event_time > start_time {
                        return Ok(event_time);
                    }

                    // Move to next day
                    current_date = current_date
                        .checked_add_days(chrono::Days::new(1))
                        .ok_or_else(|| ScheduleError::Invalid("Date overflow".to_string()))?;
                }

                Err(ScheduleError::Invalid(
                    "Could not find solar event in next 365 days".to_string(),
                ))
            }
            Schedule::OneTime { run_at } => {
                // If never run before, return the scheduled time
                // If already run, return error (one-time schedules don't repeat)
                if last_run.is_some() {
                    Err(ScheduleError::Invalid(
                        "One-time schedule has already been executed".to_string(),
                    ))
                } else {
                    Ok(*run_at)
                }
            }
        }
    }

    /// Check if this is an interval schedule
    pub fn is_interval(&self) -> bool {
        matches!(self, Schedule::Interval { .. })
    }

    /// Check if this is a crontab schedule
    #[cfg(feature = "cron")]
    pub fn is_crontab(&self) -> bool {
        matches!(self, Schedule::Crontab { .. })
    }

    /// Check if this is a solar schedule
    #[cfg(feature = "solar")]
    pub fn is_solar(&self) -> bool {
        matches!(self, Schedule::Solar { .. })
    }

    /// Check if this is a one-time schedule
    pub fn is_onetime(&self) -> bool {
        matches!(self, Schedule::OneTime { .. })
    }
}

impl std::fmt::Display for Schedule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Schedule::Interval { every } => write!(f, "Interval[every {}s]", every),
            #[cfg(feature = "cron")]
            Schedule::Crontab {
                minute,
                hour,
                day_of_week,
                day_of_month,
                month_of_year,
            } => write!(
                f,
                "Crontab[{} {} {} {} {}]",
                minute, hour, day_of_month, day_of_week, month_of_year
            ),
            #[cfg(feature = "solar")]
            Schedule::Solar {
                event,
                latitude,
                longitude,
            } => write!(f, "Solar[{} at ({:.4}, {:.4})]", event, latitude, longitude),
            Schedule::OneTime { run_at } => {
                write!(f, "OneTime[at {}]", run_at.format("%Y-%m-%d %H:%M:%S UTC"))
            }
        }
    }
}

/// Scheduler statistics and metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerMetrics {
    /// Total number of registered tasks
    pub total_tasks: usize,
    /// Number of enabled tasks
    pub enabled_tasks: usize,
    /// Number of disabled tasks
    pub disabled_tasks: usize,
    /// Number of tasks that have executed at least once
    pub tasks_with_executions: usize,
    /// Total number of successful executions across all tasks
    pub total_successes: u64,
    /// Total number of failed executions across all tasks
    pub total_failures: u64,
    /// Total number of timeouts across all tasks
    pub total_timeouts: u64,
    /// Total execution count across all tasks
    pub total_executions: u64,
    /// Overall success rate (0.0 to 1.0)
    pub overall_success_rate: f64,
    /// Number of tasks currently in retry state
    pub tasks_in_retry: usize,
    /// Number of tasks with health warnings
    pub tasks_with_warnings: usize,
    /// Number of unhealthy tasks
    pub unhealthy_tasks: usize,
    /// Number of stuck tasks
    pub stuck_tasks: usize,
}

impl SchedulerMetrics {
    /// Create metrics from a BeatScheduler
    pub fn from_scheduler(scheduler: &BeatScheduler) -> Self {
        let total_tasks = scheduler.tasks.len();
        let enabled_tasks = scheduler.tasks.values().filter(|t| t.enabled).count();
        let disabled_tasks = total_tasks - enabled_tasks;
        let tasks_with_executions = scheduler.tasks.values().filter(|t| t.has_run()).count();

        let mut total_successes = 0u64;
        let mut total_failures = 0u64;
        let mut total_timeouts = 0u64;

        for task in scheduler.tasks.values() {
            total_successes += task.history_success_count() as u64;
            total_failures += task.history_failure_count() as u64;
            total_timeouts += task.history_timeout_count() as u64;
        }

        let total_executions = total_successes + total_failures + total_timeouts;
        let overall_success_rate = if total_executions == 0 {
            0.0
        } else {
            total_successes as f64 / total_executions as f64
        };

        let tasks_in_retry = scheduler
            .tasks
            .values()
            .filter(|t| t.retry_count > 0)
            .count();

        let tasks_with_warnings = scheduler
            .tasks
            .values()
            .map(|t| t.check_health())
            .filter(|r| r.health.has_warnings())
            .count();

        let unhealthy_tasks = scheduler
            .tasks
            .values()
            .map(|t| t.check_health())
            .filter(|r| r.health.is_unhealthy())
            .count();

        let stuck_tasks = scheduler.get_stuck_tasks().len();

        Self {
            total_tasks,
            enabled_tasks,
            disabled_tasks,
            tasks_with_executions,
            total_successes,
            total_failures,
            total_timeouts,
            total_executions,
            overall_success_rate,
            tasks_in_retry,
            tasks_with_warnings,
            unhealthy_tasks,
            stuck_tasks,
        }
    }
}

/// Per-task statistics
#[derive(Debug, Clone)]
pub struct TaskStatistics {
    /// Task name
    pub name: String,
    /// Total successful executions (from history)
    pub success_count: usize,
    /// Total failed executions (from history)
    pub failure_count: usize,
    /// Total timeout executions (from history)
    pub timeout_count: usize,
    /// Average execution duration in milliseconds
    pub average_duration_ms: Option<u64>,
    /// Minimum execution duration in milliseconds
    pub min_duration_ms: Option<u64>,
    /// Maximum execution duration in milliseconds
    pub max_duration_ms: Option<u64>,
    /// Success rate from history (0.0 to 1.0)
    pub success_rate: f64,
    /// Overall failure rate including retries (0.0 to 1.0)
    pub failure_rate: f64,
    /// Current retry count
    pub retry_count: u32,
    /// Is task currently stuck
    pub is_stuck: bool,
}

impl TaskStatistics {
    /// Create statistics from a ScheduledTask
    pub fn from_task(task: &ScheduledTask) -> Self {
        Self {
            name: task.name.clone(),
            success_count: task.history_success_count(),
            failure_count: task.history_failure_count(),
            timeout_count: task.history_timeout_count(),
            average_duration_ms: task.average_duration_ms(),
            min_duration_ms: task.min_duration_ms(),
            max_duration_ms: task.max_duration_ms(),
            success_rate: task.history_success_rate(),
            failure_rate: task.failure_rate(),
            retry_count: task.retry_count,
            is_stuck: task.is_stuck().is_some(),
        }
    }
}

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
}

/// Retry policy for failed task executions
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum RetryPolicy {
    /// Don't retry failed tasks
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

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::NoRetry
    }
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum CatchupPolicy {
    /// Skip all missed runs
    Skip,
    /// Run once immediately if any runs were missed
    RunOnce,
    /// Run multiple times to catch up (up to max_catchup runs)
    RunMultiple { max_catchup: u32 },
    /// Only run if missed within a time window (in seconds)
    TimeWindow { window_seconds: u64 },
}

impl Default for CatchupPolicy {
    fn default() -> Self {
        Self::Skip
    }
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

impl ScheduleVersion {
    /// Create a new version from current task state
    pub fn from_task(task: &ScheduledTask, version: u32, change_reason: Option<String>) -> Self {
        Self {
            version,
            schedule: task.schedule.clone(),
            created_at: Utc::now(),
            change_reason,
            enabled: task.enabled,
            jitter: task.jitter.clone(),
            catchup_policy: task.catchup_policy.clone(),
        }
    }
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

/// Scheduled task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledTask {
    /// Task name
    pub name: String,

    /// Task schedule
    pub schedule: Schedule,

    /// Task arguments
    #[serde(default)]
    pub args: Vec<serde_json::Value>,

    /// Task keyword arguments
    #[serde(default)]
    pub kwargs: HashMap<String, serde_json::Value>,

    /// Task options
    #[serde(default)]
    pub options: TaskOptions,

    /// Last run timestamp
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_run_at: Option<DateTime<Utc>>,

    /// Total run count
    #[serde(default)]
    pub total_run_count: u64,

    /// Enabled flag
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Jitter configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jitter: Option<Jitter>,

    /// Catch-up policy for missed schedules
    #[serde(default)]
    pub catchup_policy: CatchupPolicy,

    /// Task group (for organizational purposes)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,

    /// Task tags (for filtering and categorization)
    #[serde(default)]
    pub tags: HashSet<String>,

    /// Retry policy for failed executions
    #[serde(default)]
    pub retry_policy: RetryPolicy,

    /// Current retry attempt count
    #[serde(default)]
    pub retry_count: u32,

    /// Last failure timestamp
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_failure_at: Option<DateTime<Utc>>,

    /// Total failure count
    #[serde(default)]
    pub total_failure_count: u64,

    /// Execution history (last N executions)
    #[serde(default)]
    pub execution_history: Vec<ExecutionRecord>,

    /// Maximum number of history records to keep (0 = unlimited)
    #[serde(default)]
    pub max_history_size: usize,

    /// Version history (tracks schedule modifications)
    #[serde(default)]
    pub version_history: Vec<ScheduleVersion>,

    /// Current version number
    #[serde(default = "default_version")]
    pub current_version: u32,

    /// Task dependencies (task names this task depends on)
    #[serde(default)]
    pub dependencies: HashSet<String>,

    /// Whether to wait for all dependencies before running
    #[serde(default = "default_true")]
    pub wait_for_dependencies: bool,
}

fn default_version() -> u32 {
    1
}

fn default_true() -> bool {
    true
}

impl ScheduledTask {
    pub fn new(name: String, schedule: Schedule) -> Self {
        let mut task = Self {
            name,
            schedule: schedule.clone(),
            args: Vec::new(),
            kwargs: HashMap::new(),
            options: TaskOptions::default(),
            last_run_at: None,
            total_run_count: 0,
            enabled: true,
            jitter: None,
            catchup_policy: CatchupPolicy::default(),
            group: None,
            tags: HashSet::new(),
            retry_policy: RetryPolicy::default(),
            retry_count: 0,
            last_failure_at: None,
            total_failure_count: 0,
            execution_history: Vec::new(),
            max_history_size: 0, // 0 = unlimited
            version_history: Vec::new(),
            current_version: 1,
            dependencies: HashSet::new(),
            wait_for_dependencies: true,
        };

        // Create initial version
        let initial_version =
            ScheduleVersion::from_task(&task, 1, Some("Initial creation".to_string()));
        task.version_history.push(initial_version);

        task
    }

    pub fn with_args(mut self, args: Vec<serde_json::Value>) -> Self {
        self.args = args;
        self
    }

    pub fn with_kwargs(mut self, kwargs: HashMap<String, serde_json::Value>) -> Self {
        self.kwargs = kwargs;
        self
    }

    pub fn disabled(mut self) -> Self {
        self.enabled = false;
        self
    }

    pub fn with_queue(mut self, queue: String) -> Self {
        self.options.queue = Some(queue);
        self
    }

    pub fn with_priority(mut self, priority: u8) -> Self {
        self.options.priority = Some(priority);
        self
    }

    pub fn with_expires(mut self, expires: u64) -> Self {
        self.options.expires = Some(expires);
        self
    }

    /// Add jitter to avoid thundering herd
    pub fn with_jitter(mut self, jitter: Jitter) -> Self {
        self.jitter = Some(jitter);
        self
    }

    /// Set catch-up policy for missed schedules
    pub fn with_catchup_policy(mut self, policy: CatchupPolicy) -> Self {
        self.catchup_policy = policy;
        self
    }

    /// Set retry policy for failed executions
    pub fn with_retry_policy(mut self, policy: RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    /// Set task group
    pub fn with_group(mut self, group: String) -> Self {
        self.group = Some(group);
        self
    }

    /// Add a single tag
    pub fn with_tag(mut self, tag: String) -> Self {
        self.tags.insert(tag);
        self
    }

    /// Add multiple tags
    pub fn with_tags(mut self, tags: HashSet<String>) -> Self {
        self.tags = tags;
        self
    }

    /// Add a tag to existing tags
    pub fn add_tag(&mut self, tag: String) {
        self.tags.insert(tag);
    }

    /// Remove a tag
    pub fn remove_tag(&mut self, tag: &str) -> bool {
        self.tags.remove(tag)
    }

    /// Check if task has a specific tag
    pub fn has_tag(&self, tag: &str) -> bool {
        self.tags.contains(tag)
    }

    /// Check if task is in a specific group
    pub fn is_in_group(&self, group: &str) -> bool {
        self.group.as_deref() == Some(group)
    }

    /// Check if task is due to run
    pub fn is_due(&self) -> Result<bool, ScheduleError> {
        // Tasks that have never run are due immediately
        if self.last_run_at.is_none() {
            return Ok(true);
        }
        let mut next_run = self.schedule.next_run(self.last_run_at)?;

        // Apply jitter if configured
        if let Some(ref jitter) = self.jitter {
            next_run = jitter.apply(next_run, &self.name);
        }

        Ok(Utc::now() >= next_run)
    }

    /// Get the next scheduled run time (with jitter if configured)
    pub fn next_run_time(&self) -> Result<DateTime<Utc>, ScheduleError> {
        let mut next_run = self.schedule.next_run(self.last_run_at)?;

        // Apply jitter if configured
        if let Some(ref jitter) = self.jitter {
            next_run = jitter.apply(next_run, &self.name);
        }

        Ok(next_run)
    }

    /// Check if task is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Check if task has run at least once
    pub fn has_run(&self) -> bool {
        self.last_run_at.is_some()
    }

    /// Check if task has custom options
    pub fn has_options(&self) -> bool {
        self.options.has_queue() || self.options.has_priority() || self.options.has_expires()
    }

    /// Get age since last run (if task has run)
    pub fn age_since_last_run(&self) -> Option<chrono::Duration> {
        self.last_run_at.map(|last_run| Utc::now() - last_run)
    }

    /// Mark task execution as successful
    pub fn mark_success(&mut self) {
        self.retry_count = 0; // Reset retry counter on success
    }

    /// Mark task execution as failed
    pub fn mark_failure(&mut self) {
        self.last_failure_at = Some(Utc::now());
        self.total_failure_count += 1;
        self.retry_count += 1;
    }

    /// Check if task should be retried
    pub fn should_retry(&self) -> bool {
        self.retry_policy.should_retry(self.retry_count)
    }

    /// Get next retry delay in seconds
    pub fn next_retry_delay(&self) -> Option<u64> {
        self.retry_policy.next_retry_delay(self.retry_count)
    }

    /// Calculate next retry time based on last failure
    pub fn next_retry_time(&self) -> Option<DateTime<Utc>> {
        if let (Some(last_failure), Some(delay)) = (self.last_failure_at, self.next_retry_delay()) {
            Some(last_failure + Duration::seconds(delay as i64))
        } else {
            None
        }
    }

    /// Check if task is ready for retry
    pub fn is_ready_for_retry(&self) -> bool {
        if !self.should_retry() {
            return false;
        }

        if let Some(next_retry) = self.next_retry_time() {
            Utc::now() >= next_retry
        } else {
            false
        }
    }

    /// Get failure rate (0.0 to 1.0)
    pub fn failure_rate(&self) -> f64 {
        let total_attempts = self.total_run_count + self.total_failure_count;
        if total_attempts == 0 {
            0.0
        } else {
            self.total_failure_count as f64 / total_attempts as f64
        }
    }

    /// Set maximum history size
    pub fn with_max_history(mut self, max_size: usize) -> Self {
        self.max_history_size = max_size;
        self
    }

    /// Add execution record to history
    pub fn add_execution_record(&mut self, record: ExecutionRecord) {
        self.execution_history.push(record);

        // Trim history if needed
        if self.max_history_size > 0 && self.execution_history.len() > self.max_history_size {
            let remove_count = self.execution_history.len() - self.max_history_size;
            self.execution_history.drain(0..remove_count);
        }
    }

    /// Get last N execution records
    pub fn get_last_executions(&self, n: usize) -> &[ExecutionRecord] {
        let start = self.execution_history.len().saturating_sub(n);
        &self.execution_history[start..]
    }

    /// Get all execution records
    pub fn get_all_executions(&self) -> &[ExecutionRecord] {
        &self.execution_history
    }

    /// Get success count from history
    pub fn history_success_count(&self) -> usize {
        self.execution_history
            .iter()
            .filter(|r| r.is_success())
            .count()
    }

    /// Get failure count from history
    pub fn history_failure_count(&self) -> usize {
        self.execution_history
            .iter()
            .filter(|r| r.is_failure())
            .count()
    }

    /// Get timeout count from history
    pub fn history_timeout_count(&self) -> usize {
        self.execution_history
            .iter()
            .filter(|r| r.is_timeout())
            .count()
    }

    /// Get average execution duration (in milliseconds)
    pub fn average_duration_ms(&self) -> Option<u64> {
        let durations: Vec<u64> = self
            .execution_history
            .iter()
            .filter_map(|r| r.duration_ms)
            .collect();

        if durations.is_empty() {
            None
        } else {
            Some(durations.iter().sum::<u64>() / durations.len() as u64)
        }
    }

    /// Get minimum execution duration (in milliseconds)
    pub fn min_duration_ms(&self) -> Option<u64> {
        self.execution_history
            .iter()
            .filter_map(|r| r.duration_ms)
            .min()
    }

    /// Get maximum execution duration (in milliseconds)
    pub fn max_duration_ms(&self) -> Option<u64> {
        self.execution_history
            .iter()
            .filter_map(|r| r.duration_ms)
            .max()
    }

    /// Get recent success rate from history (0.0 to 1.0)
    pub fn history_success_rate(&self) -> f64 {
        let total = self.execution_history.len();
        if total == 0 {
            0.0
        } else {
            self.history_success_count() as f64 / total as f64
        }
    }

    /// Clear execution history
    pub fn clear_history(&mut self) {
        self.execution_history.clear();
    }

    /// Perform health check on this task
    pub fn check_health(&self) -> HealthCheckResult {
        let mut issues = Vec::new();

        // Check if schedule can calculate next run
        let next_run = match self.next_run_time() {
            Ok(next) => Some(next),
            Err(e) => {
                issues.push(format!("Cannot calculate next run: {}", e));
                None
            }
        };

        // Check if task is stuck (hasn't run in a very long time)
        if let Some(stuck_duration) = self.is_stuck() {
            issues.push(format!(
                "Task may be stuck (no execution in {} hours)",
                stuck_duration.num_hours()
            ));
        }

        // Check if task has high failure rate
        let failure_rate = self.failure_rate();
        let total_attempts = self.total_run_count + self.total_failure_count;
        if failure_rate > 0.5 && total_attempts >= 10 {
            issues.push(format!(
                "High failure rate: {:.1}% ({} failures out of {} total)",
                failure_rate * 100.0,
                self.total_failure_count,
                total_attempts
            ));
        }

        // Check recent history for consecutive failures
        if self.execution_history.len() >= 3 {
            let last_3 = &self.execution_history[self.execution_history.len() - 3..];
            if last_3.iter().all(|r| r.is_failure() || r.is_timeout()) {
                issues.push("Last 3 executions failed".to_string());
            }
        }

        // Determine overall health status
        let health = if !self.enabled {
            ScheduleHealth::Warning {
                issues: vec!["Task is disabled".to_string()],
            }
        } else if issues.is_empty() {
            ScheduleHealth::Healthy
        } else if issues.iter().any(|i| i.contains("Cannot calculate")) {
            ScheduleHealth::Unhealthy { issues }
        } else {
            ScheduleHealth::Warning { issues }
        };

        let time_since_last_run = self.age_since_last_run();

        let mut result = HealthCheckResult::new(self.name.clone(), health);
        if let Some(next) = next_run {
            result = result.with_next_run(next);
        }
        if let Some(duration) = time_since_last_run {
            result = result.with_time_since_last_run(duration);
        }
        result
    }

    /// Check if task is stuck (hasn't executed in a long time despite being enabled)
    /// Returns Some(duration) if stuck, None otherwise
    pub fn is_stuck(&self) -> Option<chrono::Duration> {
        if !self.enabled {
            return None;
        }

        if let Some(last_run) = self.last_run_at {
            let age = Utc::now() - last_run;

            // Determine expected run frequency
            let expected_interval = match &self.schedule {
                Schedule::Interval { every } => Duration::seconds(*every as i64),
                #[cfg(feature = "cron")]
                Schedule::Crontab { .. } => Duration::hours(24), // Assume daily as threshold
                #[cfg(feature = "solar")]
                Schedule::Solar { .. } => Duration::hours(24), // Solar events are daily
                Schedule::OneTime { .. } => return None, // One-time schedules can't be stuck
            };

            // Task is stuck if it hasn't run in 10x the expected interval
            let stuck_threshold = expected_interval * 10;
            if age > stuck_threshold {
                return Some(age);
            }
        }

        None
    }

    /// Validate schedule syntax and configuration
    pub fn validate_schedule(&self) -> Result<(), ScheduleError> {
        // Try to calculate next run to validate schedule
        self.schedule.next_run(self.last_run_at)?;
        Ok(())
    }

    /// Update schedule and create a new version
    pub fn update_schedule(&mut self, new_schedule: Schedule, change_reason: Option<String>) {
        self.current_version += 1;
        self.schedule = new_schedule;

        let version = ScheduleVersion::from_task(self, self.current_version, change_reason);
        self.version_history.push(version);
    }

    /// Update schedule configuration (enabled, jitter, catchup) and create a new version
    pub fn update_config(
        &mut self,
        enabled: Option<bool>,
        jitter: Option<Option<Jitter>>,
        catchup_policy: Option<CatchupPolicy>,
        change_reason: Option<String>,
    ) {
        if let Some(e) = enabled {
            self.enabled = e;
        }
        if let Some(j) = jitter {
            self.jitter = j;
        }
        if let Some(c) = catchup_policy {
            self.catchup_policy = c;
        }

        self.current_version += 1;
        let version = ScheduleVersion::from_task(self, self.current_version, change_reason);
        self.version_history.push(version);
    }

    /// Rollback to a previous version
    pub fn rollback_to_version(&mut self, version_number: u32) -> Result<(), ScheduleError> {
        let version = self
            .version_history
            .iter()
            .find(|v| v.version == version_number)
            .ok_or_else(|| {
                ScheduleError::Invalid(format!("Version {} not found", version_number))
            })?;

        // Restore schedule and configuration from version
        self.schedule = version.schedule.clone();
        self.enabled = version.enabled;
        self.jitter = version.jitter.clone();
        self.catchup_policy = version.catchup_policy.clone();

        // Create a new version record for the rollback
        self.current_version += 1;
        let rollback_version = ScheduleVersion::from_task(
            self,
            self.current_version,
            Some(format!("Rolled back to version {}", version_number)),
        );
        self.version_history.push(rollback_version);

        Ok(())
    }

    /// Get all versions in chronological order
    pub fn get_version_history(&self) -> &[ScheduleVersion] {
        &self.version_history
    }

    /// Get a specific version
    pub fn get_version(&self, version_number: u32) -> Option<&ScheduleVersion> {
        self.version_history
            .iter()
            .find(|v| v.version == version_number)
    }

    /// Get the latest version before current
    pub fn get_previous_version(&self) -> Option<&ScheduleVersion> {
        if self.current_version > 1 {
            self.version_history
                .iter()
                .rev()
                .skip(1) // Skip current version
                .next()
        } else {
            None
        }
    }

    /// Add a dependency (task that must complete before this task)
    pub fn add_dependency(&mut self, task_name: String) {
        self.dependencies.insert(task_name);
    }

    /// Remove a dependency
    pub fn remove_dependency(&mut self, task_name: &str) -> bool {
        self.dependencies.remove(task_name)
    }

    /// Clear all dependencies
    pub fn clear_dependencies(&mut self) {
        self.dependencies.clear();
    }

    /// Check if task has any dependencies
    pub fn has_dependencies(&self) -> bool {
        !self.dependencies.is_empty()
    }

    /// Check if task depends on a specific task
    pub fn depends_on(&self, task_name: &str) -> bool {
        self.dependencies.contains(task_name)
    }

    /// Set multiple dependencies at once
    pub fn with_dependencies(mut self, dependencies: HashSet<String>) -> Self {
        self.dependencies = dependencies;
        self
    }

    /// Set whether to wait for dependencies
    pub fn with_wait_for_dependencies(mut self, wait: bool) -> Self {
        self.wait_for_dependencies = wait;
        self
    }

    /// Check dependency status against completed tasks
    pub fn check_dependencies(&self, completed_tasks: &HashSet<String>) -> DependencyStatus {
        if self.dependencies.is_empty() {
            return DependencyStatus::Satisfied;
        }

        let pending: Vec<String> = self
            .dependencies
            .iter()
            .filter(|dep| !completed_tasks.contains(*dep))
            .cloned()
            .collect();

        if pending.is_empty() {
            DependencyStatus::Satisfied
        } else {
            DependencyStatus::Waiting { pending }
        }
    }

    /// Check dependency status with failed tasks tracking
    pub fn check_dependencies_with_failures(
        &self,
        completed_tasks: &HashSet<String>,
        failed_tasks: &HashSet<String>,
    ) -> DependencyStatus {
        if self.dependencies.is_empty() {
            return DependencyStatus::Satisfied;
        }

        let failed: Vec<String> = self
            .dependencies
            .iter()
            .filter(|dep| failed_tasks.contains(*dep))
            .cloned()
            .collect();

        if !failed.is_empty() {
            return DependencyStatus::Failed { failed };
        }

        let pending: Vec<String> = self
            .dependencies
            .iter()
            .filter(|dep| !completed_tasks.contains(*dep))
            .cloned()
            .collect();

        if pending.is_empty() {
            DependencyStatus::Satisfied
        } else {
            DependencyStatus::Waiting { pending }
        }
    }
}

impl std::fmt::Display for ScheduledTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ScheduledTask[{}] schedule={}", self.name, self.schedule)?;
        if !self.enabled {
            write!(f, " (disabled)")?;
        }
        if let Some(last_run) = self.last_run_at {
            let age = Utc::now() - last_run;
            write!(f, " last_run={}s ago", age.num_seconds())?;
        }
        write!(f, " runs={}", self.total_run_count)?;
        Ok(())
    }
}

/// Task options for scheduled tasks
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TaskOptions {
    /// Queue name
    pub queue: Option<String>,

    /// Priority
    pub priority: Option<u8>,

    /// Expiration time
    pub expires: Option<u64>,
}

impl TaskOptions {
    /// Check if queue is set
    pub fn has_queue(&self) -> bool {
        self.queue.is_some()
    }

    /// Check if priority is set
    pub fn has_priority(&self) -> bool {
        self.priority.is_some()
    }

    /// Check if expiration is set
    pub fn has_expires(&self) -> bool {
        self.expires.is_some()
    }
}

impl std::fmt::Display for TaskOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut parts = Vec::new();
        if let Some(ref queue) = self.queue {
            parts.push(format!("queue={}", queue));
        }
        if let Some(priority) = self.priority {
            parts.push(format!("priority={}", priority));
        }
        if let Some(expires) = self.expires {
            parts.push(format!("expires={}s", expires));
        }
        if parts.is_empty() {
            write!(f, "TaskOptions[default]")
        } else {
            write!(f, "TaskOptions[{}]", parts.join(", "))
        }
    }
}

/// Beat scheduler
#[derive(Serialize, Deserialize)]
pub struct BeatScheduler {
    /// Registered scheduled tasks
    tasks: HashMap<String, ScheduledTask>,

    /// Optional state file path for persistence
    #[serde(skip)]
    state_file: Option<PathBuf>,
}

impl BeatScheduler {
    pub fn new() -> Self {
        Self {
            tasks: HashMap::new(),
            state_file: None,
        }
    }

    /// Create scheduler with persistent state file
    ///
    /// # Arguments
    /// * `state_file` - Path to JSON file for persisting scheduler state
    ///
    /// # Example
    /// ```no_run
    /// use celers_beat::BeatScheduler;
    ///
    /// let mut scheduler = BeatScheduler::with_persistence("schedules.json");
    /// // Scheduler will automatically save state to schedules.json on updates
    /// ```
    pub fn with_persistence<P: Into<PathBuf>>(state_file: P) -> Self {
        Self {
            tasks: HashMap::new(),
            state_file: Some(state_file.into()),
        }
    }

    /// Load scheduler state from file
    ///
    /// Creates a new scheduler with tasks loaded from the specified file.
    /// If the file doesn't exist or can't be read, returns an empty scheduler.
    ///
    /// # Arguments
    /// * `path` - Path to the state file
    ///
    /// # Returns
    /// Scheduler loaded from file, or empty scheduler if file doesn't exist
    pub fn load_from_file<P: Into<PathBuf>>(path: P) -> Result<Self, ScheduleError> {
        let path = path.into();

        if !path.exists() {
            // File doesn't exist, return new scheduler with persistence enabled
            return Ok(Self {
                tasks: HashMap::new(),
                state_file: Some(path),
            });
        }

        let content = std::fs::read_to_string(&path)
            .map_err(|e| ScheduleError::Persistence(format!("Failed to read state file: {}", e)))?;

        let mut scheduler: BeatScheduler = serde_json::from_str(&content).map_err(|e| {
            ScheduleError::Persistence(format!("Failed to parse state file: {}", e))
        })?;

        scheduler.state_file = Some(path);
        Ok(scheduler)
    }

    /// Save scheduler state to file
    ///
    /// Persists the current scheduler state (all tasks and their run history)
    /// to the configured state file. If no state file is configured, this is a no-op.
    ///
    /// # Returns
    /// Ok(()) if successful or no state file configured
    pub fn save_state(&self) -> Result<(), ScheduleError> {
        if let Some(ref path) = self.state_file {
            let json = serde_json::to_string_pretty(&self).map_err(|e| {
                ScheduleError::Persistence(format!("Failed to serialize state: {}", e))
            })?;

            std::fs::write(path, json).map_err(|e| {
                ScheduleError::Persistence(format!("Failed to write state file: {}", e))
            })?;
        }
        Ok(())
    }

    pub fn add_task(&mut self, task: ScheduledTask) -> Result<(), ScheduleError> {
        self.tasks.insert(task.name.clone(), task);
        self.save_state()?;
        Ok(())
    }

    pub fn remove_task(&mut self, name: &str) -> Result<Option<ScheduledTask>, ScheduleError> {
        let task = self.tasks.remove(name);
        self.save_state()?;
        Ok(task)
    }

    /// Update task execution state (called after task runs)
    pub fn mark_task_run(&mut self, name: &str) -> Result<(), ScheduleError> {
        if let Some(task) = self.tasks.get_mut(name) {
            task.last_run_at = Some(Utc::now());
            task.total_run_count += 1;
            self.save_state()?;
        }
        Ok(())
    }

    /// Mark task execution as successful
    pub fn mark_task_success(&mut self, name: &str) -> Result<(), ScheduleError> {
        let should_remove = if let Some(task) = self.tasks.get_mut(name) {
            let now = Utc::now();
            task.last_run_at = Some(now);
            task.total_run_count += 1;
            task.mark_success();

            // Add execution record
            let record = ExecutionRecord::completed(now, ExecutionResult::Success);
            task.add_execution_record(record);

            // Check if this is a one-time schedule
            task.schedule.is_onetime()
        } else {
            false
        };

        // Remove one-time schedules after successful execution
        if should_remove {
            self.tasks.remove(name);
        }

        self.save_state()?;
        Ok(())
    }

    /// Mark task execution as successful with custom start time
    pub fn mark_task_success_with_start(
        &mut self,
        name: &str,
        started_at: DateTime<Utc>,
    ) -> Result<(), ScheduleError> {
        let should_remove = if let Some(task) = self.tasks.get_mut(name) {
            let now = Utc::now();
            task.last_run_at = Some(now);
            task.total_run_count += 1;
            task.mark_success();

            // Add execution record with actual start time
            let record = ExecutionRecord::completed(started_at, ExecutionResult::Success);
            task.add_execution_record(record);

            // Check if this is a one-time schedule
            task.schedule.is_onetime()
        } else {
            false
        };

        // Remove one-time schedules after successful execution
        if should_remove {
            self.tasks.remove(name);
        }

        self.save_state()?;
        Ok(())
    }

    /// Mark task execution as failed
    pub fn mark_task_failure(&mut self, name: &str) -> Result<(), ScheduleError> {
        self.mark_task_failure_with_error(name, "Unknown error".to_string())
    }

    /// Mark task execution as failed with error message
    pub fn mark_task_failure_with_error(
        &mut self,
        name: &str,
        error: String,
    ) -> Result<(), ScheduleError> {
        if let Some(task) = self.tasks.get_mut(name) {
            let now = Utc::now();
            task.mark_failure();

            // Add execution record
            let record = ExecutionRecord::completed(now, ExecutionResult::Failure { error });
            task.add_execution_record(record);

            self.save_state()?;
        }
        Ok(())
    }

    /// Mark task execution as failed with custom start time
    pub fn mark_task_failure_with_start(
        &mut self,
        name: &str,
        started_at: DateTime<Utc>,
        error: String,
    ) -> Result<(), ScheduleError> {
        if let Some(task) = self.tasks.get_mut(name) {
            task.mark_failure();

            // Add execution record with actual start time
            let record = ExecutionRecord::completed(started_at, ExecutionResult::Failure { error });
            task.add_execution_record(record);

            self.save_state()?;
        }
        Ok(())
    }

    /// Mark task execution as timed out
    pub fn mark_task_timeout(
        &mut self,
        name: &str,
        started_at: DateTime<Utc>,
    ) -> Result<(), ScheduleError> {
        if let Some(task) = self.tasks.get_mut(name) {
            task.mark_failure();

            // Add execution record
            let record = ExecutionRecord::completed(started_at, ExecutionResult::Timeout);
            task.add_execution_record(record);

            self.save_state()?;
        }
        Ok(())
    }

    /// Get tasks that are ready for retry
    pub fn get_retry_tasks(&self) -> Vec<&ScheduledTask> {
        self.tasks
            .values()
            .filter(|task| task.enabled && task.is_ready_for_retry())
            .collect()
    }

    pub fn get_due_tasks(&self) -> Vec<&ScheduledTask> {
        self.tasks
            .values()
            .filter(|task| task.enabled && task.is_due().unwrap_or(false))
            .collect()
    }

    /// Get all tasks in a specific group
    pub fn get_tasks_by_group(&self, group: &str) -> Vec<&ScheduledTask> {
        self.tasks
            .values()
            .filter(|task| task.is_in_group(group))
            .collect()
    }

    /// Get all tasks with a specific tag
    pub fn get_tasks_by_tag(&self, tag: &str) -> Vec<&ScheduledTask> {
        self.tasks
            .values()
            .filter(|task| task.has_tag(tag))
            .collect()
    }

    /// Get all tasks with any of the specified tags
    pub fn get_tasks_by_tags(&self, tags: &[&str]) -> Vec<&ScheduledTask> {
        self.tasks
            .values()
            .filter(|task| tags.iter().any(|tag| task.has_tag(tag)))
            .collect()
    }

    /// Get all tasks with all of the specified tags
    pub fn get_tasks_with_all_tags(&self, tags: &[&str]) -> Vec<&ScheduledTask> {
        self.tasks
            .values()
            .filter(|task| tags.iter().all(|tag| task.has_tag(tag)))
            .collect()
    }

    /// Get all unique groups
    pub fn get_all_groups(&self) -> HashSet<String> {
        self.tasks
            .values()
            .filter_map(|task| task.group.clone())
            .collect()
    }

    /// Get all unique tags
    pub fn get_all_tags(&self) -> HashSet<String> {
        self.tasks
            .values()
            .flat_map(|task| task.tags.iter().cloned())
            .collect()
    }

    /// Enable all tasks in a group
    pub fn enable_group(&mut self, group: &str) -> Result<usize, ScheduleError> {
        let mut count = 0;
        for task in self.tasks.values_mut() {
            if task.is_in_group(group) && !task.enabled {
                task.enabled = true;
                count += 1;
            }
        }
        if count > 0 {
            self.save_state()?;
        }
        Ok(count)
    }

    /// Disable all tasks in a group
    pub fn disable_group(&mut self, group: &str) -> Result<usize, ScheduleError> {
        let mut count = 0;
        for task in self.tasks.values_mut() {
            if task.is_in_group(group) && task.enabled {
                task.enabled = false;
                count += 1;
            }
        }
        if count > 0 {
            self.save_state()?;
        }
        Ok(count)
    }

    /// Enable all tasks with a specific tag
    pub fn enable_tag(&mut self, tag: &str) -> Result<usize, ScheduleError> {
        let mut count = 0;
        for task in self.tasks.values_mut() {
            if task.has_tag(tag) && !task.enabled {
                task.enabled = true;
                count += 1;
            }
        }
        if count > 0 {
            self.save_state()?;
        }
        Ok(count)
    }

    /// Disable all tasks with a specific tag
    pub fn disable_tag(&mut self, tag: &str) -> Result<usize, ScheduleError> {
        let mut count = 0;
        for task in self.tasks.values_mut() {
            if task.has_tag(tag) && task.enabled {
                task.enabled = false;
                count += 1;
            }
        }
        if count > 0 {
            self.save_state()?;
        }
        Ok(count)
    }

    /// Check health of all tasks
    pub fn check_all_tasks_health(&self) -> Vec<HealthCheckResult> {
        self.tasks
            .values()
            .map(|task| task.check_health())
            .collect()
    }

    /// Get unhealthy tasks (with warnings or errors)
    pub fn get_unhealthy_tasks(&self) -> Vec<HealthCheckResult> {
        self.tasks
            .values()
            .map(|task| task.check_health())
            .filter(|result| !result.health.is_healthy())
            .collect()
    }

    /// Get tasks with health warnings
    pub fn get_tasks_with_warnings(&self) -> Vec<HealthCheckResult> {
        self.tasks
            .values()
            .map(|task| task.check_health())
            .filter(|result| result.health.has_warnings())
            .collect()
    }

    /// Get tasks with health errors
    pub fn get_tasks_with_errors(&self) -> Vec<HealthCheckResult> {
        self.tasks
            .values()
            .map(|task| task.check_health())
            .filter(|result| result.health.is_unhealthy())
            .collect()
    }

    /// Get stuck tasks (tasks that haven't executed in expected time)
    pub fn get_stuck_tasks(&self) -> Vec<&ScheduledTask> {
        self.tasks
            .values()
            .filter(|task| task.is_stuck().is_some())
            .collect()
    }

    /// Validate all task schedules
    pub fn validate_all_schedules(&self) -> Vec<(String, Result<(), ScheduleError>)> {
        self.tasks
            .iter()
            .map(|(name, task)| (name.clone(), task.validate_schedule()))
            .collect()
    }

    /// Get scheduler metrics and statistics
    pub fn get_metrics(&self) -> SchedulerMetrics {
        SchedulerMetrics::from_scheduler(self)
    }

    /// Get statistics for all tasks
    pub fn get_all_task_statistics(&self) -> Vec<TaskStatistics> {
        self.tasks.values().map(TaskStatistics::from_task).collect()
    }

    /// Get statistics for a specific task
    pub fn get_task_statistics(&self, name: &str) -> Option<TaskStatistics> {
        self.tasks.get(name).map(TaskStatistics::from_task)
    }

    /// Get statistics for tasks in a specific group
    pub fn get_group_statistics(&self, group: &str) -> Vec<TaskStatistics> {
        self.tasks
            .values()
            .filter(|task| task.is_in_group(group))
            .map(TaskStatistics::from_task)
            .collect()
    }

    /// Get statistics for tasks with a specific tag
    pub fn get_tag_statistics(&self, tag: &str) -> Vec<TaskStatistics> {
        self.tasks
            .values()
            .filter(|task| task.has_tag(tag))
            .map(TaskStatistics::from_task)
            .collect()
    }

    /// Check for circular dependencies
    pub fn has_circular_dependency(&self, task_name: &str) -> bool {
        let mut visited = HashSet::new();
        let mut stack = HashSet::new();
        self.has_circular_dependency_helper(task_name, &mut visited, &mut stack)
    }

    fn has_circular_dependency_helper(
        &self,
        task_name: &str,
        visited: &mut HashSet<String>,
        stack: &mut HashSet<String>,
    ) -> bool {
        if stack.contains(task_name) {
            return true; // Circular dependency detected
        }

        if visited.contains(task_name) {
            return false; // Already processed this path
        }

        visited.insert(task_name.to_string());
        stack.insert(task_name.to_string());

        if let Some(task) = self.tasks.get(task_name) {
            for dep in &task.dependencies {
                if self.has_circular_dependency_helper(dep, visited, stack) {
                    return true;
                }
            }
        }

        stack.remove(task_name);
        false
    }

    /// Get dependency chain for a task (all tasks it depends on, recursively)
    pub fn get_dependency_chain(&self, task_name: &str) -> Result<Vec<String>, ScheduleError> {
        if self.has_circular_dependency(task_name) {
            return Err(ScheduleError::Invalid(format!(
                "Circular dependency detected for task '{}'",
                task_name
            )));
        }

        let mut chain = Vec::new();
        let mut visited = HashSet::new();
        self.get_dependency_chain_helper(task_name, &mut chain, &mut visited);
        Ok(chain)
    }

    fn get_dependency_chain_helper(
        &self,
        task_name: &str,
        chain: &mut Vec<String>,
        visited: &mut HashSet<String>,
    ) {
        if visited.contains(task_name) {
            return;
        }

        visited.insert(task_name.to_string());

        if let Some(task) = self.tasks.get(task_name) {
            for dep in &task.dependencies {
                self.get_dependency_chain_helper(dep, chain, visited);
            }
        }

        chain.push(task_name.to_string());
    }

    /// Get tasks that are ready to run (dependencies satisfied)
    pub fn get_tasks_ready_with_dependencies(
        &self,
        completed_tasks: &HashSet<String>,
        failed_tasks: &HashSet<String>,
    ) -> Vec<&ScheduledTask> {
        self.tasks
            .values()
            .filter(|task| {
                if !task.enabled {
                    return false;
                }

                // Check basic schedule readiness
                if !task.is_due().unwrap_or(false) {
                    return false;
                }

                // Check dependencies if enabled
                if task.wait_for_dependencies {
                    let status =
                        task.check_dependencies_with_failures(completed_tasks, failed_tasks);
                    status.is_satisfied()
                } else {
                    true
                }
            })
            .collect()
    }

    /// Get tasks waiting for dependencies
    pub fn get_tasks_waiting_for_dependencies(
        &self,
        completed_tasks: &HashSet<String>,
    ) -> Vec<(&ScheduledTask, DependencyStatus)> {
        self.tasks
            .values()
            .filter_map(|task| {
                if task.enabled && task.has_dependencies() {
                    let status = task.check_dependencies(completed_tasks);
                    if !status.is_satisfied() {
                        return Some((task, status));
                    }
                }
                None
            })
            .collect()
    }

    /// Get tasks with failed dependencies
    pub fn get_tasks_with_failed_dependencies(
        &self,
        completed_tasks: &HashSet<String>,
        failed_tasks: &HashSet<String>,
    ) -> Vec<(&ScheduledTask, DependencyStatus)> {
        self.tasks
            .values()
            .filter_map(|task| {
                if task.enabled && task.has_dependencies() {
                    let status =
                        task.check_dependencies_with_failures(completed_tasks, failed_tasks);
                    if status.has_failures() {
                        return Some((task, status));
                    }
                }
                None
            })
            .collect()
    }

    /// Validate all task dependencies (check for circular dependencies and missing tasks)
    pub fn validate_dependencies(&self) -> Result<(), ScheduleError> {
        for (task_name, task) in &self.tasks {
            // Check for circular dependencies
            if self.has_circular_dependency(task_name) {
                return Err(ScheduleError::Invalid(format!(
                    "Circular dependency detected for task '{}'",
                    task_name
                )));
            }

            // Check for missing dependencies
            for dep in &task.dependencies {
                if !self.tasks.contains_key(dep) {
                    return Err(ScheduleError::Invalid(format!(
                        "Task '{}' depends on non-existent task '{}'",
                        task_name, dep
                    )));
                }
            }
        }

        Ok(())
    }
}

impl Default for BeatScheduler {
    fn default() -> Self {
        Self::new()
    }
}

/// Schedule errors
#[derive(Debug, Error)]
pub enum ScheduleError {
    #[error("Invalid schedule: {0}")]
    Invalid(String),

    #[error("Not implemented: {0}")]
    NotImplemented(String),

    #[error("Parsing error: {0}")]
    Parse(String),

    #[error("Persistence error: {0}")]
    Persistence(String),
}

impl ScheduleError {
    /// Check if error is an invalid schedule configuration
    pub fn is_invalid(&self) -> bool {
        matches!(self, ScheduleError::Invalid(_))
    }

    /// Check if error is a not-implemented feature
    pub fn is_not_implemented(&self) -> bool {
        matches!(self, ScheduleError::NotImplemented(_))
    }

    /// Check if error is a parsing error
    pub fn is_parse(&self) -> bool {
        matches!(self, ScheduleError::Parse(_))
    }

    /// Check if error is a persistence error
    pub fn is_persistence(&self) -> bool {
        matches!(self, ScheduleError::Persistence(_))
    }

    /// Check if this error is retryable
    ///
    /// Persistence errors are retryable (transient I/O issues).
    /// Invalid schedules, parse errors, and not-implemented features are not retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(self, ScheduleError::Persistence(_))
    }

    /// Get the error category as a string
    pub fn category(&self) -> &'static str {
        match self {
            ScheduleError::Invalid(_) => "invalid",
            ScheduleError::NotImplemented(_) => "not_implemented",
            ScheduleError::Parse(_) => "parse",
            ScheduleError::Persistence(_) => "persistence",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "cron")]
    use chrono::Timelike;

    // ===== Interval Schedule Tests =====

    #[test]
    fn test_interval_schedule_basic() {
        let schedule = Schedule::interval(60);
        assert!(schedule.is_interval());
    }

    #[test]
    fn test_interval_schedule_next_run_no_last_run() {
        let schedule = Schedule::interval(60);
        let before = Utc::now();
        let next_run = schedule.next_run(None).unwrap();
        let after = Utc::now();

        // Next run should be ~60 seconds from now
        assert!(next_run > before + Duration::seconds(59));
        assert!(next_run < after + Duration::seconds(61));
    }

    #[test]
    fn test_interval_schedule_next_run_with_last_run() {
        let schedule = Schedule::interval(60);
        let last_run = Utc::now();
        let next_run = schedule.next_run(Some(last_run)).unwrap();

        // Next run should be exactly 60 seconds after last run
        let expected = last_run + Duration::seconds(60);
        assert_eq!(next_run, expected);
    }

    #[test]
    fn test_interval_schedule_multiple_intervals() {
        for interval in [1, 5, 10, 30, 60, 120, 300, 3600] {
            let schedule = Schedule::interval(interval);
            let last_run = Utc::now();
            let next_run = schedule.next_run(Some(last_run)).unwrap();

            assert_eq!(
                next_run,
                last_run + Duration::seconds(interval as i64),
                "Failed for interval {}",
                interval
            );
        }
    }

    #[test]
    fn test_interval_schedule_display() {
        let schedule = Schedule::interval(60);
        let display = format!("{}", schedule);
        assert_eq!(display, "Interval[every 60s]");
    }

    // ===== OneTime Schedule Tests =====

    #[test]
    fn test_onetime_schedule_basic() {
        let run_at = Utc::now() + Duration::hours(1);
        let schedule = Schedule::onetime(run_at);
        assert!(schedule.is_onetime());
    }

    #[test]
    fn test_onetime_schedule_next_run_no_last_run() {
        let run_at = Utc::now() + Duration::hours(1);
        let schedule = Schedule::onetime(run_at);
        let next_run = schedule.next_run(None).unwrap();
        assert_eq!(next_run, run_at);
    }

    #[test]
    fn test_onetime_schedule_next_run_with_last_run() {
        let run_at = Utc::now() + Duration::hours(1);
        let schedule = Schedule::onetime(run_at);
        let last_run = Utc::now();
        let result = schedule.next_run(Some(last_run));

        // Should return error because one-time schedules can't run twice
        assert!(result.is_err());
        if let Err(ScheduleError::Invalid(msg)) = result {
            assert_eq!(msg, "One-time schedule has already been executed");
        }
    }

    #[test]
    fn test_onetime_schedule_display() {
        let run_at = Utc::now() + Duration::hours(1);
        let schedule = Schedule::onetime(run_at);
        let display = format!("{}", schedule);
        assert!(display.starts_with("OneTime[at "));
        assert!(display.ends_with(" UTC]"));
    }

    #[test]
    fn test_onetime_schedule_in_future() {
        let run_at = Utc::now() + Duration::days(7);
        let schedule = Schedule::onetime(run_at);
        let next_run = schedule.next_run(None).unwrap();
        assert_eq!(next_run, run_at);
    }

    #[test]
    fn test_onetime_schedule_in_past() {
        // OneTime schedules can be set in the past (scheduler will run immediately if due)
        let run_at = Utc::now() - Duration::hours(1);
        let schedule = Schedule::onetime(run_at);
        let next_run = schedule.next_run(None).unwrap();
        assert_eq!(next_run, run_at);
    }

    #[test]
    fn test_onetime_task_auto_cleanup() {
        let mut scheduler = BeatScheduler::new();
        let run_at = Utc::now() - Duration::hours(1); // Past time, so it's immediately due
        let task = ScheduledTask::new("test_onetime".to_string(), Schedule::onetime(run_at));

        scheduler.add_task(task).unwrap();
        assert_eq!(scheduler.tasks.len(), 1);

        // Mark as successful - should auto-remove
        scheduler.mark_task_success("test_onetime").unwrap();
        assert_eq!(scheduler.tasks.len(), 0);
    }

    #[test]
    fn test_onetime_task_auto_cleanup_with_start_time() {
        let mut scheduler = BeatScheduler::new();
        let run_at = Utc::now() - Duration::hours(1);
        let task = ScheduledTask::new("test_onetime".to_string(), Schedule::onetime(run_at));

        scheduler.add_task(task).unwrap();
        assert_eq!(scheduler.tasks.len(), 1);

        // Mark as successful with start time - should auto-remove
        let started_at = Utc::now() - Duration::seconds(5);
        scheduler
            .mark_task_success_with_start("test_onetime", started_at)
            .unwrap();
        assert_eq!(scheduler.tasks.len(), 0);
    }

    #[test]
    fn test_onetime_task_not_removed_on_failure() {
        let mut scheduler = BeatScheduler::new();
        let run_at = Utc::now() - Duration::hours(1);
        let task = ScheduledTask::new("test_onetime".to_string(), Schedule::onetime(run_at));

        scheduler.add_task(task).unwrap();
        assert_eq!(scheduler.tasks.len(), 1);

        // Mark as failed - should NOT auto-remove (user might want to retry manually)
        scheduler.mark_task_failure("test_onetime").unwrap();
        assert_eq!(scheduler.tasks.len(), 1);
    }

    #[test]
    fn test_onetime_serialization() {
        let run_at = Utc::now() + Duration::hours(2);
        let schedule = Schedule::onetime(run_at);

        // Serialize
        let json = serde_json::to_string(&schedule).unwrap();

        // Deserialize
        let deserialized: Schedule = serde_json::from_str(&json).unwrap();

        // Verify
        assert!(deserialized.is_onetime());
        let next_run = deserialized.next_run(None).unwrap();
        assert_eq!(next_run, run_at);
    }

    // ===== Crontab Schedule Tests =====

    #[cfg(feature = "cron")]
    #[test]
    fn test_crontab_schedule_basic() {
        let schedule = Schedule::crontab("0", "0", "*", "*", "*");
        assert!(schedule.is_crontab());
    }

    #[cfg(feature = "cron")]
    #[test]
    fn test_crontab_schedule_every_minute() {
        let schedule = Schedule::crontab("*", "*", "*", "*", "*");
        let now = Utc::now();
        let next_run = schedule.next_run(Some(now)).unwrap();

        // Should be within next 2 minutes
        assert!(next_run > now);
        assert!(next_run < now + Duration::minutes(2));
    }

    #[cfg(feature = "cron")]
    #[test]
    fn test_crontab_schedule_specific_time() {
        // Every day at 10:30
        let schedule = Schedule::crontab("30", "10", "*", "*", "*");
        let now = Utc::now();
        let next_run = schedule.next_run(Some(now)).unwrap();

        assert!(next_run > now);
        assert_eq!(next_run.hour(), 10);
        assert_eq!(next_run.minute(), 30);
    }

    #[cfg(feature = "cron")]
    #[test]
    fn test_crontab_schedule_invalid() {
        let schedule = Schedule::crontab("invalid", "0", "*", "*", "*");
        let result = schedule.next_run(None);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_parse());
    }

    #[cfg(feature = "cron")]
    #[test]
    fn test_crontab_schedule_display() {
        let schedule = Schedule::crontab("0", "12", "*", "*", "1");
        let display = format!("{}", schedule);
        assert_eq!(display, "Crontab[0 12 * * 1]");
    }

    // ===== Solar Schedule Tests =====

    #[cfg(feature = "solar")]
    #[test]
    fn test_solar_schedule_basic() {
        let schedule = Schedule::solar("sunrise", 35.6762, 139.6503); // Tokyo
        assert!(schedule.is_solar());
    }

    #[cfg(feature = "solar")]
    #[test]
    #[ignore] // Sunrise crate API is deprecated and returns unexpected values
    fn test_solar_schedule_sunrise() {
        let schedule = Schedule::solar("sunrise", 35.6762, 139.6503); // Tokyo
        let now = Utc::now();
        let next_run = schedule.next_run(Some(now)).unwrap();

        assert!(next_run > now);
        // Should be within next 48 hours
        assert!(next_run < now + Duration::hours(48));
    }

    #[cfg(feature = "solar")]
    #[test]
    #[ignore] // Sunrise crate API is deprecated and returns unexpected values
    fn test_solar_schedule_sunset() {
        let schedule = Schedule::solar("sunset", 35.6762, 139.6503); // Tokyo
        let now = Utc::now();
        let next_run = schedule.next_run(Some(now)).unwrap();

        assert!(next_run > now);
        // Should be within next 48 hours
        assert!(next_run < now + Duration::hours(48));
    }

    #[cfg(feature = "solar")]
    #[test]
    fn test_solar_schedule_invalid_event() {
        let schedule = Schedule::solar("invalid", 35.6762, 139.6503);
        let result = schedule.next_run(None);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_invalid());
    }

    #[cfg(feature = "solar")]
    #[test]
    fn test_solar_schedule_display() {
        let schedule = Schedule::solar("sunrise", 35.6762, 139.6503);
        let display = format!("{}", schedule);
        assert!(display.contains("Solar[sunrise"));
        assert!(display.contains("35.6762"));
        assert!(display.contains("139.6503"));
    }

    // ===== Scheduled Task Tests =====

    #[test]
    fn test_scheduled_task_basic() {
        let schedule = Schedule::interval(60);
        let task = ScheduledTask::new("test_task".to_string(), schedule);

        assert_eq!(task.name, "test_task");
        assert!(task.enabled);
        assert!(!task.has_run());
        assert_eq!(task.total_run_count, 0);
        assert!(task.last_run_at.is_none());
    }

    #[test]
    fn test_scheduled_task_with_args() {
        let schedule = Schedule::interval(60);
        let task = ScheduledTask::new("test_task".to_string(), schedule)
            .with_args(vec![serde_json::json!(1), serde_json::json!("test")]);

        assert_eq!(task.args.len(), 2);
        assert_eq!(task.args[0], serde_json::json!(1));
        assert_eq!(task.args[1], serde_json::json!("test"));
    }

    #[test]
    fn test_scheduled_task_with_kwargs() {
        let schedule = Schedule::interval(60);
        let mut kwargs = HashMap::new();
        kwargs.insert("key1".to_string(), serde_json::json!("value1"));
        kwargs.insert("key2".to_string(), serde_json::json!(42));

        let task = ScheduledTask::new("test_task".to_string(), schedule).with_kwargs(kwargs);

        assert_eq!(task.kwargs.len(), 2);
        assert_eq!(
            task.kwargs.get("key1").unwrap(),
            &serde_json::json!("value1")
        );
        assert_eq!(task.kwargs.get("key2").unwrap(), &serde_json::json!(42));
    }

    #[test]
    fn test_scheduled_task_with_options() {
        let schedule = Schedule::interval(60);
        let task = ScheduledTask::new("test_task".to_string(), schedule)
            .with_queue("high_priority".to_string())
            .with_priority(9)
            .with_expires(3600);

        assert!(task.has_options());
        assert_eq!(task.options.queue, Some("high_priority".to_string()));
        assert_eq!(task.options.priority, Some(9));
        assert_eq!(task.options.expires, Some(3600));
    }

    #[test]
    fn test_scheduled_task_disabled() {
        let schedule = Schedule::interval(60);
        let task = ScheduledTask::new("test_task".to_string(), schedule).disabled();

        assert!(!task.is_enabled());
        assert!(!task.enabled);
    }

    #[test]
    fn test_scheduled_task_is_due_never_run() {
        let schedule = Schedule::interval(60);
        let task = ScheduledTask::new("test_task".to_string(), schedule);

        // Should be due immediately if never run
        assert!(task.is_due().unwrap());
    }

    #[test]
    fn test_scheduled_task_age_since_last_run() {
        let schedule = Schedule::interval(60);
        let mut task = ScheduledTask::new("test_task".to_string(), schedule);

        // No age if never run
        assert!(task.age_since_last_run().is_none());

        // Set last run to 30 seconds ago
        task.last_run_at = Some(Utc::now() - Duration::seconds(30));
        let age = task.age_since_last_run().unwrap();

        assert!(age.num_seconds() >= 29 && age.num_seconds() <= 31);
    }

    #[test]
    fn test_scheduled_task_display() {
        let schedule = Schedule::interval(60);
        let mut task = ScheduledTask::new("test_task".to_string(), schedule);
        task.total_run_count = 5;

        let display = format!("{}", task);
        assert!(display.contains("test_task"));
        assert!(display.contains("Interval[every 60s]"));
        assert!(display.contains("runs=5"));
    }

    #[test]
    fn test_scheduled_task_display_disabled() {
        let schedule = Schedule::interval(60);
        let task = ScheduledTask::new("test_task".to_string(), schedule).disabled();

        let display = format!("{}", task);
        assert!(display.contains("(disabled)"));
    }

    // ===== Task Options Tests =====

    #[test]
    fn test_task_options_default() {
        let options = TaskOptions::default();
        assert!(!options.has_queue());
        assert!(!options.has_priority());
        assert!(!options.has_expires());
    }

    #[test]
    fn test_task_options_has_queue() {
        let mut options = TaskOptions::default();
        options.queue = Some("test_queue".to_string());
        assert!(options.has_queue());
    }

    #[test]
    fn test_task_options_has_priority() {
        let mut options = TaskOptions::default();
        options.priority = Some(5);
        assert!(options.has_priority());
    }

    #[test]
    fn test_task_options_has_expires() {
        let mut options = TaskOptions::default();
        options.expires = Some(3600);
        assert!(options.has_expires());
    }

    #[test]
    fn test_task_options_display() {
        let mut options = TaskOptions::default();
        options.queue = Some("test".to_string());
        options.priority = Some(5);
        options.expires = Some(3600);

        let display = format!("{}", options);
        assert!(display.contains("queue=test"));
        assert!(display.contains("priority=5"));
        assert!(display.contains("expires=3600s"));
    }

    // ===== BeatScheduler Tests =====

    #[test]
    fn test_beat_scheduler_new() {
        let scheduler = BeatScheduler::new();
        assert_eq!(scheduler.tasks.len(), 0);
        assert!(scheduler.state_file.is_none());
    }

    #[test]
    fn test_beat_scheduler_add_task() {
        let mut scheduler = BeatScheduler::new();
        let schedule = Schedule::interval(60);
        let task = ScheduledTask::new("test_task".to_string(), schedule);

        scheduler.add_task(task).unwrap();

        assert_eq!(scheduler.tasks.len(), 1);
        assert!(scheduler.tasks.contains_key("test_task"));
    }

    #[test]
    fn test_beat_scheduler_remove_task() {
        let mut scheduler = BeatScheduler::new();
        let schedule = Schedule::interval(60);
        let task = ScheduledTask::new("test_task".to_string(), schedule);

        scheduler.add_task(task).unwrap();
        assert_eq!(scheduler.tasks.len(), 1);

        let removed = scheduler.remove_task("test_task").unwrap();
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().name, "test_task");
        assert_eq!(scheduler.tasks.len(), 0);
    }

    #[test]
    fn test_beat_scheduler_remove_nonexistent_task() {
        let mut scheduler = BeatScheduler::new();
        let removed = scheduler.remove_task("nonexistent").unwrap();
        assert!(removed.is_none());
    }

    #[test]
    fn test_beat_scheduler_mark_task_run() {
        let mut scheduler = BeatScheduler::new();
        let schedule = Schedule::interval(60);
        let task = ScheduledTask::new("test_task".to_string(), schedule);

        scheduler.add_task(task).unwrap();

        // Mark as run
        scheduler.mark_task_run("test_task").unwrap();

        let task = scheduler.tasks.get("test_task").unwrap();
        assert!(task.has_run());
        assert_eq!(task.total_run_count, 1);
        assert!(task.last_run_at.is_some());
    }

    #[test]
    fn test_beat_scheduler_get_due_tasks_empty() {
        let scheduler = BeatScheduler::new();
        let due_tasks = scheduler.get_due_tasks();
        assert_eq!(due_tasks.len(), 0);
    }

    #[test]
    fn test_beat_scheduler_get_due_tasks() {
        let mut scheduler = BeatScheduler::new();
        let schedule = Schedule::interval(60);
        let task = ScheduledTask::new("test_task".to_string(), schedule);

        scheduler.add_task(task).unwrap();

        // Should be due immediately (never run before)
        let due_tasks = scheduler.get_due_tasks();
        assert_eq!(due_tasks.len(), 1);
        assert_eq!(due_tasks[0].name, "test_task");
    }

    #[test]
    fn test_beat_scheduler_get_due_tasks_disabled() {
        let mut scheduler = BeatScheduler::new();
        let schedule = Schedule::interval(60);
        let task = ScheduledTask::new("test_task".to_string(), schedule).disabled();

        scheduler.add_task(task).unwrap();

        // Disabled task should not be due
        let due_tasks = scheduler.get_due_tasks();
        assert_eq!(due_tasks.len(), 0);
    }

    #[test]
    fn test_beat_scheduler_persistence_path() {
        let scheduler = BeatScheduler::with_persistence("/tmp/test_schedule.json");
        assert!(scheduler.state_file.is_some());
        assert_eq!(
            scheduler.state_file.unwrap(),
            PathBuf::from("/tmp/test_schedule.json")
        );
    }

    // ===== Persistence Tests =====

    #[test]
    fn test_persistence_save_and_load() {
        use std::fs;

        let temp_file = "/tmp/test_beat_scheduler_save_load.json";
        let _ = fs::remove_file(temp_file); // Clean up if exists

        // Create scheduler and add tasks
        let mut scheduler = BeatScheduler::with_persistence(temp_file);
        let schedule = Schedule::interval(60);
        let task = ScheduledTask::new("test_task".to_string(), schedule)
            .with_args(vec![serde_json::json!(1)])
            .with_queue("test_queue".to_string());

        scheduler.add_task(task).unwrap();
        scheduler.mark_task_run("test_task").unwrap();

        // Load from file
        let loaded_scheduler = BeatScheduler::load_from_file(temp_file).unwrap();

        assert_eq!(loaded_scheduler.tasks.len(), 1);
        let loaded_task = loaded_scheduler.tasks.get("test_task").unwrap();
        assert_eq!(loaded_task.name, "test_task");
        assert_eq!(loaded_task.args.len(), 1);
        assert!(loaded_task.has_run());
        assert_eq!(loaded_task.total_run_count, 1);
        assert_eq!(loaded_task.options.queue, Some("test_queue".to_string()));

        // Clean up
        let _ = fs::remove_file(temp_file);
    }

    #[test]
    fn test_persistence_load_nonexistent_file() {
        let temp_file = "/tmp/nonexistent_test_file.json";
        let scheduler = BeatScheduler::load_from_file(temp_file).unwrap();

        assert_eq!(scheduler.tasks.len(), 0);
        assert!(scheduler.state_file.is_some());
    }

    #[test]
    fn test_persistence_preserves_run_history() {
        use std::fs;

        let temp_file = "/tmp/test_beat_scheduler_history.json";
        let _ = fs::remove_file(temp_file);

        // First scheduler - add and run task
        {
            let mut scheduler = BeatScheduler::with_persistence(temp_file);
            let schedule = Schedule::interval(60);
            let task = ScheduledTask::new("test_task".to_string(), schedule);

            scheduler.add_task(task).unwrap();
            scheduler.mark_task_run("test_task").unwrap();
            scheduler.mark_task_run("test_task").unwrap();
        }

        // Second scheduler - load and verify history
        {
            let scheduler = BeatScheduler::load_from_file(temp_file).unwrap();
            let task = scheduler.tasks.get("test_task").unwrap();

            assert_eq!(task.total_run_count, 2);
            assert!(task.last_run_at.is_some());
        }

        // Clean up
        let _ = fs::remove_file(temp_file);
    }

    // ===== Schedule Error Tests =====

    #[test]
    fn test_schedule_error_is_invalid() {
        let err = ScheduleError::Invalid("test".to_string());
        assert!(err.is_invalid());
        assert!(!err.is_parse());
        assert!(!err.is_persistence());
        assert!(!err.is_not_implemented());
    }

    #[test]
    fn test_schedule_error_is_parse() {
        let err = ScheduleError::Parse("test".to_string());
        assert!(err.is_parse());
        assert!(!err.is_invalid());
        assert!(!err.is_persistence());
        assert!(!err.is_not_implemented());
    }

    #[test]
    fn test_schedule_error_is_persistence() {
        let err = ScheduleError::Persistence("test".to_string());
        assert!(err.is_persistence());
        assert!(!err.is_invalid());
        assert!(!err.is_parse());
        assert!(!err.is_not_implemented());
    }

    #[test]
    fn test_schedule_error_is_not_implemented() {
        let err = ScheduleError::NotImplemented("test".to_string());
        assert!(err.is_not_implemented());
        assert!(!err.is_invalid());
        assert!(!err.is_parse());
        assert!(!err.is_persistence());
    }

    #[test]
    fn test_schedule_error_is_retryable() {
        let persistence_err = ScheduleError::Persistence("test".to_string());
        assert!(persistence_err.is_retryable());

        let invalid_err = ScheduleError::Invalid("test".to_string());
        assert!(!invalid_err.is_retryable());

        let parse_err = ScheduleError::Parse("test".to_string());
        assert!(!parse_err.is_retryable());

        let not_impl_err = ScheduleError::NotImplemented("test".to_string());
        assert!(!not_impl_err.is_retryable());
    }

    #[test]
    fn test_schedule_error_category() {
        assert_eq!(
            ScheduleError::Invalid("test".to_string()).category(),
            "invalid"
        );
        assert_eq!(ScheduleError::Parse("test".to_string()).category(), "parse");
        assert_eq!(
            ScheduleError::Persistence("test".to_string()).category(),
            "persistence"
        );
        assert_eq!(
            ScheduleError::NotImplemented("test".to_string()).category(),
            "not_implemented"
        );
    }

    // ===== Jitter Tests =====

    #[test]
    fn test_jitter_new() {
        let jitter = Jitter::new(-10, 10);
        assert_eq!(jitter.min_seconds, -10);
        assert_eq!(jitter.max_seconds, 10);
    }

    #[test]
    fn test_jitter_positive() {
        let jitter = Jitter::positive(30);
        assert_eq!(jitter.min_seconds, 0);
        assert_eq!(jitter.max_seconds, 30);
    }

    #[test]
    fn test_jitter_symmetric() {
        let jitter = Jitter::symmetric(15);
        assert_eq!(jitter.min_seconds, -15);
        assert_eq!(jitter.max_seconds, 15);
    }

    #[test]
    fn test_jitter_apply_deterministic() {
        let jitter = Jitter::symmetric(60);
        let dt = Utc::now();
        let task_name = "test_task";

        // Same inputs should produce same output
        let result1 = jitter.apply(dt, task_name);
        let result2 = jitter.apply(dt, task_name);
        assert_eq!(result1, result2);
    }

    #[test]
    fn test_jitter_apply_different_tasks() {
        let jitter = Jitter::symmetric(60);
        let dt = Utc::now();

        // Different task names should produce different results
        let result1 = jitter.apply(dt, "task1");
        let result2 = jitter.apply(dt, "task2");
        assert_ne!(result1, result2);
    }

    #[test]
    fn test_jitter_apply_range() {
        let jitter = Jitter::new(10, 50);
        let dt = Utc::now();
        let task_name = "test_task";

        let result = jitter.apply(dt, task_name);
        let diff_seconds = (result - dt).num_seconds();

        // Result should be within range
        assert!(diff_seconds >= 10);
        assert!(diff_seconds <= 50);
    }

    #[test]
    fn test_scheduled_task_with_jitter() {
        let schedule = Schedule::interval(60);
        let jitter = Jitter::positive(10);
        let task = ScheduledTask::new("test_task".to_string(), schedule).with_jitter(jitter);

        assert!(task.jitter.is_some());
        let j = task.jitter.unwrap();
        assert_eq!(j.min_seconds, 0);
        assert_eq!(j.max_seconds, 10);
    }

    #[test]
    fn test_scheduled_task_next_run_time_with_jitter() {
        let schedule = Schedule::interval(60);
        let jitter = Jitter::positive(10);
        let mut task = ScheduledTask::new("test_task".to_string(), schedule).with_jitter(jitter);

        // Set last run to specific time
        let last_run = Utc::now() - Duration::seconds(70);
        task.last_run_at = Some(last_run);

        let next_run = task.next_run_time().unwrap();

        // Without jitter, next run would be last_run + 60 seconds
        // With jitter (0 to 10), it should be between last_run + 60 and last_run + 70
        let expected_base = last_run + Duration::seconds(60);
        let diff = (next_run - expected_base).num_seconds();

        assert!(diff >= 0);
        assert!(diff <= 10);
    }

    #[test]
    fn test_jitter_serialization() {
        let jitter = Jitter::symmetric(30);
        let json = serde_json::to_string(&jitter).unwrap();
        let deserialized: Jitter = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.min_seconds, -30);
        assert_eq!(deserialized.max_seconds, 30);
    }

    #[test]
    fn test_scheduled_task_with_jitter_serialization() {
        let schedule = Schedule::interval(60);
        let jitter = Jitter::positive(15);
        let task = ScheduledTask::new("test_task".to_string(), schedule).with_jitter(jitter);

        let json = serde_json::to_string(&task).unwrap();
        let deserialized: ScheduledTask = serde_json::from_str(&json).unwrap();

        assert!(deserialized.jitter.is_some());
        let j = deserialized.jitter.unwrap();
        assert_eq!(j.min_seconds, 0);
        assert_eq!(j.max_seconds, 15);
    }

    // ===== Catch-up Policy Tests =====

    #[test]
    fn test_catchup_policy_skip() {
        let policy = CatchupPolicy::Skip;
        let last_run = Utc::now() - Duration::seconds(200);
        let next_run = Utc::now() - Duration::seconds(50);
        let now = Utc::now();

        assert!(!policy.should_catchup(Some(last_run), next_run, now));
        assert_eq!(policy.catchup_count(Some(last_run), 60, now), 0);
    }

    #[test]
    fn test_catchup_policy_run_once() {
        let policy = CatchupPolicy::RunOnce;
        let last_run = Utc::now() - Duration::seconds(200);
        let next_run = Utc::now() - Duration::seconds(50);
        let now = Utc::now();

        assert!(policy.should_catchup(Some(last_run), next_run, now));
        assert_eq!(policy.catchup_count(Some(last_run), 60, now), 1);
    }

    #[test]
    fn test_catchup_policy_run_once_not_missed() {
        let policy = CatchupPolicy::RunOnce;
        let last_run = Utc::now() - Duration::seconds(30);
        let next_run = Utc::now() + Duration::seconds(30);
        let now = Utc::now();

        assert!(!policy.should_catchup(Some(last_run), next_run, now));
    }

    #[test]
    fn test_catchup_policy_run_multiple() {
        let policy = CatchupPolicy::RunMultiple { max_catchup: 5 };
        let last_run = Utc::now() - Duration::seconds(250); // Missed ~4 runs (250/60)
        let next_run = Utc::now() - Duration::seconds(50);
        let now = Utc::now();

        assert!(policy.should_catchup(Some(last_run), next_run, now));

        // Should be 3 catch-up runs (4 missed - 1 current)
        let count = policy.catchup_count(Some(last_run), 60, now);
        assert!(count >= 2 && count <= 4);
    }

    #[test]
    fn test_catchup_policy_run_multiple_max_limit() {
        let policy = CatchupPolicy::RunMultiple { max_catchup: 2 };
        let last_run = Utc::now() - Duration::seconds(600); // Missed ~10 runs
        let now = Utc::now();

        // Should be capped at max_catchup
        let count = policy.catchup_count(Some(last_run), 60, now);
        assert_eq!(count, 2);
    }

    #[test]
    fn test_catchup_policy_time_window_within() {
        let policy = CatchupPolicy::TimeWindow {
            window_seconds: 120,
        };
        let last_run = Utc::now() - Duration::seconds(150);
        let next_run = Utc::now() - Duration::seconds(50); // Missed by 50s (within window)
        let now = Utc::now();

        assert!(policy.should_catchup(Some(last_run), next_run, now));
        assert_eq!(policy.catchup_count(Some(last_run), 60, now), 1);
    }

    #[test]
    fn test_catchup_policy_time_window_outside() {
        let policy = CatchupPolicy::TimeWindow { window_seconds: 30 };
        let last_run = Utc::now() - Duration::seconds(200);
        let next_run = Utc::now() - Duration::seconds(100); // Missed by 100s (outside window)
        let now = Utc::now();

        assert!(!policy.should_catchup(Some(last_run), next_run, now));
        assert_eq!(policy.catchup_count(Some(last_run), 60, now), 0);
    }

    #[test]
    fn test_catchup_policy_never_run() {
        let policy = CatchupPolicy::RunOnce;
        let now = Utc::now();
        let next_run = now + Duration::seconds(60);

        // Tasks that never ran should not trigger catchup
        assert!(!policy.should_catchup(None, next_run, now));
        assert_eq!(policy.catchup_count(None, 60, now), 0);
    }

    #[test]
    fn test_catchup_policy_default() {
        let policy = CatchupPolicy::default();
        assert_eq!(policy, CatchupPolicy::Skip);
    }

    #[test]
    fn test_catchup_policy_serialization() {
        let policies = vec![
            CatchupPolicy::Skip,
            CatchupPolicy::RunOnce,
            CatchupPolicy::RunMultiple { max_catchup: 5 },
            CatchupPolicy::TimeWindow {
                window_seconds: 300,
            },
        ];

        for policy in policies {
            let json = serde_json::to_string(&policy).unwrap();
            let deserialized: CatchupPolicy = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, policy);
        }
    }

    #[test]
    fn test_scheduled_task_with_catchup_policy() {
        let schedule = Schedule::interval(60);
        let policy = CatchupPolicy::RunOnce;
        let task = ScheduledTask::new("test_task".to_string(), schedule)
            .with_catchup_policy(policy.clone());

        assert_eq!(task.catchup_policy, policy);
    }

    #[test]
    fn test_scheduled_task_catchup_policy_serialization() {
        let schedule = Schedule::interval(60);
        let policy = CatchupPolicy::RunMultiple { max_catchup: 3 };
        let task = ScheduledTask::new("test_task".to_string(), schedule)
            .with_catchup_policy(policy.clone());

        let json = serde_json::to_string(&task).unwrap();
        let deserialized: ScheduledTask = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.catchup_policy, policy);
    }

    // ===== Groups and Tags Tests =====

    #[test]
    fn test_scheduled_task_with_group() {
        let schedule = Schedule::interval(60);
        let task =
            ScheduledTask::new("test_task".to_string(), schedule).with_group("reports".to_string());

        assert_eq!(task.group, Some("reports".to_string()));
        assert!(task.is_in_group("reports"));
        assert!(!task.is_in_group("other"));
    }

    #[test]
    fn test_scheduled_task_with_tag() {
        let schedule = Schedule::interval(60);
        let task =
            ScheduledTask::new("test_task".to_string(), schedule).with_tag("daily".to_string());

        assert_eq!(task.tags.len(), 1);
        assert!(task.has_tag("daily"));
        assert!(!task.has_tag("weekly"));
    }

    #[test]
    fn test_scheduled_task_with_tags() {
        let schedule = Schedule::interval(60);
        let mut tags = HashSet::new();
        tags.insert("daily".to_string());
        tags.insert("reports".to_string());
        tags.insert("critical".to_string());

        let task = ScheduledTask::new("test_task".to_string(), schedule).with_tags(tags.clone());

        assert_eq!(task.tags.len(), 3);
        assert!(task.has_tag("daily"));
        assert!(task.has_tag("reports"));
        assert!(task.has_tag("critical"));
    }

    #[test]
    fn test_scheduled_task_add_remove_tag() {
        let schedule = Schedule::interval(60);
        let mut task = ScheduledTask::new("test_task".to_string(), schedule);

        task.add_tag("tag1".to_string());
        assert!(task.has_tag("tag1"));

        task.add_tag("tag2".to_string());
        assert!(task.has_tag("tag2"));
        assert_eq!(task.tags.len(), 2);

        let removed = task.remove_tag("tag1");
        assert!(removed);
        assert!(!task.has_tag("tag1"));
        assert_eq!(task.tags.len(), 1);

        let not_removed = task.remove_tag("nonexistent");
        assert!(!not_removed);
    }

    #[test]
    fn test_beat_scheduler_get_tasks_by_group() {
        let mut scheduler = BeatScheduler::new();

        let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60))
            .with_group("reports".to_string());
        let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(60))
            .with_group("reports".to_string());
        let task3 = ScheduledTask::new("task3".to_string(), Schedule::interval(60))
            .with_group("alerts".to_string());

        scheduler.add_task(task1).unwrap();
        scheduler.add_task(task2).unwrap();
        scheduler.add_task(task3).unwrap();

        let reports = scheduler.get_tasks_by_group("reports");
        assert_eq!(reports.len(), 2);

        let alerts = scheduler.get_tasks_by_group("alerts");
        assert_eq!(alerts.len(), 1);

        let nonexistent = scheduler.get_tasks_by_group("nonexistent");
        assert_eq!(nonexistent.len(), 0);
    }

    #[test]
    fn test_beat_scheduler_get_tasks_by_tag() {
        let mut scheduler = BeatScheduler::new();

        let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60))
            .with_tag("daily".to_string());
        let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(60))
            .with_tag("daily".to_string())
            .with_tag("critical".to_string());
        let task3 = ScheduledTask::new("task3".to_string(), Schedule::interval(60))
            .with_tag("weekly".to_string());

        scheduler.add_task(task1).unwrap();
        scheduler.add_task(task2).unwrap();
        scheduler.add_task(task3).unwrap();

        let daily = scheduler.get_tasks_by_tag("daily");
        assert_eq!(daily.len(), 2);

        let critical = scheduler.get_tasks_by_tag("critical");
        assert_eq!(critical.len(), 1);

        let weekly = scheduler.get_tasks_by_tag("weekly");
        assert_eq!(weekly.len(), 1);
    }

    #[test]
    fn test_beat_scheduler_get_tasks_by_tags() {
        let mut scheduler = BeatScheduler::new();

        let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60))
            .with_tag("daily".to_string());
        let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(60))
            .with_tag("weekly".to_string());
        let task3 = ScheduledTask::new("task3".to_string(), Schedule::interval(60))
            .with_tag("monthly".to_string());

        scheduler.add_task(task1).unwrap();
        scheduler.add_task(task2).unwrap();
        scheduler.add_task(task3).unwrap();

        // Get tasks with any of the specified tags
        let tasks = scheduler.get_tasks_by_tags(&["daily", "weekly"]);
        assert_eq!(tasks.len(), 2);
    }

    #[test]
    fn test_beat_scheduler_get_tasks_with_all_tags() {
        let mut scheduler = BeatScheduler::new();

        let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60))
            .with_tag("daily".to_string())
            .with_tag("critical".to_string());
        let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(60))
            .with_tag("daily".to_string());
        let task3 = ScheduledTask::new("task3".to_string(), Schedule::interval(60))
            .with_tag("critical".to_string());

        scheduler.add_task(task1).unwrap();
        scheduler.add_task(task2).unwrap();
        scheduler.add_task(task3).unwrap();

        // Get tasks with all of the specified tags
        let tasks = scheduler.get_tasks_with_all_tags(&["daily", "critical"]);
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].name, "task1");
    }

    #[test]
    fn test_beat_scheduler_get_all_groups() {
        let mut scheduler = BeatScheduler::new();

        let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60))
            .with_group("reports".to_string());
        let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(60))
            .with_group("alerts".to_string());
        let task3 = ScheduledTask::new("task3".to_string(), Schedule::interval(60))
            .with_group("reports".to_string());

        scheduler.add_task(task1).unwrap();
        scheduler.add_task(task2).unwrap();
        scheduler.add_task(task3).unwrap();

        let groups = scheduler.get_all_groups();
        assert_eq!(groups.len(), 2);
        assert!(groups.contains("reports"));
        assert!(groups.contains("alerts"));
    }

    #[test]
    fn test_beat_scheduler_get_all_tags() {
        let mut scheduler = BeatScheduler::new();

        let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60))
            .with_tag("daily".to_string())
            .with_tag("critical".to_string());
        let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(60))
            .with_tag("weekly".to_string());
        let task3 = ScheduledTask::new("task3".to_string(), Schedule::interval(60))
            .with_tag("daily".to_string());

        scheduler.add_task(task1).unwrap();
        scheduler.add_task(task2).unwrap();
        scheduler.add_task(task3).unwrap();

        let tags = scheduler.get_all_tags();
        assert_eq!(tags.len(), 3);
        assert!(tags.contains("daily"));
        assert!(tags.contains("weekly"));
        assert!(tags.contains("critical"));
    }

    #[test]
    fn test_beat_scheduler_enable_disable_group() {
        let mut scheduler = BeatScheduler::new();

        let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60))
            .with_group("reports".to_string());
        let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(60))
            .with_group("reports".to_string());
        let task3 = ScheduledTask::new("task3".to_string(), Schedule::interval(60))
            .with_group("alerts".to_string());

        scheduler.add_task(task1).unwrap();
        scheduler.add_task(task2).unwrap();
        scheduler.add_task(task3).unwrap();

        // Disable reports group
        let count = scheduler.disable_group("reports").unwrap();
        assert_eq!(count, 2);

        let reports = scheduler.get_tasks_by_group("reports");
        for task in reports {
            assert!(!task.enabled);
        }

        // Enable reports group
        let count = scheduler.enable_group("reports").unwrap();
        assert_eq!(count, 2);

        let reports = scheduler.get_tasks_by_group("reports");
        for task in reports {
            assert!(task.enabled);
        }
    }

    #[test]
    fn test_beat_scheduler_enable_disable_tag() {
        let mut scheduler = BeatScheduler::new();

        let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60))
            .with_tag("daily".to_string());
        let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(60))
            .with_tag("daily".to_string());
        let task3 = ScheduledTask::new("task3".to_string(), Schedule::interval(60))
            .with_tag("weekly".to_string());

        scheduler.add_task(task1).unwrap();
        scheduler.add_task(task2).unwrap();
        scheduler.add_task(task3).unwrap();

        // Disable daily tag
        let count = scheduler.disable_tag("daily").unwrap();
        assert_eq!(count, 2);

        let daily_tasks = scheduler.get_tasks_by_tag("daily");
        for task in daily_tasks {
            assert!(!task.enabled);
        }

        // Enable daily tag
        let count = scheduler.enable_tag("daily").unwrap();
        assert_eq!(count, 2);

        let daily_tasks = scheduler.get_tasks_by_tag("daily");
        for task in daily_tasks {
            assert!(task.enabled);
        }
    }

    #[test]
    fn test_groups_tags_serialization() {
        let schedule = Schedule::interval(60);
        let mut tags = HashSet::new();
        tags.insert("daily".to_string());
        tags.insert("critical".to_string());

        let task = ScheduledTask::new("test_task".to_string(), schedule)
            .with_group("reports".to_string())
            .with_tags(tags.clone());

        let json = serde_json::to_string(&task).unwrap();
        let deserialized: ScheduledTask = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.group, Some("reports".to_string()));
        assert_eq!(deserialized.tags.len(), 2);
        assert!(deserialized.has_tag("daily"));
        assert!(deserialized.has_tag("critical"));
    }

    // ===== Retry Policy Tests =====

    #[test]
    fn test_retry_policy_no_retry() {
        let policy = RetryPolicy::NoRetry;
        assert!(!policy.should_retry(0));
        assert!(!policy.should_retry(1));
        assert_eq!(policy.next_retry_delay(0), None);
    }

    #[test]
    fn test_retry_policy_fixed_delay() {
        let policy = RetryPolicy::FixedDelay {
            delay_seconds: 30,
            max_retries: 3,
        };

        assert!(policy.should_retry(0));
        assert_eq!(policy.next_retry_delay(0), Some(30));

        assert!(policy.should_retry(1));
        assert_eq!(policy.next_retry_delay(1), Some(30));

        assert!(policy.should_retry(2));
        assert_eq!(policy.next_retry_delay(2), Some(30));

        assert!(!policy.should_retry(3));
        assert_eq!(policy.next_retry_delay(3), None);
    }

    #[test]
    fn test_retry_policy_exponential_backoff() {
        let policy = RetryPolicy::ExponentialBackoff {
            initial_delay_seconds: 10,
            multiplier: 2.0,
            max_delay_seconds: 300,
            max_retries: 5,
        };

        // First retry: 10 * 2^0 = 10
        assert_eq!(policy.next_retry_delay(0), Some(10));

        // Second retry: 10 * 2^1 = 20
        assert_eq!(policy.next_retry_delay(1), Some(20));

        // Third retry: 10 * 2^2 = 40
        assert_eq!(policy.next_retry_delay(2), Some(40));

        // Fourth retry: 10 * 2^3 = 80
        assert_eq!(policy.next_retry_delay(3), Some(80));

        // Fifth retry: 10 * 2^4 = 160
        assert_eq!(policy.next_retry_delay(4), Some(160));

        // Sixth retry: exceeds max_retries
        assert_eq!(policy.next_retry_delay(5), None);
        assert!(!policy.should_retry(5));
    }

    #[test]
    fn test_retry_policy_exponential_backoff_max_delay() {
        let policy = RetryPolicy::ExponentialBackoff {
            initial_delay_seconds: 10,
            multiplier: 2.0,
            max_delay_seconds: 100,
            max_retries: 10,
        };

        // Seventh retry: 10 * 2^6 = 640, capped at 100
        assert_eq!(policy.next_retry_delay(6), Some(100));
    }

    #[test]
    fn test_retry_policy_default() {
        let policy = RetryPolicy::default();
        assert_eq!(policy, RetryPolicy::NoRetry);
    }

    #[test]
    fn test_retry_policy_serialization() {
        let policies = vec![
            RetryPolicy::NoRetry,
            RetryPolicy::FixedDelay {
                delay_seconds: 30,
                max_retries: 3,
            },
            RetryPolicy::ExponentialBackoff {
                initial_delay_seconds: 10,
                multiplier: 2.0,
                max_delay_seconds: 300,
                max_retries: 5,
            },
        ];

        for policy in policies {
            let json = serde_json::to_string(&policy).unwrap();
            let deserialized: RetryPolicy = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, policy);
        }
    }

    // ===== Task Retry Tests =====

    #[test]
    fn test_scheduled_task_with_retry_policy() {
        let schedule = Schedule::interval(60);
        let policy = RetryPolicy::FixedDelay {
            delay_seconds: 30,
            max_retries: 3,
        };
        let task =
            ScheduledTask::new("test_task".to_string(), schedule).with_retry_policy(policy.clone());

        assert_eq!(task.retry_policy, policy);
        assert_eq!(task.retry_count, 0);
        assert_eq!(task.total_failure_count, 0);
    }

    #[test]
    fn test_scheduled_task_mark_failure() {
        let schedule = Schedule::interval(60);
        let mut task = ScheduledTask::new("test_task".to_string(), schedule).with_retry_policy(
            RetryPolicy::FixedDelay {
                delay_seconds: 30,
                max_retries: 3,
            },
        );

        task.mark_failure();
        assert_eq!(task.retry_count, 1);
        assert_eq!(task.total_failure_count, 1);
        assert!(task.last_failure_at.is_some());
    }

    #[test]
    fn test_scheduled_task_mark_success() {
        let schedule = Schedule::interval(60);
        let mut task = ScheduledTask::new("test_task".to_string(), schedule).with_retry_policy(
            RetryPolicy::FixedDelay {
                delay_seconds: 30,
                max_retries: 3,
            },
        );

        task.mark_failure();
        task.mark_failure();
        assert_eq!(task.retry_count, 2);

        task.mark_success();
        assert_eq!(task.retry_count, 0); // Reset on success
    }

    #[test]
    fn test_scheduled_task_should_retry() {
        let schedule = Schedule::interval(60);
        let mut task = ScheduledTask::new("test_task".to_string(), schedule).with_retry_policy(
            RetryPolicy::FixedDelay {
                delay_seconds: 30,
                max_retries: 2,
            },
        );

        assert!(task.should_retry()); // Can retry (0 < 2)

        task.mark_failure();
        assert!(task.should_retry()); // Can retry (1 < 2)

        task.mark_failure();
        assert!(!task.should_retry()); // Cannot retry (2 >= 2)
    }

    #[test]
    fn test_scheduled_task_next_retry_time() {
        let schedule = Schedule::interval(60);
        let mut task = ScheduledTask::new("test_task".to_string(), schedule).with_retry_policy(
            RetryPolicy::FixedDelay {
                delay_seconds: 30,
                max_retries: 3,
            },
        );

        assert!(task.next_retry_time().is_none()); // No failure yet

        task.mark_failure();
        let next_retry = task.next_retry_time().unwrap();
        let expected = task.last_failure_at.unwrap() + Duration::seconds(30);

        assert_eq!(next_retry, expected);
    }

    #[test]
    fn test_scheduled_task_failure_rate() {
        let schedule = Schedule::interval(60);
        let mut task = ScheduledTask::new("test_task".to_string(), schedule);

        assert_eq!(task.failure_rate(), 0.0);

        task.total_run_count = 7;
        task.total_failure_count = 3;
        assert_eq!(task.failure_rate(), 0.3); // 3/10 = 0.3
    }

    #[test]
    fn test_beat_scheduler_mark_task_success() {
        let mut scheduler = BeatScheduler::new();
        let schedule = Schedule::interval(60);
        let task = ScheduledTask::new("test_task".to_string(), schedule).with_retry_policy(
            RetryPolicy::FixedDelay {
                delay_seconds: 30,
                max_retries: 3,
            },
        );

        scheduler.add_task(task).unwrap();

        // Simulate failure then success
        scheduler.mark_task_failure("test_task").unwrap();
        let task = scheduler.tasks.get("test_task").unwrap();
        assert_eq!(task.retry_count, 1);

        scheduler.mark_task_success("test_task").unwrap();
        let task = scheduler.tasks.get("test_task").unwrap();
        assert_eq!(task.retry_count, 0);
        assert_eq!(task.total_run_count, 1);
    }

    #[test]
    fn test_beat_scheduler_mark_task_failure() {
        let mut scheduler = BeatScheduler::new();
        let schedule = Schedule::interval(60);
        let task = ScheduledTask::new("test_task".to_string(), schedule).with_retry_policy(
            RetryPolicy::FixedDelay {
                delay_seconds: 30,
                max_retries: 3,
            },
        );

        scheduler.add_task(task).unwrap();

        scheduler.mark_task_failure("test_task").unwrap();
        scheduler.mark_task_failure("test_task").unwrap();

        let task = scheduler.tasks.get("test_task").unwrap();
        assert_eq!(task.retry_count, 2);
        assert_eq!(task.total_failure_count, 2);
        assert!(task.last_failure_at.is_some());
    }

    #[test]
    fn test_beat_scheduler_get_retry_tasks() {
        let mut scheduler = BeatScheduler::new();
        let schedule = Schedule::interval(60);

        // Task with retry policy
        let task1 = ScheduledTask::new("task1".to_string(), schedule.clone()).with_retry_policy(
            RetryPolicy::FixedDelay {
                delay_seconds: 1, // Short delay for testing
                max_retries: 3,
            },
        );

        // Task without retry policy
        let task2 = ScheduledTask::new("task2".to_string(), schedule);

        scheduler.add_task(task1).unwrap();
        scheduler.add_task(task2).unwrap();

        // Mark failures
        scheduler.mark_task_failure("task1").unwrap();
        scheduler.mark_task_failure("task2").unwrap();

        // Wait for retry time
        std::thread::sleep(std::time::Duration::from_secs(2));

        let retry_tasks = scheduler.get_retry_tasks();
        assert_eq!(retry_tasks.len(), 1);
        assert_eq!(retry_tasks[0].name, "task1");
    }

    #[test]
    fn test_retry_policy_serialization_in_task() {
        let schedule = Schedule::interval(60);
        let policy = RetryPolicy::ExponentialBackoff {
            initial_delay_seconds: 10,
            multiplier: 2.0,
            max_delay_seconds: 300,
            max_retries: 5,
        };
        let task =
            ScheduledTask::new("test_task".to_string(), schedule).with_retry_policy(policy.clone());

        let json = serde_json::to_string(&task).unwrap();
        let deserialized: ScheduledTask = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.retry_policy, policy);
    }

    // ===== Execution History Tests =====

    #[test]
    fn test_execution_result_success() {
        let result = ExecutionResult::Success;
        assert!(matches!(result, ExecutionResult::Success));
    }

    #[test]
    fn test_execution_result_failure() {
        let result = ExecutionResult::Failure {
            error: "Test error".to_string(),
        };
        assert!(matches!(result, ExecutionResult::Failure { .. }));
    }

    #[test]
    fn test_execution_result_timeout() {
        let result = ExecutionResult::Timeout;
        assert!(matches!(result, ExecutionResult::Timeout));
    }

    #[test]
    fn test_execution_record_new() {
        let started_at = Utc::now();
        let record = ExecutionRecord::new(started_at);

        assert_eq!(record.started_at, started_at);
        assert!(record.completed_at.is_none());
        assert!(matches!(record.result, ExecutionResult::Success));
        assert!(record.duration_ms.is_none());
    }

    #[test]
    fn test_execution_record_completed() {
        let started_at = Utc::now() - Duration::milliseconds(100);
        let record = ExecutionRecord::completed(started_at, ExecutionResult::Success);

        assert_eq!(record.started_at, started_at);
        assert!(record.completed_at.is_some());
        assert!(record.duration_ms.is_some());
        assert!(record.duration_ms.unwrap() >= 100);
    }

    #[test]
    fn test_execution_record_is_success() {
        let record = ExecutionRecord::completed(Utc::now(), ExecutionResult::Success);
        assert!(record.is_success());
        assert!(!record.is_failure());
        assert!(!record.is_timeout());
    }

    #[test]
    fn test_execution_record_is_failure() {
        let record = ExecutionRecord::completed(
            Utc::now(),
            ExecutionResult::Failure {
                error: "Test error".to_string(),
            },
        );
        assert!(record.is_failure());
        assert!(!record.is_success());
        assert!(!record.is_timeout());
    }

    #[test]
    fn test_execution_record_is_timeout() {
        let record = ExecutionRecord::completed(Utc::now(), ExecutionResult::Timeout);
        assert!(record.is_timeout());
        assert!(!record.is_success());
        assert!(!record.is_failure());
    }

    #[test]
    fn test_execution_record_is_completed() {
        let record = ExecutionRecord::completed(Utc::now(), ExecutionResult::Success);
        assert!(record.is_completed());

        let incomplete = ExecutionRecord::new(Utc::now());
        assert!(!incomplete.is_completed());
    }

    #[test]
    fn test_scheduled_task_add_execution_record() {
        let schedule = Schedule::interval(60);
        let mut task = ScheduledTask::new("test_task".to_string(), schedule);

        let record = ExecutionRecord::completed(Utc::now(), ExecutionResult::Success);
        task.add_execution_record(record);

        assert_eq!(task.execution_history.len(), 1);
        assert!(task.execution_history[0].is_success());
    }

    #[test]
    fn test_scheduled_task_with_max_history() {
        let schedule = Schedule::interval(60);
        let task = ScheduledTask::new("test_task".to_string(), schedule).with_max_history(3);

        assert_eq!(task.max_history_size, 3);
    }

    #[test]
    fn test_scheduled_task_history_trimming() {
        let schedule = Schedule::interval(60);
        let mut task = ScheduledTask::new("test_task".to_string(), schedule).with_max_history(3);

        // Add 5 records
        for _ in 0..5 {
            let record = ExecutionRecord::completed(Utc::now(), ExecutionResult::Success);
            task.add_execution_record(record);
            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        // Should only keep last 3
        assert_eq!(task.execution_history.len(), 3);
    }

    #[test]
    fn test_scheduled_task_get_last_executions() {
        let schedule = Schedule::interval(60);
        let mut task = ScheduledTask::new("test_task".to_string(), schedule);

        // Add 5 records
        for _ in 0..5 {
            let record = ExecutionRecord::completed(Utc::now(), ExecutionResult::Success);
            task.add_execution_record(record);
        }

        let last_3 = task.get_last_executions(3);
        assert_eq!(last_3.len(), 3);

        let last_10 = task.get_last_executions(10);
        assert_eq!(last_10.len(), 5); // Only 5 exist
    }

    #[test]
    fn test_scheduled_task_get_all_executions() {
        let schedule = Schedule::interval(60);
        let mut task = ScheduledTask::new("test_task".to_string(), schedule);

        // Add 3 records
        for _ in 0..3 {
            let record = ExecutionRecord::completed(Utc::now(), ExecutionResult::Success);
            task.add_execution_record(record);
        }

        let all = task.get_all_executions();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn test_scheduled_task_history_success_count() {
        let schedule = Schedule::interval(60);
        let mut task = ScheduledTask::new("test_task".to_string(), schedule);

        task.add_execution_record(ExecutionRecord::completed(
            Utc::now(),
            ExecutionResult::Success,
        ));
        task.add_execution_record(ExecutionRecord::completed(
            Utc::now(),
            ExecutionResult::Failure {
                error: "Error".to_string(),
            },
        ));
        task.add_execution_record(ExecutionRecord::completed(
            Utc::now(),
            ExecutionResult::Success,
        ));

        assert_eq!(task.history_success_count(), 2);
    }

    #[test]
    fn test_scheduled_task_history_failure_count() {
        let schedule = Schedule::interval(60);
        let mut task = ScheduledTask::new("test_task".to_string(), schedule);

        task.add_execution_record(ExecutionRecord::completed(
            Utc::now(),
            ExecutionResult::Success,
        ));
        task.add_execution_record(ExecutionRecord::completed(
            Utc::now(),
            ExecutionResult::Failure {
                error: "Error 1".to_string(),
            },
        ));
        task.add_execution_record(ExecutionRecord::completed(
            Utc::now(),
            ExecutionResult::Failure {
                error: "Error 2".to_string(),
            },
        ));

        assert_eq!(task.history_failure_count(), 2);
    }

    #[test]
    fn test_scheduled_task_history_timeout_count() {
        let schedule = Schedule::interval(60);
        let mut task = ScheduledTask::new("test_task".to_string(), schedule);

        task.add_execution_record(ExecutionRecord::completed(
            Utc::now(),
            ExecutionResult::Success,
        ));
        task.add_execution_record(ExecutionRecord::completed(
            Utc::now(),
            ExecutionResult::Timeout,
        ));
        task.add_execution_record(ExecutionRecord::completed(
            Utc::now(),
            ExecutionResult::Timeout,
        ));

        assert_eq!(task.history_timeout_count(), 2);
    }

    #[test]
    fn test_scheduled_task_average_duration_ms() {
        let schedule = Schedule::interval(60);
        let mut task = ScheduledTask::new("test_task".to_string(), schedule);

        // No history
        assert!(task.average_duration_ms().is_none());

        // Add records with known durations
        let started1 = Utc::now() - Duration::milliseconds(100);
        task.add_execution_record(ExecutionRecord::completed(
            started1,
            ExecutionResult::Success,
        ));

        std::thread::sleep(std::time::Duration::from_millis(10));

        let started2 = Utc::now() - Duration::milliseconds(200);
        task.add_execution_record(ExecutionRecord::completed(
            started2,
            ExecutionResult::Success,
        ));

        let avg = task.average_duration_ms().unwrap();
        assert!(avg >= 100); // At least 100ms average
    }

    #[test]
    fn test_scheduled_task_min_max_duration() {
        let schedule = Schedule::interval(60);
        let mut task = ScheduledTask::new("test_task".to_string(), schedule);

        let started1 = Utc::now() - Duration::milliseconds(100);
        task.add_execution_record(ExecutionRecord::completed(
            started1,
            ExecutionResult::Success,
        ));

        std::thread::sleep(std::time::Duration::from_millis(10));

        let started2 = Utc::now() - Duration::milliseconds(200);
        task.add_execution_record(ExecutionRecord::completed(
            started2,
            ExecutionResult::Success,
        ));

        let min = task.min_duration_ms().unwrap();
        let max = task.max_duration_ms().unwrap();

        assert!(min >= 100);
        assert!(max >= 200);
        assert!(max >= min);
    }

    #[test]
    fn test_scheduled_task_history_success_rate() {
        let schedule = Schedule::interval(60);
        let mut task = ScheduledTask::new("test_task".to_string(), schedule);

        // No history
        assert_eq!(task.history_success_rate(), 0.0);

        // Add 3 success, 1 failure
        task.add_execution_record(ExecutionRecord::completed(
            Utc::now(),
            ExecutionResult::Success,
        ));
        task.add_execution_record(ExecutionRecord::completed(
            Utc::now(),
            ExecutionResult::Success,
        ));
        task.add_execution_record(ExecutionRecord::completed(
            Utc::now(),
            ExecutionResult::Failure {
                error: "Error".to_string(),
            },
        ));
        task.add_execution_record(ExecutionRecord::completed(
            Utc::now(),
            ExecutionResult::Success,
        ));

        assert_eq!(task.history_success_rate(), 0.75); // 3/4 = 0.75
    }

    #[test]
    fn test_scheduled_task_clear_history() {
        let schedule = Schedule::interval(60);
        let mut task = ScheduledTask::new("test_task".to_string(), schedule);

        // Add records
        for _ in 0..3 {
            task.add_execution_record(ExecutionRecord::completed(
                Utc::now(),
                ExecutionResult::Success,
            ));
        }

        assert_eq!(task.execution_history.len(), 3);

        task.clear_history();
        assert_eq!(task.execution_history.len(), 0);
    }

    #[test]
    fn test_beat_scheduler_mark_task_success_with_history() {
        let mut scheduler = BeatScheduler::new();
        let schedule = Schedule::interval(60);
        let task = ScheduledTask::new("test_task".to_string(), schedule);

        scheduler.add_task(task).unwrap();
        scheduler.mark_task_success("test_task").unwrap();

        let task = scheduler.tasks.get("test_task").unwrap();
        assert_eq!(task.execution_history.len(), 1);
        assert!(task.execution_history[0].is_success());
    }

    #[test]
    fn test_beat_scheduler_mark_task_failure_with_history() {
        let mut scheduler = BeatScheduler::new();
        let schedule = Schedule::interval(60);
        let task = ScheduledTask::new("test_task".to_string(), schedule);

        scheduler.add_task(task).unwrap();
        scheduler
            .mark_task_failure_with_error("test_task", "Test error".to_string())
            .unwrap();

        let task = scheduler.tasks.get("test_task").unwrap();
        assert_eq!(task.execution_history.len(), 1);
        assert!(task.execution_history[0].is_failure());
    }

    #[test]
    fn test_beat_scheduler_mark_task_timeout_with_history() {
        let mut scheduler = BeatScheduler::new();
        let schedule = Schedule::interval(60);
        let task = ScheduledTask::new("test_task".to_string(), schedule);

        scheduler.add_task(task).unwrap();

        let started_at = Utc::now() - Duration::seconds(5);
        scheduler
            .mark_task_timeout("test_task", started_at)
            .unwrap();

        let task = scheduler.tasks.get("test_task").unwrap();
        assert_eq!(task.execution_history.len(), 1);
        assert!(task.execution_history[0].is_timeout());
    }

    #[test]
    fn test_beat_scheduler_mark_task_success_with_start_time() {
        let mut scheduler = BeatScheduler::new();
        let schedule = Schedule::interval(60);
        let task = ScheduledTask::new("test_task".to_string(), schedule);

        scheduler.add_task(task).unwrap();

        let started_at = Utc::now() - Duration::milliseconds(150);
        scheduler
            .mark_task_success_with_start("test_task", started_at)
            .unwrap();

        let task = scheduler.tasks.get("test_task").unwrap();
        assert_eq!(task.execution_history.len(), 1);
        assert!(task.execution_history[0].is_success());
        assert!(task.execution_history[0].duration_ms.unwrap() >= 150);
    }

    #[test]
    fn test_execution_history_serialization() {
        let schedule = Schedule::interval(60);
        let mut task = ScheduledTask::new("test_task".to_string(), schedule);

        task.add_execution_record(ExecutionRecord::completed(
            Utc::now(),
            ExecutionResult::Success,
        ));
        task.add_execution_record(ExecutionRecord::completed(
            Utc::now(),
            ExecutionResult::Failure {
                error: "Test error".to_string(),
            },
        ));

        let json = serde_json::to_string(&task).unwrap();
        let deserialized: ScheduledTask = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.execution_history.len(), 2);
        assert!(deserialized.execution_history[0].is_success());
        assert!(deserialized.execution_history[1].is_failure());
    }

    #[test]
    fn test_execution_history_persistence() {
        use std::fs;

        let temp_file = "/tmp/test_beat_scheduler_history_persist.json";
        let _ = fs::remove_file(temp_file);

        // Create scheduler and add task with history
        {
            let mut scheduler = BeatScheduler::with_persistence(temp_file);
            let schedule = Schedule::interval(60);
            let task = ScheduledTask::new("test_task".to_string(), schedule);

            scheduler.add_task(task).unwrap();
            scheduler.mark_task_success("test_task").unwrap();
            scheduler
                .mark_task_failure_with_error("test_task", "Test error".to_string())
                .unwrap();
            scheduler.mark_task_success("test_task").unwrap();
        }

        // Load and verify history
        {
            let scheduler = BeatScheduler::load_from_file(temp_file).unwrap();
            let task = scheduler.tasks.get("test_task").unwrap();

            assert_eq!(task.execution_history.len(), 3);
            assert!(task.execution_history[0].is_success());
            assert!(task.execution_history[1].is_failure());
            assert!(task.execution_history[2].is_success());
        }

        // Clean up
        let _ = fs::remove_file(temp_file);
    }

    // ===== Health Check Tests =====

    #[test]
    fn test_schedule_health_healthy() {
        let health = ScheduleHealth::Healthy;
        assert!(health.is_healthy());
        assert!(!health.has_warnings());
        assert!(!health.is_unhealthy());
        assert_eq!(health.get_issues().len(), 0);
    }

    #[test]
    fn test_schedule_health_warning() {
        let health = ScheduleHealth::Warning {
            issues: vec!["Warning 1".to_string(), "Warning 2".to_string()],
        };
        assert!(!health.is_healthy());
        assert!(health.has_warnings());
        assert!(!health.is_unhealthy());
        assert_eq!(health.get_issues().len(), 2);
    }

    #[test]
    fn test_schedule_health_unhealthy() {
        let health = ScheduleHealth::Unhealthy {
            issues: vec!["Error 1".to_string()],
        };
        assert!(!health.is_healthy());
        assert!(!health.has_warnings());
        assert!(health.is_unhealthy());
        assert_eq!(health.get_issues().len(), 1);
    }

    #[test]
    fn test_health_check_result_creation() {
        let result = HealthCheckResult::healthy("test_task".to_string());
        assert_eq!(result.task_name, "test_task");
        assert!(result.health.is_healthy());
        assert!(result.next_run.is_none());
        assert!(result.time_since_last_run.is_none());
    }

    #[test]
    fn test_health_check_result_with_details() {
        let next_run = Utc::now() + Duration::seconds(60);
        let duration = Duration::seconds(30);

        let result = HealthCheckResult::healthy("test_task".to_string())
            .with_next_run(next_run)
            .with_time_since_last_run(duration);

        assert_eq!(result.next_run, Some(next_run));
        assert_eq!(result.time_since_last_run, Some(duration));
    }

    #[test]
    fn test_scheduled_task_check_health_healthy() {
        let schedule = Schedule::interval(60);
        let task = ScheduledTask::new("test_task".to_string(), schedule);

        let result = task.check_health();
        assert!(result.health.is_healthy());
        assert!(result.next_run.is_some());
    }

    #[test]
    fn test_scheduled_task_check_health_disabled() {
        let schedule = Schedule::interval(60);
        let task = ScheduledTask::new("test_task".to_string(), schedule).disabled();

        let result = task.check_health();
        assert!(result.health.has_warnings());
        let issues = result.health.get_issues();
        assert!(issues.iter().any(|i| i.contains("disabled")));
    }

    #[test]
    fn test_scheduled_task_check_health_high_failure_rate() {
        let schedule = Schedule::interval(60);
        let mut task = ScheduledTask::new("test_task".to_string(), schedule);

        // Simulate high failure rate
        task.total_run_count = 5;
        task.total_failure_count = 10; // 66% failure rate

        let result = task.check_health();
        assert!(result.health.has_warnings());
        let issues = result.health.get_issues();
        assert!(issues.iter().any(|i| i.contains("High failure rate")));
    }

    #[test]
    fn test_scheduled_task_check_health_consecutive_failures() {
        let schedule = Schedule::interval(60);
        let mut task = ScheduledTask::new("test_task".to_string(), schedule);

        // Add 3 consecutive failures
        for _ in 0..3 {
            task.add_execution_record(ExecutionRecord::completed(
                Utc::now(),
                ExecutionResult::Failure {
                    error: "Test error".to_string(),
                },
            ));
        }

        let result = task.check_health();
        assert!(result.health.has_warnings() || result.health.is_unhealthy());
        let issues = result.health.get_issues();
        assert!(issues
            .iter()
            .any(|i| i.contains("Last 3 executions failed")));
    }

    #[test]
    fn test_scheduled_task_is_stuck_not_stuck() {
        let schedule = Schedule::interval(60);
        let mut task = ScheduledTask::new("test_task".to_string(), schedule);

        // Recently ran
        task.last_run_at = Some(Utc::now() - Duration::seconds(30));

        assert!(task.is_stuck().is_none());
    }

    #[test]
    fn test_scheduled_task_is_stuck() {
        let schedule = Schedule::interval(60);
        let mut task = ScheduledTask::new("test_task".to_string(), schedule);

        // Hasn't run in a very long time (100x the interval)
        task.last_run_at = Some(Utc::now() - Duration::seconds(6000));

        let stuck_duration = task.is_stuck();
        assert!(stuck_duration.is_some());
        assert!(stuck_duration.unwrap().num_seconds() >= 6000);
    }

    #[test]
    fn test_scheduled_task_is_stuck_disabled() {
        let schedule = Schedule::interval(60);
        let mut task = ScheduledTask::new("test_task".to_string(), schedule).disabled();

        // Disabled tasks are never considered stuck
        task.last_run_at = Some(Utc::now() - Duration::seconds(10000));

        assert!(task.is_stuck().is_none());
    }

    #[test]
    fn test_scheduled_task_validate_schedule_valid() {
        let schedule = Schedule::interval(60);
        let task = ScheduledTask::new("test_task".to_string(), schedule);

        assert!(task.validate_schedule().is_ok());
    }

    #[test]
    #[cfg(feature = "cron")]
    fn test_scheduled_task_validate_schedule_invalid_cron() {
        let schedule = Schedule::crontab("invalid", "0", "*", "*", "*");
        let task = ScheduledTask::new("test_task".to_string(), schedule);

        assert!(task.validate_schedule().is_err());
    }

    #[test]
    fn test_beat_scheduler_check_all_tasks_health() {
        let mut scheduler = BeatScheduler::new();

        let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60));
        let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(120));

        scheduler.add_task(task1).unwrap();
        scheduler.add_task(task2).unwrap();

        let results = scheduler.check_all_tasks_health();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_beat_scheduler_get_unhealthy_tasks() {
        let mut scheduler = BeatScheduler::new();

        // Add healthy task
        let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60));

        // Add unhealthy task (disabled)
        let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(60)).disabled();

        scheduler.add_task(task1).unwrap();
        scheduler.add_task(task2).unwrap();

        let unhealthy = scheduler.get_unhealthy_tasks();
        assert_eq!(unhealthy.len(), 1);
        assert_eq!(unhealthy[0].task_name, "task2");
    }

    #[test]
    fn test_beat_scheduler_get_tasks_with_warnings() {
        let mut scheduler = BeatScheduler::new();

        // Add healthy task
        let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60));

        // Add task with warning (disabled)
        let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(60)).disabled();

        scheduler.add_task(task1).unwrap();
        scheduler.add_task(task2).unwrap();

        let warnings = scheduler.get_tasks_with_warnings();
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].task_name, "task2");
    }

    #[test]
    fn test_beat_scheduler_get_stuck_tasks() {
        let mut scheduler = BeatScheduler::new();

        // Add recently run task
        let mut task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60));
        task1.last_run_at = Some(Utc::now() - Duration::seconds(30));

        // Add stuck task
        let mut task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(60));
        task2.last_run_at = Some(Utc::now() - Duration::seconds(10000));

        scheduler.add_task(task1).unwrap();
        scheduler.add_task(task2).unwrap();

        let stuck = scheduler.get_stuck_tasks();
        assert_eq!(stuck.len(), 1);
        assert_eq!(stuck[0].name, "task2");
    }

    #[test]
    fn test_beat_scheduler_validate_all_schedules() {
        let mut scheduler = BeatScheduler::new();

        let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60));
        let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(120));

        scheduler.add_task(task1).unwrap();
        scheduler.add_task(task2).unwrap();

        let results = scheduler.validate_all_schedules();
        assert_eq!(results.len(), 2);

        for (_, result) in results {
            assert!(result.is_ok());
        }
    }

    #[test]
    fn test_schedule_health_serialization() {
        let health_variants = vec![
            ScheduleHealth::Healthy,
            ScheduleHealth::Warning {
                issues: vec!["Warning".to_string()],
            },
            ScheduleHealth::Unhealthy {
                issues: vec!["Error".to_string()],
            },
        ];

        for health in health_variants {
            let json = serde_json::to_string(&health).unwrap();
            let deserialized: ScheduleHealth = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, health);
        }
    }

    // ===== Scheduler Metrics Tests =====

    #[test]
    fn test_scheduler_metrics_empty_scheduler() {
        let scheduler = BeatScheduler::new();
        let metrics = scheduler.get_metrics();

        assert_eq!(metrics.total_tasks, 0);
        assert_eq!(metrics.enabled_tasks, 0);
        assert_eq!(metrics.disabled_tasks, 0);
        assert_eq!(metrics.total_executions, 0);
        assert_eq!(metrics.overall_success_rate, 0.0);
    }

    #[test]
    fn test_scheduler_metrics_basic() {
        let mut scheduler = BeatScheduler::new();

        let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60));
        let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(60)).disabled();

        scheduler.add_task(task1).unwrap();
        scheduler.add_task(task2).unwrap();

        let metrics = scheduler.get_metrics();
        assert_eq!(metrics.total_tasks, 2);
        assert_eq!(metrics.enabled_tasks, 1);
        assert_eq!(metrics.disabled_tasks, 1);
    }

    #[test]
    fn test_scheduler_metrics_with_executions() {
        let mut scheduler = BeatScheduler::new();

        let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60));
        scheduler.add_task(task1).unwrap();

        // Add some execution history
        scheduler.mark_task_success("task1").unwrap();
        scheduler.mark_task_success("task1").unwrap();
        scheduler
            .mark_task_failure_with_error("task1", "Error".to_string())
            .unwrap();

        let metrics = scheduler.get_metrics();
        assert_eq!(metrics.tasks_with_executions, 1);
        assert_eq!(metrics.total_successes, 2);
        assert_eq!(metrics.total_failures, 1);
        assert_eq!(metrics.total_executions, 3);
        assert_eq!(metrics.overall_success_rate, 2.0 / 3.0);
    }

    #[test]
    fn test_scheduler_metrics_retry_state() {
        let mut scheduler = BeatScheduler::new();

        let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60))
            .with_retry_policy(RetryPolicy::FixedDelay {
                delay_seconds: 30,
                max_retries: 3,
            });

        scheduler.add_task(task1).unwrap();
        scheduler
            .mark_task_failure_with_error("task1", "Error".to_string())
            .unwrap();

        let metrics = scheduler.get_metrics();
        assert_eq!(metrics.tasks_in_retry, 1);
    }

    #[test]
    fn test_scheduler_metrics_health_status() {
        let mut scheduler = BeatScheduler::new();

        // Add healthy task
        let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60));

        // Add disabled task (warning)
        let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(60)).disabled();

        scheduler.add_task(task1).unwrap();
        scheduler.add_task(task2).unwrap();

        let metrics = scheduler.get_metrics();
        assert_eq!(metrics.tasks_with_warnings, 1);
    }

    #[test]
    fn test_scheduler_metrics_stuck_tasks() {
        let mut scheduler = BeatScheduler::new();

        // Add stuck task
        let mut task = ScheduledTask::new("task1".to_string(), Schedule::interval(60));
        task.last_run_at = Some(Utc::now() - Duration::seconds(10000));
        scheduler.add_task(task).unwrap();

        let metrics = scheduler.get_metrics();
        assert_eq!(metrics.stuck_tasks, 1);
    }

    #[test]
    fn test_scheduler_metrics_serialization() {
        let scheduler = BeatScheduler::new();
        let metrics = scheduler.get_metrics();

        let json = serde_json::to_string(&metrics).unwrap();
        let deserialized: SchedulerMetrics = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.total_tasks, metrics.total_tasks);
        assert_eq!(deserialized.enabled_tasks, metrics.enabled_tasks);
    }

    // ===== Task Statistics Tests =====

    #[test]
    fn test_task_statistics_basic() {
        let schedule = Schedule::interval(60);
        let task = ScheduledTask::new("test_task".to_string(), schedule);

        let stats = TaskStatistics::from_task(&task);
        assert_eq!(stats.name, "test_task");
        assert_eq!(stats.success_count, 0);
        assert_eq!(stats.failure_count, 0);
        assert_eq!(stats.success_rate, 0.0);
    }

    #[test]
    fn test_task_statistics_with_history() {
        let schedule = Schedule::interval(60);
        let mut task = ScheduledTask::new("test_task".to_string(), schedule);

        // Add execution history
        task.add_execution_record(ExecutionRecord::completed(
            Utc::now() - Duration::milliseconds(100),
            ExecutionResult::Success,
        ));
        task.add_execution_record(ExecutionRecord::completed(
            Utc::now() - Duration::milliseconds(200),
            ExecutionResult::Success,
        ));
        task.add_execution_record(ExecutionRecord::completed(
            Utc::now(),
            ExecutionResult::Failure {
                error: "Error".to_string(),
            },
        ));

        let stats = TaskStatistics::from_task(&task);
        assert_eq!(stats.success_count, 2);
        assert_eq!(stats.failure_count, 1);
        assert_eq!(stats.success_rate, 2.0 / 3.0);
        assert!(stats.average_duration_ms.is_some());
    }

    #[test]
    fn test_beat_scheduler_get_all_task_statistics() {
        let mut scheduler = BeatScheduler::new();

        let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60));
        let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(120));

        scheduler.add_task(task1).unwrap();
        scheduler.add_task(task2).unwrap();

        let stats = scheduler.get_all_task_statistics();
        assert_eq!(stats.len(), 2);
    }

    #[test]
    fn test_beat_scheduler_get_task_statistics() {
        let mut scheduler = BeatScheduler::new();

        let task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));
        scheduler.add_task(task).unwrap();

        let stats = scheduler.get_task_statistics("test_task");
        assert!(stats.is_some());
        assert_eq!(stats.unwrap().name, "test_task");

        let missing = scheduler.get_task_statistics("nonexistent");
        assert!(missing.is_none());
    }

    #[test]
    fn test_beat_scheduler_get_group_statistics() {
        let mut scheduler = BeatScheduler::new();

        let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60))
            .with_group("reports".to_string());
        let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(60))
            .with_group("reports".to_string());
        let task3 = ScheduledTask::new("task3".to_string(), Schedule::interval(60))
            .with_group("alerts".to_string());

        scheduler.add_task(task1).unwrap();
        scheduler.add_task(task2).unwrap();
        scheduler.add_task(task3).unwrap();

        let stats = scheduler.get_group_statistics("reports");
        assert_eq!(stats.len(), 2);
    }

    #[test]
    fn test_beat_scheduler_get_tag_statistics() {
        let mut scheduler = BeatScheduler::new();

        let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60))
            .with_tag("daily".to_string());
        let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(60))
            .with_tag("daily".to_string());
        let task3 = ScheduledTask::new("task3".to_string(), Schedule::interval(60))
            .with_tag("weekly".to_string());

        scheduler.add_task(task1).unwrap();
        scheduler.add_task(task2).unwrap();
        scheduler.add_task(task3).unwrap();

        let stats = scheduler.get_tag_statistics("daily");
        assert_eq!(stats.len(), 2);
    }

    #[test]
    fn test_task_statistics_retry_count() {
        let schedule = Schedule::interval(60);
        let mut task = ScheduledTask::new("test_task".to_string(), schedule).with_retry_policy(
            RetryPolicy::FixedDelay {
                delay_seconds: 30,
                max_retries: 3,
            },
        );

        task.mark_failure();
        task.mark_failure();

        let stats = TaskStatistics::from_task(&task);
        assert_eq!(stats.retry_count, 2);
    }

    #[test]
    fn test_task_statistics_stuck_detection() {
        let schedule = Schedule::interval(60);
        let mut task = ScheduledTask::new("test_task".to_string(), schedule);

        // Not stuck initially
        let stats = TaskStatistics::from_task(&task);
        assert!(!stats.is_stuck);

        // Make it stuck
        task.last_run_at = Some(Utc::now() - Duration::seconds(10000));
        let stats = TaskStatistics::from_task(&task);
        assert!(stats.is_stuck);
    }

    // ===== Schedule Versioning Tests =====

    #[test]
    fn test_version_initial_creation() {
        let task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));

        assert_eq!(task.current_version, 1);
        assert_eq!(task.version_history.len(), 1);

        let initial_version = &task.version_history[0];
        assert_eq!(initial_version.version, 1);
        assert!(initial_version.schedule.is_interval());
        assert_eq!(
            initial_version.change_reason,
            Some("Initial creation".to_string())
        );
    }

    #[test]
    fn test_version_update_schedule() {
        let mut task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));

        // Update to a different interval
        task.update_schedule(
            Schedule::interval(120),
            Some("Changed interval".to_string()),
        );

        assert_eq!(task.current_version, 2);
        assert_eq!(task.version_history.len(), 2);

        // Check new schedule is active
        if let Schedule::Interval { every } = task.schedule {
            assert_eq!(every, 120);
        } else {
            panic!("Expected interval schedule");
        }

        // Check version history
        let v2 = &task.version_history[1];
        assert_eq!(v2.version, 2);
        assert_eq!(v2.change_reason, Some("Changed interval".to_string()));
    }

    #[test]
    fn test_version_update_config() {
        let mut task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));

        // Update configuration
        task.update_config(
            Some(false), // disable
            Some(Some(Jitter::positive(30))),
            Some(CatchupPolicy::RunOnce),
            Some("Changed config".to_string()),
        );

        assert_eq!(task.current_version, 2);
        assert!(!task.enabled);
        assert!(task.jitter.is_some());
        assert!(matches!(task.catchup_policy, CatchupPolicy::RunOnce));
    }

    #[test]
    fn test_version_rollback() {
        let mut task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));

        // Make several changes
        task.update_schedule(Schedule::interval(120), Some("Change 1".to_string()));
        task.update_schedule(Schedule::interval(180), Some("Change 2".to_string()));

        assert_eq!(task.current_version, 3);

        // Rollback to version 1
        task.rollback_to_version(1).unwrap();

        // Check schedule is back to original
        if let Schedule::Interval { every } = task.schedule {
            assert_eq!(every, 60);
        } else {
            panic!("Expected interval schedule");
        }

        // Current version should be incremented
        assert_eq!(task.current_version, 4);

        // Should have rollback entry in history
        let rollback_version = &task.version_history[3];
        assert!(rollback_version
            .change_reason
            .as_ref()
            .unwrap()
            .contains("Rolled back to version 1"));
    }

    #[test]
    fn test_version_rollback_invalid() {
        let mut task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));

        // Try to rollback to non-existent version
        let result = task.rollback_to_version(999);
        assert!(result.is_err());

        if let Err(ScheduleError::Invalid(msg)) = result {
            assert_eq!(msg, "Version 999 not found");
        }
    }

    #[test]
    fn test_version_get_history() {
        let mut task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));

        task.update_schedule(Schedule::interval(120), Some("Change 1".to_string()));
        task.update_schedule(Schedule::interval(180), Some("Change 2".to_string()));

        let history = task.get_version_history();
        assert_eq!(history.len(), 3);

        // Verify chronological order
        assert_eq!(history[0].version, 1);
        assert_eq!(history[1].version, 2);
        assert_eq!(history[2].version, 3);
    }

    #[test]
    fn test_version_get_specific() {
        let mut task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));

        task.update_schedule(Schedule::interval(120), Some("Change 1".to_string()));

        let v1 = task.get_version(1).unwrap();
        assert_eq!(v1.version, 1);

        let v2 = task.get_version(2).unwrap();
        assert_eq!(v2.version, 2);

        assert!(task.get_version(999).is_none());
    }

    #[test]
    fn test_version_get_previous() {
        let mut task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));

        // No previous version initially
        assert!(task.get_previous_version().is_none());

        // Add a version
        task.update_schedule(Schedule::interval(120), Some("Change 1".to_string()));

        // Now there's a previous version
        let prev = task.get_previous_version().unwrap();
        assert_eq!(prev.version, 1);
    }

    #[test]
    fn test_version_serialization() {
        let mut task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));

        task.update_schedule(Schedule::interval(120), Some("Change 1".to_string()));

        // Serialize
        let json = serde_json::to_string(&task).unwrap();

        // Deserialize
        let deserialized: ScheduledTask = serde_json::from_str(&json).unwrap();

        // Verify version history is preserved
        assert_eq!(deserialized.current_version, task.current_version);
        assert_eq!(
            deserialized.version_history.len(),
            task.version_history.len()
        );
        assert_eq!(deserialized.version_history[0].version, 1);
        assert_eq!(deserialized.version_history[1].version, 2);
    }

    #[test]
    fn test_version_multiple_rollbacks() {
        let mut task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));

        task.update_schedule(Schedule::interval(120), Some("Change 1".to_string()));
        task.update_schedule(Schedule::interval(180), Some("Change 2".to_string()));

        // Rollback to v1
        task.rollback_to_version(1).unwrap();
        assert_eq!(task.current_version, 4);

        // Rollback to v2
        task.rollback_to_version(2).unwrap();
        assert_eq!(task.current_version, 5);

        // Should have all versions in history
        assert_eq!(task.version_history.len(), 5);
    }

    // ===== Task Dependency Tests =====

    #[test]
    fn test_dependency_basic() {
        let mut task = ScheduledTask::new("task_b".to_string(), Schedule::interval(60));

        assert!(!task.has_dependencies());

        task.add_dependency("task_a".to_string());

        assert!(task.has_dependencies());
        assert!(task.depends_on("task_a"));
        assert!(!task.depends_on("task_c"));
    }

    #[test]
    fn test_dependency_add_remove() {
        let mut task = ScheduledTask::new("task_b".to_string(), Schedule::interval(60));

        task.add_dependency("task_a".to_string());
        assert_eq!(task.dependencies.len(), 1);

        task.add_dependency("task_c".to_string());
        assert_eq!(task.dependencies.len(), 2);

        assert!(task.remove_dependency("task_a"));
        assert_eq!(task.dependencies.len(), 1);
        assert!(!task.depends_on("task_a"));

        assert!(!task.remove_dependency("nonexistent"));
    }

    #[test]
    fn test_dependency_clear() {
        let mut task = ScheduledTask::new("task_b".to_string(), Schedule::interval(60));

        task.add_dependency("task_a".to_string());
        task.add_dependency("task_c".to_string());
        assert_eq!(task.dependencies.len(), 2);

        task.clear_dependencies();
        assert_eq!(task.dependencies.len(), 0);
        assert!(!task.has_dependencies());
    }

    #[test]
    fn test_dependency_with_dependencies() {
        let mut deps = HashSet::new();
        deps.insert("task_a".to_string());
        deps.insert("task_b".to_string());

        let task = ScheduledTask::new("task_c".to_string(), Schedule::interval(60))
            .with_dependencies(deps);

        assert_eq!(task.dependencies.len(), 2);
        assert!(task.depends_on("task_a"));
        assert!(task.depends_on("task_b"));
    }

    #[test]
    fn test_dependency_status_satisfied() {
        let mut task = ScheduledTask::new("task_b".to_string(), Schedule::interval(60));
        task.add_dependency("task_a".to_string());

        let mut completed = HashSet::new();
        completed.insert("task_a".to_string());

        let status = task.check_dependencies(&completed);
        assert!(status.is_satisfied());
    }

    #[test]
    fn test_dependency_status_waiting() {
        let mut task = ScheduledTask::new("task_b".to_string(), Schedule::interval(60));
        task.add_dependency("task_a".to_string());

        let completed = HashSet::new();

        let status = task.check_dependencies(&completed);
        assert!(!status.is_satisfied());

        if let DependencyStatus::Waiting { pending } = status {
            assert_eq!(pending.len(), 1);
            assert_eq!(pending[0], "task_a");
        } else {
            panic!("Expected Waiting status");
        }
    }

    #[test]
    fn test_dependency_status_with_failures() {
        let mut task = ScheduledTask::new("task_c".to_string(), Schedule::interval(60));
        task.add_dependency("task_a".to_string());
        task.add_dependency("task_b".to_string());

        let mut completed = HashSet::new();
        completed.insert("task_a".to_string());

        let mut failed = HashSet::new();
        failed.insert("task_b".to_string());

        let status = task.check_dependencies_with_failures(&completed, &failed);
        assert!(status.has_failures());

        if let DependencyStatus::Failed {
            failed: failed_tasks,
        } = status
        {
            assert_eq!(failed_tasks.len(), 1);
            assert_eq!(failed_tasks[0], "task_b");
        } else {
            panic!("Expected Failed status");
        }
    }

    #[test]
    fn test_circular_dependency_simple() {
        let mut scheduler = BeatScheduler::new();

        let mut task_a = ScheduledTask::new("task_a".to_string(), Schedule::interval(60));
        task_a.add_dependency("task_b".to_string());

        let mut task_b = ScheduledTask::new("task_b".to_string(), Schedule::interval(60));
        task_b.add_dependency("task_a".to_string());

        scheduler.add_task(task_a).unwrap();
        scheduler.add_task(task_b).unwrap();

        assert!(scheduler.has_circular_dependency("task_a"));
        assert!(scheduler.has_circular_dependency("task_b"));
    }

    #[test]
    fn test_circular_dependency_complex() {
        let mut scheduler = BeatScheduler::new();

        let mut task_a = ScheduledTask::new("task_a".to_string(), Schedule::interval(60));
        task_a.add_dependency("task_b".to_string());

        let mut task_b = ScheduledTask::new("task_b".to_string(), Schedule::interval(60));
        task_b.add_dependency("task_c".to_string());

        let mut task_c = ScheduledTask::new("task_c".to_string(), Schedule::interval(60));
        task_c.add_dependency("task_a".to_string());

        scheduler.add_task(task_a).unwrap();
        scheduler.add_task(task_b).unwrap();
        scheduler.add_task(task_c).unwrap();

        assert!(scheduler.has_circular_dependency("task_a"));
        assert!(scheduler.has_circular_dependency("task_b"));
        assert!(scheduler.has_circular_dependency("task_c"));
    }

    #[test]
    fn test_no_circular_dependency() {
        let mut scheduler = BeatScheduler::new();

        let task_a = ScheduledTask::new("task_a".to_string(), Schedule::interval(60));

        let mut task_b = ScheduledTask::new("task_b".to_string(), Schedule::interval(60));
        task_b.add_dependency("task_a".to_string());

        let mut task_c = ScheduledTask::new("task_c".to_string(), Schedule::interval(60));
        task_c.add_dependency("task_b".to_string());

        scheduler.add_task(task_a).unwrap();
        scheduler.add_task(task_b).unwrap();
        scheduler.add_task(task_c).unwrap();

        assert!(!scheduler.has_circular_dependency("task_a"));
        assert!(!scheduler.has_circular_dependency("task_b"));
        assert!(!scheduler.has_circular_dependency("task_c"));
    }

    #[test]
    fn test_dependency_chain() {
        let mut scheduler = BeatScheduler::new();

        let task_a = ScheduledTask::new("task_a".to_string(), Schedule::interval(60));

        let mut task_b = ScheduledTask::new("task_b".to_string(), Schedule::interval(60));
        task_b.add_dependency("task_a".to_string());

        let mut task_c = ScheduledTask::new("task_c".to_string(), Schedule::interval(60));
        task_c.add_dependency("task_b".to_string());

        scheduler.add_task(task_a).unwrap();
        scheduler.add_task(task_b).unwrap();
        scheduler.add_task(task_c).unwrap();

        let chain = scheduler.get_dependency_chain("task_c").unwrap();

        // Chain should be in execution order: a -> b -> c
        assert_eq!(chain.len(), 3);
        assert_eq!(chain[0], "task_a");
        assert_eq!(chain[1], "task_b");
        assert_eq!(chain[2], "task_c");
    }

    #[test]
    fn test_dependency_chain_circular() {
        let mut scheduler = BeatScheduler::new();

        let mut task_a = ScheduledTask::new("task_a".to_string(), Schedule::interval(60));
        task_a.add_dependency("task_b".to_string());

        let mut task_b = ScheduledTask::new("task_b".to_string(), Schedule::interval(60));
        task_b.add_dependency("task_a".to_string());

        scheduler.add_task(task_a).unwrap();
        scheduler.add_task(task_b).unwrap();

        let result = scheduler.get_dependency_chain("task_a");
        assert!(result.is_err());

        if let Err(ScheduleError::Invalid(msg)) = result {
            assert!(msg.contains("Circular dependency"));
        }
    }

    #[test]
    fn test_validate_dependencies_success() {
        let mut scheduler = BeatScheduler::new();

        let task_a = ScheduledTask::new("task_a".to_string(), Schedule::interval(60));

        let mut task_b = ScheduledTask::new("task_b".to_string(), Schedule::interval(60));
        task_b.add_dependency("task_a".to_string());

        scheduler.add_task(task_a).unwrap();
        scheduler.add_task(task_b).unwrap();

        assert!(scheduler.validate_dependencies().is_ok());
    }

    #[test]
    fn test_validate_dependencies_missing_task() {
        let mut scheduler = BeatScheduler::new();

        let mut task_b = ScheduledTask::new("task_b".to_string(), Schedule::interval(60));
        task_b.add_dependency("nonexistent_task".to_string());

        scheduler.add_task(task_b).unwrap();

        let result = scheduler.validate_dependencies();
        assert!(result.is_err());

        if let Err(ScheduleError::Invalid(msg)) = result {
            assert!(msg.contains("non-existent task"));
        }
    }

    #[test]
    fn test_tasks_ready_with_dependencies() {
        let mut scheduler = BeatScheduler::new();

        // task_a has no dependencies and is due (past time)
        let task_a = ScheduledTask::new(
            "task_a".to_string(),
            Schedule::onetime(Utc::now() - Duration::hours(1)),
        );

        // task_b depends on task_a and is due
        let mut task_b = ScheduledTask::new(
            "task_b".to_string(),
            Schedule::onetime(Utc::now() - Duration::hours(1)),
        );
        task_b.add_dependency("task_a".to_string());

        scheduler.add_task(task_a).unwrap();
        scheduler.add_task(task_b).unwrap();

        let completed = HashSet::new();
        let failed = HashSet::new();

        // Only task_a should be ready (no dependencies)
        let ready = scheduler.get_tasks_ready_with_dependencies(&completed, &failed);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].name, "task_a");

        // After task_a completes, both tasks should be ready
        // (task_a is still in scheduler since we didn't call mark_task_success, and task_b now has satisfied dependencies)
        let mut completed = HashSet::new();
        completed.insert("task_a".to_string());

        let ready = scheduler.get_tasks_ready_with_dependencies(&completed, &failed);
        assert_eq!(ready.len(), 2); // Both tasks are ready now

        // Find task_b in the ready list
        let task_b_ready = ready.iter().any(|t| t.name == "task_b");
        assert!(
            task_b_ready,
            "task_b should be ready after task_a completes"
        );
    }

    #[test]
    fn test_dependency_serialization() {
        let mut deps = HashSet::new();
        deps.insert("task_a".to_string());
        deps.insert("task_b".to_string());

        let task = ScheduledTask::new("task_c".to_string(), Schedule::interval(60))
            .with_dependencies(deps);

        // Serialize
        let json = serde_json::to_string(&task).unwrap();

        // Deserialize
        let deserialized: ScheduledTask = serde_json::from_str(&json).unwrap();

        // Verify
        assert_eq!(deserialized.dependencies.len(), 2);
        assert!(deserialized.depends_on("task_a"));
        assert!(deserialized.depends_on("task_b"));
        assert!(deserialized.wait_for_dependencies);
    }
}
