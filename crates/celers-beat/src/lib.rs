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
use std::collections::HashMap;
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
                minute, hour, day_of_month, month_of_year, day_of_week
            ),
            #[cfg(feature = "solar")]
            Schedule::Solar {
                event,
                latitude,
                longitude,
            } => write!(f, "Solar[{} at ({:.4}, {:.4})]", event, latitude, longitude),
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
}

fn default_true() -> bool {
    true
}

impl ScheduledTask {
    pub fn new(name: String, schedule: Schedule) -> Self {
        Self {
            name,
            schedule,
            args: Vec::new(),
            kwargs: HashMap::new(),
            options: TaskOptions::default(),
            last_run_at: None,
            total_run_count: 0,
            enabled: true,
        }
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

    /// Check if task is due to run
    pub fn is_due(&self) -> Result<bool, ScheduleError> {
        let next_run = self.schedule.next_run(self.last_run_at)?;
        Ok(Utc::now() >= next_run)
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

    pub fn get_due_tasks(&self) -> Vec<&ScheduledTask> {
        self.tasks
            .values()
            .filter(|task| task.enabled && task.is_due().unwrap_or(false))
            .collect()
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

    #[test]
    fn test_interval_schedule() {
        let schedule = Schedule::interval(60);
        let last_run = Utc::now();
        let next_run = schedule.next_run(Some(last_run)).unwrap();

        assert!(next_run > last_run);
    }

    #[test]
    fn test_scheduled_task() {
        let schedule = Schedule::interval(60);
        let task = ScheduledTask::new("test_task".to_string(), schedule)
            .with_args(vec![serde_json::json!(1)]);

        assert_eq!(task.name, "test_task");
        assert_eq!(task.args.len(), 1);
        assert!(task.enabled);
    }

    #[test]
    fn test_beat_scheduler() {
        let mut scheduler = BeatScheduler::new();
        let schedule = Schedule::interval(60);
        let task = ScheduledTask::new("test_task".to_string(), schedule);

        scheduler.add_task(task).unwrap();

        assert_eq!(scheduler.tasks.len(), 1);
    }
}
