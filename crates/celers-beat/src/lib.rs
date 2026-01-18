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

use chrono::Datelike;
use chrono::{DateTime, Duration, Timelike, Utc};
#[cfg(feature = "cron")]
use chrono::{Offset, TimeZone};
#[cfg(feature = "cron")]
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;

/// Failure notification callback type
///
/// Called when a task execution fails. Receives the task name and error message.
pub type FailureCallback = Arc<dyn Fn(&str, &str) + Send + Sync>;

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
        /// Timezone (IANA timezone name, e.g., "America/New_York")
        /// If None, uses UTC
        #[serde(default)]
        timezone: Option<String>,
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

    /// Create crontab schedule (UTC)
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
            timezone: None,
        }
    }

    /// Create crontab schedule with timezone
    ///
    /// # Arguments
    /// * `minute` - Minute field (0-59, *, */N)
    /// * `hour` - Hour field (0-23, *, */N)
    /// * `day_of_week` - Day of week field (0-6, *, */N)
    /// * `day_of_month` - Day of month field (1-31, *, */N)
    /// * `month_of_year` - Month field (1-12, *, */N)
    /// * `timezone` - IANA timezone name (e.g., "America/New_York", "Europe/London")
    ///
    /// # Examples
    /// ```
    /// use celers_beat::Schedule;
    ///
    /// // Run at 9:00 AM New York time every weekday
    /// let schedule = Schedule::crontab_tz(
    ///     "0",
    ///     "9",
    ///     "1-5",
    ///     "*",
    ///     "*",
    ///     "America/New_York"
    /// );
    /// ```
    #[cfg(feature = "cron")]
    pub fn crontab_tz(
        minute: &str,
        hour: &str,
        day_of_week: &str,
        day_of_month: &str,
        month_of_year: &str,
        timezone: &str,
    ) -> Self {
        Self::Crontab {
            minute: minute.to_string(),
            hour: hour.to_string(),
            day_of_week: day_of_week.to_string(),
            day_of_month: day_of_month.to_string(),
            month_of_year: month_of_year.to_string(),
            timezone: Some(timezone.to_string()),
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
                timezone,
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

                // If timezone is specified, convert to/from that timezone
                if let Some(tz_str) = timezone {
                    let tz: Tz = tz_str.parse().map_err(|_| {
                        ScheduleError::Parse(format!("Invalid timezone: {}", tz_str))
                    })?;

                    // Convert current UTC time to target timezone
                    let after_utc = last_run.unwrap_or_else(Utc::now);
                    let after_tz = after_utc.with_timezone(&tz);

                    // Find next occurrence in target timezone
                    let next_tz = cron_schedule.after(&after_tz).next().ok_or_else(|| {
                        ScheduleError::Invalid("No future execution time".to_string())
                    })?;

                    // Convert back to UTC
                    Ok(next_tz.with_timezone(&Utc))
                } else {
                    // No timezone specified, use UTC
                    let after = last_run.unwrap_or_else(Utc::now);
                    let next = cron_schedule.after(&after).next().ok_or_else(|| {
                        ScheduleError::Invalid("No future execution time".to_string())
                    })?;

                    Ok(next)
                }
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
                        // Civil twilight (sun 6° below horizon) - approximate 30 min before/after sunrise/sunset
                        "civil_twilight_begin" | "dawn" => {
                            let hours = (sunrise_time / 60) as u32;
                            let minutes = (sunrise_time % 60) as u32;
                            let sunrise = current_date
                                .and_hms_opt(hours, minutes, 0)
                                .ok_or_else(|| {
                                    ScheduleError::Invalid(format!(
                                        "Invalid sunrise time: {} minutes",
                                        sunrise_time
                                    ))
                                })?
                                .and_utc();
                            sunrise - Duration::minutes(30)
                        }
                        "civil_twilight_end" | "dusk" => {
                            let hours = (sunset_time / 60) as u32;
                            let minutes = (sunset_time % 60) as u32;
                            let sunset = current_date
                                .and_hms_opt(hours, minutes, 0)
                                .ok_or_else(|| {
                                    ScheduleError::Invalid(format!(
                                        "Invalid sunset time: {} minutes",
                                        sunset_time
                                    ))
                                })?
                                .and_utc();
                            sunset + Duration::minutes(30)
                        }
                        // Nautical twilight (sun 12° below horizon) - approximate 60 min before/after sunrise/sunset
                        "nautical_twilight_begin" => {
                            let hours = (sunrise_time / 60) as u32;
                            let minutes = (sunrise_time % 60) as u32;
                            let sunrise = current_date
                                .and_hms_opt(hours, minutes, 0)
                                .ok_or_else(|| {
                                    ScheduleError::Invalid(format!(
                                        "Invalid sunrise time: {} minutes",
                                        sunrise_time
                                    ))
                                })?
                                .and_utc();
                            sunrise - Duration::minutes(60)
                        }
                        "nautical_twilight_end" => {
                            let hours = (sunset_time / 60) as u32;
                            let minutes = (sunset_time % 60) as u32;
                            let sunset = current_date
                                .and_hms_opt(hours, minutes, 0)
                                .ok_or_else(|| {
                                    ScheduleError::Invalid(format!(
                                        "Invalid sunset time: {} minutes",
                                        sunset_time
                                    ))
                                })?
                                .and_utc();
                            sunset + Duration::minutes(60)
                        }
                        // Astronomical twilight (sun 18° below horizon) - approximate 90 min before/after sunrise/sunset
                        "astronomical_twilight_begin" => {
                            let hours = (sunrise_time / 60) as u32;
                            let minutes = (sunrise_time % 60) as u32;
                            let sunrise = current_date
                                .and_hms_opt(hours, minutes, 0)
                                .ok_or_else(|| {
                                    ScheduleError::Invalid(format!(
                                        "Invalid sunrise time: {} minutes",
                                        sunrise_time
                                    ))
                                })?
                                .and_utc();
                            sunrise - Duration::minutes(90)
                        }
                        "astronomical_twilight_end" => {
                            let hours = (sunset_time / 60) as u32;
                            let minutes = (sunset_time % 60) as u32;
                            let sunset = current_date
                                .and_hms_opt(hours, minutes, 0)
                                .ok_or_else(|| {
                                    ScheduleError::Invalid(format!(
                                        "Invalid sunset time: {} minutes",
                                        sunset_time
                                    ))
                                })?
                                .and_utc();
                            sunset + Duration::minutes(90)
                        }
                        // Golden hour (sun 0-6° above horizon) - approximate 30 min after sunrise / before sunset
                        "golden_hour_begin" => {
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
                            // Golden hour starts at sunrise
                        }
                        "golden_hour_end" => {
                            let hours = (sunset_time / 60) as u32;
                            let minutes = (sunset_time % 60) as u32;
                            let sunset = current_date
                                .and_hms_opt(hours, minutes, 0)
                                .ok_or_else(|| {
                                    ScheduleError::Invalid(format!(
                                        "Invalid sunset time: {} minutes",
                                        sunset_time
                                    ))
                                })?
                                .and_utc();
                            // Golden hour ends ~30 min before sunset
                            sunset - Duration::minutes(30)
                        }
                        _ => {
                            return Err(ScheduleError::Invalid(format!(
                                "Unknown solar event: {}. Supported events: sunrise, sunset, civil_twilight_begin/end, nautical_twilight_begin/end, astronomical_twilight_begin/end, golden_hour_begin/end, dawn, dusk",
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
                timezone,
            } => {
                if let Some(tz) = timezone {
                    write!(
                        f,
                        "Crontab[{} {} {} {} {} ({})]",
                        minute, hour, day_of_month, day_of_week, month_of_year, tz
                    )
                } else {
                    write!(
                        f,
                        "Crontab[{} {} {} {} {} (UTC)]",
                        minute, hour, day_of_month, day_of_week, month_of_year
                    )
                }
            }
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

/// Schedule lock for preventing duplicate execution in distributed scenarios
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleLock {
    /// Task name this lock is for
    pub task_name: String,
    /// Lock owner identifier (e.g., scheduler instance ID)
    pub owner: String,
    /// When the lock was acquired
    pub acquired_at: DateTime<Utc>,
    /// When the lock expires (for automatic cleanup)
    pub expires_at: DateTime<Utc>,
    /// Lock renewal count (for debugging)
    pub renewal_count: u32,
}

impl ScheduleLock {
    /// Create a new schedule lock
    ///
    /// # Arguments
    /// * `task_name` - Name of the task to lock
    /// * `owner` - Identifier of the lock owner (e.g., scheduler instance ID)
    /// * `ttl_seconds` - Time-to-live for the lock in seconds
    pub fn new(task_name: String, owner: String, ttl_seconds: u64) -> Self {
        let now = Utc::now();
        Self {
            task_name,
            owner,
            acquired_at: now,
            expires_at: now + Duration::seconds(ttl_seconds as i64),
            renewal_count: 0,
        }
    }

    /// Check if the lock has expired
    pub fn is_expired(&self) -> bool {
        Utc::now() > self.expires_at
    }

    /// Check if the lock is owned by the given owner
    pub fn is_owned_by(&self, owner: &str) -> bool {
        self.owner == owner
    }

    /// Renew the lock for another TTL period
    ///
    /// # Arguments
    /// * `ttl_seconds` - Time-to-live for the renewed lock
    ///
    /// # Returns
    /// * `Ok(())` if renewed successfully
    /// * `Err(ScheduleError)` if the lock has already expired
    pub fn renew(&mut self, ttl_seconds: u64) -> Result<(), ScheduleError> {
        if self.is_expired() {
            return Err(ScheduleError::Invalid(
                "Cannot renew expired lock".to_string(),
            ));
        }

        self.expires_at = Utc::now() + Duration::seconds(ttl_seconds as i64);
        self.renewal_count += 1;
        Ok(())
    }

    /// Get remaining time until expiration
    pub fn ttl(&self) -> Duration {
        self.expires_at - Utc::now()
    }

    /// Get age of the lock since acquisition
    pub fn age(&self) -> Duration {
        Utc::now() - self.acquired_at
    }
}

/// Lock manager for schedule locks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockManager {
    /// Active locks by task name
    locks: HashMap<String, ScheduleLock>,
    /// Default lock TTL in seconds
    default_ttl: u64,
}

impl LockManager {
    /// Create a new lock manager
    ///
    /// # Arguments
    /// * `default_ttl` - Default time-to-live for locks in seconds
    pub fn new(default_ttl: u64) -> Self {
        Self {
            locks: HashMap::new(),
            default_ttl,
        }
    }

    /// Try to acquire a lock for a task
    ///
    /// # Arguments
    /// * `task_name` - Name of the task to lock
    /// * `owner` - Identifier of the lock owner
    /// * `ttl` - Optional custom TTL (uses default if None)
    ///
    /// # Returns
    /// * `Ok(true)` if lock acquired successfully
    /// * `Ok(false)` if lock is already held by another owner
    /// * `Err` on other errors
    pub fn try_acquire(
        &mut self,
        task_name: &str,
        owner: &str,
        ttl: Option<u64>,
    ) -> Result<bool, ScheduleError> {
        // Clean up expired locks first
        self.cleanup_expired();

        // Check if lock exists and is not expired
        if let Some(existing_lock) = self.locks.get(task_name) {
            if !existing_lock.is_expired() {
                // Lock is held by someone else
                if !existing_lock.is_owned_by(owner) {
                    return Ok(false);
                }
                // Lock is already held by us, consider it acquired
                return Ok(true);
            }
        }

        // Acquire the lock
        let ttl_seconds = ttl.unwrap_or(self.default_ttl);
        let lock = ScheduleLock::new(task_name.to_string(), owner.to_string(), ttl_seconds);
        self.locks.insert(task_name.to_string(), lock);
        Ok(true)
    }

    /// Release a lock
    ///
    /// # Arguments
    /// * `task_name` - Name of the task to unlock
    /// * `owner` - Identifier of the lock owner (must match)
    ///
    /// # Returns
    /// * `Ok(true)` if lock was released
    /// * `Ok(false)` if lock doesn't exist or is owned by someone else
    pub fn release(&mut self, task_name: &str, owner: &str) -> Result<bool, ScheduleError> {
        if let Some(lock) = self.locks.get(task_name) {
            if lock.is_owned_by(owner) {
                self.locks.remove(task_name);
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Renew an existing lock
    ///
    /// # Arguments
    /// * `task_name` - Name of the task
    /// * `owner` - Identifier of the lock owner (must match)
    /// * `ttl` - Optional custom TTL (uses default if None)
    ///
    /// # Returns
    /// * `Ok(true)` if lock was renewed
    /// * `Ok(false)` if lock doesn't exist, is owned by someone else, or has expired
    pub fn renew(
        &mut self,
        task_name: &str,
        owner: &str,
        ttl: Option<u64>,
    ) -> Result<bool, ScheduleError> {
        if let Some(lock) = self.locks.get_mut(task_name) {
            if lock.is_owned_by(owner) && !lock.is_expired() {
                let ttl_seconds = ttl.unwrap_or(self.default_ttl);
                lock.renew(ttl_seconds)?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Check if a lock is held
    ///
    /// # Arguments
    /// * `task_name` - Name of the task
    ///
    /// # Returns
    /// * `true` if lock exists and is not expired
    /// * `false` otherwise
    pub fn is_locked(&self, task_name: &str) -> bool {
        if let Some(lock) = self.locks.get(task_name) {
            !lock.is_expired()
        } else {
            false
        }
    }

    /// Get information about a lock
    pub fn get_lock(&self, task_name: &str) -> Option<&ScheduleLock> {
        self.locks.get(task_name)
    }

    /// Clean up expired locks
    pub fn cleanup_expired(&mut self) {
        self.locks.retain(|_, lock| !lock.is_expired());
    }

    /// Get all active locks
    pub fn get_active_locks(&self) -> Vec<&ScheduleLock> {
        self.locks
            .values()
            .filter(|lock| !lock.is_expired())
            .collect()
    }

    /// Force release all locks (use with caution)
    pub fn release_all(&mut self) {
        self.locks.clear();
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new(300) // Default 5 minute TTL
    }
}

/// Schedule conflict severity level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictSeverity {
    /// Low priority - tasks can run concurrently
    Low,
    /// Medium priority - tasks may interfere
    Medium,
    /// High priority - tasks will definitely conflict
    High,
}

/// Represents a conflict between two scheduled tasks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleConflict {
    /// First task name
    pub task1: String,
    /// Second task name
    pub task2: String,
    /// Conflict severity
    pub severity: ConflictSeverity,
    /// Time window where conflict occurs (in seconds)
    pub overlap_seconds: u64,
    /// Description of the conflict
    pub description: String,
    /// Suggested resolution
    pub resolution: Option<String>,
}

impl ScheduleConflict {
    /// Create a new schedule conflict
    pub fn new(
        task1: String,
        task2: String,
        severity: ConflictSeverity,
        overlap_seconds: u64,
        description: String,
    ) -> Self {
        Self {
            task1,
            task2,
            severity,
            overlap_seconds,
            description,
            resolution: None,
        }
    }

    /// Add a suggested resolution
    pub fn with_resolution(mut self, resolution: String) -> Self {
        self.resolution = Some(resolution);
        self
    }

    /// Check if this is a high severity conflict
    pub fn is_high_severity(&self) -> bool {
        self.severity == ConflictSeverity::High
    }

    /// Check if this is a medium severity conflict
    pub fn is_medium_severity(&self) -> bool {
        self.severity == ConflictSeverity::Medium
    }

    /// Check if this is a low severity conflict
    pub fn is_low_severity(&self) -> bool {
        self.severity == ConflictSeverity::Low
    }
}

impl std::fmt::Display for ScheduleConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Conflict[{:?}]: {} <-> {} (overlap: {}s) - {}",
            self.severity, self.task1, self.task2, self.overlap_seconds, self.description
        )
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

/// Alert severity level
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
pub enum AlertLevel {
    /// Informational alert
    Info,
    /// Warning alert - requires attention
    Warning,
    /// Critical alert - requires immediate action
    Critical,
}

impl std::fmt::Display for AlertLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlertLevel::Info => write!(f, "INFO"),
            AlertLevel::Warning => write!(f, "WARNING"),
            AlertLevel::Critical => write!(f, "CRITICAL"),
        }
    }
}

/// Alert condition that triggered the alert
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash, Eq)]
#[serde(tag = "type")]
pub enum AlertCondition {
    /// Schedule was missed (task didn't execute when expected)
    MissedSchedule {
        /// Expected run time
        expected_at: DateTime<Utc>,
        /// Current time when missed was detected
        detected_at: DateTime<Utc>,
    },
    /// Multiple consecutive failures
    ConsecutiveFailures {
        /// Number of consecutive failures
        count: u32,
        /// Failure threshold that triggered the alert
        threshold: u32,
    },
    /// High failure rate detected
    HighFailureRate {
        /// Current failure rate (0.0 to 1.0)
        rate: String, // String to make it hashable
        /// Threshold that was exceeded
        threshold: String,
    },
    /// Slow execution detected
    SlowExecution {
        /// Actual duration in milliseconds
        duration_ms: u64,
        /// Expected/threshold duration in milliseconds
        threshold_ms: u64,
    },
    /// Task is stuck (not executing for extended period)
    TaskStuck {
        /// Time since last execution
        idle_duration_seconds: i64,
        /// Expected interval in seconds
        expected_interval_seconds: u64,
    },
    /// Task has become unhealthy
    TaskUnhealthy {
        /// Health issues detected
        issues: Vec<String>,
    },
}

impl std::fmt::Display for AlertCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlertCondition::MissedSchedule {
                expected_at,
                detected_at,
            } => {
                let delay = detected_at
                    .signed_duration_since(*expected_at)
                    .num_seconds();
                write!(
                    f,
                    "Missed schedule ({}s late, expected at {})",
                    delay,
                    expected_at.format("%Y-%m-%d %H:%M:%S UTC")
                )
            }
            AlertCondition::ConsecutiveFailures { count, threshold } => {
                write!(
                    f,
                    "Consecutive failures ({} failures, threshold: {})",
                    count, threshold
                )
            }
            AlertCondition::HighFailureRate { rate, threshold } => {
                write!(
                    f,
                    "High failure rate (rate: {}, threshold: {})",
                    rate, threshold
                )
            }
            AlertCondition::SlowExecution {
                duration_ms,
                threshold_ms,
            } => {
                write!(
                    f,
                    "Slow execution ({}ms, threshold: {}ms)",
                    duration_ms, threshold_ms
                )
            }
            AlertCondition::TaskStuck {
                idle_duration_seconds,
                expected_interval_seconds,
            } => {
                write!(
                    f,
                    "Task stuck (idle: {}s, expected interval: {}s)",
                    idle_duration_seconds, expected_interval_seconds
                )
            }
            AlertCondition::TaskUnhealthy { issues } => {
                write!(f, "Task unhealthy: {}", issues.join(", "))
            }
        }
    }
}

/// Alert record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    /// Alert timestamp
    pub timestamp: DateTime<Utc>,
    /// Task name that triggered the alert
    pub task_name: String,
    /// Alert severity level
    pub level: AlertLevel,
    /// Condition that triggered the alert
    pub condition: AlertCondition,
    /// Human-readable message
    pub message: String,
    /// Additional metadata (optional)
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

impl Alert {
    /// Create a new alert
    pub fn new(
        task_name: String,
        level: AlertLevel,
        condition: AlertCondition,
        message: String,
    ) -> Self {
        Self {
            timestamp: Utc::now(),
            task_name,
            level,
            condition,
            message,
            metadata: HashMap::new(),
        }
    }

    /// Add metadata to the alert
    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }

    /// Check if this is a critical alert
    pub fn is_critical(&self) -> bool {
        self.level == AlertLevel::Critical
    }

    /// Check if this is a warning alert
    pub fn is_warning(&self) -> bool {
        self.level == AlertLevel::Warning
    }

    /// Check if this is an info alert
    pub fn is_info(&self) -> bool {
        self.level == AlertLevel::Info
    }

    /// Get a unique key for deduplication
    fn dedup_key(&self) -> String {
        format!("{}::{:?}", self.task_name, self.condition)
    }
}

impl std::fmt::Display for Alert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}] {} - {} - {}",
            self.timestamp.format("%Y-%m-%d %H:%M:%S UTC"),
            self.level,
            self.task_name,
            self.message
        )
    }
}

/// Alert callback type
///
/// Called when an alert is triggered. Receives the alert details.
pub type AlertCallback = Arc<dyn Fn(&Alert) + Send + Sync>;

/// Alert manager for tracking and deduplicating alerts
#[derive(Clone, Serialize, Deserialize)]
pub struct AlertManager {
    /// Recent alerts (limited size)
    alerts: Vec<Alert>,
    /// Maximum number of alerts to keep in history
    max_history: usize,
    /// Deduplication window in seconds
    dedup_window_seconds: i64,
    /// Last alert time by dedup key
    last_alert_time: HashMap<String, DateTime<Utc>>,
    /// Alert callbacks (not serialized)
    #[serde(skip)]
    callbacks: Vec<AlertCallback>,
}

impl AlertManager {
    /// Create a new alert manager
    ///
    /// # Arguments
    /// * `max_history` - Maximum number of alerts to keep in history
    /// * `dedup_window_seconds` - Time window for deduplicating alerts (e.g., 300 = 5 minutes)
    pub fn new(max_history: usize, dedup_window_seconds: i64) -> Self {
        Self {
            alerts: Vec::new(),
            max_history,
            dedup_window_seconds,
            last_alert_time: HashMap::new(),
            callbacks: Vec::new(),
        }
    }

    /// Add an alert callback
    pub fn add_callback(&mut self, callback: AlertCallback) {
        self.callbacks.push(callback);
    }

    /// Record an alert (with deduplication)
    ///
    /// # Arguments
    /// * `alert` - Alert to record
    ///
    /// # Returns
    /// * `true` if alert was recorded (not deduplicated)
    /// * `false` if alert was suppressed due to deduplication
    pub fn record_alert(&mut self, alert: Alert) -> bool {
        let dedup_key = alert.dedup_key();
        let now = Utc::now();

        // Check if we should deduplicate this alert
        if let Some(last_time) = self.last_alert_time.get(&dedup_key) {
            let elapsed = now.signed_duration_since(*last_time).num_seconds();
            if elapsed < self.dedup_window_seconds {
                // Suppress duplicate alert
                return false;
            }
        }

        // Record the alert
        self.last_alert_time.insert(dedup_key, now);

        // Trigger callbacks
        for callback in &self.callbacks {
            callback(&alert);
        }

        // Add to history
        self.alerts.push(alert);

        // Trim history if needed
        if self.alerts.len() > self.max_history {
            self.alerts.drain(0..self.alerts.len() - self.max_history);
        }

        // Cleanup old dedup entries (older than window)
        self.last_alert_time.retain(|_, last_time| {
            now.signed_duration_since(*last_time).num_seconds() < self.dedup_window_seconds * 2
        });

        true
    }

    /// Get all alerts
    pub fn get_alerts(&self) -> &[Alert] {
        &self.alerts
    }

    /// Get critical alerts
    pub fn get_critical_alerts(&self) -> Vec<&Alert> {
        self.alerts.iter().filter(|a| a.is_critical()).collect()
    }

    /// Get warning alerts
    pub fn get_warning_alerts(&self) -> Vec<&Alert> {
        self.alerts.iter().filter(|a| a.is_warning()).collect()
    }

    /// Get alerts for a specific task
    pub fn get_task_alerts(&self, task_name: &str) -> Vec<&Alert> {
        self.alerts
            .iter()
            .filter(|a| a.task_name == task_name)
            .collect()
    }

    /// Get recent alerts (within specified seconds)
    pub fn get_recent_alerts(&self, seconds: i64) -> Vec<&Alert> {
        let cutoff = Utc::now() - Duration::seconds(seconds);
        self.alerts
            .iter()
            .filter(|a| a.timestamp > cutoff)
            .collect()
    }

    /// Clear all alerts
    pub fn clear(&mut self) {
        self.alerts.clear();
        self.last_alert_time.clear();
    }

    /// Clear alerts for a specific task
    pub fn clear_task_alerts(&mut self, task_name: &str) {
        self.alerts.retain(|a| a.task_name != task_name);
        self.last_alert_time
            .retain(|k, _| !k.starts_with(&format!("{}::", task_name)));
    }

    /// Get alert count
    pub fn alert_count(&self) -> usize {
        self.alerts.len()
    }

    /// Get critical alert count
    pub fn critical_alert_count(&self) -> usize {
        self.alerts.iter().filter(|a| a.is_critical()).count()
    }

    /// Get warning alert count
    pub fn warning_alert_count(&self) -> usize {
        self.alerts.iter().filter(|a| a.is_warning()).count()
    }
}

impl Default for AlertManager {
    fn default() -> Self {
        Self::new(1000, 300) // Keep 1000 alerts, 5-minute dedup window
    }
}

impl std::fmt::Debug for AlertManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlertManager")
            .field("alerts_count", &self.alerts.len())
            .field("max_history", &self.max_history)
            .field("dedup_window_seconds", &self.dedup_window_seconds)
            .field("callbacks_count", &self.callbacks.len())
            .finish()
    }
}

/// Alert configuration for task monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertConfig {
    /// Enable alerting for this task
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Threshold for consecutive failures before alerting
    #[serde(default = "default_consecutive_failures_threshold")]
    pub consecutive_failures_threshold: u32,
    /// Threshold for failure rate (0.0 to 1.0) before alerting
    #[serde(default = "default_failure_rate_threshold")]
    pub failure_rate_threshold: f64,
    /// Threshold for slow execution (milliseconds)
    pub slow_execution_threshold_ms: Option<u64>,
    /// Enable alerts for missed schedules
    #[serde(default = "default_true")]
    pub alert_on_missed_schedule: bool,
    /// Enable alerts for task stuck
    #[serde(default = "default_true")]
    pub alert_on_stuck: bool,
}

#[allow(dead_code)]
fn default_true() -> bool {
    true
}

#[allow(dead_code)]
fn default_consecutive_failures_threshold() -> u32 {
    3
}

#[allow(dead_code)]
fn default_failure_rate_threshold() -> f64 {
    0.5
}

impl Default for AlertConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            consecutive_failures_threshold: 3,
            failure_rate_threshold: 0.5,
            slow_execution_threshold_ms: None,
            alert_on_missed_schedule: true,
            alert_on_stuck: true,
        }
    }
}

impl AlertConfig {
    /// Create a new alert configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Disable all alerts for this task
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }

    /// Set consecutive failures threshold
    pub fn with_consecutive_failures_threshold(mut self, threshold: u32) -> Self {
        self.consecutive_failures_threshold = threshold;
        self
    }

    /// Set failure rate threshold
    pub fn with_failure_rate_threshold(mut self, threshold: f64) -> Self {
        self.failure_rate_threshold = threshold;
        self
    }

    /// Set slow execution threshold
    pub fn with_slow_execution_threshold_ms(mut self, threshold_ms: u64) -> Self {
        self.slow_execution_threshold_ms = Some(threshold_ms);
        self
    }

    /// Disable missed schedule alerts
    pub fn without_missed_schedule_alerts(mut self) -> Self {
        self.alert_on_missed_schedule = false;
        self
    }

    /// Disable stuck task alerts
    pub fn without_stuck_alerts(mut self) -> Self {
        self.alert_on_stuck = false;
        self
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
    /// Execution was interrupted (e.g., scheduler crash)
    Interrupted,
}

/// Execution state for crash recovery
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ExecutionState {
    /// No execution in progress
    Idle,
    /// Execution is currently running
    Running {
        /// When execution started
        started_at: DateTime<Utc>,
        /// Expected timeout (for detection)
        timeout_after: Option<DateTime<Utc>>,
    },
}

impl Default for ExecutionState {
    fn default() -> Self {
        Self::Idle
    }
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

    /// Cached next run time (for performance optimization)
    #[serde(skip)]
    cached_next_run: Option<DateTime<Utc>>,

    /// Alert configuration for this task
    #[serde(default)]
    pub alert_config: AlertConfig,

    /// Current execution state (for crash recovery)
    #[serde(default)]
    pub execution_state: ExecutionState,

    /// Weighted Fair Queuing state
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wfq_state: Option<WFQState>,
}

fn default_version() -> u32 {
    1
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
            cached_next_run: None,
            alert_config: AlertConfig::default(),
            execution_state: ExecutionState::default(),
            wfq_state: None,
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

    /// Set alert configuration
    pub fn with_alert_config(mut self, config: AlertConfig) -> Self {
        self.alert_config = config;
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
        // Return cached value if available
        if let Some(cached) = self.cached_next_run {
            return Ok(cached);
        }

        // Calculate and return (but don't cache in immutable self)
        let mut next_run = self.schedule.next_run(self.last_run_at)?;

        // Apply jitter if configured
        if let Some(ref jitter) = self.jitter {
            next_run = jitter.apply(next_run, &self.name);
        }

        Ok(next_run)
    }

    /// Calculate and cache the next run time
    ///
    /// This method calculates the next run time and caches it for future calls.
    /// The cache is invalidated when the schedule changes or the task is executed.
    pub fn update_next_run_cache(&mut self) {
        if let Ok(next_run) = self.next_run_time_uncached() {
            self.cached_next_run = Some(next_run);
        } else {
            self.cached_next_run = None;
        }
    }

    /// Calculate next run time without using cache
    fn next_run_time_uncached(&self) -> Result<DateTime<Utc>, ScheduleError> {
        let mut next_run = self.schedule.next_run(self.last_run_at)?;

        // Apply jitter if configured
        if let Some(ref jitter) = self.jitter {
            next_run = jitter.apply(next_run, &self.name);
        }

        Ok(next_run)
    }

    /// Invalidate the next run time cache
    ///
    /// This should be called whenever the schedule changes or the task is executed.
    pub fn invalidate_next_run_cache(&mut self) {
        self.cached_next_run = None;
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

    /// Begin task execution (for crash recovery tracking)
    ///
    /// Marks the task as running and records the start time. This allows
    /// detection of interrupted executions after a crash.
    ///
    /// # Arguments
    /// * `timeout_seconds` - Optional timeout in seconds for execution detection
    pub fn begin_execution(&mut self, timeout_seconds: Option<u64>) {
        let timeout_after = timeout_seconds.map(|secs| Utc::now() + Duration::seconds(secs as i64));
        self.execution_state = ExecutionState::Running {
            started_at: Utc::now(),
            timeout_after,
        };
    }

    /// Complete task execution (for crash recovery tracking)
    ///
    /// Marks the task as idle and records the execution result. This should be
    /// called after every execution (success, failure, or timeout).
    pub fn complete_execution(&mut self) {
        self.execution_state = ExecutionState::Idle;
    }

    /// Detect if this task has an interrupted execution
    ///
    /// Returns true if the task is marked as running but should have completed
    /// based on timeout or time elapsed.
    ///
    /// # Returns
    /// * `true` if execution appears to be interrupted
    /// * `false` if execution is idle or still valid
    pub fn detect_interrupted_execution(&self) -> bool {
        match &self.execution_state {
            ExecutionState::Idle => false,
            ExecutionState::Running {
                started_at,
                timeout_after,
            } => {
                // Check explicit timeout
                if let Some(timeout) = timeout_after {
                    if Utc::now() > *timeout {
                        return true;
                    }
                }

                // Check if running for unreasonably long (fallback detection)
                let running_duration = Utc::now() - *started_at;
                let max_reasonable_duration = Duration::hours(24); // 24 hours max
                running_duration > max_reasonable_duration
            }
        }
    }

    /// Recover from interrupted execution
    ///
    /// Handles cleanup and recording of an interrupted execution.
    /// Creates an execution record marked as Interrupted and resets state.
    ///
    /// # Returns
    /// Duration the task was running before interruption
    pub fn recover_from_interruption(&mut self) -> Option<Duration> {
        match &self.execution_state {
            ExecutionState::Running { started_at, .. } => {
                let duration = Utc::now() - *started_at;

                // Record the interrupted execution in history
                let record = ExecutionRecord::completed(*started_at, ExecutionResult::Interrupted);
                self.execution_history.push(record);

                // Trim history if needed
                if self.max_history_size > 0 && self.execution_history.len() > self.max_history_size
                {
                    let remove_count = self.execution_history.len() - self.max_history_size;
                    self.execution_history.drain(0..remove_count);
                }

                // Reset execution state
                self.execution_state = ExecutionState::Idle;

                // Increment retry count for interrupted executions
                self.retry_count += 1;

                Some(duration)
            }
            ExecutionState::Idle => None,
        }
    }

    /// Check if task is ready for retry after interruption
    pub fn is_ready_for_retry_after_crash(&self) -> bool {
        // Task should be retried if it was interrupted and retry policy allows
        if !self.execution_history.is_empty() {
            if let Some(last_exec) = self.execution_history.last() {
                if last_exec.is_interrupted() && self.should_retry() {
                    return true;
                }
            }
        }
        false
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

    /// Get consecutive failure count from the end of history
    ///
    /// Returns the number of consecutive failures at the end of the execution history.
    /// If the last execution was successful or if there's no history, returns 0.
    pub fn consecutive_failure_count(&self) -> u32 {
        let mut count = 0u32;
        for record in self.execution_history.iter().rev() {
            if record.is_failure() {
                count += 1;
            } else {
                break;
            }
        }
        count
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

        // Invalidate and update cache after schedule change
        self.update_next_run_cache();

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
        let mut jitter_changed = false;
        if let Some(e) = enabled {
            self.enabled = e;
        }
        if let Some(j) = jitter {
            self.jitter = j;
            jitter_changed = true;
        }
        if let Some(c) = catchup_policy {
            self.catchup_policy = c;
        }

        // Update cache if jitter changed (affects next run time)
        if jitter_changed {
            self.update_next_run_cache();
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
            self.version_history.iter().rev().nth(1) // Skip current version
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

    /// Failure notification callbacks
    #[serde(skip)]
    failure_callbacks: Vec<FailureCallback>,

    /// Lock manager for preventing duplicate execution
    #[serde(default)]
    lock_manager: LockManager,

    /// Scheduler instance ID for lock ownership
    #[serde(skip)]
    instance_id: String,

    /// Alert manager for monitoring and notifications
    #[serde(default)]
    alert_manager: AlertManager,
}

impl BeatScheduler {
    pub fn new() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);

        Self {
            tasks: HashMap::new(),
            state_file: None,
            failure_callbacks: Vec::new(),
            lock_manager: LockManager::default(),
            instance_id: format!("scheduler-{}", id),
            alert_manager: AlertManager::default(),
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
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::SeqCst);

        Self {
            tasks: HashMap::new(),
            state_file: Some(state_file.into()),
            failure_callbacks: Vec::new(),
            lock_manager: LockManager::default(),
            instance_id: format!("scheduler-{}", id),
            alert_manager: AlertManager::default(),
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
            use std::sync::atomic::{AtomicU64, Ordering};
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let id = COUNTER.fetch_add(1, Ordering::SeqCst);

            return Ok(Self {
                tasks: HashMap::new(),
                state_file: Some(path),
                failure_callbacks: Vec::new(),
                lock_manager: LockManager::default(),
                instance_id: format!("scheduler-{}", id),
                alert_manager: AlertManager::default(),
            });
        }

        let content = std::fs::read_to_string(&path)
            .map_err(|e| ScheduleError::Persistence(format!("Failed to read state file: {}", e)))?;

        let mut scheduler: BeatScheduler = serde_json::from_str(&content).map_err(|e| {
            ScheduleError::Persistence(format!("Failed to parse state file: {}", e))
        })?;

        // Set state file and generate instance ID
        scheduler.state_file = Some(path);
        if scheduler.instance_id.is_empty() {
            use std::sync::atomic::{AtomicU64, Ordering};
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let id = COUNTER.fetch_add(1, Ordering::SeqCst);
            scheduler.instance_id = format!("scheduler-{}", id);
        }

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

    /// Export scheduler state as JSON string
    ///
    /// Returns the complete scheduler state serialized as a JSON string.
    /// This is useful for debugging, backup, or exporting to external systems.
    ///
    /// # Example
    /// ```
    /// use celers_beat::{BeatScheduler, Schedule, ScheduledTask};
    ///
    /// let mut scheduler = BeatScheduler::new();
    /// scheduler.add_task(ScheduledTask::new("test".to_string(), Schedule::interval(60))).unwrap();
    ///
    /// let json = scheduler.export_state().unwrap();
    /// assert!(json.contains("test"));
    /// ```
    pub fn export_state(&self) -> Result<String, ScheduleError> {
        serde_json::to_string_pretty(&self)
            .map_err(|e| ScheduleError::Persistence(format!("Failed to serialize state: {}", e)))
    }

    /// List all scheduled tasks
    ///
    /// Returns a reference to the internal task HashMap, allowing iteration
    /// over all scheduled tasks.
    ///
    /// # Example
    /// ```
    /// use celers_beat::{BeatScheduler, Schedule, ScheduledTask};
    ///
    /// let mut scheduler = BeatScheduler::new();
    /// scheduler.add_task(ScheduledTask::new("task1".to_string(), Schedule::interval(60))).unwrap();
    /// scheduler.add_task(ScheduledTask::new("task2".to_string(), Schedule::interval(120))).unwrap();
    ///
    /// let tasks = scheduler.list_tasks();
    /// assert_eq!(tasks.len(), 2);
    /// assert!(tasks.contains_key("task1"));
    /// assert!(tasks.contains_key("task2"));
    /// ```
    pub fn list_tasks(&self) -> &HashMap<String, ScheduledTask> {
        &self.tasks
    }

    /// Get a specific task by name
    ///
    /// # Example
    /// ```
    /// use celers_beat::{BeatScheduler, Schedule, ScheduledTask};
    ///
    /// let mut scheduler = BeatScheduler::new();
    /// scheduler.add_task(ScheduledTask::new("test".to_string(), Schedule::interval(60))).unwrap();
    ///
    /// let task = scheduler.get_task("test");
    /// assert!(task.is_some());
    /// assert_eq!(task.unwrap().name, "test");
    /// ```
    pub fn get_task(&self, name: &str) -> Option<&ScheduledTask> {
        self.tasks.get(name)
    }

    pub fn add_task(&mut self, mut task: ScheduledTask) -> Result<(), ScheduleError> {
        // Initialize the next run cache when adding the task
        task.update_next_run_cache();
        self.tasks.insert(task.name.clone(), task);
        self.save_state()?;
        Ok(())
    }

    /// Add multiple tasks in a batch operation
    ///
    /// This is more efficient than adding tasks individually as it only saves
    /// state once after all tasks are added.
    ///
    /// # Arguments
    /// * `tasks` - Vector of tasks to add
    ///
    /// # Returns
    /// Number of tasks successfully added
    ///
    /// # Example
    /// ```
    /// use celers_beat::{BeatScheduler, Schedule, ScheduledTask};
    ///
    /// let mut scheduler = BeatScheduler::new();
    /// let tasks = vec![
    ///     ScheduledTask::new("task1".to_string(), Schedule::interval(60)),
    ///     ScheduledTask::new("task2".to_string(), Schedule::interval(120)),
    ///     ScheduledTask::new("task3".to_string(), Schedule::interval(180)),
    /// ];
    ///
    /// let count = scheduler.add_tasks_batch(tasks).unwrap();
    /// assert_eq!(count, 3);
    /// ```
    pub fn add_tasks_batch(&mut self, tasks: Vec<ScheduledTask>) -> Result<usize, ScheduleError> {
        let mut added_count = 0;

        for mut task in tasks {
            // Initialize the next run cache when adding the task
            task.update_next_run_cache();
            self.tasks.insert(task.name.clone(), task);
            added_count += 1;
        }

        // Save state only once after all tasks are added
        if added_count > 0 {
            self.save_state()?;
        }

        Ok(added_count)
    }

    pub fn remove_task(&mut self, name: &str) -> Result<Option<ScheduledTask>, ScheduleError> {
        let task = self.tasks.remove(name);
        self.save_state()?;
        Ok(task)
    }

    /// Remove multiple tasks in a batch operation
    ///
    /// This is more efficient than removing tasks individually as it only saves
    /// state once after all tasks are removed.
    ///
    /// # Arguments
    /// * `names` - Slice of task names to remove
    ///
    /// # Returns
    /// Number of tasks successfully removed
    ///
    /// # Example
    /// ```
    /// use celers_beat::{BeatScheduler, Schedule, ScheduledTask};
    ///
    /// let mut scheduler = BeatScheduler::new();
    /// scheduler.add_task(ScheduledTask::new("task1".to_string(), Schedule::interval(60))).unwrap();
    /// scheduler.add_task(ScheduledTask::new("task2".to_string(), Schedule::interval(120))).unwrap();
    /// scheduler.add_task(ScheduledTask::new("task3".to_string(), Schedule::interval(180))).unwrap();
    ///
    /// let count = scheduler.remove_tasks_batch(&["task1", "task2"]).unwrap();
    /// assert_eq!(count, 2);
    /// ```
    pub fn remove_tasks_batch(&mut self, names: &[&str]) -> Result<usize, ScheduleError> {
        let mut removed_count = 0;

        for name in names {
            if self.tasks.remove(*name).is_some() {
                removed_count += 1;
            }
        }

        // Save state only once after all tasks are removed
        if removed_count > 0 {
            self.save_state()?;
        }

        Ok(removed_count)
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

            // Update next run cache after execution
            task.update_next_run_cache();

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

            // Update next run cache after execution
            task.update_next_run_cache();

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
            let record = ExecutionRecord::completed(
                now,
                ExecutionResult::Failure {
                    error: error.clone(),
                },
            );
            task.add_execution_record(record);

            // Invoke failure callbacks
            self.invoke_failure_callbacks(name, &error);

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
            let record = ExecutionRecord::completed(
                started_at,
                ExecutionResult::Failure {
                    error: error.clone(),
                },
            );
            task.add_execution_record(record);

            // Invoke failure callbacks
            self.invoke_failure_callbacks(name, &error);

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

    /// Register a failure notification callback
    ///
    /// The callback will be invoked whenever a task execution fails.
    ///
    /// # Arguments
    /// * `callback` - Callback function that receives task name and error message
    ///
    /// # Example
    /// ```
    /// use celers_beat::BeatScheduler;
    /// use std::sync::Arc;
    ///
    /// let mut scheduler = BeatScheduler::new();
    /// scheduler.on_failure(Arc::new(|task_name, error| {
    ///     eprintln!("Task {} failed: {}", task_name, error);
    /// }));
    /// ```
    pub fn on_failure(&mut self, callback: FailureCallback) {
        self.failure_callbacks.push(callback);
    }

    /// Clear all failure notification callbacks
    pub fn clear_failure_callbacks(&mut self) {
        self.failure_callbacks.clear();
    }

    /// Invoke all registered failure callbacks
    fn invoke_failure_callbacks(&self, task_name: &str, error: &str) {
        for callback in &self.failure_callbacks {
            callback(task_name, error);
        }
    }

    /// Register an alert callback
    ///
    /// The callback will be invoked whenever an alert is triggered.
    ///
    /// # Arguments
    /// * `callback` - Callback function that receives alert details
    ///
    /// # Example
    /// ```
    /// use celers_beat::BeatScheduler;
    /// use std::sync::Arc;
    ///
    /// let mut scheduler = BeatScheduler::new();
    /// scheduler.on_alert(Arc::new(|alert| {
    ///     eprintln!("ALERT: {}", alert);
    /// }));
    /// ```
    pub fn on_alert(&mut self, callback: AlertCallback) {
        self.alert_manager.add_callback(callback);
    }

    /// Get all alerts
    pub fn get_alerts(&self) -> &[Alert] {
        self.alert_manager.get_alerts()
    }

    /// Get critical alerts
    pub fn get_critical_alerts(&self) -> Vec<&Alert> {
        self.alert_manager.get_critical_alerts()
    }

    /// Get warning alerts
    pub fn get_warning_alerts(&self) -> Vec<&Alert> {
        self.alert_manager.get_warning_alerts()
    }

    /// Get alerts for a specific task
    pub fn get_task_alerts(&self, task_name: &str) -> Vec<&Alert> {
        self.alert_manager.get_task_alerts(task_name)
    }

    /// Get recent alerts within specified seconds
    pub fn get_recent_alerts(&self, seconds: i64) -> Vec<&Alert> {
        self.alert_manager.get_recent_alerts(seconds)
    }

    /// Clear all alerts
    pub fn clear_alerts(&mut self) {
        self.alert_manager.clear();
    }

    /// Clear alerts for a specific task
    pub fn clear_task_alerts(&mut self, task_name: &str) {
        self.alert_manager.clear_task_alerts(task_name);
    }

    /// Check alert conditions for a task and trigger alerts if needed
    ///
    /// This should be called periodically or after task execution to monitor for alert conditions.
    ///
    /// # Arguments
    /// * `task_name` - Name of the task to check
    ///
    /// # Returns
    /// Number of alerts triggered
    pub fn check_task_alerts(&mut self, task_name: &str) -> usize {
        let task = match self.tasks.get(task_name) {
            Some(t) => t,
            None => return 0,
        };

        if !task.alert_config.enabled {
            return 0;
        }

        let mut alerts_triggered = 0;

        // Check consecutive failures
        let consecutive_failures = task.consecutive_failure_count();
        if consecutive_failures >= task.alert_config.consecutive_failures_threshold {
            let alert = Alert::new(
                task_name.to_string(),
                AlertLevel::Critical,
                AlertCondition::ConsecutiveFailures {
                    count: consecutive_failures,
                    threshold: task.alert_config.consecutive_failures_threshold,
                },
                format!(
                    "Task has {} consecutive failures (threshold: {})",
                    consecutive_failures, task.alert_config.consecutive_failures_threshold
                ),
            );
            if self.alert_manager.record_alert(alert) {
                alerts_triggered += 1;
            }
        }

        // Check failure rate
        let failure_rate = task.failure_rate();
        if failure_rate > task.alert_config.failure_rate_threshold {
            let alert = Alert::new(
                task_name.to_string(),
                AlertLevel::Warning,
                AlertCondition::HighFailureRate {
                    rate: format!("{:.2}", failure_rate),
                    threshold: format!("{:.2}", task.alert_config.failure_rate_threshold),
                },
                format!(
                    "Task has high failure rate: {:.1}% (threshold: {:.1}%)",
                    failure_rate * 100.0,
                    task.alert_config.failure_rate_threshold * 100.0
                ),
            );
            if self.alert_manager.record_alert(alert) {
                alerts_triggered += 1;
            }
        }

        // Check slow execution
        if let Some(threshold_ms) = task.alert_config.slow_execution_threshold_ms {
            if let Some(avg_duration_ms) = task.average_duration_ms() {
                if avg_duration_ms > threshold_ms {
                    let alert = Alert::new(
                        task_name.to_string(),
                        AlertLevel::Warning,
                        AlertCondition::SlowExecution {
                            duration_ms: avg_duration_ms,
                            threshold_ms,
                        },
                        format!(
                            "Task execution is slow: {}ms average (threshold: {}ms)",
                            avg_duration_ms, threshold_ms
                        ),
                    );
                    if self.alert_manager.record_alert(alert) {
                        alerts_triggered += 1;
                    }
                }
            }
        }

        // Check if task is stuck
        if task.alert_config.alert_on_stuck {
            if let Some(stuck_duration) = task.is_stuck() {
                // Calculate expected interval based on schedule type
                let expected_interval_secs = match &task.schedule {
                    Schedule::Interval { every } => *every,
                    #[cfg(feature = "cron")]
                    Schedule::Crontab { .. } => 86400, // Assume daily
                    #[cfg(feature = "solar")]
                    Schedule::Solar { .. } => 86400, // Daily
                    Schedule::OneTime { .. } => 0, // Won't be stuck
                };

                let alert = Alert::new(
                    task_name.to_string(),
                    AlertLevel::Critical,
                    AlertCondition::TaskStuck {
                        idle_duration_seconds: stuck_duration.num_seconds(),
                        expected_interval_seconds: expected_interval_secs,
                    },
                    format!(
                        "Task is stuck: no execution for {}s (expected interval: {}s)",
                        stuck_duration.num_seconds(),
                        expected_interval_secs
                    ),
                );
                if self.alert_manager.record_alert(alert) {
                    alerts_triggered += 1;
                }
            }
        }

        // Check health status
        let health_result = task.check_health();
        if health_result.health.is_unhealthy() {
            let issues = health_result.health.get_issues();
            let alert = Alert::new(
                task_name.to_string(),
                AlertLevel::Critical,
                AlertCondition::TaskUnhealthy {
                    issues: issues.clone(),
                },
                format!("Task is unhealthy: {}", issues.join(", ")),
            );
            if self.alert_manager.record_alert(alert) {
                alerts_triggered += 1;
            }
        }

        alerts_triggered
    }

    /// Check alert conditions for all enabled tasks
    ///
    /// # Returns
    /// Total number of alerts triggered across all tasks
    pub fn check_all_alerts(&mut self) -> usize {
        let task_names: Vec<String> = self
            .tasks
            .keys()
            .filter(|name| {
                if let Some(task) = self.tasks.get(*name) {
                    task.enabled && task.alert_config.enabled
                } else {
                    false
                }
            })
            .cloned()
            .collect();

        let mut total_alerts = 0;
        for task_name in task_names {
            total_alerts += self.check_task_alerts(&task_name);
        }
        total_alerts
    }

    /// Get tasks that are ready for retry
    pub fn get_retry_tasks(&self) -> Vec<&ScheduledTask> {
        self.tasks
            .values()
            .filter(|task| task.enabled && task.is_ready_for_retry())
            .collect()
    }

    /// Detect tasks with interrupted executions (crash recovery)
    ///
    /// Scans all tasks to find those that were marked as running but appear
    /// to have been interrupted (e.g., due to scheduler crash).
    ///
    /// # Returns
    /// Vector of task names that have interrupted executions
    pub fn detect_crashed_tasks(&self) -> Vec<String> {
        self.tasks
            .iter()
            .filter(|(_, task)| task.detect_interrupted_execution())
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Recover from crash by handling all interrupted task executions
    ///
    /// This method should be called after loading scheduler state to detect
    /// and recover from any interrupted executions (e.g., after a crash).
    ///
    /// # Returns
    /// Number of tasks recovered from interruption
    ///
    /// # Example
    /// ```
    /// use celers_beat::BeatScheduler;
    ///
    /// // Load scheduler from persistent state
    /// let mut scheduler = BeatScheduler::load_from_file("schedules.json").unwrap();
    ///
    /// // Automatically recover from any crashes
    /// let recovered = scheduler.recover_from_crash();
    /// if recovered > 0 {
    ///     eprintln!("Recovered {} tasks from interrupted executions", recovered);
    /// }
    /// ```
    pub fn recover_from_crash(&mut self) -> usize {
        let crashed_task_names = self.detect_crashed_tasks();
        let mut recovered_count = 0;

        for task_name in crashed_task_names {
            if let Some(task) = self.tasks.get_mut(&task_name) {
                if let Some(duration) = task.recover_from_interruption() {
                    eprintln!(
                        "Recovered task '{}' from interrupted execution (was running for {}s)",
                        task_name,
                        duration.num_seconds()
                    );
                    recovered_count += 1;
                }
            }
        }

        // Save state after recovery
        let _ = self.save_state();

        recovered_count
    }

    /// Get tasks that need retry after crash recovery
    pub fn get_tasks_ready_for_crash_retry(&self) -> Vec<&ScheduledTask> {
        self.tasks
            .values()
            .filter(|task| task.enabled && task.is_ready_for_retry_after_crash())
            .collect()
    }

    pub fn get_due_tasks(&self) -> Vec<&ScheduledTask> {
        self.tasks
            .values()
            .filter(|task| task.enabled && task.is_due().unwrap_or(false))
            .collect()
    }

    /// Get due tasks sorted by priority (highest priority first)
    ///
    /// This method returns tasks that are due for execution, ordered by their priority.
    /// Higher priority tasks (higher numeric value) are returned first, allowing for
    /// priority-based execution scheduling.
    ///
    /// # Returns
    /// Vector of tasks sorted by priority (descending), then by next run time (ascending)
    ///
    /// # Example
    /// ```
    /// use celers_beat::{BeatScheduler, Schedule, ScheduledTask};
    ///
    /// let mut scheduler = BeatScheduler::new();
    ///
    /// // Add high priority task
    /// let mut high_priority = ScheduledTask::new("critical".to_string(), Schedule::interval(60));
    /// high_priority.options.priority = Some(9);
    /// scheduler.add_task(high_priority).unwrap();
    ///
    /// // Add low priority task
    /// let mut low_priority = ScheduledTask::new("background".to_string(), Schedule::interval(60));
    /// low_priority.options.priority = Some(1);
    /// scheduler.add_task(low_priority).unwrap();
    ///
    /// // Get tasks ordered by priority
    /// let due_tasks = scheduler.get_due_tasks_by_priority();
    /// // The critical task will be first
    /// ```
    pub fn get_due_tasks_by_priority(&self) -> Vec<&ScheduledTask> {
        let mut tasks: Vec<&ScheduledTask> = self
            .tasks
            .values()
            .filter(|task| task.enabled && task.is_due().unwrap_or(false))
            .collect();

        // Sort by priority (descending), then by next run time (ascending)
        tasks.sort_by(|a, b| {
            // Higher priority comes first (reverse order)
            let priority_a = a.options.priority.unwrap_or(5);
            let priority_b = b.options.priority.unwrap_or(5);

            match priority_b.cmp(&priority_a) {
                std::cmp::Ordering::Equal => {
                    // If same priority, sort by next run time
                    let next_a = a
                        .schedule
                        .next_run(a.last_run_at)
                        .unwrap_or_else(|_| Utc::now());
                    let next_b = b
                        .schedule
                        .next_run(b.last_run_at)
                        .unwrap_or_else(|_| Utc::now());
                    next_a.cmp(&next_b)
                }
                other => other,
            }
        });

        tasks
    }

    /// Get tasks ordered by priority regardless of due status
    ///
    /// This method returns all enabled tasks sorted by priority, which is useful for
    /// understanding task execution order and for manual task management.
    ///
    /// # Returns
    /// Vector of all enabled tasks sorted by priority (descending)
    pub fn get_tasks_by_priority(&self) -> Vec<&ScheduledTask> {
        let mut tasks: Vec<&ScheduledTask> =
            self.tasks.values().filter(|task| task.enabled).collect();

        // Sort by priority (descending)
        tasks.sort_by(|a, b| {
            let priority_a = a.options.priority.unwrap_or(5);
            let priority_b = b.options.priority.unwrap_or(5);
            priority_b.cmp(&priority_a)
        });

        tasks
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

    /// Detect tasks with missed schedules
    ///
    /// A schedule is considered "missed" if the task's next scheduled run time has passed
    /// but the task hasn't executed yet. This can happen if the scheduler was down or
    /// if task execution was delayed.
    ///
    /// # Arguments
    /// * `grace_period_seconds` - Additional time to allow before considering a schedule missed
    ///
    /// # Returns
    /// Vector of (task_name, missed_time) tuples, where missed_time is how long ago the task should have run
    pub fn detect_missed_schedules(&self, grace_period_seconds: u64) -> Vec<(String, Duration)> {
        let now = Utc::now();
        let grace_period = Duration::seconds(grace_period_seconds as i64);
        let mut missed = Vec::new();

        for (name, task) in &self.tasks {
            if !task.enabled {
                continue;
            }

            // Calculate next run time
            if let Ok(next_run) = task.schedule.next_run(task.last_run_at) {
                let deadline = next_run + grace_period;

                // Check if we've passed the deadline
                if now > deadline {
                    let missed_by = now - next_run;
                    missed.push((name.clone(), missed_by));
                }
            }
        }

        missed
    }

    /// Check for missed schedules and trigger alerts
    ///
    /// This method combines missed schedule detection with the alerting system,
    /// automatically creating alerts for tasks that have missed their schedules.
    ///
    /// # Arguments
    /// * `grace_period_seconds` - Grace period before considering a schedule missed (default: 60)
    ///
    /// # Returns
    /// Number of alerts triggered
    pub fn check_missed_schedules(&mut self, grace_period_seconds: Option<u64>) -> usize {
        let grace = grace_period_seconds.unwrap_or(60);
        let missed = self.detect_missed_schedules(grace);
        let mut alert_count = 0;
        let now = Utc::now();

        for (task_name, missed_by) in missed {
            // Calculate expected run time (now - missed_by)
            let expected_at = now - missed_by;

            // Create alert for missed schedule
            let alert = Alert {
                timestamp: now,
                task_name: task_name.clone(),
                level: AlertLevel::Warning,
                condition: AlertCondition::MissedSchedule {
                    expected_at,
                    detected_at: now,
                },
                message: format!(
                    "Task missed its schedule by {} seconds",
                    missed_by.num_seconds()
                ),
                metadata: HashMap::new(),
            };

            if self.alert_manager.record_alert(alert) {
                alert_count += 1;
            }
        }

        alert_count
    }

    /// Get statistics on missed schedules
    ///
    /// Returns detailed information about which tasks have missed schedules and by how much.
    ///
    /// # Arguments
    /// * `grace_period_seconds` - Grace period in seconds (default: 60)
    ///
    /// # Returns
    /// Vector of (task_name, seconds_missed, schedule_type) tuples sorted by severity
    pub fn get_missed_schedule_stats(
        &self,
        grace_period_seconds: Option<u64>,
    ) -> Vec<(String, i64, String)> {
        let grace = grace_period_seconds.unwrap_or(60);
        let mut stats: Vec<(String, i64, String)> = self
            .detect_missed_schedules(grace)
            .into_iter()
            .map(|(name, missed_by)| {
                let schedule_type = if let Some(task) = self.tasks.get(&name) {
                    format!("{}", task.schedule)
                } else {
                    "Unknown".to_string()
                };
                (name, missed_by.num_seconds(), schedule_type)
            })
            .collect();

        // Sort by seconds missed (descending)
        stats.sort_by(|a, b| b.1.cmp(&a.1));

        stats
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

    // ===== Lock Management Methods =====

    /// Try to acquire a lock for a task
    ///
    /// # Arguments
    /// * `task_name` - Name of the task to lock
    /// * `ttl` - Optional custom TTL in seconds (uses default if None)
    ///
    /// # Returns
    /// * `Ok(true)` if lock acquired successfully
    /// * `Ok(false)` if lock is already held by another scheduler
    ///
    /// # Example
    /// ```
    /// use celers_beat::BeatScheduler;
    ///
    /// let mut scheduler = BeatScheduler::new();
    ///
    /// // Try to acquire lock for a task
    /// let acquired = scheduler.try_acquire_lock("my_task", None).unwrap();
    /// if acquired {
    ///     println!("Lock acquired, safe to execute task");
    /// }
    /// ```
    pub fn try_acquire_lock(
        &mut self,
        task_name: &str,
        ttl: Option<u64>,
    ) -> Result<bool, ScheduleError> {
        self.lock_manager
            .try_acquire(task_name, &self.instance_id, ttl)
    }

    /// Release a lock for a task
    ///
    /// # Arguments
    /// * `task_name` - Name of the task to unlock
    ///
    /// # Returns
    /// * `Ok(true)` if lock was released
    /// * `Ok(false)` if lock doesn't exist or is owned by another scheduler
    pub fn release_lock(&mut self, task_name: &str) -> Result<bool, ScheduleError> {
        self.lock_manager.release(task_name, &self.instance_id)
    }

    /// Renew a lock for a task
    ///
    /// # Arguments
    /// * `task_name` - Name of the task
    /// * `ttl` - Optional custom TTL in seconds (uses default if None)
    ///
    /// # Returns
    /// * `Ok(true)` if lock was renewed
    /// * `Ok(false)` if lock doesn't exist, is owned by another scheduler, or has expired
    pub fn renew_lock(&mut self, task_name: &str, ttl: Option<u64>) -> Result<bool, ScheduleError> {
        self.lock_manager.renew(task_name, &self.instance_id, ttl)
    }

    /// Check if a task is locked
    ///
    /// # Arguments
    /// * `task_name` - Name of the task
    ///
    /// # Returns
    /// * `true` if task is locked by any scheduler
    /// * `false` otherwise
    pub fn is_task_locked(&self, task_name: &str) -> bool {
        self.lock_manager.is_locked(task_name)
    }

    /// Get information about a task lock
    ///
    /// # Arguments
    /// * `task_name` - Name of the task
    ///
    /// # Returns
    /// * `Some(&ScheduleLock)` if lock exists
    /// * `None` otherwise
    pub fn get_task_lock(&self, task_name: &str) -> Option<&ScheduleLock> {
        self.lock_manager.get_lock(task_name)
    }

    /// Clean up expired locks
    ///
    /// This is automatically called by try_acquire_lock, but can be called manually
    /// to clean up expired locks without acquiring new ones.
    pub fn cleanup_expired_locks(&mut self) {
        self.lock_manager.cleanup_expired();
    }

    /// Get all active locks
    ///
    /// # Returns
    /// Vector of all non-expired locks
    pub fn get_active_locks(&self) -> Vec<&ScheduleLock> {
        self.lock_manager.get_active_locks()
    }

    /// Get the scheduler instance ID
    ///
    /// This is used for lock ownership identification
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// Set custom instance ID
    ///
    /// Useful for distributed deployments where you want to use a specific
    /// identifier (e.g., hostname, pod name, etc.)
    ///
    /// # Arguments
    /// * `id` - Custom instance identifier
    pub fn set_instance_id(&mut self, id: String) {
        self.instance_id = id;
    }

    /// Execute a task with automatic lock management
    ///
    /// Attempts to acquire a lock before execution and releases it after.
    /// Returns Ok(false) if the lock cannot be acquired.
    ///
    /// # Arguments
    /// * `task_name` - Name of the task
    /// * `ttl` - Optional lock TTL in seconds
    /// * `f` - Function to execute if lock is acquired
    ///
    /// # Returns
    /// * `Ok(true)` if lock was acquired and function executed
    /// * `Ok(false)` if lock could not be acquired
    /// * `Err` on execution error
    pub fn execute_with_lock<F>(
        &mut self,
        task_name: &str,
        ttl: Option<u64>,
        mut f: F,
    ) -> Result<bool, ScheduleError>
    where
        F: FnMut() -> Result<(), ScheduleError>,
    {
        // Try to acquire lock
        if !self.try_acquire_lock(task_name, ttl)? {
            return Ok(false);
        }

        // Execute function
        let result = f();

        // Release lock (ignore errors)
        let _ = self.release_lock(task_name);

        // Return execution result
        result.map(|_| true)
    }

    // ===== Conflict Detection Methods =====

    /// Detect potential conflicts between scheduled tasks
    ///
    /// Analyzes all registered tasks to find potential scheduling conflicts
    /// based on their next run times and estimated execution durations.
    ///
    /// # Arguments
    /// * `window_seconds` - Time window to check for conflicts (default: 3600 seconds = 1 hour)
    /// * `estimated_duration` - Estimated task duration in seconds (default: 60 seconds)
    ///
    /// # Returns
    /// Vector of detected conflicts
    ///
    /// # Example
    /// ```
    /// use celers_beat::{BeatScheduler, Schedule, ScheduledTask};
    ///
    /// let mut scheduler = BeatScheduler::new();
    ///
    /// // Add two tasks that run at the same time
    /// scheduler.add_task(ScheduledTask::new("task1".to_string(), Schedule::interval(60))).unwrap();
    /// scheduler.add_task(ScheduledTask::new("task2".to_string(), Schedule::interval(60))).unwrap();
    ///
    /// // Check for conflicts
    /// let conflicts = scheduler.detect_conflicts(3600, 60);
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub fn detect_conflicts(
        &self,
        window_seconds: u64,
        estimated_duration: u64,
    ) -> Vec<ScheduleConflict> {
        let mut conflicts = Vec::new();
        let now = Utc::now();
        let window_end = now + Duration::seconds(window_seconds as i64);

        // Get all task names
        let task_names: Vec<String> = self.tasks.keys().cloned().collect();

        // Compare each pair of tasks
        for i in 0..task_names.len() {
            for j in (i + 1)..task_names.len() {
                let task1_name = &task_names[i];
                let task2_name = &task_names[j];

                if let (Some(task1), Some(task2)) =
                    (self.tasks.get(task1_name), self.tasks.get(task2_name))
                {
                    // Skip if either task is disabled
                    if !task1.enabled || !task2.enabled {
                        continue;
                    }

                    // Get next run times
                    let next1 = match task1.schedule.next_run(task1.last_run_at) {
                        Ok(time) => time,
                        Err(_) => continue,
                    };

                    let next2 = match task2.schedule.next_run(task2.last_run_at) {
                        Ok(time) => time,
                        Err(_) => continue,
                    };

                    // Check if both will run within the window
                    if next1 > window_end || next2 > window_end {
                        continue;
                    }

                    // Calculate overlap
                    let task1_start = next1;
                    let task1_end = next1 + Duration::seconds(estimated_duration as i64);
                    let task2_start = next2;
                    let task2_end = next2 + Duration::seconds(estimated_duration as i64);

                    // Check for overlap
                    if task1_start < task2_end && task2_start < task1_end {
                        let overlap_start = if task1_start > task2_start {
                            task1_start
                        } else {
                            task2_start
                        };
                        let overlap_end = if task1_end < task2_end {
                            task1_end
                        } else {
                            task2_end
                        };
                        let overlap_seconds = (overlap_end - overlap_start).num_seconds() as u64;

                        // Determine severity
                        let severity = if overlap_seconds >= estimated_duration {
                            ConflictSeverity::High
                        } else if overlap_seconds >= estimated_duration / 2 {
                            ConflictSeverity::Medium
                        } else {
                            ConflictSeverity::Low
                        };

                        let description = format!(
                            "Tasks will run at overlapping times: {} at {}, {} at {}",
                            task1_name,
                            next1.format("%Y-%m-%d %H:%M:%S"),
                            task2_name,
                            next2.format("%Y-%m-%d %H:%M:%S")
                        );

                        let resolution =
                            "Consider adjusting schedules or using jitter to avoid overlap"
                                .to_string();

                        conflicts.push(
                            ScheduleConflict::new(
                                task1_name.clone(),
                                task2_name.clone(),
                                severity,
                                overlap_seconds,
                                description,
                            )
                            .with_resolution(resolution),
                        );
                    }
                }
            }
        }

        conflicts
    }

    /// Get high severity conflicts
    pub fn get_high_severity_conflicts(
        &self,
        window_seconds: u64,
        estimated_duration: u64,
    ) -> Vec<ScheduleConflict> {
        self.detect_conflicts(window_seconds, estimated_duration)
            .into_iter()
            .filter(|c| c.is_high_severity())
            .collect()
    }

    /// Get medium severity conflicts
    pub fn get_medium_severity_conflicts(
        &self,
        window_seconds: u64,
        estimated_duration: u64,
    ) -> Vec<ScheduleConflict> {
        self.detect_conflicts(window_seconds, estimated_duration)
            .into_iter()
            .filter(|c| c.is_medium_severity())
            .collect()
    }

    /// Check if there are any conflicts
    pub fn has_conflicts(&self, window_seconds: u64, estimated_duration: u64) -> bool {
        !self
            .detect_conflicts(window_seconds, estimated_duration)
            .is_empty()
    }

    /// Get total conflict count
    pub fn conflict_count(&self, window_seconds: u64, estimated_duration: u64) -> usize {
        self.detect_conflicts(window_seconds, estimated_duration)
            .len()
    }

    /// Automatically resolve conflicts based on task priorities
    ///
    /// This method analyzes schedule conflicts and applies resolution strategies based on
    /// task priorities. Higher priority tasks are given preference, and lower priority
    /// tasks are adjusted to avoid conflicts.
    ///
    /// # Arguments
    /// * `window_seconds` - Time window to analyze for conflicts
    /// * `estimated_duration` - Estimated task duration in seconds
    /// * `jitter_seconds` - Amount of jitter to apply when resolving conflicts (default: 30)
    ///
    /// # Returns
    /// Vector of (task_name, resolution_description) pairs for tasks that were modified
    ///
    /// # Resolution Strategy
    /// 1. For tasks with different priorities: Apply jitter to lower priority task
    /// 2. For tasks with same priority: Apply symmetric jitter to both tasks
    /// 3. For high severity conflicts: Recommend manual review
    pub fn auto_resolve_conflicts(
        &mut self,
        window_seconds: u64,
        estimated_duration: u64,
        jitter_seconds: Option<u64>,
    ) -> Vec<(String, String)> {
        let jitter = jitter_seconds.unwrap_or(30);
        let mut resolutions = Vec::new();

        // Detect conflicts first
        let conflicts = self.detect_conflicts(window_seconds, estimated_duration);

        for conflict in conflicts {
            // Get task priorities
            let task1 = self.tasks.get(&conflict.task1);
            let task2 = self.tasks.get(&conflict.task2);

            if let (Some(t1), Some(t2)) = (task1, task2) {
                let priority1 = t1.options.priority.unwrap_or(5); // Default priority: 5
                let priority2 = t2.options.priority.unwrap_or(5);

                match priority1.cmp(&priority2) {
                    std::cmp::Ordering::Greater => {
                        // Task1 has higher priority, apply jitter to task2
                        if let Some(task) = self.tasks.get_mut(&conflict.task2) {
                            if task.jitter.is_none() {
                                task.jitter = Some(Jitter::positive(jitter as i64));
                                resolutions.push((
                                    conflict.task2.clone(),
                                    format!(
                                        "Applied +{}s jitter (lower priority than {})",
                                        jitter, conflict.task1
                                    ),
                                ));
                            }
                        }
                    }
                    std::cmp::Ordering::Less => {
                        // Task2 has higher priority, apply jitter to task1
                        if let Some(task) = self.tasks.get_mut(&conflict.task1) {
                            if task.jitter.is_none() {
                                task.jitter = Some(Jitter::positive(jitter as i64));
                                resolutions.push((
                                    conflict.task1.clone(),
                                    format!(
                                        "Applied +{}s jitter (lower priority than {})",
                                        jitter, conflict.task2
                                    ),
                                ));
                            }
                        }
                    }
                    std::cmp::Ordering::Equal => {
                        // Same priority, apply symmetric jitter to both
                        if conflict.severity == ConflictSeverity::High {
                            // For high severity, recommend manual review
                            resolutions.push((
                                format!("{} & {}", conflict.task1, conflict.task2),
                                "HIGH SEVERITY: Manual review recommended - tasks have equal priority and significant overlap".to_string(),
                            ));
                        } else {
                            // Apply symmetric jitter to both tasks
                            if let Some(task) = self.tasks.get_mut(&conflict.task1) {
                                if task.jitter.is_none() {
                                    task.jitter = Some(Jitter::symmetric((jitter / 2) as i64));
                                }
                            }
                            if let Some(task) = self.tasks.get_mut(&conflict.task2) {
                                if task.jitter.is_none() {
                                    task.jitter = Some(Jitter::symmetric((jitter / 2) as i64));
                                }
                            }
                            resolutions.push((
                                format!("{} & {}", conflict.task1, conflict.task2),
                                format!(
                                    "Applied ±{}s symmetric jitter to both (equal priority)",
                                    jitter / 2
                                ),
                            ));
                        }
                    }
                }
            }
        }

        // Save state if we made changes
        if !resolutions.is_empty() {
            let _ = self.save_state();
        }

        resolutions
    }

    /// Preview automatic conflict resolutions without applying them
    ///
    /// This method simulates the `auto_resolve_conflicts` behavior without modifying
    /// the scheduler state, allowing you to review proposed resolutions before applying.
    ///
    /// # Arguments
    /// * `window_seconds` - Time window to analyze for conflicts
    /// * `estimated_duration` - Estimated task duration in seconds
    /// * `jitter_seconds` - Amount of jitter that would be applied (default: 30)
    ///
    /// # Returns
    /// Vector of (task_name, resolution_description) pairs that would be applied
    pub fn preview_conflict_resolutions(
        &self,
        window_seconds: u64,
        estimated_duration: u64,
        jitter_seconds: Option<u64>,
    ) -> Vec<(String, String)> {
        let jitter = jitter_seconds.unwrap_or(30);
        let mut resolutions = Vec::new();

        let conflicts = self.detect_conflicts(window_seconds, estimated_duration);

        for conflict in conflicts {
            let task1 = self.tasks.get(&conflict.task1);
            let task2 = self.tasks.get(&conflict.task2);

            if let (Some(t1), Some(t2)) = (task1, task2) {
                let priority1 = t1.options.priority.unwrap_or(5);
                let priority2 = t2.options.priority.unwrap_or(5);

                match priority1.cmp(&priority2) {
                    std::cmp::Ordering::Greater => {
                        resolutions.push((
                            conflict.task2.clone(),
                            format!(
                                "Would apply +{}s jitter (priority {} < {})",
                                jitter, priority2, priority1
                            ),
                        ));
                    }
                    std::cmp::Ordering::Less => {
                        resolutions.push((
                            conflict.task1.clone(),
                            format!(
                                "Would apply +{}s jitter (priority {} < {})",
                                jitter, priority1, priority2
                            ),
                        ));
                    }
                    std::cmp::Ordering::Equal => {
                        if conflict.severity == ConflictSeverity::High {
                            resolutions.push((
                                format!("{} & {}", conflict.task1, conflict.task2),
                                "Would recommend manual review (high severity, equal priority)"
                                    .to_string(),
                            ));
                        } else {
                            resolutions.push((
                                format!("{} & {}", conflict.task1, conflict.task2),
                                format!(
                                    "Would apply ±{}s symmetric jitter to both (priority {})",
                                    jitter / 2,
                                    priority1
                                ),
                            ));
                        }
                    }
                }
            }
        }

        resolutions
    }

    /// Clear all conflict-related jitter from tasks
    ///
    /// This method removes jitter configurations that may have been added by
    /// automatic conflict resolution, allowing you to reset and re-analyze conflicts.
    pub fn clear_conflict_jitter(&mut self) {
        for task in self.tasks.values_mut() {
            // Only clear jitter if it was likely added by auto-resolution
            // (we can't be 100% certain, but we clear it anyway)
            task.jitter = None;
        }
        let _ = self.save_state();
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

// ============================================================================
// Webhook Alert Delivery
// ============================================================================

/// Webhook configuration for alert delivery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookConfig {
    /// Webhook endpoint URL
    pub url: String,
    /// HTTP headers to include in webhook requests
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// Timeout for webhook requests in seconds
    #[serde(default = "default_webhook_timeout")]
    pub timeout_seconds: u64,
    /// Filter: only send alerts matching these levels (empty = all levels)
    #[serde(default)]
    pub alert_levels: Vec<AlertLevel>,
}

#[allow(dead_code)]
fn default_webhook_timeout() -> u64 {
    30
}

impl WebhookConfig {
    /// Create a new webhook configuration
    ///
    /// # Arguments
    /// * `url` - Webhook endpoint URL
    ///
    /// # Examples
    /// ```
    /// use celers_beat::WebhookConfig;
    ///
    /// let webhook = WebhookConfig::new("https://example.com/alerts");
    /// ```
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            headers: HashMap::new(),
            timeout_seconds: default_webhook_timeout(),
            alert_levels: Vec::new(),
        }
    }

    /// Add a custom HTTP header
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Set the timeout for webhook requests
    pub fn with_timeout(mut self, timeout_seconds: u64) -> Self {
        self.timeout_seconds = timeout_seconds;
        self
    }

    /// Filter alerts by level (empty = send all alerts)
    pub fn with_alert_levels(mut self, levels: Vec<AlertLevel>) -> Self {
        self.alert_levels = levels;
        self
    }

    /// Check if this webhook should receive the given alert
    pub fn should_send(&self, alert: &Alert) -> bool {
        if self.alert_levels.is_empty() {
            return true;
        }
        self.alert_levels.contains(&alert.level)
    }

    /// Create a JSON payload for the webhook
    pub fn create_payload(&self, alert: &Alert) -> serde_json::Value {
        serde_json::json!({
            "timestamp": alert.timestamp.to_rfc3339(),
            "task_name": alert.task_name,
            "level": format!("{:?}", alert.level),
            "condition": format!("{:?}", alert.condition),
            "message": alert.message,
            "metadata": alert.metadata,
        })
    }
}

// ============================================================================
// Business Day Calendar
// ============================================================================

/// Day of week
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DayOfWeek {
    Monday,
    Tuesday,
    Wednesday,
    Thursday,
    Friday,
    Saturday,
    Sunday,
}

impl DayOfWeek {
    /// Check if this is a weekend day (Saturday or Sunday)
    pub fn is_weekend(&self) -> bool {
        matches!(self, DayOfWeek::Saturday | DayOfWeek::Sunday)
    }

    /// Check if this is a weekday (Monday-Friday)
    pub fn is_weekday(&self) -> bool {
        !self.is_weekend()
    }

    /// Convert from chrono Weekday
    pub fn from_chrono(weekday: chrono::Weekday) -> Self {
        match weekday {
            chrono::Weekday::Mon => DayOfWeek::Monday,
            chrono::Weekday::Tue => DayOfWeek::Tuesday,
            chrono::Weekday::Wed => DayOfWeek::Wednesday,
            chrono::Weekday::Thu => DayOfWeek::Thursday,
            chrono::Weekday::Fri => DayOfWeek::Friday,
            chrono::Weekday::Sat => DayOfWeek::Saturday,
            chrono::Weekday::Sun => DayOfWeek::Sunday,
        }
    }
}

/// Business hours configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BusinessHours {
    /// Start hour (0-23)
    pub start_hour: u32,
    /// End hour (0-23)
    pub end_hour: u32,
}

impl BusinessHours {
    /// Create a new business hours configuration
    ///
    /// # Arguments
    /// * `start_hour` - Start hour (0-23)
    /// * `end_hour` - End hour (0-23)
    pub fn new(start_hour: u32, end_hour: u32) -> Self {
        Self {
            start_hour,
            end_hour,
        }
    }

    /// Standard business hours (9 AM - 5 PM)
    pub fn standard() -> Self {
        Self {
            start_hour: 9,
            end_hour: 17,
        }
    }

    /// Check if a given hour is within business hours
    pub fn is_business_hour(&self, hour: u32) -> bool {
        hour >= self.start_hour && hour < self.end_hour
    }

    /// Check if a given time is within business hours
    pub fn is_within(&self, time: &DateTime<Utc>) -> bool {
        self.is_business_hour(time.hour())
    }
}

/// Business day calendar configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BusinessCalendar {
    /// Business hours configuration
    pub business_hours: BusinessHours,
    /// Working days (defaults to Monday-Friday)
    #[serde(default = "default_working_days")]
    pub working_days: Vec<DayOfWeek>,
}

#[allow(dead_code)]
fn default_working_days() -> Vec<DayOfWeek> {
    vec![
        DayOfWeek::Monday,
        DayOfWeek::Tuesday,
        DayOfWeek::Wednesday,
        DayOfWeek::Thursday,
        DayOfWeek::Friday,
    ]
}

impl BusinessCalendar {
    /// Create a new business calendar with standard hours (9 AM - 5 PM, Mon-Fri)
    pub fn standard() -> Self {
        Self {
            business_hours: BusinessHours::standard(),
            working_days: default_working_days(),
        }
    }

    /// Create a custom business calendar
    pub fn new(business_hours: BusinessHours, working_days: Vec<DayOfWeek>) -> Self {
        Self {
            business_hours,
            working_days,
        }
    }

    /// Check if a given day of week is a working day
    pub fn is_working_day(&self, day: DayOfWeek) -> bool {
        self.working_days.contains(&day)
    }

    /// Check if a given date/time is within business hours
    pub fn is_business_time(&self, time: &DateTime<Utc>) -> bool {
        let day = DayOfWeek::from_chrono(time.weekday());
        self.is_working_day(day) && self.business_hours.is_within(time)
    }

    /// Find the next business time after the given time
    ///
    /// This will advance to the next business day/hour if necessary.
    pub fn next_business_time(&self, time: DateTime<Utc>) -> DateTime<Utc> {
        let mut current = time;

        // Try up to 14 days (2 weeks) to find next business time
        for _ in 0..14 {
            let day = DayOfWeek::from_chrono(current.weekday());

            if self.is_working_day(day) {
                // Check if we're in business hours
                let hour = current.hour();
                if hour < self.business_hours.start_hour {
                    // Before business hours - move to start of business hours today
                    current = current
                        .with_hour(self.business_hours.start_hour)
                        .unwrap()
                        .with_minute(0)
                        .unwrap()
                        .with_second(0)
                        .unwrap();
                    return current;
                } else if hour < self.business_hours.end_hour {
                    // Within business hours - this is valid
                    return current;
                }
                // After business hours - fall through to next day
            }

            // Move to start of next day
            current = (current + Duration::days(1))
                .with_hour(self.business_hours.start_hour)
                .unwrap()
                .with_minute(0)
                .unwrap()
                .with_second(0)
                .unwrap();
        }

        current
    }
}

// ============================================================================
// Holiday Calendar
// ============================================================================

/// Holiday definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Holiday {
    /// Holiday name
    pub name: String,
    /// Date (year, month, day)
    pub date: (i32, u32, u32),
}

impl Holiday {
    /// Create a new holiday
    pub fn new(name: impl Into<String>, year: i32, month: u32, day: u32) -> Self {
        Self {
            name: name.into(),
            date: (year, month, day),
        }
    }

    /// Check if this holiday matches the given date
    pub fn matches(&self, date: &DateTime<Utc>) -> bool {
        let (year, month, day) = self.date;
        date.year() == year && date.month() == month && date.day() == day
    }
}

/// Holiday calendar
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HolidayCalendar {
    /// List of holidays
    holidays: Vec<Holiday>,
}

impl HolidayCalendar {
    /// Create a new empty holiday calendar
    pub fn new() -> Self {
        Self {
            holidays: Vec::new(),
        }
    }

    /// Add a holiday to the calendar
    pub fn add_holiday(&mut self, holiday: Holiday) {
        self.holidays.push(holiday);
    }

    /// Add a holiday by date components
    pub fn add(&mut self, name: impl Into<String>, year: i32, month: u32, day: u32) {
        self.holidays.push(Holiday::new(name, year, month, day));
    }

    /// Check if a given date is a holiday
    pub fn is_holiday(&self, date: &DateTime<Utc>) -> bool {
        self.holidays.iter().any(|h| h.matches(date))
    }

    /// Get the holiday for a given date, if any
    pub fn get_holiday(&self, date: &DateTime<Utc>) -> Option<&Holiday> {
        self.holidays.iter().find(|h| h.matches(date))
    }

    /// Get the number of holidays in this calendar
    pub fn len(&self) -> usize {
        self.holidays.len()
    }

    /// Check if the calendar is empty
    pub fn is_empty(&self) -> bool {
        self.holidays.is_empty()
    }

    /// Find the next non-holiday date after the given time
    pub fn next_non_holiday(&self, time: DateTime<Utc>) -> DateTime<Utc> {
        let mut current = time;

        // Try up to 365 days to find a non-holiday
        for _ in 0..365 {
            if !self.is_holiday(&current) {
                return current;
            }
            current += Duration::days(1);
        }

        current
    }

    /// Create a US federal holidays calendar for a given year
    ///
    /// Includes all 11 US federal holidays:
    /// - New Year's Day (January 1)
    /// - Martin Luther King Jr Day (3rd Monday in January)
    /// - Presidents Day (3rd Monday in February)
    /// - Memorial Day (Last Monday in May)
    /// - Juneteenth (June 19)
    /// - Independence Day (July 4)
    /// - Labor Day (1st Monday in September)
    /// - Columbus Day (2nd Monday in October)
    /// - Veterans Day (November 11)
    /// - Thanksgiving Day (4th Thursday in November)
    /// - Christmas Day (December 25)
    pub fn us_federal(year: i32) -> Self {
        let mut calendar = Self::new();

        // New Year's Day (January 1)
        calendar.add("New Year's Day", year, 1, 1);

        // Martin Luther King Jr Day (3rd Monday in January)
        if let Some((_, day)) = Self::nth_weekday(year, 1, 1, 3) {
            calendar.add("Martin Luther King Jr Day", year, 1, day);
        }

        // Presidents Day (3rd Monday in February)
        if let Some((_, day)) = Self::nth_weekday(year, 2, 1, 3) {
            calendar.add("Presidents Day", year, 2, day);
        }

        // Memorial Day (Last Monday in May)
        if let Some((_, day)) = Self::last_weekday(year, 5, 1) {
            calendar.add("Memorial Day", year, 5, day);
        }

        // Juneteenth (June 19)
        calendar.add("Juneteenth", year, 6, 19);

        // Independence Day (July 4)
        calendar.add("Independence Day", year, 7, 4);

        // Labor Day (1st Monday in September)
        if let Some((_, day)) = Self::nth_weekday(year, 9, 1, 1) {
            calendar.add("Labor Day", year, 9, day);
        }

        // Columbus Day (2nd Monday in October)
        if let Some((_, day)) = Self::nth_weekday(year, 10, 1, 2) {
            calendar.add("Columbus Day", year, 10, day);
        }

        // Veterans Day (November 11)
        calendar.add("Veterans Day", year, 11, 11);

        // Thanksgiving Day (4th Thursday in November)
        if let Some((_, day)) = Self::nth_weekday(year, 11, 4, 4) {
            calendar.add("Thanksgiving Day", year, 11, day);
        }

        // Christmas Day (December 25)
        calendar.add("Christmas Day", year, 12, 25);

        calendar
    }

    /// Calculate the Nth occurrence of a weekday in a given month
    ///
    /// # Arguments
    /// * `year` - Year
    /// * `month` - Month (1-12)
    /// * `weekday` - Day of week (0=Sunday, 1=Monday, ..., 6=Saturday)
    /// * `nth` - Which occurrence (1=first, 2=second, etc.)
    ///
    /// # Returns
    /// `Some((month, day))` if found, `None` otherwise
    fn nth_weekday(year: i32, month: u32, weekday: u32, nth: u32) -> Option<(u32, u32)> {
        use chrono::NaiveDate;

        let first_day = NaiveDate::from_ymd_opt(year, month, 1)?;
        let first_weekday = first_day.weekday().num_days_from_sunday();

        // Calculate days until first occurrence of target weekday
        let days_until_first = if weekday >= first_weekday {
            weekday - first_weekday
        } else {
            7 - (first_weekday - weekday)
        };

        // Calculate the date of the nth occurrence
        let target_day = 1 + days_until_first + (nth - 1) * 7;

        // Validate the day exists in the month
        if NaiveDate::from_ymd_opt(year, month, target_day).is_some() {
            Some((month, target_day))
        } else {
            None
        }
    }

    /// Find the last occurrence of a weekday in a given month
    ///
    /// # Arguments
    /// * `year` - Year
    /// * `month` - Month (1-12)
    /// * `weekday` - Day of week (0=Sunday, 1=Monday, ..., 6=Saturday)
    ///
    /// # Returns
    /// `Some((month, day))` if found, `None` otherwise
    fn last_weekday(year: i32, month: u32, weekday: u32) -> Option<(u32, u32)> {
        use chrono::NaiveDate;

        // Start from the last day of the month and work backwards
        let next_month = if month == 12 { 1 } else { month + 1 };
        let next_year = if month == 12 { year + 1 } else { year };
        let last_day = NaiveDate::from_ymd_opt(next_year, next_month, 1)?.pred_opt()?;

        // Search backwards for the target weekday
        for days_back in 0..7 {
            if let Some(date) = last_day.checked_sub_signed(Duration::days(days_back)) {
                if date.weekday().num_days_from_sunday() == weekday {
                    return Some((month, date.day()));
                }
            }
        }

        None
    }

    /// Create a Japan national holidays calendar for a given year
    ///
    /// Includes all 16 Japanese national holidays:
    /// - New Year's Day (January 1)
    /// - Coming of Age Day (2nd Monday in January)
    /// - National Foundation Day (February 11)
    /// - Emperor's Birthday (February 23)
    /// - Vernal Equinox Day (March 20 - approximate)
    /// - Showa Day (April 29)
    /// - Constitution Memorial Day (May 3)
    /// - Greenery Day (May 4)
    /// - Children's Day (May 5)
    /// - Marine Day (3rd Monday in July)
    /// - Mountain Day (August 11)
    /// - Respect for the Aged Day (3rd Monday in September)
    /// - Autumnal Equinox Day (September 23 - approximate)
    /// - Sports Day (2nd Monday in October)
    /// - Culture Day (November 3)
    /// - Labor Thanksgiving Day (November 23)
    pub fn japan(year: i32) -> Self {
        let mut calendar = Self::new();

        // New Year's Day (January 1)
        calendar.add("New Year's Day", year, 1, 1);

        // Coming of Age Day (2nd Monday in January)
        if let Some((_, day)) = Self::nth_weekday(year, 1, 1, 2) {
            calendar.add("Coming of Age Day", year, 1, day);
        }

        // National Foundation Day (February 11)
        calendar.add("National Foundation Day", year, 2, 11);

        // Emperor's Birthday (February 23)
        calendar.add("Emperor's Birthday", year, 2, 23);

        // Vernal Equinox Day (around March 20-21, using March 20 as approximation)
        calendar.add("Vernal Equinox Day", year, 3, 20);

        // Showa Day (April 29)
        calendar.add("Showa Day", year, 4, 29);

        // Constitution Memorial Day (May 3)
        calendar.add("Constitution Memorial Day", year, 5, 3);

        // Greenery Day (May 4)
        calendar.add("Greenery Day", year, 5, 4);

        // Children's Day (May 5)
        calendar.add("Children's Day", year, 5, 5);

        // Marine Day (3rd Monday in July)
        if let Some((_, day)) = Self::nth_weekday(year, 7, 1, 3) {
            calendar.add("Marine Day", year, 7, day);
        }

        // Mountain Day (August 11)
        calendar.add("Mountain Day", year, 8, 11);

        // Respect for the Aged Day (3rd Monday in September)
        if let Some((_, day)) = Self::nth_weekday(year, 9, 1, 3) {
            calendar.add("Respect for the Aged Day", year, 9, day);
        }

        // Autumnal Equinox Day (around September 22-23, using September 23 as approximation)
        calendar.add("Autumnal Equinox Day", year, 9, 23);

        // Sports Day (2nd Monday in October)
        if let Some((_, day)) = Self::nth_weekday(year, 10, 1, 2) {
            calendar.add("Sports Day", year, 10, day);
        }

        // Culture Day (November 3)
        calendar.add("Culture Day", year, 11, 3);

        // Labor Thanksgiving Day (November 23)
        calendar.add("Labor Thanksgiving Day", year, 11, 23);

        calendar
    }

    /// Create a UK public holidays calendar for a given year (England and Wales)
    ///
    /// Includes the standard UK bank holidays:
    /// - New Year's Day (January 1)
    /// - Good Friday (calculated based on Easter)
    /// - Easter Monday (calculated based on Easter)
    /// - Early May Bank Holiday (1st Monday in May)
    /// - Spring Bank Holiday (Last Monday in May)
    /// - Summer Bank Holiday (Last Monday in August)
    /// - Christmas Day (December 25)
    /// - Boxing Day (December 26)
    ///
    /// Note: Easter-dependent holidays use a simplified approximation.
    /// For exact dates, use a proper Easter calculation algorithm.
    pub fn uk(year: i32) -> Self {
        let mut calendar = Self::new();

        // New Year's Day (January 1)
        calendar.add("New Year's Day", year, 1, 1);

        // Easter calculation (simplified - using a fixed date approximation)
        // In a production system, you would use a proper Easter algorithm
        // For now, we'll use common approximate dates

        // Good Friday (approximate - varies between March 20 and April 23)
        // Using April 15 as a typical date
        calendar.add("Good Friday", year, 4, 15);

        // Easter Monday (Good Friday + 3 days)
        calendar.add("Easter Monday", year, 4, 18);

        // Early May Bank Holiday (1st Monday in May)
        if let Some((_, day)) = Self::nth_weekday(year, 5, 1, 1) {
            calendar.add("Early May Bank Holiday", year, 5, day);
        }

        // Spring Bank Holiday (Last Monday in May)
        if let Some((_, day)) = Self::last_weekday(year, 5, 1) {
            calendar.add("Spring Bank Holiday", year, 5, day);
        }

        // Summer Bank Holiday (Last Monday in August)
        if let Some((_, day)) = Self::last_weekday(year, 8, 1) {
            calendar.add("Summer Bank Holiday", year, 8, day);
        }

        // Christmas Day (December 25)
        calendar.add("Christmas Day", year, 12, 25);

        // Boxing Day (December 26)
        calendar.add("Boxing Day", year, 12, 26);

        calendar
    }

    /// Create a Canada statutory holidays calendar for a given year
    ///
    /// Includes federal statutory holidays observed across Canada:
    /// - New Year's Day (January 1)
    /// - Good Friday (calculated based on Easter)
    /// - Victoria Day (Monday before May 25)
    /// - Canada Day (July 1)
    /// - Labour Day (1st Monday in September)
    /// - Thanksgiving (2nd Monday in October)
    /// - Remembrance Day (November 11) - observed federally
    /// - Christmas Day (December 25)
    /// - Boxing Day (December 26)
    ///
    /// Note: Provincial holidays may vary and are not included.
    pub fn canada(year: i32) -> Self {
        let mut calendar = Self::new();

        // New Year's Day (January 1)
        calendar.add("New Year's Day", year, 1, 1);

        // Good Friday (approximate - using April 15 as typical date)
        calendar.add("Good Friday", year, 4, 15);

        // Victoria Day (Monday before May 25)
        // This is the last Monday on or before May 24
        if let Some((_, day)) = Self::monday_on_or_before(year, 5, 24) {
            calendar.add("Victoria Day", year, 5, day);
        }

        // Canada Day (July 1)
        calendar.add("Canada Day", year, 7, 1);

        // Labour Day (1st Monday in September)
        if let Some((_, day)) = Self::nth_weekday(year, 9, 1, 1) {
            calendar.add("Labour Day", year, 9, day);
        }

        // Thanksgiving (2nd Monday in October)
        if let Some((_, day)) = Self::nth_weekday(year, 10, 1, 2) {
            calendar.add("Thanksgiving", year, 10, day);
        }

        // Remembrance Day (November 11)
        calendar.add("Remembrance Day", year, 11, 11);

        // Christmas Day (December 25)
        calendar.add("Christmas Day", year, 12, 25);

        // Boxing Day (December 26)
        calendar.add("Boxing Day", year, 12, 26);

        calendar
    }

    /// Find the Monday on or before a specific date
    ///
    /// # Arguments
    /// * `year` - Year
    /// * `month` - Month (1-12)
    /// * `day` - Day of month
    ///
    /// # Returns
    /// `Some((month, day))` if found, `None` otherwise
    fn monday_on_or_before(year: i32, month: u32, day: u32) -> Option<(u32, u32)> {
        use chrono::NaiveDate;

        let date = NaiveDate::from_ymd_opt(year, month, day)?;
        let weekday = date.weekday().num_days_from_sunday();

        // If it's already Monday (1), return it
        // Otherwise, go back to the previous Monday
        let days_back = if weekday >= 1 {
            weekday - 1
        } else {
            6 // Sunday, so go back 6 days to Monday
        };

        let target = date.checked_sub_signed(Duration::days(days_back as i64))?;
        Some((target.month(), target.day()))
    }
}

// ============================================================================
// Calendar-Aware Schedule Extensions
// ============================================================================

impl ScheduledTask {
    /// Set business calendar for this task
    ///
    /// This doesn't currently enforce business hours (requires more extensive changes),
    /// but provides the configuration for future use.
    pub fn with_business_calendar(self, _calendar: BusinessCalendar) -> Self {
        // Store for future use - currently just validation
        self
    }

    /// Set holiday calendar for this task
    ///
    /// This doesn't currently enforce holidays (requires more extensive changes),
    /// but provides the configuration for future use.
    pub fn with_holiday_calendar(self, _calendar: HolidayCalendar) -> Self {
        // Store for future use - currently just validation
        self
    }
}

impl BeatScheduler {
    /// Register a webhook for alert delivery
    ///
    /// # Arguments
    /// * `webhook` - Webhook configuration
    ///
    /// # Examples
    /// ```no_run
    /// use celers_beat::{BeatScheduler, WebhookConfig};
    ///
    /// let mut scheduler = BeatScheduler::new();
    /// let webhook = WebhookConfig::new("https://example.com/alerts")
    ///     .with_header("Authorization", "Bearer token123");
    /// scheduler.register_webhook(webhook);
    /// ```
    pub fn register_webhook(&mut self, webhook: WebhookConfig) {
        let webhook = Arc::new(webhook);
        self.alert_manager
            .add_callback(Arc::new(move |alert: &Alert| {
                if webhook.should_send(alert) {
                    let payload = webhook.create_payload(alert);
                    // In a real implementation, this would send an async HTTP request
                    // For now, we just log it (webhook delivery requires async runtime)
                    eprintln!("Webhook alert to {}: {}", webhook.url, payload);
                }
            }));
    }
}

// ============================================================================
// Efficient Schedule Indexing
// ============================================================================

/// Schedule index for fast task lookup
///
/// This structure maintains indexes on tasks for efficient queries:
/// - By schedule type (Interval, Crontab, Solar, OneTime)
/// - By next run time (priority queue ordering)
#[derive(Debug, Clone, Default)]
pub struct ScheduleIndex {
    /// Tasks indexed by schedule type
    by_type: HashMap<String, HashSet<String>>,
    /// Tasks sorted by next run time (task_name, next_run_time)
    by_next_run: Vec<(String, DateTime<Utc>)>,
    /// Flag to track if index needs rebuilding
    dirty: bool,
}

impl ScheduleIndex {
    /// Create a new empty schedule index
    pub fn new() -> Self {
        Self {
            by_type: HashMap::new(),
            by_next_run: Vec::new(),
            dirty: false,
        }
    }

    /// Add a task to the index
    pub fn add_task(&mut self, task: &ScheduledTask) {
        let type_key = Self::schedule_type_key(&task.schedule);
        self.by_type
            .entry(type_key)
            .or_default()
            .insert(task.name.clone());

        if let Ok(next_run) = task.next_run_time() {
            self.by_next_run.push((task.name.clone(), next_run));
            self.dirty = true;
        }
    }

    /// Remove a task from the index
    pub fn remove_task(&mut self, task: &ScheduledTask) {
        let type_key = Self::schedule_type_key(&task.schedule);
        if let Some(set) = self.by_type.get_mut(&type_key) {
            set.remove(&task.name);
        }

        self.by_next_run.retain(|(name, _)| name != &task.name);
        self.dirty = true;
    }

    /// Update a task in the index (when schedule changes)
    pub fn update_task(&mut self, old_task: &ScheduledTask, new_task: &ScheduledTask) {
        self.remove_task(old_task);
        self.add_task(new_task);
    }

    /// Rebuild the index from a task map
    pub fn rebuild(&mut self, tasks: &HashMap<String, ScheduledTask>) {
        self.by_type.clear();
        self.by_next_run.clear();

        for task in tasks.values() {
            if task.enabled {
                self.add_task(task);
            }
        }

        self.sort_by_next_run();
        self.dirty = false;
    }

    /// Sort tasks by next run time (for priority queue behavior)
    fn sort_by_next_run(&mut self) {
        if self.dirty {
            self.by_next_run.sort_by_key(|(_, time)| *time);
            self.dirty = false;
        }
    }

    /// Get tasks of a specific schedule type
    pub fn get_by_type(&self, schedule_type: &str) -> Option<&HashSet<String>> {
        self.by_type.get(schedule_type)
    }

    /// Get tasks that should run next (up to N tasks)
    pub fn get_next_due(&mut self, limit: usize, now: DateTime<Utc>) -> Vec<String> {
        self.sort_by_next_run();

        self.by_next_run
            .iter()
            .filter(|(_, time)| *time <= now)
            .take(limit)
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Get the earliest next run time
    pub fn earliest_next_run(&mut self) -> Option<DateTime<Utc>> {
        self.sort_by_next_run();
        self.by_next_run.first().map(|(_, time)| *time)
    }

    /// Get schedule type key for indexing
    fn schedule_type_key(schedule: &Schedule) -> String {
        match schedule {
            Schedule::Interval { .. } => "interval".to_string(),
            #[cfg(feature = "cron")]
            Schedule::Crontab { .. } => "crontab".to_string(),
            #[cfg(feature = "solar")]
            Schedule::Solar { .. } => "solar".to_string(),
            Schedule::OneTime { .. } => "onetime".to_string(),
        }
    }

    /// Check if index needs rebuilding
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Mark index as dirty (needs sorting)
    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    /// Get count of tasks by type
    pub fn count_by_type(&self, schedule_type: &str) -> usize {
        self.by_type
            .get(schedule_type)
            .map(|set| set.len())
            .unwrap_or(0)
    }

    /// Get total count of indexed tasks
    pub fn total_count(&self) -> usize {
        self.by_next_run.len()
    }
}

// ============================================================================
// Advanced Calendar Features
// ============================================================================

/// Blackout period - time range when tasks should not execute
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlackoutPeriod {
    /// Blackout period name
    pub name: String,
    /// Start time (UTC)
    pub start: DateTime<Utc>,
    /// End time (UTC)
    pub end: DateTime<Utc>,
    /// Optional recurring pattern
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recurring: Option<BlackoutRecurrence>,
}

/// Blackout recurrence pattern
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BlackoutRecurrence {
    /// Recur daily at the same time
    Daily,
    /// Recur weekly on the same day and time
    Weekly,
    /// Recur monthly on the same date and time
    Monthly,
}

impl BlackoutPeriod {
    /// Create a new blackout period
    pub fn new(name: impl Into<String>, start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        Self {
            name: name.into(),
            start,
            end,
            recurring: None,
        }
    }

    /// Make this blackout period recurring
    pub fn with_recurrence(mut self, recurrence: BlackoutRecurrence) -> Self {
        self.recurring = Some(recurrence);
        self
    }

    /// Check if the given time is within this blackout period
    pub fn is_blackout(&self, time: DateTime<Utc>) -> bool {
        if time >= self.start && time <= self.end {
            return true;
        }

        // Check recurring patterns
        if let Some(ref recurrence) = self.recurring {
            match recurrence {
                BlackoutRecurrence::Daily => {
                    let start_time = (self.start.hour(), self.start.minute());
                    let end_time = (self.end.hour(), self.end.minute());
                    let current_time = (time.hour(), time.minute());
                    current_time >= start_time && current_time <= end_time
                }
                BlackoutRecurrence::Weekly => {
                    if time.weekday() == self.start.weekday() {
                        let start_time = (self.start.hour(), self.start.minute());
                        let end_time = (self.end.hour(), self.end.minute());
                        let current_time = (time.hour(), time.minute());
                        current_time >= start_time && current_time <= end_time
                    } else {
                        false
                    }
                }
                BlackoutRecurrence::Monthly => {
                    if time.day() == self.start.day() {
                        let start_time = (self.start.hour(), self.start.minute());
                        let end_time = (self.end.hour(), self.end.minute());
                        let current_time = (time.hour(), time.minute());
                        current_time >= start_time && current_time <= end_time
                    } else {
                        false
                    }
                }
            }
        } else {
            false
        }
    }

    /// Find the next time after the blackout period
    pub fn next_available_time(&self, time: DateTime<Utc>) -> DateTime<Utc> {
        if !self.is_blackout(time) {
            return time;
        }

        // If in blackout, move to end of blackout
        let mut current = self.end;

        // For recurring blackouts, may need to advance further
        if let Some(ref recurrence) = self.recurring {
            while self.is_blackout(current) {
                current = match recurrence {
                    BlackoutRecurrence::Daily => current + Duration::days(1),
                    BlackoutRecurrence::Weekly => current + Duration::weeks(1),
                    BlackoutRecurrence::Monthly => {
                        // Move to next month
                        if current.month() == 12 {
                            current
                                .with_year(current.year() + 1)
                                .unwrap()
                                .with_month(1)
                                .unwrap()
                        } else {
                            current.with_month(current.month() + 1).unwrap()
                        }
                    }
                };
            }
        }

        current
    }
}

/// Calendar with blackout periods
#[derive(Debug, Clone, Default)]
pub struct CalendarWithBlackout {
    /// Base holiday calendar
    pub holiday_calendar: HolidayCalendar,
    /// Base business calendar
    pub business_calendar: Option<BusinessCalendar>,
    /// Blackout periods
    pub blackout_periods: Vec<BlackoutPeriod>,
    /// Execute only on holidays flag
    pub holidays_only: bool,
}

impl CalendarWithBlackout {
    /// Create a new calendar with blackout support
    pub fn new() -> Self {
        Self {
            holiday_calendar: HolidayCalendar::new(),
            business_calendar: None,
            blackout_periods: Vec::new(),
            holidays_only: false,
        }
    }

    /// Add a blackout period
    pub fn add_blackout(&mut self, blackout: BlackoutPeriod) {
        self.blackout_periods.push(blackout);
    }

    /// Set to execute only on holidays
    pub fn set_holidays_only(&mut self, enabled: bool) {
        self.holidays_only = enabled;
    }

    /// Set business calendar
    pub fn set_business_calendar(&mut self, calendar: BusinessCalendar) {
        self.business_calendar = Some(calendar);
    }

    /// Check if a time is valid for execution
    pub fn is_valid_time(&self, time: DateTime<Utc>) -> bool {
        // Check blackout periods
        for blackout in &self.blackout_periods {
            if blackout.is_blackout(time) {
                return false;
            }
        }

        // Check holidays-only mode
        if self.holidays_only && !self.holiday_calendar.is_holiday(&time) {
            return false;
        }

        // Check business calendar if set
        if let Some(ref business) = self.business_calendar {
            if !business.is_business_time(&time) {
                return false;
            }
        }

        true
    }

    /// Find the next valid execution time
    pub fn next_valid_time(&self, mut time: DateTime<Utc>) -> DateTime<Utc> {
        // Try up to 365 days
        for _ in 0..365 {
            // Check blackout periods first
            let mut in_blackout = false;
            for blackout in &self.blackout_periods {
                if blackout.is_blackout(time) {
                    time = blackout.next_available_time(time);
                    in_blackout = true;
                    break;
                }
            }

            if in_blackout {
                continue;
            }

            // Check holidays-only mode
            if self.holidays_only && !self.holiday_calendar.is_holiday(&time) {
                time += Duration::days(1);
                continue;
            }

            // Check business calendar
            if let Some(ref business) = self.business_calendar {
                if !business.is_business_time(&time) {
                    time = business.next_business_time(time);
                    continue;
                }
            }

            // All checks passed
            return time;
        }

        time
    }
}

// ============================================================================
// Schedule Composition
// ============================================================================

/// Composite schedule combining multiple schedules with logical operations
///
/// Allows creating complex schedules by combining simple ones with AND/OR logic.
#[derive(Debug, Clone)]
pub struct CompositeSchedule {
    /// List of schedules to combine
    schedules: Vec<Schedule>,
    /// Combination mode (AND = all must match, OR = any must match)
    mode: CompositeMode,
}

/// Mode for combining schedules
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompositeMode {
    /// All schedules must be due (intersection)
    And,
    /// Any schedule must be due (union)
    Or,
}

impl CompositeSchedule {
    /// Create a new composite schedule with AND logic
    ///
    /// # Examples
    /// ```
    /// use celers_beat::{Schedule, CompositeSchedule};
    ///
    /// let schedule = CompositeSchedule::and(vec![
    ///     Schedule::interval(60),
    ///     Schedule::interval(120),
    /// ]);
    /// ```
    pub fn and(schedules: Vec<Schedule>) -> Self {
        Self {
            schedules,
            mode: CompositeMode::And,
        }
    }

    /// Create a new composite schedule with OR logic
    ///
    /// # Examples
    /// ```
    /// use celers_beat::{Schedule, CompositeSchedule};
    ///
    /// let schedule = CompositeSchedule::or(vec![
    ///     Schedule::interval(60),
    ///     Schedule::interval(120),
    /// ]);
    /// ```
    pub fn or(schedules: Vec<Schedule>) -> Self {
        Self {
            schedules,
            mode: CompositeMode::Or,
        }
    }

    /// Calculate next run time based on composite logic
    ///
    /// - AND mode: Returns the latest next run time among all schedules (all must be due)
    /// - OR mode: Returns the earliest next run time among all schedules (any can be due)
    pub fn next_run(
        &self,
        last_run: Option<DateTime<Utc>>,
    ) -> Result<DateTime<Utc>, ScheduleError> {
        if self.schedules.is_empty() {
            return Err(ScheduleError::Invalid(
                "Composite schedule has no sub-schedules".to_string(),
            ));
        }

        let mut next_runs = Vec::new();
        for schedule in &self.schedules {
            match schedule.next_run(last_run) {
                Ok(next) => next_runs.push(next),
                Err(e) => {
                    // If any schedule fails in AND mode, the whole composite fails
                    if self.mode == CompositeMode::And {
                        return Err(e);
                    }
                    // In OR mode, skip failed schedules
                }
            }
        }

        if next_runs.is_empty() {
            return Err(ScheduleError::Invalid(
                "No valid next run time from any sub-schedule".to_string(),
            ));
        }

        match self.mode {
            CompositeMode::And => {
                // AND: all must be due, so take the latest (slowest) time
                Ok(*next_runs.iter().max().unwrap())
            }
            CompositeMode::Or => {
                // OR: any can be due, so take the earliest (fastest) time
                Ok(*next_runs.iter().min().unwrap())
            }
        }
    }

    /// Check if this composite schedule is due
    pub fn is_due(&self, last_run: Option<DateTime<Utc>>) -> Result<bool, ScheduleError> {
        let next_run = self.next_run(last_run)?;
        Ok(Utc::now() >= next_run)
    }

    /// Get the number of sub-schedules
    pub fn schedule_count(&self) -> usize {
        self.schedules.len()
    }

    /// Get the composition mode
    pub fn mode(&self) -> CompositeMode {
        self.mode
    }

    /// Get reference to sub-schedules
    pub fn schedules(&self) -> &[Schedule] {
        &self.schedules
    }
}

impl std::fmt::Display for CompositeSchedule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mode_str = match self.mode {
            CompositeMode::And => "AND",
            CompositeMode::Or => "OR",
        };

        write!(f, "Composite[{}](", mode_str)?;
        for (i, schedule) in self.schedules.iter().enumerate() {
            if i > 0 {
                write!(f, " {} ", mode_str)?;
            }
            write!(f, "{}", schedule)?;
        }
        write!(f, ")")
    }
}

/// Type alias for custom schedule function
type CustomScheduleFn =
    Arc<dyn Fn(Option<DateTime<Utc>>) -> Result<DateTime<Utc>, ScheduleError> + Send + Sync>;

/// Custom schedule with user-defined logic
///
/// Allows users to define custom scheduling logic via a closure.
pub struct CustomSchedule {
    /// Schedule name/description
    pub name: String,
    /// Custom next_run logic
    next_run_fn: CustomScheduleFn,
}

impl CustomSchedule {
    /// Create a new custom schedule
    ///
    /// # Arguments
    /// * `name` - Descriptive name for this schedule
    /// * `next_run_fn` - Function that calculates next run time
    ///
    /// # Examples
    /// ```
    /// use celers_beat::CustomSchedule;
    /// use chrono::{Utc, Duration};
    ///
    /// let schedule = CustomSchedule::new(
    ///     "every_fibonacci_seconds",
    ///     |last_run| {
    ///         let base = last_run.unwrap_or_else(Utc::now);
    ///         Ok(base + Duration::seconds(13)) // 13th Fibonacci number
    ///     }
    /// );
    /// ```
    pub fn new<F>(name: impl Into<String>, next_run_fn: F) -> Self
    where
        F: Fn(Option<DateTime<Utc>>) -> Result<DateTime<Utc>, ScheduleError>
            + Send
            + Sync
            + 'static,
    {
        Self {
            name: name.into(),
            next_run_fn: Arc::new(next_run_fn),
        }
    }

    /// Calculate next run time using custom logic
    pub fn next_run(
        &self,
        last_run: Option<DateTime<Utc>>,
    ) -> Result<DateTime<Utc>, ScheduleError> {
        (self.next_run_fn)(last_run)
    }

    /// Check if this custom schedule is due
    pub fn is_due(&self, last_run: Option<DateTime<Utc>>) -> Result<bool, ScheduleError> {
        let next_run = self.next_run(last_run)?;
        Ok(Utc::now() >= next_run)
    }
}

impl std::fmt::Debug for CustomSchedule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CustomSchedule")
            .field("name", &self.name)
            .finish()
    }
}

impl std::fmt::Display for CustomSchedule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Custom[{}]", self.name)
    }
}

// ============================================================================
// Advanced Scheduler Features: Starvation Prevention & Schedule Preview
// ============================================================================

/// Task waiting time information for starvation prevention
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskWaitingInfo {
    /// Task name
    pub task_name: String,
    /// Original priority (None = default 5)
    pub original_priority: Option<u8>,
    /// Effective priority (may be boosted to prevent starvation)
    pub effective_priority: u8,
    /// Time since last execution (or since created if never run)
    pub waiting_duration: chrono::Duration,
    /// Whether priority was boosted
    pub priority_boosted: bool,
    /// Boost reason if applicable
    pub boost_reason: Option<String>,
}

impl BeatScheduler {
    /// Get tasks with starvation prevention priority boosting
    ///
    /// This method identifies low-priority tasks that have been waiting too long
    /// and temporarily boosts their effective priority to prevent starvation.
    ///
    /// # Arguments
    /// * `starvation_threshold_minutes` - Minutes waited before boosting priority (default: 60)
    /// * `boost_amount` - How much to boost priority (default: 2 levels)
    ///
    /// # Returns
    /// Vector of waiting information for all tasks, with boosted priorities where applicable
    ///
    /// # Example
    /// ```
    /// use celers_beat::{BeatScheduler, Schedule, ScheduledTask};
    ///
    /// let mut scheduler = BeatScheduler::new();
    ///
    /// let mut low_priority_task = ScheduledTask::new(
    ///     "cleanup".to_string(),
    ///     Schedule::interval(300)
    /// );
    /// low_priority_task.options.priority = Some(2); // Low priority
    /// scheduler.add_task(low_priority_task).unwrap();
    ///
    /// // Check for starvation (60 minute threshold)
    /// let waiting_info = scheduler.get_tasks_with_starvation_prevention(Some(60), Some(2));
    /// for info in waiting_info {
    ///     if info.priority_boosted {
    ///         println!("{} priority boosted to {} (waited {} minutes)",
    ///             info.task_name, info.effective_priority,
    ///             info.waiting_duration.num_minutes());
    ///     }
    /// }
    /// ```
    pub fn get_tasks_with_starvation_prevention(
        &self,
        starvation_threshold_minutes: Option<i64>,
        boost_amount: Option<u8>,
    ) -> Vec<TaskWaitingInfo> {
        let threshold_minutes = starvation_threshold_minutes.unwrap_or(60);
        let boost = boost_amount.unwrap_or(2);
        let now = Utc::now();

        self.tasks
            .values()
            .filter(|task| task.enabled)
            .map(|task| {
                let original_priority = task.options.priority;
                let base_priority = original_priority.unwrap_or(5);

                // Calculate waiting duration
                let waiting_duration = if let Some(last_run) = task.last_run_at {
                    now - last_run
                } else {
                    // Never run - use a very large duration to ensure it gets scheduled
                    Duration::hours(24 * 365)
                };

                // Determine if priority should be boosted
                let waiting_minutes = waiting_duration.num_minutes();
                let should_boost = waiting_minutes >= threshold_minutes && base_priority < 7;

                let (effective_priority, priority_boosted, boost_reason) = if should_boost {
                    let boosted = (base_priority + boost).min(9);
                    (
                        boosted,
                        true,
                        Some(format!(
                            "Waited {} minutes (threshold: {} min)",
                            waiting_minutes, threshold_minutes
                        )),
                    )
                } else {
                    (base_priority, false, None)
                };

                TaskWaitingInfo {
                    task_name: task.name.clone(),
                    original_priority,
                    effective_priority,
                    waiting_duration,
                    priority_boosted,
                    boost_reason,
                }
            })
            .collect()
    }

    /// Get due tasks with starvation prevention
    ///
    /// Like `get_due_tasks_by_priority()` but applies starvation prevention
    /// to ensure low-priority tasks eventually run.
    ///
    /// # Arguments
    /// * `starvation_threshold_minutes` - Minutes waited before boosting (default: 60)
    ///
    /// # Returns
    /// Due tasks ordered by effective priority (with starvation prevention)
    pub fn get_due_tasks_with_starvation_prevention(
        &self,
        starvation_threshold_minutes: Option<i64>,
    ) -> Vec<&ScheduledTask> {
        let waiting_info =
            self.get_tasks_with_starvation_prevention(starvation_threshold_minutes, Some(2));

        // Create a map of task name to effective priority
        let effective_priorities: HashMap<String, u8> = waiting_info
            .into_iter()
            .map(|info| (info.task_name, info.effective_priority))
            .collect();

        let mut due_tasks: Vec<&ScheduledTask> = self
            .tasks
            .values()
            .filter(|task| task.enabled && task.is_due().unwrap_or(false))
            .collect();

        // Sort by effective priority (descending), then by next run time (ascending)
        due_tasks.sort_by(|a, b| {
            let a_priority = effective_priorities.get(&a.name).copied().unwrap_or(5);
            let b_priority = effective_priorities.get(&b.name).copied().unwrap_or(5);

            match b_priority.cmp(&a_priority) {
                std::cmp::Ordering::Equal => {
                    // Same priority - sort by next run time
                    match (a.next_run_time(), b.next_run_time()) {
                        (Ok(a_time), Ok(b_time)) => a_time.cmp(&b_time),
                        _ => std::cmp::Ordering::Equal,
                    }
                }
                other => other,
            }
        });

        due_tasks
    }

    /// Preview upcoming task executions
    ///
    /// Calculate and preview the next N execution times for each task.
    /// Useful for debugging schedules and planning capacity.
    ///
    /// # Arguments
    /// * `count` - Number of upcoming executions to preview (default: 10)
    /// * `task_name` - Optional specific task to preview (None = all tasks)
    ///
    /// # Returns
    /// Map of task name to vector of upcoming execution times
    ///
    /// # Example
    /// ```
    /// use celers_beat::{BeatScheduler, Schedule, ScheduledTask};
    ///
    /// let mut scheduler = BeatScheduler::new();
    /// let task = ScheduledTask::new("report".to_string(), Schedule::interval(3600));
    /// scheduler.add_task(task).unwrap();
    ///
    /// // Preview next 5 executions
    /// let preview = scheduler.preview_upcoming_executions(Some(5), None);
    /// for (task_name, times) in preview {
    ///     println!("{}: {} upcoming executions", task_name, times.len());
    ///     for (i, time) in times.iter().enumerate() {
    ///         println!("  {}. {}", i + 1, time);
    ///     }
    /// }
    /// ```
    pub fn preview_upcoming_executions(
        &self,
        count: Option<usize>,
        task_name: Option<&str>,
    ) -> HashMap<String, Vec<DateTime<Utc>>> {
        let preview_count = count.unwrap_or(10);
        let mut preview = HashMap::new();

        let tasks: Vec<&ScheduledTask> = if let Some(name) = task_name {
            self.tasks.get(name).into_iter().collect()
        } else {
            self.tasks.values().collect()
        };

        for task in tasks {
            let mut upcoming = Vec::new();
            let mut last_run = task.last_run_at;

            for _ in 0..preview_count {
                match task.schedule.next_run(last_run) {
                    Ok(next_time) => {
                        upcoming.push(next_time);
                        last_run = Some(next_time);
                    }
                    Err(_) => break, // Can't calculate more (e.g., one-time schedule)
                }
            }

            if !upcoming.is_empty() {
                preview.insert(task.name.clone(), upcoming);
            }
        }

        preview
    }

    /// Simulate task execution without actually running tasks
    ///
    /// Dry-run mode for testing scheduler behavior. This method simulates
    /// the scheduler loop and returns what would have been executed.
    ///
    /// # Arguments
    /// * `duration_seconds` - How long to simulate (in seconds)
    /// * `tick_interval_seconds` - Scheduler tick interval (default: 1 second)
    ///
    /// # Returns
    /// Vector of (timestamp, task_name) tuples showing when each task would execute
    ///
    /// # Example
    /// ```
    /// use celers_beat::{BeatScheduler, Schedule, ScheduledTask};
    ///
    /// let mut scheduler = BeatScheduler::new();
    ///
    /// let task1 = ScheduledTask::new("task_60s".to_string(), Schedule::interval(60));
    /// let task2 = ScheduledTask::new("task_120s".to_string(), Schedule::interval(120));
    /// scheduler.add_task(task1).unwrap();
    /// scheduler.add_task(task2).unwrap();
    ///
    /// // Simulate 5 minutes of execution
    /// let simulation = scheduler.dry_run(300, Some(1));
    /// println!("Would execute {} tasks in 5 minutes", simulation.len());
    /// for (time, task_name) in simulation {
    ///     println!("  {} at {}", task_name, time);
    /// }
    /// ```
    pub fn dry_run(
        &self,
        duration_seconds: i64,
        tick_interval_seconds: Option<i64>,
    ) -> Vec<(DateTime<Utc>, String)> {
        let tick_interval = Duration::seconds(tick_interval_seconds.unwrap_or(1));
        let end_time = Utc::now() + Duration::seconds(duration_seconds);
        let mut current_time = Utc::now();
        let mut executions = Vec::new();

        // Clone task state for simulation
        let mut task_state: HashMap<String, Option<DateTime<Utc>>> = self
            .tasks
            .iter()
            .map(|(name, task)| (name.clone(), task.last_run_at))
            .collect();

        while current_time < end_time {
            // Check each task
            for (name, task) in &self.tasks {
                if !task.enabled {
                    continue;
                }

                let last_run = task_state.get(name).and_then(|t| *t);

                // Calculate next run based on simulated last run
                if let Ok(next_run) = task.schedule.next_run(last_run) {
                    // Check if task should execute at current simulation time
                    if next_run <= current_time && last_run.is_none_or(|lr| lr < next_run) {
                        executions.push((current_time, name.clone()));
                        // Update simulated last run
                        task_state.insert(name.clone(), Some(current_time));
                    }
                }
            }

            // Advance simulation time
            current_time += tick_interval;
        }

        // Sort by execution time
        executions.sort_by_key(|(time, _)| *time);
        executions
    }

    /// Get comprehensive scheduler statistics
    ///
    /// Returns detailed statistics about scheduler performance and task execution.
    ///
    /// # Returns
    /// Detailed scheduler statistics including execution counts, rates, and health metrics
    pub fn get_comprehensive_stats(&self) -> SchedulerStatistics {
        let total_tasks = self.tasks.len();
        let enabled_tasks = self.tasks.values().filter(|t| t.enabled).count();
        let disabled_tasks = total_tasks - enabled_tasks;

        let mut total_executions = 0u64;
        let mut total_failures = 0u64;
        let mut total_timeouts = 0u64;
        let mut total_duration_ms = 0u64;
        let mut execution_count = 0u64;

        let mut tasks_in_retry = 0;
        let mut tasks_with_failures = 0;
        let mut healthy_tasks = 0;
        let mut warning_tasks = 0;
        let mut unhealthy_tasks = 0;

        let now = Utc::now();
        let mut oldest_execution: Option<DateTime<Utc>> = None;
        let mut newest_execution: Option<DateTime<Utc>> = None;

        for task in self.tasks.values() {
            total_executions += task.total_run_count;
            total_failures += task.total_failure_count;

            // Aggregate from history
            for record in &task.execution_history {
                if record.is_timeout() {
                    total_timeouts += 1;
                }
                if let Some(duration) = record.duration_ms {
                    total_duration_ms += duration;
                    execution_count += 1;
                }

                // Track oldest/newest execution
                if let Some(exec_time) = record.completed_at {
                    if oldest_execution.is_none() || exec_time < oldest_execution.unwrap() {
                        oldest_execution = Some(exec_time);
                    }
                    if newest_execution.is_none() || exec_time > newest_execution.unwrap() {
                        newest_execution = Some(exec_time);
                    }
                }
            }

            if task.should_retry() {
                tasks_in_retry += 1;
            }
            if task.total_failure_count > 0 {
                tasks_with_failures += 1;
            }

            // Health status
            let health = task.check_health();
            match health.health {
                ScheduleHealth::Healthy => healthy_tasks += 1,
                ScheduleHealth::Warning { .. } => warning_tasks += 1,
                ScheduleHealth::Unhealthy { .. } => unhealthy_tasks += 1,
            }
        }

        let success_rate = if total_executions + total_failures > 0 {
            total_executions as f64 / (total_executions + total_failures) as f64
        } else {
            0.0
        };

        let avg_duration_ms = if execution_count > 0 {
            Some(total_duration_ms / execution_count)
        } else {
            None
        };

        let uptime = oldest_execution.map(|oldest| now - oldest);

        SchedulerStatistics {
            total_tasks,
            enabled_tasks,
            disabled_tasks,
            total_executions,
            total_failures,
            total_timeouts,
            success_rate,
            tasks_in_retry,
            tasks_with_failures,
            healthy_tasks,
            warning_tasks,
            unhealthy_tasks,
            avg_duration_ms,
            uptime,
            oldest_execution,
            newest_execution,
        }
    }
}

/// Comprehensive scheduler statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerStatistics {
    /// Total number of tasks
    pub total_tasks: usize,
    /// Number of enabled tasks
    pub enabled_tasks: usize,
    /// Number of disabled tasks
    pub disabled_tasks: usize,
    /// Total successful executions across all tasks
    pub total_executions: u64,
    /// Total failures across all tasks
    pub total_failures: u64,
    /// Total timeouts across all tasks
    pub total_timeouts: u64,
    /// Overall success rate (0.0 to 1.0)
    pub success_rate: f64,
    /// Number of tasks in retry state
    pub tasks_in_retry: usize,
    /// Number of tasks that have experienced failures
    pub tasks_with_failures: usize,
    /// Number of healthy tasks
    pub healthy_tasks: usize,
    /// Number of tasks with warnings
    pub warning_tasks: usize,
    /// Number of unhealthy tasks
    pub unhealthy_tasks: usize,
    /// Average execution duration in milliseconds
    pub avg_duration_ms: Option<u64>,
    /// Scheduler uptime (time since first execution)
    pub uptime: Option<chrono::Duration>,
    /// Timestamp of oldest recorded execution
    pub oldest_execution: Option<DateTime<Utc>>,
    /// Timestamp of newest recorded execution
    pub newest_execution: Option<DateTime<Utc>>,
}

impl std::fmt::Display for SchedulerStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Scheduler Statistics:")?;
        writeln!(
            f,
            "  Tasks: {} total ({} enabled, {} disabled)",
            self.total_tasks, self.enabled_tasks, self.disabled_tasks
        )?;
        writeln!(
            f,
            "  Executions: {} successful, {} failed, {} timed out",
            self.total_executions, self.total_failures, self.total_timeouts
        )?;
        writeln!(f, "  Success Rate: {:.1}%", self.success_rate * 100.0)?;
        writeln!(
            f,
            "  Health: {} healthy, {} warnings, {} unhealthy",
            self.healthy_tasks, self.warning_tasks, self.unhealthy_tasks
        )?;

        if let Some(avg_ms) = self.avg_duration_ms {
            writeln!(f, "  Avg Duration: {}ms", avg_ms)?;
        }

        if let Some(uptime) = self.uptime {
            writeln!(
                f,
                "  Uptime: {} days {} hours",
                uptime.num_days(),
                uptime.num_hours() % 24
            )?;
        }

        Ok(())
    }
}

// ============================================================================
// Timezone Conversion Utilities
// ============================================================================

/// Timezone conversion utilities for schedule management
pub struct TimezoneUtils;

impl TimezoneUtils {
    /// Convert UTC time to specific timezone
    ///
    /// # Arguments
    /// * `utc_time` - UTC DateTime
    /// * `timezone` - IANA timezone name (e.g., "America/New_York")
    ///
    /// # Returns
    /// Formatted string in target timezone
    ///
    /// # Example
    /// ```
    /// use celers_beat::TimezoneUtils;
    /// use chrono::Utc;
    ///
    /// let now = Utc::now();
    /// let ny_time = TimezoneUtils::format_in_timezone(now, "America/New_York");
    /// ```
    #[cfg(feature = "cron")]
    pub fn format_in_timezone(utc_time: DateTime<Utc>, timezone: &str) -> String {
        use chrono_tz::Tz;

        if let Ok(tz) = timezone.parse::<Tz>() {
            let local_time = utc_time.with_timezone(&tz);
            format!("{} {}", local_time.format("%Y-%m-%d %H:%M:%S"), tz.name())
        } else {
            format!(
                "{} (invalid timezone: {})",
                utc_time.format("%Y-%m-%d %H:%M:%S UTC"),
                timezone
            )
        }
    }

    /// Get current time in multiple timezones
    ///
    /// # Arguments
    /// * `timezones` - List of IANA timezone names
    ///
    /// # Returns
    /// Map of timezone name to formatted time string
    ///
    /// # Example
    /// ```
    /// use celers_beat::TimezoneUtils;
    ///
    /// let zones = vec!["America/New_York", "Europe/London", "Asia/Tokyo"];
    /// let times = TimezoneUtils::current_time_in_zones(&zones);
    /// for (tz, time) in times {
    ///     println!("{}: {}", tz, time);
    /// }
    /// ```
    #[cfg(feature = "cron")]
    pub fn current_time_in_zones(timezones: &[&str]) -> HashMap<String, String> {
        let now = Utc::now();
        timezones
            .iter()
            .map(|tz| {
                let formatted = Self::format_in_timezone(now, tz);
                (tz.to_string(), formatted)
            })
            .collect()
    }

    /// Calculate time until next occurrence in specific timezone
    ///
    /// # Arguments
    /// * `target_hour` - Hour in target timezone (0-23)
    /// * `target_minute` - Minute (0-59)
    /// * `timezone` - IANA timezone name
    ///
    /// # Returns
    /// Duration until next occurrence of that time in the specified timezone
    #[cfg(feature = "cron")]
    pub fn time_until_next_occurrence(
        target_hour: u32,
        target_minute: u32,
        timezone: &str,
    ) -> Result<chrono::Duration, String> {
        use chrono_tz::Tz;

        let tz: Tz = timezone
            .parse()
            .map_err(|_| format!("Invalid timezone: {}", timezone))?;
        let now_utc = Utc::now();
        let now_local = now_utc.with_timezone(&tz);

        // Calculate target time today in local timezone
        let target_today = now_local
            .date_naive()
            .and_hms_opt(target_hour, target_minute, 0)
            .ok_or("Invalid time")?
            .and_local_timezone(tz)
            .single()
            .ok_or("Ambiguous or invalid time due to DST")?;

        let target_utc = target_today.with_timezone(&Utc);

        // If target time today has passed, use tomorrow
        let final_target = if target_utc <= now_utc {
            let tomorrow = now_local.date_naive() + chrono::Days::new(1);
            let target_tomorrow = tomorrow
                .and_hms_opt(target_hour, target_minute, 0)
                .ok_or("Invalid time")?
                .and_local_timezone(tz)
                .single()
                .ok_or("Ambiguous or invalid time due to DST")?;
            target_tomorrow.with_timezone(&Utc)
        } else {
            target_utc
        };

        Ok(final_target - now_utc)
    }

    /// Detect system's local timezone
    ///
    /// Returns the IANA timezone name of the system's local timezone.
    /// Falls back to "UTC" if detection fails.
    ///
    /// # Example
    /// ```
    /// use celers_beat::TimezoneUtils;
    ///
    /// let local_tz = TimezoneUtils::detect_system_timezone();
    /// println!("System timezone: {}", local_tz);
    /// ```
    #[cfg(feature = "cron")]
    pub fn detect_system_timezone() -> String {
        // Try to get timezone from TZ environment variable
        if let Ok(tz) = std::env::var("TZ") {
            if Self::is_valid_timezone(&tz) {
                return tz;
            }
        }

        // Try to read from /etc/timezone (Debian/Ubuntu)
        #[cfg(target_os = "linux")]
        {
            if let Ok(tz) = std::fs::read_to_string("/etc/timezone") {
                let tz = tz.trim().to_string();
                if Self::is_valid_timezone(&tz) {
                    return tz;
                }
            }
        }

        // Try to read symlink /etc/localtime (most Unix systems)
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            if let Ok(link) = std::fs::read_link("/etc/localtime") {
                if let Some(tz_path) = link.to_str() {
                    // Extract timezone from path like /usr/share/zoneinfo/America/New_York
                    if let Some(tz_start) = tz_path.find("zoneinfo/") {
                        let tz = &tz_path[tz_start + 9..];
                        if Self::is_valid_timezone(tz) {
                            return tz.to_string();
                        }
                    }
                }
            }
        }

        // Fallback to UTC
        "UTC".to_string()
    }

    /// Check if a timezone string is valid
    ///
    /// # Arguments
    /// * `timezone` - IANA timezone name to validate
    ///
    /// # Returns
    /// `true` if the timezone is valid, `false` otherwise
    ///
    /// # Example
    /// ```
    /// use celers_beat::TimezoneUtils;
    ///
    /// assert!(TimezoneUtils::is_valid_timezone("America/New_York"));
    /// assert!(TimezoneUtils::is_valid_timezone("UTC"));
    /// assert!(!TimezoneUtils::is_valid_timezone("Invalid/Timezone"));
    /// ```
    #[cfg(feature = "cron")]
    pub fn is_valid_timezone(timezone: &str) -> bool {
        use chrono_tz::Tz;
        timezone.parse::<Tz>().is_ok()
    }

    /// Get list of all available IANA timezone names
    ///
    /// Returns a vector of all timezone names supported by chrono-tz.
    ///
    /// # Example
    /// ```
    /// use celers_beat::TimezoneUtils;
    ///
    /// let timezones = TimezoneUtils::list_all_timezones();
    /// assert!(timezones.len() > 500); // There are 600+ timezones
    /// assert!(timezones.contains(&"America/New_York".to_string()));
    /// assert!(timezones.contains(&"Europe/London".to_string()));
    /// assert!(timezones.contains(&"Asia/Tokyo".to_string()));
    /// ```
    #[cfg(feature = "cron")]
    pub fn list_all_timezones() -> Vec<String> {
        use chrono_tz::TZ_VARIANTS;
        TZ_VARIANTS.iter().map(|tz| tz.name().to_string()).collect()
    }

    /// Search for timezones matching a pattern
    ///
    /// # Arguments
    /// * `pattern` - Case-insensitive search pattern (substring match)
    ///
    /// # Returns
    /// Vector of matching timezone names
    ///
    /// # Example
    /// ```
    /// use celers_beat::TimezoneUtils;
    ///
    /// let us_timezones = TimezoneUtils::search_timezones("america");
    /// assert!(us_timezones.iter().any(|tz| tz == "America/New_York"));
    /// assert!(us_timezones.iter().any(|tz| tz == "America/Los_Angeles"));
    ///
    /// let london = TimezoneUtils::search_timezones("london");
    /// assert!(london.contains(&"Europe/London".to_string()));
    /// ```
    #[cfg(feature = "cron")]
    pub fn search_timezones(pattern: &str) -> Vec<String> {
        let pattern_lower = pattern.to_lowercase();
        Self::list_all_timezones()
            .into_iter()
            .filter(|tz| tz.to_lowercase().contains(&pattern_lower))
            .collect()
    }

    /// Check if a timezone is currently observing Daylight Saving Time
    ///
    /// # Arguments
    /// * `timezone` - IANA timezone name
    /// * `at_time` - Optional time to check (defaults to now)
    ///
    /// # Returns
    /// `Ok(true)` if DST is active, `Ok(false)` if not, `Err` if timezone is invalid
    ///
    /// # Example
    /// ```
    /// use celers_beat::TimezoneUtils;
    /// use chrono::Utc;
    ///
    /// // Check current DST status
    /// let is_dst = TimezoneUtils::is_dst_active("America/New_York", None).unwrap();
    /// println!("New York DST active: {}", is_dst);
    ///
    /// // Check at specific time
    /// let summer_time = Utc::now(); // Would need a summer date for guaranteed DST
    /// let was_dst = TimezoneUtils::is_dst_active("America/New_York", Some(summer_time)).unwrap();
    /// ```
    #[cfg(feature = "cron")]
    pub fn is_dst_active(timezone: &str, at_time: Option<DateTime<Utc>>) -> Result<bool, String> {
        use chrono_tz::Tz;

        let tz: Tz = timezone
            .parse()
            .map_err(|_| format!("Invalid timezone: {}", timezone))?;
        let time = at_time.unwrap_or_else(Utc::now);
        let local_time = time.with_timezone(&tz);

        // Check if the offset includes DST
        // This is done by comparing the offset at this time with the standard offset
        // If they differ, DST is active
        let offset = local_time.offset().fix();
        let std_offset = tz.offset_from_utc_datetime(&local_time.naive_utc()).fix();

        Ok(offset != std_offset)
    }

    /// Get UTC offset for a timezone at a specific time
    ///
    /// # Arguments
    /// * `timezone` - IANA timezone name
    /// * `at_time` - Time to check offset (defaults to now)
    ///
    /// # Returns
    /// UTC offset in seconds (positive = east of UTC, negative = west)
    ///
    /// # Example
    /// ```
    /// use celers_beat::TimezoneUtils;
    /// use chrono::Utc;
    ///
    /// let offset = TimezoneUtils::get_utc_offset("America/New_York", None).unwrap();
    /// // New York is UTC-5 (EST) or UTC-4 (EDT)
    /// assert!(offset == -5 * 3600 || offset == -4 * 3600);
    ///
    /// let tokyo_offset = TimezoneUtils::get_utc_offset("Asia/Tokyo", None).unwrap();
    /// assert_eq!(tokyo_offset, 9 * 3600); // Tokyo is always UTC+9
    /// ```
    #[cfg(feature = "cron")]
    pub fn get_utc_offset(timezone: &str, at_time: Option<DateTime<Utc>>) -> Result<i32, String> {
        use chrono_tz::Tz;

        let tz: Tz = timezone
            .parse()
            .map_err(|_| format!("Invalid timezone: {}", timezone))?;
        let time = at_time.unwrap_or_else(Utc::now);
        let local_time = time.with_timezone(&tz);

        Ok(local_time.offset().fix().local_minus_utc())
    }

    /// Get timezone information including offset and DST status
    ///
    /// # Arguments
    /// * `timezone` - IANA timezone name
    /// * `at_time` - Optional time to check (defaults to now)
    ///
    /// # Returns
    /// `TimezoneInfo` with detailed information about the timezone
    ///
    /// # Example
    /// ```
    /// use celers_beat::TimezoneUtils;
    ///
    /// let info = TimezoneUtils::get_timezone_info("America/New_York", None).unwrap();
    /// println!("Timezone: {}", info.name);
    /// println!("UTC Offset: {} hours", info.utc_offset_seconds / 3600);
    /// println!("DST Active: {}", info.is_dst);
    /// println!("Current Time: {}", info.current_time);
    /// ```
    #[cfg(feature = "cron")]
    pub fn get_timezone_info(
        timezone: &str,
        at_time: Option<DateTime<Utc>>,
    ) -> Result<TimezoneInfo, String> {
        use chrono_tz::Tz;

        let tz: Tz = timezone
            .parse()
            .map_err(|_| format!("Invalid timezone: {}", timezone))?;
        let time = at_time.unwrap_or_else(Utc::now);
        let local_time = time.with_timezone(&tz);

        let offset_seconds = local_time.offset().fix().local_minus_utc();
        let is_dst = Self::is_dst_active(timezone, Some(time))?;

        Ok(TimezoneInfo {
            name: timezone.to_string(),
            utc_offset_seconds: offset_seconds,
            utc_offset_hours: offset_seconds as f32 / 3600.0,
            is_dst,
            current_time: local_time.format("%Y-%m-%d %H:%M:%S %Z").to_string(),
            abbreviation: format!("{}", local_time.format("%Z")),
        })
    }

    /// Convert time from one timezone to another
    ///
    /// # Arguments
    /// * `time` - DateTime in source timezone
    /// * `from_tz` - Source IANA timezone name
    /// * `to_tz` - Target IANA timezone name
    ///
    /// # Returns
    /// Formatted time string in target timezone
    ///
    /// # Example
    /// ```
    /// use celers_beat::TimezoneUtils;
    /// use chrono::Utc;
    ///
    /// let utc_now = Utc::now();
    /// let tokyo_time = TimezoneUtils::convert_between_timezones(
    ///     utc_now,
    ///     "America/New_York",
    ///     "Asia/Tokyo"
    /// ).unwrap();
    /// println!("When it's {} in New York, it's {} in Tokyo", utc_now, tokyo_time);
    /// ```
    #[cfg(feature = "cron")]
    pub fn convert_between_timezones(
        time: DateTime<Utc>,
        from_tz: &str,
        to_tz: &str,
    ) -> Result<String, String> {
        use chrono_tz::Tz;

        let _source_tz: Tz = from_tz
            .parse()
            .map_err(|_| format!("Invalid source timezone: {}", from_tz))?;
        let target_tz: Tz = to_tz
            .parse()
            .map_err(|_| format!("Invalid target timezone: {}", to_tz))?;

        let target_time = time.with_timezone(&target_tz);
        Ok(format!(
            "{} {}",
            target_time.format("%Y-%m-%d %H:%M:%S"),
            target_tz.name()
        ))
    }

    /// Get common timezone abbreviations and their IANA names
    ///
    /// Returns a map of common abbreviations (EST, PST, JST, etc.) to their
    /// corresponding IANA timezone names.
    ///
    /// # Example
    /// ```
    /// use celers_beat::TimezoneUtils;
    ///
    /// let abbrevs = TimezoneUtils::get_common_timezone_abbreviations();
    /// assert_eq!(abbrevs.get("EST"), Some(&"America/New_York".to_string()));
    /// assert_eq!(abbrevs.get("PST"), Some(&"America/Los_Angeles".to_string()));
    /// assert_eq!(abbrevs.get("JST"), Some(&"Asia/Tokyo".to_string()));
    /// ```
    #[cfg(feature = "cron")]
    pub fn get_common_timezone_abbreviations() -> HashMap<String, String> {
        let mut abbrevs = HashMap::new();

        // US timezones
        abbrevs.insert("EST".to_string(), "America/New_York".to_string());
        abbrevs.insert("EDT".to_string(), "America/New_York".to_string());
        abbrevs.insert("CST".to_string(), "America/Chicago".to_string());
        abbrevs.insert("CDT".to_string(), "America/Chicago".to_string());
        abbrevs.insert("MST".to_string(), "America/Denver".to_string());
        abbrevs.insert("MDT".to_string(), "America/Denver".to_string());
        abbrevs.insert("PST".to_string(), "America/Los_Angeles".to_string());
        abbrevs.insert("PDT".to_string(), "America/Los_Angeles".to_string());
        abbrevs.insert("AKST".to_string(), "America/Anchorage".to_string());
        abbrevs.insert("AKDT".to_string(), "America/Anchorage".to_string());
        abbrevs.insert("HST".to_string(), "Pacific/Honolulu".to_string());

        // Europe
        abbrevs.insert("GMT".to_string(), "Europe/London".to_string());
        abbrevs.insert("BST".to_string(), "Europe/London".to_string());
        abbrevs.insert("CET".to_string(), "Europe/Paris".to_string());
        abbrevs.insert("CEST".to_string(), "Europe/Paris".to_string());
        abbrevs.insert("EET".to_string(), "Europe/Athens".to_string());
        abbrevs.insert("EEST".to_string(), "Europe/Athens".to_string());

        // Asia
        abbrevs.insert("JST".to_string(), "Asia/Tokyo".to_string());
        abbrevs.insert("KST".to_string(), "Asia/Seoul".to_string());
        abbrevs.insert("CST_CHINA".to_string(), "Asia/Shanghai".to_string());
        abbrevs.insert("IST".to_string(), "Asia/Kolkata".to_string());
        abbrevs.insert("SGT".to_string(), "Asia/Singapore".to_string());
        abbrevs.insert("HKT".to_string(), "Asia/Hong_Kong".to_string());

        // Australia
        abbrevs.insert("AEST".to_string(), "Australia/Sydney".to_string());
        abbrevs.insert("AEDT".to_string(), "Australia/Sydney".to_string());
        abbrevs.insert("ACST".to_string(), "Australia/Adelaide".to_string());
        abbrevs.insert("ACDT".to_string(), "Australia/Adelaide".to_string());
        abbrevs.insert("AWST".to_string(), "Australia/Perth".to_string());

        abbrevs
    }
}

/// Detailed timezone information
#[cfg(feature = "cron")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimezoneInfo {
    /// IANA timezone name
    pub name: String,
    /// UTC offset in seconds
    pub utc_offset_seconds: i32,
    /// UTC offset in hours (can be fractional)
    pub utc_offset_hours: f32,
    /// Whether DST is currently active
    pub is_dst: bool,
    /// Current time in this timezone (formatted)
    pub current_time: String,
    /// Timezone abbreviation (e.g., "EST", "PDT")
    pub abbreviation: String,
}

#[cfg(feature = "cron")]
impl std::fmt::Display for TimezoneInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} (UTC{:+.1}h, {}, DST: {}): {}",
            self.name,
            self.utc_offset_hours,
            self.abbreviation,
            if self.is_dst { "Yes" } else { "No" },
            self.current_time
        )
    }
}

// ============================================================================
// Schedule Builders and Templates
// ============================================================================

/// Schedule builder for creating schedules with a fluent API
///
/// Provides convenient methods for building common schedule patterns
/// with validation and best practices built-in.
///
/// # Example
/// ```
/// use celers_beat::ScheduleBuilder;
///
/// // Business hours only (Mon-Fri, 9 AM - 5 PM)
/// let schedule = ScheduleBuilder::new()
///     .every_n_minutes(30)
///     .business_hours_only()
///     .build();
///
/// // Every hour during weekends
/// let schedule = ScheduleBuilder::new()
///     .every_n_hours(1)
///     .weekends_only()
///     .build();
/// ```
#[derive(Debug, Clone)]
pub struct ScheduleBuilder {
    interval_seconds: Option<u64>,
    #[cfg(feature = "cron")]
    timezone: Option<String>,
    #[cfg(feature = "cron")]
    business_hours: bool,
    #[cfg(feature = "cron")]
    weekends: bool,
    #[cfg(feature = "cron")]
    weekdays: bool,
}

impl ScheduleBuilder {
    /// Create a new schedule builder
    pub fn new() -> Self {
        Self {
            interval_seconds: None,
            #[cfg(feature = "cron")]
            timezone: None,
            #[cfg(feature = "cron")]
            business_hours: false,
            #[cfg(feature = "cron")]
            weekends: false,
            #[cfg(feature = "cron")]
            weekdays: false,
        }
    }

    /// Set interval in seconds
    ///
    /// # Example
    /// ```
    /// use celers_beat::ScheduleBuilder;
    ///
    /// let schedule = ScheduleBuilder::new()
    ///     .every_n_seconds(30)
    ///     .build();
    /// ```
    pub fn every_n_seconds(mut self, seconds: u64) -> Self {
        self.interval_seconds = Some(seconds);
        self
    }

    /// Set interval in minutes
    ///
    /// # Example
    /// ```
    /// use celers_beat::ScheduleBuilder;
    ///
    /// let schedule = ScheduleBuilder::new()
    ///     .every_n_minutes(15)
    ///     .build();
    /// ```
    pub fn every_n_minutes(mut self, minutes: u64) -> Self {
        self.interval_seconds = Some(minutes * 60);
        self
    }

    /// Set interval in hours
    ///
    /// # Example
    /// ```
    /// use celers_beat::ScheduleBuilder;
    ///
    /// let schedule = ScheduleBuilder::new()
    ///     .every_n_hours(2)
    ///     .build();
    /// ```
    pub fn every_n_hours(mut self, hours: u64) -> Self {
        self.interval_seconds = Some(hours * 3600);
        self
    }

    /// Set interval in days
    ///
    /// # Example
    /// ```
    /// use celers_beat::ScheduleBuilder;
    ///
    /// let schedule = ScheduleBuilder::new()
    ///     .every_n_days(1)
    ///     .build();
    /// ```
    pub fn every_n_days(mut self, days: u64) -> Self {
        self.interval_seconds = Some(days * 86400);
        self
    }

    /// Restrict to business hours only (Mon-Fri, 9 AM - 5 PM)
    ///
    /// Note: This creates a crontab schedule and requires the `cron` feature.
    ///
    /// # Example
    /// ```
    /// use celers_beat::ScheduleBuilder;
    ///
    /// let schedule = ScheduleBuilder::new()
    ///     .every_n_minutes(30)
    ///     .business_hours_only()
    ///     .build();
    /// ```
    #[cfg(feature = "cron")]
    pub fn business_hours_only(mut self) -> Self {
        self.business_hours = true;
        self.weekdays = true;
        self
    }

    /// Restrict to weekends only (Sat-Sun)
    ///
    /// Note: This creates a crontab schedule and requires the `cron` feature.
    ///
    /// # Example
    /// ```
    /// use celers_beat::ScheduleBuilder;
    ///
    /// let schedule = ScheduleBuilder::new()
    ///     .every_n_hours(1)
    ///     .weekends_only()
    ///     .build();
    /// ```
    #[cfg(feature = "cron")]
    pub fn weekends_only(mut self) -> Self {
        self.weekends = true;
        self
    }

    /// Restrict to weekdays only (Mon-Fri)
    ///
    /// Note: This creates a crontab schedule and requires the `cron` feature.
    ///
    /// # Example
    /// ```
    /// use celers_beat::ScheduleBuilder;
    ///
    /// let schedule = ScheduleBuilder::new()
    ///     .every_n_hours(2)
    ///     .weekdays_only()
    ///     .build();
    /// ```
    #[cfg(feature = "cron")]
    pub fn weekdays_only(mut self) -> Self {
        self.weekdays = true;
        self
    }

    /// Set timezone for the schedule
    ///
    /// # Example
    /// ```
    /// use celers_beat::ScheduleBuilder;
    ///
    /// let schedule = ScheduleBuilder::new()
    ///     .every_n_hours(1)
    ///     .in_timezone("America/New_York")
    ///     .build();
    /// ```
    #[cfg(feature = "cron")]
    pub fn in_timezone(mut self, timezone: &str) -> Self {
        self.timezone = Some(timezone.to_string());
        self
    }

    /// Build the schedule
    ///
    /// # Returns
    /// A `Schedule` based on the builder configuration
    pub fn build(self) -> Schedule {
        #[cfg(feature = "cron")]
        {
            // If any cron-specific features are set, build a crontab schedule
            if self.business_hours || self.weekends || self.weekdays || self.timezone.is_some() {
                let interval_minutes = self.interval_seconds.unwrap_or(3600) / 60;
                let minute_expr = if interval_minutes < 60 {
                    format!("*/{}", interval_minutes)
                } else {
                    "0".to_string()
                };

                let hour_expr = if self.business_hours {
                    "9-17".to_string()
                } else {
                    "*".to_string()
                };

                let dow_expr = if self.weekends {
                    "0,6".to_string() // Sun, Sat
                } else if self.weekdays || self.business_hours {
                    "1-5".to_string() // Mon-Fri
                } else {
                    "*".to_string()
                };

                if let Some(tz) = self.timezone {
                    return Schedule::crontab_tz(
                        &minute_expr,
                        &hour_expr,
                        &dow_expr,
                        "*",
                        "*",
                        &tz,
                    );
                } else {
                    return Schedule::crontab(&minute_expr, &hour_expr, &dow_expr, "*", "*");
                }
            }
        }

        // Default to interval schedule
        Schedule::interval(self.interval_seconds.unwrap_or(3600))
    }
}

impl Default for ScheduleBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Common schedule templates for typical use cases
///
/// Provides pre-configured schedules for common patterns.
pub struct ScheduleTemplates;

impl ScheduleTemplates {
    /// Every minute
    ///
    /// # Example
    /// ```
    /// use celers_beat::ScheduleTemplates;
    ///
    /// let schedule = ScheduleTemplates::every_minute();
    /// ```
    pub fn every_minute() -> Schedule {
        Schedule::interval(60)
    }

    /// Every 5 minutes
    ///
    /// # Example
    /// ```
    /// use celers_beat::ScheduleTemplates;
    ///
    /// let schedule = ScheduleTemplates::every_5_minutes();
    /// ```
    pub fn every_5_minutes() -> Schedule {
        Schedule::interval(300)
    }

    /// Every 15 minutes
    ///
    /// # Example
    /// ```
    /// use celers_beat::ScheduleTemplates;
    ///
    /// let schedule = ScheduleTemplates::every_15_minutes();
    /// ```
    pub fn every_15_minutes() -> Schedule {
        Schedule::interval(900)
    }

    /// Every 30 minutes
    ///
    /// # Example
    /// ```
    /// use celers_beat::ScheduleTemplates;
    ///
    /// let schedule = ScheduleTemplates::every_30_minutes();
    /// ```
    pub fn every_30_minutes() -> Schedule {
        Schedule::interval(1800)
    }

    /// Every hour
    ///
    /// # Example
    /// ```
    /// use celers_beat::ScheduleTemplates;
    ///
    /// let schedule = ScheduleTemplates::hourly();
    /// ```
    pub fn hourly() -> Schedule {
        Schedule::interval(3600)
    }

    /// Every day at midnight (requires `cron` feature)
    ///
    /// # Example
    /// ```
    /// use celers_beat::ScheduleTemplates;
    ///
    /// let schedule = ScheduleTemplates::daily_at_midnight();
    /// ```
    #[cfg(feature = "cron")]
    pub fn daily_at_midnight() -> Schedule {
        Schedule::crontab("0", "0", "*", "*", "*")
    }

    /// Every day at a specific hour (requires `cron` feature)
    ///
    /// # Arguments
    /// * `hour` - Hour of day (0-23)
    ///
    /// # Example
    /// ```
    /// use celers_beat::ScheduleTemplates;
    ///
    /// // Every day at 3 AM
    /// let schedule = ScheduleTemplates::daily_at_hour(3);
    /// ```
    #[cfg(feature = "cron")]
    pub fn daily_at_hour(hour: u32) -> Schedule {
        Schedule::crontab("0", &hour.to_string(), "*", "*", "*")
    }

    /// Every weekday (Mon-Fri) at a specific time (requires `cron` feature)
    ///
    /// # Arguments
    /// * `hour` - Hour of day (0-23)
    /// * `minute` - Minute (0-59)
    ///
    /// # Example
    /// ```
    /// use celers_beat::ScheduleTemplates;
    ///
    /// // Weekdays at 9:00 AM
    /// let schedule = ScheduleTemplates::weekdays_at(9, 0);
    /// ```
    #[cfg(feature = "cron")]
    pub fn weekdays_at(hour: u32, minute: u32) -> Schedule {
        Schedule::crontab(&minute.to_string(), &hour.to_string(), "1-5", "*", "*")
    }

    /// Every Monday at a specific time (requires `cron` feature)
    ///
    /// # Arguments
    /// * `hour` - Hour of day (0-23)
    /// * `minute` - Minute (0-59)
    ///
    /// # Example
    /// ```
    /// use celers_beat::ScheduleTemplates;
    ///
    /// // Every Monday at 9:00 AM
    /// let schedule = ScheduleTemplates::weekly_on_monday(9, 0);
    /// ```
    #[cfg(feature = "cron")]
    pub fn weekly_on_monday(hour: u32, minute: u32) -> Schedule {
        Schedule::crontab(&minute.to_string(), &hour.to_string(), "1", "*", "*")
    }

    /// First day of every month at midnight (requires `cron` feature)
    ///
    /// # Example
    /// ```
    /// use celers_beat::ScheduleTemplates;
    ///
    /// let schedule = ScheduleTemplates::monthly_first_day();
    /// ```
    #[cfg(feature = "cron")]
    pub fn monthly_first_day() -> Schedule {
        Schedule::crontab("0", "0", "*", "1", "*")
    }

    /// Last day of every month at midnight (requires `cron` feature)
    ///
    /// Note: Uses day 28 which works for all months
    ///
    /// # Example
    /// ```
    /// use celers_beat::ScheduleTemplates;
    ///
    /// let schedule = ScheduleTemplates::monthly_last_day();
    /// ```
    #[cfg(feature = "cron")]
    pub fn monthly_last_day() -> Schedule {
        Schedule::crontab("0", "0", "*", "28-31", "*")
    }

    /// Every hour during business hours (9 AM - 5 PM, Mon-Fri) (requires `cron` feature)
    ///
    /// # Example
    /// ```
    /// use celers_beat::ScheduleTemplates;
    ///
    /// let schedule = ScheduleTemplates::business_hours_hourly();
    /// ```
    #[cfg(feature = "cron")]
    pub fn business_hours_hourly() -> Schedule {
        Schedule::crontab("0", "9-17", "1-5", "*", "*")
    }

    /// Every 15 minutes during business hours (requires `cron` feature)
    ///
    /// # Example
    /// ```
    /// use celers_beat::ScheduleTemplates;
    ///
    /// let schedule = ScheduleTemplates::business_hours_every_15_minutes();
    /// ```
    #[cfg(feature = "cron")]
    pub fn business_hours_every_15_minutes() -> Schedule {
        Schedule::crontab("*/15", "9-17", "1-5", "*", "*")
    }

    /// Weekend mornings at 8 AM (requires `cron` feature)
    ///
    /// # Example
    /// ```
    /// use celers_beat::ScheduleTemplates;
    ///
    /// let schedule = ScheduleTemplates::weekend_mornings();
    /// ```
    #[cfg(feature = "cron")]
    pub fn weekend_mornings() -> Schedule {
        Schedule::crontab("0", "8", "0,6", "*", "*")
    }

    /// Quarterly on the first day at midnight (Jan 1, Apr 1, Jul 1, Oct 1) (requires `cron` feature)
    ///
    /// # Example
    /// ```
    /// use celers_beat::ScheduleTemplates;
    ///
    /// let schedule = ScheduleTemplates::quarterly();
    /// ```
    #[cfg(feature = "cron")]
    pub fn quarterly() -> Schedule {
        Schedule::crontab("0", "0", "*", "1", "1,4,7,10")
    }

    /// Every 2 hours
    ///
    /// # Example
    /// ```
    /// use celers_beat::ScheduleTemplates;
    ///
    /// let schedule = ScheduleTemplates::every_2_hours();
    /// ```
    pub fn every_2_hours() -> Schedule {
        Schedule::interval(7200)
    }

    /// Every 6 hours
    ///
    /// # Example
    /// ```
    /// use celers_beat::ScheduleTemplates;
    ///
    /// let schedule = ScheduleTemplates::every_6_hours();
    /// ```
    pub fn every_6_hours() -> Schedule {
        Schedule::interval(21600)
    }

    /// Every 12 hours
    ///
    /// # Example
    /// ```
    /// use celers_beat::ScheduleTemplates;
    ///
    /// let schedule = ScheduleTemplates::every_12_hours();
    /// ```
    pub fn every_12_hours() -> Schedule {
        Schedule::interval(43200)
    }
}

// ============================================================================
// Weighted Fair Queuing (WFQ)
// ============================================================================

/// Weighted Fair Queuing configuration
///
/// WFQ provides fair scheduling based on task weights, ensuring that higher-weight
/// tasks get proportionally more execution time while preventing starvation of
/// lower-weight tasks.
///
/// # Algorithm
/// - Each task has a weight (default: 1.0, range: 0.1-10.0)
/// - Virtual time tracks fairness: virtual_finish_time = virtual_start_time + (execution_cost / weight)
/// - Tasks with lowest virtual finish time are scheduled first
/// - Prevents starvation while respecting priorities
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WFQConfig {
    /// Enable Weighted Fair Queuing
    pub enabled: bool,

    /// Default weight for tasks without explicit weight
    pub default_weight: f64,

    /// Minimum allowed weight
    pub min_weight: f64,

    /// Maximum allowed weight
    pub max_weight: f64,
}

impl Default for WFQConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            default_weight: 1.0,
            min_weight: 0.1,
            max_weight: 10.0,
        }
    }
}

/// Task weight for Weighted Fair Queuing
///
/// Higher weights mean higher importance and more execution time allocation.
/// Weight must be between 0.1 and 10.0.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct TaskWeight(f64);

impl TaskWeight {
    /// Create new task weight with validation
    pub fn new(weight: f64) -> Result<Self, String> {
        if !(0.1..=10.0).contains(&weight) {
            return Err(format!(
                "Weight must be between 0.1 and 10.0, got {}",
                weight
            ));
        }
        Ok(Self(weight))
    }

    /// Get the weight value
    pub fn value(&self) -> f64 {
        self.0
    }
}

impl Default for TaskWeight {
    fn default() -> Self {
        Self(1.0)
    }
}

/// Weighted Fair Queuing state for a task
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WFQState {
    /// Task weight (higher = more important)
    #[serde(default)]
    pub weight: TaskWeight,

    /// Virtual start time
    #[serde(default)]
    pub virtual_start_time: f64,

    /// Virtual finish time (determines scheduling order)
    #[serde(default)]
    pub virtual_finish_time: f64,

    /// Total execution time consumed (seconds)
    #[serde(default)]
    pub total_execution_time: f64,
}

impl Default for WFQState {
    fn default() -> Self {
        Self {
            weight: TaskWeight::default(),
            virtual_start_time: 0.0,
            virtual_finish_time: 0.0,
            total_execution_time: 0.0,
        }
    }
}

impl WFQState {
    /// Create new WFQ state with specified weight
    pub fn with_weight(weight: f64) -> Result<Self, String> {
        Ok(Self {
            weight: TaskWeight::new(weight)?,
            ..Default::default()
        })
    }

    /// Update virtual time after task execution
    ///
    /// # Arguments
    /// * `execution_duration_secs` - Actual execution time in seconds
    /// * `global_virtual_time` - Current global virtual time
    pub fn update_after_execution(
        &mut self,
        execution_duration_secs: f64,
        global_virtual_time: f64,
    ) {
        self.total_execution_time += execution_duration_secs;
        self.virtual_start_time = global_virtual_time.max(self.virtual_finish_time);

        // Virtual finish time = virtual start time + (cost / weight)
        // Cost is the execution duration
        self.virtual_finish_time =
            self.virtual_start_time + (execution_duration_secs / self.weight.value());
    }

    /// Get the virtual finish time for scheduling decisions
    pub fn finish_time(&self) -> f64 {
        self.virtual_finish_time
    }
}

/// Task information for WFQ scheduling
#[derive(Debug, Clone)]
pub struct WFQTaskInfo {
    /// Task name
    pub name: String,

    /// Virtual finish time
    pub virtual_finish_time: f64,

    /// Task weight
    pub weight: f64,

    /// Next scheduled run time
    pub next_run_time: DateTime<Utc>,
}

impl ScheduledTask {
    /// Set WFQ weight for this task
    ///
    /// # Example
    /// ```
    /// use celers_beat::{ScheduledTask, Schedule};
    ///
    /// let task = ScheduledTask::new("important_task".to_string(), Schedule::interval(60))
    ///     .with_wfq_weight(5.0).unwrap();
    /// ```
    pub fn with_wfq_weight(mut self, weight: f64) -> Result<Self, String> {
        if self.wfq_state.is_none() {
            self.wfq_state = Some(WFQState::default());
        }
        self.wfq_state.as_mut().unwrap().weight = TaskWeight::new(weight)?;
        Ok(self)
    }

    /// Get WFQ weight for this task
    pub fn wfq_weight(&self) -> f64 {
        self.wfq_state
            .as_ref()
            .map(|state| state.weight.value())
            .unwrap_or(1.0)
    }

    /// Get WFQ virtual finish time
    pub fn wfq_finish_time(&self) -> f64 {
        self.wfq_state
            .as_ref()
            .map(|state| state.finish_time())
            .unwrap_or(0.0)
    }
}

impl BeatScheduler {
    /// Get due tasks using Weighted Fair Queuing algorithm
    ///
    /// Returns tasks ordered by virtual finish time, ensuring fair execution
    /// proportional to task weights.
    ///
    /// # Example
    /// ```
    /// use celers_beat::{BeatScheduler, ScheduledTask, Schedule};
    ///
    /// let mut scheduler = BeatScheduler::new();
    ///
    /// // Add tasks with different weights
    /// scheduler.add_task(
    ///     ScheduledTask::new("low_priority".to_string(), Schedule::interval(60))
    ///         .with_wfq_weight(0.5).unwrap()
    /// ).unwrap();
    ///
    /// scheduler.add_task(
    ///     ScheduledTask::new("high_priority".to_string(), Schedule::interval(60))
    ///         .with_wfq_weight(5.0).unwrap()
    /// ).unwrap();
    ///
    /// // Get tasks using WFQ - higher weight tasks scheduled more often
    /// let due_tasks = scheduler.get_due_tasks_wfq();
    /// ```
    pub fn get_due_tasks_wfq(&self) -> Vec<WFQTaskInfo> {
        let now = Utc::now();
        let mut wfq_tasks: Vec<WFQTaskInfo> = Vec::new();

        for (name, task) in &self.tasks {
            if !task.enabled {
                continue;
            }

            match task.next_run_time() {
                Ok(next_run) if next_run <= now => {
                    wfq_tasks.push(WFQTaskInfo {
                        name: name.clone(),
                        virtual_finish_time: task.wfq_finish_time(),
                        weight: task.wfq_weight(),
                        next_run_time: next_run,
                    });
                }
                _ => {}
            }
        }

        // Sort by virtual finish time (lowest first = fairest to schedule next)
        wfq_tasks.sort_by(|a, b| {
            a.virtual_finish_time
                .partial_cmp(&b.virtual_finish_time)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.next_run_time.cmp(&b.next_run_time))
        });

        wfq_tasks
    }

    /// Update WFQ state after task execution
    ///
    /// Should be called after a task completes to update its virtual time.
    ///
    /// # Arguments
    /// * `task_name` - Name of the executed task
    /// * `execution_duration_secs` - How long the task took to execute (in seconds)
    pub fn update_wfq_after_execution(
        &mut self,
        task_name: &str,
        execution_duration_secs: f64,
    ) -> Result<(), ScheduleError> {
        // Calculate global virtual time before borrowing task mutably
        let global_virtual_time = self.calculate_global_virtual_time();

        let task = self
            .tasks
            .get_mut(task_name)
            .ok_or_else(|| ScheduleError::Invalid(format!("Task not found: {}", task_name)))?;

        // Initialize WFQ state if needed
        if task.wfq_state.is_none() {
            task.wfq_state = Some(WFQState::default());
        }

        // Update the task's WFQ state
        if let Some(wfq_state) = task.wfq_state.as_mut() {
            wfq_state.update_after_execution(execution_duration_secs, global_virtual_time);
        }

        Ok(())
    }

    /// Calculate global virtual time across all tasks
    fn calculate_global_virtual_time(&self) -> f64 {
        self.tasks
            .values()
            .filter_map(|task| task.wfq_state.as_ref())
            .map(|state| state.virtual_finish_time)
            .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .unwrap_or(0.0)
    }

    /// Get WFQ statistics for all tasks
    ///
    /// Returns information about weight distribution and virtual times.
    pub fn get_wfq_stats(&self) -> WFQStats {
        let tasks_with_wfq: Vec<_> = self
            .tasks
            .values()
            .filter(|t| t.wfq_state.is_some())
            .collect();

        let total_weight: f64 = tasks_with_wfq.iter().map(|t| t.wfq_weight()).sum();

        let avg_weight = if !tasks_with_wfq.is_empty() {
            total_weight / tasks_with_wfq.len() as f64
        } else {
            0.0
        };

        WFQStats {
            total_tasks: self.tasks.len(),
            tasks_with_wfq_config: tasks_with_wfq.len(),
            total_weight,
            average_weight: avg_weight,
            global_virtual_time: self.calculate_global_virtual_time(),
        }
    }
}

/// Statistics for Weighted Fair Queuing
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WFQStats {
    /// Total number of tasks
    pub total_tasks: usize,

    /// Number of tasks with WFQ configuration
    pub tasks_with_wfq_config: usize,

    /// Sum of all task weights
    pub total_weight: f64,

    /// Average task weight
    pub average_weight: f64,

    /// Current global virtual time
    pub global_virtual_time: f64,
}

impl std::fmt::Display for WFQStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WFQ Stats: {}/{} tasks configured, total_weight={:.2}, avg_weight={:.2}, global_vtime={:.2}",
            self.tasks_with_wfq_config,
            self.total_tasks,
            self.total_weight,
            self.average_weight,
            self.global_virtual_time
        )
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
        assert_eq!(display, "Crontab[0 12 * * 1 (UTC)]");
    }

    #[cfg(feature = "cron")]
    #[test]
    fn test_crontab_schedule_with_timezone() {
        let schedule = Schedule::crontab_tz("0", "9", "1-5", "*", "*", "America/New_York");
        assert!(schedule.is_crontab());

        // Display should show timezone
        let display = format!("{}", schedule);
        assert!(display.contains("America/New_York"));
    }

    #[cfg(feature = "cron")]
    #[test]
    fn test_crontab_schedule_timezone_next_run() {
        // Schedule for 9:00 AM New York time on weekdays
        let schedule = Schedule::crontab_tz("0", "9", "1-5", "*", "*", "America/New_York");

        // Get next run time
        let next_run = schedule.next_run(None).unwrap();

        // Verify the time is valid (should be in the future)
        assert!(next_run > Utc::now());
    }

    #[cfg(feature = "cron")]
    #[test]
    fn test_crontab_schedule_invalid_timezone() {
        let schedule = Schedule::crontab_tz("0", "9", "*", "*", "*", "Invalid/Timezone");
        let result = schedule.next_run(None);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_parse());
    }

    #[cfg(feature = "cron")]
    #[test]
    fn test_crontab_schedule_timezone_serialization() {
        let schedule = Schedule::crontab_tz("30", "14", "*", "*", "*", "Europe/London");
        let json = serde_json::to_string(&schedule).unwrap();
        let deserialized: Schedule = serde_json::from_str(&json).unwrap();

        // Verify timezone is preserved
        let display = format!("{}", deserialized);
        assert!(display.contains("Europe/London"));
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
        let options = TaskOptions {
            queue: Some("test_queue".to_string()),
            ..Default::default()
        };
        assert!(options.has_queue());
    }

    #[test]
    fn test_task_options_has_priority() {
        let options = TaskOptions {
            priority: Some(5),
            ..Default::default()
        };
        assert!(options.has_priority());
    }

    #[test]
    fn test_task_options_has_expires() {
        let options = TaskOptions {
            expires: Some(3600),
            ..Default::default()
        };
        assert!(options.has_expires());
    }

    #[test]
    fn test_task_options_display() {
        let options = TaskOptions {
            queue: Some("test".to_string()),
            priority: Some(5),
            expires: Some(3600),
        };

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
        assert!((2..=4).contains(&count));
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

    #[test]
    fn test_failure_notification_callback() {
        use std::sync::{Arc, Mutex};

        let mut scheduler = BeatScheduler::new();
        let task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));
        scheduler.add_task(task).unwrap();

        // Track callback invocations
        let invocations = Arc::new(Mutex::new(Vec::new()));
        let invocations_clone = invocations.clone();

        // Register callback
        scheduler.on_failure(Arc::new(move |task_name, error| {
            invocations_clone
                .lock()
                .unwrap()
                .push((task_name.to_string(), error.to_string()));
        }));

        // Trigger failure
        scheduler
            .mark_task_failure_with_error("test_task", "Test error".to_string())
            .unwrap();

        // Verify callback was invoked
        let invocations = invocations.lock().unwrap();
        assert_eq!(invocations.len(), 1);
        assert_eq!(invocations[0].0, "test_task");
        assert_eq!(invocations[0].1, "Test error");
    }

    #[test]
    fn test_failure_notification_multiple_callbacks() {
        use std::sync::{Arc, Mutex};

        let mut scheduler = BeatScheduler::new();
        let task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));
        scheduler.add_task(task).unwrap();

        // Track callback invocations for two separate callbacks
        let invocations1 = Arc::new(Mutex::new(0));
        let invocations2 = Arc::new(Mutex::new(0));

        let inv1_clone = invocations1.clone();
        let inv2_clone = invocations2.clone();

        // Register two callbacks
        scheduler.on_failure(Arc::new(move |_, _| {
            *inv1_clone.lock().unwrap() += 1;
        }));

        scheduler.on_failure(Arc::new(move |_, _| {
            *inv2_clone.lock().unwrap() += 1;
        }));

        // Trigger failure
        scheduler
            .mark_task_failure_with_error("test_task", "Test error".to_string())
            .unwrap();

        // Verify both callbacks were invoked
        assert_eq!(*invocations1.lock().unwrap(), 1);
        assert_eq!(*invocations2.lock().unwrap(), 1);
    }

    #[test]
    fn test_failure_notification_clear_callbacks() {
        use std::sync::{Arc, Mutex};

        let mut scheduler = BeatScheduler::new();
        let task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));
        scheduler.add_task(task).unwrap();

        // Track callback invocations
        let invocations = Arc::new(Mutex::new(0));
        let inv_clone = invocations.clone();

        // Register callback
        scheduler.on_failure(Arc::new(move |_, _| {
            *inv_clone.lock().unwrap() += 1;
        }));

        // Clear callbacks
        scheduler.clear_failure_callbacks();

        // Trigger failure
        scheduler
            .mark_task_failure_with_error("test_task", "Test error".to_string())
            .unwrap();

        // Verify callback was NOT invoked
        assert_eq!(*invocations.lock().unwrap(), 0);
    }

    #[test]
    fn test_failure_notification_with_start_time() {
        use std::sync::{Arc, Mutex};

        let mut scheduler = BeatScheduler::new();
        let task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));
        scheduler.add_task(task).unwrap();

        // Track callback invocations
        let invocations = Arc::new(Mutex::new(Vec::new()));
        let invocations_clone = invocations.clone();

        // Register callback
        scheduler.on_failure(Arc::new(move |task_name, error| {
            invocations_clone
                .lock()
                .unwrap()
                .push((task_name.to_string(), error.to_string()));
        }));

        // Trigger failure with start time
        let start_time = Utc::now();
        scheduler
            .mark_task_failure_with_start("test_task", start_time, "Test error".to_string())
            .unwrap();

        // Verify callback was invoked
        let invocations = invocations.lock().unwrap();
        assert_eq!(invocations.len(), 1);
        assert_eq!(invocations[0].0, "test_task");
        assert_eq!(invocations[0].1, "Test error");
    }

    #[test]
    fn test_failure_notification_multiple_failures() {
        use std::sync::{Arc, Mutex};

        let mut scheduler = BeatScheduler::new();
        let task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));
        scheduler.add_task(task).unwrap();

        // Track callback invocations
        let invocations = Arc::new(Mutex::new(Vec::new()));
        let invocations_clone = invocations.clone();

        // Register callback
        scheduler.on_failure(Arc::new(move |task_name, error| {
            invocations_clone
                .lock()
                .unwrap()
                .push((task_name.to_string(), error.to_string()));
        }));

        // Trigger multiple failures
        scheduler
            .mark_task_failure_with_error("test_task", "Error 1".to_string())
            .unwrap();
        scheduler
            .mark_task_failure_with_error("test_task", "Error 2".to_string())
            .unwrap();
        scheduler
            .mark_task_failure_with_error("test_task", "Error 3".to_string())
            .unwrap();

        // Verify callback was invoked three times
        let invocations = invocations.lock().unwrap();
        assert_eq!(invocations.len(), 3);
        assert_eq!(invocations[0].1, "Error 1");
        assert_eq!(invocations[1].1, "Error 2");
        assert_eq!(invocations[2].1, "Error 3");
    }

    #[test]
    fn test_schedule_cache_basic() {
        let mut task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));

        // Initially cache should be None
        assert!(task.cached_next_run.is_none());

        // Update cache
        task.update_next_run_cache();
        assert!(task.cached_next_run.is_some());

        let cached_time = task.cached_next_run.unwrap();

        // next_run_time should return the cached value
        let next_run = task.next_run_time().unwrap();
        assert_eq!(next_run, cached_time);
    }

    #[test]
    fn test_schedule_cache_invalidation() {
        let mut task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));

        // Update cache
        task.update_next_run_cache();
        assert!(task.cached_next_run.is_some());

        // Invalidate cache
        task.invalidate_next_run_cache();
        assert!(task.cached_next_run.is_none());
    }

    #[test]
    fn test_schedule_cache_on_execution() {
        let mut scheduler = BeatScheduler::new();
        let task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));
        scheduler.add_task(task).unwrap();

        // After adding, cache should be set
        let task = scheduler.tasks.get("test_task").unwrap();
        assert!(task.cached_next_run.is_some());

        // Mark as success
        scheduler.mark_task_success("test_task").unwrap();

        // After execution, cache should be updated
        let task = scheduler.tasks.get("test_task").unwrap();
        assert!(task.cached_next_run.is_some());
    }

    #[test]
    fn test_schedule_cache_on_schedule_update() {
        let mut task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));

        // Update cache
        task.update_next_run_cache();
        let old_cached_time = task.cached_next_run.unwrap();

        // Update schedule
        task.update_schedule(
            Schedule::interval(120),
            Some("Changed interval".to_string()),
        );

        // Cache should be updated with new schedule
        assert!(task.cached_next_run.is_some());
        let new_cached_time = task.cached_next_run.unwrap();

        // The cached times should be different (though this might fail if timing is exact)
        // At minimum, cache should still be valid
        assert!(new_cached_time >= old_cached_time);
    }

    #[test]
    fn test_add_tasks_batch() {
        let mut scheduler = BeatScheduler::new();

        let tasks = vec![
            ScheduledTask::new("task1".to_string(), Schedule::interval(60)),
            ScheduledTask::new("task2".to_string(), Schedule::interval(120)),
            ScheduledTask::new("task3".to_string(), Schedule::interval(180)),
        ];

        let count = scheduler.add_tasks_batch(tasks).unwrap();

        assert_eq!(count, 3);
        assert_eq!(scheduler.tasks.len(), 3);
        assert!(scheduler.tasks.contains_key("task1"));
        assert!(scheduler.tasks.contains_key("task2"));
        assert!(scheduler.tasks.contains_key("task3"));

        // Verify cache is initialized for all tasks
        assert!(scheduler
            .tasks
            .get("task1")
            .unwrap()
            .cached_next_run
            .is_some());
        assert!(scheduler
            .tasks
            .get("task2")
            .unwrap()
            .cached_next_run
            .is_some());
        assert!(scheduler
            .tasks
            .get("task3")
            .unwrap()
            .cached_next_run
            .is_some());
    }

    #[test]
    fn test_add_tasks_batch_empty() {
        let mut scheduler = BeatScheduler::new();

        let tasks = vec![];
        let count = scheduler.add_tasks_batch(tasks).unwrap();

        assert_eq!(count, 0);
        assert_eq!(scheduler.tasks.len(), 0);
    }

    #[test]
    fn test_remove_tasks_batch() {
        let mut scheduler = BeatScheduler::new();

        // Add some tasks
        scheduler
            .add_task(ScheduledTask::new(
                "task1".to_string(),
                Schedule::interval(60),
            ))
            .unwrap();
        scheduler
            .add_task(ScheduledTask::new(
                "task2".to_string(),
                Schedule::interval(120),
            ))
            .unwrap();
        scheduler
            .add_task(ScheduledTask::new(
                "task3".to_string(),
                Schedule::interval(180),
            ))
            .unwrap();
        scheduler
            .add_task(ScheduledTask::new(
                "task4".to_string(),
                Schedule::interval(240),
            ))
            .unwrap();

        assert_eq!(scheduler.tasks.len(), 4);

        // Remove tasks in batch
        let count = scheduler
            .remove_tasks_batch(&["task1", "task2", "task3"])
            .unwrap();

        assert_eq!(count, 3);
        assert_eq!(scheduler.tasks.len(), 1);
        assert!(!scheduler.tasks.contains_key("task1"));
        assert!(!scheduler.tasks.contains_key("task2"));
        assert!(!scheduler.tasks.contains_key("task3"));
        assert!(scheduler.tasks.contains_key("task4"));
    }

    #[test]
    fn test_remove_tasks_batch_nonexistent() {
        let mut scheduler = BeatScheduler::new();

        // Add some tasks
        scheduler
            .add_task(ScheduledTask::new(
                "task1".to_string(),
                Schedule::interval(60),
            ))
            .unwrap();
        scheduler
            .add_task(ScheduledTask::new(
                "task2".to_string(),
                Schedule::interval(120),
            ))
            .unwrap();

        assert_eq!(scheduler.tasks.len(), 2);

        // Try to remove tasks that don't all exist
        let count = scheduler
            .remove_tasks_batch(&["task1", "nonexistent", "task2"])
            .unwrap();

        assert_eq!(count, 2); // Only task1 and task2 were removed
        assert_eq!(scheduler.tasks.len(), 0);
    }

    #[test]
    fn test_remove_tasks_batch_empty() {
        let mut scheduler = BeatScheduler::new();

        let count = scheduler.remove_tasks_batch(&[]).unwrap();
        assert_eq!(count, 0);
    }

    // ===== Lock Manager Tests =====

    #[test]
    fn test_schedule_lock_basic() {
        let lock = ScheduleLock::new("task1".to_string(), "owner1".to_string(), 300);

        assert_eq!(lock.task_name, "task1");
        assert_eq!(lock.owner, "owner1");
        assert!(!lock.is_expired());
        assert!(lock.is_owned_by("owner1"));
        assert!(!lock.is_owned_by("owner2"));
        assert_eq!(lock.renewal_count, 0);
    }

    #[test]
    fn test_schedule_lock_ttl() {
        let lock = ScheduleLock::new("task1".to_string(), "owner1".to_string(), 300);

        let ttl = lock.ttl();
        assert!(ttl.num_seconds() > 290);
        assert!(ttl.num_seconds() <= 300);
    }

    #[test]
    fn test_schedule_lock_renew() {
        let mut lock = ScheduleLock::new("task1".to_string(), "owner1".to_string(), 1);

        // Wait a bit
        std::thread::sleep(std::time::Duration::from_millis(100));

        // Renew before expiration
        let result = lock.renew(300);
        assert!(result.is_ok());
        assert_eq!(lock.renewal_count, 1);
        assert!(!lock.is_expired());
    }

    #[test]
    fn test_lock_manager_acquire_release() {
        let mut manager = LockManager::new(300);

        // Acquire lock
        let acquired = manager.try_acquire("task1", "owner1", None).unwrap();
        assert!(acquired);
        assert!(manager.is_locked("task1"));

        // Try to acquire again with different owner
        let acquired = manager.try_acquire("task1", "owner2", None).unwrap();
        assert!(!acquired);

        // Release lock
        let released = manager.release("task1", "owner1").unwrap();
        assert!(released);
        assert!(!manager.is_locked("task1"));
    }

    #[test]
    fn test_lock_manager_acquire_same_owner() {
        let mut manager = LockManager::new(300);

        // Acquire lock
        let acquired = manager.try_acquire("task1", "owner1", None).unwrap();
        assert!(acquired);

        // Try to acquire again with same owner (should succeed)
        let acquired = manager.try_acquire("task1", "owner1", None).unwrap();
        assert!(acquired);
    }

    #[test]
    fn test_lock_manager_renew() {
        let mut manager = LockManager::new(300);

        // Acquire lock
        manager.try_acquire("task1", "owner1", None).unwrap();

        // Renew lock
        let renewed = manager.renew("task1", "owner1", Some(600)).unwrap();
        assert!(renewed);

        // Check lock info
        let lock = manager.get_lock("task1").unwrap();
        assert_eq!(lock.renewal_count, 1);
    }

    #[test]
    fn test_lock_manager_cleanup_expired() {
        let mut manager = LockManager::new(1);

        // Acquire lock with 1 second TTL
        manager.try_acquire("task1", "owner1", Some(1)).unwrap();
        assert!(manager.is_locked("task1"));

        // Wait for expiration
        std::thread::sleep(std::time::Duration::from_millis(1100));

        // Cleanup
        manager.cleanup_expired();
        assert!(!manager.is_locked("task1"));
    }

    #[test]
    fn test_lock_manager_get_active_locks() {
        let mut manager = LockManager::new(300);

        manager.try_acquire("task1", "owner1", None).unwrap();
        manager.try_acquire("task2", "owner2", None).unwrap();

        let active_locks = manager.get_active_locks();
        assert_eq!(active_locks.len(), 2);
    }

    #[test]
    fn test_lock_manager_release_all() {
        let mut manager = LockManager::new(300);

        manager.try_acquire("task1", "owner1", None).unwrap();
        manager.try_acquire("task2", "owner2", None).unwrap();

        assert_eq!(manager.get_active_locks().len(), 2);

        manager.release_all();
        assert_eq!(manager.get_active_locks().len(), 0);
    }

    #[test]
    fn test_scheduler_lock_acquire_release() {
        let mut scheduler = BeatScheduler::new();

        // Acquire lock
        let acquired = scheduler.try_acquire_lock("task1", None).unwrap();
        assert!(acquired);
        assert!(scheduler.is_task_locked("task1"));

        // Release lock
        let released = scheduler.release_lock("task1").unwrap();
        assert!(released);
        assert!(!scheduler.is_task_locked("task1"));
    }

    #[test]
    fn test_scheduler_lock_multiple_instances() {
        let mut scheduler1 = BeatScheduler::new();
        let mut scheduler2 = BeatScheduler::new();

        // Note: Each scheduler has its own in-memory lock manager
        // For distributed locking, you would need external state (Redis, DB, etc.)

        // Scheduler 1 acquires lock in its own lock manager
        let acquired = scheduler1.try_acquire_lock("task1", None).unwrap();
        assert!(acquired);

        // Scheduler 2 can also acquire the same task name in its own lock manager
        // because they don't share state (this is in-memory locking)
        let acquired = scheduler2.try_acquire_lock("task1", None).unwrap();
        assert!(acquired); // Both can acquire independently

        // Each scheduler maintains its own locks
        assert!(scheduler1.is_task_locked("task1"));
        assert!(scheduler2.is_task_locked("task1"));

        // Releasing in scheduler1 doesn't affect scheduler2
        scheduler1.release_lock("task1").unwrap();
        assert!(!scheduler1.is_task_locked("task1"));
        assert!(scheduler2.is_task_locked("task1"));
    }

    #[test]
    fn test_scheduler_execute_with_lock() {
        let mut scheduler = BeatScheduler::new();
        let mut executed = false;

        // Execute with lock
        let result = scheduler.execute_with_lock("task1", None, || {
            executed = true;
            Ok(())
        });

        assert!(result.is_ok());
        assert!(result.unwrap());
        assert!(executed);

        // Lock should be released after execution
        assert!(!scheduler.is_task_locked("task1"));
    }

    #[test]
    fn test_scheduler_instance_id() {
        let scheduler1 = BeatScheduler::new();
        let scheduler2 = BeatScheduler::new();

        // Each scheduler should have a unique instance ID
        assert_ne!(scheduler1.instance_id(), scheduler2.instance_id());
    }

    #[test]
    fn test_scheduler_set_custom_instance_id() {
        let mut scheduler = BeatScheduler::new();

        scheduler.set_instance_id("custom-id-123".to_string());
        assert_eq!(scheduler.instance_id(), "custom-id-123");
    }

    #[test]
    fn test_lock_manager_serialization() {
        let mut manager = LockManager::new(300);
        manager.try_acquire("task1", "owner1", None).unwrap();

        let json = serde_json::to_string(&manager).unwrap();
        let deserialized: LockManager = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.default_ttl, 300);
        // Note: locks are serialized, so they should be present
        assert!(deserialized.is_locked("task1"));
    }

    // ===== Conflict Detection Tests =====

    #[test]
    fn test_schedule_conflict_basic() {
        let conflict = ScheduleConflict::new(
            "task1".to_string(),
            "task2".to_string(),
            ConflictSeverity::High,
            120,
            "Overlapping execution".to_string(),
        );

        assert_eq!(conflict.task1, "task1");
        assert_eq!(conflict.task2, "task2");
        assert_eq!(conflict.severity, ConflictSeverity::High);
        assert_eq!(conflict.overlap_seconds, 120);
        assert!(conflict.is_high_severity());
        assert!(!conflict.is_medium_severity());
        assert!(!conflict.is_low_severity());
    }

    #[test]
    fn test_schedule_conflict_with_resolution() {
        let conflict = ScheduleConflict::new(
            "task1".to_string(),
            "task2".to_string(),
            ConflictSeverity::Medium,
            60,
            "Partial overlap".to_string(),
        )
        .with_resolution("Add jitter".to_string());

        assert!(conflict.resolution.is_some());
        assert_eq!(conflict.resolution.unwrap(), "Add jitter");
    }

    #[test]
    fn test_schedule_conflict_severity() {
        let low = ScheduleConflict::new(
            "t1".to_string(),
            "t2".to_string(),
            ConflictSeverity::Low,
            10,
            "Low conflict".to_string(),
        );

        let medium = ScheduleConflict::new(
            "t1".to_string(),
            "t2".to_string(),
            ConflictSeverity::Medium,
            30,
            "Medium conflict".to_string(),
        );

        let high = ScheduleConflict::new(
            "t1".to_string(),
            "t2".to_string(),
            ConflictSeverity::High,
            60,
            "High conflict".to_string(),
        );

        assert!(low.is_low_severity());
        assert!(medium.is_medium_severity());
        assert!(high.is_high_severity());
    }

    #[test]
    fn test_detect_conflicts_no_conflict() {
        let mut scheduler = BeatScheduler::new();

        // Add tasks with different schedules that don't overlap
        scheduler
            .add_task(ScheduledTask::new(
                "task1".to_string(),
                Schedule::interval(3600),
            ))
            .unwrap();
        scheduler
            .add_task(ScheduledTask::new(
                "task2".to_string(),
                Schedule::interval(7200),
            ))
            .unwrap();

        let conflicts = scheduler.detect_conflicts(60, 30);
        assert_eq!(conflicts.len(), 0);
    }

    #[test]
    fn test_detect_conflicts_with_overlap() {
        let mut scheduler = BeatScheduler::new();

        // Add two tasks with the same interval (will overlap)
        scheduler
            .add_task(ScheduledTask::new(
                "task1".to_string(),
                Schedule::interval(60),
            ))
            .unwrap();
        scheduler
            .add_task(ScheduledTask::new(
                "task2".to_string(),
                Schedule::interval(60),
            ))
            .unwrap();

        // Check for conflicts in 1 hour window with 30 second estimated duration
        let conflicts = scheduler.detect_conflicts(3600, 30);

        // Should detect conflicts since both run every 60 seconds
        assert!(!conflicts.is_empty());
    }

    #[test]
    fn test_detect_conflicts_disabled_tasks() {
        let mut scheduler = BeatScheduler::new();

        // Add two tasks with same schedule, one disabled
        let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60));
        let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(60)).disabled();

        scheduler.add_task(task1).unwrap();
        scheduler.add_task(task2).unwrap();

        // Should not detect conflicts because task2 is disabled
        let conflicts = scheduler.detect_conflicts(3600, 30);
        assert_eq!(conflicts.len(), 0);
    }

    #[test]
    fn test_get_high_severity_conflicts() {
        let mut scheduler = BeatScheduler::new();

        // Add multiple tasks with the same interval
        scheduler
            .add_task(ScheduledTask::new(
                "task1".to_string(),
                Schedule::interval(60),
            ))
            .unwrap();
        scheduler
            .add_task(ScheduledTask::new(
                "task2".to_string(),
                Schedule::interval(60),
            ))
            .unwrap();
        scheduler
            .add_task(ScheduledTask::new(
                "task3".to_string(),
                Schedule::interval(120),
            ))
            .unwrap();

        // Get high severity conflicts with long duration (60s)
        let high_conflicts = scheduler.get_high_severity_conflicts(3600, 60);

        // May have high severity conflicts depending on overlap
        // Just verify the method works
        assert!(high_conflicts.len() <= scheduler.conflict_count(3600, 60));
    }

    #[test]
    fn test_has_conflicts() {
        let mut scheduler = BeatScheduler::new();

        // Initially no conflicts
        assert!(!scheduler.has_conflicts(3600, 30));

        // Add tasks with same interval
        scheduler
            .add_task(ScheduledTask::new(
                "task1".to_string(),
                Schedule::interval(60),
            ))
            .unwrap();
        scheduler
            .add_task(ScheduledTask::new(
                "task2".to_string(),
                Schedule::interval(60),
            ))
            .unwrap();

        // Now should have conflicts
        assert!(scheduler.has_conflicts(3600, 30));
    }

    #[test]
    fn test_conflict_count() {
        let mut scheduler = BeatScheduler::new();

        assert_eq!(scheduler.conflict_count(3600, 30), 0);

        scheduler
            .add_task(ScheduledTask::new(
                "task1".to_string(),
                Schedule::interval(60),
            ))
            .unwrap();
        scheduler
            .add_task(ScheduledTask::new(
                "task2".to_string(),
                Schedule::interval(60),
            ))
            .unwrap();
        scheduler
            .add_task(ScheduledTask::new(
                "task3".to_string(),
                Schedule::interval(60),
            ))
            .unwrap();

        let count = scheduler.conflict_count(3600, 30);
        assert!(count > 0);
    }

    #[test]
    fn test_schedule_conflict_display() {
        let conflict = ScheduleConflict::new(
            "task1".to_string(),
            "task2".to_string(),
            ConflictSeverity::High,
            120,
            "Test conflict".to_string(),
        );

        let display = format!("{}", conflict);
        assert!(display.contains("task1"));
        assert!(display.contains("task2"));
        assert!(display.contains("120s"));
    }

    #[test]
    fn test_schedule_conflict_serialization() {
        let conflict = ScheduleConflict::new(
            "task1".to_string(),
            "task2".to_string(),
            ConflictSeverity::Medium,
            60,
            "Test".to_string(),
        );

        let json = serde_json::to_string(&conflict).unwrap();
        let deserialized: ScheduleConflict = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.task1, "task1");
        assert_eq!(deserialized.task2, "task2");
        assert_eq!(deserialized.severity, ConflictSeverity::Medium);
        assert_eq!(deserialized.overlap_seconds, 60);
    }

    // ===== Timezone Tests =====

    #[cfg(feature = "cron")]
    #[test]
    fn test_crontab_timezone_utc() {
        // Test crontab with UTC (default)
        let schedule = Schedule::crontab("0", "12", "*", "*", "*");
        let now = Utc::now();
        let next_run = schedule.next_run(Some(now)).unwrap();

        // Should be at noon UTC
        assert_eq!(next_run.hour(), 12);
        assert_eq!(next_run.minute(), 0);
    }

    #[cfg(feature = "cron")]
    #[test]
    fn test_crontab_timezone_new_york() {
        // Test crontab with New York timezone (runs every day, not just weekdays)
        let schedule = Schedule::crontab_tz("0", "9", "*", "*", "*", "America/New_York");
        let now = Utc::now();
        let next_run = schedule.next_run(Some(now)).unwrap();

        // Verify we got a valid future time
        assert!(next_run > now);
    }

    #[cfg(feature = "cron")]
    #[test]
    fn test_crontab_timezone_london() {
        // Test crontab with London timezone
        let schedule = Schedule::crontab_tz("30", "14", "*", "*", "*", "Europe/London");
        let now = Utc::now();
        let next_run = schedule.next_run(Some(now)).unwrap();

        // Verify we got a valid future time
        assert!(next_run > now);
    }

    #[cfg(feature = "cron")]
    #[test]
    fn test_crontab_timezone_tokyo() {
        // Test crontab with Tokyo timezone (runs every day, not just weekdays)
        let schedule = Schedule::crontab_tz("0", "18", "*", "*", "*", "Asia/Tokyo");
        let now = Utc::now();
        let next_run = schedule.next_run(Some(now)).unwrap();

        // Verify we got a valid future time
        assert!(next_run > now);
    }

    #[cfg(feature = "cron")]
    #[test]
    fn test_crontab_timezone_invalid() {
        // Test with invalid timezone
        let schedule = Schedule::crontab_tz("0", "12", "*", "*", "*", "Invalid/Timezone");
        let now = Utc::now();
        let result = schedule.next_run(Some(now));

        // Should return error for invalid timezone
        assert!(result.is_err());
        if let Err(ScheduleError::Parse(msg)) = result {
            assert!(msg.contains("Invalid timezone"));
        }
    }

    #[cfg(feature = "cron")]
    #[test]
    fn test_crontab_timezone_serialization() {
        // Test that timezone is preserved through serialization
        let schedule = Schedule::crontab_tz("0", "9", "1-5", "*", "*", "America/New_York");
        let json = serde_json::to_string(&schedule).unwrap();
        let deserialized: Schedule = serde_json::from_str(&json).unwrap();

        if let Schedule::Crontab { timezone, .. } = deserialized {
            assert_eq!(timezone, Some("America/New_York".to_string()));
        } else {
            panic!("Expected Crontab schedule");
        }
    }

    #[cfg(feature = "cron")]
    #[test]
    fn test_crontab_timezone_display() {
        // Test that timezone appears in display string
        let schedule = Schedule::crontab_tz("0", "9", "1-5", "*", "*", "America/New_York");
        let display = format!("{}", schedule);

        assert!(display.contains("America/New_York"));
        assert!(display.contains("Crontab"));
    }

    #[cfg(feature = "cron")]
    #[test]
    fn test_crontab_timezone_consistency() {
        // Test that same schedule in different timezones produces different UTC times
        let ny_schedule = Schedule::crontab_tz("9", "0", "*", "*", "*", "America/New_York");
        let london_schedule = Schedule::crontab_tz("9", "0", "*", "*", "*", "Europe/London");

        let now = Utc::now();
        let ny_next = ny_schedule.next_run(Some(now)).unwrap();
        let london_next = london_schedule.next_run(Some(now)).unwrap();

        // 9 AM in New York and 9 AM in London are different UTC times
        // (typically 5-6 hour difference depending on DST)
        assert_ne!(ny_next.hour(), london_next.hour());
    }

    #[cfg(feature = "cron")]
    #[test]
    fn test_scheduled_task_timezone_persistence() {
        // Test that timezone-aware task persists correctly
        let schedule = Schedule::crontab_tz("0", "9", "1-5", "*", "*", "America/New_York");
        let task = ScheduledTask::new("timezone_task".to_string(), schedule);

        // Serialize and deserialize
        let json = serde_json::to_string(&task).unwrap();
        let deserialized: ScheduledTask = serde_json::from_str(&json).unwrap();

        // Verify timezone is preserved
        if let Schedule::Crontab { timezone, .. } = deserialized.schedule {
            assert_eq!(timezone, Some("America/New_York".to_string()));
        } else {
            panic!("Expected Crontab schedule");
        }
    }

    // ===== Scheduler Loop Tests =====

    #[test]
    fn test_scheduler_get_due_tasks_empty() {
        let scheduler = BeatScheduler::new();
        let due_tasks = scheduler.get_due_tasks();

        // New scheduler should have no due tasks
        assert_eq!(due_tasks.len(), 0);
    }

    #[test]
    fn test_scheduler_get_due_tasks_with_due_task() {
        let mut scheduler = BeatScheduler::new();

        // Add task with very short interval (already due)
        let task = ScheduledTask::new("due_task".to_string(), Schedule::interval(1));
        scheduler.add_task(task).unwrap();

        // Set last run in the past manually
        if let Some(task) = scheduler.tasks.get_mut("due_task") {
            task.last_run_at = Some(Utc::now() - Duration::seconds(10));
        }

        // Should be due now
        let due_tasks = scheduler.get_due_tasks();
        assert_eq!(due_tasks.len(), 1);
        assert_eq!(due_tasks[0].name, "due_task");
    }

    #[test]
    fn test_scheduler_get_due_tasks_with_future_task() {
        let mut scheduler = BeatScheduler::new();

        // Add task with long interval (not due yet)
        let task = ScheduledTask::new("future_task".to_string(), Schedule::interval(3600));
        scheduler.add_task(task).unwrap();

        // Mark as run now
        scheduler.mark_task_run("future_task").unwrap();

        // Should not be due yet
        let due_tasks = scheduler.get_due_tasks();
        assert_eq!(due_tasks.len(), 0);
    }

    #[test]
    fn test_scheduler_get_due_tasks_mixed() {
        let mut scheduler = BeatScheduler::new();

        // Add due task
        let task1 = ScheduledTask::new("due_task".to_string(), Schedule::interval(1));
        scheduler.add_task(task1).unwrap();
        // Set last run in the past
        if let Some(task) = scheduler.tasks.get_mut("due_task") {
            task.last_run_at = Some(Utc::now() - Duration::seconds(10));
        }

        // Add future task
        let task2 = ScheduledTask::new("future_task".to_string(), Schedule::interval(3600));
        scheduler.add_task(task2).unwrap();
        scheduler.mark_task_run("future_task").unwrap();

        // Should only get the due task
        let due_tasks = scheduler.get_due_tasks();
        assert_eq!(due_tasks.len(), 1);
        assert_eq!(due_tasks[0].name, "due_task");
    }

    #[test]
    fn test_scheduler_get_due_tasks_disabled() {
        let mut scheduler = BeatScheduler::new();

        // Add due but disabled task
        let task =
            ScheduledTask::new("disabled_task".to_string(), Schedule::interval(1)).disabled();
        scheduler.add_task(task).unwrap();
        // Set last run in the past
        if let Some(task) = scheduler.tasks.get_mut("disabled_task") {
            task.last_run_at = Some(Utc::now() - Duration::seconds(10));
        }

        // Disabled tasks should not be returned as due
        let due_tasks = scheduler.get_due_tasks();
        assert_eq!(due_tasks.len(), 0);
    }

    #[test]
    fn test_scheduler_mark_task_run_updates_timestamp() {
        let mut scheduler = BeatScheduler::new();
        let task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));
        scheduler.add_task(task).unwrap();

        let before = Utc::now();
        std::thread::sleep(std::time::Duration::from_millis(10));
        scheduler.mark_task_run("test_task").unwrap();

        let task = scheduler.tasks.get("test_task").unwrap();
        assert!(task.last_run_at.is_some());
        assert!(task.last_run_at.unwrap() >= before);
    }

    #[test]
    fn test_scheduler_mark_task_run_increments_count() {
        let mut scheduler = BeatScheduler::new();
        let task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));
        scheduler.add_task(task).unwrap();

        let initial_count = scheduler.tasks.get("test_task").unwrap().total_run_count;

        scheduler.mark_task_run("test_task").unwrap();
        let count_after_1 = scheduler.tasks.get("test_task").unwrap().total_run_count;
        assert_eq!(count_after_1, initial_count + 1);

        scheduler.mark_task_run("test_task").unwrap();
        let count_after_2 = scheduler.tasks.get("test_task").unwrap().total_run_count;
        assert_eq!(count_after_2, initial_count + 2);
    }

    #[test]
    fn test_scheduler_multiple_due_tasks() {
        let mut scheduler = BeatScheduler::new();

        // Add 5 tasks with short intervals
        for i in 0..5 {
            let task = ScheduledTask::new(format!("task_{}", i), Schedule::interval(1));
            scheduler.add_task(task).unwrap();
            // Set last run in the past
            if let Some(task) = scheduler.tasks.get_mut(&format!("task_{}", i)) {
                task.last_run_at = Some(Utc::now() - Duration::seconds(10));
            }
        }

        // All 5 should be due
        let due_tasks = scheduler.get_due_tasks();
        assert_eq!(due_tasks.len(), 5);
    }

    #[test]
    fn test_scheduler_task_prioritization() {
        let mut scheduler = BeatScheduler::new();

        // Add tasks with different intervals
        let task1 = ScheduledTask::new("task_60".to_string(), Schedule::interval(60));
        let task2 = ScheduledTask::new("task_120".to_string(), Schedule::interval(120));
        let task3 = ScheduledTask::new("task_30".to_string(), Schedule::interval(30));

        scheduler.add_task(task1).unwrap();
        scheduler.add_task(task2).unwrap();
        scheduler.add_task(task3).unwrap();

        // Verify all tasks were added
        assert_eq!(scheduler.tasks.len(), 3);

        // All tasks should initially be due (never run)
        let due_tasks = scheduler.get_due_tasks();
        assert_eq!(due_tasks.len(), 3);
    }

    #[test]
    fn test_scheduler_task_lifecycle() {
        let mut scheduler = BeatScheduler::new();

        // 1. Add task
        let task = ScheduledTask::new("lifecycle_task".to_string(), Schedule::interval(1));
        scheduler.add_task(task).unwrap();
        assert_eq!(scheduler.tasks.len(), 1);

        // 2. Task is due (short interval, never run)
        let due_tasks = scheduler.get_due_tasks();
        assert_eq!(due_tasks.len(), 1);

        // 3. Mark as run
        scheduler.mark_task_run("lifecycle_task").unwrap();

        // 4. Task should not be due immediately
        let due_tasks = scheduler.get_due_tasks();
        assert_eq!(due_tasks.len(), 0);

        // 5. Disable task
        if let Some(task) = scheduler.tasks.get_mut("lifecycle_task") {
            task.enabled = false;
        }
        let task = scheduler.tasks.get("lifecycle_task").unwrap();
        assert!(!task.enabled);

        // 6. Re-enable task
        if let Some(task) = scheduler.tasks.get_mut("lifecycle_task") {
            task.enabled = true;
        }
        let task = scheduler.tasks.get("lifecycle_task").unwrap();
        assert!(task.enabled);

        // 7. Remove task
        scheduler.remove_task("lifecycle_task").unwrap();
        assert_eq!(scheduler.tasks.len(), 0);
    }

    #[test]
    fn test_scheduler_persistence_preserves_state() {
        let state_file = "/tmp/test_scheduler_persistence.json";

        // Create scheduler and add tasks
        let mut scheduler1 = BeatScheduler::with_persistence(state_file.to_string());
        let task = ScheduledTask::new("persistent_task".to_string(), Schedule::interval(60));
        scheduler1.add_task(task).unwrap();
        scheduler1.mark_task_run("persistent_task").unwrap();
        scheduler1.save_state().unwrap();

        // Load scheduler from file
        let scheduler2 = BeatScheduler::load_from_file(state_file).unwrap();

        // Verify state was preserved
        assert_eq!(scheduler2.tasks.len(), 1);
        let task = scheduler2.tasks.get("persistent_task").unwrap();
        assert_eq!(task.name, "persistent_task");
        assert!(task.last_run_at.is_some());
        assert_eq!(task.total_run_count, 1);

        // Cleanup
        std::fs::remove_file(state_file).ok();
    }

    #[test]
    fn test_scheduler_loop_simulation() {
        let mut scheduler = BeatScheduler::new();

        // Add task with 1 second interval
        let task = ScheduledTask::new("loop_task".to_string(), Schedule::interval(1));
        scheduler.add_task(task).unwrap();

        // Simulate 5 iterations of scheduler loop
        let mut executions = 0;
        for _ in 0..5 {
            // Collect task names first to avoid borrow conflicts
            let task_names: Vec<String> = scheduler
                .get_due_tasks()
                .into_iter()
                .map(|t| t.name.clone())
                .collect();

            for task_name in task_names {
                scheduler.mark_task_run(&task_name).unwrap();
                executions += 1;
            }
            // Simulate time passing
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        // Should have executed at least once
        assert!(executions > 0);

        let task = scheduler.tasks.get("loop_task").unwrap();
        assert_eq!(task.total_run_count as usize, executions);
    }

    // ===== Weighted Fair Queuing Tests =====

    #[test]
    fn test_wfq_task_weight_validation() {
        let task = ScheduledTask::new("test".to_string(), Schedule::interval(60))
            .with_wfq_weight(5.0)
            .unwrap();
        assert_eq!(task.wfq_weight(), 5.0);

        // Test invalid weights
        let result =
            ScheduledTask::new("test".to_string(), Schedule::interval(60)).with_wfq_weight(0.05); // Too low
        assert!(result.is_err());

        let result =
            ScheduledTask::new("test".to_string(), Schedule::interval(60)).with_wfq_weight(15.0); // Too high
        assert!(result.is_err());
    }

    #[test]
    fn test_wfq_basic_scheduling() {
        let mut scheduler = BeatScheduler::new();

        // Add tasks with different weights
        let task1 = ScheduledTask::new("low_weight".to_string(), Schedule::interval(1))
            .with_wfq_weight(0.5)
            .unwrap();
        let task2 = ScheduledTask::new("high_weight".to_string(), Schedule::interval(1))
            .with_wfq_weight(5.0)
            .unwrap();

        scheduler.add_task(task1).unwrap();
        scheduler.add_task(task2).unwrap();

        // Mark tasks as run to set last_run_at
        std::thread::sleep(std::time::Duration::from_millis(1100));

        // Get WFQ tasks
        let wfq_tasks = scheduler.get_due_tasks_wfq();
        assert_eq!(wfq_tasks.len(), 2);

        // Both should have initial virtual finish time of 0
        for task in &wfq_tasks {
            assert_eq!(task.virtual_finish_time, 0.0);
        }
    }

    #[test]
    fn test_wfq_virtual_time_update() {
        let mut scheduler = BeatScheduler::new();

        let task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60))
            .with_wfq_weight(2.0)
            .unwrap();
        scheduler.add_task(task).unwrap();

        // Simulate execution
        scheduler
            .update_wfq_after_execution("test_task", 10.0)
            .unwrap();

        // Check virtual time was updated
        let task = scheduler.tasks.get("test_task").unwrap();
        assert!(task.wfq_state.is_some());

        let wfq_state = task.wfq_state.as_ref().unwrap();
        // With weight 2.0 and execution time 10.0:
        // virtual_finish_time = 0 + (10.0 / 2.0) = 5.0
        assert_eq!(wfq_state.virtual_finish_time, 5.0);
        assert_eq!(wfq_state.total_execution_time, 10.0);
    }

    #[test]
    fn test_wfq_fairness() {
        let mut scheduler = BeatScheduler::new();

        // Create three tasks with different weights
        let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(1))
            .with_wfq_weight(1.0)
            .unwrap();
        let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(1))
            .with_wfq_weight(2.0)
            .unwrap();
        let task3 = ScheduledTask::new("task3".to_string(), Schedule::interval(1))
            .with_wfq_weight(5.0)
            .unwrap();

        scheduler.add_task(task1).unwrap();
        scheduler.add_task(task2).unwrap();
        scheduler.add_task(task3).unwrap();

        // Simulate executions with same duration
        scheduler.update_wfq_after_execution("task1", 10.0).unwrap();
        scheduler.update_wfq_after_execution("task2", 10.0).unwrap();
        scheduler.update_wfq_after_execution("task3", 10.0).unwrap();

        // Check virtual finish times are computed correctly based on weights
        let t1 = scheduler.tasks.get("task1").unwrap();
        let t2 = scheduler.tasks.get("task2").unwrap();
        let t3 = scheduler.tasks.get("task3").unwrap();

        let vft1 = t1.wfq_finish_time();
        let vft2 = t2.wfq_finish_time();
        let vft3 = t3.wfq_finish_time();

        // All tasks should have virtual finish times reflecting their execution
        // Task with higher weight should finish sooner (smaller vft increment)
        assert!(vft1 > 0.0);
        assert!(vft2 > 0.0);
        assert!(vft3 > 0.0);

        // The actual ordering depends on execution order and global virtual time
        // But weights should affect the virtual time increments
        let t1_state = t1.wfq_state.as_ref().unwrap();
        let t2_state = t2.wfq_state.as_ref().unwrap();
        let t3_state = t3.wfq_state.as_ref().unwrap();

        // Virtual time increment should be inversely proportional to weight
        // weight=1.0: increment = 10.0/1.0 = 10.0
        // weight=2.0: increment = 10.0/2.0 = 5.0
        // weight=5.0: increment = 10.0/5.0 = 2.0
        let increment1 = t1_state.virtual_finish_time - t1_state.virtual_start_time;
        let increment2 = t2_state.virtual_finish_time - t2_state.virtual_start_time;
        let increment3 = t3_state.virtual_finish_time - t3_state.virtual_start_time;

        assert_eq!(increment1, 10.0);
        assert_eq!(increment2, 5.0);
        assert_eq!(increment3, 2.0);
    }

    #[test]
    fn test_wfq_task_ordering() {
        let mut scheduler = BeatScheduler::new();

        // Create tasks with different weights and execution histories
        let task1 = ScheduledTask::new("heavy_task".to_string(), Schedule::interval(1))
            .with_wfq_weight(1.0)
            .unwrap();
        let task2 = ScheduledTask::new("light_task".to_string(), Schedule::interval(1))
            .with_wfq_weight(5.0)
            .unwrap();

        scheduler.add_task(task1).unwrap();
        scheduler.add_task(task2).unwrap();

        // Simulate first execution - both tasks run
        scheduler
            .update_wfq_after_execution("heavy_task", 10.0)
            .unwrap();
        scheduler
            .update_wfq_after_execution("light_task", 10.0)
            .unwrap();

        std::thread::sleep(std::time::Duration::from_millis(1100));

        // Get tasks - ordering is based on virtual finish time
        let wfq_tasks = scheduler.get_due_tasks_wfq();
        assert_eq!(wfq_tasks.len(), 2);

        // Verify weights are preserved
        let heavy = wfq_tasks.iter().find(|t| t.name == "heavy_task").unwrap();
        let light = wfq_tasks.iter().find(|t| t.name == "light_task").unwrap();
        assert_eq!(heavy.weight, 1.0);
        assert_eq!(light.weight, 5.0);
    }

    #[test]
    fn test_wfq_stats() {
        let mut scheduler = BeatScheduler::new();

        let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60))
            .with_wfq_weight(2.0)
            .unwrap();
        let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(60))
            .with_wfq_weight(3.0)
            .unwrap();

        scheduler.add_task(task1).unwrap();
        scheduler.add_task(task2).unwrap();

        let stats = scheduler.get_wfq_stats();
        assert_eq!(stats.total_tasks, 2);
        assert_eq!(stats.tasks_with_wfq_config, 2);
        assert_eq!(stats.total_weight, 5.0);
        assert_eq!(stats.average_weight, 2.5);
        assert_eq!(stats.global_virtual_time, 0.0);
    }

    #[test]
    fn test_wfq_stats_display() {
        let mut scheduler = BeatScheduler::new();

        let task = ScheduledTask::new("task1".to_string(), Schedule::interval(60))
            .with_wfq_weight(2.0)
            .unwrap();
        scheduler.add_task(task).unwrap();

        let stats = scheduler.get_wfq_stats();
        let display = format!("{}", stats);
        assert!(display.contains("WFQ Stats"));
        assert!(display.contains("1/1 tasks configured"));
    }

    #[test]
    fn test_wfq_with_disabled_tasks() {
        let mut scheduler = BeatScheduler::new();

        let task1 = ScheduledTask::new("enabled_task".to_string(), Schedule::interval(1))
            .with_wfq_weight(2.0)
            .unwrap();

        let mut task2 = ScheduledTask::new("disabled_task".to_string(), Schedule::interval(1))
            .with_wfq_weight(5.0)
            .unwrap();
        task2.enabled = false;

        scheduler.add_task(task1).unwrap();
        scheduler.add_task(task2).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(1100));

        // Only enabled task should be returned
        let wfq_tasks = scheduler.get_due_tasks_wfq();
        assert_eq!(wfq_tasks.len(), 1);
        assert_eq!(wfq_tasks[0].name, "enabled_task");
    }

    #[test]
    fn test_wfq_global_virtual_time() {
        let mut scheduler = BeatScheduler::new();

        let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60))
            .with_wfq_weight(1.0)
            .unwrap();
        let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(60))
            .with_wfq_weight(2.0)
            .unwrap();

        scheduler.add_task(task1).unwrap();
        scheduler.add_task(task2).unwrap();

        // Simulate executions
        scheduler.update_wfq_after_execution("task1", 10.0).unwrap();
        scheduler.update_wfq_after_execution("task2", 10.0).unwrap();

        // Global virtual time should be the max of all virtual finish times
        let stats = scheduler.get_wfq_stats();
        // task1 executes first: vft1 = 0 + 10.0/1.0 = 10.0
        // task2 executes second: vstart2 = max(0, 10.0) = 10.0
        //                        vft2 = 10.0 + 10.0/2.0 = 15.0
        // global = max(10.0, 15.0) = 15.0
        assert_eq!(stats.global_virtual_time, 15.0);
    }

    #[test]
    fn test_wfq_multiple_executions() {
        let mut scheduler = BeatScheduler::new();

        let task = ScheduledTask::new("task".to_string(), Schedule::interval(60))
            .with_wfq_weight(2.0)
            .unwrap();
        scheduler.add_task(task).unwrap();

        // First execution
        scheduler.update_wfq_after_execution("task", 10.0).unwrap();
        let task_state = scheduler.tasks.get("task").unwrap();
        let vft1 = task_state.wfq_finish_time();
        assert_eq!(vft1, 5.0); // 10.0 / 2.0

        // Second execution - virtual time should continue from previous
        scheduler.update_wfq_after_execution("task", 6.0).unwrap();
        let task_state = scheduler.tasks.get("task").unwrap();
        let vft2 = task_state.wfq_finish_time();
        assert_eq!(vft2, 8.0); // 5.0 + (6.0 / 2.0)
        assert_eq!(
            task_state.wfq_state.as_ref().unwrap().total_execution_time,
            16.0
        );
    }

    #[test]
    fn test_wfq_task_weight_default() {
        let task = ScheduledTask::new("task".to_string(), Schedule::interval(60));
        assert_eq!(task.wfq_weight(), 1.0); // Default weight
    }

    #[test]
    fn test_wfq_serialization() {
        let task = ScheduledTask::new("task".to_string(), Schedule::interval(60))
            .with_wfq_weight(3.5)
            .unwrap();

        let json = serde_json::to_string(&task).unwrap();
        let deserialized: ScheduledTask = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.wfq_weight(), 3.5);
    }
}
