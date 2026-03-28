//! Extended schedule types
//!
//! Contains advanced schedule types including schedule indexing, blackout periods,
//! composite schedules, custom schedules, timezone utilities, and schedule builders.

use crate::config::ScheduleError;
use crate::schedule::{BusinessCalendar, HolidayCalendar, Schedule};
use crate::task::ScheduledTask;
use chrono::{DateTime, Datelike, Duration, Timelike, Utc};
#[cfg(feature = "cron")]
use chrono::{Offset, TimeZone};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

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
