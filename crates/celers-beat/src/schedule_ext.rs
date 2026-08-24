//! Extended schedule types
//!
//! Contains advanced schedule types including schedule indexing, blackout periods,
//! composite schedules, custom schedules, timezone utilities, and schedule builders.

use crate::config::ScheduleError;
use crate::schedule::{BusinessCalendar, HolidayCalendar, Schedule};
use crate::task::ScheduledTask;
use chrono::{DateTime, Datelike, Duration, Months, Timelike, Utc};
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
            Schedule::MonthlyLastDay { .. } => "monthly_last_day".to_string(),
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

    /// Check whether `time`'s time-of-day falls inside the recurring window.
    ///
    /// Handles windows that cross midnight (e.g. 22:00 → 02:00): for those the
    /// window is the *union* of `[start, 23:59]` and `[00:00, end]`, not the
    /// empty intersection an ordinary `>= start && <= end` comparison yields.
    fn matches_time_of_day(&self, time: DateTime<Utc>) -> bool {
        let start_time = (self.start.hour(), self.start.minute());
        let end_time = (self.end.hour(), self.end.minute());
        let current_time = (time.hour(), time.minute());

        if start_time <= end_time {
            current_time >= start_time && current_time <= end_time
        } else {
            // Window wraps past midnight.
            current_time >= start_time || current_time <= end_time
        }
    }

    /// Whether the recurring window covers every minute of the day, which would
    /// make "advance until out of the blackout" unsatisfiable.
    fn covers_whole_day(&self) -> bool {
        let start_time = (self.start.hour(), self.start.minute());
        let end_time = (self.end.hour(), self.end.minute());
        start_time == (0, 0) && end_time == (23, 59)
    }

    /// Check if the given time is within this blackout period
    pub fn is_blackout(&self, time: DateTime<Utc>) -> bool {
        if time >= self.start && time <= self.end {
            return true;
        }

        // Check recurring patterns
        match self.recurring {
            Some(BlackoutRecurrence::Daily) => self.matches_time_of_day(time),
            Some(BlackoutRecurrence::Weekly) => {
                time.weekday() == self.start.weekday() && self.matches_time_of_day(time)
            }
            Some(BlackoutRecurrence::Monthly) => {
                time.day() == self.start.day() && self.matches_time_of_day(time)
            }
            None => false,
        }
    }

    /// Find the next time after the blackout period.
    ///
    /// The returned instant is always strictly outside the blackout when one
    /// exists, and strictly greater than `time` — callers loop on this method,
    /// so a non-advancing result would spin. An unsatisfiable configuration (a
    /// daily blackout covering the whole day, or a recurrence that never
    /// clears within a year) returns the last candidate rather than looping
    /// forever; use [`BlackoutPeriod::try_next_available_time`] to detect that
    /// case.
    pub fn next_available_time(&self, time: DateTime<Utc>) -> DateTime<Utc> {
        match self.try_next_available_time(time) {
            Ok(next) => next,
            Err(_) => time,
        }
    }

    /// Fallible form of [`BlackoutPeriod::next_available_time`].
    ///
    /// # Errors
    /// Returns [`ScheduleError::Invalid`] when the blackout can never clear —
    /// for example a `Daily` recurrence spanning 00:00–23:59 — instead of
    /// silently handing back an instant that is still blacked out.
    pub fn try_next_available_time(
        &self,
        time: DateTime<Utc>,
    ) -> Result<DateTime<Utc>, ScheduleError> {
        if !self.is_blackout(time) {
            return Ok(time);
        }

        if self.recurring.is_some() && self.covers_whole_day() {
            return Err(ScheduleError::Invalid(format!(
                "blackout period '{}' recurs over the entire day and never clears",
                self.name
            )));
        }

        // `is_blackout` uses an inclusive upper bound, so `self.end` is itself
        // still inside the window: step one second past it so every pass makes
        // strict progress.
        let mut current = std::cmp::max(self.end + Duration::seconds(1), time);

        let Some(ref recurrence) = self.recurring else {
            return Ok(current);
        };

        // Bounded search: 366 iterations covers a full year of daily
        // recurrences, 53 weeks of weekly ones and 12+ months of monthly ones.
        for _ in 0..366 {
            if !self.is_blackout(current) {
                return Ok(current);
            }

            let advanced = match recurrence {
                BlackoutRecurrence::Daily => Some(current + Duration::days(1)),
                BlackoutRecurrence::Weekly => Some(current + Duration::weeks(1)),
                // `with_month` preserves the day-of-month and returns `None`
                // for e.g. 31 January -> 31 February; `checked_add_months`
                // clamps to the last valid day instead.
                BlackoutRecurrence::Monthly => current.checked_add_months(Months::new(1)),
            };

            match advanced {
                Some(next) if next > current => current = next,
                _ => {
                    return Err(ScheduleError::Invalid(format!(
                        "blackout period '{}' could not be advanced past {}",
                        self.name, current
                    )))
                }
            }
        }

        Err(ScheduleError::Invalid(format!(
            "blackout period '{}' did not clear within the search horizon",
            self.name
        )))
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

    /// Find the next valid execution time.
    ///
    /// Returns the input instant unchanged when no valid time can be found
    /// within the search horizon; use
    /// [`CalendarWithBlackout::try_next_valid_time`] to distinguish "found a
    /// valid instant" from "gave up", which a bare `DateTime` cannot express.
    pub fn next_valid_time(&self, time: DateTime<Utc>) -> DateTime<Utc> {
        self.try_next_valid_time(time).unwrap_or(time)
    }

    /// Fallible form of [`CalendarWithBlackout::next_valid_time`].
    ///
    /// # Errors
    /// Returns [`ScheduleError::Invalid`] when the calendar is unsatisfiable —
    /// an always-on blackout, or a combination of constraints that never clears
    /// within the search horizon — rather than returning an instant that is
    /// still blacked out.
    pub fn try_next_valid_time(&self, time: DateTime<Utc>) -> Result<DateTime<Utc>, ScheduleError> {
        let mut time = time;

        // Each iteration advances `time` strictly, so the horizon bounds the
        // search rather than merely capping a potentially stalled loop.
        for _ in 0..365 {
            // Check blackout periods first
            let mut in_blackout = false;
            for blackout in &self.blackout_periods {
                if blackout.is_blackout(time) {
                    let advanced = blackout.try_next_available_time(time)?;
                    if advanced <= time {
                        return Err(ScheduleError::Invalid(format!(
                            "blackout period '{}' made no progress from {}",
                            blackout.name, time
                        )));
                    }
                    time = advanced;
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
                    let advanced = business.next_business_time(time);
                    if advanced <= time {
                        return Err(ScheduleError::Invalid(
                            "business calendar made no progress; check working days/hours"
                                .to_string(),
                        ));
                    }
                    time = advanced;
                    continue;
                }
            }

            // All checks passed
            return Ok(time);
        }

        Err(ScheduleError::Invalid(
            "no valid execution time found within the 365-iteration search horizon".to_string(),
        ))
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

        let selected = match self.mode {
            // AND: all must be due, so take the latest (slowest) time
            CompositeMode::And => next_runs.iter().max().copied(),
            // OR: any can be due, so take the earliest (fastest) time
            CompositeMode::Or => next_runs.iter().min().copied(),
        };

        selected.ok_or_else(|| {
            ScheduleError::Invalid("No valid next run time from any sub-schedule".to_string())
        })
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
/// // Every 30 minutes (always-available interval schedule)
/// let schedule = ScheduleBuilder::new()
///     .every_n_minutes(30)
///     .build();
///
/// // Every 2 hours
/// let schedule = ScheduleBuilder::new()
///     .every_n_hours(2)
///     .build();
/// ```
///
/// # Feature-gated methods
/// The `cron` feature unlocks time-window and timezone restrictions:
/// `business_hours_only()`, `weekdays_only()`, `weekends_only()`, `in_timezone()`.
/// Each of those methods carries its own example in its own doc comment.
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

    /// Last day of every month at midnight.
    ///
    /// Fires on the *real* last calendar day — 31 January, 28 or 29 February,
    /// 30 April — exactly once per month. Cron cannot express this (there is no
    /// `L` token in the `cron` crate, and a `28-31` day-of-month range fires
    /// two to four times a month), so this uses
    /// [`Schedule::MonthlyLastDay`].
    ///
    /// # Example
    /// ```
    /// use celers_beat::ScheduleTemplates;
    ///
    /// let schedule = ScheduleTemplates::monthly_last_day();
    /// ```
    pub fn monthly_last_day() -> Schedule {
        Schedule::monthly_last_day(0, 0)
    }

    /// Last day of every month at a specific time.
    ///
    /// # Arguments
    /// * `hour` - Hour of day, UTC (0-23)
    /// * `minute` - Minute (0-59)
    ///
    /// # Example
    /// ```
    /// use celers_beat::ScheduleTemplates;
    ///
    /// // Month-end reconciliation at 23:30 UTC
    /// let schedule = ScheduleTemplates::monthly_last_day_at(23, 30);
    /// ```
    pub fn monthly_last_day_at(hour: u32, minute: u32) -> Schedule {
        Schedule::monthly_last_day(hour, minute)
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

#[cfg(test)]
mod blackout_tests {
    use super::{BlackoutPeriod, BlackoutRecurrence, CalendarWithBlackout};
    use chrono::{DateTime, Datelike, TimeZone, Utc};

    fn at(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, 0)
            .single()
            .expect("valid timestamp")
    }

    /// Regression: `with_month(month + 1)` preserves the day-of-month and
    /// returns `None` for e.g. 31 January -> 31 February, so the `.expect`
    /// panicked for any monthly blackout ending on the 29th-31st.
    #[test]
    fn monthly_blackout_ending_on_31st_does_not_panic() {
        for (month, day) in [(1u32, 31u32), (3, 31), (5, 31), (8, 31), (10, 31)] {
            let blackout = BlackoutPeriod::new(
                "month-end freeze",
                at(2026, month, day, 22, 0),
                at(2026, month, day, 23, 0),
            )
            .with_recurrence(BlackoutRecurrence::Monthly);

            let inside = at(2026, month, day, 22, 30);
            assert!(blackout.is_blackout(inside));
            // Must return a real instant instead of panicking on `with_month`.
            let next = blackout.next_available_time(inside);
            assert!(next > inside, "expected progress past {inside}, got {next}");
            assert!(!blackout.is_blackout(next));
        }
    }

    #[test]
    fn monthly_blackout_on_29th_of_non_leap_february_does_not_panic() {
        let blackout = BlackoutPeriod::new("freeze", at(2026, 1, 29, 1, 0), at(2026, 1, 29, 2, 0))
            .with_recurrence(BlackoutRecurrence::Monthly);
        let inside = at(2026, 1, 29, 1, 30);
        let next = blackout.next_available_time(inside);
        assert!(next > inside);
    }

    /// Regression: a window crossing midnight compared `(hour, minute)` tuples
    /// with `>= start && <= end`, which is empty for 22:00 -> 02:00, so the
    /// blackout was silently ignored.
    #[test]
    fn midnight_crossing_window_is_honoured() {
        let blackout =
            BlackoutPeriod::new("overnight", at(2026, 6, 13, 22, 0), at(2026, 6, 14, 2, 0))
                .with_recurrence(BlackoutRecurrence::Daily);

        // Both sides of midnight, on a later day than the anchor period.
        assert!(blackout.is_blackout(at(2026, 7, 1, 23, 0)));
        assert!(blackout.is_blackout(at(2026, 7, 2, 1, 0)));
        // Outside the window.
        assert!(!blackout.is_blackout(at(2026, 7, 2, 12, 0)));
    }

    /// Regression: a `Daily` blackout covering the whole day looped forever
    /// (`current + 1 day` is always still in the blackout) until the date
    /// overflowed and panicked.
    #[test]
    fn all_day_daily_blackout_reports_error_instead_of_looping() {
        let blackout =
            BlackoutPeriod::new("always", at(2026, 6, 13, 0, 0), at(2026, 6, 13, 23, 59))
                .with_recurrence(BlackoutRecurrence::Daily);

        let inside = at(2026, 6, 13, 12, 0);
        assert!(blackout.try_next_available_time(inside).is_err());
        // The infallible wrapper degrades instead of hanging or panicking.
        assert_eq!(blackout.next_available_time(inside), inside);
    }

    /// Regression: `next_available_time` returned `self.end`, which
    /// `is_blackout`'s inclusive upper bound still considers blacked out, so
    /// `next_valid_time` made no progress and eventually returned an instant
    /// inside the blackout.
    #[test]
    fn next_valid_time_escapes_a_non_recurring_blackout() {
        let start = at(2026, 6, 13, 9, 0);
        let end = at(2026, 6, 13, 17, 0);
        let mut calendar = CalendarWithBlackout::new();
        calendar.add_blackout(BlackoutPeriod::new("deploy freeze", start, end));

        let inside = at(2026, 6, 13, 12, 0);
        let next = calendar
            .try_next_valid_time(inside)
            .expect("a non-recurring blackout always clears");
        assert!(next > end, "expected an instant past {end}, got {next}");
        assert!(calendar.is_valid_time(next));
        assert_eq!(next.day(), 13);
    }

    #[test]
    fn next_valid_time_is_identity_when_nothing_applies() {
        let calendar = CalendarWithBlackout::new();
        let t = at(2026, 6, 13, 12, 0);
        assert_eq!(calendar.next_valid_time(t), t);
        assert_eq!(calendar.try_next_valid_time(t).expect("no constraints"), t);
    }

    #[test]
    fn unsatisfiable_calendar_surfaces_an_error() {
        let mut calendar = CalendarWithBlackout::new();
        calendar.add_blackout(
            BlackoutPeriod::new("always", at(2026, 6, 13, 0, 0), at(2026, 6, 13, 23, 59))
                .with_recurrence(BlackoutRecurrence::Daily),
        );
        assert!(calendar
            .try_next_valid_time(at(2026, 6, 13, 12, 0))
            .is_err());
    }
}
