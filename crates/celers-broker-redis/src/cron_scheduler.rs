//! Cron-based task scheduling for recurring tasks
//!
//! This module provides cron-like scheduling for tasks that need to run
//! on a regular schedule.
//!
//! # Example
//!
//! ```rust,no_run
//! use celers_broker_redis::cron_scheduler::{CronScheduler, CronExpression};
//! use celers_core::SerializedTask;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let scheduler = CronScheduler::new();
//!
//!     // Schedule a task to run every hour
//!     let task = SerializedTask::new("cleanup".to_string(), vec![]);
//!     scheduler.schedule("cleanup_job", "0 * * * *", task)?;
//!
//!     // Get tasks that are due to run
//!     let due_tasks = scheduler.get_due_tasks()?;
//!
//!     Ok(())
//! }
//! ```

use celers_core::{CelersError, Result, SerializedTask};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Return the current time as a Unix timestamp in seconds.
///
/// If the system clock is somehow set before the Unix epoch, this returns `0`
/// rather than panicking, keeping callers infallible.
fn current_unix_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Translate a standard Unix cron day-of-week field into the `cron` crate's
/// Quartz convention.
///
/// Standard Unix cron numbers days `0-6` with `0 = Sunday` (and accepts `7`
/// as an alternative for Sunday). The `cron` crate (Quartz style) numbers days
/// `1-7` with `1 = Sunday`. The numeric mapping is therefore
/// `quartz = (unix % 7) + 1`.
///
/// This preserves the cron field grammar: wildcards (`*`), comma lists
/// (`a,b,c`), ranges (`a-b`), step suffixes (`*/n`, `a-b/n`) and named days
/// (`mon`, `fri`, …, which the `cron` crate already understands) are handled.
/// Any token that is not a recognized numeric form is passed through unchanged.
fn translate_day_of_week(field: &str) -> String {
    if field == "*" || field == "?" {
        return field.to_string();
    }

    field
        .split(',')
        .map(translate_day_of_week_term)
        .collect::<Vec<_>>()
        .join(",")
}

/// Translate a single comma-separated day-of-week term (which may carry a
/// `/step` suffix and may itself be a range).
fn translate_day_of_week_term(term: &str) -> String {
    // Separate an optional step suffix ("base/step"); the step itself is a plain
    // integer interval and must not be remapped.
    let (base, step) = match term.split_once('/') {
        Some((base, step)) => (base, Some(step)),
        None => (term, None),
    };

    let translated_base = if let Some((start, end)) = base.split_once('-') {
        match (map_unix_dow(start), map_unix_dow(end)) {
            (Some(s), Some(e)) => format!("{s}-{e}"),
            _ => base.to_string(),
        }
    } else {
        match map_unix_dow(base) {
            Some(v) => v.to_string(),
            None => base.to_string(),
        }
    };

    match step {
        Some(step) => format!("{translated_base}/{step}"),
        None => translated_base,
    }
}

/// Map a single Unix day-of-week ordinal (`0..=7`, `0`/`7` = Sunday) to the
/// Quartz ordinal used by the `cron` crate (`1..=7`, `1` = Sunday).
///
/// Returns `None` for non-numeric tokens (e.g. named days like `mon`) and for
/// out-of-range values, so callers leave them untouched.
fn map_unix_dow(token: &str) -> Option<u32> {
    let value: u32 = token.trim().parse().ok()?;
    if value > 7 {
        return None;
    }
    Some((value % 7) + 1)
}

/// A cron expression for scheduling
#[derive(Debug, Clone)]
pub struct CronExpression {
    /// The cron expression string (e.g., "0 * * * *" for every hour)
    expression: String,
}

impl CronExpression {
    /// Create a new cron expression
    pub fn new(expression: impl Into<String>) -> Result<Self> {
        let expression = expression.into();
        Self::validate(&expression)?;
        Ok(Self { expression })
    }

    /// Validate a cron expression format
    fn validate(expr: &str) -> Result<()> {
        let parts: Vec<&str> = expr.split_whitespace().collect();
        if parts.len() != 5 {
            return Err(CelersError::Other(format!(
                "Invalid cron expression: expected 5 fields, got {}",
                parts.len()
            )));
        }
        Ok(())
    }

    /// Get the expression string
    pub fn as_str(&self) -> &str {
        &self.expression
    }

    /// Parse common cron expressions
    pub fn every_minute() -> Self {
        Self {
            expression: "* * * * *".to_string(),
        }
    }

    /// Every hour at minute 0
    pub fn hourly() -> Self {
        Self {
            expression: "0 * * * *".to_string(),
        }
    }

    /// Every day at midnight
    pub fn daily() -> Self {
        Self {
            expression: "0 0 * * *".to_string(),
        }
    }

    /// Every week on Sunday at midnight
    pub fn weekly() -> Self {
        Self {
            expression: "0 0 * * 0".to_string(),
        }
    }

    /// Every month on the 1st at midnight
    pub fn monthly() -> Self {
        Self {
            expression: "0 0 1 * *".to_string(),
        }
    }
}

/// A scheduled task entry
#[derive(Debug, Clone)]
pub struct ScheduledTask {
    /// Unique identifier for this scheduled task
    pub id: String,
    /// The cron expression
    pub cron: CronExpression,
    /// The task template to execute
    pub task_template: SerializedTask,
    /// Last execution time (Unix timestamp)
    pub last_run: Option<i64>,
    /// Next scheduled execution time (Unix timestamp)
    pub next_run: i64,
    /// Whether the task is enabled
    pub enabled: bool,
}

impl ScheduledTask {
    /// Create a new scheduled task
    pub fn new(id: String, cron: CronExpression, task_template: SerializedTask) -> Self {
        let next_run = Self::calculate_next_run(&cron, None);
        Self {
            id,
            cron,
            task_template,
            last_run: None,
            next_run,
            enabled: true,
        }
    }

    /// Calculate the next run time based on the cron expression.
    ///
    /// This parses the standard 5-field cron expression (`min hour day month
    /// day_of_week`) using the [`cron`] crate and computes the next firing
    /// instant strictly after `from` (or now). The `cron` crate expects a
    /// 6- or 7-field expression that includes a leading seconds field and an
    /// optional trailing year field, so the 5-field form is normalized into
    /// `"0 <expr> *"` (mirroring how `celers-beat` builds its schedules).
    ///
    /// If parsing fails or no future occurrence can be found, this falls back
    /// to a sensible fixed interval (matching the original heuristic for the
    /// well-known presets, otherwise one minute) so the function never panics.
    fn calculate_next_run(cron: &CronExpression, from: Option<i64>) -> i64 {
        let now = from.unwrap_or_else(current_unix_secs);

        match Self::next_run_from_cron(cron.as_str(), now) {
            Some(next) => next,
            None => now + Self::fallback_interval(cron.as_str()),
        }
    }

    /// Compute the next run timestamp using the real cron parser.
    ///
    /// Returns `None` when the expression cannot be parsed, when `from` is not
    /// a representable timestamp, or when the schedule yields no future time.
    fn next_run_from_cron(expression: &str, from: i64) -> Option<i64> {
        use cron::Schedule as CronSchedule;
        use std::str::FromStr;

        // Normalize a standard 5-field Unix expression into the 7-field form the
        // `cron` crate expects: "sec min hour day_of_month month day_of_week year".
        // We use "0" for seconds (fire at the top of the minute) and "*" for year.
        //
        // The day-of-week field must also be translated: standard Unix cron uses
        // 0-6 with 0 = Sunday (and 7 = Sunday), whereas the `cron` crate uses the
        // Quartz convention 1-7 with 1 = Sunday. Without translation, "1-5"
        // (Mon-Fri in Unix) would be interpreted as Sun-Thu.
        let parts: Vec<&str> = expression.split_whitespace().collect();
        let normalized = if parts.len() == 5 {
            let day_of_week = translate_day_of_week(parts[4]);
            format!(
                "0 {} {} {} {} {} *",
                parts[0], parts[1], parts[2], parts[3], day_of_week
            )
        } else {
            // Already includes seconds (6 fields) or seconds + year (7 fields):
            // pass through unchanged so explicit expressions still work.
            expression.to_string()
        };

        let schedule = CronSchedule::from_str(&normalized).ok()?;
        let after = chrono::DateTime::<chrono::Utc>::from_timestamp(from, 0)?;
        let next = schedule.after(&after).next()?;
        Some(next.timestamp())
    }

    /// Fallback interval (in seconds) used when cron parsing is not possible.
    ///
    /// Preserves the original heuristic for the well-known presets so behavior
    /// never regresses, and otherwise defaults to one minute (the finest
    /// standard cron granularity).
    fn fallback_interval(expression: &str) -> i64 {
        match expression {
            "* * * * *" => 60,      // every minute
            "0 * * * *" => 3600,    // every hour
            "0 0 * * *" => 86400,   // daily
            "0 0 * * 0" => 604800,  // weekly
            "0 0 1 * *" => 2592000, // monthly (approx)
            _ => 60,
        }
    }

    /// Update the next run time after execution
    pub fn update_after_run(&mut self) {
        let now = current_unix_secs();

        self.last_run = Some(now);
        self.next_run = Self::calculate_next_run(&self.cron, Some(now));
    }

    /// Check if this task is due to run
    pub fn is_due(&self) -> bool {
        if !self.enabled {
            return false;
        }

        let now = current_unix_secs();

        now >= self.next_run
    }
}

/// Cron-based task scheduler
pub struct CronScheduler {
    tasks: Arc<RwLock<HashMap<String, ScheduledTask>>>,
}

impl CronScheduler {
    /// Create a new cron scheduler
    pub fn new() -> Self {
        Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Schedule a new task
    pub fn schedule(
        &self,
        id: impl Into<String>,
        cron_expr: impl Into<String>,
        task_template: SerializedTask,
    ) -> Result<()> {
        let id = id.into();
        let cron = CronExpression::new(cron_expr)?;
        let scheduled_task = ScheduledTask::new(id.clone(), cron, task_template);

        let mut tasks = self
            .tasks
            .write()
            .map_err(|e| CelersError::Other(format!("Failed to acquire write lock: {}", e)))?;

        tasks.insert(id, scheduled_task);
        Ok(())
    }

    /// Remove a scheduled task
    pub fn unschedule(&self, id: &str) -> Result<bool> {
        let mut tasks = self
            .tasks
            .write()
            .map_err(|e| CelersError::Other(format!("Failed to acquire write lock: {}", e)))?;

        Ok(tasks.remove(id).is_some())
    }

    /// Enable a scheduled task
    pub fn enable(&self, id: &str) -> Result<()> {
        let mut tasks = self
            .tasks
            .write()
            .map_err(|e| CelersError::Other(format!("Failed to acquire write lock: {}", e)))?;

        if let Some(task) = tasks.get_mut(id) {
            task.enabled = true;
            Ok(())
        } else {
            Err(CelersError::Other(format!("Task not found: {}", id)))
        }
    }

    /// Disable a scheduled task
    pub fn disable(&self, id: &str) -> Result<()> {
        let mut tasks = self
            .tasks
            .write()
            .map_err(|e| CelersError::Other(format!("Failed to acquire write lock: {}", e)))?;

        if let Some(task) = tasks.get_mut(id) {
            task.enabled = false;
            Ok(())
        } else {
            Err(CelersError::Other(format!("Task not found: {}", id)))
        }
    }

    /// Get all tasks that are due to run
    pub fn get_due_tasks(&self) -> Result<Vec<SerializedTask>> {
        let mut tasks = self
            .tasks
            .write()
            .map_err(|e| CelersError::Other(format!("Failed to acquire write lock: {}", e)))?;

        let mut due_tasks = Vec::new();

        for task in tasks.values_mut() {
            if task.is_due() {
                due_tasks.push(task.task_template.clone());
                task.update_after_run();
            }
        }

        Ok(due_tasks)
    }

    /// Get all scheduled tasks
    pub fn list_all(&self) -> Result<Vec<ScheduledTask>> {
        let tasks = self
            .tasks
            .read()
            .map_err(|e| CelersError::Other(format!("Failed to acquire read lock: {}", e)))?;

        Ok(tasks.values().cloned().collect())
    }

    /// Get a specific scheduled task
    pub fn get(&self, id: &str) -> Result<Option<ScheduledTask>> {
        let tasks = self
            .tasks
            .read()
            .map_err(|e| CelersError::Other(format!("Failed to acquire read lock: {}", e)))?;

        Ok(tasks.get(id).cloned())
    }

    /// Get the number of scheduled tasks
    pub fn count(&self) -> Result<usize> {
        let tasks = self
            .tasks
            .read()
            .map_err(|e| CelersError::Other(format!("Failed to acquire read lock: {}", e)))?;

        Ok(tasks.len())
    }

    /// Clear all scheduled tasks
    pub fn clear(&self) -> Result<()> {
        let mut tasks = self
            .tasks
            .write()
            .map_err(|e| CelersError::Other(format!("Failed to acquire write lock: {}", e)))?;

        tasks.clear();
        Ok(())
    }
}

impl Default for CronScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for CronScheduler {
    fn clone(&self) -> Self {
        Self {
            tasks: Arc::clone(&self.tasks),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use celers_core::TaskMetadata;

    fn create_test_task() -> SerializedTask {
        SerializedTask {
            metadata: TaskMetadata::new("test_task".to_string()),
            payload: vec![],
        }
    }

    #[test]
    fn test_cron_expression_validation() {
        assert!(CronExpression::new("* * * * *").is_ok());
        assert!(CronExpression::new("0 * * * *").is_ok());
        assert!(CronExpression::new("invalid").is_err());
        assert!(CronExpression::new("* * *").is_err());
    }

    #[test]
    fn test_cron_expression_presets() {
        assert_eq!(CronExpression::every_minute().as_str(), "* * * * *");
        assert_eq!(CronExpression::hourly().as_str(), "0 * * * *");
        assert_eq!(CronExpression::daily().as_str(), "0 0 * * *");
        assert_eq!(CronExpression::weekly().as_str(), "0 0 * * 0");
        assert_eq!(CronExpression::monthly().as_str(), "0 0 1 * *");
    }

    #[test]
    fn test_scheduled_task_creation() {
        let cron = CronExpression::hourly();
        let task = create_test_task();
        let scheduled = ScheduledTask::new("test".to_string(), cron, task);

        assert_eq!(scheduled.id, "test");
        assert!(scheduled.enabled);
        assert!(scheduled.last_run.is_none());
        assert!(scheduled.next_run > 0);
    }

    #[test]
    fn test_cron_scheduler_schedule() {
        let scheduler = CronScheduler::new();
        let task = create_test_task();

        assert!(scheduler.schedule("job1", "* * * * *", task).is_ok());
        assert_eq!(scheduler.count().unwrap(), 1);
    }

    #[test]
    fn test_cron_scheduler_unschedule() {
        let scheduler = CronScheduler::new();
        let task = create_test_task();

        scheduler.schedule("job1", "* * * * *", task).unwrap();
        assert_eq!(scheduler.count().unwrap(), 1);

        assert!(scheduler.unschedule("job1").unwrap());
        assert_eq!(scheduler.count().unwrap(), 0);
    }

    #[test]
    fn test_cron_scheduler_enable_disable() {
        let scheduler = CronScheduler::new();
        let task = create_test_task();

        scheduler.schedule("job1", "* * * * *", task).unwrap();

        assert!(scheduler.disable("job1").is_ok());
        let scheduled = scheduler.get("job1").unwrap().unwrap();
        assert!(!scheduled.enabled);

        assert!(scheduler.enable("job1").is_ok());
        let scheduled = scheduler.get("job1").unwrap().unwrap();
        assert!(scheduled.enabled);
    }

    #[test]
    fn test_cron_scheduler_list_all() {
        let scheduler = CronScheduler::new();
        let task1 = create_test_task();
        let task2 = create_test_task();

        scheduler.schedule("job1", "* * * * *", task1).unwrap();
        scheduler.schedule("job2", "0 * * * *", task2).unwrap();

        let all_tasks = scheduler.list_all().unwrap();
        assert_eq!(all_tasks.len(), 2);
    }

    #[test]
    fn test_cron_scheduler_clear() {
        let scheduler = CronScheduler::new();
        let task = create_test_task();

        scheduler
            .schedule("job1", "* * * * *", task.clone())
            .unwrap();
        scheduler.schedule("job2", "0 * * * *", task).unwrap();
        assert_eq!(scheduler.count().unwrap(), 2);

        scheduler.clear().unwrap();
        assert_eq!(scheduler.count().unwrap(), 0);
    }

    #[test]
    fn test_cron_scheduler_clone() {
        let scheduler = CronScheduler::new();
        let task = create_test_task();

        scheduler.schedule("job1", "* * * * *", task).unwrap();

        let cloned = scheduler.clone();
        assert_eq!(cloned.count().unwrap(), 1);
    }

    /// Convert a UTC date/time into a Unix timestamp for deterministic tests.
    fn ts(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> i64 {
        use chrono::{TimeZone, Utc};
        match Utc.with_ymd_and_hms(year, month, day, hour, minute, second) {
            chrono::LocalResult::Single(dt) => dt.timestamp(),
            _ => panic!("invalid test timestamp"),
        }
    }

    #[test]
    fn test_calculate_next_run_every_minute() {
        // 2021-01-01 00:00:30 UTC -> next "* * * * *" is 00:01:00.
        let from = ts(2021, 1, 1, 0, 0, 30);
        let cron = CronExpression::every_minute();
        let next = ScheduledTask::calculate_next_run(&cron, Some(from));
        assert_eq!(next, ts(2021, 1, 1, 0, 1, 0));
    }

    #[test]
    fn test_calculate_next_run_every_15_minutes() {
        // "*/15 * * * *" fires at :00, :15, :30, :45. From 00:07 the next is 00:15.
        let cron = CronExpression::new("*/15 * * * *").unwrap();
        let from = ts(2021, 1, 1, 0, 7, 0);
        let next = ScheduledTask::calculate_next_run(&cron, Some(from));
        assert_eq!(next, ts(2021, 1, 1, 0, 15, 0));

        // From exactly :15 the next strictly-future occurrence is :30.
        let from = ts(2021, 1, 1, 0, 15, 0);
        let next = ScheduledTask::calculate_next_run(&cron, Some(from));
        assert_eq!(next, ts(2021, 1, 1, 0, 30, 0));

        // Crossing the hour boundary: from :48 -> next hour :00.
        let from = ts(2021, 1, 1, 0, 48, 0);
        let next = ScheduledTask::calculate_next_run(&cron, Some(from));
        assert_eq!(next, ts(2021, 1, 1, 1, 0, 0));
    }

    #[test]
    fn test_calculate_next_run_weekday_morning() {
        // "30 9 * * 1-5" = 09:30 Monday..Friday.
        let cron = CronExpression::new("30 9 * * 1-5").unwrap();

        // 2021-01-01 is a Friday. At 08:00 the next run is the same day 09:30.
        let from = ts(2021, 1, 1, 8, 0, 0);
        let next = ScheduledTask::calculate_next_run(&cron, Some(from));
        assert_eq!(next, ts(2021, 1, 1, 9, 30, 0));

        // After 09:30 on Friday, the next run skips the weekend to Monday 09:30
        // (2021-01-04 is the following Monday).
        let from = ts(2021, 1, 1, 10, 0, 0);
        let next = ScheduledTask::calculate_next_run(&cron, Some(from));
        assert_eq!(next, ts(2021, 1, 4, 9, 30, 0));
    }

    #[test]
    fn test_calculate_next_run_daily_midnight() {
        // "0 0 * * *" -> next midnight strictly after the given time.
        let cron = CronExpression::daily();
        let from = ts(2021, 6, 13, 12, 0, 0);
        let next = ScheduledTask::calculate_next_run(&cron, Some(from));
        assert_eq!(next, ts(2021, 6, 14, 0, 0, 0));
    }

    #[test]
    fn test_calculate_next_run_specific_datetime() {
        // "0 12 25 12 *" = noon on December 25th.
        let cron = CronExpression::new("0 12 25 12 *").unwrap();
        let from = ts(2021, 1, 1, 0, 0, 0);
        let next = ScheduledTask::calculate_next_run(&cron, Some(from));
        assert_eq!(next, ts(2021, 12, 25, 12, 0, 0));
    }

    #[test]
    fn test_calculate_next_run_weekly_sunday_preset() {
        // The weekly() preset is "0 0 * * 0" = Sunday midnight (Unix dow 0).
        // 2021-01-01 is a Friday, so the next Sunday is 2021-01-03.
        let cron = CronExpression::weekly();
        let from = ts(2021, 1, 1, 12, 0, 0);
        let next = ScheduledTask::calculate_next_run(&cron, Some(from));
        assert_eq!(next, ts(2021, 1, 3, 0, 0, 0));
    }

    #[test]
    fn test_day_of_week_translation_unix_to_quartz() {
        // Unix 0 (Sun) -> Quartz 1; Unix 6 (Sat) -> Quartz 7; Unix 7 (Sun) -> 1.
        assert_eq!(translate_day_of_week("0"), "1");
        assert_eq!(translate_day_of_week("1"), "2");
        assert_eq!(translate_day_of_week("6"), "7");
        assert_eq!(translate_day_of_week("7"), "1");

        // Ranges and lists are remapped element-wise.
        assert_eq!(translate_day_of_week("1-5"), "2-6");
        assert_eq!(translate_day_of_week("0,6"), "1,7");

        // Step suffixes keep their interval; the base is remapped.
        assert_eq!(translate_day_of_week("1-5/2"), "2-6/2");

        // Wildcards and named days pass through unchanged.
        assert_eq!(translate_day_of_week("*"), "*");
        assert_eq!(translate_day_of_week("mon-fri"), "mon-fri");
    }

    #[test]
    fn test_calculate_next_run_falls_back_gracefully() {
        // A structurally valid 5-field expression that the parser rejects must
        // not panic; it falls back to a positive future timestamp.
        let cron = CronExpression::new("99 99 99 99 99").unwrap();
        let from = ts(2021, 1, 1, 0, 0, 0);
        let next = ScheduledTask::calculate_next_run(&cron, Some(from));
        assert!(next > from);
    }
}
