//! Cron-based task scheduling for recurring tasks
//!
//! This module provides cron-like scheduling for tasks that need to run
//! on a regular schedule, with Redis-backed distributed dispatch locking:
//! each process holds its own in-memory copy of the schedule (so `is_due()`
//! checks are cheap and local), but actually *firing* a due occurrence is
//! gated by a Redis `SET key value NX PX ttl` claim keyed by
//! `(job_id, scheduled_fire_instant)`. Because `next_run` values are
//! calendar-grid-aligned (computed by the `cron` crate, not `now + interval`),
//! independent processes holding the same schedule converge on the same
//! claim key for the same logical occurrence even if their local clocks or
//! registration times differ slightly, so exactly one process wins the claim
//! and fires each occurrence regardless of how many workers run the same
//! schedule.
//!
//! # Example
//!
//! ```rust,no_run
//! use celers_broker_redis::cron_scheduler::{CronScheduler, CronExpression};
//! use celers_core::SerializedTask;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // `namespace` scopes the Redis dispatch-claim keys: every process
//!     // that should coordinate over the same jobs uses the same namespace.
//!     let scheduler = CronScheduler::new("redis://localhost:6379", "my_app")?;
//!
//!     // Schedule a task to run every hour
//!     let task = SerializedTask::new("cleanup".to_string(), vec![]);
//!     scheduler.schedule("cleanup_job", "0 * * * *", task)?;
//!
//!     // Get tasks that are due to run. Running this scheduler in multiple
//!     // worker processes (same namespace, same registered jobs) still
//!     // fires each occurrence exactly once.
//!     let due_tasks = scheduler.get_due_tasks().await?;
//!
//!     Ok(())
//! }
//! ```

use crate::connection::RedisClientExt;
use celers_core::{CelersError, Result, SerializedTask, TaskState};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tracing::{debug, warn};
use uuid::Uuid;

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

/// Normalize a standard 5-field Unix cron expression into the 7-field form
/// the `cron` crate expects: `"sec min hour day_of_month month day_of_week
/// year"`. Uses `"0"` for seconds (fire at the top of the minute) and `"*"`
/// for year, and translates the day-of-week field from Unix to Quartz
/// convention (see [`translate_day_of_week`]). An expression that already
/// has 6 or 7 fields (explicit seconds, optionally a year) is passed through
/// unchanged.
fn normalize_cron_expression(expression: &str) -> String {
    let parts: Vec<&str> = expression.split_whitespace().collect();
    if parts.len() == 5 {
        let day_of_week = translate_day_of_week(parts[4]);
        format!(
            "0 {} {} {} {} {} *",
            parts[0], parts[1], parts[2], parts[3], day_of_week
        )
    } else {
        expression.to_string()
    }
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

    /// Validate a cron expression format.
    ///
    /// Checks both field count *and* that the expression actually parses via
    /// the real `cron` crate — a structurally 5-field expression with
    /// out-of-range values (e.g. `"99 99 99 99 99"`) is rejected here rather
    /// than being silently accepted and later degrading to a fixed
    /// once-a-minute fallback the first time `calculate_next_run` is called.
    fn validate(expr: &str) -> Result<()> {
        let parts: Vec<&str> = expr.split_whitespace().collect();
        if parts.len() != 5 {
            return Err(CelersError::Other(format!(
                "Invalid cron expression: expected 5 fields, got {}",
                parts.len()
            )));
        }

        use cron::Schedule as CronSchedule;
        use std::str::FromStr;
        let normalized = normalize_cron_expression(expr);
        CronSchedule::from_str(&normalized).map_err(|e| {
            CelersError::Other(format!("Invalid cron expression '{}': {}", expr, e))
        })?;

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
    /// well-known presets, otherwise one minute) so the function never
    /// panics. In practice this fallback is unreachable through the public
    /// API today, since [`CronExpression::new`] already rejects anything
    /// that fails to parse — it remains as defense-in-depth (e.g. for a
    /// schedule with no future occurrence at all) and is exercised directly
    /// in this module's tests.
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

        let normalized = normalize_cron_expression(expression);
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

    /// Update the next run time after execution.
    ///
    /// Computes from the *scheduled* previous `next_run`, not from `now`, so
    /// the cadence does not drift later on every firing by however late the
    /// poll happened to run. If the scheduler fell behind (the previous
    /// `next_run` is far in the past — e.g. after downtime), this naturally
    /// produces one occurrence per missed tick on subsequent polls rather
    /// than silently skipping straight to "now"; paired with
    /// [`CronScheduler`]'s per-occurrence Redis claim, that catch-up is safe
    /// even with multiple worker processes racing to fire it.
    pub fn update_after_run(&mut self) {
        let now = current_unix_secs();

        self.last_run = Some(now);
        self.next_run = Self::calculate_next_run(&self.cron, Some(self.next_run));
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

/// Cron-based task scheduler. See the module documentation for the
/// distributed dispatch-locking design.
pub struct CronScheduler {
    tasks: Arc<RwLock<HashMap<String, ScheduledTask>>>,
    client: redis::Client,
    /// Scopes the Redis keys used for per-occurrence dispatch claims.
    /// Every process that should coordinate over the same jobs must use the
    /// same namespace (and register equivalent job ids/expressions).
    namespace: String,
    /// TTL (milliseconds) of the per-occurrence dispatch claim. Should be at
    /// least the polling interval so a claim from one tick cannot expire and
    /// be re-won by another process before the winner had a chance to hand
    /// the fired task off.
    claim_ttl_ms: u64,
}

impl CronScheduler {
    /// Create a new distributed cron scheduler.
    ///
    /// `namespace` scopes the Redis keys used for dispatch claims (typically
    /// the queue or application name) — every process that should coordinate
    /// over the same set of jobs must use the same `namespace` and register
    /// the same job ids with equivalent cron expressions.
    pub fn new(redis_url: &str, namespace: impl Into<String>) -> Result<Self> {
        let client = redis::Client::open(redis_url)
            .map_err(|e| CelersError::Broker(format!("Failed to connect to Redis: {}", e)))?;

        Ok(Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
            client,
            namespace: namespace.into(),
            claim_ttl_ms: 5 * 60 * 1000,
        })
    }

    /// Set the dispatch-claim TTL in milliseconds (default: 5 minutes).
    pub fn with_claim_ttl_ms(mut self, ttl_ms: u64) -> Self {
        self.claim_ttl_ms = ttl_ms;
        self
    }

    /// The Redis key used to claim exclusive dispatch rights for one
    /// occurrence of `job_id` scheduled to fire at `fire_instant`.
    fn claim_key(&self, job_id: &str, fire_instant: i64) -> String {
        format!("{}:cron:claim:{}:{}", self.namespace, job_id, fire_instant)
    }

    /// Attempt to claim exclusive dispatch rights for one occurrence of a
    /// job via an atomic `SET key value NX PX ttl`.
    ///
    /// Returns `true` for exactly one caller racing this for the same
    /// `(job_id, fire_instant)` pair across any number of processes sharing
    /// this scheduler's `namespace`; every other racer gets `false`.
    async fn claim_occurrence(&self, job_id: &str, fire_instant: i64) -> Result<bool> {
        let mut conn = self
            .client
            .celers_multiplexed_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Connection error: {}", e)))?;

        let key = self.claim_key(job_id, fire_instant);
        let claimed: Option<String> = redis::cmd("SET")
            .arg(&key)
            .arg(1)
            .arg("NX")
            .arg("PX")
            .arg(self.claim_ttl_ms)
            .query_async(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to claim cron occurrence: {}", e)))?;

        Ok(claimed.is_some())
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

    /// Force a scheduled job to be considered due on the next
    /// [`Self::get_due_tasks`] call, regardless of its cron schedule (a "run
    /// now" trigger). Does not change the job's cron expression; after this
    /// firing, `next_run` is recalculated from the cron expression as usual.
    ///
    /// Note: the dispatch claim (see [`Self::claim_occurrence`]) is keyed by
    /// `(job_id, next_run)` at one-second resolution, so calling this twice
    /// for the same job within the same wall-clock second — before the
    /// first firing's `get_due_tasks` call — dispatches only once; the
    /// second call is deduplicated as the same occurrence rather than
    /// producing a second, distinct firing.
    pub fn trigger_now(&self, id: &str) -> Result<()> {
        let mut tasks = self
            .tasks
            .write()
            .map_err(|e| CelersError::Other(format!("Failed to acquire write lock: {}", e)))?;

        let task = tasks
            .get_mut(id)
            .ok_or_else(|| CelersError::Other(format!("Task not found: {}", id)))?;
        task.next_run = current_unix_secs();
        Ok(())
    }

    /// Get all tasks that are due to run, claiming exclusive dispatch rights
    /// for each via Redis so that only one process — of however many share
    /// this scheduler's `namespace` and job set — actually fires each
    /// occurrence. Every firing gets a fresh task id (and fresh
    /// created_at/updated_at/state), so downstream systems keyed on task id
    /// (dedup, result backends, databases) do not collide across
    /// occurrences of the same recurring job.
    ///
    /// On a Redis error, this fails *open* (fires locally anyway, logging a
    /// warning) rather than *closed*: a scheduled job silently never firing
    /// during a Redis outage is judged worse than an occasional duplicate
    /// firing during that same outage. Every process still advances its own
    /// local schedule regardless of whether it wins the claim, so a losing
    /// process's next `is_due()` check reflects the *next* occurrence rather
    /// than retrying the same one forever.
    pub async fn get_due_tasks(&self) -> Result<Vec<SerializedTask>> {
        let due: Vec<(String, i64, SerializedTask)> = {
            let mut tasks = self
                .tasks
                .write()
                .map_err(|e| CelersError::Other(format!("Failed to acquire write lock: {}", e)))?;

            let mut due = Vec::new();
            for task in tasks.values_mut() {
                if task.is_due() {
                    due.push((task.id.clone(), task.next_run, task.task_template.clone()));
                    task.update_after_run();
                }
            }
            due
        };

        let mut due_tasks = Vec::with_capacity(due.len());
        for (job_id, fire_instant, template) in due {
            match self.claim_occurrence(&job_id, fire_instant).await {
                Ok(true) => due_tasks.push(Self::instantiate_firing(template)),
                Ok(false) => {
                    debug!(
                        "Skipping cron occurrence {}@{}: claimed by another process",
                        job_id, fire_instant
                    );
                }
                Err(e) => {
                    warn!(
                        "Failed to claim cron occurrence {}@{}: {} — firing locally to avoid \
                         silently dropping a scheduled occurrence",
                        job_id, fire_instant, e
                    );
                    due_tasks.push(Self::instantiate_firing(template));
                }
            }
        }

        Ok(due_tasks)
    }

    /// Produce a fresh, independently-identified task instance for one
    /// firing of a recurring job.
    fn instantiate_firing(mut template: SerializedTask) -> SerializedTask {
        let now = chrono::Utc::now();
        template.metadata.id = Uuid::new_v4();
        template.metadata.state = TaskState::Pending;
        template.metadata.created_at = now;
        template.metadata.updated_at = now;
        template
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

impl Clone for CronScheduler {
    fn clone(&self) -> Self {
        Self {
            tasks: Arc::clone(&self.tasks),
            client: self.client.clone(),
            namespace: self.namespace.clone(),
            claim_ttl_ms: self.claim_ttl_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use celers_core::TaskMetadata;

    /// Redis connection URL used by the integration-style tests below. A
    /// local Redis is expected to be reachable in this crate's test
    /// environment (see the crate's other Redis-backed modules).
    const TEST_REDIS_URL: &str = "redis://127.0.0.1:6379";

    fn create_test_task() -> SerializedTask {
        SerializedTask {
            metadata: TaskMetadata::new("test_task".to_string()),
            payload: vec![],
        }
    }

    fn test_scheduler(namespace: &str) -> CronScheduler {
        CronScheduler::new(TEST_REDIS_URL, namespace).unwrap()
    }

    #[test]
    fn test_cron_expression_validation() {
        assert!(CronExpression::new("* * * * *").is_ok());
        assert!(CronExpression::new("0 * * * *").is_ok());
        assert!(CronExpression::new("invalid").is_err());
        assert!(CronExpression::new("* * *").is_err());
        // Structurally 5 fields but semantically nonsense (every field out
        // of range) must be rejected too, not silently accepted and later
        // silently degraded to a 60-second fallback interval.
        assert!(CronExpression::new("99 99 99 99 99").is_err());
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
    fn test_update_after_run_computes_from_scheduled_time_not_now() {
        let cron = CronExpression::every_minute();
        let task = create_test_task();
        let mut scheduled = ScheduledTask::new("job".to_string(), cron, task);

        let original_next_run = scheduled.next_run; // grid-aligned to a minute boundary
        scheduled.update_after_run();

        // Must be exactly 60s after the *scheduled* time, not "now + 60s"
        // (which would drift later by however long this call took).
        assert_eq!(scheduled.next_run, original_next_run + 60);
    }

    #[test]
    fn test_cron_scheduler_schedule() {
        let scheduler = test_scheduler("test-ns-schedule");
        let task = create_test_task();

        assert!(scheduler.schedule("job1", "* * * * *", task).is_ok());
        assert_eq!(scheduler.count().unwrap(), 1);
    }

    #[test]
    fn test_cron_scheduler_unschedule() {
        let scheduler = test_scheduler("test-ns-unschedule");
        let task = create_test_task();

        scheduler.schedule("job1", "* * * * *", task).unwrap();
        assert_eq!(scheduler.count().unwrap(), 1);

        assert!(scheduler.unschedule("job1").unwrap());
        assert_eq!(scheduler.count().unwrap(), 0);
    }

    #[test]
    fn test_cron_scheduler_enable_disable() {
        let scheduler = test_scheduler("test-ns-enable-disable");
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
        let scheduler = test_scheduler("test-ns-list-all");
        let task1 = create_test_task();
        let task2 = create_test_task();

        scheduler.schedule("job1", "* * * * *", task1).unwrap();
        scheduler.schedule("job2", "0 * * * *", task2).unwrap();

        let all_tasks = scheduler.list_all().unwrap();
        assert_eq!(all_tasks.len(), 2);
    }

    #[test]
    fn test_cron_scheduler_clear() {
        let scheduler = test_scheduler("test-ns-clear");
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
        let scheduler = test_scheduler("test-ns-clone");
        let task = create_test_task();

        scheduler.schedule("job1", "* * * * *", task).unwrap();

        let cloned = scheduler.clone();
        assert_eq!(cloned.count().unwrap(), 1);
    }

    #[tokio::test]
    async fn test_get_due_tasks_assigns_fresh_id_per_firing() {
        let scheduler = test_scheduler(&format!("test-cron-freshid-{}", Uuid::new_v4()));
        let template = create_test_task();
        let template_id = template.metadata.id;
        scheduler.schedule("job1", "* * * * *", template).unwrap();

        scheduler.trigger_now("job1").unwrap();
        let due1 = scheduler.get_due_tasks().await.unwrap();
        assert_eq!(due1.len(), 1);
        assert_ne!(
            due1[0].metadata.id, template_id,
            "each firing must get a fresh id, not the template's"
        );
        assert_eq!(due1[0].metadata.state, TaskState::Pending);
    }

    /// Two firings issued for the exact same wall-clock second are, by
    /// design, treated as the same dispatch occurrence and deduplicated by
    /// the Redis claim (see `test_claim_occurrence_is_exclusive_...`) — so
    /// "successive firings get distinct ids" is tested directly against
    /// `instantiate_firing`, independent of claim timing, rather than by
    /// racing two real `trigger_now` calls within the same second.
    #[test]
    fn test_instantiate_firing_assigns_fresh_id_and_resets_state() {
        let template = create_test_task();
        let template_id = template.metadata.id;

        let fired1 = CronScheduler::instantiate_firing(template.clone());
        let fired2 = CronScheduler::instantiate_firing(template.clone());

        assert_ne!(fired1.metadata.id, template_id);
        assert_ne!(fired2.metadata.id, template_id);
        assert_ne!(
            fired1.metadata.id, fired2.metadata.id,
            "successive firings of the same recurring job must not share an id"
        );
        assert_eq!(fired1.metadata.state, TaskState::Pending);
        assert_eq!(fired2.metadata.state, TaskState::Pending);
    }

    #[tokio::test]
    async fn test_claim_occurrence_is_exclusive_across_independent_schedulers() {
        // Two independently constructed `CronScheduler`s sharing a namespace
        // simulate two worker processes racing to fire the exact same
        // logical occurrence of a job.
        let namespace = format!("test-cron-claim-{}", Uuid::new_v4());
        let scheduler_a = test_scheduler(&namespace);
        let scheduler_b = test_scheduler(&namespace);

        let fire_instant = 1_700_000_000i64; // arbitrary fixed occurrence

        let won_a = scheduler_a
            .claim_occurrence("shared_job", fire_instant)
            .await
            .unwrap();
        let won_b = scheduler_b
            .claim_occurrence("shared_job", fire_instant)
            .await
            .unwrap();

        assert!(won_a, "the first claimant should win");
        assert!(
            !won_b,
            "a second, independent scheduler racing the SAME occurrence must lose the claim"
        );

        // A different occurrence (different fire_instant) is independent.
        let won_a_next = scheduler_a
            .claim_occurrence("shared_job", fire_instant + 60)
            .await
            .unwrap();
        assert!(won_a_next, "a different occurrence has its own claim");
    }

    #[tokio::test]
    async fn test_get_due_tasks_skips_occurrence_already_claimed_by_another_process() {
        let scheduler = test_scheduler(&format!("test-cron-e2e-{}", Uuid::new_v4()));
        scheduler
            .schedule("job1", "* * * * *", create_test_task())
            .unwrap();
        scheduler.trigger_now("job1").unwrap();

        let fire_instant = scheduler.get("job1").unwrap().unwrap().next_run;

        // Simulate another process having already won the claim for this
        // exact occurrence before we call get_due_tasks.
        let pre_claimed = scheduler
            .claim_occurrence("job1", fire_instant)
            .await
            .unwrap();
        assert!(pre_claimed);

        let due = scheduler.get_due_tasks().await.unwrap();
        assert!(
            due.is_empty(),
            "an occurrence already claimed by another process must not fire again here"
        );

        // The local schedule must still advance so the next poll reflects
        // the *next* occurrence rather than retrying this one forever.
        let advanced = scheduler.get("job1").unwrap().unwrap();
        assert!(
            advanced.next_run > fire_instant,
            "schedule must advance even when the claim is lost"
        );
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
        // `CronExpression::new` now rejects this expression outright (see
        // `test_cron_expression_validation`), so bypass it via direct
        // struct construction — available to tests in the same module tree
        // — to exercise `calculate_next_run`'s defensive fallback path
        // directly: it must never panic even given an expression that could
        // not have passed validation.
        let cron = CronExpression {
            expression: "99 99 99 99 99".to_string(),
        };
        let from = ts(2021, 1, 1, 0, 0, 0);
        let next = ScheduledTask::calculate_next_run(&cron, Some(from));
        assert!(next > from);
    }
}
