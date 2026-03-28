//! Scheduled task types
//!
//! Contains `ScheduledTask`, `TaskOptions`, and related helper types
//! for defining and managing scheduled tasks.

use crate::alert::AlertConfig;
use crate::config::ScheduleError;
use crate::history::{
    CatchupPolicy, DependencyStatus, ExecutionRecord, ExecutionResult, ExecutionState,
    HealthCheckResult, Jitter, RetryPolicy, ScheduleHealth, ScheduleVersion,
};
use crate::schedule::{BusinessCalendar, HolidayCalendar, Schedule};
use crate::wfq::{TaskWeight, WFQState};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

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
    pub(crate) cached_next_run: Option<DateTime<Utc>>,

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

#[allow(dead_code)]
fn default_true() -> bool {
    true
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
