//! Extended BeatScheduler functionality
//!
//! Contains additional `BeatScheduler` methods including lock management,
//! conflict detection, conflict resolution, starvation prevention,
//! schedule preview, dry-run simulation, and comprehensive statistics.

use crate::alert::{Alert, WebhookConfig};
use crate::config::ScheduleError;
use crate::history::{Jitter, ScheduleHealth};
use crate::lock::ScheduleLock;
use crate::scheduler::{BeatScheduler, ConflictSeverity, ScheduleConflict};
use crate::task::ScheduledTask;
use crate::wfq::{WFQState, WFQTaskInfo};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

impl BeatScheduler {
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

        let avg_duration_ms = total_duration_ms.checked_div(execution_count);

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
