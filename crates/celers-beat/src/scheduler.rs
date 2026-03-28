//! Beat scheduler and related types
//!
//! Contains the main `BeatScheduler` struct and its core implementation,
//! along with conflict detection, scheduler metrics, and statistics types.

use crate::alert::{Alert, AlertCallback, AlertCondition, AlertLevel, AlertManager};
use crate::config::ScheduleError;
use crate::heartbeat::BeatHeartbeat;
use crate::history::{DependencyStatus, ExecutionRecord, ExecutionResult, HealthCheckResult};
use crate::lock::LockManager;
use crate::schedule::Schedule;
use crate::task::ScheduledTask;
use crate::FailureCallback;
use celers_core::lock::DistributedLockBackend;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

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

/// Beat scheduler
#[derive(Serialize, Deserialize)]
pub struct BeatScheduler {
    /// Registered scheduled tasks
    pub(crate) tasks: HashMap<String, ScheduledTask>,

    /// Optional state file path for persistence
    #[serde(skip)]
    pub(crate) state_file: Option<PathBuf>,

    /// Failure notification callbacks
    #[serde(skip)]
    pub(crate) failure_callbacks: Vec<FailureCallback>,

    /// Lock manager for preventing duplicate execution
    #[serde(default)]
    pub(crate) lock_manager: LockManager,

    /// Scheduler instance ID for lock ownership
    #[serde(skip)]
    pub(crate) instance_id: String,

    /// Alert manager for monitoring and notifications
    #[serde(default)]
    pub(crate) alert_manager: AlertManager,

    /// Optional distributed lock backend for cross-instance coordination.
    ///
    /// When set, the scheduler will use this backend instead of the local
    /// `LockManager` for all lock operations, enabling distributed locking
    /// across multiple scheduler instances.
    #[serde(skip)]
    pub(crate) distributed_lock_backend: Option<Arc<dyn DistributedLockBackend>>,

    /// Optional heartbeat manager for leader election and failover.
    ///
    /// When set, the scheduler participates in multi-instance coordination
    /// where only the leader executes scheduled tasks.
    #[serde(skip)]
    pub(crate) heartbeat: Option<BeatHeartbeat>,
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
            distributed_lock_backend: None,
            heartbeat: None,
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
            distributed_lock_backend: None,
            heartbeat: None,
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
                distributed_lock_backend: None,
                heartbeat: None,
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

    /// Set the distributed lock backend for cross-instance coordination.
    ///
    /// When a distributed lock backend is set, the scheduler will use it
    /// for all lock operations. This enables distributed locking across
    /// multiple scheduler instances (e.g., using Redis or a database).
    ///
    /// # Arguments
    /// * `backend` - The distributed lock backend implementation
    ///
    /// # Example
    /// ```
    /// use celers_beat::BeatScheduler;
    /// use celers_beat::lock::InMemoryLockBackend;
    /// use std::sync::Arc;
    ///
    /// let mut scheduler = BeatScheduler::new();
    /// let backend = Arc::new(InMemoryLockBackend::new());
    /// scheduler.with_lock_backend(backend);
    /// ```
    pub fn with_lock_backend(&mut self, backend: Arc<dyn DistributedLockBackend>) -> &mut Self {
        self.distributed_lock_backend = Some(backend);
        self
    }

    /// Get a reference to the distributed lock backend, if set.
    pub fn distributed_lock_backend(&self) -> Option<&Arc<dyn DistributedLockBackend>> {
        self.distributed_lock_backend.as_ref()
    }

    /// Set the heartbeat manager for leader election and failover.
    ///
    /// When a heartbeat is configured, the scheduler participates in
    /// multi-instance coordination. Only the leader instance executes
    /// scheduled tasks; standby instances remain idle until failover.
    pub fn with_heartbeat(&mut self, heartbeat: BeatHeartbeat) -> &mut Self {
        self.heartbeat = Some(heartbeat);
        self
    }

    /// Get a reference to the heartbeat manager, if configured.
    pub fn heartbeat(&self) -> Option<&BeatHeartbeat> {
        self.heartbeat.as_ref()
    }

    /// Check if this scheduler instance is the leader.
    ///
    /// Returns `true` if no heartbeat is configured (single-instance mode)
    /// or if this instance holds the leader lock.
    pub async fn is_leader(&self) -> bool {
        match &self.heartbeat {
            Some(hb) => hb.is_leader().await,
            None => true,
        }
    }

    /// Try to acquire a distributed lock for a task (async version).
    ///
    /// If a distributed lock backend is set, uses it. Otherwise falls back
    /// to the local `LockManager`.
    ///
    /// # Arguments
    /// * `task_name` - Name of the task to lock
    /// * `ttl_secs` - TTL for the lock in seconds
    pub async fn try_acquire_distributed_lock(
        &mut self,
        task_name: &str,
        ttl_secs: u64,
    ) -> Result<bool, ScheduleError> {
        if let Some(ref backend) = self.distributed_lock_backend {
            backend
                .try_acquire(task_name, &self.instance_id, ttl_secs)
                .await
                .map_err(|e| ScheduleError::Invalid(format!("Distributed lock error: {}", e)))
        } else {
            self.lock_manager
                .try_acquire(task_name, &self.instance_id, Some(ttl_secs))
        }
    }

    /// Release a distributed lock for a task (async version).
    ///
    /// If a distributed lock backend is set, uses it. Otherwise falls back
    /// to the local `LockManager`.
    pub async fn release_distributed_lock(
        &mut self,
        task_name: &str,
    ) -> Result<bool, ScheduleError> {
        if let Some(ref backend) = self.distributed_lock_backend {
            backend
                .release(task_name, &self.instance_id)
                .await
                .map_err(|e| ScheduleError::Invalid(format!("Distributed lock error: {}", e)))
        } else {
            self.lock_manager.release(task_name, &self.instance_id)
        }
    }

    /// Renew a distributed lock for a task (async version).
    ///
    /// If a distributed lock backend is set, uses it. Otherwise falls back
    /// to the local `LockManager`.
    pub async fn renew_distributed_lock(
        &mut self,
        task_name: &str,
        ttl_secs: u64,
    ) -> Result<bool, ScheduleError> {
        if let Some(ref backend) = self.distributed_lock_backend {
            backend
                .renew(task_name, &self.instance_id, ttl_secs)
                .await
                .map_err(|e| ScheduleError::Invalid(format!("Distributed lock error: {}", e)))
        } else {
            self.lock_manager
                .renew(task_name, &self.instance_id, Some(ttl_secs))
        }
    }

    /// Check if a task is locked via the distributed backend (async version).
    ///
    /// If a distributed lock backend is set, uses it. Otherwise falls back
    /// to the local `LockManager`.
    pub async fn is_distributed_locked(&self, task_name: &str) -> Result<bool, ScheduleError> {
        if let Some(ref backend) = self.distributed_lock_backend {
            backend
                .is_locked(task_name)
                .await
                .map_err(|e| ScheduleError::Invalid(format!("Distributed lock error: {}", e)))
        } else {
            Ok(self.lock_manager.is_locked(task_name))
        }
    }

    /// Release all distributed locks owned by this scheduler instance (async).
    ///
    /// Useful during graceful shutdown to release all held locks.
    pub async fn release_all_distributed_locks(&mut self) -> Result<u64, ScheduleError> {
        if let Some(ref backend) = self.distributed_lock_backend {
            backend
                .release_all(&self.instance_id)
                .await
                .map_err(|e| ScheduleError::Invalid(format!("Distributed lock error: {}", e)))
        } else {
            let count = self.lock_manager.get_active_locks().len() as u64;
            self.lock_manager.release_all();
            Ok(count)
        }
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
        stats.sort_by_key(|b| std::cmp::Reverse(b.1));

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

    // ===== Heartbeat-Aware Execution Methods =====

    /// Execute one tick of the heartbeat (if configured).
    ///
    /// Returns the current role after the tick, or `None` if no heartbeat
    /// is configured (single-instance mode).
    pub async fn heartbeat_tick(
        &self,
    ) -> Result<Option<crate::heartbeat::BeatRole>, ScheduleError> {
        if let Some(ref hb) = self.heartbeat {
            hb.tick()
                .await
                .map_err(|e| ScheduleError::Invalid(format!("Heartbeat tick error: {}", e)))?;
            Ok(Some(hb.role().await))
        } else {
            Ok(None)
        }
    }

    /// Update heartbeat info with current scheduler state.
    ///
    /// Should be called periodically to keep heartbeat metadata fresh.
    /// This is a no-op if no heartbeat is configured.
    pub async fn update_heartbeat_info(&self) {
        if let Some(ref hb) = self.heartbeat {
            let schedule_count = self.tasks.len();
            let next_due = self.get_due_tasks().first().map(|t| t.name.clone());
            hb.update_info(schedule_count, next_due).await;
        }
    }

    /// Get due tasks, but only if this instance is the leader.
    ///
    /// Returns an empty vector if in standby mode (heartbeat configured but
    /// not leader). Returns all due tasks if no heartbeat is configured
    /// (single-instance mode).
    pub async fn get_leader_due_tasks(&self) -> Vec<&ScheduledTask> {
        if !self.is_leader().await {
            return Vec::new();
        }
        self.get_due_tasks()
    }

    /// Get due tasks by priority, but only if this instance is the leader.
    ///
    /// Returns an empty vector if in standby mode (heartbeat configured but
    /// not leader). Returns all due tasks sorted by priority if no heartbeat
    /// is configured (single-instance mode).
    pub async fn get_leader_due_tasks_by_priority(&self) -> Vec<&ScheduledTask> {
        if !self.is_leader().await {
            return Vec::new();
        }
        self.get_due_tasks_by_priority()
    }

    /// Perform a full scheduler tick:
    ///
    /// 1. Tick heartbeat (leader election / lease renewal)
    /// 2. If leader, get due tasks by priority
    /// 3. Mark due tasks as running
    /// 4. Update heartbeat info
    /// 5. Auto-save state if persistence is configured
    ///
    /// Returns the list of due task names (empty if this instance is standby).
    pub async fn tick(&mut self) -> Result<Vec<String>, ScheduleError> {
        // Step 1: Heartbeat tick
        let role = self.heartbeat_tick().await?;

        // Step 2: Check if we should execute
        let is_leader = match role {
            Some(crate::heartbeat::BeatRole::Leader) => true,
            Some(_) => false,
            None => true, // No heartbeat = single instance, always execute
        };

        if !is_leader {
            // Update heartbeat info even in standby mode
            self.update_heartbeat_info().await;
            return Ok(Vec::new());
        }

        // Step 3: Get due tasks
        let due_task_names: Vec<String> = self
            .get_due_tasks_by_priority()
            .iter()
            .map(|t| t.name.clone())
            .collect();

        // Step 4: Mark tasks as running
        for name in &due_task_names {
            let _ = self.mark_task_run(name);
        }

        // Step 5: Update heartbeat info
        self.update_heartbeat_info().await;

        // Step 6: Auto-save state if persistence configured
        let _ = self.save_state();

        Ok(due_task_names)
    }

    /// Gracefully shutdown the scheduler.
    ///
    /// Releases the leader lock if this instance is the leader, saves
    /// state, and releases all distributed locks.
    pub async fn shutdown_graceful(&mut self) -> Result<(), ScheduleError> {
        if let Some(ref hb) = self.heartbeat {
            hb.shutdown()
                .await
                .map_err(|e| ScheduleError::Invalid(format!("Heartbeat shutdown error: {}", e)))?;
        }
        self.save_state()?;
        Ok(())
    }
}

impl Default for BeatScheduler {
    fn default() -> Self {
        Self::new()
    }
}
