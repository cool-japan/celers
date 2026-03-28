use crate::state::TaskState;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt;
use uuid::Uuid;

/// Unique identifier for a task
pub type TaskId = Uuid;

/// Batch utility functions for working with multiple tasks
pub mod batch {
    use super::{SerializedTask, TaskState, Uuid};

    /// Validate a collection of tasks, returning all errors
    ///
    /// # Example
    /// ```
    /// use celers_core::{SerializedTask, task::batch};
    ///
    /// let tasks = vec![
    ///     SerializedTask::new("task1".to_string(), vec![1, 2, 3]),
    ///     SerializedTask::new("task2".to_string(), vec![4, 5, 6]),
    /// ];
    ///
    /// let errors = batch::validate_all(&tasks);
    /// assert!(errors.is_empty());
    /// ```
    #[must_use]
    pub fn validate_all(tasks: &[SerializedTask]) -> Vec<(usize, String)> {
        tasks
            .iter()
            .enumerate()
            .filter_map(|(idx, task)| task.validate().err().map(|e| (idx, e)))
            .collect()
    }

    /// Filter tasks by state
    ///
    /// # Example
    /// ```
    /// use celers_core::{SerializedTask, TaskState, task::batch};
    ///
    /// let mut tasks = vec![
    ///     SerializedTask::new("task1".to_string(), vec![1, 2, 3]),
    ///     SerializedTask::new("task2".to_string(), vec![4, 5, 6]),
    /// ];
    /// tasks[0].metadata.state = TaskState::Running;
    ///
    /// let running = batch::filter_by_state(&tasks, |s| matches!(s, TaskState::Running));
    /// assert_eq!(running.len(), 1);
    /// ```
    #[must_use]
    pub fn filter_by_state<F>(tasks: &[SerializedTask], predicate: F) -> Vec<&SerializedTask>
    where
        F: Fn(&TaskState) -> bool,
    {
        tasks
            .iter()
            .filter(|task| predicate(&task.metadata.state))
            .collect()
    }

    /// Filter tasks by priority
    ///
    /// # Example
    /// ```
    /// use celers_core::{SerializedTask, task::batch};
    ///
    /// let tasks = vec![
    ///     SerializedTask::new("task1".to_string(), vec![1]).with_priority(10),
    ///     SerializedTask::new("task2".to_string(), vec![2]).with_priority(5),
    ///     SerializedTask::new("task3".to_string(), vec![3]),
    /// ];
    ///
    /// let high_priority = batch::filter_high_priority(&tasks);
    /// assert_eq!(high_priority.len(), 2);
    /// ```
    #[must_use]
    pub fn filter_high_priority(tasks: &[SerializedTask]) -> Vec<&SerializedTask> {
        tasks
            .iter()
            .filter(|task| task.metadata.is_high_priority())
            .collect()
    }

    /// Sort tasks by priority (highest first)
    ///
    /// # Example
    /// ```
    /// use celers_core::{SerializedTask, task::batch};
    ///
    /// let mut tasks = vec![
    ///     SerializedTask::new("task1".to_string(), vec![1]).with_priority(5),
    ///     SerializedTask::new("task2".to_string(), vec![2]).with_priority(10),
    ///     SerializedTask::new("task3".to_string(), vec![3]).with_priority(1),
    /// ];
    ///
    /// batch::sort_by_priority(&mut tasks);
    /// assert_eq!(tasks[0].metadata.priority, 10);
    /// assert_eq!(tasks[1].metadata.priority, 5);
    /// assert_eq!(tasks[2].metadata.priority, 1);
    /// ```
    pub fn sort_by_priority(tasks: &mut [SerializedTask]) {
        tasks.sort_by_key(|b| std::cmp::Reverse(b.metadata.priority));
    }

    /// Count tasks by state
    ///
    /// # Example
    /// ```
    /// use celers_core::{SerializedTask, TaskState, task::batch};
    ///
    /// let mut tasks = vec![
    ///     SerializedTask::new("task1".to_string(), vec![1]),
    ///     SerializedTask::new("task2".to_string(), vec![2]),
    /// ];
    /// tasks[0].metadata.state = TaskState::Running;
    ///
    /// let counts = batch::count_by_state(&tasks);
    /// assert_eq!(counts.get("RUNNING"), Some(&1));
    /// assert_eq!(counts.get("PENDING"), Some(&1));
    /// ```
    #[must_use]
    pub fn count_by_state(tasks: &[SerializedTask]) -> std::collections::HashMap<String, usize> {
        let mut counts = std::collections::HashMap::new();
        for task in tasks {
            *counts
                .entry(task.metadata.state.name().to_string())
                .or_insert(0) += 1;
        }
        counts
    }

    /// Check if any tasks have expired
    ///
    /// # Example
    /// ```
    /// use celers_core::{SerializedTask, task::batch};
    ///
    /// let tasks = vec![
    ///     SerializedTask::new("task1".to_string(), vec![1]).with_timeout(60),
    ///     SerializedTask::new("task2".to_string(), vec![2]),
    /// ];
    ///
    /// // Fresh tasks shouldn't be expired
    /// assert!(!batch::has_expired_tasks(&tasks));
    /// ```
    #[inline]
    #[must_use]
    pub fn has_expired_tasks(tasks: &[SerializedTask]) -> bool {
        tasks.iter().any(super::SerializedTask::is_expired)
    }

    /// Get tasks that have expired
    ///
    /// # Example
    /// ```
    /// use celers_core::{SerializedTask, task::batch};
    ///
    /// let tasks = vec![
    ///     SerializedTask::new("task1".to_string(), vec![1]).with_timeout(60),
    ///     SerializedTask::new("task2".to_string(), vec![2]),
    /// ];
    ///
    /// let expired = batch::get_expired_tasks(&tasks);
    /// // Fresh tasks shouldn't be expired
    /// assert_eq!(expired.len(), 0);
    /// ```
    #[inline]
    #[must_use]
    pub fn get_expired_tasks(tasks: &[SerializedTask]) -> Vec<&SerializedTask> {
        tasks.iter().filter(|task| task.is_expired()).collect()
    }

    /// Calculate total payload size for a collection of tasks
    ///
    /// # Example
    /// ```
    /// use celers_core::{SerializedTask, task::batch};
    ///
    /// let tasks = vec![
    ///     SerializedTask::new("task1".to_string(), vec![1, 2, 3]),
    ///     SerializedTask::new("task2".to_string(), vec![4, 5]),
    /// ];
    ///
    /// let total_size = batch::total_payload_size(&tasks);
    /// assert_eq!(total_size, 5);
    /// ```
    #[must_use]
    pub fn total_payload_size(tasks: &[SerializedTask]) -> usize {
        tasks.iter().map(super::SerializedTask::payload_size).sum()
    }

    /// Find tasks with dependencies
    ///
    /// # Example
    /// ```
    /// use celers_core::{SerializedTask, task::batch};
    /// use uuid::Uuid;
    ///
    /// let parent_id = Uuid::new_v4();
    /// let tasks = vec![
    ///     SerializedTask::new("task1".to_string(), vec![1]),
    ///     SerializedTask::new("task2".to_string(), vec![2]).with_dependency(parent_id),
    /// ];
    ///
    /// let with_deps = batch::filter_with_dependencies(&tasks);
    /// assert_eq!(with_deps.len(), 1);
    /// ```
    #[must_use]
    pub fn filter_with_dependencies(tasks: &[SerializedTask]) -> Vec<&SerializedTask> {
        tasks
            .iter()
            .filter(|task| task.metadata.has_dependencies())
            .collect()
    }

    /// Find tasks that can be retried
    ///
    /// # Example
    /// ```
    /// use celers_core::{SerializedTask, TaskState, task::batch};
    ///
    /// let mut tasks = vec![
    ///     SerializedTask::new("task1".to_string(), vec![1]).with_max_retries(3),
    ///     SerializedTask::new("task2".to_string(), vec![2]).with_max_retries(3),
    /// ];
    /// tasks[0].metadata.state = TaskState::Failed("error".to_string());
    /// tasks[1].metadata.state = TaskState::Succeeded(vec![]);
    ///
    /// let can_retry = batch::filter_retryable(&tasks);
    /// assert_eq!(can_retry.len(), 1);
    /// ```
    #[must_use]
    pub fn filter_retryable(tasks: &[SerializedTask]) -> Vec<&SerializedTask> {
        tasks.iter().filter(|task| task.can_retry()).collect()
    }

    /// Find tasks by name pattern (contains)
    ///
    /// # Example
    /// ```
    /// use celers_core::{SerializedTask, task::batch};
    ///
    /// let tasks = vec![
    ///     SerializedTask::new("process_data".to_string(), vec![1]),
    ///     SerializedTask::new("process_image".to_string(), vec![2]),
    ///     SerializedTask::new("send_email".to_string(), vec![3]),
    /// ];
    ///
    /// let process_tasks = batch::filter_by_name_pattern(&tasks, "process");
    /// assert_eq!(process_tasks.len(), 2);
    /// ```
    #[must_use]
    pub fn filter_by_name_pattern<'a>(
        tasks: &'a [SerializedTask],
        pattern: &str,
    ) -> Vec<&'a SerializedTask> {
        tasks
            .iter()
            .filter(|task| task.metadata.name.contains(pattern))
            .collect()
    }

    /// Group tasks by their workflow group ID
    ///
    /// # Example
    /// ```
    /// use celers_core::{SerializedTask, task::batch};
    /// use uuid::Uuid;
    ///
    /// let group1 = Uuid::new_v4();
    /// let group2 = Uuid::new_v4();
    ///
    /// let tasks = vec![
    ///     SerializedTask::new("task1".to_string(), vec![1]).with_group_id(group1),
    ///     SerializedTask::new("task2".to_string(), vec![2]).with_group_id(group1),
    ///     SerializedTask::new("task3".to_string(), vec![3]).with_group_id(group2),
    ///     SerializedTask::new("task4".to_string(), vec![4]),
    /// ];
    ///
    /// let groups = batch::group_by_workflow_id(&tasks);
    /// assert_eq!(groups.len(), 2);
    /// ```
    #[must_use]
    pub fn group_by_workflow_id(
        tasks: &[SerializedTask],
    ) -> std::collections::HashMap<Uuid, Vec<&SerializedTask>> {
        let mut groups = std::collections::HashMap::new();
        for task in tasks {
            if let Some(group_id) = task.metadata.group_id {
                groups.entry(group_id).or_insert_with(Vec::new).push(task);
            }
        }
        groups
    }

    /// Find terminal tasks (succeeded or failed)
    ///
    /// # Example
    /// ```
    /// use celers_core::{SerializedTask, TaskState, task::batch};
    ///
    /// let mut tasks = vec![
    ///     SerializedTask::new("task1".to_string(), vec![1]),
    ///     SerializedTask::new("task2".to_string(), vec![2]),
    /// ];
    /// tasks[0].metadata.state = TaskState::Succeeded(vec![1, 2, 3]);
    ///
    /// let terminal = batch::filter_terminal(&tasks);
    /// assert_eq!(terminal.len(), 1);
    /// ```
    #[must_use]
    pub fn filter_terminal(tasks: &[SerializedTask]) -> Vec<&SerializedTask> {
        tasks.iter().filter(|task| task.is_terminal()).collect()
    }

    /// Find active tasks (pending, running, or retrying)
    ///
    /// # Example
    /// ```
    /// use celers_core::{SerializedTask, TaskState, task::batch};
    ///
    /// let mut tasks = vec![
    ///     SerializedTask::new("task1".to_string(), vec![1]),
    ///     SerializedTask::new("task2".to_string(), vec![2]),
    /// ];
    /// tasks[1].metadata.state = TaskState::Succeeded(vec![]);
    ///
    /// let active = batch::filter_active(&tasks);
    /// assert_eq!(active.len(), 1);
    /// ```
    #[must_use]
    pub fn filter_active(tasks: &[SerializedTask]) -> Vec<&SerializedTask> {
        tasks.iter().filter(|task| task.is_active()).collect()
    }

    /// Calculate average payload size
    ///
    /// # Example
    /// ```
    /// use celers_core::{SerializedTask, task::batch};
    ///
    /// let tasks = vec![
    ///     SerializedTask::new("task1".to_string(), vec![1, 2, 3]),
    ///     SerializedTask::new("task2".to_string(), vec![4, 5]),
    /// ];
    ///
    /// let avg = batch::average_payload_size(&tasks);
    /// assert_eq!(avg, 2); // (3 + 2) / 2 = 2
    /// ```
    #[must_use]
    pub fn average_payload_size(tasks: &[SerializedTask]) -> usize {
        if tasks.is_empty() {
            0
        } else {
            total_payload_size(tasks) / tasks.len()
        }
    }

    /// Find the oldest task by creation time
    ///
    /// # Example
    /// ```
    /// use celers_core::{SerializedTask, task::batch};
    ///
    /// let tasks = vec![
    ///     SerializedTask::new("task1".to_string(), vec![1]),
    ///     SerializedTask::new("task2".to_string(), vec![2]),
    /// ];
    ///
    /// let oldest = batch::find_oldest(&tasks);
    /// assert!(oldest.is_some());
    /// ```
    #[must_use]
    pub fn find_oldest(tasks: &[SerializedTask]) -> Option<&SerializedTask> {
        tasks.iter().min_by_key(|task| task.metadata.created_at)
    }

    /// Find the newest task by creation time
    ///
    /// # Example
    /// ```
    /// use celers_core::{SerializedTask, task::batch};
    ///
    /// let tasks = vec![
    ///     SerializedTask::new("task1".to_string(), vec![1]),
    ///     SerializedTask::new("task2".to_string(), vec![2]),
    /// ];
    ///
    /// let newest = batch::find_newest(&tasks);
    /// assert!(newest.is_some());
    /// ```
    #[must_use]
    pub fn find_newest(tasks: &[SerializedTask]) -> Option<&SerializedTask> {
        tasks.iter().max_by_key(|task| task.metadata.created_at)
    }
}

/// Task metadata including execution information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskMetadata {
    /// Unique task identifier
    pub id: TaskId,

    /// Task name/type identifier
    pub name: String,

    /// Current state of the task
    pub state: TaskState,

    /// When the task was created
    pub created_at: DateTime<Utc>,

    /// When the task was last updated
    pub updated_at: DateTime<Utc>,

    /// Maximum number of retry attempts
    pub max_retries: u32,

    /// Task timeout in seconds
    pub timeout_secs: Option<u64>,

    /// Task priority (higher = more important)
    pub priority: i32,

    /// Group ID (for workflow grouping)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_id: Option<Uuid>,

    /// Chord ID (for barrier synchronization)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chord_id: Option<Uuid>,

    /// Task dependencies (tasks that must complete before this task can execute)
    #[serde(skip_serializing_if = "HashSet::is_empty", default)]
    pub dependencies: HashSet<TaskId>,
}

impl TaskMetadata {
    #[inline]
    #[must_use]
    pub fn new(name: String) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            name,
            state: TaskState::Pending,
            created_at: now,
            updated_at: now,
            max_retries: 3,
            timeout_secs: None,
            priority: 0,
            group_id: None,
            chord_id: None,
            dependencies: HashSet::new(),
        }
    }

    #[inline]
    #[must_use]
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    #[inline]
    #[must_use]
    pub fn with_timeout(mut self, timeout_secs: u64) -> Self {
        self.timeout_secs = Some(timeout_secs);
        self
    }

    #[inline]
    #[must_use]
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// Set the group ID for workflow grouping
    #[inline]
    #[must_use]
    pub fn with_group_id(mut self, group_id: Uuid) -> Self {
        self.group_id = Some(group_id);
        self
    }

    /// Set the chord ID for barrier synchronization
    #[inline]
    #[must_use]
    pub fn with_chord_id(mut self, chord_id: Uuid) -> Self {
        self.chord_id = Some(chord_id);
        self
    }

    /// Get the age of the task (time since creation)
    #[inline]
    #[must_use]
    pub fn age(&self) -> chrono::Duration {
        Utc::now() - self.created_at
    }

    /// Check if the task has expired based on its timeout
    #[inline]
    #[must_use]
    #[allow(clippy::cast_possible_wrap)]
    pub fn is_expired(&self) -> bool {
        if let Some(timeout_secs) = self.timeout_secs {
            let elapsed = (Utc::now() - self.created_at).num_seconds();
            elapsed > timeout_secs as i64
        } else {
            false
        }
    }

    /// Check if the task is in a terminal state (Succeeded or Failed)
    #[inline]
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }

    /// Check if the task is in a running or active state
    #[inline]
    #[must_use]
    pub fn is_active(&self) -> bool {
        matches!(
            self.state,
            TaskState::Pending | TaskState::Reserved | TaskState::Running | TaskState::Retrying(_)
        )
    }

    /// Validate the task metadata
    ///
    /// Returns an error if any of the metadata fields are invalid:
    /// - Name must not be empty
    /// - Max retries must be reasonable (< 1000)
    /// - Timeout must be at least 1 second if set
    /// - Priority must be in valid range (-2147483648 to 2147483647)
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails.
    pub fn validate(&self) -> Result<(), String> {
        if self.name.is_empty() {
            return Err("Task name cannot be empty".to_string());
        }

        if self.max_retries > 1000 {
            return Err("Max retries cannot exceed 1000".to_string());
        }

        if let Some(timeout) = self.timeout_secs {
            if timeout == 0 {
                return Err("Timeout must be at least 1 second".to_string());
            }
            if timeout > 86400 {
                return Err("Timeout cannot exceed 24 hours (86400 seconds)".to_string());
            }
        }

        Ok(())
    }

    /// Check if task has a timeout configured
    #[inline]
    #[must_use]
    pub fn has_timeout(&self) -> bool {
        self.timeout_secs.is_some()
    }

    /// Check if task is part of a group
    #[inline]
    #[must_use]
    pub fn has_group_id(&self) -> bool {
        self.group_id.is_some()
    }

    /// Check if task is part of a chord
    #[inline]
    #[must_use]
    pub fn has_chord_id(&self) -> bool {
        self.chord_id.is_some()
    }

    /// Check if task has custom priority (non-zero)
    #[inline]
    #[must_use]
    pub const fn has_priority(&self) -> bool {
        self.priority != 0
    }

    /// Check if task has high priority (priority > 0)
    #[inline]
    #[must_use]
    pub const fn is_high_priority(&self) -> bool {
        self.priority > 0
    }

    /// Check if task has low priority (priority < 0)
    #[inline]
    #[must_use]
    pub const fn is_low_priority(&self) -> bool {
        self.priority < 0
    }

    /// Add a task dependency
    #[inline]
    #[must_use]
    pub fn with_dependency(mut self, dependency: TaskId) -> Self {
        self.dependencies.insert(dependency);
        self
    }

    /// Add multiple task dependencies
    #[inline]
    #[must_use]
    pub fn with_dependencies(mut self, dependencies: impl IntoIterator<Item = TaskId>) -> Self {
        self.dependencies.extend(dependencies);
        self
    }

    /// Check if task has any dependencies
    #[inline]
    #[must_use]
    pub fn has_dependencies(&self) -> bool {
        !self.dependencies.is_empty()
    }

    /// Get the number of dependencies
    #[inline]
    #[must_use]
    pub fn dependency_count(&self) -> usize {
        self.dependencies.len()
    }

    /// Check if a specific task is a dependency
    #[inline]
    #[must_use]
    pub fn depends_on(&self, task_id: &TaskId) -> bool {
        self.dependencies.contains(task_id)
    }

    /// Remove a dependency
    #[inline]
    pub fn remove_dependency(&mut self, task_id: &TaskId) -> bool {
        self.dependencies.remove(task_id)
    }

    /// Clear all dependencies
    #[inline]
    pub fn clear_dependencies(&mut self) {
        self.dependencies.clear();
    }

    // ===== Convenience State Checks =====

    /// Check if task is in Pending state
    #[inline]
    #[must_use]
    pub fn is_pending(&self) -> bool {
        matches!(self.state, TaskState::Pending)
    }

    /// Check if task is in Running state
    #[inline]
    #[must_use]
    pub fn is_running(&self) -> bool {
        matches!(self.state, TaskState::Running)
    }

    /// Check if task is in Succeeded state
    #[inline]
    #[must_use]
    pub fn is_succeeded(&self) -> bool {
        matches!(self.state, TaskState::Succeeded(_))
    }

    /// Check if task is in Failed state
    #[inline]
    #[must_use]
    pub fn is_failed(&self) -> bool {
        matches!(self.state, TaskState::Failed(_))
    }

    /// Check if task is in Retrying state
    #[inline]
    #[must_use]
    pub fn is_retrying(&self) -> bool {
        matches!(self.state, TaskState::Retrying(_))
    }

    /// Check if task is in Reserved state
    #[inline]
    #[must_use]
    pub fn is_reserved(&self) -> bool {
        matches!(self.state, TaskState::Reserved)
    }

    // ===== Time-related Helpers =====

    /// Get remaining time before timeout (None if no timeout or already expired)
    ///
    /// # Example
    /// ```
    /// use celers_core::TaskMetadata;
    ///
    /// let task = TaskMetadata::new("test".to_string()).with_timeout(60);
    /// if let Some(remaining) = task.time_remaining() {
    ///     println!("Task has {} seconds remaining", remaining.num_seconds());
    /// }
    /// ```
    #[inline]
    #[must_use]
    #[allow(clippy::cast_possible_wrap)]
    pub fn time_remaining(&self) -> Option<chrono::Duration> {
        self.timeout_secs.and_then(|timeout| {
            let elapsed = Utc::now() - self.created_at;
            let timeout_duration = chrono::Duration::seconds(timeout as i64);
            let remaining = timeout_duration - elapsed;
            if remaining.num_seconds() > 0 {
                Some(remaining)
            } else {
                None
            }
        })
    }

    /// Get the time elapsed since task creation
    ///
    /// # Example
    /// ```
    /// use celers_core::TaskMetadata;
    ///
    /// let task = TaskMetadata::new("test".to_string());
    /// let elapsed = task.time_elapsed();
    /// assert!(elapsed.num_seconds() >= 0);
    /// ```
    #[inline]
    #[must_use]
    pub fn time_elapsed(&self) -> chrono::Duration {
        Utc::now() - self.created_at
    }

    // ===== Retry Helpers =====

    /// Check if task can be retried based on current retry count
    ///
    /// # Example
    /// ```
    /// use celers_core::{TaskMetadata, TaskState};
    ///
    /// let mut task = TaskMetadata::new("test".to_string()).with_max_retries(3);
    /// task.state = TaskState::Failed("error".to_string());
    /// assert!(task.can_retry());
    ///
    /// task.state = TaskState::Retrying(3);
    /// assert!(!task.can_retry());
    /// ```
    #[inline]
    #[must_use]
    pub fn can_retry(&self) -> bool {
        self.state.can_retry(self.max_retries)
    }

    /// Get current retry count
    ///
    /// # Example
    /// ```
    /// use celers_core::{TaskMetadata, TaskState};
    ///
    /// let mut task = TaskMetadata::new("test".to_string());
    /// task.state = TaskState::Retrying(2);
    /// assert_eq!(task.retry_count(), 2);
    /// ```
    #[inline]
    #[must_use]
    pub const fn retry_count(&self) -> u32 {
        self.state.retry_count()
    }

    /// Get remaining retry attempts
    ///
    /// # Example
    /// ```
    /// use celers_core::{TaskMetadata, TaskState};
    ///
    /// let mut task = TaskMetadata::new("test".to_string()).with_max_retries(5);
    /// task.state = TaskState::Retrying(2);
    /// assert_eq!(task.retries_remaining(), 3);
    /// ```
    #[inline]
    #[must_use]
    pub const fn retries_remaining(&self) -> u32 {
        let current = self.retry_count();
        self.max_retries.saturating_sub(current)
    }

    // ===== Workflow Helpers =====

    /// Check if task is part of any workflow (group or chord)
    ///
    /// # Example
    /// ```
    /// use celers_core::TaskMetadata;
    /// use uuid::Uuid;
    ///
    /// let task = TaskMetadata::new("test".to_string())
    ///     .with_group_id(Uuid::new_v4());
    /// assert!(task.is_part_of_workflow());
    /// ```
    #[inline]
    #[must_use]
    pub fn is_part_of_workflow(&self) -> bool {
        self.group_id.is_some() || self.chord_id.is_some()
    }

    /// Get group ID if task is part of a group
    #[inline]
    #[must_use]
    pub fn get_group_id(&self) -> Option<&Uuid> {
        self.group_id.as_ref()
    }

    /// Get chord ID if task is part of a chord
    #[inline]
    #[must_use]
    pub fn get_chord_id(&self) -> Option<&Uuid> {
        self.chord_id.as_ref()
    }

    // ===== State Transition Helpers =====

    /// Update task state to Running
    ///
    /// # Example
    /// ```
    /// use celers_core::{TaskMetadata, TaskState};
    ///
    /// let mut task = TaskMetadata::new("test".to_string());
    /// task.mark_as_running();
    /// assert!(task.is_running());
    /// ```
    #[inline]
    pub fn mark_as_running(&mut self) {
        self.state = TaskState::Running;
        self.updated_at = Utc::now();
    }

    /// Update task state to Succeeded with result
    ///
    /// # Example
    /// ```
    /// use celers_core::{TaskMetadata, TaskState};
    ///
    /// let mut task = TaskMetadata::new("test".to_string());
    /// task.mark_as_succeeded(vec![1, 2, 3]);
    /// assert!(task.is_succeeded());
    /// ```
    #[inline]
    pub fn mark_as_succeeded(&mut self, result: Vec<u8>) {
        self.state = TaskState::Succeeded(result);
        self.updated_at = Utc::now();
    }

    /// Update task state to Failed with error message
    ///
    /// # Example
    /// ```
    /// use celers_core::{TaskMetadata, TaskState};
    ///
    /// let mut task = TaskMetadata::new("test".to_string());
    /// task.mark_as_failed("Connection timeout");
    /// assert!(task.is_failed());
    /// ```
    #[inline]
    pub fn mark_as_failed(&mut self, error: impl Into<String>) {
        self.state = TaskState::Failed(error.into());
        self.updated_at = Utc::now();
    }

    /// Clone task with a new ID (useful for task retry/duplication)
    ///
    /// # Example
    /// ```
    /// use celers_core::TaskMetadata;
    ///
    /// let task = TaskMetadata::new("test".to_string()).with_priority(5);
    /// let cloned = task.with_new_id();
    /// assert_ne!(task.id, cloned.id);
    /// assert_eq!(task.name, cloned.name);
    /// assert_eq!(task.priority, cloned.priority);
    /// ```
    #[inline]
    #[must_use]
    pub fn with_new_id(&self) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            name: self.name.clone(),
            state: TaskState::Pending,
            created_at: now,
            updated_at: now,
            max_retries: self.max_retries,
            timeout_secs: self.timeout_secs,
            priority: self.priority,
            group_id: self.group_id,
            chord_id: self.chord_id,
            dependencies: self.dependencies.clone(),
        }
    }
}

impl fmt::Display for TaskMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Task[{}] name={} state={} priority={} retries={}/{}",
            &self.id.to_string()[..8],
            self.name,
            self.state,
            self.priority,
            self.state.retry_count(),
            self.max_retries
        )?;

        if let Some(timeout) = self.timeout_secs {
            write!(f, " timeout={timeout}s")?;
        }

        if let Some(chord_id) = self.chord_id {
            write!(f, " chord={}", &chord_id.to_string()[..8])?;
        }

        Ok(())
    }
}

/// Trait for tasks that can be executed
#[async_trait::async_trait]
pub trait Task: Send + Sync {
    /// The input type for this task
    type Input: Serialize + for<'de> Deserialize<'de> + Send;

    /// The output type for this task
    type Output: Serialize + for<'de> Deserialize<'de> + Send;

    /// Execute the task with the given input
    async fn execute(&self, input: Self::Input) -> crate::Result<Self::Output>;

    /// Get the task name
    fn name(&self) -> &str;
}

/// Serialized task ready for queue
///
/// # Zero-Copy Serialization Considerations
///
/// The current implementation uses `Vec<u8>` for the payload, which requires
/// copying data during serialization and deserialization. For high-performance
/// scenarios, consider these alternatives:
///
/// 1. **`Bytes` from `bytes` crate**: Provides cheap cloning via reference counting
///    ```ignore
///    use bytes::Bytes;
///    pub payload: Bytes,
///    ```
///
/// 2. **`Arc<[u8]>`**: Reference-counted slice for shared ownership
///    ```ignore
///    use std::sync::Arc;
///    pub payload: Arc<[u8]>,
///    ```
///
/// 3. **Borrowed payloads with lifetimes**: For truly zero-copy deserialization
///    ```ignore
///    #[derive(Deserialize)]
///    pub struct SerializedTask<'a> {
///        #[serde(borrow)]
///        pub payload: &'a [u8],
///    }
///    ```
///
/// Trade-offs:
/// - `Vec<u8>`: Simple, owned data, but requires copying
/// - `Bytes`: Cheap cloning, but requires external dependency
/// - `Arc<[u8]>`: Cheap cloning, but atomic operations have overhead
/// - Borrowed: True zero-copy, but lifetime complexity and limited use cases
///
/// For most use cases, the current `Vec<u8>` approach provides a good balance
/// of simplicity and performance. Consider alternatives only when profiling shows
/// serialization as a bottleneck.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializedTask {
    /// Task metadata
    pub metadata: TaskMetadata,

    /// Serialized task payload
    pub payload: Vec<u8>,
}

impl SerializedTask {
    #[inline]
    #[must_use]
    pub fn new(name: String, payload: Vec<u8>) -> Self {
        Self {
            metadata: TaskMetadata::new(name),
            payload,
        }
    }

    #[inline]
    #[must_use]
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.metadata.priority = priority;
        self
    }

    #[inline]
    #[must_use]
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.metadata.max_retries = max_retries;
        self
    }

    #[inline]
    #[must_use]
    pub fn with_timeout(mut self, timeout_secs: u64) -> Self {
        self.metadata.timeout_secs = Some(timeout_secs);
        self
    }

    /// Set the group ID for workflow grouping
    #[inline]
    #[must_use]
    pub fn with_group_id(mut self, group_id: Uuid) -> Self {
        self.metadata.group_id = Some(group_id);
        self
    }

    /// Set the chord ID for barrier synchronization
    #[inline]
    #[must_use]
    pub fn with_chord_id(mut self, chord_id: Uuid) -> Self {
        self.metadata.chord_id = Some(chord_id);
        self
    }

    /// Get the age of the task (time since creation)
    #[inline]
    #[must_use]
    pub fn age(&self) -> chrono::Duration {
        self.metadata.age()
    }

    /// Check if the task has expired based on its timeout
    #[inline]
    #[must_use]
    pub fn is_expired(&self) -> bool {
        self.metadata.is_expired()
    }

    /// Check if the task is in a terminal state (Success or Failure)
    #[inline]
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        self.metadata.is_terminal()
    }

    /// Check if the task is in a running or active state
    #[inline]
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.metadata.is_active()
    }

    /// Validate the serialized task
    ///
    /// Validates both metadata and payload constraints:
    /// - Delegates metadata validation to `TaskMetadata::validate()`
    /// - Checks payload size (must be < 1MB by default)
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails.
    pub fn validate(&self) -> Result<(), String> {
        self.metadata.validate()?;

        if self.payload.is_empty() {
            return Err("Task payload cannot be empty".to_string());
        }

        if self.payload.len() > 1_048_576 {
            return Err(format!(
                "Task payload too large: {} bytes (max 1MB)",
                self.payload.len()
            ));
        }

        Ok(())
    }

    /// Validate with custom payload size limit
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails.
    pub fn validate_with_limit(&self, max_payload_bytes: usize) -> Result<(), String> {
        self.metadata.validate()?;

        if self.payload.is_empty() {
            return Err("Task payload cannot be empty".to_string());
        }

        if self.payload.len() > max_payload_bytes {
            return Err(format!(
                "Task payload too large: {} bytes (max {} bytes)",
                self.payload.len(),
                max_payload_bytes
            ));
        }

        Ok(())
    }

    /// Check if task has a timeout configured
    #[inline]
    #[must_use]
    pub fn has_timeout(&self) -> bool {
        self.metadata.has_timeout()
    }

    /// Check if task is part of a group
    #[inline]
    #[must_use]
    pub fn has_group_id(&self) -> bool {
        self.metadata.has_group_id()
    }

    /// Check if task is part of a chord
    #[inline]
    #[must_use]
    pub fn has_chord_id(&self) -> bool {
        self.metadata.has_chord_id()
    }

    /// Check if task has custom priority (non-zero)
    #[inline]
    #[must_use]
    pub fn has_priority(&self) -> bool {
        self.metadata.has_priority()
    }

    /// Get payload size in bytes
    #[inline]
    #[must_use]
    pub const fn payload_size(&self) -> usize {
        self.payload.len()
    }

    /// Check if payload is empty
    #[inline]
    #[must_use]
    pub fn has_empty_payload(&self) -> bool {
        self.payload.is_empty()
    }

    /// Add a task dependency
    #[inline]
    #[must_use]
    pub fn with_dependency(mut self, dependency: TaskId) -> Self {
        self.metadata.dependencies.insert(dependency);
        self
    }

    /// Add multiple task dependencies
    #[inline]
    #[must_use]
    pub fn with_dependencies(mut self, dependencies: impl IntoIterator<Item = TaskId>) -> Self {
        self.metadata.dependencies.extend(dependencies);
        self
    }

    /// Check if task has any dependencies
    #[inline]
    #[must_use]
    pub fn has_dependencies(&self) -> bool {
        self.metadata.has_dependencies()
    }

    /// Get the number of dependencies
    #[inline]
    #[must_use]
    pub fn dependency_count(&self) -> usize {
        self.metadata.dependency_count()
    }

    /// Check if a specific task is a dependency
    #[inline]
    #[must_use]
    pub fn depends_on(&self, task_id: &TaskId) -> bool {
        self.metadata.depends_on(task_id)
    }

    /// Check if task has high priority (priority > 0)
    #[inline]
    #[must_use]
    pub fn is_high_priority(&self) -> bool {
        self.metadata.is_high_priority()
    }

    /// Check if task has low priority (priority < 0)
    #[inline]
    #[must_use]
    pub fn is_low_priority(&self) -> bool {
        self.metadata.is_low_priority()
    }

    // ===== Convenience State Checks (Delegated) =====

    /// Check if task is in Pending state
    #[inline]
    #[must_use]
    pub fn is_pending(&self) -> bool {
        self.metadata.is_pending()
    }

    /// Check if task is in Running state
    #[inline]
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.metadata.is_running()
    }

    /// Check if task is in Succeeded state
    #[inline]
    #[must_use]
    pub fn is_succeeded(&self) -> bool {
        self.metadata.is_succeeded()
    }

    /// Check if task is in Failed state
    #[inline]
    #[must_use]
    pub fn is_failed(&self) -> bool {
        self.metadata.is_failed()
    }

    /// Check if task is in Retrying state
    #[inline]
    #[must_use]
    pub fn is_retrying(&self) -> bool {
        self.metadata.is_retrying()
    }

    /// Check if task is in Reserved state
    #[inline]
    #[must_use]
    pub fn is_reserved(&self) -> bool {
        self.metadata.is_reserved()
    }

    // ===== Time-related Helpers (Delegated) =====

    /// Get remaining time before timeout
    #[inline]
    #[must_use]
    pub fn time_remaining(&self) -> Option<chrono::Duration> {
        self.metadata.time_remaining()
    }

    /// Get the time elapsed since task creation
    #[inline]
    #[must_use]
    pub fn time_elapsed(&self) -> chrono::Duration {
        self.metadata.time_elapsed()
    }

    // ===== Retry Helpers (Delegated) =====

    /// Check if task can be retried
    #[inline]
    #[must_use]
    pub fn can_retry(&self) -> bool {
        self.metadata.can_retry()
    }

    /// Get current retry count
    #[inline]
    #[must_use]
    pub const fn retry_count(&self) -> u32 {
        self.metadata.retry_count()
    }

    /// Get remaining retry attempts
    #[inline]
    #[must_use]
    pub const fn retries_remaining(&self) -> u32 {
        self.metadata.retries_remaining()
    }

    // ===== Workflow Helpers (Delegated) =====

    /// Check if task is part of any workflow
    #[inline]
    #[must_use]
    pub fn is_part_of_workflow(&self) -> bool {
        self.metadata.is_part_of_workflow()
    }

    /// Get group ID if task is part of a group
    #[inline]
    #[must_use]
    pub fn get_group_id(&self) -> Option<&Uuid> {
        self.metadata.get_group_id()
    }

    /// Get chord ID if task is part of a chord
    #[inline]
    #[must_use]
    pub fn get_chord_id(&self) -> Option<&Uuid> {
        self.metadata.get_chord_id()
    }
}

impl fmt::Display for SerializedTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SerializedTask[{}] name={} payload={}B state={}",
            &self.metadata.id.to_string()[..8],
            self.metadata.name,
            self.payload.len(),
            self.metadata.state
        )?;
        if self.metadata.has_priority() {
            write!(f, " priority={}", self.metadata.priority)?;
        }
        if let Some(group_id) = self.metadata.group_id {
            write!(f, " group={}", &group_id.to_string()[..8])?;
        }
        if let Some(chord_id) = self.metadata.chord_id {
            write!(f, " chord={}", &chord_id.to_string()[..8])?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_metadata_creation() {
        let metadata = TaskMetadata::new("test_task".to_string())
            .with_max_retries(5)
            .with_timeout(60)
            .with_priority(10);

        assert_eq!(metadata.name, "test_task");
        assert_eq!(metadata.max_retries, 5);
        assert_eq!(metadata.timeout_secs, Some(60));
        assert_eq!(metadata.priority, 10);
        assert_eq!(metadata.state, TaskState::Pending);
    }

    #[test]
    fn test_task_dependencies() {
        let dep1 = TaskId::new_v4();
        let dep2 = TaskId::new_v4();

        let metadata = TaskMetadata::new("test_task".to_string())
            .with_dependency(dep1)
            .with_dependency(dep2);

        assert!(metadata.has_dependencies());
        assert_eq!(metadata.dependency_count(), 2);
        assert!(metadata.depends_on(&dep1));
        assert!(metadata.depends_on(&dep2));
    }

    #[test]
    fn test_task_with_dependencies() {
        let dep1 = TaskId::new_v4();
        let dep2 = TaskId::new_v4();
        let deps = vec![dep1, dep2];

        let metadata = TaskMetadata::new("test_task".to_string()).with_dependencies(deps);

        assert_eq!(metadata.dependency_count(), 2);
        assert!(metadata.depends_on(&dep1));
        assert!(metadata.depends_on(&dep2));
    }

    #[test]
    fn test_remove_dependency() {
        let dep1 = TaskId::new_v4();
        let dep2 = TaskId::new_v4();

        let mut metadata = TaskMetadata::new("test_task".to_string())
            .with_dependency(dep1)
            .with_dependency(dep2);

        assert_eq!(metadata.dependency_count(), 2);

        let removed = metadata.remove_dependency(&dep1);
        assert!(removed);
        assert_eq!(metadata.dependency_count(), 1);
        assert!(!metadata.depends_on(&dep1));
        assert!(metadata.depends_on(&dep2));
    }

    #[test]
    fn test_clear_dependencies() {
        let dep1 = TaskId::new_v4();
        let dep2 = TaskId::new_v4();

        let mut metadata = TaskMetadata::new("test_task".to_string())
            .with_dependency(dep1)
            .with_dependency(dep2);

        assert!(metadata.has_dependencies());
        metadata.clear_dependencies();
        assert!(!metadata.has_dependencies());
        assert_eq!(metadata.dependency_count(), 0);
    }

    #[test]
    fn test_serialized_task_dependencies() {
        let dep1 = TaskId::new_v4();
        let dep2 = TaskId::new_v4();

        let task = SerializedTask::new("test_task".to_string(), vec![1, 2, 3])
            .with_dependency(dep1)
            .with_dependency(dep2);

        assert!(task.has_dependencies());
        assert_eq!(task.dependency_count(), 2);
        assert!(task.depends_on(&dep1));
        assert!(task.depends_on(&dep2));
    }

    // Integration tests for full task lifecycle
    #[cfg(test)]
    mod integration_tests {
        use super::*;
        use crate::{StateHistory, TaskState};

        #[test]
        fn test_complete_task_lifecycle() {
            // Create a task
            let mut task = SerializedTask::new("process_data".to_string(), vec![1, 2, 3, 4, 5])
                .with_priority(5)
                .with_max_retries(3)
                .with_timeout(60);

            assert_eq!(task.metadata.state, TaskState::Pending);
            assert!(task.is_active());
            assert!(!task.is_terminal());

            // Simulate task progression through states
            let mut history = StateHistory::with_initial(task.metadata.state.clone());

            // Task is received by worker
            task.metadata.state = TaskState::Received;
            history.transition(task.metadata.state.clone());

            // Task is reserved
            task.metadata.state = TaskState::Reserved;
            history.transition(task.metadata.state.clone());

            // Task starts running
            task.metadata.state = TaskState::Running;
            history.transition(task.metadata.state.clone());

            // Task completes successfully
            task.metadata.state = TaskState::Succeeded(vec![10, 20, 30]);
            history.transition(task.metadata.state.clone());

            assert!(task.is_terminal());
            assert!(!task.is_active());
            assert_eq!(history.transition_count(), 4);
        }

        #[test]
        fn test_task_retry_lifecycle() {
            let mut task =
                SerializedTask::new("failing_task".to_string(), vec![1, 2, 3]).with_max_retries(3);

            let mut history = StateHistory::with_initial(TaskState::Pending);

            // First attempt fails
            task.metadata.state = TaskState::Running;
            history.transition(task.metadata.state.clone());

            task.metadata.state = TaskState::Failed("Network error".to_string());
            history.transition(task.metadata.state.clone());

            // Check if can retry
            assert!(task.metadata.state.can_retry(task.metadata.max_retries));

            // First retry
            task.metadata.state = TaskState::Retrying(1);
            history.transition(task.metadata.state.clone());
            assert_eq!(task.metadata.state.retry_count(), 1);

            task.metadata.state = TaskState::Failed("Still failing".to_string());
            history.transition(task.metadata.state.clone());

            // Second retry
            task.metadata.state = TaskState::Retrying(2);
            history.transition(task.metadata.state.clone());
            assert_eq!(task.metadata.state.retry_count(), 2);

            // Finally succeeds
            task.metadata.state = TaskState::Succeeded(vec![]);
            history.transition(task.metadata.state.clone());

            assert!(task.is_terminal());
            assert_eq!(history.transition_count(), 6);
        }

        #[test]
        fn test_task_with_dependencies_lifecycle() {
            // Create parent tasks
            let parent1_id = TaskId::new_v4();
            let parent2_id = TaskId::new_v4();

            // Create child task that depends on parents
            let child_task = SerializedTask::new("child_task".to_string(), vec![1, 2, 3])
                .with_dependency(parent1_id)
                .with_dependency(parent2_id)
                .with_priority(10);

            assert!(child_task.has_dependencies());
            assert_eq!(child_task.dependency_count(), 2);
            assert!(child_task.depends_on(&parent1_id));
            assert!(child_task.depends_on(&parent2_id));

            // Verify task properties
            assert_eq!(child_task.metadata.priority, 10);
            assert!(child_task.is_high_priority());
        }

        #[test]
        fn test_task_serialization_roundtrip() {
            let original = SerializedTask::new("test_task".to_string(), vec![1, 2, 3, 4, 5])
                .with_priority(5)
                .with_max_retries(3)
                .with_timeout(120)
                .with_dependency(TaskId::new_v4());

            // Serialize to JSON
            let json = serde_json::to_string(&original).expect("Failed to serialize");

            // Deserialize back
            let deserialized: SerializedTask =
                serde_json::from_str(&json).expect("Failed to deserialize");

            assert_eq!(deserialized.metadata.name, original.metadata.name);
            assert_eq!(deserialized.metadata.priority, original.metadata.priority);
            assert_eq!(
                deserialized.metadata.max_retries,
                original.metadata.max_retries
            );
            assert_eq!(
                deserialized.metadata.timeout_secs,
                original.metadata.timeout_secs
            );
            assert_eq!(deserialized.payload, original.payload);
            assert_eq!(deserialized.dependency_count(), original.dependency_count());
        }

        #[test]
        fn test_task_validation_lifecycle() {
            // Valid task
            let valid_task = SerializedTask::new("valid_task".to_string(), vec![1, 2, 3])
                .with_max_retries(5)
                .with_timeout(30);

            assert!(valid_task.validate().is_ok());

            // Invalid task - empty name
            let mut invalid_task = SerializedTask::new(String::new(), vec![1, 2, 3]);
            assert!(invalid_task.metadata.validate().is_err());

            // Invalid task - excessive retries
            invalid_task =
                SerializedTask::new("task".to_string(), vec![1, 2, 3]).with_max_retries(10000);
            assert!(invalid_task.metadata.validate().is_err());

            // Invalid task - zero timeout
            let mut invalid_metadata = TaskMetadata::new("task".to_string());
            invalid_metadata.timeout_secs = Some(0);
            assert!(invalid_metadata.validate().is_err());
        }

        #[test]
        fn test_task_expiration_lifecycle() {
            // Create task with 1 second timeout
            let task =
                SerializedTask::new("expiring_task".to_string(), vec![1, 2, 3]).with_timeout(1);

            // Task should not be expired immediately
            assert!(!task.is_expired());

            // Wait for task to expire
            std::thread::sleep(std::time::Duration::from_secs(2));

            // Task should now be expired
            assert!(task.is_expired());
        }

        #[test]
        fn test_workflow_with_multiple_dependencies() {
            use crate::TaskDag;

            // Create a workflow: task1 -> task2 -> task3
            //                      \-> task4 -^
            let mut dag = TaskDag::new();

            let task1 = TaskId::new_v4();
            let task2 = TaskId::new_v4();
            let task3 = TaskId::new_v4();
            let task4 = TaskId::new_v4();

            dag.add_node(task1, "load_data");
            dag.add_node(task2, "transform_data");
            dag.add_node(task3, "save_results");
            dag.add_node(task4, "validate_data");

            // task2 depends on task1
            dag.add_dependency(task2, task1).unwrap();
            // task4 depends on task1
            dag.add_dependency(task4, task1).unwrap();
            // task3 depends on both task2 and task4
            dag.add_dependency(task3, task2).unwrap();
            dag.add_dependency(task3, task4).unwrap();

            // Validate DAG
            assert!(dag.validate().is_ok());

            // Get execution order
            let order = dag.topological_sort().unwrap();
            assert_eq!(order.len(), 4);

            // task1 should be first
            assert_eq!(order[0], task1);
            // task3 should be last
            assert_eq!(order[3], task3);
        }

        #[test]
        fn test_task_state_history_full_lifecycle() {
            let mut history = StateHistory::with_initial(TaskState::Pending);

            // Simulate complete lifecycle
            history.transition(TaskState::Received);
            history.transition(TaskState::Reserved);
            history.transition(TaskState::Running);
            history.transition(TaskState::Failed("Temporary error".to_string()));
            history.transition(TaskState::Retrying(1));
            history.transition(TaskState::Running);
            history.transition(TaskState::Succeeded(vec![1, 2, 3]));

            assert_eq!(history.transition_count(), 7);
            assert!(history.current_state().unwrap().is_terminal());

            // Check states we transitioned TO
            assert!(history.has_been_in_state("RECEIVED"));
            assert!(history.has_been_in_state("RESERVED"));
            assert!(history.has_been_in_state("RUNNING"));
            assert!(history.has_been_in_state("FAILURE"));
            assert!(history.has_been_in_state("RETRYING"));
            assert!(history.has_been_in_state("SUCCESS"));

            // Current state should be SUCCESS
            assert_eq!(history.current_state().unwrap().name(), "SUCCESS");
        }
    }
}
