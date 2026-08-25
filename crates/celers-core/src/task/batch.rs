//! Batch utility functions for working with multiple tasks.
//!
//! Split out of [`task`](super) to keep that module inside the workspace's
//! file-size budget; the public path `celers_core::task::batch` is unchanged.

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

/// Check if any *messages* have passed their `expires_at` deadline
///
/// This reads message expiry only — a long-queued task whose *execution*
/// time limit (`timeout_secs`) has elapsed is not expired, because it has
/// not started running yet.
///
/// # Example
/// ```
/// use celers_core::{SerializedTask, task::batch};
///
/// let tasks = vec![
///     // An execution time limit is NOT a message expiry.
///     SerializedTask::new("task1".to_string(), vec![1]).with_timeout(60),
///     SerializedTask::new("task2".to_string(), vec![2])
///         .with_expires_in(chrono::Duration::seconds(60)),
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

/// Get the tasks whose message has passed its `expires_at` deadline
///
/// # Example
/// ```
/// use celers_core::{SerializedTask, task::batch};
///
/// let tasks = vec![
///     SerializedTask::new("task1".to_string(), vec![1])
///         .with_expires_in(chrono::Duration::seconds(-1)),
///     SerializedTask::new("task2".to_string(), vec![2])
///         .with_expires_in(chrono::Duration::seconds(60)),
/// ];
///
/// let expired = batch::get_expired_tasks(&tasks);
/// assert_eq!(expired.len(), 1);
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
