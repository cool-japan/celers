use crate::dispatch::{self, MAX_COUNTDOWN_SECS};
use crate::{CanvasError, Signature};
use celers_core::Broker;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Group: Parallel execution
///
/// (task1 | task2 | task3)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Group {
    /// Tasks in the group
    pub tasks: Vec<Signature>,

    /// Group ID
    pub group_id: Option<Uuid>,
}

impl Group {
    pub fn new() -> Self {
        Self {
            tasks: Vec::new(),
            group_id: Some(Uuid::new_v4()),
        }
    }

    pub fn add(mut self, task: &str, args: Vec<serde_json::Value>) -> Self {
        self.tasks
            .push(Signature::new(task.to_string()).with_args(args));
        self
    }

    pub fn add_signature(mut self, signature: Signature) -> Self {
        self.tasks.push(signature);
        self
    }

    /// Apply the group by enqueuing all tasks to the broker
    ///
    /// Every member is built first and the whole set is then handed to
    /// [`Broker::enqueue_batch`] in a single call, so a 1000-task group costs
    /// one round trip rather than 1000 sequential ones (Redis pipelines it,
    /// Postgres wraps it in one transaction, SQS publishes it as a batch).
    ///
    /// Members carrying a [`countdown`](crate::TaskOptions::countdown) or
    /// [`eta`](crate::TaskOptions::eta) cannot ride in the immediate batch —
    /// they are dispatched individually through the broker's scheduling
    /// variants so `skew`/`jitter`/`with_rate_limit` actually stagger the group.
    pub async fn apply<B: Broker>(self, broker: &B) -> Result<Uuid, CanvasError> {
        if self.tasks.is_empty() {
            return Err(CanvasError::Invalid("Group cannot be empty".to_string()));
        }

        let group_id = self.group_id.unwrap_or_else(Uuid::new_v4);

        let built = dispatch::build_fanout(&self.tasks, Some(group_id), None)?;
        dispatch::dispatch_all(broker, built).await?;

        Ok(group_id)
    }

    /// Apply the group as one parallel branch of an enclosing workflow,
    /// stamping every member with the caller-supplied `group_id`.
    ///
    /// Returns the ids of the enqueued member tasks in declaration order.
    pub(crate) async fn apply_within<B: Broker>(
        &self,
        broker: &B,
        group_id: Uuid,
    ) -> Result<Vec<Uuid>, CanvasError> {
        if self.tasks.is_empty() {
            return Err(CanvasError::Invalid("Group cannot be empty".to_string()));
        }

        let built = dispatch::build_fanout(&self.tasks, Some(group_id), None)?;
        dispatch::dispatch_all(broker, built).await
    }
}

impl Default for Group {
    fn default() -> Self {
        Self::new()
    }
}

impl Group {
    /// Check if group is empty
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Get the first task in the group
    pub fn first(&self) -> Option<&Signature> {
        self.tasks.first()
    }

    /// Get the last task in the group
    pub fn last(&self) -> Option<&Signature> {
        self.tasks.last()
    }

    /// Get an iterator over the tasks
    pub fn iter(&self) -> std::slice::Iter<'_, Signature> {
        self.tasks.iter()
    }

    /// Get a mutable iterator over the tasks
    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, Signature> {
        self.tasks.iter_mut()
    }

    /// Get a task by index
    pub fn get(&self, index: usize) -> Option<&Signature> {
        self.tasks.get(index)
    }

    /// Get a mutable task by index
    pub fn get_mut(&mut self, index: usize) -> Option<&mut Signature> {
        self.tasks.get_mut(index)
    }

    /// Create a group with pre-allocated capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            tasks: Vec::with_capacity(capacity),
            group_id: Some(Uuid::new_v4()),
        }
    }

    /// Extend the group with additional tasks
    pub fn extend(mut self, tasks: impl IntoIterator<Item = Signature>) -> Self {
        self.tasks.extend(tasks);
        self
    }

    /// Retain only tasks that satisfy the predicate
    pub fn retain<F>(mut self, f: F) -> Self
    where
        F: FnMut(&Signature) -> bool,
    {
        self.tasks.retain(f);
        self
    }

    /// Find a task by predicate
    pub fn find<F>(&self, predicate: F) -> Option<&Signature>
    where
        F: Fn(&Signature) -> bool,
    {
        self.tasks.iter().find(|sig| predicate(sig))
    }

    /// Filter tasks by predicate and return a new group
    pub fn filter<F>(mut self, predicate: F) -> Self
    where
        F: FnMut(&Signature) -> bool,
    {
        self.tasks.retain(predicate);
        self
    }

    /// Stagger task execution with countdown delays
    ///
    /// Each task gets a countdown that increases by the specified interval.
    /// This helps prevent thundering herd problems when launching many tasks.
    ///
    /// # Arguments
    /// * `start` - Initial countdown in seconds for the first task
    /// * `step` - Increment in seconds for each subsequent task
    ///
    /// # Example
    /// ```
    /// use celers_canvas::{Group, Signature};
    ///
    /// let group = Group::new()
    ///     .add("task1", vec![])
    ///     .add("task2", vec![])
    ///     .add("task3", vec![])
    ///     .skew(0.0, 1.0); // task1: 0s, task2: 1s, task3: 2s
    ///
    /// assert_eq!(group.tasks[0].options.countdown, Some(0));
    /// assert_eq!(group.tasks[1].options.countdown, Some(1));
    /// assert_eq!(group.tasks[2].options.countdown, Some(2));
    /// ```
    ///
    /// Countdowns are clamped to [`MAX_COUNTDOWN_SECS`]; negative or
    /// non-finite inputs collapse to `0` rather than producing a nonsensical
    /// delay.
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    pub fn skew(mut self, start: f64, step: f64) -> Self {
        let mut countdown = start;
        for task in &mut self.tasks {
            // `as u64` saturates at the bounds and maps NaN to 0 in Rust, but
            // clamp explicitly so the intent is on the page.
            let secs = if countdown.is_finite() && countdown > 0.0 {
                (countdown as u64).min(MAX_COUNTDOWN_SECS)
            } else {
                0
            };
            task.options.countdown = Some(secs);
            countdown += step;
        }
        self
    }

    /// Stagger task execution with random jitter
    ///
    /// Each task gets a random countdown between 0 and max_delay.
    /// This provides more even load distribution than linear skew.
    ///
    /// # Arguments
    /// * `max_delay` - Maximum countdown in seconds, clamped to
    ///   [`MAX_COUNTDOWN_SECS`]
    ///
    /// The modulus is computed with saturating arithmetic: a `max_delay` of
    /// `u64::MAX` used to overflow `max_delay + 1` (a debug panic) and wrap to a
    /// modulus of zero (a division-by-zero panic in release).
    pub fn jitter(mut self, max_delay: u64) -> Self {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        // `+ 1` makes the range inclusive of `max_delay`; saturating add and a
        // `max(1)` floor keep the modulus non-zero for every input.
        let modulus = max_delay.min(MAX_COUNTDOWN_SECS).saturating_add(1).max(1);

        for (i, task) in self.tasks.iter_mut().enumerate() {
            // Use a deterministic "random" based on task index and name
            let mut hasher = DefaultHasher::new();
            i.hash(&mut hasher);
            task.task.hash(&mut hasher);
            let hash = hasher.finish();
            let delay = hash % modulus;
            task.options.countdown = Some(delay);
        }
        self
    }

    /// Get number of tasks in group
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    /// Check if group ID is set
    pub fn has_group_id(&self) -> bool {
        self.group_id.is_some()
    }

    /// Merge another group into this group
    ///
    /// All tasks from the other group are added to this group.
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Group;
    ///
    /// let group1 = Group::new()
    ///     .add("task1", vec![])
    ///     .add("task2", vec![]);
    ///
    /// let group2 = Group::new()
    ///     .add("task3", vec![])
    ///     .add("task4", vec![]);
    ///
    /// let merged = group1.merge(group2);
    /// assert_eq!(merged.len(), 4);
    /// ```
    pub fn merge(mut self, other: Group) -> Self {
        self.tasks.extend(other.tasks);
        self
    }

    /// Partition tasks into multiple groups based on a predicate
    ///
    /// Returns a tuple of (matching, non_matching) groups.
    ///
    /// # Example
    /// ```
    /// use celers_canvas::{Group, Signature};
    ///
    /// let group = Group::new()
    ///     .add_signature(Signature::new("high_priority".to_string()).with_priority(9))
    ///     .add_signature(Signature::new("normal".to_string()).with_priority(5))
    ///     .add_signature(Signature::new("urgent".to_string()).with_priority(9))
    ///     .add_signature(Signature::new("low".to_string()).with_priority(1));
    ///
    /// let (high, low) = group.partition(|sig| sig.options.priority.unwrap_or(0) >= 9);
    /// assert_eq!(high.len(), 2);
    /// assert_eq!(low.len(), 2);
    /// ```
    pub fn partition<F>(self, mut predicate: F) -> (Group, Group)
    where
        F: FnMut(&Signature) -> bool,
    {
        let (matching, non_matching): (Vec<_>, Vec<_>) =
            self.tasks.into_iter().partition(|sig| predicate(sig));

        (
            Group {
                tasks: matching,
                group_id: self.group_id,
            },
            Group {
                tasks: non_matching,
                group_id: None, // Different group
            },
        )
    }

    /// Add a task name prefix to all tasks in the group
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Group;
    ///
    /// let group = Group::new()
    ///     .add("process", vec![])
    ///     .add("validate", vec![]);
    ///
    /// let prefixed = group.with_task_prefix("batch_");
    /// assert_eq!(prefixed.first().unwrap().task, "batch_process");
    /// ```
    pub fn with_task_prefix(mut self, prefix: &str) -> Self {
        for task in &mut self.tasks {
            task.task = format!("{}{}", prefix, task.task);
        }
        self
    }

    /// Add a task name suffix to all tasks in the group
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Group;
    ///
    /// let group = Group::new()
    ///     .add("process", vec![])
    ///     .add("validate", vec![]);
    ///
    /// let suffixed = group.with_task_suffix("_v2");
    /// assert_eq!(suffixed.first().unwrap().task, "process_v2");
    /// ```
    pub fn with_task_suffix(mut self, suffix: &str) -> Self {
        for task in &mut self.tasks {
            task.task = format!("{}{}", task.task, suffix);
        }
        self
    }

    /// Set priority on all tasks in the group
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Group;
    ///
    /// let group = Group::new()
    ///     .add("task1", vec![])
    ///     .add("task2", vec![])
    ///     .with_priority(9);
    ///
    /// assert_eq!(group.first().unwrap().options.priority, Some(9));
    /// ```
    pub fn with_priority(mut self, priority: u8) -> Self {
        for task in &mut self.tasks {
            task.options.priority = Some(priority);
        }
        self
    }

    /// Set queue on all tasks in the group
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Group;
    ///
    /// let group = Group::new()
    ///     .add("task1", vec![])
    ///     .add("task2", vec![])
    ///     .with_queue("high_priority".to_string());
    ///
    /// assert_eq!(group.first().unwrap().options.queue, Some("high_priority".to_string()));
    /// ```
    pub fn with_queue(mut self, queue: String) -> Self {
        for task in &mut self.tasks {
            task.options.queue = Some(queue.clone());
        }
        self
    }

    /// Validate that all tasks in the group have non-empty names
    ///
    /// Returns true if all tasks are valid, false otherwise.
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Group;
    ///
    /// let valid = Group::new()
    ///     .add("task1", vec![])
    ///     .add("task2", vec![]);
    /// assert!(valid.is_valid());
    ///
    /// let invalid = Group { tasks: vec![], group_id: None };
    /// assert!(!invalid.is_valid());
    /// ```
    pub fn is_valid(&self) -> bool {
        !self.tasks.is_empty() && self.tasks.iter().all(|t| !t.task.is_empty())
    }

    /// Count tasks that match a predicate
    ///
    /// # Example
    /// ```
    /// use celers_canvas::{Group, Signature};
    ///
    /// let group = Group::new()
    ///     .add_signature(Signature::new("high".to_string()).with_priority(9))
    ///     .add_signature(Signature::new("low".to_string()).with_priority(1))
    ///     .add_signature(Signature::new("urgent".to_string()).with_priority(9));
    ///
    /// let high_priority = group.count_matching(|sig| sig.options.priority.unwrap_or(0) >= 9);
    /// assert_eq!(high_priority, 2);
    /// ```
    pub fn count_matching<F>(&self, predicate: F) -> usize
    where
        F: Fn(&Signature) -> bool,
    {
        self.tasks.iter().filter(|t| predicate(t)).count()
    }

    /// Check if any task matches a predicate
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Group;
    ///
    /// let group = Group::new()
    ///     .add("process", vec![])
    ///     .add("validate", vec![]);
    ///
    /// assert!(group.any(|sig| sig.task == "validate"));
    /// assert!(!group.any(|sig| sig.task == "missing"));
    /// ```
    pub fn any<F>(&self, predicate: F) -> bool
    where
        F: Fn(&Signature) -> bool,
    {
        self.tasks.iter().any(predicate)
    }

    /// Check if all tasks match a predicate
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Group;
    ///
    /// let group = Group::new()
    ///     .add("process", vec![])
    ///     .add("validate", vec![]);
    ///
    /// assert!(group.all(|sig| !sig.task.is_empty()));
    /// ```
    pub fn all<F>(&self, predicate: F) -> bool
    where
        F: Fn(&Signature) -> bool,
    {
        self.tasks.iter().all(predicate)
    }

    /// Map over all tasks, transforming each signature
    ///
    /// # Example
    /// ```
    /// use celers_canvas::{Group, Signature};
    ///
    /// let group = Group::new()
    ///     .add("task1", vec![])
    ///     .add("task2", vec![]);
    ///
    /// let modified = group.map_tasks(|sig| {
    ///     Signature::new(format!("parallel_{}", sig.task))
    /// });
    ///
    /// assert_eq!(modified.first().unwrap().task, "parallel_task1");
    /// ```
    pub fn map_tasks<F>(mut self, f: F) -> Self
    where
        F: FnMut(Signature) -> Signature,
    {
        self.tasks = self.tasks.into_iter().map(f).collect();
        self
    }

    /// Filter and map tasks in one operation
    ///
    /// # Example
    /// ```
    /// use celers_canvas::{Group, Signature};
    ///
    /// let group = Group::new()
    ///     .add_signature(Signature::new("high".to_string()).with_priority(9))
    ///     .add_signature(Signature::new("low".to_string()).with_priority(1))
    ///     .add_signature(Signature::new("urgent".to_string()).with_priority(9));
    ///
    /// let high_priority = group.filter_map(|sig| {
    ///     if sig.options.priority.unwrap_or(0) >= 9 {
    ///         Some(sig)
    ///     } else {
    ///         None
    ///     }
    /// });
    ///
    /// assert_eq!(high_priority.len(), 2);
    /// ```
    pub fn filter_map<F>(mut self, f: F) -> Self
    where
        F: FnMut(Signature) -> Option<Signature>,
    {
        self.tasks = self.tasks.into_iter().filter_map(f).collect();
        self
    }

    /// Take the first n tasks from the group
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Group;
    ///
    /// let group = Group::new()
    ///     .add("task1", vec![])
    ///     .add("task2", vec![])
    ///     .add("task3", vec![])
    ///     .add("task4", vec![]);
    ///
    /// let first_two = group.take(2);
    /// assert_eq!(first_two.len(), 2);
    /// ```
    pub fn take(mut self, n: usize) -> Self {
        self.tasks.truncate(n);
        self
    }

    /// Skip the first n tasks from the group
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Group;
    ///
    /// let group = Group::new()
    ///     .add("task1", vec![])
    ///     .add("task2", vec![])
    ///     .add("task3", vec![])
    ///     .add("task4", vec![]);
    ///
    /// let skipped = group.skip(2);
    /// assert_eq!(skipped.len(), 2);
    /// assert_eq!(skipped.first().unwrap().task, "task3");
    /// ```
    pub fn skip(mut self, n: usize) -> Self {
        self.tasks = self.tasks.into_iter().skip(n).collect();
        self
    }

    /// Find the index of the first task with the given name
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Group;
    ///
    /// let group = Group::new()
    ///     .add("task1", vec![])
    ///     .add("task2", vec![])
    ///     .add("task1", vec![]);
    ///
    /// assert_eq!(group.find_task("task1"), Some(0));
    /// assert_eq!(group.find_task("task2"), Some(1));
    /// assert_eq!(group.find_task("task3"), None);
    /// ```
    pub fn find_task(&self, task_name: &str) -> Option<usize> {
        self.tasks.iter().position(|t| t.task == task_name)
    }

    /// Find all indices of tasks with the given name
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Group;
    ///
    /// let group = Group::new()
    ///     .add("task1", vec![])
    ///     .add("task2", vec![])
    ///     .add("task1", vec![]);
    ///
    /// assert_eq!(group.find_all_tasks("task1"), vec![0, 2]);
    /// assert_eq!(group.find_all_tasks("task2"), vec![1]);
    /// ```
    pub fn find_all_tasks(&self, task_name: &str) -> Vec<usize> {
        self.tasks
            .iter()
            .enumerate()
            .filter(|(_, t)| t.task == task_name)
            .map(|(i, _)| i)
            .collect()
    }

    /// Check if the group contains a task with the given name
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Group;
    ///
    /// let group = Group::new()
    ///     .add("task1", vec![])
    ///     .add("task2", vec![]);
    ///
    /// assert!(group.contains_task("task1"));
    /// assert!(!group.contains_task("task3"));
    /// ```
    pub fn contains_task(&self, task_name: &str) -> bool {
        self.tasks.iter().any(|t| t.task == task_name)
    }

    /// Get the maximum estimated duration in seconds based on task time limits
    ///
    /// Since tasks run in parallel, the group duration is the maximum of all task durations.
    /// Returns None if no tasks have time limits set.
    ///
    /// # Example
    /// ```
    /// use celers_canvas::{Group, Signature};
    ///
    /// let group = Group::new()
    ///     .add_signature(Signature::new("task1".to_string()).with_time_limit(10))
    ///     .add_signature(Signature::new("task2".to_string()).with_time_limit(20));
    ///
    /// assert_eq!(group.estimated_duration(), Some(20)); // Max of 10 and 20
    /// ```
    pub fn estimated_duration(&self) -> Option<u64> {
        self.tasks
            .iter()
            .filter_map(|t| t.options.time_limit.or(t.options.soft_time_limit))
            .max()
    }

    /// Get a summary of all task names in the group
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Group;
    ///
    /// let group = Group::new()
    ///     .add("fetch", vec![])
    ///     .add("process", vec![])
    ///     .add("save", vec![]);
    ///
    /// assert_eq!(group.task_names(), vec!["fetch", "process", "save"]);
    /// ```
    pub fn task_names(&self) -> Vec<&str> {
        self.tasks.iter().map(|t| t.task.as_str()).collect()
    }

    /// Get all unique task names in the group
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Group;
    ///
    /// let group = Group::new()
    ///     .add("task1", vec![])
    ///     .add("task2", vec![])
    ///     .add("task1", vec![]);
    ///
    /// let unique = group.unique_task_names();
    /// assert_eq!(unique.len(), 2);
    /// assert!(unique.contains(&"task1"));
    /// assert!(unique.contains(&"task2"));
    /// ```
    pub fn unique_task_names(&self) -> std::collections::HashSet<&str> {
        self.tasks.iter().map(|t| t.task.as_str()).collect()
    }

    /// Clone the group with a transformation applied to each task
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Group;
    ///
    /// let group = Group::new()
    ///     .add("task1", vec![])
    ///     .add("task2", vec![]);
    ///
    /// let prioritized = group.clone_with_transform(|sig| {
    ///     sig.clone().with_priority(5)
    /// });
    ///
    /// assert!(prioritized.tasks.iter().all(|t| t.options.priority == Some(5)));
    /// ```
    pub fn clone_with_transform<F>(&self, mut transform: F) -> Self
    where
        F: FnMut(&Signature) -> Signature,
    {
        Self {
            tasks: self.tasks.iter().map(&mut transform).collect(),
            group_id: self.group_id,
        }
    }

    /// Count tasks by priority level
    ///
    /// Returns a map of priority values to the count of tasks at that priority.
    ///
    /// # Example
    /// ```
    /// use celers_canvas::{Group, Signature};
    ///
    /// let group = Group::new()
    ///     .add_signature(Signature::new("task1".to_string()).with_priority(1))
    ///     .add_signature(Signature::new("task2".to_string()).with_priority(5))
    ///     .add_signature(Signature::new("task3".to_string()).with_priority(1));
    ///
    /// let counts = group.count_by_priority();
    /// assert_eq!(counts.get(&1), Some(&2));
    /// assert_eq!(counts.get(&5), Some(&1));
    /// ```
    pub fn count_by_priority(&self) -> std::collections::HashMap<u8, usize> {
        let mut counts = std::collections::HashMap::new();
        for task in &self.tasks {
            if let Some(priority) = task.options.priority {
                *counts.entry(priority).or_insert(0) += 1;
            }
        }
        counts
    }

    /// Count tasks by queue name
    ///
    /// Returns a map of queue names to the count of tasks targeting that queue.
    ///
    /// # Example
    /// ```
    /// use celers_canvas::{Group, Signature};
    ///
    /// let group = Group::new()
    ///     .add_signature(Signature::new("task1".to_string()).with_queue("queue_a".to_string()))
    ///     .add_signature(Signature::new("task2".to_string()).with_queue("queue_b".to_string()))
    ///     .add_signature(Signature::new("task3".to_string()).with_queue("queue_a".to_string()));
    ///
    /// let counts = group.count_by_queue();
    /// assert_eq!(counts.get("queue_a"), Some(&2));
    /// assert_eq!(counts.get("queue_b"), Some(&1));
    /// ```
    pub fn count_by_queue(&self) -> std::collections::HashMap<String, usize> {
        let mut counts = std::collections::HashMap::new();
        for task in &self.tasks {
            if let Some(ref queue) = task.options.queue {
                *counts.entry(queue.clone()).or_insert(0) += 1;
            }
        }
        counts
    }
}

impl std::fmt::Display for Group {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Group[{} tasks]", self.tasks.len())?;
        if let Some(group_id) = self.group_id {
            write!(f, " id={}", &group_id.to_string()[..8])?;
        }
        Ok(())
    }
}

impl IntoIterator for Group {
    type Item = Signature;
    type IntoIter = std::vec::IntoIter<Signature>;

    fn into_iter(self) -> Self::IntoIter {
        self.tasks.into_iter()
    }
}

impl<'a> IntoIterator for &'a Group {
    type Item = &'a Signature;
    type IntoIter = std::slice::Iter<'a, Signature>;

    fn into_iter(self) -> Self::IntoIter {
        self.tasks.iter()
    }
}

impl From<Vec<Signature>> for Group {
    fn from(tasks: Vec<Signature>) -> Self {
        Self {
            tasks,
            group_id: Some(Uuid::new_v4()),
        }
    }
}

impl FromIterator<Signature> for Group {
    fn from_iter<T: IntoIterator<Item = Signature>>(iter: T) -> Self {
        Self {
            tasks: iter.into_iter().collect(),
            group_id: Some(Uuid::new_v4()),
        }
    }
}

#[cfg(test)]
mod dispatch_tests {
    use super::*;
    use celers_core::SerializedTask;
    use std::sync::{Arc, Mutex};

    /// How a task reached the broker.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Arrival {
        /// Part of a single `enqueue_batch` call, identified by batch number.
        Batch(usize),
        /// Delivered on its own with a relative delay.
        After(u64),
    }

    /// Broker distinguishing batched enqueues from scheduled ones.
    #[derive(Clone, Default)]
    struct BatchAwareBroker {
        entries: Arc<Mutex<Vec<(SerializedTask, Arrival)>>>,
        batches: Arc<Mutex<usize>>,
    }

    impl BatchAwareBroker {
        fn entries(&self) -> Vec<(SerializedTask, Arrival)> {
            self.entries
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }

        fn batch_count(&self) -> usize {
            *self
                .batches
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
        }

        fn arrivals(&self) -> Vec<(String, Arrival)> {
            self.entries()
                .into_iter()
                .map(|(task, arrival)| (task.metadata.name, arrival))
                .collect()
        }

        fn push(&self, task: SerializedTask, arrival: Arrival) -> celers_core::TaskId {
            let id = task.metadata.id;
            self.entries
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push((task, arrival));
            id
        }
    }

    #[async_trait::async_trait]
    impl Broker for BatchAwareBroker {
        async fn enqueue(&self, task: SerializedTask) -> celers_core::Result<celers_core::TaskId> {
            // A lone `enqueue` counts as its own single-item batch.
            let batch = {
                let mut batches = self
                    .batches
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                *batches += 1;
                *batches
            };
            Ok(self.push(task, Arrival::Batch(batch)))
        }

        async fn enqueue_batch(
            &self,
            tasks: Vec<SerializedTask>,
        ) -> celers_core::Result<Vec<celers_core::TaskId>> {
            let batch = {
                let mut batches = self
                    .batches
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                *batches += 1;
                *batches
            };
            Ok(tasks
                .into_iter()
                .map(|task| self.push(task, Arrival::Batch(batch)))
                .collect())
        }

        async fn enqueue_after(
            &self,
            task: SerializedTask,
            delay_secs: u64,
        ) -> celers_core::Result<celers_core::TaskId> {
            Ok(self.push(task, Arrival::After(delay_secs)))
        }

        async fn dequeue(&self) -> celers_core::Result<Option<celers_core::BrokerMessage>> {
            Ok(None)
        }

        async fn ack(
            &self,
            _task_id: &celers_core::TaskId,
            _receipt_handle: Option<&str>,
        ) -> celers_core::Result<()> {
            Ok(())
        }

        async fn reject(
            &self,
            _task_id: &celers_core::TaskId,
            _receipt_handle: Option<&str>,
            _requeue: bool,
        ) -> celers_core::Result<()> {
            Ok(())
        }

        async fn queue_size(&self) -> celers_core::Result<usize> {
            Ok(self.entries().len())
        }

        async fn cancel(&self, _task_id: &celers_core::TaskId) -> celers_core::Result<bool> {
            Ok(false)
        }
    }

    /// A group of immediate members must cost exactly one broker round trip,
    /// not one per member.
    #[tokio::test]
    async fn apply_uses_a_single_batch_for_immediate_members() {
        let broker = BatchAwareBroker::default();

        let group = Group::new()
            .add("m1", vec![])
            .add("m2", vec![])
            .add("m3", vec![]);

        let group_id = group.apply(&broker).await.expect("group dispatches");

        assert_eq!(
            broker.batch_count(),
            1,
            "the whole group must go out in one batched call"
        );
        assert_eq!(
            broker.arrivals(),
            vec![
                ("m1".to_string(), Arrival::Batch(1)),
                ("m2".to_string(), Arrival::Batch(1)),
                ("m3".to_string(), Arrival::Batch(1)),
            ]
        );
        for (task, _) in broker.entries() {
            assert_eq!(task.metadata.group_id, Some(group_id));
        }
    }

    /// Staggered members must actually be staggered: their countdowns select
    /// the delayed-enqueue path instead of being dropped.
    #[tokio::test]
    async fn skewed_members_are_dispatched_through_the_scheduling_path() {
        let broker = BatchAwareBroker::default();

        Group::new()
            .add("first", vec![])
            .add("second", vec![])
            .add("third", vec![])
            .skew(0.0, 10.0)
            .apply(&broker)
            .await
            .expect("group dispatches");

        assert_eq!(
            broker.arrivals(),
            vec![
                ("first".to_string(), Arrival::Batch(1)),
                ("second".to_string(), Arrival::After(10)),
                ("third".to_string(), Arrival::After(20)),
            ],
            "only the zero-countdown member rides the immediate batch"
        );
    }

    /// Rate-limited groups get the same treatment.
    #[tokio::test]
    async fn rate_limited_members_are_staggered_at_dispatch() {
        let broker = BatchAwareBroker::default();
        let config = celers_core::RateLimitConfig::new(1.0).with_burst(1);

        Group::new()
            .add("a", vec![])
            .add("b", vec![])
            .add("c", vec![])
            .with_rate_limit(&config)
            .apply(&broker)
            .await
            .expect("group dispatches");

        assert_eq!(
            broker.arrivals(),
            vec![
                ("a".to_string(), Arrival::Batch(1)),
                ("b".to_string(), Arrival::After(1)),
                ("c".to_string(), Arrival::After(2)),
            ],
            "the rate-derived countdowns must reach the broker"
        );
    }

    /// `jitter(u64::MAX)` used to overflow `max_delay + 1` (a debug panic) and
    /// wrap to a zero modulus (a release division-by-zero panic).
    #[test]
    fn jitter_does_not_overflow_at_the_extreme() {
        let group = Group::new()
            .add("a", vec![])
            .add("b", vec![])
            .jitter(u64::MAX);

        for task in &group.tasks {
            let countdown = task.options.countdown.expect("jitter sets a countdown");
            assert!(
                countdown <= MAX_COUNTDOWN_SECS,
                "jitter must clamp to the documented ceiling, got {countdown}"
            );
        }
    }

    /// Jitter must stay deterministic and inside the requested bound.
    #[test]
    fn jitter_is_deterministic_and_bounded() {
        let build = || {
            Group::new()
                .add("alpha", vec![])
                .add("beta", vec![])
                .jitter(30)
        };

        let first: Vec<Option<u64>> = build().tasks.iter().map(|t| t.options.countdown).collect();
        let second: Vec<Option<u64>> = build().tasks.iter().map(|t| t.options.countdown).collect();

        assert_eq!(first, second, "jitter must be reproducible");
        for countdown in first.into_iter().flatten() {
            assert!(countdown <= 30);
        }
    }

    /// `skew` must not produce nonsense for negative or non-finite inputs.
    #[test]
    fn skew_clamps_negative_and_non_finite_inputs() {
        let group = Group::new()
            .add("a", vec![])
            .add("b", vec![])
            .skew(-100.0, 50.0);

        assert_eq!(group.tasks[0].options.countdown, Some(0));
        assert_eq!(group.tasks[1].options.countdown, Some(0));

        let nan_group = Group::new().add("a", vec![]).skew(f64::NAN, 1.0);
        assert_eq!(nan_group.tasks[0].options.countdown, Some(0));
    }

    /// An empty group is not dispatchable.
    #[tokio::test]
    async fn empty_group_is_rejected() {
        let broker = BatchAwareBroker::default();
        assert!(Group::new().apply(&broker).await.is_err());
        assert_eq!(broker.entries().len(), 0);
    }
}
