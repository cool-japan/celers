use crate::dispatch::{self, MAX_COUNTDOWN_SECS};
use crate::{CanvasError, Signature};
use celers_core::Broker;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Chain: Sequential execution
///
/// task1(args1) -> task2(result1) -> task3(result2)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Chain {
    /// Tasks in the chain
    pub tasks: Vec<Signature>,
}

impl Chain {
    pub fn new() -> Self {
        Self { tasks: Vec::new() }
    }

    pub fn then(mut self, task: &str, args: Vec<serde_json::Value>) -> Self {
        self.tasks
            .push(Signature::new(task.to_string()).with_args(args));
        self
    }

    pub fn then_signature(mut self, signature: Signature) -> Self {
        self.tasks.push(signature);
        self
    }

    /// Apply the chain by enqueuing the first task with the rest of the chain
    /// attached to it.
    ///
    /// Only the head task is enqueued now; steps 2..N ride along inside the head
    /// task's payload (see [`crate::dispatch`] for the wire format) and the
    /// immediate successor's name is written into
    /// [`on_success_link`](celers_core::TaskMetadata::on_success_link). When the
    /// head succeeds, the worker pops the next step off that tail, applies the
    /// head's result to it and re-dispatches it with the remainder still
    /// attached — so all N steps run, in order, each with its own args, kwargs,
    /// priority and countdown intact.
    ///
    /// The head's [`countdown`](crate::TaskOptions::countdown) /
    /// [`eta`](crate::TaskOptions::eta) select the broker's scheduling enqueue
    /// variant, so a deferred chain really is deferred.
    ///
    /// Returns the id of the head task.
    pub async fn apply<B: Broker>(self, broker: &B) -> Result<Uuid, CanvasError> {
        if self.tasks.is_empty() {
            return Err(CanvasError::Invalid("Chain cannot be empty".to_string()));
        }

        let mut tasks = self.tasks;
        // `tasks` is non-empty, so the split always yields a head.
        let tail: Vec<dispatch::ChainStep> = tasks
            .split_off(1)
            .into_iter()
            .map(dispatch::ChainStep::Task)
            .collect();
        let Some(head) = tasks.pop() else {
            return Err(CanvasError::Invalid("Failed to build chain".to_string()));
        };

        dispatch::dispatch_signature(broker, &head, &tail).await
    }
}

impl Default for Chain {
    fn default() -> Self {
        Self::new()
    }
}

impl Chain {
    /// Check if chain is empty
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Get number of tasks in chain
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    /// Get the first task in the chain
    pub fn first(&self) -> Option<&Signature> {
        self.tasks.first()
    }

    /// Get the last task in the chain
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

    /// Create a chain with pre-allocated capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            tasks: Vec::with_capacity(capacity),
        }
    }

    /// Extend the chain with additional tasks
    pub fn extend(mut self, tasks: impl IntoIterator<Item = Signature>) -> Self {
        self.tasks.extend(tasks);
        self
    }

    /// Reverse the order of tasks in the chain
    pub fn reverse(mut self) -> Self {
        self.tasks.reverse();
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

    /// Apply the chain with a countdown (delay in seconds)
    ///
    /// The first task will be delayed by the countdown amount.
    /// Subsequent tasks are linked and will execute after the previous completes.
    ///
    /// # Example
    /// ```ignore
    /// let chain = Chain::new()
    ///     .then("task1", vec![])
    ///     .then("task2", vec![]);
    ///
    /// // Start chain execution after 60 seconds
    /// chain.apply_with_countdown(broker, 60).await?;
    /// ```
    pub async fn apply_with_countdown<B: Broker>(
        mut self,
        broker: &B,
        countdown: u64,
    ) -> Result<Uuid, CanvasError> {
        if self.tasks.is_empty() {
            return Err(CanvasError::Invalid("Chain cannot be empty".to_string()));
        }

        // Set countdown on the first task
        if let Some(first) = self.tasks.first_mut() {
            first.options.countdown = Some(countdown);
        }

        // Use regular apply to handle the chain
        self.apply(broker).await
    }

    /// Apply the chain with an ETA (execution time as Unix timestamp)
    ///
    /// The first task is scheduled for execution at the specified ETA via
    /// [`Broker::enqueue_at`], so the absolute instant is preserved rather than
    /// being rounded through a relative countdown. Subsequent tasks are linked
    /// and execute after the previous one completes.
    ///
    /// # Example
    /// ```ignore
    /// use std::time::{SystemTime, UNIX_EPOCH, Duration};
    ///
    /// let chain = Chain::new()
    ///     .then("task1", vec![])
    ///     .then("task2", vec![]);
    ///
    /// // Schedule chain for 1 hour from now
    /// let eta = SystemTime::now()
    ///     .duration_since(UNIX_EPOCH).expect("SystemTime should be after UNIX_EPOCH").as_secs() + 3600;
    /// chain.apply_with_eta(broker, eta).await?;
    /// ```
    pub async fn apply_with_eta<B: Broker>(
        mut self,
        broker: &B,
        eta: u64,
    ) -> Result<Uuid, CanvasError> {
        if self.tasks.is_empty() {
            return Err(CanvasError::Invalid("Chain cannot be empty".to_string()));
        }

        // Clamp to the signed range the broker scheduling API uses; timestamps
        // beyond i64::MAX seconds are not representable and are treated as
        // "as far in the future as the transport can express".
        let eta_secs = i64::try_from(eta).unwrap_or(i64::MAX);

        // Schedule the first task at the absolute ETA.
        if let Some(first) = self.tasks.first_mut() {
            first.options.eta = Some(eta_secs);
        }

        self.apply(broker).await
    }

    /// Set countdown on all tasks in the chain (staggered execution)
    ///
    /// Each task gets a progressively larger countdown. The accumulation
    /// saturates instead of overflowing, and each countdown is clamped to
    /// [`MAX_COUNTDOWN_SECS`] so a pathological `step` cannot produce a delay
    /// no broker could honour.
    ///
    /// # Arguments
    /// * `start` - Initial countdown for first task
    /// * `step` - Additional delay added for each subsequent task
    pub fn with_staggered_countdown(mut self, start: u64, step: u64) -> Self {
        let mut countdown = start.min(MAX_COUNTDOWN_SECS);
        for task in &mut self.tasks {
            task.options.countdown = Some(countdown);
            countdown = countdown.saturating_add(step).min(MAX_COUNTDOWN_SECS);
        }
        self
    }

    /// Append another chain to this chain
    ///
    /// # Example
    /// ```
    /// use celers_canvas::{Chain, Signature};
    ///
    /// let chain1 = Chain::new()
    ///     .then("task1", vec![])
    ///     .then("task2", vec![]);
    ///
    /// let chain2 = Chain::new()
    ///     .then("task3", vec![])
    ///     .then("task4", vec![]);
    ///
    /// let combined = chain1.append(chain2);
    /// assert_eq!(combined.len(), 4);
    /// ```
    pub fn append(mut self, other: Chain) -> Self {
        self.tasks.extend(other.tasks);
        self
    }

    /// Prepend another chain to this chain
    ///
    /// # Example
    /// ```
    /// use celers_canvas::{Chain, Signature};
    ///
    /// let chain1 = Chain::new()
    ///     .then("task1", vec![])
    ///     .then("task2", vec![]);
    ///
    /// let chain2 = Chain::new()
    ///     .then("task3", vec![])
    ///     .then("task4", vec![]);
    ///
    /// let combined = chain1.prepend(chain2);
    /// assert_eq!(combined.len(), 4);
    /// assert_eq!(combined.first().unwrap().task, "task3");
    /// ```
    pub fn prepend(mut self, other: Chain) -> Self {
        let mut new_tasks = other.tasks;
        new_tasks.extend(self.tasks);
        self.tasks = new_tasks;
        self
    }

    /// Split chain at the specified index
    ///
    /// Returns a tuple of (before, after) chains.
    /// The task at `index` will be the first task in the second chain.
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Chain;
    ///
    /// let chain = Chain::new()
    ///     .then("task1", vec![])
    ///     .then("task2", vec![])
    ///     .then("task3", vec![])
    ///     .then("task4", vec![]);
    ///
    /// let (before, after) = chain.split_at(2);
    /// assert_eq!(before.len(), 2);
    /// assert_eq!(after.len(), 2);
    /// ```
    pub fn split_at(self, index: usize) -> (Chain, Chain) {
        let (before, after) = self.tasks.split_at(index.min(self.tasks.len()));
        (
            Chain {
                tasks: before.to_vec(),
            },
            Chain {
                tasks: after.to_vec(),
            },
        )
    }

    /// Concatenate multiple chains into a single chain
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Chain;
    ///
    /// let chains = vec![
    ///     Chain::new().then("task1", vec![]),
    ///     Chain::new().then("task2", vec![]),
    ///     Chain::new().then("task3", vec![]),
    /// ];
    ///
    /// let combined = Chain::concat(chains);
    /// assert_eq!(combined.len(), 3);
    /// ```
    pub fn concat<I>(chains: I) -> Self
    where
        I: IntoIterator<Item = Chain>,
    {
        let mut result = Chain::new();
        for chain in chains {
            result.tasks.extend(chain.tasks);
        }
        result
    }

    /// Clone all tasks in the chain with a new task name prefix
    ///
    /// Useful for creating workflow variants.
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Chain;
    ///
    /// let chain = Chain::new()
    ///     .then("process", vec![])
    ///     .then("validate", vec![]);
    ///
    /// let prefixed = chain.with_task_prefix("batch_");
    /// assert_eq!(prefixed.first().unwrap().task, "batch_process");
    /// ```
    pub fn with_task_prefix(mut self, prefix: &str) -> Self {
        for task in &mut self.tasks {
            task.task = format!("{}{}", prefix, task.task);
        }
        self
    }

    /// Clone all tasks in the chain with a new task name suffix
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Chain;
    ///
    /// let chain = Chain::new()
    ///     .then("process", vec![])
    ///     .then("validate", vec![]);
    ///
    /// let suffixed = chain.with_task_suffix("_v2");
    /// assert_eq!(suffixed.first().unwrap().task, "process_v2");
    /// ```
    pub fn with_task_suffix(mut self, suffix: &str) -> Self {
        for task in &mut self.tasks {
            task.task = format!("{}{}", task.task, suffix);
        }
        self
    }

    /// Validate that all tasks in the chain have non-empty names
    ///
    /// Returns true if all tasks are valid, false otherwise.
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Chain;
    ///
    /// let valid = Chain::new()
    ///     .then("task1", vec![])
    ///     .then("task2", vec![]);
    /// assert!(valid.is_valid());
    ///
    /// let invalid = Chain { tasks: vec![] };
    /// assert!(!invalid.is_valid());
    /// ```
    pub fn is_valid(&self) -> bool {
        !self.tasks.is_empty() && self.tasks.iter().all(|t| !t.task.is_empty())
    }

    /// Count tasks that match a predicate
    ///
    /// # Example
    /// ```
    /// use celers_canvas::{Chain, Signature};
    ///
    /// let chain = Chain::new()
    ///     .then_signature(Signature::new("high".to_string()).with_priority(9))
    ///     .then_signature(Signature::new("low".to_string()).with_priority(1))
    ///     .then_signature(Signature::new("urgent".to_string()).with_priority(9));
    ///
    /// let high_priority = chain.count_matching(|sig| sig.options.priority.unwrap_or(0) >= 9);
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
    /// use celers_canvas::Chain;
    ///
    /// let chain = Chain::new()
    ///     .then("process", vec![])
    ///     .then("validate", vec![]);
    ///
    /// assert!(chain.any(|sig| sig.task == "validate"));
    /// assert!(!chain.any(|sig| sig.task == "missing"));
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
    /// use celers_canvas::Chain;
    ///
    /// let chain = Chain::new()
    ///     .then("process", vec![])
    ///     .then("validate", vec![]);
    ///
    /// assert!(chain.all(|sig| !sig.task.is_empty()));
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
    /// use celers_canvas::{Chain, Signature};
    ///
    /// let chain = Chain::new()
    ///     .then("task1", vec![])
    ///     .then("task2", vec![]);
    ///
    /// let modified = chain.map_tasks(|sig| {
    ///     Signature::new(format!("modified_{}", sig.task))
    /// });
    ///
    /// assert_eq!(modified.first().unwrap().task, "modified_task1");
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
    /// use celers_canvas::{Chain, Signature};
    ///
    /// let chain = Chain::new()
    ///     .then_signature(Signature::new("high".to_string()).with_priority(9))
    ///     .then_signature(Signature::new("low".to_string()).with_priority(1))
    ///     .then_signature(Signature::new("urgent".to_string()).with_priority(9));
    ///
    /// let high_priority = chain.filter_map(|sig| {
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

    /// Take the first n tasks from the chain
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Chain;
    ///
    /// let chain = Chain::new()
    ///     .then("task1", vec![])
    ///     .then("task2", vec![])
    ///     .then("task3", vec![])
    ///     .then("task4", vec![]);
    ///
    /// let first_two = chain.take(2);
    /// assert_eq!(first_two.len(), 2);
    /// ```
    pub fn take(mut self, n: usize) -> Self {
        self.tasks.truncate(n);
        self
    }

    /// Skip the first n tasks from the chain
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Chain;
    ///
    /// let chain = Chain::new()
    ///     .then("task1", vec![])
    ///     .then("task2", vec![])
    ///     .then("task3", vec![])
    ///     .then("task4", vec![]);
    ///
    /// let skipped = chain.skip(2);
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
    /// use celers_canvas::Chain;
    ///
    /// let chain = Chain::new()
    ///     .then("task1", vec![])
    ///     .then("task2", vec![])
    ///     .then("task1", vec![]);
    ///
    /// assert_eq!(chain.find_task("task1"), Some(0));
    /// assert_eq!(chain.find_task("task2"), Some(1));
    /// assert_eq!(chain.find_task("task3"), None);
    /// ```
    pub fn find_task(&self, task_name: &str) -> Option<usize> {
        self.tasks.iter().position(|t| t.task == task_name)
    }

    /// Find all indices of tasks with the given name
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Chain;
    ///
    /// let chain = Chain::new()
    ///     .then("task1", vec![])
    ///     .then("task2", vec![])
    ///     .then("task1", vec![]);
    ///
    /// assert_eq!(chain.find_all_tasks("task1"), vec![0, 2]);
    /// assert_eq!(chain.find_all_tasks("task2"), vec![1]);
    /// ```
    pub fn find_all_tasks(&self, task_name: &str) -> Vec<usize> {
        self.tasks
            .iter()
            .enumerate()
            .filter(|(_, t)| t.task == task_name)
            .map(|(i, _)| i)
            .collect()
    }

    /// Check if the chain contains a task with the given name
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Chain;
    ///
    /// let chain = Chain::new()
    ///     .then("task1", vec![])
    ///     .then("task2", vec![]);
    ///
    /// assert!(chain.contains_task("task1"));
    /// assert!(!chain.contains_task("task3"));
    /// ```
    pub fn contains_task(&self, task_name: &str) -> bool {
        self.tasks.iter().any(|t| t.task == task_name)
    }

    /// Get the total estimated duration in seconds based on task time limits
    ///
    /// This sums up all task time limits (or soft_time_limit if time_limit is not set).
    /// Returns None if no tasks have time limits set.
    ///
    /// # Example
    /// ```
    /// use celers_canvas::{Chain, Signature};
    ///
    /// let chain = Chain::new()
    ///     .then_signature(Signature::new("task1".to_string()).with_time_limit(10))
    ///     .then_signature(Signature::new("task2".to_string()).with_time_limit(20));
    ///
    /// assert_eq!(chain.estimated_duration(), Some(30));
    /// ```
    pub fn estimated_duration(&self) -> Option<u64> {
        let mut total = 0u64;
        let mut found_any = false;

        for task in &self.tasks {
            if let Some(limit) = task.options.time_limit.or(task.options.soft_time_limit) {
                total += limit;
                found_any = true;
            }
        }

        if found_any {
            Some(total)
        } else {
            None
        }
    }

    /// Get a summary of all task names in the chain
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Chain;
    ///
    /// let chain = Chain::new()
    ///     .then("fetch", vec![])
    ///     .then("process", vec![])
    ///     .then("save", vec![]);
    ///
    /// assert_eq!(chain.task_names(), vec!["fetch", "process", "save"]);
    /// ```
    pub fn task_names(&self) -> Vec<&str> {
        self.tasks.iter().map(|t| t.task.as_str()).collect()
    }

    /// Get all unique task names in the chain
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Chain;
    ///
    /// let chain = Chain::new()
    ///     .then("task1", vec![])
    ///     .then("task2", vec![])
    ///     .then("task1", vec![]);
    ///
    /// let unique = chain.unique_task_names();
    /// assert_eq!(unique.len(), 2);
    /// assert!(unique.contains(&"task1"));
    /// assert!(unique.contains(&"task2"));
    /// ```
    pub fn unique_task_names(&self) -> std::collections::HashSet<&str> {
        self.tasks.iter().map(|t| t.task.as_str()).collect()
    }

    /// Clone the chain with a transformation applied to each task
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Chain;
    ///
    /// let chain = Chain::new()
    ///     .then("task1", vec![])
    ///     .then("task2", vec![]);
    ///
    /// let prioritized = chain.clone_with_transform(|sig| {
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
        }
    }
}

impl std::fmt::Display for Chain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Chain[{} tasks]", self.tasks.len())?;
        if !self.tasks.is_empty() {
            write!(
                f,
                " {} -> ... -> {}",
                self.tasks
                    .first()
                    .expect("tasks validated to be non-empty")
                    .task,
                self.tasks
                    .last()
                    .expect("tasks validated to be non-empty")
                    .task
            )?;
        }
        Ok(())
    }
}

impl IntoIterator for Chain {
    type Item = Signature;
    type IntoIter = std::vec::IntoIter<Signature>;

    fn into_iter(self) -> Self::IntoIter {
        self.tasks.into_iter()
    }
}

impl<'a> IntoIterator for &'a Chain {
    type Item = &'a Signature;
    type IntoIter = std::slice::Iter<'a, Signature>;

    fn into_iter(self) -> Self::IntoIter {
        self.tasks.iter()
    }
}

impl From<Vec<Signature>> for Chain {
    fn from(tasks: Vec<Signature>) -> Self {
        Self { tasks }
    }
}

impl FromIterator<Signature> for Chain {
    fn from_iter<T: IntoIterator<Item = Signature>>(iter: T) -> Self {
        Self {
            tasks: iter.into_iter().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use celers_core::SerializedTask;
    use std::sync::{Arc, Mutex};

    /// One recorded dispatch: the task, its relative delay, its absolute ETA.
    type Entry = (SerializedTask, Option<u64>, Option<i64>);

    /// Broker recording each task together with how it was enqueued.
    #[derive(Clone, Default)]
    struct RecordingBroker {
        entries: Arc<Mutex<Vec<Entry>>>,
    }

    impl RecordingBroker {
        fn entries(&self) -> Vec<Entry> {
            self.entries
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }

        fn only(&self) -> Entry {
            let entries = self.entries();
            assert_eq!(entries.len(), 1, "expected exactly one enqueued task");
            entries[0].clone()
        }

        fn record(
            &self,
            task: SerializedTask,
            after: Option<u64>,
            at: Option<i64>,
        ) -> celers_core::Result<celers_core::TaskId> {
            let id = task.metadata.id;
            self.entries
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push((task, after, at));
            Ok(id)
        }
    }

    #[async_trait::async_trait]
    impl Broker for RecordingBroker {
        async fn enqueue(&self, task: SerializedTask) -> celers_core::Result<celers_core::TaskId> {
            self.record(task, None, None)
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

        async fn enqueue_after(
            &self,
            task: SerializedTask,
            delay_secs: u64,
        ) -> celers_core::Result<celers_core::TaskId> {
            self.record(task, Some(delay_secs), None)
        }

        async fn enqueue_at(
            &self,
            task: SerializedTask,
            execute_at: i64,
        ) -> celers_core::Result<celers_core::TaskId> {
            self.record(task, None, Some(execute_at))
        }
    }

    fn tail_names(task: &SerializedTask) -> Vec<String> {
        let envelope: serde_json::Value =
            serde_json::from_slice(&task.payload).expect("payload is JSON");
        envelope
            .get(crate::CHAIN_TAIL_KEY)
            .and_then(|tail| tail.as_array())
            .map(|steps| {
                steps
                    .iter()
                    .map(|step| {
                        step["task"]
                            .as_str()
                            .expect("task steps carry a name")
                            .to_string()
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The whole tail of an N-task chain must travel with the head, and the
    /// immediate successor must be linked by name. Previously the link was
    /// built and then thrown away, so tasks 2..N were silently dropped.
    #[tokio::test]
    async fn apply_carries_every_later_step_with_the_head() {
        let broker = RecordingBroker::default();

        let chain = Chain::new()
            .then("a", vec![serde_json::json!(1)])
            .then("b", vec![])
            .then("c", vec![]);

        chain.apply(&broker).await.expect("chain dispatches");

        let (task, after, at) = broker.only();
        assert_eq!(task.metadata.name, "a");
        assert_eq!(after, None);
        assert_eq!(at, None);
        assert_eq!(task.metadata.on_success_link.as_deref(), Some("b"));
        assert_eq!(tail_names(&task), vec!["b".to_string(), "c".to_string()]);
    }

    /// A later step's own args, kwargs and options must survive the trip; the
    /// `on_success_link` name alone cannot carry them.
    #[tokio::test]
    async fn later_steps_keep_their_arguments_and_options() {
        let broker = RecordingBroker::default();

        let mut kwargs = std::collections::HashMap::new();
        kwargs.insert("mode".to_string(), serde_json::json!("fast"));

        let chain = Chain::new().then("head", vec![]).then_signature(
            Signature::new("tail".to_string())
                .with_args(vec![serde_json::json!("own")])
                .with_kwargs(kwargs)
                .with_priority(7),
        );

        chain.apply(&broker).await.expect("chain dispatches");

        let (task, _, _) = broker.only();
        let envelope: serde_json::Value =
            serde_json::from_slice(&task.payload).expect("payload is JSON");
        let step = &envelope[crate::CHAIN_TAIL_KEY][0];
        assert_eq!(step["args"], serde_json::json!(["own"]));
        assert_eq!(step["kwargs"]["mode"], "fast");
        assert_eq!(step["options"]["priority"], 7);
    }

    /// An empty chain is not dispatchable.
    #[tokio::test]
    async fn empty_chain_is_rejected() {
        let broker = RecordingBroker::default();
        assert!(Chain::new().apply(&broker).await.is_err());
        assert_eq!(broker.entries().len(), 0);
    }

    /// A countdown must select the delayed-enqueue variant rather than being
    /// silently discarded.
    #[tokio::test]
    async fn apply_with_countdown_uses_the_delayed_enqueue_path() {
        let broker = RecordingBroker::default();

        Chain::new()
            .then("head", vec![])
            .then("tail", vec![])
            .apply_with_countdown(&broker, 45)
            .await
            .expect("countdown chain dispatches");

        let (task, after, at) = broker.only();
        assert_eq!(task.metadata.name, "head");
        assert_eq!(after, Some(45), "the countdown must reach the broker");
        assert_eq!(at, None);
    }

    /// An ETA must select the absolute-scheduling variant.
    #[tokio::test]
    async fn apply_with_eta_uses_the_absolute_enqueue_path() {
        let broker = RecordingBroker::default();

        Chain::new()
            .then("head", vec![])
            .apply_with_eta(&broker, 1_900_000_000)
            .await
            .expect("eta chain dispatches");

        let (_, after, at) = broker.only();
        assert_eq!(after, None);
        assert_eq!(at, Some(1_900_000_000));
    }

    /// A huge `step` must clamp rather than overflow-panic.
    #[test]
    fn staggered_countdown_saturates_instead_of_overflowing() {
        let chain = Chain::new()
            .then("a", vec![])
            .then("b", vec![])
            .then("c", vec![])
            .with_staggered_countdown(u64::MAX, u64::MAX);

        let countdowns: Vec<Option<u64>> =
            chain.tasks.iter().map(|t| t.options.countdown).collect();
        assert_eq!(
            countdowns,
            vec![
                Some(MAX_COUNTDOWN_SECS),
                Some(MAX_COUNTDOWN_SECS),
                Some(MAX_COUNTDOWN_SECS)
            ],
            "countdowns clamp to the documented ceiling"
        );
    }

    /// The ordinary staggering case still increments as documented.
    #[test]
    fn staggered_countdown_increments_normally() {
        let chain = Chain::new()
            .then("a", vec![])
            .then("b", vec![])
            .then("c", vec![])
            .with_staggered_countdown(10, 5);

        let countdowns: Vec<Option<u64>> =
            chain.tasks.iter().map(|t| t.options.countdown).collect();
        assert_eq!(countdowns, vec![Some(10), Some(15), Some(20)]);
    }
}
