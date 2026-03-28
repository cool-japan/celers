use crate::{CanvasError, Signature};
use celers_core::{Broker, SerializedTask};
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

    /// Apply the chain by enqueuing the first task with links to subsequent tasks
    pub async fn apply<B: Broker>(self, broker: &B) -> Result<Uuid, CanvasError> {
        if self.tasks.is_empty() {
            return Err(CanvasError::Invalid("Chain cannot be empty".to_string()));
        }

        // Build chain backwards: last task -> second-to-last -> ... -> first
        let mut chain_iter = self.tasks.into_iter().rev();
        let mut next_sig: Option<Signature> = None;

        // Start from the last task (no link)
        if let Some(last_task) = chain_iter.next() {
            // Last task has no link
            next_sig = Some(last_task);

            // Link remaining tasks backwards
            for mut task in chain_iter {
                task.options.link = next_sig.map(Box::new);
                next_sig = Some(task);
            }
        }

        // Enqueue the first task (which is now in next_sig)
        if let Some(first_sig) = next_sig {
            let task_id = Self::enqueue_signature(broker, &first_sig).await?;
            Ok(task_id)
        } else {
            Err(CanvasError::Invalid("Failed to build chain".to_string()))
        }
    }

    async fn enqueue_signature<B: Broker>(
        broker: &B,
        sig: &Signature,
    ) -> Result<Uuid, CanvasError> {
        let args_json = serde_json::json!({
            "args": sig.args,
            "kwargs": sig.kwargs
        });
        let args_bytes = serde_json::to_vec(&args_json)
            .map_err(|e| CanvasError::Serialization(e.to_string()))?;

        let mut task = SerializedTask::new(sig.task.clone(), args_bytes);

        if let Some(priority) = sig.options.priority {
            task = task.with_priority(priority.into());
        }

        let task_id = task.metadata.id;
        broker
            .enqueue(task)
            .await
            .map_err(|e| CanvasError::Broker(e.to_string()))?;

        Ok(task_id)
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
    /// The first task will be scheduled for execution at the specified ETA.
    /// Subsequent tasks are linked and will execute after the previous completes.
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
    ///     .duration_since(UNIX_EPOCH).unwrap().as_secs() + 3600;
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

        // Calculate countdown from ETA
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let countdown = eta.saturating_sub(now);

        // Set countdown on the first task
        if let Some(first) = self.tasks.first_mut() {
            first.options.countdown = Some(countdown);
        }

        self.apply(broker).await
    }

    /// Set countdown on all tasks in the chain (staggered execution)
    ///
    /// Each task gets a progressively larger countdown.
    ///
    /// # Arguments
    /// * `start` - Initial countdown for first task
    /// * `step` - Additional delay added for each subsequent task
    pub fn with_staggered_countdown(mut self, start: u64, step: u64) -> Self {
        let mut countdown = start;
        for task in &mut self.tasks {
            task.options.countdown = Some(countdown);
            countdown += step;
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
                self.tasks.first().unwrap().task,
                self.tasks.last().unwrap().task
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
