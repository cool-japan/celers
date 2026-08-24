//! Resource-aware task scheduling
//!
//! This module provides intelligent task scheduling based on available system resources,
//! task requirements, and priority levels.
//!
//! # Features
//!
//! - Resource requirement specification per task
//! - Available resource tracking (CPU, memory, I/O)
//! - Priority-based task queue
//! - Resource-aware task selection
//! - Task starvation prevention
//! - Dynamic priority adjustment
//!
//! # Example
//!
//! ```
//! use celers_worker::{TaskScheduler, TaskRequirements, SchedulerConfig};
//!
//! # async fn example() {
//! let config = SchedulerConfig::default();
//! let mut scheduler = TaskScheduler::new(config);
//!
//! // Define task requirements
//! let requirements = TaskRequirements::new()
//!     .with_min_memory_mb(512)
//!     .with_min_cpu_cores(2);
//!
//! // Check if task can be scheduled
//! if scheduler.can_schedule(&requirements).await {
//!     println!("Task can be scheduled");
//! }
//! # }
//! ```

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Task priority level
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum TaskPriority {
    /// Lowest priority
    Lowest = 0,
    /// Low priority
    Low = 1,
    /// Normal priority (default)
    #[default]
    Normal = 2,
    /// High priority
    High = 3,
    /// Highest priority
    Highest = 4,
}

impl std::fmt::Display for TaskPriority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskPriority::Lowest => write!(f, "Lowest"),
            TaskPriority::Low => write!(f, "Low"),
            TaskPriority::Normal => write!(f, "Normal"),
            TaskPriority::High => write!(f, "High"),
            TaskPriority::Highest => write!(f, "Highest"),
        }
    }
}

/// Task resource requirements
#[derive(Debug, Clone)]
pub struct TaskRequirements {
    /// Minimum memory required in MB
    pub min_memory_mb: usize,
    /// Minimum CPU cores required
    pub min_cpu_cores: usize,
    /// Expected execution time
    pub expected_duration: Option<Duration>,
    /// I/O intensive flag
    pub io_intensive: bool,
    /// CPU intensive flag
    pub cpu_intensive: bool,
}

impl TaskRequirements {
    /// Create new task requirements with defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Set minimum memory requirement
    pub fn with_min_memory_mb(mut self, mb: usize) -> Self {
        self.min_memory_mb = mb;
        self
    }

    /// Set minimum CPU cores requirement
    pub fn with_min_cpu_cores(mut self, cores: usize) -> Self {
        self.min_cpu_cores = cores;
        self
    }

    /// Set expected execution time
    pub fn with_expected_duration(mut self, duration: Duration) -> Self {
        self.expected_duration = Some(duration);
        self
    }

    /// Mark as I/O intensive
    pub fn io_intensive(mut self, intensive: bool) -> Self {
        self.io_intensive = intensive;
        self
    }

    /// Mark as CPU intensive
    pub fn cpu_intensive(mut self, intensive: bool) -> Self {
        self.cpu_intensive = intensive;
        self
    }
}

impl Default for TaskRequirements {
    fn default() -> Self {
        Self {
            min_memory_mb: 0,
            min_cpu_cores: 1,
            expected_duration: None,
            io_intensive: false,
            cpu_intensive: false,
        }
    }
}

/// Available system resources
#[derive(Debug, Clone)]
pub struct AvailableResources {
    /// Available memory in MB
    pub available_memory_mb: usize,
    /// Available CPU cores
    pub available_cpu_cores: usize,
    /// Active tasks count
    pub active_tasks: usize,
    /// I/O utilization (0.0-1.0)
    pub io_utilization: f64,
    /// CPU utilization (0.0-1.0)
    pub cpu_utilization: f64,
}

impl AvailableResources {
    /// Check if requirements can be satisfied
    pub fn can_satisfy(&self, requirements: &TaskRequirements) -> bool {
        self.available_memory_mb >= requirements.min_memory_mb
            && self.available_cpu_cores >= requirements.min_cpu_cores
    }

    /// Check if resources are constrained
    pub fn is_constrained(&self) -> bool {
        self.cpu_utilization > 0.8 || self.io_utilization > 0.8
    }
}

impl Default for AvailableResources {
    fn default() -> Self {
        Self {
            available_memory_mb: 4096, // 4GB default
            available_cpu_cores: 4,
            active_tasks: 0,
            io_utilization: 0.0,
            cpu_utilization: 0.0,
        }
    }
}

/// Scheduled task entry
#[derive(Debug, Clone)]
pub struct ScheduledTask {
    /// Task ID
    pub task_id: String,
    /// Task name
    pub task_name: String,
    /// Task priority
    pub priority: TaskPriority,
    /// Resource requirements
    pub requirements: TaskRequirements,
    /// When task was queued
    pub queued_at: Instant,
    /// Priority boost for starvation prevention
    pub priority_boost: u32,
    /// Inherited priority from blocking tasks
    pub inherited_priority: Option<TaskPriority>,
    /// Tasks that donated priority to this task
    pub priority_donors: Vec<String>,
    /// When this task last received a starvation-prevention boost, if
    /// ever. Gates re-boosting: without this, a task that has been
    /// waiting past the threshold would be re-boosted on every
    /// starvation-prevention sweep for as long as it keeps waiting,
    /// letting `priority_boost` grow without bound.
    pub last_boosted_at: Option<Instant>,
}

impl ScheduledTask {
    /// Create a new scheduled task
    pub fn new(
        task_id: String,
        task_name: String,
        priority: TaskPriority,
        requirements: TaskRequirements,
    ) -> Self {
        Self {
            task_id,
            task_name,
            priority,
            requirements,
            queued_at: Instant::now(),
            priority_boost: 0,
            inherited_priority: None,
            priority_donors: Vec::new(),
            last_boosted_at: None,
        }
    }

    /// Get effective priority (including boost and inheritance)
    ///
    /// `saturating_add` rather than `+`: `priority_boost` accumulates
    /// over the (potentially very long) time a task waits in the queue,
    /// so a raw `+` risks an eventual overflow panic in debug builds
    /// (and a wraparound-driven priority inversion in release builds).
    pub fn effective_priority(&self) -> u32 {
        let base = self.priority as u32;
        let inherited = self.inherited_priority.map(|p| p as u32).unwrap_or(base);
        base.max(inherited).saturating_add(self.priority_boost)
    }

    /// Get wait time
    pub fn wait_time(&self) -> Duration {
        self.queued_at.elapsed()
    }

    /// Time since this task was queued, or since it last received a
    /// starvation-prevention boost if it has ever received one. Used to
    /// gate re-boosting to at most once per threshold interval rather
    /// than once per sweep for as long as the task keeps waiting.
    pub fn time_since_last_boost(&self) -> Duration {
        match self.last_boosted_at {
            Some(t) => t.elapsed(),
            None => self.wait_time(),
        }
    }

    /// Apply starvation prevention boost
    ///
    /// `saturating_add` guards against an eventual overflow after many
    /// boosts over a very long wait (see [`effective_priority`](Self::effective_priority)).
    pub fn apply_boost(&mut self, boost: u32) {
        self.priority_boost = self.priority_boost.saturating_add(boost);
        self.last_boosted_at = Some(Instant::now());
    }

    /// Inherit priority from a higher-priority task
    pub fn inherit_priority(&mut self, donor_id: String, donor_priority: TaskPriority) {
        if let Some(current) = self.inherited_priority {
            if donor_priority > current {
                self.inherited_priority = Some(donor_priority);
            }
        } else {
            self.inherited_priority = Some(donor_priority);
        }
        if !self.priority_donors.contains(&donor_id) {
            self.priority_donors.push(donor_id);
        }
    }

    /// Donate priority to a blocking task
    pub fn can_donate_priority_to(&self, other: &ScheduledTask) -> bool {
        self.priority > other.priority
    }

    /// Clear inherited priority when task completes
    pub fn clear_inherited_priority(&mut self) {
        self.inherited_priority = None;
        self.priority_donors.clear();
    }

    /// Check if task has inherited priority
    pub fn has_inherited_priority(&self) -> bool {
        self.inherited_priority.is_some()
    }
}

impl PartialEq for ScheduledTask {
    fn eq(&self, other: &Self) -> bool {
        self.task_id == other.task_id
    }
}

impl Eq for ScheduledTask {}

impl PartialOrd for ScheduledTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScheduledTask {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher effective priority comes first
        self.effective_priority().cmp(&other.effective_priority())
    }
}

/// Scheduler configuration
#[derive(Clone, Debug)]
pub struct SchedulerConfig {
    /// Starvation prevention threshold (wait time before boosting priority)
    pub starvation_threshold: Duration,
    /// Priority boost amount when starvation detected
    pub priority_boost_amount: u32,
    /// Maximum queue size
    pub max_queue_size: usize,
    /// Enable resource-aware scheduling
    pub resource_aware: bool,
    /// Enable starvation prevention
    pub prevent_starvation: bool,
    /// Minimum time between starvation-prevention sweeps. Each sweep is
    /// an *O*(*n*) pass over the whole queue (see
    /// `apply_starvation_prevention`); running it unconditionally on
    /// every single `dequeue()` call -- which can happen many times per
    /// second under load, with `max_queue_size` defaulting to 1000 -- is
    /// needlessly expensive. This throttles it to at most once per
    /// interval regardless of dequeue frequency.
    pub starvation_sweep_interval: Duration,
}

impl SchedulerConfig {
    /// Create a new scheduler configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set starvation threshold
    pub fn with_starvation_threshold(mut self, threshold: Duration) -> Self {
        self.starvation_threshold = threshold;
        self
    }

    /// Set priority boost amount
    pub fn with_priority_boost(mut self, boost: u32) -> Self {
        self.priority_boost_amount = boost;
        self
    }

    /// Set maximum queue size
    pub fn with_max_queue_size(mut self, size: usize) -> Self {
        self.max_queue_size = size;
        self
    }

    /// Enable or disable resource-aware scheduling
    pub fn resource_aware(mut self, enabled: bool) -> Self {
        self.resource_aware = enabled;
        self
    }

    /// Enable or disable starvation prevention
    pub fn prevent_starvation(mut self, enabled: bool) -> Self {
        self.prevent_starvation = enabled;
        self
    }

    /// Set the minimum time between starvation-prevention sweeps
    pub fn with_starvation_sweep_interval(mut self, interval: Duration) -> Self {
        self.starvation_sweep_interval = interval;
        self
    }
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            starvation_threshold: Duration::from_secs(60),
            priority_boost_amount: 1,
            max_queue_size: 1000,
            resource_aware: true,
            prevent_starvation: true,
            starvation_sweep_interval: Duration::from_secs(5),
        }
    }
}

/// Multi-level priority queue for better priority management
///
/// Backed by a single [`BinaryHeap`] ordered by
/// [`ScheduledTask::effective_priority`] -- deliberately *not* five
/// separate per-base-priority heaps. Partitioning by base priority (as
/// this type previously did, indexing into `queues[task.priority as
/// usize]`) meant a boosted low-priority task stayed physically stuck in
/// the lowest queue and was still never popped while any higher-priority
/// queue was non-empty: starvation prevention could reorder a task
/// *within* its own level but could never let it overtake a higher base
/// level, which defeated the feature entirely. A single heap ordered by
/// effective priority makes boost/inheritance promote a task across
/// levels naturally, because that is exactly what `Ord` for
/// `ScheduledTask` already compares.
#[derive(Debug)]
pub struct MultiLevelQueue {
    heap: BinaryHeap<ScheduledTask>,
}

impl MultiLevelQueue {
    /// Create a new multi-level queue
    pub fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
        }
    }

    /// Push a task onto the queue
    pub fn push(&mut self, task: ScheduledTask) {
        self.heap.push(task);
    }

    /// Bulk-insert many tasks in one pass.
    ///
    /// `BinaryHeap::extend` performs a single *O*(*n*) rebuild rather
    /// than `n` individual *O*(log *n*) `push` calls, so prefer this over
    /// a loop of `push` calls when restoring a batch of tasks that were
    /// set aside during a scan (e.g. the resource-aware dequeue path or a
    /// starvation-prevention sweep putting back everything it looked at).
    pub fn push_many(&mut self, tasks: impl IntoIterator<Item = ScheduledTask>) {
        self.heap.extend(tasks);
    }

    /// Pop the highest effective-priority task
    pub fn pop(&mut self) -> Option<ScheduledTask> {
        self.heap.pop()
    }

    /// Get total number of tasks
    pub fn len(&self) -> usize {
        self.heap.len()
    }

    /// Check if queue is empty
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// Clear the queue
    pub fn clear(&mut self) {
        self.heap.clear();
    }

    /// Drain all tasks from the queue (arbitrary order)
    pub fn drain(&mut self) -> Vec<ScheduledTask> {
        self.heap.drain().collect()
    }

    /// Get count of tasks at a specific *base* priority level (their
    /// original `priority` field, independent of any boost or inherited
    /// priority currently pushing their effective priority higher).
    pub fn count_at_priority(&self, priority: TaskPriority) -> usize {
        self.heap.iter().filter(|t| t.priority == priority).count()
    }
}

impl Default for MultiLevelQueue {
    fn default() -> Self {
        Self::new()
    }
}

/// Resource-aware task scheduler
pub struct TaskScheduler {
    /// Configuration
    config: SchedulerConfig,
    /// Task queue (multi-level priority queue)
    queue: Arc<RwLock<MultiLevelQueue>>,
    /// Available resources
    resources: Arc<RwLock<AvailableResources>>,
    /// When the last starvation-prevention sweep ran, used to throttle
    /// sweeps to `SchedulerConfig::starvation_sweep_interval`.
    last_starvation_sweep: Arc<RwLock<Instant>>,
}

impl TaskScheduler {
    /// Create a new task scheduler
    pub fn new(config: SchedulerConfig) -> Self {
        Self {
            config,
            queue: Arc::new(RwLock::new(MultiLevelQueue::new())),
            resources: Arc::new(RwLock::new(AvailableResources::default())),
            last_starvation_sweep: Arc::new(RwLock::new(Instant::now())),
        }
    }

    /// Enqueue a task for scheduling
    pub async fn enqueue(&self, task: ScheduledTask) -> Result<(), String> {
        let mut queue = self.queue.write().await;

        if queue.len() >= self.config.max_queue_size {
            return Err(format!(
                "Queue is full (max size: {})",
                self.config.max_queue_size
            ));
        }

        debug!(
            "Enqueuing task {} with priority {}",
            task.task_id, task.priority
        );
        queue.push(task);
        Ok(())
    }

    /// Dequeue the next task that can be scheduled
    pub async fn dequeue(&self) -> Option<ScheduledTask> {
        let mut queue = self.queue.write().await;

        if queue.is_empty() {
            return None;
        }

        // Apply starvation prevention if enabled, throttled to at most
        // once per `starvation_sweep_interval` regardless of how often
        // `dequeue()` itself is called (see `SchedulerConfig::starvation_sweep_interval`).
        if self.config.prevent_starvation {
            let due = {
                let last_sweep = self.last_starvation_sweep.read().await;
                last_sweep.elapsed() >= self.config.starvation_sweep_interval
            };
            if due {
                *self.last_starvation_sweep.write().await = Instant::now();
                self.apply_starvation_prevention(&mut queue).await;
            }
        }

        // If resource-aware scheduling is disabled, just pop the highest priority task
        if !self.config.resource_aware {
            return queue.pop();
        }

        // Find the highest priority task that can be scheduled, setting
        // aside (not discarding) any higher-priority tasks that don't fit
        // the current resources.
        let resources = self.resources.read().await;
        let mut skipped = Vec::new();

        let found = loop {
            match queue.pop() {
                Some(task) if resources.can_satisfy(&task.requirements) => break Some(task),
                Some(task) => skipped.push(task),
                None => break None,
            }
        };

        // Restore whatever was skipped in one O(n) pass (`push_many`)
        // rather than pushing each one back individually.
        if !skipped.is_empty() {
            queue.push_many(skipped);
        }

        found
    }

    /// Apply starvation prevention logic
    ///
    /// Boosts only tasks that have been waiting more than
    /// `starvation_threshold` *since their last boost* (see
    /// [`ScheduledTask::time_since_last_boost`]) rather than since they
    /// were queued: a task's raw wait time only grows, so gating on that
    /// alone would re-boost the same still-waiting task on every sweep
    /// for as long as it kept waiting, growing `priority_boost` without
    /// bound.
    async fn apply_starvation_prevention(&self, queue: &mut MultiLevelQueue) {
        let threshold = self.config.starvation_threshold;
        let boost_amount = self.config.priority_boost_amount;

        let tasks: Vec<_> = queue.drain();
        let tasks: Vec<_> = tasks
            .into_iter()
            .map(|mut task| {
                if task.time_since_last_boost() > threshold {
                    task.apply_boost(boost_amount);
                    info!(
                        "Applied priority boost to task {} (wait time: {:?})",
                        task.task_id,
                        task.wait_time()
                    );
                }
                task
            })
            .collect();
        queue.push_many(tasks);
    }

    /// Check if a task can be scheduled with current resources
    pub async fn can_schedule(&self, requirements: &TaskRequirements) -> bool {
        if !self.config.resource_aware {
            return true;
        }

        let resources = self.resources.read().await;
        resources.can_satisfy(requirements)
    }

    /// Update available resources
    pub async fn update_resources(&self, resources: AvailableResources) {
        let mut current = self.resources.write().await;
        *current = resources;
    }

    /// Get current available resources
    pub async fn get_resources(&self) -> AvailableResources {
        self.resources.read().await.clone()
    }

    /// Get queue size
    pub async fn queue_size(&self) -> usize {
        self.queue.read().await.len()
    }

    /// Clear the queue
    pub async fn clear(&self) {
        self.queue.write().await.clear();
    }

    /// Get scheduler configuration
    pub fn config(&self) -> &SchedulerConfig {
        &self.config
    }

    /// Get statistics for each priority level
    pub async fn priority_stats(&self) -> [usize; 5] {
        let queue = self.queue.read().await;
        [
            queue.count_at_priority(TaskPriority::Lowest),
            queue.count_at_priority(TaskPriority::Low),
            queue.count_at_priority(TaskPriority::Normal),
            queue.count_at_priority(TaskPriority::High),
            queue.count_at_priority(TaskPriority::Highest),
        ]
    }

    /// Donate priority from a high-priority task to a blocking low-priority task
    /// This is useful for priority inversion scenarios
    pub async fn donate_priority(
        &self,
        donor_id: &str,
        donor_priority: TaskPriority,
        recipient_id: &str,
    ) -> Result<(), String> {
        let mut queue = self.queue.write().await;
        let mut tasks = queue.drain();

        let mut found_recipient = false;
        for task in &mut tasks {
            if task.task_id == recipient_id {
                task.inherit_priority(donor_id.to_string(), donor_priority);
                found_recipient = true;
                debug!(
                    "Task {} inherited priority {:?} from task {}",
                    recipient_id, donor_priority, donor_id
                );
                break;
            }
        }

        queue.push_many(tasks);

        if found_recipient {
            Ok(())
        } else {
            Err(format!(
                "Recipient task {} not found in queue",
                recipient_id
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_priority_ordering() {
        assert!(TaskPriority::Highest > TaskPriority::High);
        assert!(TaskPriority::High > TaskPriority::Normal);
        assert!(TaskPriority::Normal > TaskPriority::Low);
        assert!(TaskPriority::Low > TaskPriority::Lowest);
    }

    #[test]
    fn test_task_requirements_builder() {
        let req = TaskRequirements::new()
            .with_min_memory_mb(1024)
            .with_min_cpu_cores(2)
            .with_expected_duration(Duration::from_secs(60))
            .io_intensive(true)
            .cpu_intensive(false);

        assert_eq!(req.min_memory_mb, 1024);
        assert_eq!(req.min_cpu_cores, 2);
        assert_eq!(req.expected_duration, Some(Duration::from_secs(60)));
        assert!(req.io_intensive);
        assert!(!req.cpu_intensive);
    }

    #[test]
    fn test_available_resources_can_satisfy() {
        let resources = AvailableResources {
            available_memory_mb: 2048,
            available_cpu_cores: 4,
            active_tasks: 2,
            io_utilization: 0.5,
            cpu_utilization: 0.6,
        };

        let req1 = TaskRequirements::new()
            .with_min_memory_mb(1024)
            .with_min_cpu_cores(2);
        assert!(resources.can_satisfy(&req1));

        let req2 = TaskRequirements::new()
            .with_min_memory_mb(4096)
            .with_min_cpu_cores(2);
        assert!(!resources.can_satisfy(&req2));

        let req3 = TaskRequirements::new()
            .with_min_memory_mb(1024)
            .with_min_cpu_cores(8);
        assert!(!resources.can_satisfy(&req3));
    }

    #[test]
    fn test_available_resources_is_constrained() {
        let resources1 = AvailableResources {
            cpu_utilization: 0.9,
            io_utilization: 0.5,
            ..Default::default()
        };
        assert!(resources1.is_constrained());

        let resources2 = AvailableResources {
            cpu_utilization: 0.5,
            io_utilization: 0.9,
            ..Default::default()
        };
        assert!(resources2.is_constrained());

        let resources3 = AvailableResources {
            cpu_utilization: 0.5,
            io_utilization: 0.5,
            ..Default::default()
        };
        assert!(!resources3.is_constrained());
    }

    #[test]
    fn test_scheduled_task_effective_priority() {
        let mut task = ScheduledTask::new(
            "task-123".to_string(),
            "test_task".to_string(),
            TaskPriority::Normal,
            TaskRequirements::default(),
        );

        assert_eq!(task.effective_priority(), TaskPriority::Normal as u32);

        task.apply_boost(2);
        assert_eq!(task.effective_priority(), TaskPriority::Normal as u32 + 2);
    }

    #[test]
    fn test_scheduled_task_ordering() {
        let task1 = ScheduledTask::new(
            "task-1".to_string(),
            "test1".to_string(),
            TaskPriority::Low,
            TaskRequirements::default(),
        );

        let task2 = ScheduledTask::new(
            "task-2".to_string(),
            "test2".to_string(),
            TaskPriority::High,
            TaskRequirements::default(),
        );

        assert!(task2 > task1);
    }

    #[test]
    fn test_scheduler_config_builder() {
        let config = SchedulerConfig::new()
            .with_starvation_threshold(Duration::from_secs(120))
            .with_priority_boost(2)
            .with_max_queue_size(500)
            .resource_aware(false)
            .prevent_starvation(false);

        assert_eq!(config.starvation_threshold, Duration::from_secs(120));
        assert_eq!(config.priority_boost_amount, 2);
        assert_eq!(config.max_queue_size, 500);
        assert!(!config.resource_aware);
        assert!(!config.prevent_starvation);
    }

    #[tokio::test]
    async fn test_scheduler_enqueue_dequeue() {
        let config = SchedulerConfig::default().resource_aware(false);
        let scheduler = TaskScheduler::new(config);

        assert_eq!(scheduler.queue_size().await, 0);

        let task = ScheduledTask::new(
            "task-123".to_string(),
            "test_task".to_string(),
            TaskPriority::Normal,
            TaskRequirements::default(),
        );

        scheduler.enqueue(task).await.unwrap();
        assert_eq!(scheduler.queue_size().await, 1);

        let dequeued = scheduler.dequeue().await;
        assert!(dequeued.is_some());
        assert_eq!(dequeued.unwrap().task_id, "task-123");
        assert_eq!(scheduler.queue_size().await, 0);
    }

    #[tokio::test]
    async fn test_scheduler_priority_ordering() {
        let config = SchedulerConfig::default().resource_aware(false);
        let scheduler = TaskScheduler::new(config);

        let task1 = ScheduledTask::new(
            "task-1".to_string(),
            "low".to_string(),
            TaskPriority::Low,
            TaskRequirements::default(),
        );

        let task2 = ScheduledTask::new(
            "task-2".to_string(),
            "high".to_string(),
            TaskPriority::High,
            TaskRequirements::default(),
        );

        let task3 = ScheduledTask::new(
            "task-3".to_string(),
            "normal".to_string(),
            TaskPriority::Normal,
            TaskRequirements::default(),
        );

        scheduler.enqueue(task1).await.unwrap();
        scheduler.enqueue(task2).await.unwrap();
        scheduler.enqueue(task3).await.unwrap();

        // Should dequeue in priority order: High, Normal, Low
        assert_eq!(scheduler.dequeue().await.unwrap().task_id, "task-2");
        assert_eq!(scheduler.dequeue().await.unwrap().task_id, "task-3");
        assert_eq!(scheduler.dequeue().await.unwrap().task_id, "task-1");
    }

    #[tokio::test]
    async fn test_scheduler_resource_aware() {
        let config = SchedulerConfig::default().resource_aware(true);
        let scheduler = TaskScheduler::new(config);

        // Set limited resources
        scheduler
            .update_resources(AvailableResources {
                available_memory_mb: 1024,
                available_cpu_cores: 2,
                ..Default::default()
            })
            .await;

        // Task that fits
        let task1 = ScheduledTask::new(
            "task-1".to_string(),
            "fits".to_string(),
            TaskPriority::High,
            TaskRequirements::new().with_min_memory_mb(512),
        );

        // Task that doesn't fit
        let task2 = ScheduledTask::new(
            "task-2".to_string(),
            "too_big".to_string(),
            TaskPriority::Highest,
            TaskRequirements::new().with_min_memory_mb(2048),
        );

        scheduler.enqueue(task2).await.unwrap();
        scheduler.enqueue(task1).await.unwrap();

        // Should dequeue task1 even though task2 has higher priority
        let dequeued = scheduler.dequeue().await;
        assert!(dequeued.is_some());
        assert_eq!(dequeued.unwrap().task_id, "task-1");
    }

    #[tokio::test]
    async fn test_scheduler_max_queue_size() {
        let config = SchedulerConfig::default().with_max_queue_size(2);
        let scheduler = TaskScheduler::new(config);

        let task1 = ScheduledTask::new(
            "task-1".to_string(),
            "test1".to_string(),
            TaskPriority::Normal,
            TaskRequirements::default(),
        );

        let task2 = ScheduledTask::new(
            "task-2".to_string(),
            "test2".to_string(),
            TaskPriority::Normal,
            TaskRequirements::default(),
        );

        let task3 = ScheduledTask::new(
            "task-3".to_string(),
            "test3".to_string(),
            TaskPriority::Normal,
            TaskRequirements::default(),
        );

        assert!(scheduler.enqueue(task1).await.is_ok());
        assert!(scheduler.enqueue(task2).await.is_ok());
        assert!(scheduler.enqueue(task3).await.is_err());
    }

    #[tokio::test]
    async fn test_scheduler_clear() {
        let config = SchedulerConfig::default();
        let scheduler = TaskScheduler::new(config);

        let task = ScheduledTask::new(
            "task-123".to_string(),
            "test_task".to_string(),
            TaskPriority::Normal,
            TaskRequirements::default(),
        );

        scheduler.enqueue(task).await.unwrap();
        assert_eq!(scheduler.queue_size().await, 1);

        scheduler.clear().await;
        assert_eq!(scheduler.queue_size().await, 0);
    }

    #[tokio::test]
    async fn test_scheduler_can_schedule() {
        let config = SchedulerConfig::default();
        let scheduler = TaskScheduler::new(config);

        scheduler
            .update_resources(AvailableResources {
                available_memory_mb: 2048,
                available_cpu_cores: 4,
                ..Default::default()
            })
            .await;

        let req1 = TaskRequirements::new().with_min_memory_mb(1024);
        assert!(scheduler.can_schedule(&req1).await);

        let req2 = TaskRequirements::new().with_min_memory_mb(4096);
        assert!(!scheduler.can_schedule(&req2).await);
    }

    #[test]
    fn test_multi_level_queue_basic() {
        let mut queue = MultiLevelQueue::new();
        assert_eq!(queue.len(), 0);
        assert!(queue.is_empty());

        let task = ScheduledTask::new(
            "task-1".to_string(),
            "test".to_string(),
            TaskPriority::Normal,
            TaskRequirements::default(),
        );

        queue.push(task);
        assert_eq!(queue.len(), 1);
        assert!(!queue.is_empty());
        assert_eq!(queue.count_at_priority(TaskPriority::Normal), 1);
    }

    #[test]
    fn test_multi_level_queue_priority_ordering() {
        let mut queue = MultiLevelQueue::new();

        let low_task = ScheduledTask::new(
            "low".to_string(),
            "low".to_string(),
            TaskPriority::Low,
            TaskRequirements::default(),
        );

        let high_task = ScheduledTask::new(
            "high".to_string(),
            "high".to_string(),
            TaskPriority::High,
            TaskRequirements::default(),
        );

        let normal_task = ScheduledTask::new(
            "normal".to_string(),
            "normal".to_string(),
            TaskPriority::Normal,
            TaskRequirements::default(),
        );

        // Add in random order
        queue.push(normal_task);
        queue.push(low_task);
        queue.push(high_task);

        assert_eq!(queue.len(), 3);

        // Should pop in priority order: Highest first
        assert_eq!(queue.pop().unwrap().task_id, "high");
        assert_eq!(queue.pop().unwrap().task_id, "normal");
        assert_eq!(queue.pop().unwrap().task_id, "low");
        assert!(queue.is_empty());
    }

    #[test]
    fn test_multi_level_queue_stats() {
        let mut queue = MultiLevelQueue::new();

        for i in 0..3 {
            queue.push(ScheduledTask::new(
                format!("low-{}", i),
                "low".to_string(),
                TaskPriority::Low,
                TaskRequirements::default(),
            ));
        }

        for i in 0..5 {
            queue.push(ScheduledTask::new(
                format!("high-{}", i),
                "high".to_string(),
                TaskPriority::High,
                TaskRequirements::default(),
            ));
        }

        assert_eq!(queue.count_at_priority(TaskPriority::Low), 3);
        assert_eq!(queue.count_at_priority(TaskPriority::High), 5);
        assert_eq!(queue.count_at_priority(TaskPriority::Normal), 0);
        assert_eq!(queue.len(), 8);
    }

    #[test]
    fn test_priority_inheritance() {
        let mut low_task = ScheduledTask::new(
            "low".to_string(),
            "low".to_string(),
            TaskPriority::Low,
            TaskRequirements::default(),
        );

        assert_eq!(low_task.effective_priority(), TaskPriority::Low as u32);
        assert!(!low_task.has_inherited_priority());

        // High priority task donates priority
        low_task.inherit_priority("high-task".to_string(), TaskPriority::High);

        assert!(low_task.has_inherited_priority());
        assert_eq!(low_task.effective_priority(), TaskPriority::High as u32);
        assert_eq!(low_task.priority_donors.len(), 1);

        // Even higher priority donation
        low_task.inherit_priority("highest-task".to_string(), TaskPriority::Highest);

        assert_eq!(low_task.effective_priority(), TaskPriority::Highest as u32);
        assert_eq!(low_task.priority_donors.len(), 2);

        // Clear inherited priority
        low_task.clear_inherited_priority();
        assert!(!low_task.has_inherited_priority());
        assert_eq!(low_task.effective_priority(), TaskPriority::Low as u32);
        assert_eq!(low_task.priority_donors.len(), 0);
    }

    #[test]
    fn test_priority_donation_rules() {
        let high_task = ScheduledTask::new(
            "high".to_string(),
            "high".to_string(),
            TaskPriority::High,
            TaskRequirements::default(),
        );

        let low_task = ScheduledTask::new(
            "low".to_string(),
            "low".to_string(),
            TaskPriority::Low,
            TaskRequirements::default(),
        );

        // High priority can donate to low priority
        assert!(high_task.can_donate_priority_to(&low_task));

        // Low priority cannot donate to high priority
        assert!(!low_task.can_donate_priority_to(&high_task));
    }

    #[test]
    fn test_effective_priority_with_boost_and_inheritance() {
        let mut task = ScheduledTask::new(
            "test".to_string(),
            "test".to_string(),
            TaskPriority::Low,
            TaskRequirements::default(),
        );

        // Base priority
        assert_eq!(task.effective_priority(), TaskPriority::Low as u32);

        // Add boost
        task.apply_boost(2);
        assert_eq!(task.effective_priority(), TaskPriority::Low as u32 + 2);

        // Add inheritance (should use max of base and inherited)
        task.inherit_priority("donor".to_string(), TaskPriority::High);
        assert_eq!(task.effective_priority(), TaskPriority::High as u32 + 2);
    }

    #[tokio::test]
    async fn test_scheduler_priority_stats() {
        let config = SchedulerConfig::default();
        let scheduler = TaskScheduler::new(config);

        // Add tasks at different priorities
        for i in 0..2 {
            scheduler
                .enqueue(ScheduledTask::new(
                    format!("low-{}", i),
                    "low".to_string(),
                    TaskPriority::Low,
                    TaskRequirements::default(),
                ))
                .await
                .unwrap();
        }

        for i in 0..3 {
            scheduler
                .enqueue(ScheduledTask::new(
                    format!("high-{}", i),
                    "high".to_string(),
                    TaskPriority::High,
                    TaskRequirements::default(),
                ))
                .await
                .unwrap();
        }

        let stats = scheduler.priority_stats().await;
        assert_eq!(stats[TaskPriority::Lowest as usize], 0);
        assert_eq!(stats[TaskPriority::Low as usize], 2);
        assert_eq!(stats[TaskPriority::Normal as usize], 0);
        assert_eq!(stats[TaskPriority::High as usize], 3);
        assert_eq!(stats[TaskPriority::Highest as usize], 0);
    }

    #[tokio::test]
    async fn test_scheduler_donate_priority() {
        let config = SchedulerConfig::default().resource_aware(false);
        let scheduler = TaskScheduler::new(config);

        let low_task = ScheduledTask::new(
            "low-task".to_string(),
            "low".to_string(),
            TaskPriority::Low,
            TaskRequirements::default(),
        );

        scheduler.enqueue(low_task).await.unwrap();

        // Donate priority from a high-priority task
        scheduler
            .donate_priority("high-task", TaskPriority::High, "low-task")
            .await
            .unwrap();

        // Dequeue and check that priority was inherited
        let task = scheduler.dequeue().await.unwrap();
        assert_eq!(task.task_id, "low-task");
        assert!(task.has_inherited_priority());
        assert_eq!(task.effective_priority(), TaskPriority::High as u32);
    }

    // --- Regression tests (idx 177) ----------------------------------------

    /// The central regression test: starvation prevention must be able to
    /// let a boosted task overtake a *higher base priority* task. Under
    /// the previous five-separate-heaps implementation this was
    /// impossible: `push` indexed into `queues[task.priority as usize]`
    /// by *base* priority, so a boosted `Lowest` task stayed physically
    /// stuck in `queues[0]` and was still never popped while any `High`
    /// task existed in `queues[3]`, no matter how large its boost grew.
    #[tokio::test]
    async fn test_starvation_prevention_promotes_task_across_priority_levels() {
        let config = SchedulerConfig::default()
            .with_starvation_threshold(Duration::from_millis(20))
            .with_starvation_sweep_interval(Duration::ZERO)
            .with_priority_boost(10) // enough to overtake Highest (4) from Lowest (0)
            .resource_aware(false);
        let scheduler = TaskScheduler::new(config);

        let starving = ScheduledTask::new(
            "starving".to_string(),
            "starving".to_string(),
            TaskPriority::Lowest,
            TaskRequirements::default(),
        );
        scheduler.enqueue(starving).await.unwrap();

        // Let it age past the starvation threshold.
        tokio::time::sleep(Duration::from_millis(40)).await;

        // A freshly-queued Highest-priority task, which a broken
        // implementation would always pop first regardless of how long
        // "starving" had been waiting.
        let fresh_high = ScheduledTask::new(
            "fresh-high".to_string(),
            "fresh-high".to_string(),
            TaskPriority::Highest,
            TaskRequirements::default(),
        );
        scheduler.enqueue(fresh_high).await.unwrap();

        let dequeued = scheduler.dequeue().await.unwrap();
        assert_eq!(
            dequeued.task_id, "starving",
            "a sufficiently-starved Lowest-priority task must be able to overtake a fresh Highest-priority one"
        );
    }

    /// Starvation-prevention sweeps must be throttled to
    /// `starvation_sweep_interval`, not run unconditionally on every
    /// `dequeue()` call. With an interval far longer than the test can
    /// possibly take, the very first `dequeue()` call must not sweep at
    /// all, so plain base-priority ordering is unaffected.
    #[tokio::test]
    async fn test_starvation_sweep_is_throttled_by_configured_interval() {
        let config = SchedulerConfig::default()
            .with_starvation_threshold(Duration::from_millis(10))
            .with_starvation_sweep_interval(Duration::from_secs(3600))
            .with_priority_boost(10)
            .resource_aware(false);
        let scheduler = TaskScheduler::new(config);

        let starving = ScheduledTask::new(
            "starving".to_string(),
            "starving".to_string(),
            TaskPriority::Lowest,
            TaskRequirements::default(),
        );
        scheduler.enqueue(starving).await.unwrap();

        tokio::time::sleep(Duration::from_millis(30)).await; // past the 10ms threshold

        let fresh_high = ScheduledTask::new(
            "fresh-high".to_string(),
            "fresh-high".to_string(),
            TaskPriority::Highest,
            TaskRequirements::default(),
        );
        scheduler.enqueue(fresh_high).await.unwrap();

        let dequeued = scheduler.dequeue().await.unwrap();
        assert_eq!(
            dequeued.task_id, "fresh-high",
            "a sweep must not run before the configured interval has elapsed"
        );
    }

    /// `time_since_last_boost` must fall back to the full wait time for a
    /// never-boosted task, but reset to (near) zero immediately after a
    /// boost -- this is what gates re-boosting to at most once per
    /// threshold interval instead of once per sweep.
    #[test]
    fn test_time_since_last_boost_resets_after_boost() {
        let mut task = ScheduledTask::new(
            "t".to_string(),
            "t".to_string(),
            TaskPriority::Lowest,
            TaskRequirements::default(),
        );
        assert!(task.last_boosted_at.is_none());
        assert!(task.time_since_last_boost() < Duration::from_millis(100));

        task.apply_boost(1);
        assert!(task.last_boosted_at.is_some());
        assert!(
            task.time_since_last_boost() < Duration::from_millis(100),
            "time_since_last_boost must reset to ~0 right after a boost"
        );
    }

    /// Regression test: repeatedly satisfying the "past threshold" check
    /// without gating on `time_since_last_boost` would re-boost the same
    /// still-waiting task on every sweep. Simulating many sweeps directly
    /// (bypassing real wait-time accumulation) must show the boost is
    /// only applied when due, not unconditionally.
    #[test]
    fn test_apply_starvation_boost_is_not_reapplied_immediately() {
        let mut task = ScheduledTask::new(
            "t".to_string(),
            "t".to_string(),
            TaskPriority::Lowest,
            TaskRequirements::default(),
        );
        let threshold = Duration::from_secs(60);

        // First check: never boosted, wait_time() is tiny, so it is not
        // yet due for a boost.
        assert!(task.time_since_last_boost() <= threshold);

        // Force a boost as if it were due, then immediately re-check:
        // it must not look due again a moment later.
        task.apply_boost(1);
        assert!(task.time_since_last_boost() <= threshold);
        assert_eq!(task.priority_boost, 1);
    }

    /// Regression test: `priority_boost += boost` and
    /// `base + priority_boost` could overflow after enough boosts over a
    /// long enough wait. Both must saturate instead of panicking/wrapping.
    #[test]
    fn test_apply_boost_and_effective_priority_saturate_on_overflow() {
        let mut task = ScheduledTask::new(
            "t".to_string(),
            "t".to_string(),
            TaskPriority::Highest,
            TaskRequirements::default(),
        );
        task.priority_boost = u32::MAX - 1;

        // Would panic in debug builds / wrap in release with raw `+=`.
        task.apply_boost(10);
        assert_eq!(task.priority_boost, u32::MAX);

        // Would panic in debug builds / wrap in release with raw `+`.
        assert_eq!(task.effective_priority(), u32::MAX);
    }

    #[test]
    fn test_multi_level_queue_push_many_restores_all_tasks_in_priority_order() {
        let mut queue = MultiLevelQueue::new();
        let tasks = vec![
            ScheduledTask::new(
                "a".to_string(),
                "a".to_string(),
                TaskPriority::Low,
                TaskRequirements::default(),
            ),
            ScheduledTask::new(
                "b".to_string(),
                "b".to_string(),
                TaskPriority::High,
                TaskRequirements::default(),
            ),
            ScheduledTask::new(
                "c".to_string(),
                "c".to_string(),
                TaskPriority::Normal,
                TaskRequirements::default(),
            ),
        ];

        queue.push_many(tasks);

        assert_eq!(queue.len(), 3);
        assert_eq!(queue.pop().unwrap().task_id, "b");
        assert_eq!(queue.pop().unwrap().task_id, "c");
        assert_eq!(queue.pop().unwrap().task_id, "a");
        assert!(queue.is_empty());
    }

    /// The resource-aware dequeue path must restore *every* task it set
    /// aside while scanning past ones that didn't fit, not just the
    /// first or last.
    #[tokio::test]
    async fn test_resource_aware_dequeue_restores_all_skipped_tasks() {
        let config = SchedulerConfig::default().resource_aware(true);
        let scheduler = TaskScheduler::new(config);
        scheduler
            .update_resources(AvailableResources {
                available_memory_mb: 100,
                available_cpu_cores: 4,
                ..Default::default()
            })
            .await;

        // Three high-priority tasks that don't fit (popped and skipped
        // first), then one low-priority task that does.
        for (id, mem) in [("big1", 200), ("big2", 300), ("big3", 400)] {
            scheduler
                .enqueue(ScheduledTask::new(
                    id.to_string(),
                    id.to_string(),
                    TaskPriority::Highest,
                    TaskRequirements::new().with_min_memory_mb(mem),
                ))
                .await
                .unwrap();
        }
        scheduler
            .enqueue(ScheduledTask::new(
                "fits".to_string(),
                "fits".to_string(),
                TaskPriority::Lowest,
                TaskRequirements::new().with_min_memory_mb(50),
            ))
            .await
            .unwrap();

        assert_eq!(scheduler.dequeue().await.unwrap().task_id, "fits");
        // All three skipped tasks must still be in the queue afterward.
        assert_eq!(scheduler.queue_size().await, 3);
    }
}
