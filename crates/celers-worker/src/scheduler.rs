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
        }
    }

    /// Get effective priority (including boost and inheritance)
    pub fn effective_priority(&self) -> u32 {
        let base = self.priority as u32;
        let inherited = self.inherited_priority.map(|p| p as u32).unwrap_or(base);
        base.max(inherited) + self.priority_boost
    }

    /// Get wait time
    pub fn wait_time(&self) -> Duration {
        self.queued_at.elapsed()
    }

    /// Apply starvation prevention boost
    pub fn apply_boost(&mut self, boost: u32) {
        self.priority_boost += boost;
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
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            starvation_threshold: Duration::from_secs(60),
            priority_boost_amount: 1,
            max_queue_size: 1000,
            resource_aware: true,
            prevent_starvation: true,
        }
    }
}

/// Multi-level priority queue for better priority management
#[derive(Debug)]
pub struct MultiLevelQueue {
    /// Separate queues for each priority level
    queues: [BinaryHeap<ScheduledTask>; 5],
    /// Total task count across all queues
    total_count: usize,
}

impl MultiLevelQueue {
    /// Create a new multi-level queue
    pub fn new() -> Self {
        Self {
            queues: [
                BinaryHeap::new(), // Lowest
                BinaryHeap::new(), // Low
                BinaryHeap::new(), // Normal
                BinaryHeap::new(), // High
                BinaryHeap::new(), // Highest
            ],
            total_count: 0,
        }
    }

    /// Push a task to the appropriate queue
    pub fn push(&mut self, task: ScheduledTask) {
        let priority_idx = task.priority as usize;
        self.queues[priority_idx].push(task);
        self.total_count += 1;
    }

    /// Pop the highest priority task
    pub fn pop(&mut self) -> Option<ScheduledTask> {
        // Check queues from highest to lowest priority
        for queue in self.queues.iter_mut().rev() {
            if let Some(task) = queue.pop() {
                self.total_count -= 1;
                return Some(task);
            }
        }
        None
    }

    /// Get total number of tasks
    pub fn len(&self) -> usize {
        self.total_count
    }

    /// Check if queue is empty
    pub fn is_empty(&self) -> bool {
        self.total_count == 0
    }

    /// Clear all queues
    pub fn clear(&mut self) {
        for queue in &mut self.queues {
            queue.clear();
        }
        self.total_count = 0;
    }

    /// Drain all tasks from all queues
    pub fn drain(&mut self) -> Vec<ScheduledTask> {
        let mut all_tasks = Vec::with_capacity(self.total_count);
        for queue in &mut self.queues {
            all_tasks.extend(queue.drain());
        }
        self.total_count = 0;
        all_tasks
    }

    /// Get count for specific priority level
    pub fn count_at_priority(&self, priority: TaskPriority) -> usize {
        self.queues[priority as usize].len()
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
}

impl TaskScheduler {
    /// Create a new task scheduler
    pub fn new(config: SchedulerConfig) -> Self {
        Self {
            config,
            queue: Arc::new(RwLock::new(MultiLevelQueue::new())),
            resources: Arc::new(RwLock::new(AvailableResources::default())),
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

        // Apply starvation prevention if enabled
        if self.config.prevent_starvation {
            self.apply_starvation_prevention(&mut queue).await;
        }

        // If resource-aware scheduling is disabled, just pop the highest priority task
        if !self.config.resource_aware {
            return queue.pop();
        }

        // Find the highest priority task that can be scheduled
        let resources = self.resources.read().await;
        let mut temp_tasks = Vec::new();

        while let Some(task) = queue.pop() {
            if resources.can_satisfy(&task.requirements) {
                // Put back the tasks we skipped
                for t in temp_tasks {
                    queue.push(t);
                }
                return Some(task);
            }
            temp_tasks.push(task);
        }

        // No task could be scheduled, put them all back
        for task in temp_tasks {
            queue.push(task);
        }

        None
    }

    /// Apply starvation prevention logic
    async fn apply_starvation_prevention(&self, queue: &mut MultiLevelQueue) {
        let threshold = self.config.starvation_threshold;
        let boost_amount = self.config.priority_boost_amount;

        let tasks: Vec<_> = queue.drain();
        for mut task in tasks {
            if task.wait_time() > threshold {
                task.apply_boost(boost_amount);
                info!(
                    "Applied priority boost to task {} (wait time: {:?})",
                    task.task_id,
                    task.wait_time()
                );
            }
            queue.push(task);
        }
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

        for task in tasks {
            queue.push(task);
        }

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
}
