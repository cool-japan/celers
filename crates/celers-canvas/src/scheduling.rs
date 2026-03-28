use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Task priority for scheduling
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Serialize, Deserialize)]
pub enum TaskPriority {
    /// Low priority (value: 0)
    Low = 0,
    /// Normal priority (value: 5)
    #[default]
    Normal = 5,
    /// High priority (value: 10)
    High = 10,
    /// Critical priority (value: 15)
    Critical = 15,
}

impl std::fmt::Display for TaskPriority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Low => write!(f, "Low"),
            Self::Normal => write!(f, "Normal"),
            Self::High => write!(f, "High"),
            Self::Critical => write!(f, "Critical"),
        }
    }
}

/// Worker resource capacity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerCapacity {
    /// Worker ID
    pub worker_id: String,
    /// CPU cores available
    pub cpu_cores: u32,
    /// Memory available (MB)
    pub memory_mb: u64,
    /// Current load (0.0 to 1.0)
    pub current_load: f64,
    /// Active tasks count
    pub active_tasks: usize,
}

impl WorkerCapacity {
    /// Create a new worker capacity
    pub fn new(worker_id: impl Into<String>, cpu_cores: u32, memory_mb: u64) -> Self {
        Self {
            worker_id: worker_id.into(),
            cpu_cores,
            memory_mb,
            current_load: 0.0,
            active_tasks: 0,
        }
    }

    /// Check if worker has capacity for a task
    pub fn has_capacity(&self, required_load: f64) -> bool {
        self.current_load + required_load <= 1.0
    }

    /// Get available capacity
    pub fn available_capacity(&self) -> f64 {
        (1.0 - self.current_load).max(0.0)
    }
}

/// Task scheduling decision
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulingDecision {
    /// Task ID
    pub task_id: Uuid,
    /// Assigned worker ID
    pub worker_id: String,
    /// Priority
    pub priority: TaskPriority,
    /// Estimated execution time (seconds)
    pub estimated_time: Option<u64>,
}

impl SchedulingDecision {
    /// Create a new scheduling decision
    pub fn new(task_id: Uuid, worker_id: impl Into<String>, priority: TaskPriority) -> Self {
        Self {
            task_id,
            worker_id: worker_id.into(),
            priority,
            estimated_time: None,
        }
    }

    /// Set estimated execution time
    pub fn with_estimated_time(mut self, seconds: u64) -> Self {
        self.estimated_time = Some(seconds);
        self
    }
}

/// Scheduling strategy for task distribution
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchedulingStrategy {
    /// Round-robin distribution
    RoundRobin,
    /// Assign to worker with lowest load
    #[default]
    LeastLoaded,
    /// Priority-based scheduling
    PriorityBased,
    /// Resource-aware scheduling
    ResourceAware,
}

impl std::fmt::Display for SchedulingStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RoundRobin => write!(f, "RoundRobin"),
            Self::LeastLoaded => write!(f, "LeastLoaded"),
            Self::PriorityBased => write!(f, "PriorityBased"),
            Self::ResourceAware => write!(f, "ResourceAware"),
        }
    }
}

/// Parallel workflow scheduler for task distribution
#[derive(Debug, Clone)]
pub struct ParallelScheduler {
    /// Scheduling strategy
    pub strategy: SchedulingStrategy,
    /// Worker capacities
    pub workers: Vec<WorkerCapacity>,
    /// Enable load balancing
    pub load_balancing: bool,
    /// Maximum tasks per worker
    pub max_tasks_per_worker: Option<usize>,
}

impl ParallelScheduler {
    /// Create a new parallel scheduler
    pub fn new(strategy: SchedulingStrategy) -> Self {
        Self {
            strategy,
            workers: Vec::new(),
            load_balancing: true,
            max_tasks_per_worker: None,
        }
    }

    /// Add a worker to the scheduler
    pub fn add_worker(&mut self, worker: WorkerCapacity) {
        self.workers.push(worker);
    }

    /// Enable load balancing
    pub fn with_load_balancing(mut self, enabled: bool) -> Self {
        self.load_balancing = enabled;
        self
    }

    /// Set maximum tasks per worker
    pub fn with_max_tasks_per_worker(mut self, max: usize) -> Self {
        self.max_tasks_per_worker = Some(max);
        self
    }

    /// Schedule a task to a worker
    pub fn schedule_task(
        &self,
        task_id: Uuid,
        priority: TaskPriority,
    ) -> Option<SchedulingDecision> {
        if self.workers.is_empty() {
            return None;
        }

        let worker_id = match self.strategy {
            SchedulingStrategy::RoundRobin => {
                // Simple round-robin based on task count
                self.workers
                    .iter()
                    .min_by_key(|w| w.active_tasks)
                    .map(|w| w.worker_id.clone())
            }
            SchedulingStrategy::LeastLoaded => {
                // Assign to worker with lowest load
                self.workers
                    .iter()
                    .filter(|w| {
                        if let Some(max) = self.max_tasks_per_worker {
                            w.active_tasks < max
                        } else {
                            true
                        }
                    })
                    .min_by(|a, b| {
                        a.current_load
                            .partial_cmp(&b.current_load)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .map(|w| w.worker_id.clone())
            }
            SchedulingStrategy::PriorityBased => {
                // Higher priority tasks go to less loaded workers
                let priority_weight = priority as u8 as f64 / 15.0;
                self.workers
                    .iter()
                    .filter(|w| {
                        if let Some(max) = self.max_tasks_per_worker {
                            w.active_tasks < max
                        } else {
                            true
                        }
                    })
                    .min_by(|a, b| {
                        let a_score = a.current_load * (1.0 - priority_weight);
                        let b_score = b.current_load * (1.0 - priority_weight);
                        a_score
                            .partial_cmp(&b_score)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .map(|w| w.worker_id.clone())
            }
            SchedulingStrategy::ResourceAware => {
                // Consider both CPU and memory availability
                self.workers
                    .iter()
                    .filter(|w| {
                        if let Some(max) = self.max_tasks_per_worker {
                            w.active_tasks < max
                        } else {
                            true
                        }
                    })
                    .max_by(|a, b| {
                        let a_score = a.available_capacity()
                            * (a.cpu_cores as f64 / 100.0)
                            * (a.memory_mb as f64 / 1_000_000.0);
                        let b_score = b.available_capacity()
                            * (b.cpu_cores as f64 / 100.0)
                            * (b.memory_mb as f64 / 1_000_000.0);
                        a_score
                            .partial_cmp(&b_score)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .map(|w| w.worker_id.clone())
            }
        };

        worker_id.map(|id| SchedulingDecision::new(task_id, id, priority))
    }

    /// Get worker count
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// Get total capacity across all workers
    pub fn total_capacity(&self) -> f64 {
        self.workers.iter().map(|w| w.available_capacity()).sum()
    }

    /// Get average load across all workers
    pub fn average_load(&self) -> f64 {
        if self.workers.is_empty() {
            return 0.0;
        }
        let total_load: f64 = self.workers.iter().map(|w| w.current_load).sum();
        total_load / self.workers.len() as f64
    }
}

impl Default for ParallelScheduler {
    fn default() -> Self {
        Self::new(SchedulingStrategy::default())
    }
}

impl std::fmt::Display for ParallelScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ParallelScheduler[strategy={}, workers={}, avg_load={:.2}]",
            self.strategy,
            self.workers.len(),
            self.average_load()
        )
    }
}

// ============================================================================
// Workflow Batching
// ============================================================================

/// Workflow batch for grouping similar workflows
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowBatch {
    /// Batch ID
    pub batch_id: Uuid,
    /// Workflow IDs in this batch
    pub workflow_ids: Vec<Uuid>,
    /// Batch priority
    pub priority: TaskPriority,
    /// Maximum batch size
    pub max_size: usize,
    /// Batch timeout (seconds)
    pub timeout: Option<u64>,
    /// Creation timestamp
    pub created_at: u64,
}

impl WorkflowBatch {
    /// Create a new workflow batch
    pub fn new(max_size: usize) -> Self {
        Self {
            batch_id: Uuid::new_v4(),
            workflow_ids: Vec::new(),
            priority: TaskPriority::Normal,
            max_size,
            timeout: None,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    /// Add a workflow to the batch
    pub fn add_workflow(&mut self, workflow_id: Uuid) -> bool {
        if self.workflow_ids.len() < self.max_size {
            self.workflow_ids.push(workflow_id);
            true
        } else {
            false
        }
    }

    /// Check if batch is full
    pub fn is_full(&self) -> bool {
        self.workflow_ids.len() >= self.max_size
    }

    /// Check if batch is empty
    pub fn is_empty(&self) -> bool {
        self.workflow_ids.is_empty()
    }

    /// Get batch size
    pub fn size(&self) -> usize {
        self.workflow_ids.len()
    }

    /// Check if batch has timed out
    pub fn is_timed_out(&self) -> bool {
        if let Some(timeout) = self.timeout {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let age = now.saturating_sub(self.created_at);
            age >= timeout
        } else {
            false
        }
    }

    /// Set priority
    pub fn with_priority(mut self, priority: TaskPriority) -> Self {
        self.priority = priority;
        self
    }

    /// Set timeout
    pub fn with_timeout(mut self, seconds: u64) -> Self {
        self.timeout = Some(seconds);
        self
    }
}

impl std::fmt::Display for WorkflowBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WorkflowBatch[id={}, size={}/{}, priority={}]",
            self.batch_id,
            self.size(),
            self.max_size,
            self.priority
        )
    }
}

/// Batching strategy for workflow grouping
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum BatchingStrategy {
    /// Batch by workflow type
    #[default]
    ByType,
    /// Batch by priority
    ByPriority,
    /// Batch by size (group similar-sized workflows)
    BySize,
    /// Batch by time window
    ByTimeWindow,
}

impl std::fmt::Display for BatchingStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ByType => write!(f, "ByType"),
            Self::ByPriority => write!(f, "ByPriority"),
            Self::BySize => write!(f, "BySize"),
            Self::ByTimeWindow => write!(f, "ByTimeWindow"),
        }
    }
}

/// Workflow batcher for grouping similar workflows
#[derive(Debug, Clone)]
pub struct WorkflowBatcher {
    /// Batching strategy
    pub strategy: BatchingStrategy,
    /// Active batches
    pub batches: Vec<WorkflowBatch>,
    /// Default batch size
    pub default_batch_size: usize,
    /// Default batch timeout (seconds)
    pub default_timeout: Option<u64>,
}

impl WorkflowBatcher {
    /// Create a new workflow batcher
    pub fn new(strategy: BatchingStrategy) -> Self {
        Self {
            strategy,
            batches: Vec::new(),
            default_batch_size: 10,
            default_timeout: Some(60), // 1 minute default
        }
    }

    /// Set default batch size
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.default_batch_size = size;
        self
    }

    /// Set default batch timeout
    pub fn with_timeout(mut self, seconds: u64) -> Self {
        self.default_timeout = Some(seconds);
        self
    }

    /// Add a workflow to a batch
    pub fn add_workflow(&mut self, workflow_id: Uuid, priority: TaskPriority) -> Uuid {
        // Find or create appropriate batch
        let batch_id = match self.strategy {
            BatchingStrategy::ByPriority => {
                // Find batch with matching priority
                let batch = self
                    .batches
                    .iter_mut()
                    .find(|b| b.priority == priority && !b.is_full() && !b.is_timed_out());

                if let Some(batch) = batch {
                    batch.add_workflow(workflow_id);
                    batch.batch_id
                } else {
                    // Create new batch
                    let mut new_batch =
                        WorkflowBatch::new(self.default_batch_size).with_priority(priority);
                    if let Some(timeout) = self.default_timeout {
                        new_batch = new_batch.with_timeout(timeout);
                    }
                    new_batch.add_workflow(workflow_id);
                    let batch_id = new_batch.batch_id;
                    self.batches.push(new_batch);
                    batch_id
                }
            }
            _ => {
                // For other strategies, use first available batch
                let batch = self
                    .batches
                    .iter_mut()
                    .find(|b| !b.is_full() && !b.is_timed_out());

                if let Some(batch) = batch {
                    batch.add_workflow(workflow_id);
                    batch.batch_id
                } else {
                    // Create new batch
                    let mut new_batch = WorkflowBatch::new(self.default_batch_size);
                    if let Some(timeout) = self.default_timeout {
                        new_batch = new_batch.with_timeout(timeout);
                    }
                    new_batch.add_workflow(workflow_id);
                    let batch_id = new_batch.batch_id;
                    self.batches.push(new_batch);
                    batch_id
                }
            }
        };

        batch_id
    }

    /// Get ready batches (full or timed out)
    pub fn get_ready_batches(&self) -> Vec<&WorkflowBatch> {
        self.batches
            .iter()
            .filter(|b| b.is_full() || b.is_timed_out())
            .collect()
    }

    /// Remove ready batches
    pub fn remove_ready_batches(&mut self) -> Vec<WorkflowBatch> {
        let (ready, pending): (Vec<_>, Vec<_>) = self
            .batches
            .drain(..)
            .partition(|b| b.is_full() || b.is_timed_out());
        self.batches = pending;
        ready
    }

    /// Get batch count
    pub fn batch_count(&self) -> usize {
        self.batches.len()
    }

    /// Get total workflow count across all batches
    pub fn total_workflow_count(&self) -> usize {
        self.batches.iter().map(|b| b.size()).sum()
    }
}

impl Default for WorkflowBatcher {
    fn default() -> Self {
        Self::new(BatchingStrategy::default())
    }
}

impl std::fmt::Display for WorkflowBatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WorkflowBatcher[strategy={}, batches={}, workflows={}]",
            self.strategy,
            self.batch_count(),
            self.total_workflow_count()
        )
    }
}
