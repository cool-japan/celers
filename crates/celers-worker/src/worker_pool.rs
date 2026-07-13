//! Worker pool with automatic scaling
//!
//! This module provides a worker pool that can automatically scale up or down
//! based on queue depth and system load.
//!
//! # Features
//!
//! - Automatic worker spawning based on queue depth
//! - Worker pool size limits and quotas
//! - Worker specialization (dedicated workers for task types)
//! - Dynamic scaling policies (manual, queue-based, load-based)
//! - Worker health monitoring and replacement
//! - Graceful pool shutdown
//!
//! # Example
//!
//! ```no_run
//! use celers_worker::{WorkerPool, WorkerPoolConfig, ScalingPolicy};
//! use std::time::Duration;
//!
//! # async fn example() {
//! let config = WorkerPoolConfig::new()
//!     .with_min_workers(2)
//!     .with_max_workers(10)
//!     .with_scaling_policy(ScalingPolicy::QueueBased {
//!         tasks_per_worker: 5,
//!         scale_up_threshold: 10,
//!         scale_down_threshold: 2,
//!     });
//!
//! // Create and start the pool
//! // let pool = WorkerPool::new(config);
//! // pool.start().await;
//! # }
//! ```

use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tokio::sync::{Notify, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, error, info};

/// A task function ready to execute in a worker.
///
/// The closure is `FnOnce` + `Send` and returns a boxed future so that it can
/// be sent across thread boundaries and driven to completion on any worker.
pub type WorkerTaskFn =
    Box<dyn FnOnce() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send>;

/// Scaling policy for the worker pool
#[derive(Debug, Clone)]
pub enum ScalingPolicy {
    /// Manual scaling - no automatic scaling
    Manual,
    /// Scale based on queue depth
    QueueBased {
        /// Target tasks per worker
        tasks_per_worker: usize,
        /// Scale up when queue exceeds this threshold
        scale_up_threshold: usize,
        /// Scale down when queue is below this threshold
        scale_down_threshold: usize,
    },
    /// Scale based on system load
    LoadBased {
        /// Target CPU utilization percentage (0-100)
        target_cpu_utilization: f64,
        /// Target memory utilization percentage (0-100)
        target_memory_utilization: f64,
    },
    /// Hybrid policy combining queue and load
    Hybrid {
        /// Queue-based parameters
        tasks_per_worker: usize,
        scale_up_threshold: usize,
        scale_down_threshold: usize,
        /// Load-based parameters
        max_cpu_utilization: f64,
        max_memory_utilization: f64,
    },
}

impl Default for ScalingPolicy {
    fn default() -> Self {
        Self::QueueBased {
            tasks_per_worker: 10,
            scale_up_threshold: 20,
            scale_down_threshold: 5,
        }
    }
}

/// Worker pool configuration
#[derive(Clone)]
pub struct WorkerPoolConfig {
    /// Minimum number of workers
    pub min_workers: usize,
    /// Maximum number of workers
    pub max_workers: usize,
    /// Scaling policy
    pub scaling_policy: ScalingPolicy,
    /// Scaling check interval
    pub scaling_interval: Duration,
    /// Cool-down period after scaling
    pub scaling_cooldown: Duration,
    /// Worker idle timeout (scale down if idle)
    pub worker_idle_timeout: Duration,
    /// Enable worker specialization
    pub enable_specialization: bool,
    /// Worker health check interval
    pub health_check_interval: Duration,
}

impl WorkerPoolConfig {
    /// Create a new worker pool configuration
    pub fn new() -> Self {
        Self {
            min_workers: 1,
            max_workers: 10,
            scaling_policy: ScalingPolicy::default(),
            scaling_interval: Duration::from_secs(30),
            scaling_cooldown: Duration::from_secs(60),
            worker_idle_timeout: Duration::from_secs(300),
            enable_specialization: false,
            health_check_interval: Duration::from_secs(30),
        }
    }

    /// Set minimum number of workers
    pub fn with_min_workers(mut self, min_workers: usize) -> Self {
        self.min_workers = min_workers;
        self
    }

    /// Set maximum number of workers
    pub fn with_max_workers(mut self, max_workers: usize) -> Self {
        self.max_workers = max_workers;
        self
    }

    /// Set scaling policy
    pub fn with_scaling_policy(mut self, policy: ScalingPolicy) -> Self {
        self.scaling_policy = policy;
        self
    }

    /// Set scaling check interval
    pub fn with_scaling_interval(mut self, interval: Duration) -> Self {
        self.scaling_interval = interval;
        self
    }

    /// Set scaling cooldown period
    pub fn with_scaling_cooldown(mut self, cooldown: Duration) -> Self {
        self.scaling_cooldown = cooldown;
        self
    }

    /// Enable worker specialization
    pub fn with_specialization(mut self, enable: bool) -> Self {
        self.enable_specialization = enable;
        self
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.min_workers == 0 {
            return Err("Minimum workers must be greater than 0".to_string());
        }
        if self.max_workers < self.min_workers {
            return Err("Maximum workers must be >= minimum workers".to_string());
        }
        Ok(())
    }
}

impl Default for WorkerPoolConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Worker state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerState {
    /// Worker is starting up
    Starting,
    /// Worker is running and processing tasks
    Running,
    /// Worker is idle (no tasks)
    Idle,
    /// Worker is unhealthy
    Unhealthy,
    /// Worker is shutting down
    ShuttingDown,
    /// Worker has stopped
    Stopped,
}

/// Worker information
#[derive(Clone)]
pub struct WorkerInfo {
    /// Worker ID
    pub id: String,
    /// Worker state
    pub state: WorkerState,
    /// When the worker was created
    pub created_at: Instant,
    /// Last activity timestamp
    pub last_activity: Instant,
    /// Number of tasks processed
    pub tasks_processed: usize,
    /// Specialized task types (if specialization is enabled)
    pub specialized_tasks: Vec<String>,
}

impl WorkerInfo {
    /// Create a new worker info
    pub fn new(id: String) -> Self {
        Self {
            id,
            state: WorkerState::Starting,
            created_at: Instant::now(),
            last_activity: Instant::now(),
            tasks_processed: 0,
            specialized_tasks: Vec::new(),
        }
    }

    /// Check if the worker is idle
    pub fn is_idle(&self, idle_timeout: Duration) -> bool {
        self.state == WorkerState::Idle && self.last_activity.elapsed() >= idle_timeout
    }

    /// Update activity timestamp
    pub fn update_activity(&mut self) {
        self.last_activity = Instant::now();
    }

    /// Increment task counter
    pub fn increment_tasks(&mut self) {
        self.tasks_processed += 1;
    }
}

/// Scaling decision
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalingDecision {
    /// No scaling needed
    None,
    /// Scale up by N workers
    ScaleUp(usize),
    /// Scale down by N workers
    ScaleDown(usize),
}

/// Worker pool statistics
#[derive(Clone, Debug)]
pub struct WorkerPoolStats {
    /// Current number of workers
    pub worker_count: usize,
    /// Number of running workers
    pub running_workers: usize,
    /// Number of idle workers
    pub idle_workers: usize,
    /// Number of unhealthy workers
    pub unhealthy_workers: usize,
    /// Total tasks processed by the pool
    pub total_tasks_processed: usize,
    /// Number of scale up events
    pub scale_up_count: usize,
    /// Number of scale down events
    pub scale_down_count: usize,
    /// Last scaling event timestamp
    pub last_scaling_event: Option<Instant>,
}

impl WorkerPoolStats {
    /// Create new statistics
    pub fn new() -> Self {
        Self {
            worker_count: 0,
            running_workers: 0,
            idle_workers: 0,
            unhealthy_workers: 0,
            total_tasks_processed: 0,
            scale_up_count: 0,
            scale_down_count: 0,
            last_scaling_event: None,
        }
    }
}

impl Default for WorkerPoolStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Worker pool manager
pub struct WorkerPool {
    /// Configuration
    config: WorkerPoolConfig,
    /// Active workers
    workers: Arc<RwLock<HashMap<String, WorkerInfo>>>,
    /// Worker join handles
    handles: Arc<RwLock<HashMap<String, JoinHandle<()>>>>,
    /// Pool statistics
    stats: Arc<RwLock<WorkerPoolStats>>,
    /// Shutdown signal
    shutdown: Arc<Notify>,
    /// Scaling monitor handle
    scaling_handle: Arc<RwLock<Option<JoinHandle<()>>>>,
    /// Current queue depth — set by the caller that owns the broker.
    ///
    /// The pool is broker-agnostic; a broker-aware wrapper should call
    /// [`WorkerPool::set_queue_depth`] periodically (e.g. every poll cycle) so
    /// the autoscaler has a real signal to work with.
    queue_depth: Arc<AtomicUsize>,
    /// Sender half of the task channel — call [`WorkerPool::submit_task`] to push work.
    task_tx: tokio::sync::mpsc::UnboundedSender<WorkerTaskFn>,
    /// Shared receiver used by all workers to compete for tasks (work-stealing).
    task_rx: Arc<tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<WorkerTaskFn>>>,
}

impl WorkerPool {
    /// Create a new worker pool
    pub fn new(config: WorkerPoolConfig) -> Result<Self, String> {
        config.validate()?;

        let (task_tx, task_rx) = tokio::sync::mpsc::unbounded_channel::<WorkerTaskFn>();
        let task_rx = Arc::new(tokio::sync::Mutex::new(task_rx));

        Ok(Self {
            config,
            workers: Arc::new(RwLock::new(HashMap::new())),
            handles: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(WorkerPoolStats::new())),
            shutdown: Arc::new(Notify::new()),
            scaling_handle: Arc::new(RwLock::new(None)),
            queue_depth: Arc::new(AtomicUsize::new(0)),
            task_tx,
            task_rx,
        })
    }

    /// Start the worker pool
    pub async fn start(&self) -> Result<(), String> {
        info!(
            "Starting worker pool with min={} max={} workers",
            self.config.min_workers, self.config.max_workers
        );

        // Spawn minimum workers
        for i in 0..self.config.min_workers {
            let worker_id = format!("worker-{}", i);
            self.spawn_worker(worker_id, vec![]).await?;
        }

        // Start scaling monitor
        self.start_scaling_monitor().await;

        info!(
            "Worker pool started with {} workers",
            self.config.min_workers
        );
        Ok(())
    }

    /// Stop the worker pool
    pub async fn stop(&self) {
        info!("Stopping worker pool");
        self.shutdown.notify_waiters();

        // Stop scaling monitor
        if let Some(handle) = self.scaling_handle.write().await.take() {
            handle.abort();
        }

        // Stop all workers
        let worker_ids: Vec<String> = self.workers.read().await.keys().cloned().collect();
        for worker_id in worker_ids {
            self.stop_worker(&worker_id).await;
        }

        info!("Worker pool stopped");
    }

    /// Spawn a new worker
    async fn spawn_worker(
        &self,
        worker_id: String,
        specialized_tasks: Vec<String>,
    ) -> Result<(), String> {
        let mut info = WorkerInfo::new(worker_id.clone());
        info.specialized_tasks = specialized_tasks;
        info.state = WorkerState::Running;

        let worker_id_clone = worker_id.clone();
        let shutdown = Arc::clone(&self.shutdown);
        let task_rx = Arc::clone(&self.task_rx);

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.notified() => {
                        debug!("Worker {} received shutdown signal", worker_id_clone);
                        break;
                    }
                    task = async { task_rx.lock().await.recv().await } => {
                        match task {
                            Some(task_fn) => {
                                debug!("Worker {} executing task", worker_id_clone);
                                task_fn().await;
                            }
                            None => {
                                // Channel closed — pool is shutting down
                                debug!("Worker {} task channel closed", worker_id_clone);
                                break;
                            }
                        }
                    }
                }
            }
        });

        self.workers.write().await.insert(worker_id.clone(), info);
        self.handles.write().await.insert(worker_id.clone(), handle);

        let mut stats = self.stats.write().await;
        stats.worker_count += 1;
        stats.running_workers += 1;

        info!("Spawned worker: {}", worker_id);
        Ok(())
    }

    /// Stop a worker
    async fn stop_worker(&self, worker_id: &str) {
        let mut workers = self.workers.write().await;
        if let Some(info) = workers.get_mut(worker_id) {
            info.state = WorkerState::ShuttingDown;
        }

        let mut handles = self.handles.write().await;
        if let Some(handle) = handles.remove(worker_id) {
            handle.abort();
        }

        if let Some(_info) = workers.remove(worker_id) {
            let mut stats = self.stats.write().await;
            stats.worker_count = stats.worker_count.saturating_sub(1);
            stats.running_workers = stats.running_workers.saturating_sub(1);
            info!("Stopped worker: {}", worker_id);
        }
    }

    /// Get current worker count
    pub async fn worker_count(&self) -> usize {
        self.workers.read().await.len()
    }

    /// Get pool statistics
    pub async fn get_stats(&self) -> WorkerPoolStats {
        self.stats.read().await.clone()
    }

    /// Update the externally-observed queue depth used by the autoscaler.
    ///
    /// The pool is broker-agnostic and cannot read the queue itself.  A
    /// broker-aware layer should call this on every poll cycle (or whenever
    /// the queue size changes significantly) so the `LoadBased` and
    /// `QueueBased` policies receive real signal instead of zeros.
    pub fn set_queue_depth(&self, depth: usize) {
        self.queue_depth.store(depth, Ordering::Relaxed);
    }

    /// Return a shared handle to the queue-depth counter.
    ///
    /// Callers that already hold the `Arc` from the broker side can increment
    /// or overwrite the counter without going through `set_queue_depth`.
    pub fn queue_depth_handle(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.queue_depth)
    }

    /// Submit a task closure for execution by the next available worker.
    ///
    /// The closure is sent to whichever worker dequeues it first (work-stealing).
    /// Returns `Err` if the pool has been shut down and the channel is closed.
    pub fn submit_task(&self, task: WorkerTaskFn) -> Result<(), String> {
        self.task_tx
            .send(task)
            .map_err(|_| "Worker pool is shut down".to_string())
    }

    /// Make a scaling decision based on the current policy.
    ///
    /// Delegates to [`compute_scaling_decision`] for all policy logic.
    #[allow(dead_code)]
    fn make_scaling_decision(
        &self,
        current_workers: usize,
        queue_depth: usize,
        cpu_usage: f64,
        memory_usage: f64,
    ) -> ScalingDecision {
        compute_scaling_decision(
            &self.config.scaling_policy,
            current_workers,
            queue_depth,
            cpu_usage,
            memory_usage,
            self.config.min_workers,
            self.config.max_workers,
        )
    }

    /// Execute a scaling decision
    #[allow(dead_code)]
    async fn execute_scaling(&self, decision: ScalingDecision) -> Result<(), String> {
        match decision {
            ScalingDecision::None => Ok(()),
            ScalingDecision::ScaleUp(count) => {
                info!("Scaling up by {} workers", count);
                let current_count = self.worker_count().await;

                for i in 0..count {
                    let worker_id = format!("worker-{}", current_count + i);
                    self.spawn_worker(worker_id, vec![]).await?;
                }

                let mut stats = self.stats.write().await;
                stats.scale_up_count += 1;
                stats.last_scaling_event = Some(Instant::now());
                Ok(())
            }
            ScalingDecision::ScaleDown(count) => {
                info!("Scaling down by {} workers", count);

                // Find idle workers to remove
                let workers = self.workers.read().await;
                let idle_workers: Vec<String> = workers
                    .iter()
                    .filter(|(_, info)| info.is_idle(self.config.worker_idle_timeout))
                    .take(count)
                    .map(|(id, _)| id.clone())
                    .collect();
                drop(workers);

                for worker_id in idle_workers {
                    self.stop_worker(&worker_id).await;
                }

                let mut stats = self.stats.write().await;
                stats.scale_down_count += 1;
                stats.last_scaling_event = Some(Instant::now());
                Ok(())
            }
        }
    }

    /// Start the scaling monitor
    async fn start_scaling_monitor(&self) {
        let workers = Arc::clone(&self.workers);
        let stats = Arc::clone(&self.stats);
        let shutdown = Arc::clone(&self.shutdown);
        let config = self.config.clone();

        let pool_ref = Arc::new(self.clone_for_monitor());

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.scaling_interval);
            let mut last_scaling = Instant::now();
            // Local state for delta CPU sampling across monitor ticks.
            let mut prev_cpu_sample: Option<(std::time::Duration, Instant)> = None;

            loop {
                tokio::select! {
                    _ = shutdown.notified() => {
                        debug!("Scaling monitor received shutdown signal");
                        break;
                    }
                    _ = interval.tick() => {
                        // Check if we're in cooldown period
                        if last_scaling.elapsed() < config.scaling_cooldown {
                            continue;
                        }

                        let current_workers = workers.read().await.len();

                        // Real queue depth — set externally by the broker layer.
                        let queue_depth = pool_ref.queue_depth.load(Ordering::Relaxed);

                        // Delta-sampled process CPU utilisation.
                        let cpu_usage = {
                            let now = Instant::now();
                            if let Some(current_cpu) = crate::sysinfo::read_process_cpu_time() {
                                let pct = if let Some((prev_cpu, prev_time)) = prev_cpu_sample {
                                    let cpu_delta = current_cpu.saturating_sub(prev_cpu).as_secs_f64();
                                    let wall_delta = now.duration_since(prev_time).as_secs_f64();
                                    if wall_delta > 0.0 { (cpu_delta / wall_delta) * 100.0 } else { 0.0 }
                                } else {
                                    0.0
                                };
                                prev_cpu_sample = Some((current_cpu, now));
                                pct
                            } else {
                                prev_cpu_sample = None;
                                0.0
                            }
                        };

                        // Process memory as a percentage of total system memory.
                        let memory_usage = {
                            let proc_bytes = crate::sysinfo::read_process_memory_bytes();
                            let total_bytes = crate::sysinfo::read_total_memory_bytes();
                            if total_bytes > 0 {
                                (proc_bytes as f64 / total_bytes as f64) * 100.0
                            } else {
                                0.0
                            }
                        };

                        let decision = pool_ref.make_scaling_decision(
                            current_workers,
                            queue_depth,
                            cpu_usage,
                            memory_usage,
                        );

                        if decision != ScalingDecision::None {
                            if let Err(e) = pool_ref.execute_scaling(decision).await {
                                error!("Failed to execute scaling decision: {}", e);
                            } else {
                                last_scaling = Instant::now();
                            }
                        }

                        // Update statistics
                        let workers_lock = workers.read().await;
                        let mut stats_lock = stats.write().await;
                        stats_lock.running_workers = workers_lock
                            .values()
                            .filter(|w| w.state == WorkerState::Running)
                            .count();
                        stats_lock.idle_workers = workers_lock
                            .values()
                            .filter(|w| w.state == WorkerState::Idle)
                            .count();
                        stats_lock.unhealthy_workers = workers_lock
                            .values()
                            .filter(|w| w.state == WorkerState::Unhealthy)
                            .count();
                    }
                }
            }
        });

        *self.scaling_handle.write().await = Some(handle);
    }

    /// Clone fields needed for the monitor task
    fn clone_for_monitor(&self) -> WorkerPoolMonitor {
        WorkerPoolMonitor {
            config: self.config.clone(),
            workers: Arc::clone(&self.workers),
            handles: Arc::clone(&self.handles),
            stats: Arc::clone(&self.stats),
            shutdown: Arc::clone(&self.shutdown),
            queue_depth: Arc::clone(&self.queue_depth),
            task_rx: Arc::clone(&self.task_rx),
        }
    }
}

/// Helper struct for the scaling monitor
struct WorkerPoolMonitor {
    config: WorkerPoolConfig,
    workers: Arc<RwLock<HashMap<String, WorkerInfo>>>,
    handles: Arc<RwLock<HashMap<String, JoinHandle<()>>>>,
    stats: Arc<RwLock<WorkerPoolStats>>,
    shutdown: Arc<Notify>,
    queue_depth: Arc<AtomicUsize>,
    /// Shared receiver forwarded from the owning `WorkerPool` so dynamically
    /// spawned workers (created by the autoscaler) also pull from the same
    /// task channel.
    task_rx: Arc<tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<WorkerTaskFn>>>,
}

impl WorkerPoolMonitor {
    fn make_scaling_decision(
        &self,
        current_workers: usize,
        queue_depth: usize,
        cpu_usage: f64,
        memory_usage: f64,
    ) -> ScalingDecision {
        compute_scaling_decision(
            &self.config.scaling_policy,
            current_workers,
            queue_depth,
            cpu_usage,
            memory_usage,
            self.config.min_workers,
            self.config.max_workers,
        )
    }

    async fn execute_scaling(&self, decision: ScalingDecision) -> Result<(), String> {
        match decision {
            ScalingDecision::None => Ok(()),
            ScalingDecision::ScaleUp(count) => {
                info!("Scaling up by {} workers", count);
                let current_count = self.workers.read().await.len();

                for i in 0..count {
                    let worker_id = format!("worker-{}", current_count + i);
                    self.spawn_worker(worker_id, vec![]).await?;
                }

                let mut stats = self.stats.write().await;
                stats.scale_up_count += 1;
                stats.last_scaling_event = Some(Instant::now());
                Ok(())
            }
            ScalingDecision::ScaleDown(count) => {
                info!("Scaling down by {} workers", count);

                let workers = self.workers.read().await;
                let idle_workers: Vec<String> = workers
                    .iter()
                    .filter(|(_, info)| info.is_idle(self.config.worker_idle_timeout))
                    .take(count)
                    .map(|(id, _)| id.clone())
                    .collect();
                drop(workers);

                for worker_id in idle_workers {
                    self.stop_worker(&worker_id).await;
                }

                let mut stats = self.stats.write().await;
                stats.scale_down_count += 1;
                stats.last_scaling_event = Some(Instant::now());
                Ok(())
            }
        }
    }

    async fn spawn_worker(
        &self,
        worker_id: String,
        specialized_tasks: Vec<String>,
    ) -> Result<(), String> {
        let mut info = WorkerInfo::new(worker_id.clone());
        info.specialized_tasks = specialized_tasks;
        info.state = WorkerState::Running;

        let worker_id_clone = worker_id.clone();
        let shutdown = Arc::clone(&self.shutdown);
        let task_rx = Arc::clone(&self.task_rx);

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.notified() => {
                        debug!("Worker {} received shutdown signal", worker_id_clone);
                        break;
                    }
                    task = async { task_rx.lock().await.recv().await } => {
                        match task {
                            Some(task_fn) => {
                                debug!("Worker {} executing task", worker_id_clone);
                                task_fn().await;
                            }
                            None => {
                                // Channel closed — pool is shutting down
                                debug!("Worker {} task channel closed", worker_id_clone);
                                break;
                            }
                        }
                    }
                }
            }
        });

        self.workers.write().await.insert(worker_id.clone(), info);
        self.handles.write().await.insert(worker_id.clone(), handle);

        let mut stats = self.stats.write().await;
        stats.worker_count += 1;
        stats.running_workers += 1;

        info!("Spawned worker: {}", worker_id);
        Ok(())
    }

    async fn stop_worker(&self, worker_id: &str) {
        let mut workers = self.workers.write().await;
        if let Some(info) = workers.get_mut(worker_id) {
            info.state = WorkerState::ShuttingDown;
        }

        let mut handles = self.handles.write().await;
        if let Some(handle) = handles.remove(worker_id) {
            handle.abort();
        }

        if let Some(_info) = workers.remove(worker_id) {
            let mut stats = self.stats.write().await;
            stats.worker_count = stats.worker_count.saturating_sub(1);
            stats.running_workers = stats.running_workers.saturating_sub(1);
            info!("Stopped worker: {}", worker_id);
        }
    }
}

/// Compute a scaling decision from the given policy and live metrics.
///
/// This is a pure function (no I/O, no async) so it is easy to unit-test.
/// Both [`WorkerPool`] and [`WorkerPoolMonitor`] delegate to it.
///
/// # Load-based scaling heuristics
///
/// - **`LoadBased`**: Scale up when either CPU or memory utilisation exceeds
///   the configured target.  Scale down when both are below 50 % of their
///   respective targets (hysteresis avoids thrashing).
/// - **`Hybrid`**: Scale up when the queue is deep *or* load is high; scale
///   down only when the queue is shallow *and* load is low.
fn compute_scaling_decision(
    policy: &ScalingPolicy,
    current_workers: usize,
    queue_depth: usize,
    cpu_usage: f64,
    memory_usage: f64,
    min_workers: usize,
    max_workers: usize,
) -> ScalingDecision {
    match policy {
        ScalingPolicy::Manual => ScalingDecision::None,

        ScalingPolicy::QueueBased {
            tasks_per_worker,
            scale_up_threshold,
            scale_down_threshold,
        } => {
            let needed_workers = queue_depth.div_ceil(*tasks_per_worker);
            if queue_depth >= *scale_up_threshold && current_workers < max_workers {
                let to_spawn = (needed_workers.saturating_sub(current_workers))
                    .min(max_workers - current_workers);
                if to_spawn > 0 {
                    return ScalingDecision::ScaleUp(to_spawn);
                }
            } else if queue_depth <= *scale_down_threshold && current_workers > min_workers {
                let to_remove = current_workers.saturating_sub(needed_workers.max(min_workers));
                if to_remove > 0 {
                    return ScalingDecision::ScaleDown(to_remove);
                }
            }
            ScalingDecision::None
        }

        ScalingPolicy::LoadBased {
            target_cpu_utilization,
            target_memory_utilization,
        } => {
            let cpu_high = cpu_usage > *target_cpu_utilization;
            let mem_high = memory_usage > *target_memory_utilization;
            // 50% hysteresis band: scale down only when both metrics are well below target
            let cpu_low = cpu_usage < target_cpu_utilization * 0.5;
            let mem_low = memory_usage < target_memory_utilization * 0.5;

            if (cpu_high || mem_high) && current_workers < max_workers {
                ScalingDecision::ScaleUp(1)
            } else if cpu_low && mem_low && current_workers > min_workers {
                ScalingDecision::ScaleDown(1)
            } else {
                ScalingDecision::None
            }
        }

        ScalingPolicy::Hybrid {
            tasks_per_worker,
            scale_up_threshold,
            scale_down_threshold,
            max_cpu_utilization,
            max_memory_utilization,
        } => {
            let queue_high = queue_depth >= *scale_up_threshold;
            let queue_low = queue_depth <= *scale_down_threshold;
            let load_high =
                cpu_usage > *max_cpu_utilization || memory_usage > *max_memory_utilization;
            let load_low = cpu_usage < max_cpu_utilization * 0.5
                && memory_usage < max_memory_utilization * 0.5;

            if (queue_high || load_high) && current_workers < max_workers {
                let needed_workers = queue_depth.div_ceil(*tasks_per_worker);
                let to_spawn = (needed_workers.saturating_sub(current_workers))
                    .min(max_workers - current_workers)
                    .max(1);
                ScalingDecision::ScaleUp(to_spawn)
            } else if queue_low && load_low && current_workers > min_workers {
                ScalingDecision::ScaleDown(1)
            } else {
                ScalingDecision::None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_pool_config_default() {
        let config = WorkerPoolConfig::default();
        assert_eq!(config.min_workers, 1);
        assert_eq!(config.max_workers, 10);
    }

    #[test]
    fn test_worker_pool_config_builder() {
        let config = WorkerPoolConfig::new()
            .with_min_workers(2)
            .with_max_workers(20)
            .with_scaling_interval(Duration::from_secs(60))
            .with_specialization(true);

        assert_eq!(config.min_workers, 2);
        assert_eq!(config.max_workers, 20);
        assert_eq!(config.scaling_interval, Duration::from_secs(60));
        assert!(config.enable_specialization);
    }

    #[test]
    fn test_worker_pool_config_validation() {
        let invalid_config = WorkerPoolConfig::new().with_min_workers(0);
        assert!(invalid_config.validate().is_err());

        let invalid_config2 = WorkerPoolConfig::new()
            .with_min_workers(10)
            .with_max_workers(5);
        assert!(invalid_config2.validate().is_err());

        let valid_config = WorkerPoolConfig::new()
            .with_min_workers(2)
            .with_max_workers(10);
        assert!(valid_config.validate().is_ok());
    }

    #[tokio::test]
    async fn test_worker_pool_creation() {
        let config = WorkerPoolConfig::default();
        let pool = WorkerPool::new(config);
        assert!(pool.is_ok());
    }

    #[tokio::test]
    async fn test_worker_pool_start_stop() {
        let config = WorkerPoolConfig::new().with_min_workers(2);
        let pool = WorkerPool::new(config).unwrap();

        pool.start().await.unwrap();
        assert_eq!(pool.worker_count().await, 2);

        pool.stop().await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(pool.worker_count().await, 0);
    }

    #[test]
    fn test_worker_info_creation() {
        let info = WorkerInfo::new("worker-1".to_string());
        assert_eq!(info.id, "worker-1");
        assert_eq!(info.state, WorkerState::Starting);
        assert_eq!(info.tasks_processed, 0);
    }

    #[test]
    fn test_worker_info_idle_check() {
        let mut info = WorkerInfo::new("worker-1".to_string());
        info.state = WorkerState::Idle;

        // Not idle yet (just created)
        assert!(!info.is_idle(Duration::from_secs(1)));
    }

    #[test]
    fn test_scaling_decision() {
        let config = WorkerPoolConfig::new()
            .with_min_workers(1)
            .with_max_workers(10)
            .with_scaling_policy(ScalingPolicy::QueueBased {
                tasks_per_worker: 5,
                scale_up_threshold: 10,
                scale_down_threshold: 2,
            });

        let pool = WorkerPool::new(config).unwrap();

        // Should scale up when queue is high
        let decision = pool.make_scaling_decision(2, 15, 0.0, 0.0);
        assert!(matches!(decision, ScalingDecision::ScaleUp(_)));

        // Should scale down when queue is low
        let decision = pool.make_scaling_decision(5, 1, 0.0, 0.0);
        assert!(matches!(decision, ScalingDecision::ScaleDown(_)));

        // Should not scale when queue is moderate
        let decision = pool.make_scaling_decision(2, 8, 0.0, 0.0);
        assert_eq!(decision, ScalingDecision::None);
    }

    #[tokio::test]
    async fn test_worker_pool_stats() {
        let config = WorkerPoolConfig::new().with_min_workers(3);
        let pool = WorkerPool::new(config).unwrap();

        pool.start().await.unwrap();

        let stats = pool.get_stats().await;
        assert_eq!(stats.worker_count, 3);

        pool.stop().await;
    }

    #[test]
    fn test_scaling_policy_default() {
        let policy = ScalingPolicy::default();
        assert!(matches!(policy, ScalingPolicy::QueueBased { .. }));
    }

    #[test]
    fn test_worker_states() {
        assert_eq!(WorkerState::Running, WorkerState::Running);
        assert_ne!(WorkerState::Running, WorkerState::Idle);
    }

    // --- compute_scaling_decision unit tests ---

    #[test]
    fn test_load_based_scale_up_on_high_cpu() {
        let decision = compute_scaling_decision(
            &ScalingPolicy::LoadBased {
                target_cpu_utilization: 70.0,
                target_memory_utilization: 80.0,
            },
            /*current*/ 2,
            /*queue*/ 0,
            /*cpu*/ 85.0,
            /*mem*/ 50.0,
            /*min*/ 1,
            /*max*/ 10,
        );
        assert!(
            matches!(decision, ScalingDecision::ScaleUp(1)),
            "Expected ScaleUp(1), got {:?}",
            decision
        );
    }

    #[test]
    fn test_load_based_scale_up_on_high_memory() {
        let decision = compute_scaling_decision(
            &ScalingPolicy::LoadBased {
                target_cpu_utilization: 70.0,
                target_memory_utilization: 80.0,
            },
            2,
            0,
            /*cpu*/ 30.0,
            /*mem*/ 90.0,
            1,
            10,
        );
        assert!(matches!(decision, ScalingDecision::ScaleUp(1)));
    }

    #[test]
    fn test_load_based_scale_down_when_idle() {
        let decision = compute_scaling_decision(
            &ScalingPolicy::LoadBased {
                target_cpu_utilization: 70.0,
                target_memory_utilization: 80.0,
            },
            /*current*/ 5,
            0,
            /*cpu*/ 10.0,
            /*mem*/ 15.0,
            /*min*/ 1,
            10,
        );
        assert!(matches!(decision, ScalingDecision::ScaleDown(1)));
    }

    #[test]
    fn test_load_based_no_scale_in_band() {
        let decision = compute_scaling_decision(
            &ScalingPolicy::LoadBased {
                target_cpu_utilization: 70.0,
                target_memory_utilization: 80.0,
            },
            3,
            0,
            /*cpu*/ 55.0,
            /*mem*/ 60.0,
            1,
            10,
        );
        assert_eq!(decision, ScalingDecision::None);
    }

    #[test]
    fn test_hybrid_scale_up_on_queue_depth() {
        let policy = ScalingPolicy::Hybrid {
            tasks_per_worker: 5,
            scale_up_threshold: 10,
            scale_down_threshold: 2,
            max_cpu_utilization: 80.0,
            max_memory_utilization: 80.0,
        };
        // Queue high, load low → should scale up
        let decision = compute_scaling_decision(&policy, 2, 20, 10.0, 10.0, 1, 10);
        assert!(matches!(decision, ScalingDecision::ScaleUp(_)));
    }

    #[test]
    fn test_hybrid_scale_up_on_high_load() {
        let policy = ScalingPolicy::Hybrid {
            tasks_per_worker: 5,
            scale_up_threshold: 10,
            scale_down_threshold: 2,
            max_cpu_utilization: 80.0,
            max_memory_utilization: 80.0,
        };
        // Queue shallow but load high → should scale up
        let decision = compute_scaling_decision(&policy, 2, 3, 95.0, 10.0, 1, 10);
        assert!(matches!(decision, ScalingDecision::ScaleUp(_)));
    }

    #[test]
    fn test_hybrid_scale_down_when_quiet() {
        let policy = ScalingPolicy::Hybrid {
            tasks_per_worker: 5,
            scale_up_threshold: 10,
            scale_down_threshold: 2,
            max_cpu_utilization: 80.0,
            max_memory_utilization: 80.0,
        };
        // Queue low AND load low → should scale down
        let decision = compute_scaling_decision(&policy, 5, 1, 5.0, 5.0, 1, 10);
        assert!(matches!(decision, ScalingDecision::ScaleDown(1)));
    }

    #[test]
    fn test_hybrid_no_scale_mixed_signals() {
        let policy = ScalingPolicy::Hybrid {
            tasks_per_worker: 5,
            scale_up_threshold: 10,
            scale_down_threshold: 2,
            max_cpu_utilization: 80.0,
            max_memory_utilization: 80.0,
        };
        // Queue low but load moderate → neither clear scale-up nor scale-down
        let decision = compute_scaling_decision(&policy, 3, 1, 50.0, 50.0, 1, 10);
        assert_eq!(decision, ScalingDecision::None);
    }

    #[test]
    fn test_set_queue_depth_and_handle() {
        let pool = WorkerPool::new(WorkerPoolConfig::default()).unwrap();
        pool.set_queue_depth(42);
        assert_eq!(pool.queue_depth.load(Ordering::Relaxed), 42);

        let handle = pool.queue_depth_handle();
        handle.store(99, Ordering::Relaxed);
        assert_eq!(pool.queue_depth.load(Ordering::Relaxed), 99);
    }

    #[tokio::test]
    async fn test_submit_task_executes() {
        use std::sync::atomic::AtomicBool;

        let config = WorkerPoolConfig::new()
            .with_min_workers(1)
            .with_max_workers(2);
        let pool = WorkerPool::new(config).unwrap();
        pool.start().await.unwrap();

        let flag = Arc::new(AtomicBool::new(false));
        let flag2 = Arc::clone(&flag);

        pool.submit_task(Box::new(move || {
            Box::pin(async move {
                flag2.store(true, Ordering::SeqCst);
            })
        }))
        .expect("submit should succeed");

        // Give the worker time to execute
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(flag.load(Ordering::SeqCst), "task should have executed");

        pool.stop().await;
    }

    #[tokio::test]
    async fn test_submit_multiple_tasks() {
        use std::sync::atomic::AtomicUsize as StdAtomicUsize;

        let config = WorkerPoolConfig::new()
            .with_min_workers(2)
            .with_max_workers(4);
        let pool = WorkerPool::new(config).unwrap();
        pool.start().await.unwrap();

        let counter = Arc::new(StdAtomicUsize::new(0));
        for _ in 0..10 {
            let counter2 = Arc::clone(&counter);
            pool.submit_task(Box::new(move || {
                Box::pin(async move {
                    counter2.fetch_add(1, Ordering::SeqCst);
                })
            }))
            .expect("submit should succeed");
        }

        // Wait for all tasks to be picked up and executed
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(
            counter.load(Ordering::SeqCst),
            10,
            "all 10 tasks should have executed"
        );

        pool.stop().await;
    }
}
