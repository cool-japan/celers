//! Worker pool with automatic scaling
//!
//! This module provides a worker pool that can automatically scale up or down
//! based on queue depth and system load.
//!
//! # Features
//!
//! - Automatic worker spawning based on queue depth
//! - Worker pool size limits and quotas
//! - Dynamic scaling policies (manual, queue-based, load-based)
//! - Worker health monitoring and replacement
//! - Graceful pool shutdown (drain, then abort stragglers)
//! - Bounded submission queue with real backpressure
//!
//! # Task distribution
//!
//! Submissions go through a single bounded MPSC channel whose receiver is
//! shared between workers behind a mutex: workers compete for the next task,
//! first-come-first-served. This is a **shared queue**, not work stealing —
//! there are no per-worker deques and nothing is ever stolen from another
//! worker. The distinction matters for latency expectations: each hand-off
//! costs a mutex hand-over, so exactly one worker at a time is parked in
//! `recv()` while the rest wait on the mutex.
//!
//! # Scaling signals
//!
//! Queue depth is supplied by the embedding application through
//! [`WorkerPool::set_queue_depth`] — the pool is broker-agnostic. CPU and
//! memory utilisation are sampled from [`crate::sysinfo`] and are
//! [`Option`]al: a missing sample means *unknown*, never *idle*. An unknown
//! load signal can never trigger a scale-down, because "I cannot measure the
//! load" and "the load is zero" are very different statements.
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
use tokio::sync::{mpsc, watch, Mutex, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

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
        /// Target tasks per worker
        tasks_per_worker: usize,
        /// Scale up when the queue exceeds this threshold
        scale_up_threshold: usize,
        /// Scale down when the queue is below this threshold
        scale_down_threshold: usize,
        /// Scale up when CPU utilisation exceeds this percentage
        max_cpu_utilization: f64,
        /// Scale up when memory utilisation exceeds this percentage
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
    /// Capacity of the submission queue.
    ///
    /// [`WorkerPool::submit_task`] refuses work once this many tasks are
    /// waiting, and [`WorkerPool::submit_task_async`] parks the producer —
    /// either way the producer, not the heap, absorbs the overload.
    pub max_queue_depth: usize,
    /// How long [`WorkerPool::stop`] lets in-flight tasks finish before
    /// aborting the workers still running them.
    pub shutdown_grace_period: Duration,
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
            max_queue_depth: 1024,
            shutdown_grace_period: Duration::from_secs(30),
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

    /// Set the idle timeout after which a worker becomes a scale-down candidate
    pub fn with_worker_idle_timeout(mut self, timeout: Duration) -> Self {
        self.worker_idle_timeout = timeout;
        self
    }

    /// Set the health check interval
    pub fn with_health_check_interval(mut self, interval: Duration) -> Self {
        self.health_check_interval = interval;
        self
    }

    /// Set the submission queue capacity
    pub fn with_max_queue_depth(mut self, depth: usize) -> Self {
        self.max_queue_depth = depth;
        self
    }

    /// Set the graceful shutdown budget
    pub fn with_shutdown_grace_period(mut self, grace: Duration) -> Self {
        self.shutdown_grace_period = grace;
        self
    }

    /// Enable worker specialization
    pub fn with_specialization(mut self, enable: bool) -> Self {
        self.enable_specialization = enable;
        self
    }

    /// Validate configuration
    ///
    /// # Errors
    ///
    /// Returns a message describing the first inconsistent setting.
    pub fn validate(&self) -> Result<(), String> {
        if self.min_workers == 0 {
            return Err("Minimum workers must be greater than 0".to_string());
        }
        if self.max_workers < self.min_workers {
            return Err("Maximum workers must be >= minimum workers".to_string());
        }
        if self.max_queue_depth == 0 {
            return Err("Maximum queue depth must be greater than 0".to_string());
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
    ///
    /// "Idle" means the worker finished its last task (or has never had one)
    /// *and* has been waiting for at least `idle_timeout`. The worker loop
    /// maintains both halves of that condition.
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

/// A live worker's join handle plus its private stop signal.
struct WorkerHandle {
    join: JoinHandle<()>,
    /// Per-worker stop signal, used by scale-down so a single worker can be
    /// retired without disturbing its peers.
    stop: watch::Sender<bool>,
}

/// State shared by the pool and its scaling monitor.
///
/// The pool and its monitor used to keep parallel copies of this state and
/// duplicate `spawn_worker`/`stop_worker`/`execute_scaling` — which is how the
/// two of them ended up minting colliding worker IDs from independent
/// counters. There is exactly one copy now.
struct PoolInner {
    config: WorkerPoolConfig,
    workers: RwLock<HashMap<String, WorkerInfo>>,
    handles: RwLock<HashMap<String, WorkerHandle>>,
    /// Cumulative counters only; live counts are derived from the maps.
    stats: RwLock<WorkerPoolStats>,
    /// Pool-wide shutdown signal.
    ///
    /// A `watch` channel rather than a `Notify`: it retains the latest value,
    /// so a worker busy inside a task observes the signal at its next loop
    /// iteration. `Notify::notify_waiters` only wakes futures that are already
    /// registered, which is precisely never for a busy worker.
    shutdown: watch::Sender<bool>,
    queue_depth: Arc<AtomicUsize>,
    /// Monotonic worker-ID source. Never reused, so a scale-up after a
    /// scale-down cannot regenerate a live worker's name and silently replace
    /// (and detach) its join handle.
    next_worker_id: AtomicUsize,
    total_tasks_processed: AtomicUsize,
    task_rx: Mutex<mpsc::Receiver<WorkerTaskFn>>,
}

impl PoolInner {
    fn next_worker_id(&self) -> String {
        format!(
            "worker-{}",
            self.next_worker_id.fetch_add(1, Ordering::Relaxed)
        )
    }

    async fn worker_count(&self) -> usize {
        self.workers.read().await.len()
    }

    /// Update a worker's state, ignoring workers that have already been removed.
    async fn set_state(&self, worker_id: &str, state: WorkerState) {
        if let Some(info) = self.workers.write().await.get_mut(worker_id) {
            info.state = state;
        }
    }

    /// Mark the worker busy with a task.
    async fn begin_task(&self, worker_id: &str) {
        if let Some(info) = self.workers.write().await.get_mut(worker_id) {
            info.state = WorkerState::Running;
            info.update_activity();
        }
    }

    /// Mark the worker idle again and account for the completed task.
    async fn finish_task(&self, worker_id: &str) {
        self.total_tasks_processed.fetch_add(1, Ordering::Relaxed);
        if let Some(info) = self.workers.write().await.get_mut(worker_id) {
            info.increment_tasks();
            info.update_activity();
            info.state = WorkerState::Idle;
        }
    }

    /// Spawn a worker and register it.
    async fn spawn_worker(
        self: &Arc<Self>,
        worker_id: String,
        specialized_tasks: Vec<String>,
    ) -> Result<(), String> {
        let mut info = WorkerInfo::new(worker_id.clone());
        info.specialized_tasks = specialized_tasks;
        // A freshly spawned worker has no task yet: it starts idle, which is
        // also what makes it eligible for scale-down once the idle timeout
        // elapses.
        info.state = WorkerState::Idle;

        let (stop_tx, stop_rx) = watch::channel(false);
        let inner = Arc::clone(self);
        let loop_id = worker_id.clone();
        let join = tokio::spawn(async move { worker_loop(inner, loop_id, stop_rx).await });

        self.workers.write().await.insert(worker_id.clone(), info);
        if let Some(previous) = self.handles.write().await.insert(
            worker_id.clone(),
            WorkerHandle {
                join,
                stop: stop_tx,
            },
        ) {
            // Defensive: IDs are minted from a monotonic counter, so this
            // cannot happen -- but if it ever did, dropping the old handle
            // would detach a worker that keeps draining the task channel
            // forever, invisible to `stop()`.
            error!(
                "Worker ID {} was reused; aborting the displaced worker",
                worker_id
            );
            let _ = previous.stop.send(true);
            previous.join.abort();
        }

        info!("Spawned worker: {}", worker_id);
        Ok(())
    }

    /// Stop one worker: signal it, let it drain, abort if it overstays.
    async fn stop_worker(&self, worker_id: &str, grace: Duration) {
        self.set_state(worker_id, WorkerState::ShuttingDown).await;

        let handle = self.handles.write().await.remove(worker_id);
        if let Some(WorkerHandle { mut join, stop }) = handle {
            let _ = stop.send(true);
            // `&mut join` keeps ownership: a consumed handle would be dropped
            // on timeout, which detaches the task instead of stopping it.
            if tokio::time::timeout(grace, &mut join).await.is_err() {
                warn!(
                    "Worker {} did not finish within {:?}; aborting",
                    worker_id, grace
                );
                join.abort();
            }
        }

        if self.workers.write().await.remove(worker_id).is_some() {
            info!("Stopped worker: {}", worker_id);
        }
    }

    /// Replace workers whose task ended unexpectedly.
    ///
    /// A worker loop only returns on shutdown or on its own stop signal, so a
    /// finished join handle for a worker still in the map means the loop
    /// panicked or exited on its own — the pool is short one worker until it
    /// is replaced.
    async fn health_sweep(self: &Arc<Self>) -> usize {
        if *self.shutdown.borrow() {
            return 0;
        }

        let dead: Vec<String> = {
            let handles = self.handles.read().await;
            handles
                .iter()
                .filter(|(_, handle)| handle.join.is_finished())
                .map(|(id, _)| id.clone())
                .collect()
        };

        let mut replaced = 0;
        for worker_id in dead {
            warn!("Worker {} exited unexpectedly; replacing it", worker_id);
            self.set_state(&worker_id, WorkerState::Unhealthy).await;
            // Already finished, so this returns immediately.
            self.stop_worker(&worker_id, Duration::ZERO).await;

            let replacement = self.next_worker_id();
            match self.spawn_worker(replacement, Vec::new()).await {
                Ok(()) => replaced += 1,
                Err(e) => error!("Failed to replace worker {}: {}", worker_id, e),
            }
        }
        replaced
    }

    /// Apply a scaling decision.
    ///
    /// Returns how many workers actually changed, which is *not* the same as
    /// the requested count: a scale-down with no idle candidates changes
    /// nothing, and reporting that honestly is what keeps the cooldown timer
    /// and the scale-down counter from recording events that never happened.
    async fn execute_scaling(self: &Arc<Self>, decision: ScalingDecision) -> Result<usize, String> {
        match decision {
            ScalingDecision::None => Ok(0),
            ScalingDecision::ScaleUp(count) => {
                // `max_workers` is an invariant, not a hint: honour it even
                // when a caller asks for more directly.
                let room = self
                    .config
                    .max_workers
                    .saturating_sub(self.worker_count().await);
                let count = count.min(room);
                if count == 0 {
                    debug!("Scale-up requested but the pool is already at max_workers");
                    return Ok(0);
                }

                info!("Scaling up by {} workers", count);
                let mut spawned = 0;
                for _ in 0..count {
                    let worker_id = self.next_worker_id();
                    self.spawn_worker(worker_id, Vec::new()).await?;
                    spawned += 1;
                }

                if spawned > 0 {
                    let mut stats = self.stats.write().await;
                    stats.scale_up_count += 1;
                    stats.last_scaling_event = Some(Instant::now());
                }
                Ok(spawned)
            }
            ScalingDecision::ScaleDown(count) => {
                // Likewise for `min_workers`.
                let spare = self
                    .worker_count()
                    .await
                    .saturating_sub(self.config.min_workers);
                let count = count.min(spare);
                if count == 0 {
                    debug!("Scale-down requested but the pool is already at min_workers");
                    return Ok(0);
                }

                let idle_workers: Vec<String> = {
                    let workers = self.workers.read().await;
                    workers
                        .iter()
                        .filter(|(_, info)| info.is_idle(self.config.worker_idle_timeout))
                        .take(count)
                        .map(|(id, _)| id.clone())
                        .collect()
                };

                if idle_workers.is_empty() {
                    debug!(
                        "Scale-down by {} requested but no worker has been idle for {:?}",
                        count, self.config.worker_idle_timeout
                    );
                    return Ok(0);
                }

                info!(
                    "Scaling down by {} workers (requested {})",
                    idle_workers.len(),
                    count
                );
                let removed = idle_workers.len();
                for worker_id in idle_workers {
                    self.stop_worker(&worker_id, self.config.shutdown_grace_period)
                        .await;
                }

                let mut stats = self.stats.write().await;
                stats.scale_down_count += 1;
                stats.last_scaling_event = Some(Instant::now());
                Ok(removed)
            }
        }
    }

    /// Snapshot the statistics, deriving the live counts from the worker map
    /// rather than from counters that can drift out of step with it.
    async fn snapshot_stats(&self) -> WorkerPoolStats {
        let workers = self.workers.read().await;
        let mut stats = self.stats.read().await.clone();

        stats.worker_count = workers.len();
        stats.running_workers = workers
            .values()
            .filter(|w| w.state == WorkerState::Running)
            .count();
        stats.idle_workers = workers
            .values()
            .filter(|w| w.state == WorkerState::Idle)
            .count();
        stats.unhealthy_workers = workers
            .values()
            .filter(|w| w.state == WorkerState::Unhealthy)
            .count();
        stats.total_tasks_processed = self.total_tasks_processed.load(Ordering::Relaxed);
        stats
    }
}

/// The worker task loop.
///
/// Exits on pool shutdown, on its own stop signal, or when the submission
/// channel closes.
async fn worker_loop(inner: Arc<PoolInner>, worker_id: String, mut stop_rx: watch::Receiver<bool>) {
    let mut shutdown_rx = inner.shutdown.subscribe();

    loop {
        // Re-check both signals at the top of every iteration. This is what a
        // `watch` buys over a `Notify`: a worker that was busy executing a
        // task while the signal fired still sees it here.
        if *shutdown_rx.borrow_and_update() || *stop_rx.borrow_and_update() {
            debug!("Worker {} observed a stop signal", worker_id);
            break;
        }

        // About to block: the worker is idle from now until a task arrives.
        inner.set_state(&worker_id, WorkerState::Idle).await;

        let task = tokio::select! {
            biased;
            _ = shutdown_rx.changed() => {
                debug!("Worker {} received shutdown signal", worker_id);
                break;
            }
            _ = stop_rx.changed() => {
                debug!("Worker {} received stop signal", worker_id);
                break;
            }
            task = async { inner.task_rx.lock().await.recv().await } => task,
        };

        match task {
            Some(task_fn) => {
                debug!("Worker {} executing task", worker_id);
                inner.begin_task(&worker_id).await;
                task_fn().await;
                inner.finish_task(&worker_id).await;
            }
            None => {
                // Channel closed — pool is shutting down
                debug!("Worker {} task channel closed", worker_id);
                break;
            }
        }
    }

    inner.set_state(&worker_id, WorkerState::Stopped).await;
}

/// Worker pool manager
pub struct WorkerPool {
    inner: Arc<PoolInner>,
    /// Sender half of the bounded task channel — see [`WorkerPool::submit_task`].
    task_tx: mpsc::Sender<WorkerTaskFn>,
    /// Scaling monitor handle
    scaling_handle: RwLock<Option<JoinHandle<()>>>,
}

impl WorkerPool {
    /// Create a new worker pool
    ///
    /// # Errors
    ///
    /// Returns a message describing the first invalid configuration setting.
    pub fn new(config: WorkerPoolConfig) -> Result<Self, String> {
        config.validate()?;

        let (task_tx, task_rx) = mpsc::channel::<WorkerTaskFn>(config.max_queue_depth);
        let (shutdown, _) = watch::channel(false);

        Ok(Self {
            inner: Arc::new(PoolInner {
                config,
                workers: RwLock::new(HashMap::new()),
                handles: RwLock::new(HashMap::new()),
                stats: RwLock::new(WorkerPoolStats::new()),
                shutdown,
                queue_depth: Arc::new(AtomicUsize::new(0)),
                next_worker_id: AtomicUsize::new(0),
                total_tasks_processed: AtomicUsize::new(0),
                task_rx: Mutex::new(task_rx),
            }),
            task_tx,
            scaling_handle: RwLock::new(None),
        })
    }

    /// The pool's configuration.
    pub fn config(&self) -> &WorkerPoolConfig {
        &self.inner.config
    }

    /// Start the worker pool
    ///
    /// # Errors
    ///
    /// Returns an error if a worker could not be spawned.
    pub async fn start(&self) -> Result<(), String> {
        info!(
            "Starting worker pool with min={} max={} workers",
            self.inner.config.min_workers, self.inner.config.max_workers
        );

        // Spawn minimum workers
        for _ in 0..self.inner.config.min_workers {
            let worker_id = self.inner.next_worker_id();
            self.inner.spawn_worker(worker_id, Vec::new()).await?;
        }

        // Start scaling monitor
        self.start_scaling_monitor().await;

        info!(
            "Worker pool started with {} workers",
            self.inner.config.min_workers
        );
        Ok(())
    }

    /// Stop the worker pool, draining in-flight work first.
    ///
    /// Workers are signalled, then awaited for up to
    /// [`WorkerPoolConfig::shutdown_grace_period`] *in total*; whoever is
    /// still running a task when that budget is spent is aborted. Returns the
    /// number of workers that had to be aborted, which is a useful shutdown
    /// health signal (zero means everything drained cleanly).
    pub async fn stop(&self) -> usize {
        self.stop_with_grace(self.inner.config.shutdown_grace_period)
            .await
    }

    /// [`WorkerPool::stop`] with an explicit grace budget.
    pub async fn stop_with_grace(&self, grace: Duration) -> usize {
        info!("Stopping worker pool");
        // Retained by the watch channel, so even a worker in the middle of a
        // task sees this at its next loop iteration.
        let _ = self.inner.shutdown.send(true);

        // Stop scaling monitor
        if let Some(handle) = self.scaling_handle.write().await.take() {
            handle.abort();
        }

        // Take every handle out of the map first: awaiting while holding the
        // map would block `stop_worker`, `spawn_worker` and the health sweep.
        let handles: Vec<(String, WorkerHandle)> =
            self.inner.handles.write().await.drain().collect();

        {
            let mut workers = self.inner.workers.write().await;
            for info in workers.values_mut() {
                info.state = WorkerState::ShuttingDown;
            }
        }

        let deadline = Instant::now() + grace;
        let mut aborted = 0;
        for (worker_id, WorkerHandle { mut join, stop }) in handles {
            let _ = stop.send(true);
            let remaining = deadline.saturating_duration_since(Instant::now());
            match tokio::time::timeout(remaining, &mut join).await {
                Ok(Ok(())) => debug!("Worker {} drained cleanly", worker_id),
                Ok(Err(join_err)) => {
                    if !join_err.is_cancelled() {
                        warn!("Worker {} panicked: {}", worker_id, join_err);
                    }
                }
                Err(_) => {
                    warn!(
                        "Worker {} still busy after the {:?} shutdown grace period; aborting",
                        worker_id, grace
                    );
                    join.abort();
                    aborted += 1;
                }
            }
        }

        self.inner.workers.write().await.clear();

        if aborted > 0 {
            warn!("Worker pool stopped ({} workers aborted mid-task)", aborted);
        } else {
            info!("Worker pool stopped");
        }
        aborted
    }

    /// Get current worker count
    pub async fn worker_count(&self) -> usize {
        self.inner.worker_count().await
    }

    /// Get pool statistics
    pub async fn get_stats(&self) -> WorkerPoolStats {
        self.inner.snapshot_stats().await
    }

    /// Number of tasks waiting in the submission queue.
    pub fn pending_tasks(&self) -> usize {
        self.inner
            .config
            .max_queue_depth
            .saturating_sub(self.task_tx.capacity())
    }

    /// Update the externally-observed queue depth used by the autoscaler.
    ///
    /// The pool is broker-agnostic and cannot read the queue itself.  A
    /// broker-aware layer should call this on every poll cycle (or whenever
    /// the queue size changes significantly) so the `LoadBased` and
    /// `QueueBased` policies receive real signal instead of zeros.
    pub fn set_queue_depth(&self, depth: usize) {
        self.inner.queue_depth.store(depth, Ordering::Relaxed);
    }

    /// Return a shared handle to the queue-depth counter.
    ///
    /// Callers that already hold the `Arc` from the broker side can increment
    /// or overwrite the counter without going through `set_queue_depth`.
    pub fn queue_depth_handle(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.inner.queue_depth)
    }

    /// Submit a task closure for execution by the next available worker.
    ///
    /// Non-blocking: returns `Err` when the pool has shut down *or* when the
    /// submission queue is full. A full queue is real backpressure — the
    /// caller decides whether to retry, shed the task or slow down, instead of
    /// the pool growing an unbounded heap of boxed futures.
    ///
    /// # Errors
    ///
    /// Returns a message describing whether the queue was full or the pool had
    /// shut down.
    pub fn submit_task(&self, task: WorkerTaskFn) -> Result<(), String> {
        self.task_tx.try_send(task).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => format!(
                "Worker pool queue is full ({} tasks pending)",
                self.inner.config.max_queue_depth
            ),
            mpsc::error::TrySendError::Closed(_) => "Worker pool is shut down".to_string(),
        })
    }

    /// Submit a task, waiting for queue space instead of failing.
    ///
    /// # Errors
    ///
    /// Returns an error only if the pool has shut down.
    pub async fn submit_task_async(&self, task: WorkerTaskFn) -> Result<(), String> {
        self.task_tx
            .send(task)
            .await
            .map_err(|_| "Worker pool is shut down".to_string())
    }

    /// Make a scaling decision based on the current policy.
    ///
    /// Exposed so an application driving [`ScalingPolicy::Manual`] can reuse
    /// the same policy evaluation the built-in monitor uses. `cpu_usage` and
    /// `memory_usage` are percentages, or `None` when unavailable — see the
    /// module docs for why the distinction matters.
    pub fn make_scaling_decision(
        &self,
        current_workers: usize,
        queue_depth: usize,
        cpu_usage: Option<f64>,
        memory_usage: Option<f64>,
    ) -> ScalingDecision {
        compute_scaling_decision(
            &self.inner.config.scaling_policy,
            current_workers,
            queue_depth,
            cpu_usage,
            memory_usage,
            self.inner.config.min_workers,
            self.inner.config.max_workers,
        )
    }

    /// Execute a scaling decision, returning how many workers actually changed.
    ///
    /// The returned count can be lower than the requested one: a scale-down
    /// only retires workers that have genuinely been idle for
    /// [`WorkerPoolConfig::worker_idle_timeout`], and reports `0` when there
    /// are none rather than pretending an event happened.
    ///
    /// # Errors
    ///
    /// Returns an error if a worker could not be spawned.
    pub async fn execute_scaling(&self, decision: ScalingDecision) -> Result<usize, String> {
        self.inner.execute_scaling(decision).await
    }

    /// Start the scaling monitor
    async fn start_scaling_monitor(&self) {
        let inner = Arc::clone(&self.inner);

        let handle = tokio::spawn(async move {
            let mut scaling_interval = tokio::time::interval(inner.config.scaling_interval);
            let mut health_interval = tokio::time::interval(inner.config.health_check_interval);
            let mut shutdown_rx = inner.shutdown.subscribe();
            let mut last_scaling = Instant::now();
            let mut cpu_sampler = CpuSampler::default();

            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        debug!("Scaling monitor received shutdown signal");
                        break;
                    }
                    _ = health_interval.tick() => {
                        let replaced = inner.health_sweep().await;
                        if replaced > 0 {
                            info!("Health sweep replaced {} dead worker(s)", replaced);
                        }
                    }
                    _ = scaling_interval.tick() => {
                        // Never scale a pool that is being torn down: a
                        // scale-up racing `stop()` would spawn a worker into a
                        // pool whose handles have already been drained,
                        // leaving a live task behind after shutdown returns.
                        if *shutdown_rx.borrow() {
                            break;
                        }

                        // Check if we're in cooldown period
                        if last_scaling.elapsed() < inner.config.scaling_cooldown {
                            continue;
                        }

                        let current_workers = inner.worker_count().await;

                        // Real queue depth — set externally by the broker layer.
                        let queue_depth = inner.queue_depth.load(Ordering::Relaxed);
                        let cpu_usage = cpu_sampler.sample();
                        let memory_usage = read_memory_utilization();

                        let decision = compute_scaling_decision(
                            &inner.config.scaling_policy,
                            current_workers,
                            queue_depth,
                            cpu_usage,
                            memory_usage,
                            inner.config.min_workers,
                            inner.config.max_workers,
                        );

                        if decision != ScalingDecision::None {
                            match inner.execute_scaling(decision).await {
                                // Only a scaling action that actually happened
                                // consumes the cooldown.
                                Ok(changed) if changed > 0 => last_scaling = Instant::now(),
                                Ok(_) => {}
                                Err(e) => error!("Failed to execute scaling decision: {}", e),
                            }
                        }
                    }
                }
            }
        });

        *self.scaling_handle.write().await = Some(handle);
    }
}

/// Delta-sampled process CPU utilisation, with a host load-average fallback.
#[derive(Default)]
struct CpuSampler {
    previous: Option<(Duration, Instant)>,
}

impl CpuSampler {
    /// Utilisation percentage since the previous sample, or `None` when it
    /// cannot be measured.
    ///
    /// The first call establishes the baseline and deliberately reports
    /// `None`: a delta needs two samples, and reporting `0.0` for "I have not
    /// measured yet" is exactly the mistake that makes an autoscaler shrink a
    /// busy pool on its first tick.
    fn sample(&mut self) -> Option<f64> {
        let now = Instant::now();
        match crate::sysinfo::read_process_cpu_time() {
            Some(current_cpu) => {
                let pct = self.previous.and_then(|(prev_cpu, prev_time)| {
                    let cpu_delta = current_cpu.saturating_sub(prev_cpu).as_secs_f64();
                    let wall_delta = now.duration_since(prev_time).as_secs_f64();
                    (wall_delta > 0.0).then(|| (cpu_delta / wall_delta) * 100.0)
                });
                self.previous = Some((current_cpu, now));
                pct.or_else(load_average_utilization)
            }
            None => {
                self.previous = None;
                load_average_utilization()
            }
        }
    }
}

/// Host-wide 1-minute load average as a percentage of available CPUs.
///
/// A coarser signal than the process CPU delta — it measures the whole host,
/// including neighbours — but a real one, and it is available on the first
/// tick. Used only as a fallback.
fn load_average_utilization() -> Option<f64> {
    let load = crate::sysinfo::read_load_average()?;
    let cpus = num_cpus::get().max(1) as f64;
    Some((load[0] / cpus) * 100.0)
}

/// Process memory as a percentage of the memory available to it, or `None`
/// when either reading is unavailable.
fn read_memory_utilization() -> Option<f64> {
    let proc_bytes = crate::sysinfo::read_process_memory_bytes();
    let total_bytes = crate::sysinfo::read_total_memory_bytes();
    if proc_bytes == 0 || total_bytes == 0 {
        return None;
    }
    Some((proc_bytes as f64 / total_bytes as f64) * 100.0)
}

/// Compute a scaling decision from the given policy and live metrics.
///
/// This is a pure function (no I/O, no async) so it is easy to unit-test.
///
/// # Load signals
///
/// `cpu_usage` and `memory_usage` are `Option`al and `None` means
/// **unavailable**, not idle. An unavailable signal can never make
/// `load_high` or `load_low` true, so it can neither trigger a scale-up nor
/// authorise a scale-down. `QueueBased` ignores the load signals entirely and
/// keeps working on platforms where they cannot be read.
///
/// # Load-based scaling heuristics
///
/// - **`LoadBased`**: Scale up when either CPU or memory utilisation exceeds
///   the configured target.  Scale down when both are below 50 % of their
///   respective targets (hysteresis avoids thrashing).
/// - **`Hybrid`**: Scale up when the queue is deep *or* load is high; scale
///   down only when the queue is shallow *and* load is measurably low.
fn compute_scaling_decision(
    policy: &ScalingPolicy,
    current_workers: usize,
    queue_depth: usize,
    cpu_usage: Option<f64>,
    memory_usage: Option<f64>,
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
            let needed_workers = queue_depth.div_ceil((*tasks_per_worker).max(1));
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
            let cpu_high = cpu_usage.is_some_and(|cpu| cpu > *target_cpu_utilization);
            let mem_high = memory_usage.is_some_and(|mem| mem > *target_memory_utilization);
            // 50% hysteresis band: scale down only when both metrics are
            // *measured* and well below target.
            let cpu_low = cpu_usage.is_some_and(|cpu| cpu < target_cpu_utilization * 0.5);
            let mem_low = memory_usage.is_some_and(|mem| mem < target_memory_utilization * 0.5);

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
            let load_high = cpu_usage.is_some_and(|cpu| cpu > *max_cpu_utilization)
                || memory_usage.is_some_and(|mem| mem > *max_memory_utilization);
            let load_low = cpu_usage.is_some_and(|cpu| cpu < max_cpu_utilization * 0.5)
                && memory_usage.is_some_and(|mem| mem < max_memory_utilization * 0.5);

            if (queue_high || load_high) && current_workers < max_workers {
                let needed_workers = queue_depth.div_ceil((*tasks_per_worker).max(1));
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
mod tests;
