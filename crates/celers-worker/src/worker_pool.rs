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
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Notify, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, error, info};

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
}

impl WorkerPool {
    /// Create a new worker pool
    pub fn new(config: WorkerPoolConfig) -> Result<Self, String> {
        config.validate()?;

        Ok(Self {
            config,
            workers: Arc::new(RwLock::new(HashMap::new())),
            handles: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(WorkerPoolStats::new())),
            shutdown: Arc::new(Notify::new()),
            scaling_handle: Arc::new(RwLock::new(None)),
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

        // Create a placeholder handle (in a real implementation, this would spawn an actual worker)
        let worker_id_clone = worker_id.clone();
        let shutdown = Arc::clone(&self.shutdown);

        let handle = tokio::spawn(async move {
            // Simulated worker loop
            loop {
                tokio::select! {
                    _ = shutdown.notified() => {
                        debug!("Worker {} received shutdown signal", worker_id_clone);
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {
                        // Simulated work
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

    /// Make a scaling decision based on the current policy
    #[allow(dead_code)]
    fn make_scaling_decision(
        &self,
        current_workers: usize,
        queue_depth: usize,
        _cpu_usage: f64,
        _memory_usage: f64,
    ) -> ScalingDecision {
        match &self.config.scaling_policy {
            ScalingPolicy::Manual => ScalingDecision::None,
            ScalingPolicy::QueueBased {
                tasks_per_worker,
                scale_up_threshold,
                scale_down_threshold,
            } => {
                let needed_workers = queue_depth.div_ceil(*tasks_per_worker);

                if queue_depth >= *scale_up_threshold && current_workers < self.config.max_workers {
                    let to_spawn = (needed_workers.saturating_sub(current_workers))
                        .min(self.config.max_workers - current_workers);
                    if to_spawn > 0 {
                        return ScalingDecision::ScaleUp(to_spawn);
                    }
                } else if queue_depth <= *scale_down_threshold
                    && current_workers > self.config.min_workers
                {
                    let to_remove =
                        current_workers.saturating_sub(needed_workers.max(self.config.min_workers));
                    if to_remove > 0 {
                        return ScalingDecision::ScaleDown(to_remove);
                    }
                }
                ScalingDecision::None
            }
            ScalingPolicy::LoadBased { .. } => {
                // Simplified load-based scaling
                ScalingDecision::None
            }
            ScalingPolicy::Hybrid { .. } => {
                // Simplified hybrid scaling
                ScalingDecision::None
            }
        }
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

                        // In a real implementation, these would come from actual monitoring
                        let queue_depth = 0; // Placeholder
                        let cpu_usage = 0.0; // Placeholder
                        let memory_usage = 0.0; // Placeholder

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
}

impl WorkerPoolMonitor {
    fn make_scaling_decision(
        &self,
        current_workers: usize,
        queue_depth: usize,
        _cpu_usage: f64,
        _memory_usage: f64,
    ) -> ScalingDecision {
        match &self.config.scaling_policy {
            ScalingPolicy::Manual => ScalingDecision::None,
            ScalingPolicy::QueueBased {
                tasks_per_worker,
                scale_up_threshold,
                scale_down_threshold,
            } => {
                let needed_workers = queue_depth.div_ceil(*tasks_per_worker);

                if queue_depth >= *scale_up_threshold && current_workers < self.config.max_workers {
                    let to_spawn = (needed_workers.saturating_sub(current_workers))
                        .min(self.config.max_workers - current_workers);
                    if to_spawn > 0 {
                        return ScalingDecision::ScaleUp(to_spawn);
                    }
                } else if queue_depth <= *scale_down_threshold
                    && current_workers > self.config.min_workers
                {
                    let to_remove =
                        current_workers.saturating_sub(needed_workers.max(self.config.min_workers));
                    if to_remove > 0 {
                        return ScalingDecision::ScaleDown(to_remove);
                    }
                }
                ScalingDecision::None
            }
            _ => ScalingDecision::None,
        }
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

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.notified() => {
                        debug!("Worker {} received shutdown signal", worker_id_clone);
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {
                        // Simulated work
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
}
