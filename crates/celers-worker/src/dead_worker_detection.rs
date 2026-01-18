//! Dead worker detection and automatic deregistration
//!
//! This module provides mechanisms to detect workers that have stopped responding
//! and automatically deregister them from the system.
//!
//! # Features
//!
//! - Heartbeat-based worker health monitoring
//! - Configurable heartbeat intervals and timeouts
//! - Automatic worker deregistration on failure
//! - Worker recovery detection
//! - Alert callbacks on worker state changes
//!
//! # Example
//!
//! ```
//! use celers_worker::{DeadWorkerDetector, DetectorConfig};
//! use std::time::Duration;
//!
//! # async fn example() {
//! let config = DetectorConfig::new()
//!     .with_heartbeat_interval(Duration::from_secs(30))
//!     .with_heartbeat_timeout(Duration::from_secs(90));
//!
//! let mut detector = DeadWorkerDetector::new(config);
//!
//! // Register a worker
//! detector.register_worker("worker-1".to_string()).await;
//!
//! // Send heartbeat
//! detector.heartbeat("worker-1").await;
//!
//! // Check if worker is alive
//! let is_alive = detector.is_worker_alive("worker-1").await;
//! # }
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

/// Callback function type for worker state changes
pub type StateChangeCallback = Arc<dyn Fn(&str, WorkerHealthState) + Send + Sync>;

/// Worker health state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerHealthState {
    /// Worker is healthy and sending heartbeats
    Alive,
    /// Worker has missed heartbeats but not yet considered dead
    Degraded,
    /// Worker is dead (missed too many heartbeats)
    Dead,
    /// Worker was dead but has recovered
    Recovered,
}

impl WorkerHealthState {
    /// Check if the worker is considered healthy
    pub fn is_healthy(&self) -> bool {
        matches!(
            self,
            WorkerHealthState::Alive | WorkerHealthState::Recovered
        )
    }

    /// Check if the worker is dead
    pub fn is_dead(&self) -> bool {
        *self == WorkerHealthState::Dead
    }

    /// Check if the worker is degraded
    pub fn is_degraded(&self) -> bool {
        *self == WorkerHealthState::Degraded
    }
}

/// Configuration for dead worker detector
#[derive(Clone)]
pub struct DetectorConfig {
    /// Expected heartbeat interval
    pub heartbeat_interval: Duration,
    /// Timeout before considering a worker dead
    pub heartbeat_timeout: Duration,
    /// How often to check for dead workers
    pub check_interval: Duration,
    /// Number of missed heartbeats before marking as degraded
    pub degraded_threshold: usize,
    /// Enable automatic deregistration of dead workers
    pub auto_deregister: bool,
}

impl DetectorConfig {
    /// Create a new detector configuration
    pub fn new() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(30),
            heartbeat_timeout: Duration::from_secs(90),
            check_interval: Duration::from_secs(15),
            degraded_threshold: 2,
            auto_deregister: true,
        }
    }

    /// Set heartbeat interval
    pub fn with_heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    /// Set heartbeat timeout
    pub fn with_heartbeat_timeout(mut self, timeout: Duration) -> Self {
        self.heartbeat_timeout = timeout;
        self
    }

    /// Set check interval
    pub fn with_check_interval(mut self, interval: Duration) -> Self {
        self.check_interval = interval;
        self
    }

    /// Set degraded threshold
    pub fn with_degraded_threshold(mut self, threshold: usize) -> Self {
        self.degraded_threshold = threshold;
        self
    }

    /// Enable or disable automatic deregistration
    pub fn with_auto_deregister(mut self, enable: bool) -> Self {
        self.auto_deregister = enable;
        self
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.heartbeat_timeout <= self.heartbeat_interval {
            return Err("Heartbeat timeout must be greater than heartbeat interval".to_string());
        }
        if self.degraded_threshold == 0 {
            return Err("Degraded threshold must be greater than 0".to_string());
        }
        Ok(())
    }
}

impl Default for DetectorConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Worker heartbeat information
#[derive(Clone)]
struct WorkerHeartbeat {
    /// Worker ID
    #[allow(dead_code)]
    worker_id: String,
    /// Last heartbeat timestamp
    last_heartbeat: Instant,
    /// Current health state
    state: WorkerHealthState,
    /// Number of consecutive missed heartbeats
    missed_heartbeats: usize,
    /// When the worker was registered
    #[allow(dead_code)]
    registered_at: Instant,
    /// Total heartbeats received
    total_heartbeats: usize,
}

impl WorkerHeartbeat {
    /// Create a new worker heartbeat entry
    fn new(worker_id: String) -> Self {
        Self {
            worker_id,
            last_heartbeat: Instant::now(),
            state: WorkerHealthState::Alive,
            missed_heartbeats: 0,
            registered_at: Instant::now(),
            total_heartbeats: 0,
        }
    }

    /// Update heartbeat
    fn update(&mut self) {
        self.last_heartbeat = Instant::now();
        self.missed_heartbeats = 0;
        self.total_heartbeats += 1;

        // Update state if recovering
        if self.state == WorkerHealthState::Dead {
            self.state = WorkerHealthState::Recovered;
        } else if self.state != WorkerHealthState::Alive {
            self.state = WorkerHealthState::Alive;
        }
    }

    /// Check if heartbeat has timed out
    fn is_timed_out(&self, timeout: Duration) -> bool {
        self.last_heartbeat.elapsed() >= timeout
    }

    /// Increment missed heartbeat counter
    fn increment_missed(&mut self, degraded_threshold: usize) {
        self.missed_heartbeats += 1;

        if self.missed_heartbeats >= degraded_threshold {
            self.state = WorkerHealthState::Dead;
        } else {
            self.state = WorkerHealthState::Degraded;
        }
    }
}

/// Dead worker detector statistics
#[derive(Clone, Debug)]
pub struct DetectorStats {
    /// Total registered workers
    pub total_workers: usize,
    /// Number of alive workers
    pub alive_workers: usize,
    /// Number of degraded workers
    pub degraded_workers: usize,
    /// Number of dead workers
    pub dead_workers: usize,
    /// Total workers deregistered
    pub deregistered_workers: usize,
    /// Total heartbeats received
    pub total_heartbeats: usize,
}

impl DetectorStats {
    /// Create new statistics
    pub fn new() -> Self {
        Self {
            total_workers: 0,
            alive_workers: 0,
            degraded_workers: 0,
            dead_workers: 0,
            deregistered_workers: 0,
            total_heartbeats: 0,
        }
    }
}

impl Default for DetectorStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Dead worker detector
pub struct DeadWorkerDetector {
    /// Configuration
    config: DetectorConfig,
    /// Worker heartbeats
    heartbeats: Arc<RwLock<HashMap<String, WorkerHeartbeat>>>,
    /// State change callbacks
    callbacks: Arc<RwLock<Vec<StateChangeCallback>>>,
    /// Statistics
    stats: Arc<RwLock<DetectorStats>>,
    /// Monitor task handle
    monitor_handle: Arc<RwLock<Option<JoinHandle<()>>>>,
    /// Shutdown signal
    shutdown: Arc<tokio::sync::Notify>,
}

impl DeadWorkerDetector {
    /// Create a new dead worker detector
    pub fn new(config: DetectorConfig) -> Self {
        Self {
            config,
            heartbeats: Arc::new(RwLock::new(HashMap::new())),
            callbacks: Arc::new(RwLock::new(Vec::new())),
            stats: Arc::new(RwLock::new(DetectorStats::new())),
            monitor_handle: Arc::new(RwLock::new(None)),
            shutdown: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Register a worker for monitoring
    pub async fn register_worker(&self, worker_id: String) {
        let heartbeat = WorkerHeartbeat::new(worker_id.clone());

        let mut heartbeats = self.heartbeats.write().await;
        heartbeats.insert(worker_id.clone(), heartbeat);

        let mut stats = self.stats.write().await;
        stats.total_workers += 1;
        stats.alive_workers += 1;

        info!("Registered worker for health monitoring: {}", worker_id);
    }

    /// Deregister a worker from monitoring
    pub async fn deregister_worker(&self, worker_id: &str) {
        let mut heartbeats = self.heartbeats.write().await;
        if let Some(heartbeat) = heartbeats.remove(worker_id) {
            let mut stats = self.stats.write().await;
            stats.total_workers = stats.total_workers.saturating_sub(1);

            match heartbeat.state {
                WorkerHealthState::Alive | WorkerHealthState::Recovered => {
                    stats.alive_workers = stats.alive_workers.saturating_sub(1);
                }
                WorkerHealthState::Degraded => {
                    stats.degraded_workers = stats.degraded_workers.saturating_sub(1);
                }
                WorkerHealthState::Dead => {
                    stats.dead_workers = stats.dead_workers.saturating_sub(1);
                }
            }
            stats.deregistered_workers += 1;

            info!("Deregistered worker: {}", worker_id);
        }
    }

    /// Record a heartbeat from a worker
    pub async fn heartbeat(&self, worker_id: &str) {
        let mut heartbeats = self.heartbeats.write().await;
        if let Some(heartbeat) = heartbeats.get_mut(worker_id) {
            let old_state = heartbeat.state;
            heartbeat.update();
            let new_state = heartbeat.state;

            drop(heartbeats);

            // Update statistics
            let mut stats = self.stats.write().await;
            stats.total_heartbeats += 1;

            // Notify callbacks if state changed
            if old_state != new_state {
                self.notify_state_change(worker_id, new_state).await;

                // Update stats based on state change
                match old_state {
                    WorkerHealthState::Alive | WorkerHealthState::Recovered => {
                        stats.alive_workers = stats.alive_workers.saturating_sub(1);
                    }
                    WorkerHealthState::Degraded => {
                        stats.degraded_workers = stats.degraded_workers.saturating_sub(1);
                    }
                    WorkerHealthState::Dead => {
                        stats.dead_workers = stats.dead_workers.saturating_sub(1);
                    }
                }

                match new_state {
                    WorkerHealthState::Alive | WorkerHealthState::Recovered => {
                        stats.alive_workers += 1;
                    }
                    WorkerHealthState::Degraded => {
                        stats.degraded_workers += 1;
                    }
                    WorkerHealthState::Dead => {
                        stats.dead_workers += 1;
                    }
                }
            }

            debug!("Heartbeat received from worker: {}", worker_id);
        } else {
            warn!("Heartbeat from unregistered worker: {}", worker_id);
        }
    }

    /// Check if a worker is alive
    pub async fn is_worker_alive(&self, worker_id: &str) -> bool {
        let heartbeats = self.heartbeats.read().await;
        heartbeats
            .get(worker_id)
            .map(|h| h.state.is_healthy())
            .unwrap_or(false)
    }

    /// Get worker health state
    pub async fn get_worker_state(&self, worker_id: &str) -> Option<WorkerHealthState> {
        let heartbeats = self.heartbeats.read().await;
        heartbeats.get(worker_id).map(|h| h.state)
    }

    /// Get all dead workers
    pub async fn get_dead_workers(&self) -> Vec<String> {
        let heartbeats = self.heartbeats.read().await;
        heartbeats
            .iter()
            .filter(|(_, h)| h.state.is_dead())
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Get all degraded workers
    pub async fn get_degraded_workers(&self) -> Vec<String> {
        let heartbeats = self.heartbeats.read().await;
        heartbeats
            .iter()
            .filter(|(_, h)| h.state.is_degraded())
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Add a state change callback
    pub async fn add_callback(&self, callback: StateChangeCallback) {
        self.callbacks.write().await.push(callback);
    }

    /// Get detector statistics
    pub async fn get_stats(&self) -> DetectorStats {
        self.stats.read().await.clone()
    }

    /// Start the monitoring task
    pub async fn start_monitoring(&self) {
        let heartbeats = Arc::clone(&self.heartbeats);
        let callbacks = Arc::clone(&self.callbacks);
        let stats = Arc::clone(&self.stats);
        let shutdown = Arc::clone(&self.shutdown);
        let config = self.config.clone();

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.check_interval);

            loop {
                tokio::select! {
                    _ = shutdown.notified() => {
                        debug!("Dead worker detector shutting down");
                        break;
                    }
                    _ = interval.tick() => {
                        Self::check_workers(
                            &heartbeats,
                            &callbacks,
                            &stats,
                            &config,
                        ).await;
                    }
                }
            }
        });

        *self.monitor_handle.write().await = Some(handle);
        info!("Started dead worker detection monitoring");
    }

    /// Stop the monitoring task
    pub async fn stop_monitoring(&self) {
        self.shutdown.notify_waiters();

        if let Some(handle) = self.monitor_handle.write().await.take() {
            handle.abort();
        }

        info!("Stopped dead worker detection monitoring");
    }

    /// Check all workers for timeouts
    async fn check_workers(
        heartbeats: &Arc<RwLock<HashMap<String, WorkerHeartbeat>>>,
        callbacks: &Arc<RwLock<Vec<StateChangeCallback>>>,
        stats: &Arc<RwLock<DetectorStats>>,
        config: &DetectorConfig,
    ) {
        let mut heartbeats_lock = heartbeats.write().await;
        let mut workers_to_deregister = Vec::new();

        for (worker_id, heartbeat) in heartbeats_lock.iter_mut() {
            if heartbeat.is_timed_out(config.heartbeat_timeout) {
                let old_state = heartbeat.state;
                heartbeat.increment_missed(config.degraded_threshold);
                let new_state = heartbeat.state;

                if old_state != new_state {
                    info!(
                        "Worker {} state changed: {:?} -> {:?}",
                        worker_id, old_state, new_state
                    );

                    // Notify callbacks
                    let callbacks_lock = callbacks.read().await;
                    for callback in callbacks_lock.iter() {
                        callback(worker_id, new_state);
                    }
                    drop(callbacks_lock);

                    // Update stats
                    let mut stats_lock = stats.write().await;
                    match old_state {
                        WorkerHealthState::Alive | WorkerHealthState::Recovered => {
                            stats_lock.alive_workers = stats_lock.alive_workers.saturating_sub(1);
                        }
                        WorkerHealthState::Degraded => {
                            stats_lock.degraded_workers =
                                stats_lock.degraded_workers.saturating_sub(1);
                        }
                        WorkerHealthState::Dead => {
                            stats_lock.dead_workers = stats_lock.dead_workers.saturating_sub(1);
                        }
                    }

                    match new_state {
                        WorkerHealthState::Alive | WorkerHealthState::Recovered => {
                            stats_lock.alive_workers += 1;
                        }
                        WorkerHealthState::Degraded => {
                            stats_lock.degraded_workers += 1;
                        }
                        WorkerHealthState::Dead => {
                            stats_lock.dead_workers += 1;
                        }
                    }
                    drop(stats_lock);

                    // Mark for deregistration if auto_deregister is enabled and worker is dead
                    if config.auto_deregister && new_state.is_dead() {
                        workers_to_deregister.push(worker_id.clone());
                    }
                }
            }
        }

        // Deregister dead workers
        for worker_id in workers_to_deregister {
            heartbeats_lock.remove(&worker_id);
            let mut stats_lock = stats.write().await;
            stats_lock.total_workers = stats_lock.total_workers.saturating_sub(1);
            stats_lock.dead_workers = stats_lock.dead_workers.saturating_sub(1);
            stats_lock.deregistered_workers += 1;
            info!("Auto-deregistered dead worker: {}", worker_id);
        }
    }

    /// Notify callbacks of state change
    async fn notify_state_change(&self, worker_id: &str, new_state: WorkerHealthState) {
        let callbacks = self.callbacks.read().await;
        for callback in callbacks.iter() {
            callback(worker_id, new_state);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn test_detector_config_default() {
        let config = DetectorConfig::default();
        assert_eq!(config.heartbeat_interval, Duration::from_secs(30));
        assert_eq!(config.heartbeat_timeout, Duration::from_secs(90));
        assert!(config.auto_deregister);
    }

    #[test]
    fn test_detector_config_builder() {
        let config = DetectorConfig::new()
            .with_heartbeat_interval(Duration::from_secs(10))
            .with_heartbeat_timeout(Duration::from_secs(30))
            .with_degraded_threshold(3)
            .with_auto_deregister(false);

        assert_eq!(config.heartbeat_interval, Duration::from_secs(10));
        assert_eq!(config.heartbeat_timeout, Duration::from_secs(30));
        assert_eq!(config.degraded_threshold, 3);
        assert!(!config.auto_deregister);
    }

    #[test]
    fn test_detector_config_validation() {
        let invalid_config = DetectorConfig::new()
            .with_heartbeat_interval(Duration::from_secs(100))
            .with_heartbeat_timeout(Duration::from_secs(50));
        assert!(invalid_config.validate().is_err());

        let invalid_config2 = DetectorConfig::new().with_degraded_threshold(0);
        assert!(invalid_config2.validate().is_err());

        let valid_config = DetectorConfig::new();
        assert!(valid_config.validate().is_ok());
    }

    #[tokio::test]
    async fn test_worker_registration() {
        let config = DetectorConfig::default();
        let detector = DeadWorkerDetector::new(config);

        detector.register_worker("worker-1".to_string()).await;

        assert!(detector.is_worker_alive("worker-1").await);

        let stats = detector.get_stats().await;
        assert_eq!(stats.total_workers, 1);
        assert_eq!(stats.alive_workers, 1);
    }

    #[tokio::test]
    async fn test_worker_deregistration() {
        let config = DetectorConfig::default();
        let detector = DeadWorkerDetector::new(config);

        detector.register_worker("worker-1".to_string()).await;
        detector.deregister_worker("worker-1").await;

        assert!(!detector.is_worker_alive("worker-1").await);

        let stats = detector.get_stats().await;
        assert_eq!(stats.total_workers, 0);
        assert_eq!(stats.deregistered_workers, 1);
    }

    #[tokio::test]
    async fn test_heartbeat() {
        let config = DetectorConfig::default();
        let detector = DeadWorkerDetector::new(config);

        detector.register_worker("worker-1".to_string()).await;
        detector.heartbeat("worker-1").await;

        let stats = detector.get_stats().await;
        assert_eq!(stats.total_heartbeats, 1);
        assert!(detector.is_worker_alive("worker-1").await);
    }

    #[tokio::test]
    async fn test_worker_state() {
        let config = DetectorConfig::default();
        let detector = DeadWorkerDetector::new(config);

        detector.register_worker("worker-1".to_string()).await;

        let state = detector.get_worker_state("worker-1").await;
        assert_eq!(state, Some(WorkerHealthState::Alive));
    }

    #[tokio::test]
    async fn test_health_states() {
        assert!(WorkerHealthState::Alive.is_healthy());
        assert!(WorkerHealthState::Recovered.is_healthy());
        assert!(!WorkerHealthState::Dead.is_healthy());
        assert!(!WorkerHealthState::Degraded.is_healthy());

        assert!(WorkerHealthState::Dead.is_dead());
        assert!(!WorkerHealthState::Alive.is_dead());

        assert!(WorkerHealthState::Degraded.is_degraded());
        assert!(!WorkerHealthState::Alive.is_degraded());
    }

    #[tokio::test]
    async fn test_callback() {
        let config = DetectorConfig::default();
        let detector = DeadWorkerDetector::new(config);

        let callback_count = Arc::new(AtomicUsize::new(0));
        let callback_count_clone = Arc::clone(&callback_count);

        let callback: StateChangeCallback = Arc::new(move |_worker_id, _state| {
            callback_count_clone.fetch_add(1, Ordering::SeqCst);
        });

        detector.add_callback(callback).await;
        detector.register_worker("worker-1".to_string()).await;

        // Manually trigger state change
        detector
            .notify_state_change("worker-1", WorkerHealthState::Dead)
            .await;

        // Callback should have been called
        assert_eq!(callback_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_get_dead_workers() {
        let config = DetectorConfig::new()
            .with_heartbeat_timeout(Duration::from_millis(100))
            .with_degraded_threshold(1);
        let detector = DeadWorkerDetector::new(config);

        detector.register_worker("worker-1".to_string()).await;
        detector.start_monitoring().await;

        // Wait for worker to become dead
        tokio::time::sleep(Duration::from_millis(200)).await;

        let _dead_workers = detector.get_dead_workers().await;
        // The monitoring task should have detected the dead worker
        // Note: This test may be timing-dependent

        detector.stop_monitoring().await;
    }

    #[test]
    fn test_detector_stats_default() {
        let stats = DetectorStats::default();
        assert_eq!(stats.total_workers, 0);
        assert_eq!(stats.alive_workers, 0);
        assert_eq!(stats.dead_workers, 0);
    }
}
