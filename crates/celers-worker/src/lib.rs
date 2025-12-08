//! Worker runtime for CeleRS
//!
//! The worker module provides a high-performance, production-ready task execution engine
//! with advanced features including:
//!
//! - **Batch Task Processing**: Dequeue and process multiple tasks in parallel using
//!   `enable_batch_dequeue` and `batch_size` configuration options. Reduces broker
//!   round-trips and improves throughput for high-volume workloads.
//!
//! - **Circuit Breaker**: Automatically isolate failing task types to prevent cascading
//!   failures. Configure threshold, recovery timeout, and time window.
//!
//! - **Health Checks**: Built-in liveness and readiness probes for Kubernetes deployments.
//!   Track uptime, task success/failure rates, and consecutive failures.
//!
//! - **Graceful Shutdown**: Respond to SIGTERM/SIGINT signals and complete in-flight
//!   tasks before exiting.
//!
//! - **Retry Logic**: Exponential backoff with configurable max retries and delay bounds.
//!
//! - **Metrics Integration**: Optional Prometheus metrics for task execution time,
//!   completion, failures, and retries (requires `metrics` feature).
//!
//! - **Memory Tracking**: Monitor and limit task result sizes to prevent memory issues.
//!
//! # Configuration Example
//!
//! ```no_run
//! use celers_worker::{Worker, WorkerConfig};
//! use celers_core::TaskRegistry;
//!
//! # async fn example() {
//! let config = WorkerConfig {
//!     concurrency: 10,
//!     enable_batch_dequeue: true,  // Enable batch processing
//!     batch_size: 10,               // Fetch up to 10 tasks at once
//!     enable_circuit_breaker: true,
//!     ..Default::default()
//! };
//! # }
//! ```

pub mod cancellation;
pub mod circuit_breaker;
pub mod health;
pub mod memory;
pub mod middleware;
pub mod performance_metrics;
pub mod prefetch;
pub mod queue_monitor;
pub mod rate_limit;
pub mod resource_tracker;
pub mod shutdown;

// Internal metrics wrapper module
#[cfg(feature = "metrics")]
mod metrics {
    pub use celers_metrics::*;
}

#[cfg(not(feature = "metrics"))]
#[allow(dead_code)]
mod metrics {
    // No-op stubs when metrics feature is disabled
}

#[cfg(feature = "canvas")]
pub mod workflows;

use celers_core::{
    Broker, Event, EventEmitter, NoOpEventEmitter, Result, TaskEventBuilder, TaskRegistry,
    TaskState, WorkerEventBuilder,
};

use circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Duration, Instant};
use tracing::{debug, error, info, warn};

pub use cancellation::{CancellationError, CancellationRegistry, CancellationToken};
pub use circuit_breaker::{CircuitBreaker as WorkerCircuitBreaker, CircuitState};
pub use performance_metrics::{PerformanceConfig, PerformanceStats, PerformanceTracker};
pub use prefetch::{PrefetchBuffer, PrefetchConfig, PrefetchStats};
pub use queue_monitor::{QueueAlertLevel, QueueMonitor, QueueMonitorConfig, QueueStats};
pub use rate_limit::{RateLimitConfig, RateLimiter};
pub use resource_tracker::{ResourceLimits, ResourceStats, ResourceTracker};
pub use shutdown::wait_for_signal;

#[cfg(feature = "metrics")]
use celers_metrics::{
    TASKS_COMPLETED_BY_TYPE, TASKS_COMPLETED_TOTAL, TASKS_FAILED_BY_TYPE, TASKS_FAILED_TOTAL,
    TASKS_RETRIED_BY_TYPE, TASKS_RETRIED_TOTAL, TASK_EXECUTION_TIME, TASK_EXECUTION_TIME_BY_TYPE,
};

/// Worker configuration
#[derive(Clone)]
pub struct WorkerConfig {
    /// Number of concurrent tasks to process
    pub concurrency: usize,

    /// Polling interval when queue is empty (milliseconds)
    pub poll_interval_ms: u64,

    /// Enable graceful shutdown
    pub graceful_shutdown: bool,

    /// Maximum number of retry attempts
    pub max_retries: u32,

    /// Base delay for exponential backoff (milliseconds)
    pub retry_base_delay_ms: u64,

    /// Maximum delay between retries (milliseconds)
    pub retry_max_delay_ms: u64,

    /// Default task timeout in seconds
    pub default_timeout_secs: u64,

    // Memory optimization options
    /// Enable batch dequeue for better throughput
    pub enable_batch_dequeue: bool,

    /// Number of tasks to fetch per batch (when batch dequeue enabled)
    pub batch_size: usize,

    /// Maximum task result size in bytes (0 = unlimited)
    pub max_result_size_bytes: usize,

    /// Enable memory usage tracking and reporting
    pub track_memory_usage: bool,

    // Circuit breaker options
    /// Enable circuit breaker for failing tasks
    pub enable_circuit_breaker: bool,

    /// Circuit breaker configuration
    pub circuit_breaker_config: CircuitBreakerConfig,

    // Event emission options
    /// Worker hostname for event identification
    pub hostname: String,

    /// Enable event emission for task and worker lifecycle events
    pub enable_events: bool,

    /// Heartbeat interval in seconds (0 = disabled)
    pub heartbeat_interval_secs: u64,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            concurrency: 4,
            poll_interval_ms: 1000,
            graceful_shutdown: true,
            max_retries: 3,
            retry_base_delay_ms: 1000,
            retry_max_delay_ms: 60000,
            default_timeout_secs: 300,
            enable_batch_dequeue: false,
            batch_size: 10,
            max_result_size_bytes: 0, // unlimited
            track_memory_usage: false,
            enable_circuit_breaker: false,
            circuit_breaker_config: CircuitBreakerConfig::default(),
            hostname: gethostname(),
            enable_events: false,
            heartbeat_interval_secs: 0, // disabled by default
        }
    }
}

/// Get the hostname of the current machine
fn gethostname() -> String {
    hostname::get()
        .map(|h| h.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string())
}

impl WorkerConfig {
    /// Create a new builder for WorkerConfig
    ///
    /// # Example
    ///
    /// ```
    /// use celers_worker::WorkerConfig;
    ///
    /// let config = WorkerConfig::builder()
    ///     .concurrency(10)
    ///     .max_retries(5)
    ///     .enable_batch_dequeue(true)
    ///     .batch_size(20)
    ///     .build();
    /// ```
    pub fn builder() -> WorkerConfigBuilder {
        WorkerConfigBuilder::new()
    }

    /// Check if batch dequeue is enabled
    pub fn has_batch_dequeue(&self) -> bool {
        self.enable_batch_dequeue
    }

    /// Check if circuit breaker is enabled
    pub fn has_circuit_breaker(&self) -> bool {
        self.enable_circuit_breaker
    }

    /// Check if memory tracking is enabled
    pub fn has_memory_tracking(&self) -> bool {
        self.track_memory_usage
    }

    /// Check if result size limiting is enabled
    pub fn has_result_size_limit(&self) -> bool {
        self.max_result_size_bytes > 0
    }

    /// Check if graceful shutdown is enabled
    pub fn has_graceful_shutdown(&self) -> bool {
        self.graceful_shutdown
    }

    /// Check if event emission is enabled
    pub fn has_events(&self) -> bool {
        self.enable_events
    }

    /// Check if heartbeat is enabled
    pub fn has_heartbeat(&self) -> bool {
        self.heartbeat_interval_secs > 0
    }

    /// Validate the worker configuration
    ///
    /// Returns an error if any configuration values are invalid:
    /// - Concurrency must be at least 1
    /// - Batch size must be at least 1 if batch dequeue is enabled
    /// - Timeout values must be reasonable
    /// - Retry configuration must be valid
    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.concurrency == 0 {
            return Err("Concurrency must be at least 1".to_string());
        }

        if self.enable_batch_dequeue && self.batch_size == 0 {
            return Err("Batch size must be at least 1 when batch dequeue is enabled".to_string());
        }

        if self.default_timeout_secs == 0 {
            return Err("Default timeout must be at least 1 second".to_string());
        }

        if self.retry_base_delay_ms == 0 {
            return Err("Retry base delay must be at least 1ms".to_string());
        }

        if self.retry_max_delay_ms < self.retry_base_delay_ms {
            return Err("Max retry delay must be greater than or equal to base delay".to_string());
        }

        if self.enable_circuit_breaker && !self.circuit_breaker_config.is_valid() {
            return Err("Circuit breaker configuration is invalid".to_string());
        }

        Ok(())
    }
}

impl std::fmt::Display for WorkerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WorkerConfig[concurrency={}, poll={}ms, retries={}, timeout={}s",
            self.concurrency, self.poll_interval_ms, self.max_retries, self.default_timeout_secs
        )?;
        if self.enable_batch_dequeue {
            write!(f, ", batch={}", self.batch_size)?;
        }
        if self.enable_circuit_breaker {
            write!(f, ", circuit_breaker=enabled")?;
        }
        if self.track_memory_usage {
            write!(f, ", memory_tracking=enabled")?;
        }
        if self.max_result_size_bytes > 0 {
            write!(f, ", max_result={}B", self.max_result_size_bytes)?;
        }
        if self.enable_events {
            write!(f, ", events=enabled")?;
        }
        if self.heartbeat_interval_secs > 0 {
            write!(f, ", heartbeat={}s", self.heartbeat_interval_secs)?;
        }
        write!(f, "]")
    }
}

/// Builder for WorkerConfig with fluent API
#[derive(Default)]
pub struct WorkerConfigBuilder {
    config: WorkerConfig,
}

impl WorkerConfigBuilder {
    /// Create a new WorkerConfigBuilder with default values
    pub fn new() -> Self {
        Self {
            config: WorkerConfig::default(),
        }
    }

    /// Set the number of concurrent tasks to process
    ///
    /// Default: 4
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.config.concurrency = concurrency;
        self
    }

    /// Set the polling interval when queue is empty (milliseconds)
    ///
    /// Default: 1000ms
    pub fn poll_interval_ms(mut self, interval_ms: u64) -> Self {
        self.config.poll_interval_ms = interval_ms;
        self
    }

    /// Enable or disable graceful shutdown
    ///
    /// Default: true
    pub fn graceful_shutdown(mut self, enabled: bool) -> Self {
        self.config.graceful_shutdown = enabled;
        self
    }

    /// Set the maximum number of retry attempts
    ///
    /// Default: 3
    pub fn max_retries(mut self, retries: u32) -> Self {
        self.config.max_retries = retries;
        self
    }

    /// Set the base delay for exponential backoff (milliseconds)
    ///
    /// Default: 1000ms
    pub fn retry_base_delay_ms(mut self, delay_ms: u64) -> Self {
        self.config.retry_base_delay_ms = delay_ms;
        self
    }

    /// Set the maximum delay between retries (milliseconds)
    ///
    /// Default: 60000ms (1 minute)
    pub fn retry_max_delay_ms(mut self, delay_ms: u64) -> Self {
        self.config.retry_max_delay_ms = delay_ms;
        self
    }

    /// Set the default task timeout in seconds
    ///
    /// Default: 300s (5 minutes)
    pub fn default_timeout_secs(mut self, timeout_secs: u64) -> Self {
        self.config.default_timeout_secs = timeout_secs;
        self
    }

    /// Enable batch dequeue for better throughput
    ///
    /// Default: false
    pub fn enable_batch_dequeue(mut self, enabled: bool) -> Self {
        self.config.enable_batch_dequeue = enabled;
        self
    }

    /// Set the number of tasks to fetch per batch (when batch dequeue enabled)
    ///
    /// Default: 10
    pub fn batch_size(mut self, size: usize) -> Self {
        self.config.batch_size = size;
        self
    }

    /// Set the maximum task result size in bytes (0 = unlimited)
    ///
    /// Default: 0 (unlimited)
    pub fn max_result_size_bytes(mut self, size: usize) -> Self {
        self.config.max_result_size_bytes = size;
        self
    }

    /// Enable memory usage tracking and reporting
    ///
    /// Default: false
    pub fn track_memory_usage(mut self, enabled: bool) -> Self {
        self.config.track_memory_usage = enabled;
        self
    }

    /// Enable circuit breaker for failing tasks
    ///
    /// Default: false
    pub fn enable_circuit_breaker(mut self, enabled: bool) -> Self {
        self.config.enable_circuit_breaker = enabled;
        self
    }

    /// Set the circuit breaker configuration
    pub fn circuit_breaker_config(mut self, config: CircuitBreakerConfig) -> Self {
        self.config.circuit_breaker_config = config;
        self
    }

    /// Set the worker hostname for event identification
    ///
    /// Default: system hostname
    pub fn hostname(mut self, hostname: impl Into<String>) -> Self {
        self.config.hostname = hostname.into();
        self
    }

    /// Enable event emission for task and worker lifecycle events
    ///
    /// Default: false
    pub fn enable_events(mut self, enabled: bool) -> Self {
        self.config.enable_events = enabled;
        self
    }

    /// Set the heartbeat interval in seconds (0 = disabled)
    ///
    /// Default: 0 (disabled)
    pub fn heartbeat_interval_secs(mut self, interval: u64) -> Self {
        self.config.heartbeat_interval_secs = interval;
        self
    }

    /// Preset: High throughput configuration
    ///
    /// - High concurrency (16 tasks)
    /// - Batch dequeue enabled (batch size: 20)
    /// - Circuit breaker enabled
    /// - Memory tracking enabled
    pub fn preset_high_throughput(mut self) -> Self {
        self.config.concurrency = 16;
        self.config.enable_batch_dequeue = true;
        self.config.batch_size = 20;
        self.config.enable_circuit_breaker = true;
        self.config.track_memory_usage = true;
        self
    }

    /// Preset: Low latency configuration
    ///
    /// - Moderate concurrency (8 tasks)
    /// - Short poll interval (100ms)
    /// - Batch dequeue disabled
    /// - Circuit breaker enabled
    pub fn preset_low_latency(mut self) -> Self {
        self.config.concurrency = 8;
        self.config.poll_interval_ms = 100;
        self.config.enable_batch_dequeue = false;
        self.config.enable_circuit_breaker = true;
        self
    }

    /// Preset: Reliable processing configuration
    ///
    /// - Conservative concurrency (4 tasks)
    /// - High retry count (5 retries)
    /// - Circuit breaker enabled
    /// - Graceful shutdown enabled
    pub fn preset_reliable(mut self) -> Self {
        self.config.concurrency = 4;
        self.config.max_retries = 5;
        self.config.enable_circuit_breaker = true;
        self.config.graceful_shutdown = true;
        self
    }

    /// Preset: Development/testing configuration
    ///
    /// - Low concurrency (2 tasks)
    /// - Short poll interval (500ms)
    /// - Circuit breaker disabled
    /// - Memory tracking enabled
    pub fn preset_development(mut self) -> Self {
        self.config.concurrency = 2;
        self.config.poll_interval_ms = 500;
        self.config.enable_circuit_breaker = false;
        self.config.track_memory_usage = true;
        self
    }

    /// Validate and build the WorkerConfig
    ///
    /// Returns an error if the configuration is invalid
    pub fn build(self) -> std::result::Result<WorkerConfig, String> {
        // Validation
        if self.config.concurrency == 0 {
            return Err("Concurrency must be greater than 0".to_string());
        }

        if self.config.batch_size == 0 && self.config.enable_batch_dequeue {
            return Err(
                "Batch size must be greater than 0 when batch dequeue is enabled".to_string(),
            );
        }

        if self.config.retry_base_delay_ms > self.config.retry_max_delay_ms {
            return Err("Retry base delay cannot be greater than max delay".to_string());
        }

        if self.config.default_timeout_secs == 0 {
            return Err("Default timeout must be greater than 0".to_string());
        }

        Ok(self.config)
    }

    /// Build without validation (use with caution)
    pub fn build_unchecked(self) -> WorkerConfig {
        self.config
    }
}

/// Worker statistics for heartbeat reporting
#[derive(Debug, Default)]
pub struct WorkerStats {
    /// Number of currently active (executing) tasks
    active: AtomicU64,
    /// Total number of tasks processed since worker start
    processed: AtomicU64,
}

impl WorkerStats {
    /// Create new worker statistics
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the number of active tasks
    pub fn active(&self) -> u64 {
        self.active.load(Ordering::Relaxed)
    }

    /// Get the number of processed tasks
    pub fn processed(&self) -> u64 {
        self.processed.load(Ordering::Relaxed)
    }

    /// Increment the active task count (called when a task starts)
    pub fn task_started(&self) {
        self.active.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement the active task count and increment processed (called when a task completes)
    pub fn task_completed(&self) {
        self.active.fetch_sub(1, Ordering::Relaxed);
        self.processed.fetch_add(1, Ordering::Relaxed);
    }
}

impl Clone for WorkerStats {
    fn clone(&self) -> Self {
        Self {
            active: AtomicU64::new(self.active.load(Ordering::Relaxed)),
            processed: AtomicU64::new(self.processed.load(Ordering::Relaxed)),
        }
    }
}

/// Worker runtime for consuming and executing tasks
pub struct Worker<B: Broker, E: EventEmitter = NoOpEventEmitter> {
    broker: Arc<B>,
    registry: Arc<TaskRegistry>,
    config: WorkerConfig,
    circuit_breaker: Option<Arc<CircuitBreaker>>,
    shutdown_tx: Option<mpsc::Sender<()>>,
    event_emitter: Arc<E>,
    stats: Arc<WorkerStats>,
}

/// Handle for controlling a running worker
pub struct WorkerHandle {
    shutdown_tx: mpsc::Sender<()>,
}

impl WorkerHandle {
    /// Request graceful shutdown of the worker
    pub async fn shutdown(&self) -> Result<()> {
        self.shutdown_tx.send(()).await.map_err(|_| {
            celers_core::CelersError::Other("Failed to send shutdown signal".to_string())
        })?;
        Ok(())
    }
}

impl<B: Broker + 'static> Worker<B, NoOpEventEmitter> {
    /// Create a new worker with default (no-op) event emitter
    pub fn new(broker: B, registry: TaskRegistry, config: WorkerConfig) -> Self {
        Worker::with_event_emitter(broker, registry, config, NoOpEventEmitter::new())
    }
}

impl<B: Broker + 'static, E: EventEmitter + 'static> Worker<B, E> {
    /// Create a new worker with a custom event emitter
    pub fn with_event_emitter(
        broker: B,
        registry: TaskRegistry,
        config: WorkerConfig,
        event_emitter: E,
    ) -> Self {
        let circuit_breaker = if config.enable_circuit_breaker {
            Some(Arc::new(CircuitBreaker::with_config(
                config.circuit_breaker_config.clone(),
            )))
        } else {
            None
        };

        Self {
            broker: Arc::new(broker),
            registry: Arc::new(registry),
            config,
            circuit_breaker,
            shutdown_tx: None,
            event_emitter: Arc::new(event_emitter),
            stats: Arc::new(WorkerStats::new()),
        }
    }

    /// Get the worker statistics
    pub fn stats(&self) -> &WorkerStats {
        &self.stats
    }

    /// Start the worker loop with graceful shutdown support
    /// Returns a WorkerHandle that can be used to signal shutdown
    pub async fn run_with_shutdown(mut self) -> Result<WorkerHandle> {
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);
        let handle = WorkerHandle {
            shutdown_tx: shutdown_tx.clone(),
        };

        self.shutdown_tx = Some(shutdown_tx);

        tokio::spawn(async move {
            if let Err(e) = self.run_loop(Some(&mut shutdown_rx)).await {
                error!("Worker error: {}", e);
            }
        });

        Ok(handle)
    }

    /// Start the worker loop (blocks until shutdown or error)
    pub async fn run(&self) -> Result<()> {
        self.run_loop(None).await
    }

    /// Internal worker loop implementation
    async fn run_loop(&self, mut shutdown_rx: Option<&mut mpsc::Receiver<()>>) -> Result<()> {
        let hostname = self.config.hostname.clone();
        let pid = std::process::id();

        info!(
            "Starting worker with concurrency {} and max retries {}",
            self.config.concurrency, self.config.max_retries
        );

        // Emit worker online event
        if self.config.enable_events {
            let event = WorkerEventBuilder::new(&hostname).online();
            if let Err(e) = self.event_emitter.emit(event).await {
                warn!("Failed to emit worker-online event: {}", e);
            }
        }

        // Start heartbeat task if configured
        let heartbeat_handle =
            if self.config.enable_events && self.config.heartbeat_interval_secs > 0 {
                let heartbeat_hostname = hostname.clone();
                let heartbeat_interval = Duration::from_secs(self.config.heartbeat_interval_secs);
                let heartbeat_emitter = Arc::clone(&self.event_emitter);
                let heartbeat_stats = Arc::clone(&self.stats);
                let heartbeat_freq = self.config.heartbeat_interval_secs as f64;

                Some(tokio::spawn(async move {
                    Self::heartbeat_loop(
                        heartbeat_hostname,
                        heartbeat_interval,
                        heartbeat_emitter,
                        heartbeat_stats,
                        heartbeat_freq,
                    )
                    .await;
                }))
            } else {
                None
            };

        let result = self.run_loop_inner(&mut shutdown_rx, &hostname, pid).await;

        // Stop heartbeat task
        if let Some(handle) = heartbeat_handle {
            handle.abort();
        }

        // Emit worker offline event
        if self.config.enable_events {
            let event = WorkerEventBuilder::new(&hostname).offline();
            if let Err(e) = self.event_emitter.emit(event).await {
                warn!("Failed to emit worker-offline event: {}", e);
            }
        }

        result
    }

    /// Heartbeat loop that periodically emits worker-heartbeat events
    async fn heartbeat_loop<EE: EventEmitter>(
        hostname: String,
        interval: Duration,
        event_emitter: Arc<EE>,
        stats: Arc<WorkerStats>,
        freq: f64,
    ) {
        loop {
            sleep(interval).await;

            let active = stats.active() as u32;
            let processed = stats.processed();

            // Get system load average (on Unix systems)
            let loadavg = Self::get_load_average();

            let event =
                WorkerEventBuilder::new(&hostname).heartbeat(active, processed, loadavg, freq);

            if let Err(e) = event_emitter.emit(event).await {
                debug!("Failed to emit worker-heartbeat event: {}", e);
            }
        }
    }

    /// Get system load average (returns [0.0, 0.0, 0.0] on non-Unix systems)
    fn get_load_average() -> [f64; 3] {
        #[cfg(unix)]
        {
            use std::fs;
            if let Ok(contents) = fs::read_to_string("/proc/loadavg") {
                let parts: Vec<&str> = contents.split_whitespace().collect();
                if parts.len() >= 3 {
                    let load1 = parts[0].parse().unwrap_or(0.0);
                    let load5 = parts[1].parse().unwrap_or(0.0);
                    let load15 = parts[2].parse().unwrap_or(0.0);
                    return [load1, load5, load15];
                }
            }
            [0.0, 0.0, 0.0]
        }

        #[cfg(not(unix))]
        {
            [0.0, 0.0, 0.0]
        }
    }

    /// Inner worker loop (separated to ensure offline event is always emitted)
    async fn run_loop_inner(
        &self,
        shutdown_rx: &mut Option<&mut mpsc::Receiver<()>>,
        hostname: &str,
        pid: u32,
    ) -> Result<()> {
        loop {
            // Check for shutdown signal if receiver is provided
            if let Some(ref mut rx) = shutdown_rx {
                match rx.try_recv() {
                    Ok(_) => {
                        info!("Shutdown signal received, stopping worker gracefully");
                        return Ok(());
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        warn!("Shutdown channel disconnected, stopping worker");
                        return Ok(());
                    }
                    Err(mpsc::error::TryRecvError::Empty) => {
                        // No shutdown signal, continue
                    }
                }
            }

            // Dequeue tasks (single or batch depending on configuration)
            let messages_result = if self.config.enable_batch_dequeue {
                debug!(
                    "Batch dequeue enabled, fetching up to {} tasks",
                    self.config.batch_size
                );
                self.broker.dequeue_batch(self.config.batch_size).await
            } else {
                // Single task dequeue (convert to Vec for uniform handling)
                match self.broker.dequeue().await {
                    Ok(Some(msg)) => Ok(vec![msg]),
                    Ok(None) => Ok(vec![]),
                    Err(e) => Err(e),
                }
            };

            match messages_result {
                Ok(messages) if !messages.is_empty() => {
                    if self.config.enable_batch_dequeue {
                        info!("Dequeued {} tasks in batch", messages.len());
                    }

                    // Process each message
                    for msg in messages {
                        let task_id = msg.task.metadata.id;
                        let task_name = msg.task.metadata.name.clone();
                        info!("Processing task {} ({})", task_id, task_name);

                        // Emit task-received event
                        if self.config.enable_events {
                            let event = TaskEventBuilder::new(task_id, &task_name)
                                .hostname(hostname)
                                .pid(pid)
                                .received();
                            if let Err(e) = self.event_emitter.emit(event).await {
                                debug!("Failed to emit task-received event: {}", e);
                            }
                        }

                        // Check circuit breaker
                        if let Some(ref cb) = self.circuit_breaker {
                            if !cb.should_allow(&task_name).await {
                                warn!(
                                    "Circuit breaker OPEN for task type '{}', rejecting task {}",
                                    task_name, task_id
                                );

                                // Emit task-rejected event
                                if self.config.enable_events {
                                    let event = Event::Task(celers_core::TaskEvent::Rejected {
                                        task_id,
                                        task_name: Some(task_name.clone()),
                                        hostname: hostname.to_string(),
                                        timestamp: chrono::Utc::now(),
                                        reason: "Circuit breaker OPEN".to_string(),
                                    });
                                    if let Err(e) = self.event_emitter.emit(event).await {
                                        debug!("Failed to emit task-rejected event: {}", e);
                                    }
                                }

                                // Reject task without retrying
                                if let Err(e) = self
                                    .broker
                                    .reject(&task_id, msg.receipt_handle.as_deref(), false)
                                    .await
                                {
                                    error!("Failed to reject task {}: {}", task_id, e);
                                }
                                continue;
                            }
                        }

                        // Execute task with timeout
                        let timeout_secs = msg
                            .task
                            .metadata
                            .timeout_secs
                            .unwrap_or(self.config.default_timeout_secs);

                        let broker = Arc::clone(&self.broker);
                        let registry = Arc::clone(&self.registry);
                        let circuit_breaker = self.circuit_breaker.clone();
                        let receipt_handle = msg.receipt_handle.clone();
                        let task = msg.task.clone();
                        let event_emitter = Arc::clone(&self.event_emitter);
                        let enable_events = self.config.enable_events;
                        let hostname = hostname.to_string();
                        let stats = Arc::clone(&self.stats);

                        tokio::spawn(async move {
                            let start_time = Instant::now();

                            // Track active task
                            stats.task_started();

                            // Emit task-started event
                            if enable_events {
                                let event = TaskEventBuilder::new(task_id, &task.metadata.name)
                                    .hostname(&hostname)
                                    .pid(pid)
                                    .started();
                                if let Err(e) = event_emitter.emit(event).await {
                                    debug!("Failed to emit task-started event: {}", e);
                                }
                            }

                            match timeout(
                                Duration::from_secs(timeout_secs),
                                registry.execute(&task),
                            )
                            .await
                            {
                                Ok(Ok(result)) => {
                                    // Task succeeded
                                    let duration = start_time.elapsed();
                                    info!(
                                        "Task {} completed successfully in {:?}",
                                        task_id, duration
                                    );
                                    debug!("Result size: {} bytes", result.len());

                                    // Emit task-succeeded event
                                    if enable_events {
                                        let event =
                                            TaskEventBuilder::new(task_id, &task.metadata.name)
                                                .hostname(&hostname)
                                                .pid(pid)
                                                .succeeded(duration.as_secs_f64());
                                        if let Err(e) = event_emitter.emit(event).await {
                                            debug!("Failed to emit task-succeeded event: {}", e);
                                        }
                                    }

                                    // Record success in circuit breaker
                                    if let Some(ref cb) = circuit_breaker {
                                        cb.record_success(&task.metadata.name).await;
                                    }

                                    #[cfg(feature = "metrics")]
                                    {
                                        TASKS_COMPLETED_TOTAL.inc();
                                        TASK_EXECUTION_TIME.observe(duration.as_secs_f64());

                                        // Track per-task-type metrics
                                        let task_name = &task.metadata.name;
                                        TASKS_COMPLETED_BY_TYPE
                                            .with_label_values(&[task_name])
                                            .inc();
                                        TASK_EXECUTION_TIME_BY_TYPE
                                            .with_label_values(&[task_name])
                                            .observe(duration.as_secs_f64());
                                    }

                                    if let Err(e) =
                                        broker.ack(&task_id, receipt_handle.as_deref()).await
                                    {
                                        error!("Failed to acknowledge task {}: {}", task_id, e);
                                    }
                                }
                                Ok(Err(e)) => {
                                    // Task failed
                                    let error_msg = e.to_string();
                                    error!("Task {} failed: {}", task_id, error_msg);

                                    // Check if we should retry
                                    let current_retry = match task.metadata.state {
                                        TaskState::Retrying(count) => count,
                                        _ => 0,
                                    };

                                    if current_retry < task.metadata.max_retries {
                                        // Requeue for retry
                                        warn!(
                                            "Requeuing task {} for retry {}/{}",
                                            task_id,
                                            current_retry + 1,
                                            task.metadata.max_retries
                                        );

                                        // Emit task-retried event
                                        if enable_events {
                                            let event =
                                                TaskEventBuilder::new(task_id, &task.metadata.name)
                                                    .hostname(&hostname)
                                                    .pid(pid)
                                                    .retried(&error_msg, current_retry + 1);
                                            if let Err(e) = event_emitter.emit(event).await {
                                                debug!("Failed to emit task-retried event: {}", e);
                                            }
                                        }

                                        #[cfg(feature = "metrics")]
                                        {
                                            TASKS_RETRIED_TOTAL.inc();

                                            // Track per-task-type metrics
                                            let task_name = &task.metadata.name;
                                            TASKS_RETRIED_BY_TYPE
                                                .with_label_values(&[task_name])
                                                .inc();
                                        }

                                        if let Err(e) = broker
                                            .reject(&task_id, receipt_handle.as_deref(), true)
                                            .await
                                        {
                                            error!("Failed to requeue task {}: {}", task_id, e);
                                        }
                                    } else {
                                        // Max retries reached, permanently fail
                                        error!(
                                            "Task {} failed permanently after {} retries",
                                            task_id, current_retry
                                        );

                                        // Emit task-failed event
                                        if enable_events {
                                            let event =
                                                TaskEventBuilder::new(task_id, &task.metadata.name)
                                                    .hostname(&hostname)
                                                    .pid(pid)
                                                    .failed(&error_msg);
                                            if let Err(e) = event_emitter.emit(event).await {
                                                debug!("Failed to emit task-failed event: {}", e);
                                            }
                                        }

                                        // Record failure in circuit breaker
                                        if let Some(ref cb) = circuit_breaker {
                                            cb.record_failure(&task.metadata.name).await;
                                        }

                                        #[cfg(feature = "metrics")]
                                        {
                                            TASKS_FAILED_TOTAL.inc();

                                            // Track per-task-type metrics
                                            let task_name = &task.metadata.name;
                                            TASKS_FAILED_BY_TYPE
                                                .with_label_values(&[task_name])
                                                .inc();
                                        }

                                        if let Err(e) = broker
                                            .reject(&task_id, receipt_handle.as_deref(), false)
                                            .await
                                        {
                                            error!("Failed to reject task {}: {}", task_id, e);
                                        }
                                    }
                                }
                                Err(_) => {
                                    // Timeout
                                    let error_msg =
                                        format!("Task timed out after {}s", timeout_secs);
                                    error!("Task {} timed out after {}s", task_id, timeout_secs);

                                    // Requeue if retries remaining
                                    let current_retry = match task.metadata.state {
                                        TaskState::Retrying(count) => count,
                                        _ => 0,
                                    };

                                    if current_retry < task.metadata.max_retries {
                                        // Emit task-retried event
                                        if enable_events {
                                            let event =
                                                TaskEventBuilder::new(task_id, &task.metadata.name)
                                                    .hostname(&hostname)
                                                    .pid(pid)
                                                    .retried(&error_msg, current_retry + 1);
                                            if let Err(e) = event_emitter.emit(event).await {
                                                debug!("Failed to emit task-retried event: {}", e);
                                            }
                                        }

                                        #[cfg(feature = "metrics")]
                                        {
                                            TASKS_RETRIED_TOTAL.inc();

                                            // Track per-task-type metrics
                                            let task_name = &task.metadata.name;
                                            TASKS_RETRIED_BY_TYPE
                                                .with_label_values(&[task_name])
                                                .inc();
                                        }

                                        if let Err(e) = broker
                                            .reject(&task_id, receipt_handle.as_deref(), true)
                                            .await
                                        {
                                            error!("Failed to requeue task {}: {}", task_id, e);
                                        }
                                    } else {
                                        // Record timeout failure in circuit breaker
                                        if let Some(ref cb) = circuit_breaker {
                                            cb.record_failure(&task.metadata.name).await;
                                        }

                                        // Emit task-failed event
                                        if enable_events {
                                            let event =
                                                TaskEventBuilder::new(task_id, &task.metadata.name)
                                                    .hostname(&hostname)
                                                    .pid(pid)
                                                    .failed(&error_msg);
                                            if let Err(e) = event_emitter.emit(event).await {
                                                debug!("Failed to emit task-failed event: {}", e);
                                            }
                                        }

                                        #[cfg(feature = "metrics")]
                                        {
                                            TASKS_FAILED_TOTAL.inc();

                                            // Track per-task-type metrics
                                            let task_name = &task.metadata.name;
                                            TASKS_FAILED_BY_TYPE
                                                .with_label_values(&[task_name])
                                                .inc();
                                        }

                                        if let Err(e) = broker
                                            .reject(&task_id, receipt_handle.as_deref(), false)
                                            .await
                                        {
                                            error!("Failed to reject task {}: {}", task_id, e);
                                        }
                                    }
                                }
                            };

                            // Track task completion
                            stats.task_completed();
                        });
                    }
                }
                Ok(_) => {
                    // Queue is empty (messages vec is empty), sleep before next poll
                    debug!(
                        "Queue empty, sleeping for {}ms",
                        self.config.poll_interval_ms
                    );
                    sleep(Duration::from_millis(self.config.poll_interval_ms)).await;
                }
                Err(e) => {
                    error!("Error dequeueing tasks: {}", e);
                    sleep(Duration::from_millis(self.config.poll_interval_ms)).await;
                }
            }
        }
    }

    /// Calculate exponential backoff delay
    #[allow(dead_code)]
    fn calculate_backoff_delay(&self, retry_count: u32) -> StdDuration {
        let delay_ms = self.config.retry_base_delay_ms * 2_u64.pow(retry_count);
        let delay_ms = delay_ms.min(self.config.retry_max_delay_ms);
        StdDuration::from_millis(delay_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_calculation() {
        let config = WorkerConfig::default();
        let worker: Worker<MockBroker, NoOpEventEmitter> = Worker {
            broker: Arc::new(MockBroker),
            registry: Arc::new(TaskRegistry::new()),
            config,
            circuit_breaker: None,
            shutdown_tx: None,
            event_emitter: Arc::new(NoOpEventEmitter::new()),
            stats: Arc::new(WorkerStats::new()),
        };

        assert_eq!(worker.calculate_backoff_delay(0).as_millis(), 1000);
        assert_eq!(worker.calculate_backoff_delay(1).as_millis(), 2000);
        assert_eq!(worker.calculate_backoff_delay(2).as_millis(), 4000);
        assert_eq!(worker.calculate_backoff_delay(3).as_millis(), 8000);

        // Should cap at max delay
        assert_eq!(worker.calculate_backoff_delay(10).as_millis(), 60000);
    }

    #[test]
    fn test_worker_stats() {
        let stats = WorkerStats::new();

        assert_eq!(stats.active(), 0);
        assert_eq!(stats.processed(), 0);

        stats.task_started();
        assert_eq!(stats.active(), 1);
        assert_eq!(stats.processed(), 0);

        stats.task_started();
        assert_eq!(stats.active(), 2);

        stats.task_completed();
        assert_eq!(stats.active(), 1);
        assert_eq!(stats.processed(), 1);

        stats.task_completed();
        assert_eq!(stats.active(), 0);
        assert_eq!(stats.processed(), 2);
    }

    #[test]
    fn test_worker_config_default() {
        let config = WorkerConfig::default();
        assert_eq!(config.concurrency, 4);
        assert_eq!(config.poll_interval_ms, 1000);
        assert!(config.graceful_shutdown);
        assert_eq!(config.max_retries, 3);
        assert!(!config.enable_batch_dequeue);
        assert!(!config.enable_circuit_breaker);
        assert!(!config.track_memory_usage);
    }

    #[test]
    fn test_worker_config_predicates() {
        let mut config = WorkerConfig::default();

        assert!(!config.has_batch_dequeue());
        assert!(!config.has_circuit_breaker());
        assert!(!config.has_memory_tracking());
        assert!(!config.has_result_size_limit());
        assert!(config.has_graceful_shutdown());
        assert!(!config.has_events());
        assert!(!config.has_heartbeat());

        config.enable_batch_dequeue = true;
        config.enable_circuit_breaker = true;
        config.track_memory_usage = true;
        config.max_result_size_bytes = 1024;
        config.graceful_shutdown = false;
        config.enable_events = true;
        config.heartbeat_interval_secs = 30;

        assert!(config.has_batch_dequeue());
        assert!(config.has_circuit_breaker());
        assert!(config.has_memory_tracking());
        assert!(config.has_result_size_limit());
        assert!(!config.has_graceful_shutdown());
        assert!(config.has_events());
        assert!(config.has_heartbeat());
    }

    #[test]
    fn test_worker_config_validate_concurrency_zero() {
        let mut config = WorkerConfig::default();
        config.concurrency = 0;
        let result = config.validate();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Concurrency must be at least 1");
    }

    #[test]
    fn test_worker_config_validate_batch_size_zero() {
        let mut config = WorkerConfig::default();
        config.enable_batch_dequeue = true;
        config.batch_size = 0;
        let result = config.validate();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "Batch size must be at least 1 when batch dequeue is enabled"
        );
    }

    #[test]
    fn test_worker_config_validate_timeout_zero() {
        let mut config = WorkerConfig::default();
        config.default_timeout_secs = 0;
        let result = config.validate();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "Default timeout must be at least 1 second"
        );
    }

    #[test]
    fn test_worker_config_validate_retry_delays() {
        let mut config = WorkerConfig::default();
        config.retry_base_delay_ms = 0;
        let result = config.validate();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Retry base delay must be at least 1ms");

        config.retry_base_delay_ms = 1000;
        config.retry_max_delay_ms = 500;
        let result = config.validate();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "Max retry delay must be greater than or equal to base delay"
        );
    }

    #[test]
    fn test_worker_config_display() {
        let config = WorkerConfig::default();
        let display = format!("{}", config);
        assert!(display.contains("WorkerConfig"));
        assert!(display.contains("concurrency=4"));
        assert!(display.contains("poll=1000ms"));
        assert!(display.contains("retries=3"));
        assert!(display.contains("timeout=300s"));
    }

    #[test]
    fn test_worker_config_display_with_features() {
        let config = WorkerConfig {
            concurrency: 8,
            enable_batch_dequeue: true,
            batch_size: 20,
            enable_circuit_breaker: true,
            track_memory_usage: true,
            max_result_size_bytes: 1048576,
            ..Default::default()
        };
        let display = format!("{}", config);
        assert!(display.contains("batch=20"));
        assert!(display.contains("circuit_breaker=enabled"));
        assert!(display.contains("memory_tracking=enabled"));
        assert!(display.contains("max_result=1048576B"));
    }

    #[test]
    fn test_worker_config_builder_basic() {
        let config = WorkerConfig::builder()
            .concurrency(10)
            .max_retries(5)
            .default_timeout_secs(600)
            .build()
            .unwrap();

        assert_eq!(config.concurrency, 10);
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.default_timeout_secs, 600);
    }

    #[test]
    fn test_worker_config_builder_batch_settings() {
        let config = WorkerConfig::builder()
            .enable_batch_dequeue(true)
            .batch_size(25)
            .build()
            .unwrap();

        assert!(config.enable_batch_dequeue);
        assert_eq!(config.batch_size, 25);
    }

    #[test]
    fn test_worker_config_builder_validation_errors() {
        // Zero concurrency should fail
        let result = WorkerConfig::builder().concurrency(0).build();
        assert!(result.is_err());

        // Batch dequeue with zero batch size should fail
        let result = WorkerConfig::builder()
            .enable_batch_dequeue(true)
            .batch_size(0)
            .build();
        assert!(result.is_err());

        // Zero timeout should fail
        let result = WorkerConfig::builder().default_timeout_secs(0).build();
        assert!(result.is_err());

        // Invalid retry delays should fail
        let result = WorkerConfig::builder()
            .retry_base_delay_ms(2000)
            .retry_max_delay_ms(1000)
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_worker_config_builder_preset_high_throughput() {
        let config = WorkerConfig::builder()
            .preset_high_throughput()
            .build()
            .unwrap();

        assert_eq!(config.concurrency, 16);
        assert!(config.enable_batch_dequeue);
        assert_eq!(config.batch_size, 20);
        assert!(config.enable_circuit_breaker);
        assert!(config.track_memory_usage);
    }

    #[test]
    fn test_worker_config_builder_preset_low_latency() {
        let config = WorkerConfig::builder()
            .preset_low_latency()
            .build()
            .unwrap();

        assert_eq!(config.concurrency, 8);
        assert_eq!(config.poll_interval_ms, 100);
        assert!(!config.enable_batch_dequeue);
        assert!(config.enable_circuit_breaker);
    }

    #[test]
    fn test_worker_config_builder_preset_reliable() {
        let config = WorkerConfig::builder().preset_reliable().build().unwrap();

        assert_eq!(config.concurrency, 4);
        assert_eq!(config.max_retries, 5);
        assert!(config.enable_circuit_breaker);
        assert!(config.graceful_shutdown);
    }

    #[test]
    fn test_worker_config_builder_preset_development() {
        let config = WorkerConfig::builder()
            .preset_development()
            .build()
            .unwrap();

        assert_eq!(config.concurrency, 2);
        assert_eq!(config.poll_interval_ms, 500);
        assert!(!config.enable_circuit_breaker);
        assert!(config.track_memory_usage);
    }

    #[test]
    fn test_worker_config_builder_build_unchecked() {
        // Should allow invalid config when using build_unchecked
        let config = WorkerConfig::builder().concurrency(0).build_unchecked();

        assert_eq!(config.concurrency, 0);
    }

    #[test]
    fn test_worker_config_builder_events() {
        let config = WorkerConfig::builder()
            .hostname("my-worker")
            .enable_events(true)
            .heartbeat_interval_secs(30)
            .build()
            .unwrap();

        assert_eq!(config.hostname, "my-worker");
        assert!(config.enable_events);
        assert_eq!(config.heartbeat_interval_secs, 30);
        assert!(config.has_events());
        assert!(config.has_heartbeat());
    }

    #[test]
    fn test_worker_config_display_with_events() {
        let config = WorkerConfig {
            enable_events: true,
            heartbeat_interval_secs: 60,
            ..Default::default()
        };
        let display = format!("{}", config);
        assert!(display.contains("events=enabled"));
        assert!(display.contains("heartbeat=60s"));
    }

    // Mock broker for testing
    struct MockBroker;

    #[async_trait::async_trait]
    impl Broker for MockBroker {
        async fn enqueue(&self, _task: celers_core::SerializedTask) -> Result<celers_core::TaskId> {
            unimplemented!()
        }

        async fn dequeue(&self) -> Result<Option<celers_core::BrokerMessage>> {
            Ok(None)
        }

        async fn ack(
            &self,
            _task_id: &celers_core::TaskId,
            _receipt_handle: Option<&str>,
        ) -> Result<()> {
            Ok(())
        }

        async fn reject(
            &self,
            _task_id: &celers_core::TaskId,
            _receipt_handle: Option<&str>,
            _requeue: bool,
        ) -> Result<()> {
            Ok(())
        }

        async fn queue_size(&self) -> Result<usize> {
            Ok(0)
        }

        async fn cancel(&self, _task_id: &celers_core::TaskId) -> Result<bool> {
            Ok(false)
        }
    }
}
