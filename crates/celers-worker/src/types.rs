//! Types, structs, enums, and configuration for the worker runtime.

use crate::circuit_breaker::CircuitBreakerConfig;
use crate::dlq::DlqConfig;
use crate::feature_flags::FeatureFlags;
use crate::metadata::WorkerMetadata;
use crate::retry::{RetryConfig, RetryStrategy};
use crate::routing::{RoutingStrategy, WorkerTags};

use celers_core::Result;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;
use tokio::time::Duration;
use tracing::{debug, info, warn};

/// Worker operational mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WorkerMode {
    /// Normal operation - accepting and processing tasks
    Normal = 0,
    /// Maintenance mode - not accepting new tasks, completing existing ones
    Maintenance = 1,
    /// Draining - gracefully stopping after completing current tasks
    Draining = 2,
}

impl WorkerMode {
    /// Check if the worker should accept new tasks
    pub fn should_accept_tasks(&self) -> bool {
        matches!(self, WorkerMode::Normal)
    }

    /// Check if the worker is in maintenance mode
    pub fn is_maintenance(&self) -> bool {
        matches!(self, WorkerMode::Maintenance)
    }

    /// Check if the worker is draining
    pub fn is_draining(&self) -> bool {
        matches!(self, WorkerMode::Draining)
    }

    /// Check if the worker should continue running
    pub fn should_continue(&self) -> bool {
        !matches!(self, WorkerMode::Draining)
    }
}

impl From<u8> for WorkerMode {
    fn from(value: u8) -> Self {
        match value {
            1 => WorkerMode::Maintenance,
            2 => WorkerMode::Draining,
            _ => WorkerMode::Normal,
        }
    }
}

impl std::fmt::Display for WorkerMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkerMode::Normal => write!(f, "Normal"),
            WorkerMode::Maintenance => write!(f, "Maintenance"),
            WorkerMode::Draining => write!(f, "Draining"),
        }
    }
}

/// Dynamic worker configuration that can be updated at runtime
#[derive(Debug, Clone)]
pub struct DynamicConfig {
    /// Polling interval when queue is empty (milliseconds)
    pub poll_interval_ms: u64,
    /// Default task timeout in seconds
    pub default_timeout_secs: u64,
    /// Maximum number of retry attempts
    pub max_retries: u32,
}

impl Default for DynamicConfig {
    fn default() -> Self {
        Self {
            poll_interval_ms: 1000,
            default_timeout_secs: 300,
            max_retries: 3,
        }
    }
}

impl DynamicConfig {
    /// Validate the dynamic configuration
    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.default_timeout_secs == 0 {
            return Err("Default timeout must be at least 1 second".to_string());
        }
        Ok(())
    }
}

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
    /// DEPRECATED: Use retry_config instead
    pub retry_base_delay_ms: u64,

    /// Maximum delay between retries (milliseconds)
    /// DEPRECATED: Use retry_config instead
    pub retry_max_delay_ms: u64,

    /// Retry configuration (strategy and options)
    /// If set, this overrides retry_base_delay_ms and retry_max_delay_ms
    pub retry_config: Option<RetryConfig>,

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

    // Dead Letter Queue options
    /// Enable Dead Letter Queue for permanently failed tasks
    pub enable_dlq: bool,

    /// DLQ configuration
    pub dlq_config: DlqConfig,

    // Routing options
    /// Enable worker tagging and routing
    pub enable_routing: bool,

    /// Worker tags for task routing
    pub worker_tags: WorkerTags,

    /// Routing strategy
    pub routing_strategy: RoutingStrategy,

    // Event emission options
    /// Worker hostname for event identification
    pub hostname: String,

    /// Enable event emission for task and worker lifecycle events
    pub enable_events: bool,

    /// Heartbeat interval in seconds (0 = disabled)
    pub heartbeat_interval_secs: u64,

    /// Worker metadata (version, build info, labels)
    pub metadata: WorkerMetadata,

    /// Feature flags enabled on this worker
    pub feature_flags: FeatureFlags,
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
            retry_config: None, // Use legacy config by default
            default_timeout_secs: 300,
            enable_batch_dequeue: false,
            batch_size: 10,
            max_result_size_bytes: 0, // unlimited
            track_memory_usage: false,
            enable_circuit_breaker: false,
            circuit_breaker_config: CircuitBreakerConfig::default(),
            enable_dlq: false,
            dlq_config: DlqConfig::default(),
            enable_routing: false,
            worker_tags: WorkerTags::new(),
            routing_strategy: RoutingStrategy::default(),
            hostname: gethostname(),
            enable_events: false,
            heartbeat_interval_secs: 0, // disabled by default
            metadata: WorkerMetadata::default(),
            feature_flags: FeatureFlags::default(),
        }
    }
}

/// Get the hostname of the current machine
pub(crate) fn gethostname() -> String {
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

    /// Create configuration for development environment
    ///
    /// Optimized for fast iteration and debugging:
    /// - Low concurrency (2 tasks)
    /// - Short poll interval (500ms)
    /// - Memory tracking enabled
    /// - Verbose logging via metadata
    pub fn for_development() -> Self {
        Self::builder()
            .preset_development()
            .metadata(WorkerMetadata::builder().environment("development").build())
            .build_unchecked()
    }

    /// Create configuration for staging environment
    ///
    /// Balanced configuration for testing:
    /// - Moderate concurrency (8 tasks)
    /// - Circuit breaker enabled
    /// - Memory tracking enabled
    /// - Events enabled for monitoring
    pub fn for_staging() -> Self {
        Self::builder()
            .concurrency(8)
            .enable_circuit_breaker(true)
            .track_memory_usage(true)
            .enable_events(true)
            .heartbeat_interval_secs(30)
            .metadata(WorkerMetadata::builder().environment("staging").build())
            .build_unchecked()
    }

    /// Create configuration for production environment
    ///
    /// Optimized for reliability and performance:
    /// - High concurrency (16 tasks)
    /// - Batch dequeue enabled
    /// - Circuit breaker enabled
    /// - High retry count (5)
    /// - Events and heartbeat enabled
    pub fn for_production() -> Self {
        Self::builder()
            .preset_high_throughput()
            .max_retries(5)
            .enable_events(true)
            .heartbeat_interval_secs(30)
            .metadata(WorkerMetadata::builder().environment("production").build())
            .build_unchecked()
    }

    /// Create configuration based on environment variable
    ///
    /// Reads the `CELERS_ENV` environment variable and returns:
    /// - "development" or "dev" -> `for_development()`
    /// - "staging" or "stage" -> `for_staging()`
    /// - "production" or "prod" -> `for_production()`
    /// - Otherwise -> `for_development()` (default)
    ///
    /// # Example
    ///
    /// ```
    /// use celers_worker::WorkerConfig;
    ///
    /// // Set environment: export CELERS_ENV=production
    /// let config = WorkerConfig::from_env();
    /// ```
    pub fn from_env() -> Self {
        let env = std::env::var("CELERS_ENV")
            .unwrap_or_else(|_| "development".to_string())
            .to_lowercase();

        match env.as_str() {
            "production" | "prod" => Self::for_production(),
            "staging" | "stage" => Self::for_staging(),
            "development" | "dev" => Self::for_development(),
            _ => Self::for_development(),
        }
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

    /// Check if DLQ is enabled
    pub fn has_dlq(&self) -> bool {
        self.enable_dlq
    }

    /// Check if routing is enabled
    pub fn has_routing(&self) -> bool {
        self.enable_routing
    }

    /// Check if using new retry configuration
    pub fn has_retry_config(&self) -> bool {
        self.retry_config.is_some()
    }

    /// Get the effective retry configuration
    ///
    /// Returns the new retry_config if set, otherwise creates a legacy config
    /// from retry_base_delay_ms and retry_max_delay_ms
    pub fn get_retry_config(&self) -> RetryConfig {
        self.retry_config.clone().unwrap_or_else(|| {
            // Use legacy config
            RetryConfig::new(
                self.max_retries,
                RetryStrategy::Exponential {
                    base_delay: Duration::from_millis(self.retry_base_delay_ms),
                    max_delay: Duration::from_millis(self.retry_max_delay_ms),
                    multiplier: 2.0,
                },
            )
        })
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

        if self.enable_dlq {
            self.dlq_config.validate()?;
        }

        if let Some(ref retry_config) = self.retry_config {
            retry_config.validate()?;
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
        if self.enable_dlq {
            write!(f, ", dlq=enabled")?;
        }
        if self.enable_routing {
            write!(f, ", routing=enabled")?;
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

    /// Set the retry configuration (strategy and options)
    ///
    /// This overrides retry_base_delay_ms and retry_max_delay_ms
    pub fn retry_config(mut self, config: RetryConfig) -> Self {
        self.config.retry_config = Some(config);
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

    /// Enable Dead Letter Queue for permanently failed tasks
    ///
    /// Default: false
    pub fn enable_dlq(mut self, enabled: bool) -> Self {
        self.config.enable_dlq = enabled;
        self
    }

    /// Set the DLQ configuration
    pub fn dlq_config(mut self, config: DlqConfig) -> Self {
        self.config.dlq_config = config;
        self
    }

    /// Enable worker tagging and routing
    ///
    /// Default: false
    pub fn enable_routing(mut self, enabled: bool) -> Self {
        self.config.enable_routing = enabled;
        self
    }

    /// Set worker tags for task routing
    pub fn worker_tags(mut self, tags: WorkerTags) -> Self {
        self.config.worker_tags = tags;
        self
    }

    /// Set routing strategy
    pub fn routing_strategy(mut self, strategy: RoutingStrategy) -> Self {
        self.config.routing_strategy = strategy;
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

    /// Set the worker metadata
    ///
    /// Default: Auto-generated metadata with package version
    pub fn metadata(mut self, metadata: WorkerMetadata) -> Self {
        self.config.metadata = metadata;
        self
    }

    /// Set the feature flags
    ///
    /// Default: No features enabled
    pub fn feature_flags(mut self, flags: FeatureFlags) -> Self {
        self.config.feature_flags = flags;
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

/// Handle for controlling a running worker
pub struct WorkerHandle {
    pub(crate) shutdown_tx: mpsc::Sender<()>,
    pub(crate) mode: Arc<AtomicU8>,
    pub(crate) stats: Arc<WorkerStats>,
    pub(crate) dynamic_config: Arc<RwLock<DynamicConfig>>,
}

impl WorkerHandle {
    /// Request graceful shutdown of the worker
    pub async fn shutdown(&self) -> Result<()> {
        self.shutdown_tx.send(()).await.map_err(|_| {
            celers_core::CelersError::Other("Failed to send shutdown signal".to_string())
        })?;
        Ok(())
    }

    /// Enter maintenance mode (stop accepting new tasks, complete existing ones)
    pub fn enter_maintenance(&self) {
        info!("Worker entering maintenance mode");
        self.mode
            .store(WorkerMode::Maintenance as u8, Ordering::SeqCst);
    }

    /// Exit maintenance mode (resume normal operation)
    pub fn exit_maintenance(&self) {
        info!("Worker exiting maintenance mode");
        self.mode.store(WorkerMode::Normal as u8, Ordering::SeqCst);
    }

    /// Start draining (gracefully stop after completing current tasks)
    pub async fn drain(&self) -> Result<()> {
        info!("Worker starting drain");
        self.mode
            .store(WorkerMode::Draining as u8, Ordering::SeqCst);

        // Wait for all active tasks to complete
        while self.stats.active() > 0 {
            debug!(
                "Waiting for {} active tasks to complete",
                self.stats.active()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        info!("Worker drain complete");
        Ok(())
    }

    /// Get the current worker mode
    pub fn mode(&self) -> WorkerMode {
        WorkerMode::from(self.mode.load(Ordering::SeqCst))
    }

    /// Get the worker statistics
    pub fn stats(&self) -> &WorkerStats {
        &self.stats
    }

    /// Update the dynamic configuration
    ///
    /// This allows changing certain configuration parameters without restarting the worker.
    /// Returns an error if the new configuration is invalid.
    pub fn update_config(&self, config: DynamicConfig) -> std::result::Result<(), String> {
        config.validate()?;

        let mut current = self
            .dynamic_config
            .write()
            .map_err(|e| format!("Failed to acquire write lock: {}", e))?;
        info!(
            "Updating worker config: poll_interval={}ms, timeout={}s, max_retries={}",
            config.poll_interval_ms, config.default_timeout_secs, config.max_retries
        );
        *current = config;
        Ok(())
    }

    /// Get a copy of the current dynamic configuration
    pub fn get_config(&self) -> DynamicConfig {
        self.dynamic_config
            .read()
            .map(|c| c.clone())
            .unwrap_or_default()
    }

    /// Update the poll interval (in milliseconds)
    pub fn set_poll_interval(&self, interval_ms: u64) {
        if let Ok(mut config) = self.dynamic_config.write() {
            info!("Updating poll interval to {}ms", interval_ms);
            config.poll_interval_ms = interval_ms;
        } else {
            warn!("Failed to acquire write lock for poll interval update");
        }
    }

    /// Update the default task timeout (in seconds)
    pub fn set_timeout(&self, timeout_secs: u64) -> std::result::Result<(), String> {
        if timeout_secs == 0 {
            return Err("Timeout must be at least 1 second".to_string());
        }
        let mut config = self
            .dynamic_config
            .write()
            .map_err(|e| format!("Failed to acquire write lock: {}", e))?;
        info!("Updating default timeout to {}s", timeout_secs);
        config.default_timeout_secs = timeout_secs;
        Ok(())
    }

    /// Update the maximum retry count
    pub fn set_max_retries(&self, max_retries: u32) {
        if let Ok(mut config) = self.dynamic_config.write() {
            info!("Updating max retries to {}", max_retries);
            config.max_retries = max_retries;
        } else {
            warn!("Failed to acquire write lock for max retries update");
        }
    }
}
