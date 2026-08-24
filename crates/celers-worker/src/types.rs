//! Types, structs, enums, and configuration for the worker runtime.

use crate::adaptive_poll::AdaptivePollConfig;
use crate::affinity::WorkerLabels;
use crate::batching::BatchConfig;
use crate::circuit_breaker::CircuitBreakerConfig;
use crate::dlq::DlqConfig;
use crate::feature_flags::{FeatureFlags, TaskFeatureRequirements};
use crate::metadata::WorkerMetadata;
use crate::retry::{RetryConfig, RetryStrategy};
use crate::routing::{RoutingStrategy, WorkerTags};

use celers_core::Result;
use std::collections::HashMap;
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

    /// Name of the queue this worker consumes from.
    ///
    /// Used for cluster-wide rate limiting when
    /// [`RateLimitKeyStrategy::Queue`](crate::RateLimitKeyStrategy::Queue) is
    /// selected, so per-queue budgets are actually per queue.
    pub queue_name: String,

    /// Enable graceful shutdown.
    ///
    /// When set (the default), a shutdown signal stops the worker from
    /// dequeuing and waits up to [`shutdown_timeout_secs`](Self::shutdown_timeout_secs)
    /// for in-flight tasks to finish. Messages that are still undisposed when
    /// the deadline expires (or immediately, when this is `false`) are requeued
    /// so the broker redelivers them instead of stranding them.
    pub graceful_shutdown: bool,

    /// How long a graceful shutdown waits for in-flight tasks (seconds).
    ///
    /// `0` disables draining entirely (in-flight messages are requeued at once).
    pub shutdown_timeout_secs: u64,

    /// Base delay applied before polling again when every dequeued message was
    /// deferred by admission control (routing / affinity / feature flags /
    /// rate limiting), in milliseconds.
    ///
    /// Without it a task no worker can currently serve produces a
    /// dequeue -> requeue -> dequeue spin at 100% CPU.
    pub defer_delay_ms: u64,

    /// Upper bound for deferral delays, in milliseconds.
    ///
    /// Also clamps externally supplied hints such as a distributed rate
    /// limiter's `retry_after`, which can be effectively unbounded for a
    /// zero-rate limiter.
    pub defer_max_delay_ms: u64,

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

    // Adaptive polling options
    /// Use an adaptive poll-interval controller instead of the fixed
    /// `poll_interval_ms` when the queue is empty. The controller backs the
    /// interval off toward `adaptive_poll_config.max_interval_ms` on repeated
    /// empties and snaps it back toward the minimum when work is found.
    pub enable_adaptive_poll: bool,

    /// Configuration for the adaptive poll-interval controller (used only when
    /// `enable_adaptive_poll` is true).
    pub adaptive_poll_config: AdaptivePollConfig,

    // Task coalescing options
    /// Coalesce duplicate tasks (sharing a coalescing key) within each dequeued
    /// batch before processing, dropping redundant work. Most effective together
    /// with `enable_batch_dequeue`.
    pub enable_coalescing: bool,

    /// Configuration for batch coalescing (used only when `enable_coalescing` is
    /// true).
    pub coalescing_config: BatchConfig,

    /// Restrict coalescing to true redelivery duplicates (same task id).
    ///
    /// **Defaults to `true`**, which is the only lossless setting. Disabling it
    /// widens the coalescing key to `(task name, payload hash)`, so two
    /// *independent* submissions with identical arguments — different task ids
    /// — collapse into one: the dropped one never runs, never produces a result
    /// and leaves its caller waiting forever on an `AsyncResult` that can never
    /// resolve. Only turn it off for genuinely idempotent, fire-and-forget work
    /// where nobody is waiting on the second submission's result.
    pub coalesce_require_same_task_id: bool,

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
    /// Enable Dead Letter Queue for permanently failed tasks.
    ///
    /// This flag is authoritative: the worker enables the handler built from
    /// [`dlq_config`](Self::dlq_config) even when that config's own `enabled`
    /// field is left at its `false` default.
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

    // Task-affinity options
    /// Capability labels advertised by this worker for label-based task-affinity
    /// admission (matched against per-task [`crate::affinity::TaskAffinity`] when
    /// an affinity registry is installed via
    /// [`Worker::with_affinity`](crate::Worker::with_affinity)). Empty by
    /// default, in which case any task declaring required labels is deferred.
    pub worker_labels: WorkerLabels,

    // Event emission options
    /// Worker hostname for event identification
    pub hostname: String,

    /// Enable event emission for task and worker lifecycle events
    pub enable_events: bool,

    /// Capacity of the lifecycle-event buffer.
    ///
    /// Events are handed to a background emitter instead of being awaited on
    /// the task critical path; when the buffer is full events are dropped
    /// (and counted) rather than back-pressuring task execution.
    pub event_buffer_capacity: usize,

    /// Heartbeat interval in seconds (0 = disabled)
    pub heartbeat_interval_secs: u64,

    /// Worker metadata (version, build info, labels)
    pub metadata: WorkerMetadata,

    /// Feature flags enabled on this worker
    pub feature_flags: FeatureFlags,

    /// Per-task feature requirements matched against
    /// [`feature_flags`](Self::feature_flags) before a task is executed.
    ///
    /// A task whose requirements this worker does not satisfy is deferred
    /// (requeued) for a worker that does. Tasks with no entry here are admitted
    /// unconditionally, so this is a no-op until populated.
    pub task_feature_requirements: HashMap<String, TaskFeatureRequirements>,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            concurrency: 4,
            poll_interval_ms: 1000,
            queue_name: "celery".to_string(),
            graceful_shutdown: true,
            shutdown_timeout_secs: 30,
            defer_delay_ms: 250,
            defer_max_delay_ms: 2000,
            max_retries: 3,
            retry_base_delay_ms: 1000,
            retry_max_delay_ms: 60000,
            retry_config: None, // Use legacy config by default
            default_timeout_secs: 300,
            enable_batch_dequeue: false,
            batch_size: 10,
            enable_adaptive_poll: false,
            adaptive_poll_config: AdaptivePollConfig::default(),
            enable_coalescing: false,
            coalescing_config: BatchConfig::default(),
            coalesce_require_same_task_id: true,
            max_result_size_bytes: 0, // unlimited
            track_memory_usage: false,
            enable_circuit_breaker: false,
            circuit_breaker_config: CircuitBreakerConfig::default(),
            enable_dlq: false,
            dlq_config: DlqConfig::default(),
            enable_routing: false,
            worker_tags: WorkerTags::new(),
            routing_strategy: RoutingStrategy::default(),
            worker_labels: WorkerLabels::new(),
            hostname: gethostname(),
            enable_events: false,
            event_buffer_capacity: 1024,
            heartbeat_interval_secs: 0, // disabled by default
            metadata: WorkerMetadata::default(),
            feature_flags: FeatureFlags::default(),
            task_feature_requirements: HashMap::new(),
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

        if self.enable_adaptive_poll {
            self.adaptive_poll_config.validate()?;
        }

        if self.enable_coalescing {
            self.coalescing_config.validate()?;
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

        if self.defer_max_delay_ms < self.defer_delay_ms {
            return Err(
                "Max deferral delay must be greater than or equal to the deferral delay"
                    .to_string(),
            );
        }

        if self.enable_events && self.event_buffer_capacity == 0 {
            return Err(
                "Event buffer capacity must be at least 1 when events are enabled".to_string(),
            );
        }

        for (task_name, requirements) in &self.task_feature_requirements {
            requirements
                .validate()
                .map_err(|e| format!("Invalid feature requirements for '{task_name}': {e}"))?;
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
        if self.graceful_shutdown {
            write!(f, ", drain={}s", self.shutdown_timeout_secs)?;
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

    /// Set the name of the queue this worker consumes from
    ///
    /// Used as the rate-limit key under
    /// [`RateLimitKeyStrategy::Queue`](crate::RateLimitKeyStrategy::Queue).
    ///
    /// Default: `"celery"`
    pub fn queue_name(mut self, queue: impl Into<String>) -> Self {
        self.config.queue_name = queue.into();
        self
    }

    /// Enable or disable graceful shutdown
    ///
    /// Default: true
    pub fn graceful_shutdown(mut self, enabled: bool) -> Self {
        self.config.graceful_shutdown = enabled;
        self
    }

    /// Set how long a graceful shutdown waits for in-flight tasks (seconds)
    ///
    /// Default: 30
    pub fn shutdown_timeout_secs(mut self, timeout_secs: u64) -> Self {
        self.config.shutdown_timeout_secs = timeout_secs;
        self
    }

    /// Set the base delay applied when admission control defers every dequeued
    /// message (milliseconds)
    ///
    /// Default: 250ms
    pub fn defer_delay_ms(mut self, delay_ms: u64) -> Self {
        self.config.defer_delay_ms = delay_ms;
        self
    }

    /// Set the upper bound for deferral delays (milliseconds)
    ///
    /// Default: 2000ms
    pub fn defer_max_delay_ms(mut self, delay_ms: u64) -> Self {
        self.config.defer_max_delay_ms = delay_ms;
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

    /// Enable the adaptive poll-interval controller.
    ///
    /// When enabled, the worker backs off its empty-queue poll interval up to
    /// `adaptive_poll_config.max_interval_ms` on repeated empties and snaps it
    /// back toward the minimum when work is found.
    ///
    /// Default: false
    pub fn enable_adaptive_poll(mut self, enabled: bool) -> Self {
        self.config.enable_adaptive_poll = enabled;
        self
    }

    /// Set the adaptive poll-interval controller configuration.
    ///
    /// Has effect only when adaptive polling is enabled.
    pub fn adaptive_poll_config(mut self, config: AdaptivePollConfig) -> Self {
        self.config.adaptive_poll_config = config;
        self
    }

    /// Enable coalescing of duplicate tasks within each dequeued batch.
    ///
    /// Default: false
    pub fn enable_coalescing(mut self, enabled: bool) -> Self {
        self.config.enable_coalescing = enabled;
        self
    }

    /// Set the batch coalescing configuration.
    ///
    /// Has effect only when coalescing is enabled.
    pub fn coalescing_config(mut self, config: BatchConfig) -> Self {
        self.config.coalescing_config = config;
        self
    }

    /// Coalesce only repeated deliveries of the *same* task id (lossless).
    ///
    /// Turning this **off** widens the key to `(task name, payload hash)`, so
    /// independent submissions with identical arguments coalesce into one and
    /// the dropped submissions never produce a result.
    ///
    /// Default: true
    pub fn coalesce_require_same_task_id(mut self, enabled: bool) -> Self {
        self.config.coalesce_require_same_task_id = enabled;
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

    /// Set the capability labels advertised by this worker for label-based
    /// task-affinity admission.
    ///
    /// These labels are matched against each task's
    /// [`crate::affinity::TaskAffinity`] when an affinity registry is installed
    /// via [`Worker::with_affinity`](crate::Worker::with_affinity). Empty by
    /// default.
    pub fn worker_labels(mut self, labels: WorkerLabels) -> Self {
        self.config.worker_labels = labels;
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

    /// Declare the feature requirements of a task type.
    ///
    /// Before executing a task the worker checks its
    /// [`feature_flags`](WorkerConfig::feature_flags) against these
    /// requirements and defers the task if they are not satisfied.
    pub fn task_features(
        mut self,
        task_name: impl Into<String>,
        requirements: TaskFeatureRequirements,
    ) -> Self {
        self.config
            .task_feature_requirements
            .insert(task_name.into(), requirements);
        self
    }

    /// Set the lifecycle-event buffer capacity
    ///
    /// Default: 1024
    pub fn event_buffer_capacity(mut self, capacity: usize) -> Self {
        self.config.event_buffer_capacity = capacity;
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
    /// Total number of tasks revoked (cancelled) since worker start
    revoked: AtomicU64,
    /// Total number of retry attempts re-enqueued since worker start
    retried: AtomicU64,
    /// Total number of tasks deferred due to distributed rate limiting
    rate_limited: AtomicU64,
    /// Total number of tasks deferred by admission control (routing, affinity,
    /// feature flags, rate limiting)
    deferred: AtomicU64,
    /// Total number of task handlers that panicked
    panicked: AtomicU64,
    /// Total number of tasks whose **soft** time limit expired while they were
    /// still running (Celery's `SoftTimeLimitExceeded`)
    soft_timeouts: AtomicU64,
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

    /// Get the number of tasks revoked (cancelled) during execution
    pub fn revoked(&self) -> u64 {
        self.revoked.load(Ordering::Relaxed)
    }

    /// Get the number of retry attempts re-enqueued since worker start.
    ///
    /// Counts *attempts scheduled*, not distinct tasks: a task retried three
    /// times contributes three.
    pub fn retried(&self) -> u64 {
        self.retried.load(Ordering::Relaxed)
    }

    /// Get the number of tasks deferred due to distributed rate limiting
    pub fn rate_limited(&self) -> u64 {
        self.rate_limited.load(Ordering::Relaxed)
    }

    /// Get the number of tasks deferred by admission control (routing,
    /// affinity, feature flags or rate limiting)
    pub fn deferred(&self) -> u64 {
        self.deferred.load(Ordering::Relaxed)
    }

    /// Get the number of task handlers that panicked
    pub fn panicked(&self) -> u64 {
        self.panicked.load(Ordering::Relaxed)
    }

    /// Get the number of tasks whose soft time limit expired while running.
    ///
    /// A soft-limit expiry is a *warning*, not a disposition: the task keeps
    /// running (and may still succeed) until its hard limit, so this counter is
    /// independent of `processed` / `revoked`.
    pub fn soft_timeouts(&self) -> u64 {
        self.soft_timeouts.load(Ordering::Relaxed)
    }

    /// Increment the active task count (called when a task starts)
    pub fn task_started(&self) {
        self.active.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement the active task count and increment processed (called when a task completes)
    ///
    /// The decrement saturates at zero: both drain implementations wait for
    /// `active() == 0`, so an unbalanced decrement wrapping `u64` around to
    /// `u64::MAX` would hang graceful shutdown forever.
    pub fn task_completed(&self) {
        let _ = self
            .active
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                if current == 0 {
                    None
                } else {
                    Some(current - 1)
                }
            });
        self.processed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a task was revoked (cancelled) during execution
    pub fn task_revoked(&self) {
        self.revoked.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a retry attempt was scheduled for a failed task
    pub fn task_retried(&self) {
        self.retried.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a task was deferred/skipped due to distributed rate limiting
    pub fn task_rate_limited(&self) {
        self.rate_limited.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a task was deferred (requeued) by admission control
    pub fn task_deferred(&self) {
        self.deferred.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a task handler panicked
    pub fn task_panicked(&self) {
        self.panicked.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a task's soft time limit expired while it was still running
    pub fn task_soft_timeout(&self) {
        self.soft_timeouts.fetch_add(1, Ordering::Relaxed);
    }
}

impl Clone for WorkerStats {
    fn clone(&self) -> Self {
        Self {
            active: AtomicU64::new(self.active.load(Ordering::Relaxed)),
            processed: AtomicU64::new(self.processed.load(Ordering::Relaxed)),
            revoked: AtomicU64::new(self.revoked.load(Ordering::Relaxed)),
            retried: AtomicU64::new(self.retried.load(Ordering::Relaxed)),
            rate_limited: AtomicU64::new(self.rate_limited.load(Ordering::Relaxed)),
            deferred: AtomicU64::new(self.deferred.load(Ordering::Relaxed)),
            panicked: AtomicU64::new(self.panicked.load(Ordering::Relaxed)),
            soft_timeouts: AtomicU64::new(self.soft_timeouts.load(Ordering::Relaxed)),
        }
    }
}

/// Handle for controlling a running worker
pub struct WorkerHandle {
    pub(crate) shutdown_tx: mpsc::Sender<()>,
    pub(crate) mode: Arc<AtomicU8>,
    pub(crate) stats: Arc<WorkerStats>,
    pub(crate) dynamic_config: Arc<RwLock<DynamicConfig>>,
    pub(crate) health: crate::health::HealthChecker,
}

impl WorkerHandle {
    /// Liveness/readiness accounting for the running worker.
    ///
    /// The worker itself is moved into its run loop by
    /// [`Worker::run_with_shutdown`](crate::Worker::run_with_shutdown), so this
    /// is how an embedder reaches the health state afterwards — to serve a
    /// Kubernetes `livenessProbe` / `readinessProbe`, for example.
    pub fn health(&self) -> crate::health::HealthChecker {
        self.health.clone()
    }

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
    ///
    /// Waits until every in-flight task has finished. The worker's own run loop
    /// applies [`WorkerConfig::shutdown_timeout_secs`] as a hard deadline and
    /// requeues whatever is still running when it expires, so this call cannot
    /// wait indefinitely on a hung task once that deadline passes; use
    /// [`drain_with_timeout`](Self::drain_with_timeout) to bound the wait on
    /// the caller's side as well.
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

    /// Start draining, waiting at most `timeout` for in-flight tasks.
    ///
    /// Returns `true` when every task finished within the deadline and `false`
    /// when tasks were still running (the worker's run loop then requeues their
    /// messages so the broker redelivers them).
    pub async fn drain_with_timeout(&self, timeout: Duration) -> Result<bool> {
        match tokio::time::timeout(timeout, self.drain()).await {
            Ok(result) => result.map(|()| true),
            Err(_elapsed) => {
                warn!(
                    "Worker drain timed out after {:?} with {} task(s) still active",
                    timeout,
                    self.stats.active()
                );
                Ok(false)
            }
        }
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
