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
pub mod checkpoint;
pub mod circuit_breaker;
pub mod cpu_affinity;
pub mod crash_dump;
pub mod dead_worker_detection;
pub mod degradation;
pub mod dependencies;
pub mod distributed_rate_limit;
pub mod dlq;
pub mod dlq_storage;
pub mod error_aggregation;
pub mod feature_flags;
pub mod gc_tuning;
pub mod health;
pub mod incremental_deser;
pub mod leak_detection;
pub mod lockfree_queue;
pub mod memory;
pub mod memory_pool;
pub mod metadata;
pub mod middleware;
pub mod performance_metrics;
pub mod pipeline;
pub mod prefetch;
pub mod queue_monitor;
pub mod rate_limit;
pub mod resource_tracker;
pub mod restart_manager;
pub mod retry;
pub mod routing;
pub mod sandbox;
pub mod scheduler;
pub mod shutdown;
pub mod streaming;
pub mod task_resources;
pub mod task_timeout;
pub mod types;
pub mod worker_coordination;
pub mod worker_core;
pub mod worker_pool;
pub mod zero_copy;

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

#[cfg(test)]
mod tests;

// Re-export types module
pub use types::*;

// Re-export worker core
pub use worker_core::*;

pub use cancellation::{CancellationError, CancellationRegistry, CancellationToken};
pub use checkpoint::{
    Checkpoint, CheckpointConfig, CheckpointManager, CheckpointStats, CheckpointStrategy,
};
pub use circuit_breaker::{CircuitBreaker as WorkerCircuitBreaker, CircuitState};
pub use cpu_affinity::{AffinityConfig, AffinityPolicy, NumaNode};
pub use crash_dump::{CrashDump, CrashDumpConfig, CrashDumpManager, CrashDumpStats, CrashSeverity};
pub use dead_worker_detection::{
    DeadWorkerDetector, DetectorConfig, DetectorStats, StateChangeCallback, WorkerHealthState,
};
pub use degradation::{DegradationLevel, DegradationPolicy, DegradationThresholds};
pub use dependencies::{DependencyGraph, DependencyStats, DependencyStatus, TaskDependency};
pub use dlq::{
    DlqConfig, DlqEntry, DlqHandler, DlqReprocessConfig, DlqReprocessor, DlqStats,
    TaskStats as DlqTaskStats,
};
pub use error_aggregation::{
    ErrorAggregator, ErrorAggregatorConfig, ErrorEntry, ErrorPattern, ErrorStats,
};
pub use feature_flags::{FeatureFlags, TaskFeatureRequirements};
pub use incremental_deser::{DeserConfig, DeserState, DeserStats, IncrementalDeserializer};
pub use leak_detection::{LeakDetector, LeakDetectorConfig, LeakInfo, MemorySample};
pub use lockfree_queue::LockFreeQueue;
pub use memory_pool::{MemoryPool, PoolConfig, PoolStats, PooledBuffer, SizeClass};
pub use metadata::{WorkerMetadata, WorkerMetadataBuilder};
pub use middleware::{
    Middleware, MiddlewareError, MiddlewareStack, TaskContext, TracingMiddleware,
};
pub use performance_metrics::{PerformanceConfig, PerformanceStats, PerformanceTracker};
pub use pipeline::{Pipeline, PipelineConfig, PipelineStage, PipelineStats};
pub use prefetch::{PrefetchBuffer, PrefetchConfig, PrefetchStats};
pub use queue_monitor::{QueueAlertLevel, QueueMonitor, QueueMonitorConfig, QueueStats};
pub use rate_limit::{RateLimitConfig, RateLimiter, SlidingWindowConfig, SlidingWindowLimiter};
pub use resource_tracker::{ResourceLimits, ResourceStats, ResourceTracker};
pub use restart_manager::{
    ErrorSeverity, RestartManager, RestartPolicy, RestartStats, RestartStrategy,
};
pub use retry::{RetryConfig, RetryStrategy};
pub use routing::{RoutingStrategy, TaskRoutingRequirements, WorkerTags};
pub use sandbox::{IsolationLevel, Sandbox, SandboxConfig, SandboxStats, SandboxViolation};
pub use scheduler::{
    AvailableResources, ScheduledTask, SchedulerConfig, TaskPriority, TaskRequirements,
    TaskScheduler,
};
pub use shutdown::wait_for_signal;
pub use streaming::{
    ChunkReceiver, ChunkSender, DataChunk, ResultStreamer, StreamConfig, StreamStats,
};
pub use task_resources::{ResourceConsumptionManager, ResourceUsage, TaskResourceTracker};
pub use task_timeout::{
    CleanupHook, TimeoutConfig, TimeoutContext, TimeoutManager, TimeoutStrategy,
};
pub use worker_pool::{
    ScalingDecision, ScalingPolicy, WorkerInfo, WorkerPool, WorkerPoolConfig, WorkerPoolStats,
};
pub use zero_copy::{BufferMetadata, SharedBuffer, ZeroCopyBuffer, ZeroCopyPool};

#[cfg(feature = "metrics")]
pub use middleware::MetricsMiddleware;
