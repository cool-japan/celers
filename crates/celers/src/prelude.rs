// Core types
pub use crate::AsyncResult;
pub use crate::Broker;
pub use crate::CompositeEventEmitter;
pub use crate::ControlCommand;
pub use crate::ControlResponse;
pub use crate::Event;
pub use crate::EventEmitter;
pub use crate::InMemoryEventEmitter;
pub use crate::InspectCommand;
pub use crate::InspectResponse;
pub use crate::LogLevel;
pub use crate::LoggingEventEmitter;
pub use crate::NoOpEventEmitter;
pub use crate::ResultStore;
pub use crate::SerializedTask;
pub use crate::TaskEvent;
pub use crate::TaskEventBuilder;
pub use crate::TaskResultValue;
pub use crate::TaskState;
pub use crate::WorkerEvent;
pub use crate::WorkerEventBuilder;
pub use crate::WorkerStats;

// Rate limiting types
pub use crate::RateLimitConfig;
pub use crate::RateLimiter;
pub use crate::SlidingWindow;
pub use crate::TaskRateLimiter;
pub use crate::TokenBucket;
pub use crate::WorkerRateLimiter;

// Task routing types
pub use crate::GlobPattern;
pub use crate::PatternMatcher;
pub use crate::RegexPattern;
pub use crate::RouteResult;
pub use crate::RouteRule;
pub use crate::Router;
pub use crate::RouterBuilder;
pub use crate::RoutingConfig;

// Worker types
pub use crate::Worker;
pub use crate::WorkerConfig;
pub use celers_worker::WorkerConfigBuilder;

// Broker helper functions
pub use crate::broker_helper::{create_broker, create_broker_from_env, BrokerConfigError};

// Configuration validation
pub use crate::config_validation::{
    check_feature_compatibility, feature_compatibility_matrix, validate_broker_url,
    validate_worker_config, ConfigValidator, ValidationError,
};

// Broker implementations
// Note: monitoring and utilities modules are available at crate root level
// when the corresponding broker feature is enabled, but not re-exported in
// prelude to avoid naming conflicts when multiple brokers are used.
#[cfg(feature = "redis")]
pub use crate::{circuit_breaker, dedup, health, rate_limit, RedisBroker};

#[cfg(feature = "postgres")]
pub use crate::PostgresBroker;

#[cfg(feature = "mysql")]
pub use crate::MysqlBroker;

#[cfg(feature = "amqp")]
pub use crate::AmqpBroker;

#[cfg(feature = "sqs")]
pub use crate::{optimization, SqsBroker};

// Backend implementations
#[cfg(feature = "backend-redis")]
pub use crate::{
    batch_size, ttl, ChordState, RedisEventConfig, RedisEventEmitter, RedisEventReceiver,
    RedisResultBackend, ResultBackend, TaskMeta,
};

#[cfg(feature = "backend-db")]
pub use crate::{MysqlResultBackend, PostgresResultBackend};

#[cfg(feature = "backend-rpc")]
pub use crate::GrpcResultBackend;

// Beat scheduler
#[cfg(feature = "beat")]
pub use crate::{BeatScheduler, Schedule, ScheduledTask};

// Metrics
#[cfg(feature = "metrics")]
pub use crate::{gather_metrics, reset_metrics};

// Tracing
#[cfg(feature = "tracing")]
pub use crate::tracing::{
    create_tracer_provider, extract_trace_context, init_tracing, inject_trace_context,
    publish_span, task_span,
};

// Macros (task attribute and Task derive)
pub use celers_macros::{task, Task};

// Canvas primitives
pub use crate::{
    Chain, Chord, Chunks, Group, Map, Signature, Starmap, TaskOptions, XMap, XStarmap,
};

// Error types
pub use crate::BrokerError;

// Common external crates
pub use async_trait::async_trait;
pub use serde::{Deserialize, Serialize};
pub use serde_json;
pub use serde_json::json;
pub use tokio;
pub use uuid::Uuid;

// Development utilities (test/dev-utils feature)
#[cfg(any(test, feature = "dev-utils"))]
pub use crate::dev_utils::{
    create_test_task, EventTracker, MockBroker, PerformanceProfiler, QueueInspector, TaskBuilder,
    TaskDebugger,
};

// Type aliases for common patterns
/// Result type for task execution with thread-safe error handling
pub type TaskResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
/// Async task function signature
pub type AsyncTaskFn<T> =
    fn(Vec<u8>) -> std::pin::Pin<Box<dyn std::future::Future<Output = TaskResult<T>> + Send>>;

// Re-export common Result type from kombu
pub use celers_kombu::Result as KombuResult;

// Kombu utility functions
pub use crate::utils;

// Convenience functions for ergonomic workflow creation
pub use crate::convenience::{
    batch, best_effort, chain, chain_from, chord, chunks, critical, delay, expire_in, fan_in,
    fan_out, group, group_from, high_priority, low_priority, map, options, parallel, pipeline,
    retry_with_backoff, starmap, task, task_with_options, transient, with_countdown, with_expires,
    with_priority, with_retry, with_timeout,
};

// Beat-specific convenience functions
#[cfg(feature = "beat-cron")]
pub use crate::convenience::recurring;

#[cfg(feature = "beat")]
pub use crate::convenience::recurring_interval;

// Workflow templates for common patterns
pub use crate::workflow_templates::{
    batch_processing, etl_pipeline, map_reduce_workflow, priority_workflow, scatter_gather,
    sequential_pipeline,
};

// Task composition utilities
pub use crate::task_composition::{
    circuit_breaker_group, rate_limited_workflow, retry_wrapper, timeout_wrapper,
};

// Error recovery patterns
pub use crate::error_recovery::{ignore_errors, with_dlq, with_exponential_backoff, with_fallback};

// Workflow validation utilities
pub use crate::workflow_validation::{
    check_performance_concerns_chain, check_performance_concerns_group, validate_chain,
    validate_chord, validate_group, ValidationError as WorkflowValidationError,
};

// Result aggregation helpers
pub use crate::result_helpers::{
    create_result_collector, create_result_filter, create_result_reducer, create_result_transformer,
};

// Advanced workflow patterns
pub use crate::advanced_patterns::{
    create_conditional_workflow, create_dynamic_workflow, create_parallel_chains,
    create_saga_workflow,
};

// Monitoring and observability helpers
pub use crate::monitoring_helpers::TaskMonitor;

// Batch processing helpers
pub use crate::batch_helpers::{
    create_adaptive_batches, create_dynamic_batches, create_prioritized_batches,
};

// Health check utilities
pub use crate::health_check::{
    DependencyChecker, HealthCheckResult, HealthStatus, WorkerHealthChecker,
};

// Resource management utilities
pub use crate::resource_management::{ResourceLimits, ResourcePool, ResourceTracker};

// Task lifecycle hooks
pub use crate::task_hooks::{
    HookRegistry, HookResult, LoggingHook, PostExecutionHook, PreExecutionHook, ValidationHook,
};

// Metrics aggregation utilities
pub use crate::metrics_aggregation::{DataPoint, Histogram, MetricsAggregator};

// Task cancellation utilities
pub use crate::task_cancellation::{CancellationToken, ExecutionGuard, TimeoutManager};

// Advanced retry strategies
pub use crate::retry_strategies::{
    DefaultRetryPolicy, ErrorPatternRetryPolicy, RetryPolicy, RetryStrategy,
};

// Task dependency management
pub use crate::task_dependencies::DependencyGraph;

// Performance profiling utilities
// Note: PerformanceProfiler conflicts with dev_utils::PerformanceProfiler
// Access via crate::performance_profiling::PerformanceProfiler
pub use crate::performance_profiling::{PerformanceProfile, ProfileSpan};
