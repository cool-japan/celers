//! `CeleRS` Core - Type-safe distributed task queue library
//!
//! This crate provides the core abstractions and types for building
//! distributed task processing systems in Rust.
//!
//! # Quick Start
//!
//! ## Basic Task Creation
//!
//! ```rust
//! use celers_core::{SerializedTask, TaskMetadata, TaskState};
//!
//! // Create a simple task
//! let task = SerializedTask::new("process_data".to_string(), vec![1, 2, 3, 4, 5])
//!     .with_priority(5)
//!     .with_max_retries(3)
//!     .with_timeout(60);
//!
//! assert_eq!(task.metadata.state, TaskState::Pending);
//! assert!(task.has_priority());
//! ```
//!
//! ## Task Dependencies and Workflows
//!
//! ```rust
//! use celers_core::{TaskDag, TaskId, SerializedTask};
//! use uuid::Uuid;
//!
//! // Create a DAG for task dependencies
//! let mut dag = TaskDag::new();
//!
//! let load_task = Uuid::new_v4();
//! let transform_task = Uuid::new_v4();
//! let save_task = Uuid::new_v4();
//!
//! dag.add_node(load_task, "load_data");
//! dag.add_node(transform_task, "transform_data");
//! dag.add_node(save_task, "save_results");
//!
//! // Define dependencies: transform depends on load, save depends on transform
//! dag.add_dependency(transform_task, load_task).unwrap();
//! dag.add_dependency(save_task, transform_task).unwrap();
//!
//! // Validate DAG (no cycles)
//! assert!(dag.validate().is_ok());
//!
//! // Get execution order
//! let order = dag.topological_sort().unwrap();
//! assert_eq!(order, vec![load_task, transform_task, save_task]);
//! ```
//!
//! ## Retry Strategies
//!
//! ```rust
//! use celers_core::RetryStrategy;
//!
//! // Exponential backoff
//! let strategy = RetryStrategy::exponential(1000, 2.0); // 1s initial, 2x multiplier
//!
//! assert_eq!(strategy.calculate_delay(0, None), 1000); // 1s
//! assert_eq!(strategy.calculate_delay(1, Some(1000)), 2000); // 2s
//! assert_eq!(strategy.calculate_delay(2, Some(2000)), 4000); // 4s
//!
//! // Fixed delay retry strategy
//! let fixed = RetryStrategy::fixed(5000); // 5 seconds
//! assert_eq!(fixed.calculate_delay(0, None), 5000);
//! assert_eq!(fixed.calculate_delay(10, Some(5000)), 5000);
//! ```
//!
//! ## State Tracking
//!
//! ```rust
//! use celers_core::{StateHistory, TaskState};
//!
//! let mut history = StateHistory::with_initial(TaskState::Pending);
//!
//! // Track state transitions
//! history.transition(TaskState::Running);
//! history.transition(TaskState::Succeeded(vec![1, 2, 3]));
//!
//! assert_eq!(history.transition_count(), 2);
//! assert!(history.current_state().unwrap().is_terminal());
//! ```
//!
//! ## Exception Handling
//!
//! ```rust
//! use celers_core::{TaskException, ExceptionCategory, ExceptionPolicy};
//!
//! // Create an exception policy
//! let policy = ExceptionPolicy::new()
//!     .retry_on(&["Temporary*", "Network*"])
//!     .fail_on(&["Fatal*", "Critical*"])
//!     .ignore_on(&["Ignorable*"]);
//!
//! // Build an exception
//! let exception = TaskException::new("TemporaryError", "Connection timeout")
//!     .with_category(ExceptionCategory::Retryable);
//! ```
//!
//! # Core Concepts
//!
//! - **Tasks**: Units of work that can be executed asynchronously
//! - **Brokers**: Message queue backends for task distribution
//! - **States**: Task lifecycle state machine (Pending → Running → Success/Failure)
//! - **Dependencies**: DAG-based task dependency management
//! - **Retries**: Flexible retry strategies with backoff
//! - **Routing**: Pattern-based task routing to queues
//! - **Rate Limiting**: Token bucket and sliding window algorithms

pub mod alerting;
pub mod broker;
pub mod caching_backend;
pub mod circuit_breaker_registry;
pub mod config;
pub mod control;
pub mod control_transport;
pub mod dag;
pub mod error;
pub mod event;
pub mod event_filter;
pub mod event_persistence;
pub mod exception;
pub mod executor;
pub mod in_memory_broker;
pub mod lock;
pub mod pii;
pub mod rate_limit;
pub mod rate_limit_distributed;
pub mod result;
pub mod result_groups;
pub mod result_tombstone;
pub mod result_ttl;
pub mod retry;
pub mod revocation;
pub mod revocation_channel;
pub mod router;
pub mod sanitize;
pub mod state;
pub mod task;
pub mod task_security;
pub mod task_signature;
pub mod tenant_rate_limit;
pub mod time_limit;

pub use alerting::{
    AlertEvaluator, AlertRule, AlertRuleKind, AlertStatus, EventSnapshot, EventSnapshotBuilder,
    RuleAlert,
};
pub use broker::{Broker, BrokerMessage};
pub use caching_backend::CachingResultBackend;
pub use circuit_breaker_registry::{
    CircuitBreaker, CircuitBreakerConfig, CircuitBreakerError, CircuitSnapshot, CircuitState,
    TaskTypeCircuitBreakers,
};
pub use config::{
    BackendTransport, BeatSchedule, BrokerTransport, CeleryConfig, ConfigError, ConfigValidation,
    ConfigWarning, ScheduleDefinition, TaskConfig, TaskRoute,
};
pub use control::{
    ActiveTaskInfo, BrokerStats, ControlCommand, ControlResponse, DeliveryInfo, InspectCommand,
    InspectResponse, PoolStats, QueueCommand, QueueResponse, QueueStats, RequestInfo,
    ReservedTaskInfo, ScheduledTaskInfo, WorkerConf, WorkerReport, WorkerStats,
};
pub use control_transport::{
    reply_channel, ControlClient, ControlCommandStream, ControlEnvelope, ControlReply,
    ControlReplyStream, ControlTransport, InMemoryControlTransport, DEFAULT_CONTROL_CHANNEL,
    DEFAULT_GATHER_TIMEOUT, DEFAULT_REPLY_CHANNEL_PREFIX,
};
pub use dag::{DagNode, TaskDag};
pub use error::{CelersError, Result};
pub use event::{
    event_timestamp_now, Alert, AlertCondition, AlertContext, AlertHandler, AlertManager,
    AlertSeverity, CompositeEventEmitter, Event, EventDispatcher, EventEmitter, EventEnvelope,
    EventFilter, EventMonitor, EventReceiver, EventStats, EventStorage, EventStream,
    FileEventStorage, InMemoryEventEmitter, InMemoryEventStorage, LogLevel, LoggingAlertHandler,
    LoggingEventEmitter, NoOpEventEmitter, TaskEvent, TaskEventBuilder, WorkerEvent,
    WorkerEventBuilder,
};
pub use event_filter::{
    CollectingEventHandler, CompositeEventFilter, DispatchReport, EventFilterTrait,
    EventHandlerTrait, EventRouter, ExactEventFilter, FilterMode, GlobEventFilter,
    LoggingEventHandler, PrefixEventFilter,
};
pub use event_persistence::{
    EventPersister, FileEventPersister, FileEventPersisterConfig, RotationPolicy,
};
pub use exception::{
    ExceptionAction, ExceptionCategory, ExceptionHandler, ExceptionHandlerChain, ExceptionPolicy,
    LoggingExceptionHandler, PolicyExceptionHandler, TaskException, TracebackFrame,
};
pub use executor::TaskRegistry;
pub use in_memory_broker::{InMemoryBroker, InMemoryResultBackend};
pub use lock::DistributedLockBackend;
pub use pii::{PiiConfig, PiiDetector, PiiKind, PiiMatch, PiiReport};
pub use rate_limit::{
    create_rate_limiter, DistributedRateLimiter, DistributedRateLimiterCoordinator,
    DistributedRateLimiterState, DistributedSlidingWindowSpec, DistributedTokenBucketSpec,
    RateLimitConfig, RateLimiter, SlidingWindow, TaskRateLimiter, TokenBucket, WorkerRateLimiter,
};
// Note: `rate_limit_distributed::DistributedRateLimiter` is intentionally NOT
// re-exported at the crate root to avoid clashing with the existing
// `rate_limit::DistributedRateLimiter` trait above. Access the cluster-wide
// limiter struct via `celers_core::rate_limit_distributed::DistributedRateLimiter`.
pub use rate_limit_distributed::{
    AcquireOutcome, DistributedAlgorithm, DistributedRateLimitBackend, InMemoryDistributedBackend,
    RateLimitParams,
};
pub use result::{
    AsyncResult, AsyncResultConfig, ExtendedResultStore, ResultChunk, ResultChunker,
    ResultCompressor, ResultMetadata, ResultStore, ResultTombstone, TaskResultValue,
};
pub use result_groups::{GroupChild, GroupStatus, ResultGroup};
pub use result_tombstone::{ResultExistence, TombstoneExt, TombstoneRegistry};
pub use result_ttl::{ResolvedResultTtl, ResultTtlConfig};
pub use retry::{RetryPolicy, RetryStrategy};
pub use revocation::{
    PatternRevocation, RevocationManager, RevocationMode, RevocationRequest, RevocationResult,
    RevocationState, WorkerRevocationManager,
};
pub use revocation_channel::{RevocationNotice, RevocationStream};
pub use router::{
    ArgumentCondition, GlobPattern, PatternMatcher, RegexPattern, RouteResult, RouteRule, Router,
    RouterBuilder, RoutingConfig,
};
pub use sanitize::{
    OversizeAction, SanitizeError, SanitizeReport, Sanitizer, SanitizerConfig, TaskValue, ValueKind,
};
pub use state::{StateHistory, StateTransition, TaskState};
pub use task::{SerializedTask, Task, TaskId, TaskMetadata};
pub use task_security::{
    sign_task, signed_fields, task_call_from_payload, verify_task, CallShape, HygieneReport,
    PayloadHygiene, RedactedPayload, SignatureEnvelope, SignaturePolicy, SigningOptions, TaskCall,
};
pub use task_signature::{
    FreshnessWindow, HmacSha256, ReplayGuard, Sha256, SignatureAlgorithm, SignatureError,
    SignedCallback, SignedFields, TaskSignature, TaskSigner,
};
pub use tenant_rate_limit::{TenantRateLimit, TenantRateLimiter, TenantUsage};
pub use time_limit::{
    TaskTimeLimits, TimeLimit, TimeLimitConfig, TimeLimitExceeded, TimeLimitSettings,
    TimeLimitStatus, WorkerTimeLimits,
};
