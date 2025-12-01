//! CeleRS Core - Type-safe distributed task queue library
//!
//! This crate provides the core abstractions and types for building
//! distributed task processing systems in Rust.

pub mod broker;
pub mod control;
pub mod error;
pub mod event;
pub mod exception;
pub mod executor;
pub mod rate_limit;
pub mod result;
pub mod retry;
pub mod revocation;
pub mod router;
pub mod state;
pub mod task;
pub mod time_limit;

pub use broker::{Broker, BrokerMessage};
pub use control::{
    ActiveTaskInfo, BrokerStats, ControlCommand, ControlResponse, DeliveryInfo, InspectCommand,
    InspectResponse, PoolStats, QueueCommand, QueueResponse, QueueStats, RequestInfo,
    ReservedTaskInfo, ScheduledTaskInfo, WorkerConf, WorkerReport, WorkerStats,
};
pub use error::{CelersError, Result};
pub use event::{
    CompositeEventEmitter, Event, EventEmitter, InMemoryEventEmitter, LogLevel,
    LoggingEventEmitter, NoOpEventEmitter, TaskEvent, TaskEventBuilder, WorkerEvent,
    WorkerEventBuilder,
};
pub use exception::{
    ExceptionAction, ExceptionCategory, ExceptionHandler, ExceptionHandlerChain, ExceptionPolicy,
    LoggingExceptionHandler, PolicyExceptionHandler, TaskException, TracebackFrame,
};
pub use executor::TaskRegistry;
pub use rate_limit::{
    create_rate_limiter, RateLimitConfig, RateLimiter, SlidingWindow, TaskRateLimiter, TokenBucket,
    WorkerRateLimiter,
};
pub use result::{AsyncResult, ResultStore, TaskResultValue};
pub use retry::{RetryPolicy, RetryStrategy};
pub use revocation::{
    PatternRevocation, RevocationManager, RevocationMode, RevocationRequest, RevocationResult,
    RevocationState, WorkerRevocationManager,
};
pub use router::{
    ArgumentCondition, GlobPattern, PatternMatcher, RegexPattern, RouteResult, RouteRule, Router,
    RouterBuilder, RoutingConfig,
};
pub use state::{StateHistory, StateTransition, TaskState};
pub use task::{SerializedTask, Task, TaskId, TaskMetadata};
pub use time_limit::{
    TaskTimeLimits, TimeLimit, TimeLimitConfig, TimeLimitExceeded, TimeLimitSettings,
    TimeLimitStatus, WorkerTimeLimits,
};
