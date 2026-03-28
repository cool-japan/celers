use std::future::Future;
use std::pin::Pin;

/// Common result type for async operations with Send + Sync error bounds
///
/// This type alias helps IDEs provide better autocomplete and type hints.
///
/// # Example
///
/// ```rust
/// use celers::ide_support::BoxedResult;
///
/// async fn my_operation() -> BoxedResult<String> {
///     Ok("success".to_string())
/// }
/// ```
pub type BoxedResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Pinned boxed future for async task functions
///
/// This is the standard return type for async task implementations.
///
/// # Example
///
/// ```rust
/// use celers::ide_support::BoxedFuture;
///
/// fn create_task() -> BoxedFuture<String> {
///     Box::pin(async { Ok("result".to_string()) })
/// }
/// ```
pub type BoxedFuture<T> = Pin<Box<dyn Future<Output = BoxedResult<T>> + Send + 'static>>;

/// Task function signature for type hints
///
/// Use this when defining task handler functions for better IDE support.
pub type TaskFn<Args, Output> =
    fn(Args) -> Pin<Box<dyn Future<Output = BoxedResult<Output>> + Send>>;

/// Broker instance type for common use cases
///
/// This type alias helps with IDE autocomplete when working with brokers.
pub type BoxedBroker = Box<dyn crate::Broker>;

/// Result backend instance type
///
/// Use this when working with result backends for better type hints.
#[cfg(feature = "backend-redis")]
pub type BoxedResultBackend = Box<dyn crate::ResultBackend>;

/// Worker configuration builder type for fluent API
///
/// This alias improves IDE support when building worker configurations.
pub type WorkerBuilder = celers_worker::WorkerConfigBuilder;

/// Signature builder type for creating task signatures
///
/// Use this for better autocomplete when building task signatures.
pub type TaskSignature = crate::Signature;

/// Chain workflow builder type
pub type ChainBuilder = crate::Chain;

/// Group workflow builder type
pub type GroupBuilder = crate::Group;

/// Chord workflow builder type
pub type ChordBuilder = crate::Chord;

/// Serialized task type for queue operations
pub type QueueTask = crate::SerializedTask;

/// Task metadata type
pub use crate::TaskState;

/// Broker error type
pub use crate::BrokerError;

/// Task ID type for tracking tasks
pub type TaskId = uuid::Uuid;

/// Task result value type
pub use crate::TaskResultValue;

/// Event emitter type for task events
pub type EventEmitter = Box<dyn crate::EventEmitter>;

/// Async result type for task result retrieval
pub use crate::AsyncResult;

/// Worker statistics type
pub use crate::WorkerStats;

/// Task options type for task configuration
pub use crate::TaskOptions;

/// Rate limiter configuration type
pub use crate::RateLimitConfig;

/// Router type for task routing
pub use crate::Router;

/// Queue name type for better readability
///
/// Use this when defining queue names for clearer code intent.
pub type QueueName = String;

/// Broker URL type for configuration
///
/// Use this when defining broker connection strings.
pub type BrokerUrl = String;

/// Retry count type for task configuration
///
/// Use this when specifying maximum retry attempts.
pub type RetryCount = u32;

/// Priority level type for task prioritization
///
/// Valid range: 0-9, where 9 is highest priority.
pub type PriorityLevel = u8;

/// Timeout duration in seconds
///
/// Use this when specifying task time limits and countdowns.
pub type TimeoutSeconds = u64;

/// Task name type for task identification
///
/// Use this when defining or referencing task names.
pub type TaskName = String;

/// Concurrency level type for worker configuration
///
/// Represents the number of concurrent tasks a worker can execute.
pub type ConcurrencyLevel = usize;

/// Prefetch count type for worker configuration
///
/// Number of tasks to prefetch from the broker.
pub type PrefetchCount = usize;

/// Type trait bounds helper for task arguments
///
/// This trait bound helps IDEs understand the requirements for task arguments.
pub trait TaskArgs: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static {}
impl<T> TaskArgs for T where
    T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static
{
}

/// Type trait bounds helper for task results
///
/// This trait bound helps IDEs understand the requirements for task results.
pub trait TaskResult: serde::Serialize + serde::de::DeserializeOwned + Send + 'static {}
impl<T> TaskResult for T where T: serde::Serialize + serde::de::DeserializeOwned + Send + 'static {}

/// Marker trait for broker implementations
///
/// Helps IDEs identify valid broker types.
pub trait BrokerImpl: crate::Broker + Send + Sync {}

/// Helper constants for common configuration values
pub mod defaults {
    /// Default concurrency level (number of CPU cores)
    pub const DEFAULT_CONCURRENCY: usize = 4;

    /// Default prefetch count
    pub const DEFAULT_PREFETCH: usize = 10;

    /// Default max retries
    pub const DEFAULT_MAX_RETRIES: u32 = 3;

    /// Default retry delay in seconds
    pub const DEFAULT_RETRY_DELAY_SECS: u64 = 60;

    /// Default task timeout in seconds
    pub const DEFAULT_TASK_TIMEOUT_SECS: u64 = 3600;

    /// Default broker port for Redis
    pub const DEFAULT_REDIS_PORT: u16 = 6379;

    /// Default broker port for PostgreSQL
    pub const DEFAULT_POSTGRES_PORT: u16 = 5432;

    /// Default broker port for MySQL
    pub const DEFAULT_MYSQL_PORT: u16 = 3306;

    /// Default broker port for RabbitMQ
    pub const DEFAULT_RABBITMQ_PORT: u16 = 5672;

    /// Default queue name
    pub const DEFAULT_QUEUE_NAME: &str = "celery";
}

/// Common configuration patterns and examples
pub mod examples {
    /// Example broker URL for Redis
    pub const REDIS_URL_EXAMPLE: &str = "redis://localhost:6379";

    /// Example broker URL for PostgreSQL
    pub const POSTGRES_URL_EXAMPLE: &str = "postgres://user:password@localhost:5432/celery";

    /// Example broker URL for MySQL
    pub const MYSQL_URL_EXAMPLE: &str = "mysql://user:password@localhost:3306/celery";

    /// Example broker URL for RabbitMQ
    pub const RABBITMQ_URL_EXAMPLE: &str = "amqp://guest:guest@localhost:5672/";

    /// Example broker URL for SQS
    pub const SQS_URL_EXAMPLE: &str = "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue";
}
