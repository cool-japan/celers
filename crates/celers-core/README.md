# celers-core

Core abstractions and traits for the CeleRS distributed task queue system.

**Status: [Stable] — v0.3.1 (2026-08-26) — 718 tests + 78 doctests**

## Overview

This crate provides the fundamental building blocks for CeleRS:
- **Task trait**: Type-safe executable task interface
- **Broker trait**: Abstract interface for message brokers (Redis, Postgres, AMQP, SQS, etc.)
- **ResultBackend trait**: Task result storage abstraction
- **TaskExecutor / TaskRegistry**: Task execution and registration
- **Error types**: Comprehensive error handling with retryability classification
- **Batch operations**: High-performance bulk enqueue/dequeue (10-100x faster)
- **CeleryConfig**: Full configuration with 23+ environment variables
- **Router**: Pattern-based task routing
- **RateLimit, TimeLimit**: Task rate and time limiting
- **Revocation**: Task cancellation and revocation
- **Retry / Exception**: Advanced retry strategies and exception policies
- **Event / Control**: Worker events and control commands
- **Distributed Locks**: `DistributedLockBackend` trait (implementations live downstream: `InMemoryLockBackend` in celers-beat, `RedisLockBackend` in celers-backend-redis, `DbLockBackend` in celers-backend-db)
- **DAG support**: Task dependency graphs with cycle detection
- **Local development mode**: `InMemoryBroker` / `InMemoryResultBackend` (full trait coverage, no external services) plus a `CachingResultBackend<B>` LRU wrapper fronting any `ResultStore`
- **Distributed rate limiting**: `DistributedRateLimiter` + `DistributedRateLimitBackend` trait (Redis-ready), `TenantRateLimiter` for per-tenant quotas, and `TaskTypeCircuitBreakers` for per-task-type circuit breaking
- **Result lifecycle**: tombstones (`TombstoneRegistry` / `ResultExistence`), result groups (`ResultGroup`), and per-task-type result TTL (`ResultTtlConfig`)
- **Alerting**: event snapshots (`EventSnapshot`) and a rule engine (`AlertRule` / `AlertEvaluator`) for failure-rate, queue-depth, and no-heartbeat alerts
- **Task security**: HMAC-SHA256 task signing (`TaskSigner`), argument sanitization (`Sanitizer`), and PII detection/masking (`PiiDetector`)

## Features

- Type-safe task definitions
- Broker abstraction for multiple backends
- Task state machine (Pending → Running → Success/Failure/Retry)
- **Batch operation support** (10-100x performance improvement)
- Workflow support (Group, Chain, Chord)
- Async/await native
- Property-based testing coverage

## Usage

```rust
use celers_core::{Broker, SerializedTask, TaskRegistry};

// Create a task
let task = SerializedTask::new(
    "my_task".to_string(),
    serde_json::to_vec(&args)?,
);

// Enqueue to broker
let task_id = broker.enqueue(task).await?;

// Batch enqueue for better performance
let task_ids = broker.enqueue_batch(tasks).await?;

// Dequeue and execute
if let Some(msg) = broker.dequeue().await? {
    let result = registry.execute(&msg.task).await?;
    broker.ack(&msg.task.metadata.id, msg.receipt_handle.as_deref()).await?;
}
```

## Broker Trait

The core abstraction for message brokers:

```rust
#[async_trait]
pub trait Broker: Send + Sync {
    // Single operations
    async fn enqueue(&self, task: SerializedTask) -> Result<TaskId>;
    async fn dequeue(&self) -> Result<Option<BrokerMessage>>;
    async fn ack(&self, task_id: &TaskId, receipt_handle: Option<&str>) -> Result<()>;
    async fn reject(&self, task_id: &TaskId, receipt_handle: Option<&str>, requeue: bool) -> Result<()>;

    // Batch operations (10-100x faster)
    async fn enqueue_batch(&self, tasks: Vec<SerializedTask>) -> Result<Vec<TaskId>>;
    async fn dequeue_batch(&self, count: usize) -> Result<Vec<BrokerMessage>>;
    async fn ack_batch(&self, tasks: &[(TaskId, Option<String>)]) -> Result<()>;

    // Delayed execution (optional, default: execute immediately via enqueue())
    async fn enqueue_at(&self, task: SerializedTask, execute_at: i64) -> Result<TaskId>;
    async fn enqueue_after(&self, task: SerializedTask, delay_secs: u64) -> Result<TaskId>;

    // Management
    async fn queue_size(&self) -> Result<usize>;
    async fn cancel(&self, task_id: &TaskId) -> Result<bool>;
}
```

## Task Types

### SerializedTask

```rust
pub struct SerializedTask {
    pub metadata: TaskMetadata,
    pub payload: Vec<u8>,       // JSON-serialized arguments
}

impl SerializedTask {
    pub fn new(name: String, payload: Vec<u8>) -> Self;
    pub fn with_priority(self, priority: i32) -> Self;
    pub fn with_max_retries(self, max_retries: u32) -> Self;
    pub fn with_timeout(self, timeout_secs: u64) -> Self;
}
```

### TaskMetadata

```rust
pub struct TaskMetadata {
    pub id: Uuid,
    pub name: String,
    pub state: TaskState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub max_retries: u32,
    pub timeout_secs: Option<u64>,
    pub priority: i32,

    // Workflow support
    pub group_id: Option<Uuid>,   // Group ID for parallel execution
    pub chord_id: Option<Uuid>,    // Chord ID for barrier synchronization
    pub on_success_link: Option<String>,  // Next task to chain-enqueue on success
    pub dependencies: HashSet<TaskId>,    // Tasks that must complete before this one
}
```

### TaskState

```rust
pub enum TaskState {
    Pending,
    Running,
    Retrying(u32),  // retry count
    Success,
    Failure,
    Cancelled,
}
```

## Task Registry

Execute registered tasks by name:

```rust
use celers_core::{Task, TaskRegistry};

// Implement `Task` for your task type (register() takes a value, not a closure)
struct MyTask;

#[async_trait::async_trait]
impl Task for MyTask {
    type Input = MyInput;
    type Output = MyOutput;

    async fn execute(&self, input: Self::Input) -> celers_core::Result<Self::Output> {
        // Task implementation
        Ok(output)
    }

    fn name(&self) -> &str {
        "my_task"
    }
}

let registry = TaskRegistry::new();

// Register by value — the task name comes from `Task::name()`
registry.register(MyTask).await;

// Execute by name
let result = registry.execute(&task).await?;
```

## Error Handling

```rust
pub enum CelersError {
    Serialization(String),
    Deserialization(String),
    Broker(String),
    TaskNotFound(String),
    TaskExecution(String),
    TaskRevoked(TaskId),
    Timeout(String),
    InvalidStateTransition { from: String, to: String },
    Configuration(String),
    Io(std::io::Error),
    Other(String),
}

impl CelersError {
    pub const fn is_retryable(&self) -> bool;
    pub const fn category(&self) -> &'static str;
}

pub type Result<T> = std::result::Result<T, CelersError>;
```

## Performance

### Batch Operations

Batch operations provide significant performance improvements:

| Operation | Individual | Batch (10) | Batch (100) |
|-----------|-----------|------------|-------------|
| Throughput | 1K/sec | 10K/sec | 50K/sec |
| Latency | 1ms/task | 0.1ms/task | 0.01ms/task |
| Network RTT | N | 1 | 1 |

### Best Practices

```rust
// Good: Batch enqueue for bulk operations
let tasks = create_many_tasks(100);
broker.enqueue_batch(tasks).await?;

// Less efficient: Individual enqueue
for task in tasks {
    broker.enqueue(task).await?;
}
```

## Workflow Support

TaskMetadata includes fields for Canvas workflow primitives:

```rust
// Group tasks
task.metadata.group_id = Some(group_id);

// Chord tasks (map-reduce)
task.metadata.chord_id = Some(chord_id);
```

See `celers-canvas` for high-level workflow APIs.

## Configuration

`CeleryConfig` supports 23+ environment variables including:

- `CELERY_BROKER_URL`, `CELERY_RESULT_BACKEND`
- `CELERY_TASK_SERIALIZER`, `CELERY_RESULT_SERIALIZER`
- `CELERY_TASK_DEFAULT_QUEUE`, `CELERY_TASK_ROUTES`
- `CELERY_WORKER_CONCURRENCY`, `CELERY_TASK_SOFT_TIME_LIMIT`, `CELERY_TASK_TIME_LIMIT`
- And more — use `CeleryConfig::from_env()` or `ConfigValidation::validate_detailed()`

## Dependencies

```toml
[dependencies]
async-trait = "0.1"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
thiserror = "2"
tokio = { version = "1", features = ["full"] }
uuid = { version = "1", features = ["v4", "serde"] }
chrono = { version = "0.4", features = ["serde"] }
tracing = "0.1"
regex = "1"
rand = "0.10"
num_cpus = "1"
```

## See Also

- **celers-broker-redis**: Redis broker with batch operations
- **celers-broker-postgres**: PostgreSQL broker implementation
- **celers-worker**: Worker runtime for executing tasks
- **celers-canvas**: High-level workflow APIs (Chain, Group, Chord)

## License

Apache-2.0
