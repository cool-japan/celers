# CeleRS Core Architecture

> Comprehensive architecture documentation for the CeleRS Core crate

## Table of Contents

1. [Overview](#overview)
2. [Design Principles](#design-principles)
3. [Module Organization](#module-organization)
4. [Core Abstractions](#core-abstractions)
5. [Data Flow](#data-flow)
6. [State Machine](#state-machine)
7. [Dependency Graph](#dependency-graph)
8. [Error Handling Strategy](#error-handling-strategy)
9. [Extension Points](#extension-points)
10. [Integration with Other Crates](#integration-with-other-crates)
11. [Performance Considerations](#performance-considerations)

---

## Overview

CeleRS Core is the foundational crate in the CeleRS distributed task queue system. It provides:

- **Core traits** for extensibility (`Task`, `Broker`, `EventEmitter`, `ExceptionHandler`)
- **Type system** for task representation (`TaskMetadata`, `SerializedTask`, `TaskState`)
- **Infrastructure** for task management (retry strategies, routing, rate limiting, time limits)
- **Zero runtime dependencies** on specific broker implementations

### Design Goals

1. **Separation of Concerns**: Core abstractions separate from implementations
2. **Type Safety**: Leverage Rust's type system for compile-time guarantees
3. **Flexibility**: Support multiple backends, retry strategies, and execution patterns
4. **Performance**: Minimal overhead with inline optimizations
5. **Observability**: Built-in event system for monitoring and debugging

---

## Design Principles

### 1. Trait-Based Extensibility

All core abstractions are defined as traits, allowing users to:
- Implement custom brokers for any message queue backend
- Create custom exception handlers for domain-specific error handling
- Build custom event emitters for monitoring and logging
- Define custom rate limiters for specific use cases

```rust
#[async_trait]
pub trait Broker: Send + Sync {
    async fn publish(&self, task: SerializedTask) -> Result<()>;
    async fn consume(&self) -> Result<Option<BrokerMessage>>;
    // ...
}
```

### 2. Builder Pattern for Ergonomics

All complex types use builder patterns for construction:

```rust
let task = SerializedTask::new("my_task".to_string(), payload)
    .with_priority(5)
    .with_max_retries(3)
    .with_timeout(60)
    .with_dependency(parent_id);
```

### 3. Explicit State Management

Tasks use an explicit state machine with clear transitions:

```
Pending → Reserved → Running → Succeeded
                              ↘ Failed → Retrying → Reserved
```

### 4. Composable Handlers

Exception handlers and event emitters can be composed:

```rust
let chain = ExceptionHandlerChain::new()
    .add_handler(Box::new(LoggingExceptionHandler::new()))
    .add_handler(Box::new(PolicyExceptionHandler::new(policy)));
```

### 5. Zero-Copy Where Possible

Task payloads use `Vec<u8>` for flexibility, with documented trade-offs for zero-copy alternatives.

---

## Module Organization

```
celers-core/
├── broker.rs          # Broker trait and BrokerMessage wrapper
├── control.rs         # Control plane types (stats, commands, responses)
├── dag.rs             # Task dependency DAG (Directed Acyclic Graph)
├── error.rs           # Unified error type (CelersError)
├── event.rs           # Event system for observability
├── exception.rs       # Structured exception handling
├── executor.rs        # Task registry for task type mapping
├── rate_limit.rs      # Rate limiting (token bucket, sliding window)
├── result.rs          # Task result storage abstraction
├── retry.rs           # Retry strategies and policies
├── revocation.rs      # Task revocation and cancellation
├── router.rs          # Pattern-based task routing
├── state.rs           # Task state machine and history
├── task.rs            # Task types (Task, TaskMetadata, SerializedTask)
├── time_limit.rs      # Soft/hard time limits for tasks
└── lib.rs             # Public API and documentation
```

### Module Dependencies

```
task.rs
  ├→ state.rs (TaskState)
  ├→ error.rs (CelersError)
  └→ dag.rs (TaskId for dependencies)

broker.rs
  └→ task.rs (SerializedTask)

exception.rs
  └→ error.rs (for error conversion)

retry.rs
  └→ error.rs (for retry decision)

executor.rs
  ├→ task.rs (Task trait)
  └→ error.rs

event.rs
  ├→ task.rs (TaskMetadata)
  └→ state.rs (TaskState)

control.rs
  ├→ task.rs (TaskId, TaskMetadata)
  └→ time_limit.rs (TimeLimitStatus)
```

---

## Core Abstractions

### 1. Task Trait

The fundamental abstraction for executable units of work:

```rust
#[async_trait]
pub trait Task: Send + Sync {
    async fn execute(&self, payload: &[u8]) -> Result<Vec<u8>>;
    fn name(&self) -> &str;
}
```

**Purpose**: Define a common interface for all task types
**Responsibility**: Execute business logic given a payload
**Extension Point**: Implement for custom task types

### 2. Broker Trait

Message queue backend abstraction:

```rust
#[async_trait]
pub trait Broker: Send + Sync {
    async fn publish(&self, task: SerializedTask) -> Result<()>;
    async fn consume(&self) -> Result<Option<BrokerMessage>>;
    async fn acknowledge(&self, message: BrokerMessage) -> Result<()>;
    async fn reject(&self, message: BrokerMessage, requeue: bool) -> Result<()>;
}
```

**Purpose**: Decouple task processing from message queue implementation
**Responsibility**: Handle message queue operations
**Extension Point**: Implement for any message queue (Redis, RabbitMQ, SQS, SQL, etc.)

### 3. TaskMetadata

Rich metadata for task lifecycle management:

```rust
pub struct TaskMetadata {
    pub id: TaskId,
    pub name: String,
    pub state: TaskState,
    pub priority: u8,
    pub max_retries: u32,
    pub created_at: DateTime<Utc>,
    pub timeout: Option<u64>,
    pub group_id: Option<String>,
    pub chord_id: Option<String>,
    pub dependencies: HashSet<TaskId>,
    // ...
}
```

**Purpose**: Track task lifecycle and configuration
**Features**:
- Unique ID generation (UUID v4)
- State tracking with history
- Priority-based scheduling (0-255)
- Retry configuration
- Timeout management
- Task dependencies (DAG support)
- Workflow primitives (groups, chords)

### 4. SerializedTask

Wire format for task transmission:

```rust
pub struct SerializedTask {
    pub metadata: TaskMetadata,
    pub payload: Vec<u8>,
}
```

**Purpose**: Represent tasks in serialized form for transmission
**Design Decision**: Use `Vec<u8>` for payload to support any serialization format
**Trade-offs**:
- ✅ Flexibility: Works with JSON, MessagePack, Protobuf, etc.
- ✅ Simplicity: No lifetime parameters
- ⚠️ Copy overhead: One allocation per task (acceptable for most use cases)

### 5. BrokerMessage

Wrapper for broker-specific metadata:

```rust
pub struct BrokerMessage {
    pub task: SerializedTask,
    pub receipt_handle: Option<String>,
}
```

**Purpose**: Attach broker-specific information (e.g., SQS receipt handle)
**Design**: Thin wrapper with delegation methods for ergonomics

---

## Data Flow

### Task Submission Flow

```
User Code
    │
    ├─→ Create Task
    │   ├─→ TaskMetadata::new()
    │   ├─→ Builder methods (.with_priority(), .with_timeout())
    │   └─→ SerializedTask::new()
    │
    ├─→ Route Task (optional)
    │   └─→ Router::route() → queue name
    │
    └─→ Publish to Broker
        └─→ Broker::publish(task)
            └─→ Message Queue Backend
```

### Task Execution Flow

```
Message Queue Backend
    │
    └─→ Broker::consume()
        │
        ├─→ BrokerMessage (with receipt handle)
        │
        ├─→ Check Revocation
        │   └─→ RevocationManager::is_revoked()
        │
        ├─→ Check Time Limits
        │   └─→ TimeLimitTracker::check()
        │
        ├─→ Check Rate Limits
        │   └─→ RateLimiter::try_acquire()
        │
        ├─→ Execute Task
        │   ├─→ TaskRegistry::get()
        │   ├─→ Task::execute(payload)
        │   └─→ Handle Exceptions
        │       └─→ ExceptionHandler::handle()
        │
        ├─→ Update State
        │   ├─→ TaskState::transition()
        │   └─→ StateHistory::record()
        │
        └─→ Acknowledge/Reject
            └─→ Broker::acknowledge() or Broker::reject()
```

### Error Flow

```
Task Execution Error
    │
    ├─→ Wrap in TaskException
    │   ├─→ Capture traceback
    │   ├─→ Classify category (Retryable, Fatal, etc.)
    │   └─→ Add metadata
    │
    ├─→ ExceptionHandler::handle()
    │   ├─→ Apply ExceptionPolicy
    │   │   ├─→ Match patterns (retry_on, fail_on, ignore_on)
    │   │   └─→ Return ExceptionAction
    │   │
    │   └─→ ExceptionHandler::on_exception()
    │       └─→ Log, emit metrics, etc.
    │
    └─→ Apply Action
        ├─→ Retry: RetryStrategy::calculate_delay()
        ├─→ Fail: Send to DLQ
        ├─→ Ignore: Mark as succeeded
        └─→ Reject: Remove from queue
```

---

## State Machine

### Task State Transitions

```
                ┌───────────┐
                │  Pending  │ (Initial state)
                └─────┬─────┘
                      │
                      ▼
                ┌───────────┐
                │ Reserved  │ (Claimed by worker)
                └─────┬─────┘
                      │
                      ▼
                ┌───────────┐
           ┌───→│  Running  │ (Actively executing)
           │    └─────┬─────┘
           │          │
           │    ┌─────┴─────┐
           │    │           │
           │    ▼           ▼
           │ ┌──────────┐ ┌──────────┐
           │ │Succeeded │ │  Failed  │ (Terminal states)
           │ └──────────┘ └─────┬────┘
           │                    │
           │                    ▼
           │              ┌───────────┐
           └──────────────┤ Retrying  │ (If retries remain)
                          └───────────┘
```

### State Properties

| State      | Active | Terminal | Can Retry | Description                    |
|------------|--------|----------|-----------|--------------------------------|
| Pending    | ✓      | ✗        | N/A       | Waiting to be claimed          |
| Reserved   | ✓      | ✗        | N/A       | Claimed by worker, not started |
| Running    | ✓      | ✗        | N/A       | Currently executing            |
| Retrying   | ✓      | ✗        | ✓         | Waiting before retry           |
| Succeeded  | ✗      | ✓        | ✗         | Completed successfully         |
| Failed     | ✗      | ✓        | ✓         | Failed (may have retries left) |

### State History

Tasks maintain a complete history of state transitions:

```rust
pub struct StateHistory {
    transitions: Vec<StateTransition>,
}

pub struct StateTransition {
    from: Option<TaskState>,
    to: TaskState,
    timestamp: DateTime<Utc>,
}
```

**Benefits**:
- Audit trail for debugging
- Performance analysis (time in each state)
- Retry analysis
- SLA monitoring

---

## Dependency Graph

### Task DAG (Directed Acyclic Graph)

Tasks can have dependencies, forming a DAG:

```rust
pub struct TaskDag {
    nodes: HashMap<TaskId, DagNode>,
}

pub struct DagNode {
    task_id: TaskId,
    task_name: String,
    dependencies: HashSet<TaskId>,
    dependents: HashSet<TaskId>,
}
```

### Operations

1. **Add Node**: `dag.add_node(id, name)`
2. **Add Dependency**: `dag.add_dependency(child, parent)`
3. **Validate**: `dag.validate()` (detect cycles)
4. **Topological Sort**: `dag.topological_sort()` (execution order)
5. **Query**:
   - `dag.roots()` - Tasks with no dependencies
   - `dag.leaves()` - Tasks with no dependents
   - `dag.dependencies(id)` - Direct dependencies
   - `dag.dependents(id)` - Direct dependents

### Example Workflow

```rust
// Data pipeline: Load → Transform → Save
let mut dag = TaskDag::new();

let load_id = Uuid::new_v4();
let transform_id = Uuid::new_v4();
let save_id = Uuid::new_v4();

dag.add_node(load_id, "load_data");
dag.add_node(transform_id, "transform_data");
dag.add_node(save_id, "save_results");

dag.add_dependency(transform_id, load_id)?;
dag.add_dependency(save_id, transform_id)?;

// Get execution order
let order = dag.topological_sort()?;
// [load_id, transform_id, save_id]
```

---

## Error Handling Strategy

### Unified Error Type

```rust
pub enum CelersError {
    Serialization(String),
    Deserialization(String),
    BrokerError(String),
    TaskExecution(String),
    NotFound(String),
    Timeout(String),
    InvalidState(String),
    Configuration(String),
    Network(String),
}
```

**Design**: Use `thiserror` for Display implementations
**Strategy**: Convert all errors to `CelersError` at API boundaries

### Exception Handling Layers

1. **TaskException**: Structured exception with traceback
   - Captures file/line/function information
   - Categorizes exceptions (Retryable, Fatal, etc.)
   - Preserves cause chain

2. **ExceptionPolicy**: Declarative error handling
   - Pattern matching on exception types
   - Actions: Retry, Fail, Ignore, Reject

3. **ExceptionHandler**: Programmatic error handling
   - Custom logic for exception handling
   - Composable via ExceptionHandlerChain

### Retry Decision Flow

```
Exception Raised
    │
    ├─→ Convert to TaskException
    │
    ├─→ ExceptionPolicy::should_retry(exception)
    │   ├─→ Check fail_on patterns → No retry
    │   ├─→ Check reject_on patterns → No retry, no DLQ
    │   ├─→ Check ignore_on patterns → Mark success
    │   └─→ Check retry_on patterns → Retry
    │
    ├─→ Check retry count vs max_retries
    │
    └─→ RetryStrategy::calculate_delay(attempt)
        └─→ Schedule retry or send to DLQ
```

---

## Extension Points

### 1. Custom Brokers

Implement the `Broker` trait for any message queue:

```rust
pub struct MyBroker {
    // Your broker state
}

#[async_trait]
impl Broker for MyBroker {
    async fn publish(&self, task: SerializedTask) -> Result<()> {
        // Your implementation
    }
    // ... other methods
}
```

**Examples in workspace**:
- `celers-broker-sql`: PostgreSQL/SQLite backend
- `celers-broker-sqs`: AWS SQS backend

### 2. Custom Exception Handlers

Implement the `ExceptionHandler` trait:

```rust
pub struct MyHandler;

#[async_trait]
impl ExceptionHandler for MyHandler {
    async fn handle(&self, exception: &TaskException) -> ExceptionAction {
        // Your decision logic
    }
}
```

### 3. Custom Event Emitters

Implement the `EventEmitter` trait:

```rust
pub struct MyEmitter;

#[async_trait]
impl EventEmitter for MyEmitter {
    async fn emit_task_event(&self, event: &TaskEvent) {
        // Send to your monitoring system
    }
}
```

### 4. Custom Rate Limiters

Implement the `RateLimiter` trait:

```rust
pub struct MyRateLimiter;

#[async_trait]
impl RateLimiter for MyRateLimiter {
    async fn try_acquire(&self, task_name: &str) -> Result<bool> {
        // Your rate limiting logic
    }
}
```

### 5. Custom Retry Strategies

Use `RetryStrategy::Custom`:

```rust
let strategy = RetryStrategy::Custom(vec![100, 500, 1000, 5000]);
```

---

## Integration with Other Crates

### CeleRS Ecosystem

```
┌─────────────────────────────────────────────────────────┐
│                     User Application                     │
└───────────┬──────────────────────────────┬──────────────┘
            │                              │
            ▼                              ▼
    ┌───────────────┐            ┌─────────────────┐
    │ celers-macros │            │     celers      │
    │  (proc macro) │            │  (high-level)   │
    └───────┬───────┘            └────────┬────────┘
            │                             │
            └──────────┬──────────────────┘
                       ▼
              ┌─────────────────┐
              │   celers-core   │ ← This crate
              │   (traits +     │
              │    types)       │
              └────────┬────────┘
                       │
         ┌─────────────┼─────────────┐
         │             │             │
         ▼             ▼             ▼
┌─────────────┐ ┌──────────┐ ┌──────────────┐
│celers-broker│ │ celers-  │ │  celers-     │
│    -sql     │ │  broker  │ │   metrics    │
│             │ │   -sqs   │ │              │
└─────────────┘ └──────────┘ └──────────────┘
```

### Integration Points

1. **celers-macros**: Uses `Task` trait for code generation
2. **celers**: Uses all core types for high-level API
3. **celers-broker-***: Implements `Broker` trait
4. **celers-metrics**: Uses `EventEmitter` for metrics collection

---

## Performance Considerations

### 1. Inline Optimization

Critical path functions are marked `#[inline]`:

```rust
#[inline]
pub fn has_priority(&self) -> bool {
    self.priority.is_some()
}
```

**Affected functions** (30+ total):
- All builder methods
- State check methods (`is_terminal()`, `is_active()`)
- Delegation methods in `SerializedTask` and `BrokerMessage`

### 2. Zero Allocations for Checks

State checks and metadata queries don't allocate:

```rust
task.is_high_priority()  // No allocation
task.has_timeout()       // No allocation
state.is_terminal()      // No allocation
```

### 3. Cloning Strategy

- `TaskMetadata`: Implements `Clone` for cheap copies
- `SerializedTask`: Clone copies both metadata and payload (acceptable overhead)
- Consider `Arc<SerializedTask>` for scenarios with many consumers

### 4. Serialization Format

- Use `Vec<u8>` for payload flexibility
- Support any serialization format (JSON, MessagePack, Protobuf)
- Trade-off: One allocation per task (optimized for flexibility over zero-copy)

### 5. Benchmarking

Comprehensive benchmarks in `benches/task_bench.rs`:

```bash
cargo bench
```

**Benchmarks include**:
- Task metadata creation
- Task metadata cloning
- Task serialization (various payload sizes)
- Validation operations

### 6. Memory Layout

```rust
// TaskMetadata: ~200 bytes
// SerializedTask: ~200 bytes + payload size
// BrokerMessage: ~200 bytes + payload size + receipt_handle

sizeof(TaskMetadata) ≈ 200 bytes
sizeof(SerializedTask) ≈ 200 bytes + len(payload)
```

**Optimization tips**:
- Use `Arc<SerializedTask>` for broadcast scenarios
- Use `Bytes` crate for zero-copy payload sharing (if needed)
- Limit payload size (use external storage for large data)

---

## Conclusion

CeleRS Core provides a robust, flexible, and performant foundation for distributed task processing. Its trait-based design allows for extensive customization while maintaining type safety and clear abstractions.

**Key Strengths**:
- ✅ Clean separation of concerns
- ✅ Comprehensive error handling
- ✅ Flexible retry strategies
- ✅ Built-in observability
- ✅ DAG-based dependency management
- ✅ Production-ready state machine

**Next Steps**:
- Explore broker implementations (`celers-broker-sql`, `celers-broker-sqs`)
- Review high-level API in `celers` crate
- Check out procedural macros in `celers-macros`
- Contribute custom handlers or strategies
