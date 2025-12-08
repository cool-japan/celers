# celers-core TODO

> Core traits and types for the CeleRS task queue system

## Status: ✅ FEATURE COMPLETE

The core crate provides all fundamental building blocks for task queue systems.

## Completed Features

### Core Traits ✅
- [x] `Task` trait for executable tasks
- [x] `Broker` trait for queue backends
- [x] `TaskRegistry` for task type mapping
- [x] Async execution support via async-trait

### Task Management ✅
- [x] `TaskMetadata` with full lifecycle tracking
  - [x] `has_timeout()` - Check if timeout is configured
  - [x] `has_group_id()` - Check if part of a group
  - [x] `has_chord_id()` - Check if part of a chord
  - [x] `has_priority()` - Check if has custom priority
  - [x] `is_high_priority()` - Check if priority > 0
  - [x] `is_low_priority()` - Check if priority < 0
- [x] `TaskState` state machine (Pending → Processing → Completed/Failed/Retrying)
- [x] `SerializedTask` with JSON payload support
  - [x] `has_timeout()`, `has_group_id()`, `has_chord_id()`, `has_priority()` - delegation methods
  - [x] `payload_size()` - Get payload size in bytes
  - [x] `has_empty_payload()` - Check if payload is empty
  - [x] `Display` implementation for debugging
- [x] Task ID generation using UUID v4
- [x] Priority support (0-255)
- [x] Max retries configuration
- [x] Timeout support
- [x] TaskState utility methods
  - [x] `is_terminal()`, `is_active()` - state classification
  - [x] `is_pending()`, `is_reserved()`, `is_running()`, `is_retrying()` - state checks
  - [x] `is_succeeded()`, `is_failed()` - terminal state checks
  - [x] `success_result()`, `error_message()` - result extraction
  - [x] `can_retry()`, `retry_count()` - retry logic

### Error Handling ✅
- [x] Comprehensive `CelersError` enum
- [x] Serialization/deserialization errors
- [x] Broker operation errors
- [x] Task execution errors
- [x] Type-erased error conversion
- [x] Error utility methods
  - [x] `is_*()` methods for error type checking
  - [x] `is_retryable()` - Check if error should trigger retry
  - [x] `category()` - Get error category as string

### Type System ✅
- [x] `TaskId` type alias
- [x] `Result<T>` type alias
- [x] `BrokerMessage` wrapper
  - [x] `new()` - Create broker message
  - [x] `with_receipt_handle()` - Create with receipt handle
  - [x] `has_receipt_handle()` - Check if has receipt handle
  - [x] `task_id()`, `task_name()`, `priority()` - Convenience getters
  - [x] `is_expired()`, `age()` - Delegation to inner task
  - [x] `Display` implementation for debugging
- [x] Builder pattern for task creation

### Display Implementations ✅
- [x] `Display` for `TaskState` (human-readable state output)
- [x] `Display` for `TaskMetadata` (concise task summary)
- [x] `Display` for `CelersError` (via thiserror)

### Retry Strategies ✅
- [x] `RetryStrategy` enum with comprehensive strategies
  - [x] `Fixed` - Constant delay between retries
  - [x] `Linear` - Linear backoff (initial + increment per retry)
  - [x] `Exponential` - Exponential backoff with configurable multiplier
  - [x] `Polynomial` - Polynomial backoff (n^power)
  - [x] `Fibonacci` - Fibonacci sequence delays
  - [x] `DecorrelatedJitter` - AWS recommended jitter strategy
  - [x] `FullJitter` - Random between 0 and exponential delay
  - [x] `EqualJitter` - Half fixed, half random
  - [x] `Custom` - User-defined delay sequences
  - [x] `Immediate` - No delay between retries
- [x] `RetryPolicy` for policy-based retry configuration
  - [x] Max retries configuration
  - [x] Retry on specific error patterns
  - [x] Don't retry on specific error patterns
  - [x] Retry on timeout flag
  - [x] `should_retry()` - Check if retry is allowed
  - [x] `get_retry_delay()` - Calculate delay for retry attempt

### Exception Handling ✅
- [x] `TaskException` structured exception type
  - [x] Exception type and message
  - [x] Traceback frames with file/line/function
  - [x] Raw traceback string (Python compatibility)
  - [x] Exception category (Retryable, Fatal, Ignorable, etc.)
  - [x] Cause/context chaining for nested exceptions
  - [x] Metadata support for additional context
  - [x] JSON serialization for cross-language support
  - [x] Celery-compatible format export
- [x] `ExceptionCategory` enum for exception classification
  - [x] Retryable, Fatal, Ignorable, RequiresIntervention, Unknown
- [x] `ExceptionAction` enum for action decisions
  - [x] Retry, Fail, Ignore, Reject, Default
- [x] `ExceptionPolicy` for policy-based exception handling
  - [x] retry_on patterns (retry specific exception types)
  - [x] ignore_on patterns (task succeeds despite exception)
  - [x] fail_on patterns (no retry, go to DLQ)
  - [x] reject_on patterns (no retry, no DLQ)
  - [x] Glob pattern matching for exception types
  - [x] Traceback preservation configuration
  - [x] Max traceback depth truncation
- [x] `ExceptionHandler` trait for custom handlers
  - [x] `handle()` - Determine action for exception
  - [x] `transform()` - Modify exception (add metadata, truncate traceback)
  - [x] `on_exception()` - Hook for logging/metrics
- [x] `ExceptionHandlerChain` for composable handlers
- [x] Built-in handlers
  - [x] `LoggingExceptionHandler` - Log exceptions with tracing
  - [x] `PolicyExceptionHandler` - Apply ExceptionPolicy rules
- [x] Common exception type constants for easy categorization

### Task Dependencies & DAG Support ✅
- [x] DAG (Directed Acyclic Graph) module for task dependencies
  - [x] `TaskDag` for managing task dependencies
  - [x] `DagNode` for representing tasks in the DAG
  - [x] Cycle detection to prevent circular dependencies
  - [x] Topological sorting for execution order
  - [x] Root and leaf node identification
  - [x] 8 comprehensive tests for DAG functionality
- [x] Task dependency tracking in `TaskMetadata`
  - [x] `dependencies` field for storing task dependencies
  - [x] `with_dependency()` and `with_dependencies()` builder methods
  - [x] `has_dependencies()`, `dependency_count()`, `depends_on()` helper methods
  - [x] `remove_dependency()` and `clear_dependencies()` for management
  - [x] 6 comprehensive tests for dependency management
- [x] Integrated with `SerializedTask` for full workflow support

### Potential Improvements
- [x] Add task dependencies/DAG support ✅
- [x] Implement task result storage backend (celers-backend-*)
- [x] Add task scheduling (cron-like) (celers-beat)
- [x] Support for task chains/workflows (celers-canvas)
- [x] Add task groups/batching primitives (celers-canvas)

### Performance ✅
- [x] Benchmark task creation overhead
  - [x] Created comprehensive benchmarks in benches/task_bench.rs
  - [x] Benchmarks for task metadata creation, cloning, serialization
  - [x] Benchmarks for different payload sizes
  - [x] Benchmarks for validation operations
  - [x] Benchmarks for state transitions (4 benchmarks)
  - [x] Benchmarks for retry strategies (5 benchmarks covering all strategies)
  - [x] Benchmarks for DAG operations (5 benchmarks for add/sort/validate)
- [x] Optimize metadata cloning
  - [x] Added `#[inline]` attributes to hot-path functions (30+ functions)
  - [x] Optimized builder methods for better performance
  - [x] Improved delegation methods with inline hints
  - [x] Added high/low priority delegation methods to SerializedTask
- [x] Consider zero-copy serialization options ✅
  - [x] Documented trade-offs in SerializedTask documentation
  - [x] Provided alternatives: `Bytes`, `Arc<[u8]>`, borrowed payloads
  - [x] Analyzed performance vs. complexity trade-offs
  - [x] Current `Vec<u8>` approach is optimal for most use cases

### API Enhancements
- [x] Add more builder methods for common patterns ✅
  - [x] with_group_id() and with_chord_id() for workflow support
  - [x] age() method to get task age
  - [x] is_expired() method to check timeout
  - [x] is_terminal() and is_active() helper methods
- [x] Implement Display for better debugging
- [x] Add validation helpers ✅
  - [x] TaskMetadata::validate() - Validate name, retries, timeout
  - [x] SerializedTask::validate() - Validate metadata and payload size
  - [x] SerializedTask::validate_with_limit() - Custom size limits
- [x] Add batch utility functions ✅ (18 functions total)
  - [x] task::batch::validate_all() - Batch validation with error reporting
  - [x] task::batch::filter_by_state() - Filter tasks by state predicate
  - [x] task::batch::filter_high_priority() - Filter high priority tasks
  - [x] task::batch::sort_by_priority() - Sort tasks by priority (highest first)
  - [x] task::batch::count_by_state() - Count tasks grouped by state
  - [x] task::batch::has_expired_tasks() - Check for expired tasks
  - [x] task::batch::get_expired_tasks() - Get all expired tasks
  - [x] task::batch::total_payload_size() - Calculate total payload size
  - [x] task::batch::filter_with_dependencies() - Find tasks with dependencies
  - [x] task::batch::filter_retryable() - Find tasks that can be retried
  - [x] task::batch::filter_by_name_pattern() - Find tasks by name pattern
  - [x] task::batch::group_by_workflow_id() - Group tasks by workflow group ID
  - [x] task::batch::filter_terminal() - Find terminal tasks
  - [x] task::batch::filter_active() - Find active tasks
  - [x] task::batch::average_payload_size() - Calculate average payload size
  - [x] task::batch::find_oldest() - Find oldest task by creation time
  - [x] task::batch::find_newest() - Find newest task by creation time
- [x] Add TaskMetadata convenience methods ✅ (22 new methods)
  - [x] State checks: is_pending(), is_running(), is_succeeded(), is_failed(), is_retrying(), is_reserved()
  - [x] Time helpers: time_remaining(), time_elapsed()
  - [x] Retry helpers: can_retry(), retry_count(), retries_remaining()
  - [x] Workflow helpers: is_part_of_workflow(), get_group_id(), get_chord_id()
  - [x] State transitions: mark_as_running(), mark_as_succeeded(), mark_as_failed()
  - [x] Cloning: with_new_id() - Clone task with new ID
- [x] Add SerializedTask delegation methods ✅ (16 delegated methods)
  - [x] Delegated all new TaskMetadata convenience methods for ergonomic access

## Testing Status

- [x] Unit tests for state machine (2 tests)
- [x] Unit tests for metadata creation (1 test)
- [x] Unit tests for task registry (1 test)
- [x] Unit tests for retry strategies (16+ tests)
- [x] Unit tests for router patterns (20+ tests)
- [x] Unit tests for rate limiting (4 tests)
- [x] Unit tests for time limits (13 tests)
- [x] Unit tests for revocation (8 tests)
- [x] Unit tests for error types (12 tests)
- [x] Unit tests for exception handling (19+ tests)
- [x] Unit tests for task dependencies (6 tests)
- [x] Unit tests for DAG operations (8 tests)
- [x] Doc tests for all public APIs (45 tests including batch utilities and convenience methods)
- [x] Property-based testing for state transitions (10 tests) ✅
  - [x] Terminal states consistency
  - [x] Retry count validation
  - [x] Max retries enforcement
  - [x] Failed state retry logic
  - [x] Terminal states retry prevention
  - [x] State name consistency
  - [x] Success result extraction
  - [x] Error message extraction
  - [x] State history transitions
  - [x] State history current state tracking
- [x] Property-based testing for DAG operations (7 tests) ✅
  - [x] Node count matches added nodes
  - [x] Linear chain sorts correctly
  - [x] Validate always succeeds for acyclic graphs
  - [x] Roots have no dependencies
  - [x] Leaves have no dependents
  - [x] Edge count matches added dependencies
  - [x] Remove dependency decreases edge count
- [x] Property-based testing for retry strategies (11 tests) ✅
  - [x] Fixed delay is constant
  - [x] Linear delay increases linearly
  - [x] Exponential delay grows
  - [x] Exponential with max respects limit
  - [x] Fibonacci delay grows
  - [x] Immediate is always zero
  - [x] Full jitter within bounds
  - [x] Decorrelated jitter within bounds
  - [x] Polynomial delay grows
  - [x] Custom strategy uses provided delays
  - [x] Retry policy respects max retries
- [x] Integration tests for full task lifecycle (8 tests) ✅
  - [x] Complete task lifecycle (Pending → Running → Success)
  - [x] Task retry lifecycle with multiple retries
  - [x] Task with dependencies workflow
  - [x] Task serialization roundtrip
  - [x] Task validation lifecycle
  - [x] Task expiration lifecycle
  - [x] Workflow with multiple dependencies (DAG)
  - [x] Task state history full lifecycle

**Total: 180 unit tests + 45 doc tests = 225 tests, all passing** ✅

New in this release (Dec 2025):
- Added 18 property-based tests for DAG operations and retry strategies
- Added 18 batch utility functions with comprehensive doc tests
- Added 22 TaskMetadata convenience methods (state checks, time helpers, retry helpers, workflow helpers)
- Added 16 SerializedTask delegation methods for ergonomic access
- Added 14 new benchmarks for state transitions, retry strategies, and DAG operations
- **56 new helper methods** total for improved developer experience

## Documentation

- [x] Module-level documentation
- [x] Trait documentation with examples
- [x] Type documentation
- [x] Add more usage examples ✅
  - [x] Basic task creation example in lib.rs
  - [x] Task dependencies and workflows (DAG) example
  - [x] Retry strategies example
  - [x] State tracking example
  - [x] Exception handling example
  - [x] Core concepts overview
  - [x] 5 comprehensive doc test examples
- [x] Zero-copy serialization design documentation
- [x] Create architecture documentation ✅
  - [x] Design principles and goals
  - [x] Module organization and dependencies
  - [x] Core abstractions (Task, Broker, TaskMetadata, etc.)
  - [x] Complete data flow diagrams
  - [x] State machine documentation
  - [x] Dependency graph (DAG) documentation
  - [x] Error handling strategy
  - [x] Extension points for customization
  - [x] Integration with other crates
  - [x] Performance considerations and optimization tips

## Dependencies

- `tokio`: Async runtime
- `async-trait`: Async trait support
- `serde`: Serialization
- `uuid`: Unique IDs
- `chrono`: Timestamps
- `thiserror`: Error types
- `regex`: Pattern matching for routing
- `rand`: Random number generation for jittered retry strategies

## Notes

- Core crate is intentionally minimal
- No broker implementations (those are separate crates)
- No procedural macros (those are in celers-macros)
- Focus on traits and types only
