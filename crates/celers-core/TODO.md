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
  - [x] Created comprehensive benchmarks in benches/task_bench.rs (22 benchmarks)
    - [x] Benchmarks for task metadata creation, cloning, serialization
    - [x] Benchmarks for different payload sizes
    - [x] Benchmarks for validation operations
    - [x] Benchmarks for state transitions (4 benchmarks)
    - [x] Benchmarks for retry strategies (5 benchmarks covering all strategies)
    - [x] Benchmarks for DAG operations (5 benchmarks for add/sort/validate)
  - [x] Created advanced benchmarks in benches/advanced_bench.rs (31 benchmarks)
    - [x] Exception handling benchmarks (8 benchmarks)
    - [x] Router/pattern matching benchmarks (9 benchmarks)
    - [x] Batch operations benchmarks (14 benchmarks)
  - [x] **Total: 53 benchmarks** covering all core operations
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

**Total: 185 unit tests + 58 doc tests = 243 tests, all passing** ✅

## Latest Enhancements (Jan 7, 2026):

### Performance Optimizations - Additional Inline Attributes (Session 10)
- **Added 27 `#[inline]` attributes** to constructor and builder methods for better performance
  - **control.rs** (15 functions):
    - `ControlCommand::ping()` - Create ping command
    - `ControlCommand::inspect_active/scheduled/reserved/revoked/registered/stats/queue_info()` - Create inspect commands
    - `ControlCommand::shutdown()` - Create shutdown command
    - `ControlCommand::revoke/bulk_revoke/revoke_by_pattern()` - Create revoke commands
    - `ControlCommand::queue_purge/length/delete/bind/unbind/declare()` - Create queue commands
    - `ControlResponse::pong/ack/error()` - Create response objects
  - **config.rs** (11 functions):
    - `CeleryConfig::new()` - Create new configuration
    - `CeleryConfig::with_broker_url/result_backend/task_serializer/result_serializer()` - URL and serializer builders
    - `CeleryConfig::with_accept_content/timezone/default_queue()` - Configuration builders
    - `CeleryConfig::with_task_route/task_annotation/result_compression/beat_schedule()` - Advanced configuration builders
  - **executor.rs** (1 function):
    - `TaskRegistry::new()` - Create new task registry
  - **Benefits**: Reduced function call overhead for control commands, configuration builders, and task registry creation
  - **Impact**: Better performance for worker control operations, configuration setup, and task registration; constructor and builder methods now inline for reduced overhead

### Testing and Verification
- **All 243 tests passing** (185 unit + 58 doc tests)
- **Zero standard clippy warnings** - complete compliance maintained
- **Zero pedantic clippy warnings** - NO WARNINGS policy preserved

## Previous Enhancements (Jan 7, 2026):

### Performance Optimizations - Additional Inline Attributes (Session 9)
- **Added 9 `#[inline]` attributes** to batch utility and query methods for better performance
  - **broker.rs** (6 functions):
    - `broker_batch::sort_by_priority()` - Sort broker messages by priority
    - `broker_batch::group_by_task_name()` - Group messages by task name
    - `broker_batch::filter_by_name_prefix()` - Filter messages by name prefix
    - `broker_batch::total_payload_size()` - Calculate total payload size
    - `broker_batch::filter_expired()` - Filter expired messages
    - `broker_batch::prepare_ack_batch()` - Prepare batch acknowledgement data
  - **event.rs** (1 function):
    - `EventBus::subscriber_count()` - Get number of active subscribers
  - **time_limit.rs** (2 functions):
    - `TaskTracker::elapsed_seconds()` - Get elapsed time in seconds
    - `TaskTracker::soft_limit_warned()` - Check if soft limit was warned
  - **Benefits**: Reduced function call overhead for batch operations, event monitoring, and time limit tracking
  - **Impact**: Better performance for broker batch operations, event subscriber queries, and time limit status checks

### Performance Optimizations - Additional Const Functions (Session 9)
- **Added 1 `const fn` declaration** to enable compile-time evaluation of time limit checks
  - **time_limit.rs** (1 function):
    - `TaskTracker::soft_limit_warned()` - Check if soft limit warning flag is set (const evaluable)
  - **Benefits**: Enables compile-time evaluation for soft limit warning checks
  - **Impact**: Time limit warning status can now be evaluated at compile-time where applicable, reducing runtime overhead

### Testing and Verification
- **All 243 tests passing** (185 unit + 58 doc tests)
- **Zero standard clippy warnings** - complete compliance maintained
- **Zero pedantic clippy warnings** - NO WARNINGS policy preserved

## Previous Enhancements (Jan 5, 2026):

### Performance Optimizations - Additional Inline Attributes (Session 8)
- **Added 12 `#[inline]` attributes** to getter and query methods for better performance
  - **rate_limit.rs** (5 functions):
    - `TaskRateLimiter::has_rate_limit()` - Check if task has rate limit configured
    - `ThreadSafeTaskRateLimiter::has_rate_limit()` - Thread-safe rate limit check
    - `DistributedRateLimiterCoordinator::get_token_bucket_spec()` - Get token bucket spec with read lock
    - `DistributedRateLimiterCoordinator::get_sliding_window_spec()` - Get sliding window spec with read lock
    - `DistributedRateLimiterCoordinator::has_rate_limit()` - Check distributed rate limit configuration
  - **router.rs** (4 functions):
    - `Router::has_route()` - Check if task has matching route
    - `TopicPattern::has_wildcards()` - Check if pattern contains wildcards
    - `TopicPattern::is_exact()` - Check if pattern is exact match
    - `TopicRouter::has_match()` - Check if routing key has matches
  - **dag.rs** (2 functions):
    - `TaskDag::get_roots()` - Get all root nodes (nodes with no dependencies)
    - `TaskDag::get_leaves()` - Get all leaf nodes (nodes with no dependents)
  - **state.rs** (1 function):
    - `StateHistory::has_been_in_state()` - Check if task has been in specific state
  - **task.rs** (2 functions in batch module):
    - `batch::has_expired_tasks()` - Check if any tasks expired
    - `batch::get_expired_tasks()` - Get all expired tasks
  - **Benefits**: Reduced function call overhead for frequently-called query and getter methods
  - **Impact**: Better performance for rate limiting checks, routing queries, DAG operations, state history queries, and batch operations
- **Total: 203 inline attributes** across the codebase (191 previous + 12 new)

### Performance Optimizations - Additional Const Functions (Session 8)
- **Added 9 `const fn` declarations** to enable compile-time evaluation of category checks, compression decisions, and count operations
  - **exception.rs** (3 functions):
    - `TaskException::is_retryable()` - Check if exception is retryable (const evaluable)
    - `TaskException::is_fatal()` - Check if exception is fatal (const evaluable)
    - `TaskException::is_ignorable()` - Check if exception should be ignored (const evaluable)
  - **result.rs** (1 function):
    - `CompressionConfig::should_compress()` - Check if data should be compressed (const evaluable)
  - **task.rs** (5 functions):
    - `TaskMetadata::retry_count()` - Get current retry count (const evaluable)
    - `TaskMetadata::retries_remaining()` - Get remaining retry attempts (const evaluable)
    - `SerializedTask::payload_size()` - Get payload size in bytes (const evaluable)
    - `SerializedTask::retry_count()` - Delegated retry count (const evaluable)
    - `SerializedTask::retries_remaining()` - Delegated retries remaining (const evaluable)
  - **state.rs** (1 function):
    - `StateHistory::transition_count()` - Get number of state transitions (const evaluable)
  - **router.rs** (2 functions):
    - `TopicPattern::complexity()` - Get pattern complexity (const evaluable)
    - `TopicRouter::binding_count()` - Get number of bindings (const evaluable)
  - **Benefits**: Enables compile-time evaluation for exception category checks, compression decisions, retry calculations, and count operations
  - **Impact**: Exception category checks, compression thresholds, retry logic, state transition counts, and router complexity can now be evaluated at compile-time where applicable, reducing runtime overhead
- **Total: 64 const fn** across the codebase (55 previous + 9 new)

### Testing and Verification
- **All 243 tests passing** (185 unit + 58 doc tests)
- **Zero standard clippy warnings** - complete compliance maintained
- **Zero pedantic clippy warnings** - NO WARNINGS policy preserved

## Previous Enhancements (Jan 4, 2026):

### Performance Optimizations - Additional Inline Attributes (Session 7)
- **Added 7 `#[inline]` attributes** to simple getter methods for better performance
  - **router.rs** (3 functions): `Router::rules()`, `TopicPattern::pattern()`, `TopicPattern::complexity()`
  - **rate_limit.rs** (3 functions): `TaskRateLimiter::get_rate_limit()`, `DistributedTokenBucketSpec::state()`, `DistributedSlidingWindowSpec::state()`
  - **time_limit.rs** (2 functions): `TaskTimeLimits::task_id()`, `TaskTimeLimits::config()`
  - **Benefits**: Reduced function call overhead for frequently-called getters
  - **Impact**: Better performance for router queries, rate limit lookups, and time limit configuration access
- **Total: 191 inline attributes** across the codebase (184 previous + 7 new)

### Performance Optimizations - Additional Const Functions (Session 6)
- **Added 13 `const fn` declarations** to enable compile-time evaluation of state checks and comparisons
  - **result.rs** (6 functions):
    - `TaskResultValue::is_terminal()` - Check if result is in terminal state (const evaluable)
    - `TaskResultValue::is_pending()` - Check if task is pending (const evaluable)
    - `TaskResultValue::is_ready()` - Check if task is ready (const evaluable)
    - `TaskResultValue::is_successful()` - Check if task succeeded (const evaluable)
    - `TaskResultValue::is_failed()` - Check if task failed (const evaluable)
    - `ResultChunk::is_last()` - Check if chunk is last (const evaluable)
  - **task.rs** (3 functions):
    - `TaskMetadata::has_priority()` - Check if task has custom priority (const evaluable)
    - `TaskMetadata::is_high_priority()` - Check if task has high priority (const evaluable)
    - `TaskMetadata::is_low_priority()` - Check if task has low priority (const evaluable)
  - **event.rs** (2 functions):
    - `Event::is_task_event()` - Check if event is task event (const evaluable)
    - `Event::is_worker_event()` - Check if event is worker event (const evaluable)
  - **config.rs** (1 function):
    - `CeleryConfig::result_expires_duration()` - Get result expiration duration (const evaluable)
  - **time_limit.rs** (1 function):
    - `TimeLimitConfig::has_limits()` - Check if time limits are configured (const evaluable)
  - **Benefits**: Enables compile-time evaluation for state checks, priority comparisons, and event type checks; better performance in const contexts, zero-cost abstractions
  - **Impact**: Result state checks, task priority queries, event type checks, and time limit checks can now be evaluated at compile-time where applicable, reducing runtime overhead
- **Total: 55 const fn** across the codebase (42 previous + 13 new)

### Testing and Verification
- **All 243 tests passing** (185 unit + 58 doc tests)
- **Zero standard clippy warnings** - complete compliance maintained
- **Zero pedantic clippy warnings** - NO WARNINGS policy preserved

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
