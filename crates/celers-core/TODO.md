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

### Potential Improvements
- [ ] Add task dependencies/DAG support
- [x] Implement task result storage backend (celers-backend-*)
- [x] Add task scheduling (cron-like) (celers-beat)
- [x] Support for task chains/workflows (celers-canvas)
- [x] Add task groups/batching primitives (celers-canvas)

### Performance
- [ ] Benchmark task creation overhead
- [ ] Optimize metadata cloning
- [ ] Consider zero-copy serialization options

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

## Testing Status

- [x] Unit tests for state machine (4 tests)
- [x] Unit tests for metadata creation (1 test)
- [x] Unit tests for task registry (1 test)
- [x] Unit tests for retry strategies (16+ tests)
- [x] Unit tests for router patterns (20+ tests)
- [x] Unit tests for rate limiting (4 tests)
- [x] Unit tests for time limits (13 tests)
- [x] Unit tests for revocation (8 tests)
- [x] Unit tests for error types (12 tests)
- [x] Unit tests for exception handling (19+ tests)
- [x] Doc tests for all public APIs (12 tests)
- [ ] Add property-based testing for state transitions
- [ ] Add integration tests for full task lifecycle

## Documentation

- [x] Module-level documentation
- [x] Trait documentation with examples
- [x] Type documentation
- [ ] Add more usage examples
- [ ] Create architecture documentation

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
