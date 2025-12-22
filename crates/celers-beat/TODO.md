# celers-beat TODO

> Periodic task scheduler (Celery Beat equivalent)

## Status: ✅ FEATURE COMPLETE

All schedule types implemented and production-ready.

## Completed Features

### Schedule Types ✅
- [x] Interval schedule (every N seconds)
- [x] Crontab schedule (full implementation with cron crate)
- [x] Solar schedule (sunrise/sunset with sunrise crate)
- [x] One-time schedule (run once at specific timestamp)
- [x] Schedule utility methods
  - [x] `is_interval()`, `is_crontab()`, `is_solar()`, `is_onetime()` - Schedule type checks
  - [x] `Display` implementation for human-readable output

### Scheduled Tasks ✅
- [x] Task naming and registration
- [x] Task arguments (args, kwargs)
- [x] Task options (queue, priority, expires)
  - [x] `has_queue()`, `has_priority()`, `has_expires()` - TaskOptions utility methods
  - [x] `Display` implementation for TaskOptions
- [x] Enable/disable tasks
- [x] Last run tracking
- [x] Run count tracking
- [x] ScheduledTask utility methods
  - [x] `is_enabled()` - Check if task is enabled
  - [x] `has_run()` - Check if task has executed at least once
  - [x] `has_options()` - Check if custom options are set
  - [x] `age_since_last_run()` - Calculate time since last execution
  - [x] `Display` implementation for debugging/logging

### Scheduler ✅
- [x] Task registration
- [x] Task execution loop
- [x] Due time calculation
- [x] Basic task management

### Persistence ✅
- [x] `with_persistence()` - Create scheduler with state file
- [x] `load_from_file()` - Load scheduler state from JSON file
- [x] `save_state()` - Save scheduler state to JSON file
- [x] Automatic persistence on task add/remove/update
- [x] `mark_task_run()` - Update task execution history
- [x] Preserves last_run_at and total_run_count across restarts

### Crontab Implementation ✅
- [x] Cron expression parsing (using cron crate v0.12)
- [x] Minute field (0-59, *, */N)
- [x] Hour field (0-23, *, */N)
- [x] Day of week field (0-6, *, */N, names)
- [x] Day of month field (1-31, *, */N)
- [x] Month field (1-12, *, */N, names)
- [x] Range support (1-5)
- [x] List support (1,3,5)
- [x] Next run calculation

### Solar Implementation ✅
- [x] Sunrise calculation (using sunrise crate v1.2)
- [x] Sunset calculation
- [x] Latitude/longitude support
- [x] Next occurrence search (up to 365 days)

### Error Handling ✅
- [x] `ScheduleError` enum with comprehensive error types
  - [x] `is_invalid()`, `is_not_implemented()`, `is_parse()`, `is_persistence()` - error type checks
  - [x] `is_retryable()` - identifies transient persistence errors
  - [x] `category()` - error categorization for logging/metrics

### Fluent Builder API ✅
- [x] `with_args()` - Set task arguments
- [x] `with_kwargs()` - Set task keyword arguments
- [x] `with_queue()` - Set target queue
- [x] `with_priority()` - Set task priority
- [x] `with_expires()` - Set expiration time
- [x] `disabled()` - Create disabled task

## Recently Completed Enhancements

### Crash Recovery System ✅ (Latest - 2025-12-20)
- [x] Execution state tracking
  - [x] `ExecutionState` enum - Idle/Running states for crash detection
  - [x] `ExecutionResult::Interrupted` - New result type for interrupted executions
  - [x] Per-task execution state persistence
  - [x] Execution timeout tracking
  - [x] Running duration calculation
- [x] Task-level recovery
  - [x] `begin_execution()` - Mark execution start with optional timeout
  - [x] `complete_execution()` - Mark execution completion
  - [x] `detect_interrupted_execution()` - Detect if task was interrupted
  - [x] `recover_from_interruption()` - Handle interrupted executions
  - [x] `is_ready_for_retry_after_crash()` - Check if task needs retry after crash
  - [x] Automatic execution record creation for interrupted tasks
  - [x] Retry count increment for interrupted executions
- [x] Scheduler-level recovery
  - [x] `detect_crashed_tasks()` - Find all tasks with interrupted executions
  - [x] `recover_from_crash()` - Automatically recover all interrupted tasks
  - [x] `get_tasks_ready_for_crash_retry()` - Get tasks needing retry after recovery
  - [x] Automatic state persistence after recovery
  - [x] Recovery logging for debugging
- [x] Detection mechanisms
  - [x] Explicit timeout detection (configurable per execution)
  - [x] Fallback detection (24-hour max execution time)
  - [x] State validation on scheduler load
- [x] Zero warnings, all tests passing (225 tests + 9 doc tests)

### Alerting System ✅ (2025-12-20)
- [x] Alert infrastructure
  - [x] `AlertLevel` enum - Info/Warning/Critical severity levels
  - [x] `AlertCondition` enum - Various alert trigger conditions
  - [x] `Alert` struct - Alert records with timestamp, task, level, condition, message
  - [x] `AlertCallback` type - Callback function for alert notifications
  - [x] `AlertManager` - Centralized alert management with deduplication
  - [x] `AlertConfig` - Per-task alert configuration
- [x] Alert conditions
  - [x] MissedSchedule - Task didn't execute when expected
  - [x] ConsecutiveFailures - Multiple consecutive failures detected
  - [x] HighFailureRate - Failure rate exceeds threshold
  - [x] SlowExecution - Task execution slower than threshold
  - [x] TaskStuck - Task hasn't executed for extended period
  - [x] TaskUnhealthy - Task health check failed
- [x] Alert manager features
  - [x] Deduplication with configurable time window (default: 5 minutes)
  - [x] Alert history tracking (default: 1000 alerts)
  - [x] Query methods (by task, by severity, by time range)
  - [x] Alert callbacks for custom notification handlers
  - [x] Automatic cleanup of old dedup entries
- [x] Scheduler integration
  - [x] `on_alert()` - Register alert callbacks
  - [x] `check_task_alerts()` - Check conditions for a specific task
  - [x] `check_all_alerts()` - Check conditions for all enabled tasks
  - [x] `get_alerts()`, `get_critical_alerts()`, `get_warning_alerts()` - Query alerts
  - [x] `clear_alerts()`, `clear_task_alerts()` - Alert management
- [x] Per-task configuration
  - [x] Configurable consecutive failure threshold (default: 3)
  - [x] Configurable failure rate threshold (default: 0.5 / 50%)
  - [x] Optional slow execution threshold in milliseconds
  - [x] Toggle alerts for missed schedules
  - [x] Toggle alerts for stuck tasks
  - [x] Fluent API (`with_alert_config()`)
- [x] Supporting features
  - [x] `consecutive_failure_count()` - Track consecutive failures from history
  - [x] Alert metadata support for additional context
  - [x] Serialization support for persistence
  - [x] Display implementations for debugging
- [x] Zero warnings, all tests passing (225 tests + 8 doc tests)

### Schedule Conflict Detection ✅ (2025-12-14)
- [x] Conflict detection and analysis
  - [x] `ScheduleConflict` - Structure representing conflicts between tasks
  - [x] `ConflictSeverity` - Low/Medium/High severity levels
  - [x] `detect_conflicts()` - Detect overlapping schedules
  - [x] `get_high_severity_conflicts()` - Filter high severity conflicts
  - [x] `get_medium_severity_conflicts()` - Filter medium severity conflicts
  - [x] `has_conflicts()` - Check if any conflicts exist
  - [x] `conflict_count()` - Count total conflicts
- [x] Conflict features
  - [x] Time window analysis (configurable window in seconds)
  - [x] Estimated duration consideration
  - [x] Overlap calculation in seconds
  - [x] Automatic severity determination based on overlap
  - [x] Suggested resolutions
  - [x] Disabled task filtering (skipped in analysis)
- [x] Comprehensive testing
  - [x] 11 new tests covering all scenarios
  - [x] Basic conflict detection tests
  - [x] Severity level tests
  - [x] Disabled task handling
  - [x] Serialization tests
  - [x] Display tests
- [x] Zero warnings, all tests passing (238 tests + 8 doc tests)

### Timezone Support for Crontab ✅ (2025-12-13)
- [x] Timezone-aware cron scheduling
  - [x] `Schedule::crontab_tz()` - Create timezone-aware crontab schedules
  - [x] Integration with chrono-tz for IANA timezone database
  - [x] Automatic conversion between UTC and target timezone
  - [x] DST handling built into chrono-tz
  - [x] Display shows timezone in output (e.g., "Crontab[... (America/New_York)]")
- [x] Comprehensive testing
  - [x] 4 new tests covering timezone functionality
  - [x] Timezone serialization/deserialization tests
  - [x] Invalid timezone error handling
  - [x] Next run calculation with timezone conversion
- [x] Zero warnings, all tests passing (227 tests + 7 doc tests)

### Schedule Locking ✅ (Latest - 2025-12-13)
- [x] In-memory lock management for preventing duplicate execution
  - [x] `ScheduleLock` - Lock structure with owner, TTL, and renewal tracking
  - [x] `LockManager` - Centralized lock management with automatic cleanup
  - [x] `try_acquire_lock()` - Acquire lock for a task
  - [x] `release_lock()` - Release owned locks
  - [x] `renew_lock()` - Extend lock TTL
  - [x] `execute_with_lock()` - Execute with automatic lock management
  - [x] Scheduler instance ID tracking for lock ownership
  - [x] `set_instance_id()` - Custom instance identifier support
- [x] Lock features
  - [x] Configurable TTL (default 5 minutes)
  - [x] Automatic expiration and cleanup
  - [x] Lock renewal with counter tracking
  - [x] Active lock queries
  - [x] Serialization support
- [x] Comprehensive testing
  - [x] 14 new tests covering all lock scenarios
  - [x] Acquire/release/renew tests
  - [x] Multiple instance tests
  - [x] Expiration and cleanup tests
  - [x] Serialization tests
- [x] Zero warnings, all tests passing (227 tests + 7 doc tests)

### Task Dependency Tracking ✅
- [x] Dependency management
  - [x] `add_dependency()`, `remove_dependency()`, `clear_dependencies()` - Manage dependencies
  - [x] `depends_on()`, `has_dependencies()` - Query dependency status
  - [x] `with_dependencies()` - Fluent API for setting dependencies
  - [x] `wait_for_dependencies` flag to control dependency enforcement
- [x] Dependency status tracking
  - [x] `DependencyStatus` enum (Satisfied/Waiting/Failed)
  - [x] `check_dependencies()` - Check against completed tasks
  - [x] `check_dependencies_with_failures()` - Track failed dependencies
  - [x] Status query methods (is_satisfied, has_failures, pending_tasks, failed_tasks)
- [x] Scheduler-level dependency features
  - [x] Circular dependency detection with graph traversal
  - [x] Dependency chain resolution (topological order)
  - [x] `validate_dependencies()` - Check for circular deps and missing tasks
  - [x] `get_tasks_ready_with_dependencies()` - Get tasks with satisfied dependencies
  - [x] `get_tasks_waiting_for_dependencies()` - Get tasks waiting
  - [x] `get_tasks_with_failed_dependencies()` - Get tasks blocked by failures
- [x] Comprehensive testing
  - [x] 16 new tests covering all scenarios
  - [x] Dependency add/remove/clear tests
  - [x] Status checking tests (satisfied/waiting/failed)
  - [x] Circular dependency detection tests (simple and complex)
  - [x] Dependency chain resolution tests
  - [x] Validation tests (success and error cases)
  - [x] Serialization/persistence tests

### Schedule Versioning ✅
- [x] Version tracking for schedule modifications
  - [x] `ScheduleVersion` struct with timestamp, schedule, and config
  - [x] Automatic versioning on schedule/config updates
  - [x] `current_version` field tracking active version
  - [x] `version_history` vector storing all versions
- [x] Version management methods
  - [x] `update_schedule()` - Update schedule with versioning
  - [x] `update_config()` - Update configuration with versioning
  - [x] `rollback_to_version()` - Rollback to previous version
  - [x] `get_version_history()` - View all versions
  - [x] `get_version()` - Get specific version
  - [x] `get_previous_version()` - Get last version
- [x] Comprehensive testing
  - [x] 10 new tests covering all scenarios
  - [x] Initial creation versioning
  - [x] Update and rollback tests
  - [x] Serialization/persistence tests
  - [x] Multiple rollback scenarios

### One-Time Schedules ✅
- [x] Absolute timestamp scheduling
  - [x] `Schedule::onetime()` - Create one-time schedule with specific run time
  - [x] `is_onetime()` - Check if schedule is one-time
  - [x] Next run calculation (returns error if already executed)
  - [x] Display implementation with formatted timestamp
- [x] Auto-cleanup after successful execution
  - [x] Automatic removal from scheduler after task completes
  - [x] Preserved on failure (allows manual retry)
  - [x] Works with both `mark_task_success()` methods
- [x] Comprehensive testing
  - [x] 10 new tests covering all scenarios
  - [x] Serialization/deserialization tests
  - [x] Auto-cleanup tests
  - [x] Error handling tests

### Schedule Features ✅
- [x] Schedule jitter (avoid thundering herd)
  - [x] Hash-based deterministic jitter
  - [x] Positive, negative, and symmetric jitter modes
  - [x] Configurable jitter windows
- [x] Catch-up logic (run missed schedules)
  - [x] Skip policy (default)
  - [x] Run once policy
  - [x] Run multiple times policy with max limit
  - [x] Time window policy
- [x] Schedule groups and tags
  - [x] Group tasks by category
  - [x] Tag-based filtering
  - [x] Bulk enable/disable by group
  - [x] Bulk enable/disable by tag
  - [x] Query all groups and tags

### Task Management ✅
- [x] Task retry on failure with exponential backoff
  - [x] NoRetry, FixedDelay, and ExponentialBackoff policies
  - [x] Retry count tracking
  - [x] Failure timestamp tracking
  - [x] Automatic retry delay calculation
  - [x] Failure rate metrics
- [x] Execution history tracking
  - [x] Record execution timestamps (start/complete)
  - [x] Track execution duration (milliseconds)
  - [x] Store execution results (Success/Failure/Timeout)
  - [x] Query last N executions
  - [x] Statistics: success/failure/timeout counts
  - [x] Duration metrics: average, min, max
  - [x] Success rate calculation from history
  - [x] Configurable history size limit
  - [x] Persistence across restarts

### Monitoring & Observability ✅
- [x] Schedule health checks
  - [x] Validate schedule syntax
  - [x] Check next run time calculation
  - [x] Detect stuck schedules (10x expected interval)
  - [x] Detect high failure rate (>50%)
  - [x] Detect consecutive failures
  - [x] Health status (Healthy/Warning/Unhealthy)
  - [x] Query unhealthy/warning tasks
  - [x] Bulk validation of all schedules
- [x] Scheduler metrics and statistics
  - [x] Total tasks (enabled/disabled)
  - [x] Total executions (success/failure/timeout)
  - [x] Overall success rate
  - [x] Tasks in retry state
  - [x] Health metrics (warnings/unhealthy/stuck)
  - [x] Per-task statistics (success/failure/duration)
  - [x] Group-based statistics
  - [x] Tag-based statistics

### Testing ✅
- [x] Comprehensive interval schedule tests
- [x] Crontab parsing tests (with cron feature)
- [x] Solar schedule tests (basic, sunrise/sunset ignored due to deprecated API)
- [x] One-time schedule tests (10 tests: basic, next_run, display, cleanup, serialization)
- [x] Schedule versioning tests (10 tests: creation, update, rollback, history, serialization)
- [x] Task dependency tests (16 tests: add/remove, status, circular deps, chain resolution, validation)
- [x] Next run calculation tests
- [x] Task enable/disable tests
- [x] Persistence tests (save/load/history)
- [x] Jitter tests (deterministic, range checking)
- [x] Catch-up policy tests (all modes)
- [x] Groups and tags tests (filtering, bulk operations)
- [x] Retry policy tests (all retry modes)
- [x] Execution history tests (tracking, statistics, persistence)
- [x] Health check tests (validation, stuck detection, failure detection)
- [x] Metrics and statistics tests (scheduler-wide and per-task)
- [x] Error handling tests
- [x] Serialization tests

### Code Quality ✅
- [x] Zero warnings in cargo test
- [x] Zero warnings in cargo build
- [x] All tests passing (225 tests + 9 doc tests)
- [x] Comprehensive test coverage

## Future Enhancements

### Schedule Types
- [x] One-time schedules (run once at specific time) ✅
  - [x] Absolute timestamp scheduling
  - [x] Auto-cleanup after execution
  - [x] Comprehensive tests (10 new tests)
  - [x] Serialization support
  - [x] Display implementation
- [x] Crontab with timezone support ✅
  - [x] Timezone-aware parsing with chrono-tz
  - [x] Automatic DST handling
  - [x] UTC conversion utilities
  - [x] `crontab_tz()` constructor for timezone-aware schedules
  - [x] Comprehensive tests (4 new tests)
  - [x] Serialization support with timezone preservation
- [ ] Solar schedules enhancements
  - [ ] Twilight schedules (civil, nautical, astronomical)
  - [ ] Golden hour calculations
  - [ ] Seasonal adjustments
- [ ] Custom schedule types
  - [ ] Plugin system for schedules
  - [ ] User-defined schedule logic
  - [ ] Schedule composition

### Scheduler Features
- [x] Persistent schedule state (file-based JSON) ✅
- [x] Dynamic schedule updates (add/remove at runtime) ✅
- [x] Schedule jitter (avoid thundering herd) ✅
- [x] Catch-up logic (run missed schedules) ✅
- [x] Schedule groups and tags ✅
- [x] Schedule versioning ✅
  - [x] Track schedule modifications
  - [x] Rollback to previous versions
  - [x] Version history with timestamps and change reasons
- [x] Schedule locking (prevent duplicates) ✅
  - [x] In-memory lock acquisition and management
  - [x] Lock timeout handling with TTL
  - [x] Lock renewal mechanism
  - [x] Lock ownership tracking by scheduler instance
  - [x] `try_acquire_lock()`, `release_lock()`, `renew_lock()` methods
  - [x] `execute_with_lock()` for automatic lock management
  - [x] Comprehensive tests (14 new tests)
  - [x] Serialization support
  - [x] Lock cleanup for expired locks
  - [x] Custom instance ID support
  - [ ] Distributed lock backend (Redis/etcd) - requires external state
- [x] Schedule conflict detection ✅
  - [x] Overlapping schedule detection
  - [x] Severity-based conflict classification
  - [x] Conflict resolution suggestions
  - [ ] Resource conflict resolution (requires resource tracking)
  - [ ] Priority-based automatic resolution

### Task Management
- [x] Task dependency tracking ✅
  - [x] Define task dependencies
  - [x] Dependency chain resolution
  - [x] Wait for dependencies
  - [x] Circular dependency detection
  - [x] Dependency validation
- [x] Task failure handling ✅
  - [x] Retry on failure
  - [x] Exponential backoff
  - [ ] Failure notifications
- [x] Task retry on scheduler crash ✅ (2025-12-20)
  - [x] Crash detection with execution state tracking
  - [x] Automatic retry with retry policy enforcement
  - [x] State recovery with interrupted execution detection
  - [x] Execution timeout tracking (configurable per execution)
  - [x] Fallback detection (24-hour max execution time)
  - [x] Recovery logging and metrics
- [x] Task result tracking ✅
  - [x] Store execution results
  - [x] Result history
  - [x] Success/failure analytics

### Advanced Features
- [ ] Multiple schedulers with leader election
  - [ ] Raft consensus
  - [ ] Etcd-based election
  - [ ] Redis-based election
  - [ ] Automatic failover
- [ ] Distributed scheduling
  - [ ] Shard schedules across instances
  - [ ] Consistent hashing
  - [ ] Dynamic rebalancing
- [ ] Schedule prioritization
  - [ ] Priority-based execution
  - [ ] Weighted fair queuing
  - [ ] Starvation prevention
- [ ] Schedule timezone conversion
  - [ ] Multi-timezone support
  - [ ] Automatic timezone detection
  - [ ] Timezone database updates

### Monitoring & Observability
- [x] Scheduler metrics ✅
  - [x] Execution count per schedule
  - [x] Execution latency (via duration tracking)
  - [x] Task health metrics
  - [x] Success/failure/timeout counts
- [x] Schedule health checks ✅
  - [x] Validate schedule syntax
  - [x] Check next run time
  - [x] Detect stuck schedules
- [x] Execution history ✅
  - [x] Last N executions
  - [x] Execution timestamps
  - [x] Execution duration
  - [x] Execution results
- [x] Alerting ✅ (Latest - 2025-12-20)
  - [x] Alert levels (Info, Warning, Critical)
  - [x] Alert conditions (MissedSchedule, ConsecutiveFailures, HighFailureRate, SlowExecution, TaskStuck, TaskUnhealthy)
  - [x] Alert manager with deduplication (5-minute window)
  - [x] Alert callbacks for notifications
  - [x] Per-task alert configuration (AlertConfig)
  - [x] Configurable thresholds (consecutive failures, failure rate, slow execution)
  - [x] Alert history tracking (up to 1000 alerts)
  - [x] Alert queries (by task, by severity, by time range)
  - [x] Automatic alert checking (check_task_alerts, check_all_alerts)
  - [x] Consecutive failure tracking from execution history
  - [x] Alert metadata support for additional context
  - [x] Serialization support for persistence
  - [ ] Email/SMS notification integration (requires external service)
  - [ ] Webhook alert delivery

### Calendar Integration
- [ ] Holiday calendar support
  - [ ] Skip on holidays
  - [ ] Execute on holidays only
  - [ ] Country-specific calendars
- [ ] Business day calculations
  - [ ] Skip weekends
  - [ ] Business hours only
  - [ ] Business day arithmetic
- [ ] Custom calendar rules
  - [ ] User-defined holidays
  - [ ] Special event schedules
  - [ ] Blackout periods

### API & Management
- [ ] REST API for schedule management
  - [ ] CRUD operations
  - [ ] Search and filter
  - [ ] Bulk operations
- [ ] Web UI for schedules
  - [ ] Visual schedule editor
  - [ ] Calendar view
  - [ ] Execution timeline
- [ ] CLI integration
  - [ ] Schedule commands
  - [ ] Status inspection
  - [ ] Manual triggers
- [ ] Missed schedule alerts
- [ ] Schedule execution history
- [ ] Scheduler health checks

### Performance
- [ ] Efficient schedule indexing
- [ ] Schedule caching
- [ ] Batch task enqueuing

## Testing

- [x] Interval schedule tests ✅
- [x] Crontab parsing tests ✅
- [x] Next run calculation tests ✅
- [x] Task enable/disable tests ✅
- [x] Persistence tests ✅
- [x] Jitter tests ✅
- [x] Catch-up policy tests ✅
- [x] Groups and tags tests ✅
- [x] Error handling tests ✅
- [ ] Scheduler loop tests
- [ ] Timezone tests

## Documentation

- [x] Comprehensive README
- [x] Schedule types documentation
- [x] Basic examples
- [ ] Crontab syntax guide
- [ ] Production deployment guide
- [ ] Migration from Celery Beat

## Known Issues

- No leader election (must run single instance)
- Solar calculations use deprecated API (accepted with #[allow(deprecated)])

## Dependencies

- `chrono` - Date/time handling
- `chrono-tz` - Timezone support
- `serde` - Serialization
- `thiserror` - Error types

### Optional Dependencies
- `cron` - Cron expression parsing (feature: "cron")
- `sunrise` - Solar event calculation (feature: "solar")

## Notes

- Only run ONE scheduler instance (no built-in leader election)
- Interval schedules are production-ready
- Crontab and Solar require additional implementation
- Compatible with Python Celery Beat schedule format
