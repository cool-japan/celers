# celers-beat TODO

> Periodic task scheduler (Celery Beat equivalent)

**Version: 0.2.0 | Status: [Stable] | Updated: 2026-03-27 | Tests: 312**

## Status: ✅ FEATURE COMPLETE + v0.2.0 ENHANCED

All schedule types implemented and production-ready. v0.2.0 adds distributed locks and modular file structure.

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
- [x] Distributed lock integration (BeatScheduler with DistributedLockBackend)

### v0.2.0: Distributed Locks ✅
- [x] BeatScheduler integration with DistributedLockBackend trait
- [x] RedisLockBackend support (SET NX EX + Lua CAS)
- [x] DbLockBackend support (PostgreSQL table-based locks)
- [x] InMemoryLockBackend for single-instance/testing
- [x] Leader election for multi-beat deployments

### v0.2.0: File Structure Refactoring ✅
- [x] Split 12,515-line lib.rs into 12 focused modules
- [x] All modules under 2000 lines (refactoring policy compliant)

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

### Schedule Builders and Templates ✅ (Latest - 2026-01-06)
- [x] Fluent Schedule Builder API
  - [x] `ScheduleBuilder` - Chainable builder for creating schedules
  - [x] `every_n_seconds()`, `every_n_minutes()`, `every_n_hours()`, `every_n_days()` - Time interval methods
  - [x] `business_hours_only()` - Restrict to Mon-Fri, 9 AM - 5 PM
  - [x] `weekdays_only()` - Restrict to Mon-Fri
  - [x] `weekends_only()` - Restrict to Sat-Sun
  - [x] `in_timezone()` - Set timezone for schedule
  - [x] `build()` - Create the schedule
  - [x] Automatic crontab generation for time-based restrictions
- [x] Schedule Templates - Pre-built patterns for common use cases
  - [x] Common intervals: `every_minute()`, `every_5_minutes()`, `every_15_minutes()`, `every_30_minutes()`
  - [x] Hourly patterns: `hourly()`, `every_2_hours()`, `every_6_hours()`, `every_12_hours()`
  - [x] Daily patterns: `daily_at_midnight()`, `daily_at_hour()`
  - [x] Weekly patterns: `weekdays_at()`, `weekly_on_monday()`, `weekend_mornings()`
  - [x] Monthly patterns: `monthly_first_day()`, `monthly_last_day()`
  - [x] Business hours: `business_hours_hourly()`, `business_hours_every_15_minutes()`
  - [x] Quarterly: `quarterly()` - Jan/Apr/Jul/Oct 1st
  - [x] 18 pre-built templates covering common scheduling needs
- [x] Comprehensive example (`examples/schedule_builders.rs`)
  - [x] Demonstrates all builder methods
  - [x] Shows all template patterns
  - [x] 8 practical use cases
  - [x] Builder vs Template comparison
- [x] Full doc test coverage (26 new doc tests)
- [x] Zero warnings, all tests passing (271 tests + 62 doc tests)

### Enhanced Timezone Utilities ✅ (2026-01-06)
- [x] Comprehensive timezone conversion and detection utilities
  - [x] `detect_system_timezone()` - Automatic system timezone detection
  - [x] `is_valid_timezone()` - Timezone validation
  - [x] `list_all_timezones()` - List all 600+ IANA timezones
  - [x] `search_timezones()` - Pattern-based timezone search
  - [x] `is_dst_active()` - DST status detection
  - [x] `get_utc_offset()` - UTC offset calculation
  - [x] `get_timezone_info()` - Detailed timezone information
  - [x] `convert_between_timezones()` - Time conversion between zones
  - [x] `get_common_timezone_abbreviations()` - Common abbreviation mapping
  - [x] `TimezoneInfo` struct with detailed timezone data
  - [x] Display trait implementation for human-readable output
  - [x] Full doc test coverage (12 new doc tests)
  - [x] Platform-specific timezone detection (Linux, macOS)
  - [x] Zero warnings, all tests passing (268 tests + 36 doc tests)

### Weighted Fair Queuing (WFQ) ✅ (2026-01-05)
- [x] Advanced fair scheduling algorithm
  - [x] Virtual time-based task scheduling
  - [x] Proportional fairness based on task weights
  - [x] Weight validation (0.1-10.0 range)
  - [x] Global virtual time tracking
  - [x] Task weight configuration via fluent API
  - [x] `get_due_tasks_wfq()` - Get tasks ordered by fairness
  - [x] `update_wfq_after_execution()` - Update virtual times
  - [x] `get_wfq_stats()` - Statistics on weight distribution
  - [x] `WFQState` serialization for persistence
  - [x] Prevents starvation while respecting priorities
  - [x] Production-ready example demonstrating fairness
  - [x] 12 comprehensive tests validating WFQ behavior
  - [x] Doc tests for all public APIs

### Production-Ready Tooling & Examples ✅
- [x] API enhancements for production tooling
  - [x] `export_state()` - Export scheduler state as JSON string
  - [x] `list_tasks()` - Access all scheduled tasks via HashMap
  - [x] `get_task(name)` - Get specific task by name
  - [x] Full doc tests for new API methods
- [x] Production deployment example
  - [x] Graceful shutdown with signal handling
  - [x] State persistence across restarts
  - [x] Health check background task
  - [x] Metrics collection and reporting
  - [x] Alert monitoring background task
  - [x] Error recovery with exponential backoff
  - [x] Distributed deployment support with instance IDs
  - [x] Comprehensive production patterns
- [x] CLI utilities example
  - [x] Task listing with formatted output
  - [x] Task inspection with detailed information
  - [x] Schedule validation and health checks
  - [x] Schedule preview and reporting
  - [x] Metrics display
  - [x] State export functionality
  - [x] Conflict detection and dependency analysis
  - [x] Production-ready CLI command patterns
- [x] Performance benchmark suite
  - [x] Task addition performance (10,000 tasks)
  - [x] Due task lookup benchmarks
  - [x] Schedule evaluation latency validation
  - [x] Memory usage estimation
  - [x] Priority-based retrieval benchmarks
  - [x] Batch operation benchmarks
  - [x] State persistence benchmarks
- [x] Comprehensive user documentation
  - [x] CRONTAB_GUIDE.md - Complete crontab syntax reference
  - [x] PRODUCTION_GUIDE.md - Production deployment patterns
  - [x] CELERY_MIGRATION.md - Migration guide from Python Celery
- [x] Zero warnings, all tests passing (249 tests + 23 doc tests)

### Advanced Scheduler Features: Starvation Prevention & Utilities ✅
- [x] Starvation prevention mechanism
  - [x] `get_tasks_with_starvation_prevention()` - Identify and boost priority of starved tasks
  - [x] `get_due_tasks_with_starvation_prevention()` - Get due tasks with anti-starvation boosting
  - [x] Configurable starvation threshold (default: 60 minutes)
  - [x] Configurable priority boost amount (default: 2 levels)
  - [x] Automatic boost for tasks waiting longer than threshold
  - [x] `TaskWaitingInfo` structure with boost reason tracking
- [x] Schedule preview and dry-run utilities
  - [x] `preview_upcoming_executions()` - Preview next N execution times for tasks
  - [x] `dry_run()` - Simulate scheduler execution without running tasks
  - [x] Configurable simulation duration and tick interval
  - [x] Execution timeline generation for capacity planning
- [x] Comprehensive scheduler statistics
  - [x] `get_comprehensive_stats()` - Detailed scheduler performance metrics
  - [x] `SchedulerStatistics` structure with execution counts, rates, health metrics
  - [x] Uptime tracking and oldest/newest execution timestamps
  - [x] Average execution duration calculations
  - [x] Display implementation for human-readable output
- [x] Timezone conversion utilities
  - [x] `TimezoneUtils::format_in_timezone()` - Convert UTC to specific timezone
  - [x] `TimezoneUtils::current_time_in_zones()` - Get current time in multiple timezones
  - [x] `TimezoneUtils::time_until_next_occurrence()` - Calculate time until target hour in timezone
  - [x] Full DST handling via chrono-tz integration
- [x] Documentation and testing
  - [x] Full doc test coverage for all new features
  - [x] Zero warnings, all tests passing (237 tests + 18 doc tests)

### Priority-Based Scheduling & Missed Schedule Detection ✅
- [x] Priority-based task execution
  - [x] `get_due_tasks_by_priority()` - Get due tasks sorted by priority (highest first)
  - [x] `get_tasks_by_priority()` - Get all tasks sorted by priority
  - [x] Priority range: 1 (lowest) to 9 (highest), default: 5
  - [x] Secondary sort by next run time for equal priorities
  - [x] Full doc test coverage
- [x] Missed schedule detection system
  - [x] `detect_missed_schedules()` - Identify tasks that missed execution windows
  - [x] `check_missed_schedules()` - Automatically trigger alerts for missed schedules
  - [x] `get_missed_schedule_stats()` - Get detailed statistics sorted by severity
  - [x] Configurable grace period (default: 60 seconds)
  - [x] Integration with alert manager for automatic notifications
- [x] Country-specific holiday calendar enhancements
  - [x] `HolidayCalendar::len()` and `is_empty()` - Query methods
  - [x] Comprehensive US federal holidays (11 holidays with floating dates)
  - [x] Japan national holidays (16 holidays)
  - [x] UK public holidays (8 bank holidays)
  - [x] Canada statutory holidays (9 federal holidays)
  - [x] Helper functions for Nth weekday and last weekday of month
- [x] Advanced examples
  - [x] `advanced_features.rs` - Comprehensive demonstration of all features
  - [x] Examples for holiday calendars, priority execution, conflict resolution
  - [x] Examples for missed schedule detection and health monitoring
- [x] Zero warnings, all tests passing (237 tests + 15 doc tests)

### Advanced Scheduling Features ✅
- [x] Efficient schedule indexing
  - [x] `ScheduleIndex` - Fast task lookup by schedule type and next run time
  - [x] Priority queue ordering for next due tasks
  - [x] O(log n) task lookup vs O(n) iteration
  - [x] Type-based indexing (interval, crontab, solar, onetime)
  - [x] Automatic index rebuild and dirty tracking
- [x] Solar schedule enhancements
  - [x] Civil twilight support (dawn/dusk, 6° below horizon)
  - [x] Nautical twilight support (12° below horizon)
  - [x] Astronomical twilight support (18° below horizon)
  - [x] Golden hour calculations (sunrise to +30min, sunset -30min)
  - [x] Backward compatible with existing sunrise/sunset
- [x] Advanced calendar features
  - [x] `BlackoutPeriod` - Define time ranges when tasks should not execute
  - [x] Recurring blackout patterns (Daily, Weekly, Monthly)
  - [x] `CalendarWithBlackout` - Combined holiday/business/blackout calendar
  - [x] Holidays-only execution mode
  - [x] Next valid time calculation respecting all calendar constraints
- [x] Schedule composition
  - [x] `CompositeSchedule` - Combine multiple schedules with AND/OR logic
  - [x] AND mode - All schedules must be due (intersection)
  - [x] OR mode - Any schedule can be due (union)
  - [x] `CustomSchedule` - User-defined scheduling logic via closures
  - [x] Display implementations for debugging
- [x] Zero warnings, all tests passing (225 tests + 14 doc tests)

### Performance & Calendar Features ✅
- [x] Performance optimizations
  - [x] Schedule caching - Next run time caching with automatic invalidation
  - [x] Batch task operations - `add_tasks_batch()` and `remove_tasks_batch()`
  - [x] Reduced state persistence overhead for bulk operations
- [x] Webhook alert delivery
  - [x] `WebhookConfig` - Configure webhook endpoints with headers and timeout
  - [x] Alert level filtering - Send only specific alert levels to webhooks
  - [x] JSON payload generation - Structured alert data for external systems
  - [x] Integration with AlertManager callbacks
- [x] Business day calendar support
  - [x] `DayOfWeek` enum with weekend/weekday detection
  - [x] `BusinessHours` - Configure business hours (default 9 AM - 5 PM)
  - [x] `BusinessCalendar` - Define working days and business hours
  - [x] Business time validation and next business time calculation
- [x] Holiday calendar integration
  - [x] `Holiday` and `HolidayCalendar` - Define and manage holidays
  - [x] Holiday checking by date
  - [x] Next non-holiday calculation
  - [x] US federal holidays preset
  - [x] Full serialization support
- [x] Zero warnings, all tests passing (225 tests + 11 doc tests)

### Crash Recovery System ✅
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

### Alerting System ✅
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

### Schedule Conflict Detection ✅
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

### Timezone Support for Crontab ✅
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

### Schedule Locking ✅
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
- [x] Zero warnings in cargo clippy
- [x] All tests passing (312 total: unit + doc tests across 12 focused modules)
- [x] Comprehensive test coverage

### Phase 9: Heartbeat & Failover ✅ COMPLETE
- [x] BeatRole enum (Leader, Standby, Unknown)
- [x] HeartbeatInfo with instance metadata
- [x] HeartbeatConfig (5s heartbeat, 30s lease, 45s failover)
- [x] BeatHeartbeat with leader election and lease renewal
- [x] Automatic failover detection and promotion
- [x] HeartbeatStats with atomic counters
- [x] BeatScheduler integration (is_leader check)

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
- [x] Solar schedules enhancements ✅
  - [x] Twilight schedules (civil, nautical, astronomical)
  - [x] Golden hour calculations
  - [ ] Seasonal adjustments (requires more complex solar calculations)
- [x] Custom schedule types ✅
  - [x] `CustomSchedule` - User-defined schedule logic via closures
  - [x] `CompositeSchedule` - Schedule composition with AND/OR logic
  - [ ] Plugin system for schedules (requires dynamic loading)

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
  - [x] Priority-based automatic resolution ✅
    - [x] `auto_resolve_conflicts()` - Apply resolutions based on task priorities
    - [x] `preview_conflict_resolutions()` - Preview resolutions without applying
    - [x] `clear_conflict_jitter()` - Reset jitter configurations
    - [x] Different priority handling: Jitter applied to lower priority task
    - [x] Equal priority handling: Symmetric jitter applied to both tasks
    - [x] High severity handling: Manual review recommendation
  - [ ] Resource conflict resolution (requires resource tracking)

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
  - [x] Failure notifications ✅ (built-in callback system)
- [x] Task retry on scheduler crash ✅
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
- [x] Schedule prioritization ✅
  - [x] Priority-based execution (`get_due_tasks_by_priority()`, `get_tasks_by_priority()`)
  - [x] Task ordering by priority level (9=highest, 1=lowest)
  - [x] Secondary sorting by next run time for equal priorities
  - [x] Starvation prevention ✅
    - [x] `get_tasks_with_starvation_prevention()` - Automatic priority boosting
    - [x] `get_due_tasks_with_starvation_prevention()` - Anti-starvation task scheduling
    - [x] Configurable threshold and boost amount
  - [x] Weighted fair queuing ✅ (2026-01-05)
    - [x] WFQ algorithm implementation with virtual time tracking
    - [x] `TaskWeight` structure with validation (0.1-10.0 range)
    - [x] `WFQState` for tracking virtual start/finish times
    - [x] `get_due_tasks_wfq()` - Get tasks ordered by virtual finish time
    - [x] `update_wfq_after_execution()` - Update virtual times after execution
    - [x] `get_wfq_stats()` - Get WFQ statistics (weights, global virtual time)
    - [x] `with_wfq_weight()` - Fluent API for setting task weights
    - [x] Global virtual time calculation across all tasks
    - [x] Fair execution proportional to task weights
    - [x] Comprehensive test suite (12 tests covering all WFQ scenarios)
    - [x] Example demonstrating WFQ usage and fairness
    - [x] Serialization support for persistence
- [x] Schedule timezone conversion ✅ (2026-01-06)
  - [x] Multi-timezone support
    - [x] `current_time_in_zones()` - Get current time in multiple timezones
    - [x] `convert_between_timezones()` - Convert time between timezones
    - [x] `get_timezone_info()` - Get detailed timezone information
  - [x] Automatic timezone detection
    - [x] `detect_system_timezone()` - Detect system's local timezone
    - [x] Supports TZ environment variable
    - [x] Reads /etc/timezone (Debian/Ubuntu)
    - [x] Reads /etc/localtime symlink (Unix/macOS)
  - [x] Timezone utilities
    - [x] `is_valid_timezone()` - Validate IANA timezone names
    - [x] `list_all_timezones()` - List all 600+ available timezones
    - [x] `search_timezones()` - Search timezones by pattern
    - [x] `is_dst_active()` - Check DST status at specific time
    - [x] `get_utc_offset()` - Get UTC offset in seconds
    - [x] `get_common_timezone_abbreviations()` - Map abbreviations (EST, PST, etc.) to IANA names
    - [x] `TimezoneInfo` struct with Display trait
  - [x] Full doc test coverage for all new APIs
  - [x] Zero warnings, all tests passing (268 tests + 36 doc tests)

### Monitoring & Observability
- [x] Scheduler metrics ✅
  - [x] Execution count per schedule
  - [x] Execution latency (via duration tracking)
  - [x] Task health metrics
  - [x] Success/failure/timeout counts
  - [x] Comprehensive statistics ✅
    - [x] `get_comprehensive_stats()` - Detailed performance metrics
    - [x] Uptime tracking and execution timestamps
    - [x] Average duration calculations
- [x] Schedule preview and testing utilities ✅
  - [x] `preview_upcoming_executions()` - Preview next N executions
  - [x] `dry_run()` - Simulate scheduler execution for testing
  - [x] Execution timeline generation
- [x] Timezone utilities ✅
  - [x] `TimezoneUtils::format_in_timezone()` - UTC to timezone conversion
  - [x] `TimezoneUtils::current_time_in_zones()` - Multi-timezone display
  - [x] `TimezoneUtils::time_until_next_occurrence()` - Timezone-aware scheduling
- [x] Schedule health checks ✅
  - [x] Validate schedule syntax
  - [x] Check next run time
  - [x] Detect stuck schedules
- [x] Execution history ✅
  - [x] Last N executions
  - [x] Execution timestamps
  - [x] Execution duration
  - [x] Execution results
- [x] Alerting ✅
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
  - [x] Webhook alert delivery ✅
    - [x] WebhookConfig for configuring webhook endpoints
    - [x] Custom HTTP headers support
    - [x] Timeout configuration
    - [x] Alert level filtering
    - [x] JSON payload generation
    - [x] Integration with alert manager callbacks
  - [ ] Email/SMS notification integration (requires external service)

### Calendar Integration
- [x] Holiday calendar support ✅
  - [x] Holiday definition and storage
  - [x] Holiday checking by date
  - [x] Next non-holiday calculation
  - [x] US federal holidays preset (all 11 federal holidays) ✅
  - [x] Japan national holidays preset (16 holidays) ✅
  - [x] UK public holidays preset (8 bank holidays) ✅
  - [x] Canada statutory holidays preset (9 federal holidays) ✅
  - [x] Custom holiday addition
  - [x] Serialization support
- [x] Business day calculations ✅
  - [x] DayOfWeek enum (Monday-Sunday)
  - [x] Weekend/weekday detection
  - [x] Business hours configuration
  - [x] BusinessCalendar with working days
  - [x] Business time validation
  - [x] Next business time calculation
  - [x] Standard business hours (9 AM - 5 PM, Mon-Fri)
  - [x] Custom business hours and working days
- [x] Advanced calendar features ✅
  - [x] Execute on holidays only (holidays_only mode)
  - [x] Blackout periods with recurring patterns
  - [x] `CalendarWithBlackout` - Unified calendar system
  - [x] Next valid time calculation
  - [x] Country-specific calendar presets ✅
    - [x] US federal holidays (comprehensive 11-holiday preset)
    - [x] Japan national holidays (16-holiday preset)
    - [x] UK public holidays (8-holiday preset for England and Wales)
    - [x] Canada statutory holidays (9-holiday federal preset)
  - [ ] Special event schedules (can be implemented via CustomSchedule)

### API & Management
- [x] CLI integration ✅
  - [x] Schedule listing and inspection (cli_utilities example)
  - [x] Status inspection and health checks
  - [x] Metrics and statistics display
  - [x] Schedule validation
  - [x] State export functionality
  - [x] Conflict and dependency analysis
  - [ ] Manual trigger execution (requires broker integration)
- [ ] REST API for schedule management
  - [ ] CRUD operations
  - [ ] Search and filter
  - [ ] Bulk operations
- [ ] Web UI for schedules
  - [ ] Visual schedule editor
  - [ ] Calendar view
  - [ ] Execution timeline
- [x] Missed schedule alerts ✅
  - [x] `detect_missed_schedules()` - Detect tasks that missed their schedule window
  - [x] `check_missed_schedules()` - Trigger alerts for missed schedules
  - [x] `get_missed_schedule_stats()` - Get detailed statistics on missed schedules
  - [x] Configurable grace period
  - [x] Integration with alerting system
- [x] Schedule execution history ✅ (already implemented)
  - [x] Execution tracking with timestamps
  - [x] Duration metrics
  - [x] Success/failure/timeout counts
- [x] Scheduler health checks ✅ (already implemented)
  - [x] Task health validation
  - [x] Stuck detection
  - [x] Failure rate monitoring

### Performance
- [x] Schedule caching ✅
  - [x] Next run time caching in ScheduledTask
  - [x] Automatic cache invalidation on schedule changes
  - [x] Cache update on task execution
  - [x] Reduced expensive cron/solar calculations
- [x] Batch task enqueuing ✅
  - [x] `add_tasks_batch()` - Add multiple tasks efficiently
  - [x] `remove_tasks_batch()` - Remove multiple tasks efficiently
  - [x] Single state save per batch operation
  - [x] Improved performance for bulk operations
- [x] Efficient schedule indexing ✅
  - [x] `ScheduleIndex` - Priority queue for next run times
  - [x] Index by schedule type (interval, crontab, solar, onetime)
  - [x] Fast due task lookup with O(log n) complexity
  - [x] Automatic index rebuild and maintenance
  - [x] Dirty tracking for incremental updates
- [x] Performance benchmarks ✅
  - [x] Task addition benchmarks (10,000 tasks)
  - [x] Due task lookup performance validation
  - [x] Schedule evaluation latency tests (<10ms requirement)
  - [x] Memory usage per task validation (100-300B range)
  - [x] Priority retrieval performance tests
  - [x] Batch operation performance tests
  - [x] State persistence I/O benchmarks

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
- [x] Scheduler loop tests ✅
- [x] Timezone tests ✅

## Documentation

- [x] Comprehensive README
- [x] Schedule types documentation
- [x] Basic examples
- [x] Crontab syntax guide ✅
- [x] Production deployment guide ✅
- [x] Migration from Celery Beat ✅

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
