# celers-beat TODO

> Periodic task scheduler (Celery Beat equivalent)

## Status: ✅ FEATURE COMPLETE

All schedule types implemented and production-ready.

## Completed Features

### Schedule Types ✅
- [x] Interval schedule (every N seconds)
- [x] Crontab schedule (full implementation with cron crate)
- [x] Solar schedule (sunrise/sunset with sunrise crate)
- [x] Schedule utility methods
  - [x] `is_interval()`, `is_crontab()`, `is_solar()` - Schedule type checks
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

## Future Enhancements

### Schedule Types
- [ ] Crontab with timezone support
  - [ ] Timezone-aware parsing
  - [ ] DST handling
  - [ ] UTC conversion utilities
- [ ] Solar schedules enhancements
  - [ ] Twilight schedules (civil, nautical, astronomical)
  - [ ] Golden hour calculations
  - [ ] Seasonal adjustments
- [ ] Custom schedule types
  - [ ] Plugin system for schedules
  - [ ] User-defined schedule logic
  - [ ] Schedule composition
- [ ] One-time schedules (run once at specific time)
  - [ ] Absolute timestamp scheduling
  - [ ] Relative delay scheduling
  - [ ] Auto-cleanup after execution

### Scheduler Features
- [x] Persistent schedule state (file-based JSON) ✅
- [x] Dynamic schedule updates (add/remove at runtime) ✅
- [ ] Schedule versioning
  - [ ] Track schedule modifications
  - [ ] Rollback to previous versions
  - [ ] Migration on version changes
- [ ] Schedule locking (prevent duplicates)
  - [ ] Distributed lock acquisition
  - [ ] Lock timeout handling
  - [ ] Lock renewal mechanism
- [ ] Catch-up logic (run missed schedules)
  - [ ] Configurable catch-up window
  - [ ] Catch-up throttling
  - [ ] Skip vs execute decision logic
- [ ] Schedule conflict detection
  - [ ] Overlapping schedule detection
  - [ ] Resource conflict resolution
  - [ ] Priority-based execution order
- [ ] Schedule groups and tags
  - [ ] Group schedules by category
  - [ ] Tag-based filtering
  - [ ] Bulk operations on groups

### Task Management
- [ ] Task dependency tracking
  - [ ] Define task dependencies
  - [ ] Dependency chain resolution
  - [ ] Wait for dependencies
- [ ] Task failure handling
  - [ ] Retry on failure
  - [ ] Exponential backoff
  - [ ] Failure notifications
- [ ] Task retry on scheduler crash
  - [ ] Crash detection
  - [ ] Automatic retry
  - [ ] State recovery
- [ ] Task result tracking
  - [ ] Store execution results
  - [ ] Result history
  - [ ] Success/failure analytics

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
- [ ] Schedule jitter (avoid thundering herd)
  - [ ] Random jitter
  - [ ] Configurable jitter window
  - [ ] Hash-based jitter
- [ ] Schedule timezone conversion
  - [ ] Multi-timezone support
  - [ ] Automatic timezone detection
  - [ ] Timezone database updates

### Monitoring & Observability
- [ ] Scheduler metrics
  - [ ] Execution count per schedule
  - [ ] Execution latency
  - [ ] Missed execution count
  - [ ] Catch-up execution count
- [ ] Schedule health checks
  - [ ] Validate schedule syntax
  - [ ] Check next run time
  - [ ] Detect stuck schedules
- [ ] Execution history
  - [ ] Last N executions
  - [ ] Execution timestamps
  - [ ] Execution duration
  - [ ] Execution results
- [ ] Alerting
  - [ ] Alert on missed schedules
  - [ ] Alert on failures
  - [ ] Alert on slow execution

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

- [ ] Interval schedule tests
- [ ] Crontab parsing tests
- [ ] Next run calculation tests
- [ ] Scheduler loop tests
- [ ] Task enable/disable tests
- [ ] Persistence tests
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
