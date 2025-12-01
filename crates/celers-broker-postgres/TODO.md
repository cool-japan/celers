# celers-broker-postgres TODO

> PostgreSQL-based broker implementation for CeleRS

## Status: ✅ FEATURE COMPLETE

PostgreSQL broker with FOR UPDATE SKIP LOCKED pattern, migrations, DLQ support, high-performance batch operations, queue control, and comprehensive maintenance tools.

## Completed Features

### Core Operations ✅
- [x] `enqueue()` - Insert tasks into database
- [x] `dequeue()` - Fetch with FOR UPDATE SKIP LOCKED
- [x] `ack()` - Update task state to completed
- [x] `reject()` - Handle failed tasks
- [x] `queue_size()` - Count pending tasks
- [x] `cancel()` - Cancel pending/processing tasks
- [x] Transaction support for atomicity

### Database Schema ✅
- [x] `tasks` table with all required columns
- [x] `dlq` (dead letter queue) table
- [x] `task_history` table for auditing
- [x] Indexes for performance
- [x] State enum (pending, processing, completed, failed, cancelled)
- [x] Priority column for task ordering

### Dead Letter Queue ✅
- [x] Automatic DLQ on max retries
- [x] DLQ table structure
- [x] Failed task archiving
- [x] `list_dlq()` - List DLQ tasks with pagination
- [x] `requeue_from_dlq()` - Requeue task from DLQ
- [x] `purge_dlq()` - Delete single DLQ task
- [x] `purge_all_dlq()` - Delete all DLQ tasks

### Migrations ✅
- [x] Initial schema migration (001_init.sql)
- [x] Includes DLQ table and indexes
- [x] `move_to_dlq()` stored function
- [x] Migration documentation

## Configuration

### Connection ✅
- [x] PostgreSQL connection string
- [x] Connection pooling via sqlx
- [x] Configurable queue table name
- [x] Async query execution

### Batch Operations ✅
- [x] `enqueue_batch()` - Multiple tasks in single transaction
- [x] `dequeue_batch()` - Fetch multiple tasks atomically
- [x] `ack_batch()` - Acknowledge multiple tasks efficiently
- [x] Optimized for high-throughput scenarios
- [x] Maintains FOR UPDATE SKIP LOCKED safety

### Delayed Task Execution ✅
- [x] `enqueue_at(task, timestamp)` - Schedule for specific Unix timestamp
- [x] `enqueue_after(task, delay_secs)` - Schedule after delay in seconds
- [x] Uses existing `scheduled_at` column with index
- [x] Automatic processing when tasks are ready (in dequeue)
- [x] Supports both immediate and delayed execution

### Queue Control ✅
- [x] `pause()` - Pause queue processing
- [x] `resume()` - Resume queue processing
- [x] `is_paused()` - Check pause state
- [x] Dequeue respects pause state

### Task Inspection ✅
- [x] `get_task()` - Get detailed task information
- [x] `list_tasks()` - List tasks by state with pagination
- [x] `get_statistics()` - Get comprehensive queue statistics
- [x] `TaskInfo` struct with all task details
- [x] `DbTaskState` enum for type-safe state handling
- [x] `QueueStatistics` struct for queue metrics

### Observability ✅
- [x] Prometheus metrics (optional feature)
- [x] Tasks enqueued counter (total and per-type)
- [x] Queue size gauges (pending, processing, DLQ)
- [x] `update_metrics()` method for gauge updates
- [x] Batch operation metrics tracking
- [x] Connection pool metrics via `check_health()`

### Maintenance ✅
- [x] `check_health()` - Database health check with pool stats
- [x] `archive_completed_tasks()` - Archive old completed tasks
- [x] `recover_stuck_tasks()` - Recover tasks stuck in processing
- [x] `purge_all()` - Purge all tasks (with warning)
- [x] `HealthStatus` struct with comprehensive health info
- [x] `analyze_tables()` - Update PostgreSQL statistics for query planner

### Task Result Storage ✅
- [x] `celers_task_results` table for result backend
- [x] `store_result()` - Store task execution results
- [x] `get_result()` - Retrieve task results by ID
- [x] `delete_result()` - Remove task results
- [x] `archive_results()` - Archive old results
- [x] `TaskResult` struct with status, result, error, traceback
- [x] `TaskResultStatus` enum (PENDING, STARTED, SUCCESS, FAILURE, RETRY, REVOKED)
- [x] Upsert support for result updates

### Database Monitoring ✅
- [x] `get_table_sizes()` - Table size information for CeleRS tables
- [x] `get_index_usage()` - Index usage statistics
- [x] `get_unused_indexes()` - Identify unused indexes for cleanup
- [x] `TableSizeInfo` struct with row count, sizes
- [x] `IndexUsageInfo` struct with scan counts

### Migrations ✅
- [x] 001_init.sql - Initial schema (tasks, DLQ, history)
- [x] 002_results.sql - Task results table with indexes
- [x] Additional indexes for performance

## Future Enhancements

### Performance
- [ ] Table partitioning for large queues
- [ ] Query optimization for high throughput

### Advanced Features
- [ ] Task dependencies/DAG support
- [ ] Multi-tenant queue support (beyond queue_name)

### Maintenance
- [ ] VACUUM automation

## Testing Status

- [x] Compilation tests
- [x] Unit tests for types (DbTaskState, TaskResultStatus, etc.)
- [ ] Unit tests with mock database
- [ ] Integration tests with real PostgreSQL
- [ ] Concurrency tests (FOR UPDATE SKIP LOCKED)
- [ ] Performance benchmarks
- [ ] Migration testing

## Documentation

- [x] Module-level documentation
- [x] Migration files with comments
- [ ] PostgreSQL tuning guide
- [ ] Index strategy documentation
- [ ] Scaling recommendations
- [ ] Backup/restore procedures

## Dependencies

- `celers-core`: Core traits
- `sqlx`: PostgreSQL async driver
- `serde_json`: Task serialization
- `tracing`: Logging

## PostgreSQL Configuration

Recommended settings:
```sql
-- Connection pooling
max_connections = 100

-- Query performance
shared_buffers = 256MB
effective_cache_size = 1GB
work_mem = 16MB

-- Autovacuum
autovacuum = on
autovacuum_naptime = 60s

-- Logging
log_min_duration_statement = 1000
```

## Schema Design

### Tasks Table
- `id`: UUID primary key
- `name`: Task type identifier
- `payload`: JSONB task data
- `state`: Enum (pending/processing/completed/failed)
- `priority`: Integer for ordering
- `created_at`: Timestamp
- `retries`: Current retry count
- `max_retries`: Maximum allowed retries
- `timeout_secs`: Task timeout

### Indexes
- `(state, priority DESC)` for efficient dequeue
- `created_at` for archiving queries
- Consider GIN index on `payload` for searches

## Notes

- Uses PostgreSQL FOR UPDATE SKIP LOCKED for atomic dequeue
- Supports concurrent workers safely
- JSONB payload allows flexible task data
- Priority ordering for task selection
- Transaction-based operations for consistency
- Automatic retry handling via reject()
