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
- [x] Custom pool configuration (`with_pool_config`)
- [x] Queue name accessor method (`queue_name()`)

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
- [x] `vacuum_tables()` - Manual VACUUM for space reclamation
- [x] `count_by_state()` - Count tasks by state
- [x] `count_scheduled()` - Count tasks scheduled for future execution
- [x] `cancel_all_pending()` - Cancel all pending tasks

### Diagnostics & Metrics ✅
- [x] `test_connection()` - Simple connectivity test
- [x] `oldest_pending_age_secs()` - Age of oldest pending task
- [x] `oldest_processing_age_secs()` - Age of oldest processing task (stuck detection)
- [x] `avg_processing_time_ms()` - Average task processing time
- [x] `retry_rate()` - Percentage of retried tasks
- [x] `success_rate()` - Task success rate

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

### Recent Enhancements ✅

#### Custom Retry Strategies (2025-12)
- [x] `RetryStrategy` enum with multiple strategies:
  - `Exponential`: Classic exponential backoff (2^n seconds)
  - `ExponentialWithJitter`: Exponential with jitter to prevent thundering herd
  - `Linear`: Linear backoff (base_delay * retry_count)
  - `Fixed`: Fixed delay between retries
  - `Immediate`: No delay (immediate retry)
- [x] `set_retry_strategy()` method to configure retry behavior
- [x] `retry_strategy()` getter method
- [x] Comprehensive unit tests for all strategies

#### Connection Pool Metrics (2025-12)
- [x] `PoolMetrics` struct with detailed pool statistics
- [x] `get_pool_metrics()` method for real-time pool monitoring
- [x] Prometheus metrics integration:
  - `celers_postgres_pool_max_size`
  - `celers_postgres_pool_size`
  - `celers_postgres_pool_idle`
  - `celers_postgres_pool_in_use`
- [x] Updated `update_metrics()` to include pool metrics

#### Task Query & Filtering (2025-12)
- [x] `find_tasks_by_metadata()` - Query tasks using JSONB metadata
- [x] `count_tasks_by_metadata()` - Count tasks matching metadata criteria
- [x] `find_tasks_by_name()` - Find tasks by task name with optional state filter
- [x] Leverages existing GIN index on metadata for efficient queries

#### Automated Maintenance (2025-12)
- [x] `start_maintenance_scheduler()` - Background maintenance task with configurable interval
- [x] Automatic VACUUM and ANALYZE scheduling
- [x] Automatic archiving of old completed tasks (7 days)
- [x] Automatic archiving of old results (30 days)
- [x] Automatic recovery of stuck processing tasks (1 hour threshold)
- [x] Configurable VACUUM vs ANALYZE-only mode
- [x] Graceful error handling with tracing

#### Performance Benchmarks (2025-12)
- [x] Criterion-based benchmark suite
- [x] Retry strategy performance benchmarks
- [x] State conversion benchmarks
- [x] Scaling benchmarks for retry calculations
- [x] Benchmarks for serialization/deserialization

## Future Enhancements

### Performance
- [x] Connection pool metrics exporter (get_pool_metrics, Prometheus integration)
- [ ] Table partitioning for large queues (for very large deployments)
- [ ] Query optimization for high throughput (current performance is good)

### Advanced Features
- [x] Custom retry strategies (Exponential, ExponentialWithJitter, Linear, Fixed, Immediate)
- [x] Task metadata query methods (find_tasks_by_metadata, count_tasks_by_metadata)
- [x] Task name filtering (find_tasks_by_name)
- [ ] Task dependencies/DAG support (would require schema changes)
- [ ] Multi-tenant queue support with stronger isolation (beyond queue_name)
- [ ] Task chaining and workflows

### Maintenance
- [x] Manual VACUUM (`vacuum_tables()` method available)
- [x] Automated VACUUM scheduler (`start_maintenance_scheduler()` method)

## Testing Status

- [x] Compilation tests (all passing, no warnings)
- [x] Unit tests for types (DbTaskState, TaskResultStatus, etc.)
- [x] Unit tests for retry strategies (all 5 strategies tested)
- [x] Unit tests for QueueStatistics
- [x] Doc tests for module-level examples (6 total, 3 passing, 3 ignored)
- [x] Integration test examples (marked as #[ignore])
- [x] Total: 18 tests (15 passing, 3 ignored requiring PostgreSQL)
- [x] Performance benchmarks (Criterion-based, 3 benchmark groups)
  - Retry strategy benchmarks (5 strategies)
  - State conversion benchmarks (4 operations)
  - Scaling benchmarks (4 scales × 2 strategies)
- [ ] Unit tests with mock database (future enhancement)
- [ ] Full integration test suite with real PostgreSQL (future enhancement)
- [ ] Concurrency stress tests (future enhancement)
- [ ] Migration testing (migrations are idempotent and safe)

## Documentation

- [x] Module-level documentation
- [x] Migration files with comments
- [x] Comprehensive README with usage examples
- [x] Doc tests for key functionality
- [x] PostgreSQL tuning guide (in README)
- [x] Performance characteristics (in README)
- [x] Index strategy documentation (in migration files)
- [x] Scaling recommendations (in README)
- [x] Backup/restore procedures (comprehensive guide in README)

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
