# celers-broker-sql TODO

> MySQL database broker implementation for CeleRS

## Status: FEATURE COMPLETE

MySQL broker with FOR UPDATE SKIP LOCKED pattern, migrations, DLQ support, high-performance batch operations, queue control, task inspection, result storage, worker tracking, and comprehensive maintenance utilities.

## Completed Features

### Core Operations
- [x] `enqueue()` - Insert tasks into MySQL database
- [x] `dequeue()` - Fetch with FOR UPDATE SKIP LOCKED
- [x] `ack()` - Update task state to completed
- [x] `reject()` - Handle failed tasks with retry logic
- [x] `queue_size()` - Count pending tasks
- [x] `cancel()` - Cancel pending/processing tasks
- [x] Transaction support for atomicity

### Database Schema
- [x] `celers_tasks` table with all required columns
- [x] `celers_dead_letter_queue` (DLQ) table
- [x] `celers_task_history` table for auditing
- [x] `celers_task_results` table for result storage
- [x] Indexes for performance (including 003_performance_indexes.sql)
- [x] State enum (pending, processing, completed, failed, cancelled)
- [x] Priority column for task ordering

### Dead Letter Queue
- [x] Automatic DLQ on max retries
- [x] DLQ table structure
- [x] Failed task archiving via stored procedure
- [x] DLQ inspection queries (`list_dlq`)
- [x] Requeue from DLQ (`requeue_from_dlq`)
- [x] Purge DLQ (`purge_dlq`, `purge_all_dlq`)

### Migrations
- [x] Initial schema migration (001_init.sql)
- [x] Results table migration (002_results.sql)
- [x] Performance indexes migration (003_performance_indexes.sql)
- [x] MySQL-specific data types (CHAR(36) for UUID, MEDIUMBLOB, JSON)
- [x] Stored procedure for DLQ operations
- [x] Migration documentation

### Batch Operations
- [x] Batch enqueue (multiple tasks in single transaction)
- [x] Batch dequeue (fetch multiple tasks atomically)
- [x] Batch ack (acknowledge multiple tasks in single query)
- [x] Optimized for high-throughput scenarios
- [x] Maintains FOR UPDATE SKIP LOCKED safety

### Delayed Task Execution
- [x] `enqueue_at(task, timestamp)` - Schedule for specific Unix timestamp
- [x] `enqueue_after(task, delay_secs)` - Schedule after delay in seconds
- [x] Uses existing `scheduled_at` column with index
- [x] Automatic processing when tasks are ready (in dequeue)
- [x] MySQL DATE_ADD() for relative delays

### Queue Control
- [x] `pause()` - Pause the queue (dequeue returns None)
- [x] `resume()` - Resume queue processing
- [x] `is_paused()` - Check queue pause state
- [x] Atomic pause state with AtomicBool

### Task Inspection
- [x] `get_task()` - Get detailed info about a specific task
- [x] `list_tasks()` - List tasks by state with pagination
- [x] `get_statistics()` - Get queue statistics (pending, processing, completed, failed, cancelled, DLQ)
- [x] `count_by_task_name()` - Get statistics grouped by task name
- [x] `get_processing_tasks()` - Get all currently processing tasks
- [x] `get_tasks_by_worker()` - Get tasks by worker ID
- [x] `list_scheduled_tasks()` - List tasks scheduled for the future
- [x] `count_scheduled_tasks()` - Count scheduled tasks
- [x] Data types: `DbTaskState`, `TaskInfo`, `QueueStatistics`, `TaskNameCount`, `ScheduledTaskInfo`

### Task Updates
- [x] `update_error_message()` - Update error message on a task
- [x] `set_worker_id()` - Set worker ID on a processing task
- [x] `dequeue_with_worker_id()` - Dequeue and set worker ID atomically

### Task Result Storage
- [x] `store_result()` - Store task execution result
- [x] `get_result()` - Retrieve task result
- [x] `delete_result()` - Delete a task result
- [x] `archive_results()` - Archive old results
- [x] Data types: `TaskResult`, `TaskResultStatus`
- [x] MySQL ON DUPLICATE KEY UPDATE for upsert

### Health & Maintenance
- [x] `check_health()` - Database health check with version info
- [x] `archive_completed_tasks()` - Archive old completed/failed/cancelled tasks
- [x] `recover_stuck_tasks()` - Recover tasks stuck in processing state
- [x] `purge_all()` - Purge all tasks (dangerous)
- [x] `purge_by_state()` - Purge tasks by specific state
- [x] `purge_completed()` - Purge completed tasks only
- [x] `purge_failed()` - Purge failed tasks only
- [x] `purge_cancelled()` - Purge cancelled tasks only
- [x] `purge_by_task_name()` - Purge tasks by task name
- [x] Connection pool metrics (size, idle connections)
- [x] Data type: `HealthStatus`

### Database Monitoring
- [x] `get_table_sizes()` - Get CeleRS table size info
- [x] `optimize_tables()` - MySQL OPTIMIZE TABLE for performance
- [x] `analyze_tables()` - MySQL ANALYZE TABLE for query optimization
- [x] Data type: `TableSizeInfo`

### Observability
- [x] Prometheus metrics (optional feature)
- [x] Tasks enqueued counter (total and per-type)
- [x] Queue size gauges (pending, processing, DLQ)
- [x] `update_metrics()` method for gauge updates
- [x] Batch operation metrics tracking

## Configuration

### Connection
- [x] MySQL connection string
- [x] Connection pooling via sqlx
- [x] Configurable queue table name
- [x] Async query execution

## MySQL-Specific Implementation Details

### Data Type Mappings
- UUID -> `CHAR(36)` (text representation)
- BYTEA -> `MEDIUMBLOB` (binary large object)
- TIMESTAMP WITH TIME ZONE -> `TIMESTAMP` (MySQL doesn't have timezone-aware timestamps)
- JSONB -> `JSON` (MySQL native JSON type)

### Query Differences from PostgreSQL
- PostgreSQL `$1, $2` placeholders -> MySQL `?, ?` placeholders
- PostgreSQL `ANY($1)` array parameter -> MySQL `IN (?, ?, ...)` dynamic placeholders
- PostgreSQL `gen_random_uuid()` -> MySQL `UUID()` function
- PostgreSQL `NOW() + INTERVAL '5 seconds'` -> MySQL `DATE_ADD(NOW(), INTERVAL 5 SECOND)`
- PostgreSQL `FILTER (WHERE ...)` -> MySQL `SUM(CASE WHEN ... THEN 1 ELSE 0 END)`
- PostgreSQL `ON CONFLICT DO UPDATE` -> MySQL `ON DUPLICATE KEY UPDATE`

### Stored Procedures
- Uses MySQL stored procedure syntax instead of PostgreSQL PL/pgSQL
- `DELIMITER //` and `DELIMITER ;` for procedure definition
- `CALL move_to_dlq(?)` to invoke

## Recent Enhancements (2025)

### New Features Added
- [x] **README.md** - Comprehensive documentation with usage examples
- [x] **Migration Version Tracking** - Track applied migrations with `celers_migrations` table
- [x] **Connection Pool Configuration** - Custom pool settings via `PoolConfig`
- [x] **Query Performance Tracking** - MySQL performance_schema integration
- [x] **Batch Reject Operation** - Reject multiple tasks efficiently
- [x] **Task Chain Support** - Enqueue dependent task sequences
- [x] **Index Usage Statistics** - Monitor index effectiveness
- [x] **Query Optimization Tools** - EXPLAIN plan analysis utilities
- [x] **Connection Diagnostics** - Pool utilization and connection metrics
- [x] **Performance Metrics** - Comprehensive performance snapshot API
- [x] **Readiness Checks** - `is_ready()` method for health monitoring
- [x] **Server Variables** - Query MySQL configuration settings
- [x] **Backup/Restore Documentation** - Complete disaster recovery guide
- [x] **Integration Tests** - 13 comprehensive integration tests
- [x] **Concurrency Tests** - SKIP LOCKED behavior verification

## Future Enhancements

### Performance
- [ ] Table partitioning for large queues (by created_at)
- [x] Query optimization with EXPLAIN ANALYZE (COMPLETED)
- [ ] Consider BINARY(16) for UUIDs instead of CHAR(36)

### Advanced Features
- [x] Task scheduling/delayed execution (COMPLETED)
- [x] Task dependencies/DAG support (COMPLETED - via TaskChain)
- [x] Task result storage in database (COMPLETED)
- [x] Multi-tenant queue support (COMPLETED - queue_name implemented)
- [x] Queue pause/resume functionality (COMPLETED)
- [x] Worker tracking (COMPLETED)
- [x] Batch operations (COMPLETED - enqueue, dequeue, ack, reject)

### Monitoring
- [x] Prometheus metrics integration (COMPLETED)
- [x] Query performance tracking (COMPLETED)
- [x] Connection pool metrics (COMPLETED)
- [x] Table size monitoring (COMPLETED)
- [x] Index usage statistics (COMPLETED)
- [x] Query plan analysis (COMPLETED - EXPLAIN support)

### Maintenance
- [x] Automatic archiving of old tasks (COMPLETED)
- [x] OPTIMIZE TABLE automation (COMPLETED)
- [x] ANALYZE TABLE for index maintenance (COMPLETED)
- [x] Database health checks (COMPLETED)
- [x] Selective purge operations (COMPLETED)
- [x] Migration tracking system (COMPLETED)

## Testing Status

- [x] Compilation tests
- [x] Unit test structure
- [x] DbTaskState tests (display, from_str, serialization)
- [x] TaskResultStatus tests (display, from_str, serialization)
- [x] QueueStatistics tests
- [x] PoolConfig tests (COMPLETED)
- [x] TaskChain builder tests (COMPLETED)
- [x] Integration tests with real MySQL (COMPLETED - 13 tests)
  - [x] Batch operations test
  - [x] Task chain test
  - [x] Connection diagnostics test
  - [x] Performance metrics test
  - [x] Migration tracking test
  - [x] Readiness check test
- [x] Concurrency tests (FOR UPDATE SKIP LOCKED) (COMPLETED)
  - [x] Concurrent dequeue test
  - [x] SKIP LOCKED behavior test
- [ ] Performance benchmarks vs PostgreSQL
- [x] Migration testing (COMPLETED)

## Documentation

- [x] Module-level documentation
- [x] Migration files with comments
- [x] API documentation
- [x] README.md with comprehensive examples (COMPLETED)
- [x] MySQL tuning guide (COMPLETED - in README.md)
- [x] Index strategy documentation (COMPLETED - in migration files and README.md)
- [x] Scaling recommendations (COMPLETED - in README.md)
- [x] Backup/restore procedures (COMPLETED - comprehensive guide in README.md)
  - [x] Database backup strategies
  - [x] Point-in-time recovery procedures
  - [x] Disaster recovery checklist
  - [x] Automated backup scripts
  - [x] Data migration examples

## Dependencies

- `celers-core`: Core traits and types
- `sqlx`: MySQL async driver (v0.8 with mysql feature)
- `serde_json`: Task serialization
- `tracing`: Logging
- `uuid`: Task ID generation
- `chrono`: Timestamp handling
- `rust_decimal`: Decimal handling for MySQL SUM results

## API Summary

### Core Broker Trait Methods
```rust
enqueue(task) -> TaskId
dequeue() -> Option<BrokerMessage>
ack(task_id, receipt_handle)
reject(task_id, receipt_handle, requeue: bool)
queue_size() -> usize
cancel(task_id) -> bool
enqueue_at(task, timestamp) -> TaskId
enqueue_after(task, delay_secs) -> TaskId
enqueue_batch(tasks) -> Vec<TaskId>
dequeue_batch(count) -> Vec<BrokerMessage>
ack_batch(tasks)
```

### Queue Control
```rust
pause()
resume()
is_paused() -> bool
```

### Task Inspection
```rust
get_task(task_id) -> Option<TaskInfo>
list_tasks(state, limit, offset) -> Vec<TaskInfo>
get_statistics() -> QueueStatistics
count_by_task_name() -> Vec<TaskNameCount>
get_processing_tasks(limit, offset) -> Vec<TaskInfo>
get_tasks_by_worker(worker_id) -> Vec<TaskInfo>
list_scheduled_tasks(limit, offset) -> Vec<ScheduledTaskInfo>
count_scheduled_tasks() -> i64
```

### Task Updates
```rust
update_error_message(task_id, error_message) -> bool
set_worker_id(task_id, worker_id) -> bool
dequeue_with_worker_id(worker_id) -> Option<BrokerMessage>
```

### DLQ Operations
```rust
list_dlq(limit, offset) -> Vec<DlqTaskInfo>
requeue_from_dlq(dlq_id) -> TaskId
purge_dlq(dlq_id) -> bool
purge_all_dlq() -> u64
```

### Result Storage
```rust
store_result(task_id, task_name, status, result, error, traceback, runtime_ms)
get_result(task_id) -> Option<TaskResult>
delete_result(task_id) -> bool
archive_results(older_than: Duration) -> u64
```

### Health & Maintenance
```rust
check_health() -> HealthStatus
archive_completed_tasks(older_than: Duration) -> u64
recover_stuck_tasks(stuck_threshold: Duration) -> u64
purge_all() -> u64
purge_by_state(state) -> u64
purge_completed() -> u64
purge_failed() -> u64
purge_cancelled() -> u64
purge_by_task_name(task_name) -> u64
```

### Database Monitoring
```rust
get_table_sizes() -> Vec<TableSizeInfo>
optimize_tables()
analyze_tables()
```

### NEW: Connection Pool Configuration
```rust
with_config(url, queue_name, config: PoolConfig) -> MysqlBroker
// PoolConfig fields: max_connections, min_connections, acquire_timeout_secs,
//                    max_lifetime_secs, idle_timeout_secs
```

### NEW: Migration Management
```rust
list_migrations() -> Vec<MigrationInfo>
// Migrations are now tracked in celers_migrations table
// migrate() is idempotent and skips already-applied migrations
```

### NEW: Query Performance Tracking
```rust
get_query_stats() -> Vec<QueryStats>
reset_query_stats()
// Requires MySQL performance_schema to be enabled
```

### NEW: Index Usage and Query Optimization
```rust
get_index_stats() -> Vec<IndexStats>
explain_dequeue() -> Vec<QueryPlan>
explain_query(query) -> Vec<QueryPlan>
check_index_usage() -> Vec<String>
// Returns warnings about index usage issues
```

### NEW: Batch Operations
```rust
reject_batch(tasks: &[(TaskId, Option<String>, bool)]) -> u64
// Efficiently reject multiple tasks with retry logic
```

### NEW: Task Chain Support
```rust
enqueue_chain(chain: TaskChain) -> Vec<TaskId>
// TaskChain::new().then(task1).then(task2).with_delay(5)
// Creates sequential task execution with optional delays
```

### NEW: Connection Diagnostics and Performance
```rust
get_connection_diagnostics() -> ConnectionDiagnostics
get_performance_metrics() -> PerformanceMetrics
is_ready() -> bool
get_server_variables() -> HashMap<String, String>
// Monitor connection pool, query performance, and server config
```

## Schema Design

### Tasks Table
- `id`: CHAR(36) - UUID as string
- `task_name`: VARCHAR(255) - Task type identifier
- `payload`: MEDIUMBLOB - Binary task data
- `state`: VARCHAR(20) - Enum (pending/processing/completed/failed/cancelled)
- `priority`: INT - Integer for ordering (higher = more important)
- `retry_count`: INT - Current retry count
- `max_retries`: INT - Maximum allowed retries
- `created_at`: TIMESTAMP - Task creation time
- `scheduled_at`: TIMESTAMP - When task should be processed
- `started_at`: TIMESTAMP - Processing start time
- `completed_at`: TIMESTAMP - Completion time
- `worker_id`: VARCHAR(255) - Worker that processed task
- `error_message`: TEXT - Error details if failed
- `metadata`: JSON - Additional task metadata

### Results Table
- `task_id`: CHAR(36) PRIMARY KEY - Task UUID
- `task_name`: VARCHAR(255) - Task type identifier
- `status`: VARCHAR(20) - Result status (PENDING/STARTED/SUCCESS/FAILURE/RETRY/REVOKED)
- `result`: JSON - Task result data
- `error`: TEXT - Error message if failed
- `traceback`: TEXT - Stack trace if failed
- `created_at`: TIMESTAMP - Result creation time
- `completed_at`: TIMESTAMP - Task completion time
- `runtime_ms`: BIGINT - Task runtime in milliseconds

### Indexes (001_init.sql)
- `idx_tasks_state_priority`: `(state, priority DESC, created_at ASC)` for efficient dequeue
- `idx_tasks_scheduled`: `(scheduled_at, state)` for scheduled tasks
- `idx_tasks_worker`: `(worker_id, state)` for worker tracking
- `idx_dlq_failed_at`: Dead letter queue timestamp index
- `idx_history_task_id`: Task history lookup index

### Indexes (002_results.sql)
- `idx_results_task_name`: Results by task name
- `idx_results_completed_at`: Results cleanup index
- `idx_results_status`: Results by status

### Indexes (003_performance_indexes.sql)
- `idx_tasks_task_name`: Task name lookups
- `idx_tasks_task_name_state`: Task name + state combination
- `idx_tasks_worker_started`: Worker monitoring
- `idx_tasks_created_at`: Time-based queries
- `idx_tasks_completed_at`: Archiving queries
- `idx_dlq_task_id`: DLQ task ID lookups
- `idx_history_timestamp`: History by timestamp

## Notes

- Uses MySQL FOR UPDATE SKIP LOCKED for atomic dequeue (MySQL 8.0+)
- Supports concurrent workers safely
- JSON payload allows flexible task data
- Priority ordering for task selection
- Transaction-based operations for consistency
- Automatic retry handling with exponential backoff
- Compatible with MySQL 8.0+ (requires SKIP LOCKED support)
- Worker ID tracking for distributed worker monitoring

## Comparison with PostgreSQL Broker

### Similarities
- Same FOR UPDATE SKIP LOCKED pattern
- Same table structure and indexes
- Same batch operations API
- Same DLQ mechanism
- Same transaction safety guarantees
- Same task inspection methods
- Same result storage API
- Same queue control (pause/resume)
- Same worker tracking API

### Differences
- MySQL uses `?` placeholders vs PostgreSQL `$1, $2`
- MySQL UUIDs stored as CHAR(36) vs native UUID type
- MySQL stored procedures vs PostgreSQL functions
- MySQL DATE_ADD() vs PostgreSQL INTERVAL syntax
- MySQL doesn't support partial indexes (WHERE clause in CREATE INDEX)
- MySQL uses ON DUPLICATE KEY UPDATE vs ON CONFLICT
- MySQL SUM returns DECIMAL vs integer
- MySQL TIMESTAMPDIFF vs PostgreSQL EXTRACT(EPOCH FROM)
