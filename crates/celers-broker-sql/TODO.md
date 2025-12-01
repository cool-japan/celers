# celers-broker-sql TODO

> MySQL database broker implementation for CeleRS

## Status: ✅ FEATURE COMPLETE

MySQL broker with FOR UPDATE SKIP LOCKED pattern, migrations, DLQ support, and high-performance batch operations.

## Completed Features

### Core Operations ✅
- [x] `enqueue()` - Insert tasks into MySQL database
- [x] `dequeue()` - Fetch with FOR UPDATE SKIP LOCKED
- [x] `ack()` - Update task state to completed
- [x] `reject()` - Handle failed tasks with retry logic
- [x] `queue_size()` - Count pending tasks
- [x] `cancel()` - Cancel pending/processing tasks
- [x] Transaction support for atomicity

### Database Schema ✅
- [x] `celers_tasks` table with all required columns
- [x] `celers_dead_letter_queue` (DLQ) table
- [x] `celers_task_history` table for auditing
- [x] Indexes for performance
- [x] State enum (pending, processing, completed, failed, cancelled)
- [x] Priority column for task ordering

### Dead Letter Queue ✅
- [x] Automatic DLQ on max retries
- [x] DLQ table structure
- [x] Failed task archiving via stored procedure
- [x] DLQ inspection queries

### Migrations ✅
- [x] Initial schema migration (001_init.sql)
- [x] MySQL-specific data types (CHAR(36) for UUID, MEDIUMBLOB, JSON)
- [x] Stored procedure for DLQ operations
- [x] Migration documentation

### Batch Operations ✅
- [x] Batch enqueue (multiple tasks in single transaction)
- [x] Batch dequeue (fetch multiple tasks atomically)
- [x] Optimized for high-throughput scenarios
- [x] Maintains FOR UPDATE SKIP LOCKED safety

### Delayed Task Execution ✅
- [x] `enqueue_at(task, timestamp)` - Schedule for specific Unix timestamp
- [x] `enqueue_after(task, delay_secs)` - Schedule after delay in seconds
- [x] Uses existing `scheduled_at` column with index
- [x] Automatic processing when tasks are ready (in dequeue)
- [x] MySQL DATE_ADD() for relative delays

### Observability ✅
- [x] Prometheus metrics (optional feature)
- [x] Tasks enqueued counter (total and per-type)
- [x] Queue size gauges (pending, processing, DLQ)
- [x] `update_metrics()` method for gauge updates
- [x] Batch operation metrics tracking

## Configuration

### Connection ✅
- [x] MySQL connection string
- [x] Connection pooling via sqlx
- [x] Configurable queue table name
- [x] Async query execution

## MySQL-Specific Implementation Details

### Data Type Mappings
- UUID → `CHAR(36)` (text representation)
- BYTEA → `MEDIUMBLOB` (binary large object)
- TIMESTAMP WITH TIME ZONE → `TIMESTAMP` (MySQL doesn't have timezone-aware timestamps)
- JSONB → `JSON` (MySQL native JSON type)

### Query Differences from PostgreSQL
- PostgreSQL `$1, $2` placeholders → MySQL `?, ?` placeholders
- PostgreSQL `ANY($1)` array parameter → MySQL `IN (?, ?, ...)` dynamic placeholders
- PostgreSQL `gen_random_uuid()` → MySQL `UUID()` function
- PostgreSQL `NOW() + INTERVAL '5 seconds'` → MySQL `DATE_ADD(NOW(), INTERVAL 5 SECOND)`

### Stored Procedures
- Uses MySQL stored procedure syntax instead of PostgreSQL PL/pgSQL
- `DELIMITER //` and `DELIMITER ;` for procedure definition
- `CALL move_to_dlq(?)` to invoke

## Future Enhancements

### Performance
- [ ] Additional indexes for common query patterns
- [ ] Table partitioning for large queues (by created_at)
- [ ] Query optimization with EXPLAIN ANALYZE
- [ ] Consider BINARY(16) for UUIDs instead of CHAR(36)

### Advanced Features
- [x] Task scheduling/delayed execution ✅ (COMPLETED)
- [ ] Task dependencies/DAG support
- [ ] Task result storage in database
- [ ] Multi-tenant queue support
- [ ] Queue pause/resume functionality

### Monitoring
- [x] Prometheus metrics integration ✅ (COMPLETED)
- [ ] Query performance tracking
- [ ] Connection pool metrics
- [ ] Table size monitoring
- [ ] Index usage statistics

### Maintenance
- [ ] Automatic archiving of old tasks
- [ ] OPTIMIZE TABLE automation
- [ ] Index maintenance tools
- [ ] Database health checks

## Testing Status

- [x] Compilation tests
- [x] Unit test structure
- [ ] Integration tests with real MySQL
- [ ] Concurrency tests (FOR UPDATE SKIP LOCKED)
- [ ] Performance benchmarks vs PostgreSQL
- [ ] Migration testing

## Documentation

- [x] Module-level documentation
- [x] Migration files with comments
- [x] API documentation
- [ ] MySQL tuning guide
- [ ] Index strategy documentation
- [ ] Scaling recommendations
- [ ] Backup/restore procedures

## Dependencies

- `celers-core`: Core traits and types
- `sqlx`: MySQL async driver (v0.8 with mysql feature)
- `serde_json`: Task serialization
- `tracing`: Logging
- `uuid`: Task ID generation
- `chrono`: Timestamp handling

## MySQL Configuration

Recommended settings for production:

```ini
[mysqld]
# Connection pooling
max_connections = 200

# Query cache (MySQL 5.7 and earlier)
query_cache_type = 1
query_cache_size = 64M

# InnoDB settings
innodb_buffer_pool_size = 1G
innodb_log_file_size = 256M
innodb_flush_log_at_trx_commit = 1

# Binary logging (for replication)
log_bin = mysql-bin
binlog_format = ROW

# Character set
character_set_server = utf8mb4
collation_server = utf8mb4_unicode_ci

# Performance
table_open_cache = 2000
tmp_table_size = 64M
max_heap_table_size = 64M
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

### Indexes
- `idx_tasks_state_priority`: `(state, priority DESC, created_at ASC)` for efficient dequeue
- `idx_tasks_scheduled`: `(scheduled_at, state)` for scheduled tasks
- `idx_tasks_worker`: `(worker_id, state)` for worker tracking
- `idx_dlq_failed_at`: Dead letter queue timestamp index
- `idx_history_task_id`: Task history lookup index

## Notes

- Uses MySQL FOR UPDATE SKIP LOCKED for atomic dequeue (MySQL 8.0+)
- Supports concurrent workers safely
- JSON payload allows flexible task data
- Priority ordering for task selection
- Transaction-based operations for consistency
- Automatic retry handling with exponential backoff
- Compatible with MySQL 8.0+ (requires SKIP LOCKED support)

## Comparison with PostgreSQL Broker

### Similarities
- Same FOR UPDATE SKIP LOCKED pattern
- Same table structure and indexes
- Same batch operations API
- Same DLQ mechanism
- Same transaction safety guarantees

### Differences
- MySQL uses `?` placeholders vs PostgreSQL `$1, $2`
- MySQL UUIDs stored as CHAR(36) vs native UUID type
- MySQL stored procedures vs PostgreSQL functions
- MySQL DATE_ADD() vs PostgreSQL INTERVAL syntax
- MySQL doesn't support partial indexes (WHERE clause in CREATE INDEX)
