# celers-broker-postgres TODO

> PostgreSQL-based broker implementation for CeleRS

## Status: ✅ FEATURE COMPLETE

PostgreSQL broker with FOR UPDATE SKIP LOCKED pattern, migrations, DLQ support, and high-performance batch operations.

## Completed Features

### Core Operations ✅
- [x] `enqueue()` - Insert tasks into database
- [x] `dequeue()` - Fetch with FOR UPDATE SKIP LOCKED
- [x] `ack()` - Update task state to completed
- [x] `reject()` - Handle failed tasks
- [x] `queue_size()` - Count pending tasks
- [x] Transaction support for atomicity

### Database Schema ✅
- [x] `tasks` table with all required columns
- [x] `dlq` (dead letter queue) table
- [x] Indexes for performance
- [x] State enum (pending, processing, completed, failed)
- [x] Priority column for task ordering

### Dead Letter Queue ✅
- [x] Automatic DLQ on max retries
- [x] DLQ table structure
- [x] Failed task archiving
- [x] DLQ inspection queries

### Migrations ✅
- [x] Initial schema migration (001_initial.sql)
- [x] DLQ table migration (002_dlq.sql)
- [x] Migration documentation

## Configuration

### Connection ✅
- [x] PostgreSQL connection string
- [x] Connection pooling via sqlx
- [x] Configurable queue table name
- [x] Async query execution

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
- [x] Supports both immediate and delayed execution

### Observability ✅
- [x] Prometheus metrics (optional feature)
- [x] Tasks enqueued counter (total and per-type)
- [x] Queue size gauges (pending, processing, DLQ)
- [x] `update_metrics()` method for gauge updates
- [x] Batch operation metrics tracking

## Future Enhancements

### Performance
- [x] Batch enqueue operations ✅ (COMPLETED)
- [x] Batch dequeue operations ✅ (COMPLETED)
- [ ] Additional indexes for common queries
- [ ] Table partitioning for large queues
- [ ] Query optimization for high throughput

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
- [ ] VACUUM automation
- [ ] Index maintenance tools
- [ ] Database health checks

## Testing Status

- [x] Compilation tests
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
