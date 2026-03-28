# celers-backend-db TODO

> Database (PostgreSQL/MySQL) result backend for CeleRS

**Version: 0.2.0 | Status: [Alpha] | Updated: 2026-03-27 | Tests: 7**

## Status: ✅ FEATURE COMPLETE + v0.2.0 ENHANCED

Full database result backend implementation with PostgreSQL and MySQL support for durable task result storage and chord synchronization. v0.2.0 adds distributed lock support.

## Completed Features

### Core Operations ✅
- [x] `store_result()` - Store task results with upsert (INSERT ... ON CONFLICT)
- [x] `get_result()` - Retrieve task results
- [x] `delete_result()` - Delete task results
- [x] `set_expiration()` - Set TTL for results
- [x] Connection pooling (20 connections)
- [x] Async query execution

### Batch Operations ✅
- [x] `store_results_batch()` - Batch store with transactions
- [x] `get_results_batch()` - Batch retrieve with single query
- [x] `delete_results_batch()` - Batch delete with single query
- [x] PostgreSQL: Array parameter queries (= ANY($1))
- [x] MySQL: Dynamic IN clause generation

### Chord Synchronization ✅
- [x] `chord_init()` - Initialize chord state with task list
- [x] `chord_complete_task()` - Atomically increment completion counter
- [x] `chord_get_state()` - Retrieve chord state
- [x] PostgreSQL: Function-based atomic counter
- [x] MySQL: UPDATE + SELECT based counter

### Database Schema ✅
- [x] `celers_task_results` table
- [x] `celers_chord_state` table
- [x] Indexes for performance
- [x] Expiration tracking (expires_at column)
- [x] State enum constraint (pending/started/success/failure/revoked/retry)

### Migrations ✅
- [x] PostgreSQL migration (001_init_postgres.sql)
- [x] MySQL migration (001_init_mysql.sql)
- [x] Functions/stored procedures for cleanup and counters
- [x] Migration execution in code

### Database Support ✅
- [x] PostgreSQL 12+ (native UUID, JSONB, functions)
- [x] MySQL 5.7+ / 8.0+ (CHAR(36) UUID, JSON, procedures)
- [x] Feature flags for postgres/mysql

## PostgreSQL vs MySQL Implementation Details

### Data Types
|Feature|PostgreSQL|MySQL|
|-------|----------|-----|
|UUID|UUID (native)|CHAR(36)|
|JSON|JSONB|JSON|
|Timestamps|TIMESTAMP WITH TIME ZONE|TIMESTAMP|
|Auto-increment|SERIAL|AUTO_INCREMENT|

### Atomic Counter
- **PostgreSQL:** Uses PL/pgSQL function with RETURNING clause
  ```sql
  CREATE FUNCTION chord_increment_counter(UUID) RETURNS INTEGER
  UPDATE ... RETURNING completed INTO new_count
  ```
- **MySQL:** Uses UPDATE + SELECT (two queries, not fully atomic)
  ```sql
  UPDATE ... SET completed = completed + 1
  SELECT completed FROM ...
  ```

### Upsert Syntax
- **PostgreSQL:** `ON CONFLICT (task_id) DO UPDATE SET ...`
- **MySQL:** `ON DUPLICATE KEY UPDATE ...`

## Schema Design

### celers_task_results Table
```sql
task_id          UUID/CHAR(36) PRIMARY KEY
task_name        VARCHAR(255)
result_state     VARCHAR(20)   -- pending/started/success/failure/revoked/retry
result_data      JSONB/JSON    -- Success result value
error_message    TEXT          -- Failure error message
retry_count      INTEGER       -- Retry attempt number
created_at       TIMESTAMP
started_at       TIMESTAMP NULL
completed_at     TIMESTAMP NULL
worker           VARCHAR(255)  -- Worker hostname/ID
expires_at       TIMESTAMP NULL -- Expiration time
```

### celers_chord_state Table
```sql
chord_id        UUID/CHAR(36) PRIMARY KEY
total           INTEGER       -- Total tasks in chord
completed       INTEGER       -- Completed tasks count
callback        TEXT          -- Callback task name
task_ids        JSONB/JSON    -- Array of task UUIDs
created_at      TIMESTAMP
```

### Indexes
- `idx_task_results_expires` - Find expired results
- `idx_task_results_state` - Filter by state
- `idx_task_results_created` - Order by creation time (analytics)
- `idx_chord_completed` - Check chord completion

## Result Expiration

### Automatic Cleanup Function (PostgreSQL)
```sql
SELECT cleanup_expired_results();  -- Returns number of deleted rows
```

Can be scheduled with pg_cron:
```sql
SELECT cron.schedule('cleanup-results', '0 * * * *',
  $$SELECT cleanup_expired_results()$$);
```

### MySQL Cleanup (Stored Procedure)
```sql
CALL cleanup_expired_results(@deleted);
SELECT @deleted;  -- Number of deleted rows
```

Can be scheduled with MySQL Event Scheduler:
```sql
CREATE EVENT cleanup_expired
ON SCHEDULE EVERY 1 HOUR
DO CALL cleanup_expired_results(@deleted);
```

## Usage Examples

### PostgreSQL Backend
```rust
use celers_backend_db::PostgresResultBackend;
use celers_backend_redis::{ResultBackend, TaskMeta, TaskResult};

let mut backend = PostgresResultBackend::new(
    "postgres://user:pass@localhost/celers"
).await?;

backend.migrate().await?;

// Store result
let mut meta = TaskMeta::new(task_id, "my_task".to_string());
meta.result = TaskResult::Success(json!({"value": 42}));
backend.store_result(task_id, &meta).await?;

// Get result
let result = backend.get_result(task_id).await?;

// Set expiration (1 hour)
backend.set_expiration(task_id, Duration::from_secs(3600)).await?;

// Cleanup expired results
let deleted = backend.cleanup_expired().await?;
println!("Deleted {} expired results", deleted);
```

### MySQL Backend
```rust
use celers_backend_db::MysqlResultBackend;

let mut backend = MysqlResultBackend::new(
    "mysql://root:password@localhost/celers"
).await?;

backend.migrate().await?;

// Same API as PostgreSQL backend
backend.store_result(task_id, &meta).await?;
```

### Chord Synchronization
```rust
use celers_backend_redis::ChordState;

// Initialize chord
let chord_state = ChordState {
    chord_id: chord_id,
    total: 10,
    completed: 0,
    callback: Some("finalize_task".to_string()),
    task_ids: vec![task1_id, task2_id, ...],
};
backend.chord_init(chord_state).await?;

// When each task completes
let completed = backend.chord_complete_task(chord_id).await?;
if completed == 10 {
    // All tasks done, trigger callback
}

// Check chord state
let state = backend.chord_get_state(chord_id).await?;
```

### v0.2.0: Distributed Locks ✅
- [x] DbLockBackend for PostgreSQL table-based distributed locks
- [x] Lock acquire/release/renew operations
- [x] Integration with BeatScheduler for leader election

### Phase 9: Event Persistence ✅ COMPLETE
- [x] DbEventPersister with batch insert buffer
- [x] SQL migration for celers_events table with indexes
- [x] EventPersister trait implementation (query, count, cleanup)

## Future Enhancements

### Performance
- [ ] Prepared statement caching
- [ ] Read replica support
- [ ] Result data compression
- [ ] Partitioning for large result tables

### Advanced Features
- [ ] Result versioning (store history)
- [ ] Result aggregation queries
- [ ] Full-text search on result data (PostgreSQL)
- [ ] Result retention policies per task type
- [ ] Multi-database sharding

### Monitoring
- [ ] Query performance metrics
- [ ] Result storage size tracking
- [ ] Chord completion rate metrics
- [ ] Expiration efficiency metrics

### Analytics
- [ ] Task success/failure rate queries
- [ ] Average task duration calculations
- [ ] Worker performance analytics
- [ ] Result data statistics

## Testing Status

- [x] Compilation tests
- [x] Unit tests (7 passing: backend creation, ResultStore conversions, event persister)
- [ ] Integration tests with PostgreSQL
- [ ] Integration tests with MySQL
- [ ] Chord synchronization tests
- [ ] Expiration tests
- [ ] Concurrency tests

## Documentation

- [x] Module-level documentation
- [x] API documentation
- [x] Migration files with comments
- [x] Usage examples
- [ ] Performance tuning guide
- [ ] Scaling recommendations
- [ ] Backup/restore procedures

## Dependencies

- `celers-backend-redis`: Trait definitions and types
- `sqlx`: PostgreSQL/MySQL async driver
- `serde_json`: Result serialization
- `chrono`: Timestamp handling
- `uuid`: Task ID type

## Database Configuration

### PostgreSQL Recommendations
```ini
# postgresql.conf
max_connections = 100
shared_buffers = 256MB
effective_cache_size = 1GB
work_mem = 16MB
maintenance_work_mem = 64MB

# For JSONB performance
shared_preload_libraries = 'pg_stat_statements'

# Autovacuum for cleanup
autovacuum = on
autovacuum_naptime = 60s
```

### MySQL Recommendations
```ini
# my.cnf
[mysqld]
max_connections = 200
innodb_buffer_pool_size = 1G
innodb_log_file_size = 256M

# JSON column performance
optimizer_switch = 'index_merge=on'

# Character set
character_set_server = utf8mb4
collation_server = utf8mb4_unicode_ci
```

## Comparison with Redis Backend

| Feature | PostgreSQL/MySQL | Redis |
|---------|------------------|-------|
| Durability | ✅ High | ⚠️ Optional |
| Persistence | ✅ Disk | ⚠️ Memory + AOF |
| Query Complexity | ✅ SQL | ❌ Limited |
| Atomic Counters | ✅ Native | ✅ Native |
| Expiration | ✅ Manual cleanup | ✅ Automatic (TTL) |
| Scalability | ⚠️ Vertical | ✅ Horizontal |
| Cost | ⚠️ Storage | ⚠️ Memory |
| Analytics | ✅ Full SQL | ❌ Limited |

## Use Cases

### When to Use Database Backend
- ✅ Need durable result storage
- ✅ Want SQL-based analytics
- ✅ Long-term result retention
- ✅ Audit trail requirements
- ✅ Compliance/regulatory needs
- ✅ Already have PostgreSQL/MySQL infrastructure

### When to Use Redis Backend
- ✅ Need fastest possible lookups
- ✅ Results are transient
- ✅ Memory is abundant
- ✅ Don't need complex queries
- ✅ Want automatic expiration

### Hybrid Approach
Use both backends:
- **Redis**: Fast recent results (with TTL)
- **Database**: Long-term storage and analytics
- **Pattern**: Write to both, read from Redis first, fallback to database

## Notes

- Database backends provide durable result storage
- SQL enables powerful analytics and reporting
- Automatic cleanup requires scheduler (pg_cron, MySQL Events)
- Chord counter in MySQL uses two queries (not fully atomic)
- Consider result data size (large results may impact performance)
- Use indexes wisely (balance read vs write performance)
- Monitor table growth and implement archiving strategy
