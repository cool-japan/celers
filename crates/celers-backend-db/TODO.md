# celers-backend-db TODO

> Database (PostgreSQL/MySQL) result backend for CeleRS

**Version: 0.3.0 | Status: [Alpha] | Updated: 2026-07-13 | Tests: 55**

## Status: ✅ FEATURE COMPLETE + v0.2.0 ENHANCED + v0.3.0 PURE-RUST MIGRATION + ANALYTICS

Full database result backend implementation with PostgreSQL and MySQL support for durable task result storage and chord synchronization. v0.2.0 adds distributed lock support. v0.3.0 migrates the
SQL client from `sqlx` to OxiSQL (`oxisql-postgres`/`oxisql-mysql`, Pure Rust), fixes a TLS-downgrade
security bug found during that migration, and adds a database analytics module.

## Completed Features

### Core Operations ✅
- [x] `store_result()` - Store task results with upsert (INSERT ... ON CONFLICT)
- [x] `get_result()` - Retrieve task results
- [x] `delete_result()` - Delete task results
- [x] `set_expiration()` - Set TTL for results
- [x] Async query execution over a single multiplexed connection (`connection()` accessor; see
      "Real N-connection pooling" under Future Enhancements — this is not a real N-connection pool)

### Batch Operations ✅
- [x] `store_results_batch()` - Batch store with transactions
- [x] `get_results_batch()` - Batch retrieve with single query
- [x] `delete_results_batch()` - Batch delete with single query
- [x] PostgreSQL: Dynamic `IN ($1, $2, ...)` clause with one placeholder per element (changed from
      `= ANY($1)` in the v0.3.0 migration — `oxisql-core` has no array-parameter bridge for `Vec<Uuid>`)
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

### v0.3.0: Pure-Rust Migration ✅
- [x] Replaced `sqlx` with `oxisql-postgres`/`oxisql-mysql`/`oxisql-core` — zero `sqlx` dependency
      remains in this crate
- [x] `pool()` getters renamed to `connection()` (reflects that each now wraps a single multiplexed
      connection, not a real connection pool — see Known Limitations below): `PostgresResultBackend`,
      `MysqlResultBackend` (both in `lib.rs`), `DbEventPersister` (`event_persistence.rs`),
      `DbLockBackend` (`lock.rs`)
- [x] `uuid_param`/`json_param`/`uuid_from_row`/`json_from_row` bridging helpers (internal, `row_ext.rs`)
      since `oxisql-core` has no built-in `ToSqlValue`/`FromValue` impl for `uuid::Uuid`/`serde_json::Value`
- [x] `DateTime<Utc>` binding fixed per backend: PostgreSQL binds the RFC3339 string via an explicit
      `$n::text::timestamptz` SQL-side cast; MySQL formats as `%Y-%m-%d %H:%M:%S%.6f` (RFC3339 is
      rejected by MySQL's `DATETIME` grammar)
- [x] **Security fix**: connections no longer hardcode `TlsMode::Disabled` — `tls_mode.rs` parses
      `sslmode` (PostgreSQL) / `ssl-mode`+`tls` (MySQL) from the connection URL and resolves the
      matching `TlsMode`, so `sslmode=require` (etc.) is actually honored

### v0.3.0: Database Analytics ✅ NEW
- [x] `PostgresAnalytics` / `MysqlAnalytics` (`analytics.rs`), returned via `.analytics()` on either
      backend, sharing the same five async methods:
  - [x] `task_stats()` → `TaskStats` (total/success/failure/retry/pending counts + success/failure rate)
  - [x] `percentile_latencies()` → `PercentileLatencies` (mean/p50/p95/p99/min/max task duration)
  - [x] `worker_stats()` → `Vec<WorkerStat>` (per-worker task counts + `tasks_per_hour`)
  - [x] `storage_stats()` → `StorageStats` (row counts by state, estimated result bytes, active/completed chords)
  - [x] `chord_completion_rate()` → `f64`

## Future Enhancements

### Performance
- [ ] Real N-connection pooling — `PostgresResultBackend`/`MysqlResultBackend` currently wrap a single
      multiplexed `oxisql_postgres::PgConnection`/`oxisql_mysql::MyConnection` (renamed `pool()` →
      `connection()` in v0.3.0 to reflect this), not a real connection pool; functional but
      lower-concurrency, tracked as a v0.3.0 Pure-Rust migration follow-up
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
- [x] Result storage size tracking (`StorageStats` via `PostgresAnalytics`/`MysqlAnalytics`)
- [x] Chord completion rate metrics (`chord_completion_rate()`)
- [ ] Expiration efficiency metrics

### Analytics
- [x] Task success/failure rate queries (`task_stats()` — `TaskStats.success_rate`/`failure_rate`)
- [x] Average task duration calculations (`percentile_latencies()` — mean/p50/p95/p99)
- [x] Worker performance analytics (`worker_stats()` — `WorkerStat` with `tasks_per_hour`)
- [x] Result data statistics (`storage_stats()` — `StorageStats.estimated_result_bytes`)

## Testing Status

- [x] Compilation tests
- [x] Unit tests (55 passing via `cargo nextest run --all-features`, 6 skipped/`#[ignore]`d):
  - 25 in `tls_mode.rs` (URL TLS-mode resolution — PostgreSQL `sslmode`, MySQL `ssl-mode`/`tls`)
  - 13 in `row_ext.rs` (UUID/JSON param + row-extraction bridging helpers)
  - 10 in `analytics.rs` (+ 2 `#[ignore]`d, require a live PostgreSQL instance)
  - 3 in `result_store.rs` (`ResultStore` trait conversions)
  - 2 in `event_persistence.rs`
  - 2 in `lock.rs` (+ 2 `#[ignore]`d, require a live PostgreSQL instance)
  - 0 in `lib.rs` (both its tests — Postgres/MySQL backend creation — are `#[ignore]`d)
- [x] Doc tests (1 passing; 5 more intentionally `ignore`d as illustrative-only, require a live DB)
- [ ] Integration tests with PostgreSQL (unit-level coverage only; all live-DB tests are `#[ignore]`d)
- [ ] Integration tests with MySQL (unit-level coverage only; all live-DB tests are `#[ignore]`d)
- [ ] Chord synchronization tests (against a live database)
- [ ] Expiration tests (against a live database)
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
- `oxisql-postgres` / `oxisql-mysql` / `oxisql-core`: Pure-Rust PostgreSQL/MySQL async driver (replaced `sqlx` in v0.3.0)
- `oxitls` / `rustls`: TLS support for database connections
- `anyhow` / `url`: TLS-mode URL parsing (`tls_mode.rs`)
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
