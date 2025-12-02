# celers-broker-postgres

PostgreSQL-based broker implementation for CeleRS using `FOR UPDATE SKIP LOCKED`.

## Overview

**Status: ✅ FEATURE COMPLETE**

This broker provides production-ready, durable task queue functionality using PostgreSQL as the backend. It's suitable for scenarios where:

- You want transactional consistency with your application database
- Redis is not available or desired
- You need stronger durability guarantees
- You're already using PostgreSQL
- You need advanced features like DLQ, delayed tasks, and result storage

## Features

### Core Queue Operations
- ✅ **Enqueue/Dequeue**: Atomic task queue operations with `FOR UPDATE SKIP LOCKED`
- ✅ **Ack/Reject**: Task completion and retry handling
- ✅ **Priority Queues**: Native priority-based task ordering
- ✅ **Delayed Execution**: Schedule tasks for future execution with `enqueue_at()` and `enqueue_after()`
- ✅ **Batch Operations**: High-performance batch enqueue/dequeue/ack
- ✅ **Queue Control**: Pause/resume queue processing

### Reliability & Error Handling
- ✅ **Dead Letter Queue (DLQ)**: Automatic handling of permanently failed tasks
- ✅ **Stuck Task Recovery**: Recover tasks from crashed workers
- ✅ **Exponential Backoff**: Automatic retry with backoff
- ✅ **Max Retry Limits**: Configurable retry policies

### Observability
- ✅ **Prometheus Metrics**: Optional metrics support (enable with `metrics` feature)
- ✅ **Task Inspection**: Query task state and history
- ✅ **Queue Statistics**: Get comprehensive queue metrics
- ✅ **Health Checks**: Database and connection pool health monitoring
- ✅ **Database Monitoring**: Table sizes, index usage, and performance stats

### Storage & Maintenance
- ✅ **Result Backend**: Store and retrieve task execution results
- ✅ **Task Archiving**: Clean up old completed tasks
- ✅ **DLQ Management**: List, requeue, or purge dead-lettered tasks
- ✅ **Database Migrations**: Automated schema setup

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
celers-broker-postgres = "0.1"

# Enable Prometheus metrics (optional)
celers-broker-postgres = { version = "0.1", features = ["metrics"] }
```

## Quick Start

```rust
use celers_broker_postgres::PostgresBroker;
use celers_core::{Broker, SerializedTask};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create broker
    let broker = PostgresBroker::new("postgres://user:pass@localhost/mydb").await?;

    // Run migrations
    broker.migrate().await?;

    // Enqueue a task
    let task = SerializedTask::new("my_task".to_string(), vec![1, 2, 3]);
    let task_id = broker.enqueue(task).await?;
    println!("Enqueued task: {}", task_id);

    // Dequeue and process
    if let Some(msg) = broker.dequeue().await? {
        println!("Processing: {}", msg.task.metadata.name);

        // Acknowledge completion
        broker.ack(&msg.task.metadata.id, msg.receipt_handle.as_deref()).await?;
    }

    Ok(())
}
```

## Advanced Usage

### Delayed Task Execution

```rust
use std::time::{SystemTime, UNIX_EPOCH};

// Schedule for specific timestamp (Unix seconds)
let execute_at = SystemTime::now()
    .duration_since(UNIX_EPOCH)?
    .as_secs() as i64 + 3600; // 1 hour from now
broker.enqueue_at(task, execute_at).await?;

// Schedule after delay (seconds)
broker.enqueue_after(task, 300).await?; // 5 minutes
```

### Batch Operations

```rust
// Enqueue multiple tasks in a single transaction
let tasks = vec![
    SerializedTask::new("task1".to_string(), vec![1]),
    SerializedTask::new("task2".to_string(), vec![2]),
    SerializedTask::new("task3".to_string(), vec![3]),
];
let task_ids = broker.enqueue_batch(tasks).await?;

// Dequeue multiple tasks atomically
let messages = broker.dequeue_batch(10).await?;

// Acknowledge multiple tasks
let acks: Vec<_> = messages.iter()
    .map(|m| (m.task.metadata.id, m.receipt_handle.clone()))
    .collect();
broker.ack_batch(&acks).await?;
```

### Queue Control

```rust
// Pause queue processing
broker.pause();

// Check if paused
if broker.is_paused() {
    println!("Queue is paused");
}

// Resume processing
broker.resume();
```

### Dead Letter Queue Management

```rust
// List DLQ tasks (with pagination)
let dlq_tasks = broker.list_dlq(10, 0).await?;
for task in dlq_tasks {
    println!("Failed task: {} - {}", task.task_name, task.error_message.unwrap_or_default());
}

// Requeue a task from DLQ
let new_task_id = broker.requeue_from_dlq(&dlq_task.id).await?;

// Purge a single DLQ task
broker.purge_dlq(&dlq_task.id).await?;

// Purge all DLQ tasks (use with caution!)
let purged_count = broker.purge_all_dlq().await?;
```

### Task Inspection & Statistics

```rust
use celers_broker_postgres::DbTaskState;

// Get specific task info
if let Some(task_info) = broker.get_task(&task_id).await? {
    println!("Task state: {:?}", task_info.state);
    println!("Retries: {}/{}", task_info.retry_count, task_info.max_retries);
}

// List tasks by state
let pending_tasks = broker.list_tasks(Some(DbTaskState::Pending), 100, 0).await?;

// Get queue statistics
let stats = broker.get_statistics().await?;
println!("Pending: {}, Processing: {}, DLQ: {}",
    stats.pending, stats.processing, stats.dlq);
```

### Result Backend

```rust
use celers_broker_postgres::TaskResultStatus;
use serde_json::json;

// Store task result
broker.store_result(
    &task_id,
    "my_task",
    TaskResultStatus::Success,
    Some(json!({"value": 42})),
    None,
    None,
    Some(1234), // runtime in ms
).await?;

// Retrieve result
if let Some(result) = broker.get_result(&task_id).await? {
    println!("Status: {:?}", result.status);
    println!("Result: {:?}", result.result);
}

// Delete result
broker.delete_result(&task_id).await?;

// Archive old results (older than 7 days)
let archived = broker.archive_results(std::time::Duration::from_secs(7 * 24 * 3600)).await?;
```

### Health Checks & Maintenance

```rust
// Check database health
let health = broker.check_health().await?;
println!("Database: {} (pool: {}/{})",
    health.database_version,
    health.connection_pool_size - health.idle_connections,
    health.connection_pool_size);

// Recover stuck tasks (stuck for > 1 hour)
let recovered = broker.recover_stuck_tasks(std::time::Duration::from_secs(3600)).await?;

// Archive old completed tasks (older than 30 days)
let archived = broker.archive_completed_tasks(std::time::Duration::from_secs(30 * 24 * 3600)).await?;

// Update PostgreSQL statistics for query planner
broker.analyze_tables().await?;
```

### Database Monitoring

```rust
// Get table size information
let table_sizes = broker.get_table_sizes().await?;
for table in table_sizes {
    println!("{}: {} rows, {}",
        table.table_name, table.row_count, table.total_size_pretty);
}

// Get index usage statistics
let index_usage = broker.get_index_usage().await?;
for idx in index_usage {
    println!("{}: {} scans", idx.index_name, idx.index_scans);
}

// Find unused indexes
let unused = broker.get_unused_indexes().await?;
if !unused.is_empty() {
    println!("Warning: {} unused indexes found", unused.len());
}
```

### Prometheus Metrics

Enable the `metrics` feature:

```toml
[dependencies]
celers-broker-postgres = { version = "0.1", features = ["metrics"] }
```

Update metrics periodically:

```rust
// In a background task
loop {
    broker.update_metrics().await?;
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
}
```

Metrics exported:
- `celers_tasks_enqueued_total` - Total tasks enqueued
- `celers_tasks_enqueued_by_type` - Tasks enqueued per task type
- `celers_queue_size` - Current pending queue size
- `celers_processing_queue_size` - Tasks currently processing
- `celers_dlq_size` - Dead letter queue size

## Database Schema

The broker creates the following tables:

### `celers_tasks`
Main task queue with columns for task state, priority, retries, scheduling, and metadata.

### `celers_dead_letter_queue`
Storage for permanently failed tasks.

### `celers_task_results`
Result backend for task execution outcomes.

### `celers_task_history`
Optional audit log of task state changes.

See `migrations/001_init.sql` and `migrations/002_results.sql` for full schema details.

## Performance Characteristics

### Throughput
- Single task operations: 100-500 tasks/sec
- Batch operations: 1,000-5,000 tasks/sec
- Actual performance depends on PostgreSQL configuration and hardware

### Latency
- Enqueue: 2-10ms
- Dequeue: 5-20ms
- Batch operations: Lower per-task latency

### Concurrency
- `FOR UPDATE SKIP LOCKED` ensures no lock contention between workers
- Scales horizontally with multiple workers
- Limited by PostgreSQL connection pool size

## PostgreSQL Configuration

Recommended settings for high-throughput workloads:

```sql
-- Connection pooling
max_connections = 100

-- Query performance
shared_buffers = 256MB
effective_cache_size = 1GB
work_mem = 16MB

-- Autovacuum (important for queue tables!)
autovacuum = on
autovacuum_naptime = 60s

-- Logging
log_min_duration_statement = 1000  -- Log slow queries
```

For queue tables specifically:

```sql
-- More aggressive autovacuum for high-churn tables
ALTER TABLE celers_tasks SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_analyze_scale_factor = 0.005
);
```

## Trade-offs

### Advantages
- ✅ Strong consistency guarantees
- ✅ Integrated with application database
- ✅ No additional infrastructure required
- ✅ Can participate in distributed transactions
- ✅ Rich querying capabilities
- ✅ Durable across restarts

### Disadvantages
- ❌ Lower throughput than Redis (~1K vs ~10K tasks/sec)
- ❌ Higher latency per operation
- ❌ Puts load on your main database
- ❌ Requires periodic VACUUM maintenance

## Migrations

Run migrations on application startup:

```rust
let broker = PostgresBroker::new(database_url).await?;
broker.migrate().await?;
```

Migrations are idempotent and safe to run multiple times.

## Backup & Restore Procedures

### Database Backup

Since CeleRS uses PostgreSQL tables, you can use standard PostgreSQL backup tools:

#### Full Database Backup
```bash
# Using pg_dump
pg_dump -h localhost -U postgres -d mydb -F c -f celers_backup.dump

# Or using pg_basebackup for physical backup
pg_basebackup -h localhost -U postgres -D /backup/postgres -Ft -z -P
```

#### CeleRS Tables Only
```bash
# Backup only CeleRS tables
pg_dump -h localhost -U postgres -d mydb \
  -t celers_tasks \
  -t celers_dead_letter_queue \
  -t celers_task_results \
  -t celers_task_history \
  -F c -f celers_tables_backup.dump
```

#### Continuous Archiving (Point-in-Time Recovery)
Enable WAL archiving in `postgresql.conf`:
```sql
wal_level = replica
archive_mode = on
archive_command = 'cp %p /archive/%f'
```

### Restore Procedures

#### Full Restore
```bash
# Restore from custom format dump
pg_restore -h localhost -U postgres -d mydb -c celers_backup.dump

# Or from plain SQL dump
psql -h localhost -U postgres -d mydb < celers_backup.sql
```

#### Table-Level Restore
```bash
# Restore specific tables
pg_restore -h localhost -U postgres -d mydb \
  -t celers_tasks \
  -t celers_dead_letter_queue \
  celers_tables_backup.dump
```

#### Point-in-Time Recovery
```bash
# 1. Stop PostgreSQL
systemctl stop postgresql

# 2. Restore base backup
rm -rf /var/lib/postgresql/data/*
tar -xf base_backup.tar -C /var/lib/postgresql/data/

# 3. Create recovery.conf
cat > /var/lib/postgresql/data/recovery.conf <<EOF
restore_command = 'cp /archive/%f %p'
recovery_target_time = '2024-01-15 12:00:00'
EOF

# 4. Start PostgreSQL
systemctl start postgresql
```

### Data Export/Import

#### Export Tasks to JSON
```sql
-- Export pending tasks to JSON file
COPY (
  SELECT json_agg(row_to_json(t))
  FROM (
    SELECT id, task_name, encode(payload, 'base64') as payload_b64,
           state, priority, created_at, scheduled_at
    FROM celers_tasks
    WHERE state = 'pending'
  ) t
) TO '/tmp/pending_tasks.json';
```

#### Application-Level Backup
```rust
use celers_broker_postgres::{PostgresBroker, DbTaskState};

async fn backup_tasks(broker: &PostgresBroker) -> Result<(), Box<dyn std::error::Error>> {
    // Get all tasks
    let tasks = broker.list_tasks(None, 10000, 0).await?;

    // Serialize to JSON
    let json = serde_json::to_string_pretty(&tasks)?;
    std::fs::write("tasks_backup.json", json)?;

    // Also backup DLQ
    let dlq = broker.list_dlq(10000, 0).await?;
    let dlq_json = serde_json::to_string_pretty(&dlq)?;
    std::fs::write("dlq_backup.json", dlq_json)?;

    Ok(())
}
```

### Disaster Recovery Best Practices

1. **Regular Automated Backups**
   - Schedule daily full backups
   - Keep backups for 30 days minimum
   - Test restore procedures monthly

2. **Replication for High Availability**
   ```sql
   -- On primary server
   CREATE USER replicator WITH REPLICATION ENCRYPTED PASSWORD 'secret';

   -- On standby server
   pg_basebackup -h primary -D /var/lib/postgresql/data -U replicator -P --wal-method=stream
   ```

3. **Monitor Backup Success**
   ```bash
   # Verify backup file exists and is recent
   find /backup -name "*.dump" -mtime -1 -ls

   # Test backup integrity
   pg_restore --list celers_backup.dump > /dev/null
   ```

4. **Archive Old Data Before Backup**
   ```rust
   // Archive completed tasks older than 30 days before backup
   let thirty_days = std::time::Duration::from_secs(30 * 24 * 3600);
   broker.archive_completed_tasks(thirty_days).await?;
   broker.archive_results(thirty_days).await?;
   ```

## Testing

Unit tests:
```bash
cargo test
```

Integration tests (requires PostgreSQL):
```bash
export DATABASE_URL="postgres://postgres:postgres@localhost/celers_test"
cargo test --all-features -- --ignored
```

## License

See workspace LICENSE file.

## See Also

- `celers-core`: Core traits and types
- `celers-broker-redis`: Alternative Redis-based broker (higher throughput)
- `celers-metrics`: Prometheus metrics support
