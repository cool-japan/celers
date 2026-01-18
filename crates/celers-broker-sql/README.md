# celers-broker-sql

MySQL database broker implementation for CeleRS - a high-performance Celery-compatible task queue framework for Rust.

## Features

- **Reliable Task Queue**: MySQL-based task queue with `FOR UPDATE SKIP LOCKED` for distributed workers
- **Priority Queues**: Tasks can be prioritized for execution order
- **Dead Letter Queue (DLQ)**: Automatic handling of permanently failed tasks
- **Delayed Execution**: Schedule tasks for future execution with `enqueue_at` and `enqueue_after`
- **Batch Operations**: High-throughput batch enqueue/dequeue/ack operations
- **Queue Control**: Pause/resume queue processing at runtime
- **Task Inspection**: Query task status, statistics, and worker assignments
- **Result Storage**: Store and retrieve task execution results
- **Worker Tracking**: Monitor which workers are processing which tasks
- **Health Monitoring**: Database health checks and table size monitoring
- **Maintenance Tools**: Task archiving, stuck task recovery, and selective purging
- **Prometheus Metrics**: Optional metrics integration (with `metrics` feature)

## Requirements

- **MySQL 8.0+** (requires `FOR UPDATE SKIP LOCKED` support)
- Rust 2021 edition

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
celers-broker-sql = "0.1"
celers-core = "0.1"

# Optional: Enable Prometheus metrics
# celers-broker-sql = { version = "0.1", features = ["metrics"] }
```

## Quick Start

### 1. Create a MySQL Database

```sql
CREATE DATABASE celers;
```

### 2. Initialize the Broker

```rust
use celers_broker_sql::MysqlBroker;
use celers_core::Broker;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create broker
    let broker = MysqlBroker::new("mysql://user:pass@localhost/celers").await?;

    // Run migrations (creates tables and indexes)
    broker.migrate().await?;

    Ok(())
}
```

### 3. Enqueue Tasks

```rust
use celers_core::SerializedTask;

// Create a task
let task = SerializedTask::new("my_task".to_string(), vec![1, 2, 3, 4]);

// Enqueue it
let task_id = broker.enqueue(task).await?;
println!("Enqueued task: {}", task_id);
```

### 4. Dequeue and Process Tasks

```rust
// Dequeue a task
if let Some(message) = broker.dequeue().await? {
    let task = message.task;
    println!("Processing task: {}", task.metadata.name);

    // Process the task...

    // Acknowledge completion
    broker.ack(&task.metadata.id, message.receipt_handle.as_deref()).await?;
}
```

## Advanced Usage

### Delayed Task Execution

```rust
use std::time::SystemTime;

// Schedule for specific timestamp (Unix seconds)
let execute_at = SystemTime::now()
    .duration_since(SystemTime::UNIX_EPOCH)?
    .as_secs() as i64 + 3600; // 1 hour from now

broker.enqueue_at(task, execute_at).await?;

// Or schedule after a delay (seconds)
broker.enqueue_after(task, 300).await?; // 5 minutes
```

### Batch Operations

```rust
// Batch enqueue (high throughput)
let tasks = vec![
    SerializedTask::new("task1".to_string(), vec![1]),
    SerializedTask::new("task2".to_string(), vec![2]),
    SerializedTask::new("task3".to_string(), vec![3]),
];
let task_ids = broker.enqueue_batch(tasks).await?;

// Batch dequeue
let messages = broker.dequeue_batch(10).await?;

// Batch ack
let tasks_to_ack: Vec<_> = messages.iter()
    .map(|m| (m.task.metadata.id, m.receipt_handle.clone()))
    .collect();
broker.ack_batch(&tasks_to_ack).await?;
```

### Queue Control

```rust
// Pause queue (dequeue returns None)
broker.pause();
assert!(broker.is_paused());

// Resume queue
broker.resume();
assert!(!broker.is_paused());
```

### Task Inspection

```rust
use celers_broker_sql::DbTaskState;

// Get task details
if let Some(task_info) = broker.get_task(&task_id).await? {
    println!("Task: {} - State: {:?}", task_info.task_name, task_info.state);
}

// List tasks by state
let pending_tasks = broker.list_tasks(Some(DbTaskState::Pending), 100, 0).await?;

// Get queue statistics
let stats = broker.get_statistics().await?;
println!("Pending: {}, Processing: {}, Completed: {}",
    stats.pending, stats.processing, stats.completed);

// Count by task name
let counts = broker.count_by_task_name().await?;
for count in counts {
    println!("{}: {} pending, {} completed",
        count.task_name, count.pending, count.completed);
}

// List scheduled tasks
let scheduled = broker.list_scheduled_tasks(100, 0).await?;
for task in scheduled {
    println!("Task {} scheduled in {} seconds",
        task.task_name, task.delay_remaining_secs);
}
```

### Worker Tracking

```rust
// Dequeue with worker ID
let worker_id = "worker-001";
if let Some(message) = broker.dequeue_with_worker_id(worker_id).await? {
    // Process task...
    broker.ack(&message.task.metadata.id, message.receipt_handle.as_deref()).await?;
}

// Get tasks by worker
let worker_tasks = broker.get_tasks_by_worker(worker_id).await?;
```

### Task Result Storage

```rust
use celers_broker_sql::TaskResultStatus;
use serde_json::json;

// Store result
broker.store_result(
    &task_id,
    "my_task",
    TaskResultStatus::Success,
    Some(json!({"output": "done"})),
    None, // error
    None, // traceback
    Some(1500), // runtime in ms
).await?;

// Retrieve result
if let Some(result) = broker.get_result(&task_id).await? {
    println!("Result: {:?}", result.result);
    println!("Runtime: {}ms", result.runtime_ms.unwrap_or(0));
}
```

### Dead Letter Queue (DLQ)

```rust
// List DLQ tasks
let dlq_tasks = broker.list_dlq(100, 0).await?;

// Requeue from DLQ
if let Some(dlq_task) = dlq_tasks.first() {
    let new_task_id = broker.requeue_from_dlq(&dlq_task.id).await?;
    println!("Requeued as: {}", new_task_id);
}

// Purge DLQ
let purged = broker.purge_all_dlq().await?;
println!("Purged {} tasks from DLQ", purged);
```

### Health Checks and Maintenance

```rust
use std::time::Duration;

// Health check
let health = broker.check_health().await?;
println!("MySQL version: {}", health.database_version);
println!("Pending tasks: {}", health.pending_tasks);

// Archive old completed tasks (older than 7 days)
let archived = broker.archive_completed_tasks(Duration::from_secs(7 * 24 * 3600)).await?;
println!("Archived {} old tasks", archived);

// Recover stuck tasks (processing > 1 hour)
let recovered = broker.recover_stuck_tasks(Duration::from_secs(3600)).await?;
println!("Recovered {} stuck tasks", recovered);

// Archive old results (older than 30 days)
let archived_results = broker.archive_results(Duration::from_secs(30 * 24 * 3600)).await?;
println!("Archived {} old results", archived_results);
```

### Database Monitoring

```rust
// Get table sizes
let table_sizes = broker.get_table_sizes().await?;
for table in table_sizes {
    println!("{}: {} rows, {} bytes data, {} bytes indexes",
        table.table_name, table.row_count, table.data_size_bytes, table.index_size_bytes);
}

// Optimize tables (run periodically)
broker.optimize_tables().await?;

// Analyze tables (update index statistics)
broker.analyze_tables().await?;
```

### Prometheus Metrics (with `metrics` feature)

```rust
// Update metrics (call periodically, e.g., every 10 seconds)
#[cfg(feature = "metrics")]
broker.update_metrics().await?;

// Metrics exposed:
// - celers_tasks_enqueued_total
// - celers_tasks_enqueued_by_type
// - celers_queue_size (pending tasks)
// - celers_processing_queue_size
// - celers_dlq_size
```

### Multi-tenant Queues

```rust
// Create broker with specific queue name
let queue_a = MysqlBroker::with_queue("mysql://...", "queue_a").await?;
let queue_b = MysqlBroker::with_queue("mysql://...", "queue_b").await?;

// Each queue is logically separated (stored in metadata)
queue_a.enqueue(task_a).await?;
queue_b.enqueue(task_b).await?;
```

## Database Schema

### Tables

- `celers_tasks` - Main task queue
- `celers_dead_letter_queue` - Failed tasks that exceeded max retries
- `celers_task_results` - Task execution results
- `celers_task_history` - Task audit trail (future)

### Key Indexes

- `idx_tasks_state_priority` - Efficient dequeue by state and priority
- `idx_tasks_scheduled` - Scheduled task processing
- `idx_tasks_worker` - Worker tracking
- `idx_tasks_task_name` - Task name lookups
- `idx_results_task_name` - Result queries by task type
- See migration files for complete index strategy

## Performance Tuning

### Connection Pool

```rust
// Default: 20 connections, 5s timeout
// For high throughput, increase max_connections:
// Edit MysqlBroker::new() or MysqlBroker::with_queue()
```

### Batch Operations

Use batch operations for high throughput:
- `enqueue_batch()` - Up to 10x faster than individual enqueues
- `dequeue_batch()` - Fetch multiple tasks in one transaction
- `ack_batch()` - Acknowledge multiple tasks at once

### MySQL Configuration

Recommended `my.cnf` settings:

```ini
[mysqld]
# Connection settings
max_connections = 500
connect_timeout = 10
wait_timeout = 28800

# Performance
innodb_buffer_pool_size = 2G  # 70-80% of RAM
innodb_log_file_size = 512M
innodb_flush_log_at_trx_commit = 2
innodb_flush_method = O_DIRECT

# Query cache (MySQL 5.7)
query_cache_type = 0
query_cache_size = 0
```

### Maintenance Schedule

Run these operations periodically:

```rust
// Daily: Archive old tasks
broker.archive_completed_tasks(Duration::from_secs(7 * 24 * 3600)).await?;

// Daily: Recover stuck tasks
broker.recover_stuck_tasks(Duration::from_secs(3600)).await?;

// Weekly: Optimize tables
broker.optimize_tables().await?;

// Weekly: Analyze tables
broker.analyze_tables().await?;

// Monthly: Archive old results
broker.archive_results(Duration::from_secs(30 * 24 * 3600)).await?;
```

## Comparison with PostgreSQL Broker

### Similarities

- Same `FOR UPDATE SKIP LOCKED` pattern
- Same API (implements `Broker` trait)
- Same performance characteristics
- Same safety guarantees

### Differences

- **MySQL** uses `?` placeholders vs PostgreSQL `$1, $2`
- **MySQL** stores UUIDs as `CHAR(36)` vs native `UUID` type
- **MySQL** uses stored procedures vs PostgreSQL functions
- **MySQL** `DATE_ADD()` vs PostgreSQL `INTERVAL` syntax
- **MySQL** `ON DUPLICATE KEY UPDATE` vs PostgreSQL `ON CONFLICT`

## Error Handling

All operations return `Result<T, CelersError>`:

```rust
match broker.enqueue(task).await {
    Ok(task_id) => println!("Enqueued: {}", task_id),
    Err(e) => eprintln!("Failed to enqueue: {}", e),
}
```

## Migration

Migrations are embedded in the binary and run via `broker.migrate()`:

- `001_init.sql` - Initial schema (tasks, DLQ, history tables)
- `002_results.sql` - Results table
- `003_performance_indexes.sql` - Additional performance indexes

To run migrations:

```rust
broker.migrate().await?;
```

Migrations are idempotent and can be run multiple times safely.

## Backup and Restore Procedures

### Database Backup

Use `mysqldump` for backing up CeleRS tables:

```bash
# Backup all CeleRS tables
mysqldump -u user -p database_name \
  celers_tasks \
  celers_dead_letter_queue \
  celers_task_results \
  celers_task_history \
  celers_migrations \
  > celers_backup_$(date +%Y%m%d_%H%M%S).sql

# Backup with compression
mysqldump -u user -p database_name \
  celers_tasks \
  celers_dead_letter_queue \
  celers_task_results \
  celers_task_history \
  celers_migrations \
  | gzip > celers_backup_$(date +%Y%m%d_%H%M%S).sql.gz

# Include routines (stored procedures)
mysqldump -u user -p database_name \
  --routines \
  --triggers \
  celers_tasks \
  celers_dead_letter_queue \
  celers_task_results \
  celers_task_history \
  celers_migrations \
  > celers_full_backup_$(date +%Y%m%d_%H%M%S).sql
```

### Selective Backup Strategies

```bash
# Backup only pending and processing tasks (for migration)
mysqldump -u user -p database_name celers_tasks \
  --where="state IN ('pending', 'processing')" \
  > celers_active_tasks_$(date +%Y%m%d).sql

# Backup DLQ for analysis
mysqldump -u user -p database_name celers_dead_letter_queue \
  > celers_dlq_$(date +%Y%m%d).sql

# Backup results for auditing
mysqldump -u user -p database_name celers_task_results \
  > celers_results_$(date +%Y%m%d).sql
```

### Database Restore

```bash
# Restore from backup
mysql -u user -p database_name < celers_backup_20260118_120000.sql

# Restore from compressed backup
gunzip < celers_backup_20260118_120000.sql.gz | mysql -u user -p database_name

# Restore specific table
mysql -u user -p database_name < celers_tasks_backup.sql
```

### Point-in-Time Recovery

Enable binary logging in MySQL for PITR:

```ini
[mysqld]
log_bin = /var/log/mysql/mysql-bin.log
binlog_format = ROW
expire_logs_days = 7
```

Recovery procedure:

```bash
# 1. Restore from last full backup
mysql -u user -p database_name < last_full_backup.sql

# 2. Apply binary logs up to specific point
mysqlbinlog --start-datetime="2026-01-18 10:00:00" \
            --stop-datetime="2026-01-18 11:30:00" \
            /var/log/mysql/mysql-bin.000001 | \
            mysql -u user -p database_name
```

### Automated Backup Script

```bash
#!/bin/bash
# celers_backup.sh - Automated CeleRS backup script

BACKUP_DIR="/backups/celers"
DB_NAME="your_database"
DB_USER="backup_user"
RETENTION_DAYS=30
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Create backup directory
mkdir -p "$BACKUP_DIR"

# Perform backup
mysqldump -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" \
  --routines \
  --triggers \
  celers_tasks \
  celers_dead_letter_queue \
  celers_task_results \
  celers_task_history \
  celers_migrations \
  | gzip > "$BACKUP_DIR/celers_$TIMESTAMP.sql.gz"

# Remove old backups
find "$BACKUP_DIR" -name "celers_*.sql.gz" -mtime +$RETENTION_DAYS -delete

# Verify backup
if [ -f "$BACKUP_DIR/celers_$TIMESTAMP.sql.gz" ]; then
    echo "Backup completed: celers_$TIMESTAMP.sql.gz"
    # Optional: Upload to S3 or other storage
    # aws s3 cp "$BACKUP_DIR/celers_$TIMESTAMP.sql.gz" s3://my-bucket/celers-backups/
else
    echo "Backup failed!" >&2
    exit 1
fi
```

### Disaster Recovery Checklist

1. **Before Disaster:**
   - Regular automated backups (daily minimum)
   - Test restore procedures monthly
   - Store backups offsite (S3, GCS, etc.)
   - Monitor backup success/failure
   - Document recovery procedures

2. **During Recovery:**
   - Stop all workers to prevent new tasks
   - Assess data loss window
   - Restore from most recent backup
   - Apply binary logs if available
   - Verify data integrity
   - Resume workers gradually

3. **After Recovery:**
   - Check for lost tasks (compare with application logs)
   - Verify DLQ items
   - Monitor for anomalies
   - Document incident for post-mortem

### Data Migration Between Environments

```bash
# Export from production
mysqldump -u user -p prod_db \
  --where="state IN ('pending', 'processing')" \
  celers_tasks > prod_tasks.sql

# Import to staging
mysql -u user -p staging_db < prod_tasks.sql

# Or use programmatic approach
```

```rust
// Programmatic migration example
async fn migrate_pending_tasks(
    source_broker: &MysqlBroker,
    target_broker: &MysqlBroker,
) -> Result<u64> {
    let pending_tasks = source_broker
        .list_tasks(Some(DbTaskState::Pending), 10000, 0)
        .await?;

    let mut migrated = 0u64;
    for task_info in pending_tasks {
        // Fetch task payload and metadata
        // Enqueue to target broker
        // Mark as migrated in source
        migrated += 1;
    }

    Ok(migrated)
}
```

## Examples

See the [examples](examples/) directory for complete working examples:

- **task_producer.rs** - Comprehensive task enqueueing with different patterns (single, batch, scheduled, priority)
- **worker_pool.rs** - Production-ready worker pool with health monitoring and graceful shutdown
- **circuit_breaker.rs** - Circuit breaker pattern for resilient database operations
- **bulk_import_export.rs** - Data migration and backup utilities using JSON format
- **recurring_tasks.rs** - Scheduled periodic task execution (cron-like functionality)
- **advanced_retry.rs** - Sophisticated retry strategies with exponential backoff and jitter

Each example includes detailed documentation and can be run with:
```bash
cargo run --example <example_name>
```

For detailed usage instructions, see [examples/README.md](examples/README.md).

## License

MIT OR Apache-2.0

## Contributing

Contributions welcome! Please ensure:

- All tests pass: `cargo test`
- No warnings: `cargo clippy`
- Code is formatted: `cargo fmt`
