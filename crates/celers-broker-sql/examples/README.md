# CeleRS MySQL Broker Examples

This directory contains practical examples demonstrating how to use the CeleRS MySQL broker in real-world scenarios.

## Prerequisites

1. **MySQL Server**: Install and run MySQL 8.0+
   ```bash
   # Ubuntu/Debian
   sudo apt-get install mysql-server

   # macOS
   brew install mysql

   # Start MySQL
   sudo systemctl start mysql  # Linux
   brew services start mysql   # macOS
   ```

2. **Create Database**
   ```bash
   mysql -u root -p -e "CREATE DATABASE IF NOT EXISTS celers_dev;"
   ```

3. **Set Environment Variable**
   ```bash
   export DATABASE_URL="mysql://root:password@localhost/celers_dev"
   ```

## Examples

### 1. Task Producer (`task_producer.rs`)

Demonstrates how to enqueue tasks to the broker.

**Features:**
- Single task enqueue
- Batch task enqueue (10 tasks)
- Scheduled task execution (60 seconds delay)
- Priority task enqueue (priorities 1, 5, 10)
- Custom retry configuration (max 5 retries)
- Queue statistics and monitoring
- Task listing and inspection

**Run:**
```bash
cargo run --example task_producer
```

**Expected Output:**
```
Starting Task Producer Example
Connected to MySQL broker
Migrations complete

=== Example 1: Single Task Enqueue ===
Enqueued email task: 550e8400-e29b-41d4-a716-446655440000

=== Example 2: Batch Task Enqueue ===
Enqueued 10 processing tasks in batch

=== Example 3: Scheduled Task Enqueue ===
Scheduled task for 60 seconds from now: 6ba7b810-9dad-11d1-80b4-00c04fd430c8

=== Example 4: Priority Tasks ===
Enqueued priority 1 task: ...
Enqueued priority 5 task: ...
Enqueued priority 10 task: ...

=== Example 5: Task with Custom Retry ===
Enqueued task with max 5 retries: ...

=== Queue Statistics ===
Pending tasks: 15
Processing tasks: 0
Completed tasks: 0
Failed tasks: 0
DLQ tasks: 0
```

### 2. Worker Pool (`worker_pool.rs`)

Demonstrates a production-ready worker pool implementation.

**Features:**
- Multi-threaded task processing (10 concurrent workers)
- Graceful shutdown on Ctrl+C
- Task acknowledgment and rejection
- Error handling and retry logic
- Worker ID tracking
- Health monitoring (every 30 seconds)
- Automatic maintenance (hourly):
  - Archive completed tasks older than 7 days
  - Recover stuck tasks (stuck for >1 hour)
  - Optimize database tables
- Composite task processor pattern
- Configurable worker settings

**Run:**
```bash
cargo run --example worker_pool
```

**Expected Output:**
```
Starting CeleRS Worker Pool Example
Connecting to MySQL: localhost/celers_dev
Running migrations...
Starting worker pool: worker-550e8400-e29b-41d4-a716-446655440000
Max concurrent tasks: 10
Worker pool started. Press Ctrl+C to shutdown...

Health check: OK (MySQL 8.0.35), pool: 5/20, tasks: 15 pending, 0 processing
Queue stats - pending: 15, processing: 0, completed: 0, failed: 0, dlq: 0

Dequeued task: 550e8400-e29b-41d4-a716-446655440000 (send_email)
Sending email to: user@example.com, subject: Welcome!
Email sent successfully to: user@example.com
Task completed successfully: 550e8400-e29b-41d4-a716-446655440000

Dequeued task: 6ba7b810-9dad-11d1-80b4-00c04fd430c8 (process_data)
Processing data for ID: 0, size: 1024 bytes
Data processing completed for ID: 0
Task completed successfully: 6ba7b810-9dad-11d1-80b4-00c04fd430c8

...

^CShutdown signal received
Shutdown signal received, waiting for tasks to complete...
Worker pool stopped gracefully
Worker pool shut down successfully
```

## Complete Workflow Example

### Step 1: Start the Worker Pool

In terminal 1:
```bash
export DATABASE_URL="mysql://root:password@localhost/celers_dev"
cargo run --example worker_pool
```

### Step 2: Enqueue Tasks

In terminal 2:
```bash
export DATABASE_URL="mysql://root:password@localhost/celers_dev"
cargo run --example task_producer
```

### Step 3: Monitor Progress

In terminal 1 (worker pool), you'll see:
```
Dequeued task: xxx (send_email)
Sending email to: user@example.com, subject: Welcome!
Email sent successfully to: user@example.com
Task completed successfully: xxx

Dequeued task: yyy (process_data)
Processing data for ID: 0, size: 1024 bytes
Data processing completed for ID: 0
Task completed successfully: yyy
...

Health check: OK (MySQL 8.0.35), pool: 5/20, tasks: 5 pending, 3 processing
Queue stats - pending: 5, processing: 3, completed: 7, failed: 0, dlq: 0
```

### Step 4: Graceful Shutdown

Press Ctrl+C in terminal 1:
```
^CShutdown signal received
Shutdown signal received, waiting for tasks to complete...
Worker pool stopped gracefully
```

## Task Types

Both examples use these task types:

### EmailTask
```rust
struct EmailTask {
    to: String,
    subject: String,
    body: String,
}
```

Used for `send_email` tasks.

### ProcessingTask
```rust
struct ProcessingTask {
    id: u64,
    data: Vec<u8>,
}
```

Used for `process_data` tasks.

## Customizing Examples

### Adding New Task Types

1. Define your task structure:
```rust
#[derive(Debug, Serialize, Deserialize)]
struct MyCustomTask {
    field1: String,
    field2: i32,
}
```

2. Create a processor:
```rust
struct MyTaskProcessor;

#[async_trait::async_trait]
impl TaskProcessor for MyTaskProcessor {
    async fn process(&self, task_name: &str, payload: &[u8]) -> Result<(), String> {
        if task_name == "my_custom_task" {
            let task: MyCustomTask = serde_json::from_slice(payload)?;
            // Process task...
            Ok(())
        } else {
            Err(format!("Unknown task: {}", task_name))
        }
    }
}
```

3. Add to CompositeProcessor in worker_pool.rs:
```rust
Self {
    processors: vec![
        Box::new(EmailProcessor),
        Box::new(DataProcessor),
        Box::new(MyTaskProcessor),  // Add your processor
    ],
}
```

### Adjusting Worker Pool Settings

In `worker_pool.rs`, modify `WorkerConfig`:

```rust
let worker_config = WorkerConfig {
    worker_id: format!("worker-{}", uuid::Uuid::new_v4()),
    max_concurrent_tasks: 20,           // Increase for more parallelism
    poll_interval: Duration::from_millis(50),  // Poll more frequently
    retry_delay: Duration::from_secs(10),      // Wait longer on errors
};
```

### Connection Pool Tuning

In both examples, adjust `PoolConfig`:

```rust
let pool_config = PoolConfig {
    max_connections: 50,        // More connections for high throughput
    min_connections: 10,        // Keep more idle connections
    acquire_timeout_secs: 30,
    max_lifetime_secs: Some(3600),  // Recycle connections hourly
    idle_timeout_secs: Some(600),
};
```

## Monitoring and Debugging

### Check Queue Status

```bash
mysql -u root -p celers_dev -e "
SELECT
    state,
    COUNT(*) as count
FROM celers_tasks
GROUP BY state;
"
```

### View Processing Tasks

```bash
mysql -u root -p celers_dev -e "
SELECT
    id,
    task_name,
    worker_id,
    started_at,
    retry_count
FROM celers_tasks
WHERE state = 'processing'
ORDER BY started_at DESC
LIMIT 10;
"
```

### Check Dead Letter Queue

```bash
mysql -u root -p celers_dev -e "
SELECT
    task_id,
    task_name,
    error_message,
    failed_at
FROM celers_dead_letter_queue
ORDER BY failed_at DESC
LIMIT 10;
"
```

### Enable MySQL Query Log

For debugging slow queries:
```sql
SET GLOBAL general_log = 'ON';
SET GLOBAL log_output = 'TABLE';

-- View queries
SELECT * FROM mysql.general_log ORDER BY event_time DESC LIMIT 10;
```

## Troubleshooting

### "Failed to create broker"

- Check MySQL is running: `mysql -u root -p -e "SELECT 1"`
- Verify DATABASE_URL is correct
- Check user permissions: `GRANT ALL ON celers_dev.* TO 'root'@'localhost'`

### "No tasks available"

- Run task_producer first to enqueue tasks
- Check pending count: `SELECT COUNT(*) FROM celers_tasks WHERE state = 'pending'`
- Verify worker is running and not paused

### "Task processing is slow"

- Increase `max_concurrent_tasks` in WorkerConfig
- Increase `max_connections` in PoolConfig
- Check MySQL query performance with EXPLAIN
- Verify indexes are in place: `SHOW INDEX FROM celers_tasks`

### "Too many connections"

- Reduce `max_connections` in PoolConfig
- Increase MySQL `max_connections`: `SET GLOBAL max_connections = 500;`
- Check for connection leaks

## Best Practices

1. **Use Connection Pooling**: Configure pool size based on workload
2. **Set Appropriate Timeouts**: Balance between responsiveness and patience
3. **Monitor Health**: Implement health checks and alerting
4. **Handle Failures Gracefully**: Use retry logic and dead letter queues
5. **Log Everything**: Use structured logging for debugging
6. **Test Under Load**: Benchmark with production-like data
7. **Clean Up Regularly**: Archive old tasks and optimize tables
8. **Use Transactions**: Ensure data consistency

## Performance Tips

1. **Batch Operations**: Use `enqueue_batch()` for multiple tasks
2. **Optimize Indexes**: Ensure proper indexes on queue_name, state, priority
3. **Tune Buffer Pool**: Set `innodb_buffer_pool_size` to 70-80% of RAM
4. **Use SSDs**: Much faster than HDDs for database operations
5. **Reduce Polling**: Increase `poll_interval` if queue is usually empty
6. **Connection Reuse**: Keep connections alive with proper pool settings

## License

Same as parent project (MIT OR Apache-2.0)
