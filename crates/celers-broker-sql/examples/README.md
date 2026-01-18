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

### 2. Idempotency Keys (`idempotency_keys.rs`)

Demonstrates how to use idempotency keys to prevent duplicate task execution in distributed systems.

**Features:**
- Payment processing with duplicate prevention
- Notification sending with idempotency
- TTL-based key expiration
- Statistics and monitoring
- Automatic cleanup of expired keys

**Use Cases:**
- Financial transactions and payment processing
- Email/notification sending
- External API calls
- Any operation that must be executed exactly once

**Run:**
```bash
cargo run --example idempotency_keys
```

**Expected Output:**
```
=== CeleRS MySQL Broker: Idempotency Keys Demo ===

✓ Connected to MySQL broker
✓ Migrations applied

--- Demo 1: Payment Processing with Idempotency ---
  1. Submitting payment request (first time)...
     ✓ Task created: 550e8400-e29b-41d4-a716-446655440000
  2. Submitting same payment request (retry)...
     ✓ Returned existing task: 550e8400-e29b-41d4-a716-446655440000
     ✓ Idempotency verified: Same task ID returned

  Idempotency Record:
    - Key: payment-550e8400-e29b-41d4-a716-446655440000
    - Task ID: 550e8400-e29b-41d4-a716-446655440000
    - Task Name: process_payment
    - Created: 2026-01-18 08:00:00
    - Expires: 2026-01-18 09:00:00

--- Demo 2: Notification Sending ---
  1. Sending order confirmation email...
     ✓ Email task created: 6ba7b810-9dad-11d1-80b4-00c04fd430c8
  2. Retrying email send (simulated network retry)...
     ✓ Returned existing task: 6ba7b810-9dad-11d1-80b4-00c04fd430c8
     ✓ Duplicate notification prevented!

--- Demo 3: TTL and Expiration ---
  1. Submitting API call with 3-second TTL...
     ✓ Task created: 7c9e6679-7425-40de-944b-e07fc1f90ae7
  2. Trying to submit again (before expiration)...
     ✓ Returned existing task: 7c9e6679-7425-40de-944b-e07fc1f90ae7
  3. Waiting for TTL to expire (3 seconds)...
     ✓ Cleaned up 1 expired keys
  4. Submitting again after expiration...
     ✓ New task created: 8f14e45f-ceea-467a-9af1-4f3f0c0c3e0f
     ✓ New task allowed after expiration

--- Demo 4: Statistics and Monitoring ---
  1. Creating tasks with various idempotency keys...
     ✓ Created 5 tasks with unique idempotency keys
  2. Attempting to create duplicates...
     ✓ Duplicate submissions handled (returned existing tasks)

  Idempotency Statistics:
    Task: batch_operation
      - Total keys: 5
      - Unique keys: 5
      - Active keys: 5
      - Expired keys: 0

--- Demo 5: Cleanup Expired Keys ---
  1. Creating tasks with 2-second TTL...
     ✓ Created 10 tasks
  2. Active keys before expiration: 10
  3. Waiting for keys to expire (3 seconds)...
  4. Running cleanup...
     ✓ Cleaned up 10 expired keys
  5. Active keys after cleanup: 0

  💡 Tip: In production, run cleanup_expired_idempotency_keys()
     periodically (e.g., via cron job or scheduled task) to prevent
     table bloat and maintain optimal performance.

=== Demo Complete ===
```

**Key Concepts:**
- **Idempotency Key**: A unique identifier for an operation (e.g., payment ID, request ID)
- **Task Name Scoping**: Same key can be reused across different task types
- **TTL (Time-to-Live)**: How long the key remains valid (prevents permanent blocks)
- **Duplicate Detection**: Returns existing task ID if duplicate found within TTL window
- **Automatic Cleanup**: Expired keys are removed to prevent table bloat

### 3. Advanced Queue Management (`advanced_queue_management.rs`)

Demonstrates production-critical queue management features for enterprise deployments.

**Features:**
- Transactional operations (atomic batch enqueues)
- Metadata-based queries (search by JSON fields)
- Capacity management and backpressure control
- Task expiration (TTL for stale tasks)
- Batch state updates (efficient bulk operations)
- Date range queries (analytics and auditing)

**Use Cases:**
- Implementing queue limits to prevent overload
- Multi-tenant task filtering by customer/organization
- Cleaning up abandoned or stale tasks
- Complex task dependencies and workflows
- Time-based analytics and reporting
- SLA monitoring and compliance

**Run:**
```bash
cargo run --example advanced_queue_management
```

**Expected Output:**
```
=== CeleRS MySQL Broker: Advanced Queue Management ===

✓ Connected to MySQL broker
✓ Migrations applied

--- Demo 1: Transactional Operations ---
  1. Creating tasks using batch enqueue (atomic operation)...
     ✓ Batch transaction committed successfully
     ✓ Created 3 tasks atomically
     ✓ Queue now has 3 pending tasks
  2. Updating metadata atomically...
     ✓ Metadata updated on task xxx

  💡 Transaction features:
     - Batch operations are automatically transactional
     - Use with_transaction() for custom SQL operations
     - All-or-nothing semantics ensure data consistency

--- Demo 2: Metadata-Based Queries ---
  1. Creating tasks and updating with metadata...
     ✓ Created 3 tasks with customer metadata
  2. Querying tasks for customer 'cust-0'...
     ✓ Found 2 tasks for customer 'cust-0'
       - Task xxx: process_order
       - Task yyy: process_order
  3. Querying priority customer tasks...
     ✓ Found 2 priority customer tasks

  💡 Metadata query use cases:
     - Filter tasks by customer, tenant, or organization
     - Find tasks with specific business attributes
     - Implement priority queues based on metadata
     - Track and query tasks by workflow or campaign ID

--- Demo 3: Capacity Management (Backpressure) ---
  1. Checking queue capacity (max: 5)...
     ✓ Queue has capacity: true
  2. Enqueuing tasks with capacity check...
     ✓ Task 0 enqueued
     ✓ Task 1 enqueued
     ...
     ✗ Task 6 rejected (queue full)
     ✗ Task 7 rejected (queue full)

     ✓ Successfully enqueued 5 tasks (max capacity: 5)
     ✓ Current queue size: 5
  3. Processing 3 tasks to free capacity...
     ✓ Queue now has capacity: true

--- Demo 4: Task Expiration (TTL) ---
  1. Creating tasks with short TTL...
     ✓ Created 5 tasks
     ✓ Pending tasks: 5
  2. Waiting 2 seconds for tasks to age...
  3. Expiring tasks older than 1 second...
     ✓ Expired 5 stale tasks
     ✓ Pending tasks now: 0
     ✓ Cancelled tasks: 5

  💡 Tip: Run expire_pending_tasks() periodically to clean up
     abandoned tasks (e.g., from crashed clients or cancelled operations)

--- Demo 5: Batch State Updates ---
  1. Creating 10 test tasks...
     ✓ Created 10 tasks
  2. Updating 5 tasks to 'processing' state in batch...
     ✓ Updated 5 tasks to processing

  Queue State Distribution:
    - Pending: 5
    - Processing: 5

  3. Cancelling remaining 5 tasks in batch...
     ✓ Cancelled 5 tasks

  Final State Distribution:
    - Pending: 0
    - Processing: 5
    - Cancelled: 5

--- Demo 6: Date Range Queries ---
  1. Creating tasks across a time range...
     ✓ Created tasks across a time range
  2. Querying all tasks (entire time range)...
     ✓ Found 4 total tasks
  3. Querying tasks created in last time window...
     ✓ Found 3 recent tasks
  4. Querying pending tasks in date range...
     ✓ Found 4 pending tasks in range

  💡 Use cases for date range queries:
     - Generate time-based reports and analytics
     - Audit task processing within specific periods
     - Clean up tasks older than retention period
     - Monitor SLA compliance (tasks created vs completed time)

=== Demo Complete ===
```

**Key Features Demonstrated:**

1. **Transactional Operations**
   - Atomic batch enqueues ensure all-or-nothing semantics
   - Metadata updates integrated with transactions
   - Rollback on errors preserves data consistency

2. **Metadata Queries**
   - JSON path queries ($.customer_id, $.priority_customer)
   - Filter tasks by business attributes
   - Enable multi-tenant architectures

3. **Capacity Management**
   - `has_capacity()` checks queue limits
   - `enqueue_with_capacity()` implements backpressure
   - Prevents queue overload in high-traffic scenarios

4. **Task Expiration**
   - `expire_pending_tasks()` cleans stale tasks
   - Configurable TTL per cleanup run
   - Moves expired tasks to cancelled state

5. **Batch State Updates**
   - `update_batch_state()` for efficient bulk operations
   - Single query updates multiple tasks
   - Reduces database round trips

6. **Date Range Queries**
   - `search_tasks_by_date_range()` for time-based filtering
   - Analytics and reporting capabilities
   - SLA monitoring and compliance tracking

### 4. Worker Pool (`worker_pool.rs`)

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

### 6. Distributed Tracing (`distributed_tracing.rs`)

Demonstrates W3C Trace Context propagation for distributed tracing with OpenTelemetry, Jaeger, and Zipkin compatibility.

**Features:**
- W3C traceparent header parsing and generation
- Enqueue tasks with trace context
- Extract trace context from tasks
- Create child spans for nested operations
- Automatic trace propagation across task chains
- Multi-level task chain tracing
- Sampling decision support
- Microservices trace propagation patterns

**Key Concepts:**
- **Trace ID**: Unique identifier for entire distributed trace (32 hex chars)
- **Span ID**: Unique identifier for specific operation (16 hex chars)
- **Child Span**: New span with same trace ID but different span ID
- **Trace Flags**: Sampling and debug flags (typically "01" for sampled)

**Run:**
```bash
cargo run --example distributed_tracing
```

**Expected Output:**
```
=== Distributed Tracing Example ===

Demo 1: Parsing W3C Traceparent Header
---------------------------------------
Incoming traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
Parsed trace context:
  Trace ID: 4bf92f3577b34da6a3ce929d0e0e4736
  Span ID:  00f067aa0ba902b7
  Flags:    01 (sampled: true)
  Regenerated: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
✓ Successfully parsed and regenerated traceparent

Demo 2: Enqueue Task with Trace Context
----------------------------------------
Created trace context:
  Trace ID: 4bf92f3577b34da6a3ce929d0e0e4736
  Span ID:  00f067aa0ba902b7
✓ Enqueued task with trace: xxx
✓ Extracted trace context from task:
  Trace ID: 4bf92f3577b34da6a3ce929d0e0e4736
  Span ID:  00f067aa0ba902b7

Demo 3: Child Span Propagation
-------------------------------
Parent trace:
  Trace ID: a1b2c3d4e5f6789012345678abcdef01
  Span ID:  1234567890abcdef
✓ Enqueued parent task: xxx

Child span created:
  Trace ID: a1b2c3d4e5f6789012345678abcdef01 (same as parent)
  Span ID:  fedcba0987654321 (different from parent)
✓ Enqueued child task: yyy
✓ Verified parent-child relationship

Demo 4: Task Chain with Trace Propagation
------------------------------------------
Root trace (from API request):
  Trace ID: fedcba9876543210fedcba9876543210
  Span ID:  fedcba9876543210
✓ Level 1 task enqueued: xxx
✓ Level 2 task enqueued: yyy
✓ Level 3 task enqueued: zzz

Trace chain verification:
  Root:    Trace=fedcba9876543210fedcba9876543210, Span=fedcba9876543210
  Level 2: Trace=fedcba9876543210fedcba9876543210, Span=abcd123456789012
  Level 3: Trace=fedcba9876543210fedcba9876543210, Span=9876543210abcdef
✓ All tasks share trace ID with unique span IDs

Demo 5: Trace Sampling
----------------------
Sampled trace:
  Flags: 01
  Is sampled: true
✓ Sampling decision verified

=== All Demos Completed Successfully ===
```

**Use Cases:**
- Microservices observability
- Performance debugging across distributed systems
- Request flow tracking
- Latency analysis
- Error root cause analysis
- Integration with Jaeger, Zipkin, OpenTelemetry

### 7. Lifecycle Hooks (`lifecycle_hooks.rs`)

Demonstrates task lifecycle hooks for extending broker behavior without modifying core code.

**Features:**
- Validation hooks to reject invalid tasks
- Logging hooks for observability
- Metrics collection hooks
- Task enrichment with metadata
- Multiple hooks execution (in registration order)
- Hook clearing and management
- Authorization patterns
- Rate limiting patterns
- External system integration
- Audit logging

**Hook Types:**
- `BeforeEnqueue` - Before a task is enqueued
- `AfterEnqueue` - After a task is successfully enqueued
- `BeforeDequeue` - Before a task is dequeued (reserved)
- `AfterDequeue` - After a task is dequeued
- `BeforeAck` - Before a task is acknowledged
- `AfterAck` - After a task is acknowledged
- `BeforeReject` - Before a task is rejected
- `AfterReject` - After a task is rejected

**Run:**
```bash
cargo run --example lifecycle_hooks
```

**Expected Output:**
```
=== Task Lifecycle Hooks Example ===

Demo 1: Validation Hooks
------------------------
✓ Added validation hook
✓ Valid task accepted: xxx
✓ Invalid task rejected as expected: Validation failed: Task payload cannot be empty

Demo 2: Logging Hooks
---------------------
✓ Added logging hooks
  [LOG] Enqueueing task 'send_email' to queue 'default'
  [LOG] Task 'send_email' enqueued successfully: Some(xxx)
  [LOG] Enqueueing task 'process_image' to queue 'default'
  [LOG] Task 'process_image' enqueued successfully: Some(yyy)

Demo 3: Metrics Collection Hooks
---------------------------------
✓ Added metrics collection hooks

Metrics collected:
  Tasks enqueue attempts: 5
  Tasks enqueued successfully: 5

Demo 4: Task Enrichment Hooks
------------------------------
✓ Added enrichment hook
  [ENRICH] Adding metadata: timestamp=2026-01-06T12:34:56Z
✓ Task enqueued with enrichment: xxx

Demo 5: Multiple Hooks Execution Order
---------------------------------------
✓ Added 3 BeforeEnqueue hooks

Enqueueing task to trigger hooks:
  [HOOK 1] First hook: multi_hook_task
  [HOOK 2] Second hook: multi_hook_task
  [HOOK 3] Third hook: multi_hook_task
✓ All hooks executed in order

Demo 6: Clear Hooks
-------------------
✓ Added hook

Enqueueing task before clearing hooks:
  [HOOK] This hook will be cleared

✓ Cleared all hooks

Enqueueing task after clearing hooks:
  (No hook output - hooks were cleared)

=== All Demos Completed Successfully ===
```

**Use Cases:**
- Input validation and sanitization
- Custom logging and audit trails
- Business metrics tracking
- Task enrichment with contextual data
- Authorization and access control
- Rate limiting and throttling
- External system integration (webhooks, notifications)
- Error tracking and aggregation
- Compliance and regulatory requirements
- Custom middleware patterns

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

### 5. Production Operations (`production_operations.rs`)

Demonstrates the production operations toolkit for disaster recovery, performance testing, deployment validation, and query optimization.

**Features:**
- Migration verification for deployment validation
- Load generation for performance testing
- Query performance profiling
- DLQ batch replay for disaster recovery
- Connection health monitoring
- Complete operational workflow

**Run:**
```bash
cargo run --example production_operations
```

**Expected Output:**
```
=== CeleRS MySQL Broker: Production Operations Toolkit ===

✓ Connecting to MySQL broker...
✓ Running migrations...

--- Demo 1: Migration Verification ---
  Verifying database schema integrity...
  ✓ Migration verification complete:
    - Applied migrations: 5
    - Missing migrations: 0
    - Schema valid: true
    - Complete: true

  Applied migrations:
    ✓ 001_init.sql
    ✓ 002_results.sql
    ✓ 003_performance_indexes.sql
    ✓ 006_idempotency.sql
    ✓ 008_production_features.sql

  ✓ All migrations applied successfully!

  💡 Use Case: Run this check in CI/CD pipelines before deployment
     to ensure database schema is up-to-date.

--- Demo 2: Load Generation for Performance Testing ---
  Generating synthetic load for benchmarking...
  ✓ Generated 100 test tasks
  ✓ Current queue size: 100 pending tasks

  💡 Use Cases:
     - Load testing before production deployment
     - Capacity planning for peak traffic
     - Stress testing connection pool and database
     - Benchmarking different configurations

--- Demo 3: Query Performance Profiling ---
  Analyzing query performance (requires performance_schema)...
  ✓ Found 5 slow queries:

  Query #1:
    Digest: SELECT * FROM celers_tasks WHERE state = ?
    Executions: 120
    Avg time: 2.45ms
    Rows examined: 1200
    Rows sent: 100
    ⚠ NEEDS OPTIMIZATION
      - No index used: 120 times

  💡 Use Cases:
     - Identify and optimize slow queries in production
     - Find missing or unused indexes
     - Optimize table access patterns
     - Performance troubleshooting

--- Demo 4: Simulating Failed Tasks ---
  Creating tasks that will fail and move to DLQ...
  ✓ Enqueued 10 tasks
  Simulating task failures (rejecting without requeue)...
  ✓ Simulated 5 task failures
  ✓ DLQ size: 5 tasks

--- Demo 5: Batch Replay from DLQ ---
  Recovering failed tasks from Dead Letter Queue...
  ✓ Replayed 5 tasks from DLQ
  ✓ Queue status after replay: 5 pending, 0 DLQ

  💡 Use Cases:
     - Recover from systematic failures after bug fix
     - Replay failed payment transactions
     - Bulk retry of failed notification deliveries
     - Disaster recovery operations

--- Demo 6: Connection Health Monitoring ---
  Checking connection pool health...
  ✓ Connection pool is healthy
  Connection pool metrics:
    - Total connections: 20
    - Active connections: 2
    - Idle connections: 18
    - Utilization: 10.0%

  💡 Use Cases:
     - Health check endpoints for load balancers
     - Auto-scaling decisions
     - Proactive alerting before issues occur
     - Capacity planning

--- Demo 7: Cleanup ---
  Cleaning up test data...
  ✓ Cleaned up 105 test tasks

=== Production Operations Toolkit Demo Complete ===

📚 Key Takeaways:

1. Migration Verification
   → verify_migrations() - Validate schema in CI/CD

2. Load Generation
   → generate_load() - Benchmark and capacity planning

3. Query Profiling
   → profile_query_performance() - Find slow queries

4. DLQ Batch Replay
   → replay_dlq_batch() - Disaster recovery

5. Connection Health
   → check_connection_health() - Proactive monitoring

💡 These utilities are essential for production operations:
   - Deployment safety and validation
   - Performance testing and optimization
   - Disaster recovery and task replay
   - Proactive health monitoring
   - Capacity planning and scaling
```

**Key Operations Demonstrated:**

1. **Migration Verification**
   - `verify_migrations()` - Validate all required migrations are applied
   - Essential for CI/CD pipelines and deployment validation
   - Checks core table schema integrity

2. **Load Generation**
   - `generate_load()` - Create synthetic test tasks
   - Configurable payload size and priority ranges
   - Critical for performance testing and capacity planning

3. **Query Profiling**
   - `profile_query_performance()` - Analyze slow queries
   - Identifies missing indexes and optimization opportunities
   - Requires MySQL performance_schema

4. **DLQ Batch Replay**
   - `replay_dlq_batch()` - Bulk recovery from DLQ
   - Filter by task name and retry count
   - Essential for disaster recovery

5. **Connection Health**
   - `check_connection_health()` - Monitor pool status
   - Proactive health checks for auto-scaling
   - Detailed connection pool metrics

**Production Use Cases:**

- **CI/CD Integration**: Verify migrations before deployment
- **Load Testing**: Generate realistic load for benchmarking
- **Performance Optimization**: Identify and fix slow queries
- **Disaster Recovery**: Replay failed tasks after bug fixes
- **Health Monitoring**: Proactive detection of issues
- **Capacity Planning**: Test infrastructure under load

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
