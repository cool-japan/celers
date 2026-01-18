# PostgreSQL Broker Examples

This directory contains comprehensive examples demonstrating the features and capabilities of the PostgreSQL broker for CeleRS.

## Prerequisites

Before running these examples, ensure you have:

1. **PostgreSQL installed and running**
   ```bash
   # On macOS
   brew install postgresql
   brew services start postgresql

   # On Ubuntu/Debian
   sudo apt-get install postgresql
   sudo systemctl start postgresql
   ```

2. **Create a test database**
   ```bash
   createdb celers_example
   ```

3. **Set the DATABASE_URL environment variable** (optional)
   ```bash
   export DATABASE_URL="postgres://user:password@localhost/celers_example"
   ```

## Running Examples

### Basic Usage

Demonstrates fundamental broker operations:
- Creating and configuring a broker
- Enqueuing and dequeuing tasks
- Batch operations
- Priority tasks
- Delayed execution
- Queue statistics and control
- Task retries

```bash
cargo run --example basic_usage
```

**Key Features Demonstrated:**
- Simple enqueue/dequeue workflow
- Batch enqueue and batch acknowledge
- Priority-based task processing
- Delayed task scheduling
- Queue pause/resume functionality
- Task rejection and automatic retry
- Queue statistics and monitoring

### Monitoring and Performance

Showcases production-grade monitoring and optimization utilities:
- Consumer lag analysis and autoscaling
- Message velocity and growth trends
- Worker scaling recommendations
- SLA monitoring with age distribution
- Processing capacity estimation
- Queue health scoring
- Performance optimization utilities

```bash
cargo run --example monitoring_performance
```

**Key Features Demonstrated:**
- Real-time lag analysis with scaling recommendations
- Queue growth trend detection
- Intelligent worker scaling suggestions
- SLA compliance monitoring with percentiles
- System capacity planning
- PostgreSQL-specific optimizations:
  - Batch size calculation
  - Memory estimation
  - Connection pool sizing
  - VACUUM strategy recommendations
  - Index usage analysis
  - Query optimization strategies
  - Configuration tuning (shared_buffers, work_mem)

### Advanced Utilities

Demonstrates the latest production utilities for optimization and monitoring:
- Task result compression analysis
- Query performance regression detection
- Task execution metrics tracking
- DLQ automatic retry policies with intelligent backoff

```bash
cargo run --example advanced_utilities
```

**Key Features Demonstrated:**
- Compression recommendations for different payload types
- Cost-benefit analysis for compression decisions
- Algorithm selection (zstd, gzip, lz4)
- Query performance regression detection with severity classification
- CPU and memory usage tracking per task type
- Resource intensity classification and optimization suggestions
- Intelligent DLQ retry policies based on task characteristics
- Exponential backoff with jitter to prevent thundering herd
- Real-world workflow examples

## Example Scenarios

### Scenario 1: Getting Started

If you're new to the PostgreSQL broker, start with:
```bash
cargo run --example basic_usage
```

This will walk you through the essential operations you need to build a task queue system.

### Scenario 2: Production Monitoring

For production deployments, use the monitoring example to:
- Monitor queue health in real-time
- Get autoscaling recommendations
- Analyze SLA compliance
- Optimize PostgreSQL configuration

```bash
cargo run --example monitoring_performance
```

### Scenario 3: Advanced Production Operations

For advanced optimization and monitoring:
```bash
cargo run --example advanced_utilities
```

This demonstrates:
- When and how to compress task payloads
- Detecting performance regressions in queries
- Tracking resource usage per task type
- Configuring intelligent DLQ retry policies

### Scenario 4: Integration Testing

Both examples can be adapted for integration testing:

1. Modify the database URL to point to your test database
2. Run migrations programmatically
3. Execute tests against real PostgreSQL
4. Clean up test data afterward

## Code Organization

### basic_usage.rs
- **Lines 1-50**: Setup and connection
- **Lines 51-100**: Basic enqueue/dequeue
- **Lines 101-150**: Batch operations
- **Lines 151-200**: Priority and delayed tasks
- **Lines 201-250**: Statistics and queue control
- **Lines 251-300**: Retries and error handling

### monitoring_performance.rs
- **Lines 1-100**: Lag and velocity analysis
- **Lines 101-200**: Worker scaling and capacity
- **Lines 201-300**: SLA monitoring and health scores
- **Lines 301-400**: PostgreSQL optimization utilities

## Common Patterns

### Pattern 1: Graceful Shutdown

```rust
use tokio::signal;

let broker = PostgresBroker::new(&database_url).await?;

// Spawn processing loop
let processing_task = tokio::spawn(async move {
    loop {
        if let Some(msg) = broker.dequeue().await? {
            // Process task...
            broker.ack(&msg.task.metadata.id, msg.receipt_handle.as_deref()).await?;
        }
    }
});

// Wait for shutdown signal
signal::ctrl_c().await?;
println!("Shutting down gracefully...");
processing_task.abort();
```

### Pattern 2: Worker Pool

```rust
use futures::future::join_all;

let broker = Arc::new(PostgresBroker::new(&database_url).await?);
let worker_count = 5;

let workers: Vec<_> = (0..worker_count)
    .map(|id| {
        let broker = Arc::clone(&broker);
        tokio::spawn(async move {
            loop {
                if let Some(msg) = broker.dequeue().await? {
                    println!("Worker {} processing task", id);
                    // Process...
                    broker.ack(&msg.task.metadata.id, msg.receipt_handle.as_deref()).await?;
                }
            }
        })
    })
    .collect();

join_all(workers).await;
```

### Pattern 3: Monitoring Loop

```rust
use tokio::time::{interval, Duration};

let broker = PostgresBroker::new(&database_url).await?;
let mut monitor = interval(Duration::from_secs(30));

loop {
    monitor.tick().await;

    let stats = broker.get_statistics().await?;
    let lag = analyze_postgres_consumer_lag(
        stats.pending,
        calculate_processing_rate(&stats),
        60,
    );

    match lag.recommendation {
        ScalingRecommendation::ScaleUp { additional_workers } => {
            println!("⚠️  Need {} more workers", additional_workers);
        }
        _ => println!("✓ System healthy"),
    }
}
```

## Environment Variables

Configure the examples using environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABASE_URL` | PostgreSQL connection string | `postgres://localhost/celers_example` |
| `RUST_LOG` | Logging level | Not set |

Example:
```bash
export DATABASE_URL="postgres://myuser:mypass@localhost:5432/mydb"
export RUST_LOG="info"
cargo run --example basic_usage
```

## Troubleshooting

### Connection Refused

**Problem**: `failed to connect to PostgreSQL: connection refused`

**Solution**:
- Ensure PostgreSQL is running: `pg_isready`
- Check the connection string matches your setup
- Verify PostgreSQL is listening on the expected port (default: 5432)

### Database Does Not Exist

**Problem**: `database "celers_example" does not exist`

**Solution**:
```bash
createdb celers_example
```

### Permission Denied

**Problem**: `FATAL: role "username" does not exist`

**Solution**:
```bash
createuser -s myuser
# Or connect as a different user:
export DATABASE_URL="postgres://postgres:password@localhost/celers_example"
```

### Migration Errors

**Problem**: `migration failed: table already exists`

**Solution**:
- Drop and recreate the database:
  ```bash
  dropdb celers_example
  createdb celers_example
  ```
- Or manually clean up tables if needed

## Performance Tips

1. **Connection Pooling**: The broker uses connection pooling by default. Adjust pool size for your workload:
   ```rust
   let pool_options = PgPoolOptions::new()
       .max_connections(20)
       .acquire_timeout(Duration::from_secs(5));
   ```

2. **Batch Operations**: Use batch operations for better throughput:
   ```rust
   broker.enqueue_batch(tasks).await?;
   let messages = broker.dequeue_batch(10).await?;
   broker.ack_batch(&acks).await?;
   ```

3. **Indexes**: The migrations create optimized indexes. For large queues, consider:
   - Table partitioning (see partitioning methods)
   - Regular VACUUM ANALYZE
   - Monitoring index usage

4. **PostgreSQL Configuration**: See `monitoring_performance` example for tuning recommendations.

## Next Steps

After exploring these examples:

1. **Read the main documentation** for detailed API reference
2. **Review TODO.md** for a complete feature list
3. **Check migrations/** for database schema details
4. **Explore monitoring utilities** for production deployments
5. **Configure PostgreSQL** using recommendations from utilities module

## Contributing

Found a bug or have an improvement? Please open an issue or pull request!

## License

Same as the parent project.
