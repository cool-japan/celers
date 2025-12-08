# CeleRS MySQL Broker Performance Benchmarks

This directory contains comprehensive performance benchmarks for the MySQL broker implementation using [Criterion.rs](https://github.com/bheisler/criterion.rs).

## Running Benchmarks

### Prerequisites

1. **MySQL Server**: You need a running MySQL 8.0+ instance
2. **Test Database**: Create a dedicated database for benchmarking
3. **Environment Variable**: Set `DATABASE_URL` to your MySQL connection string

```bash
# Create benchmark database
mysql -u root -p -e "CREATE DATABASE IF NOT EXISTS celers_bench;"

# Set database URL
export DATABASE_URL="mysql://root:password@localhost/celers_bench"
```

### Running All Benchmarks

```bash
# Run all benchmarks
cargo bench

# Run with verbose output
cargo bench --verbose

# Save results to a specific directory
cargo bench -- --save-baseline my-baseline
```

### Running Specific Benchmarks

```bash
# Run only enqueue benchmarks
cargo bench enqueue

# Run only batch operations
cargo bench batch

# Run specific benchmark function
cargo bench bench_dequeue_single
```

### Comparing Baselines

```bash
# Save baseline before changes
cargo bench -- --save-baseline before

# Make your changes...

# Save baseline after changes
cargo bench -- --save-baseline after

# Compare results
cargo bench -- --baseline before --load-baseline after
```

## Benchmark Suite

### 1. Single Operations

- **`bench_enqueue_single`**: Benchmark single task enqueue
- **`bench_dequeue_single`**: Benchmark single task dequeue with acknowledgment
- **`bench_ack`**: Benchmark task acknowledgment
- **`bench_reject_requeue`**: Benchmark task rejection with requeue
- **`bench_queue_size`**: Benchmark queue size query
- **`bench_statistics`**: Benchmark queue statistics query
- **`bench_enqueue_scheduled`**: Benchmark scheduled task enqueue

### 2. Batch Operations

- **`bench_enqueue_batch`**: Benchmark batch enqueue with sizes: 10, 50, 100, 500, 1000
- **`bench_dequeue_batch`**: Benchmark batch dequeue with sizes: 10, 50, 100
- **`bench_ack_batch`**: Benchmark batch acknowledgment with sizes: 10, 50, 100

## Benchmark Configuration

The benchmarks are configured with:

- **Sample Size**: 100 iterations per benchmark
- **Measurement Time**: 10 seconds per benchmark
- **Warm-up Time**: 3 seconds before measurements

You can adjust these in `broker_benchmark.rs`:

```rust
criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(100)           // Adjust sample size
        .measurement_time(Duration::from_secs(10))  // Adjust measurement time
        .warm_up_time(Duration::from_secs(3));      // Adjust warm-up time
    targets = ...
}
```

## Understanding Results

Criterion provides detailed statistics for each benchmark:

```
bench_enqueue_single    time:   [1.234 ms 1.256 ms 1.278 ms]
                        change: [-2.3% -1.8% -1.3%] (p = 0.00 < 0.05)
                        Performance has improved.
```

- **time**: Lower/estimate/upper bounds of execution time
- **change**: Performance change compared to previous run
- **p-value**: Statistical significance (< 0.05 is significant)

## Performance Targets

Based on typical MySQL configurations, expected performance:

| Operation              | Target (ops/sec) | Notes                          |
|------------------------|------------------|--------------------------------|
| Enqueue Single         | ~800-1200        | Depends on payload size        |
| Dequeue Single         | ~700-1000        | Includes FOR UPDATE SKIP LOCKED|
| Enqueue Batch (100)    | ~8000-12000      | Total tasks/sec                |
| Dequeue Batch (100)    | ~7000-10000      | Total tasks/sec                |
| Ack Single             | ~1000-1500       | Simple UPDATE query            |
| Ack Batch (100)        | ~10000-15000     | Total tasks/sec                |
| Queue Size             | ~5000-10000      | Cached query, very fast        |
| Statistics             | ~1000-2000       | Multiple aggregations          |

*These targets assume:*
- MySQL 8.0+ on modern hardware
- SSD storage
- Proper indexes in place
- Local or low-latency network connection

## Comparing with PostgreSQL

To compare MySQL broker performance with PostgreSQL:

1. Run benchmarks on MySQL:
```bash
export DATABASE_URL="mysql://root:password@localhost/celers_bench"
cargo bench -- --save-baseline mysql
```

2. Run benchmarks on PostgreSQL (if available):
```bash
cd ../celers-broker-postgres
export DATABASE_URL="postgresql://postgres:password@localhost/celers_bench"
cargo bench -- --save-baseline postgres
```

3. Compare results manually using Criterion's HTML reports at:
   - `target/criterion/*/mysql/report/index.html`
   - `target/criterion/*/postgres/report/index.html`

## Optimization Tips

If benchmarks show poor performance:

### 1. Check MySQL Configuration

```sql
-- Check buffer pool size (should be 70-80% of RAM)
SHOW VARIABLES LIKE 'innodb_buffer_pool_size';

-- Check query cache (disabled in MySQL 8.0+)
SHOW VARIABLES LIKE 'query_cache%';

-- Check connection settings
SHOW VARIABLES LIKE 'max_connections';
```

### 2. Verify Indexes

```sql
-- Check if indexes are being used
EXPLAIN SELECT * FROM celers_tasks
WHERE queue_name = 'default' AND state = 'pending'
ORDER BY priority DESC, created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED;

-- Should use idx_tasks_state_priority index
```

### 3. Optimize Connection Pool

Adjust pool settings in your application:

```rust
let config = PoolConfig {
    max_connections: 10,
    min_connections: 2,
    acquire_timeout_secs: 30,
    max_lifetime_secs: Some(1800),
    idle_timeout_secs: Some(600),
};

let broker = MysqlBroker::with_config(&database_url, "default", config).await?;
```

### 4. Hardware Considerations

- **CPU**: More cores help with concurrent operations
- **Memory**: Larger buffer pool improves cache hit rates
- **Storage**: SSDs provide 10-100x faster I/O than HDDs
- **Network**: Use localhost or low-latency connections

## Continuous Benchmarking

For CI/CD integration:

```bash
# Run benchmarks without HTML report generation
cargo bench --no-fail-fast -- --noplot

# Check for performance regressions (requires baseline)
cargo bench -- --baseline main

# Exit with error if performance degrades by >10%
cargo bench -- --baseline main --significance-level 0.05
```

## Troubleshooting

### Error: "Failed to create broker"

- Ensure MySQL is running: `mysql -u root -p -e "SELECT 1"`
- Check DATABASE_URL is correct
- Verify database exists: `mysql -u root -p -e "SHOW DATABASES"`
- Check user permissions: `GRANT ALL ON celers_bench.* TO 'root'@'localhost'`

### Error: "Table doesn't exist"

Benchmarks automatically run migrations. If this fails:

```bash
# Manually run migrations
mysql -u root -p celers_bench < migrations/001_init.sql
mysql -u root -p celers_bench < migrations/002_results.sql
mysql -u root -p celers_bench < migrations/003_performance_indexes.sql
```

### Slow Performance

- Check if MySQL is under load: `SHOW PROCESSLIST;`
- Verify indexes: `SHOW INDEX FROM celers_tasks;`
- Check for lock contention: `SHOW ENGINE INNODB STATUS;`
- Ensure proper hardware (SSD, sufficient RAM)

## Benchmark Data

Benchmarks use small payloads (~20 bytes). For real-world testing with larger payloads:

Modify `create_task()` in `broker_benchmark.rs`:

```rust
fn create_task(id: u64) -> SerializedTask {
    // Small payload (current)
    let data = format!("test data {}", id);

    // Large payload (uncomment for testing)
    // let data = vec![0u8; 10_000]; // 10KB payload

    SerializedTask::new("bench_task".to_string(), data.into_bytes())
}
```

## License

Same as parent project (MIT OR Apache-2.0)
