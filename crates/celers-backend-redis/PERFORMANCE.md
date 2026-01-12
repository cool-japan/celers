# Performance Tuning Guide

> Optimize Redis result backend performance for high-throughput production workloads

## Table of Contents

- [Quick Wins](#quick-wins)
- [Configuration Tuning](#configuration-tuning)
- [Compression Strategies](#compression-strategies)
- [Caching Strategies](#caching-strategies)
- [Batch Operations](#batch-operations)
- [TTL Management](#ttl-management)
- [Redis Configuration](#redis-configuration)
- [Network Optimization](#network-optimization)
- [Monitoring and Metrics](#monitoring-and-metrics)
- [Benchmarking](#benchmarking)

## Quick Wins

Top 5 performance optimizations (implement these first):

### 1. Enable Result Caching

```rust
use celers_backend_redis::{RedisResultBackend, ttl};

let backend = RedisResultBackend::new("redis://localhost:6379")?
    .with_cache(1000, ttl::MEDIUM);  // Cache 1000 results for 5 minutes

// Cache hit: ~10-100x faster than Redis roundtrip
```

**Impact:** 10-100x speedup for frequently accessed results

### 2. Use Compression for Large Results

```rust
use celers_backend_redis::batch_size;

let backend = RedisResultBackend::new("redis://localhost:6379")?
    .with_compression(batch_size::SMALL, 6);  // Compress results >1KB at level 6

// Typical compression ratio: 0.4-0.7 (60-40% size reduction)
```

**Impact:** 40-60% reduction in network traffic and memory usage

### 3. Use Batch Operations

```rust
use uuid::Uuid;

// Store 100 results in one roundtrip
let results: Vec<(Uuid, TaskMeta)> = /* ... */;
backend.batch_store_results(&results).await?;

// Get 100 results in one roundtrip
let task_ids: Vec<Uuid> = /* ... */;
let results = backend.batch_get_results(&task_ids).await?;
```

**Impact:** Up to 100x faster for bulk operations

### 4. Set Appropriate TTLs

```rust
use celers_backend_redis::ttl;

// Use predefined constants
backend.set_expiration(task_id, ttl::SHORT).await?;     // 1 hour
backend.set_expiration(task_id, ttl::MEDIUM).await?;    // 6 hours
backend.set_expiration(task_id, ttl::LONG).await?;      // 1 day
backend.set_expiration(task_id, ttl::VERY_LONG).await?; // 7 days

// Cleanup old results periodically
backend.cleanup_old_results(ttl::LONG).await?;
```

**Impact:** Prevents memory bloat, improves cache locality

### 5. Use Multiplexed Connections (Built-in)

```rust
// Already enabled by default!
let backend = RedisResultBackend::new("redis://localhost:6379")?;
// Automatically uses multiplexed async connections
```

**Impact:** Efficient connection reuse without manual pooling

## Configuration Tuning

### Compression Configuration

Compression trades CPU for network bandwidth and memory. Tune based on your bottleneck:

```rust
use celers_backend_redis::batch_size;

// Network-bound? Use aggressive compression
let backend = RedisResultBackend::new("redis://localhost:6379")?
    .with_compression(batch_size::TINY, 9);  // Compress >100B at max level

// CPU-bound? Use light compression
let backend = RedisResultBackend::new("redis://localhost:6379")?
    .with_compression(batch_size::MEDIUM, 1);  // Compress >10KB at min level

// Balanced (recommended for most workloads)
let backend = RedisResultBackend::new("redis://localhost:6379")?
    .with_compression(batch_size::SMALL, 6);  // Compress >1KB at level 6
```

**Compression Levels:**
- Level 1: Fastest compression, lower ratio (~0.6-0.7)
- Level 6: Balanced (default, recommended)
- Level 9: Best compression, slower (~0.3-0.5)

**Threshold Guidelines:**
- `batch_size::TINY` (100B): For tiny payloads with high compression ratio
- `batch_size::SMALL` (1KB): Recommended default
- `batch_size::MEDIUM` (10KB): For larger results
- `batch_size::LARGE` (100KB): Only compress very large payloads
- `batch_size::VERY_LARGE` (1MB): Rare, only for huge results

### Cache Configuration

Cache configuration depends on your access patterns:

```rust
use celers_backend_redis::ttl;

// High read/write ratio? Large cache with long TTL
let backend = RedisResultBackend::new("redis://localhost:6379")?
    .with_cache(10000, ttl::LONG);  // 10K results, 1 day TTL

// Low read/write ratio? Small cache with short TTL
let backend = RedisResultBackend::new("redis://localhost:6379")?
    .with_cache(100, ttl::SHORT);  // 100 results, 1 hour TTL

// Balanced (recommended)
let backend = RedisResultBackend::new("redis://localhost:6379")?
    .with_cache(1000, ttl::MEDIUM);  // 1K results, 6 hours TTL
```

**Cache Size Guidelines:**
- 100-500: Low-traffic applications
- 1000-5000: Medium-traffic applications (recommended)
- 10000+: High-traffic applications with hot results

**TTL Guidelines:**
- Short (1h): Frequently changing results
- Medium (6h): Balanced (recommended)
- Long (1d): Stable results with high reuse

### Encryption Configuration

Encryption adds overhead. Only use when necessary:

```rust
use celers_backend_redis::EncryptionKey;

// Only encrypt sensitive results
let key = EncryptionKey::generate();
let backend = RedisResultBackend::new("redis://localhost:6379")?
    .with_encryption(key);

// Overhead: ~100-200μs per operation
// Use only for PII, credentials, or sensitive data
```

**When to encrypt:**
- Personal Identifiable Information (PII)
- Credentials, API keys, tokens
- Financial data, payment information
- Regulated data (HIPAA, GDPR)

**When NOT to encrypt:**
- Public data
- Non-sensitive aggregates
- Internal task coordination data

## Compression Strategies

### Result Size Analysis

Measure your typical result sizes to choose the right threshold:

```rust
use celers_backend_redis::RedisResultBackend;

async fn analyze_result_sizes(backend: &RedisResultBackend) {
    let metrics = backend.get_metrics();

    println!("Average original size: {} bytes", metrics.data_original_bytes / metrics.store_count);
    println!("Average stored size: {} bytes", metrics.data_stored_bytes / metrics.store_count);
    println!("Compression ratio: {:.2}", metrics.compression_ratio);

    // Adjust threshold based on these metrics
}
```

### Compression Performance

| Result Size | No Compression | Level 1 | Level 6 | Level 9 |
|-------------|----------------|---------|---------|---------|
| 100B        | 50μs           | 60μs    | 70μs    | 90μs    |
| 1KB         | 100μs          | 120μs   | 150μs   | 200μs   |
| 10KB        | 500μs          | 600μs   | 800μs   | 1200μs  |
| 100KB       | 3ms            | 4ms     | 6ms     | 10ms    |
| 1MB         | 25ms           | 30ms    | 50ms    | 90ms    |

*Benchmarks on typical hardware with Redis on localhost*

### Adaptive Compression

For variable result sizes, use dynamic thresholds:

```rust
use celers_backend_redis::{RedisResultBackend, batch_size};

async fn store_with_adaptive_compression(
    backend: &mut RedisResultBackend,
    task_id: Uuid,
    meta: &TaskMeta,
) -> Result<(), BackendError> {
    let size = serde_json::to_vec(meta)?.len();

    // Dynamically adjust compression based on size
    if size > batch_size::MEDIUM as usize {
        // Large results: use compression
        let backend_with_compression = backend
            .clone()
            .with_compression(batch_size::MEDIUM, 6);
        backend_with_compression.store_result(task_id, meta).await?;
    } else {
        // Small results: skip compression overhead
        backend.store_result(task_id, meta).await?;
    }

    Ok(())
}
```

## Caching Strategies

### Cache Hit Rate Optimization

Monitor and optimize cache hit rates:

```rust
use celers_backend_redis::RedisResultBackend;

async fn monitor_cache_performance(backend: &RedisResultBackend) {
    let metrics = backend.get_metrics();

    let hit_rate = metrics.cache_hit_rate();
    println!("Cache hit rate: {:.1}%", hit_rate * 100.0);

    if hit_rate < 0.5 {
        println!("WARNING: Low cache hit rate. Consider:");
        println!("  - Increasing cache size");
        println!("  - Increasing cache TTL");
        println!("  - Analyzing access patterns");
    }
}
```

**Target hit rates:**
- 70-90%: Excellent
- 50-70%: Good
- 30-50%: Fair (tune cache settings)
- <30%: Poor (rethink caching strategy)

### Cache Invalidation

Proactively invalidate stale entries:

```rust
// Manual cache invalidation
backend.delete_result(task_id).await?;  // Removes from cache and Redis

// Automatic cleanup runs on cache operations
// No manual intervention needed
```

### Read-Heavy Workloads

For read-heavy workloads, maximize cache size:

```rust
use celers_backend_redis::ttl;

// Large cache for read-heavy workloads
let backend = RedisResultBackend::new("redis://localhost:6379")?
    .with_cache(50000, ttl::VERY_LONG)  // 50K results, 7 days TTL
    .with_compression(batch_size::SMALL, 6);  // Compress to fit more in Redis

// Typical read pattern: 95% cache hits
```

### Write-Heavy Workloads

For write-heavy workloads, use a smaller cache:

```rust
use celers_backend_redis::ttl;

// Small cache for write-heavy workloads
let backend = RedisResultBackend::new("redis://localhost:6379")?
    .with_cache(500, ttl::SHORT);  // 500 results, 1 hour TTL

// Focus on fast writes, not cache hits
```

## Batch Operations

### Batch Size Recommendations

Use the `batch_size` module for common patterns:

```rust
use celers_backend_redis::batch_size;

// Process results in batches
let task_ids: Vec<Uuid> = get_all_task_ids();

for chunk in task_ids.chunks(batch_size::SMALL as usize) {
    let results = backend.batch_get_results(chunk).await?;
    process_results(results).await?;
}
```

**Batch size guidelines:**
- `batch_size::TINY` (10): Minimal batching
- `batch_size::SMALL` (100): Recommended default
- `batch_size::MEDIUM` (500): Large batches
- `batch_size::LARGE` (1000): Very large batches
- `batch_size::VERY_LARGE` (5000): Maximum batching

### Pipelined Operations

Batch operations use Redis pipelining automatically:

```rust
// Single roundtrip for 100 operations
let results: Vec<(Uuid, TaskMeta)> = /* 100 results */;
backend.batch_store_results(&results).await?;

// vs. 100 roundtrips
for (task_id, meta) in results {
    backend.store_result(task_id, &meta).await?;  // Slow!
}
```

**Performance comparison:**

| Batch Size | Individual Ops | Batch Ops | Speedup |
|------------|----------------|-----------|---------|
| 10         | 10ms           | 1ms       | 10x     |
| 100        | 100ms          | 5ms       | 20x     |
| 500        | 500ms          | 15ms      | 33x     |
| 1000       | 1000ms         | 25ms      | 40x     |

### Stream Processing

For very large result sets, use streaming:

```rust
use futures_util::StreamExt;
use celers_backend_redis::batch_size;

let mut stream = backend.stream_results(&task_ids, batch_size::SMALL as usize);

while let Some(result) = stream.next().await {
    match result {
        Ok(meta) => process_result(meta).await?,
        Err(e) => eprintln!("Error: {}", e),
    }
}

// Memory usage: O(batch_size) instead of O(task_ids.len())
```

## TTL Management

### TTL Best Practices

Use predefined TTL constants for consistency:

```rust
use celers_backend_redis::ttl;

// Short-lived temporary results
backend.set_expiration(task_id, ttl::IMMEDIATE).await?;  // 5 minutes

// Standard results
backend.set_expiration(task_id, ttl::SHORT).await?;      // 1 hour
backend.set_expiration(task_id, ttl::MEDIUM).await?;     // 6 hours
backend.set_expiration(task_id, ttl::LONG).await?;       // 1 day

// Long-term storage
backend.set_expiration(task_id, ttl::VERY_LONG).await?;  // 7 days
backend.set_expiration(task_id, ttl::PERMANENT).await?;  // 30 days
```

### Smart TTL Strategy

Set TTLs based on task result state:

```rust
use celers_backend_redis::{TaskResult, ttl};

async fn set_smart_ttl(
    backend: &mut RedisResultBackend,
    task_id: Uuid,
    meta: &TaskMeta,
) -> Result<(), BackendError> {
    let ttl = match &meta.result {
        // Successful results: medium retention
        TaskResult::Success(_) => ttl::LONG,

        // Failures: keep longer for debugging
        TaskResult::Failure(_) => ttl::VERY_LONG,

        // Temporary states: short TTL
        TaskResult::Pending | TaskResult::Started => ttl::SHORT,

        // Retries: medium TTL
        TaskResult::Retry(_) => ttl::MEDIUM,

        // Revoked: immediate cleanup
        TaskResult::Revoked => ttl::IMMEDIATE,
    };

    backend.set_expiration(task_id, ttl).await?;
    Ok(())
}
```

### Periodic Cleanup

Clean up old results to prevent memory bloat:

```rust
use celers_backend_redis::ttl;
use tokio::time::{interval, Duration};

async fn periodic_cleanup(backend: &mut RedisResultBackend) {
    let mut ticker = interval(Duration::from_secs(3600));  // Every hour

    loop {
        ticker.tick().await;

        // Remove results older than 7 days
        match backend.cleanup_old_results(ttl::VERY_LONG).await {
            Ok(count) => println!("Cleaned up {} old results", count),
            Err(e) => eprintln!("Cleanup error: {}", e),
        }

        // Clean up completed chords
        match backend.cleanup_completed_chords(ttl::LONG).await {
            Ok(count) => println!("Cleaned up {} completed chords", count),
            Err(e) => eprintln!("Chord cleanup error: {}", e),
        }
    }
}
```

## Redis Configuration

### Redis Server Settings

Optimize Redis configuration for backend workloads:

```conf
# redis.conf

# Memory
maxmemory 4gb
maxmemory-policy allkeys-lru  # Evict least recently used keys

# Persistence (for chord state durability)
appendonly yes
appendfsync everysec

# Network
tcp-backlog 511
timeout 0
tcp-keepalive 300

# Performance
save ""  # Disable RDB snapshots (use AOF only)
```

### Memory Management

Monitor Redis memory usage:

```rust
async fn check_redis_memory(backend: &RedisResultBackend) -> Result<(), BackendError> {
    let stats = backend.get_stats().await?;

    println!("Total keys: {}", stats.total_keys);
    println!("Memory used: {} MB", stats.memory_used_bytes / 1_048_576);

    if stats.memory_used_bytes > 3_000_000_000 {  // 3GB
        println!("WARNING: High memory usage. Consider:");
        println!("  - Reducing TTLs");
        println!("  - Enabling compression");
        println!("  - Running cleanup");
    }

    Ok(())
}
```

### Redis Persistence

Choose persistence strategy based on requirements:

**AOF (Append-Only File):**
- Pros: Durable, minimal data loss
- Cons: Larger disk usage, slower restarts
- Use for: Production with chord state

**RDB (Snapshots):**
- Pros: Compact, fast restarts
- Cons: Potential data loss between snapshots
- Use for: Development, non-critical workloads

**Both:**
- Pros: Best of both worlds
- Cons: Highest disk usage
- Use for: Critical production systems

## Network Optimization

### Co-location

Deploy Redis close to workers:

```
Same Datacenter: 0.5-2ms latency
Same Region: 2-10ms latency
Cross-Region: 50-200ms latency
```

**Impact:** 100x latency reduction (same datacenter vs. cross-region)

### Connection Pooling

Multiplexed connections are automatic:

```rust
// Already optimized!
let backend = RedisResultBackend::new("redis://localhost:6379")?;

// Shares single connection across all async tasks
// No manual pooling needed
```

### Redis Cluster

For horizontal scaling, use Redis Cluster:

```rust
// Connect to Redis Cluster
let backend = RedisResultBackend::new(
    "redis://node1:6379,redis://node2:6379,redis://node3:6379"
)?;

// Automatic key distribution across nodes
```

## Monitoring and Metrics

### Built-in Metrics

Track performance with built-in metrics:

```rust
use celers_backend_redis::RedisResultBackend;

async fn monitor_performance(backend: &RedisResultBackend) {
    let metrics = backend.get_metrics();

    println!("=== Operation Counts ===");
    println!("Store: {}", metrics.store_count);
    println!("Get: {}", metrics.get_count);
    println!("Delete: {}", metrics.delete_count);

    println!("\n=== Latencies (avg) ===");
    println!("Store: {:.2}ms", metrics.avg_store_latency_ms());
    println!("Get: {:.2}ms", metrics.avg_get_latency_ms());
    println!("Delete: {:.2}ms", metrics.avg_delete_latency_ms());

    println!("\n=== Compression ===");
    println!("Ratio: {:.2}", metrics.compression_ratio);

    println!("\n=== Cache ===");
    println!("Hit rate: {:.1}%", metrics.cache_hit_rate() * 100.0);
    println!("Hits: {}", metrics.cache_hits);
    println!("Misses: {}", metrics.cache_misses);

    println!("\n=== Errors ===");
    for (category, count) in &metrics.error_counts {
        println!("{}: {}", category, count);
    }
}
```

### Health Checks

Implement health checks for monitoring:

```rust
use tokio::time::Duration;

async fn health_check_loop(backend: &RedisResultBackend) {
    loop {
        match backend.health_check().await {
            Ok(true) => println!("✓ Redis backend healthy"),
            Ok(false) => eprintln!("✗ Redis backend unhealthy"),
            Err(e) => eprintln!("✗ Health check failed: {}", e),
        }

        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}
```

### Performance Alerts

Set up alerts for performance degradation:

```rust
async fn check_performance_alerts(backend: &RedisResultBackend) {
    let metrics = backend.get_metrics();

    // Alert: High latency
    if metrics.avg_get_latency_ms() > 10.0 {
        alert!("High GET latency: {:.2}ms", metrics.avg_get_latency_ms());
    }

    // Alert: Low cache hit rate
    if metrics.cache_hit_rate() < 0.5 {
        alert!("Low cache hit rate: {:.1}%", metrics.cache_hit_rate() * 100.0);
    }

    // Alert: High error rate
    let total_ops = metrics.store_count + metrics.get_count + metrics.delete_count;
    let total_errors: u64 = metrics.error_counts.values().sum();
    let error_rate = total_errors as f64 / total_ops as f64;

    if error_rate > 0.01 {  // >1% error rate
        alert!("High error rate: {:.2}%", error_rate * 100.0);
    }
}
```

## Benchmarking

### Run Benchmarks

The crate includes comprehensive benchmarks:

```bash
# Run all benchmarks
cargo bench --bench backend_bench

# Run specific benchmarks
cargo bench --bench backend_bench -- store
cargo bench --bench backend_bench -- batch
cargo bench --bench backend_bench -- compression
```

### Custom Benchmarks

Create custom benchmarks for your workload:

```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use celers_backend_redis::RedisResultBackend;

fn benchmark_my_workload(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut backend = RedisResultBackend::new("redis://localhost:6379").unwrap();

    c.bench_function("my_workload", |b| {
        b.iter(|| {
            rt.block_on(async {
                // Your workload here
                let task_id = black_box(Uuid::new_v4());
                let meta = black_box(TaskMeta::new(task_id, "task".to_string()));
                backend.store_result(task_id, &meta).await.unwrap();
            });
        });
    });
}

criterion_group!(benches, benchmark_my_workload);
criterion_main!(benches);
```

### Load Testing

Simulate production load:

```rust
use tokio::task::JoinSet;
use uuid::Uuid;

async fn load_test(
    backend: &RedisResultBackend,
    num_tasks: usize,
    concurrency: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = std::time::Instant::now();
    let mut set = JoinSet::new();

    for i in 0..num_tasks {
        if set.len() >= concurrency {
            set.join_next().await;
        }

        let mut backend = backend.clone();
        set.spawn(async move {
            let task_id = Uuid::new_v4();
            let meta = TaskMeta::new(task_id, format!("task_{}", i));
            backend.store_result(task_id, &meta).await
        });
    }

    while set.join_next().await.is_some() {}

    let duration = start.elapsed();
    let throughput = num_tasks as f64 / duration.as_secs_f64();

    println!("Completed {} tasks in {:.2}s", num_tasks, duration.as_secs_f64());
    println!("Throughput: {:.0} ops/sec", throughput);

    Ok(())
}

// Example: 10,000 tasks with 100 concurrent operations
// load_test(&backend, 10_000, 100).await?;
```

## Summary

### Quick Reference

| Optimization | Impact | Complexity |
|--------------|--------|------------|
| Enable caching | High (10-100x) | Low |
| Use batch operations | High (10-100x) | Low |
| Enable compression | Medium (40-60% reduction) | Low |
| Set appropriate TTLs | Medium | Low |
| Co-locate Redis | High (100x latency) | Medium |
| Use Redis Cluster | High (horizontal scaling) | High |
| Enable encryption | Low (-10-20% perf) | Low |

### Recommended Configuration

For most production workloads:

```rust
use celers_backend_redis::{RedisResultBackend, ttl, batch_size};

let backend = RedisResultBackend::new("redis://localhost:6379")?
    .with_cache(5000, ttl::MEDIUM)              // 5K results, 6h TTL
    .with_compression(batch_size::SMALL, 6);    // Compress >1KB at level 6

// Set TTLs on all results
backend.store_result(task_id, &meta).await?;
backend.set_expiration(task_id, ttl::LONG).await?;

// Use batch operations for bulk
backend.batch_store_results(&results).await?;

// Periodic cleanup
backend.cleanup_old_results(ttl::VERY_LONG).await?;
```

This configuration provides:
- 70-90% cache hit rate
- 40-60% compression ratio
- 10-100x batch operation speedup
- Automatic memory management via TTLs
