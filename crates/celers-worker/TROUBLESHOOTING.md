# CeleRS Worker Troubleshooting Guide

This guide helps diagnose and resolve common issues with the CeleRS worker runtime.

## Table of Contents

- [Performance Issues](#performance-issues)
- [Memory Problems](#memory-problems)
- [Connection Issues](#connection-issues)
- [Task Execution Problems](#task-execution-problems)
- [Circuit Breaker Issues](#circuit-breaker-issues)
- [Rate Limiting Problems](#rate-limiting-problems)
- [Dead Letter Queue Issues](#dead-letter-queue-issues)
- [Worker Coordination Problems](#worker-coordination-problems)
- [Health Check Failures](#health-check-failures)
- [Monitoring and Debugging](#monitoring-and-debugging)

---

## Performance Issues

### Problem: Low Task Throughput

**Symptoms:**
- Tasks are processed slowly
- Worker utilization is low
- Queue depth is growing

**Diagnosis:**
```rust
use celers_worker::performance_metrics::PerformanceMetrics;

let metrics = PerformanceMetrics::new();
let stats = metrics.get_stats().await;

println!("Throughput: {:.2} tasks/sec", stats.throughput);
println!("Utilization: {:.1}%", stats.utilization);
println!("Avg latency: {:.2}ms", stats.avg_latency);
```

**Solutions:**

1. **Increase Concurrency:**
   ```rust
   let config = WorkerConfig {
       concurrency: 20,  // Increase from default
       ..Default::default()
   };
   ```

2. **Enable Batch Dequeue:**
   ```rust
   let config = WorkerConfig {
       enable_batch_dequeue: true,
       batch_size: 10,  // Fetch multiple tasks at once
       ..Default::default()
   };
   ```

3. **Enable Task Prefetching:**
   ```rust
   use celers_worker::prefetch::PrefetchConfig;

   let prefetch_config = PrefetchConfig {
       enabled: true,
       prefetch_count: 5,
       adaptive: true,  // Auto-adjust based on processing speed
       ..Default::default()
   };
   ```

4. **Use Lock-Free Queue:**
   ```rust
   use celers_worker::lockfree_queue::LockFreeQueue;

   let queue = LockFreeQueue::new();
   // Reduces contention in high-concurrency scenarios
   ```

### Problem: High Task Latency

**Symptoms:**
- P95/P99 latencies are high
- Tasks take longer than expected

**Diagnosis:**
```rust
let stats = metrics.get_stats().await;
println!("P50: {:.2}ms", stats.p50_latency);
println!("P95: {:.2}ms", stats.p95_latency);
println!("P99: {:.2}ms", stats.p99_latency);
```

**Solutions:**

1. **Check for Resource Contention:**
   ```rust
   use celers_worker::resource_tracker::ResourceTracker;

   let tracker = ResourceTracker::new(config);
   let status = tracker.check_resources().await;

   if status.cpu_usage > 80.0 {
       println!("High CPU usage: {:.1}%", status.cpu_usage);
   }
   ```

2. **Use CPU Affinity:**
   ```rust
   use celers_worker::cpu_affinity::{CpuAffinityConfig, AffinityStrategy};

   let affinity_config = CpuAffinityConfig {
       enabled: true,
       strategy: AffinityStrategy::RoundRobin,
       ..Default::default()
   };
   ```

3. **Enable Task Pipelining:**
   ```rust
   use celers_worker::pipeline::{PipelineConfig, PipelineStage};

   let pipeline = PipelineConfig::new()
       .add_stage(PipelineStage::Fetch)
       .add_stage(PipelineStage::Deserialize)
       .add_stage(PipelineStage::Execute)
       .add_stage(PipelineStage::Serialize);
   ```

---

## Memory Problems

### Problem: High Memory Usage

**Symptoms:**
- Worker memory keeps growing
- OOM errors occur
- System swap is being used

**Diagnosis:**
```rust
use celers_worker::resource_tracker::ResourceTracker;

let tracker = ResourceTracker::new(config);
let status = tracker.check_resources().await;

println!("Memory usage: {} MB", status.memory_usage_bytes / 1024 / 1024);
println!("Active tasks: {}", status.active_tasks);
```

**Solutions:**

1. **Enable Memory Limits:**
   ```rust
   use celers_worker::resource_tracker::ResourceConfig;

   let config = ResourceConfig {
       memory_limit_mb: 512,  // Limit to 512 MB
       memory_warning_threshold_mb: 400,
       ..Default::default()
   };
   ```

2. **Enable Result Streaming:**
   ```rust
   use celers_worker::streaming::{StreamConfig, ResultStreamer};

   let stream_config = StreamConfig {
       enabled: true,
       chunk_size_bytes: 64 * 1024,  // 64 KB chunks
       max_buffer_size_mb: 10,
       ..Default::default()
   };
   ```

3. **Use Memory Pooling:**
   ```rust
   use celers_worker::memory_pool::{MemoryPool, PoolConfig};

   let pool_config = PoolConfig {
       block_size: 4096,
       initial_blocks: 100,
       max_blocks: 1000,
   };
   let pool = MemoryPool::new(pool_config);
   ```

4. **Enable GC Tuning:**
   ```rust
   use celers_worker::gc_tuning::{GcTuner, GcConfig};

   let gc_config = GcConfig::aggressive();  // More frequent GC
   let tuner = GcTuner::new(gc_config);

   if tuner.should_gc().await {
       // Trigger manual GC or reduce load
       println!("High memory pressure detected");
   }
   ```

### Problem: Memory Leaks

**Symptoms:**
- Memory usage grows over time without releasing
- Memory doesn't stabilize after task completion

**Diagnosis:**
```rust
use celers_worker::leak_detection::{LeakDetector, LeakConfig};

let leak_config = LeakConfig::default();
let detector = LeakDetector::new(leak_config);

// Start tracking
detector.start_tracking("task-123").await;

// After task completes
let leak_info = detector.check_task("task-123").await?;
if leak_info.is_leak_suspected {
    println!("Potential leak: {} bytes over {} samples",
             leak_info.current_usage,
             leak_info.sample_count);
}
```

**Solutions:**

1. **Enable Leak Detection:**
   ```rust
   let leak_config = LeakConfig {
       enabled: true,
       check_interval_secs: 30,
       leak_threshold_mb: 50,
       growth_rate_threshold: 1.5,  // 50% growth
   };
   ```

2. **Use Checkpoint/Resume:**
   ```rust
   use celers_worker::checkpoint::{CheckpointManager, CheckpointConfig};

   // Release memory periodically by checkpointing
   let checkpoint_config = CheckpointConfig {
       strategy: CheckpointStrategy::Interval(Duration::from_secs(60)),
       cleanup_old: true,
       ..Default::default()
   };
   ```

---

## Connection Issues

### Problem: Broker Connection Failures

**Symptoms:**
- Workers can't connect to broker
- "Connection refused" or timeout errors
- Intermittent connection drops

**Diagnosis:**
```bash
# Check if broker is accessible
redis-cli -h 127.0.0.1 -p 6379 PING
# or
psql -h localhost -U user -d celery -c "SELECT 1;"
```

**Solutions:**

1. **Increase Connection Timeout:**
   ```rust
   let config = WorkerConfig {
       connection_timeout_secs: 30,  // Increase from default
       ..Default::default()
   };
   ```

2. **Enable Connection Retry:**
   ```rust
   use celers_worker::retry::{RetryConfig, RetryStrategy};

   let retry_config = RetryConfig {
       max_retries: 5,
       strategy: RetryStrategy::ExponentialBackoff {
           initial_delay_ms: 1000,
           max_delay_ms: 30000,
           multiplier: 2.0,
       },
       ..Default::default()
   };
   ```

3. **Check Network Configuration:**
   ```bash
   # Verify network connectivity
   telnet localhost 6379

   # Check firewall rules
   sudo iptables -L -n | grep 6379
   ```

### Problem: Connection Pool Exhaustion

**Symptoms:**
- "No available connections" errors
- High connection acquisition time
- Workers waiting for connections

**Diagnosis:**
```rust
// Check pool statistics (if using Redis)
#[cfg(feature = "metrics")]
{
    use celers_metrics::{REDIS_CONNECTIONS_ACTIVE, REDIS_CONNECTION_ACQUIRE_TIME};

    println!("Active connections: {}", REDIS_CONNECTIONS_ACTIVE.get());
    println!("Avg acquire time: {:.2}ms",
             REDIS_CONNECTION_ACQUIRE_TIME.get_sample_sum() /
             REDIS_CONNECTION_ACQUIRE_TIME.get_sample_count() as f64);
}
```

**Solutions:**

1. **Increase Pool Size:**
   ```rust
   // For Redis backend
   let pool_config = redis::PoolConfig {
       max_size: 50,  // Increase from default
       min_idle: 10,
       ..Default::default()
   };
   ```

2. **Enable Connection Reuse:**
   ```rust
   let config = WorkerConfig {
       connection_pool_enabled: true,
       connection_pool_size: 20,
       connection_idle_timeout_secs: 300,
       ..Default::default()
   };
   ```

---

## Task Execution Problems

### Problem: Tasks Failing Repeatedly

**Symptoms:**
- High task failure rate
- Tasks retrying but still failing
- DLQ filling up

**Diagnosis:**
```rust
use celers_worker::error_aggregation::ErrorAggregator;

let aggregator = ErrorAggregator::new(config);
let stats = aggregator.get_statistics().await;

println!("Total errors: {}", stats.total_errors);
println!("Error rate: {:.2}%", stats.error_rate * 100.0);

// Get top errors
let top_errors = aggregator.get_top_errors(5).await;
for (error_type, count) in top_errors {
    println!("  {}: {} occurrences", error_type, count);
}
```

**Solutions:**

1. **Adjust Retry Strategy:**
   ```rust
   let retry_config = RetryConfig {
       max_retries: 3,
       strategy: RetryStrategy::ExponentialBackoff {
           initial_delay_ms: 1000,
           max_delay_ms: 60000,
           multiplier: 2.0,
       },
       jitter: true,  // Add randomness to avoid thundering herd
   };
   ```

2. **Enable Circuit Breaker:**
   ```rust
   use celers_worker::circuit_breaker::{CircuitBreakerConfig, CircuitBreaker};

   let cb_config = CircuitBreakerConfig {
       failure_threshold: 5,
       recovery_timeout_secs: 60,
       success_threshold: 2,
       time_window_secs: 300,
   };
   ```

3. **Check Error Patterns:**
   ```rust
   let patterns = aggregator.detect_patterns().await;
   for pattern in patterns {
       println!("Pattern: {} (frequency: {:.1}%)",
                pattern.description,
                pattern.frequency * 100.0);
   }
   ```

### Problem: Tasks Timing Out

**Symptoms:**
- Tasks exceed time limits
- Timeout errors in logs
- Incomplete task results

**Diagnosis:**
```rust
use celers_worker::task_timeout::TimeoutManager;

let timeout_mgr = TimeoutManager::new(config);
let stats = timeout_mgr.get_statistics().await;

println!("Timeouts: {}", stats.timeout_count);
println!("Avg task duration: {:.2}s", stats.avg_duration_secs);
```

**Solutions:**

1. **Increase Timeout:**
   ```rust
   let config = WorkerConfig {
       default_timeout_secs: 300,  // 5 minutes
       ..Default::default()
   };
   ```

2. **Use Checkpoint/Resume:**
   ```rust
   use celers_worker::checkpoint::{CheckpointManager, CheckpointConfig};

   // Allow long-running tasks to checkpoint and resume
   let checkpoint_mgr = CheckpointManager::new(CheckpointConfig {
       strategy: CheckpointStrategy::Interval(Duration::from_secs(60)),
       ..Default::default()
   });
   ```

3. **Enable Graceful Timeout:**
   ```rust
   use celers_worker::task_timeout::TimeoutConfig;

   let timeout_config = TimeoutConfig {
       default_timeout: Duration::from_secs(180),
       grace_period: Duration::from_secs(30),  // Time to cleanup
       force_kill: true,
       ..Default::default()
   };
   ```

---

## Circuit Breaker Issues

### Problem: Circuit Breaker Opens Too Frequently

**Symptoms:**
- Circuit breaker constantly opening
- Tasks rejected due to open circuit
- Frequent state transitions

**Diagnosis:**
```rust
use celers_worker::circuit_breaker::CircuitBreaker;

let cb = CircuitBreaker::new(config);
let state = cb.get_state("task_type").await;

println!("Circuit state: {:?}", state);
println!("Failure count: {}", cb.get_failure_count("task_type").await);
```

**Solutions:**

1. **Adjust Threshold:**
   ```rust
   let cb_config = CircuitBreakerConfig {
       failure_threshold: 10,  // Increase threshold
       time_window_secs: 600,  // Longer window
       recovery_timeout_secs: 120,  // Longer recovery time
       ..Default::default()
   };
   ```

2. **Use Lenient Configuration:**
   ```rust
   let cb_config = CircuitBreakerConfig::lenient();
   ```

### Problem: Circuit Breaker Not Opening

**Symptoms:**
- Failures continue without protection
- No circuit breaker activation
- System overload

**Solutions:**

1. **Lower Threshold:**
   ```rust
   let cb_config = CircuitBreakerConfig {
       failure_threshold: 3,  // More sensitive
       time_window_secs: 60,  // Shorter window
       ..Default::default()
   };
   ```

2. **Verify Configuration:**
   ```rust
   let config = WorkerConfig {
       enable_circuit_breaker: true,  // Ensure it's enabled
       circuit_breaker_config: Some(cb_config),
       ..Default::default()
   };
   ```

---

## Rate Limiting Problems

### Problem: Too Many Tasks Rejected

**Symptoms:**
- High rate limit rejection rate
- Legitimate tasks being dropped
- Queue backlog growing

**Diagnosis:**
```rust
use celers_worker::rate_limit::RateLimiter;

let rate_limiter = RateLimiter::new();
let stats = rate_limiter.get_statistics("task_type").await?;

println!("Requests: {}", stats.total_requests);
println!("Allowed: {}", stats.allowed_requests);
println!("Rejected: {}", stats.rejected_requests);
println!("Rejection rate: {:.1}%",
         (stats.rejected_requests as f64 / stats.total_requests as f64) * 100.0);
```

**Solutions:**

1. **Increase Rate Limits:**
   ```rust
   use celers_worker::rate_limit::RateLimitConfig;

   let rate_config = RateLimitConfig {
       requests_per_second: 100.0,  // Increase limit
       burst_size: 150,  // Allow bursts
       ..Default::default()
   };
   ```

2. **Use Lenient Preset:**
   ```rust
   let rate_config = RateLimitConfig::lenient();
   ```

3. **Enable Distributed Rate Limiting:**
   ```rust
   use celers_worker::distributed_rate_limit::{DistributedRateLimiter, RedisRateLimiter};

   // Share limits across workers
   let limiter = RedisRateLimiter::new(redis_url, "task_type", rate_config).await?;
   ```

---

## Dead Letter Queue Issues

### Problem: DLQ Filling Up

**Symptoms:**
- DLQ size growing continuously
- Storage space concerns
- Unable to process failed tasks

**Diagnosis:**
```rust
use celers_worker::dlq::DlqHandler;

let dlq = DlqHandler::new(config);
let stats = dlq.get_statistics().await;

println!("DLQ size: {}", stats.total_entries);
println!("Oldest entry: {:?}", stats.oldest_entry_age);

// Get entries by task type
let entries = dlq.get_entries_by_task_name("failing_task").await?;
println!("Failed 'failing_task' count: {}", entries.len());
```

**Solutions:**

1. **Enable TTL Cleanup:**
   ```rust
   use celers_worker::dlq::DlqConfig;

   let dlq_config = DlqConfig {
       enabled: true,
       max_entries: 10000,
       entry_ttl_secs: 86400 * 7,  // 7 days
       auto_cleanup: true,
       ..Default::default()
   };
   ```

2. **Enable Reprocessing:**
   ```rust
   use celers_worker::dlq::{DlqReprocessor, DlqReprocessConfig};

   let reprocess_config = DlqReprocessConfig {
       enabled: true,
       max_retry_attempts: 3,
       retry_delay_secs: 3600,  // 1 hour
       ..Default::default()
   };

   let reprocessor = DlqReprocessor::new(dlq.clone(), reprocess_config);
   reprocessor.start_background_reprocessing().await;
   ```

3. **Export and Archive:**
   ```rust
   // Export to JSON for analysis
   let entries = dlq.export_to_json().await?;
   let export_path = std::env::temp_dir().join("dlq_export.json");
   std::fs::write(&export_path, entries)?;

   // Clear old entries after export
   dlq.cleanup_old_entries(Duration::from_secs(86400 * 30)).await?;
   ```

---

## Worker Coordination Problems

### Problem: Leader Election Failures

**Symptoms:**
- Multiple workers think they're leader
- No leader elected
- Singleton tasks not executing

**Diagnosis:**
```rust
use celers_worker::worker_coordination::{WorkerCoordinator, CoordinatorConfig};

let coordinator = WorkerCoordinator::new(config).await?;
let is_leader = coordinator.is_leader("singleton_task").await?;

println!("Am I leader? {}", is_leader);

// List all workers
let workers = coordinator.get_workers().await?;
println!("Active workers: {}", workers.len());
```

**Solutions:**

1. **Adjust TTLs:**
   ```rust
   let coord_config = CoordinatorConfig {
       leader_ttl_secs: 120,  // Longer TTL
       heartbeat_interval_secs: 30,
       ..Default::default()
   };
   ```

2. **Ensure Heartbeats:**
   ```rust
   // Start heartbeat task
   let coordinator_clone = coordinator.clone();
   tokio::spawn(async move {
       let mut interval = tokio::time::interval(
           Duration::from_secs(coord_config.heartbeat_interval_secs)
       );
       loop {
           interval.tick().await;
           if let Err(e) = coordinator_clone.heartbeat().await {
               eprintln!("Heartbeat failed: {}", e);
           }
       }
   });
   ```

### Problem: Distributed Lock Contention

**Symptoms:**
- High lock acquisition failures
- Tasks waiting for locks
- Deadlocks

**Solutions:**

1. **Adjust Lock TTL:**
   ```rust
   let coord_config = CoordinatorConfig {
       lock_ttl_secs: 30,  // Shorter TTL
       ..Default::default()
   };
   ```

2. **Implement Lock Retry:**
   ```rust
   async fn acquire_with_retry(
       coordinator: &WorkerCoordinator,
       lock_name: &str,
       max_attempts: u32,
   ) -> Result<bool> {
       for attempt in 0..max_attempts {
           if coordinator.acquire_lock(lock_name, 30).await? {
               return Ok(true);
           }
           tokio::time::sleep(Duration::from_millis(100 * (attempt + 1))).await;
       }
       Ok(false)
   }
   ```

---

## Health Check Failures

### Problem: Readiness Probe Failing

**Symptoms:**
- Kubernetes not routing traffic to worker
- Health check endpoint returning unhealthy
- Worker marked as not ready

**Diagnosis:**
```rust
use celers_worker::health::{HealthChecker, HealthStatus};

let health_checker = HealthChecker::new();
let info = health_checker.get_health_info().await;

println!("Status: {:?}", info.status);
println!("Can accept traffic: {}", info.can_accept_traffic());
println!("Uptime: {}s", info.uptime_secs);
println!("Tasks completed: {}", info.tasks_completed);
println!("Tasks failed: {}", info.tasks_failed);
```

**Solutions:**

1. **Adjust Health Thresholds:**
   ```rust
   use celers_worker::health::HealthConfig;

   let health_config = HealthConfig {
       consecutive_failure_threshold: 10,  // More lenient
       min_success_rate: 0.8,  // 80% success rate
       ..Default::default()
   };
   ```

2. **Check Recent Activity:**
   ```rust
   if !info.has_recent_activity() {
       // Worker hasn't processed tasks in 5 minutes
       // This might be normal during low traffic
       println!("No recent activity - consider this normal for low traffic");
   }
   ```

---

## Monitoring and Debugging

### Enable Detailed Logging

```rust
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

tracing_subscriber::registry()
    .with(tracing_subscriber::EnvFilter::new(
        std::env::var("RUST_LOG")
            .unwrap_or_else(|_| "celers_worker=debug,celers_core=debug".into()),
    ))
    .with(tracing_subscriber::fmt::layer())
    .init();
```

### Enable Prometheus Metrics

```rust
#[cfg(feature = "metrics")]
{
    use celers_metrics;

    // Expose metrics endpoint
    let metrics_server = warp::path!("metrics")
        .map(|| {
            use prometheus::Encoder;
            let encoder = prometheus::TextEncoder::new();
            let metric_families = prometheus::gather();
            let mut buffer = vec![];
            encoder.encode(&metric_families, &mut buffer).unwrap();
            warp::reply::with_header(
                buffer,
                "Content-Type",
                encoder.format_type(),
            )
        });

    warp::serve(metrics_server).run(([0, 0, 0, 0], 9090)).await;
}
```

### Debug Worker State

```rust
// Print comprehensive worker state
async fn debug_worker_state(worker: &Worker<MyBroker>) {
    println!("=== Worker State ===");

    // Health
    let health = worker.get_health_info().await;
    println!("Health: {:?}", health.status);

    // Performance
    let perf = worker.get_performance_metrics().await;
    println!("Throughput: {:.2} tasks/sec", perf.throughput);

    // Resources
    let resources = worker.get_resource_status().await;
    println!("Memory: {} MB", resources.memory_usage_bytes / 1024 / 1024);
    println!("CPU: {:.1}%", resources.cpu_usage);

    // Queue
    let queue_stats = worker.get_queue_stats().await;
    println!("Queue depth: {}", queue_stats.current_depth);

    // Circuit breakers
    let cb_states = worker.get_circuit_breaker_states().await;
    for (task_type, state) in cb_states {
        println!("Circuit breaker [{}]: {:?}", task_type, state);
    }
}
```

### Common Debugging Commands

```bash
# Check worker process
ps aux | grep celers-worker

# Monitor resource usage
top -p $(pgrep -f celers-worker)

# Check open connections
lsof -i -P -n | grep celers-worker

# View worker logs
journalctl -u celers-worker -f

# Check broker connectivity
redis-cli -h localhost -p 6379 MONITOR

# Monitor task queue
redis-cli -h localhost -p 6379
> LLEN celery:queue
> LRANGE celery:queue 0 10
```

---

## Getting Help

If you're still experiencing issues:

1. **Check Logs**: Enable debug logging and examine the output
2. **Review Metrics**: Look at Prometheus metrics for anomalies
3. **Test in Isolation**: Try reproducing the issue with a minimal example
4. **Check Documentation**: Review the module documentation and examples
5. **File an Issue**: Create a detailed bug report with:
   - Configuration used
   - Error messages and stack traces
   - Steps to reproduce
   - Expected vs actual behavior
   - Environment details (OS, Rust version, dependencies)

For performance issues, include:
- Worker configuration
- Task types and volumes
- Resource usage statistics
- Broker type and configuration
