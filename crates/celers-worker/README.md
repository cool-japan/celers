# celers-worker

Production-ready worker runtime for consuming and executing CeleRS tasks with comprehensive monitoring, memory optimization, and workflow support.

## Overview

High-performance worker runtime with:

- ✅ **Concurrent Execution**: Configurable parallelism (default: 4 workers)
- ✅ **Batch Dequeue**: Fetch multiple tasks per round-trip (10-100x faster)
- ✅ **Memory Optimization**: Result size limits and tracking
- ✅ **Retry Logic**: Exponential backoff with configurable limits
- ✅ **Graceful Shutdown**: Complete in-flight tasks before termination
- ✅ **Timeout Enforcement**: Per-task execution timeouts
- ✅ **Workflow Support**: Canvas workflow integration (Chain, Chord, Group)
- ✅ **Prometheus Metrics**: Comprehensive monitoring (optional)
- ✅ **Health Checks**: HTTP health endpoint
- ✅ **Middleware Support**: Pre/post execution hooks

## Quick Start

### Basic Worker

```rust
use celers_broker_redis::RedisBroker;
use celers_worker::{Worker, WorkerConfig};
use celers_core::TaskRegistry;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create broker
    let broker = RedisBroker::new("redis://localhost:6379", "celery")?;

    // Create task registry
    let mut registry = TaskRegistry::new();
    registry.register("tasks.add", |args: Vec<i32>| async move {
        Ok(args[0] + args[1])
    });

    // Configure worker
    let config = WorkerConfig {
        concurrency: 4,
        poll_interval_ms: 1000,
        graceful_shutdown: true,
        max_retries: 3,
        ..Default::default()
    };

    // Create and run worker
    let worker = Worker::new(broker, registry, config);
    worker.run().await?;

    Ok(())
}
```

### High-Performance Worker (Batch Mode)

```rust
let config = WorkerConfig {
    concurrency: 16,                    // More concurrent tasks
    enable_batch_dequeue: true,         // Batch fetching
    batch_size: 50,                     // 50 tasks per fetch
    poll_interval_ms: 100,              // Poll more frequently
    max_result_size_bytes: 10_000_000,  // 10MB limit
    track_memory_usage: true,           // Monitor memory
    ..Default::default()
};

let worker = Worker::new(broker, registry, config);
worker.run().await?;
```

## Configuration

### WorkerConfig

```rust
pub struct WorkerConfig {
    /// Number of concurrent tasks to process (default: 4)
    pub concurrency: usize,

    /// Polling interval when queue is empty in milliseconds (default: 1000)
    pub poll_interval_ms: u64,

    /// Enable graceful shutdown (default: true)
    pub graceful_shutdown: bool,

    /// Maximum number of retry attempts (default: 3)
    pub max_retries: u32,

    /// Base delay for exponential backoff in milliseconds (default: 1000)
    pub retry_base_delay_ms: u64,

    /// Maximum delay between retries in milliseconds (default: 60000)
    pub retry_max_delay_ms: u64,

    /// Default task timeout in seconds (default: 300)
    pub default_timeout_secs: u64,

    // Memory optimization options

    /// Enable batch dequeue for better throughput (default: false)
    pub enable_batch_dequeue: bool,

    /// Number of tasks to fetch per batch (default: 10)
    pub batch_size: usize,

    /// Maximum task result size in bytes, 0 = unlimited (default: 0)
    pub max_result_size_bytes: usize,

    /// Enable memory usage tracking and reporting (default: false)
    pub track_memory_usage: bool,
}
```

### Configuration Examples

**Low Latency:**
```rust
let config = WorkerConfig {
    concurrency: 1,
    poll_interval_ms: 100,
    enable_batch_dequeue: false,
    ..Default::default()
};
```

**High Throughput:**
```rust
let config = WorkerConfig {
    concurrency: 32,
    poll_interval_ms: 100,
    enable_batch_dequeue: true,
    batch_size: 100,
    ..Default::default()
};
```

**Memory Constrained:**
```rust
let config = WorkerConfig {
    concurrency: 4,
    max_result_size_bytes: 1_000_000,  // 1MB limit
    track_memory_usage: true,
    ..Default::default()
};
```

**Production (Recommended):**
```rust
let config = WorkerConfig {
    concurrency: 16,
    enable_batch_dequeue: true,
    batch_size: 20,
    max_result_size_bytes: 10_000_000,
    track_memory_usage: true,
    graceful_shutdown: true,
    max_retries: 3,
    default_timeout_secs: 300,
    ..Default::default()
};
```

## Features

### Batch Dequeue (10-100x Faster)

Fetch multiple tasks in a single round-trip:

```rust
let config = WorkerConfig {
    enable_batch_dequeue: true,
    batch_size: 50,  // Fetch 50 tasks at once
    ..Default::default()
};
```

**Performance comparison:**
| Mode | Throughput | Latency |
|------|-----------|---------|
| Individual | 1K tasks/sec | 1ms per task |
| Batch (50) | 40K tasks/sec | 0.025ms per task |

**When to use:**
- High task volume (>1000 tasks/sec)
- Network latency to broker
- CPU-bound tasks (fast execution)

**When not to use:**
- Low task volume (<100 tasks/sec)
- I/O-bound tasks (long execution)
- Need low latency (process ASAP)

### Memory Optimization

Limit task result sizes and track memory usage:

```rust
let config = WorkerConfig {
    max_result_size_bytes: 10_000_000,  // 10MB limit
    track_memory_usage: true,           // Enable tracking
    ..Default::default()
};
```

**Features:**
- Result size validation
- Memory usage metrics (Prometheus)
- Oversized result rejection

**Metrics:**
- `celers_worker_memory_usage_bytes`: Current memory usage
- `celers_task_result_size_bytes`: Result size histogram
- `celers_oversized_results_total`: Rejected oversized results

### Retry Logic

Exponential backoff with configurable limits:

```rust
let config = WorkerConfig {
    max_retries: 3,
    retry_base_delay_ms: 1000,   // Start with 1s
    retry_max_delay_ms: 60000,   // Cap at 60s
    ..Default::default()
};
```

**Backoff calculation:**
```
Retry 0: 1000ms (1s)
Retry 1: 2000ms (2s)
Retry 2: 4000ms (4s)
Retry 3: 8000ms (8s)
...capped at retry_max_delay_ms
```

**Behavior:**
- Task fails → Check retry count
- If retries < max_retries → Requeue with incremented count
- If retries >= max_retries → Send to Dead Letter Queue

### Graceful Shutdown

Complete in-flight tasks before termination:

```rust
use celers_worker::wait_for_signal;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let worker = Worker::new(broker, registry, config);
    let handle = worker.run_with_shutdown().await?;

    // Wait for SIGTERM/SIGINT
    wait_for_signal().await;

    // Request graceful shutdown
    handle.shutdown().await?;

    Ok(())
}
```

**Shutdown flow:**
1. Signal received (CTRL+C, SIGTERM)
2. Stop dequeuing new tasks
3. Wait for in-flight tasks to complete
4. Disconnect from broker
5. Exit cleanly

### Task Timeouts

Per-task execution time limits:

```rust
// Default timeout (from WorkerConfig)
let config = WorkerConfig {
    default_timeout_secs: 300,  // 5 minutes
    ..Default::default()
};

// Per-task timeout (in task metadata)
let task = SerializedTask::new("long_task", args)
    .with_timeout(600);  // 10 minutes for this specific task
```

**Timeout handling:**
- Task execution wrapped in `tokio::time::timeout()`
- On timeout: Task treated as failed
- Retry logic applies (if retries remaining)

### Workflow Integration

Support for Canvas workflows (requires `workflows` feature):

```toml
[dependencies]
celers-worker = { version = "0.1", features = ["workflows"] }
```

```rust
use celers_worker::workflows::handle_workflow_completion;
use celers_backend_redis::RedisResultBackend;

// Worker automatically handles:
// - Chord barrier synchronization
// - Chain callback execution
// - Group task tracking

let mut backend = RedisResultBackend::new("redis://localhost:6379")?;

// Chord completion is automatically detected and callback enqueued
// when all tasks complete
```

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                     Worker Runtime                        │
│                                                           │
│  ┌────────────────────────────────────────────────────┐  │
│  │            Main Polling Loop                       │  │
│  │                                                     │  │
│  │  1. Dequeue task(s) from broker                    │  │
│  │     - Individual or batch mode                     │  │
│  │  2. Spawn concurrent execution tasks               │  │
│  │     - Respects concurrency limit                   │  │
│  │  3. Execute with timeout                           │  │
│  │  4. Handle result                                  │  │
│  │     - Success: Ack to broker                       │  │
│  │     - Failure: Retry or DLQ                        │  │
│  │     - Timeout: Retry or DLQ                        │  │
│  │  5. Update metrics (if enabled)                    │  │
│  │  6. Check memory limits (if enabled)               │  │
│  └────────────────────────────────────────────────────┘  │
│                                                           │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐  ┌─────────┐      │
│  │ Task 1  │ │ Task 2  │ │ Task 3  │  │ Task N  │      │
│  │ Execute │ │ Execute │ │ Execute │..│ Execute │      │
│  └─────────┘ └─────────┘ └─────────┘  └─────────┘      │
│      ↓            ↓            ↓            ↓            │
│  ┌──────────────────────────────────────────────────┐   │
│  │        Task Registry (function dispatch)         │   │
│  └──────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────┘
                        │
                        ▼
        ┌───────────────────────────────┐
        │  Broker (Redis, PostgreSQL)   │
        └───────────────────────────────┘
```

## Monitoring

### Prometheus Metrics (Optional)

```toml
[dependencies]
celers-worker = { version = "0.1", features = ["metrics"] }
```

**Metrics emitted:**
- `celers_tasks_completed_total`: Successfully completed tasks
- `celers_tasks_failed_total`: Permanently failed tasks
- `celers_tasks_retried_total`: Retry attempts
- `celers_task_execution_seconds`: Execution time histogram
- `celers_worker_memory_usage_bytes`: Memory usage (if tracking enabled)
- `celers_task_result_size_bytes`: Result size histogram
- `celers_oversized_results_total`: Oversized result rejections

**Setup:**
```rust
use celers_metrics::gather_metrics;

// Expose metrics endpoint
let listener = TcpListener::bind("0.0.0.0:9090").await?;
loop {
    let (mut socket, _) = listener.accept().await?;
    tokio::spawn(async move {
        let metrics = gather_metrics();
        let response = format!("HTTP/1.1 200 OK\r\n\r\n{}", metrics);
        socket.write_all(response.as_bytes()).await.unwrap();
    });
}
```

### Logging

Uses `tracing` for structured logging:

```rust
use tracing_subscriber;

// Initialize logging
tracing_subscriber::fmt::init();

// Worker emits logs at various levels:
// - info: Task started, completed, worker started
// - warn: Task retry, queue empty
// - error: Task failure, broker errors
```

### Health Checks

```rust
use celers_worker::health::HealthCheck;

let health = HealthCheck::new();

// HTTP endpoint
let listener = TcpListener::bind("0.0.0.0:8080").await?;
loop {
    let (mut socket, _) = listener.accept().await?;
    let status = if health.is_healthy() { "OK" } else { "UNHEALTHY" };
    let response = format!("HTTP/1.1 200 OK\r\n\r\n{}", status);
    socket.write_all(response.as_bytes()).await.unwrap();
}
```

## Best Practices

### 1. Choose Appropriate Concurrency

```rust
// CPU-bound tasks: concurrency = CPU cores
let config = WorkerConfig {
    concurrency: num_cpus::get(),
    ..Default::default()
};

// I/O-bound tasks: concurrency = 2-4x CPU cores
let config = WorkerConfig {
    concurrency: num_cpus::get() * 4,
    ..Default::default()
};
```

### 2. Enable Batch Dequeue for High Throughput

```rust
// High task volume: enable batching
if expected_tasks_per_sec > 1000 {
    let config = WorkerConfig {
        enable_batch_dequeue: true,
        batch_size: 50,
        ..Default::default()
    };
}
```

### 3. Set Memory Limits

```rust
// Prevent memory bloat
let config = WorkerConfig {
    max_result_size_bytes: 10_000_000,  // 10MB
    track_memory_usage: true,
    ..Default::default()
};
```

### 4. Implement Graceful Shutdown

```rust
// Always use graceful shutdown in production
let config = WorkerConfig {
    graceful_shutdown: true,
    ..Default::default()
};

let handle = worker.run_with_shutdown().await?;
wait_for_signal().await;
handle.shutdown().await?;
```

### 5. Monitor with Metrics

```rust
// Enable metrics in production
#[cfg(feature = "metrics")]
{
    // Start metrics endpoint
    tokio::spawn(async {
        start_metrics_server("0.0.0.0:9090").await
    });
}
```

## Performance Tuning

### Batch Size Selection

| Task Rate | Batch Size | Reasoning |
|-----------|-----------|-----------|
| <100/sec | 1 (disabled) | Low latency more important |
| 100-1K/sec | 10-20 | Balanced |
| 1K-10K/sec | 50-100 | High throughput |
| >10K/sec | 100-200 | Maximum throughput |

### Concurrency Tuning

```rust
// Measure throughput with different concurrency levels
for concurrency in [1, 2, 4, 8, 16, 32] {
    let config = WorkerConfig {
        concurrency,
        ..Default::default()
    };
    // Benchmark and measure throughput
}
```

**Rule of thumb:**
- Start with CPU cores
- Increase until throughput stops improving
- Watch for memory pressure

## Error Handling

```rust
use celers_core::CelersError;

// Worker handles errors automatically, but you can customize:
registry.register("my_task", |args: Vec<i32>| async move {
    match risky_operation(args).await {
        Ok(result) => Ok(result),
        Err(e) => {
            // Log error
            eprintln!("Task failed: {}", e);

            // Return error (triggers retry logic)
            Err(CelersError::Other(e.to_string()))
        }
    }
});
```

## Examples

See `examples/` directory:
- `phase1_complete.rs` - Basic worker setup
- `graceful_shutdown.rs` - Shutdown handling
- `prometheus_metrics.rs` - Metrics integration
- `health_checks.rs` - Health check endpoint
- `canvas_workflows.rs` - Workflow support

## Troubleshooting

### Worker not processing tasks

**Check:**
1. Broker connection: `broker.is_connected()`
2. Queue has tasks: `broker.queue_size().await`
3. Tasks registered: `registry.list()`

### High memory usage

**Solution:**
```rust
let config = WorkerConfig {
    max_result_size_bytes: 10_000_000,
    track_memory_usage: true,
    ..Default::default()
};
```

### Tasks timing out

**Solution:**
```rust
// Increase timeout
let config = WorkerConfig {
    default_timeout_secs: 600,  // 10 minutes
    ..Default::default()
};
```

### Tasks being retried forever

**Solution:**
```rust
// Set max retries
let config = WorkerConfig {
    max_retries: 3,
    ..Default::default()
};
```

## See Also

- **Core**: `celers-core` - Task registry and execution
- **Broker**: `celers-broker-redis` - Redis broker implementation
- **Metrics**: `celers-metrics` - Prometheus metrics
- **Canvas**: `celers-canvas` - Workflow primitives

## License

MIT OR Apache-2.0
