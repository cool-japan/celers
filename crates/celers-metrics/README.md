# celers-metrics

**Version: 0.2.0 | Status: [Stable] | Updated: 2026-03-27**

Prometheus metrics integration for CeleRS distributed task queue monitoring. Comprehensive instrumentation with 25+ metrics for production observability.

## Overview

Production-ready Prometheus metrics for monitoring:

- ✅ **Task Metrics**: Enqueued, completed, failed, retried, cancelled
- ✅ **Queue Metrics**: Queue size, processing queue, dead letter queue
- ✅ **Performance Metrics**: Execution time histograms, batch sizes
- ✅ **Connection Metrics**: Redis connection pooling and errors
- ✅ **Memory Metrics**: Worker memory usage, result sizes
- ✅ **Worker Metrics**: Active worker count
- ✅ **Zero Overhead**: Fully optional via feature flag
- ✅ **Thread-Safe**: Atomic operations for concurrent access

## Quick Start

### Enable Metrics

```toml
[dependencies]
celers-metrics = "0.1"
celers-worker = { version = "0.1", features = ["metrics"] }
celers-broker-redis = { version = "0.1", features = ["metrics"] }
```

### Expose HTTP Endpoint

```rust
use celers_metrics::gather_metrics;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    // Start metrics HTTP server
    let listener = TcpListener::bind("0.0.0.0:9090").await.unwrap();
    println!("Metrics available at http://localhost:9090/metrics");

    loop {
        let (mut socket, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            let metrics = gather_metrics();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\n{}",
                metrics
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
    }
}
```

### Configure Prometheus

Add to `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'celers'
    static_configs:
      - targets: ['localhost:9090']
    scrape_interval: 5s
```

## Available Metrics

### Task Counters

| Metric | Type | Description |
|--------|------|-------------|
| `celers_tasks_enqueued_total` | Counter | Total tasks added to queue |
| `celers_tasks_completed_total` | Counter | Total successfully completed tasks |
| `celers_tasks_failed_total` | Counter | Total permanently failed tasks |
| `celers_tasks_retried_total` | Counter | Total task retry attempts |
| `celers_tasks_cancelled_total` | Counter | Total cancelled tasks |

**Usage in queries:**
```promql
# Task completion rate
rate(celers_tasks_completed_total[5m])

# Task failure rate
rate(celers_tasks_failed_total[5m]) / rate(celers_tasks_enqueued_total[5m])

# Success rate
rate(celers_tasks_completed_total[5m]) /
(rate(celers_tasks_completed_total[5m]) + rate(celers_tasks_failed_total[5m]))
```

### Queue Gauges

| Metric | Type | Description |
|--------|------|-------------|
| `celers_queue_size` | Gauge | Current pending tasks |
| `celers_processing_queue_size` | Gauge | Currently processing tasks |
| `celers_dlq_size` | Gauge | Dead letter queue size |
| `celers_active_workers` | Gauge | Number of active workers |

**Usage in queries:**
```promql
# Queue backlog
celers_queue_size

# Worker utilization
celers_processing_queue_size / celers_active_workers

# DLQ growth rate
increase(celers_dlq_size[5m])
```

### Performance Histograms

| Metric | Type | Buckets | Description |
|--------|------|---------|-------------|
| `celers_task_execution_seconds` | Histogram | 1ms - 300s | Task execution time |
| `celers_batch_size` | Histogram | 1 - 1000 | Tasks per batch operation |
| `celers_task_result_size_bytes` | Histogram | 100B - 10MB | Task result sizes |

**Histogram buckets:**
- **Execution time**: 1ms, 10ms, 100ms, 500ms, 1s, 5s, 10s, 30s, 60s, 300s
- **Batch size**: 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000
- **Result size**: 100B, 1KB, 10KB, 100KB, 1MB, 10MB

**Usage in queries:**
```promql
# P95 execution time
histogram_quantile(0.95, rate(celers_task_execution_seconds_bucket[5m]))

# P99 execution time
histogram_quantile(0.99, rate(celers_task_execution_seconds_bucket[5m]))

# Average batch size
rate(celers_batch_size_sum[5m]) / rate(celers_batch_size_count[5m])
```

### Connection Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `celers_redis_connections_acquired_total` | Counter | Total connections acquired |
| `celers_redis_connection_errors_total` | Counter | Total connection errors |
| `celers_redis_connections_active` | Gauge | Current active connections |
| `celers_redis_connection_acquire_seconds` | Histogram | Connection acquisition time |

**Usage in queries:**
```promql
# Connection error rate
rate(celers_redis_connection_errors_total[5m])

# Average connection acquisition time
rate(celers_redis_connection_acquire_seconds_sum[5m]) /
rate(celers_redis_connection_acquire_seconds_count[5m])
```

### Batch Operation Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `celers_batch_enqueue_total` | Counter | Total batch enqueue operations |
| `celers_batch_dequeue_total` | Counter | Total batch dequeue operations |
| `celers_batch_size` | Histogram | Number of tasks per batch |

**Usage in queries:**
```promql
# Batch operation throughput
rate(celers_batch_enqueue_total[5m])

# Average tasks per batch
rate(celers_batch_size_sum[5m]) / rate(celers_batch_size_count[5m])
```

### Memory Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `celers_worker_memory_usage_bytes` | Gauge | Worker memory usage |
| `celers_task_result_size_bytes` | Histogram | Task result sizes |
| `celers_oversized_results_total` | Counter | Tasks with oversized results |

**Usage in queries:**
```promql
# Worker memory usage (MB)
celers_worker_memory_usage_bytes / 1024 / 1024

# Oversized result rate
rate(celers_oversized_results_total[5m])

# Average result size
rate(celers_task_result_size_bytes_sum[5m]) /
rate(celers_task_result_size_bytes_count[5m])
```

## Integration

### Worker Integration

Metrics are automatically updated when the `metrics` feature is enabled:

```rust
use celers_worker::{Worker, WorkerConfig};
use celers_broker_redis::RedisBroker;
use celers_core::TaskRegistry;

#[tokio::main]
async fn main() {
    let broker = RedisBroker::new("redis://localhost:6379", "celery").unwrap();
    let registry = TaskRegistry::new();
    let config = WorkerConfig::default();

    let worker = Worker::new(broker, registry, config);

    // Metrics automatically updated:
    // - TASKS_COMPLETED_TOTAL (on success)
    // - TASKS_FAILED_TOTAL (on failure)
    // - TASKS_RETRIED_TOTAL (on retry)
    // - TASK_EXECUTION_TIME (execution duration)

    worker.run().await.unwrap();
}
```

### Broker Integration

Redis broker automatically updates metrics:

```rust
use celers_broker_redis::RedisBroker;
use celers_core::Broker;

#[tokio::main]
async fn main() {
    let broker = RedisBroker::new("redis://localhost:6379", "celery").unwrap();

    // Metrics automatically updated:
    // - TASKS_ENQUEUED_TOTAL (on enqueue)
    // - QUEUE_SIZE (current queue depth)
    // - BATCH_ENQUEUE_TOTAL (batch operations)
    // - BATCH_SIZE (batch size histogram)

    let task = celers_core::SerializedTask::new("my_task", vec![]);
    broker.enqueue(task).await.unwrap();
}
```

### Manual Metric Updates

For custom metrics or non-standard workflows:

```rust
use celers_metrics::{
    TASKS_ENQUEUED_TOTAL,
    TASK_EXECUTION_TIME,
    QUEUE_SIZE,
};

// Increment counter
TASKS_ENQUEUED_TOTAL.inc();

// Set gauge
QUEUE_SIZE.set(42.0);

// Observe histogram
TASK_EXECUTION_TIME.observe(1.5);  // 1.5 seconds
```

## Prometheus Configuration

### Basic Configuration

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'celers'
    static_configs:
      - targets: ['localhost:9090']
```

### Service Discovery (Kubernetes)

```yaml
scrape_configs:
  - job_name: 'celers-workers'
    kubernetes_sd_configs:
      - role: pod
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_label_app]
        action: keep
        regex: celers-worker
```

### Recording Rules

Optimize queries with recording rules:

```yaml
groups:
  - name: celers
    interval: 30s
    rules:
      # Task success rate (last 5 minutes)
      - record: celers:task_success_rate:5m
        expr: >
          rate(celers_tasks_completed_total[5m]) /
          (rate(celers_tasks_completed_total[5m]) + rate(celers_tasks_failed_total[5m]))

      # Task throughput (tasks/sec)
      - record: celers:task_throughput:5m
        expr: rate(celers_tasks_completed_total[5m])

      # P95 latency
      - record: celers:task_p95_latency:5m
        expr: histogram_quantile(0.95, rate(celers_task_execution_seconds_bucket[5m]))

      # P99 latency
      - record: celers:task_p99_latency:5m
        expr: histogram_quantile(0.99, rate(celers_task_execution_seconds_bucket[5m]))

      # Average batch size
      - record: celers:avg_batch_size:5m
        expr: >
          rate(celers_batch_size_sum[5m]) /
          rate(celers_batch_size_count[5m])
```

### Alert Rules

```yaml
groups:
  - name: celers_alerts
    rules:
      # High failure rate
      - alert: CelersHighFailureRate
        expr: |
          rate(celers_tasks_failed_total[5m]) /
          rate(celers_tasks_enqueued_total[5m]) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High task failure rate (>10%)"
          description: "{{ $value | humanizePercentage }} of tasks are failing"

      # Queue backlog
      - alert: CelersQueueBacklog
        expr: celers_queue_size > 1000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Queue backlog detected"
          description: "{{ $value }} tasks waiting in queue"

      # DLQ growing
      - alert: CelersDLQGrowing
        expr: increase(celers_dlq_size[5m]) > 10
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Dead letter queue is growing"
          description: "{{ $value }} tasks added to DLQ in last 5 minutes"

      # Slow tasks
      - alert: CelersSlowTasks
        expr: histogram_quantile(0.95, rate(celers_task_execution_seconds_bucket[5m])) > 30
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Tasks are slow (P95 > 30s)"
          description: "P95 execution time: {{ $value | humanizeDuration }}"

      # No workers
      - alert: CelersNoWorkers
        expr: celers_active_workers < 1
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "No active workers"
          description: "All workers are down"

      # Redis connection errors
      - alert: CelersRedisConnectionErrors
        expr: rate(celers_redis_connection_errors_total[5m]) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Redis connection errors detected"
          description: "{{ $value }} connection errors per second"

      # Memory usage high
      - alert: CelersHighMemoryUsage
        expr: celers_worker_memory_usage_bytes > 1000000000  # 1GB
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Worker memory usage high"
          description: "Worker using {{ $value | humanize }}B of memory"
```

## Grafana Dashboards

### Import Dashboard

```json
{
  "dashboard": {
    "title": "CeleRS Monitoring",
    "panels": [
      {
        "title": "Task Throughput",
        "targets": [
          {
            "expr": "rate(celers_tasks_completed_total[5m])"
          }
        ]
      },
      {
        "title": "Queue Size",
        "targets": [
          {
            "expr": "celers_queue_size"
          }
        ]
      },
      {
        "title": "P95 Execution Time",
        "targets": [
          {
            "expr": "histogram_quantile(0.95, rate(celers_task_execution_seconds_bucket[5m]))"
          }
        ]
      }
    ]
  }
}
```

### Key Panels

1. **Task Throughput**: `rate(celers_tasks_completed_total[5m])`
2. **Success Rate**: Success rate recording rule
3. **Queue Depth**: `celers_queue_size`
4. **Active Workers**: `celers_active_workers`
5. **P95/P99 Latency**: Latency recording rules
6. **DLQ Size**: `celers_dlq_size`
7. **Memory Usage**: `celers_worker_memory_usage_bytes`
8. **Batch Sizes**: `celers_batch_size` histogram

## Performance Impact

- **Overhead**: <0.1% CPU, <1MB memory
- **Thread-Safe**: Lock-free atomic operations
- **Zero Cost**: When feature disabled, no overhead
- **Lazy Initialization**: Metrics initialized only when used

## Best Practices

### 1. Use Recording Rules

Pre-compute expensive queries:

```yaml
- record: celers:task_success_rate:5m
  expr: |
    rate(celers_tasks_completed_total[5m]) /
    (rate(celers_tasks_completed_total[5m]) + rate(celers_tasks_failed_total[5m]))
```

### 2. Set Appropriate Scrape Intervals

- **High-traffic**: 5-15 seconds
- **Normal**: 15-30 seconds
- **Low-traffic**: 30-60 seconds

### 3. Use Histograms for Percentiles

Always use `histogram_quantile()` for P50/P95/P99:

```promql
histogram_quantile(0.95, rate(celers_task_execution_seconds_bucket[5m]))
```

### 4. Alert on Trends, Not Absolutes

Use `rate()` and `increase()` instead of raw counters:

```promql
# Good: Rate of failures
rate(celers_tasks_failed_total[5m]) > 10

# Bad: Total failures
celers_tasks_failed_total > 1000
```

## Testing

**183 tests passing** (unit + doc tests)

```rust
#[cfg(test)]
mod tests {
    use celers_metrics::*;

    #[test]
    fn test_task_metrics() {
        reset_metrics();

        TASKS_ENQUEUED_TOTAL.inc();
        TASKS_COMPLETED_TOTAL.inc();
        QUEUE_SIZE.set(5.0);

        let metrics = gather_metrics();
        assert!(metrics.contains("celers_tasks_enqueued_total 1"));
        assert!(metrics.contains("celers_queue_size 5"));
    }

    #[test]
    fn test_histogram_metrics() {
        reset_metrics();

        TASK_EXECUTION_TIME.observe(1.5);
        TASK_EXECUTION_TIME.observe(0.5);

        let metrics = gather_metrics();
        assert!(metrics.contains("celers_task_execution_seconds"));
    }
}
```

## Troubleshooting

### Metrics not appearing in Prometheus

1. Check HTTP endpoint is accessible: `curl http://localhost:9090/metrics`
2. Verify `metrics` feature is enabled in `Cargo.toml`
3. Check Prometheus target status: Prometheus UI → Status → Targets

### High cardinality warnings

- Avoid labels with high cardinality (e.g., task IDs, timestamps)
- Use recording rules to aggregate high-cardinality metrics
- Consider sampling for very high-frequency metrics

### Missing metrics

- Ensure worker/broker has `metrics` feature enabled
- Check that metrics are imported: `use celers_metrics::*;`
- Verify metrics are registered (check `gather_metrics()` output)

## API Reference

```rust
// Gather all metrics in Prometheus text format
pub fn gather_metrics() -> String

// Reset all metrics (testing only)
pub fn reset_metrics()

// Task counters
pub static ref TASKS_ENQUEUED_TOTAL: Counter
pub static ref TASKS_COMPLETED_TOTAL: Counter
pub static ref TASKS_FAILED_TOTAL: Counter
pub static ref TASKS_RETRIED_TOTAL: Counter
pub static ref TASKS_CANCELLED_TOTAL: Counter

// Queue gauges
pub static ref QUEUE_SIZE: Gauge
pub static ref PROCESSING_QUEUE_SIZE: Gauge
pub static ref DLQ_SIZE: Gauge
pub static ref ACTIVE_WORKERS: Gauge

// Performance histograms
pub static ref TASK_EXECUTION_TIME: Histogram
pub static ref BATCH_SIZE: Histogram
pub static ref TASK_RESULT_SIZE_BYTES: Histogram

// Connection metrics
pub static ref REDIS_CONNECTIONS_ACQUIRED_TOTAL: Counter
pub static ref REDIS_CONNECTION_ERRORS_TOTAL: Counter
pub static ref REDIS_CONNECTIONS_ACTIVE: Gauge
pub static ref REDIS_CONNECTION_ACQUIRE_TIME: Histogram

// Batch metrics
pub static ref BATCH_ENQUEUE_TOTAL: Counter
pub static ref BATCH_DEQUEUE_TOTAL: Counter

// Memory metrics
pub static ref WORKER_MEMORY_USAGE_BYTES: Gauge
pub static ref OVERSIZED_RESULTS_TOTAL: Counter
```

## See Also

- **Worker**: `celers-worker` - Worker runtime with metrics
- **Broker**: `celers-broker-redis` - Redis broker with metrics
- **Prometheus**: https://prometheus.io/docs/

## License

Apache-2.0
