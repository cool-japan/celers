# celers-metrics TODO

> Prometheus metrics integration for CeleRS monitoring

## Status: ✅ FEATURE COMPLETE

Complete Prometheus metrics implementation with comprehensive task queue monitoring.

## Completed Features

### Metrics ✅

#### Counters
- [x] `celers_tasks_enqueued_total` - Total tasks added to queue
- [x] `celers_tasks_completed_total` - Total successfully completed tasks
- [x] `celers_tasks_failed_total` - Total permanently failed tasks
- [x] `celers_tasks_retried_total` - Total task retry attempts
- [x] `celers_tasks_cancelled_total` - Total cancelled tasks

#### Gauges
- [x] `celers_queue_size` - Current pending tasks
- [x] `celers_processing_queue_size` - Currently processing tasks
- [x] `celers_dlq_size` - Dead letter queue size
- [x] `celers_active_workers` - Number of active workers

#### Histograms
- [x] `celers_task_execution_seconds` - Task execution time distribution
  - Buckets: 1ms, 10ms, 100ms, 500ms, 1s, 5s, 10s, 30s, 60s, 300s

### Utilities ✅
- [x] `gather_metrics()` - Export metrics in Prometheus format
- [x] `reset_metrics()` - Reset all counters (for testing)
- [x] Lazy static initialization
- [x] Thread-safe atomic operations

## Integration

### Worker Integration ✅
- [x] Track task completions
- [x] Track task failures
- [x] Track retry attempts
- [x] Measure execution time
- [x] Optional feature flag

### Broker Integration ✅
- [x] Track enqueue operations
- [x] Update queue size gauges
- [x] Update processing queue size
- [x] Update DLQ size
- [x] Optional feature flag

### Per-Task-Type Metrics ✅
- [x] Task enqueued by type counter
- [x] Task completed by type counter
- [x] Task failed by type counter
- [x] Task retried by type counter
- [x] Task cancelled by type counter
- [x] Execution time by type histogram
- [x] Result size by type histogram
- [x] Worker integration (lib.rs and middleware.rs)
- [x] Broker integration (enqueue and batch enqueue)

## Future Enhancements

### Additional Metrics
- [x] Per-task-type metrics (using labels) ✅
- [x] Queue depth over time (via gauges) ✅
- [x] Average wait time in queue (histogram) ✅
- [x] Task age histogram ✅
- [x] Worker utilization percentage ✅
- [x] Broker operation latency ✅
  - [x] Enqueue latency
  - [x] Dequeue latency
  - [x] Ack latency
  - [x] Reject latency
  - [x] Queue size check latency
- [x] Delayed task metrics ✅

### Advanced Features
- [ ] Custom metric labels
- [ ] Metric aggregation across workers
- [ ] SLO/SLA tracking
- [ ] Anomaly detection helpers
- [ ] Rate calculation helpers

### Alternative Backends
- [ ] StatsD backend
- [ ] OpenTelemetry metrics
- [ ] CloudWatch metrics
- [ ] Datadog integration

### Performance
- [ ] Reduce metrics overhead
- [ ] Sampling for high-frequency metrics
- [ ] Configurable histogram buckets
- [ ] Metric pre-aggregation

## Testing Status

- [x] Unit tests for metrics increment (1 test)
- [x] Unit tests for execution time (1 test)
- [ ] Integration tests with worker
- [ ] Integration tests with broker
- [ ] Performance impact testing
- [ ] Concurrent access testing

## Documentation

- [x] Module-level documentation
- [x] Metric descriptions
- [x] Integration example (prometheus_metrics.rs)
- [ ] Grafana dashboard documentation (separate)
- [ ] Alerting rules examples
- [ ] Recording rules examples

## Dependencies

- `prometheus`: Rust Prometheus client
- `lazy_static`: Lazy static initialization
- `tokio`: Async runtime (for examples)

## HTTP Endpoint Example

```rust
use celers_metrics::gather_metrics;

#[tokio::main]
async fn main() {
    // Start HTTP server
    let listener = TcpListener::bind("127.0.0.1:9090").await.unwrap();

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        tokio::spawn(async move {
            let metrics = gather_metrics();
            // Send HTTP response with metrics
        });
    }
}
```

## Prometheus Configuration

Add to `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'celers'
    static_configs:
      - targets: ['localhost:9090']
    scrape_interval: 5s
```

## Grafana Integration

Dashboard JSON available in project:
- `/tmp/grafana-dashboard-celers.json`
- Import to Grafana for instant visualizations
- Pre-configured panels for all metrics

## Recording Rules

Optimize queries with recording rules:

```yaml
groups:
  - name: celers
    interval: 30s
    rules:
      - record: celers:task_success_rate
        expr: >
          rate(celers_tasks_completed_total[5m]) /
          (rate(celers_tasks_completed_total[5m]) + rate(celers_tasks_failed_total[5m]))

      - record: celers:task_throughput
        expr: rate(celers_tasks_completed_total[5m])

      - record: celers:p95_latency
        expr: histogram_quantile(0.95, rate(celers_task_execution_seconds_bucket[5m]))
```

## Alert Rules

Example Prometheus alerts:

```yaml
groups:
  - name: celers_alerts
    rules:
      - alert: HighFailureRate
        expr: |
          rate(celers_tasks_failed_total[5m]) /
          rate(celers_tasks_enqueued_total[5m]) > 0.1
        for: 5m
        annotations:
          summary: "High task failure rate"

      - alert: QueueBacklog
        expr: celers_queue_size > 1000
        for: 5m
        annotations:
          summary: "Queue backlog detected"

      - alert: DLQGrowing
        expr: increase(celers_dlq_size[5m]) > 10
        for: 5m
        annotations:
          summary: "Dead letter queue is growing"
```

## Notes

- Metrics are optional (feature flag: `metrics`)
- Zero overhead when feature is disabled
- Thread-safe atomic operations
- Compatible with Prometheus, Grafana, and other tools
- Follows Prometheus naming conventions
- Uses histogram for latency (not summary)
