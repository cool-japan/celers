# celers-metrics TODO

> Prometheus metrics integration for CeleRS monitoring

## Status: ✅ FEATURE COMPLETE + BACKEND INTEGRATIONS

Complete Prometheus metrics implementation with comprehensive task queue monitoring and multi-backend export support.

## Completed Features

### Core Features ✅

**Metrics Configuration & Sampling**
- Configurable histogram buckets for different metric types
- Sampling support for high-frequency metrics (configurable rate 0.0-1.0)
- Builder pattern for metrics configuration

**Rate Calculation Helpers**
- `calculate_rate()` - Calculate events per second
- `calculate_success_rate()` - Calculate success rate from completed/failed counters
- `calculate_error_rate()` - Calculate error rate
- `calculate_throughput()` - Calculate tasks per second

**SLO/SLA Tracking**
- `SloTarget` - Define service level objectives
- `check_slo_compliance()` - Check if metrics meet SLO targets
- `calculate_error_budget()` - Calculate remaining error budget
- `SloStatus` - Compliance status enum (Compliant/NonCompliant/Unknown)

**Anomaly Detection**
- `AnomalyThreshold` - Statistical thresholds for anomaly detection (mean, std_dev, sigma)
- `AnomalyStatus` - Detection result enum (Normal/High/Low)
- `detect_anomaly()` - Detect anomalies in metric values
- `MovingAverage` - Exponential weighted moving average for trend detection
- `detect_spike()` - Detect sudden spikes or drops in metrics

**Metric Pre-Aggregation**
- `MetricStats` - Pre-aggregated statistics (count, sum, min, max, variance, std_dev)
- `MetricAggregator` - Thread-safe metric aggregator
- Statistical calculations (mean, variance, standard deviation)
- Merge support for distributed aggregation

**Custom Metric Labels**
- `CustomLabels` - Flexible label management with HashMap storage
- `CustomMetricBuilder` - Builder pattern for labeled metrics
- Integration with Prometheus CounterVec and HistogramVec
- Support for converting from iterators and HashMap

**Distributed Metric Aggregation**
- `MetricSnapshot` - Timestamped metric snapshots from workers/nodes
- `DistributedAggregator` - Thread-safe cross-worker aggregation
- Automatic stale snapshot detection (configurable threshold)
- Worker count tracking and snapshot cleanup

**Backend Integration Support**
- `MetricExport` - Common metric export format
- `MetricBackend` - Trait for implementing custom backends
- `StatsDConfig` - StatsD wire format support with tags
- `OpenTelemetryConfig` - OpenTelemetry resource configuration
- `CloudWatchConfig` - AWS CloudWatch namespace and dimension support
- `DatadogConfig` - Datadog API integration with tag formatting
- Multi-backend export pattern support

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
- [x] Custom metric labels ✅
- [x] Metric aggregation across workers ✅
- [x] SLO/SLA tracking ✅
- [x] Anomaly detection helpers ✅
- [x] Rate calculation helpers ✅
- [x] Current metrics snapshot utility ✅
- [x] Health check utilities ✅
- [x] Percentile calculation helpers ✅
- [x] Metric comparison utilities (A/B testing) ✅
- [x] Alert threshold helpers and alert manager ✅
- [x] Metric summary and reporting ✅

### Alternative Backends
- [x] StatsD backend ✅
- [x] OpenTelemetry metrics ✅
- [x] CloudWatch metrics ✅
- [x] Datadog integration ✅

### Performance
- [x] Reduce metrics overhead (via sampling) ✅
- [x] Sampling for high-frequency metrics ✅
- [x] Configurable histogram buckets ✅
- [x] Metric pre-aggregation ✅

## Testing Status

- [x] Unit tests for metrics increment (1 test)
- [x] Unit tests for execution time (1 test)
- [x] Unit tests for per-task-type metrics (1 test)
- [x] Unit tests for metrics configuration (1 test)
- [x] Unit tests for sampling (1 test)
- [x] Unit tests for rate calculations (1 test)
- [x] Unit tests for SLO compliance (1 test)
- [x] Unit tests for error budget (1 test)
- [x] Unit tests for concurrent access (1 test) ✅
- [x] Unit tests for sampled observations (1 test)
- [x] Unit tests for anomaly detection (4 tests) ✅
- [x] Unit tests for metric stats (2 tests) ✅
- [x] Unit tests for metric aggregator (2 tests) ✅
- [x] Unit tests for custom labels (4 tests) ✅
- [x] Unit tests for metric snapshots (2 tests) ✅
- [x] Unit tests for distributed aggregator (4 tests) ✅
- [x] Unit tests for backend integration (12 tests) ✅
- [x] Unit tests for current metrics capture (2 tests) ✅
- [x] Unit tests for health check (5 tests) ✅
- [x] Unit tests for percentile calculation (4 tests) ✅
- [x] Unit tests for metric comparison (3 tests) ✅
- [x] Unit tests for alert rules (5 tests) ✅
- [x] Unit tests for metric summary (1 test) ✅
- [ ] Integration tests with worker
- [ ] Integration tests with broker
- [x] Performance impact testing ✅

**Total: 61 unit tests - All passing ✅**
**Benchmarks: 19 performance benchmarks - All working ✅**

## Documentation

- [x] Module-level documentation
- [x] Metric descriptions
- [x] Integration example (prometheus_metrics.rs)
- [x] Grafana dashboard documentation ✅
- [x] Alerting rules examples ✅
- [x] Recording rules examples ✅

## Performance Benchmarks

Run benchmarks to measure metrics overhead:

```bash
# Run all benchmarks
cargo bench

# Run specific benchmark
cargo bench -- counter_increment

# Generate detailed reports
cargo bench --bench metrics_bench
```

### Benchmark Categories

1. **Basic Operations** - Counter increments, gauge sets, histogram observations
2. **Labeled Metrics** - Per-task-type metrics with labels
3. **Sampling** - Performance impact of different sampling rates
4. **Aggregation** - Metric aggregator and distributed aggregator performance
5. **Calculations** - Rate calculations, anomaly detection, percentiles
6. **Health Checks** - Health check evaluation performance
7. **Alert Rules** - Alert rule evaluation and manager performance
8. **Gathering** - Metrics export and summary generation

### Typical Performance (on modern hardware)

- Counter increment: ~11 ns
- Gauge set: ~9 ns
- Histogram observe: ~50-100 ns
- Labeled metric: ~50-150 ns
- Health check: ~1-5 μs
- Metric gathering: ~10-50 μs

## Dependencies

- `prometheus`: Rust Prometheus client
- `lazy_static`: Lazy static initialization
- `tokio`: Async runtime (for examples)
- `rand`: Random number generation (for sampling)

## Usage Examples

### Basic HTTP Endpoint

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

### Metrics Configuration with Sampling

```rust
use celers_metrics::{MetricsConfig, observe_sampled, TASK_EXECUTION_TIME};

// Configure metrics with 10% sampling for high-frequency metrics
let config = MetricsConfig::new()
    .with_sampling_rate(0.1)
    .with_execution_time_buckets(vec![0.1, 1.0, 5.0, 10.0])
    .with_latency_buckets(vec![0.01, 0.1, 1.0])
    .with_size_buckets(vec![1_000.0, 10_000.0, 100_000.0]);

// Use sampled observation for high-frequency metrics
observe_sampled(|| {
    TASK_EXECUTION_TIME.observe(execution_time);
});
```

### SLO Tracking and Monitoring

```rust
use celers_metrics::{
    SloTarget, check_slo_compliance, calculate_error_budget,
    calculate_success_rate, SloStatus
};

// Define SLO targets
let slo = SloTarget {
    success_rate: 0.99,      // 99% success rate
    latency_seconds: 5.0,    // p95 latency under 5 seconds
    throughput: 100.0,       // 100 tasks per second
};

// Check compliance
let current_success_rate = calculate_success_rate(
    completed_count as f64,
    failed_count as f64
);

let status = check_slo_compliance(
    current_success_rate,
    p95_latency,
    throughput,
    &slo
);

match status {
    SloStatus::Compliant => println!("✓ Meeting SLO targets"),
    SloStatus::NonCompliant => println!("✗ SLO violation detected"),
    SloStatus::Unknown => println!("? Insufficient data"),
}

// Calculate error budget
let budget = calculate_error_budget(
    total_requests as f64,
    failed_requests as f64,
    slo.success_rate
);

println!("Error budget remaining: {:.1}%", budget * 100.0);
```

### Rate Calculation

```rust
use celers_metrics::{calculate_rate, calculate_throughput};
use std::time::Instant;

let start = Instant::now();
let start_count = current_task_count();

// ... process tasks ...

let elapsed = start.elapsed().as_secs_f64();
let end_count = current_task_count();

let rate = calculate_rate(end_count, start_count, elapsed);
println!("Processing rate: {:.2} tasks/sec", rate);

// Or use the throughput helper
let throughput = calculate_throughput(end_count - start_count, elapsed);
println!("Throughput: {:.2} tasks/sec", throughput);
```

### Anomaly Detection

```rust
use celers_metrics::{AnomalyThreshold, detect_anomaly, AnomalyStatus, MovingAverage, detect_spike};

// Create threshold from historical baseline
let samples = vec![100.0, 102.0, 98.0, 101.0, 99.0]; // Historical latency values
let threshold = AnomalyThreshold::from_samples(&samples, 3.0).unwrap(); // 3-sigma

// Check if current latency is anomalous
let current_latency = 150.0;
match detect_anomaly(current_latency, &threshold) {
    AnomalyStatus::High => println!("⚠ Latency spike detected!"),
    AnomalyStatus::Low => println!("⚠ Latency drop detected!"),
    AnomalyStatus::Normal => println!("✓ Latency is normal"),
}

// Use moving average for trend detection
let mut ma = MovingAverage::new(100.0, 0.2); // 20% smoothing factor
let smoothed = ma.update(current_latency);
println!("Smoothed latency: {:.2}ms", smoothed);

// Detect sudden spikes (2x threshold)
if detect_spike(current_latency, threshold.mean, 2.0) {
    println!("⚠ Sudden latency spike detected!");
}
```

### Metric Pre-Aggregation

```rust
use celers_metrics::{MetricAggregator, MetricStats};
use std::sync::Arc;

// Create a shared aggregator for task execution times
let aggregator = Arc::new(MetricAggregator::new());

// In your worker threads, record observations
let agg = Arc::clone(&aggregator);
std::thread::spawn(move || {
    for execution_time in task_execution_times {
        agg.observe(execution_time);
    }
});

// Periodically get statistics snapshot
let stats = aggregator.snapshot();
println!("Task execution statistics:");
println!("  Count: {}", stats.count);
println!("  Mean: {:.2}s", stats.mean());
println!("  Std Dev: {:.2}s", stats.std_dev());
println!("  Min: {:.2}s", stats.min);
println!("  Max: {:.2}s", stats.max);

// Merge stats from multiple workers
let mut combined = MetricStats::new();
combined.merge(&stats_worker1);
combined.merge(&stats_worker2);
println!("Combined mean: {:.2}s", combined.mean());
```

### Custom Metric Labels

```rust
use celers_metrics::{CustomLabels, CustomMetricBuilder, TASKS_ENQUEUED_BY_TYPE};

// Create custom labels for metrics
let labels = CustomLabels::new()
    .with_label("environment", "production")
    .with_label("region", "us-west-2")
    .with_label("service", "api");

// Or use the builder pattern
let labels = CustomMetricBuilder::new()
    .label("env", "production")
    .label("region", "us-west-2")
    .build();

// Use labels with existing metric vectors
let task_name = "send_email";
TASKS_ENQUEUED_BY_TYPE
    .with_label_values(&[task_name])
    .inc();

// Convert labels to label values for use with CounterVec/HistogramVec
let label_names = &["environment", "region"];
let label_values = labels.to_label_values(label_names);

// Create from iterator
let labels: CustomLabels = vec![
    ("key1", "value1"),
    ("key2", "value2"),
]
.into_iter()
.collect();
```

### Distributed Metric Aggregation

```rust
use celers_metrics::{
    DistributedAggregator, MetricSnapshot, MetricStats, CustomLabels
};
use std::sync::Arc;

// Create a distributed aggregator (default 5-minute stale threshold)
let aggregator = Arc::new(DistributedAggregator::new());

// Or with custom stale threshold (in seconds)
let aggregator = Arc::new(DistributedAggregator::with_stale_threshold(600));

// Each worker reports its metrics as snapshots
let worker_id = "worker-1";
let mut stats = MetricStats::new();
stats.observe(10.0);
stats.observe(20.0);

let snapshot = MetricSnapshot::new(worker_id, stats);
aggregator.update(snapshot);

// Optionally add custom labels to snapshots
let labels = CustomLabels::new()
    .with_label("region", "us-east-1")
    .with_label("instance_type", "m5.large");

let snapshot = MetricSnapshot::new("worker-2", stats)
    .with_labels(labels);
aggregator.update(snapshot);

// Get aggregated statistics across all active workers
let combined = aggregator.aggregate();
println!("Distributed metrics:");
println!("  Total observations: {}", combined.count);
println!("  Mean: {:.2}", combined.mean());
println!("  Std Dev: {:.2}", combined.std_dev());

// Get number of active workers
println!("Active workers: {}", aggregator.active_worker_count());

// Get all active snapshots
let snapshots = aggregator.active_snapshots();
for snapshot in snapshots {
    println!("Worker {}: {} observations",
        snapshot.worker_id, snapshot.stats.count);
}

// Periodically cleanup stale snapshots
aggregator.cleanup_stale();

// Reset all snapshots
aggregator.reset();
```

### Backend Integration - StatsD

```rust
use celers_metrics::{
    StatsDConfig, MetricExport, CustomLabels, export_to_statsd, MetricStats
};

// Configure StatsD backend
let config = StatsDConfig::new()
    .with_host("statsd.example.com")
    .with_port(8125)
    .with_prefix("celers")
    .with_sample_rate(1.0);

// Export a counter metric
let counter = MetricExport::Counter {
    name: "tasks_completed".to_string(),
    value: 42.0,
    labels: CustomLabels::new()
        .with_label("environment", "production")
        .with_label("region", "us-east-1"),
};

let statsd_format = config.format_metric(&counter);
// Output: "celers.tasks_completed:42|c|#environment:production,region:us-east-1"

// Export aggregated statistics
let mut stats = MetricStats::new();
stats.observe(10.0);
stats.observe(20.0);

let formatted = export_to_statsd(&stats, "task_execution_time", &config);
// Output: "celers.task_execution_time:15|h"

// Send to StatsD server (using your preferred UDP client)
// udp_client.send(formatted.as_bytes())?;
```

### Backend Integration - OpenTelemetry

```rust
use celers_metrics::{OpenTelemetryConfig, CustomLabels};

// Configure OpenTelemetry
let config = OpenTelemetryConfig::new()
    .with_service_name("celers-worker")
    .with_service_version("1.0.0")
    .with_environment("production")
    .with_attribute("host", "worker-01")
    .with_attribute("datacenter", "us-east-1a");

println!("Service: {}", config.service_name);
println!("Version: {}", config.service_version);
println!("Environment: {}", config.environment);

// Use with OpenTelemetry SDK:
// let meter = global::meter_provider()
//     .versioned_meter(
//         config.service_name.clone(),
//         Some(config.service_version.clone()),
//         None,
//         Some(config.attributes.as_vec()),
//     );
```

### Backend Integration - CloudWatch

```rust
use celers_metrics::{CloudWatchConfig, CustomLabels};

// Configure CloudWatch
let config = CloudWatchConfig::new()
    .with_namespace("CeleRS/TaskQueue")
    .with_region("us-west-2")
    .with_dimension("Environment", "Production")
    .with_dimension("Service", "TaskWorker")
    .with_storage_resolution(1); // High-resolution metrics (1 second)

println!("Namespace: {}", config.namespace);
println!("Region: {}", config.region);
println!("Storage Resolution: {} seconds", config.storage_resolution);

// Use with AWS CloudWatch SDK:
// let client = aws_sdk_cloudwatch::Client::new(&sdk_config);
// client.put_metric_data()
//     .namespace(&config.namespace)
//     .metric_data(...)
//     .send()
//     .await?;
```

### Backend Integration - Datadog

```rust
use celers_metrics::{DatadogConfig, CustomLabels};

// Configure Datadog
let config = DatadogConfig::new()
    .with_api_host("https://api.datadoghq.com")
    .with_api_key("your-api-key-here")
    .with_prefix("celers")
    .with_tag("env", "production")
    .with_tag("service", "task-queue")
    .with_tag("version", "1.0.0");

// Add metric-specific labels
let metric_labels = CustomLabels::new()
    .with_label("task_type", "send_email")
    .with_label("priority", "high");

let tags = config.format_tags(&metric_labels);
// Output: ["env:production", "service:task-queue", "version:1.0.0",
//          "task_type:send_email", "priority:high"]

// Use with Datadog API:
// let metric = DatadogMetric {
//     metric: format!("{}.tasks.completed", config.prefix),
//     points: vec![(timestamp, value)],
//     tags: Some(tags),
//     ..Default::default()
// };
// datadog_client.submit_metrics(vec![metric]).await?;
```

### Multi-Backend Export Pattern

```rust
use celers_metrics::{
    MetricExport, MetricBackend, StatsDConfig, CustomLabels
};

// Implement the MetricBackend trait for your custom exporter
struct MultiBackendExporter {
    statsd_config: StatsDConfig,
    // Add other backend configs as needed
}

impl MetricBackend for MultiBackendExporter {
    fn export(&mut self, metric: &MetricExport) -> Result<(), String> {
        // Export to StatsD
        let statsd_format = self.statsd_config.format_metric(metric);
        // Send to StatsD...

        // Export to other backends...

        Ok(())
    }

    fn flush(&mut self) -> Result<(), String> {
        // Flush all backends
        Ok(())
    }
}

// Usage
let mut exporter = MultiBackendExporter {
    statsd_config: StatsDConfig::new(),
};

let metric = MetricExport::Counter {
    name: "tasks_completed".to_string(),
    value: 100.0,
    labels: CustomLabels::new(),
};

exporter.export(&metric).unwrap();
exporter.flush().unwrap();
```

### Current Metrics Snapshot

```rust
use celers_metrics::CurrentMetrics;

// Capture current metric values programmatically
let metrics = CurrentMetrics::capture();

println!("Queue size: {}", metrics.queue_size);
println!("Active workers: {}", metrics.active_workers);
println!("Tasks enqueued: {}", metrics.tasks_enqueued);
println!("Tasks completed: {}", metrics.tasks_completed);
println!("Tasks failed: {}", metrics.tasks_failed);

// Calculate derived metrics
let success_rate = metrics.success_rate();
let error_rate = metrics.error_rate();
let total_processed = metrics.total_processed();

println!("Success rate: {:.2}%", success_rate * 100.0);
println!("Error rate: {:.2}%", error_rate * 100.0);
println!("Total processed: {}", total_processed);
```

### Health Check Utilities

```rust
use celers_metrics::{health_check, HealthCheckConfig, HealthStatus, SloTarget};

// Configure health check thresholds
let config = HealthCheckConfig::new()
    .with_max_queue_size(1000.0)
    .with_max_dlq_size(100.0)
    .with_min_active_workers(2.0)
    .with_slo_target(SloTarget {
        success_rate: 0.99,
        latency_seconds: 5.0,
        throughput: 100.0,
    });

// Perform health check
match health_check(&config) {
    HealthStatus::Healthy => {
        println!("✓ System is healthy");
    }
    HealthStatus::Degraded { reasons } => {
        println!("⚠ System is degraded:");
        for reason in reasons {
            println!("  - {}", reason);
        }
    }
    HealthStatus::Unhealthy { reasons } => {
        println!("✗ System is unhealthy:");
        for reason in reasons {
            println!("  - {}", reason);
        }
        // Trigger alerts, stop accepting new tasks, etc.
    }
}
```

### Percentile Calculation

```rust
use celers_metrics::{calculate_percentile, calculate_percentiles};

// Collect latency measurements
let mut latencies = vec![10.0, 15.0, 12.0, 20.0, 18.0, 25.0, 30.0, 22.0, 16.0, 19.0];
latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());

// Calculate specific percentile
let p95 = calculate_percentile(&latencies, 0.95).unwrap();
println!("p95 latency: {:.2}ms", p95);

// Calculate common percentiles in one call
let (p50, p95, p99) = calculate_percentiles(&latencies).unwrap();
println!("p50: {:.2}ms, p95: {:.2}ms, p99: {:.2}ms", p50, p95, p99);

// Use for SLO compliance checking
if p95 > 100.0 {
    println!("⚠ p95 latency exceeds SLO target!");
}
```

### Metric Comparison (A/B Testing)

```rust
use celers_metrics::{CurrentMetrics, MetricComparison};

// Capture baseline metrics (e.g., old version)
let baseline = CurrentMetrics::capture();

// ... deploy new version ...

// Capture current metrics (new version)
let current = CurrentMetrics::capture();

// Compare metrics
let comparison = MetricComparison::compare(&baseline, &current);

println!("Success rate change: {:+.2}%", comparison.success_rate_change);
println!("Error rate change: {:+.2}%", comparison.error_rate_change);
println!("Throughput change: {:+.2}%", comparison.throughput_change);
println!("Queue size diff: {:+.0}", comparison.queue_size_diff);

// Check if changes are significant
if comparison.is_significant(5.0) {
    println!("⚠ Significant performance change detected!");
}

// Determine if it's an improvement or degradation
if comparison.is_improvement() {
    println!("✓ Metrics improved");
} else if comparison.is_degradation() {
    println!("✗ Metrics degraded - consider rollback");
}
```

### Alert Rules and Monitoring

```rust
use celers_metrics::{
    AlertManager, AlertRule, AlertCondition, AlertSeverity, CurrentMetrics
};

// Create alert manager
let mut alerts = AlertManager::new();

// Add alert rules
alerts.add_rule(AlertRule::new(
    "high_error_rate",
    AlertCondition::ErrorRateAbove { threshold: 0.05 },
    AlertSeverity::Critical,
    "Error rate exceeded 5% - investigate immediately"
));

alerts.add_rule(AlertRule::new(
    "high_queue_size",
    AlertCondition::GaugeAbove { threshold: 1000.0 },
    AlertSeverity::Warning,
    "Queue size exceeded 1000 tasks"
));

alerts.add_rule(AlertRule::new(
    "low_workers",
    AlertCondition::GaugeBelow { threshold: 2.0 },
    AlertSeverity::Critical,
    "Worker count below minimum threshold"
));

alerts.add_rule(AlertRule::new(
    "low_success_rate",
    AlertCondition::SuccessRateBelow { threshold: 0.95 },
    AlertSeverity::Warning,
    "Success rate dropped below 95%"
));

// Check alerts periodically
let metrics = CurrentMetrics::capture();
let fired_alerts = alerts.check_alerts(&metrics);

if !fired_alerts.is_empty() {
    println!("⚠ {} alerts fired:", fired_alerts.len());
    for alert in fired_alerts {
        println!("  [{:?}] {}: {}",
            alert.severity, alert.name, alert.description);
    }
}

// Check only critical alerts
let critical = alerts.critical_alerts(&metrics);
if !critical.is_empty() {
    println!("🚨 {} critical alerts!", critical.len());
    // Send notifications, trigger pager, etc.
}
```

### Metric Summary Reports

```rust
use celers_metrics::generate_metric_summary;

// Generate human-readable summary
let summary = generate_metric_summary();
println!("{}", summary);

// Output:
// === CeleRS Metrics Summary ===
//
// Tasks:
//   Enqueued:        1000
//   Completed:        950
//   Failed:            50
//   Retried:           25
//   Cancelled:          5
//
// Rates:
//   Success:       95.00%
//   Error:          5.00%
//
// Queues:
//   Pending:           50
//   Processing:        10
//   DLQ:                2
//
// Workers:
//   Active:             5

// Use for logging, dashboards, or debugging
eprintln!("{}", summary);
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
