# celers-metrics TODO

> Prometheus metrics integration for CeleRS monitoring

## Status: ✅ FEATURE COMPLETE + ENHANCED WITH ADVANCED ANALYTICS & PRODUCTION-READY TOOLS + VALIDATION & REGISTRY + EXPORT UTILITIES + SLA & HEALTH MANAGEMENT + CAPACITY PLANNING + ALERT HISTORY + QUERY BUILDER

Complete Prometheus metrics implementation with comprehensive task queue monitoring, multi-backend export support, and advanced analytics capabilities including time-series analysis, auto-scaling recommendations, forecasting (linear & exponential), cost estimation with optimization recommendations, cardinality protection, trend-based alerting, correlation analysis, windowed statistics, adaptive sampling, metric export batching, label sanitization and validation, histogram heatmap generation, dynamic metric registry, resource usage tracking, **JSON/CSV export utilities**, **performance profiling utilities**, **SLA reporting**, **alert debouncing**, **health scoring**, **metric retention management**, **capacity planning and resource exhaustion prediction**, **alert history tracking**, **Prometheus query builder**, and **metric collection scheduler**.

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
- [x] **Metric history tracking and time-series analysis** ✅
  - [x] `MetricHistory` - Time-series data collection with configurable retention
  - [x] Trend calculation (rate of change over time)
  - [x] Moving average computation
  - [x] Min/max tracking
  - [x] Historical sample retrieval
- [x] **Auto-scaling recommendations** ✅
  - [x] `AutoScalingConfig` - Configure scaling parameters
  - [x] `recommend_scaling()` - Generate scaling recommendations
  - [x] Queue-based scaling (scale up when queue grows)
  - [x] Utilization-based scaling (scale based on worker busy ratio)
  - [x] Min/max worker constraints
  - [x] Cooldown period support
- [x] **Metric forecasting** ✅
  - [x] `forecast_metric()` - Linear regression-based forecasting
  - [x] `ForecastResult` - Predicted value, confidence, and trend
  - [x] R² confidence calculation
  - [x] Trend direction detection
- [x] **Cost estimation** ✅
  - [x] `CostConfig` - Configure cost parameters
  - [x] `estimate_costs()` - Calculate infrastructure costs
  - [x] `cost_per_task()` - Per-task cost calculation
  - [x] Compute cost tracking (worker-hours)
  - [x] Task execution cost tracking
  - [x] Cost breakdown (compute, tasks, data)
- [x] **Cardinality protection** ✅
  - [x] `CardinalityLimiter` - Prevent label explosion in production
  - [x] Track unique label combinations
  - [x] Configurable maximum cardinality
  - [x] Check and record labels within limits
  - [x] Reset functionality for testing
- [x] **Trend-based alerting** ✅
  - [x] `TrendAlertCondition` - Alert based on metric trends
  - [x] `TrendDirection` - Increasing, Decreasing, Stable trends
  - [x] Configurable trend thresholds
  - [x] Minimum sample requirements
  - [x] Integration with MetricHistory
- [x] **Correlation analysis** ✅
  - [x] `CorrelationResult` - Pearson correlation coefficient
  - [x] `calculate_correlation()` - Compute correlation between metrics
  - [x] `are_metrics_correlated()` - Quick correlation check
  - [x] Statistical significance approximation
  - [x] Time-aligned sample pairing
- [x] **Windowed statistics** ✅
  - [x] `TimeWindow` - Predefined time windows (1m, 5m, 15m, 1h, 1d)
  - [x] `WindowedStats` - Statistics within time windows
  - [x] `calculate_windowed_stats()` - Compute stats for windows
  - [x] Mean, min, max, std dev calculations
  - [x] **Percentile tracking** (p50, p95, p99) ✅
  - [x] Sample count tracking
- [x] **AlertManager integration with trend alerts** ✅
  - [x] `TrendAlertRule` - Trend-based alert rules
  - [x] `add_trend_rule()` - Add trend alerts to AlertManager
  - [x] `check_trend_alerts()` - Check trend-based alerts
  - [x] `critical_trend_alerts()` - Get critical trend alerts
  - [x] Unified alert management for static and trend-based alerts
- [x] **Exponential Smoothing Forecasting (Holt's Method)** ✅
  - [x] `ExponentialForecast` - Forecast result with level, trend, confidence
  - [x] `ExponentialSmoothingConfig` - Alpha/beta parameters for smoothing
  - [x] `forecast_exponential()` - Double exponential smoothing forecast
  - [x] More accurate than linear regression for trending data
  - [x] Confidence calculation based on recent forecast accuracy
  - [x] Handles stable and trending metrics
- [x] **Cost Optimization Recommendations** ✅
  - [x] `CostOptimization` - Enum of optimization recommendations
  - [x] `CostOptimizationConfig` - Configuration for cost analysis
  - [x] `recommend_cost_optimizations()` - Analyze and recommend optimizations
  - [x] Scale down recommendations for low utilization
  - [x] Batch size optimization for overhead reduction
  - [x] Spot instance recommendations for stable workloads
  - [x] Polling frequency optimization for idle systems
- [x] **Adaptive Sampling with Load Monitoring** ✅
  - [x] `AdaptiveSampler` - Adjusts sampling rate based on system load
  - [x] `update_based_on_load()` - Dynamic rate adjustment
  - [x] `should_sample()` - Check if observation should be sampled
  - [x] `current_rate()` - Get current sampling rate
  - [x] Automatic reduction during high load
  - [x] Automatic increase during low load
  - [x] Minimum 1% sampling rate guarantee
- [x] **Metric Export Batching** ✅
  - [x] `MetricBatch` - Batch container for efficient exports
  - [x] `MetricBatcher` - Thread-safe batching with size and age limits
  - [x] `add()` - Add metrics with auto-flush detection
  - [x] `flush()` - Manual batch export
  - [x] Size-based and time-based flush triggers
  - [x] Zero-copy batch operations
- [x] **Label Sanitization and Validation** ✅
  - [x] `sanitize_label_name()` - Sanitize labels to Prometheus standards
  - [x] `sanitize_label_value()` - Clean control characters from values
  - [x] `is_valid_metric_name()` - Validate metric names
  - [x] `is_valid_label_name()` - Validate label names
  - [x] Automatic name normalization (replace invalid chars)
  - [x] Whitespace normalization
- [x] **Histogram Heatmap Generation** ✅
  - [x] `HistogramHeatmap` - Time-series histogram visualization
  - [x] `HeatmapBucket` - Bucket data structure
  - [x] Snapshot recording with timestamps
  - [x] Configurable retention limits
  - [x] Historical data retrieval
- [x] **Dynamic Metric Registry** ✅
  - [x] `MetricRegistry` - Runtime metric management
  - [x] `MetricMetadata` - Metric information tracking
  - [x] `MetricType` - Counter, Gauge, Histogram, Summary
  - [x] Dynamic counter and gauge registration
  - [x] Metric listing and querying
  - [x] Unregister and clear operations
  - [x] Automatic validation during registration
- [x] **Resource Usage Tracking** ✅
  - [x] `ResourceTracker` - Track metrics collection overhead
  - [x] Collection time tracking (microsecond precision)
  - [x] Peak memory usage monitoring
  - [x] Average collection time calculation
  - [x] Operation wrapper for automatic tracking
  - [x] Reset functionality
- [x] **Metric Export Utilities** ✅
  - [x] `export_metrics_json()` - Export metrics in JSON format
  - [x] `export_metrics_csv()` - Export metrics in CSV format
  - [x] `export_history_csv()` - Export metric history to CSV
  - [x] `MetricExportBatch` - Batch export in multiple formats
  - [x] Timestamp support for all exports
  - [x] Structured JSON output with nested objects
- [x] **Performance Profiling Utilities** ✅
  - [x] `MetricsProfiler` - Profile metric collection operations
  - [x] `profile_operation()` - Profile specific operations
  - [x] `get_operation_stats()` - Get operation statistics
  - [x] `all_stats()` - Get all profiled operations
  - [x] `generate_report()` - Generate profiling report
  - [x] `OperationStats` - Statistics per operation
  - [x] Mean, min, max, std dev tracking
  - [x] Call count tracking
- [x] **SLA Report Generator** ✅ (NEW)
  - [x] `SlaReport` - Automated SLA compliance reports
  - [x] `generate()` - Generate report from SLO target
  - [x] `format_text()` - Human-readable report formatting
  - [x] Error budget calculation
  - [x] Compliance status tracking
  - [x] Automated recommendations
  - [x] Success rate and throughput analysis
- [x] **Alert Debouncer** ✅ (NEW)
  - [x] `AlertDebouncer` - Prevent alert fatigue
  - [x] `should_fire()` - Check if alert should fire
  - [x] `reset()` - Reset specific alert debounce
  - [x] `reset_all()` - Reset all alerts
  - [x] `time_until_next()` - Time until alert can fire again
  - [x] Configurable debounce periods
  - [x] Per-alert state tracking
- [x] **Health Score Calculator** ✅ (NEW)
  - [x] `HealthScore` - Composite system health scoring
  - [x] `calculate()` - Calculate overall health score
  - [x] `format_report()` - Human-readable health report
  - [x] Component-based scoring (success rate, queue, workers, DLQ)
  - [x] Weighted health calculation
  - [x] Letter grade system (A-F)
  - [x] Issue detection and reporting
- [x] **Metric Retention Manager** ✅
  - [x] `MetricRetentionManager` - Manage metric history retention
  - [x] `RetentionPolicy` - Configurable retention policies
  - [x] `set_policy()` / `get_policy()` - Policy management
  - [x] `apply_retention()` - Apply retention to history
  - [x] Pre-defined policies (default, high-frequency, long-term)
  - [x] Per-metric retention configuration
- [x] **Capacity Planning** ✅ (NEW)
  - [x] `CapacityPrediction` - Predict resource exhaustion
  - [x] `predict_capacity_exhaustion()` - Predict when resources will be exhausted
  - [x] `CapacityRecommendation` - Action recommendations (Healthy, Monitor, ActionNeeded, Critical, Decreasing)
  - [x] Growth rate calculation
  - [x] Utilization tracking
  - [x] Time-until-exhaustion prediction
- [x] **Alert History Tracking** ✅ (NEW)
  - [x] `AlertHistory` - Track alert firing events
  - [x] `AlertEvent` - Alert event with timestamp, severity, and trigger value
  - [x] `record_alert()` - Record when alerts fire
  - [x] `events_for_alert()` - Get history for specific alert
  - [x] `alert_fire_count()` - Count how many times alert fired
  - [x] `time_since_last_alert()` - Time since alert last fired
  - [x] Automatic retention management
- [x] **Prometheus Query Builder** ✅ (NEW)
  - [x] `PrometheusQueryBuilder` - Fluent API for building Prometheus queries
  - [x] Label filtering support
  - [x] Rate/increase aggregations
  - [x] Sum/avg aggregations
  - [x] Histogram quantile support
  - [x] Builder pattern for complex queries
- [x] **Metric Collection Scheduler** ✅ (NEW)
  - [x] `CollectionTask` - Scheduled metric collection tasks
  - [x] `MetricCollectionScheduler` - Manage collection tasks
  - [x] `should_collect()` - Check if it's time to collect
  - [x] `mark_collected()` - Mark task as collected
  - [x] Interval-based scheduling
  - [x] Task registration and management

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
- [x] **Unit tests for metric history** (7 tests) ✅
  - [x] Recording and retrieval
  - [x] Max samples limit
  - [x] Trend calculation
  - [x] Moving average
  - [x] Min/max tracking
  - [x] Clear functionality
  - [x] Latest sample retrieval
- [x] **Unit tests for auto-scaling** (6 tests) ✅
  - [x] No workers scenario
  - [x] High queue scaling
  - [x] High utilization scaling
  - [x] Low utilization scaling
  - [x] No change scenario
  - [x] Config builder
- [x] **Unit tests for cost estimation** (4 tests) ✅
  - [x] Cost estimation calculation
  - [x] Cost per task
  - [x] Zero tasks edge case
  - [x] Config builder
- [x] **Unit tests for forecasting** (4 tests) ✅
  - [x] Insufficient samples
  - [x] Linear trend prediction
  - [x] Stable values
  - [x] Result field validation
- [x] **Unit tests for cardinality protection** (3 tests) ✅
  - [x] Basic limiter functionality
  - [x] Reset functionality
  - [x] Duplicate label handling
- [x] **Unit tests for trend-based alerting** (3 tests) ✅
  - [x] Increasing trend detection
  - [x] Insufficient samples handling
  - [x] Stable trend detection
- [x] **Unit tests for correlation analysis** (4 tests) ✅
  - [x] Perfect positive correlation
  - [x] Perfect negative correlation
  - [x] Insufficient samples handling
  - [x] Correlation detection helper
- [x] **Unit tests for windowed statistics** (5 tests) ✅
  - [x] Basic windowed stats (with percentile validation)
  - [x] Empty history handling
  - [x] All time window variants
  - [x] Time window duration conversion
  - [x] Statistics accuracy validation (including p50, p95, p99)
- [x] **Unit tests for AlertManager integration** (2 tests) ✅
  - [x] Trend alert manager functionality
  - [x] Trend alert rule API
- [x] **Unit tests for exponential forecasting** (4 tests) ✅
  - [x] Basic forecast with increasing trend
  - [x] Config creation and defaults
  - [x] Insufficient samples handling
  - [x] Stable values forecasting
- [x] **Unit tests for cost optimization** (6 tests) ✅
  - [x] Scale down recommendation
  - [x] Increase batching recommendation
  - [x] Spot instances recommendation
  - [x] Polling optimization recommendation
  - [x] No optimization needed scenario
  - [x] Config defaults
- [x] **Unit tests for adaptive sampling** (5 tests) ✅
  - [x] Basic sampling behavior
  - [x] High load rate reduction
  - [x] Low load rate increase
  - [x] Base rate adjustment
  - [x] Minimum rate guarantee
- [x] **Unit tests for metric batching** (6 tests) ✅
  - [x] Basic batch operations
  - [x] Batch clear functionality
  - [x] Batcher basic operations
  - [x] Size-based flush trigger
  - [x] Manual flush
  - [x] Default batch creation
- [x] **Unit tests for label sanitization** (4 tests) ✅
  - [x] Label name sanitization
  - [x] Label value sanitization
  - [x] Metric name validation
  - [x] Label name validation
- [x] **Unit tests for histogram heatmap** (3 tests) ✅
  - [x] Basic heatmap operations
  - [x] Retention limit enforcement
  - [x] Clear functionality
- [x] **Unit tests for metric registry** (6 tests) ✅
  - [x] Counter registration and increment
  - [x] Gauge registration and set
  - [x] Metadata retrieval
  - [x] Name validation during registration
  - [x] Unregister functionality
  - [x] Clear all metrics
- [x] **Unit tests for resource tracker** (4 tests) ✅
  - [x] Basic operation tracking
  - [x] Multiple operations
  - [x] Memory usage tracking
  - [x] Reset functionality
- [x] **Unit tests for metric export utilities** (4 tests) ✅
  - [x] JSON export
  - [x] CSV export
  - [x] History CSV export
  - [x] Batch export (all formats)
- [x] **Unit tests for performance profiling** (4 tests) ✅
  - [x] Basic profiler operations
  - [x] Multiple operation tracking
  - [x] Report generation
  - [x] Reset functionality
- [x] **Unit tests for SLA reporting** (3 tests) ✅ (NEW)
  - [x] SLA report generation (compliant)
  - [x] SLA report non-compliant scenario
  - [x] Report text formatting
- [x] **Unit tests for alert debouncer** (4 tests) ✅ (NEW)
  - [x] Basic debounce functionality
  - [x] Different alert handling
  - [x] Reset functionality
  - [x] Time until next alert
- [x] **Unit tests for health score** (3 tests) ✅ (NEW)
  - [x] Perfect health score
  - [x] Degraded health score
  - [x] Health report formatting
- [x] **Unit tests for retention manager** (3 tests) ✅
  - [x] Retention policy defaults
  - [x] Set/get policy
  - [x] Default policy fallback
- [x] **Unit tests for capacity planning** (3 tests) ✅ (NEW)
  - [x] Growing capacity prediction
  - [x] Critical capacity scenarios
  - [x] Decreasing usage scenarios
- [x] **Unit tests for alert history** (3 tests) ✅ (NEW)
  - [x] Basic alert recording
  - [x] Retention limits
  - [x] Clear functionality
- [x] **Unit tests for Prometheus query builder** (5 tests) ✅ (NEW)
  - [x] Basic query generation
  - [x] Label filtering
  - [x] Rate aggregation
  - [x] Sum aggregation
  - [x] Avg aggregation
- [x] **Unit tests for collection scheduler** (3 tests) ✅ (NEW)
  - [x] Collection task basics
  - [x] Scheduler registration
  - [x] Should collect timing
- [x] Integration-style tests (7 tests) ✅
  - [x] Complete worker lifecycle simulation
  - [x] Broker operations simulation
  - [x] Multi-worker concurrent scenario
  - [x] End-to-end task lifecycle with monitoring
  - [x] Memory pressure and oversized results
  - [x] PostgreSQL connection pool metrics
  - [x] Delayed task scheduling
- [x] Performance impact testing ✅

**Total: 183 tests (172 unit + 4 profiling + 14 new features + 7 integration-style) - All passing ✅**
**Doc tests: 31 tests (29 original + 2 new features) - All passing ✅**
**Benchmarks: 33 performance benchmarks - All working ✅** (includes benchmarks for all features)
**Clippy warnings: 0 ✅**

## Documentation

- [x] Module-level documentation
- [x] Metric descriptions
- [x] Integration examples ✅
  - [x] basic_prometheus_endpoint.rs - Basic HTTP endpoint
  - [x] simple_metrics.rs - Simple metrics tracking
  - [x] health_and_alerts.rs - Health checks and alerting
  - [x] **production_monitoring.rs** - **Comprehensive production monitoring** (NEW)
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

### Cardinality Protection

```rust
use celers_metrics::CardinalityLimiter;

// Create a limiter with max 1000 unique label combinations
let limiter = CardinalityLimiter::new(1000);

// Check if label is within limit before recording metric
let task_label = format!("task_type:{}", task_name);
if limiter.check_and_record(&task_label) {
    // Safe to record metric
    TASKS_ENQUEUED_BY_TYPE
        .with_label_values(&[&task_name])
        .inc();
} else {
    // Use fallback label to prevent cardinality explosion
    TASKS_ENQUEUED_BY_TYPE
        .with_label_values(&["other"])
        .inc();
}

// Monitor cardinality
println!("Current cardinality: {}", limiter.current_cardinality());
if limiter.is_at_limit() {
    eprintln!("⚠ Cardinality limit reached!");
}
```

### Trend-Based Alerting

```rust
use celers_metrics::{TrendAlertCondition, TrendDirection, MetricHistory};

// Track queue size over time
let queue_history = MetricHistory::new(100);

// Periodically record metrics
loop {
    let queue_size = get_current_queue_size();
    queue_history.record(queue_size as f64);

    // Check for concerning trends
    let growing_alert = TrendAlertCondition::new(
        10.0, // Alert if growing by 10+ items/second
        TrendDirection::Increasing
    );

    if growing_alert.should_alert(&queue_history) {
        eprintln!("⚠ Queue is growing rapidly!");
        // Trigger auto-scaling, send alerts, etc.
    }

    std::thread::sleep(std::time::Duration::from_secs(10));
}
```

### Correlation Analysis

```rust
use celers_metrics::{MetricHistory, calculate_correlation, are_metrics_correlated};

// Track two metrics over time
let error_rate_history = MetricHistory::new(100);
let latency_history = MetricHistory::new(100);

// Record metrics...
error_rate_history.record(0.05);
latency_history.record(150.0);

// Check for correlation
if let Some(corr) = calculate_correlation(&error_rate_history, &latency_history) {
    println!("Correlation coefficient: {:.3}", corr.coefficient);
    println!("Significance: {:.3}", corr.significance);

    if corr.coefficient > 0.7 {
        println!("✓ Strong positive correlation detected");
        println!("  → High latency may be causing errors");
    } else if corr.coefficient < -0.7 {
        println!("✓ Strong negative correlation detected");
    }
}

// Quick correlation check
if are_metrics_correlated(&error_rate_history, &latency_history, 0.8) {
    println!("⚠ These metrics are strongly correlated!");
}
```

### Windowed Statistics

```rust
use celers_metrics::{MetricHistory, TimeWindow, calculate_windowed_stats};

// Track latency over time
let latency_history = MetricHistory::new(1000);

// Record measurements...
latency_history.record(50.0);
latency_history.record(75.0);
latency_history.record(60.0);

// Analyze last 5 minutes
if let Some(stats) = calculate_windowed_stats(&latency_history, TimeWindow::FiveMinutes) {
    println!("Last 5 minutes:");
    println!("  Mean latency: {:.2}ms", stats.mean);
    println!("  Min: {:.2}ms", stats.min);
    println!("  Max: {:.2}ms", stats.max);
    println!("  Std Dev: {:.2}ms", stats.std_dev);
    println!("  Sample count: {}", stats.sample_count);

    // Check SLO compliance
    if stats.mean > 100.0 {
        eprintln!("⚠ Average latency exceeds SLO!");
    }
}

// Compare different time windows
let last_minute = calculate_windowed_stats(&latency_history, TimeWindow::OneMinute);
let last_hour = calculate_windowed_stats(&latency_history, TimeWindow::OneHour);

if let (Some(recent), Some(hourly)) = (last_minute, last_hour) {
    if recent.mean > hourly.mean * 1.5 {
        eprintln!("⚠ Recent latency is 50% higher than hourly average!");
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
