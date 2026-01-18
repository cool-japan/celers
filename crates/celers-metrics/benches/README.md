# CeleRS Metrics Performance Benchmarks

This directory contains Criterion-based benchmarks to measure the performance impact of metrics collection.

## Running Benchmarks

```bash
# Run all benchmarks
cargo bench

# Run specific benchmark
cargo bench -- counter_increment

# Run with custom sample size
cargo bench -- --sample-size 50

# Generate HTML reports (requires gnuplot or plotters)
cargo bench --bench metrics_bench
```

Benchmark results are saved to `target/criterion/`.

## Benchmark Categories

### 1. Basic Operations (`bench_counter_*`, `bench_gauge_*`, `bench_histogram_*`)

Measures the overhead of fundamental metric operations:
- **counter_increment**: Single counter increment (`inc()`)
- **counter_increment_by**: Counter increment by value (`inc_by(n)`)
- **gauge_set**: Setting gauge to absolute value
- **histogram_observe**: Recording histogram observation

**Expected Performance**: 9-15 ns per operation

### 2. Labeled Metrics (`bench_per_task_type_metrics`)

Measures metrics with labels (per-task-type tracking):
- **per_task_type_counter**: Counter with task type label
- **per_task_type_histogram**: Histogram with task type label

**Expected Performance**: 50-150 ns per operation (higher due to label lookup)

### 3. Sampling (`bench_sampling`, `bench_observe_sampled`)

Tests performance impact of different sampling rates:
- **sampling/rate_0.01**: 1% sampling rate
- **sampling/rate_0.1**: 10% sampling rate
- **sampling/rate_0.5**: 50% sampling rate
- **sampling/rate_1.0**: 100% sampling (no sampling)
- **observe_sampled**: Using the global sampler

**Expected Performance**: Varies by rate; lower rates have less overhead

### 4. Metric Aggregation (`bench_metric_aggregation`)

Tests pre-aggregation performance:
- **metric_aggregator_observe**: Adding observation to aggregator
- **metric_aggregator_snapshot**: Getting snapshot of aggregated stats

**Expected Performance**: 20-50 ns per observe, 50-200 ns per snapshot

### 5. Distributed Aggregation (`bench_distributed_aggregation`)

Tests cross-worker metric aggregation:
- **distributed_aggregator_update**: Updating with worker snapshot
- **distributed_aggregator_aggregate**: Aggregating across all workers

**Expected Performance**: 200-500 ns per update, 1-5 μs per aggregate (with 10 workers)

### 6. Rate Calculations (`bench_rate_calculations`)

Measures helper calculation performance:
- **calculate_rate**: Events per second calculation
- **calculate_success_rate**: Success percentage calculation
- **calculate_error_budget**: Remaining error budget calculation

**Expected Performance**: 5-20 ns per calculation

### 7. Anomaly Detection (`bench_anomaly_detection`)

Tests anomaly detection helpers:
- **detect_anomaly**: Statistical anomaly detection
- **moving_average_update**: Exponential moving average update
- **detect_spike**: Sudden spike detection

**Expected Performance**: 5-30 ns per operation

### 8. Health Checks (`bench_health_check`)

Measures health check evaluation:
- **health_check**: Full health evaluation with SLO targets

**Expected Performance**: 1-5 μs per check (includes multiple gauge reads + calculations)

### 9. Percentile Calculation (`bench_percentile_calculation`)

Tests percentile calculation on sorted data:
- **calculate_percentile_p50**: Median calculation
- **calculate_percentile_p95**: 95th percentile
- **calculate_percentiles_batch**: p50, p95, p99 in one call

**Expected Performance**: 10-50 ns per percentile (on 1000 values)

### 10. Metric Comparison (`bench_metric_comparison`)

Tests A/B testing metric comparison:
- **metric_comparison**: Comparing two metric snapshots

**Expected Performance**: 50-200 ns per comparison

### 11. Alert Rules (`bench_alert_rules`)

Measures alert evaluation performance:
- **alert_rule_check**: Single rule evaluation
- **alert_manager_check**: Checking multiple rules (3 rules)

**Expected Performance**: 100-500 ns per rule, 500-2000 ns for manager

### 12. Gathering & Export (`bench_gather_metrics`, `bench_current_metrics_capture`, `bench_generate_summary`)

Tests metric export operations:
- **gather_metrics**: Export all metrics in Prometheus format
- **current_metrics_capture**: Snapshot current counter/gauge values
- **generate_metric_summary**: Generate human-readable summary

**Expected Performance**:
- Capture: 500-1000 ns
- Gather: 10-50 μs (depends on number of metrics)
- Summary: 5-20 μs

## Performance Guidelines

### Low Overhead (< 100 ns)
- Basic counter/gauge operations
- Rate calculations
- Anomaly detection checks
- Percentile calculations

### Moderate Overhead (100 ns - 1 μs)
- Labeled metrics (per-task-type)
- Metric aggregation
- Alert rule evaluation
- Metric comparisons

### Higher Overhead (> 1 μs)
- Health checks (due to multiple operations)
- Distributed aggregation (depends on worker count)
- Metric gathering/export (depends on total metrics)

## Interpreting Results

### What to Look For

1. **Regression Detection**: Compare with previous runs to detect performance regressions
2. **Overhead Analysis**: Understand the cost of enabling metrics in your application
3. **Sampling Impact**: Verify that sampling reduces overhead proportionally
4. **Scaling Behavior**: Test with varying numbers of workers/metrics

### Acceptable Overhead

For most applications, metrics overhead should be:
- **< 1%** of total application time for basic operations
- **< 5%** when using labeled metrics and aggregation
- **< 10%** with full observability (health checks, alerts, sampling)

### Optimization Tips

1. **Use Sampling**: For high-frequency operations, use `observe_sampled()` or configure sampling rate
2. **Batch Operations**: Aggregate metrics locally before exporting
3. **Label Cardinality**: Limit the number of unique label values
4. **Selective Metrics**: Only collect metrics that provide value

## Example Output

```
counter_increment       time:   [11.332 ns 11.363 ns 11.396 ns]
gauge_set               time:   [9.247 ns 9.275 ns 9.308 ns]
histogram_observe       time:   [52.123 ns 52.567 ns 53.012 ns]
per_task_type_counter   time:   [89.456 ns 90.123 ns 90.789 ns]
health_check           time:   [2.345 μs 2.389 μs 2.433 μs]
gather_metrics         time:   [23.456 μs 24.123 μs 24.789 μs]
```

## CI Integration

To run benchmarks in CI and compare against baseline:

```bash
# Save baseline
cargo bench -- --save-baseline main

# Compare against baseline
cargo bench -- --baseline main
```

## Profiling

For detailed profiling:

```bash
# Install flamegraph
cargo install flamegraph

# Profile specific benchmark
cargo flamegraph --bench metrics_bench -- --bench counter_increment
```
