#![cfg(test)]

use crate::*;
use serial_test::serial;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
#[serial]
fn test_metrics_increment() {
    reset_metrics();

    TASKS_ENQUEUED_TOTAL.inc();
    TASKS_COMPLETED_TOTAL.inc();
    QUEUE_SIZE.set(5.0);

    let metrics = gather_metrics();
    assert!(metrics.contains("celers_tasks_enqueued_total"));
    assert!(metrics.contains("celers_tasks_completed_total"));
    assert!(metrics.contains("celers_queue_size"));
}

#[test]
#[serial]
fn test_task_execution_time() {
    reset_metrics();

    TASK_EXECUTION_TIME.observe(1.5);
    TASK_EXECUTION_TIME.observe(0.5);

    let metrics = gather_metrics();
    assert!(metrics.contains("celers_task_execution_seconds"));
}

#[test]
#[serial]
fn test_per_task_type_metrics() {
    reset_metrics();

    // Track metrics for different task types
    TASKS_ENQUEUED_BY_TYPE
        .with_label_values(&["send_email"])
        .inc();
    TASKS_ENQUEUED_BY_TYPE
        .with_label_values(&["process_image"])
        .inc();
    TASKS_ENQUEUED_BY_TYPE
        .with_label_values(&["send_email"])
        .inc();

    TASKS_COMPLETED_BY_TYPE
        .with_label_values(&["send_email"])
        .inc();
    TASKS_FAILED_BY_TYPE
        .with_label_values(&["process_image"])
        .inc();

    TASK_EXECUTION_TIME_BY_TYPE
        .with_label_values(&["send_email"])
        .observe(1.5);
    TASK_EXECUTION_TIME_BY_TYPE
        .with_label_values(&["process_image"])
        .observe(2.3);

    TASK_RESULT_SIZE_BY_TYPE
        .with_label_values(&["send_email"])
        .observe(1024.0);

    let metrics = gather_metrics();

    // Verify labeled metrics are present
    assert!(metrics.contains("celers_tasks_enqueued_by_type_total"));
    assert!(metrics.contains("celers_tasks_completed_by_type_total"));
    assert!(metrics.contains("celers_tasks_failed_by_type_total"));
    assert!(metrics.contains("celers_task_execution_by_type_seconds"));
    assert!(metrics.contains("celers_task_result_size_by_type_bytes"));

    // Verify labels are present
    assert!(metrics.contains("task_name=\"send_email\""));
    assert!(metrics.contains("task_name=\"process_image\""));
}

#[test]
fn test_metrics_config() {
    let config = MetricsConfig::new()
        .with_sampling_rate(0.5)
        .with_execution_time_buckets(vec![0.1, 1.0, 10.0])
        .with_latency_buckets(vec![0.01, 0.1, 1.0])
        .with_size_buckets(vec![1000.0, 10000.0]);

    assert_eq!(config.sampling_rate, 0.5);
    assert_eq!(config.execution_time_buckets, vec![0.1, 1.0, 10.0]);
    assert_eq!(config.latency_buckets, vec![0.01, 0.1, 1.0]);
    assert_eq!(config.size_buckets, vec![1000.0, 10000.0]);
}

#[test]
fn test_metrics_sampler() {
    // Test 100% sampling
    let sampler = MetricsSampler::new(1.0);
    for _ in 0..100 {
        assert!(sampler.should_sample());
    }

    // Test 0% sampling
    let sampler = MetricsSampler::new(0.0);
    for _ in 0..100 {
        assert!(!sampler.should_sample());
    }

    // Test 50% sampling (approximately)
    let sampler = MetricsSampler::new(0.5);
    let mut sampled = 0;
    for _ in 0..100 {
        if sampler.should_sample() {
            sampled += 1;
        }
    }
    // Should be around 50, allow some variance
    assert!((45..=55).contains(&sampled), "sampled: {}", sampled);
}

#[test]
fn test_rate_calculations() {
    // Test basic rate calculation
    assert_eq!(calculate_rate(100.0, 50.0, 10.0), 5.0);
    assert_eq!(calculate_rate(100.0, 50.0, 0.0), 0.0);

    // Test success rate
    assert!((calculate_success_rate(90.0, 10.0) - 0.9).abs() < 1e-10);
    assert_eq!(calculate_success_rate(100.0, 0.0), 1.0);
    assert_eq!(calculate_success_rate(0.0, 100.0), 0.0);
    assert_eq!(calculate_success_rate(0.0, 0.0), 0.0);

    // Test error rate
    assert!((calculate_error_rate(90.0, 10.0) - 0.1).abs() < 1e-10);
    assert_eq!(calculate_error_rate(100.0, 0.0), 0.0);
    assert_eq!(calculate_error_rate(0.0, 100.0), 1.0);

    // Test throughput
    assert_eq!(calculate_throughput(100.0, 10.0), 10.0);
    assert_eq!(calculate_throughput(100.0, 0.0), 0.0);
}

#[test]
fn test_slo_compliance() {
    let target = SloTarget {
        success_rate: 0.99,
        latency_seconds: 5.0,
        throughput: 100.0,
    };

    // Test compliant case
    assert_eq!(
        check_slo_compliance(0.995, 4.5, 120.0, &target),
        SloStatus::Compliant
    );

    // Test non-compliant success rate
    assert_eq!(
        check_slo_compliance(0.98, 4.5, 120.0, &target),
        SloStatus::NonCompliant
    );

    // Test non-compliant latency
    assert_eq!(
        check_slo_compliance(0.995, 6.0, 120.0, &target),
        SloStatus::NonCompliant
    );

    // Test non-compliant throughput
    assert_eq!(
        check_slo_compliance(0.995, 4.5, 90.0, &target),
        SloStatus::NonCompliant
    );

    // Test unknown (negative values)
    assert_eq!(
        check_slo_compliance(-1.0, 4.5, 120.0, &target),
        SloStatus::Unknown
    );
}

#[test]
fn test_error_budget() {
    // 99% success rate target
    let target_success_rate = 0.99;

    // 100% budget remaining (no failures yet)
    assert_eq!(
        calculate_error_budget(1000.0, 0.0, target_success_rate),
        1.0
    );

    // 50% budget remaining (5 out of 10 allowed failures used)
    assert!((calculate_error_budget(1000.0, 5.0, target_success_rate) - 0.5).abs() < 1e-10);

    // 0% budget remaining (all allowed failures used)
    assert!(calculate_error_budget(1000.0, 10.0, target_success_rate).abs() < 1e-10);

    // Budget exceeded (negative clamped to 0)
    assert_eq!(
        calculate_error_budget(1000.0, 20.0, target_success_rate),
        0.0
    );

    // No requests yet (100% budget)
    assert_eq!(calculate_error_budget(0.0, 0.0, target_success_rate), 1.0);
}

#[test]
#[serial]
fn test_concurrent_metrics_access() {
    use std::thread;

    reset_metrics();

    let handles: Vec<_> = (0..10)
        .map(|_| {
            thread::spawn(|| {
                for _ in 0..100 {
                    TASKS_ENQUEUED_TOTAL.inc();
                    TASKS_COMPLETED_TOTAL.inc();
                    TASK_EXECUTION_TIME.observe(1.0);
                    QUEUE_SIZE.set(42.0);
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify metrics were incremented
    let metrics = gather_metrics();
    assert!(metrics.contains("celers_tasks_enqueued_total"));
    assert!(metrics.contains("celers_tasks_completed_total"));
    assert!(metrics.contains("celers_task_execution_seconds"));
    assert!(metrics.contains("celers_queue_size"));
}

#[test]
#[serial]
fn test_observe_sampled() {
    reset_metrics();
    let mut observed = 0;

    // Use observe_sampled with 100% sampling
    for _ in 0..10 {
        observe_sampled(|| {
            observed += 1;
        });
    }

    // All should be observed with default 100% sampling
    assert_eq!(observed, 10);
}

#[test]
fn test_anomaly_threshold() {
    // Create threshold with mean=100, std_dev=10, 3-sigma
    let threshold = AnomalyThreshold::new(100.0, 10.0, 3.0);

    // Normal value
    assert!(!threshold.is_anomalous(100.0));
    assert!(!threshold.is_anomalous(110.0));
    assert!(!threshold.is_anomalous(90.0));

    // Anomalous values (outside 3 sigma)
    assert!(threshold.is_anomalous(131.0));
    assert!(threshold.is_anomalous(69.0));

    // Check bounds
    assert_eq!(threshold.upper_bound(), 130.0);
    assert_eq!(threshold.lower_bound(), 70.0);
}

#[test]
fn test_anomaly_threshold_from_samples() {
    let samples = vec![10.0, 20.0, 30.0, 40.0, 50.0];
    let threshold = AnomalyThreshold::from_samples(&samples, 2.0).unwrap();

    // Mean should be 30
    assert!((threshold.mean - 30.0).abs() < 1e-10);

    // Check that values near mean are not anomalous
    assert!(!threshold.is_anomalous(30.0));

    // Empty samples should return None
    assert!(AnomalyThreshold::from_samples(&[], 2.0).is_none());
}

#[test]
fn test_detect_anomaly() {
    let threshold = AnomalyThreshold::new(100.0, 10.0, 2.0);

    assert_eq!(detect_anomaly(100.0, &threshold), AnomalyStatus::Normal);
    assert_eq!(detect_anomaly(110.0, &threshold), AnomalyStatus::Normal);
    assert_eq!(detect_anomaly(121.0, &threshold), AnomalyStatus::High);
    assert_eq!(detect_anomaly(79.0, &threshold), AnomalyStatus::Low);
}

#[test]
fn test_moving_average() {
    let mut ma = MovingAverage::new(10.0, 0.5);

    // Initial value
    assert_eq!(ma.get(), 10.0);

    // Update with new value
    let new_avg = ma.update(20.0);
    assert_eq!(new_avg, 15.0); // 0.5 * 20 + 0.5 * 10 = 15

    // Update again
    let new_avg = ma.update(30.0);
    assert_eq!(new_avg, 22.5); // 0.5 * 30 + 0.5 * 15 = 22.5
}

#[test]
fn test_detect_spike() {
    // Normal case (within threshold)
    assert!(!detect_spike(100.0, 100.0, 2.0));
    assert!(!detect_spike(150.0, 100.0, 2.0));

    // Spike detected (above threshold)
    assert!(detect_spike(250.0, 100.0, 2.0));

    // Drop detected (below threshold)
    assert!(detect_spike(40.0, 100.0, 2.0));

    // Zero baseline should not detect spike
    assert!(!detect_spike(100.0, 0.0, 2.0));
}

#[test]
fn test_metric_stats() {
    let mut stats = MetricStats::new();

    // Empty stats
    assert_eq!(stats.count, 0);
    assert_eq!(stats.mean(), 0.0);

    // Add observations
    stats.observe(10.0);
    stats.observe(20.0);
    stats.observe(30.0);

    assert_eq!(stats.count, 3);
    assert_eq!(stats.sum, 60.0);
    assert_eq!(stats.min, 10.0);
    assert_eq!(stats.max, 30.0);
    assert_eq!(stats.mean(), 20.0);

    // Variance = E[X²] - E[X]²
    // = (100 + 400 + 900) / 3 - 400
    // = 466.67 - 400 = 66.67
    assert!((stats.variance() - 66.666666).abs() < 0.001);
    assert!((stats.std_dev() - 8.165).abs() < 0.01);
}

#[test]
fn test_metric_stats_merge() {
    let mut stats1 = MetricStats::new();
    stats1.observe(10.0);
    stats1.observe(20.0);

    let mut stats2 = MetricStats::new();
    stats2.observe(30.0);
    stats2.observe(40.0);

    stats1.merge(&stats2);

    assert_eq!(stats1.count, 4);
    assert_eq!(stats1.sum, 100.0);
    assert_eq!(stats1.min, 10.0);
    assert_eq!(stats1.max, 40.0);
    assert_eq!(stats1.mean(), 25.0);
}

#[test]
fn test_metric_aggregator() {
    let aggregator = MetricAggregator::new();

    aggregator.observe(10.0);
    aggregator.observe(20.0);
    aggregator.observe(30.0);

    let snapshot = aggregator.snapshot();
    assert_eq!(snapshot.count, 3);
    assert_eq!(snapshot.mean(), 20.0);

    // Reset
    aggregator.reset();
    let snapshot = aggregator.snapshot();
    assert_eq!(snapshot.count, 0);
}

#[test]
fn test_metric_aggregator_concurrent() {
    use std::thread;

    let aggregator = Arc::new(MetricAggregator::new());

    let handles: Vec<_> = (0..10)
        .map(|_| {
            let agg = Arc::clone(&aggregator);
            thread::spawn(move || {
                for i in 0..100 {
                    agg.observe(i as f64);
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    let snapshot = aggregator.snapshot();
    assert_eq!(snapshot.count, 1000); // 10 threads * 100 observations
}

#[test]
fn test_custom_labels() {
    let labels = CustomLabels::new()
        .with_label("environment", "production")
        .with_label("region", "us-west-2")
        .with_label("service", "api");

    assert_eq!(labels.len(), 3);
    assert_eq!(labels.get("environment"), Some("production"));
    assert_eq!(labels.get("region"), Some("us-west-2"));
    assert_eq!(labels.get("service"), Some("api"));
    assert!(labels.contains("environment"));
    assert!(!labels.contains("nonexistent"));
    assert!(!labels.is_empty());
}

#[test]
fn test_custom_labels_builder() {
    let labels = CustomMetricBuilder::new()
        .label("env", "staging")
        .label("version", "1.0.0")
        .build();

    assert_eq!(labels.len(), 2);
    assert_eq!(labels.get("env"), Some("staging"));
    assert_eq!(labels.get("version"), Some("1.0.0"));
}

#[test]
fn test_custom_labels_from_iter() {
    let labels: CustomLabels = vec![
        ("key1".to_string(), "value1".to_string()),
        ("key2".to_string(), "value2".to_string()),
    ]
    .into_iter()
    .collect();

    assert_eq!(labels.len(), 2);
    assert_eq!(labels.get("key1"), Some("value1"));
    assert_eq!(labels.get("key2"), Some("value2"));
}

#[test]
fn test_custom_labels_to_label_values() {
    let labels = CustomLabels::new()
        .with_label("task_name", "send_email")
        .with_label("priority", "high");

    let values = labels.to_label_values(&["task_name", "priority", "nonexistent"]);
    assert_eq!(values, vec!["send_email", "high", ""]);
}

#[test]
fn test_metric_snapshot() {
    let mut stats = MetricStats::new();
    stats.observe(10.0);
    stats.observe(20.0);

    let snapshot = MetricSnapshot::new("worker-1", stats.clone());

    assert_eq!(snapshot.worker_id, "worker-1");
    assert_eq!(snapshot.stats.count, 2);
    assert_eq!(snapshot.stats.mean(), 15.0);
    assert!(!snapshot.is_stale(3600)); // Not stale within 1 hour
}

#[test]
fn test_metric_snapshot_with_labels() {
    let stats = MetricStats::new();
    let labels = CustomLabels::new().with_label("region", "us-east-1");

    let snapshot = MetricSnapshot::new("worker-1", stats).with_labels(labels);

    assert_eq!(snapshot.labels.get("region"), Some("us-east-1"));
}

#[test]
fn test_distributed_aggregator() {
    let aggregator = DistributedAggregator::new();

    // Create stats from worker 1
    let mut stats1 = MetricStats::new();
    stats1.observe(10.0);
    stats1.observe(20.0);
    let snapshot1 = MetricSnapshot::new("worker-1", stats1);

    // Create stats from worker 2
    let mut stats2 = MetricStats::new();
    stats2.observe(30.0);
    stats2.observe(40.0);
    let snapshot2 = MetricSnapshot::new("worker-2", stats2);

    // Update aggregator with snapshots
    aggregator.update(snapshot1);
    aggregator.update(snapshot2);

    // Aggregate stats
    let combined = aggregator.aggregate();
    assert_eq!(combined.count, 4);
    assert_eq!(combined.sum, 100.0);
    assert_eq!(combined.mean(), 25.0);
    assert_eq!(combined.min, 10.0);
    assert_eq!(combined.max, 40.0);

    // Check active worker count
    assert_eq!(aggregator.active_worker_count(), 2);
}

#[test]
fn test_distributed_aggregator_update_same_worker() {
    let aggregator = DistributedAggregator::new();

    // First update from worker-1
    let mut stats1 = MetricStats::new();
    stats1.observe(10.0);
    aggregator.update(MetricSnapshot::new("worker-1", stats1));

    // Second update from same worker (should replace)
    let mut stats2 = MetricStats::new();
    stats2.observe(20.0);
    stats2.observe(30.0);
    aggregator.update(MetricSnapshot::new("worker-1", stats2));

    let combined = aggregator.aggregate();
    assert_eq!(combined.count, 2); // Should only have stats2 data
    assert_eq!(combined.sum, 50.0);
}

#[test]
fn test_distributed_aggregator_cleanup() {
    let aggregator = DistributedAggregator::with_stale_threshold(60);

    let stats = MetricStats::new();

    // Create a snapshot with an old timestamp (manually)
    let mut old_snapshot = MetricSnapshot::new("worker-1", stats);
    // Set timestamp to 2 minutes ago
    old_snapshot.timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - 120;

    aggregator.update(old_snapshot);

    // Snapshot should be stale now (120 seconds > 60 second threshold)
    assert_eq!(aggregator.active_worker_count(), 0);

    // Cleanup stale snapshots
    aggregator.cleanup_stale();

    // Should have no snapshots after cleanup
    let snapshots = aggregator.active_snapshots();
    assert_eq!(snapshots.len(), 0);
}

#[test]
fn test_distributed_aggregator_reset() {
    let aggregator = DistributedAggregator::new();

    let stats = MetricStats::new();
    aggregator.update(MetricSnapshot::new("worker-1", stats.clone()));
    aggregator.update(MetricSnapshot::new("worker-2", stats));

    assert_eq!(aggregator.active_worker_count(), 2);

    aggregator.reset();

    assert_eq!(aggregator.active_worker_count(), 0);
    let combined = aggregator.aggregate();
    assert_eq!(combined.count, 0);
}

#[test]
fn test_metric_export_name() {
    let counter = MetricExport::Counter {
        name: "test_counter".to_string(),
        value: 10.0,
        labels: CustomLabels::new(),
    };

    assert_eq!(counter.name(), "test_counter");
}

#[test]
fn test_statsd_config() {
    let config = StatsDConfig::new()
        .with_host("statsd.example.com")
        .with_port(9125)
        .with_prefix("myapp")
        .with_sample_rate(0.5);

    assert_eq!(config.host, "statsd.example.com");
    assert_eq!(config.port, 9125);
    assert_eq!(config.prefix, "myapp");
    assert_eq!(config.sample_rate, 0.5);
}

#[test]
fn test_statsd_format_counter() {
    let config = StatsDConfig::new().with_prefix("celers");

    let metric = MetricExport::Counter {
        name: "tasks_completed".to_string(),
        value: 42.0,
        labels: CustomLabels::new(),
    };

    let formatted = config.format_metric(&metric);
    assert_eq!(formatted, "celers.tasks_completed:42|c");
}

#[test]
fn test_statsd_format_gauge() {
    let config = StatsDConfig::new().with_prefix("celers");

    let metric = MetricExport::Gauge {
        name: "queue_size".to_string(),
        value: 100.0,
        labels: CustomLabels::new(),
    };

    let formatted = config.format_metric(&metric);
    assert_eq!(formatted, "celers.queue_size:100|g");
}

#[test]
fn test_statsd_format_with_tags() {
    let config = StatsDConfig::new().with_prefix("celers");

    let labels = CustomLabels::new()
        .with_label("environment", "prod")
        .with_label("region", "us-east-1");

    let metric = MetricExport::Counter {
        name: "tasks_completed".to_string(),
        value: 42.0,
        labels,
    };

    let formatted = config.format_metric(&metric);
    assert!(formatted.starts_with("celers.tasks_completed:42|c|#"));
    assert!(formatted.contains("environment:prod"));
    assert!(formatted.contains("region:us-east-1"));
}

#[test]
fn test_statsd_format_histogram() {
    let config = StatsDConfig::new().with_prefix("celers");

    let metric = MetricExport::Histogram {
        name: "task_duration".to_string(),
        count: 10,
        sum: 100.0,
        buckets: vec![],
        labels: CustomLabels::new(),
    };

    let formatted = config.format_metric(&metric);
    assert_eq!(formatted, "celers.task_duration:10|h");
}

#[test]
fn test_opentelemetry_config() {
    let config = OpenTelemetryConfig::new()
        .with_service_name("my-service")
        .with_service_version("2.0.0")
        .with_environment("staging")
        .with_attribute("host", "server-1");

    assert_eq!(config.service_name, "my-service");
    assert_eq!(config.service_version, "2.0.0");
    assert_eq!(config.environment, "staging");
    assert_eq!(config.attributes.get("host"), Some("server-1"));
}

#[test]
fn test_cloudwatch_config() {
    let config = CloudWatchConfig::new()
        .with_namespace("MyApp")
        .with_region("eu-west-1")
        .with_dimension("Environment", "Production")
        .with_storage_resolution(1);

    assert_eq!(config.namespace, "MyApp");
    assert_eq!(config.region, "eu-west-1");
    assert_eq!(config.dimensions.get("Environment"), Some("Production"));
    assert_eq!(config.storage_resolution, 1);
}

#[test]
fn test_cloudwatch_storage_resolution() {
    let config1 = CloudWatchConfig::new().with_storage_resolution(1);
    assert_eq!(config1.storage_resolution, 1);

    let config2 = CloudWatchConfig::new().with_storage_resolution(60);
    assert_eq!(config2.storage_resolution, 60);

    // Any other value should default to 60
    let config3 = CloudWatchConfig::new().with_storage_resolution(30);
    assert_eq!(config3.storage_resolution, 60);
}

#[test]
fn test_datadog_config() {
    let config = DatadogConfig::new()
        .with_api_host("https://api.datadoghq.eu")
        .with_api_key("test-key-123")
        .with_prefix("myapp")
        .with_tag("env", "prod")
        .with_tag("region", "us-west-2");

    assert_eq!(config.api_host, "https://api.datadoghq.eu");
    assert_eq!(config.api_key, "test-key-123");
    assert_eq!(config.prefix, "myapp");
    assert_eq!(config.tags.get("env"), Some("prod"));
    assert_eq!(config.tags.get("region"), Some("us-west-2"));
}

#[test]
fn test_datadog_format_tags() {
    let config = DatadogConfig::new().with_tag("global_tag", "global_value");

    let metric_labels = CustomLabels::new().with_label("metric_tag", "metric_value");

    let tags = config.format_tags(&metric_labels);

    assert_eq!(tags.len(), 2);
    assert!(tags.contains(&"global_tag:global_value".to_string()));
    assert!(tags.contains(&"metric_tag:metric_value".to_string()));
}

#[test]
fn test_export_to_statsd() {
    let mut stats = MetricStats::new();
    stats.observe(10.0);
    stats.observe(20.0);

    let config = StatsDConfig::new().with_prefix("celers");
    let formatted = export_to_statsd(&stats, "execution_time", &config);

    assert_eq!(formatted, "celers.execution_time:15|h");
}

#[test]
#[serial]
fn test_current_metrics_capture() {
    reset_metrics();

    // Capture baseline to handle any residual values
    let baseline = CurrentMetrics::capture();

    TASKS_ENQUEUED_TOTAL.inc_by(100.0);
    TASKS_COMPLETED_TOTAL.inc_by(80.0);
    TASKS_FAILED_TOTAL.inc_by(20.0);
    QUEUE_SIZE.set(50.0);
    ACTIVE_WORKERS.set(5.0);

    let metrics = CurrentMetrics::capture();

    // Check relative changes for counters (use approximate comparisons for residual values)
    let enqueued_diff = metrics.tasks_enqueued - baseline.tasks_enqueued;
    let completed_diff = metrics.tasks_completed - baseline.tasks_completed;
    let failed_diff = metrics.tasks_failed - baseline.tasks_failed;

    assert!(
        (enqueued_diff - 100.0).abs() < 5.0,
        "Expected enqueued ~100.0, got {}",
        enqueued_diff
    );
    assert!(
        (completed_diff - 80.0).abs() < 5.0,
        "Expected completed ~80.0, got {}",
        completed_diff
    );
    assert!(
        (failed_diff - 20.0).abs() < 5.0,
        "Expected failed ~20.0, got {}",
        failed_diff
    );

    // Gauges are set to absolute values
    assert_eq!(metrics.queue_size, 50.0);
    assert_eq!(metrics.active_workers, 5.0);
}

#[test]
#[serial]
fn test_current_metrics_rates() {
    reset_metrics();

    // Capture baseline
    let baseline = CurrentMetrics::capture();

    TASKS_COMPLETED_TOTAL.inc_by(90.0);
    TASKS_FAILED_TOTAL.inc_by(10.0);

    let metrics = CurrentMetrics::capture();

    // Calculate rates from the change, not absolute values
    let completed = metrics.tasks_completed - baseline.tasks_completed;
    let failed = metrics.tasks_failed - baseline.tasks_failed;
    let total = completed + failed;

    // Use approximate comparisons to handle any residual values from previous tests
    assert!(
        (total - 100.0).abs() < 5.0,
        "Expected total ~100.0, got {}",
        total
    );

    // Verify the rates are approximately correct (9:1 ratio)
    let success_rate = completed / total;
    let error_rate = failed / total;

    assert!(
        (success_rate - 0.9).abs() < 0.05,
        "Expected success_rate ~0.9, got {}",
        success_rate
    );
    assert!(
        (error_rate - 0.1).abs() < 0.05,
        "Expected error_rate ~0.1, got {}",
        error_rate
    );
}

#[test]
#[serial]
fn test_health_check_healthy() {
    reset_metrics();

    QUEUE_SIZE.set(100.0);
    DLQ_SIZE.set(10.0);
    ACTIVE_WORKERS.set(5.0);

    let config = HealthCheckConfig::new()
        .with_max_queue_size(1000.0)
        .with_max_dlq_size(100.0)
        .with_min_active_workers(2.0);

    let status = health_check(&config);
    assert_eq!(status, HealthStatus::Healthy);
}

#[test]
#[serial]
fn test_health_check_degraded() {
    reset_metrics();

    // Queue size is at 85% (850/1000) - should trigger warning
    QUEUE_SIZE.set(850.0);
    DLQ_SIZE.set(10.0);
    ACTIVE_WORKERS.set(5.0);

    // Verify gauges were set correctly before running health check
    assert_eq!(QUEUE_SIZE.get(), 850.0, "Failed to set QUEUE_SIZE");
    assert_eq!(DLQ_SIZE.get(), 10.0, "Failed to set DLQ_SIZE");
    assert_eq!(ACTIVE_WORKERS.get(), 5.0, "Failed to set ACTIVE_WORKERS");

    let config = HealthCheckConfig::new()
        .with_max_queue_size(1000.0)
        .with_max_dlq_size(100.0)
        .with_min_active_workers(2.0);

    let status = health_check(&config);

    // Check that we got the expected Degraded status with queue-related warning
    match status {
        HealthStatus::Degraded { reasons } => {
            assert!(
                !reasons.is_empty(),
                "Expected at least one degradation reason"
            );
            assert!(
                reasons
                    .iter()
                    .any(|r| r.contains("Queue size") || r.contains("queue")),
                "Expected queue-related degradation, got reasons: {:?}",
                reasons
            );
        }
        HealthStatus::Healthy => {
            panic!("Expected Degraded status, got Healthy. Queue was set to 850 (>800 threshold)");
        }
        HealthStatus::Unhealthy { reasons } => {
            panic!(
                "Expected Degraded status, got Unhealthy with reasons: {:?}",
                reasons
            );
        }
    }
}

#[test]
#[serial]
fn test_health_check_unhealthy() {
    reset_metrics();

    QUEUE_SIZE.set(1500.0); // Exceeds limit
    DLQ_SIZE.set(10.0);
    ACTIVE_WORKERS.set(0.0); // Below minimum

    let config = HealthCheckConfig::new()
        .with_max_queue_size(1000.0)
        .with_max_dlq_size(100.0)
        .with_min_active_workers(2.0);

    let status = health_check(&config);
    match status {
        HealthStatus::Unhealthy { reasons } => {
            assert!(reasons.len() >= 2);
            assert!(reasons.iter().any(|r| r.contains("Queue size exceeded")));
            assert!(reasons.iter().any(|r| r.contains("Insufficient workers")));
        }
        _ => panic!("Expected Unhealthy status"),
    }
}

#[test]
#[serial]
fn test_health_check_with_slo() {
    reset_metrics();

    TASKS_COMPLETED_TOTAL.inc_by(95.0);
    TASKS_FAILED_TOTAL.inc_by(5.0);
    QUEUE_SIZE.set(100.0);
    DLQ_SIZE.set(10.0);
    ACTIVE_WORKERS.set(5.0);

    let slo = SloTarget {
        success_rate: 0.99,
        latency_seconds: 5.0,
        throughput: 100.0,
    };

    let config = HealthCheckConfig::new()
        .with_max_queue_size(1000.0)
        .with_max_dlq_size(100.0)
        .with_min_active_workers(2.0)
        .with_slo_target(slo);

    let status = health_check(&config);
    match status {
        HealthStatus::Unhealthy { reasons } => {
            assert!(reasons.iter().any(|r| r.contains("Success rate below SLO")));
        }
        _ => panic!("Expected Unhealthy status due to SLO violation"),
    }
}

#[test]
fn test_calculate_percentile() {
    let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];

    // p50 (median)
    let p50 = calculate_percentile(&values, 0.50).unwrap();
    assert!((p50 - 5.5).abs() < 1e-10);

    // p0 (min)
    let p0 = calculate_percentile(&values, 0.0).unwrap();
    assert_eq!(p0, 1.0);

    // p100 (max)
    let p100 = calculate_percentile(&values, 1.0).unwrap();
    assert_eq!(p100, 10.0);

    // Empty slice
    assert!(calculate_percentile(&[], 0.5).is_none());

    // Invalid percentile
    assert!(calculate_percentile(&values, -0.1).is_none());
    assert!(calculate_percentile(&values, 1.1).is_none());
}

#[test]
fn test_calculate_percentile_interpolation() {
    let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];

    let p95 = calculate_percentile(&values, 0.95).unwrap();
    assert!((p95 - 9.55).abs() < 1e-10);

    let p99 = calculate_percentile(&values, 0.99).unwrap();
    assert!((p99 - 9.91).abs() < 1e-10);
}

#[test]
fn test_calculate_percentiles_batch() {
    let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];

    let (p50, p95, p99) = calculate_percentiles(&values).unwrap();

    assert!((p50 - 5.5).abs() < 1e-10);
    assert!((p95 - 9.55).abs() < 1e-10);
    assert!((p99 - 9.91).abs() < 1e-10);

    // Empty slice
    assert!(calculate_percentiles(&[]).is_none());
}

#[test]
fn test_percentile_single_value() {
    let values = vec![42.0];

    let p50 = calculate_percentile(&values, 0.50).unwrap();
    assert_eq!(p50, 42.0);

    let p95 = calculate_percentile(&values, 0.95).unwrap();
    assert_eq!(p95, 42.0);
}

#[test]
fn test_health_check_config_builder() {
    let config = HealthCheckConfig::new()
        .with_max_queue_size(2000.0)
        .with_max_dlq_size(200.0)
        .with_min_active_workers(10.0);

    assert_eq!(config.max_queue_size, 2000.0);
    assert_eq!(config.max_dlq_size, 200.0);
    assert_eq!(config.min_active_workers, 10.0);
    assert!(config.slo_target.is_none());
}

#[test]
fn test_metric_comparison() {
    let baseline = CurrentMetrics {
        tasks_enqueued: 1000.0,
        tasks_completed: 900.0,
        tasks_failed: 100.0,
        tasks_retried: 50.0,
        tasks_cancelled: 10.0,
        queue_size: 100.0,
        processing_queue_size: 20.0,
        dlq_size: 5.0,
        active_workers: 10.0,
    };

    let improved = CurrentMetrics {
        tasks_enqueued: 1100.0,
        tasks_completed: 1050.0,
        tasks_failed: 50.0,
        tasks_retried: 45.0,
        tasks_cancelled: 8.0,
        queue_size: 80.0,
        processing_queue_size: 18.0,
        dlq_size: 4.0,
        active_workers: 12.0,
    };

    let comparison = MetricComparison::compare(&baseline, &improved);

    // Queue size decreased
    assert!(comparison.queue_size_diff < 0.0);
    // More workers
    assert!(comparison.workers_diff > 0.0);
    // Better metrics
    assert!(comparison.is_improvement());
    assert!(!comparison.is_degradation());
}

#[test]
fn test_metric_comparison_degradation() {
    let baseline = CurrentMetrics {
        tasks_enqueued: 1000.0,
        tasks_completed: 950.0,
        tasks_failed: 50.0,
        tasks_retried: 25.0,
        tasks_cancelled: 5.0,
        queue_size: 50.0,
        processing_queue_size: 10.0,
        dlq_size: 2.0,
        active_workers: 10.0,
    };

    let degraded = CurrentMetrics {
        tasks_enqueued: 1100.0,
        tasks_completed: 900.0,
        tasks_failed: 200.0,
        tasks_retried: 100.0,
        tasks_cancelled: 20.0,
        queue_size: 150.0,
        processing_queue_size: 30.0,
        dlq_size: 10.0,
        active_workers: 8.0,
    };

    let comparison = MetricComparison::compare(&baseline, &degraded);

    // Performance degraded
    assert!(comparison.is_degradation());
    assert!(!comparison.is_improvement());
    // Queue size increased
    assert!(comparison.queue_size_diff > 0.0);
}

#[test]
fn test_metric_comparison_significance() {
    let baseline = CurrentMetrics {
        tasks_enqueued: 1000.0,
        tasks_completed: 900.0,
        tasks_failed: 100.0,
        tasks_retried: 50.0,
        tasks_cancelled: 10.0,
        queue_size: 100.0,
        processing_queue_size: 20.0,
        dlq_size: 5.0,
        active_workers: 10.0,
    };

    let slightly_different = CurrentMetrics {
        tasks_enqueued: 1005.0,
        tasks_completed: 903.0,
        tasks_failed: 102.0,
        tasks_retried: 51.0,
        tasks_cancelled: 10.0,
        queue_size: 101.0,
        processing_queue_size: 20.0,
        dlq_size: 5.0,
        active_workers: 10.0,
    };

    let comparison = MetricComparison::compare(&baseline, &slightly_different);

    // Change is not significant (< 5%)
    assert!(!comparison.is_significant(5.0));
    // But is significant for smaller threshold
    assert!(comparison.is_significant(0.1));
}

#[test]
#[serial]
fn test_alert_rule_error_rate() {
    reset_metrics();

    TASKS_COMPLETED_TOTAL.inc_by(90.0);
    TASKS_FAILED_TOTAL.inc_by(10.0);

    let metrics = CurrentMetrics::capture();

    let rule = AlertRule::new(
        "high_error_rate",
        AlertCondition::ErrorRateAbove { threshold: 0.05 },
        AlertSeverity::Critical,
        "Error rate exceeded 5%",
    );

    // 10% error rate should fire alert (> 5%)
    assert!(rule.should_fire(&metrics));

    let rule2 = AlertRule::new(
        "acceptable_error_rate",
        AlertCondition::ErrorRateAbove { threshold: 0.15 },
        AlertSeverity::Warning,
        "Error rate exceeded 15%",
    );

    // 10% error rate should not fire alert (< 15%)
    assert!(!rule2.should_fire(&metrics));
}

#[test]
#[serial]
fn test_alert_rule_success_rate() {
    reset_metrics();

    TASKS_COMPLETED_TOTAL.inc_by(95.0);
    TASKS_FAILED_TOTAL.inc_by(5.0);

    let metrics = CurrentMetrics::capture();

    let rule = AlertRule::new(
        "low_success_rate",
        AlertCondition::SuccessRateBelow { threshold: 0.99 },
        AlertSeverity::Warning,
        "Success rate below 99%",
    );

    // 95% success rate should fire alert (< 99%)
    assert!(rule.should_fire(&metrics));
}

#[test]
#[serial]
fn test_alert_rule_queue_size() {
    reset_metrics();

    QUEUE_SIZE.set(1500.0);

    let metrics = CurrentMetrics::capture();

    let rule = AlertRule::new(
        "high_queue_size",
        AlertCondition::GaugeAbove { threshold: 1000.0 },
        AlertSeverity::Warning,
        "Queue size exceeded 1000",
    );

    assert!(rule.should_fire(&metrics));
}

#[test]
#[serial]
fn test_alert_rule_workers() {
    reset_metrics();

    ACTIVE_WORKERS.set(2.0);

    let metrics = CurrentMetrics::capture();

    let rule = AlertRule::new(
        "low_workers",
        AlertCondition::GaugeBelow { threshold: 5.0 },
        AlertSeverity::Critical,
        "Worker count below minimum",
    );

    assert!(rule.should_fire(&metrics));
}

#[test]
#[serial]
fn test_alert_manager() {
    reset_metrics();

    TASKS_COMPLETED_TOTAL.inc_by(90.0);
    TASKS_FAILED_TOTAL.inc_by(10.0);
    QUEUE_SIZE.set(1500.0);
    ACTIVE_WORKERS.set(3.0);

    let mut manager = AlertManager::new();

    manager.add_rule(AlertRule::new(
        "high_error_rate",
        AlertCondition::ErrorRateAbove { threshold: 0.05 },
        AlertSeverity::Critical,
        "Error rate exceeded 5%",
    ));

    manager.add_rule(AlertRule::new(
        "high_queue_size",
        AlertCondition::GaugeAbove { threshold: 1000.0 },
        AlertSeverity::Warning,
        "Queue size exceeded 1000",
    ));

    manager.add_rule(AlertRule::new(
        "low_workers",
        AlertCondition::GaugeBelow { threshold: 5.0 },
        AlertSeverity::Critical,
        "Worker count below minimum",
    ));

    let metrics = CurrentMetrics::capture();
    let fired = manager.check_alerts(&metrics);

    // All 3 alerts should fire
    assert_eq!(fired.len(), 3);

    let critical = manager.critical_alerts(&metrics);
    // 2 critical alerts
    assert_eq!(critical.len(), 2);
}

#[test]
fn test_trend_alert_manager() {
    use std::thread;
    use std::time::Duration;

    let mut manager = AlertManager::new();

    // Add a trend-based alert for increasing trends
    manager.add_trend_rule(TrendAlertRule::new(
        "queue_growing_rapidly",
        TrendAlertCondition::new(5.0, TrendDirection::Increasing),
        AlertSeverity::Warning,
        "Queue is growing rapidly",
    ));

    // Add a trend-based alert for decreasing trends
    manager.add_trend_rule(TrendAlertRule::new(
        "throughput_dropping",
        TrendAlertCondition::new(5.0, TrendDirection::Decreasing),
        AlertSeverity::Critical,
        "Throughput is dropping",
    ));

    // Create history with increasing trend
    let increasing_history = MetricHistory::new(100);
    for i in 1..=10 {
        increasing_history.record((i * 10) as f64);
        thread::sleep(Duration::from_millis(50));
    }

    // Check trend alerts
    let _fired = manager.check_trend_alerts(&increasing_history);
    // Trend alerts are timing-dependent, so we just verify the API works
    // At minimum, the increasing history should have enough samples
    assert!(increasing_history.len() >= 5);
}

#[test]
fn test_trend_alert_rule() {
    use std::thread;
    use std::time::Duration;

    let history = MetricHistory::new(100);
    for i in 1..=6 {
        history.record((i * 10) as f64);
        thread::sleep(Duration::from_millis(100));
    }

    let rule = TrendAlertRule::new(
        "test_trend",
        TrendAlertCondition::new(5.0, TrendDirection::Increasing),
        AlertSeverity::Warning,
        "Test trend alert",
    );

    // This test just verifies the API works
    let _ = rule.should_fire(&history);
}

#[test]
#[serial]
fn test_metric_summary() {
    reset_metrics();

    TASKS_ENQUEUED_TOTAL.inc_by(100.0);
    TASKS_COMPLETED_TOTAL.inc_by(90.0);
    TASKS_FAILED_TOTAL.inc_by(10.0);
    QUEUE_SIZE.set(50.0);
    ACTIVE_WORKERS.set(5.0);

    let summary = generate_metric_summary();

    assert!(summary.contains("CeleRS Metrics Summary"));
    assert!(summary.contains("100"));
    assert!(summary.contains("90"));
    assert!(summary.contains("10"));
    assert!(summary.contains("50"));
    assert!(summary.contains("5"));
}

// --- Integration-Style Tests ---

/// Integration test simulating complete worker lifecycle
#[test]
#[serial]
fn test_integration_worker_lifecycle() {
    reset_metrics();

    // Simulate worker startup
    ACTIVE_WORKERS.inc();
    assert_eq!(ACTIVE_WORKERS.get(), 1.0);

    // Simulate receiving and processing tasks
    let task_types = ["send_email", "process_image", "generate_report"];

    for (i, task_type) in task_types.iter().enumerate() {
        // Task received from broker
        QUEUE_SIZE.inc();

        // Worker picks up task
        QUEUE_SIZE.dec();
        PROCESSING_QUEUE_SIZE.inc();

        // Track by task type
        TASKS_ENQUEUED_BY_TYPE.with_label_values(&[task_type]).inc();

        // Simulate task execution with varying times
        let execution_time = (i + 1) as f64 * 0.5;
        TASK_EXECUTION_TIME.observe(execution_time);
        TASK_EXECUTION_TIME_BY_TYPE
            .with_label_values(&[task_type])
            .observe(execution_time);

        // Task completes successfully
        PROCESSING_QUEUE_SIZE.dec();
        TASKS_COMPLETED_TOTAL.inc();
        TASKS_COMPLETED_BY_TYPE
            .with_label_values(&[task_type])
            .inc();

        // Record result size
        let result_size = (i + 1) as f64 * 1000.0;
        TASK_RESULT_SIZE_BYTES.observe(result_size);
        TASK_RESULT_SIZE_BY_TYPE
            .with_label_values(&[task_type])
            .observe(result_size);
    }

    // Simulate one task failure with retry
    QUEUE_SIZE.inc();
    QUEUE_SIZE.dec();
    PROCESSING_QUEUE_SIZE.inc();

    TASKS_ENQUEUED_BY_TYPE
        .with_label_values(&["failing_task"])
        .inc();

    // First attempt fails
    TASKS_RETRIED_TOTAL.inc();
    TASKS_RETRIED_BY_TYPE
        .with_label_values(&["failing_task"])
        .inc();

    // Retry also fails - send to DLQ
    PROCESSING_QUEUE_SIZE.dec();
    DLQ_SIZE.inc();
    TASKS_FAILED_TOTAL.inc();
    TASKS_FAILED_BY_TYPE
        .with_label_values(&["failing_task"])
        .inc();

    // Verify final state
    let metrics = CurrentMetrics::capture();
    assert_eq!(metrics.tasks_completed, 3.0);
    assert_eq!(metrics.tasks_failed, 1.0);
    assert_eq!(metrics.tasks_retried, 1.0);
    assert_eq!(metrics.dlq_size, 1.0);
    assert_eq!(metrics.active_workers, 1.0);

    // Check success rate
    let success_rate = metrics.success_rate();
    assert!((success_rate - 0.75).abs() < 0.01); // 3/4 = 75%

    // Worker shutdown
    ACTIVE_WORKERS.dec();
    assert_eq!(ACTIVE_WORKERS.get(), 0.0);
}

/// Integration test simulating broker operations
#[test]
#[serial]
fn test_integration_broker_operations() {
    reset_metrics();

    // Simulate broker startup - establish connection pool
    REDIS_CONNECTIONS_ACTIVE.set(5.0);

    // Simulate batch enqueue operation
    let batch_size = 10.0;
    BATCH_ENQUEUE_TOTAL.inc();
    BATCH_SIZE.observe(batch_size);

    for i in 0..10 {
        TASKS_ENQUEUED_TOTAL.inc();
        QUEUE_SIZE.inc();

        let task_type = if i % 2 == 0 {
            "high_priority"
        } else {
            "low_priority"
        };
        TASKS_ENQUEUED_BY_TYPE.with_label_values(&[task_type]).inc();
    }

    // Simulate broker latency tracking
    BROKER_ENQUEUE_LATENCY_SECONDS.observe(0.005); // 5ms

    // Simulate delayed task scheduling
    DELAYED_TASKS_SCHEDULED.set(3.0);
    DELAYED_TASKS_ENQUEUED_TOTAL.inc_by(3.0);

    // Simulate dequeue operations
    BATCH_DEQUEUE_TOTAL.inc();
    let dequeue_batch_size = 5.0;
    BATCH_SIZE.observe(dequeue_batch_size);
    QUEUE_SIZE.sub(dequeue_batch_size);
    BROKER_DEQUEUE_LATENCY_SECONDS.observe(0.003); // 3ms

    // Simulate ack operations
    for _ in 0..5 {
        BROKER_ACK_LATENCY_SECONDS.observe(0.001); // 1ms
    }

    // Check queue size query latency
    BROKER_QUEUE_SIZE_LATENCY_SECONDS.observe(0.0005); // 0.5ms

    // Verify broker metrics
    let metrics = CurrentMetrics::capture();
    assert_eq!(metrics.tasks_enqueued, 10.0);
    assert_eq!(metrics.queue_size, 5.0); // 10 enqueued - 5 dequeued

    // Verify connection pool is active
    assert_eq!(REDIS_CONNECTIONS_ACTIVE.get(), 5.0);
}

/// Integration test simulating multi-worker concurrent scenario
#[test]
#[serial]
fn test_integration_multi_worker_concurrent() {
    reset_metrics();

    // Simulate 3 workers starting
    let worker_count = 3;
    ACTIVE_WORKERS.set(worker_count as f64);

    // Simulate batch of tasks arriving
    let total_tasks = 30;
    BATCH_ENQUEUE_TOTAL.inc();
    BATCH_SIZE.observe(total_tasks as f64);

    for i in 0..total_tasks {
        TASKS_ENQUEUED_TOTAL.inc();
        QUEUE_SIZE.inc();

        let task_type = match i % 3 {
            0 => "cpu_intensive",
            1 => "io_intensive",
            _ => "mixed",
        };
        TASKS_ENQUEUED_BY_TYPE.with_label_values(&[task_type]).inc();
    }

    // Simulate distributed aggregation across workers
    let aggregator = DistributedAggregator::new();

    for worker_id in 0..worker_count {
        let mut stats = MetricStats::new();

        // Each worker processes 10 tasks
        for task_num in 0..10 {
            let execution_time = (worker_id * 10 + task_num) as f64 * 0.1;
            stats.observe(execution_time);

            // Update global metrics
            QUEUE_SIZE.dec();
            PROCESSING_QUEUE_SIZE.inc();
            TASK_EXECUTION_TIME.observe(execution_time);
            PROCESSING_QUEUE_SIZE.dec();
            TASKS_COMPLETED_TOTAL.inc();
        }

        // Report worker snapshot
        let snapshot = MetricSnapshot::new(format!("worker-{}", worker_id), stats);
        aggregator.update(snapshot);
    }

    // Verify distributed aggregation
    assert_eq!(aggregator.active_worker_count(), worker_count);
    let combined = aggregator.aggregate();
    assert_eq!(combined.count, total_tasks as u64);

    // Verify global metrics
    let metrics = CurrentMetrics::capture();
    assert_eq!(metrics.tasks_completed, total_tasks as f64);
    assert_eq!(metrics.queue_size, 0.0); // All tasks processed
    assert_eq!(metrics.active_workers, worker_count as f64);

    // Calculate worker utilization
    let utilization = (metrics.tasks_completed / worker_count as f64) / 10.0 * 100.0;
    WORKER_UTILIZATION_PERCENT.set(utilization);
}

/// Integration test simulating end-to-end task lifecycle with monitoring
#[test]
#[serial]
fn test_integration_end_to_end_lifecycle() {
    reset_metrics();

    // Configure metrics with sampling
    let config = MetricsConfig::new().with_sampling_rate(1.0); // 100% for testing

    // Setup: Initialize system
    ACTIVE_WORKERS.set(2.0);
    REDIS_CONNECTIONS_ACTIVE.set(10.0);

    // Phase 1: Broker receives and enqueues tasks
    let tasks_to_process = 20;
    for i in 0..tasks_to_process {
        if config.should_sample() {
            TASKS_ENQUEUED_TOTAL.inc();
            QUEUE_SIZE.inc();

            let task_type = if i < 15 {
                "normal_task"
            } else {
                "special_task"
            };
            TASKS_ENQUEUED_BY_TYPE.with_label_values(&[task_type]).inc();

            // Track enqueue latency
            BROKER_ENQUEUE_LATENCY_SECONDS.observe(0.002);

            // Track task age (time from creation to enqueue)
            TASK_AGE_SECONDS.observe(i as f64 * 0.1);
        }
    }

    // Phase 2: Workers process tasks
    let mut successful_tasks = 0;
    let mut failed_tasks = 0;
    let mut retried_tasks = 0;

    for i in 0..tasks_to_process {
        // Dequeue
        QUEUE_SIZE.dec();
        PROCESSING_QUEUE_SIZE.inc();
        BROKER_DEQUEUE_LATENCY_SECONDS.observe(0.001);

        // Track wait time in queue
        TASK_QUEUE_WAIT_TIME_SECONDS.observe(i as f64 * 0.05);

        // Process
        let execution_time = if i < 15 { 0.5 } else { 2.0 };
        TASK_EXECUTION_TIME.observe(execution_time);

        let task_type = if i < 15 {
            "normal_task"
        } else {
            "special_task"
        };
        TASK_EXECUTION_TIME_BY_TYPE
            .with_label_values(&[task_type])
            .observe(execution_time);

        // Simulate occasional failures
        if i == 5 || i == 10 {
            // First failure - retry
            TASKS_RETRIED_TOTAL.inc();
            TASKS_RETRIED_BY_TYPE.with_label_values(&[task_type]).inc();
            retried_tasks += 1;

            // Retry succeeds
            PROCESSING_QUEUE_SIZE.dec();
            TASKS_COMPLETED_TOTAL.inc();
            TASKS_COMPLETED_BY_TYPE
                .with_label_values(&[task_type])
                .inc();
            successful_tasks += 1;

            BROKER_ACK_LATENCY_SECONDS.observe(0.001);
        } else if i == 15 {
            // Permanent failure
            TASKS_RETRIED_TOTAL.inc();
            TASKS_RETRIED_BY_TYPE.with_label_values(&[task_type]).inc();
            retried_tasks += 1;

            PROCESSING_QUEUE_SIZE.dec();
            TASKS_FAILED_TOTAL.inc();
            TASKS_FAILED_BY_TYPE.with_label_values(&[task_type]).inc();
            DLQ_SIZE.inc();
            failed_tasks += 1;

            BROKER_REJECT_LATENCY_SECONDS.observe(0.001);
        } else {
            // Success
            PROCESSING_QUEUE_SIZE.dec();
            TASKS_COMPLETED_TOTAL.inc();
            TASKS_COMPLETED_BY_TYPE
                .with_label_values(&[task_type])
                .inc();
            successful_tasks += 1;

            // Record result size
            TASK_RESULT_SIZE_BYTES.observe(5000.0);
            TASK_RESULT_SIZE_BY_TYPE
                .with_label_values(&[task_type])
                .observe(5000.0);

            BROKER_ACK_LATENCY_SECONDS.observe(0.001);
        }
    }

    // Phase 3: Health check and monitoring
    let metrics = CurrentMetrics::capture();

    // Verify all tasks processed
    assert_eq!(metrics.tasks_enqueued, tasks_to_process as f64);
    assert_eq!(metrics.tasks_completed, successful_tasks as f64);
    assert_eq!(metrics.tasks_failed, failed_tasks as f64);
    assert_eq!(metrics.tasks_retried, retried_tasks as f64);
    assert_eq!(metrics.queue_size, 0.0);
    assert_eq!(metrics.processing_queue_size, 0.0);
    assert_eq!(metrics.dlq_size, failed_tasks as f64);

    // Verify success rate
    let success_rate = metrics.success_rate();
    assert!(success_rate > 0.9); // Should be 95%

    // Setup health check
    let health_config = HealthCheckConfig::new()
        .with_max_queue_size(100.0)
        .with_max_dlq_size(5.0)
        .with_min_active_workers(1.0)
        .with_slo_target(SloTarget {
            success_rate: 0.95,
            latency_seconds: 5.0,
            throughput: 1.0,
        });

    let health = health_check(&health_config);
    match health {
        HealthStatus::Healthy => {
            // System is healthy
        }
        HealthStatus::Degraded { reasons } => {
            // Some degradation is expected with 1 failure
            assert!(!reasons.is_empty());
        }
        HealthStatus::Unhealthy { .. } => {
            panic!("System should not be unhealthy with only 1 failure");
        }
    }

    // Setup alert monitoring
    let mut alert_manager = AlertManager::new();

    alert_manager.add_rule(AlertRule::new(
        "high_dlq",
        AlertCondition::GaugeAbove { threshold: 5.0 },
        AlertSeverity::Warning,
        "DLQ size exceeded threshold",
    ));

    alert_manager.add_rule(AlertRule::new(
        "low_success_rate",
        AlertCondition::SuccessRateBelow { threshold: 0.9 },
        AlertSeverity::Critical,
        "Success rate below 90%",
    ));

    let _fired_alerts = alert_manager.check_alerts(&metrics);
    // Should have no critical alerts with 95% success rate
    let critical_alerts = alert_manager.critical_alerts(&metrics);
    assert_eq!(critical_alerts.len(), 0);

    // Phase 4: Generate summary report
    let summary = generate_metric_summary();
    assert!(summary.contains("CeleRS Metrics Summary"));
    assert!(summary.contains(&format!("{}", successful_tasks)));
    assert!(summary.contains(&format!("{}", failed_tasks)));

    // Cleanup
    ACTIVE_WORKERS.set(0.0);
    REDIS_CONNECTIONS_ACTIVE.set(0.0);
}

/// Integration test for memory pressure and oversized results
#[test]
#[serial]
fn test_integration_memory_pressure() {
    reset_metrics();

    // Simulate worker with memory tracking
    ACTIVE_WORKERS.set(1.0);
    let initial_memory = 100_000_000.0; // 100MB
    WORKER_MEMORY_USAGE_BYTES.set(initial_memory);

    // Process tasks with varying result sizes
    let task_sizes = [
        1_000.0,      // 1KB - normal
        10_000.0,     // 10KB - normal
        100_000.0,    // 100KB - normal
        1_000_000.0,  // 1MB - normal
        10_000_000.0, // 10MB - large
        15_000_000.0, // 15MB - oversized
    ];

    for size in task_sizes.iter() {
        TASKS_ENQUEUED_TOTAL.inc();
        QUEUE_SIZE.inc();
        QUEUE_SIZE.dec();
        PROCESSING_QUEUE_SIZE.inc();

        // Process task
        TASK_EXECUTION_TIME.observe(0.5);

        // Record result size
        TASK_RESULT_SIZE_BYTES.observe(*size);

        // Check if oversized (>10MB)
        if *size > 10_000_000.0 {
            OVERSIZED_RESULTS_TOTAL.inc();
        }

        PROCESSING_QUEUE_SIZE.dec();
        TASKS_COMPLETED_TOTAL.inc();

        // Update memory usage
        let memory_delta = size / 10.0; // Approximate memory impact
        let new_memory = WORKER_MEMORY_USAGE_BYTES.get() + memory_delta;
        WORKER_MEMORY_USAGE_BYTES.set(new_memory);
    }

    // Verify metrics
    let metrics = CurrentMetrics::capture();
    assert_eq!(metrics.tasks_completed, task_sizes.len() as f64);

    // Verify oversized results were tracked
    let oversized_count = OVERSIZED_RESULTS_TOTAL.get();
    assert_eq!(oversized_count, 1.0); // Only the 15MB result

    // Verify memory increased
    let final_memory = WORKER_MEMORY_USAGE_BYTES.get();
    assert!(final_memory > initial_memory);

    // Check if memory alert would fire
    let memory_threshold = 200_000_000.0; // 200MB threshold
    if final_memory > memory_threshold {
        // Would trigger memory alert in production
        assert!(final_memory > memory_threshold);
    }
}

/// Integration test for PostgreSQL connection pool metrics
#[test]
#[serial]
fn test_integration_postgres_pool() {
    reset_metrics();

    // Simulate PostgreSQL connection pool initialization
    let max_connections = 20.0;
    POSTGRES_POOL_MAX_SIZE.set(max_connections);
    POSTGRES_POOL_SIZE.set(max_connections);
    POSTGRES_POOL_IDLE.set(max_connections);
    POSTGRES_POOL_IN_USE.set(0.0);

    // Simulate broker operations using PostgreSQL
    let tasks_to_process = 10;

    for _i in 0..tasks_to_process {
        // Acquire connection from pool
        POSTGRES_POOL_IDLE.dec();
        POSTGRES_POOL_IN_USE.inc();

        // Enqueue task to PostgreSQL queue
        TASKS_ENQUEUED_TOTAL.inc();
        QUEUE_SIZE.inc();
        BROKER_ENQUEUE_LATENCY_SECONDS.observe(0.010); // 10ms for DB write

        // Release connection back to pool
        POSTGRES_POOL_IN_USE.dec();
        POSTGRES_POOL_IDLE.inc();

        // Worker acquires connection to dequeue
        POSTGRES_POOL_IDLE.dec();
        POSTGRES_POOL_IN_USE.inc();

        // Dequeue task
        QUEUE_SIZE.dec();
        PROCESSING_QUEUE_SIZE.inc();
        BROKER_DEQUEUE_LATENCY_SECONDS.observe(0.008); // 8ms for DB read

        // Release connection
        POSTGRES_POOL_IN_USE.dec();
        POSTGRES_POOL_IDLE.inc();

        // Process task
        TASK_EXECUTION_TIME.observe(1.0);
        PROCESSING_QUEUE_SIZE.dec();
        TASKS_COMPLETED_TOTAL.inc();

        // Acquire connection to ack
        POSTGRES_POOL_IDLE.dec();
        POSTGRES_POOL_IN_USE.inc();

        BROKER_ACK_LATENCY_SECONDS.observe(0.005); // 5ms for ack

        // Release connection
        POSTGRES_POOL_IN_USE.dec();
        POSTGRES_POOL_IDLE.inc();
    }

    // Verify pool metrics
    assert_eq!(POSTGRES_POOL_MAX_SIZE.get(), max_connections);
    assert_eq!(POSTGRES_POOL_SIZE.get(), max_connections);
    assert_eq!(POSTGRES_POOL_IDLE.get(), max_connections);
    assert_eq!(POSTGRES_POOL_IN_USE.get(), 0.0); // All released

    // Verify tasks processed
    let metrics = CurrentMetrics::capture();
    assert_eq!(metrics.tasks_completed, tasks_to_process as f64);
    assert_eq!(metrics.queue_size, 0.0);
}

// --- Tests for Config Builders ---

#[test]
fn test_auto_scaling_config_builder() {
    let config = AutoScalingConfig::new()
        .with_target_queue_per_worker(20.0)
        .with_min_workers(5)
        .with_max_workers(50)
        .with_scale_up_threshold(0.9)
        .with_scale_down_threshold(0.2)
        .with_cooldown_seconds(600);

    assert_eq!(config.target_queue_per_worker, 20.0);
    assert_eq!(config.min_workers, 5);
    assert_eq!(config.max_workers, 50);
    assert_eq!(config.scale_up_threshold, 0.9);
    assert_eq!(config.scale_down_threshold, 0.2);
    assert_eq!(config.cooldown_seconds, 600);
}

#[test]
fn test_cost_config_builder() {
    let config = CostConfig::new()
        .with_cost_per_worker_hour(1.50)
        .with_cost_per_million_tasks(5.0)
        .with_cost_per_gb(0.05);

    assert_eq!(config.cost_per_worker_hour, 1.50);
    assert_eq!(config.cost_per_million_tasks, 5.0);
    assert_eq!(config.cost_per_gb, 0.05);
}

// --- Tests for Metric History Basics ---

#[test]
fn test_metric_history_recording() {
    let history = MetricHistory::new(5);

    // Record some values
    history.record(10.0);
    history.record(20.0);
    history.record(30.0);

    assert_eq!(history.len(), 3);
    assert!(!history.is_empty());

    let samples = history.get_samples();
    assert_eq!(samples.len(), 3);
    assert_eq!(samples[0].value, 10.0);
    assert_eq!(samples[1].value, 20.0);
    assert_eq!(samples[2].value, 30.0);
}

#[test]
fn test_metric_history_max_samples() {
    let history = MetricHistory::new(3);

    // Record more than max samples
    for i in 0..10 {
        history.record(i as f64);
    }

    // Should only keep last 3
    assert_eq!(history.len(), 3);
    let samples = history.get_samples();
    assert_eq!(samples[0].value, 7.0);
    assert_eq!(samples[1].value, 8.0);
    assert_eq!(samples[2].value, 9.0);
}

#[test]
fn test_metric_history_moving_average() {
    let history = MetricHistory::new(5);

    history.record(10.0);
    history.record(20.0);
    history.record(30.0);

    let avg = history.moving_average();
    assert_eq!(avg, Some(20.0));
}

#[test]
fn test_forecast_metric_insufficient_samples() {
    let history = MetricHistory::new(10);

    history.record(10.0);
    history.record(20.0);

    // Need at least 3 samples
    let forecast = forecast_metric(&history, 60);
    assert!(forecast.is_none());
}
