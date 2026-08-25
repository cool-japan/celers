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
        SloComplianceStatus::Compliant
    );

    // Test non-compliant success rate
    assert_eq!(
        check_slo_compliance(0.98, 4.5, 120.0, &target),
        SloComplianceStatus::NonCompliant
    );

    // Test non-compliant latency
    assert_eq!(
        check_slo_compliance(0.995, 6.0, 120.0, &target),
        SloComplianceStatus::NonCompliant
    );

    // Test non-compliant throughput
    assert_eq!(
        check_slo_compliance(0.995, 4.5, 90.0, &target),
        SloComplianceStatus::NonCompliant
    );

    // Test unknown (negative values)
    assert_eq!(
        check_slo_compliance(-1.0, 4.5, 120.0, &target),
        SloComplianceStatus::Unknown
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
        .expect("SystemTime should be after UNIX_EPOCH")
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
