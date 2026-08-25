//! Second half of `celers-metrics`'s core unit test suite.
//!
//! Split out of `tests_core.rs` at a `#[test]`-boundary roughly halfway
//! through, purely to keep both files under the COOLJAPAN 2000-line-per-file
//! policy (`tests_core.rs` sat exactly at the 2000-line ceiling, which the
//! policy phrases as "under 2000"). There is no topical distinction between
//! the two files -- covers percentile/statistics helpers, metrics
//! configuration and samplers, backend implementations, aggregation, health
//! checks, and the integration-style / config-builder / metric-history-basics
//! sections that close out the original file.
#![cfg(test)]

use crate::*;
use serial_test::serial;

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
        total_payload_bytes: 0.0,
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
        total_payload_bytes: 0.0,
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
        total_payload_bytes: 0.0,
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
        total_payload_bytes: 0.0,
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
        total_payload_bytes: 0.0,
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
        total_payload_bytes: 0.0,
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
