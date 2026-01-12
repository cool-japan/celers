use celers_metrics::*;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

fn bench_counter_increment(c: &mut Criterion) {
    c.bench_function("counter_increment", |b| {
        b.iter(|| {
            TASKS_ENQUEUED_TOTAL.inc();
        })
    });
}

fn bench_counter_increment_by(c: &mut Criterion) {
    c.bench_function("counter_increment_by", |b| {
        b.iter(|| {
            TASKS_ENQUEUED_TOTAL.inc_by(black_box(10.0));
        })
    });
}

fn bench_gauge_set(c: &mut Criterion) {
    c.bench_function("gauge_set", |b| {
        b.iter(|| {
            QUEUE_SIZE.set(black_box(100.0));
        })
    });
}

fn bench_histogram_observe(c: &mut Criterion) {
    c.bench_function("histogram_observe", |b| {
        b.iter(|| {
            TASK_EXECUTION_TIME.observe(black_box(1.5));
        })
    });
}

fn bench_per_task_type_metrics(c: &mut Criterion) {
    c.bench_function("per_task_type_counter", |b| {
        b.iter(|| {
            TASKS_ENQUEUED_BY_TYPE
                .with_label_values(&[black_box("send_email")])
                .inc();
        })
    });

    c.bench_function("per_task_type_histogram", |b| {
        b.iter(|| {
            TASK_EXECUTION_TIME_BY_TYPE
                .with_label_values(&[black_box("send_email")])
                .observe(black_box(1.5));
        })
    });
}

fn bench_sampling(c: &mut Criterion) {
    let mut group = c.benchmark_group("sampling");

    for sample_rate in [0.01, 0.1, 0.5, 1.0].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("rate_{}", sample_rate)),
            sample_rate,
            |b, &rate| {
                let config = MetricsConfig::new().with_sampling_rate(rate);
                b.iter(|| {
                    if config.should_sample() {
                        TASK_EXECUTION_TIME.observe(black_box(1.5));
                    }
                })
            },
        );
    }
    group.finish();
}

fn bench_observe_sampled(c: &mut Criterion) {
    c.bench_function("observe_sampled", |b| {
        b.iter(|| {
            observe_sampled(|| {
                TASK_EXECUTION_TIME.observe(black_box(1.5));
            })
        })
    });
}

fn bench_metric_aggregation(c: &mut Criterion) {
    c.bench_function("metric_aggregator_observe", |b| {
        let aggregator = MetricAggregator::new();
        b.iter(|| {
            aggregator.observe(black_box(10.0));
        })
    });

    c.bench_function("metric_aggregator_snapshot", |b| {
        let aggregator = MetricAggregator::new();
        for i in 0..100 {
            aggregator.observe(i as f64);
        }
        b.iter(|| {
            black_box(aggregator.snapshot());
        })
    });
}

fn bench_distributed_aggregation(c: &mut Criterion) {
    c.bench_function("distributed_aggregator_update", |b| {
        let aggregator = DistributedAggregator::new();
        let mut stats = MetricStats::new();
        stats.observe(10.0);
        stats.observe(20.0);

        let mut counter = 0;
        b.iter(|| {
            let snapshot = MetricSnapshot::new(format!("worker-{}", counter % 10), stats.clone());
            aggregator.update(snapshot);
            counter += 1;
        })
    });

    c.bench_function("distributed_aggregator_aggregate", |b| {
        let aggregator = DistributedAggregator::new();

        // Pre-populate with data from 10 workers
        for i in 0..10 {
            let mut stats = MetricStats::new();
            for j in 0..100 {
                stats.observe((i * 100 + j) as f64);
            }
            let snapshot = MetricSnapshot::new(format!("worker-{}", i), stats);
            aggregator.update(snapshot);
        }

        b.iter(|| {
            black_box(aggregator.aggregate());
        })
    });
}

fn bench_rate_calculations(c: &mut Criterion) {
    c.bench_function("calculate_rate", |b| {
        b.iter(|| calculate_rate(black_box(1000.0), black_box(900.0), black_box(10.0)))
    });

    c.bench_function("calculate_success_rate", |b| {
        b.iter(|| calculate_success_rate(black_box(900.0), black_box(100.0)))
    });

    c.bench_function("calculate_error_budget", |b| {
        b.iter(|| calculate_error_budget(black_box(1000.0), black_box(50.0), black_box(0.99)))
    });
}

fn bench_anomaly_detection(c: &mut Criterion) {
    let threshold = AnomalyThreshold::new(100.0, 10.0, 2.0);

    c.bench_function("detect_anomaly", |b| {
        b.iter(|| detect_anomaly(black_box(120.0), &threshold))
    });

    c.bench_function("moving_average_update", |b| {
        let mut ma = MovingAverage::new(100.0, 0.2);
        b.iter(|| ma.update(black_box(105.0)))
    });

    c.bench_function("detect_spike", |b| {
        b.iter(|| detect_spike(black_box(150.0), black_box(100.0), black_box(2.0)))
    });
}

fn bench_health_check(c: &mut Criterion) {
    // Set up some metrics
    QUEUE_SIZE.set(500.0);
    DLQ_SIZE.set(50.0);
    ACTIVE_WORKERS.set(10.0);
    TASKS_COMPLETED_TOTAL.inc_by(900.0);
    TASKS_FAILED_TOTAL.inc_by(100.0);

    let config = HealthCheckConfig::new()
        .with_max_queue_size(1000.0)
        .with_max_dlq_size(100.0)
        .with_min_active_workers(5.0)
        .with_slo_target(SloTarget {
            success_rate: 0.99,
            latency_seconds: 5.0,
            throughput: 100.0,
        });

    c.bench_function("health_check", |b| {
        b.iter(|| black_box(health_check(&config)))
    });
}

fn bench_percentile_calculation(c: &mut Criterion) {
    let values: Vec<f64> = (0..1000).map(|i| i as f64).collect();

    c.bench_function("calculate_percentile_p50", |b| {
        b.iter(|| calculate_percentile(&values, black_box(0.50)))
    });

    c.bench_function("calculate_percentile_p95", |b| {
        b.iter(|| calculate_percentile(&values, black_box(0.95)))
    });

    c.bench_function("calculate_percentiles_batch", |b| {
        b.iter(|| calculate_percentiles(&values))
    });
}

fn bench_metric_comparison(c: &mut Criterion) {
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

    let current = CurrentMetrics {
        tasks_enqueued: 1100.0,
        tasks_completed: 980.0,
        tasks_failed: 120.0,
        tasks_retried: 60.0,
        tasks_cancelled: 15.0,
        queue_size: 120.0,
        processing_queue_size: 25.0,
        dlq_size: 8.0,
        active_workers: 12.0,
    };

    c.bench_function("metric_comparison", |b| {
        b.iter(|| MetricComparison::compare(black_box(&baseline), black_box(&current)))
    });
}

fn bench_alert_rules(c: &mut Criterion) {
    let metrics = CurrentMetrics {
        tasks_enqueued: 1000.0,
        tasks_completed: 900.0,
        tasks_failed: 100.0,
        tasks_retried: 50.0,
        tasks_cancelled: 10.0,
        queue_size: 1500.0,
        processing_queue_size: 20.0,
        dlq_size: 5.0,
        active_workers: 3.0,
    };

    let rule = AlertRule::new(
        "high_error_rate",
        AlertCondition::ErrorRateAbove { threshold: 0.05 },
        AlertSeverity::Critical,
        "Error rate exceeded 5%",
    );

    c.bench_function("alert_rule_check", |b| {
        b.iter(|| rule.should_fire(black_box(&metrics)))
    });

    c.bench_function("alert_manager_check", |b| {
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

        b.iter(|| black_box(manager.check_alerts(&metrics)))
    });
}

fn bench_gather_metrics(c: &mut Criterion) {
    // Set up various metrics
    TASKS_ENQUEUED_TOTAL.inc_by(1000.0);
    TASKS_COMPLETED_TOTAL.inc_by(900.0);
    TASKS_FAILED_TOTAL.inc_by(100.0);
    QUEUE_SIZE.set(100.0);
    ACTIVE_WORKERS.set(10.0);

    for _ in 0..100 {
        TASK_EXECUTION_TIME.observe(1.5);
    }

    c.bench_function("gather_metrics", |b| b.iter(|| black_box(gather_metrics())));
}

fn bench_current_metrics_capture(c: &mut Criterion) {
    c.bench_function("current_metrics_capture", |b| {
        b.iter(|| black_box(CurrentMetrics::capture()))
    });
}

fn bench_generate_summary(c: &mut Criterion) {
    TASKS_ENQUEUED_TOTAL.inc_by(1000.0);
    TASKS_COMPLETED_TOTAL.inc_by(900.0);
    TASKS_FAILED_TOTAL.inc_by(100.0);
    QUEUE_SIZE.set(100.0);
    ACTIVE_WORKERS.set(10.0);

    c.bench_function("generate_metric_summary", |b| {
        b.iter(|| black_box(generate_metric_summary()))
    });
}

fn bench_cardinality_limiter(c: &mut Criterion) {
    c.bench_function("cardinality_limiter_check_new", |b| {
        let limiter = CardinalityLimiter::new(1000);
        let mut counter = 0u64;
        b.iter(|| {
            counter += 1;
            let label = format!("label_{}", counter);
            black_box(limiter.check_and_record(&label))
        })
    });

    c.bench_function("cardinality_limiter_check_existing", |b| {
        let limiter = CardinalityLimiter::new(1000);
        limiter.check_and_record("label_1");
        b.iter(|| black_box(limiter.check_and_record("label_1")))
    });

    c.bench_function("cardinality_limiter_at_limit", |b| {
        let limiter = CardinalityLimiter::new(100);
        for i in 0..100 {
            limiter.check_and_record(&format!("label_{}", i));
        }
        b.iter(|| black_box(limiter.check_and_record("label_new")))
    });
}

fn bench_trend_alerting(c: &mut Criterion) {
    c.bench_function("trend_alert_check", |b| {
        let history = MetricHistory::new(100);
        for i in 0..20 {
            history.record((i * 10) as f64);
        }
        let alert = TrendAlertCondition::new(5.0, TrendDirection::Increasing);
        b.iter(|| black_box(alert.should_alert(&history)))
    });

    c.bench_function("trend_alert_insufficient_samples", |b| {
        let history = MetricHistory::new(100);
        history.record(10.0);
        history.record(20.0);
        let alert = TrendAlertCondition::new(5.0, TrendDirection::Increasing);
        b.iter(|| black_box(alert.should_alert(&history)))
    });
}

fn bench_correlation_analysis(c: &mut Criterion) {
    c.bench_function("calculate_correlation", |b| {
        let history_a = MetricHistory::new(100);
        let history_b = MetricHistory::new(100);
        for i in 0..50 {
            history_a.record((i * 10) as f64);
            history_b.record((i * 10 + 5) as f64);
        }
        b.iter(|| black_box(calculate_correlation(&history_a, &history_b)))
    });

    c.bench_function("are_metrics_correlated", |b| {
        let history_a = MetricHistory::new(100);
        let history_b = MetricHistory::new(100);
        for i in 0..50 {
            history_a.record((i * 10) as f64);
            history_b.record((i * 10 + 5) as f64);
        }
        b.iter(|| black_box(are_metrics_correlated(&history_a, &history_b, 0.8)))
    });
}

fn bench_windowed_statistics(c: &mut Criterion) {
    let mut group = c.benchmark_group("windowed_statistics");

    let history = MetricHistory::new(1000);
    for i in 0..500 {
        history.record((i * 10) as f64);
    }

    for window in [
        ("1m", TimeWindow::OneMinute),
        ("5m", TimeWindow::FiveMinutes),
        ("15m", TimeWindow::FifteenMinutes),
        ("1h", TimeWindow::OneHour),
        ("1d", TimeWindow::OneDay),
    ]
    .iter()
    {
        group.bench_with_input(
            BenchmarkId::from_parameter(window.0),
            &window.1,
            |b, &time_window| b.iter(|| black_box(calculate_windowed_stats(&history, time_window))),
        );
    }
    group.finish();
}

fn bench_metric_history_operations(c: &mut Criterion) {
    c.bench_function("metric_history_record", |b| {
        let history = MetricHistory::new(100);
        let mut counter = 0.0;
        b.iter(|| {
            counter += 1.0;
            history.record(black_box(counter))
        })
    });

    c.bench_function("metric_history_trend", |b| {
        let history = MetricHistory::new(100);
        for i in 0..100 {
            history.record((i * 10) as f64);
        }
        b.iter(|| black_box(history.trend()))
    });

    c.bench_function("metric_history_moving_average", |b| {
        let history = MetricHistory::new(100);
        for i in 0..100 {
            history.record((i * 10) as f64);
        }
        b.iter(|| black_box(history.moving_average()))
    });
}

fn bench_label_validation(c: &mut Criterion) {
    c.bench_function("is_valid_metric_name", |b| {
        b.iter(|| is_valid_metric_name(black_box("http_requests_total")))
    });

    c.bench_function("is_valid_label_name", |b| {
        b.iter(|| is_valid_label_name(black_box("status_code")))
    });

    c.bench_function("sanitize_label_name", |b| {
        b.iter(|| sanitize_label_name(black_box("invalid-label-name")))
    });

    c.bench_function("sanitize_label_value", |b| {
        b.iter(|| sanitize_label_value(black_box("hello\nworld\ttab")))
    });
}

fn bench_histogram_heatmap(c: &mut Criterion) {
    let heatmap = HistogramHeatmap::new(100);

    c.bench_function("heatmap_record_snapshot", |b| {
        let buckets = vec![
            HeatmapBucket {
                upper_bound: 0.1,
                count: 10,
                timestamp: 0,
            },
            HeatmapBucket {
                upper_bound: 1.0,
                count: 50,
                timestamp: 0,
            },
            HeatmapBucket {
                upper_bound: 10.0,
                count: 100,
                timestamp: 0,
            },
        ];
        b.iter(|| {
            heatmap.record_snapshot(black_box(buckets.clone()));
        })
    });

    // Pre-populate for get_snapshots benchmark
    for i in 0..10 {
        let buckets = vec![HeatmapBucket {
            upper_bound: 1.0,
            count: i * 10,
            timestamp: i,
        }];
        heatmap.record_snapshot(buckets);
    }

    c.bench_function("heatmap_get_snapshots", |b| {
        b.iter(|| black_box(heatmap.get_snapshots()))
    });
}

fn bench_metric_registry(c: &mut Criterion) {
    let registry = MetricRegistry::new();

    // Pre-register some metrics
    for i in 0..10 {
        registry.register_counter(&format!("counter_{}", i), "Test counter");
        registry.register_gauge(&format!("gauge_{}", i), "Test gauge");
    }

    c.bench_function("registry_increment_counter", |b| {
        b.iter(|| registry.increment_counter(black_box("counter_0"), black_box(1)))
    });

    c.bench_function("registry_set_gauge", |b| {
        b.iter(|| registry.set_gauge(black_box("gauge_0"), black_box(100)))
    });

    c.bench_function("registry_get_counter", |b| {
        b.iter(|| black_box(registry.get_counter("counter_0")))
    });

    c.bench_function("registry_list_metrics", |b| {
        b.iter(|| black_box(registry.list_metrics()))
    });

    c.bench_function("registry_register_counter", |b| {
        let mut counter = 0;
        b.iter(|| {
            counter += 1;
            registry.register_counter(&format!("new_counter_{}", counter), "Test")
        })
    });
}

fn bench_resource_tracker(c: &mut Criterion) {
    let tracker = ResourceTracker::new();

    c.bench_function("resource_tracker_track_operation", |b| {
        b.iter(|| tracker.track_operation(|| black_box(42)))
    });

    c.bench_function("resource_tracker_record_memory", |b| {
        b.iter(|| tracker.record_memory_usage(black_box(1000)))
    });

    c.bench_function("resource_tracker_get_stats", |b| {
        b.iter(|| {
            black_box(tracker.collection_count());
            black_box(tracker.avg_collection_time_micros());
            black_box(tracker.peak_memory_bytes());
        })
    });
}

criterion_group!(
    benches,
    bench_counter_increment,
    bench_counter_increment_by,
    bench_gauge_set,
    bench_histogram_observe,
    bench_per_task_type_metrics,
    bench_sampling,
    bench_observe_sampled,
    bench_metric_aggregation,
    bench_distributed_aggregation,
    bench_rate_calculations,
    bench_anomaly_detection,
    bench_health_check,
    bench_percentile_calculation,
    bench_metric_comparison,
    bench_alert_rules,
    bench_gather_metrics,
    bench_current_metrics_capture,
    bench_generate_summary,
    bench_cardinality_limiter,
    bench_trend_alerting,
    bench_correlation_analysis,
    bench_windowed_statistics,
    bench_metric_history_operations,
    bench_label_validation,
    bench_histogram_heatmap,
    bench_metric_registry,
    bench_resource_tracker,
);

criterion_main!(benches);
