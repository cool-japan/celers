//! Benchmarks for AMQP broker operations
//!
//! These benchmarks measure the performance of various AMQP operations.
//!
//! Prerequisites:
//! - RabbitMQ running on localhost:5672
//!
//! Run with:
//! ```bash
//! cargo bench
//! ```

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use serde_json::json;

/// Benchmark serialization performance
fn bench_serialization(c: &mut Criterion) {
    use celers_protocol::builder::MessageBuilder;

    let mut group = c.benchmark_group("serialization");

    group.bench_function("simple_message", |b| {
        b.iter(|| {
            let msg = MessageBuilder::new("task.test")
                .args(vec![json!({"x": 10, "y": 20})])
                .build()
                .unwrap();
            serde_json::to_vec(&msg).unwrap()
        });
    });

    group.bench_function("complex_message", |b| {
        b.iter(|| {
            let msg = MessageBuilder::new("task.complex")
                .args(vec![
                    json!({"data": vec![1, 2, 3, 4, 5]}),
                    json!({"nested": {"field": "value"}}),
                ])
                .build()
                .unwrap();
            serde_json::to_vec(&msg).unwrap()
        });
    });

    group.finish();
}

/// Benchmark queue configuration
fn bench_queue_config(c: &mut Criterion) {
    use celers_broker_amqp::QueueConfig;

    let mut group = c.benchmark_group("queue_config");

    group.bench_function("basic_config", |b| {
        b.iter(QueueConfig::default);
    });

    group.bench_function("with_priority", |b| {
        b.iter(|| QueueConfig::default().with_max_priority(10));
    });

    group.bench_function("full_config", |b| {
        b.iter(|| {
            QueueConfig::default()
                .with_max_priority(10)
                .with_message_ttl(60000)
                .with_max_length(1000)
        });
    });

    group.finish();
}

/// Benchmark broker configuration
fn bench_broker_config(c: &mut Criterion) {
    use celers_broker_amqp::AmqpConfig;
    use std::time::Duration;

    let mut group = c.benchmark_group("broker_config");

    group.bench_function("default_config", |b| {
        b.iter(AmqpConfig::default);
    });

    group.bench_function("configured", |b| {
        b.iter(|| {
            AmqpConfig::default()
                .with_prefetch(10)
                .with_retry(3, Duration::from_secs(1))
                .with_channel_pool_size(10)
        });
    });

    group.finish();
}

/// Benchmark batch size calculations
fn bench_batch_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_operations");

    for size in [10, 50, 100, 500].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let mut vec = Vec::with_capacity(size);
                for i in 0..size {
                    vec.push(i);
                }
                vec
            });
        });
    }

    group.finish();
}

/// Benchmark v4 helper methods for queue statistics
fn bench_helper_methods(c: &mut Criterion) {
    use celers_broker_amqp::{MessageStats, QueueStats, RateDetails};

    let mut group = c.benchmark_group("helper_methods");

    // Create sample queue stats
    let stats = QueueStats {
        name: "test_queue".to_string(),
        vhost: "/".to_string(),
        durable: true,
        auto_delete: false,
        messages: 1000,
        messages_ready: 800,
        messages_unacknowledged: 200,
        consumers: 5,
        memory: 10485760,       // 10 MB
        message_bytes: 5242880, // 5 MB
        message_bytes_ready: 4194304,
        message_bytes_unacknowledged: 1048576,
        message_stats: Some(MessageStats {
            publish: 10000,
            publish_details: Some(RateDetails { rate: 100.0 }),
            deliver: 9500,
            deliver_details: Some(RateDetails { rate: 95.0 }),
            ack: 9000,
            ack_details: Some(RateDetails { rate: 90.0 }),
        }),
    };

    group.bench_function("is_empty", |b| {
        b.iter(|| stats.is_empty());
    });

    group.bench_function("has_consumers", |b| {
        b.iter(|| stats.has_consumers());
    });

    group.bench_function("ready_percentage", |b| {
        b.iter(|| stats.ready_percentage());
    });

    group.bench_function("avg_message_size", |b| {
        b.iter(|| stats.avg_message_size());
    });

    group.bench_function("publish_rate", |b| {
        b.iter(|| stats.publish_rate());
    });

    group.bench_function("is_growing", |b| {
        b.iter(|| stats.is_growing());
    });

    group.bench_function("consumers_keeping_up", |b| {
        b.iter(|| stats.consumers_keeping_up());
    });

    group.finish();
}

/// Benchmark connection info helper methods
fn bench_connection_helpers(c: &mut Criterion) {
    use celers_broker_amqp::ConnectionInfo;

    let mut group = c.benchmark_group("connection_helpers");

    let conn = ConnectionInfo {
        name: "test_connection".to_string(),
        vhost: "/".to_string(),
        user: "guest".to_string(),
        state: "running".to_string(),
        channels: 10,
        peer_host: "127.0.0.1".to_string(),
        peer_port: 5672,
        recv_oct: 10485760, // 10 MB
        send_oct: 5242880,  // 5 MB
        recv_cnt: 1000,
        send_cnt: 500,
    };

    group.bench_function("is_running", |b| {
        b.iter(|| conn.is_running());
    });

    group.bench_function("total_bytes", |b| {
        b.iter(|| conn.total_bytes());
    });

    group.bench_function("avg_message_size", |b| {
        b.iter(|| conn.avg_message_size());
    });

    group.bench_function("peer_address", |b| {
        b.iter(|| conn.peer_address());
    });

    group.finish();
}

/// Benchmark v5 monitoring functions
fn bench_monitoring_functions(c: &mut Criterion) {
    use celers_broker_amqp::monitoring::*;

    let mut group = c.benchmark_group("monitoring_functions");

    group.bench_function("analyze_consumer_lag", |b| {
        b.iter(|| analyze_amqp_consumer_lag(1000, 50.0, 100));
    });

    group.bench_function("calculate_message_velocity", |b| {
        b.iter(|| calculate_amqp_message_velocity(1000, 1500, 60.0));
    });

    group.bench_function("suggest_worker_scaling", |b| {
        b.iter(|| suggest_amqp_worker_scaling(2000, 5, 40.0, 100));
    });

    group.bench_function("estimate_processing_capacity", |b| {
        b.iter(|| estimate_amqp_processing_capacity(10, 50.0, Some(5000)));
    });

    group.bench_function("assess_queue_health", |b| {
        b.iter(|| assess_amqp_queue_health("test_queue", 1000, 5, 50.0, 10_000_000));
    });

    let ages = vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0];
    group.bench_function("calculate_message_age_distribution", |b| {
        b.iter(|| calculate_amqp_message_age_distribution(ages.clone(), 60.0));
    });

    group.finish();
}

/// Benchmark v5 utility functions
fn bench_utility_functions(c: &mut Criterion) {
    use celers_broker_amqp::utilities::*;
    use celers_broker_amqp::QueueType;

    let mut group = c.benchmark_group("utility_functions");

    group.bench_function("calculate_optimal_batch_size", |b| {
        b.iter(|| calculate_optimal_amqp_batch_size(1000, 1024, 100));
    });

    group.bench_function("estimate_queue_memory", |b| {
        b.iter(|| estimate_amqp_queue_memory(1000, 1024, QueueType::Classic));
    });

    group.bench_function("calculate_channel_pool_size", |b| {
        b.iter(|| calculate_optimal_amqp_channel_pool_size(100, 50));
    });

    group.bench_function("calculate_pipeline_depth", |b| {
        b.iter(|| calculate_amqp_pipeline_depth(100, 10));
    });

    group.bench_function("calculate_prefetch", |b| {
        b.iter(|| calculate_optimal_amqp_prefetch(5, 100, 10));
    });

    group.bench_function("estimate_drain_time", |b| {
        b.iter(|| estimate_amqp_drain_time(1000, 5, 10.0));
    });

    group.bench_function("calculate_connection_pool_size", |b| {
        b.iter(|| calculate_optimal_amqp_connection_pool_size(100, 2047));
    });

    group.bench_function("estimate_confirm_latency", |b| {
        b.iter(|| estimate_amqp_confirm_latency(QueueType::Classic, 10));
    });

    group.bench_function("calculate_max_throughput", |b| {
        b.iter(|| calculate_amqp_max_throughput(QueueType::Classic, 1024, 1000));
    });

    group.bench_function("should_use_lazy_mode", |b| {
        b.iter(|| should_use_amqp_lazy_mode(100_000, 10_240, 1_000_000_000));
    });

    group.finish();
}

/// Benchmark v6 circuit breaker operations
fn bench_circuit_breaker(c: &mut Criterion) {
    use celers_broker_amqp::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
    use std::time::Duration;

    let mut group = c.benchmark_group("v6_circuit_breaker");

    let config = CircuitBreakerConfig {
        failure_threshold: 5,
        success_threshold: 2,
        timeout: Duration::from_secs(60),
        half_open_max_calls: 3,
    };

    group.bench_function("create_circuit_breaker", |b| {
        b.iter(|| CircuitBreaker::new(config.clone()));
    });

    group.finish();
}

/// Benchmark v6 retry strategies
fn bench_retry_strategies(c: &mut Criterion) {
    use celers_broker_amqp::retry::{ExponentialBackoff, Jitter, RetryStrategy};
    use std::time::Duration;

    let mut group = c.benchmark_group("v6_retry");

    let strategy = ExponentialBackoff::new(Duration::from_millis(100))
        .with_max_delay(Duration::from_secs(30))
        .with_jitter(Jitter::Full);

    group.bench_function("exponential_backoff_no_jitter", |b| {
        let strat = ExponentialBackoff::new(Duration::from_millis(100));
        b.iter(|| strat.next_delay(3));
    });

    group.bench_function("exponential_backoff_full_jitter", |b| {
        b.iter(|| strategy.next_delay(3));
    });

    group.bench_function("exponential_backoff_equal_jitter", |b| {
        let strat = ExponentialBackoff::new(Duration::from_millis(100)).with_jitter(Jitter::Equal);
        b.iter(|| strat.next_delay(3));
    });

    group.finish();
}

/// Benchmark v6 compression operations
fn bench_compression(c: &mut Criterion) {
    use celers_broker_amqp::compression::{compress_message, decompress_message};
    use celers_protocol::compression::CompressionType;

    let mut group = c.benchmark_group("v6_compression");

    let small_data = b"Hello, World! ".repeat(10);
    let medium_data = b"Hello, World! ".repeat(100);
    let large_data = b"Hello, World! ".repeat(1000);

    group.bench_function("gzip_compress_small", |b| {
        b.iter(|| compress_message(&small_data, CompressionType::Gzip));
    });

    group.bench_function("gzip_compress_medium", |b| {
        b.iter(|| compress_message(&medium_data, CompressionType::Gzip));
    });

    group.bench_function("gzip_compress_large", |b| {
        b.iter(|| compress_message(&large_data, CompressionType::Gzip));
    });

    let compressed =
        compress_message(&medium_data, CompressionType::Gzip).expect("compress failed");
    group.bench_function("gzip_decompress_medium", |b| {
        b.iter(|| decompress_message(&compressed, CompressionType::Gzip));
    });

    group.bench_function("zstd_compress_medium", |b| {
        b.iter(|| compress_message(&medium_data, CompressionType::Zstd));
    });

    let zstd_compressed =
        compress_message(&medium_data, CompressionType::Zstd).expect("compress failed");
    group.bench_function("zstd_decompress_medium", |b| {
        b.iter(|| decompress_message(&zstd_compressed, CompressionType::Zstd));
    });

    group.finish();
}

/// Benchmark v6 topology operations
fn bench_topology(c: &mut Criterion) {
    use celers_broker_amqp::topology::{
        calculate_topology_complexity, validate_queue_naming, ExchangeDefinition, TopologyValidator,
    };
    use celers_broker_amqp::AmqpExchangeType;

    let mut group = c.benchmark_group("v6_topology");

    group.bench_function("validate_queue_naming", |b| {
        b.iter(|| validate_queue_naming("tasks.high", "tasks.*"));
    });

    group.bench_function("calculate_complexity", |b| {
        b.iter(|| calculate_topology_complexity(10, 20, 30));
    });

    group.bench_function("create_validator", |b| {
        b.iter(TopologyValidator::new);
    });

    let exchange = ExchangeDefinition {
        name: "test".to_string(),
        exchange_type: AmqpExchangeType::Direct,
        durable: true,
        auto_delete: false,
    };

    group.bench_function("add_exchange", |b| {
        b.iter(|| {
            let mut v = TopologyValidator::new();
            let _ = v.add_exchange(exchange.clone());
        });
    });

    group.finish();
}

/// Benchmark v6 consumer group operations
fn bench_consumer_groups(c: &mut Criterion) {
    use celers_broker_amqp::consumer_groups::{ConsumerGroup, ConsumerInfo, LoadBalancingStrategy};

    let mut group = c.benchmark_group("v6_consumer_groups");

    group.bench_function("create_consumer_group", |b| {
        b.iter(|| ConsumerGroup::new("test".to_string(), LoadBalancingStrategy::RoundRobin));
    });

    let mut consumer_group =
        ConsumerGroup::new("test".to_string(), LoadBalancingStrategy::RoundRobin);
    for i in 0..10 {
        consumer_group.add_consumer(ConsumerInfo::new(
            format!("consumer-{}", i),
            "queue".to_string(),
        ));
    }

    group.bench_function("next_consumer_round_robin", |b| {
        let mut g = consumer_group.clone();
        b.iter(|| g.next_consumer());
    });

    let mut least_conn_group =
        ConsumerGroup::new("test".to_string(), LoadBalancingStrategy::LeastConnections);
    for i in 0..10 {
        least_conn_group.add_consumer(ConsumerInfo::new(
            format!("consumer-{}", i),
            "queue".to_string(),
        ));
    }

    group.bench_function("next_consumer_least_connections", |b| {
        let mut g = least_conn_group.clone();
        b.iter(|| g.next_consumer());
    });

    group.bench_function("get_statistics", |b| {
        b.iter(|| consumer_group.get_statistics());
    });

    group.finish();
}

/// Benchmark v9 backpressure management
fn bench_backpressure(c: &mut Criterion) {
    use celers_broker_amqp::backpressure::{BackpressureConfig, BackpressureManager};
    use std::time::Duration;

    let mut group = c.benchmark_group("v9_backpressure");

    let config = BackpressureConfig {
        max_queue_depth: 10_000,
        warning_threshold: 0.7,
        critical_threshold: 0.9,
        check_interval: Duration::from_secs(5),
        min_prefetch: 1,
        max_prefetch: 100,
        prefetch_adjustment_factor: 0.2,
    };

    group.bench_function("create_manager", |b| {
        b.iter(|| BackpressureManager::new(config.clone()));
    });

    let mut manager = BackpressureManager::new(config);

    group.bench_function("update_queue_depth", |b| {
        b.iter(|| manager.update_queue_depth(5000));
    });

    group.bench_function("calculate_backpressure_level", |b| {
        b.iter(|| manager.calculate_backpressure_level());
    });

    group.bench_function("calculate_optimal_prefetch", |b| {
        b.iter(|| manager.calculate_optimal_prefetch());
    });

    group.bench_function("should_apply_backpressure", |b| {
        b.iter(|| manager.should_apply_backpressure());
    });

    group.finish();
}

/// Benchmark v9 poison message detection
fn bench_poison_detector(c: &mut Criterion) {
    use celers_broker_amqp::poison_detector::{PoisonDetector, PoisonDetectorConfig};
    use std::time::Duration;

    let mut group = c.benchmark_group("v9_poison_detector");

    let config = PoisonDetectorConfig {
        max_retries: 3,
        retry_window: Duration::from_secs(300),
        failure_rate_threshold: 0.8,
        min_samples: 5,
    };

    group.bench_function("create_detector", |b| {
        b.iter(|| PoisonDetector::new(config.clone()));
    });

    let mut detector = PoisonDetector::new(config);

    group.bench_function("record_failure", |b| {
        b.iter(|| detector.record_failure("test-msg", "Error"));
    });

    group.bench_function("record_success", |b| {
        b.iter(|| detector.record_success("test-msg"));
    });

    group.bench_function("is_poison", |b| {
        b.iter(|| detector.is_poison("test-msg"));
    });

    group.bench_function("get_poisoned_messages", |b| {
        b.iter(|| detector.get_poisoned_messages());
    });

    group.finish();
}

/// Benchmark v9 message routing
fn bench_router(c: &mut Criterion) {
    use celers_broker_amqp::router::{MessageRouter, RouteBuilder, RouteCondition};

    let mut group = c.benchmark_group("v9_router");

    group.bench_function("create_router", |b| {
        b.iter(|| MessageRouter::new("default"));
    });

    let mut router = MessageRouter::new("default");

    // Add some rules
    router.add_rule(
        RouteBuilder::new("high_priority")
            .condition(RouteCondition::Priority(9))
            .to_queue("priority_queue")
            .build()
            .unwrap(),
    );

    router.add_rule(
        RouteBuilder::new("large_messages")
            .condition(RouteCondition::PayloadSize {
                min: Some(1024),
                max: None,
            })
            .to_queue("large_queue")
            .build()
            .unwrap(),
    );

    group.bench_function("route_message", |b| {
        b.iter(|| router.route(b"test message", Some(5)));
    });

    group.bench_function("route_priority", |b| {
        b.iter(|| router.route(b"priority message", Some(9)));
    });

    let large_payload = vec![0u8; 2048];
    group.bench_function("route_large_message", |b| {
        b.iter(|| router.route(&large_payload, None));
    });

    group.finish();
}

/// Benchmark v9 performance optimization
fn bench_optimization(c: &mut Criterion) {
    use celers_broker_amqp::optimization::{OptimizationConfig, PerformanceOptimizer};

    let mut group = c.benchmark_group("v9_optimization");

    let config = OptimizationConfig::default();

    group.bench_function("create_optimizer", |b| {
        b.iter(|| PerformanceOptimizer::new(config.clone()));
    });

    let mut optimizer = PerformanceOptimizer::new(config);

    group.bench_function("record_latency", |b| {
        b.iter(|| optimizer.record_latency(50.0));
    });

    group.bench_function("record_throughput", |b| {
        b.iter(|| optimizer.record_throughput(1000.0));
    });

    group.bench_function("record_memory", |b| {
        b.iter(|| optimizer.record_memory_usage(100_000_000));
    });

    // Pre-populate with some data
    for i in 0..50 {
        optimizer.record_latency(50.0 + i as f64);
        optimizer.record_throughput(1000.0 + i as f64 * 10.0);
    }

    group.bench_function("get_recommendations", |b| {
        b.iter(|| optimizer.get_recommendations());
    });

    group.bench_function("calculate_optimal_prefetch", |b| {
        b.iter(|| optimizer.calculate_optimal_prefetch());
    });

    group.bench_function("calculate_optimal_batch", |b| {
        b.iter(|| optimizer.calculate_optimal_batch_size());
    });

    group.bench_function("performance_score", |b| {
        b.iter(|| optimizer.performance_score());
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_serialization,
    bench_queue_config,
    bench_broker_config,
    bench_batch_operations,
    bench_helper_methods,
    bench_connection_helpers,
    bench_monitoring_functions,
    bench_utility_functions,
    bench_circuit_breaker,
    bench_retry_strategies,
    bench_compression,
    bench_topology,
    bench_consumer_groups,
    bench_backpressure,
    bench_poison_detector,
    bench_router,
    bench_optimization
);
criterion_main!(benches);
