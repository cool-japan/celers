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

criterion_group!(
    benches,
    bench_serialization,
    bench_queue_config,
    bench_broker_config,
    bench_batch_operations,
    bench_helper_methods,
    bench_connection_helpers
);
criterion_main!(benches);
