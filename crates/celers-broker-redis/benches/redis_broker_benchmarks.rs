//! Comprehensive benchmarks for Redis broker operations
//!
//! Run with: cargo bench --bench redis_broker_benchmarks
//! Requires: Redis running on localhost:6379

use celers_broker_redis::{QueueMode, RedisBroker};
use celers_core::{Broker, SerializedTask, TaskMetadata};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
use tokio::runtime::Runtime;

fn create_task(name: &str, priority: i32, payload_size: usize) -> SerializedTask {
    let mut metadata = TaskMetadata::new(name.to_string());
    metadata.priority = priority;
    SerializedTask {
        metadata,
        payload: vec![0u8; payload_size],
    }
}

fn bench_enqueue_single(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let broker = rt.block_on(async {
        let b = RedisBroker::new("redis://localhost:6379", "bench_enqueue").unwrap();
        b.purge_all_queues().await.ok();
        b
    });

    c.bench_function("enqueue_single_small", |b| {
        b.to_async(&rt).iter(|| async {
            let task = create_task("test", 5, 100);
            broker.enqueue(black_box(task)).await.unwrap();
        });
    });

    rt.block_on(broker.purge_all_queues()).ok();
}

fn bench_enqueue_batch(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let broker = rt.block_on(async {
        let b = RedisBroker::new("redis://localhost:6379", "bench_batch").unwrap();
        b.purge_all_queues().await.ok();
        b
    });

    let mut group = c.benchmark_group("enqueue_batch");

    for batch_size in [10, 50, 100, 500].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            batch_size,
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    let tasks: Vec<_> = (0..size)
                        .map(|i| create_task(&format!("task_{}", i), 5, 100))
                        .collect();
                    broker.enqueue_batch(black_box(tasks)).await.unwrap();
                });
            },
        );
    }

    group.finish();
    rt.block_on(broker.purge_all_queues()).ok();
}

fn bench_dequeue_single(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let broker = rt.block_on(async {
        let b = RedisBroker::new("redis://localhost:6379", "bench_dequeue").unwrap();
        b.purge_all_queues().await.ok();

        // Pre-populate queue
        let tasks: Vec<_> = (0..1000)
            .map(|i| create_task(&format!("task_{}", i), 5, 100))
            .collect();
        b.enqueue_batch(tasks).await.unwrap();
        b
    });

    c.bench_function("dequeue_single", |b| {
        b.to_async(&rt).iter(|| async {
            if let Some(msg) = broker.dequeue().await.unwrap() {
                broker
                    .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
                    .await
                    .unwrap();
            }
        });
    });

    rt.block_on(broker.purge_all_queues()).ok();
}

fn bench_dequeue_batch(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let broker = rt.block_on(async {
        let b = RedisBroker::new("redis://localhost:6379", "bench_dequeue_batch").unwrap();
        b.purge_all_queues().await.ok();
        b
    });

    let mut group = c.benchmark_group("dequeue_batch");

    for batch_size in [10, 50, 100].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            batch_size,
            |b, &size| {
                b.to_async(&rt).iter_batched(
                    || {
                        // Setup: Populate queue
                        let tasks: Vec<_> = (0..size)
                            .map(|i| create_task(&format!("task_{}", i), 5, 100))
                            .collect();
                        rt.block_on(broker.enqueue_batch(tasks)).unwrap();
                    },
                    |_| async {
                        let messages = broker.dequeue_batch(black_box(size)).await.unwrap();
                        let acks: Vec<_> = messages
                            .iter()
                            .map(|msg| (msg.task.metadata.id, msg.receipt_handle.clone()))
                            .collect();
                        broker.ack_batch(&acks).await.unwrap();
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
    rt.block_on(broker.purge_all_queues()).ok();
}

fn bench_priority_queue(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let broker = rt.block_on(async {
        let b = RedisBroker::with_mode(
            "redis://localhost:6379",
            "bench_priority",
            QueueMode::Priority,
        )
        .unwrap();
        b.purge_all_queues().await.ok();
        b
    });

    let mut group = c.benchmark_group("priority_queue");

    group.bench_function("enqueue_priority", |b| {
        b.to_async(&rt).iter(|| async {
            let task = create_task("priority_task", black_box(7), 100);
            broker.enqueue(task).await.unwrap();
        });
    });

    // Pre-populate for dequeue benchmark
    rt.block_on(async {
        let tasks: Vec<_> = (0..1000)
            .map(|i| create_task(&format!("task_{}", i), i % 10, 100))
            .collect();
        broker.enqueue_batch(tasks).await.unwrap();
    });

    group.bench_function("dequeue_priority", |b| {
        b.to_async(&rt).iter(|| async {
            if let Some(msg) = broker.dequeue().await.unwrap() {
                broker
                    .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
                    .await
                    .unwrap();
            }
        });
    });

    group.finish();
    rt.block_on(broker.purge_all_queues()).ok();
}

fn bench_payload_sizes(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let broker = rt.block_on(async {
        let b = RedisBroker::new("redis://localhost:6379", "bench_payload").unwrap();
        b.purge_all_queues().await.ok();
        b
    });

    let mut group = c.benchmark_group("payload_sizes");

    // Test different payload sizes: 1KB, 10KB, 100KB, 1MB
    for size_kb in [1, 10, 100, 1024].iter() {
        let size_bytes = size_kb * 1024;
        group.throughput(Throughput::Bytes(size_bytes as u64));
        group.bench_with_input(
            BenchmarkId::new("enqueue", format!("{}KB", size_kb)),
            &size_bytes,
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    let task = create_task("payload_test", 5, size);
                    broker.enqueue(black_box(task)).await.unwrap();
                });
            },
        );
    }

    group.finish();
    rt.block_on(broker.purge_all_queues()).ok();
}

fn bench_delayed_tasks(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let broker = rt.block_on(async {
        let b = RedisBroker::new("redis://localhost:6379", "bench_delayed").unwrap();
        b.purge_all_queues().await.ok();
        b
    });

    c.bench_function("enqueue_delayed", |b| {
        b.to_async(&rt).iter(|| async {
            let task = create_task("delayed_task", 5, 100);
            let execute_at = (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                + 60) as i64; // 1 minute in the future
            broker
                .enqueue_at(black_box(task), black_box(execute_at))
                .await
                .unwrap();
        });
    });

    rt.block_on(broker.purge_all_queues()).ok();
}

fn bench_queue_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let broker = rt.block_on(async {
        let b = RedisBroker::new("redis://localhost:6379", "bench_ops").unwrap();
        b.purge_all_queues().await.ok();

        // Pre-populate
        let tasks: Vec<_> = (0..100)
            .map(|i| create_task(&format!("task_{}", i), 5, 100))
            .collect();
        b.enqueue_batch(tasks).await.unwrap();
        b
    });

    let mut group = c.benchmark_group("queue_operations");

    group.bench_function("queue_size", |b| {
        b.to_async(&rt)
            .iter(|| async { broker.queue_size().await.unwrap() });
    });

    group.bench_function("dlq_size", |b| {
        b.to_async(&rt)
            .iter(|| async { broker.dlq_size().await.unwrap() });
    });

    group.bench_function("get_queue_stats", |b| {
        b.to_async(&rt)
            .iter(|| async { broker.get_queue_stats().await.unwrap() });
    });

    group.bench_function("ping", |b| {
        b.to_async(&rt)
            .iter(|| async { broker.ping().await.unwrap() });
    });

    group.finish();
    rt.block_on(broker.purge_all_queues()).ok();
}

fn bench_concurrent_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let broker = std::sync::Arc::new(rt.block_on(async {
        let b = RedisBroker::new("redis://localhost:6379", "bench_concurrent").unwrap();
        b.purge_all_queues().await.ok();
        b
    }));

    let mut group = c.benchmark_group("concurrent_operations");

    // Benchmark concurrent enqueue operations
    group.bench_function("concurrent_enqueue_4", |b| {
        b.to_async(&rt).iter(|| {
            let broker = broker.clone();
            async move {
                let handles: Vec<_> = (0..4)
                    .map(|i| {
                        let broker = broker.clone();
                        tokio::spawn(async move {
                            let task = create_task(&format!("task_{}", i), 5, 100);
                            broker.enqueue(task).await.unwrap();
                        })
                    })
                    .collect();

                for handle in handles {
                    handle.await.ok();
                }
            }
        });
    });

    // Benchmark concurrent dequeue operations
    rt.block_on(async {
        let tasks: Vec<_> = (0..100)
            .map(|i| create_task(&format!("task_{}", i), 5, 100))
            .collect();
        broker.enqueue_batch(tasks).await.ok();
    });

    group.bench_function("concurrent_dequeue_4", |b| {
        b.to_async(&rt).iter(|| {
            let broker = broker.clone();
            async move {
                let handles: Vec<_> = (0..4)
                    .map(|_| {
                        let broker = broker.clone();
                        tokio::spawn(async move {
                            if let Ok(Some(msg)) = broker.dequeue().await {
                                broker
                                    .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
                                    .await
                                    .ok();
                            }
                        })
                    })
                    .collect();

                for handle in handles {
                    handle.await.ok();
                }
            }
        });
    });

    group.finish();
    rt.block_on(broker.purge_all_queues()).ok();
}

fn bench_backup_restore(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let broker = std::sync::Arc::new(rt.block_on(async {
        let b = RedisBroker::new("redis://localhost:6379", "bench_backup").unwrap();
        b.purge_all_queues().await.ok();

        // Pre-populate queue
        let tasks: Vec<_> = (0..100)
            .map(|i| create_task(&format!("task_{}", i), i % 10, 100))
            .collect();
        b.enqueue_batch(tasks).await.unwrap();
        b
    }));

    let mut group = c.benchmark_group("backup_restore");

    group.bench_function("create_snapshot", |b| {
        b.to_async(&rt).iter(|| {
            let broker = broker.clone();
            async move {
                let backup_mgr = broker.backup_manager();
                backup_mgr.create_snapshot().await.unwrap();
            }
        });
    });

    group.bench_function("restore_snapshot", |b| {
        let broker_clone = broker.clone();
        b.to_async(&rt).iter_batched(
            || {
                // Setup: Create snapshot
                let broker_setup = broker_clone.clone();
                rt.block_on(async move {
                    let backup_mgr = broker_setup.backup_manager();
                    backup_mgr.create_snapshot().await.unwrap()
                })
            },
            |snapshot| {
                let broker_restore = broker_clone.clone();
                async move {
                    let backup_mgr = broker_restore.backup_manager();
                    backup_mgr.restore_from_snapshot(&snapshot, true).await.ok();
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
    rt.block_on(broker.purge_all_queues()).ok();
}

fn bench_priority_management(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let broker = rt.block_on(async {
        let b = RedisBroker::with_mode(
            "redis://localhost:6379",
            "bench_priority_mgmt",
            QueueMode::Priority,
        )
        .unwrap();
        b.purge_all_queues().await.ok();

        // Pre-populate queue
        let tasks: Vec<_> = (0..200)
            .map(|i| create_task(&format!("task_{}", i), i % 10, 100))
            .collect();
        b.enqueue_batch(tasks).await.unwrap();
        b
    });

    let mut group = c.benchmark_group("priority_management");

    group.bench_function("get_priority_histogram", |b| {
        b.to_async(&rt).iter(|| async {
            let priority_mgr = broker.priority_manager();
            priority_mgr.get_priority_histogram().await.ok();
        });
    });

    group.bench_function("normalize_priorities", |b| {
        b.to_async(&rt).iter(|| async {
            let priority_mgr = broker.priority_manager();
            priority_mgr.normalize_priorities().await.ok();
        });
    });

    group.finish();
    rt.block_on(broker.purge_all_queues()).ok();
}

fn bench_task_query(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let broker = rt.block_on(async {
        let b = RedisBroker::new("redis://localhost:6379", "bench_query").unwrap();
        b.purge_all_queues().await.ok();

        // Pre-populate queue with varied tasks
        let tasks: Vec<_> = (0..100)
            .map(|i| create_task(&format!("task_{}", i % 5), i % 10, 100))
            .collect();
        b.enqueue_batch(tasks).await.unwrap();
        b
    });

    let mut group = c.benchmark_group("task_query");

    group.bench_function("peek_tasks", |b| {
        b.to_async(&rt).iter(|| async {
            let task_query = broker.task_query();
            task_query.peek(black_box(10)).await.ok();
        });
    });

    group.bench_function("count_matching", |b| {
        b.to_async(&rt).iter(|| async {
            use celers_broker_redis::TaskSearchCriteria;
            let task_query = broker.task_query();
            let criteria = TaskSearchCriteria::new().with_name("task_1");
            task_query.count_matching(&criteria).await.ok();
        });
    });

    group.bench_function("get_task_stats", |b| {
        b.to_async(&rt).iter(|| async {
            let task_query = broker.task_query();
            task_query.get_task_stats().await.ok();
        });
    });

    group.finish();
    rt.block_on(broker.purge_all_queues()).ok();
}

fn bench_batch_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let broker = rt.block_on(async {
        let b = RedisBroker::with_mode(
            "redis://localhost:6379",
            "bench_batch_ops",
            QueueMode::Priority,
        )
        .unwrap();
        b.purge_all_queues().await.ok();

        // Pre-populate queue
        let tasks: Vec<_> = (0..200)
            .map(|i| create_task(&format!("task_{}", i % 10), i % 10, 100))
            .collect();
        b.enqueue_batch(tasks).await.unwrap();
        b
    });

    let mut group = c.benchmark_group("batch_operations");

    group.bench_function("dequeue_batch_by_priority_range", |b| {
        b.to_async(&rt).iter(|| async {
            let batch_ops = broker.batch_operations();
            batch_ops
                .dequeue_batch_by_priority_range(black_box(5), black_box(8), black_box(10))
                .await
                .ok();
        });
    });

    group.bench_function("dequeue_batch_by_name", |b| {
        b.to_async(&rt).iter(|| async {
            let batch_ops = broker.batch_operations();
            batch_ops
                .dequeue_batch_by_name(black_box(10), black_box("task_1"))
                .await
                .ok();
        });
    });

    group.finish();
    rt.block_on(broker.purge_all_queues()).ok();
}

fn bench_monitoring_utilities(c: &mut Criterion) {
    use celers_broker_redis::monitoring::*;

    let mut group = c.benchmark_group("monitoring_utilities");

    group.bench_function("analyze_consumer_lag", |b| {
        b.iter(|| analyze_redis_consumer_lag(black_box(1000), black_box(50.0), black_box(100)));
    });

    group.bench_function("calculate_message_velocity", |b| {
        b.iter(|| {
            calculate_redis_message_velocity(black_box(1000), black_box(1500), black_box(60.0))
        });
    });

    group.bench_function("suggest_worker_scaling", |b| {
        b.iter(|| {
            suggest_redis_worker_scaling(
                black_box(2000),
                black_box(5),
                black_box(40.0),
                black_box(100),
            )
        });
    });

    group.bench_function("calculate_message_age_distribution", |b| {
        let ages: Vec<f64> = (1..=100).map(|i| i as f64 * 10.0).collect();
        b.iter(|| calculate_redis_message_age_distribution(black_box(&ages), black_box(500.0)));
    });

    group.bench_function("estimate_processing_capacity", |b| {
        b.iter(|| {
            estimate_redis_processing_capacity(black_box(10), black_box(50.0), black_box(5000))
        });
    });

    group.bench_function("calculate_queue_health_score", |b| {
        b.iter(|| {
            calculate_redis_queue_health_score(
                black_box(500),
                black_box(50.0),
                black_box(1000),
                black_box(40.0),
            )
        });
    });

    group.finish();
}

fn bench_utility_functions(c: &mut Criterion) {
    use celers_broker_redis::utilities::*;
    use celers_broker_redis::QueueMode;

    let mut group = c.benchmark_group("utility_functions");

    group.bench_function("calculate_optimal_batch_size", |b| {
        b.iter(|| {
            calculate_optimal_redis_batch_size(black_box(1000), black_box(1024), black_box(100))
        });
    });

    group.bench_function("estimate_queue_memory", |b| {
        b.iter(|| {
            estimate_redis_queue_memory(
                black_box(1000),
                black_box(1024),
                black_box(QueueMode::Fifo),
            )
        });
    });

    group.bench_function("calculate_optimal_pool_size", |b| {
        b.iter(|| calculate_optimal_redis_pool_size(black_box(100), black_box(50)));
    });

    group.bench_function("calculate_pipeline_size", |b| {
        b.iter(|| calculate_redis_pipeline_size(black_box(100), black_box(10)));
    });

    group.bench_function("estimate_drain_time", |b| {
        b.iter(|| estimate_redis_queue_drain_time(black_box(1000), black_box(50.0)));
    });

    group.bench_function("suggest_pipeline_strategy", |b| {
        b.iter(|| suggest_redis_pipeline_strategy(black_box(500), black_box("write")));
    });

    group.bench_function("calculate_key_ttl_by_priority", |b| {
        b.iter(|| calculate_redis_key_ttl_by_priority(black_box(150), black_box(3600)));
    });

    group.bench_function("calculate_timeout_values", |b| {
        b.iter(|| calculate_redis_timeout_values(black_box(50.0), black_box(200.0)));
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_enqueue_single,
    bench_enqueue_batch,
    bench_dequeue_single,
    bench_dequeue_batch,
    bench_priority_queue,
    bench_payload_sizes,
    bench_delayed_tasks,
    bench_queue_operations,
    bench_concurrent_operations,
    bench_backup_restore,
    bench_priority_management,
    bench_task_query,
    bench_batch_operations,
    bench_monitoring_utilities,
    bench_utility_functions,
);

criterion_main!(benches);
