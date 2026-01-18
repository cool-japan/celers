//! Performance benchmarks for MySQL broker operations
//!
//! These benchmarks measure the performance of common operations:
//! - Task enqueue (single and batch)
//! - Task dequeue (single and batch)
//! - Task acknowledgment
//! - Queue statistics
//! - Task inspection
//!
//! Run with: cargo bench
//!
//! Note: Requires a running MySQL instance configured via DATABASE_URL environment variable.
//! For fair comparison with PostgreSQL broker, use the same hardware and configuration.

use celers_broker_sql::MysqlBroker;
use celers_core::{Broker, SerializedTask};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::hint::black_box;
use std::time::Duration;
use tokio::runtime::Runtime;

/// Create a test broker instance
async fn create_broker() -> MysqlBroker {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://root:password@localhost/celers_bench".to_string());

    let broker = MysqlBroker::with_queue(&database_url, "bench_queue")
        .await
        .expect("Failed to create broker");

    // Clean up before benchmarks
    let _ = broker.purge_all().await;

    broker
}

/// Helper to create a serialized task
fn create_task(id: u64) -> SerializedTask {
    let data = format!("test data {}", id);
    SerializedTask::new("bench_task".to_string(), data.into_bytes())
}

/// Benchmark single task enqueue
fn bench_enqueue_single(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let broker = rt.block_on(create_broker());

    c.bench_function("enqueue_single", |b| {
        b.to_async(&rt).iter(|| async {
            let task = create_task(black_box(1));
            broker.enqueue(task).await.unwrap();
        });
    });
}

/// Benchmark batch task enqueue with varying batch sizes
fn bench_enqueue_batch(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("enqueue_batch");

    for size in [10, 50, 100, 500, 1000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let broker = rt.block_on(create_broker());

            b.to_async(&rt).iter(|| async {
                let tasks: Vec<SerializedTask> = (0..size).map(create_task).collect();

                broker.enqueue_batch(tasks).await.unwrap();
            });
        });
    }

    group.finish();
}

/// Benchmark single task dequeue
fn bench_dequeue_single(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let broker = rt.block_on(create_broker());

    // Pre-populate with tasks
    rt.block_on(async {
        let tasks: Vec<SerializedTask> = (0..1000).map(create_task).collect();
        broker.enqueue_batch(tasks).await.unwrap();
    });

    c.bench_function("dequeue_single", |b| {
        b.to_async(&rt).iter(|| async {
            if let Some(msg) = broker.dequeue().await.unwrap() {
                // Acknowledge to avoid reprocessing
                broker
                    .ack(&msg.task_id(), msg.receipt_handle.as_deref())
                    .await
                    .unwrap();
            }
        });
    });
}

/// Benchmark batch task dequeue with varying batch sizes
fn bench_dequeue_batch(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("dequeue_batch");

    for size in [10, 50, 100].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let broker = rt.block_on(create_broker());

            // Pre-populate with tasks
            rt.block_on(async {
                let tasks: Vec<SerializedTask> = (0..10000).map(create_task).collect();
                broker.enqueue_batch(tasks).await.unwrap();
            });

            b.to_async(&rt).iter(|| async {
                let messages = broker
                    .dequeue_batch(black_box(size as usize))
                    .await
                    .unwrap();

                // Acknowledge all to avoid reprocessing
                let acks: Vec<_> = messages
                    .iter()
                    .map(|m| (m.task_id(), m.receipt_handle.clone()))
                    .collect();
                broker.ack_batch(&acks).await.unwrap();
            });
        });
    }

    group.finish();
}

/// Benchmark task acknowledgment
fn bench_ack(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("ack", |b| {
        b.to_async(&rt).iter_batched(
            || {
                // Setup: Create broker, enqueue and dequeue a task
                rt.block_on(async {
                    let broker = create_broker().await;
                    let task = create_task(1);
                    broker.enqueue(task).await.unwrap();
                    let msg = broker.dequeue().await.unwrap().unwrap();
                    (broker, msg)
                })
            },
            |(broker, msg)| async move {
                broker
                    .ack(&msg.task_id(), msg.receipt_handle.as_deref())
                    .await
                    .unwrap();
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

/// Benchmark batch acknowledgment
fn bench_ack_batch(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("ack_batch");

    for size in [10, 50, 100].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.to_async(&rt).iter_batched(
                || {
                    // Setup: Create broker, enqueue and dequeue tasks
                    rt.block_on(async {
                        let broker = create_broker().await;
                        let tasks: Vec<SerializedTask> = (0..size).map(create_task).collect();
                        broker.enqueue_batch(tasks).await.unwrap();
                        let messages = broker.dequeue_batch(size as usize).await.unwrap();
                        (broker, messages)
                    })
                },
                |(broker, messages)| async move {
                    let acks: Vec<_> = messages
                        .iter()
                        .map(|m| (m.task_id(), m.receipt_handle.clone()))
                        .collect();
                    broker.ack_batch(&acks).await.unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

/// Benchmark queue size query
fn bench_queue_size(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let broker = rt.block_on(create_broker());

    // Pre-populate with tasks
    rt.block_on(async {
        let tasks: Vec<SerializedTask> = (0..1000).map(create_task).collect();
        broker.enqueue_batch(tasks).await.unwrap();
    });

    c.bench_function("queue_size", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(broker.queue_size().await.unwrap());
        });
    });
}

/// Benchmark queue statistics query
fn bench_statistics(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let broker = rt.block_on(create_broker());

    // Pre-populate with tasks in various states
    rt.block_on(async {
        let tasks: Vec<SerializedTask> = (0..500).map(create_task).collect();
        broker.enqueue_batch(tasks).await.unwrap();

        // Dequeue some to create processing state
        broker.dequeue_batch(100).await.unwrap();
    });

    c.bench_function("get_statistics", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(broker.get_statistics().await.unwrap());
        });
    });
}

/// Benchmark task rejection with requeue
fn bench_reject_requeue(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("reject_requeue", |b| {
        b.to_async(&rt).iter_batched(
            || {
                // Setup: Create broker, enqueue and dequeue a task
                rt.block_on(async {
                    let broker = create_broker().await;
                    let task = create_task(1);
                    broker.enqueue(task).await.unwrap();
                    let msg = broker.dequeue().await.unwrap().unwrap();
                    (broker, msg)
                })
            },
            |(broker, msg)| async move {
                broker
                    .reject(&msg.task_id(), msg.receipt_handle.as_deref(), true)
                    .await
                    .unwrap();
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

/// Benchmark scheduled task enqueue
fn bench_enqueue_scheduled(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let broker = rt.block_on(create_broker());

    c.bench_function("enqueue_after", |b| {
        b.to_async(&rt).iter(|| async {
            let task = create_task(black_box(1));
            // Schedule 60 seconds in the future
            broker.enqueue_after(task, 60).await.unwrap();
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(100)
        .measurement_time(Duration::from_secs(10))
        .warm_up_time(Duration::from_secs(3));
    targets =
        bench_enqueue_single,
        bench_enqueue_batch,
        bench_dequeue_single,
        bench_dequeue_batch,
        bench_ack,
        bench_ack_batch,
        bench_queue_size,
        bench_statistics,
        bench_reject_requeue,
        bench_enqueue_scheduled
}

criterion_main!(benches);
