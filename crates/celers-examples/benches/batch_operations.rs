//! Benchmark for batch enqueue/dequeue operations
//!
//! Compares performance of:
//! - Individual enqueue vs batch enqueue
//! - Individual dequeue vs batch dequeue
//!
//! Run with: cargo bench --bench batch_operations

use celers_core::{Broker, SerializedTask};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::hint::black_box;
// Mock broker for benchmarking
struct MockBroker {
    tasks: std::sync::Mutex<Vec<SerializedTask>>,
}

impl MockBroker {
    fn new() -> Self {
        Self {
            tasks: std::sync::Mutex::new(Vec::new()),
        }
    }
}

#[async_trait::async_trait]
impl Broker for MockBroker {
    async fn enqueue(&self, task: SerializedTask) -> celers_core::Result<celers_core::TaskId> {
        let task_id = task.metadata.id;
        self.tasks.lock().unwrap().push(task);
        Ok(task_id)
    }

    async fn dequeue(&self) -> celers_core::Result<Option<celers_core::BrokerMessage>> {
        let task = self.tasks.lock().unwrap().pop();
        Ok(task.map(|t| celers_core::BrokerMessage {
            task: t.clone(),
            receipt_handle: None,
        }))
    }

    async fn ack(
        &self,
        _task_id: &celers_core::TaskId,
        _receipt_handle: Option<&str>,
    ) -> celers_core::Result<()> {
        Ok(())
    }

    async fn reject(
        &self,
        _task_id: &celers_core::TaskId,
        _receipt_handle: Option<&str>,
        _requeue: bool,
    ) -> celers_core::Result<()> {
        Ok(())
    }

    async fn queue_size(&self) -> celers_core::Result<usize> {
        Ok(self.tasks.lock().unwrap().len())
    }

    async fn cancel(&self, _task_id: &celers_core::TaskId) -> celers_core::Result<bool> {
        Ok(false)
    }

    // Optimized batch implementation
    async fn enqueue_batch(
        &self,
        tasks: Vec<SerializedTask>,
    ) -> celers_core::Result<Vec<celers_core::TaskId>> {
        let task_ids: Vec<_> = tasks.iter().map(|t| t.metadata.id).collect();
        self.tasks.lock().unwrap().extend(tasks);
        Ok(task_ids)
    }

    async fn dequeue_batch(
        &self,
        count: usize,
    ) -> celers_core::Result<Vec<celers_core::BrokerMessage>> {
        let mut tasks_lock = self.tasks.lock().unwrap();
        let available = tasks_lock.len().min(count);
        let len = tasks_lock.len();
        let tasks: Vec<_> = tasks_lock.drain(len - available..).collect();
        Ok(tasks
            .into_iter()
            .map(|t| celers_core::BrokerMessage {
                task: t,
                receipt_handle: None,
            })
            .collect())
    }
}

fn create_test_tasks(count: usize) -> Vec<SerializedTask> {
    (0..count)
        .map(|i| {
            SerializedTask::new(
                format!("test_task_{}", i),
                serde_json::to_vec(&serde_json::json!({"arg": i})).unwrap(),
            )
        })
        .collect()
}

fn bench_enqueue_individual(c: &mut Criterion) {
    let mut group = c.benchmark_group("enqueue_individual");

    for size in [10, 50, 100, 500].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            b.iter(|| {
                runtime.block_on(async {
                    let broker = MockBroker::new();
                    let tasks = create_test_tasks(size);
                    for task in tasks {
                        black_box(broker.enqueue(task).await.unwrap());
                    }
                })
            });
        });
    }
    group.finish();
}

fn bench_enqueue_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("enqueue_batch");

    for size in [10, 50, 100, 500].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            b.iter(|| {
                runtime.block_on(async {
                    let broker = MockBroker::new();
                    let tasks = create_test_tasks(size);
                    black_box(broker.enqueue_batch(tasks).await.unwrap());
                })
            });
        });
    }
    group.finish();
}

fn bench_dequeue_individual(c: &mut Criterion) {
    let mut group = c.benchmark_group("dequeue_individual");

    for size in [10, 50, 100, 500].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            b.iter(|| {
                runtime.block_on(async {
                    let broker = MockBroker::new();
                    // Pre-fill queue
                    let tasks = create_test_tasks(size);
                    broker.enqueue_batch(tasks).await.unwrap();

                    // Dequeue one by one
                    for _ in 0..size {
                        black_box(broker.dequeue().await.unwrap());
                    }
                })
            });
        });
    }
    group.finish();
}

fn bench_dequeue_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("dequeue_batch");

    for size in [10, 50, 100, 500].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            b.iter(|| {
                runtime.block_on(async {
                    let broker = MockBroker::new();
                    // Pre-fill queue
                    let tasks = create_test_tasks(size);
                    broker.enqueue_batch(tasks).await.unwrap();

                    // Dequeue in batch
                    black_box(broker.dequeue_batch(size).await.unwrap());
                })
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_enqueue_individual,
    bench_enqueue_batch,
    bench_dequeue_individual,
    bench_dequeue_batch
);
criterion_main!(benches);
