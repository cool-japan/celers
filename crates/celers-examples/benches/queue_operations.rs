//! Queue operations performance benchmarks
//!
//! Measures enqueue/dequeue performance

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::hint::black_box;
// Mock benchmarks since we can't use async criterion easily
// These serve as templates for performance testing

fn bench_task_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("task_creation");
    group.throughput(Throughput::Elements(1));

    use celers_core::SerializedTask;

    group.bench_function("new_task", |b| {
        b.iter(|| {
            let payload = serde_json::to_vec(&vec![1u64, 2, 3, 4, 5]).unwrap();
            let task = SerializedTask::new(black_box("test_task".to_string()), black_box(payload));
            black_box(task);
        });
    });

    group.bench_function("task_with_priority", |b| {
        b.iter(|| {
            let payload = serde_json::to_vec(&vec![1u64, 2, 3, 4, 5]).unwrap();
            let task = SerializedTask::new(black_box("test_task".to_string()), black_box(payload))
                .with_priority(black_box(5));
            black_box(task);
        });
    });

    group.bench_function("task_with_options", |b| {
        b.iter(|| {
            let payload = serde_json::to_vec(&vec![1u64, 2, 3, 4, 5]).unwrap();
            let task = SerializedTask::new(black_box("test_task".to_string()), black_box(payload))
                .with_priority(black_box(5))
                .with_max_retries(black_box(3))
                .with_timeout(black_box(60));
            black_box(task);
        });
    });

    group.finish();
}

fn bench_metadata_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("metadata_operations");
    group.throughput(Throughput::Elements(1));

    use celers_core::TaskMetadata;

    group.bench_function("create_metadata", |b| {
        b.iter(|| {
            let metadata = TaskMetadata::new(black_box("test_task".to_string()));
            black_box(metadata);
        });
    });

    group.bench_function("clone_metadata", |b| {
        let metadata = TaskMetadata::new("test_task".to_string());
        b.iter(|| {
            let cloned = black_box(&metadata).clone();
            black_box(cloned);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_task_creation, bench_metadata_operations);
criterion_main!(benches);
