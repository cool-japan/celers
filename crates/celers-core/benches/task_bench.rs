use celers_core::{SerializedTask, TaskMetadata};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use uuid::Uuid;

fn bench_task_metadata_creation(c: &mut Criterion) {
    c.bench_function("task_metadata_new", |b| {
        b.iter(|| {
            black_box(TaskMetadata::new("test_task".to_string()));
        });
    });

    c.bench_function("task_metadata_with_options", |b| {
        b.iter(|| {
            black_box(
                TaskMetadata::new("test_task".to_string())
                    .with_max_retries(5)
                    .with_timeout(60)
                    .with_priority(10),
            );
        });
    });

    c.bench_function("task_metadata_with_dependencies", |b| {
        let deps: Vec<Uuid> = (0..10).map(|_| Uuid::new_v4()).collect();
        b.iter(|| {
            black_box(TaskMetadata::new("test_task".to_string()).with_dependencies(deps.clone()));
        });
    });
}

fn bench_task_metadata_clone(c: &mut Criterion) {
    let metadata = TaskMetadata::new("test_task".to_string())
        .with_max_retries(5)
        .with_timeout(60)
        .with_priority(10);

    c.bench_function("task_metadata_clone_simple", |b| {
        b.iter(|| {
            black_box(metadata.clone());
        });
    });

    let metadata_with_deps = TaskMetadata::new("test_task".to_string())
        .with_dependencies((0..10).map(|_| Uuid::new_v4()));

    c.bench_function("task_metadata_clone_with_deps", |b| {
        b.iter(|| {
            black_box(metadata_with_deps.clone());
        });
    });
}

fn bench_serialized_task_creation(c: &mut Criterion) {
    let payload = vec![1u8; 1024]; // 1KB payload

    c.bench_function("serialized_task_new_1kb", |b| {
        b.iter(|| {
            black_box(SerializedTask::new(
                "test_task".to_string(),
                payload.clone(),
            ));
        });
    });

    let large_payload = vec![1u8; 1024 * 100]; // 100KB payload

    c.bench_function("serialized_task_new_100kb", |b| {
        b.iter(|| {
            black_box(SerializedTask::new(
                "test_task".to_string(),
                large_payload.clone(),
            ));
        });
    });
}

fn bench_task_validation(c: &mut Criterion) {
    let task = SerializedTask::new("test_task".to_string(), vec![1, 2, 3, 4, 5]);

    c.bench_function("task_validate", |b| {
        b.iter(|| {
            black_box(task.validate()).ok();
        });
    });

    let metadata = TaskMetadata::new("test_task".to_string());

    c.bench_function("metadata_validate", |b| {
        b.iter(|| {
            black_box(metadata.validate()).ok();
        });
    });
}

criterion_group!(
    benches,
    bench_task_metadata_creation,
    bench_task_metadata_clone,
    bench_serialized_task_creation,
    bench_task_validation
);
criterion_main!(benches);
