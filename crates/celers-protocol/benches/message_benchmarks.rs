use celers_protocol::lazy::LazyMessage;
use celers_protocol::pool::{MessagePool, TaskArgsPool};
use celers_protocol::zerocopy::MessageRef;
use celers_protocol::{Message, TaskArgs};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use uuid::Uuid;

fn create_test_message() -> Vec<u8> {
    let task_id = Uuid::new_v4();
    let args = TaskArgs::new().with_args(vec![
        serde_json::json!(1),
        serde_json::json!(2),
        serde_json::json!(3),
    ]);
    let body = serde_json::to_vec(&args).unwrap();
    let msg = Message::new("tasks.benchmark".to_string(), task_id, body);
    serde_json::to_vec(&msg).unwrap()
}

fn benchmark_standard_deserialization(c: &mut Criterion) {
    let data = create_test_message();

    c.bench_function("standard_deserialize", |b| {
        b.iter(|| {
            let msg: Message = serde_json::from_slice(black_box(&data)).unwrap();
            black_box(msg);
        })
    });
}

fn benchmark_zerocopy_deserialization(c: &mut Criterion) {
    let data = create_test_message();

    c.bench_function("zerocopy_deserialize", |b| {
        b.iter(|| {
            let msg: MessageRef = serde_json::from_slice(black_box(&data)).unwrap();
            black_box(msg);
        })
    });
}

fn benchmark_lazy_deserialization(c: &mut Criterion) {
    let data = create_test_message();

    c.bench_function("lazy_deserialize", |b| {
        b.iter(|| {
            let msg = LazyMessage::from_json(black_box(&data)).unwrap();
            black_box(msg.task_name());
        })
    });
}

fn benchmark_lazy_with_body(c: &mut Criterion) {
    let data = create_test_message();

    c.bench_function("lazy_deserialize_with_body", |b| {
        b.iter(|| {
            let msg = LazyMessage::from_json(black_box(&data)).unwrap();
            let _body = msg.body().unwrap();
            black_box(msg.task_name());
        })
    });
}

fn benchmark_message_pool(c: &mut Criterion) {
    let pool = MessagePool::new();

    c.bench_function("message_pool_acquire", |b| {
        b.iter(|| {
            let mut msg = pool.acquire();
            msg.headers.task = "tasks.test".to_string();
            msg.headers.id = Uuid::new_v4();
            black_box(msg);
        })
    });
}

fn benchmark_message_pool_vs_new(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_creation");

    group.bench_function("new_message", |b| {
        b.iter(|| {
            let msg = Message::new("tasks.test".to_string(), Uuid::new_v4(), vec![1, 2, 3]);
            black_box(msg);
        })
    });

    let pool = MessagePool::new();
    group.bench_function("pooled_message", |b| {
        b.iter(|| {
            let mut msg = pool.acquire();
            msg.headers.task = "tasks.test".to_string();
            msg.headers.id = Uuid::new_v4();
            msg.body = vec![1, 2, 3];
            black_box(msg);
        })
    });

    group.finish();
}

fn benchmark_task_args_pool(c: &mut Criterion) {
    let pool = TaskArgsPool::new();

    c.bench_function("task_args_pool_acquire", |b| {
        b.iter(|| {
            let mut args = pool.acquire();
            args.get_mut().args.push(serde_json::json!(1));
            black_box(args);
        })
    });
}

fn benchmark_message_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_sizes");

    for size in [100, 1000, 10000].iter() {
        let task_id = Uuid::new_v4();
        let args = TaskArgs::new().with_args(vec![serde_json::json!("x".repeat(*size))]);
        let body = serde_json::to_vec(&args).unwrap();
        let msg = Message::new("tasks.benchmark".to_string(), task_id, body);
        let data = serde_json::to_vec(&msg).unwrap();

        group.bench_with_input(BenchmarkId::new("standard", size), &data, |b, data| {
            b.iter(|| {
                let msg: Message = serde_json::from_slice(black_box(data)).unwrap();
                black_box(msg);
            })
        });

        group.bench_with_input(BenchmarkId::new("lazy", size), &data, |b, data| {
            b.iter(|| {
                let msg = LazyMessage::from_json(black_box(data)).unwrap();
                black_box(msg.task_name());
            })
        });
    }

    group.finish();
}

fn benchmark_serialization(c: &mut Criterion) {
    let task_id = Uuid::new_v4();
    let args = TaskArgs::new().with_args(vec![serde_json::json!(1), serde_json::json!(2)]);
    let body = serde_json::to_vec(&args).unwrap();
    let msg = Message::new("tasks.benchmark".to_string(), task_id, body);

    c.bench_function("serialize", |b| {
        b.iter(|| {
            let data = serde_json::to_vec(black_box(&msg)).unwrap();
            black_box(data);
        })
    });
}

criterion_group!(
    benches,
    benchmark_standard_deserialization,
    benchmark_zerocopy_deserialization,
    benchmark_lazy_deserialization,
    benchmark_lazy_with_body,
    benchmark_message_pool,
    benchmark_message_pool_vs_new,
    benchmark_task_args_pool,
    benchmark_message_sizes,
    benchmark_serialization,
);
criterion_main!(benches);
