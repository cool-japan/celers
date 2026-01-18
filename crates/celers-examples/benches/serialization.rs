//! Serialization performance benchmarks

use celers_core::SerializedTask;
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::hint::black_box;use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SmallPayload {
    id: u64,
    name: String,
    value: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MediumPayload {
    id: u64,
    name: String,
    values: Vec<f64>,
    metadata: std::collections::HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LargePayload {
    id: u64,
    name: String,
    data: Vec<u8>,
    nested: Vec<MediumPayload>,
}

fn create_small_payload() -> SmallPayload {
    SmallPayload {
        id: 12345,
        name: "test_task".to_string(),
        value: 42.5,
    }
}

fn create_medium_payload() -> MediumPayload {
    let mut metadata = std::collections::HashMap::new();
    metadata.insert("key1".to_string(), "value1".to_string());
    metadata.insert("key2".to_string(), "value2".to_string());
    metadata.insert("key3".to_string(), "value3".to_string());

    MediumPayload {
        id: 67890,
        name: "medium_task".to_string(),
        values: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0],
        metadata,
    }
}

fn create_large_payload() -> LargePayload {
    LargePayload {
        id: 99999,
        name: "large_task".to_string(),
        data: vec![0u8; 1024], // 1KB of data
        nested: vec![create_medium_payload(); 5],
    }
}

fn bench_json_serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_serialize");

    let small = create_small_payload();
    group.throughput(Throughput::Elements(1));
    group.bench_function("small", |b| {
        b.iter(|| {
            let serialized = serde_json::to_vec(black_box(&small)).unwrap();
            black_box(serialized);
        });
    });

    let medium = create_medium_payload();
    group.bench_function("medium", |b| {
        b.iter(|| {
            let serialized = serde_json::to_vec(black_box(&medium)).unwrap();
            black_box(serialized);
        });
    });

    let large = create_large_payload();
    group.bench_function("large", |b| {
        b.iter(|| {
            let serialized = serde_json::to_vec(black_box(&large)).unwrap();
            black_box(serialized);
        });
    });

    group.finish();
}

fn bench_json_deserialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("json_deserialize");

    let small = create_small_payload();
    let small_bytes = serde_json::to_vec(&small).unwrap();
    group.throughput(Throughput::Elements(1));
    group.bench_function("small", |b| {
        b.iter(|| {
            let deserialized: SmallPayload =
                serde_json::from_slice(black_box(&small_bytes)).unwrap();
            black_box(deserialized);
        });
    });

    let medium = create_medium_payload();
    let medium_bytes = serde_json::to_vec(&medium).unwrap();
    group.bench_function("medium", |b| {
        b.iter(|| {
            let deserialized: MediumPayload =
                serde_json::from_slice(black_box(&medium_bytes)).unwrap();
            black_box(deserialized);
        });
    });

    let large = create_large_payload();
    let large_bytes = serde_json::to_vec(&large).unwrap();
    group.bench_function("large", |b| {
        b.iter(|| {
            let deserialized: LargePayload =
                serde_json::from_slice(black_box(&large_bytes)).unwrap();
            black_box(deserialized);
        });
    });

    group.finish();
}

fn bench_serialized_task_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("serialized_task_roundtrip");

    group.bench_function("json", |b| {
        b.iter(|| {
            let payload = serde_json::to_vec(&create_medium_payload()).unwrap();
            let task = SerializedTask::new("test_task".to_string(), payload);
            let serialized = serde_json::to_string(&task).unwrap();
            let deserialized: SerializedTask = serde_json::from_str(&serialized).unwrap();
            black_box(deserialized);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_json_serialize,
    bench_json_deserialize,
    bench_serialized_task_roundtrip
);
criterion_main!(benches);
