/// Performance benchmarks for Redis result backend
///
/// These benchmarks require a running Redis instance at localhost:6379
/// Run with: cargo bench --bench backend_bench
use celers_backend_redis::{
    cache::{CacheConfig, ResultCache},
    compression::CompressionConfig,
    encryption::{EncryptionConfig, EncryptionKey},
    metrics::BackendMetrics,
    ProgressInfo, RedisResultBackend, ResultBackend, TaskMeta, TaskResult,
};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use std::hint::black_box;
use uuid::Uuid;

// Helper to create a backend with default settings
fn create_backend() -> RedisResultBackend {
    RedisResultBackend::new("redis://localhost:6379")
        .expect("Failed to connect to Redis - ensure Redis is running at localhost:6379")
}

// Helper to create a simple task metadata
fn create_task_meta(task_id: Uuid) -> TaskMeta {
    let mut meta = TaskMeta::new(task_id, "bench_task".to_string());
    meta.result = TaskResult::Success(serde_json::json!({"benchmark": true}));
    meta
}

// Benchmark: Store result
fn bench_store_result(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut backend = create_backend();

    c.bench_function("store_result", |b| {
        b.iter(|| {
            let task_id = Uuid::new_v4();
            let meta = create_task_meta(task_id);
            rt.block_on(async {
                backend
                    .store_result(black_box(task_id), black_box(&meta))
                    .await
                    .unwrap();
                // Cleanup
                backend.delete_result(task_id).await.unwrap();
            });
        });
    });
}

// Benchmark: Get result
fn bench_get_result(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut backend = create_backend();

    // Pre-populate some tasks
    let task_id = Uuid::new_v4();
    let meta = create_task_meta(task_id);
    rt.block_on(async {
        backend.store_result(task_id, &meta).await.unwrap();
    });

    c.bench_function("get_result", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = backend.get_result(black_box(task_id)).await.unwrap();
            });
        });
    });

    // Cleanup
    rt.block_on(async {
        backend.delete_result(task_id).await.unwrap();
    });
}

// Benchmark: Batch store operations
fn bench_batch_store(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut backend = create_backend();

    let mut group = c.benchmark_group("batch_store");
    for batch_size in [10, 50, 100].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            batch_size,
            |b, &size| {
                b.iter(|| {
                    let results: Vec<(Uuid, TaskMeta)> = (0..size)
                        .map(|_| {
                            let id = Uuid::new_v4();
                            (id, create_task_meta(id))
                        })
                        .collect();
                    let task_ids: Vec<Uuid> = results.iter().map(|(id, _)| *id).collect();

                    rt.block_on(async {
                        backend
                            .store_results_batch(black_box(&results))
                            .await
                            .unwrap();
                        // Cleanup
                        backend.delete_results_batch(&task_ids).await.unwrap();
                    });
                });
            },
        );
    }
    group.finish();
}

// Benchmark: Compression impact
fn bench_compression(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("compression");

    // Without compression
    let mut backend_no_compress = create_backend();
    let task_id_no_compress = Uuid::new_v4();
    let mut meta_large = create_task_meta(task_id_no_compress);
    meta_large.result = TaskResult::Success(serde_json::json!({
        "data": "x".repeat(10000)
    }));

    group.bench_function("without_compression", |b| {
        b.iter(|| {
            let task_id = Uuid::new_v4();
            let meta = meta_large.clone();
            rt.block_on(async {
                backend_no_compress
                    .store_result(black_box(task_id), black_box(&meta))
                    .await
                    .unwrap();
                backend_no_compress.delete_result(task_id).await.unwrap();
            });
        });
    });

    // With compression
    let mut backend_compress = create_backend().with_compression(CompressionConfig {
        enabled: true,
        threshold: 100,
        level: 6,
    });

    group.bench_function("with_compression", |b| {
        b.iter(|| {
            let task_id = Uuid::new_v4();
            let meta = meta_large.clone();
            rt.block_on(async {
                backend_compress
                    .store_result(black_box(task_id), black_box(&meta))
                    .await
                    .unwrap();
                backend_compress.delete_result(task_id).await.unwrap();
            });
        });
    });

    group.finish();
}

// Benchmark: Cache impact
fn bench_cache(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("cache");

    // Without cache
    let mut backend_no_cache = create_backend().without_cache();
    let task_id = Uuid::new_v4();
    let meta = create_task_meta(task_id);
    rt.block_on(async {
        backend_no_cache.store_result(task_id, &meta).await.unwrap();
    });

    group.bench_function("without_cache", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = backend_no_cache
                    .get_result(black_box(task_id))
                    .await
                    .unwrap();
            });
        });
    });

    // With cache (first access populates cache)
    let mut backend_cache = create_backend().with_cache(ResultCache::new(CacheConfig {
        enabled: true,
        capacity: 1000,
        ttl: std::time::Duration::from_secs(300),
    }));

    let task_id_cached = Uuid::new_v4();
    let meta_cached = create_task_meta(task_id_cached);
    rt.block_on(async {
        backend_cache
            .store_result(task_id_cached, &meta_cached)
            .await
            .unwrap();
        // Warm up cache
        let _ = backend_cache.get_result(task_id_cached).await.unwrap();
    });

    group.bench_function("with_cache_hit", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = backend_cache
                    .get_result(black_box(task_id_cached))
                    .await
                    .unwrap();
            });
        });
    });

    group.finish();

    // Cleanup
    rt.block_on(async {
        backend_no_cache.delete_result(task_id).await.unwrap();
        backend_cache.delete_result(task_id_cached).await.unwrap();
    });
}

// Benchmark: Encryption overhead
fn bench_encryption(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("encryption");

    // Without encryption
    let mut backend_no_encrypt = create_backend();
    group.bench_function("without_encryption", |b| {
        b.iter(|| {
            let task_id = Uuid::new_v4();
            let meta = create_task_meta(task_id);
            rt.block_on(async {
                backend_no_encrypt
                    .store_result(black_box(task_id), black_box(&meta))
                    .await
                    .unwrap();
                backend_no_encrypt.delete_result(task_id).await.unwrap();
            });
        });
    });

    // With encryption
    let key = EncryptionKey::generate();
    let mut backend_encrypt = create_backend().with_encryption(EncryptionConfig::new(key));

    group.bench_function("with_encryption", |b| {
        b.iter(|| {
            let task_id = Uuid::new_v4();
            let meta = create_task_meta(task_id);
            rt.block_on(async {
                backend_encrypt
                    .store_result(black_box(task_id), black_box(&meta))
                    .await
                    .unwrap();
                backend_encrypt.delete_result(task_id).await.unwrap();
            });
        });
    });

    group.finish();
}

// Benchmark: Progress tracking
fn bench_progress_tracking(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut backend = create_backend();

    c.bench_function("progress_tracking", |b| {
        b.iter(|| {
            let task_id = Uuid::new_v4();
            let mut meta = create_task_meta(task_id);

            rt.block_on(async {
                // Update progress 5 times
                for i in (0..=100).step_by(25) {
                    let progress = ProgressInfo::new(i, 100).with_message(format!("Step {}", i));
                    meta.progress = Some(progress);
                    backend
                        .store_result(black_box(task_id), black_box(&meta))
                        .await
                        .unwrap();
                }
                backend.delete_result(task_id).await.unwrap();
            });
        });
    });
}

// Benchmark: Metrics overhead
fn bench_metrics_overhead(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("metrics");

    // Without metrics
    let mut backend_no_metrics = create_backend().without_metrics();
    group.bench_function("without_metrics", |b| {
        b.iter(|| {
            let task_id = Uuid::new_v4();
            let meta = create_task_meta(task_id);
            rt.block_on(async {
                backend_no_metrics
                    .store_result(black_box(task_id), black_box(&meta))
                    .await
                    .unwrap();
                backend_no_metrics.delete_result(task_id).await.unwrap();
            });
        });
    });

    // With metrics
    let mut backend_metrics = create_backend().with_metrics(BackendMetrics::new());

    group.bench_function("with_metrics", |b| {
        b.iter(|| {
            let task_id = Uuid::new_v4();
            let meta = create_task_meta(task_id);
            rt.block_on(async {
                backend_metrics
                    .store_result(black_box(task_id), black_box(&meta))
                    .await
                    .unwrap();
                backend_metrics.delete_result(task_id).await.unwrap();
            });
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_store_result,
    bench_get_result,
    bench_batch_store,
    bench_compression,
    bench_cache,
    bench_encryption,
    bench_progress_tracking,
    bench_metrics_overhead
);
criterion_main!(benches);
