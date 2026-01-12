/// Advanced Features Example
///
/// Demonstrates:
/// - Result compression for large payloads
/// - Result encryption for sensitive data
/// - In-memory caching
/// - Metrics collection
/// - Result versioning
/// - Result streaming
use celers_backend_redis::{
    cache::{CacheConfig, ResultCache},
    compression::CompressionConfig,
    encryption::{EncryptionConfig, EncryptionKey},
    metrics::BackendMetrics,
    RedisResultBackend, ResultBackend, TaskMeta, TaskResult,
};
use futures_util::StreamExt;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Redis Result Backend: Advanced Features ===\n");

    // Example 1: Compression
    println!("=== Example 1: Result Compression ===");
    let mut backend =
        RedisResultBackend::new("redis://127.0.0.1:6379")?.with_compression(CompressionConfig {
            enabled: true,
            threshold: 100, // Compress results larger than 100 bytes
            level: 6,       // Compression level 1-9
        });

    let task_id = Uuid::new_v4();
    let mut meta = TaskMeta::new(task_id, "compression.test".to_string());

    // Create a large result
    let large_data = "x".repeat(10000);
    meta.result = TaskResult::Success(serde_json::json!({
        "data": large_data,
        "compressed": true
    }));

    backend.store_result(task_id, &meta).await?;
    println!("✓ Stored large result with compression");

    // Retrieve (automatically decompressed)
    if let Some(retrieved) = backend.get_result(task_id).await? {
        println!("✓ Retrieved and decompressed result");
        if let TaskResult::Success(value) = &retrieved.result {
            if let Some(data) = value.get("data") {
                println!("  Data size: {} bytes", data.as_str().unwrap_or("").len());
            }
        }
    }

    // Example 2: Encryption
    println!("\n=== Example 2: Result Encryption ===");
    let encryption_key = EncryptionKey::generate();
    println!("✓ Generated encryption key");

    let mut backend2 = RedisResultBackend::new("redis://127.0.0.1:6379")?
        .with_encryption(EncryptionConfig::new(encryption_key));

    let sensitive_task_id = Uuid::new_v4();
    let mut sensitive_meta = TaskMeta::new(sensitive_task_id, "sensitive.data".to_string());
    sensitive_meta.result = TaskResult::Success(serde_json::json!({
        "user_id": "user_12345",
        "email": "user@example.com",
        "api_key": "secret_key_abc123"
    }));

    backend2
        .store_result(sensitive_task_id, &sensitive_meta)
        .await?;
    println!("✓ Stored encrypted sensitive data");

    // Retrieve (automatically decrypted)
    if let Some(retrieved) = backend2.get_result(sensitive_task_id).await? {
        println!("✓ Retrieved and decrypted data");
        if let TaskResult::Success(value) = &retrieved.result {
            println!("  User: {:?}", value.get("user_id"));
        }
    }

    // Example 3: Caching
    println!("\n=== Example 3: In-Memory Caching ===");
    let mut backend3 = RedisResultBackend::new("redis://127.0.0.1:6379")?.with_cache(
        ResultCache::new(CacheConfig {
            enabled: true,
            capacity: 1000,
            ttl: std::time::Duration::from_secs(300),
        }),
    );

    let cached_task_id = Uuid::new_v4();
    let mut cached_meta = TaskMeta::new(cached_task_id, "cached.task".to_string());
    cached_meta.result = TaskResult::Success(serde_json::json!({"cached": true}));

    backend3.store_result(cached_task_id, &cached_meta).await?;
    println!("✓ Stored result (cached in memory)");

    // First retrieval
    let start = std::time::Instant::now();
    let _ = backend3.get_result(cached_task_id).await?;
    let first_duration = start.elapsed();
    println!("✓ First retrieval: {:?}", first_duration);

    // Second retrieval (from cache - much faster)
    let start = std::time::Instant::now();
    let _ = backend3.get_result(cached_task_id).await?;
    let cached_duration = start.elapsed();
    println!("✓ Cached retrieval: {:?}", cached_duration);

    if cached_duration < first_duration {
        println!(
            "  Cache speedup: {:.1}x",
            first_duration.as_micros() as f64 / cached_duration.as_micros() as f64
        );
    }

    // Example 4: Metrics
    println!("\n=== Example 4: Metrics Collection ===");
    let mut backend4 =
        RedisResultBackend::new("redis://127.0.0.1:6379")?.with_metrics(BackendMetrics::new());

    // Perform operations to generate metrics
    for i in 0..10 {
        let task_id = Uuid::new_v4();
        let mut meta = TaskMeta::new(task_id, format!("metrics.task_{}", i));
        meta.result = TaskResult::Success(serde_json::json!({"index": i}));

        backend4.store_result(task_id, &meta).await?;
        let _ = backend4.get_result(task_id).await?;
    }

    let metrics = backend4.metrics().snapshot();
    println!("✓ Collected metrics:");
    println!("{}", metrics);

    // Example 5: Result Versioning
    println!("\n=== Example 5: Result Versioning ===");
    let mut backend5 = RedisResultBackend::new("redis://127.0.0.1:6379")?;

    let versioned_task_id = Uuid::new_v4();
    let mut meta_v1 = TaskMeta::new(versioned_task_id, "versioned.task".to_string());
    meta_v1.result = TaskResult::Started;
    meta_v1.version = 1;

    let version = backend5
        .store_versioned_result(versioned_task_id, &meta_v1)
        .await?;
    println!("✓ Stored version {}: STARTED", version);

    // Update to version 2
    meta_v1.result = TaskResult::Success(serde_json::json!({"progress": 50}));
    meta_v1.version = 2;
    let version = backend5
        .store_versioned_result(versioned_task_id, &meta_v1)
        .await?;
    println!("✓ Stored version {}: Partial result", version);

    // Update to version 3
    meta_v1.result = TaskResult::Success(serde_json::json!({"final": "result"}));
    meta_v1.version = 3;
    let version = backend5
        .store_versioned_result(versioned_task_id, &meta_v1)
        .await?;
    println!("✓ Stored version {}: Final result", version);

    // Retrieve specific version
    if let Some(v1) = backend5.get_result_version(versioned_task_id, 1).await? {
        println!("  Version 1: {:?}", v1.result);
    }
    if let Some(v3) = backend5.get_result_version(versioned_task_id, 3).await? {
        println!("  Version 3: {:?}", v3.result);
    }

    // Example 6: Result Streaming
    println!("\n=== Example 6: Result Streaming ===");
    let mut backend6 = RedisResultBackend::new("redis://127.0.0.1:6379")?;

    // Create multiple tasks
    let task_ids: Vec<Uuid> = (0..20).map(|_| Uuid::new_v4()).collect();
    for (i, &task_id) in task_ids.iter().enumerate() {
        let mut meta = TaskMeta::new(task_id, format!("stream.task_{}", i));
        meta.result = if i % 5 == 0 {
            TaskResult::Failure("Error".to_string())
        } else {
            TaskResult::Success(serde_json::json!({"index": i}))
        };
        backend6.store_result(task_id, &meta).await?;
    }

    // Stream results
    let mut stream = backend6.stream_results(task_ids.clone(), 5);
    let mut count = 0;
    let mut success_count = 0;
    let mut failure_count = 0;

    while let Some(result) = stream.next().await {
        if let Ok((_task_id, Some(meta))) = result {
            count += 1;
            if meta.result.is_success() {
                success_count += 1;
            } else if meta.result.is_failure() {
                failure_count += 1;
            }
        }
    }

    println!("✓ Streamed {} results", count);
    println!("  Success: {}, Failure: {}", success_count, failure_count);

    // Example 7: Pagination
    println!("\n=== Example 7: Result Pagination ===");
    let (results, total, has_more) = backend6.get_results_paginated(&task_ids, 0, 10).await?;
    println!("✓ Page 1: {} results", results.len());
    println!("  Total: {}, Has more: {}", total, has_more);

    if has_more {
        let (results2, _, _) = backend6.get_results_paginated(&task_ids, 1, 10).await?;
        println!("✓ Page 2: {} results", results2.len());
    }

    // Example 8: Combined Features
    println!("\n=== Example 8: All Features Combined ===");
    let mut full_backend = RedisResultBackend::new("redis://127.0.0.1:6379")?
        .with_compression(CompressionConfig {
            enabled: true,
            threshold: 100,
            level: 6,
        })
        .with_encryption(EncryptionConfig::new(EncryptionKey::generate()))
        .with_cache(ResultCache::new(CacheConfig {
            enabled: true,
            capacity: 1000,
            ttl: std::time::Duration::from_secs(300),
        }))
        .with_metrics(BackendMetrics::new());

    let full_task_id = Uuid::new_v4();
    let mut full_meta = TaskMeta::new(full_task_id, "full.featured".to_string());
    full_meta.result = TaskResult::Success(serde_json::json!({
        "message": "Compressed, encrypted, cached, and metered!",
        "data": "x".repeat(1000)
    }));

    full_backend.store_result(full_task_id, &full_meta).await?;
    println!("✓ Stored with all features enabled");

    let _ = full_backend.get_result(full_task_id).await?;
    println!("✓ Retrieved (with all optimizations)");

    let final_metrics = full_backend.metrics().snapshot();
    println!("\n📊 Final Metrics:\n{}", final_metrics);

    println!("\n✅ All advanced features demonstrated successfully!");
    Ok(())
}
