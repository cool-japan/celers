//! Regression tests for the storage pipeline (codec, cache, chords, archival,
//! versioning, locks and telemetry).
//!
//! Tests are split in two layers:
//!
//! * plain `#[test]`s exercising everything that is decidable without a server;
//! * `#[tokio::test] #[ignore]` integration tests requiring a live Redis, run
//!   with `cargo nextest run -p celers-backend-redis --all-features
//!   --run-ignored all` (override the server with `CELERS_TEST_REDIS_URL`).
//!
//! Every integration test uses a unique key prefix so runs never collide.

use std::time::Duration;

use uuid::Uuid;

use crate::backend::VersioningConfig;
use crate::stats::ttl;
use crate::types::{TaskMeta, TaskResult, TaskTtlConfig};
use crate::{compression, encryption, RedisResultBackend, ResultBackend};

// =============================================================================
// Helpers
// =============================================================================

fn redis_url() -> String {
    std::env::var("CELERS_TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string())
}

fn unique_prefix() -> String {
    format!("celers-test-{}-", Uuid::new_v4())
}

fn backend_with_prefix(prefix: &str) -> RedisResultBackend {
    RedisResultBackend::new(&redis_url())
        .expect("redis client")
        .with_prefix(prefix.to_string())
}

/// A backend on a private key prefix, plus the prefix itself.
fn test_backend() -> (RedisResultBackend, String) {
    let prefix = unique_prefix();
    (backend_with_prefix(&prefix), prefix)
}

/// Raw key listing, used to assert on keys the typed API hides.
async fn raw_keys(pattern: &str) -> Vec<String> {
    let client = redis::Client::open(redis_url()).expect("redis client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("redis connection");
    redis::cmd("KEYS")
        .arg(pattern)
        .query_async(&mut conn)
        .await
        .expect("KEYS")
}

async fn raw_get(key: &str) -> Option<Vec<u8>> {
    let client = redis::Client::open(redis_url()).expect("redis client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("redis connection");
    redis::cmd("GET")
        .arg(key)
        .query_async(&mut conn)
        .await
        .expect("GET")
}

async fn delete_prefix(prefix: &str) {
    let keys = raw_keys(&format!("{}*", prefix)).await;
    if keys.is_empty() {
        return;
    }
    let client = redis::Client::open(redis_url()).expect("redis client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("redis connection");
    let mut pipe = redis::pipe();
    for key in keys {
        pipe.del(key);
    }
    let _: Vec<i64> = pipe.query_async(&mut conn).await.expect("cleanup");
}

/// A result whose serialized JSON is comfortably larger than `bytes`.
fn big_meta(task_id: Uuid, name: &str, bytes: usize) -> TaskMeta {
    let mut meta = TaskMeta::new(task_id, name.to_string());
    meta.result = TaskResult::Success(serde_json::json!({ "blob": "q".repeat(bytes) }));
    meta
}

// =============================================================================
// Unit tests (no server required)
// =============================================================================

/// `find_tasks_by_pattern` prepends the key prefix, so callers must pass a bare
/// suffix. Re-adding the prefix produced `celery-task-meta-celery-task-meta-…`,
/// which matched nothing and made every `query_tasks_*` silently return empty.
#[test]
fn test_scan_pattern_is_not_double_prefixed() {
    let backend = backend_with_prefix("celery-task-meta-");

    assert_eq!(
        backend.scan_pattern(RedisResultBackend::ALL_TASKS_PATTERN),
        "celery-task-meta-*"
    );
    assert!(
        !backend
            .scan_pattern(RedisResultBackend::ALL_TASKS_PATTERN)
            .contains("celery-task-meta-celery-task-meta-"),
        "the key prefix must appear exactly once in a scan pattern"
    );

    // The composed pattern must actually match a real task key.
    let task_id = Uuid::new_v4();
    let key = backend.task_key(task_id);
    assert!(key.starts_with("celery-task-meta-"));
    assert_eq!(key, format!("celery-task-meta-{}", task_id));
}

/// Auxiliary keys must live under the task's own namespace so they can be
/// found, expired and deleted together with it.
#[test]
fn test_auxiliary_key_layout() {
    let backend = backend_with_prefix("p-");
    let task_id = Uuid::new_v4();

    assert_eq!(
        backend.archive_key(task_id),
        format!("p-archive:{}", task_id)
    );
    assert_eq!(backend.version_key(task_id, 3), format!("p-{}:v3", task_id));
    assert_eq!(
        backend.version_counter_key(task_id),
        format!("p-{}:version", task_id)
    );
    assert_eq!(
        backend.notify_channel(task_id),
        format!("p-{}:notify", task_id)
    );

    // The archive key must not collide with the live key.
    assert_ne!(backend.archive_key(task_id), backend.task_key(task_id));
}

/// Out of the box, results expire — otherwise a deployment grows one permanent
/// Redis key per task it ever runs.
#[test]
fn test_default_ttl_is_configured() {
    let backend = backend_with_prefix("p-");
    assert_eq!(
        backend.ttl_config().get_ttl("any.task"),
        Some(ttl::SUCCESS),
        "results must expire by default (Celery's result_expires is 24h)"
    );

    // ...but permanent results remain available on request.
    let permanent = backend_with_prefix("p-").without_ttl();
    assert_eq!(permanent.ttl_config().get_ttl("any.task"), None);
}

/// Chord keys inherit the result TTL unless overridden.
#[test]
fn test_chord_ttl_falls_back_to_default() {
    let empty = TaskTtlConfig::new();
    assert_eq!(empty.chord_ttl(), None);

    let defaulted = TaskTtlConfig::with_default(Duration::from_secs(600));
    assert_eq!(defaulted.chord_ttl(), Some(Duration::from_secs(600)));

    let overridden = TaskTtlConfig::with_default(Duration::from_secs(600))
        .with_chord_ttl(Duration::from_secs(60));
    assert_eq!(overridden.chord_ttl(), Some(Duration::from_secs(60)));
    // The result TTL is untouched by the chord override.
    assert_eq!(overridden.get_ttl("t"), Some(Duration::from_secs(600)));
}

/// Cancelling a chord must not forget the tasks that already completed.
#[test]
fn test_chord_cancel_preserves_progress_locally() {
    let chord_id = Uuid::new_v4();
    let task_ids: Vec<Uuid> = (0..4).map(|_| Uuid::new_v4()).collect();
    let mut state = crate::ChordState::new(chord_id, task_ids.len(), task_ids);
    state.completed = 3;

    state.cancel(Some("operator request".to_string()));

    assert!(state.is_cancelled());
    assert_eq!(
        state.completed, 3,
        "cancellation must not reset completion progress"
    );

    // A retry, by contrast, is *supposed* to reset it.
    let mut retryable = state.clone();
    retryable.max_retries = Some(2);
    assert!(retryable.retry());
    assert_eq!(retryable.completed, 0);
    assert!(!retryable.cancelled);
}

/// Failed operations must be counted, otherwise the health check's error-rate
/// branch can never fire.
#[tokio::test]
async fn test_errors_are_recorded_on_metrics() {
    use crate::pipeline::PipelineConfig;

    // Port 1 is reserved and never listening: the connection is refused.
    // A short command timeout keeps the test fast (the connection manager
    // would otherwise spend seconds on its own reconnect ladder).
    let mut backend = RedisResultBackend::new("redis://127.0.0.1:1")
        .expect("client construction does not connect")
        .without_retries()
        .with_pipeline_config(PipelineConfig::new().with_timeout(Duration::from_millis(300)));

    let task_id = Uuid::new_v4();
    let meta = TaskMeta::new(task_id, "unreachable".to_string());

    assert_eq!(backend.metrics().error_count(), 0);
    let outcome = backend.store_result(task_id, &meta).await;
    assert!(outcome.is_err(), "unreachable Redis must surface an error");
    assert!(
        backend.metrics().error_count() >= 1,
        "backend errors must increment the metrics error counter"
    );
}

/// A registered telemetry hook must actually be invoked by backend operations.
#[tokio::test]
async fn test_telemetry_hook_observes_failures() {
    use crate::pipeline::PipelineConfig;
    use crate::telemetry::MetricsHook;
    use std::sync::Arc;

    let hook = Arc::new(MetricsHook::new());
    let mut backend = RedisResultBackend::new("redis://127.0.0.1:1")
        .expect("client construction does not connect")
        .without_retries()
        .with_pipeline_config(PipelineConfig::new().with_timeout(Duration::from_millis(300)))
        .with_telemetry_hook(hook.clone());

    let task_id = Uuid::new_v4();
    let meta = TaskMeta::new(task_id, "unreachable".to_string());
    let _ = backend.store_result(task_id, &meta).await;

    assert_eq!(
        hook.operation_count(crate::telemetry::OperationType::Store),
        1,
        "telemetry hooks must be fired from the real operation paths"
    );
    assert_eq!(
        hook.error_count(crate::telemetry::OperationType::Store),
        1,
        "failed operations must reach on_error/after_operation"
    );
}

/// `MetricsHook` must count a failure exactly once, whichever callbacks the
/// caller fires — the backend fires `on_error` then `after_operation`, but
/// `after_operation` alone (the shape external callers use) must still count.
#[test]
fn test_metrics_hook_counts_each_failure_once() {
    use crate::telemetry::{
        MetricsHook, OperationContext, OperationResult, OperationType as TelemetryOp, TelemetryHook,
    };

    let hook = MetricsHook::new();
    let error = crate::BackendError::Connection("down".to_string());

    // Full backend sequence.
    let context = OperationContext::new(TelemetryOp::Store);
    hook.before_operation(&context);
    hook.on_error(TelemetryOp::Store, &error);
    hook.after_operation(&OperationResult::failure(context, &error));
    assert_eq!(hook.error_count(TelemetryOp::Store), 1);
    assert_eq!(hook.operation_count(TelemetryOp::Store), 1);

    // A caller that only reports the finished operation.
    let context = OperationContext::new(TelemetryOp::Get);
    hook.after_operation(&OperationResult::failure(context, &error));
    assert_eq!(hook.error_count(TelemetryOp::Get), 1);

    // Successes never touch the error counter.
    let context = OperationContext::new(TelemetryOp::Delete);
    hook.after_operation(&OperationResult::success(context));
    assert_eq!(hook.error_count(TelemetryOp::Delete), 0);
    assert_eq!(hook.operation_count(TelemetryOp::Delete), 1);
}

/// The command timeout configured on `PipelineConfig` must be honoured.
#[tokio::test]
async fn test_pipeline_timeout_is_enforced() {
    use crate::pipeline::PipelineConfig;

    // 203.0.113.0/24 is TEST-NET-3: routable nowhere, so the connect hangs.
    let mut backend = RedisResultBackend::new("redis://203.0.113.1:6379")
        .expect("client construction does not connect")
        .without_retries()
        .with_pipeline_config(PipelineConfig::new().with_timeout(Duration::from_millis(150)));

    let task_id = Uuid::new_v4();
    let meta = TaskMeta::new(task_id, "unreachable".to_string());

    let started = std::time::Instant::now();
    let outcome = backend.store_result(task_id, &meta).await;
    assert!(outcome.is_err());
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the configured command timeout must bound the operation"
    );
}

// =============================================================================
// Integration tests (require a live Redis)
// =============================================================================

/// A non-terminal state must never be cached: doing so pinned the observed
/// state for the whole cache TTL and made `wait_for_result` time out even
/// though the worker had already written SUCCESS.
#[tokio::test]
#[ignore]
async fn test_integration_non_terminal_results_are_not_cached() {
    let (mut reader, prefix) = test_backend();
    let task_id = Uuid::new_v4();

    let mut meta = TaskMeta::new(task_id, "cache.regression".to_string());
    meta.result = TaskResult::Pending;
    reader.store_result(task_id, &meta).await.expect("store");

    let first = reader
        .get_result(task_id)
        .await
        .expect("get")
        .expect("some");
    assert!(first.result.is_pending());
    assert!(
        reader.cache().get(task_id).is_none(),
        "a pending result must not be cached"
    );

    // Another process (its own backend, its own cache) completes the task.
    let mut worker = backend_with_prefix(&prefix);
    let mut done = meta.clone();
    done.result = TaskResult::Success(serde_json::json!({"ok": true}));
    worker.store_result(task_id, &done).await.expect("store");

    // The original reader must observe the new state immediately.
    let second = reader
        .get_result(task_id)
        .await
        .expect("get")
        .expect("some");
    assert!(
        second.result.is_success(),
        "the reader must not be pinned to the stale pending state"
    );
    assert!(
        reader.cache().get(task_id).is_some(),
        "terminal results are cacheable"
    );

    delete_prefix(&prefix).await;
}

/// `get_results_batch` must detect chunk sentinels and reassemble, in a batch
/// that mixes chunked, plain and missing entries so index alignment is checked
/// too. Without this, one oversized chord member aborted the whole chord.
#[tokio::test]
#[ignore]
async fn test_integration_batch_reads_chunked_results() {
    let prefix = unique_prefix();
    // Compression is disabled so the payload really does exceed the chunking
    // threshold (a repeated byte gzips to almost nothing).
    let mut backend = backend_with_prefix(&prefix)
        .without_compression()
        .with_chunking(
            crate::chunking::ChunkingConfig::new()
                .with_threshold(4096)
                .with_chunk_size(1024),
        );

    let plain_id = Uuid::new_v4();
    let chunked_id = Uuid::new_v4();
    let missing_id = Uuid::new_v4();

    let plain = big_meta(plain_id, "batch.plain", 16);
    let chunked = big_meta(chunked_id, "batch.chunked", 200_000);

    backend
        .store_results_batch(&[(plain_id, plain.clone()), (chunked_id, chunked.clone())])
        .await
        .expect("batch store");

    // The chunked entry really is stored as a sentinel plus chunk keys.
    let raw = raw_get(&backend.task_key(chunked_id))
        .await
        .expect("stored");
    assert!(
        crate::chunking::ResultChunker::is_chunked(&raw),
        "the oversized payload should have been chunked"
    );

    // Deliberately interleaved so an off-by-one in the chunk mapping shows up.
    let results = backend
        .get_results_batch(&[plain_id, missing_id, chunked_id])
        .await
        .expect("batch get");

    assert_eq!(results.len(), 3);
    assert_eq!(results[0].as_ref().expect("plain").result, plain.result);
    assert!(results[1].is_none(), "missing key must map to None");
    assert_eq!(
        results[2].as_ref().expect("chunked").result,
        chunked.result,
        "chunked results must be reassembled by the batch path"
    );

    delete_prefix(&prefix).await;
}

/// The batch write path must apply the same TTL policy as the single-key path.
#[tokio::test]
#[ignore]
async fn test_integration_batch_store_applies_ttl_and_cache() {
    let prefix = unique_prefix();
    let mut ttl_config = TaskTtlConfig::with_default(Duration::from_secs(3600));
    ttl_config.set_task_ttl("batch.short", Duration::from_secs(120));
    let mut backend = backend_with_prefix(&prefix).with_ttl_config(ttl_config);

    let default_id = Uuid::new_v4();
    let short_id = Uuid::new_v4();
    let mut default_meta = TaskMeta::new(default_id, "batch.normal".to_string());
    default_meta.result = TaskResult::Success(serde_json::json!(1));
    let mut short_meta = TaskMeta::new(short_id, "batch.short".to_string());
    short_meta.result = TaskResult::Success(serde_json::json!(2));

    backend
        .store_results_batch(&[(default_id, default_meta), (short_id, short_meta)])
        .await
        .expect("batch store");

    let default_ttl = backend.get_ttl(default_id).await.expect("ttl");
    let short_ttl = backend.get_ttl(short_id).await.expect("ttl");

    assert!(
        default_ttl.is_some_and(|t| t.as_secs() > 3000),
        "batched writes must inherit the default TTL, got {:?}",
        default_ttl
    );
    assert!(
        short_ttl.is_some_and(|t| t.as_secs() <= 120),
        "batched writes must honour per-task TTL overrides, got {:?}",
        short_ttl
    );

    // A batch delete must invalidate the cache, not keep serving stale results.
    assert!(backend.cache().get(default_id).is_some());
    backend
        .delete_results_batch(&[default_id, short_id])
        .await
        .expect("batch delete");
    assert!(backend.cache().get(default_id).is_none());
    assert!(backend.get_result(default_id).await.expect("get").is_none());

    delete_prefix(&prefix).await;
}

/// A result stored by the single-key path must carry a TTL out of the box.
#[tokio::test]
#[ignore]
async fn test_integration_store_result_expires_by_default() {
    let (mut backend, prefix) = test_backend();
    let task_id = Uuid::new_v4();
    let mut meta = TaskMeta::new(task_id, "ttl.default".to_string());
    meta.result = TaskResult::Success(serde_json::json!("v"));

    backend.store_result(task_id, &meta).await.expect("store");

    let remaining = backend.get_ttl(task_id).await.expect("ttl");
    assert!(
        remaining.is_some(),
        "results must not be stored permanently by default"
    );

    delete_prefix(&prefix).await;
}

/// Re-storing a task whose previous encoding needed more chunks must not leave
/// the surplus chunk keys behind — they are unreachable and never expire.
#[tokio::test]
#[ignore]
async fn test_integration_restore_cleans_orphaned_chunks() {
    let prefix = unique_prefix();
    let mut backend = backend_with_prefix(&prefix)
        .without_compression()
        .with_chunking(
            crate::chunking::ChunkingConfig::new()
                .with_threshold(2048)
                .with_chunk_size(1024),
        );

    let task_id = Uuid::new_v4();
    let key = backend.task_key(task_id);

    // Large: many chunks.
    backend
        .store_result(task_id, &big_meta(task_id, "chunk.churn", 120_000))
        .await
        .expect("store large");
    let many = raw_keys(&format!("{}:chunk:*", key)).await;
    assert!(many.len() > 10, "expected a multi-chunk encoding");

    // Smaller, but still chunked: fewer chunks.
    backend
        .store_result(task_id, &big_meta(task_id, "chunk.churn", 8_000))
        .await
        .expect("store smaller");
    let fewer = raw_keys(&format!("{}:chunk:*", key)).await;
    assert!(
        fewer.len() < many.len(),
        "expected fewer chunks after the smaller write"
    );
    assert!(
        !fewer.is_empty(),
        "the smaller payload should still be chunked"
    );

    // Below the threshold: no chunk or metadata keys may survive.
    let mut small = TaskMeta::new(task_id, "chunk.churn".to_string());
    small.result = TaskResult::Success(serde_json::json!("tiny"));
    backend
        .store_result(task_id, &small)
        .await
        .expect("store tiny");

    assert!(
        raw_keys(&format!("{}:chunk:*", key)).await.is_empty(),
        "chunk keys from the previous encoding leaked"
    );
    assert!(
        raw_keys(&format!("{}:chunks", key)).await.is_empty(),
        "chunk metadata from the previous encoding leaked"
    );

    // And the value still reads back correctly.
    let read = backend
        .get_result(task_id)
        .await
        .expect("get")
        .expect("some");
    assert_eq!(read.result, small.result);

    delete_prefix(&prefix).await;
}

/// Archival must copy the *result*, not a key that never existed, and must be
/// readable back through the normal decode pipeline.
#[tokio::test]
#[ignore]
async fn test_integration_archive_roundtrip() {
    let prefix = unique_prefix();
    let mut backend = backend_with_prefix(&prefix)
        .without_compression()
        .with_chunking(
            crate::chunking::ChunkingConfig::new()
                .with_threshold(4096)
                .with_chunk_size(1024),
        );

    // Chunked payload: the case Redis `COPY` silently mangled, because it
    // duplicates the sentinel without any of the chunk keys.
    let task_id = Uuid::new_v4();
    let meta = big_meta(task_id, "archive.big", 200_000);
    backend.store_result(task_id, &meta).await.expect("store");
    let stored = raw_get(&backend.task_key(task_id)).await.expect("stored");
    assert!(
        crate::chunking::ResultChunker::is_chunked(&stored),
        "the archival test must exercise the chunked path"
    );

    backend
        .archive_result(task_id, Duration::from_secs(3600))
        .await
        .expect("archive");

    let archived = backend
        .get_archived_result(task_id)
        .await
        .expect("get archived")
        .expect("archive present");
    assert_eq!(archived.result, meta.result);
    assert_eq!(archived.task_name, meta.task_name);

    // Archiving a task with no result must report the miss.
    let missing = Uuid::new_v4();
    let outcome = backend
        .archive_result(missing, Duration::from_secs(60))
        .await;
    assert!(
        matches!(outcome, Err(crate::BackendError::NotFound(id)) if id == missing),
        "archiving a missing result must not silently succeed"
    );

    // The batch form skips missing ids rather than counting them.
    let count = backend
        .archive_results_batch(&[task_id, missing], Duration::from_secs(60))
        .await
        .expect("batch archive");
    assert_eq!(count, 1);

    delete_prefix(&prefix).await;
}

/// Secondary write paths must not bypass encryption, and CAS must work when
/// compression is active.
#[tokio::test]
#[ignore]
async fn test_integration_secondary_writes_are_encrypted_and_cas_works() {
    let prefix = unique_prefix();
    let key_material = encryption::EncryptionKey::generate();
    let mut backend = backend_with_prefix(&prefix)
        .with_encryption(encryption::EncryptionConfig::new(key_material))
        .with_compression(compression::CompressionConfig::new().with_threshold(64));

    // atomic_store_multiple used to write plaintext JSON.
    let task_id = Uuid::new_v4();
    let mut meta = TaskMeta::new(task_id, "secrets.task".to_string());
    meta.result = TaskResult::Success(serde_json::json!({ "secret": "s3cr3t-value" }));

    backend
        .atomic_store_multiple(&[(task_id, meta.clone())], None)
        .await
        .expect("atomic store");

    let raw = raw_get(&backend.task_key(task_id)).await.expect("stored");
    assert!(
        !String::from_utf8_lossy(&raw).contains("s3cr3t-value"),
        "atomic_store_multiple must not write plaintext when encryption is on"
    );

    let read = backend
        .get_result(task_id)
        .await
        .expect("get")
        .expect("some");
    assert_eq!(read.result, meta.result);

    // CAS over a compressed + encrypted value.
    let cas_id = Uuid::new_v4();
    let mut expected = big_meta(cas_id, "cas.task", 4_000);
    expected.result = TaskResult::Started;
    backend
        .store_result(cas_id, &expected)
        .await
        .expect("store");
    // The stored record is what the server has; read it back so timestamps match.
    let expected = backend
        .get_result_uncached(cas_id)
        .await
        .expect("get")
        .expect("some");

    let mut updated = expected.clone();
    updated.result = TaskResult::Success(serde_json::json!({"n": 42}));

    assert!(
        backend
            .compare_and_swap(cas_id, &expected, &updated)
            .await
            .expect("cas"),
        "CAS must succeed with compression and encryption enabled"
    );

    // A second swap against the now-stale expectation must fail.
    assert!(
        !backend
            .compare_and_swap(cas_id, &expected, &updated)
            .await
            .expect("cas"),
        "CAS must reject a stale expectation"
    );

    delete_prefix(&prefix).await;
}

/// The chord counter must be reflected in the state, and cancelling must not
/// wipe it. Chord keys must also expire.
#[tokio::test]
#[ignore]
async fn test_integration_chord_progress_and_cancel() {
    let (mut backend, prefix) = test_backend();

    let chord_id = Uuid::new_v4();
    let task_ids: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();
    backend
        .chord_init(crate::ChordState::new(chord_id, task_ids.len(), task_ids))
        .await
        .expect("chord init");

    for expected in 1..=3usize {
        let count = backend
            .chord_complete_task(chord_id)
            .await
            .expect("chord complete");
        assert_eq!(count, expected);
    }

    let state = backend
        .chord_get_state(chord_id)
        .await
        .expect("chord state")
        .expect("present");
    assert_eq!(
        state.completed, 3,
        "chord state must reflect the completion counter"
    );
    assert!(state.is_complete(), "a finished chord must report complete");
    assert!(
        state.is_terminal(),
        "completed chords must be terminal so cleanup can collect them"
    );

    // Cancelling records the cancellation without forgetting the progress.
    backend
        .chord_cancel(chord_id, Some("operator".to_string()))
        .await
        .expect("chord cancel");
    let cancelled = backend
        .chord_get_state(chord_id)
        .await
        .expect("chord state")
        .expect("present");
    assert!(cancelled.is_cancelled());
    assert_eq!(
        cancelled.completed, 3,
        "cancellation must not reset the completion counter"
    );

    // Both chord keys must carry a TTL, otherwise every chord leaks two keys.
    let client = redis::Client::open(redis_url()).expect("redis client");
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .expect("connection");
    for key in [
        format!("celery-chord-{}", chord_id),
        format!("celery-chord-counter-{}", chord_id),
    ] {
        let remaining: i64 = redis::cmd("TTL")
            .arg(&key)
            .query_async(&mut conn)
            .await
            .expect("TTL");
        assert!(remaining > 0, "chord key {} has no expiry", key);
        let _: i64 = redis::cmd("DEL")
            .arg(&key)
            .query_async(&mut conn)
            .await
            .expect("DEL");
    }

    delete_prefix(&prefix).await;
}

/// The `query_tasks_*` family must actually find tasks.
#[tokio::test]
#[ignore]
async fn test_integration_query_tasks_by_state_and_worker() {
    let (mut backend, prefix) = test_backend();

    let failed_id = Uuid::new_v4();
    let ok_id = Uuid::new_v4();

    let mut failed = TaskMeta::new(failed_id, "query.failed".to_string());
    failed.result = TaskResult::Failure("boom".to_string());
    failed.worker = Some("worker-a".to_string());

    let mut ok = TaskMeta::new(ok_id, "query.ok".to_string());
    ok.result = TaskResult::Success(serde_json::json!(true));
    ok.worker = Some("worker-b".to_string());

    backend
        .store_result(failed_id, &failed)
        .await
        .expect("store");
    backend.store_result(ok_id, &ok).await.expect("store");

    let failures = backend
        .query_tasks_by_state(TaskResult::Failure(String::new()))
        .await
        .expect("query");
    assert_eq!(
        failures,
        vec![failed_id],
        "query_tasks_by_state must find stored tasks"
    );

    let by_worker = backend
        .query_tasks_by_worker("worker-b")
        .await
        .expect("query");
    assert_eq!(by_worker, vec![ok_id]);

    delete_prefix(&prefix).await;
}

/// The `ResultStore` adapter must not blank out the record it is updating.
#[tokio::test]
#[ignore]
async fn test_integration_result_store_preserves_metadata() {
    use celers_core::result::{ResultStore, TaskResultValue};

    let (mut backend, prefix) = test_backend();
    let task_id = Uuid::new_v4();

    let mut meta = TaskMeta::new(task_id, "billing.charge".to_string());
    meta.worker = Some("worker-7".to_string());
    meta.started_at = Some(chrono::Utc::now());
    meta.add_tag("billing");
    meta.result = TaskResult::Started;
    ResultBackend::store_result(&mut backend, task_id, &meta)
        .await
        .expect("store");

    ResultStore::store_result(
        &backend,
        task_id,
        TaskResultValue::Success(serde_json::json!({"charged": true})),
    )
    .await
    .expect("adapter store");

    let after = backend
        .get_result_uncached(task_id)
        .await
        .expect("get")
        .expect("some");

    assert_eq!(after.task_name, "billing.charge", "task name was wiped");
    assert_eq!(
        after.worker.as_deref(),
        Some("worker-7"),
        "worker was wiped"
    );
    assert!(after.started_at.is_some(), "started_at was wiped");
    assert!(after.has_tag("billing"), "tags were wiped");
    assert!(after.result.is_success());
    assert!(after.completed_at.is_some());
    assert!(after.duration().is_some(), "duration() must be computable");

    // Statistics recorded through the adapter's clone must reach the shared
    // counters rather than being dropped with the temporary.
    assert!(
        backend.compression_stats().operation_count() > 0,
        "compression statistics must survive the adapter's clone"
    );

    delete_prefix(&prefix).await;
}

/// Versioned history must actually be retained and addressable.
#[tokio::test]
#[ignore]
async fn test_integration_versioned_results() {
    let prefix = unique_prefix();
    let mut backend =
        backend_with_prefix(&prefix).with_versioning(VersioningConfig::new().with_max_versions(2));

    let task_id = Uuid::new_v4();
    let mut versions = Vec::new();

    for n in 1..=3u32 {
        let mut meta = TaskMeta::new(task_id, "versioned.task".to_string());
        meta.result = TaskResult::Success(serde_json::json!({ "round": n }));
        let version = backend
            .store_versioned_result(task_id, &meta)
            .await
            .expect("versioned store");
        assert_eq!(version, n, "version numbers must increase monotonically");
        versions.push(version);
    }

    // The two most recent versions are retained and return their own payload.
    for n in [2u32, 3] {
        let found = backend
            .get_result_version(task_id, n)
            .await
            .expect("get version")
            .unwrap_or_else(|| panic!("version {} should be retained", n));
        assert_eq!(found.version, n);
        assert_eq!(
            found.result,
            TaskResult::Success(serde_json::json!({"round": n}))
        );
    }

    // The evicted version reports a miss instead of returning a different one.
    assert!(
        backend
            .get_result_version(task_id, 1)
            .await
            .expect("get version")
            .is_none(),
        "an evicted version must not be answered with a different version"
    );

    // A version that never existed likewise.
    assert!(backend
        .get_result_version(task_id, 99)
        .await
        .expect("get version")
        .is_none());

    assert_eq!(
        backend.list_result_versions(task_id).await.expect("list"),
        vec![2, 3]
    );

    delete_prefix(&prefix).await;
}

/// `wait_for_result` must return promptly once the result lands, rather than
/// waiting out a fixed poll interval.
#[tokio::test]
#[ignore]
async fn test_integration_wait_for_result_is_prompt() {
    let (mut backend, prefix) = test_backend();
    let task_id = Uuid::new_v4();

    let mut pending = TaskMeta::new(task_id, "wait.task".to_string());
    pending.result = TaskResult::Started;
    backend
        .store_result(task_id, &pending)
        .await
        .expect("store");

    let writer_prefix = prefix.clone();
    let writer = tokio::spawn(async move {
        let mut writer = backend_with_prefix(&writer_prefix);
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut done = TaskMeta::new(task_id, "wait.task".to_string());
        done.result = TaskResult::Success(serde_json::json!("done"));
        writer.store_result(task_id, &done).await.expect("store");
    });

    let started = std::time::Instant::now();
    // A 30 s poll interval: the old fixed-interval loop would have timed out.
    let result = backend
        .wait_for_result(task_id, Duration::from_secs(10), Duration::from_secs(30))
        .await
        .expect("wait");
    let elapsed = started.elapsed();

    writer.await.expect("writer task");

    assert!(
        result.is_some_and(|meta| meta.result.is_success()),
        "wait_for_result must observe the completion"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "wait_for_result must not sit out the whole poll interval (took {:?})",
        elapsed
    );

    delete_prefix(&prefix).await;
}

/// Distributed lock acquisition must never extend somebody else's lock.
#[cfg(feature = "distributed-locks")]
#[tokio::test]
#[ignore]
async fn test_integration_lock_acquire_is_atomic() {
    use celers_core::lock::DistributedLockBackend;

    let prefix = format!("celers-test-lock-{}:", Uuid::new_v4());
    let backend = crate::lock::RedisLockBackend::with_prefix(&redis_url(), prefix.clone())
        .expect("lock backend");

    let key = "beat-task";

    assert!(
        backend
            .try_acquire(key, "owner-a", 60)
            .await
            .expect("acquire"),
        "an unheld lock must be acquirable"
    );

    // The same owner refreshes rather than being refused.
    assert!(
        backend
            .try_acquire(key, "owner-a", 90)
            .await
            .expect("acquire"),
        "the holder must be able to refresh its own lock"
    );
    assert_eq!(
        backend.owner(key).await.expect("owner").as_deref(),
        Some("owner-a")
    );

    // A different owner is refused and must not have taken ownership.
    assert!(
        !backend
            .try_acquire(key, "owner-b", 60)
            .await
            .expect("acquire"),
        "a contended lock must not be granted"
    );
    assert_eq!(
        backend.owner(key).await.expect("owner").as_deref(),
        Some("owner-a"),
        "a refused acquire must not change the owner"
    );

    // ...and a failed acquire must not release it either.
    assert!(!backend.release(key, "owner-b").await.expect("release"));
    assert!(backend.release(key, "owner-a").await.expect("release"));
    assert!(!backend.is_locked(key).await.expect("is_locked"));

    delete_prefix(&prefix).await;
}
