#![cfg(test)]

use std::time::Duration;

use chrono::Utc;
use uuid::Uuid;

use crate::backend::RedisResultBackend;
use crate::chunking;
use crate::query::TaskQuery;
use crate::result_backend_trait::{LazyTaskResult, ResultBackend};
use crate::stats::{BackendStats, BatchOperationResult, StateCount, StatePercentages, TaskSummary};
use crate::types::{BackendError, ChordState, ProgressInfo, TaskMeta, TaskResult, TaskTtlConfig};
use crate::{cache, compression, encryption, metrics};

#[test]
fn test_task_meta_creation() {
    let task_id = Uuid::new_v4();
    let meta = TaskMeta::new(task_id, "test_task".to_string());

    assert_eq!(meta.task_id, task_id);
    assert_eq!(meta.task_name, "test_task");
    assert_eq!(meta.result, TaskResult::Pending);
    assert!(meta.started_at.is_none());
}

#[test]
fn test_chord_state() {
    let chord_id = Uuid::new_v4();
    let state = ChordState::new(chord_id, 10, vec![])
        .with_callback("callback_task".to_string())
        .with_timeout(Duration::from_secs(60));

    assert_eq!(state.total, 10);
    assert_eq!(state.completed, 0);
    assert!(state.has_callback());
    assert!(state.has_timeout());
    assert!(!state.is_timed_out());
    assert!(state.remaining_timeout().is_some());
}

#[test]
fn test_chord_timeout() {
    let chord_id = Uuid::new_v4();
    let state = ChordState::new(chord_id, 5, vec![]).with_timeout(Duration::from_millis(50));

    assert!(!state.is_timed_out());
    assert!(state.remaining_timeout().is_some());

    // Wait for timeout
    std::thread::sleep(Duration::from_millis(100));

    assert!(state.is_timed_out());
    assert!(state.remaining_timeout().is_none());
}

#[test]
fn test_chord_cancellation() {
    let chord_id = Uuid::new_v4();
    let mut state = ChordState::new(chord_id, 10, vec![]);

    assert!(!state.is_cancelled());
    assert!(!state.is_terminal());

    // Cancel the chord
    state.cancel(Some("User requested".to_string()));

    assert!(state.is_cancelled());
    assert!(state.is_terminal());
    assert_eq!(
        state.cancellation_reason,
        Some("User requested".to_string())
    );

    // Cancelled chords are not complete even if all tasks finish
    assert!(!state.is_complete());
}

#[test]
fn test_chord_terminal_states() {
    let chord_id = Uuid::new_v4();

    // Complete chord
    let mut complete_state = ChordState::new(chord_id, 5, vec![]);
    complete_state.completed = 5;
    assert!(complete_state.is_complete());
    assert!(complete_state.is_terminal());

    // Cancelled chord
    let mut cancelled_state = ChordState::new(chord_id, 5, vec![]);
    cancelled_state.cancel(None);
    assert!(cancelled_state.is_cancelled());
    assert!(cancelled_state.is_terminal());

    // Timed out chord
    let timed_out_state =
        ChordState::new(chord_id, 5, vec![]).with_timeout(Duration::from_millis(1));
    std::thread::sleep(Duration::from_millis(10));
    assert!(timed_out_state.is_timed_out());
    assert!(timed_out_state.is_terminal());
}

#[test]
fn test_chord_retry_logic() {
    let chord_id = Uuid::new_v4();
    let mut state = ChordState::new(chord_id, 10, vec![]).with_max_retries(3);

    // Initial state
    assert_eq!(state.retry_count, 0);
    assert!(!state.is_retry());
    assert!(state.can_retry());
    assert_eq!(state.remaining_retries(), Some(3));

    // First retry
    assert!(state.retry());
    assert_eq!(state.retry_count, 1);
    assert!(state.is_retry());
    assert_eq!(state.remaining_retries(), Some(2));

    // Second retry
    assert!(state.retry());
    assert_eq!(state.retry_count, 2);
    assert_eq!(state.remaining_retries(), Some(1));

    // Third retry
    assert!(state.retry());
    assert_eq!(state.retry_count, 3);
    assert_eq!(state.remaining_retries(), Some(0));

    // Max retries exceeded
    assert!(!state.can_retry());
    assert!(!state.retry());
    assert_eq!(state.retry_count, 3);
}

#[test]
fn test_chord_retry_resets_state() {
    let chord_id = Uuid::new_v4();
    let mut state = ChordState::new(chord_id, 10, vec![]).with_max_retries(2);

    // Simulate partial completion and cancellation
    state.completed = 5;
    state.cancel(Some("test".to_string()));
    assert_eq!(state.completed, 5);
    assert!(state.is_cancelled());

    // Retry should reset state
    let old_created_at = state.created_at;
    std::thread::sleep(Duration::from_millis(10));
    assert!(state.retry());

    assert_eq!(state.completed, 0);
    assert!(!state.is_cancelled());
    assert!(state.cancellation_reason.is_none());
    assert!(state.created_at > old_created_at);
}

#[test]
fn test_lazy_task_result() {
    let task_id = Uuid::new_v4();
    let meta = TaskMeta::new(task_id, "test_task".to_string());

    // Create lazy result without data
    let lazy = LazyTaskResult::new(task_id);
    assert_eq!(lazy.task_id, task_id);
    assert!(!lazy.is_loaded());
    assert!(lazy.get_cached().is_none());

    // Create lazy result with data
    let lazy_with_data = LazyTaskResult::with_data(meta.clone());
    assert_eq!(lazy_with_data.task_id, task_id);
    assert!(lazy_with_data.is_loaded());
    assert!(lazy_with_data.get_cached().is_some());
    assert_eq!(lazy_with_data.get_cached().unwrap().task_name, "test_task");
}

#[test]
fn test_task_result_utility_methods() {
    // Test is_terminal
    assert!(TaskResult::Success(serde_json::json!(null)).is_terminal());
    assert!(TaskResult::Failure("error".to_string()).is_terminal());
    assert!(TaskResult::Revoked.is_terminal());
    assert!(!TaskResult::Pending.is_terminal());
    assert!(!TaskResult::Started.is_terminal());
    assert!(!TaskResult::Retry(1).is_terminal());

    // Test is_active
    assert!(!TaskResult::Success(serde_json::json!(null)).is_active());
    assert!(!TaskResult::Failure("error".to_string()).is_active());
    assert!(!TaskResult::Revoked.is_active());
    assert!(TaskResult::Pending.is_active());
    assert!(TaskResult::Started.is_active());
    assert!(TaskResult::Retry(1).is_active());

    // Test value getters
    let success = TaskResult::Success(serde_json::json!({"result": 42}));
    assert!(success.success_value().is_some());
    assert_eq!(success.success_value().unwrap()["result"], 42);

    let failure = TaskResult::Failure("test error".to_string());
    assert_eq!(failure.failure_message(), Some("test error"));

    let retry = TaskResult::Retry(3);
    assert_eq!(retry.retry_count(), Some(3));
}

#[test]
fn test_progress_info_utility_methods() {
    let progress = ProgressInfo::new(50, 100);
    assert_eq!(progress.percent, 50.0);
    assert!(!progress.is_complete());
    assert_eq!(progress.remaining(), 50);
    assert_eq!(progress.fraction(), 0.5);
    assert!(!progress.has_message());

    let complete = ProgressInfo::new(100, 100);
    assert!(complete.is_complete());
    assert_eq!(complete.remaining(), 0);

    let with_msg = ProgressInfo::new(25, 100).with_message("Processing...".to_string());
    assert!(with_msg.has_message());
    assert_eq!(with_msg.message, Some("Processing...".to_string()));
}

#[test]
fn test_task_meta_utility_methods() {
    let task_id = Uuid::new_v4();
    let mut meta = TaskMeta::new(task_id, "test_task".to_string());

    // Initially not started or completed
    assert!(!meta.has_started());
    assert!(!meta.has_completed());
    assert!(!meta.has_progress());
    assert!(meta.is_active());
    assert!(!meta.is_terminal());

    // Mark as started
    meta.started_at = Some(Utc::now());
    assert!(meta.has_started());
    assert!(meta.execution_time().is_some());

    // Add progress
    meta.progress = Some(ProgressInfo::new(10, 100));
    assert!(meta.has_progress());

    // Mark as completed
    meta.result = TaskResult::Success(serde_json::json!(null));
    meta.completed_at = Some(Utc::now());
    assert!(meta.has_completed());
    assert!(meta.is_terminal());
    assert!(!meta.is_active());
    assert!(meta.duration().is_some());

    // Test age
    assert!(meta.age().num_milliseconds() >= 0);
}

#[test]
fn test_chord_state_utility_methods() {
    let chord_id = Uuid::new_v4();
    let task_ids = vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
    let state = ChordState::new(chord_id, 10, task_ids.clone());

    assert_eq!(state.remaining(), 10);
    assert_eq!(state.percent_complete(), 0.0);
    assert!(!state.has_callback());
    assert!(!state.has_timeout());
    assert_eq!(state.task_count(), 3);
    assert!(!state.is_terminal());
    assert!(state.age().num_milliseconds() >= 0);

    let mut state_with_progress = state.clone();
    state_with_progress.completed = 5;
    assert_eq!(state_with_progress.remaining(), 5);
    assert_eq!(state_with_progress.percent_complete(), 50.0);
}

#[test]
fn test_backend_error_methods() {
    let redis_err = BackendError::Connection("test".to_string());
    assert!(redis_err.is_connection());
    assert!(redis_err.is_retryable());
    assert_eq!(redis_err.category(), "connection");

    let ser_err = BackendError::Serialization("test".to_string());
    assert!(ser_err.is_serialization());
    assert!(!ser_err.is_retryable());
    assert_eq!(ser_err.category(), "serialization");

    let not_found = BackendError::NotFound(Uuid::new_v4());
    assert!(not_found.is_not_found());
    assert!(!not_found.is_retryable());
    assert_eq!(not_found.category(), "not_found");
}

#[test]
fn test_progress_info_edge_cases() {
    // Zero total
    let zero = ProgressInfo::new(0, 0);
    assert_eq!(zero.percent, 0.0);
    assert_eq!(zero.remaining(), 0);

    // Current > total (should cap at 100%)
    let over = ProgressInfo::new(150, 100);
    assert_eq!(over.percent, 100.0);
    assert!(over.is_complete());
}

#[test]
fn test_chord_state_display() {
    let chord_id = Uuid::new_v4();
    let state = ChordState::new(chord_id, 10, vec![])
        .with_callback("callback_task".to_string())
        .with_timeout(Duration::from_secs(60));

    let display = format!("{}", state);
    assert!(display.contains("Chord"));
    assert!(display.contains("0/10"));
    assert!(display.contains("callback=callback_task"));
}

#[test]
fn test_task_result_display() {
    assert_eq!(format!("{}", TaskResult::Pending), "PENDING");
    assert_eq!(format!("{}", TaskResult::Started), "STARTED");
    assert_eq!(
        format!("{}", TaskResult::Success(serde_json::json!(null))),
        "SUCCESS"
    );
    assert_eq!(
        format!("{}", TaskResult::Failure("err".to_string())),
        "FAILURE: err"
    );
    assert_eq!(format!("{}", TaskResult::Revoked), "REVOKED");
    assert_eq!(format!("{}", TaskResult::Retry(3)), "RETRY(3)");
}

#[test]
fn test_task_meta_display() {
    let task_id = Uuid::new_v4();
    let mut meta = TaskMeta::new(task_id, "test_task".to_string());
    meta.worker = Some("worker-1".to_string());
    meta.progress = Some(ProgressInfo::new(50, 100));

    let display = format!("{}", meta);
    assert!(display.contains("Task"));
    assert!(display.contains("test_task"));
    assert!(display.contains("worker=worker-1"));
    assert!(display.contains("progress=50/100"));
}

#[test]
fn test_progress_info_display() {
    let progress = ProgressInfo::new(75, 100).with_message("Processing files".to_string());
    let display = format!("{}", progress);
    assert!(display.contains("75/100"));
    assert!(display.contains("75.0%"));
    assert!(display.contains("Processing files"));
}

#[test]
fn test_ttl_duration_values() {
    // Test various TTL durations
    let one_hour = Duration::from_secs(3600);
    assert_eq!(one_hour.as_secs(), 3600);

    let one_day = Duration::from_secs(86400);
    assert_eq!(one_day.as_secs(), 86400);

    let one_week = Duration::from_secs(604800);
    assert_eq!(one_week.as_secs(), 604800);

    // Test zero duration (immediate expiration)
    let zero = Duration::from_secs(0);
    assert_eq!(zero.as_secs(), 0);
}

#[test]
fn test_chord_barrier_completion_tracking() {
    let chord_id = Uuid::new_v4();
    let mut state = ChordState::new(chord_id, 5, vec![]);

    // Simulate task completions
    for i in 1..=5 {
        state.completed = i;
        if i < 5 {
            assert!(!state.is_complete());
            assert_eq!(state.remaining(), 5 - i);
        } else {
            assert!(state.is_complete());
            assert_eq!(state.remaining(), 0);
        }
    }

    assert_eq!(state.percent_complete(), 100.0);
}

#[test]
fn test_chord_barrier_race_condition_safety() {
    // Test that chord state correctly handles the total vs completed
    let chord_id = Uuid::new_v4();
    let mut state = ChordState::new(chord_id, 10, vec![]);

    // Simulate concurrent completions by directly setting completed count
    state.completed = 9;
    assert!(!state.is_complete());
    assert_eq!(state.remaining(), 1);

    state.completed = 10;
    assert!(state.is_complete());
    assert_eq!(state.remaining(), 0);

    // Over-completion should still be considered complete
    state.completed = 11;
    assert!(state.is_complete());
    assert_eq!(state.remaining(), 0);
}

#[test]
fn test_chord_timeout_edge_cases() {
    let chord_id = Uuid::new_v4();

    // Chord without timeout
    let state_no_timeout = ChordState::new(chord_id, 5, vec![]);
    assert!(!state_no_timeout.has_timeout());
    assert!(!state_no_timeout.is_timed_out());
    assert!(state_no_timeout.remaining_timeout().is_none());

    // Chord with very long timeout
    let state_long = ChordState::new(chord_id, 5, vec![]).with_timeout(Duration::from_secs(3600));
    assert!(state_long.has_timeout());
    assert!(!state_long.is_timed_out());
    assert!(state_long.remaining_timeout().is_some());
    assert!(state_long.remaining_timeout().unwrap().as_secs() > 3500);
}

#[test]
fn test_serialization_roundtrip() {
    // Test TaskMeta serialization
    let task_id = Uuid::new_v4();
    let meta = TaskMeta::new(task_id, "test_task".to_string());
    let json = serde_json::to_string(&meta).unwrap();
    let deserialized: TaskMeta = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.task_id, task_id);
    assert_eq!(deserialized.task_name, "test_task");

    // Test ChordState serialization
    let chord_id = Uuid::new_v4();
    let chord = ChordState::new(chord_id, 10, vec![task_id])
        .with_callback("callback".to_string())
        .with_timeout(Duration::from_secs(60));
    let json = serde_json::to_string(&chord).unwrap();
    let deserialized: ChordState = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.chord_id, chord_id);
    assert_eq!(deserialized.total, 10);
    assert_eq!(deserialized.task_ids.len(), 1);

    // Test ProgressInfo serialization
    let progress = ProgressInfo::new(50, 100).with_message("test".to_string());
    let json = serde_json::to_string(&progress).unwrap();
    let deserialized: ProgressInfo = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.current, 50);
    assert_eq!(deserialized.total, 100);
    assert_eq!(deserialized.message, Some("test".to_string()));
}

#[test]
fn test_task_result_serialization() {
    // Test all TaskResult variants
    let variants = vec![
        TaskResult::Pending,
        TaskResult::Started,
        TaskResult::Success(serde_json::json!({"data": "test"})),
        TaskResult::Failure("error message".to_string()),
        TaskResult::Revoked,
        TaskResult::Retry(3),
    ];

    for variant in variants {
        let json = serde_json::to_string(&variant).unwrap();
        let deserialized: TaskResult = serde_json::from_str(&json).unwrap();
        assert_eq!(format!("{:?}", variant), format!("{:?}", deserialized));
    }
}

#[test]
fn test_backend_error_retryable_classification() {
    // Retryable errors
    assert!(BackendError::Connection("timeout".to_string()).is_retryable());

    // Non-retryable errors
    assert!(!BackendError::Serialization("invalid json".to_string()).is_retryable());
    assert!(!BackendError::NotFound(Uuid::new_v4()).is_retryable());
}

#[test]
fn test_chord_state_version_field() {
    let chord_id = Uuid::new_v4();
    let state = ChordState::new(chord_id, 5, vec![]);

    // Verify version-related fields exist
    assert_eq!(state.retry_count, 0);
    assert!(!state.is_retry());
}

#[test]
fn test_task_meta_version_field() {
    let task_id = Uuid::new_v4();
    let meta = TaskMeta::new(task_id, "test".to_string());

    // Verify version field exists and is initialized
    assert_eq!(meta.version, 0);
}

#[test]
fn test_lazy_task_result_creation() {
    let task_id = Uuid::new_v4();

    // Test new() constructor
    let lazy = LazyTaskResult::new(task_id);
    assert_eq!(lazy.task_id, task_id);
    assert!(!lazy.is_loaded());

    // Test with_data() constructor
    let meta = TaskMeta::new(task_id, "test".to_string());
    let lazy_loaded = LazyTaskResult::with_data(meta);
    assert_eq!(lazy_loaded.task_id, task_id);
    assert!(lazy_loaded.is_loaded());
}

#[test]
fn test_chord_cancellation_with_reason() {
    let chord_id = Uuid::new_v4();
    let mut state = ChordState::new(chord_id, 10, vec![]);

    // Cancel with reason
    state.cancel(Some("User requested cancellation".to_string()));
    assert!(state.is_cancelled());
    assert_eq!(
        state.cancellation_reason,
        Some("User requested cancellation".to_string())
    );

    // Cancel without reason
    let mut state2 = ChordState::new(chord_id, 10, vec![]);
    state2.cancel(None);
    assert!(state2.is_cancelled());
    assert!(state2.cancellation_reason.is_none());
}

#[test]
fn test_progress_info_fraction_calculation() {
    let progress = ProgressInfo::new(25, 100);
    assert_eq!(progress.fraction(), 0.25);

    let half = ProgressInfo::new(50, 100);
    assert_eq!(half.fraction(), 0.5);

    let complete = ProgressInfo::new(100, 100);
    assert_eq!(complete.fraction(), 1.0);

    let empty = ProgressInfo::new(0, 100);
    assert_eq!(empty.fraction(), 0.0);
}

#[test]
fn test_chord_retry_max_retries() {
    let chord_id = Uuid::new_v4();
    let mut state = ChordState::new(chord_id, 10, vec![]).with_max_retries(2);

    assert_eq!(state.max_retries, Some(2));
    assert_eq!(state.remaining_retries(), Some(2));

    // First retry
    assert!(state.retry());
    assert_eq!(state.remaining_retries(), Some(1));

    // Second retry
    assert!(state.retry());
    assert_eq!(state.remaining_retries(), Some(0));

    // Should not allow more retries
    assert!(!state.retry());
    assert_eq!(state.retry_count, 2);
}

#[test]
fn test_redis_backend_key_generation() {
    let backend = RedisResultBackend::new("redis://localhost:6379").unwrap();
    let task_id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let chord_id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440001").unwrap();

    let task_key = backend.task_key(task_id);
    assert_eq!(
        task_key,
        "celery-task-meta-550e8400-e29b-41d4-a716-446655440000"
    );

    let chord_key = backend.chord_key(chord_id);
    assert_eq!(
        chord_key,
        "celery-chord-550e8400-e29b-41d4-a716-446655440001"
    );

    let counter_key = backend.chord_counter_key(chord_id);
    assert_eq!(
        counter_key,
        "celery-chord-counter-550e8400-e29b-41d4-a716-446655440001"
    );
}

#[test]
fn test_redis_backend_with_custom_prefix() {
    let backend = RedisResultBackend::new("redis://localhost:6379")
        .unwrap()
        .with_prefix("custom-prefix-".to_string());

    let task_id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let task_key = backend.task_key(task_id);
    assert_eq!(
        task_key,
        "custom-prefix-550e8400-e29b-41d4-a716-446655440000"
    );
}

#[test]
fn test_redis_backend_compression_config() {
    let mut backend = RedisResultBackend::new("redis://localhost:6379").unwrap();

    // Test default compression
    assert!(backend.compression_config().enabled);

    // Test disabling compression
    backend = backend.without_compression();
    assert!(!backend.compression_config().enabled);

    // Test custom compression config
    let config = compression::CompressionConfig::default()
        .with_threshold(2048)
        .with_level(9); // Best compression level
    backend = backend.with_compression(config);
    assert!(backend.compression_config().enabled);
    assert_eq!(backend.compression_config().threshold, 2048);
    assert_eq!(backend.compression_config().level, 9);
}

#[test]
fn test_redis_backend_encryption_config() {
    let mut backend = RedisResultBackend::new("redis://localhost:6379").unwrap();

    // Test default (disabled)
    assert!(!backend.encryption_config().enabled);

    // Test enabling encryption
    let key = encryption::EncryptionKey::generate();
    let config = encryption::EncryptionConfig::new(key);
    backend = backend.with_encryption(config);
    assert!(backend.encryption_config().enabled);

    // Test disabling encryption
    backend = backend.without_encryption();
    assert!(!backend.encryption_config().enabled);
}

#[test]
fn test_redis_backend_cache_config() {
    let mut backend = RedisResultBackend::new("redis://localhost:6379").unwrap();

    // Test default cache
    assert!(backend.cache().config().enabled);

    // Test disabling cache
    backend = backend.without_cache();
    assert!(!backend.cache().config().enabled);

    // Test custom cache config
    let cache = cache::ResultCache::new(
        cache::CacheConfig::default()
            .with_capacity(500)
            .with_ttl(Duration::from_secs(120)),
    );
    backend = backend.with_cache(cache);
    assert_eq!(backend.cache().config().capacity, 500);
    assert_eq!(backend.cache().config().ttl.as_secs(), 120);
}

#[test]
fn test_redis_backend_metrics_config() {
    let mut backend = RedisResultBackend::new("redis://localhost:6379").unwrap();

    // Test default metrics
    assert!(backend.metrics().is_enabled());

    // Test disabling metrics
    backend = backend.without_metrics();
    assert!(!backend.metrics().is_enabled());

    // Test custom metrics
    let metrics = metrics::BackendMetrics::new();
    backend = backend.with_metrics(metrics);
    assert!(backend.metrics().is_enabled());
}

// =============================================================================
// Integration Tests (require live Redis instance)
// Run with: cargo test -- --ignored
// =============================================================================

#[tokio::test]
#[ignore]
async fn test_integration_basic_store_and_retrieve() {
    let mut backend = RedisResultBackend::new("redis://localhost:6379").unwrap();
    let task_id = Uuid::new_v4();
    let mut meta = TaskMeta::new(task_id, "integration_test".to_string());
    meta.result = TaskResult::Success(serde_json::json!({"test": "data"}));

    // Store result
    backend.store_result(task_id, &meta).await.unwrap();

    // Retrieve result
    let retrieved = backend.get_result(task_id).await.unwrap();
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.task_id, task_id);
    assert_eq!(retrieved.task_name, "integration_test");
    assert!(retrieved.result.is_success());

    // Cleanup
    backend.delete_result(task_id).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_integration_compression() {
    let mut backend = RedisResultBackend::new("redis://localhost:6379")
        .unwrap()
        .with_compression(compression::CompressionConfig {
            enabled: true,
            threshold: 100,
            level: 6,
            algorithm: compression::CompressionAlgorithm::Gzip,
        });

    let task_id = Uuid::new_v4();
    let mut meta = TaskMeta::new(task_id, "compression_test".to_string());
    // Create large data that will be compressed
    meta.result = TaskResult::Success(serde_json::json!({
        "data": "x".repeat(10000)
    }));

    // Store and retrieve
    backend.store_result(task_id, &meta).await.unwrap();
    let retrieved = backend.get_result(task_id).await.unwrap().unwrap();

    // Verify data integrity after compression/decompression
    if let TaskResult::Success(value) = &retrieved.result {
        let data = value.get("data").unwrap().as_str().unwrap();
        assert_eq!(data.len(), 10000);
        assert_eq!(data, "x".repeat(10000));
    } else {
        panic!("Expected success result");
    }

    // Cleanup
    backend.delete_result(task_id).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_integration_encryption() {
    let key = encryption::EncryptionKey::generate();
    let mut backend = RedisResultBackend::new("redis://localhost:6379")
        .unwrap()
        .with_encryption(encryption::EncryptionConfig::new(key));

    let task_id = Uuid::new_v4();
    let mut meta = TaskMeta::new(task_id, "encryption_test".to_string());
    meta.result = TaskResult::Success(serde_json::json!({
        "secret": "sensitive_data_12345"
    }));

    // Store encrypted data
    backend.store_result(task_id, &meta).await.unwrap();

    // Retrieve and decrypt
    let retrieved = backend.get_result(task_id).await.unwrap().unwrap();
    if let TaskResult::Success(value) = &retrieved.result {
        assert_eq!(
            value.get("secret").unwrap().as_str().unwrap(),
            "sensitive_data_12345"
        );
    } else {
        panic!("Expected success result");
    }

    // Cleanup
    backend.delete_result(task_id).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_integration_chord_operations() {
    let mut backend = RedisResultBackend::new("redis://localhost:6379").unwrap();
    let chord_id = Uuid::new_v4();
    let task_ids: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();

    // Initialize chord
    let state = ChordState::new(chord_id, task_ids.len(), task_ids.clone());
    backend.chord_init(state).await.unwrap();

    // Complete tasks one by one
    for (i, &task_id) in task_ids.iter().enumerate() {
        // Store task result
        let mut meta = TaskMeta::new(task_id, format!("chord_task_{}", i));
        meta.result = TaskResult::Success(serde_json::json!({"index": i}));
        backend.store_result(task_id, &meta).await.unwrap();

        // Complete task in chord
        let completed = backend.chord_complete_task(chord_id).await.unwrap();
        assert_eq!(completed, i + 1);
    }

    // Verify chord is complete
    let final_state = backend.chord_get_state(chord_id).await.unwrap().unwrap();
    assert!(final_state.is_complete());
    assert_eq!(final_state.completed, task_ids.len());

    // Cleanup
    for task_id in &task_ids {
        backend.delete_result(*task_id).await.unwrap();
    }
}

#[tokio::test]
#[ignore]
async fn test_integration_batch_operations() {
    let mut backend = RedisResultBackend::new("redis://localhost:6379").unwrap();
    let task_ids: Vec<Uuid> = (0..10).map(|_| Uuid::new_v4()).collect();
    let results: Vec<(Uuid, TaskMeta)> = task_ids
        .iter()
        .enumerate()
        .map(|(i, &id)| {
            let mut meta = TaskMeta::new(id, format!("batch_task_{}", i));
            meta.result = TaskResult::Success(serde_json::json!({"index": i}));
            (id, meta)
        })
        .collect();

    // Batch store
    backend.store_results_batch(&results).await.unwrap();

    // Batch retrieve
    let retrieved = backend.get_results_batch(&task_ids).await.unwrap();
    assert_eq!(retrieved.len(), task_ids.len());
    assert!(retrieved.iter().all(|r| r.is_some()));

    // Batch delete
    backend.delete_results_batch(&task_ids).await.unwrap();

    // Verify deletion
    let after_delete = backend.get_results_batch(&task_ids).await.unwrap();
    assert!(after_delete.iter().all(|r| r.is_none()));
}

#[tokio::test]
#[ignore]
async fn test_integration_progress_tracking() {
    let mut backend = RedisResultBackend::new("redis://localhost:6379").unwrap();
    let task_id = Uuid::new_v4();
    let mut meta = TaskMeta::new(task_id, "progress_test".to_string());
    meta.result = TaskResult::Started;

    // Store initial state
    backend.store_result(task_id, &meta).await.unwrap();

    // Update progress multiple times
    for i in (0..=100).step_by(25) {
        let progress = ProgressInfo::new(i, 100).with_message(format!("Processing {}%", i));
        meta.progress = Some(progress);
        backend.store_result(task_id, &meta).await.unwrap();

        // Retrieve and verify
        let progress = backend.get_progress(task_id).await.unwrap();
        assert!(progress.is_some());
        let progress = progress.unwrap();
        assert_eq!(progress.current, i);
        assert_eq!(progress.total, 100);
    }

    // Cleanup
    backend.delete_result(task_id).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_integration_cache_performance() {
    let mut backend = RedisResultBackend::new("redis://localhost:6379")
        .unwrap()
        .with_cache(cache::ResultCache::new(cache::CacheConfig {
            enabled: true,
            capacity: 100,
            ttl: Duration::from_secs(60),
        }));

    let task_id = Uuid::new_v4();
    let mut meta = TaskMeta::new(task_id, "cache_test".to_string());
    meta.result = TaskResult::Success(serde_json::json!({"cached": true}));

    // Store result (also caches it)
    backend.store_result(task_id, &meta).await.unwrap();

    // First retrieval (may come from Redis)
    let start = std::time::Instant::now();
    let _ = backend.get_result(task_id).await.unwrap();
    let first_duration = start.elapsed();

    // Second retrieval (should come from cache - much faster)
    let start = std::time::Instant::now();
    let _ = backend.get_result(task_id).await.unwrap();
    let cached_duration = start.elapsed();

    // Cache should be faster
    println!("First: {:?}, Cached: {:?}", first_duration, cached_duration);
    // Note: This assertion might be flaky, but cached should generally be faster
    assert!(cached_duration <= first_duration);

    // Cleanup
    backend.delete_result(task_id).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_integration_connection_failure() {
    // Try to connect to non-existent Redis instance
    let result = RedisResultBackend::new("redis://localhost:9999");

    // Should fail to connect
    assert!(result.is_err());
    if let Err(e) = result {
        assert!(e.is_connection() || e.is_redis());
    }
}

#[tokio::test]
#[ignore]
async fn test_integration_result_versioning() {
    let mut backend = RedisResultBackend::new("redis://localhost:6379").unwrap();
    let task_id = Uuid::new_v4();

    // Store version 1
    let mut meta_v1 = TaskMeta::new(task_id, "version_test".to_string());
    meta_v1.result = TaskResult::Started;
    meta_v1.version = 1;
    let v1 = backend
        .store_versioned_result(task_id, &meta_v1)
        .await
        .unwrap();
    assert_eq!(v1, 1);

    // Store version 2
    meta_v1.result = TaskResult::Success(serde_json::json!({"progress": 50}));
    meta_v1.version = 2;
    let v2 = backend
        .store_versioned_result(task_id, &meta_v1)
        .await
        .unwrap();
    assert_eq!(v2, 2);

    // Store version 3
    meta_v1.result = TaskResult::Success(serde_json::json!({"final": "result"}));
    meta_v1.version = 3;
    let v3 = backend
        .store_versioned_result(task_id, &meta_v1)
        .await
        .unwrap();
    assert_eq!(v3, 3);

    // Retrieve specific versions
    let retrieved_v1 = backend.get_result_version(task_id, 1).await.unwrap();
    assert!(retrieved_v1.is_some());
    assert_eq!(retrieved_v1.unwrap().version, 1);

    let retrieved_v3 = backend.get_result_version(task_id, 3).await.unwrap();
    assert!(retrieved_v3.is_some());
    assert_eq!(retrieved_v3.unwrap().version, 3);

    // Cleanup
    backend.delete_result(task_id).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_integration_streaming() {
    let mut backend = RedisResultBackend::new("redis://localhost:6379").unwrap();
    let task_ids: Vec<Uuid> = (0..20).map(|_| Uuid::new_v4()).collect();

    // Store results
    for (i, &task_id) in task_ids.iter().enumerate() {
        let mut meta = TaskMeta::new(task_id, format!("stream_task_{}", i));
        meta.result = TaskResult::Success(serde_json::json!({"index": i}));
        backend.store_result(task_id, &meta).await.unwrap();
    }

    // Stream results
    let mut stream = backend.stream_results(task_ids.clone(), 5);
    let mut count = 0;

    use futures_util::StreamExt;
    while let Some(result) = stream.next().await {
        if let Ok((_id, meta_opt)) = result {
            if meta_opt.is_some() {
                count += 1;
            }
        }
    }

    assert_eq!(count, task_ids.len());

    // Cleanup
    backend.delete_results_batch(&task_ids).await.unwrap();
}

#[test]
fn test_backend_stats_display() {
    let stats = BackendStats {
        task_key_count: 100,
        chord_key_count: 10,
        total_keys: 110,
        used_memory_bytes: 1024 * 1024 * 5, // 5 MB
    };

    let display = format!("{}", stats);
    assert!(display.contains("100 task keys"));
    assert!(display.contains("10 chord keys"));
    assert!(display.contains("5.00 MB"));
}

#[tokio::test]
#[ignore]
async fn test_integration_health_check() {
    let mut backend = RedisResultBackend::new("redis://localhost:6379").unwrap();
    let is_healthy = backend.health_check().await.unwrap();
    assert!(is_healthy);
}

#[tokio::test]
#[ignore]
async fn test_integration_get_stats() {
    let mut backend = RedisResultBackend::new("redis://localhost:6379").unwrap();

    // Store some test data
    let task_id = Uuid::new_v4();
    let mut meta = TaskMeta::new(task_id, "stats_test".to_string());
    meta.result = TaskResult::Success(serde_json::json!({"test": true}));
    backend.store_result(task_id, &meta).await.unwrap();

    // Get stats
    let stats = backend.get_stats().await.unwrap();
    assert!(stats.task_key_count > 0);
    assert!(stats.total_keys > 0);

    // Cleanup
    backend.delete_result(task_id).await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_integration_cleanup_old_results() {
    let mut backend = RedisResultBackend::new("redis://localhost:6379").unwrap();

    // Store a recent result
    let task_id = Uuid::new_v4();
    let mut meta = TaskMeta::new(task_id, "recent_task".to_string());
    meta.result = TaskResult::Success(serde_json::json!({"data": 1}));
    backend.store_result(task_id, &meta).await.unwrap();

    // Try to cleanup results older than 1 day (should not delete recent one)
    let deleted = backend
        .cleanup_old_results(Duration::from_secs(86400))
        .await
        .unwrap();

    // The recent task should not be deleted
    let result = backend.get_result(task_id).await.unwrap();
    assert!(result.is_some());

    // Cleanup
    backend.delete_result(task_id).await.unwrap();

    // Note: We can't easily test deletion of old results without
    // manipulating timestamps or waiting, so we just verify the method works
    // (deleted count is always >= 0 for usize, so no need to assert)
    let _ = deleted; // Suppress unused variable warning
}

#[tokio::test]
#[ignore]
async fn test_integration_cleanup_completed_chords() {
    let mut backend = RedisResultBackend::new("redis://localhost:6379").unwrap();

    // Create a completed chord
    let chord_id = Uuid::new_v4();
    let mut state = ChordState::new(chord_id, 2, vec![]);
    state.completed = 2; // Mark as complete
    backend.chord_init(state).await.unwrap();

    // Cleanup completed chords
    let deleted = backend.cleanup_completed_chords().await.unwrap();

    // The completed chord should be deleted
    assert!(deleted >= 1);

    // Verify chord is gone
    let result = backend.chord_get_state(chord_id).await.unwrap();
    assert!(result.is_none());
}

#[test]
fn test_task_summary_creation() {
    let summary = TaskSummary {
        total: 10,
        found: 8,
        not_found: 2,
        pending: 2,
        started: 1,
        success: 4,
        failure: 1,
        retry: 0,
        revoked: 0,
    };

    assert_eq!(summary.total, 10);
    assert_eq!(summary.success, 4);
    assert_eq!(summary.failure, 1);
}

#[test]
fn test_task_summary_rates() {
    let summary = TaskSummary {
        total: 10,
        found: 10,
        not_found: 0,
        pending: 0,
        started: 0,
        success: 7,
        failure: 2,
        retry: 0,
        revoked: 1,
    };

    assert_eq!(summary.completion_rate(), 1.0);
    assert_eq!(summary.success_rate(), 0.7);
    assert_eq!(summary.failure_rate(), 0.2);
    assert!(summary.all_complete());
    assert!(summary.has_failures());
}

#[test]
fn test_task_summary_edge_cases() {
    let empty = TaskSummary {
        total: 0,
        found: 0,
        not_found: 0,
        pending: 0,
        started: 0,
        success: 0,
        failure: 0,
        retry: 0,
        revoked: 0,
    };

    assert_eq!(empty.completion_rate(), 0.0);
    assert_eq!(empty.success_rate(), 0.0);
    assert_eq!(empty.failure_rate(), 0.0);
    assert!(empty.all_complete());
    assert!(!empty.has_failures());
}

#[test]
fn test_task_summary_display() {
    let summary = TaskSummary {
        total: 5,
        found: 5,
        not_found: 0,
        pending: 1,
        started: 0,
        success: 3,
        failure: 1,
        retry: 0,
        revoked: 0,
    };

    let display = format!("{}", summary);
    assert!(display.contains("Total: 5"));
    assert!(display.contains("Success: 3"));
    assert!(display.contains("Failure: 1"));
}

#[test]
fn test_state_count_creation() {
    let counts = StateCount {
        total: 20,
        pending: 3,
        started: 2,
        success: 10,
        failure: 3,
        retry: 1,
        revoked: 1,
        not_found: 0,
    };

    assert_eq!(counts.total, 20);
    assert_eq!(counts.success, 10);
    assert_eq!(counts.failure, 3);
}

#[test]
fn test_state_count_percentages() {
    let counts = StateCount {
        total: 10,
        pending: 1,
        started: 1,
        success: 5,
        failure: 2,
        retry: 1,
        revoked: 0,
        not_found: 0,
    };

    let percentages = counts.percentages();
    assert_eq!(percentages.success, 50.0);
    assert_eq!(percentages.failure, 20.0);
    assert_eq!(percentages.pending, 10.0);
}

#[test]
fn test_state_count_percentages_zero_total() {
    let counts = StateCount {
        total: 0,
        pending: 0,
        started: 0,
        success: 0,
        failure: 0,
        retry: 0,
        revoked: 0,
        not_found: 0,
    };

    let percentages = counts.percentages();
    assert_eq!(percentages.success, 0.0);
    assert_eq!(percentages.failure, 0.0);
}

#[test]
fn test_state_count_display() {
    let counts = StateCount {
        total: 10,
        pending: 2,
        started: 1,
        success: 5,
        failure: 1,
        retry: 1,
        revoked: 0,
        not_found: 0,
    };

    let display = format!("{}", counts);
    assert!(display.contains("Total: 10"));
    assert!(display.contains("Success: 5"));
    assert!(display.contains("Pending: 2"));
}

#[test]
fn test_state_percentages_display() {
    let percentages = StatePercentages {
        pending: 10.0,
        started: 10.0,
        success: 50.0,
        failure: 20.0,
        retry: 10.0,
        revoked: 0.0,
        not_found: 0.0,
    };

    let display = format!("{}", percentages);
    assert!(display.contains("Success: 50.0%"));
    assert!(display.contains("Failure: 20.0%"));
}

#[test]
fn test_state_percentages_default() {
    let percentages = StatePercentages::default();
    assert_eq!(percentages.pending, 0.0);
    assert_eq!(percentages.success, 0.0);
    assert_eq!(percentages.failure, 0.0);
}

#[test]
fn test_task_meta_tags() {
    let mut meta = TaskMeta::new(Uuid::new_v4(), "test_task".to_string());

    // Initially no tags
    assert!(meta.tags.is_empty());
    assert!(!meta.has_tag("tag1"));

    // Add tags
    meta.add_tag("tag1");
    meta.add_tag("tag2");
    assert_eq!(meta.tags.len(), 2);
    assert!(meta.has_tag("tag1"));
    assert!(meta.has_tag("tag2"));

    // Duplicate tags should not be added
    meta.add_tag("tag1");
    assert_eq!(meta.tags.len(), 2);

    // Check any/all tags
    assert!(meta.has_any_tag(&["tag1".to_string()]));
    assert!(meta.has_any_tag(&["tag3".to_string(), "tag1".to_string()]));
    assert!(!meta.has_any_tag(&["tag3".to_string(), "tag4".to_string()]));

    assert!(meta.has_all_tags(&["tag1".to_string(), "tag2".to_string()]));
    assert!(!meta.has_all_tags(&["tag1".to_string(), "tag3".to_string()]));

    // Remove tags
    meta.remove_tag("tag1");
    assert_eq!(meta.tags.len(), 1);
    assert!(!meta.has_tag("tag1"));
    assert!(meta.has_tag("tag2"));
}

#[test]
fn test_task_meta_metadata() {
    let mut meta = TaskMeta::new(Uuid::new_v4(), "test_task".to_string());

    // Initially no metadata
    assert!(meta.metadata.is_empty());
    assert!(!meta.has_metadata("key1"));

    // Set metadata
    meta.set_metadata("key1", serde_json::json!("value1"));
    meta.set_metadata("key2", serde_json::json!(42));
    assert_eq!(meta.metadata.len(), 2);
    assert!(meta.has_metadata("key1"));
    assert!(meta.has_metadata("key2"));

    // Get metadata
    assert_eq!(
        meta.get_metadata("key1"),
        Some(&serde_json::json!("value1"))
    );
    assert_eq!(meta.get_metadata("key2"), Some(&serde_json::json!(42)));
    assert_eq!(meta.get_metadata("key3"), None);

    // Remove metadata
    let removed = meta.remove_metadata("key1");
    assert_eq!(removed, Some(serde_json::json!("value1")));
    assert_eq!(meta.metadata.len(), 1);
    assert!(!meta.has_metadata("key1"));
    assert!(meta.has_metadata("key2"));
}

#[test]
fn test_task_query_with_tags() {
    let mut meta = TaskMeta::new(Uuid::new_v4(), "test_task".to_string());
    meta.add_tag("production");
    meta.add_tag("high-priority");

    // Query with matching tags
    let query = TaskQuery::new().with_tag("production");
    assert!(query.matches(&meta));

    // Query with multiple matching tags
    let query =
        TaskQuery::new().with_tags(vec!["production".to_string(), "high-priority".to_string()]);
    assert!(query.matches(&meta));

    // Query with non-matching tag
    let query = TaskQuery::new().with_tag("low-priority");
    assert!(!query.matches(&meta));

    // Query with partial match (should fail as it requires all tags)
    let query =
        TaskQuery::new().with_tags(vec!["production".to_string(), "experimental".to_string()]);
    assert!(!query.matches(&meta));
}

#[test]
fn test_task_query_with_metadata() {
    let mut meta = TaskMeta::new(Uuid::new_v4(), "test_task".to_string());
    meta.set_metadata("environment", serde_json::json!("production"));
    meta.set_metadata("priority", serde_json::json!(5));

    // Query with matching metadata
    let query = TaskQuery::new().with_metadata("environment", serde_json::json!("production"));
    assert!(query.matches(&meta));

    // Query with multiple matching metadata fields
    let mut query = TaskQuery::new();
    query = query.with_metadata("environment", serde_json::json!("production"));
    query = query.with_metadata("priority", serde_json::json!(5));
    assert!(query.matches(&meta));

    // Query with non-matching metadata value
    let query = TaskQuery::new().with_metadata("environment", serde_json::json!("development"));
    assert!(!query.matches(&meta));

    // Query with non-existent metadata key
    let query = TaskQuery::new().with_metadata("nonexistent", serde_json::json!("value"));
    assert!(!query.matches(&meta));
}

#[test]
fn test_task_query_combined_criteria() {
    let mut meta = TaskMeta::new(Uuid::new_v4(), "test_task".to_string());
    meta.result = TaskResult::Success(serde_json::json!({"result": "ok"}));
    meta.worker = Some("worker-1".to_string());
    meta.add_tag("production");
    meta.set_metadata("environment", serde_json::json!("prod"));

    // Query with all matching criteria
    let query = TaskQuery::new()
        .with_state(TaskResult::Success(serde_json::json!(null)))
        .with_worker("worker-1")
        .with_tag("production")
        .with_metadata("environment", serde_json::json!("prod"));
    assert!(query.matches(&meta));

    // Query with one non-matching criterion
    let query = TaskQuery::new()
        .with_state(TaskResult::Success(serde_json::json!(null)))
        .with_worker("worker-2") // Different worker
        .with_tag("production")
        .with_metadata("environment", serde_json::json!("prod"));
    assert!(!query.matches(&meta));
}

#[test]
fn test_task_meta_serialization_with_tags_and_metadata() {
    let mut meta = TaskMeta::new(Uuid::new_v4(), "test_task".to_string());
    meta.add_tag("tag1");
    meta.add_tag("tag2");
    meta.set_metadata("key1", serde_json::json!("value1"));
    meta.set_metadata("key2", serde_json::json!(42));

    // Serialize
    let json = serde_json::to_string(&meta).unwrap();

    // Deserialize
    let deserialized: TaskMeta = serde_json::from_str(&json).unwrap();

    // Verify tags
    assert_eq!(deserialized.tags.len(), 2);
    assert!(deserialized.has_tag("tag1"));
    assert!(deserialized.has_tag("tag2"));

    // Verify metadata
    assert_eq!(deserialized.metadata.len(), 2);
    assert_eq!(
        deserialized.get_metadata("key1"),
        Some(&serde_json::json!("value1"))
    );
    assert_eq!(
        deserialized.get_metadata("key2"),
        Some(&serde_json::json!(42))
    );
}

#[test]
fn test_batch_operation_result() {
    let mut result = BatchOperationResult::new();

    // Initially empty
    assert_eq!(result.total, 0);
    assert_eq!(result.successful, 0);
    assert_eq!(result.failed, 0);
    assert!(!result.all_succeeded());
    assert!(!result.has_failures());

    // Record successes
    result.record_success(Uuid::new_v4());
    result.record_success(Uuid::new_v4());
    assert_eq!(result.total, 2);
    assert_eq!(result.successful, 2);
    assert_eq!(result.failed, 0);
    assert!(result.all_succeeded());
    assert!(!result.has_failures());

    // Record failure
    result.record_failure(Uuid::new_v4(), "Connection error".to_string());
    assert_eq!(result.total, 3);
    assert_eq!(result.successful, 2);
    assert_eq!(result.failed, 1);
    assert!(!result.all_succeeded());
    assert!(result.has_failures());

    // Check rates
    assert!((result.success_rate() - 2.0 / 3.0).abs() < 1e-10);
    assert!((result.failure_rate() - 1.0 / 3.0).abs() < 1e-10);
}

#[test]
fn test_batch_operation_result_default() {
    let result = BatchOperationResult::default();
    assert_eq!(result.total, 0);
    assert_eq!(result.successful, 0);
    assert_eq!(result.failed, 0);
}

#[test]
fn test_batch_operation_result_display() {
    let mut result = BatchOperationResult::new();
    result.record_success(Uuid::new_v4());
    result.record_success(Uuid::new_v4());
    result.record_failure(Uuid::new_v4(), "Error 1".to_string());

    let display = format!("{}", result);
    assert!(display.contains("Total: 3"));
    assert!(display.contains("Successful: 2"));
    assert!(display.contains("Failed: 1"));
    assert!(display.contains("66.7%")); // Success rate
}

#[test]
fn test_batch_operation_result_edge_cases() {
    let mut result = BatchOperationResult::new();

    // Empty result
    assert_eq!(result.success_rate(), 0.0);
    assert_eq!(result.failure_rate(), 1.0);
    assert!(!result.all_succeeded()); // Empty batch is not considered successful

    // All failed
    result.record_failure(Uuid::new_v4(), "Error".to_string());
    assert_eq!(result.success_rate(), 0.0);
    assert_eq!(result.failure_rate(), 1.0);
    assert!(result.has_failures());
}

// --- Tests for new TaskMeta fields ---

#[test]
fn test_task_meta_new_fields_default() {
    let task_id = Uuid::new_v4();
    let meta = TaskMeta::new(task_id, "test_task".to_string());

    assert!(meta.worker_hostname.is_none());
    assert!(meta.runtime_ms.is_none());
    assert!(meta.memory_bytes.is_none());
    assert!(meta.retries.is_none());
    assert!(meta.queue.is_none());
}

#[test]
fn test_task_meta_new_fields_serialize_deserialize() {
    let task_id = Uuid::new_v4();
    let mut meta = TaskMeta::new(task_id, "test_task".to_string());
    meta.worker_hostname = Some("worker-01.example.com".to_string());
    meta.runtime_ms = Some(1500);
    meta.memory_bytes = Some(1024 * 1024 * 50); // 50MB
    meta.retries = Some(2);
    meta.queue = Some("high-priority".to_string());
    meta.result = TaskResult::Success(serde_json::json!({"value": 42}));

    // Serialize
    let json = serde_json::to_string(&meta).expect("serialize failed");

    // Deserialize
    let deserialized: TaskMeta = serde_json::from_str(&json).expect("deserialize failed");

    assert_eq!(deserialized.task_id, task_id);
    assert_eq!(deserialized.task_name, "test_task");
    assert_eq!(
        deserialized.worker_hostname,
        Some("worker-01.example.com".to_string())
    );
    assert_eq!(deserialized.runtime_ms, Some(1500));
    assert_eq!(deserialized.memory_bytes, Some(1024 * 1024 * 50));
    assert_eq!(deserialized.retries, Some(2));
    assert_eq!(deserialized.queue, Some("high-priority".to_string()));
    assert!(deserialized.result.is_success());
}

#[test]
fn test_task_meta_new_fields_skip_none_serialization() {
    let task_id = Uuid::new_v4();
    let meta = TaskMeta::new(task_id, "test_task".to_string());

    let json = serde_json::to_string(&meta).expect("serialize failed");

    // None fields should be skipped
    assert!(!json.contains("worker_hostname"));
    assert!(!json.contains("runtime_ms"));
    assert!(!json.contains("memory_bytes"));
    assert!(!json.contains("retries"));
    assert!(!json.contains("queue"));
}

#[test]
fn test_task_meta_backward_compatible_deserialize() {
    // Simulate old JSON without new fields
    let old_json = r#"{
        "task_id": "00000000-0000-0000-0000-000000000001",
        "task_name": "old_task",
        "result": "Pending",
        "created_at": "2025-01-01T00:00:00Z",
        "started_at": null,
        "completed_at": null,
        "worker": null,
        "version": 0
    }"#;

    let meta: TaskMeta =
        serde_json::from_str(old_json).expect("backward compat deserialize failed");
    assert_eq!(meta.task_name, "old_task");
    assert!(meta.worker_hostname.is_none());
    assert!(meta.runtime_ms.is_none());
    assert!(meta.memory_bytes.is_none());
    assert!(meta.retries.is_none());
    assert!(meta.queue.is_none());
}

// --- Tests for TaskTtlConfig ---

#[test]
fn test_task_ttl_config_new() {
    let config = TaskTtlConfig::new();
    assert!(config.is_empty());
    assert!(config.default_ttl().is_none());
    assert_eq!(config.task_ttl_count(), 0);
}

#[test]
fn test_task_ttl_config_with_default() {
    let config = TaskTtlConfig::with_default(Duration::from_secs(3600));
    assert!(!config.is_empty());
    assert_eq!(config.default_ttl(), Some(Duration::from_secs(3600)));
}

#[test]
fn test_task_ttl_config_per_task() {
    let mut config = TaskTtlConfig::with_default(Duration::from_secs(3600));
    config.set_task_ttl("long_task", Duration::from_secs(86400));
    config.set_task_ttl("short_task", Duration::from_secs(60));

    // Per-task overrides
    assert_eq!(
        config.get_ttl("long_task"),
        Some(Duration::from_secs(86400))
    );
    assert_eq!(config.get_ttl("short_task"), Some(Duration::from_secs(60)));

    // Fallback to default
    assert_eq!(
        config.get_ttl("unknown_task"),
        Some(Duration::from_secs(3600))
    );

    assert_eq!(config.task_ttl_count(), 2);
}

#[test]
fn test_task_ttl_config_no_default_no_match() {
    let mut config = TaskTtlConfig::new();
    config.set_task_ttl("specific_task", Duration::from_secs(300));

    // No default, no match => None
    assert!(config.get_ttl("other_task").is_none());

    // Specific match works
    assert_eq!(
        config.get_ttl("specific_task"),
        Some(Duration::from_secs(300))
    );
}

#[test]
fn test_task_ttl_config_remove() {
    let mut config = TaskTtlConfig::with_default(Duration::from_secs(3600));
    config.set_task_ttl("task_a", Duration::from_secs(100));

    assert_eq!(
        config.remove_task_ttl("task_a"),
        Some(Duration::from_secs(100))
    );
    assert!(config.remove_task_ttl("task_a").is_none());

    // Falls back to default now
    assert_eq!(config.get_ttl("task_a"), Some(Duration::from_secs(3600)));
}

#[test]
fn test_task_ttl_config_set_default() {
    let mut config = TaskTtlConfig::new();
    assert!(config.default_ttl().is_none());

    config.set_default_ttl(Duration::from_secs(7200));
    assert_eq!(config.default_ttl(), Some(Duration::from_secs(7200)));
}

// --- Chunking integration tests (unit-level, no Redis required) ---

#[test]
fn test_chunking_integration_roundtrip() {
    // Verify that split_chunks + create_sentinel + parse_sentinel + reassemble_chunks
    // produces correct data for payloads above threshold
    let config = chunking::ChunkingConfig::new()
        .with_threshold(100)
        .with_chunk_size(64)
        .with_checksum(true);
    let chunker = chunking::ResultChunker::new(config);

    // Create data larger than threshold
    let data: Vec<u8> = (0..300u16).map(|i| (i % 256) as u8).collect();
    assert!(chunker.needs_chunking(&data));

    let (metadata, chunks) = chunker.split_chunks(&data);
    let sentinel = chunker.create_sentinel(&metadata);

    // Sentinel must be recognized as chunked
    assert!(chunking::ResultChunker::is_chunked(&sentinel));

    // Parse sentinel back to metadata
    let parsed_metadata =
        chunking::ResultChunker::parse_sentinel(&sentinel).expect("parse_sentinel should succeed");
    assert_eq!(parsed_metadata.total_chunks, metadata.total_chunks);
    assert_eq!(parsed_metadata.total_size, metadata.total_size);
    assert_eq!(parsed_metadata.checksum, metadata.checksum);

    // Reassemble must yield original data
    let reassembled = chunker
        .reassemble_chunks(&parsed_metadata, &chunks)
        .expect("reassemble should succeed");
    assert_eq!(reassembled, data);

    // Verify chunk keys generation
    let base_key = "celery-task-meta-test-uuid";
    let chunk_keys = chunking::ResultChunker::chunk_keys(base_key, metadata.total_chunks);
    assert_eq!(chunk_keys.len(), metadata.total_chunks);
    for (i, ck) in chunk_keys.iter().enumerate() {
        assert_eq!(*ck, format!("{}:chunk:{}", base_key, i));
    }

    // Verify metadata key
    let meta_key = chunking::ResultChunker::metadata_key(base_key);
    assert_eq!(meta_key, format!("{}:chunks", base_key));
}

#[test]
fn test_chunking_not_triggered_for_small_data() {
    let config = chunking::ChunkingConfig::new()
        .with_threshold(512)
        .with_chunk_size(256);
    let chunker = chunking::ResultChunker::new(config);

    // Data below threshold should not need chunking
    let small_data = vec![0u8; 100];
    assert!(!chunker.needs_chunking(&small_data));

    // Exactly at threshold should not trigger
    let exact_data = vec![0u8; 512];
    assert!(!chunker.needs_chunking(&exact_data));

    // Non-chunked data should not be detected as sentinel
    assert!(!chunking::ResultChunker::is_chunked(&small_data));
    assert!(!chunking::ResultChunker::is_chunked(&exact_data));
}

#[test]
fn test_chunking_delete_cleans_chunks() {
    // Verify that metadata can be serialized/deserialized correctly
    // (simulates what delete_result does: deserialize metadata to find chunk count)
    let config = chunking::ChunkingConfig::new()
        .with_threshold(50)
        .with_chunk_size(32);
    let chunker = chunking::ResultChunker::new(config);

    let data = vec![0xABu8; 200];
    let (metadata, chunks) = chunker.split_chunks(&data);

    // Serialize metadata as JSON (same as store_result does)
    let meta_json = serde_json::to_vec(&metadata).expect("metadata serialization should succeed");

    // Deserialize it back (same as delete_result does)
    let recovered: chunking::ChunkMetadata =
        serde_json::from_slice(&meta_json).expect("metadata deserialization should succeed");

    assert_eq!(recovered.total_chunks, chunks.len());
    assert_eq!(recovered.total_size, data.len());

    // Verify we can generate correct keys for cleanup
    let base_key = "celery-task-meta-cleanup-test";
    let chunk_keys = chunking::ResultChunker::chunk_keys(base_key, recovered.total_chunks);
    assert_eq!(chunk_keys.len(), recovered.total_chunks);

    let meta_key = chunking::ResultChunker::metadata_key(base_key);
    assert!(meta_key.ends_with(":chunks"));
}
