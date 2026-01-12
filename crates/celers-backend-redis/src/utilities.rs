//! Utility functions and helpers for Redis backend
//!
//! This module provides convenient builder patterns, helper functions,
//! and utilities to simplify common operations with the Redis backend.

use crate::{
    BackendError, ChordState, ProgressInfo, RedisResultBackend, ResultBackend, TaskMeta, TaskResult,
};
use chrono::Utc;
use std::time::Duration;
use uuid::Uuid;

/// Builder for creating RedisResultBackend with a fluent API
///
/// # Example
/// ```no_run
/// use celers_backend_redis::utilities::BackendBuilder;
/// use celers_backend_redis::{ttl, batch_size};
///
/// let backend = BackendBuilder::new("redis://localhost")
///     .with_cache(1000, ttl::MEDIUM)
///     .with_compression(batch_size::SMALL, 6)
///     .with_key_prefix("myapp")
///     .build()
///     .unwrap();
/// ```
pub struct BackendBuilder {
    url: String,
    cache_capacity: Option<usize>,
    cache_ttl: Option<Duration>,
    compression_threshold: Option<usize>,
    compression_level: Option<u32>,
    key_prefix: Option<String>,
    encryption_key: Option<crate::encryption::EncryptionKey>,
}

impl BackendBuilder {
    /// Create a new builder with the Redis URL
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            cache_capacity: None,
            cache_ttl: None,
            compression_threshold: None,
            compression_level: None,
            key_prefix: None,
            encryption_key: None,
        }
    }

    /// Enable caching with specified capacity and TTL
    pub fn with_cache(mut self, capacity: usize, ttl: Duration) -> Self {
        self.cache_capacity = Some(capacity);
        self.cache_ttl = Some(ttl);
        self
    }

    /// Enable compression with specified threshold and level
    pub fn with_compression(mut self, threshold: usize, level: u32) -> Self {
        self.compression_threshold = Some(threshold);
        self.compression_level = Some(level);
        self
    }

    /// Enable compression with a configuration object
    pub fn with_compression_config(
        mut self,
        config: crate::compression::CompressionConfig,
    ) -> Self {
        if config.enabled {
            self.compression_threshold = Some(config.threshold);
            self.compression_level = Some(config.level);
        }
        self
    }

    /// Set a custom key prefix
    pub fn with_key_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.key_prefix = Some(prefix.into());
        self
    }

    /// Enable encryption with the specified key
    pub fn with_encryption(mut self, key: crate::encryption::EncryptionKey) -> Self {
        self.encryption_key = Some(key);
        self
    }

    /// Build the RedisResultBackend
    pub fn build(self) -> Result<RedisResultBackend, BackendError> {
        let mut backend = RedisResultBackend::new(&self.url)?;

        if let Some(prefix) = self.key_prefix {
            backend = backend.with_prefix(prefix);
        }

        if let (Some(capacity), Some(ttl)) = (self.cache_capacity, self.cache_ttl) {
            let cache = crate::cache::ResultCache::new(
                crate::cache::CacheConfig::new()
                    .with_capacity(capacity)
                    .with_ttl(ttl),
            );
            backend = backend.with_cache(cache);
        }

        if let (Some(threshold), Some(level)) = (self.compression_threshold, self.compression_level)
        {
            let config = crate::compression::CompressionConfig::new()
                .with_threshold(threshold)
                .with_level(level);
            backend = backend.with_compression(config);
        }

        if let Some(key) = self.encryption_key {
            let config = crate::encryption::EncryptionConfig::new(key);
            backend = backend.with_encryption(config);
        }

        Ok(backend)
    }
}

/// Builder for creating TaskMeta with a fluent API
///
/// # Example
/// ```
/// use celers_backend_redis::utilities::TaskMetaBuilder;
/// use celers_backend_redis::TaskResult;
/// use uuid::Uuid;
///
/// let meta = TaskMetaBuilder::new(Uuid::new_v4(), "process_data")
///     .with_worker("worker-1")
///     .with_result(TaskResult::Success(serde_json::json!({"count": 42})))
///     .mark_started()
///     .build();
/// ```
pub struct TaskMetaBuilder {
    meta: TaskMeta,
}

impl TaskMetaBuilder {
    /// Create a new builder with task ID and name
    pub fn new(task_id: Uuid, task_name: impl Into<String>) -> Self {
        Self {
            meta: TaskMeta::new(task_id, task_name.into()),
        }
    }

    /// Set the task result
    pub fn with_result(mut self, result: TaskResult) -> Self {
        self.meta.result = result;
        self
    }

    /// Set the worker ID
    pub fn with_worker(mut self, worker: impl Into<String>) -> Self {
        self.meta.worker = Some(worker.into());
        self
    }

    /// Mark the task as started (sets started_at to now)
    pub fn mark_started(mut self) -> Self {
        self.meta.started_at = Some(Utc::now());
        self
    }

    /// Mark the task as completed (sets completed_at to now)
    pub fn mark_completed(mut self) -> Self {
        self.meta.completed_at = Some(Utc::now());
        self
    }

    /// Set the progress information
    pub fn with_progress(mut self, progress: ProgressInfo) -> Self {
        self.meta.progress = Some(progress);
        self
    }

    /// Set the version
    pub fn with_version(mut self, version: u32) -> Self {
        self.meta.version = version;
        self
    }

    /// Build the TaskMeta
    pub fn build(self) -> TaskMeta {
        self.meta
    }
}

/// Builder for creating ChordState with a fluent API
///
/// # Example
/// ```
/// use celers_backend_redis::utilities::ChordBuilder;
/// use uuid::Uuid;
/// use std::time::Duration;
///
/// let chord = ChordBuilder::new(Uuid::new_v4(), 10)
///     .with_callback("aggregate_results")
///     .with_timeout(Duration::from_secs(300))
///     .with_task_ids(vec![Uuid::new_v4(), Uuid::new_v4()])
///     .build();
/// ```
pub struct ChordBuilder {
    state: ChordState,
}

impl ChordBuilder {
    /// Create a new builder with chord ID and total tasks
    pub fn new(chord_id: Uuid, total: usize) -> Self {
        Self {
            state: ChordState {
                chord_id,
                total,
                completed: 0,
                callback: None,
                task_ids: Vec::new(),
                created_at: Utc::now(),
                timeout: None,
                cancelled: false,
                cancellation_reason: None,
                max_retries: None,
                retry_count: 0,
            },
        }
    }

    /// Set the callback task name
    pub fn with_callback(mut self, callback: impl Into<String>) -> Self {
        self.state.callback = Some(callback.into());
        self
    }

    /// Set the timeout duration
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.state.timeout = Some(timeout);
        self
    }

    /// Set the maximum retries
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.state.max_retries = Some(max_retries);
        self
    }

    /// Set the task IDs in the chord
    pub fn with_task_ids(mut self, task_ids: Vec<Uuid>) -> Self {
        self.state.task_ids = task_ids;
        self
    }

    /// Add a task ID to the chord
    pub fn add_task_id(mut self, task_id: Uuid) -> Self {
        self.state.task_ids.push(task_id);
        self
    }

    /// Build the ChordState
    pub fn build(self) -> ChordState {
        self.state
    }
}

/// Utility functions for common Redis backend operations
pub struct BackendUtils;

impl BackendUtils {
    /// Create a pending task result
    pub fn pending_task(task_id: Uuid, task_name: impl Into<String>) -> TaskMeta {
        TaskMetaBuilder::new(task_id, task_name)
            .with_result(TaskResult::Pending)
            .build()
    }

    /// Create a started task result
    pub fn started_task(
        task_id: Uuid,
        task_name: impl Into<String>,
        worker: impl Into<String>,
    ) -> TaskMeta {
        TaskMetaBuilder::new(task_id, task_name)
            .with_result(TaskResult::Started)
            .with_worker(worker)
            .mark_started()
            .build()
    }

    /// Create a successful task result
    pub fn success_task(
        task_id: Uuid,
        task_name: impl Into<String>,
        result: serde_json::Value,
    ) -> TaskMeta {
        TaskMetaBuilder::new(task_id, task_name)
            .with_result(TaskResult::Success(result))
            .mark_started()
            .mark_completed()
            .build()
    }

    /// Create a failed task result
    pub fn failed_task(
        task_id: Uuid,
        task_name: impl Into<String>,
        error: impl Into<String>,
    ) -> TaskMeta {
        TaskMetaBuilder::new(task_id, task_name)
            .with_result(TaskResult::Failure(error.into()))
            .mark_started()
            .mark_completed()
            .build()
    }

    /// Create a retry task result
    pub fn retry_task(task_id: Uuid, task_name: impl Into<String>, retry_count: u32) -> TaskMeta {
        TaskMetaBuilder::new(task_id, task_name)
            .with_result(TaskResult::Retry(retry_count))
            .mark_started()
            .build()
    }

    /// Create a revoked task result
    pub fn revoked_task(task_id: Uuid, task_name: impl Into<String>) -> TaskMeta {
        TaskMetaBuilder::new(task_id, task_name)
            .with_result(TaskResult::Revoked)
            .mark_completed()
            .build()
    }

    /// Bulk store tasks with the same state
    pub async fn bulk_store_with_state(
        backend: &mut RedisResultBackend,
        task_ids: &[Uuid],
        task_name: impl Into<String>,
        result: TaskResult,
    ) -> Result<(), BackendError> {
        let name = task_name.into();
        let tasks: Vec<(Uuid, TaskMeta)> = task_ids
            .iter()
            .map(|&id| {
                let mut meta = TaskMeta::new(id, name.clone());
                meta.result = result.clone();
                (id, meta)
            })
            .collect();

        backend.store_results_batch(&tasks).await
    }

    /// Check if any tasks in a list are complete
    pub async fn any_complete(
        backend: &mut RedisResultBackend,
        task_ids: &[Uuid],
    ) -> Result<bool, BackendError> {
        for &task_id in task_ids {
            if backend.is_task_complete(task_id).await? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Check if all tasks in a list are complete
    pub async fn all_complete(
        backend: &mut RedisResultBackend,
        task_ids: &[Uuid],
    ) -> Result<bool, BackendError> {
        for &task_id in task_ids {
            if !backend.is_task_complete(task_id).await? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Get completed tasks from a list
    pub async fn filter_completed(
        backend: &mut RedisResultBackend,
        task_ids: &[Uuid],
    ) -> Result<Vec<Uuid>, BackendError> {
        let mut completed = Vec::new();
        for &task_id in task_ids {
            if backend.is_task_complete(task_id).await? {
                completed.push(task_id);
            }
        }
        Ok(completed)
    }

    /// Get pending tasks from a list
    pub async fn filter_pending(
        backend: &mut RedisResultBackend,
        task_ids: &[Uuid],
    ) -> Result<Vec<Uuid>, BackendError> {
        let mut pending = Vec::new();
        for &task_id in task_ids {
            if !backend.is_task_complete(task_id).await? {
                pending.push(task_id);
            }
        }
        Ok(pending)
    }

    /// Calculate average task age from a list of tasks
    pub async fn average_task_age(
        backend: &mut RedisResultBackend,
        task_ids: &[Uuid],
    ) -> Result<Option<chrono::Duration>, BackendError> {
        let mut total_ms = 0i64;
        let mut count = 0usize;

        for &task_id in task_ids {
            if let Some(age) = backend.get_task_age(task_id).await? {
                total_ms += age.num_milliseconds();
                count += 1;
            }
        }

        if count == 0 {
            Ok(None)
        } else {
            Ok(Some(chrono::Duration::milliseconds(
                total_ms / count as i64,
            )))
        }
    }
}

/// Progress tracking utilities
pub struct ProgressUtils;

impl ProgressUtils {
    /// Create progress info with percentage and message
    pub fn with_percent(percent: u32, message: impl Into<String>) -> ProgressInfo {
        ProgressInfo::new(percent as u64, 100).with_message(message.into())
    }

    /// Create progress info from completed/total
    pub fn from_counts(completed: u32, total: u32) -> ProgressInfo {
        ProgressInfo::new(completed as u64, total as u64)
    }

    /// Create progress info from completed/total with message
    pub fn from_counts_with_message(
        completed: u32,
        total: u32,
        message: impl Into<String>,
    ) -> ProgressInfo {
        ProgressInfo::new(completed as u64, total as u64).with_message(message.into())
    }

    /// Update task progress with percentage
    pub async fn set_percent(
        backend: &mut RedisResultBackend,
        task_id: Uuid,
        percent: u32,
    ) -> Result<(), BackendError> {
        let progress = Self::with_percent(percent, "");
        backend.set_progress(task_id, progress).await
    }

    /// Update task progress with counts
    pub async fn set_counts(
        backend: &mut RedisResultBackend,
        task_id: Uuid,
        completed: u32,
        total: u32,
    ) -> Result<(), BackendError> {
        let progress = Self::from_counts(completed, total);
        backend.set_progress(task_id, progress).await
    }

    /// Update task progress with counts and message
    pub async fn set_counts_with_message(
        backend: &mut RedisResultBackend,
        task_id: Uuid,
        completed: u32,
        total: u32,
        message: impl Into<String>,
    ) -> Result<(), BackendError> {
        let progress = Self::from_counts_with_message(completed, total, message);
        backend.set_progress(task_id, progress).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backend_builder() {
        let builder = BackendBuilder::new("redis://localhost")
            .with_cache(1000, Duration::from_secs(300))
            .with_compression(1024, 6)
            .with_key_prefix("test");

        assert_eq!(builder.url, "redis://localhost");
        assert_eq!(builder.cache_capacity, Some(1000));
        assert_eq!(builder.cache_ttl, Some(Duration::from_secs(300)));
        assert_eq!(builder.compression_threshold, Some(1024));
        assert_eq!(builder.compression_level, Some(6));
        assert_eq!(builder.key_prefix, Some("test".to_string()));
    }

    #[test]
    fn test_task_meta_builder() {
        let task_id = Uuid::new_v4();
        let meta = TaskMetaBuilder::new(task_id, "test_task")
            .with_worker("worker-1")
            .with_result(TaskResult::Success(serde_json::json!(42)))
            .mark_started()
            .mark_completed()
            .build();

        assert_eq!(meta.task_id, task_id);
        assert_eq!(meta.task_name, "test_task");
        assert_eq!(meta.worker, Some("worker-1".to_string()));
        assert!(matches!(meta.result, TaskResult::Success(_)));
        assert!(meta.started_at.is_some());
        assert!(meta.completed_at.is_some());
    }

    #[test]
    fn test_chord_builder() {
        let chord_id = Uuid::new_v4();
        let task_id1 = Uuid::new_v4();
        let task_id2 = Uuid::new_v4();

        let chord = ChordBuilder::new(chord_id, 10)
            .with_callback("aggregate")
            .with_timeout(Duration::from_secs(300))
            .with_max_retries(3)
            .add_task_id(task_id1)
            .add_task_id(task_id2)
            .build();

        assert_eq!(chord.chord_id, chord_id);
        assert_eq!(chord.total, 10);
        assert_eq!(chord.callback, Some("aggregate".to_string()));
        assert_eq!(chord.timeout, Some(Duration::from_secs(300)));
        assert_eq!(chord.max_retries, Some(3));
        assert_eq!(chord.task_ids.len(), 2);
    }

    #[test]
    fn test_backend_utils_pending_task() {
        let task_id = Uuid::new_v4();
        let meta = BackendUtils::pending_task(task_id, "test");

        assert_eq!(meta.task_id, task_id);
        assert_eq!(meta.task_name, "test");
        assert!(matches!(meta.result, TaskResult::Pending));
    }

    #[test]
    fn test_backend_utils_started_task() {
        let task_id = Uuid::new_v4();
        let meta = BackendUtils::started_task(task_id, "test", "worker-1");

        assert_eq!(meta.task_id, task_id);
        assert_eq!(meta.task_name, "test");
        assert_eq!(meta.worker, Some("worker-1".to_string()));
        assert!(matches!(meta.result, TaskResult::Started));
        assert!(meta.started_at.is_some());
    }

    #[test]
    fn test_backend_utils_success_task() {
        let task_id = Uuid::new_v4();
        let meta = BackendUtils::success_task(task_id, "test", serde_json::json!({"value": 42}));

        assert_eq!(meta.task_id, task_id);
        assert_eq!(meta.task_name, "test");
        assert!(matches!(meta.result, TaskResult::Success(_)));
        assert!(meta.started_at.is_some());
        assert!(meta.completed_at.is_some());
    }

    #[test]
    fn test_backend_utils_failed_task() {
        let task_id = Uuid::new_v4();
        let meta = BackendUtils::failed_task(task_id, "test", "Error message");

        assert_eq!(meta.task_id, task_id);
        assert_eq!(meta.task_name, "test");
        assert!(matches!(meta.result, TaskResult::Failure(_)));
        assert!(meta.started_at.is_some());
        assert!(meta.completed_at.is_some());
    }

    #[test]
    fn test_progress_utils() {
        let progress = ProgressUtils::with_percent(50, "Half done");
        assert_eq!(progress.current, 50);
        assert_eq!(progress.total, 100);
        assert_eq!(progress.message, Some("Half done".to_string()));

        let progress = ProgressUtils::from_counts(5, 10);
        assert_eq!(progress.current, 5);
        assert_eq!(progress.total, 10);

        let progress = ProgressUtils::from_counts_with_message(3, 10, "Processing");
        assert_eq!(progress.current, 3);
        assert_eq!(progress.total, 10);
        assert_eq!(progress.message, Some("Processing".to_string()));
    }
}
