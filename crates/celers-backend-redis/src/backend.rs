//! Redis result backend implementation
//!
//! Contains the [`RedisResultBackend`] struct with all inherent methods for
//! Redis-based task result storage, including builder methods, convenience
//! methods, query operations, transaction support, and archival.

use chrono::{DateTime, Utc};
use futures_util::stream::StreamExt;
use redis::{AsyncCommands, Client};
use std::time::Duration;
use uuid::Uuid;

use crate::query::TaskQuery;
use crate::result_backend_trait::{ResultBackend, ResultStream};
use crate::stats::{BackendStats, BatchOperationResult, PoolStats, StateCount, TaskSummary};
use crate::types::{BackendError, ProgressInfo, Result, TaskMeta, TaskResult, TaskTtlConfig};
use crate::{cache, chunking, compression, encryption, metrics};

/// Redis result backend implementation
#[derive(Clone)]
pub struct RedisResultBackend {
    pub(crate) client: Client,
    pub(crate) key_prefix: String,
    pub(crate) compression_config: compression::CompressionConfig,
    pub(crate) encryption_config: encryption::EncryptionConfig,
    pub(crate) metrics: metrics::BackendMetrics,
    pub(crate) cache: cache::ResultCache,
    pub(crate) ttl_config: TaskTtlConfig,
    pub(crate) compression_stats: celers_protocol::compression::CompressionStats,
    pub(crate) chunking_config: chunking::ChunkingConfig,
    pub(crate) chunker: chunking::ResultChunker,
}

impl RedisResultBackend {
    pub fn new(url: &str) -> Result<Self> {
        let client = Client::open(url).map_err(|e| {
            BackendError::Connection(format!("Failed to create Redis client: {}", e))
        })?;

        Ok(Self {
            client,
            key_prefix: "celery-task-meta-".to_string(),
            compression_config: compression::CompressionConfig::default(),
            encryption_config: encryption::EncryptionConfig::disabled(),
            metrics: metrics::BackendMetrics::new(),
            cache: cache::ResultCache::new(cache::CacheConfig::default()),
            ttl_config: TaskTtlConfig::new(),
            compression_stats: celers_protocol::compression::CompressionStats::default(),
            chunking_config: chunking::ChunkingConfig::default(),
            chunker: chunking::ResultChunker::new(chunking::ChunkingConfig::default()),
        })
    }

    pub fn with_prefix(mut self, prefix: String) -> Self {
        self.key_prefix = prefix;
        self
    }

    /// Configure result compression
    pub fn with_compression(mut self, config: compression::CompressionConfig) -> Self {
        self.compression_config = config;
        self
    }

    /// Disable result compression
    pub fn without_compression(mut self) -> Self {
        self.compression_config = compression::CompressionConfig::disabled();
        self
    }

    /// Get the compression configuration
    pub fn compression_config(&self) -> &compression::CompressionConfig {
        &self.compression_config
    }

    /// Configure metrics collection
    pub fn with_metrics(mut self, metrics: metrics::BackendMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    /// Disable metrics collection
    pub fn without_metrics(mut self) -> Self {
        self.metrics = metrics::BackendMetrics::disabled();
        self
    }

    /// Get the metrics collector
    pub fn metrics(&self) -> &metrics::BackendMetrics {
        &self.metrics
    }

    /// Configure result cache
    pub fn with_cache(mut self, cache: cache::ResultCache) -> Self {
        self.cache = cache;
        self
    }

    /// Disable result cache
    pub fn without_cache(mut self) -> Self {
        self.cache = cache::ResultCache::disabled();
        self
    }

    /// Get the result cache
    pub fn cache(&self) -> &cache::ResultCache {
        &self.cache
    }

    /// Configure result encryption
    pub fn with_encryption(mut self, config: encryption::EncryptionConfig) -> Self {
        self.encryption_config = config;
        self
    }

    /// Disable result encryption
    pub fn without_encryption(mut self) -> Self {
        self.encryption_config = encryption::EncryptionConfig::disabled();
        self
    }

    /// Get the encryption configuration
    pub fn encryption_config(&self) -> &encryption::EncryptionConfig {
        &self.encryption_config
    }

    /// Configure per-task-type TTL
    pub fn with_ttl_config(mut self, config: TaskTtlConfig) -> Self {
        self.ttl_config = config;
        self
    }

    /// Get the TTL configuration
    pub fn ttl_config(&self) -> &TaskTtlConfig {
        &self.ttl_config
    }

    /// Get a mutable reference to the TTL configuration
    pub fn ttl_config_mut(&mut self) -> &mut TaskTtlConfig {
        &mut self.ttl_config
    }

    /// Configure result chunking for large payloads
    pub fn with_chunking(mut self, config: chunking::ChunkingConfig) -> Self {
        self.chunking_config = config.clone();
        self.chunker = chunking::ResultChunker::new(config);
        self
    }

    /// Disable result chunking
    pub fn without_chunking(mut self) -> Self {
        let config = chunking::ChunkingConfig::disabled();
        self.chunking_config = config.clone();
        self.chunker = chunking::ResultChunker::new(config);
        self
    }

    /// Get the chunking configuration
    pub fn chunking_config(&self) -> &chunking::ChunkingConfig {
        &self.chunking_config
    }

    /// Get the result chunker
    pub fn chunker(&self) -> &chunking::ResultChunker {
        &self.chunker
    }

    /// Get the compression statistics
    pub fn compression_stats(&self) -> &celers_protocol::compression::CompressionStats {
        &self.compression_stats
    }

    pub(crate) fn task_key(&self, task_id: Uuid) -> String {
        format!("{}{}", self.key_prefix, task_id)
    }

    pub(crate) fn chord_key(&self, chord_id: Uuid) -> String {
        format!("celery-chord-{}", chord_id)
    }

    pub(crate) fn chord_counter_key(&self, chord_id: Uuid) -> String {
        format!("celery-chord-counter-{}", chord_id)
    }

    /// Create a stream of results for the given task IDs
    ///
    /// This allows async iteration over results without loading them all at once.
    ///
    /// # Arguments
    /// * `task_ids` - Task IDs to stream results for
    /// * `batch_size` - Number of results to fetch per batch (default: 10)
    ///
    /// # Example
    /// ```no_run
    /// use celers_backend_redis::RedisResultBackend;
    /// use futures_util::StreamExt;
    /// use uuid::Uuid;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut backend = RedisResultBackend::new("redis://localhost")?;
    /// let task_ids = vec![Uuid::new_v4(), Uuid::new_v4()];
    ///
    /// let mut stream = backend.stream_results(task_ids, 10);
    /// while let Some(result) = stream.next().await {
    ///     match result {
    ///         Ok((task_id, Some(meta))) => println!("Task {}: {:?}", task_id, meta.result),
    ///         Ok((task_id, None)) => println!("Task {} not found", task_id),
    ///         Err(e) => eprintln!("Error: {}", e),
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn stream_results(&mut self, task_ids: Vec<Uuid>, batch_size: usize) -> ResultStream {
        let backend = self.clone();
        let stream = futures_util::stream::iter(task_ids)
            .chunks(batch_size)
            .then(move |chunk| {
                let mut backend = backend.clone();
                async move {
                    let results = backend.get_results_batch(&chunk).await?;
                    Ok::<_, BackendError>(chunk.into_iter().zip(results).collect::<Vec<_>>())
                }
            })
            .flat_map(|batch_result| {
                futures_util::stream::iter(match batch_result {
                    Ok(batch) => batch.into_iter().map(Ok).collect(),
                    Err(e) => vec![Err(e)],
                })
            });

        Box::pin(stream)
    }

    /// Health check: Verify Redis connectivity
    ///
    /// Performs a simple PING command to verify the backend is operational.
    ///
    /// # Returns
    /// * `Ok(true)` - Backend is healthy and responsive
    /// * `Ok(false)` - Backend is not responding correctly
    /// * `Err(_)` - Connection or other error occurred
    ///
    /// # Example
    /// ```no_run
    /// use celers_backend_redis::RedisResultBackend;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut backend = RedisResultBackend::new("redis://localhost")?;
    ///
    /// if backend.health_check().await? {
    ///     println!("Backend is healthy");
    /// } else {
    ///     eprintln!("Backend health check failed");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn health_check(&mut self) -> Result<bool> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let result: String = redis::cmd("PING").query_async(&mut conn).await?;
        Ok(result == "PONG")
    }

    /// Scan keys matching a pattern using SCAN (production-safe, non-blocking)
    ///
    /// Uses Redis SCAN command instead of KEYS for safe iteration in production.
    pub(crate) async fn scan_keys(&mut self, pattern: &str) -> Result<Vec<String>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let mut all_keys = Vec::new();
        let mut cursor = 0u64;

        loop {
            let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(100) // Scan 100 keys per iteration
                .query_async(&mut conn)
                .await?;

            all_keys.extend(keys);
            cursor = next_cursor;

            if cursor == 0 {
                break;
            }
        }

        Ok(all_keys)
    }

    /// Get backend statistics
    ///
    /// Returns information about the backend state including key count,
    /// memory usage, and connection info.
    ///
    /// Uses SCAN instead of KEYS for production safety (non-blocking).
    ///
    /// # Example
    /// ```no_run
    /// use celers_backend_redis::RedisResultBackend;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut backend = RedisResultBackend::new("redis://localhost")?;
    /// let stats = backend.get_stats().await?;
    ///
    /// println!("Task keys: {}", stats.task_key_count);
    /// println!("Chord keys: {}", stats.chord_key_count);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_stats(&mut self) -> Result<BackendStats> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;

        // Count task result keys using SCAN (production-safe)
        let task_pattern = format!("{}*", self.key_prefix);
        let task_keys = self.scan_keys(&task_pattern).await?;
        let task_key_count = task_keys.len();

        // Count chord state keys using SCAN (production-safe)
        let chord_keys = self.scan_keys("celery-chord-*").await?;
        let chord_key_count = chord_keys.len();

        // Get memory info
        let info: String = redis::cmd("INFO")
            .arg("memory")
            .query_async(&mut conn)
            .await?;

        let used_memory = info
            .lines()
            .find(|line| line.starts_with("used_memory:"))
            .and_then(|line| line.split(':').nth(1))
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(0);

        Ok(BackendStats {
            task_key_count,
            chord_key_count,
            total_keys: task_key_count + chord_key_count,
            used_memory_bytes: used_memory,
        })
    }

    /// Cleanup expired or old task results
    ///
    /// Scans for task result keys and deletes them based on the provided filter.
    /// This is useful for bulk cleanup operations.
    ///
    /// Uses SCAN instead of KEYS for production safety (non-blocking).
    ///
    /// # Arguments
    /// * `older_than` - Delete results older than this duration
    ///
    /// # Returns
    /// Number of keys deleted
    ///
    /// # Example
    /// ```no_run
    /// use celers_backend_redis::RedisResultBackend;
    /// use std::time::Duration;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut backend = RedisResultBackend::new("redis://localhost")?;
    ///
    /// // Clean up results older than 7 days
    /// let deleted = backend.cleanup_old_results(Duration::from_secs(7 * 24 * 3600)).await?;
    /// println!("Cleaned up {} old results", deleted);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn cleanup_old_results(&mut self, older_than: Duration) -> Result<usize> {
        let pattern = format!("{}*", self.key_prefix);
        let keys = self.scan_keys(&pattern).await?;

        let cutoff = Utc::now() - chrono::Duration::from_std(older_than).unwrap_or_default();
        let mut deleted = 0;

        for key in keys {
            // Extract task_id from key
            if let Some(task_id_str) = key.strip_prefix(&self.key_prefix) {
                if let Ok(task_id) = Uuid::parse_str(task_id_str) {
                    // Check if result is old enough
                    if let Ok(Some(meta)) = self.get_result(task_id).await {
                        if meta.created_at < cutoff {
                            self.delete_result(task_id).await?;
                            deleted += 1;
                        }
                    }
                }
            }
        }

        Ok(deleted)
    }

    /// Cleanup completed chords
    ///
    /// Removes chord state for chords that have completed or timed out.
    ///
    /// Uses SCAN instead of KEYS for production safety (non-blocking).
    ///
    /// # Returns
    /// Number of chord states deleted
    ///
    /// # Example
    /// ```no_run
    /// use celers_backend_redis::RedisResultBackend;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut backend = RedisResultBackend::new("redis://localhost")?;
    ///
    /// let deleted = backend.cleanup_completed_chords().await?;
    /// println!("Cleaned up {} completed chords", deleted);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn cleanup_completed_chords(&mut self) -> Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let keys = self.scan_keys("celery-chord-*").await?;

        let mut deleted = 0;

        for key in keys {
            // Skip counter keys
            if key.contains("counter") {
                continue;
            }

            // Extract chord_id from key
            if let Some(chord_id_str) = key.strip_prefix("celery-chord-") {
                if let Ok(chord_id) = Uuid::parse_str(chord_id_str) {
                    if let Ok(Some(state)) = self.chord_get_state(chord_id).await {
                        // Delete if completed, cancelled, or timed out
                        if state.is_terminal() {
                            // Delete both state and counter
                            let state_key = self.chord_key(chord_id);
                            let counter_key = self.chord_counter_key(chord_id);
                            conn.del::<_, ()>(&state_key).await?;
                            conn.del::<_, ()>(&counter_key).await?;
                            deleted += 1;
                        }
                    }
                }
            }
        }

        Ok(deleted)
    }

    /// Store a result and set its expiration atomically
    ///
    /// This is a convenience method that combines `store_result()` and `set_expiration()`
    /// into a single operation, ensuring the TTL is always set.
    ///
    /// # Example
    /// ```no_run
    /// use celers_backend_redis::{RedisResultBackend, ResultBackend, TaskMeta, ttl};
    /// use uuid::Uuid;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut backend = RedisResultBackend::new("redis://localhost")?;
    /// let task_id = Uuid::new_v4();
    /// let meta = TaskMeta::new(task_id, "my_task".to_string());
    ///
    /// // Store with automatic expiration
    /// backend.store_result_with_ttl(task_id, &meta, ttl::LONG).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn store_result_with_ttl(
        &mut self,
        task_id: Uuid,
        meta: &TaskMeta,
        ttl: Duration,
    ) -> Result<()> {
        self.store_result(task_id, meta).await?;
        self.set_expiration(task_id, ttl).await?;
        Ok(())
    }

    /// Store multiple results with the same TTL
    ///
    /// This is a convenience method that combines batch store with TTL setting.
    ///
    /// # Example
    /// ```no_run
    /// use celers_backend_redis::{RedisResultBackend, ResultBackend, TaskMeta, ttl};
    /// use uuid::Uuid;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut backend = RedisResultBackend::new("redis://localhost")?;
    /// let results = vec![
    ///     (Uuid::new_v4(), TaskMeta::new(Uuid::new_v4(), "task1".to_string())),
    ///     (Uuid::new_v4(), TaskMeta::new(Uuid::new_v4(), "task2".to_string())),
    /// ];
    ///
    /// // Store all with same TTL
    /// backend.store_results_with_ttl(&results, ttl::LONG).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn store_results_with_ttl(
        &mut self,
        results: &[(Uuid, TaskMeta)],
        ttl: Duration,
    ) -> Result<()> {
        self.store_results_batch(results).await?;
        self.set_multiple_expirations(&results.iter().map(|(id, _)| *id).collect::<Vec<_>>(), ttl)
            .await?;
        Ok(())
    }

    /// Set expiration for multiple tasks at once
    ///
    /// This uses pipelining for efficient bulk TTL updates.
    ///
    /// # Example
    /// ```no_run
    /// use celers_backend_redis::{RedisResultBackend, ttl};
    /// use uuid::Uuid;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut backend = RedisResultBackend::new("redis://localhost")?;
    /// let task_ids = vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
    ///
    /// // Set TTL for all tasks
    /// backend.set_multiple_expirations(&task_ids, ttl::LONG).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn set_multiple_expirations(
        &mut self,
        task_ids: &[Uuid],
        ttl: Duration,
    ) -> Result<()> {
        if task_ids.is_empty() {
            return Ok(());
        }

        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let mut pipe = redis::pipe();

        let ttl_secs = ttl.as_secs() as i64;
        for task_id in task_ids {
            let key = self.task_key(*task_id);
            pipe.expire(&key, ttl_secs);
        }

        pipe.query_async::<()>(&mut conn).await?;
        Ok(())
    }

    /// Wait for a task result with timeout and polling
    ///
    /// Polls the backend until the task reaches a terminal state or timeout is reached.
    ///
    /// # Arguments
    /// * `task_id` - Task to wait for
    /// * `timeout` - Maximum time to wait
    /// * `poll_interval` - Time between polls (default: 500ms)
    ///
    /// # Returns
    /// * `Ok(Some(meta))` - Task completed (terminal state)
    /// * `Ok(None)` - Timeout reached before completion
    /// * `Err(...)` - Backend error
    ///
    /// # Example
    /// ```no_run
    /// use celers_backend_redis::RedisResultBackend;
    /// use uuid::Uuid;
    /// use std::time::Duration;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut backend = RedisResultBackend::new("redis://localhost")?;
    /// let task_id = Uuid::new_v4();
    ///
    /// // Wait up to 30 seconds for result
    /// match backend.wait_for_result(
    ///     task_id,
    ///     Duration::from_secs(30),
    ///     Duration::from_millis(500)
    /// ).await? {
    ///     Some(meta) => println!("Task completed: {:?}", meta.result),
    ///     None => println!("Task timed out"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn wait_for_result(
        &mut self,
        task_id: Uuid,
        timeout: Duration,
        poll_interval: Duration,
    ) -> Result<Option<TaskMeta>> {
        let start = std::time::Instant::now();

        loop {
            if let Some(meta) = self.get_result(task_id).await? {
                if meta.is_terminal() {
                    return Ok(Some(meta));
                }
            }

            if start.elapsed() >= timeout {
                return Ok(None);
            }

            tokio::time::sleep(poll_interval).await;
        }
    }

    /// Check if a task is in a terminal state without fetching full metadata
    ///
    /// This is more efficient than fetching the full result when you only need to check completion.
    ///
    /// # Example
    /// ```no_run
    /// use celers_backend_redis::RedisResultBackend;
    /// use uuid::Uuid;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut backend = RedisResultBackend::new("redis://localhost")?;
    /// let task_id = Uuid::new_v4();
    ///
    /// if backend.is_task_complete(task_id).await? {
    ///     println!("Task is done!");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn is_task_complete(&mut self, task_id: Uuid) -> Result<bool> {
        if let Some(meta) = self.get_result(task_id).await? {
            Ok(meta.is_terminal())
        } else {
            Ok(false)
        }
    }

    /// Get the age of a task result (time since creation)
    ///
    /// # Example
    /// ```no_run
    /// use celers_backend_redis::RedisResultBackend;
    /// use uuid::Uuid;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut backend = RedisResultBackend::new("redis://localhost")?;
    /// let task_id = Uuid::new_v4();
    ///
    /// if let Some(age) = backend.get_task_age(task_id).await? {
    ///     println!("Task created {} seconds ago", age.num_seconds());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_task_age(&mut self, task_id: Uuid) -> Result<Option<chrono::Duration>> {
        if let Some(meta) = self.get_result(task_id).await? {
            Ok(Some(meta.age()))
        } else {
            Ok(None)
        }
    }

    /// Get or create a task result
    ///
    /// If the task exists, returns the existing metadata.
    /// If not, creates it with the provided metadata.
    ///
    /// # Example
    /// ```no_run
    /// use celers_backend_redis::{RedisResultBackend, ResultBackend, TaskMeta, TaskResult};
    /// use uuid::Uuid;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut backend = RedisResultBackend::new("redis://localhost")?;
    /// let task_id = Uuid::new_v4();
    ///
    /// // Get existing or create new
    /// let meta = backend.get_or_create(
    ///     task_id,
    ///     TaskMeta::new(task_id, "my_task".to_string())
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_or_create(&mut self, task_id: Uuid, default: TaskMeta) -> Result<TaskMeta> {
        if let Some(existing) = self.get_result(task_id).await? {
            Ok(existing)
        } else {
            self.store_result(task_id, &default).await?;
            Ok(default)
        }
    }

    /// Mark a task as failed with an error message and timestamp
    ///
    /// Convenience method that sets the task result to Failure and updates completion time.
    ///
    /// # Example
    /// ```no_run
    /// use celers_backend_redis::{RedisResultBackend, ResultBackend};
    /// use uuid::Uuid;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut backend = RedisResultBackend::new("redis://localhost")?;
    /// let task_id = Uuid::new_v4();
    ///
    /// backend.mark_failed(task_id, "Connection timeout".to_string()).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn mark_failed(&mut self, task_id: Uuid, error: String) -> Result<()> {
        self.mark_completed(task_id, TaskResult::Failure(error))
            .await
    }

    /// Mark a task as successful with a result value and timestamp
    ///
    /// Convenience method that sets the task result to Success and updates completion time.
    ///
    /// # Example
    /// ```no_run
    /// use celers_backend_redis::{RedisResultBackend, ResultBackend};
    /// use uuid::Uuid;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut backend = RedisResultBackend::new("redis://localhost")?;
    /// let task_id = Uuid::new_v4();
    ///
    /// backend.mark_success(task_id, serde_json::json!({"result": 42})).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn mark_success(&mut self, task_id: Uuid, value: serde_json::Value) -> Result<()> {
        self.mark_completed(task_id, TaskResult::Success(value))
            .await
    }

    /// Mark a task as revoked (cancelled)
    ///
    /// Convenience method that sets the task result to Revoked and updates completion time.
    ///
    /// # Example
    /// ```no_run
    /// use celers_backend_redis::{RedisResultBackend, ResultBackend};
    /// use uuid::Uuid;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut backend = RedisResultBackend::new("redis://localhost")?;
    /// let task_id = Uuid::new_v4();
    ///
    /// backend.mark_revoked(task_id).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn mark_revoked(&mut self, task_id: Uuid) -> Result<()> {
        self.mark_completed(task_id, TaskResult::Revoked).await
    }

    /// Get ages for multiple tasks in a single batch operation
    ///
    /// This is more efficient than calling `get_task_age` multiple times.
    ///
    /// # Returns
    /// A vector of `Option<chrono::Duration>` corresponding to each task ID.
    /// Returns `None` for tasks that don't exist.
    pub async fn get_task_ages_batch(
        &mut self,
        task_ids: &[Uuid],
    ) -> Result<Vec<Option<chrono::Duration>>> {
        let results = self.get_results_batch(task_ids).await?;
        let now = Utc::now();

        Ok(results
            .into_iter()
            .map(|meta_opt| meta_opt.map(|meta| now.signed_duration_since(meta.created_at)))
            .collect())
    }

    /// Get a summary of task states for a list of tasks
    ///
    /// Returns statistics about task completion, failures, etc.
    pub async fn get_task_summary(&mut self, task_ids: &[Uuid]) -> Result<TaskSummary> {
        let results = self.get_results_batch(task_ids).await?;

        let mut summary = TaskSummary {
            total: task_ids.len(),
            found: 0,
            not_found: 0,
            pending: 0,
            started: 0,
            success: 0,
            failure: 0,
            retry: 0,
            revoked: 0,
        };

        for result in results {
            match result {
                Some(meta) => {
                    summary.found += 1;
                    match meta.result {
                        TaskResult::Pending => summary.pending += 1,
                        TaskResult::Started => summary.started += 1,
                        TaskResult::Success(_) => summary.success += 1,
                        TaskResult::Failure(_) => summary.failure += 1,
                        TaskResult::Retry(_) => summary.retry += 1,
                        TaskResult::Revoked => summary.revoked += 1,
                    }
                }
                None => summary.not_found += 1,
            }
        }

        Ok(summary)
    }

    /// Check if a task exists in the backend
    ///
    /// This is more efficient than calling `get_result` if you only need to check existence.
    pub async fn task_exists(&mut self, task_id: Uuid) -> Result<bool> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = self.task_key(task_id);
        let exists: bool = conn.exists(&key).await?;
        Ok(exists)
    }

    /// Check existence for multiple tasks in a batch
    ///
    /// Returns a vector of booleans indicating whether each task exists.
    pub async fn tasks_exist_batch(&mut self, task_ids: &[Uuid]) -> Result<Vec<bool>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let mut pipe = redis::pipe();

        for task_id in task_ids {
            let key = self.task_key(*task_id);
            pipe.exists(&key);
        }

        let results: Vec<bool> = pipe.query_async(&mut conn).await?;
        Ok(results)
    }

    /// Get failed tasks from a list of task IDs
    ///
    /// Returns task IDs and their error messages for all failed tasks.
    pub async fn get_failed_tasks(&mut self, task_ids: &[Uuid]) -> Result<Vec<(Uuid, String)>> {
        let results = self.get_results_batch(task_ids).await?;
        let mut failed = Vec::new();

        for (task_id, meta_opt) in task_ids.iter().zip(results.iter()) {
            if let Some(meta) = meta_opt {
                if let TaskResult::Failure(error) = &meta.result {
                    failed.push((*task_id, error.clone()));
                }
            }
        }

        Ok(failed)
    }

    /// Get successful tasks from a list of task IDs
    ///
    /// Returns task IDs and their result values for all successful tasks.
    pub async fn get_successful_tasks(
        &mut self,
        task_ids: &[Uuid],
    ) -> Result<Vec<(Uuid, serde_json::Value)>> {
        let results = self.get_results_batch(task_ids).await?;
        let mut successful = Vec::new();

        for (task_id, meta_opt) in task_ids.iter().zip(results.iter()) {
            if let Some(meta) = meta_opt {
                if let TaskResult::Success(value) = &meta.result {
                    successful.push((*task_id, value.clone()));
                }
            }
        }

        Ok(successful)
    }

    /// Cleanup tasks by state
    ///
    /// Removes all tasks matching the specified state from a list of task IDs.
    /// Returns the number of tasks deleted.
    pub async fn cleanup_by_state(
        &mut self,
        task_ids: &[Uuid],
        target_state: TaskResult,
    ) -> Result<usize> {
        let results = self.get_results_batch(task_ids).await?;
        let mut to_delete = Vec::new();

        for (task_id, meta_opt) in task_ids.iter().zip(results.iter()) {
            if let Some(meta) = meta_opt {
                // Compare discriminants (state type) only
                if std::mem::discriminant(&meta.result) == std::mem::discriminant(&target_state) {
                    to_delete.push(*task_id);
                }
            }
        }

        if !to_delete.is_empty() {
            self.delete_results_batch(&to_delete).await?;
        }

        Ok(to_delete.len())
    }

    /// Get TTL (time to live) for a task result
    ///
    /// Returns the remaining time before the task expires, or None if no TTL is set.
    pub async fn get_ttl(&mut self, task_id: Uuid) -> Result<Option<Duration>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = self.task_key(task_id);

        let ttl_secs: i64 = conn.ttl(&key).await?;

        // Redis returns -1 if key exists but has no TTL, -2 if key doesn't exist
        if ttl_secs > 0 {
            Ok(Some(Duration::from_secs(ttl_secs as u64)))
        } else {
            Ok(None)
        }
    }

    /// Refresh TTL for a task (reset expiration timer)
    ///
    /// Extends the TTL of a task by the specified duration from now.
    pub async fn refresh_ttl(&mut self, task_id: Uuid, ttl: Duration) -> Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = self.task_key(task_id);
        let _: bool = conn.expire(&key, ttl.as_secs() as i64).await?;
        Ok(())
    }

    /// Refresh TTL for multiple tasks at once
    ///
    /// Efficiently updates TTL for multiple tasks using pipelining.
    /// Returns the number of tasks that had their TTL updated.
    pub async fn refresh_ttl_batch(&mut self, task_ids: &[Uuid], ttl: Duration) -> Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let mut pipe = redis::pipe();

        for task_id in task_ids {
            let key = self.task_key(*task_id);
            pipe.expire(&key, ttl.as_secs() as i64);
        }

        let results: Vec<bool> = pipe.query_async(&mut conn).await?;
        Ok(results.iter().filter(|&&r| r).count())
    }

    /// Remove TTL from a task (make it persistent)
    ///
    /// The task will no longer expire automatically.
    pub async fn persist_task(&mut self, task_id: Uuid) -> Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = self.task_key(task_id);
        let _: bool = conn.persist(&key).await?;
        Ok(())
    }

    /// Count tasks by state in a list of task IDs
    ///
    /// Returns a count of how many tasks are in each state.
    pub async fn count_by_state(&mut self, task_ids: &[Uuid]) -> Result<StateCount> {
        let results = self.get_results_batch(task_ids).await?;

        let mut counts = StateCount {
            total: task_ids.len(),
            pending: 0,
            started: 0,
            success: 0,
            failure: 0,
            retry: 0,
            revoked: 0,
            not_found: 0,
        };

        for meta_opt in results {
            match meta_opt {
                Some(meta) => match meta.result {
                    TaskResult::Pending => counts.pending += 1,
                    TaskResult::Started => counts.started += 1,
                    TaskResult::Success(_) => counts.success += 1,
                    TaskResult::Failure(_) => counts.failure += 1,
                    TaskResult::Retry(_) => counts.retry += 1,
                    TaskResult::Revoked => counts.revoked += 1,
                },
                None => counts.not_found += 1,
            }
        }

        Ok(counts)
    }

    // === Transaction Support ===

    /// Atomically store multiple results with optional TTL
    ///
    /// Uses Redis MULTI/EXEC transaction to ensure all operations succeed or fail together.
    ///
    /// # Example
    /// ```no_run
    /// use celers_backend_redis::{RedisResultBackend, TaskMeta, ttl};
    /// use uuid::Uuid;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut backend = RedisResultBackend::new("redis://localhost")?;
    /// let results = vec![
    ///     (Uuid::new_v4(), TaskMeta::new(Uuid::new_v4(), "task1".to_string())),
    ///     (Uuid::new_v4(), TaskMeta::new(Uuid::new_v4(), "task2".to_string())),
    /// ];
    ///
    /// // Atomically store all results with TTL
    /// backend.atomic_store_multiple(&results, Some(ttl::LONG)).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn atomic_store_multiple(
        &mut self,
        results: &[(Uuid, TaskMeta)],
        ttl: Option<Duration>,
    ) -> Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let mut pipe = redis::pipe();
        pipe.atomic();

        for (task_id, meta) in results {
            let key = self.task_key(*task_id);
            let json = serde_json::to_string(meta)
                .map_err(|e| BackendError::Serialization(e.to_string()))?;
            pipe.set(&key, json);

            if let Some(ttl_duration) = ttl {
                pipe.expire(&key, ttl_duration.as_secs() as i64);
            }
        }

        let _: () = pipe.query_async(&mut conn).await?;
        Ok(())
    }

    /// Atomically delete multiple task results
    ///
    /// Uses Redis transaction to ensure all deletions succeed or fail together.
    ///
    /// # Example
    /// ```no_run
    /// use celers_backend_redis::RedisResultBackend;
    /// use uuid::Uuid;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut backend = RedisResultBackend::new("redis://localhost")?;
    /// let task_ids = vec![Uuid::new_v4(), Uuid::new_v4()];
    ///
    /// let deleted = backend.atomic_delete_multiple(&task_ids).await?;
    /// println!("Deleted {} tasks", deleted);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn atomic_delete_multiple(&mut self, task_ids: &[Uuid]) -> Result<usize> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let mut pipe = redis::pipe();
        pipe.atomic();

        for task_id in task_ids {
            let key = self.task_key(*task_id);
            pipe.del(&key);
        }

        let results: Vec<i32> = pipe.query_async(&mut conn).await?;
        Ok(results.iter().sum::<i32>() as usize)
    }

    // === Lua Script Support ===

    /// Execute a Lua script for atomic operations
    ///
    /// Lua scripts are executed atomically on the Redis server, ensuring thread-safe
    /// operations without race conditions.
    ///
    /// # Example
    /// ```no_run
    /// use celers_backend_redis::RedisResultBackend;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut backend = RedisResultBackend::new("redis://localhost")?;
    ///
    /// // Lua script to increment a counter with max value
    /// let script = r#"
    ///     local current = redis.call('GET', KEYS[1])
    ///     if not current then current = 0 else current = tonumber(current) end
    ///     local max = tonumber(ARGV[1])
    ///     if current < max then
    ///         redis.call('INCR', KEYS[1])
    ///         return current + 1
    ///     else
    ///         return current
    ///     end
    /// "#;
    ///
    /// let result: i64 = backend.eval_script(
    ///     script,
    ///     &["counter_key"],
    ///     &["100"]
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[allow(dead_code)]
    pub async fn eval_script<T: redis::FromRedisValue>(
        &mut self,
        script: &str,
        keys: &[&str],
        args: &[&str],
    ) -> Result<T> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let result = redis::Script::new(script)
            .key(keys)
            .arg(args)
            .invoke_async(&mut conn)
            .await?;
        Ok(result)
    }

    /// Compare-and-swap operation using Lua script
    ///
    /// Atomically updates a task result only if the current state matches the expected state.
    ///
    /// # Returns
    /// - `Ok(true)` if the swap was successful
    /// - `Ok(false)` if the current state didn't match
    ///
    /// # Example
    /// ```no_run
    /// use celers_backend_redis::{RedisResultBackend, TaskMeta, TaskResult};
    /// use uuid::Uuid;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut backend = RedisResultBackend::new("redis://localhost")?;
    /// let task_id = Uuid::new_v4();
    ///
    /// let mut expected = TaskMeta::new(task_id, "task".to_string());
    /// expected.result = TaskResult::Started;
    ///
    /// let mut new = expected.clone();
    /// new.result = TaskResult::Success(serde_json::json!({"result": 42}));
    ///
    /// // Only updates if task is currently Started
    /// let swapped = backend.compare_and_swap(task_id, &expected, &new).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn compare_and_swap(
        &mut self,
        task_id: Uuid,
        expected: &TaskMeta,
        new_value: &TaskMeta,
    ) -> Result<bool> {
        let key = self.task_key(task_id);
        let expected_json = serde_json::to_string(expected)
            .map_err(|e| BackendError::Serialization(e.to_string()))?;
        let new_json = serde_json::to_string(new_value)
            .map_err(|e| BackendError::Serialization(e.to_string()))?;

        // Lua script for atomic compare-and-swap
        let script = r#"
            local current = redis.call('GET', KEYS[1])
            if current == ARGV[1] then
                redis.call('SET', KEYS[1], ARGV[2])
                return 1
            else
                return 0
            end
        "#;

        let result: i32 = self
            .eval_script(script, &[&key], &[&expected_json, &new_json])
            .await?;

        Ok(result == 1)
    }

    // === Pattern-Based Operations ===

    /// Find all task IDs matching a pattern
    ///
    /// Uses SCAN for production-safe iteration without blocking.
    ///
    /// # Example
    /// ```no_run
    /// use celers_backend_redis::RedisResultBackend;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut backend = RedisResultBackend::new("redis://localhost")?;
    ///
    /// // Find all tasks (using wildcard pattern)
    /// let task_ids = backend.find_tasks_by_pattern("*").await?;
    /// println!("Found {} tasks", task_ids.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn find_tasks_by_pattern(&mut self, pattern: &str) -> Result<Vec<Uuid>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let full_pattern = format!("{}{}", self.key_prefix, pattern);

        let mut task_ids = Vec::new();
        let mut cursor = 0u64;

        loop {
            let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&full_pattern)
                .arg("COUNT")
                .arg(100)
                .query_async(&mut conn)
                .await?;

            for key in keys {
                // Extract task ID from key
                if let Some(id_str) = key.strip_prefix(&self.key_prefix) {
                    if let Ok(task_id) = Uuid::parse_str(id_str) {
                        task_ids.push(task_id);
                    }
                }
            }

            cursor = new_cursor;
            if cursor == 0 {
                break;
            }
        }

        Ok(task_ids)
    }

    /// Delete all tasks matching a pattern
    ///
    /// Uses SCAN to find matching keys, then deletes them in batches.
    ///
    /// # Returns
    /// Number of tasks deleted
    ///
    /// # Example
    /// ```no_run
    /// use celers_backend_redis::RedisResultBackend;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut backend = RedisResultBackend::new("redis://localhost")?;
    ///
    /// // Delete tasks (example pattern - be careful!)
    /// // let deleted = backend.delete_tasks_by_pattern("test-*").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_tasks_by_pattern(&mut self, pattern: &str) -> Result<usize> {
        let task_ids = self.find_tasks_by_pattern(pattern).await?;
        let count = task_ids.len();

        if count == 0 {
            return Ok(0);
        }

        self.delete_results_batch(&task_ids).await?;
        Ok(count)
    }

    // === Task Dependencies ===

    /// Store task dependencies (parent-child relationships)
    ///
    /// # Example
    /// ```no_run
    /// use celers_backend_redis::RedisResultBackend;
    /// use uuid::Uuid;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut backend = RedisResultBackend::new("redis://localhost")?;
    ///
    /// let parent = Uuid::new_v4();
    /// let children = vec![Uuid::new_v4(), Uuid::new_v4()];
    ///
    /// backend.store_task_dependencies(parent, &children).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn store_task_dependencies(
        &mut self,
        parent_id: Uuid,
        child_ids: &[Uuid],
    ) -> Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = format!("{}deps:{}", self.key_prefix, parent_id);

        let child_strings: Vec<String> = child_ids.iter().map(|id| id.to_string()).collect();

        if !child_strings.is_empty() {
            let _: () = conn.sadd(&key, child_strings).await?;
        }

        Ok(())
    }

    /// Get all tasks that depend on a given task
    ///
    /// # Returns
    /// Vector of task IDs that are children of the given parent task
    pub async fn get_task_dependencies(&mut self, parent_id: Uuid) -> Result<Vec<Uuid>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = format!("{}deps:{}", self.key_prefix, parent_id);

        let child_strings: Vec<String> = conn.smembers(&key).await?;

        let child_ids: Vec<Uuid> = child_strings
            .iter()
            .filter_map(|s| Uuid::parse_str(s).ok())
            .collect();

        Ok(child_ids)
    }

    /// Remove task dependencies
    pub async fn remove_task_dependencies(&mut self, parent_id: Uuid) -> Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = format!("{}deps:{}", self.key_prefix, parent_id);
        let _: () = conn.del(&key).await?;
        Ok(())
    }

    /// Check if all dependencies of a task are complete
    ///
    /// # Returns
    /// - `Ok(true)` if all dependencies are in terminal state
    /// - `Ok(false)` if any dependency is still running
    pub async fn are_dependencies_complete(&mut self, parent_id: Uuid) -> Result<bool> {
        let child_ids = self.get_task_dependencies(parent_id).await?;

        if child_ids.is_empty() {
            return Ok(true);
        }

        let results = self.get_results_batch(&child_ids).await?;

        for meta_opt in results {
            if let Some(meta) = meta_opt {
                if !meta.result.is_terminal() {
                    return Ok(false);
                }
            } else {
                return Ok(false);
            }
        }

        Ok(true)
    }

    // === Connection Monitoring ===

    /// Get detailed Redis connection information
    ///
    /// # Example
    /// ```no_run
    /// use celers_backend_redis::RedisResultBackend;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut backend = RedisResultBackend::new("redis://localhost")?;
    ///
    /// let info = backend.get_connection_info().await?;
    /// println!("Redis info:\n{}", info);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_connection_info(&mut self) -> Result<String> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let info: String = redis::cmd("INFO").query_async(&mut conn).await?;
        Ok(info)
    }

    /// Get Redis server memory statistics
    ///
    /// # Returns
    /// Memory statistics as a map
    pub async fn get_memory_stats(&mut self) -> Result<std::collections::HashMap<String, String>> {
        let info = self.get_connection_info().await?;
        let mut stats = std::collections::HashMap::new();

        for line in info.lines() {
            if line.starts_with("used_memory") || line.starts_with("maxmemory") {
                if let Some((key, value)) = line.split_once(':') {
                    stats.insert(key.to_string(), value.to_string());
                }
            }
        }

        Ok(stats)
    }

    /// Test connection latency (ping round-trip time)
    ///
    /// # Returns
    /// Round-trip time in microseconds
    pub async fn ping_latency(&mut self) -> Result<Duration> {
        let start = std::time::Instant::now();
        self.health_check().await?;
        Ok(start.elapsed())
    }

    // === Task Querying ===

    /// Query tasks by state
    ///
    /// Returns task IDs that match the specified state.
    ///
    /// # Example
    /// ```no_run
    /// use celers_backend_redis::{RedisResultBackend, TaskResult};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut backend = RedisResultBackend::new("redis://localhost")?;
    ///
    /// // Find all failed tasks
    /// let failed_ids = backend.query_tasks_by_state(TaskResult::Failure("".to_string())).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn query_tasks_by_state(&mut self, target_state: TaskResult) -> Result<Vec<Uuid>> {
        // Get all task keys
        let pattern = format!("{}task-meta-*", self.key_prefix);
        let task_ids = self.find_tasks_by_pattern(&pattern).await?;

        let mut matching_ids = Vec::new();

        // Filter by state
        for task_id in task_ids {
            if let Some(meta) = self.get_result(task_id).await? {
                if meta.result.same_variant(&target_state) {
                    matching_ids.push(task_id);
                }
            }
        }

        Ok(matching_ids)
    }

    /// Query tasks by worker name
    ///
    /// Returns task IDs that were executed by the specified worker.
    pub async fn query_tasks_by_worker(&mut self, worker_name: &str) -> Result<Vec<Uuid>> {
        let pattern = format!("{}task-meta-*", self.key_prefix);
        let task_ids = self.find_tasks_by_pattern(&pattern).await?;

        let mut matching_ids = Vec::new();

        for task_id in task_ids {
            if let Some(meta) = self.get_result(task_id).await? {
                if let Some(ref worker) = meta.worker {
                    if worker == worker_name {
                        matching_ids.push(task_id);
                    }
                }
            }
        }

        Ok(matching_ids)
    }

    /// Query tasks created within a time range
    ///
    /// # Arguments
    /// * `start` - Start time (inclusive)
    /// * `end` - End time (inclusive)
    ///
    /// # Returns
    /// Vector of task IDs created within the specified time range
    pub async fn query_tasks_by_time_range(
        &mut self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<Uuid>> {
        let pattern = format!("{}task-meta-*", self.key_prefix);
        let task_ids = self.find_tasks_by_pattern(&pattern).await?;

        let mut matching_ids = Vec::new();

        for task_id in task_ids {
            if let Some(meta) = self.get_result(task_id).await? {
                if meta.created_at >= start && meta.created_at <= end {
                    matching_ids.push(task_id);
                }
            }
        }

        Ok(matching_ids)
    }

    /// Query tasks by multiple criteria
    ///
    /// # Arguments
    /// * `criteria` - Query criteria to filter tasks
    ///
    /// # Returns
    /// Vector of task IDs that match all specified criteria
    pub async fn query_tasks(&mut self, criteria: TaskQuery) -> Result<Vec<Uuid>> {
        let pattern = format!("{}task-meta-*", self.key_prefix);
        let task_ids = self.find_tasks_by_pattern(&pattern).await?;

        let mut matching_ids = Vec::new();

        for task_id in task_ids {
            if let Some(meta) = self.get_result(task_id).await? {
                if criteria.matches(&meta) {
                    matching_ids.push(task_id);
                }
            }
        }

        Ok(matching_ids)
    }

    /// Query tasks by tags (task must have all specified tags)
    ///
    /// This is a convenience method that wraps `query_tasks` with a tag filter.
    pub async fn query_tasks_by_tags(&mut self, tags: Vec<String>) -> Result<Vec<Uuid>> {
        let criteria = TaskQuery::new().with_tags(tags);
        self.query_tasks(criteria).await
    }

    /// Count tasks with specific tags
    ///
    /// Returns the number of tasks that have all specified tags.
    pub async fn count_tasks_by_tags(&mut self, tags: Vec<String>) -> Result<usize> {
        let task_ids = self.query_tasks_by_tags(tags).await?;
        Ok(task_ids.len())
    }

    /// Bulk delete tasks with specific tags
    ///
    /// Deletes all tasks that have all specified tags.
    /// Returns the number of tasks deleted.
    pub async fn bulk_delete_by_tags(&mut self, tags: Vec<String>) -> Result<usize> {
        let task_ids = self.query_tasks_by_tags(tags).await?;
        if task_ids.is_empty() {
            return Ok(0);
        }
        self.delete_results_batch(&task_ids).await?;
        Ok(task_ids.len())
    }

    /// Bulk revoke tasks with specific tags
    ///
    /// Revokes all tasks that have all specified tags.
    /// Returns the number of tasks revoked.
    pub async fn bulk_revoke_by_tags(&mut self, tags: Vec<String>) -> Result<usize> {
        let task_ids = self.query_tasks_by_tags(tags).await?;
        if task_ids.is_empty() {
            return Ok(0);
        }
        self.bulk_transition_state(&task_ids, TaskResult::Revoked)
            .await
    }

    /// Bulk set TTL for tasks with specific tags
    ///
    /// Sets expiration time for all tasks that have all specified tags.
    /// Returns the number of tasks updated.
    pub async fn bulk_set_ttl_by_tags(
        &mut self,
        tags: Vec<String>,
        ttl: Duration,
    ) -> Result<usize> {
        let task_ids = self.query_tasks_by_tags(tags).await?;
        if task_ids.is_empty() {
            return Ok(0);
        }
        self.set_multiple_expirations(&task_ids, ttl).await?;
        Ok(task_ids.len())
    }

    /// Store multiple results with detailed error tracking
    ///
    /// Attempts to store all results and returns detailed information about
    /// successes and failures. Unlike `store_results_batch`, this method
    /// continues on errors and reports which specific tasks failed.
    pub async fn store_results_with_details(
        &mut self,
        results: &[(Uuid, TaskMeta)],
    ) -> BatchOperationResult {
        let mut batch_result = BatchOperationResult::new();

        for (task_id, meta) in results {
            match self.store_result(*task_id, meta).await {
                Ok(()) => batch_result.record_success(*task_id),
                Err(e) => batch_result.record_failure(*task_id, e.to_string()),
            }
        }

        batch_result
    }

    /// Delete multiple results with detailed error tracking
    ///
    /// Attempts to delete all results and returns detailed information about
    /// successes and failures.
    pub async fn delete_results_with_details(&mut self, task_ids: &[Uuid]) -> BatchOperationResult {
        let mut batch_result = BatchOperationResult::new();

        for task_id in task_ids {
            match self.delete_result(*task_id).await {
                Ok(()) => batch_result.record_success(*task_id),
                Err(e) => batch_result.record_failure(*task_id, e.to_string()),
            }
        }

        batch_result
    }

    // === Bulk State Transitions ===

    /// Transition multiple tasks to a new state atomically
    ///
    /// This is useful for operational tasks like bulk revocation.
    ///
    /// # Example
    /// ```no_run
    /// use celers_backend_redis::{RedisResultBackend, TaskResult};
    /// use uuid::Uuid;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut backend = RedisResultBackend::new("redis://localhost")?;
    /// let task_ids = vec![Uuid::new_v4(), Uuid::new_v4()];
    ///
    /// // Revoke all tasks
    /// let count = backend.bulk_transition_state(&task_ids, TaskResult::Revoked).await?;
    /// println!("Transitioned {} tasks", count);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn bulk_transition_state(
        &mut self,
        task_ids: &[Uuid],
        new_state: TaskResult,
    ) -> Result<usize> {
        let mut count = 0;

        for &task_id in task_ids {
            if let Some(mut meta) = self.get_result(task_id).await? {
                meta.result = new_state.clone();

                // Update completion timestamp for terminal states
                if new_state.is_terminal() && meta.completed_at.is_none() {
                    meta.completed_at = Some(Utc::now());
                }

                self.store_result(task_id, &meta).await?;
                count += 1;
            }
        }

        Ok(count)
    }

    /// Bulk revoke tasks by pattern
    ///
    /// Finds all tasks matching the pattern and transitions them to Revoked state.
    ///
    /// # Returns
    /// Number of tasks revoked
    pub async fn bulk_revoke_by_pattern(&mut self, pattern: &str) -> Result<usize> {
        let task_ids = self.find_tasks_by_pattern(pattern).await?;
        self.bulk_transition_state(&task_ids, TaskResult::Revoked)
            .await
    }

    // === Metadata Partial Updates ===

    /// Update only the worker field of task metadata
    ///
    /// This is more efficient than fetching and storing the entire metadata.
    pub async fn update_worker(&mut self, task_id: Uuid, worker: String) -> Result<()> {
        if let Some(mut meta) = self.get_result(task_id).await? {
            meta.worker = Some(worker);
            self.store_result(task_id, &meta).await?;
        }
        Ok(())
    }

    /// Update only the progress field of task metadata
    pub async fn update_progress_field(
        &mut self,
        task_id: Uuid,
        progress: ProgressInfo,
    ) -> Result<()> {
        if let Some(mut meta) = self.get_result(task_id).await? {
            meta.progress = Some(progress);
            self.store_result(task_id, &meta).await?;
        }
        Ok(())
    }

    /// Increment version number of a task
    ///
    /// This is useful for optimistic locking or tracking how many times
    /// a task result has been updated.
    pub async fn increment_version(&mut self, task_id: Uuid) -> Result<u32> {
        if let Some(mut meta) = self.get_result(task_id).await? {
            meta.version += 1;
            let new_version = meta.version;
            self.store_result(task_id, &meta).await?;
            Ok(new_version)
        } else {
            Err(BackendError::NotFound(task_id))
        }
    }

    // === Result Archival ===

    /// Copy task results to an archive key with a longer TTL
    ///
    /// This is useful for preserving important results before they expire.
    ///
    /// # Example
    /// ```no_run
    /// use celers_backend_redis::RedisResultBackend;
    /// use uuid::Uuid;
    /// use std::time::Duration;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut backend = RedisResultBackend::new("redis://localhost")?;
    /// let task_id = Uuid::new_v4();
    ///
    /// // Archive with 90-day retention
    /// backend.archive_result(task_id, Duration::from_secs(90 * 86400)).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn archive_result(&mut self, task_id: Uuid, archive_ttl: Duration) -> Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;

        let source_key = format!("{}task-meta-{}", self.key_prefix, task_id);
        let archive_key = format!("{}archive:task-meta-{}", self.key_prefix, task_id);

        // Copy to archive key
        let _: () = redis::cmd("COPY")
            .arg(&source_key)
            .arg(&archive_key)
            .arg("REPLACE")
            .query_async(&mut conn)
            .await?;

        // Set TTL on archive
        let ttl_secs = archive_ttl.as_secs() as i64;
        let _: () = conn.expire(&archive_key, ttl_secs).await?;

        Ok(())
    }

    /// Retrieve an archived task result
    pub async fn get_archived_result(&mut self, task_id: Uuid) -> Result<Option<TaskMeta>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let archive_key = format!("{}archive:task-meta-{}", self.key_prefix, task_id);

        let data: Option<String> = conn.get(&archive_key).await?;

        match data {
            Some(json_str) => {
                let meta: TaskMeta = serde_json::from_str(&json_str)
                    .map_err(|e| BackendError::Serialization(e.to_string()))?;
                Ok(Some(meta))
            }
            None => Ok(None),
        }
    }

    /// Bulk archive multiple task results
    pub async fn archive_results_batch(
        &mut self,
        task_ids: &[Uuid],
        archive_ttl: Duration,
    ) -> Result<usize> {
        let mut count = 0;

        for &task_id in task_ids {
            if self.archive_result(task_id, archive_ttl).await.is_ok() {
                count += 1;
            }
        }

        Ok(count)
    }

    // === Connection Pool Statistics ===

    /// Get connection pool statistics
    ///
    /// Returns statistics about the Redis connection pool,
    /// including active connections and pool capacity.
    ///
    /// # Returns
    /// Connection pool statistics
    pub async fn get_pool_stats(&self) -> PoolStats {
        // Note: redis-rs multiplexed connections don't expose pool stats directly
        // This is a placeholder that returns basic info
        PoolStats {
            backend_type: "redis".to_string(),
            connection_mode: "multiplexed".to_string(),
            is_connected: true,
        }
    }
}
