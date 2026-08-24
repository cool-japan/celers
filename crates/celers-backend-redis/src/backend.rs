//! Redis result backend implementation
//!
//! Contains the [`RedisResultBackend`] struct with all inherent methods for
//! Redis-based task result storage, including builder methods, convenience
//! methods, query operations, transaction support, and archival.

use chrono::Utc;
use futures_util::stream::StreamExt;
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Client};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::OnceCell;
use uuid::Uuid;

use crate::codec;
use crate::result_backend_trait::{ResultBackend, ResultStream};
use crate::stats::{ttl, BackendStats, BatchOperationResult, PoolStats, StateCount, TaskSummary};
use crate::types::{BackendError, ProgressInfo, Result, TaskMeta, TaskResult, TaskTtlConfig};
use crate::{cache, chunking, compression, encryption, metrics, pipeline, retry, telemetry};

/// Configuration for versioned result history.
///
/// When enabled, every call to
/// [`store_versioned_result`](crate::ResultBackend::store_versioned_result)
/// keeps a copy of the payload under `{task_key}:v{n}` so previous versions can
/// be retrieved with
/// [`get_result_version`](crate::ResultBackend::get_result_version).
#[derive(Debug, Clone)]
pub struct VersioningConfig {
    /// Whether versioned history is retained.
    pub enabled: bool,
    /// Maximum number of historical versions to keep (older ones are deleted).
    pub max_versions: u32,
}

impl Default for VersioningConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_versions: 10,
        }
    }
}

impl VersioningConfig {
    /// Create a configuration with the default retention (10 versions).
    pub fn new() -> Self {
        Self::default()
    }

    /// Disable versioned history entirely.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            max_versions: 0,
        }
    }

    /// Set how many historical versions are retained.
    pub fn with_max_versions(mut self, max_versions: u32) -> Self {
        self.max_versions = max_versions;
        self
    }
}

/// Redis result backend implementation
#[derive(Clone)]
pub struct RedisResultBackend {
    pub(crate) client: Client,
    /// Lazily-established reconnecting connection, shared by every clone.
    pub(crate) connection: Arc<OnceCell<ConnectionManager>>,
    pub(crate) key_prefix: String,
    pub(crate) compression_config: compression::CompressionConfig,
    pub(crate) encryption_config: encryption::EncryptionConfig,
    pub(crate) metrics: metrics::BackendMetrics,
    pub(crate) cache: cache::ResultCache,
    pub(crate) ttl_config: TaskTtlConfig,
    /// Shared (interior-mutable) so statistics survive the `clone()` performed
    /// by the `ResultStore` adapter on every call.
    pub(crate) compression_stats: Arc<compression::CompressionStats>,
    pub(crate) chunking_config: chunking::ChunkingConfig,
    pub(crate) chunker: chunking::ResultChunker,
    pub(crate) pipeline_config: pipeline::PipelineConfig,
    pub(crate) retry_strategy: retry::RetryStrategy,
    pub(crate) telemetry: Option<Arc<dyn telemetry::TelemetryHook>>,
    pub(crate) versioning: VersioningConfig,
    /// Publish a pub/sub notification on every result write so waiters do not
    /// have to poll.
    pub(crate) notify_on_store: bool,
}

impl RedisResultBackend {
    /// Create a backend against `url`.
    ///
    /// Results expire after [`ttl::SUCCESS`] (24 hours) by default, matching
    /// Celery's `result_expires`. Use [`Self::without_ttl`] for permanent
    /// results or [`Self::with_ttl_config`] for per-task-type expiry.
    pub fn new(url: &str) -> Result<Self> {
        let client = Client::open(url).map_err(|e| {
            BackendError::Connection(format!("Failed to create Redis client: {}", e))
        })?;

        Ok(Self {
            client,
            connection: Arc::new(OnceCell::new()),
            key_prefix: "celery-task-meta-".to_string(),
            compression_config: compression::CompressionConfig::default(),
            encryption_config: encryption::EncryptionConfig::disabled(),
            metrics: metrics::BackendMetrics::new(),
            cache: cache::ResultCache::new(cache::CacheConfig::default()),
            ttl_config: TaskTtlConfig::with_default(ttl::SUCCESS),
            compression_stats: Arc::new(compression::CompressionStats::new()),
            chunking_config: chunking::ChunkingConfig::default(),
            chunker: chunking::ResultChunker::new(chunking::ChunkingConfig::default()),
            pipeline_config: pipeline::PipelineConfig::default(),
            retry_strategy: retry::RetryStrategy::default(),
            telemetry: None,
            versioning: VersioningConfig::default(),
            notify_on_store: true,
        })
    }

    pub fn with_prefix(mut self, prefix: String) -> Self {
        self.key_prefix = prefix;
        self
    }

    /// Store results permanently (no expiry).
    ///
    /// Overrides the 24-hour default installed by [`Self::new`]. Note that
    /// without a TTL, Redis memory grows with every task the deployment ever
    /// runs unless results are removed explicitly.
    pub fn without_ttl(mut self) -> Self {
        self.ttl_config = TaskTtlConfig::new();
        self
    }

    /// Configure Redis pipelining and the per-command timeout.
    pub fn with_pipeline_config(mut self, config: pipeline::PipelineConfig) -> Self {
        self.pipeline_config = config;
        self
    }

    /// Get the pipeline configuration.
    pub fn pipeline_config(&self) -> &pipeline::PipelineConfig {
        &self.pipeline_config
    }

    /// Configure how transient Redis failures are retried.
    pub fn with_retry_strategy(mut self, strategy: retry::RetryStrategy) -> Self {
        self.retry_strategy = strategy;
        self
    }

    /// Disable automatic retries of transient Redis failures.
    pub fn without_retries(mut self) -> Self {
        self.retry_strategy = retry::RetryStrategy::new().with_max_attempts(1);
        self
    }

    /// Get the retry strategy.
    pub fn retry_strategy(&self) -> &retry::RetryStrategy {
        &self.retry_strategy
    }

    /// Register a telemetry hook fired around every backend operation.
    pub fn with_telemetry_hook(mut self, hook: Arc<dyn telemetry::TelemetryHook>) -> Self {
        self.telemetry = Some(hook);
        self
    }

    /// Get the registered telemetry hook, if any.
    pub fn telemetry_hook(&self) -> Option<&Arc<dyn telemetry::TelemetryHook>> {
        self.telemetry.as_ref()
    }

    /// Configure versioned result history.
    pub fn with_versioning(mut self, config: VersioningConfig) -> Self {
        self.versioning = config;
        self
    }

    /// Get the versioning configuration.
    pub fn versioning_config(&self) -> &VersioningConfig {
        &self.versioning
    }

    /// Enable or disable the pub/sub notification published on every write.
    ///
    /// The notification is what lets
    /// [`wait_for_result`](Self::wait_for_result) return as soon as a result
    /// lands instead of waiting for the next poll.
    pub fn with_store_notifications(mut self, enabled: bool) -> Self {
        self.notify_on_store = enabled;
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
    ///
    /// The counters live behind an `Arc`, so they are shared by every clone of
    /// this backend — including the short-lived clones the `ResultStore`
    /// adapter makes per call.
    pub fn compression_stats(&self) -> &compression::CompressionStats {
        &self.compression_stats
    }

    /// Get a connection to Redis.
    ///
    /// A single [`ConnectionManager`] is established lazily and shared by every
    /// clone of this backend. It reconnects transparently, so callers never
    /// hold on to a dead socket, and a `get_result` no longer costs a fresh TCP
    /// connect + handshake per call.
    pub(crate) async fn connection(&self) -> Result<ConnectionManager> {
        self.connection
            .get_or_try_init(|| async {
                ConnectionManager::new(self.client.clone())
                    .await
                    .map_err(BackendError::from)
            })
            .await
            .cloned()
    }

    /// Run `future` under the configured command timeout, if one is set.
    pub(crate) async fn with_timeout<T, F>(&self, what: &str, future: F) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        match self.pipeline_config.timeout {
            Some(limit) => match tokio::time::timeout(limit, future).await {
                Ok(result) => result,
                Err(_) => Err(BackendError::Connection(format!(
                    "Redis operation `{}` timed out after {:?}",
                    what, limit
                ))),
            },
            None => future.await,
        }
    }

    pub(crate) fn task_key(&self, task_id: Uuid) -> String {
        format!("{}{}", self.key_prefix, task_id)
    }

    /// Build a `SCAN`/`KEYS` pattern for task keys from a suffix pattern.
    ///
    /// [`find_tasks_by_pattern`](Self::find_tasks_by_pattern) prepends the key
    /// prefix itself, so callers must pass a *suffix* only. This helper exists
    /// so the composition is testable without a live Redis.
    pub(crate) fn scan_pattern(&self, suffix: &str) -> String {
        format!("{}{}", self.key_prefix, suffix)
    }

    /// Pattern matching every task key managed by this backend.
    ///
    /// Note the leading prefix is supplied by `find_tasks_by_pattern`, so this
    /// is deliberately just `"*"`.
    pub(crate) const ALL_TASKS_PATTERN: &'static str = "*";

    pub(crate) fn archive_key(&self, task_id: Uuid) -> String {
        format!("{}archive:{}", self.key_prefix, task_id)
    }

    pub(crate) fn version_key(&self, task_id: Uuid, version: u32) -> String {
        format!("{}{}:v{}", self.key_prefix, task_id, version)
    }

    pub(crate) fn version_counter_key(&self, task_id: Uuid) -> String {
        format!("{}{}:version", self.key_prefix, task_id)
    }

    /// Pub/sub channel a result write is announced on.
    pub(crate) fn notify_channel(&self, task_id: Uuid) -> String {
        format!("{}{}:notify", self.key_prefix, task_id)
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
        let mut conn = self.connection().await?;
        let result: String = redis::cmd("PING").query_async(&mut conn).await?;
        Ok(result == "PONG")
    }

    /// Scan keys matching a pattern using SCAN (production-safe, non-blocking)
    ///
    /// Uses Redis SCAN command instead of KEYS for safe iteration in production.
    pub(crate) async fn scan_keys(&mut self, pattern: &str) -> Result<Vec<String>> {
        let mut conn = self.connection().await?;
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
        let mut conn = self.connection().await?;

        // Count task result keys using SCAN (production-safe).
        // Only keys whose suffix is a task UUID are real results — chunk,
        // version, archive and dependency keys share the prefix.
        let task_pattern = self.scan_pattern("*");
        let task_keys = self.scan_keys(&task_pattern).await?;
        let task_key_count = task_keys
            .iter()
            .filter(|key| {
                key.strip_prefix(&self.key_prefix)
                    .is_some_and(|suffix| Uuid::parse_str(suffix).is_ok())
            })
            .count();

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
        let pattern = self.scan_pattern("*");
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
        let mut conn = self.connection().await?;
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
        // One atomic write with `SET ... EX`: a crash can never leave the
        // result stored without its expiry.
        let key = self.task_key(task_id);
        self.write_meta_to_key(&key, meta, Some(ttl), None).await?;
        self.cache_terminal(task_id, meta);
        self.publish_notification(task_id, meta).await;
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
        // `atomic_store_multiple` already applies the TTL inside the same
        // write, covering chunk keys as well.
        self.atomic_store_multiple(results, Some(ttl)).await
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

        // The expire command covers the chunk and chunk-metadata keys too, so a
        // chunked result cannot end up with a bare, never-expiring body.
        let commands: Vec<redis::Cmd> = task_ids
            .iter()
            .map(|id| codec::expire_command(&self.task_key(*id), ttl))
            .collect();

        self.run_with_retry("set_multiple_expirations", |mut conn| {
            let commands = commands.clone();
            async move {
                let mut pipe = redis::pipe();
                for command in commands {
                    pipe.add_command(command);
                }
                pipe.query_async::<Vec<i64>>(&mut conn).await?;
                Ok(())
            }
        })
        .await
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
        let mut conn = self.connection().await?;
        let key = self.task_key(task_id);
        let exists: bool = conn.exists(&key).await?;
        Ok(exists)
    }

    /// Check existence for multiple tasks in a batch
    ///
    /// Returns a vector of booleans indicating whether each task exists.
    pub async fn tasks_exist_batch(&mut self, task_ids: &[Uuid]) -> Result<Vec<bool>> {
        let mut conn = self.connection().await?;
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
        let mut conn = self.connection().await?;
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
        self.set_expiration(task_id, ttl).await
    }

    /// Refresh TTL for multiple tasks at once
    ///
    /// Efficiently updates TTL for multiple tasks using pipelining.
    /// Returns the number of tasks that had their TTL updated.
    pub async fn refresh_ttl_batch(&mut self, task_ids: &[Uuid], ttl: Duration) -> Result<usize> {
        if task_ids.is_empty() {
            return Ok(0);
        }

        let commands: Vec<redis::Cmd> = task_ids
            .iter()
            .map(|id| codec::expire_command(&self.task_key(*id), ttl))
            .collect();

        let results: Vec<i64> = self
            .run_with_retry("refresh_ttl_batch", |mut conn| {
                let commands = commands.clone();
                async move {
                    let mut pipe = redis::pipe();
                    for command in commands {
                        pipe.add_command(command);
                    }
                    Ok(pipe.query_async(&mut conn).await?)
                }
            })
            .await?;

        Ok(results.iter().filter(|&&applied| applied == 1).count())
    }

    /// Remove TTL from a task (make it persistent)
    ///
    /// The task will no longer expire automatically.
    pub async fn persist_task(&mut self, task_id: Uuid) -> Result<()> {
        let mut conn = self.connection().await?;
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
        if results.is_empty() {
            return Ok(());
        }

        let mut commands = Vec::with_capacity(results.len());

        for (task_id, meta) in results {
            let key = self.task_key(*task_id);
            // Same encode pipeline as every other write path: compression and
            // encryption are never bypassed, and oversized payloads chunk.
            let encoded = codec::encode_meta(
                meta,
                &self.compression_config,
                &self.encryption_config,
                &self.chunker,
            )
            .inspect_err(|e| self.record_failure(e))?;
            self.metrics
                .record_data_size(encoded.original_size, encoded.stored_size);
            self.compression_stats
                .record(encoded.original_size, encoded.stored_size);

            // An explicit `ttl` wins; otherwise fall back to the configured
            // per-task-type policy so atomic writes expire like normal ones.
            let effective_ttl = ttl.or_else(|| self.ttl_config.get_ttl(&meta.task_name));
            commands.push(codec::write_command(&key, &encoded, effective_ttl, None));
        }

        self.run_with_retry("atomic_store_multiple", |mut conn| {
            let commands = commands.clone();
            async move {
                let mut pipe = redis::pipe();
                pipe.atomic();
                for command in commands {
                    pipe.add_command(command);
                }
                pipe.query_async::<Vec<i64>>(&mut conn).await?;
                Ok(())
            }
        })
        .await?;

        for (task_id, meta) in results {
            self.cache_terminal(*task_id, meta);
        }

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
        if task_ids.is_empty() {
            return Ok(0);
        }

        let commands: Vec<redis::Cmd> = task_ids
            .iter()
            .map(|id| codec::delete_command(&self.task_key(*id)))
            .collect();

        let deleted: Vec<i64> = self
            .run_with_retry("atomic_delete_multiple", |mut conn| {
                let commands = commands.clone();
                async move {
                    let mut pipe = redis::pipe();
                    pipe.atomic();
                    for command in commands {
                        pipe.add_command(command);
                    }
                    Ok(pipe.query_async(&mut conn).await?)
                }
            })
            .await?;

        // Deleted results must stop being served from the cache.
        for task_id in task_ids {
            self.cache.invalidate(*task_id);
        }

        Ok(deleted.iter().sum::<i64>().max(0) as usize)
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
        let mut conn = self.connection().await?;
        let result = redis::Script::new(script)
            .key(keys)
            .arg(args)
            .invoke_async(&mut conn)
            .await?;
        Ok(result)
    }

    /// Compare-and-swap operation
    ///
    /// Atomically updates a task result only if the currently stored result
    /// equals `expected`.
    ///
    /// The comparison is done in two steps so it stays correct with compression,
    /// encryption and chunking enabled: the stored bytes are read and decoded,
    /// the decoded value is compared with `expected`, and the write is then
    /// guarded server-side on those exact bytes still being present. A
    /// concurrent writer therefore always loses the race instead of being
    /// silently overwritten.
    ///
    /// # Returns
    /// - `Ok(true)` if the swap was performed
    /// - `Ok(false)` if the stored result did not match `expected`, or another
    ///   writer modified the key in between (retry in that case)
    ///
    /// # Note
    /// With encryption enabled the guard is stricter than semantic equality:
    /// AES-GCM uses a fresh nonce per write, so a concurrent rewrite of the
    /// *same* logical value still invalidates the guard. That yields a spurious
    /// `false` (safe), never a spurious `true`.
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

        // Read the raw bytes currently stored; they double as the optimistic
        // concurrency token handed to the guarded write.
        let Some(raw) = self.read_raw(&key).await? else {
            return Ok(false);
        };

        // The stored representation may be a chunk sentinel, so decode through
        // the normal read path before comparing.
        let mut current = self
            .read_metas_from_keys(std::slice::from_ref(&key))
            .await?;
        let Some(current) = current.pop().flatten() else {
            return Ok(false);
        };

        if &current != expected {
            return Ok(false);
        }

        let ttl = self.ttl_config.get_ttl(&new_value.task_name);
        let swapped = self
            .write_meta_to_key(&key, new_value, ttl, Some(&raw))
            .await?;

        if swapped {
            self.cache_terminal(task_id, new_value);
        }

        Ok(swapped)
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
        let mut conn = self.connection().await?;
        // `pattern` is a *suffix*: the key prefix is supplied here. Callers
        // must not repeat it, or the composed pattern matches nothing.
        let full_pattern = self.scan_pattern(pattern);

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
        let mut conn = self.connection().await?;
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
        let mut conn = self.connection().await?;
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
        let mut conn = self.connection().await?;
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
        let mut conn = self.connection().await?;
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

    // === Batch Operations With Error Detail ===

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

    // === Connection Pool Statistics ===

    /// Get connection pool statistics
    ///
    /// Returns statistics about the Redis connection pool,
    /// including active connections and pool capacity.
    ///
    /// # Returns
    /// Connection pool statistics
    pub async fn get_pool_stats(&self) -> PoolStats {
        let is_connected = match self.connection().await {
            Ok(mut conn) => {
                let pong: std::result::Result<String, _> =
                    redis::cmd("PING").query_async(&mut conn).await;
                pong.is_ok()
            }
            Err(_) => false,
        };

        PoolStats {
            backend_type: "redis".to_string(),
            connection_mode: "multiplexed".to_string(),
            is_connected,
        }
    }
}
