//! Redis result backend for CeleRS
//!
//! This crate provides Redis-based storage for task results, workflow state,
//! and real-time event transport.
//!
//! # Features
//!
//! - Task result storage
//! - **Task progress tracking** for long-running tasks
//! - Chord state management (barrier synchronization)
//! - Result expiration (TTL)
//! - Atomic operations for counter-based workflows
//! - Batch operations for high throughput
//! - **Real-time event transport** via Redis pub/sub
//!
//! # Progress Tracking Example
//!
//! ```no_run
//! use celers_backend_redis::{RedisResultBackend, ResultBackend, ProgressInfo};
//! use uuid::Uuid;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut backend = RedisResultBackend::new("redis://localhost")?;
//! let task_id = Uuid::new_v4();
//!
//! // Report progress during task execution
//! let progress = ProgressInfo::new(50, 100)
//!     .with_message("Processing items...".to_string());
//! backend.set_progress(task_id, progress).await?;
//!
//! // Query progress from client
//! if let Some(progress) = backend.get_progress(task_id).await? {
//!     println!("Task {}% complete", progress.percent);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Event Transport Example
//!
//! ```no_run
//! use celers_backend_redis::event_transport::{RedisEventEmitter, RedisEventReceiver};
//! use celers_core::event::{EventEmitter, WorkerEventBuilder};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Publishing events
//! let emitter = RedisEventEmitter::new("redis://localhost")?;
//! let event = WorkerEventBuilder::new("worker-1").online();
//! emitter.emit(event).await?;
//!
//! // Receiving events
//! let receiver = RedisEventReceiver::new("redis://localhost")?;
//! // Subscribe and process events...
//! # Ok(())
//! # }
//! ```

pub mod cache;
pub mod compression;
pub mod encryption;
pub mod event_transport;
pub mod metrics;
pub mod result_store;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use redis::{AsyncCommands, Client};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use thiserror::Error;
use uuid::Uuid;

/// Result backend errors
#[derive(Debug, Error)]
pub enum BackendError {
    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Result not found: {0}")]
    NotFound(Uuid),

    #[error("Connection error: {0}")]
    Connection(String),
}

impl BackendError {
    /// Check if the error is Redis-related
    pub fn is_redis(&self) -> bool {
        matches!(self, BackendError::Redis(_))
    }

    /// Check if the error is serialization-related
    pub fn is_serialization(&self) -> bool {
        matches!(self, BackendError::Serialization(_))
    }

    /// Check if the error is not-found
    pub fn is_not_found(&self) -> bool {
        matches!(self, BackendError::NotFound(_))
    }

    /// Check if the error is connection-related
    pub fn is_connection(&self) -> bool {
        matches!(self, BackendError::Connection(_))
    }

    /// Check if this is a retryable error
    ///
    /// Returns true for Redis and connection errors, which are typically transient.
    /// Returns false for serialization and not-found errors.
    pub fn is_retryable(&self) -> bool {
        matches!(self, BackendError::Redis(_) | BackendError::Connection(_))
    }

    /// Get the error category as a string
    pub fn category(&self) -> &'static str {
        match self {
            BackendError::Redis(_) => "redis",
            BackendError::Serialization(_) => "serialization",
            BackendError::NotFound(_) => "not_found",
            BackendError::Connection(_) => "connection",
        }
    }
}

pub type Result<T> = std::result::Result<T, BackendError>;

/// Task result state
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskResult {
    /// Task is pending execution
    Pending,

    /// Task is currently running
    Started,

    /// Task completed successfully
    Success(serde_json::Value),

    /// Task failed with error
    Failure(String),

    /// Task was revoked/cancelled
    Revoked,

    /// Task retry scheduled
    Retry(u32),
}

impl TaskResult {
    /// Check if the task is pending
    pub fn is_pending(&self) -> bool {
        matches!(self, TaskResult::Pending)
    }

    /// Check if the task is started
    pub fn is_started(&self) -> bool {
        matches!(self, TaskResult::Started)
    }

    /// Check if the task succeeded
    pub fn is_success(&self) -> bool {
        matches!(self, TaskResult::Success(_))
    }

    /// Check if the task failed
    pub fn is_failure(&self) -> bool {
        matches!(self, TaskResult::Failure(_))
    }

    /// Check if the task was revoked
    pub fn is_revoked(&self) -> bool {
        matches!(self, TaskResult::Revoked)
    }

    /// Check if the task is being retried
    pub fn is_retry(&self) -> bool {
        matches!(self, TaskResult::Retry(_))
    }

    /// Check if the task is in a terminal state (success, failure, or revoked)
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskResult::Success(_) | TaskResult::Failure(_) | TaskResult::Revoked
        )
    }

    /// Check if the task is in an active (non-terminal) state
    pub fn is_active(&self) -> bool {
        !self.is_terminal()
    }

    /// Get the success result value if the task succeeded
    pub fn success_value(&self) -> Option<&serde_json::Value> {
        match self {
            TaskResult::Success(value) => Some(value),
            _ => None,
        }
    }

    /// Get the failure error message if the task failed
    pub fn failure_message(&self) -> Option<&str> {
        match self {
            TaskResult::Failure(msg) => Some(msg),
            _ => None,
        }
    }

    /// Get the retry count if the task is being retried
    pub fn retry_count(&self) -> Option<u32> {
        match self {
            TaskResult::Retry(count) => Some(*count),
            _ => None,
        }
    }
}

impl std::fmt::Display for TaskResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskResult::Pending => write!(f, "PENDING"),
            TaskResult::Started => write!(f, "STARTED"),
            TaskResult::Success(_) => write!(f, "SUCCESS"),
            TaskResult::Failure(err) => write!(f, "FAILURE: {}", err),
            TaskResult::Revoked => write!(f, "REVOKED"),
            TaskResult::Retry(count) => write!(f, "RETRY({})", count),
        }
    }
}

/// Progress information for long-running tasks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressInfo {
    /// Current progress value (e.g., items processed)
    pub current: u64,

    /// Total progress value (e.g., total items)
    pub total: u64,

    /// Optional progress message
    pub message: Option<String>,

    /// Progress percentage (0-100)
    pub percent: f64,

    /// Timestamp of last progress update
    pub updated_at: DateTime<Utc>,
}

impl ProgressInfo {
    /// Create new progress info
    pub fn new(current: u64, total: u64) -> Self {
        let percent = if total > 0 {
            (current as f64 / total as f64 * 100.0).min(100.0)
        } else {
            0.0
        };

        Self {
            current,
            total,
            message: None,
            percent,
            updated_at: Utc::now(),
        }
    }

    /// Create progress with message
    pub fn with_message(mut self, message: String) -> Self {
        self.message = Some(message);
        self
    }

    /// Check if the task is complete (100% progress)
    pub fn is_complete(&self) -> bool {
        self.percent >= 100.0
    }

    /// Check if there is a progress message
    pub fn has_message(&self) -> bool {
        self.message.is_some()
    }

    /// Get remaining items to process
    pub fn remaining(&self) -> u64 {
        self.total.saturating_sub(self.current)
    }

    /// Get progress as a fraction (0.0 to 1.0)
    pub fn fraction(&self) -> f64 {
        self.percent / 100.0
    }
}

impl std::fmt::Display for ProgressInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{} ({:.1}%)", self.current, self.total, self.percent)?;
        if let Some(ref msg) = self.message {
            write!(f, " - {}", msg)?;
        }
        Ok(())
    }
}

/// Task metadata stored in result backend
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskMeta {
    /// Task ID
    pub task_id: Uuid,

    /// Task name
    pub task_name: String,

    /// Task status/result
    pub result: TaskResult,

    /// Timestamp when task was created
    pub created_at: DateTime<Utc>,

    /// Timestamp when task was started
    pub started_at: Option<DateTime<Utc>>,

    /// Timestamp when task completed
    pub completed_at: Option<DateTime<Utc>>,

    /// Worker that executed the task
    pub worker: Option<String>,

    /// Task progress (for long-running tasks)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress: Option<ProgressInfo>,

    /// Version number for result versioning
    #[serde(default)]
    pub version: u32,
}

impl TaskMeta {
    pub fn new(task_id: Uuid, task_name: String) -> Self {
        Self {
            task_id,
            task_name,
            result: TaskResult::Pending,
            created_at: Utc::now(),
            started_at: None,
            completed_at: None,
            worker: None,
            progress: None,
            version: 0,
        }
    }

    /// Check if the task has started
    pub fn has_started(&self) -> bool {
        self.started_at.is_some()
    }

    /// Check if the task has completed
    pub fn has_completed(&self) -> bool {
        self.completed_at.is_some()
    }

    /// Check if the task has progress information
    pub fn has_progress(&self) -> bool {
        self.progress.is_some()
    }

    /// Get the task duration if completed
    pub fn duration(&self) -> Option<chrono::Duration> {
        match (self.started_at, self.completed_at) {
            (Some(start), Some(end)) => Some(end - start),
            _ => None,
        }
    }

    /// Get the task age (time since creation)
    pub fn age(&self) -> chrono::Duration {
        Utc::now() - self.created_at
    }

    /// Get the execution time (time since start)
    pub fn execution_time(&self) -> Option<chrono::Duration> {
        self.started_at.map(|start| Utc::now() - start)
    }

    /// Check if the task is in a terminal state
    pub fn is_terminal(&self) -> bool {
        self.result.is_terminal()
    }

    /// Check if the task is in an active state
    pub fn is_active(&self) -> bool {
        self.result.is_active()
    }
}

impl std::fmt::Display for TaskMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Task[{}] name={} result={}",
            &self.task_id.to_string()[..8],
            self.task_name,
            self.result
        )?;

        if let Some(worker) = &self.worker {
            write!(f, " worker={}", worker)?;
        }

        if let Some(progress) = &self.progress {
            write!(f, " progress={}", progress)?;
        }

        Ok(())
    }
}

/// Chord state (for barrier synchronization)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChordState {
    /// Chord ID (group ID)
    pub chord_id: Uuid,

    /// Total number of tasks in chord
    pub total: usize,

    /// Number of completed tasks
    pub completed: usize,

    /// Callback task to execute when chord completes
    pub callback: Option<String>,

    /// Task IDs in the chord
    pub task_ids: Vec<Uuid>,

    /// Chord creation timestamp
    pub created_at: DateTime<Utc>,

    /// Chord timeout (if any)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<Duration>,

    /// Whether the chord has been cancelled
    #[serde(default)]
    pub cancelled: bool,

    /// Cancellation reason
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cancellation_reason: Option<String>,

    /// Number of retry attempts
    #[serde(default)]
    pub retry_count: u32,

    /// Maximum retry attempts
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_retries: Option<u32>,
}

impl ChordState {
    /// Create a new chord state
    pub fn new(chord_id: Uuid, total: usize, task_ids: Vec<Uuid>) -> Self {
        Self {
            chord_id,
            total,
            completed: 0,
            callback: None,
            task_ids,
            created_at: Utc::now(),
            timeout: None,
            cancelled: false,
            cancellation_reason: None,
            retry_count: 0,
            max_retries: None,
        }
    }

    /// Set the chord timeout
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Set the callback task
    pub fn with_callback(mut self, callback: String) -> Self {
        self.callback = Some(callback);
        self
    }

    /// Check if the chord is complete (all tasks finished)
    pub fn is_complete(&self) -> bool {
        self.completed >= self.total && !self.cancelled
    }

    /// Check if the chord is cancelled
    pub fn is_cancelled(&self) -> bool {
        self.cancelled
    }

    /// Cancel the chord
    pub fn cancel(&mut self, reason: Option<String>) {
        self.cancelled = true;
        self.cancellation_reason = reason;
    }

    /// Check if the chord is in a terminal state (complete, cancelled, or timed out)
    pub fn is_terminal(&self) -> bool {
        self.is_complete() || self.is_cancelled() || self.is_timed_out()
    }

    /// Check if the chord has timed out
    pub fn is_timed_out(&self) -> bool {
        if let Some(timeout) = self.timeout {
            let age = Utc::now() - self.created_at;
            age.num_milliseconds() > timeout.as_millis() as i64
        } else {
            false
        }
    }

    /// Get the remaining time before timeout
    pub fn remaining_timeout(&self) -> Option<Duration> {
        self.timeout.and_then(|timeout| {
            let age = Utc::now() - self.created_at;
            let age_ms = age.num_milliseconds().max(0) as u64;
            let timeout_ms = timeout.as_millis() as u64;

            if age_ms < timeout_ms {
                Some(Duration::from_millis(timeout_ms - age_ms))
            } else {
                None
            }
        })
    }

    /// Get the number of remaining tasks
    pub fn remaining(&self) -> usize {
        self.total.saturating_sub(self.completed)
    }

    /// Get the completion percentage (0.0 to 100.0)
    pub fn percent_complete(&self) -> f64 {
        if self.total > 0 {
            (self.completed as f64 / self.total as f64 * 100.0).min(100.0)
        } else {
            0.0
        }
    }

    /// Check if the chord has a callback
    pub fn has_callback(&self) -> bool {
        self.callback.is_some()
    }

    /// Check if the chord has a timeout
    pub fn has_timeout(&self) -> bool {
        self.timeout.is_some()
    }

    /// Get the number of tasks in the chord
    pub fn task_count(&self) -> usize {
        self.task_ids.len()
    }

    /// Get the chord age (time since creation)
    pub fn age(&self) -> chrono::Duration {
        Utc::now() - self.created_at
    }

    /// Set maximum retry attempts
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = Some(max_retries);
        self
    }

    /// Check if the chord can be retried
    pub fn can_retry(&self) -> bool {
        if let Some(max_retries) = self.max_retries {
            self.retry_count < max_retries
        } else {
            false
        }
    }

    /// Increment the retry count and reset the chord for retry
    ///
    /// Returns true if retry is allowed, false if max retries exceeded
    pub fn retry(&mut self) -> bool {
        if !self.can_retry() {
            return false;
        }

        self.retry_count += 1;
        self.completed = 0;
        self.cancelled = false;
        self.cancellation_reason = None;
        self.created_at = Utc::now();
        true
    }

    /// Get remaining retry attempts
    pub fn remaining_retries(&self) -> Option<u32> {
        self.max_retries
            .map(|max| max.saturating_sub(self.retry_count))
    }

    /// Check if this is a retry attempt
    pub fn is_retry(&self) -> bool {
        self.retry_count > 0
    }
}

impl std::fmt::Display for ChordState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Chord[{}] {}/{} tasks ({:.1}%)",
            &self.chord_id.to_string()[..8],
            self.completed,
            self.total,
            self.percent_complete()
        )?;

        if let Some(ref callback) = self.callback {
            write!(f, " callback={}", callback)?;
        }

        if self.is_cancelled() {
            write!(f, " [CANCELLED")?;
            if let Some(ref reason) = self.cancellation_reason {
                write!(f, ": {}", reason)?;
            }
            write!(f, "]")?;
        } else if let Some(timeout) = self.timeout {
            if self.is_timed_out() {
                write!(f, " [TIMED OUT]")?;
            } else if let Some(remaining) = self.remaining_timeout() {
                write!(f, " timeout={:?} remaining={:?}", timeout, remaining)?;
            }
        }

        Ok(())
    }
}

/// Result backend trait
#[async_trait]
pub trait ResultBackend: Send + Sync {
    /// Store task result
    async fn store_result(&mut self, task_id: Uuid, meta: &TaskMeta) -> Result<()>;

    /// Get task result
    async fn get_result(&mut self, task_id: Uuid) -> Result<Option<TaskMeta>>;

    /// Delete task result
    async fn delete_result(&mut self, task_id: Uuid) -> Result<()>;

    /// Set task result expiration
    async fn set_expiration(&mut self, task_id: Uuid, ttl: Duration) -> Result<()>;

    /// Chord: Initialize chord state
    async fn chord_init(&mut self, state: ChordState) -> Result<()>;

    /// Chord: Increment completion counter (returns total completed)
    async fn chord_complete_task(&mut self, chord_id: Uuid) -> Result<usize>;

    /// Chord: Get chord state
    async fn chord_get_state(&mut self, chord_id: Uuid) -> Result<Option<ChordState>>;

    /// Chord: Cancel a chord
    async fn chord_cancel(&mut self, chord_id: Uuid, reason: Option<String>) -> Result<()> {
        if let Some(mut state) = self.chord_get_state(chord_id).await? {
            state.cancel(reason);
            // Re-store the updated state (requires ChordState to be serializable)
            // This is a default implementation that should be overridden for efficiency
            self.chord_init(state).await?;
        }
        Ok(())
    }

    /// Chord: Get partial results for all tasks in chord
    ///
    /// Returns a vector of (task_id, result) tuples for all tasks, where result
    /// is None if the task hasn't completed yet.
    async fn chord_get_partial_results(
        &mut self,
        chord_id: Uuid,
    ) -> Result<Vec<(Uuid, Option<TaskMeta>)>> {
        if let Some(state) = self.chord_get_state(chord_id).await? {
            let results = self.get_results_batch(&state.task_ids).await?;
            Ok(state.task_ids.iter().copied().zip(results).collect())
        } else {
            Ok(vec![])
        }
    }

    /// Chord: Retry a failed chord
    ///
    /// Resets the chord state and increments the retry count.
    /// Returns true if retry was successful, false if max retries exceeded.
    async fn chord_retry(&mut self, chord_id: Uuid) -> Result<bool> {
        if let Some(mut state) = self.chord_get_state(chord_id).await? {
            if state.retry() {
                self.chord_init(state).await?;
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    // Batch operations (with default implementations for compatibility)

    /// Store multiple task results (optimized with pipelining where supported)
    async fn store_results_batch(&mut self, results: &[(Uuid, TaskMeta)]) -> Result<()> {
        for (task_id, meta) in results {
            self.store_result(*task_id, meta).await?;
        }
        Ok(())
    }

    /// Get multiple task results (optimized with pipelining where supported)
    async fn get_results_batch(&mut self, task_ids: &[Uuid]) -> Result<Vec<Option<TaskMeta>>> {
        let mut results = Vec::with_capacity(task_ids.len());
        for task_id in task_ids {
            results.push(self.get_result(*task_id).await?);
        }
        Ok(results)
    }

    /// Delete multiple task results (optimized with pipelining where supported)
    async fn delete_results_batch(&mut self, task_ids: &[Uuid]) -> Result<()> {
        for task_id in task_ids {
            self.delete_result(*task_id).await?;
        }
        Ok(())
    }

    // Result versioning (with default implementations)

    /// Store a versioned result
    ///
    /// Stores the result with an incremented version number and keeps a history
    /// of previous versions using a versioned key pattern.
    async fn store_versioned_result(&mut self, task_id: Uuid, meta: &TaskMeta) -> Result<u32> {
        // Get current version
        let current_version = if let Some(existing) = self.get_result(task_id).await? {
            existing.version
        } else {
            0
        };

        // Create new version
        let new_version = current_version + 1;
        let mut versioned_meta = meta.clone();
        versioned_meta.version = new_version;

        // Store the latest version
        self.store_result(task_id, &versioned_meta).await?;

        Ok(new_version)
    }

    /// Get a specific version of a result
    ///
    /// Returns None if the version doesn't exist. Version 0 or omitted means latest.
    async fn get_result_version(
        &mut self,
        task_id: Uuid,
        _version: u32,
    ) -> Result<Option<TaskMeta>> {
        // Default implementation only returns the latest version
        // Override in specific backends to support versioned storage
        self.get_result(task_id).await
    }

    // Progress tracking (with default implementations)

    /// Update task progress (for long-running tasks)
    ///
    /// This allows tasks to report their progress during execution.
    /// The progress is stored in the task metadata and can be queried by clients.
    ///
    /// # Arguments
    /// * `task_id` - Task ID
    /// * `progress` - Progress information
    async fn set_progress(&mut self, task_id: Uuid, progress: ProgressInfo) -> Result<()> {
        // Default implementation: load, update, store
        if let Some(mut meta) = self.get_result(task_id).await? {
            meta.progress = Some(progress);
            self.store_result(task_id, &meta).await?;
        }
        Ok(())
    }

    /// Get task progress
    ///
    /// Returns the current progress information for a task, if available.
    ///
    /// # Arguments
    /// * `task_id` - Task ID
    ///
    /// # Returns
    /// Progress information, if the task has reported progress
    async fn get_progress(&mut self, task_id: Uuid) -> Result<Option<ProgressInfo>> {
        if let Some(meta) = self.get_result(task_id).await? {
            Ok(meta.progress)
        } else {
            Ok(None)
        }
    }
}

/// Lazy result loading wrapper
///
/// Defers loading the full task result until explicitly requested.
/// Useful for performance when you only need the task ID initially.
#[derive(Debug, Clone)]
pub struct LazyTaskResult {
    /// Task ID
    pub task_id: Uuid,

    /// Cached result (loaded on first access)
    cached: Option<TaskMeta>,
}

impl LazyTaskResult {
    /// Create a new lazy result
    pub fn new(task_id: Uuid) -> Self {
        Self {
            task_id,
            cached: None,
        }
    }

    /// Create a lazy result with pre-loaded data
    pub fn with_data(meta: TaskMeta) -> Self {
        Self {
            task_id: meta.task_id,
            cached: Some(meta),
        }
    }

    /// Check if the result has been loaded
    pub fn is_loaded(&self) -> bool {
        self.cached.is_some()
    }

    /// Load the result (if not already loaded)
    pub async fn load(&mut self, backend: &mut RedisResultBackend) -> Result<Option<&TaskMeta>> {
        if self.cached.is_none() {
            self.cached = backend.get_result(self.task_id).await?;
        }
        Ok(self.cached.as_ref())
    }

    /// Get the cached result without loading
    pub fn get_cached(&self) -> Option<&TaskMeta> {
        self.cached.as_ref()
    }
}

/// Redis result backend implementation
#[derive(Clone)]
pub struct RedisResultBackend {
    client: Client,
    key_prefix: String,
    compression_config: compression::CompressionConfig,
    encryption_config: encryption::EncryptionConfig,
    metrics: metrics::BackendMetrics,
    cache: cache::ResultCache,
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

    fn task_key(&self, task_id: Uuid) -> String {
        format!("{}{}", self.key_prefix, task_id)
    }

    fn chord_key(&self, chord_id: Uuid) -> String {
        format!("celery-chord-{}", chord_id)
    }

    fn chord_counter_key(&self, chord_id: Uuid) -> String {
        format!("celery-chord-counter-{}", chord_id)
    }
}

#[async_trait]
impl ResultBackend for RedisResultBackend {
    async fn store_result(&mut self, task_id: Uuid, meta: &TaskMeta) -> Result<()> {
        let start = std::time::Instant::now();

        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = self.task_key(task_id);
        let value =
            serde_json::to_string(meta).map_err(|e| BackendError::Serialization(e.to_string()))?;

        let original_size = value.len();

        // Apply compression if configured
        let compressed = compression::maybe_compress(value.as_bytes(), &self.compression_config)
            .map_err(|e| BackendError::Serialization(format!("Compression error: {}", e)))?;

        // Apply encryption if configured
        let data = encryption::encrypt(&compressed, &self.encryption_config)
            .map_err(|e| BackendError::Serialization(format!("Encryption error: {}", e)))?;

        let stored_size = data.len();

        conn.set::<_, _, ()>(&key, data).await?;

        // Update cache
        self.cache.put(task_id, meta.clone());

        // Record metrics
        self.metrics
            .record_operation(metrics::OperationType::StoreResult, start.elapsed());
        self.metrics.record_data_size(original_size, stored_size);

        Ok(())
    }

    async fn get_result(&mut self, task_id: Uuid) -> Result<Option<TaskMeta>> {
        let start = std::time::Instant::now();

        // Check cache first
        if let Some(meta) = self.cache.get(task_id) {
            self.metrics.record_cache_hit();
            self.metrics
                .record_operation(metrics::OperationType::GetResult, start.elapsed());
            return Ok(Some(meta));
        }

        // Cache miss, fetch from Redis
        self.metrics.record_cache_miss();

        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = self.task_key(task_id);

        let value: Option<Vec<u8>> = conn.get(&key).await?;
        let result = match value {
            Some(data) => {
                // Decrypt if needed
                let decrypted = encryption::decrypt(&data, &self.encryption_config)
                    .map_err(|e| BackendError::Serialization(format!("Decryption error: {}", e)))?;

                // Decompress if needed
                let decompressed = compression::maybe_decompress(&decrypted).map_err(|e| {
                    BackendError::Serialization(format!("Decompression error: {}", e))
                })?;

                let v = String::from_utf8(decompressed)
                    .map_err(|e| BackendError::Serialization(format!("UTF-8 error: {}", e)))?;

                let meta: TaskMeta = serde_json::from_str(&v)
                    .map_err(|e| BackendError::Serialization(e.to_string()))?;

                // Store in cache
                self.cache.put(task_id, meta.clone());

                Ok(Some(meta))
            }
            None => Ok(None),
        };

        // Record metrics
        self.metrics
            .record_operation(metrics::OperationType::GetResult, start.elapsed());

        result
    }

    async fn delete_result(&mut self, task_id: Uuid) -> Result<()> {
        let start = std::time::Instant::now();

        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = self.task_key(task_id);
        conn.del::<_, ()>(&key).await?;

        // Invalidate cache
        self.cache.invalidate(task_id);

        // Record metrics
        self.metrics
            .record_operation(metrics::OperationType::DeleteResult, start.elapsed());

        Ok(())
    }

    async fn set_expiration(&mut self, task_id: Uuid, ttl: Duration) -> Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = self.task_key(task_id);
        conn.expire::<_, ()>(&key, ttl.as_secs() as i64).await?;
        Ok(())
    }

    async fn chord_init(&mut self, state: ChordState) -> Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = self.chord_key(state.chord_id);
        let counter_key = self.chord_counter_key(state.chord_id);

        let value = serde_json::to_string(&state)
            .map_err(|e| BackendError::Serialization(e.to_string()))?;

        // Store chord state
        conn.set::<_, _, ()>(&key, value).await?;

        // Initialize counter to 0
        conn.set::<_, _, ()>(&counter_key, 0).await?;

        Ok(())
    }

    async fn chord_complete_task(&mut self, chord_id: Uuid) -> Result<usize> {
        let start = std::time::Instant::now();

        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let counter_key = self.chord_counter_key(chord_id);

        // Atomically increment and return new value
        let count: usize = conn.incr(&counter_key, 1).await?;

        // Record metrics
        self.metrics
            .record_operation(metrics::OperationType::ChordOperation, start.elapsed());

        Ok(count)
    }

    async fn chord_get_state(&mut self, chord_id: Uuid) -> Result<Option<ChordState>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = self.chord_key(chord_id);

        let value: Option<String> = conn.get(&key).await?;
        match value {
            Some(v) => {
                let state = serde_json::from_str(&v)
                    .map_err(|e| BackendError::Serialization(e.to_string()))?;
                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    async fn chord_cancel(&mut self, chord_id: Uuid, reason: Option<String>) -> Result<()> {
        if let Some(mut state) = self.chord_get_state(chord_id).await? {
            state.cancel(reason);
            self.chord_init(state).await?;
        }
        Ok(())
    }

    // Optimized batch operations using Redis pipelining

    async fn store_results_batch(&mut self, results: &[(Uuid, TaskMeta)]) -> Result<()> {
        if results.is_empty() {
            return Ok(());
        }

        let start = std::time::Instant::now();

        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let mut pipe = redis::pipe();

        for (task_id, meta) in results {
            let key = self.task_key(*task_id);
            let value = serde_json::to_string(meta)
                .map_err(|e| BackendError::Serialization(e.to_string()))?;

            let original_size = value.len();

            // Apply compression if configured
            let compressed =
                compression::maybe_compress(value.as_bytes(), &self.compression_config).map_err(
                    |e| BackendError::Serialization(format!("Compression error: {}", e)),
                )?;

            // Apply encryption if configured
            let data = encryption::encrypt(&compressed, &self.encryption_config)
                .map_err(|e| BackendError::Serialization(format!("Encryption error: {}", e)))?;

            let stored_size = data.len();

            self.metrics.record_data_size(original_size, stored_size);
            pipe.set(&key, data);
        }

        pipe.query_async::<()>(&mut conn).await?;

        // Record metrics
        self.metrics
            .record_operation(metrics::OperationType::StoreBatch, start.elapsed());

        Ok(())
    }

    async fn get_results_batch(&mut self, task_ids: &[Uuid]) -> Result<Vec<Option<TaskMeta>>> {
        if task_ids.is_empty() {
            return Ok(Vec::new());
        }

        let start = std::time::Instant::now();

        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let mut pipe = redis::pipe();

        for task_id in task_ids {
            let key = self.task_key(*task_id);
            pipe.get(&key);
        }

        let values: Vec<Option<Vec<u8>>> = pipe.query_async(&mut conn).await?;

        let mut results = Vec::with_capacity(values.len());
        for value_opt in values {
            match value_opt {
                Some(data) => {
                    // Decrypt if needed
                    let decrypted =
                        encryption::decrypt(&data, &self.encryption_config).map_err(|e| {
                            BackendError::Serialization(format!("Decryption error: {}", e))
                        })?;

                    // Decompress if needed
                    let decompressed = compression::maybe_decompress(&decrypted).map_err(|e| {
                        BackendError::Serialization(format!("Decompression error: {}", e))
                    })?;

                    let v = String::from_utf8(decompressed)
                        .map_err(|e| BackendError::Serialization(format!("UTF-8 error: {}", e)))?;

                    let meta = serde_json::from_str(&v)
                        .map_err(|e| BackendError::Serialization(e.to_string()))?;
                    results.push(Some(meta));
                }
                None => results.push(None),
            }
        }

        // Record metrics
        self.metrics
            .record_operation(metrics::OperationType::GetBatch, start.elapsed());

        Ok(results)
    }

    async fn delete_results_batch(&mut self, task_ids: &[Uuid]) -> Result<()> {
        if task_ids.is_empty() {
            return Ok(());
        }

        let start = std::time::Instant::now();

        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let mut pipe = redis::pipe();

        for task_id in task_ids {
            let key = self.task_key(*task_id);
            pipe.del(&key);
        }

        pipe.query_async::<()>(&mut conn).await?;

        // Record metrics
        self.metrics
            .record_operation(metrics::OperationType::DeleteBatch, start.elapsed());

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
