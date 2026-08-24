//! `AsyncResult` API for querying task results
//!
//! This module provides a Celery-compatible interface for retrieving task results,
//! checking task state, and waiting for task completion.
//!
//! # Example
//!
//! ```no_run
//! use celers_core::{AsyncResult, ResultStore, TaskId, TaskResultValue};
//! use uuid::Uuid;
//! use std::time::Duration;
//! # use async_trait::async_trait;
//! #
//! # #[derive(Clone)]
//! # struct MockBackend;
//! #
//! # #[async_trait]
//! # impl ResultStore for MockBackend {
//! #     async fn store_result(&self, _: TaskId, _: TaskResultValue) -> celers_core::Result<()> { Ok(()) }
//! #     async fn get_result(&self, _: TaskId) -> celers_core::Result<Option<TaskResultValue>> { Ok(None) }
//! #     async fn get_state(&self, _: TaskId) -> celers_core::Result<celers_core::TaskState> { Ok(celers_core::TaskState::Pending) }
//! #     async fn forget(&self, _: TaskId) -> celers_core::Result<()> { Ok(()) }
//! #     async fn has_result(&self, _: TaskId) -> celers_core::Result<bool> { Ok(false) }
//! # }
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let task_id: TaskId = Uuid::new_v4();
//! let backend = MockBackend; // Use your actual backend (Redis, Database, etc.)
//! let result = AsyncResult::new(task_id, backend);
//!
//! // Check if the task is ready
//! if result.ready().await? {
//!     // Get the result
//!     if result.successful().await? {
//!         let value = result.get(Some(Duration::from_secs(30))).await?;
//!         println!("Task succeeded: {:?}", value);
//!     } else {
//!         println!("Task failed");
//!     }
//! }
//! # Ok(())
//! # }
//! ```

use crate::state::TaskState;
use crate::TaskId;
use async_trait::async_trait;
use serde_json::Value;
use std::time::Duration;

/// Result store trait for `AsyncResult` API
///
/// This trait provides the storage interface needed by `AsyncResult` for querying
/// task results in a Celery-compatible way. Implementations should provide
/// lightweight result storage focused on result state and values.
#[async_trait]
pub trait ResultStore: Send + Sync {
    /// Store a task result
    async fn store_result(&self, task_id: TaskId, result: TaskResultValue) -> crate::Result<()>;

    /// Retrieve a task result
    async fn get_result(&self, task_id: TaskId) -> crate::Result<Option<TaskResultValue>>;

    /// Get task state
    async fn get_state(&self, task_id: TaskId) -> crate::Result<TaskState>;

    /// Delete a task result
    async fn forget(&self, task_id: TaskId) -> crate::Result<()>;

    /// Check if a result exists
    async fn has_result(&self, task_id: TaskId) -> crate::Result<bool>;

    // ------------------------------------------------------------------
    // Result tombstones (default no-op / "unsupported" implementations)
    //
    // These DEFAULT methods let a backend distinguish a *forgotten* result
    // (one with a tombstone marker) from one that simply never existed.
    // Backends that do not support tombstones inherit the defaults below
    // unchanged, so existing implementations keep compiling.
    // ------------------------------------------------------------------

    /// Record a tombstone marking a result as explicitly deleted/forgotten.
    ///
    /// The default implementation is a no-op for backends that do not support
    /// tombstones. Backends that do support them should override this to
    /// persist the marker so it can later be distinguished from a result that
    /// never existed.
    async fn store_tombstone(&self, _tombstone: crate::ResultTombstone) -> crate::Result<()> {
        Ok(())
    }

    /// Fetch the tombstone marker for a task, if one was recorded.
    ///
    /// The default implementation returns `Ok(None)` ("unsupported / not
    /// recorded"). Backends that support tombstones should override this.
    async fn get_tombstone(
        &self,
        _task_id: TaskId,
    ) -> crate::Result<Option<crate::ResultTombstone>> {
        Ok(None)
    }

    /// Returns `true` if a tombstone marker is recorded for the task.
    ///
    /// The default implementation derives the answer from [`Self::get_tombstone`],
    /// so backends only need to override `get_tombstone`.
    async fn has_tombstone(&self, task_id: TaskId) -> crate::Result<bool> {
        Ok(self.get_tombstone(task_id).await?.is_some())
    }

    /// Resolve the tri-state existence of a result: present, tombstoned
    /// (explicitly deleted), or absent (never existed).
    ///
    /// The default implementation combines [`Self::has_result`] and
    /// [`Self::get_tombstone`]: a live result reports
    /// [`crate::result_tombstone::ResultExistence::Present`], otherwise a
    /// recorded tombstone reports
    /// [`crate::result_tombstone::ResultExistence::Tombstoned`], otherwise
    /// [`crate::result_tombstone::ResultExistence::Absent`]. Because the
    /// default `get_tombstone` returns `None`, backends without tombstone
    /// support will only ever distinguish present vs absent — which preserves
    /// their previous behaviour.
    async fn result_existence(
        &self,
        task_id: TaskId,
    ) -> crate::Result<crate::result_tombstone::ResultExistence> {
        if self.has_result(task_id).await? {
            return Ok(crate::result_tombstone::ResultExistence::Present);
        }
        match self.get_tombstone(task_id).await? {
            Some(tombstone) => Ok(crate::result_tombstone::ResultExistence::Tombstoned(
                tombstone,
            )),
            None => Ok(crate::result_tombstone::ResultExistence::Absent),
        }
    }

    /// Forget a result and simultaneously record a tombstone for it.
    ///
    /// The default implementation calls [`Self::forget`] followed by
    /// [`Self::store_tombstone`]. Backends that need this to be atomic should
    /// override it.
    async fn forget_with_tombstone(&self, tombstone: crate::ResultTombstone) -> crate::Result<()> {
        let task_id = tombstone.task_id;
        self.forget(task_id).await?;
        self.store_tombstone(tombstone).await
    }

    // ------------------------------------------------------------------
    // Per-task-type result TTL (default "unsupported" hook)
    // ------------------------------------------------------------------

    /// Query the effective result TTL for a task type, given a per-task-type
    /// TTL configuration.
    ///
    /// The default implementation simply resolves the TTL from the supplied
    /// [`crate::result_ttl::ResultTtlConfig`] (per-task override, else default
    /// fallback) without touching the backend. Backends that store their own
    /// per-task TTL policy may override this to consult that policy instead.
    async fn result_ttl_for(
        &self,
        config: &crate::result_ttl::ResultTtlConfig,
        task_name: &str,
    ) -> crate::Result<Option<Duration>> {
        Ok(config.ttl_for(task_name))
    }

    /// Apply a per-task-type TTL to an already-stored result.
    ///
    /// The default implementation is a no-op returning `Ok(false)` to signal
    /// that the backend did not apply any expiry ("unsupported"). Backends
    /// with native key-expiry (e.g. Redis) should override this to set the
    /// expiry derived from `config` for `task_name` and return `Ok(true)`.
    async fn apply_result_ttl(
        &self,
        _task_id: TaskId,
        _config: &crate::result_ttl::ResultTtlConfig,
        _task_name: &str,
    ) -> crate::Result<bool> {
        Ok(false)
    }
}

/// Task result value stored in backend
#[derive(Debug, Clone)]
pub enum TaskResultValue {
    /// Task is pending execution
    Pending,

    /// Task has been received by worker
    Received,

    /// Task is currently running
    Started,

    /// Task completed successfully with result
    Success(Value),

    /// Task failed with error message and optional traceback
    Failure {
        error: String,
        traceback: Option<String>,
    },

    /// Task was revoked/cancelled
    Revoked,

    /// Task is being retried
    Retry { attempt: u32, max_retries: u32 },

    /// Task was rejected (e.g., validation failed)
    Rejected { reason: String },
}

impl TaskResultValue {
    /// Check if the result is in a terminal state
    #[inline]
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskResultValue::Success(_)
                | TaskResultValue::Failure { .. }
                | TaskResultValue::Revoked
                | TaskResultValue::Rejected { .. }
        )
    }

    /// Check if the task is pending
    #[inline]
    #[must_use]
    pub const fn is_pending(&self) -> bool {
        matches!(self, TaskResultValue::Pending)
    }

    /// Check if the task is ready (in terminal state)
    #[inline]
    #[must_use]
    pub const fn is_ready(&self) -> bool {
        self.is_terminal()
    }

    /// Check if the task succeeded
    #[inline]
    #[must_use]
    pub const fn is_successful(&self) -> bool {
        matches!(self, TaskResultValue::Success(_))
    }

    /// Check if the task failed
    #[inline]
    #[must_use]
    pub const fn is_failed(&self) -> bool {
        matches!(
            self,
            TaskResultValue::Failure { .. } | TaskResultValue::Rejected { .. }
        )
    }

    /// Get the success value if available
    #[inline]
    #[must_use]
    pub fn success_value(&self) -> Option<&Value> {
        match self {
            TaskResultValue::Success(v) => Some(v),
            _ => None,
        }
    }

    /// Get the error message if failed
    #[inline]
    #[must_use]
    pub fn error_message(&self) -> Option<&str> {
        match self {
            TaskResultValue::Failure { error, .. } => Some(error),
            TaskResultValue::Rejected { reason } => Some(reason),
            _ => None,
        }
    }

    /// Get the traceback if available
    #[inline]
    #[must_use]
    pub fn traceback(&self) -> Option<&str> {
        match self {
            TaskResultValue::Failure { traceback, .. } => traceback.as_deref(),
            _ => None,
        }
    }
}

/// Polling behaviour for [`AsyncResult::get`] and friends.
///
/// The old fixed 100 ms poll was not configurable and applied per child in
/// `collect_children`, so a large group hammered the backend. Backing off
/// between polls keeps a long wait cheap while staying responsive for the
/// common case of a result that lands quickly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AsyncResultConfig {
    /// Interval before the first re-poll.
    pub initial_poll_interval: Duration,
    /// Upper bound the interval backs off to.
    pub max_poll_interval: Duration,
    /// Whether a tombstoned (explicitly forgotten) result ends the wait with an
    /// error instead of polling forever.
    pub fail_on_tombstone: bool,
}

impl Default for AsyncResultConfig {
    fn default() -> Self {
        Self {
            initial_poll_interval: Duration::from_millis(100),
            max_poll_interval: Duration::from_secs(2),
            fail_on_tombstone: true,
        }
    }
}

impl AsyncResultConfig {
    /// Create a configuration with a fixed poll interval (no backoff).
    #[must_use]
    pub const fn fixed(interval: Duration) -> Self {
        Self {
            initial_poll_interval: interval,
            max_poll_interval: interval,
            fail_on_tombstone: true,
        }
    }

    /// Set the initial poll interval.
    #[must_use]
    pub const fn with_initial_poll_interval(mut self, interval: Duration) -> Self {
        self.initial_poll_interval = interval;
        self
    }

    /// Set the maximum poll interval.
    #[must_use]
    pub const fn with_max_poll_interval(mut self, interval: Duration) -> Self {
        self.max_poll_interval = interval;
        self
    }

    /// Set whether a tombstoned result ends the wait with an error.
    #[must_use]
    pub const fn with_fail_on_tombstone(mut self, fail: bool) -> Self {
        self.fail_on_tombstone = fail;
        self
    }

    /// The interval to use after `attempt` polls (exponential, capped).
    fn interval_for(&self, attempt: u32) -> Duration {
        let initial = self.initial_poll_interval.max(Duration::from_millis(1));
        let max = self.max_poll_interval.max(initial);
        let factor = 1u32.checked_shl(attempt.min(16)).unwrap_or(u32::MAX);
        initial.saturating_mul(factor).min(max)
    }
}

/// `AsyncResult` handle for querying task results (Celery-compatible API)
#[derive(Clone)]
pub struct AsyncResult<S: ResultStore> {
    /// Task ID
    task_id: TaskId,

    /// Result store for retrieving results
    store: S,

    /// Parent result (for chained tasks)
    parent: Option<Box<AsyncResult<S>>>,

    /// Child results (for group/chord tasks)
    children: Vec<AsyncResult<S>>,

    /// Polling behaviour for the blocking accessors
    config: AsyncResultConfig,
}

impl<S: ResultStore + Clone> AsyncResult<S> {
    /// Create a new `AsyncResult` for a task
    pub fn new(task_id: TaskId, store: S) -> Self {
        Self {
            task_id,
            store,
            parent: None,
            children: Vec::new(),
            config: AsyncResultConfig::default(),
        }
    }

    /// Override the polling behaviour used by [`Self::get`] and [`Self::wait`].
    #[must_use]
    pub fn with_config(mut self, config: AsyncResultConfig) -> Self {
        self.config = config;
        self
    }

    /// The active polling configuration.
    #[inline]
    #[must_use]
    pub const fn config(&self) -> &AsyncResultConfig {
        &self.config
    }

    /// Create an `AsyncResult` with a parent
    pub fn with_parent(task_id: TaskId, store: S, parent: AsyncResult<S>) -> Self {
        Self {
            task_id,
            store,
            parent: Some(Box::new(parent)),
            children: Vec::new(),
            config: AsyncResultConfig::default(),
        }
    }

    /// Create an `AsyncResult` with children (for group/chord results)
    pub fn with_children(task_id: TaskId, store: S, children: Vec<AsyncResult<S>>) -> Self {
        Self {
            task_id,
            store,
            parent: None,
            children,
            config: AsyncResultConfig::default(),
        }
    }

    /// Get the task ID
    #[inline]
    #[must_use]
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    /// Get the parent result if this is a linked task
    #[inline]
    #[must_use]
    pub fn parent(&self) -> Option<&AsyncResult<S>> {
        self.parent.as_deref()
    }

    /// Get child results (for group/chord tasks)
    #[inline]
    #[must_use]
    pub fn children(&self) -> &[AsyncResult<S>] {
        &self.children
    }

    /// Add a child result
    pub fn add_child(&mut self, child: AsyncResult<S>) {
        self.children.push(child);
    }

    /// Check if all children are ready (completed)
    pub async fn children_ready(&self) -> crate::Result<bool> {
        for child in &self.children {
            if !child.ready().await? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Get results from all children
    ///
    /// Returns a vector of results in the same order as children were added.
    /// Returns an error if any child failed.
    pub async fn collect_children(
        &self,
        timeout: Option<Duration>,
    ) -> crate::Result<Vec<Option<Value>>> {
        let mut results = Vec::with_capacity(self.children.len());
        for child in &self.children {
            results.push(child.get(timeout).await?);
        }
        Ok(results)
    }

    /// Check if the task is ready (in terminal state)
    pub async fn ready(&self) -> crate::Result<bool> {
        let state = self.store.get_state(self.task_id).await?;
        Ok(state.is_terminal())
    }

    /// Check if the task completed successfully
    pub async fn successful(&self) -> crate::Result<bool> {
        match self.store.get_result(self.task_id).await? {
            Some(result) => Ok(result.is_successful()),
            None => Ok(false),
        }
    }

    /// Check if the task failed
    pub async fn failed(&self) -> crate::Result<bool> {
        match self.store.get_result(self.task_id).await? {
            Some(result) => Ok(result.is_failed()),
            None => Ok(false),
        }
    }

    /// Get the current task state
    pub async fn state(&self) -> crate::Result<TaskState> {
        self.store.get_state(self.task_id).await
    }

    /// Get task information/metadata
    pub async fn info(&self) -> crate::Result<Option<TaskResultValue>> {
        self.store.get_result(self.task_id).await
    }

    /// Get the result, blocking until it's ready
    ///
    /// # Arguments
    /// * `timeout` - Optional timeout duration. If None, waits indefinitely.
    ///
    /// # Returns
    /// * `Ok(Some(Value))` - Task succeeded with a result
    /// * `Ok(None)` - Task succeeded with no result (a stored JSON `null`)
    /// * `Err(_)` - Task failed, was revoked/rejected, was forgotten, or the
    ///   timeout expired
    ///
    /// When the backend reports no result, [`ResultStore::result_existence`] is
    /// consulted: a *tombstoned* result (one that was explicitly forgotten or
    /// aged out) ends the wait with an error rather than polling forever, which
    /// is exactly what the tri-state tombstone machinery exists for.
    ///
    /// # Errors
    ///
    /// Returns [`crate::CelersError::Timeout`] when `timeout` expires,
    /// [`crate::CelersError::TaskExecution`] for a failed, rejected or forgotten
    /// task, and [`crate::CelersError::TaskRevoked`] for a revoked one.
    pub async fn get(&self, timeout: Option<Duration>) -> crate::Result<Option<Value>> {
        let start = std::time::Instant::now();
        let mut attempt = 0u32;

        loop {
            // Check if timeout expired
            if let Some(timeout_duration) = timeout {
                if start.elapsed() > timeout_duration {
                    return Err(crate::CelersError::Timeout(format!(
                        "Task {} did not complete within {:?}",
                        self.task_id, timeout_duration
                    )));
                }
            }

            // Get current result
            if let Some(result) = self.store.get_result(self.task_id).await? {
                match result {
                    // A successful task that produced nothing is stored as
                    // `Success(Value::Null)`; surface that as `Ok(None)` so the
                    // documented "no result" case is actually reachable.
                    TaskResultValue::Success(Value::Null) => return Ok(None),
                    TaskResultValue::Success(value) => return Ok(Some(value)),
                    TaskResultValue::Failure { error, traceback } => {
                        let msg = if let Some(tb) = traceback {
                            format!("{error}\n{tb}")
                        } else {
                            error
                        };
                        return Err(crate::CelersError::TaskExecution(msg));
                    }
                    TaskResultValue::Revoked => {
                        return Err(crate::CelersError::TaskRevoked(self.task_id));
                    }
                    TaskResultValue::Rejected { reason } => {
                        return Err(crate::CelersError::TaskExecution(format!(
                            "Task rejected: {reason}"
                        )));
                    }
                    // Task not ready yet, continue polling
                    _ => {}
                }
            } else if self.config.fail_on_tombstone {
                // Distinguish "not ready yet" from "the result is gone". Without
                // this the loop polled forever for a forgotten or TTL-expired
                // result.
                if let crate::result_tombstone::ResultExistence::Tombstoned(tombstone) =
                    self.store.result_existence(self.task_id).await?
                {
                    let reason = tombstone
                        .reason
                        .clone()
                        .unwrap_or_else(|| "result was forgotten".to_string());
                    return Err(crate::CelersError::TaskExecution(format!(
                        "Task {} has no result: {reason}",
                        self.task_id
                    )));
                }
            }

            // Wait before next poll, backing off up to the configured ceiling.
            tokio::time::sleep(self.config.interval_for(attempt)).await;
            attempt = attempt.saturating_add(1);
        }
    }

    /// Get the result without blocking
    ///
    /// Returns None if the task is not yet complete
    pub async fn result(&self) -> crate::Result<Option<Value>> {
        match self.store.get_result(self.task_id).await? {
            Some(TaskResultValue::Success(value)) => Ok(Some(value)),
            _ => Ok(None),
        }
    }

    /// Get the error traceback if the task failed
    pub async fn traceback(&self) -> crate::Result<Option<String>> {
        match self.store.get_result(self.task_id).await? {
            Some(result) => Ok(result.traceback().map(String::from)),
            None => Ok(None),
        }
    }

    /// Revoke the task
    pub async fn revoke(&self) -> crate::Result<()> {
        self.store
            .store_result(self.task_id, TaskResultValue::Revoked)
            .await
    }

    /// Forget the task result (delete from store)
    pub async fn forget(&self) -> crate::Result<()> {
        self.store.forget(self.task_id).await
    }

    /// Wait for the task to complete and return the result
    ///
    /// This is a convenience method that combines `ready()` and `get()`
    pub async fn wait(&self, timeout: Option<Duration>) -> crate::Result<Value> {
        match self.get(timeout).await? {
            Some(value) => Ok(value),
            None => Err(crate::CelersError::TaskExecution(
                "Task completed but returned no value".to_string(),
            )),
        }
    }
}

impl<S: ResultStore + Clone> std::fmt::Debug for AsyncResult<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncResult")
            .field("store", &"<ResultStore>")
            .field("task_id", &self.task_id)
            .field("has_parent", &self.parent.is_some())
            .field("num_children", &self.children.len())
            .finish()
    }
}

impl<S: ResultStore + Clone> std::fmt::Display for AsyncResult<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AsyncResult[{}]", &self.task_id.to_string()[..8])
    }
}

// ============================================================================
// Advanced Result Features
// ============================================================================

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Result metadata for storing additional information with task results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultMetadata {
    /// Custom tags for categorization
    pub tags: Vec<String>,

    /// Custom key-value fields
    pub custom_fields: HashMap<String, Value>,

    /// Result creation timestamp
    pub created_at: DateTime<Utc>,

    /// Result expiration timestamp (TTL)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,

    /// Whether the result is compressed
    pub compressed: bool,

    /// Compression algorithm used (if compressed)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression_algorithm: Option<String>,

    /// Whether the result is chunked
    pub chunked: bool,

    /// Total number of chunks (if chunked)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_chunks: Option<usize>,

    /// Original size in bytes (before compression/chunking)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_size: Option<usize>,

    /// Compressed size in bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compressed_size: Option<usize>,
}

impl ResultMetadata {
    /// Create new result metadata
    #[must_use]
    pub fn new() -> Self {
        Self {
            tags: Vec::new(),
            custom_fields: HashMap::new(),
            created_at: Utc::now(),
            expires_at: None,
            compressed: false,
            compression_algorithm: None,
            chunked: false,
            total_chunks: None,
            original_size: None,
            compressed_size: None,
        }
    }

    /// Add a tag
    #[must_use]
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Add multiple tags
    #[must_use]
    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags.extend(tags);
        self
    }

    /// Add a custom field
    #[must_use]
    pub fn with_field(mut self, key: impl Into<String>, value: Value) -> Self {
        self.custom_fields.insert(key.into(), value);
        self
    }

    /// Set TTL (time to live)
    ///
    /// A TTL too large for `chrono::Duration` (more than `i64::MAX`
    /// milliseconds) is clamped to `chrono::Duration::MAX` rather than
    /// panicking; such a value is reachable from a deserialized configuration.
    #[must_use]
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        let delta = chrono::Duration::from_std(ttl).unwrap_or(chrono::Duration::MAX);
        self.expires_at = Some(
            Utc::now()
                .checked_add_signed(delta)
                .unwrap_or(DateTime::<Utc>::MAX_UTC),
        );
        self
    }

    /// Set expiration timestamp
    #[must_use]
    pub fn with_expires_at(mut self, expires_at: DateTime<Utc>) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    /// Check if the result has expired
    #[inline]
    #[must_use]
    pub fn is_expired(&self) -> bool {
        self.expires_at.is_some_and(|exp| Utc::now() > exp)
    }

    /// Get time until expiration
    #[inline]
    #[must_use]
    pub fn time_until_expiration(&self) -> Option<Duration> {
        self.expires_at.and_then(|exp| {
            let diff = exp - Utc::now();
            diff.to_std().ok()
        })
    }

    /// Mark as compressed
    #[must_use]
    pub fn with_compression(
        mut self,
        algorithm: impl Into<String>,
        original_size: usize,
        compressed_size: usize,
    ) -> Self {
        self.compressed = true;
        self.compression_algorithm = Some(algorithm.into());
        self.original_size = Some(original_size);
        self.compressed_size = Some(compressed_size);
        self
    }

    /// Mark as chunked
    #[must_use]
    pub fn with_chunking(mut self, total_chunks: usize) -> Self {
        self.chunked = true;
        self.total_chunks = Some(total_chunks);
        self
    }

    /// Get compression ratio
    #[allow(clippy::cast_precision_loss)]
    #[must_use]
    pub fn compression_ratio(&self) -> Option<f64> {
        if let (Some(orig), Some(comp)) = (self.original_size, self.compressed_size) {
            if orig > 0 {
                return Some(comp as f64 / orig as f64);
            }
        }
        None
    }
}

impl Default for ResultMetadata {
    fn default() -> Self {
        Self::new()
    }
}

/// Result chunk for large results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultChunk {
    /// Chunk index (0-based)
    pub index: usize,

    /// Total number of chunks
    pub total: usize,

    /// Chunk data
    pub data: Vec<u8>,

    /// Checksum for integrity verification
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
}

impl ResultChunk {
    /// Create a new result chunk
    #[must_use]
    pub fn new(index: usize, total: usize, data: Vec<u8>) -> Self {
        Self {
            index,
            total,
            data,
            checksum: None,
        }
    }

    /// Add checksum
    #[must_use]
    pub fn with_checksum(mut self, checksum: impl Into<String>) -> Self {
        self.checksum = Some(checksum.into());
        self
    }

    /// Check if this is the last chunk
    ///
    /// Total-safe: a chunk carrying `total == 0` (which a deserialized payload
    /// can, and which `ResultChunker::chunk` produces for empty data) used to
    /// underflow here — a panic in debug and a `usize::MAX` comparison in
    /// release.
    #[inline]
    #[must_use]
    pub const fn is_last(&self) -> bool {
        self.total > 0 && self.index + 1 == self.total
    }

    /// Compute the checksum of this chunk's payload.
    ///
    /// SHA-256, hex encoded, using the crate's own pure-Rust implementation.
    #[must_use]
    pub fn compute_checksum(data: &[u8]) -> String {
        crate::task_signature::to_hex(&crate::task_signature::Sha256::digest(data))
    }

    /// Attach a checksum computed from this chunk's own data.
    #[must_use]
    pub fn with_computed_checksum(mut self) -> Self {
        self.checksum = Some(Self::compute_checksum(&self.data));
        self
    }

    /// Verify the chunk against its recorded checksum.
    ///
    /// Returns `true` when no checksum is recorded (nothing to verify).
    #[must_use]
    pub fn verify_checksum(&self) -> bool {
        match &self.checksum {
            Some(expected) => {
                let actual = Self::compute_checksum(&self.data);
                crate::task_signature::constant_time_eq(actual.as_bytes(), expected.as_bytes())
            }
            None => true,
        }
    }
}

/// Result tombstone marker for deleted tasks
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResultTombstone {
    /// Task ID
    pub task_id: TaskId,

    /// Deletion timestamp
    pub deleted_at: DateTime<Utc>,

    /// Reason for deletion
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,

    /// Who deleted it (user, system, etc.)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_by: Option<String>,

    /// TTL for the tombstone itself
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tombstone_ttl: Option<Duration>,
}

impl ResultTombstone {
    /// Create a new tombstone
    #[must_use]
    pub fn new(task_id: TaskId) -> Self {
        Self {
            task_id,
            deleted_at: Utc::now(),
            reason: None,
            deleted_by: None,
            tombstone_ttl: None,
        }
    }

    /// Set deletion reason
    #[must_use]
    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    /// Set who deleted it
    #[must_use]
    pub fn with_deleted_by(mut self, deleted_by: impl Into<String>) -> Self {
        self.deleted_by = Some(deleted_by.into());
        self
    }

    /// Set tombstone TTL
    #[must_use]
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.tombstone_ttl = Some(ttl);
        self
    }
}

/// Extended result store with advanced features
#[async_trait]
pub trait ExtendedResultStore: ResultStore {
    /// Store result with metadata
    async fn store_result_with_metadata(
        &self,
        task_id: TaskId,
        result: TaskResultValue,
        metadata: ResultMetadata,
    ) -> crate::Result<()>;

    /// Get result metadata
    async fn get_metadata(&self, task_id: TaskId) -> crate::Result<Option<ResultMetadata>>;

    /// Store a result chunk
    async fn store_chunk(&self, task_id: TaskId, chunk: ResultChunk) -> crate::Result<()>;

    /// Get a result chunk
    async fn get_chunk(&self, task_id: TaskId, index: usize) -> crate::Result<Option<ResultChunk>>;

    /// Get all chunks for a task
    async fn get_all_chunks(&self, task_id: TaskId) -> crate::Result<Vec<ResultChunk>>;

    // Tombstones are NOT redeclared here.
    //
    // `store_tombstone` / `get_tombstone` / `has_tombstone` used to be required
    // methods on this trait as well as defaulted methods on `ResultStore`. A
    // subtrait cannot override a supertrait's default, so an implementor
    // satisfied `ExtendedResultStore` while `ResultStore::get_tombstone` kept
    // returning `Ok(None)` — and `ResultStore::result_existence` therefore
    // reported `Absent` for a task whose tombstone the backend was demonstrably
    // holding. Backends now override the `ResultStore` methods directly, which
    // is what makes the tri-state resolution correct.

    /// Cleanup expired results
    async fn cleanup_expired(&self) -> crate::Result<usize>;

    /// Query results by tags
    async fn query_by_tags(&self, tags: &[String]) -> crate::Result<Vec<TaskId>>;
}

/// A pluggable compression codec.
///
/// Implementations live wherever the codec's dependency lives: a backend crate
/// can register a deflate or zstd codec and every `ResultCompressor` built with
/// it then works, instead of the compressor being a type whose only two methods
/// always fail.
pub trait CompressionCodec: Send + Sync + std::fmt::Debug {
    /// Name this codec answers to (e.g. `"zstd"`, `"deflate"`).
    fn name(&self) -> &str;

    /// Compress `data`.
    ///
    /// # Errors
    ///
    /// Returns an error if the codec cannot process the input.
    fn compress(&self, data: &[u8]) -> crate::Result<Vec<u8>>;

    /// Decompress `data`.
    ///
    /// # Errors
    ///
    /// Returns an error if the input is not valid for this codec.
    fn decompress(&self, data: &[u8]) -> crate::Result<Vec<u8>>;
}

/// The identity codec: stores payloads verbatim under the name `"none"`.
///
/// Always available, so a `ResultCompressor` is never a type whose operations
/// unconditionally fail. It is what `compression_algorithm: "none"` means on
/// the wire.
#[derive(Debug, Clone, Copy, Default)]
pub struct IdentityCodec;

impl CompressionCodec for IdentityCodec {
    fn name(&self) -> &str {
        "none"
    }

    fn compress(&self, data: &[u8]) -> crate::Result<Vec<u8>> {
        Ok(data.to_vec())
    }

    fn decompress(&self, data: &[u8]) -> crate::Result<Vec<u8>> {
        Ok(data.to_vec())
    }
}

/// Compression helper for result values
///
/// Holds a registry of [`CompressionCodec`]s keyed by algorithm name. The
/// identity codec (`"none"`) is always registered; backends add real codecs with
/// [`ResultCompressor::with_codec`].
#[derive(Debug, Clone)]
pub struct ResultCompressor {
    threshold_bytes: usize,
    codecs: std::collections::HashMap<String, std::sync::Arc<dyn CompressionCodec>>,
}

impl ResultCompressor {
    /// Create a new compressor with threshold
    #[must_use]
    pub fn new(threshold_bytes: usize) -> Self {
        let mut codecs: std::collections::HashMap<String, std::sync::Arc<dyn CompressionCodec>> =
            std::collections::HashMap::new();
        codecs.insert("none".to_string(), std::sync::Arc::new(IdentityCodec));
        Self {
            threshold_bytes,
            codecs,
        }
    }

    /// Register a codec, replacing any codec already registered under its name.
    #[must_use]
    pub fn with_codec(mut self, codec: std::sync::Arc<dyn CompressionCodec>) -> Self {
        self.codecs.insert(codec.name().to_string(), codec);
        self
    }

    /// Register a codec on an existing compressor.
    pub fn register_codec(&mut self, codec: std::sync::Arc<dyn CompressionCodec>) {
        self.codecs.insert(codec.name().to_string(), codec);
    }

    /// Names of the registered algorithms, sorted.
    #[must_use]
    pub fn algorithms(&self) -> Vec<String> {
        let mut names: Vec<String> = self.codecs.keys().cloned().collect();
        names.sort();
        names
    }

    /// Whether an algorithm is available.
    #[must_use]
    pub fn supports(&self, algorithm: &str) -> bool {
        self.codecs.contains_key(algorithm)
    }

    /// Check if value should be compressed
    #[must_use]
    pub const fn should_compress(&self, data: &[u8]) -> bool {
        data.len() >= self.threshold_bytes
    }

    /// Look up a codec by name.
    fn codec(&self, algorithm: &str) -> crate::Result<&std::sync::Arc<dyn CompressionCodec>> {
        self.codecs.get(algorithm).ok_or_else(|| {
            crate::CelersError::Configuration(format!(
                "unsupported compression algorithm '{algorithm}'; registered: {}",
                self.algorithms().join(", ")
            ))
        })
    }

    /// Compress data with the named algorithm.
    ///
    /// # Errors
    ///
    /// Returns [`crate::CelersError::Configuration`] if no codec is registered
    /// under `algorithm`, or the codec's own error if compression fails.
    pub fn compress(&self, data: &[u8], algorithm: &str) -> crate::Result<Vec<u8>> {
        self.codec(algorithm)?.compress(data)
    }

    /// Decompress data with the named algorithm.
    ///
    /// # Errors
    ///
    /// Returns [`crate::CelersError::Configuration`] if no codec is registered
    /// under `algorithm`, or the codec's own error if decompression fails.
    pub fn decompress(&self, data: &[u8], algorithm: &str) -> crate::Result<Vec<u8>> {
        self.codec(algorithm)?.decompress(data)
    }
}

impl Default for ResultCompressor {
    fn default() -> Self {
        Self::new(1024 * 1024) // 1MB threshold
    }
}

/// Chunker for large results
pub struct ResultChunker {
    chunk_size: usize,
}

impl ResultChunker {
    /// Create a new chunker
    #[must_use]
    pub fn new(chunk_size: usize) -> Self {
        Self { chunk_size }
    }

    /// Split data into chunks, attaching an integrity checksum to each.
    #[must_use]
    pub fn chunk(&self, data: &[u8]) -> Vec<ResultChunk> {
        let total = data.len().div_ceil(self.chunk_size.max(1));

        data.chunks(self.chunk_size.max(1))
            .enumerate()
            .map(|(index, chunk)| {
                ResultChunk::new(index, total, chunk.to_vec()).with_computed_checksum()
            })
            .collect()
    }

    /// Reassemble chunks into original data
    ///
    /// Every chunk must agree on `total`, appear at its declared index, and —
    /// when it carries one — match its checksum. Without those checks a
    /// corrupted or substituted chunk reassembled silently.
    ///
    /// # Errors
    ///
    /// Returns an error if the chunk set is incomplete, disagrees about the
    /// total, is out of order, or fails checksum verification.
    pub fn reassemble(&self, chunks: &[ResultChunk]) -> crate::Result<Vec<u8>> {
        let Some(first) = chunks.first() else {
            return Ok(Vec::new());
        };

        // Verify chunks are complete and in order
        let total = first.total;
        if chunks.len() != total {
            return Err(crate::CelersError::Other(format!(
                "Incomplete chunks: expected {}, got {}",
                total,
                chunks.len()
            )));
        }

        let mut result = Vec::new();
        for (i, chunk) in chunks.iter().enumerate() {
            if chunk.total != total {
                return Err(crate::CelersError::Other(format!(
                    "Inconsistent chunk total at index {i}: expected {total}, got {}",
                    chunk.total
                )));
            }
            if chunk.index != i {
                return Err(crate::CelersError::Other(format!(
                    "Chunk out of order: expected index {}, got {}",
                    i, chunk.index
                )));
            }
            if !chunk.verify_checksum() {
                return Err(crate::CelersError::Other(format!(
                    "Chunk {i} failed checksum verification"
                )));
            }
            result.extend_from_slice(&chunk.data);
        }

        Ok(result)
    }
}

impl Default for ResultChunker {
    fn default() -> Self {
        Self::new(256 * 1024) // 256KB chunks
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    // Mock backend for testing
    #[derive(Clone)]
    struct MockBackend {
        results: Arc<Mutex<HashMap<TaskId, TaskResultValue>>>,
        states: Arc<Mutex<HashMap<TaskId, TaskState>>>,
    }

    impl MockBackend {
        fn new() -> Self {
            Self {
                results: Arc::new(Mutex::new(HashMap::new())),
                states: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        fn set_result(&self, task_id: TaskId, result: TaskResultValue, state: TaskState) {
            self.results
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(task_id, result);
            self.states
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(task_id, state);
        }
    }

    #[async_trait]
    impl ResultStore for MockBackend {
        async fn store_result(
            &self,
            task_id: TaskId,
            result: TaskResultValue,
        ) -> crate::Result<()> {
            self.results
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(task_id, result);
            Ok(())
        }

        async fn get_result(&self, task_id: TaskId) -> crate::Result<Option<TaskResultValue>> {
            Ok(self
                .results
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&task_id)
                .cloned())
        }

        async fn get_state(&self, task_id: TaskId) -> crate::Result<TaskState> {
            Ok(self
                .states
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&task_id)
                .cloned()
                .unwrap_or(TaskState::Pending))
        }

        async fn forget(&self, task_id: TaskId) -> crate::Result<()> {
            self.results
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&task_id);
            self.states
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&task_id);
            Ok(())
        }

        async fn has_result(&self, task_id: TaskId) -> crate::Result<bool> {
            Ok(self
                .results
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains_key(&task_id))
        }
    }

    #[tokio::test]
    async fn test_async_result_ready() {
        let backend = MockBackend::new();
        let task_id = Uuid::new_v4();

        backend.set_result(
            task_id,
            TaskResultValue::Success(Value::String("test".to_string())),
            TaskState::Succeeded(vec![]),
        );

        let result = AsyncResult::new(task_id, backend);
        assert!(result.ready().await.unwrap());
    }

    #[tokio::test]
    async fn test_async_result_successful() {
        let backend = MockBackend::new();
        let task_id = Uuid::new_v4();

        backend.set_result(
            task_id,
            TaskResultValue::Success(Value::String("test".to_string())),
            TaskState::Succeeded(vec![]),
        );

        let result = AsyncResult::new(task_id, backend);
        assert!(result.successful().await.unwrap());
        assert!(!result.failed().await.unwrap());
    }

    #[tokio::test]
    async fn test_async_result_failed() {
        let backend = MockBackend::new();
        let task_id = Uuid::new_v4();

        backend.set_result(
            task_id,
            TaskResultValue::Failure {
                error: "Test error".to_string(),
                traceback: None,
            },
            TaskState::Failed(String::from("Test error")),
        );

        let result = AsyncResult::new(task_id, backend);
        assert!(result.failed().await.unwrap());
        assert!(!result.successful().await.unwrap());
    }

    #[tokio::test]
    async fn test_async_result_get_success() {
        let backend = MockBackend::new();
        let task_id = Uuid::new_v4();

        backend.set_result(
            task_id,
            TaskResultValue::Success(Value::String("success".to_string())),
            TaskState::Succeeded(vec![]),
        );

        let result = AsyncResult::new(task_id, backend);
        let value = result.get(Some(Duration::from_secs(1))).await.unwrap();
        assert_eq!(value, Some(Value::String("success".to_string())));
    }

    #[tokio::test]
    async fn test_async_result_forget() {
        let backend = MockBackend::new();
        let task_id = Uuid::new_v4();

        backend.set_result(
            task_id,
            TaskResultValue::Success(Value::String("test".to_string())),
            TaskState::Succeeded(vec![]),
        );

        let result = AsyncResult::new(task_id, backend.clone());
        assert!(backend.has_result(task_id).await.unwrap());

        result.forget().await.unwrap();
        assert!(!backend.has_result(task_id).await.unwrap());
    }

    #[tokio::test]
    async fn test_async_result_children() {
        let backend = MockBackend::new();

        // Create parent task
        let parent_id = Uuid::new_v4();
        backend.set_result(
            parent_id,
            TaskResultValue::Success(Value::String("parent".to_string())),
            TaskState::Succeeded(vec![]),
        );

        // Create child tasks
        let child1_id = Uuid::new_v4();
        let child2_id = Uuid::new_v4();
        backend.set_result(
            child1_id,
            TaskResultValue::Success(Value::Number(serde_json::Number::from(1))),
            TaskState::Succeeded(vec![]),
        );
        backend.set_result(
            child2_id,
            TaskResultValue::Success(Value::Number(serde_json::Number::from(2))),
            TaskState::Succeeded(vec![]),
        );

        // Create child AsyncResults
        let child1 = AsyncResult::new(child1_id, backend.clone());
        let child2 = AsyncResult::new(child2_id, backend.clone());

        // Create parent with children
        let parent = AsyncResult::with_children(parent_id, backend, vec![child1, child2]);

        // Test children access
        assert_eq!(parent.children().len(), 2);
        assert_eq!(parent.children()[0].task_id(), child1_id);
        assert_eq!(parent.children()[1].task_id(), child2_id);

        // Test children_ready
        assert!(parent.children_ready().await.unwrap());

        // Test collect_children
        let results = parent
            .collect_children(Some(Duration::from_secs(1)))
            .await
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], Some(Value::Number(serde_json::Number::from(1))));
        assert_eq!(results[1], Some(Value::Number(serde_json::Number::from(2))));
    }

    #[tokio::test]
    async fn test_async_result_add_child() {
        let backend = MockBackend::new();

        let parent_id = Uuid::new_v4();
        let child_id = Uuid::new_v4();

        backend.set_result(
            child_id,
            TaskResultValue::Success(Value::String("child".to_string())),
            TaskState::Succeeded(vec![]),
        );

        let mut parent = AsyncResult::new(parent_id, backend.clone());
        assert_eq!(parent.children().len(), 0);

        let child = AsyncResult::new(child_id, backend);
        parent.add_child(child);

        assert_eq!(parent.children().len(), 1);
        assert_eq!(parent.children()[0].task_id(), child_id);
    }

    // ------------------------------------------------------------------
    // Regression tests
    // ------------------------------------------------------------------

    /// A backend that supports tombstones by overriding the `ResultStore`
    /// methods, plus the whole of `ExtendedResultStore`.
    #[derive(Clone, Default)]
    struct TombstoneBackend {
        results: Arc<Mutex<HashMap<TaskId, TaskResultValue>>>,
        tombstones: Arc<Mutex<HashMap<TaskId, ResultTombstone>>>,
    }

    #[async_trait]
    impl ResultStore for TombstoneBackend {
        async fn store_result(
            &self,
            task_id: TaskId,
            result: TaskResultValue,
        ) -> crate::Result<()> {
            self.results
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(task_id, result);
            Ok(())
        }

        async fn get_result(&self, task_id: TaskId) -> crate::Result<Option<TaskResultValue>> {
            Ok(self
                .results
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&task_id)
                .cloned())
        }

        async fn get_state(&self, _task_id: TaskId) -> crate::Result<TaskState> {
            Ok(TaskState::Pending)
        }

        async fn forget(&self, task_id: TaskId) -> crate::Result<()> {
            self.results
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&task_id);
            Ok(())
        }

        async fn has_result(&self, task_id: TaskId) -> crate::Result<bool> {
            Ok(self
                .results
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains_key(&task_id))
        }

        async fn store_tombstone(&self, tombstone: ResultTombstone) -> crate::Result<()> {
            self.tombstones
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(tombstone.task_id, tombstone);
            Ok(())
        }

        async fn get_tombstone(&self, task_id: TaskId) -> crate::Result<Option<ResultTombstone>> {
            Ok(self
                .tombstones
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&task_id)
                .cloned())
        }
    }

    #[async_trait]
    impl ExtendedResultStore for TombstoneBackend {
        async fn store_result_with_metadata(
            &self,
            task_id: TaskId,
            result: TaskResultValue,
            _metadata: ResultMetadata,
        ) -> crate::Result<()> {
            self.store_result(task_id, result).await
        }

        async fn get_metadata(&self, _task_id: TaskId) -> crate::Result<Option<ResultMetadata>> {
            Ok(None)
        }

        async fn store_chunk(&self, _task_id: TaskId, _chunk: ResultChunk) -> crate::Result<()> {
            Ok(())
        }

        async fn get_chunk(
            &self,
            _task_id: TaskId,
            _index: usize,
        ) -> crate::Result<Option<ResultChunk>> {
            Ok(None)
        }

        async fn get_all_chunks(&self, _task_id: TaskId) -> crate::Result<Vec<ResultChunk>> {
            Ok(Vec::new())
        }

        async fn cleanup_expired(&self) -> crate::Result<usize> {
            Ok(0)
        }

        async fn query_by_tags(&self, _tags: &[String]) -> crate::Result<Vec<TaskId>> {
            Ok(Vec::new())
        }
    }

    /// Regression: `ExtendedResultStore` redeclared `get_tombstone`, which a
    /// subtrait cannot use to override the supertrait default, so
    /// `ResultStore::result_existence` reported `Absent` for a task whose
    /// tombstone the backend was holding.
    #[tokio::test]
    async fn test_result_existence_sees_extended_backend_tombstones() {
        use crate::result_tombstone::ResultExistence;

        let backend = TombstoneBackend::default();
        let task_id = Uuid::new_v4();

        backend
            .store_result(task_id, TaskResultValue::Success(Value::from(1)))
            .await
            .expect("store");
        assert!(matches!(
            backend.result_existence(task_id).await.expect("existence"),
            ResultExistence::Present
        ));

        backend
            .forget_with_tombstone(ResultTombstone::new(task_id).with_reason("expired by policy"))
            .await
            .expect("forget with tombstone");

        assert!(backend.has_tombstone(task_id).await.expect("has_tombstone"));
        let existence = backend.result_existence(task_id).await.expect("existence");
        match existence {
            ResultExistence::Tombstoned(tombstone) => {
                assert_eq!(tombstone.task_id, task_id);
                assert_eq!(tombstone.reason.as_deref(), Some("expired by policy"));
            }
            other => panic!("expected Tombstoned, got {other:?}"),
        }

        // A task that never existed is still Absent.
        assert!(matches!(
            backend
                .result_existence(Uuid::new_v4())
                .await
                .expect("existence"),
            ResultExistence::Absent
        ));
    }

    /// Regression: `get()` polled forever for a result that had been forgotten.
    #[tokio::test]
    async fn test_get_fails_fast_on_a_tombstoned_result() {
        let backend = TombstoneBackend::default();
        let task_id = Uuid::new_v4();
        backend
            .store_tombstone(ResultTombstone::new(task_id).with_reason("forgotten"))
            .await
            .expect("store tombstone");

        let result = AsyncResult::new(task_id, backend);
        let err = result
            .get(None)
            .await
            .expect_err("a tombstoned result must not poll forever");
        assert!(err.to_string().contains("forgotten"), "unexpected: {err}");
    }

    #[tokio::test]
    async fn test_get_returns_none_for_a_null_success() {
        let backend = MockBackend::new();
        let task_id = Uuid::new_v4();
        backend.set_result(
            task_id,
            TaskResultValue::Success(Value::Null),
            TaskState::Succeeded(vec![]),
        );

        let result = AsyncResult::new(task_id, backend);
        assert_eq!(result.get(None).await.expect("get"), None);
        // ...which makes the previously-dead `None` arm of `wait()` reachable.
        let err = result.wait(None).await.expect_err("wait should report it");
        assert!(err.to_string().contains("no value"), "unexpected: {err}");
    }

    #[test]
    fn test_async_result_config_backoff() {
        let config = AsyncResultConfig::default();
        assert_eq!(config.interval_for(0), Duration::from_millis(100));
        assert_eq!(config.interval_for(1), Duration::from_millis(200));
        assert_eq!(config.interval_for(2), Duration::from_millis(400));
        // Capped at max_poll_interval.
        assert_eq!(config.interval_for(20), Duration::from_secs(2));

        let fixed = AsyncResultConfig::fixed(Duration::from_millis(50));
        assert_eq!(fixed.interval_for(0), Duration::from_millis(50));
        assert_eq!(fixed.interval_for(10), Duration::from_millis(50));
    }

    /// Regression: `is_last` computed `index == total - 1` on a `usize`,
    /// underflowing for `total == 0`.
    #[test]
    fn test_result_chunk_is_last_is_total_safe() {
        assert!(!ResultChunk::new(0, 0, vec![]).is_last());
        assert!(ResultChunk::new(0, 1, vec![]).is_last());
        assert!(!ResultChunk::new(0, 2, vec![]).is_last());
        assert!(ResultChunk::new(1, 2, vec![]).is_last());
    }

    /// Regression: chunk checksums were stored but never verified, so a
    /// corrupted or substituted chunk reassembled silently.
    #[test]
    fn test_reassemble_verifies_checksums_and_totals() {
        let chunker = ResultChunker::new(4);
        let data: Vec<u8> = (0..14u8).collect();
        let chunks = chunker.chunk(&data);
        assert_eq!(chunks.len(), 4);
        assert!(chunks.iter().all(|c| c.checksum.is_some()));
        assert_eq!(chunker.reassemble(&chunks).expect("reassemble"), data);

        // Tamper with one chunk's payload.
        let mut tampered = chunks.clone();
        tampered[2].data[0] ^= 0xff;
        let err = chunker
            .reassemble(&tampered)
            .expect_err("a tampered chunk must be rejected");
        assert!(err.to_string().contains("checksum"), "unexpected: {err}");

        // Disagreeing totals are rejected.
        let mut inconsistent = chunks.clone();
        inconsistent[1].total = 99;
        assert!(chunker.reassemble(&inconsistent).is_err());

        // Out-of-order chunks are still rejected.
        let mut reordered = chunks;
        reordered.swap(0, 1);
        assert!(chunker.reassemble(&reordered).is_err());

        // Empty input round-trips.
        assert!(chunker.reassemble(&[]).expect("empty").is_empty());
    }

    #[test]
    fn test_chunk_checksum_helpers() {
        let chunk = ResultChunk::new(0, 1, b"hello".to_vec()).with_computed_checksum();
        assert!(chunk.verify_checksum());
        assert_eq!(
            chunk.checksum.as_deref(),
            Some(ResultChunk::compute_checksum(b"hello").as_str())
        );

        // A chunk without a checksum verifies vacuously.
        assert!(ResultChunk::new(0, 1, b"hello".to_vec()).verify_checksum());

        // A wrong checksum fails.
        let bad = ResultChunk::new(0, 1, b"hello".to_vec()).with_checksum("deadbeef");
        assert!(!bad.verify_checksum());
    }

    /// Regression: `with_ttl` called `expect()` on `chrono::Duration::from_std`.
    #[test]
    fn test_with_ttl_clamps_instead_of_panicking() {
        let metadata = ResultMetadata::new().with_ttl(Duration::from_secs(3600));
        assert!(metadata.expires_at.is_some());
        assert!(!metadata.is_expired());

        // A TTL far beyond chrono's range clamps rather than panicking.
        let huge = ResultMetadata::new().with_ttl(Duration::from_secs(u64::MAX / 2));
        assert!(huge.expires_at.is_some());
        assert!(!huge.is_expired());
    }

    /// Regression: `ResultCompressor` was a public, constructible type whose
    /// only two operations always failed.
    #[test]
    fn test_result_compressor_dispatches_to_registered_codecs() {
        #[derive(Debug)]
        struct XorCodec;

        impl CompressionCodec for XorCodec {
            fn name(&self) -> &str {
                "xor"
            }
            fn compress(&self, data: &[u8]) -> crate::Result<Vec<u8>> {
                Ok(data.iter().map(|b| b ^ 0x5a).collect())
            }
            fn decompress(&self, data: &[u8]) -> crate::Result<Vec<u8>> {
                Ok(data.iter().map(|b| b ^ 0x5a).collect())
            }
        }

        let compressor = ResultCompressor::new(4).with_codec(Arc::new(XorCodec));
        assert_eq!(compressor.algorithms(), vec!["none", "xor"]);
        assert!(compressor.supports("none"));
        assert!(compressor.supports("xor"));
        assert!(!compressor.supports("zstd"));

        assert!(compressor.should_compress(b"12345"));
        assert!(!compressor.should_compress(b"123"));

        // The always-present identity codec round-trips verbatim.
        let identity = compressor.compress(b"payload", "none").expect("compress");
        assert_eq!(identity, b"payload");
        assert_eq!(
            compressor
                .decompress(&identity, "none")
                .expect("decompress"),
            b"payload"
        );

        // The registered codec is actually used.
        let squeezed = compressor.compress(b"payload", "xor").expect("compress");
        assert_ne!(squeezed, b"payload");
        assert_eq!(
            compressor.decompress(&squeezed, "xor").expect("decompress"),
            b"payload"
        );

        // An unregistered algorithm is a named configuration error, not a
        // blanket "compression not available".
        let err = compressor
            .compress(b"payload", "zstd")
            .expect_err("unknown algorithm must be rejected");
        assert!(
            err.to_string()
                .contains("unsupported compression algorithm"),
            "unexpected: {err}"
        );
    }
}
