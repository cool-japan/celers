//! AsyncResult API for querying task results
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

/// Result store trait for AsyncResult API
///
/// This trait provides the storage interface needed by AsyncResult for querying
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
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskResultValue::Success(_)
                | TaskResultValue::Failure { .. }
                | TaskResultValue::Revoked
                | TaskResultValue::Rejected { .. }
        )
    }

    /// Check if the task is pending
    pub fn is_pending(&self) -> bool {
        matches!(self, TaskResultValue::Pending)
    }

    /// Check if the task is ready (in terminal state)
    pub fn is_ready(&self) -> bool {
        self.is_terminal()
    }

    /// Check if the task succeeded
    pub fn is_successful(&self) -> bool {
        matches!(self, TaskResultValue::Success(_))
    }

    /// Check if the task failed
    pub fn is_failed(&self) -> bool {
        matches!(
            self,
            TaskResultValue::Failure { .. } | TaskResultValue::Rejected { .. }
        )
    }

    /// Get the success value if available
    pub fn success_value(&self) -> Option<&Value> {
        match self {
            TaskResultValue::Success(v) => Some(v),
            _ => None,
        }
    }

    /// Get the error message if failed
    pub fn error_message(&self) -> Option<&str> {
        match self {
            TaskResultValue::Failure { error, .. } => Some(error),
            TaskResultValue::Rejected { reason } => Some(reason),
            _ => None,
        }
    }

    /// Get the traceback if available
    pub fn traceback(&self) -> Option<&str> {
        match self {
            TaskResultValue::Failure { traceback, .. } => traceback.as_deref(),
            _ => None,
        }
    }
}

/// AsyncResult handle for querying task results (Celery-compatible API)
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
}

impl<S: ResultStore + Clone> AsyncResult<S> {
    /// Create a new AsyncResult for a task
    pub fn new(task_id: TaskId, store: S) -> Self {
        Self {
            task_id,
            store,
            parent: None,
            children: Vec::new(),
        }
    }

    /// Create an AsyncResult with a parent
    pub fn with_parent(task_id: TaskId, store: S, parent: AsyncResult<S>) -> Self {
        Self {
            task_id,
            store,
            parent: Some(Box::new(parent)),
            children: Vec::new(),
        }
    }

    /// Create an AsyncResult with children (for group/chord results)
    pub fn with_children(task_id: TaskId, store: S, children: Vec<AsyncResult<S>>) -> Self {
        Self {
            task_id,
            store,
            parent: None,
            children,
        }
    }

    /// Get the task ID
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    /// Get the parent result if this is a linked task
    pub fn parent(&self) -> Option<&AsyncResult<S>> {
        self.parent.as_deref()
    }

    /// Get child results (for group/chord tasks)
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
    /// * `Ok(Some(Value))` - Task succeeded with result
    /// * `Ok(None)` - Task completed but has no result
    /// * `Err(_)` - Task failed or timeout occurred
    pub async fn get(&self, timeout: Option<Duration>) -> crate::Result<Option<Value>> {
        let start = std::time::Instant::now();
        let poll_interval = Duration::from_millis(100);

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
            match self.store.get_result(self.task_id).await? {
                Some(result) => {
                    match result {
                        TaskResultValue::Success(value) => return Ok(Some(value)),
                        TaskResultValue::Failure { error, traceback } => {
                            let msg = if let Some(tb) = traceback {
                                format!("{}\n{}", error, tb)
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
                                "Task rejected: {}",
                                reason
                            )));
                        }
                        // Task not ready yet, continue polling
                        _ => {}
                    }
                }
                None => {
                    // Result not yet available
                }
            }

            // Wait before next poll
            tokio::time::sleep(poll_interval).await;
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
    /// This is a convenience method that combines ready() and get()
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
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Add multiple tags
    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags.extend(tags);
        self
    }

    /// Add a custom field
    pub fn with_field(mut self, key: impl Into<String>, value: Value) -> Self {
        self.custom_fields.insert(key.into(), value);
        self
    }

    /// Set TTL (time to live)
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.expires_at = Some(Utc::now() + chrono::Duration::from_std(ttl).unwrap());
        self
    }

    /// Set expiration timestamp
    pub fn with_expires_at(mut self, expires_at: DateTime<Utc>) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    /// Check if the result has expired
    pub fn is_expired(&self) -> bool {
        self.expires_at.is_some_and(|exp| Utc::now() > exp)
    }

    /// Get time until expiration
    pub fn time_until_expiration(&self) -> Option<Duration> {
        self.expires_at.and_then(|exp| {
            let diff = exp - Utc::now();
            diff.to_std().ok()
        })
    }

    /// Mark as compressed
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
    pub fn with_chunking(mut self, total_chunks: usize) -> Self {
        self.chunked = true;
        self.total_chunks = Some(total_chunks);
        self
    }

    /// Get compression ratio
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
    pub fn new(index: usize, total: usize, data: Vec<u8>) -> Self {
        Self {
            index,
            total,
            data,
            checksum: None,
        }
    }

    /// Add checksum
    pub fn with_checksum(mut self, checksum: impl Into<String>) -> Self {
        self.checksum = Some(checksum.into());
        self
    }

    /// Check if this is the last chunk
    pub fn is_last(&self) -> bool {
        self.index == self.total - 1
    }
}

/// Result tombstone marker for deleted tasks
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    /// Set who deleted it
    pub fn with_deleted_by(mut self, deleted_by: impl Into<String>) -> Self {
        self.deleted_by = Some(deleted_by.into());
        self
    }

    /// Set tombstone TTL
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

    /// Store a tombstone
    async fn store_tombstone(&self, tombstone: ResultTombstone) -> crate::Result<()>;

    /// Get a tombstone
    async fn get_tombstone(&self, task_id: TaskId) -> crate::Result<Option<ResultTombstone>>;

    /// Check if a task has a tombstone
    async fn has_tombstone(&self, task_id: TaskId) -> crate::Result<bool> {
        Ok(self.get_tombstone(task_id).await?.is_some())
    }

    /// Cleanup expired results
    async fn cleanup_expired(&self) -> crate::Result<usize>;

    /// Query results by tags
    async fn query_by_tags(&self, tags: &[String]) -> crate::Result<Vec<TaskId>>;
}

/// Compression helper for result values
pub struct ResultCompressor {
    threshold_bytes: usize,
}

impl ResultCompressor {
    /// Create a new compressor with threshold
    pub fn new(threshold_bytes: usize) -> Self {
        Self { threshold_bytes }
    }

    /// Check if value should be compressed
    pub fn should_compress(&self, data: &[u8]) -> bool {
        data.len() >= self.threshold_bytes
    }

    /// Compress data (to be implemented by backend-specific compressors)
    ///
    /// Note: Actual compression implementations are provided by backend crates
    /// (e.g., celers-backend-redis) which have the compression dependencies.
    pub fn compress(&self, _data: &[u8], _algorithm: &str) -> crate::Result<Vec<u8>> {
        Err(crate::CelersError::Other(
            "Compression not available - use backend-specific implementation".to_string(),
        ))
    }

    /// Decompress data (to be implemented by backend-specific compressors)
    ///
    /// Note: Actual decompression implementations are provided by backend crates
    /// (e.g., celers-backend-redis) which have the compression dependencies.
    pub fn decompress(&self, _data: &[u8], _algorithm: &str) -> crate::Result<Vec<u8>> {
        Err(crate::CelersError::Other(
            "Decompression not available - use backend-specific implementation".to_string(),
        ))
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
    pub fn new(chunk_size: usize) -> Self {
        Self { chunk_size }
    }

    /// Split data into chunks
    pub fn chunk(&self, data: &[u8]) -> Vec<ResultChunk> {
        let total = data.len().div_ceil(self.chunk_size);

        data.chunks(self.chunk_size)
            .enumerate()
            .map(|(index, chunk)| ResultChunk::new(index, total, chunk.to_vec()))
            .collect()
    }

    /// Reassemble chunks into original data
    pub fn reassemble(&self, chunks: &[ResultChunk]) -> crate::Result<Vec<u8>> {
        if chunks.is_empty() {
            return Ok(Vec::new());
        }

        // Verify chunks are complete and in order
        let total = chunks[0].total;
        if chunks.len() != total {
            return Err(crate::CelersError::Other(format!(
                "Incomplete chunks: expected {}, got {}",
                total,
                chunks.len()
            )));
        }

        let mut result = Vec::new();
        for (i, chunk) in chunks.iter().enumerate() {
            if chunk.index != i {
                return Err(crate::CelersError::Other(format!(
                    "Chunk out of order: expected index {}, got {}",
                    i, chunk.index
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
            self.results.lock().unwrap().insert(task_id, result);
            self.states.lock().unwrap().insert(task_id, state);
        }
    }

    #[async_trait]
    impl ResultStore for MockBackend {
        async fn store_result(
            &self,
            task_id: TaskId,
            result: TaskResultValue,
        ) -> crate::Result<()> {
            self.results.lock().unwrap().insert(task_id, result);
            Ok(())
        }

        async fn get_result(&self, task_id: TaskId) -> crate::Result<Option<TaskResultValue>> {
            Ok(self.results.lock().unwrap().get(&task_id).cloned())
        }

        async fn get_state(&self, task_id: TaskId) -> crate::Result<TaskState> {
            Ok(self
                .states
                .lock()
                .unwrap()
                .get(&task_id)
                .cloned()
                .unwrap_or(TaskState::Pending))
        }

        async fn forget(&self, task_id: TaskId) -> crate::Result<()> {
            self.results.lock().unwrap().remove(&task_id);
            self.states.lock().unwrap().remove(&task_id);
            Ok(())
        }

        async fn has_result(&self, task_id: TaskId) -> crate::Result<bool> {
            Ok(self.results.lock().unwrap().contains_key(&task_id))
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
}
