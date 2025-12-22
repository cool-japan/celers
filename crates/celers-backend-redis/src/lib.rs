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
use futures_util::stream::{Stream, StreamExt};
use redis::{AsyncCommands, Client};
use serde::{Deserialize, Serialize};
use std::pin::Pin;
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

    // Partial updates (with default implementations)

    /// Update only the task result state (without changing other fields)
    ///
    /// This is more efficient than loading, modifying, and storing the entire TaskMeta.
    async fn update_result_state(&mut self, task_id: Uuid, result: TaskResult) -> Result<()> {
        if let Some(mut meta) = self.get_result(task_id).await? {
            let is_terminal = result.is_terminal();
            meta.result = result;
            if is_terminal && meta.completed_at.is_none() {
                meta.completed_at = Some(Utc::now());
            }
            self.store_result(task_id, &meta).await?;
        }
        Ok(())
    }

    /// Update only the worker field
    async fn update_worker(&mut self, task_id: Uuid, worker: String) -> Result<()> {
        if let Some(mut meta) = self.get_result(task_id).await? {
            meta.worker = Some(worker);
            self.store_result(task_id, &meta).await?;
        }
        Ok(())
    }

    /// Mark task as started (updates state and timestamp)
    async fn mark_started(&mut self, task_id: Uuid, worker: Option<String>) -> Result<()> {
        if let Some(mut meta) = self.get_result(task_id).await? {
            meta.result = TaskResult::Started;
            meta.started_at = Some(Utc::now());
            if let Some(w) = worker {
                meta.worker = Some(w);
            }
            self.store_result(task_id, &meta).await?;
        }
        Ok(())
    }

    /// Mark task as completed with result
    async fn mark_completed(&mut self, task_id: Uuid, result: TaskResult) -> Result<()> {
        if let Some(mut meta) = self.get_result(task_id).await? {
            meta.result = result;
            meta.completed_at = Some(Utc::now());
            self.store_result(task_id, &meta).await?;
        }
        Ok(())
    }

    // Pagination support (with default implementations)

    /// Get paginated task results
    ///
    /// Returns a page of results based on the provided task IDs with pagination support.
    ///
    /// # Arguments
    /// * `task_ids` - All task IDs to paginate through
    /// * `page` - Page number (0-based)
    /// * `page_size` - Number of results per page
    ///
    /// # Returns
    /// Tuple of (results, total_count, has_more)
    async fn get_results_paginated(
        &mut self,
        task_ids: &[Uuid],
        page: usize,
        page_size: usize,
    ) -> Result<(Vec<Option<TaskMeta>>, usize, bool)> {
        let total = task_ids.len();
        let start = page * page_size;
        let end = (start + page_size).min(total);
        let has_more = end < total;

        if start >= total {
            return Ok((Vec::new(), total, false));
        }

        let page_ids = &task_ids[start..end];
        let results = self.get_results_batch(page_ids).await?;

        Ok((results, total, has_more))
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

/// Result stream type for async iteration
pub type ResultStream = Pin<Box<dyn Stream<Item = Result<(Uuid, Option<TaskMeta>)>> + Send>>;

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
    async fn scan_keys(&mut self, pattern: &str) -> Result<Vec<String>> {
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
}

/// Backend statistics
#[derive(Debug, Clone)]
pub struct BackendStats {
    /// Number of task result keys in Redis
    pub task_key_count: usize,

    /// Number of chord state keys in Redis
    pub chord_key_count: usize,

    /// Total number of backend keys
    pub total_keys: usize,

    /// Memory used by Redis (bytes)
    pub used_memory_bytes: u64,
}

impl std::fmt::Display for BackendStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BackendStats: {} task keys, {} chord keys, {:.2} MB memory",
            self.task_key_count,
            self.chord_key_count,
            self.used_memory_bytes as f64 / 1024.0 / 1024.0
        )
    }
}

/// Common TTL (Time-To-Live) durations for task results
///
/// These constants provide recommended TTL values for different use cases.
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
/// backend.store_result(task_id, &meta).await?;
///
/// // Use recommended TTL for success results
/// backend.set_expiration(task_id, ttl::SUCCESS).await?;
/// # Ok(())
/// # }
/// ```
pub mod ttl {
    use std::time::Duration;

    /// 1 hour - for temporary/transient results
    pub const TEMPORARY: Duration = Duration::from_secs(3600);

    /// 6 hours - for short-lived task results
    pub const SHORT: Duration = Duration::from_secs(6 * 3600);

    /// 24 hours - recommended for successful task results
    pub const SUCCESS: Duration = Duration::from_secs(86400);

    /// 3 days - for important results that need to be kept longer
    pub const MEDIUM: Duration = Duration::from_secs(3 * 86400);

    /// 7 days - recommended for failed tasks (useful for debugging)
    pub const FAILURE: Duration = Duration::from_secs(7 * 86400);

    /// 30 days - for archival/long-term storage
    pub const LONG: Duration = Duration::from_secs(30 * 86400);

    /// 90 days - maximum recommended retention
    pub const MAXIMUM: Duration = Duration::from_secs(90 * 86400);
}

/// Recommended batch sizes for optimal performance
///
/// These constants provide recommended batch sizes for different operations.
///
/// # Example
/// ```no_run
/// use celers_backend_redis::{RedisResultBackend, ResultBackend, batch_size};
/// use uuid::Uuid;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let mut backend = RedisResultBackend::new("redis://localhost")?;
///
/// // Use recommended batch size for operations
/// let task_ids: Vec<Uuid> = (0..batch_size::SMALL).map(|_| Uuid::new_v4()).collect();
/// let results = backend.get_results_batch(&task_ids).await?;
/// # Ok(())
/// # }
/// ```
pub mod batch_size {
    /// Small batch (10 items) - for low-latency requirements
    pub const SMALL: usize = 10;

    /// Medium batch (50 items) - recommended default for most operations
    pub const MEDIUM: usize = 50;

    /// Large batch (100 items) - for high-throughput scenarios
    pub const LARGE: usize = 100;

    /// Extra large batch (500 items) - for bulk operations with relaxed latency
    pub const EXTRA_LARGE: usize = 500;

    /// Maximum recommended batch (1000 items) - use with caution
    ///
    /// Batches larger than this may cause Redis to block or consume
    /// excessive memory. Consider using streaming instead.
    pub const MAXIMUM: usize = 1000;
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
        let state_long =
            ChordState::new(chord_id, 5, vec![]).with_timeout(Duration::from_secs(3600));
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
}
