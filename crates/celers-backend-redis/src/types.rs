//! Core types for the Redis result backend
//!
//! This module contains the fundamental types used throughout the backend:
//! - [`BackendError`] and [`Result`] for error handling
//! - [`TaskResult`] for task state tracking
//! - [`ProgressInfo`] for long-running task progress
//! - [`TaskMeta`] for task metadata storage
//! - [`TaskTtlConfig`] for per-task TTL configuration
//! - [`ChordState`] for barrier synchronization

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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

    /// Check if two TaskResult values are of the same variant type
    ///
    /// This compares only the variant, ignoring inner values.
    /// For example, `Success(1)` and `Success(2)` are considered the same.
    pub fn same_variant(&self, other: &TaskResult) -> bool {
        matches!(
            (self, other),
            (TaskResult::Pending, TaskResult::Pending)
                | (TaskResult::Started, TaskResult::Started)
                | (TaskResult::Success(_), TaskResult::Success(_))
                | (TaskResult::Failure(_), TaskResult::Failure(_))
                | (TaskResult::Revoked, TaskResult::Revoked)
                | (TaskResult::Retry(_), TaskResult::Retry(_))
        )
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

    /// Tags for categorizing and filtering tasks
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,

    /// Custom metadata for flexible key-value storage
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub metadata: std::collections::HashMap<String, serde_json::Value>,

    /// Worker hostname that executed the task
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_hostname: Option<String>,

    /// Task runtime in milliseconds
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_ms: Option<u64>,

    /// Peak memory usage in bytes during execution
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_bytes: Option<u64>,

    /// Number of retries before completion
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retries: Option<u32>,

    /// Queue the task was consumed from
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,

    /// The original error text of a deliberately suppressed failure
    /// ([`celers_core::TaskResultValue::Ignored`]), when `result` is the
    /// `Success(Value::Null)` projection a backend without a first-class
    /// "ignored" wire representation stores it as.
    ///
    /// `None` for every other result, including a genuine `Success`: a
    /// backend that reconstructs `TaskResultValue` from a stored
    /// `TaskMeta` must treat `Some(_)` here as authoritative over
    /// `result` being `Success(Null)` (see `celers-backend-db` and
    /// `celers-backend-rpc`'s `result_store.rs`), and must set this back
    /// to `None` on every write that is not itself an `Ignored` value —
    /// otherwise a later genuine `Success` for the same task would still
    /// read back as `Ignored`.
    ///
    /// `#[serde(default)]` so records written before this field existed
    /// (`None` in the JSON, or the key absent entirely) deserialize
    /// unchanged, reading back exactly as they did before: a plain
    /// `Success(Null)`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ignored_error: Option<String>,
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
            tags: Vec::new(),
            metadata: std::collections::HashMap::new(),
            worker_hostname: None,
            runtime_ms: None,
            memory_bytes: None,
            retries: None,
            queue: None,
            ignored_error: None,
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

    /// Add a tag to this task
    pub fn add_tag(&mut self, tag: impl Into<String>) {
        let tag = tag.into();
        if !self.tags.contains(&tag) {
            self.tags.push(tag);
        }
    }

    /// Remove a tag from this task
    pub fn remove_tag(&mut self, tag: &str) {
        self.tags.retain(|t| t != tag);
    }

    /// Check if this task has a specific tag
    pub fn has_tag(&self, tag: &str) -> bool {
        self.tags.iter().any(|t| t == tag)
    }

    /// Check if this task has any of the specified tags
    pub fn has_any_tag(&self, tags: &[String]) -> bool {
        tags.iter().any(|tag| self.has_tag(tag))
    }

    /// Check if this task has all of the specified tags
    pub fn has_all_tags(&self, tags: &[String]) -> bool {
        tags.iter().all(|tag| self.has_tag(tag))
    }

    /// Set a custom metadata field
    pub fn set_metadata(&mut self, key: impl Into<String>, value: serde_json::Value) {
        self.metadata.insert(key.into(), value);
    }

    /// Get a custom metadata field
    pub fn get_metadata(&self, key: &str) -> Option<&serde_json::Value> {
        self.metadata.get(key)
    }

    /// Remove a custom metadata field
    pub fn remove_metadata(&mut self, key: &str) -> Option<serde_json::Value> {
        self.metadata.remove(key)
    }

    /// Check if a custom metadata field exists
    pub fn has_metadata(&self, key: &str) -> bool {
        self.metadata.contains_key(key)
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

/// Per-task-type TTL configuration for result expiration
///
/// Allows setting different TTLs for different task types, with a fallback
/// to a default TTL when no per-task override is configured.
///
/// # Example
/// ```
/// use celers_backend_redis::TaskTtlConfig;
/// use std::time::Duration;
///
/// let mut config = TaskTtlConfig::with_default(Duration::from_secs(3600));
/// config.set_task_ttl("long_running_task", Duration::from_secs(86400));
/// config.set_task_ttl("ephemeral_task", Duration::from_secs(60));
///
/// assert_eq!(config.get_ttl("long_running_task"), Some(Duration::from_secs(86400)));
/// assert_eq!(config.get_ttl("unknown_task"), Some(Duration::from_secs(3600)));
/// ```
#[derive(Debug, Clone)]
pub struct TaskTtlConfig {
    /// Default TTL for all task results
    default_ttl: Option<Duration>,
    /// Per-task-type TTL overrides (task_name -> TTL)
    task_ttls: HashMap<String, Duration>,
    /// TTL applied to chord state and counter keys (falls back to `default_ttl`)
    chord_ttl: Option<Duration>,
}

impl Default for TaskTtlConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskTtlConfig {
    /// Create a new empty TTL configuration (no default, no per-task overrides)
    pub fn new() -> Self {
        Self {
            default_ttl: None,
            task_ttls: HashMap::new(),
            chord_ttl: None,
        }
    }

    /// Create a TTL configuration with a default TTL for all tasks
    pub fn with_default(ttl: Duration) -> Self {
        Self {
            default_ttl: Some(ttl),
            task_ttls: HashMap::new(),
            chord_ttl: None,
        }
    }

    /// Set a per-task-type TTL override
    pub fn set_task_ttl(&mut self, task_name: &str, ttl: Duration) {
        self.task_ttls.insert(task_name.to_string(), ttl);
    }

    /// Get the TTL for a specific task type
    ///
    /// Returns the per-task TTL if one is set, otherwise falls back to
    /// the default TTL. Returns `None` if neither is configured.
    pub fn get_ttl(&self, task_name: &str) -> Option<Duration> {
        self.task_ttls.get(task_name).copied().or(self.default_ttl)
    }

    /// Check if this configuration has any TTLs configured
    pub fn is_empty(&self) -> bool {
        self.default_ttl.is_none() && self.task_ttls.is_empty() && self.chord_ttl.is_none()
    }

    /// Get the default TTL
    pub fn default_ttl(&self) -> Option<Duration> {
        self.default_ttl
    }

    /// Set the default TTL
    pub fn set_default_ttl(&mut self, ttl: Duration) {
        self.default_ttl = Some(ttl);
    }

    /// Remove the per-task TTL override for a specific task type
    pub fn remove_task_ttl(&mut self, task_name: &str) -> Option<Duration> {
        self.task_ttls.remove(task_name)
    }

    /// Get the number of per-task TTL overrides
    pub fn task_ttl_count(&self) -> usize {
        self.task_ttls.len()
    }

    /// Get the TTL applied to chord state and counter keys.
    ///
    /// Falls back to the default result TTL when no chord-specific value is
    /// configured, so chords never outlive the results they coordinate.
    pub fn chord_ttl(&self) -> Option<Duration> {
        self.chord_ttl.or(self.default_ttl)
    }

    /// Set an explicit TTL for chord state and counter keys.
    pub fn set_chord_ttl(&mut self, ttl: Duration) {
        self.chord_ttl = Some(ttl);
    }

    /// Builder form of [`Self::set_chord_ttl`].
    pub fn with_chord_ttl(mut self, ttl: Duration) -> Self {
        self.chord_ttl = Some(ttl);
        self
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

    /// Task to enqueue as a bare, name-only successor once the chord
    /// callback task itself completes.
    ///
    /// Without this, the callback task built from [`Self::callback`] carries
    /// no continuation at all, so a chord can never be a non-terminal step
    /// of a chain — whatever was supposed to run after the callback is
    /// silently dropped. Deliberately mirrors
    /// `celers_core::TaskMetadata::on_success_link`'s name-only idiom rather
    /// than embedding a full successor signature: it carries no args,
    /// kwargs, or options of its own, so the successor runs with the
    /// callback's raw result as its payload (the same "bare `on_success_link`,
    /// no chain tail" path `celers-worker`'s workflow module already
    /// implements for ordinary tasks). A successor that needs its own
    /// arguments or further chain steps needs the full canvas chain-tail
    /// representation instead, which this field does not attempt to replace.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub callback_on_success_link: Option<String>,

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
            callback_on_success_link: None,
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

    /// Set the bare, name-only successor to enqueue once the callback task
    /// completes. See [`Self::callback_on_success_link`] for what this can
    /// and cannot carry.
    pub fn with_callback_on_success_link(mut self, task_name: String) -> Self {
        self.callback_on_success_link = Some(task_name);
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

    /// Check if the chord's callback has a bare, name-only successor
    pub fn has_callback_on_success_link(&self) -> bool {
        self.callback_on_success_link.is_some()
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
