//! Task result message format
//!
//! This module provides the Celery-compatible result message format for
//! storing and retrieving task results.
//!
//! # Result States
//!
//! - `PENDING` - Task is waiting for execution
//! - `RECEIVED` - Task was received by a worker
//! - `STARTED` - Task execution started
//! - `SUCCESS` - Task completed successfully
//! - `FAILURE` - Task execution failed
//! - `RETRY` - Task is being retried
//! - `REVOKED` - Task was revoked
//!
//! # Example
//!
//! ```
//! use celers_protocol::result::{ResultMessage, TaskStatus};
//! use uuid::Uuid;
//! use serde_json::json;
//!
//! let task_id = Uuid::new_v4();
//! let result = ResultMessage::success(task_id, json!(42));
//! assert!(result.is_success());
//! ```

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Task execution status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum TaskStatus {
    /// Task is waiting for execution
    Pending,
    /// Task was received by a worker
    Received,
    /// Task execution started
    Started,
    /// Task completed successfully
    Success,
    /// Task execution failed
    Failure,
    /// Task is being retried
    Retry,
    /// Task was revoked
    Revoked,
}

impl TaskStatus {
    /// Check if this is a terminal state (no more transitions)
    #[inline]
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskStatus::Success | TaskStatus::Failure | TaskStatus::Revoked
        )
    }

    /// Check if this is a successful state
    #[inline]
    pub fn is_success(&self) -> bool {
        matches!(self, TaskStatus::Success)
    }

    /// Check if this is a failure state
    #[inline]
    pub fn is_failure(&self) -> bool {
        matches!(self, TaskStatus::Failure)
    }

    /// Check if this is a ready state (has a result)
    #[inline]
    pub fn is_ready(&self) -> bool {
        self.is_terminal()
    }

    /// Get the string representation
    #[inline]
    pub fn as_str(&self) -> &'static str {
        match self {
            TaskStatus::Pending => "PENDING",
            TaskStatus::Received => "RECEIVED",
            TaskStatus::Started => "STARTED",
            TaskStatus::Success => "SUCCESS",
            TaskStatus::Failure => "FAILURE",
            TaskStatus::Retry => "RETRY",
            TaskStatus::Revoked => "REVOKED",
        }
    }
}

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Default for TaskStatus {
    fn default() -> Self {
        Self::Pending
    }
}

impl std::str::FromStr for TaskStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "PENDING" => Ok(TaskStatus::Pending),
            "RECEIVED" => Ok(TaskStatus::Received),
            "STARTED" => Ok(TaskStatus::Started),
            "SUCCESS" => Ok(TaskStatus::Success),
            "FAILURE" => Ok(TaskStatus::Failure),
            "RETRY" => Ok(TaskStatus::Retry),
            "REVOKED" => Ok(TaskStatus::Revoked),
            _ => Err(format!("Invalid task status: {}", s)),
        }
    }
}

/// Exception information for failed tasks
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ExceptionInfo {
    /// Exception type name
    #[serde(rename = "exc_type")]
    pub exc_type: String,

    /// Exception message
    #[serde(rename = "exc_message")]
    pub exc_message: String,

    /// Full traceback (if available)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traceback: Option<String>,
}

impl ExceptionInfo {
    /// Create new exception info
    pub fn new(exc_type: impl Into<String>, exc_message: impl Into<String>) -> Self {
        Self {
            exc_type: exc_type.into(),
            exc_message: exc_message.into(),
            traceback: None,
        }
    }

    /// Set the traceback
    #[must_use]
    pub fn with_traceback(mut self, traceback: impl Into<String>) -> Self {
        self.traceback = Some(traceback.into());
        self
    }
}

/// Task result message (Celery-compatible format)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResultMessage {
    /// Task ID
    pub task_id: Uuid,

    /// Task status
    pub status: TaskStatus,

    /// Result value (for SUCCESS)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,

    /// Traceback (for FAILURE)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traceback: Option<String>,

    /// Exception info (for FAILURE)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exception: Option<ExceptionInfo>,

    /// Timestamp when result was created
    #[serde(skip_serializing_if = "Option::is_none")]
    pub date_done: Option<DateTime<Utc>>,

    /// Task name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task: Option<String>,

    /// Worker that executed the task
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker: Option<String>,

    /// Retry count
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retries: Option<u32>,

    /// Parent task ID (for workflows)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<Uuid>,

    /// Root task ID (for workflows)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_id: Option<Uuid>,

    /// Group ID (for grouped tasks)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_id: Option<Uuid>,

    /// Children task IDs
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub children: Vec<Uuid>,

    /// Additional metadata
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub meta: HashMap<String, serde_json::Value>,
}

impl ResultMessage {
    /// Create a new result message
    pub fn new(task_id: Uuid, status: TaskStatus) -> Self {
        Self {
            task_id,
            status,
            result: None,
            traceback: None,
            exception: None,
            date_done: None,
            task: None,
            worker: None,
            retries: None,
            parent_id: None,
            root_id: None,
            group_id: None,
            children: Vec::new(),
            meta: HashMap::new(),
        }
    }

    /// Create a pending result
    pub fn pending(task_id: Uuid) -> Self {
        Self::new(task_id, TaskStatus::Pending)
    }

    /// Create a successful result
    pub fn success(task_id: Uuid, result: serde_json::Value) -> Self {
        Self {
            result: Some(result),
            date_done: Some(Utc::now()),
            ..Self::new(task_id, TaskStatus::Success)
        }
    }

    /// Create a failure result
    pub fn failure(task_id: Uuid, exc_type: &str, exc_message: &str) -> Self {
        Self {
            exception: Some(ExceptionInfo::new(exc_type, exc_message)),
            date_done: Some(Utc::now()),
            ..Self::new(task_id, TaskStatus::Failure)
        }
    }

    /// Create a failure result with traceback
    pub fn failure_with_traceback(
        task_id: Uuid,
        exc_type: &str,
        exc_message: &str,
        traceback: &str,
    ) -> Self {
        Self {
            exception: Some(ExceptionInfo::new(exc_type, exc_message).with_traceback(traceback)),
            traceback: Some(traceback.to_string()),
            date_done: Some(Utc::now()),
            ..Self::new(task_id, TaskStatus::Failure)
        }
    }

    /// Create a retry result
    pub fn retry(task_id: Uuid, retries: u32) -> Self {
        Self {
            retries: Some(retries),
            ..Self::new(task_id, TaskStatus::Retry)
        }
    }

    /// Create a revoked result
    pub fn revoked(task_id: Uuid) -> Self {
        Self {
            date_done: Some(Utc::now()),
            ..Self::new(task_id, TaskStatus::Revoked)
        }
    }

    /// Create a started result
    pub fn started(task_id: Uuid) -> Self {
        Self::new(task_id, TaskStatus::Started)
    }

    /// Create a received result
    pub fn received(task_id: Uuid) -> Self {
        Self::new(task_id, TaskStatus::Received)
    }

    /// Set the task name
    #[must_use]
    pub fn with_task(mut self, task: impl Into<String>) -> Self {
        self.task = Some(task.into());
        self
    }

    /// Set the worker name
    #[must_use]
    pub fn with_worker(mut self, worker: impl Into<String>) -> Self {
        self.worker = Some(worker.into());
        self
    }

    /// Set the parent task ID
    #[must_use]
    pub fn with_parent(mut self, parent_id: Uuid) -> Self {
        self.parent_id = Some(parent_id);
        self
    }

    /// Set the root task ID
    #[must_use]
    pub fn with_root(mut self, root_id: Uuid) -> Self {
        self.root_id = Some(root_id);
        self
    }

    /// Set the group ID
    #[must_use]
    pub fn with_group(mut self, group_id: Uuid) -> Self {
        self.group_id = Some(group_id);
        self
    }

    /// Add a child task ID
    #[must_use]
    pub fn with_child(mut self, child_id: Uuid) -> Self {
        self.children.push(child_id);
        self
    }

    /// Set children task IDs
    #[must_use]
    pub fn with_children(mut self, children: Vec<Uuid>) -> Self {
        self.children = children;
        self
    }

    /// Add metadata
    #[must_use]
    pub fn with_meta(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.meta.insert(key.into(), value);
        self
    }

    /// Set retry count
    #[must_use]
    pub fn with_retries(mut self, retries: u32) -> Self {
        self.retries = Some(retries);
        self
    }

    /// Set completion timestamp
    #[must_use]
    pub fn with_date_done(mut self, date_done: DateTime<Utc>) -> Self {
        self.date_done = Some(date_done);
        self
    }

    /// Add a single metadata entry (mutable)
    pub fn add_meta(&mut self, key: impl Into<String>, value: serde_json::Value) {
        self.meta.insert(key.into(), value);
    }

    /// Get a metadata value by key
    #[inline]
    pub fn get_meta(&self, key: &str) -> Option<&serde_json::Value> {
        self.meta.get(key)
    }

    /// Check if metadata key exists
    #[inline]
    pub fn has_meta(&self, key: &str) -> bool {
        self.meta.contains_key(key)
    }

    /// Get the number of metadata entries
    #[inline]
    pub fn meta_len(&self) -> usize {
        self.meta.len()
    }

    /// Get the retry count (defaults to 0 if not set)
    #[inline]
    pub fn retry_count(&self) -> u32 {
        self.retries.unwrap_or(0)
    }

    /// Check if the result is ready
    #[inline]
    pub fn is_ready(&self) -> bool {
        self.status.is_ready()
    }

    /// Check if the task succeeded
    #[inline]
    pub fn is_success(&self) -> bool {
        self.status.is_success()
    }

    /// Check if the task failed
    #[inline]
    pub fn is_failure(&self) -> bool {
        self.status.is_failure()
    }

    /// Get the result value (if success)
    #[inline]
    pub fn get_result(&self) -> Option<&serde_json::Value> {
        if self.is_success() {
            self.result.as_ref()
        } else {
            None
        }
    }

    /// Get the exception info (if failure)
    #[inline]
    pub fn get_exception(&self) -> Option<&ExceptionInfo> {
        if self.is_failure() {
            self.exception.as_ref()
        } else {
            None
        }
    }

    /// Serialize to JSON bytes
    pub fn to_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Deserialize from JSON bytes
    pub fn from_json(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_task_status_is_terminal() {
        assert!(!TaskStatus::Pending.is_terminal());
        assert!(!TaskStatus::Received.is_terminal());
        assert!(!TaskStatus::Started.is_terminal());
        assert!(TaskStatus::Success.is_terminal());
        assert!(TaskStatus::Failure.is_terminal());
        assert!(!TaskStatus::Retry.is_terminal());
        assert!(TaskStatus::Revoked.is_terminal());
    }

    #[test]
    fn test_task_status_as_str() {
        assert_eq!(TaskStatus::Pending.as_str(), "PENDING");
        assert_eq!(TaskStatus::Success.as_str(), "SUCCESS");
        assert_eq!(TaskStatus::Failure.as_str(), "FAILURE");
    }

    #[test]
    fn test_task_status_display() {
        assert_eq!(TaskStatus::Success.to_string(), "SUCCESS");
        assert_eq!(TaskStatus::Failure.to_string(), "FAILURE");
    }

    #[test]
    fn test_task_status_default() {
        assert_eq!(TaskStatus::default(), TaskStatus::Pending);
    }

    #[test]
    fn test_task_status_from_str() {
        use std::str::FromStr;

        assert_eq!(
            TaskStatus::from_str("PENDING").unwrap(),
            TaskStatus::Pending
        );
        assert_eq!(
            TaskStatus::from_str("pending").unwrap(),
            TaskStatus::Pending
        );
        assert_eq!(
            TaskStatus::from_str("RECEIVED").unwrap(),
            TaskStatus::Received
        );
        assert_eq!(
            TaskStatus::from_str("STARTED").unwrap(),
            TaskStatus::Started
        );
        assert_eq!(
            TaskStatus::from_str("SUCCESS").unwrap(),
            TaskStatus::Success
        );
        assert_eq!(
            TaskStatus::from_str("success").unwrap(),
            TaskStatus::Success
        );
        assert_eq!(
            TaskStatus::from_str("FAILURE").unwrap(),
            TaskStatus::Failure
        );
        assert_eq!(TaskStatus::from_str("RETRY").unwrap(), TaskStatus::Retry);
        assert_eq!(
            TaskStatus::from_str("REVOKED").unwrap(),
            TaskStatus::Revoked
        );

        assert!(TaskStatus::from_str("INVALID").is_err());
        assert!(TaskStatus::from_str("").is_err());
    }

    #[test]
    fn test_result_message_success() {
        let task_id = Uuid::new_v4();
        let result = ResultMessage::success(task_id, json!({"answer": 42}));

        assert_eq!(result.task_id, task_id);
        assert!(result.is_success());
        assert!(result.is_ready());
        assert!(!result.is_failure());
        assert!(result.date_done.is_some());
        assert_eq!(result.get_result(), Some(&json!({"answer": 42})));
    }

    #[test]
    fn test_result_message_failure() {
        let task_id = Uuid::new_v4();
        let result = ResultMessage::failure(task_id, "ValueError", "Invalid input");

        assert_eq!(result.task_id, task_id);
        assert!(result.is_failure());
        assert!(result.is_ready());
        assert!(!result.is_success());
        assert!(result.date_done.is_some());

        let exc = result.get_exception().unwrap();
        assert_eq!(exc.exc_type, "ValueError");
        assert_eq!(exc.exc_message, "Invalid input");
    }

    #[test]
    fn test_result_message_failure_with_traceback() {
        let task_id = Uuid::new_v4();
        let traceback = "Traceback (most recent call last):\n  File \"test.py\"...";
        let result = ResultMessage::failure_with_traceback(
            task_id,
            "RuntimeError",
            "Test failed",
            traceback,
        );

        assert!(result.is_failure());
        assert_eq!(result.traceback, Some(traceback.to_string()));
        assert_eq!(
            result.exception.as_ref().unwrap().traceback,
            Some(traceback.to_string())
        );
    }

    #[test]
    fn test_result_message_pending() {
        let task_id = Uuid::new_v4();
        let result = ResultMessage::pending(task_id);

        assert_eq!(result.status, TaskStatus::Pending);
        assert!(!result.is_ready());
    }

    #[test]
    fn test_result_message_retry() {
        let task_id = Uuid::new_v4();
        let result = ResultMessage::retry(task_id, 3);

        assert_eq!(result.status, TaskStatus::Retry);
        assert_eq!(result.retries, Some(3));
        assert!(!result.is_ready());
    }

    #[test]
    fn test_result_message_revoked() {
        let task_id = Uuid::new_v4();
        let result = ResultMessage::revoked(task_id);

        assert_eq!(result.status, TaskStatus::Revoked);
        assert!(result.is_ready());
        assert!(result.date_done.is_some());
    }

    #[test]
    fn test_result_message_builders() {
        let task_id = Uuid::new_v4();
        let parent_id = Uuid::new_v4();
        let root_id = Uuid::new_v4();
        let group_id = Uuid::new_v4();
        let child_id = Uuid::new_v4();

        let result = ResultMessage::success(task_id, json!(100))
            .with_task("tasks.add")
            .with_worker("worker-1")
            .with_parent(parent_id)
            .with_root(root_id)
            .with_group(group_id)
            .with_child(child_id)
            .with_meta("custom", json!("value"));

        assert_eq!(result.task, Some("tasks.add".to_string()));
        assert_eq!(result.worker, Some("worker-1".to_string()));
        assert_eq!(result.parent_id, Some(parent_id));
        assert_eq!(result.root_id, Some(root_id));
        assert_eq!(result.group_id, Some(group_id));
        assert_eq!(result.children, vec![child_id]);
        assert_eq!(result.meta.get("custom"), Some(&json!("value")));
    }

    #[test]
    fn test_result_message_json_round_trip() {
        let task_id = Uuid::new_v4();
        let result = ResultMessage::success(task_id, json!({"data": [1, 2, 3]}))
            .with_task("tasks.process")
            .with_worker("worker-2");

        let json_bytes = result.to_json().unwrap();
        let decoded = ResultMessage::from_json(&json_bytes).unwrap();

        assert_eq!(decoded.task_id, task_id);
        assert_eq!(decoded.status, TaskStatus::Success);
        assert_eq!(decoded.task, Some("tasks.process".to_string()));
        assert_eq!(decoded.worker, Some("worker-2".to_string()));
    }

    #[test]
    fn test_result_message_serialization_format() {
        let task_id = Uuid::new_v4();
        let result = ResultMessage::success(task_id, json!(42));

        let json_str = serde_json::to_string(&result).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        // Verify Celery-compatible field names
        assert!(value.get("task_id").is_some());
        assert!(value.get("status").is_some());
        assert!(value.get("result").is_some());
        assert_eq!(value["status"], "SUCCESS");
    }

    #[test]
    fn test_exception_info() {
        let exc =
            ExceptionInfo::new("TypeError", "Expected int, got str").with_traceback("at line 42");

        assert_eq!(exc.exc_type, "TypeError");
        assert_eq!(exc.exc_message, "Expected int, got str");
        assert_eq!(exc.traceback, Some("at line 42".to_string()));
    }

    #[test]
    fn test_exception_info_default() {
        let exc = ExceptionInfo::default();

        assert_eq!(exc.exc_type, "");
        assert_eq!(exc.exc_message, "");
        assert_eq!(exc.traceback, None);

        // Test that default can be used in builder patterns
        let exc_builder = ExceptionInfo::default().with_traceback("some traceback");

        assert_eq!(exc_builder.traceback, Some("some traceback".to_string()));
    }

    #[test]
    fn test_with_children() {
        let task_id = Uuid::new_v4();
        let children = vec![Uuid::new_v4(), Uuid::new_v4()];

        let result = ResultMessage::success(task_id, json!(null)).with_children(children.clone());

        assert_eq!(result.children, children);
    }

    #[test]
    fn test_result_message_with_retries() {
        let result = ResultMessage::new(Uuid::new_v4(), TaskStatus::Retry).with_retries(5);

        assert_eq!(result.retries, Some(5));
        assert_eq!(result.retry_count(), 5);
    }

    #[test]
    fn test_result_message_retry_count_default() {
        let result = ResultMessage::new(Uuid::new_v4(), TaskStatus::Success);

        assert_eq!(result.retries, None);
        assert_eq!(result.retry_count(), 0); // Defaults to 0
    }

    #[test]
    fn test_result_message_with_date_done() {
        let now = chrono::Utc::now();
        let result = ResultMessage::new(Uuid::new_v4(), TaskStatus::Success).with_date_done(now);

        assert_eq!(result.date_done, Some(now));
    }

    #[test]
    fn test_result_message_metadata() {
        let mut result = ResultMessage::new(Uuid::new_v4(), TaskStatus::Success);

        // Test add_meta (mutable)
        result.add_meta("key1", json!("value1"));
        result.add_meta("key2", json!(42));

        assert_eq!(result.meta_len(), 2);
        assert!(result.has_meta("key1"));
        assert!(result.has_meta("key2"));
        assert!(!result.has_meta("key3"));

        assert_eq!(result.get_meta("key1"), Some(&json!("value1")));
        assert_eq!(result.get_meta("key2"), Some(&json!(42)));
        assert_eq!(result.get_meta("key3"), None);
    }

    #[test]
    fn test_result_message_with_meta_builder() {
        let result = ResultMessage::new(Uuid::new_v4(), TaskStatus::Success)
            .with_meta("version", json!("1.0.0"))
            .with_meta("region", json!("us-west-2"));

        assert_eq!(result.meta_len(), 2);
        assert_eq!(result.get_meta("version"), Some(&json!("1.0.0")));
        assert_eq!(result.get_meta("region"), Some(&json!("us-west-2")));
    }

    #[test]
    fn test_result_message_builder_chaining() {
        let task_id = Uuid::new_v4();
        let parent_id = Uuid::new_v4();
        let root_id = Uuid::new_v4();
        let now = chrono::Utc::now();

        let result = ResultMessage::success(task_id, json!({"data": 42}))
            .with_task("my.task")
            .with_worker("worker-1")
            .with_parent(parent_id)
            .with_root(root_id)
            .with_retries(3)
            .with_date_done(now)
            .with_meta("source", json!("api"));

        assert_eq!(result.task_id, task_id);
        assert_eq!(result.status, TaskStatus::Success);
        assert_eq!(result.task, Some("my.task".to_string()));
        assert_eq!(result.worker, Some("worker-1".to_string()));
        assert_eq!(result.parent_id, Some(parent_id));
        assert_eq!(result.root_id, Some(root_id));
        assert_eq!(result.retry_count(), 3);
        assert_eq!(result.date_done, Some(now));
        assert_eq!(result.get_meta("source"), Some(&json!("api")));
    }
}
