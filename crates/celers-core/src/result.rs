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
