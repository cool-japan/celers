//! ResultStore implementation for Redis backend
//!
//! This module provides an adapter between the AsyncResult API (ResultStore trait)
//! and the Redis result backend implementation.

use crate::{RedisResultBackend, TaskMeta, TaskResult};
use async_trait::async_trait;
use celers_core::result::{ResultStore, TaskResultValue};
use celers_core::{TaskId, TaskState};

// Import the local ResultBackend trait explicitly
use crate::ResultBackend as LocalResultBackend;

/// Convert TaskResultValue to TaskResult (for storage)
fn to_task_result(value: &TaskResultValue) -> TaskResult {
    match value {
        TaskResultValue::Pending => TaskResult::Pending,
        TaskResultValue::Received => TaskResult::Pending, // Map to Pending
        TaskResultValue::Started => TaskResult::Started,
        TaskResultValue::Success(v) => TaskResult::Success(v.clone()),
        TaskResultValue::Failure { error, .. } => TaskResult::Failure(error.clone()),
        TaskResultValue::Revoked => TaskResult::Revoked,
        TaskResultValue::Retry { attempt, .. } => TaskResult::Retry(*attempt),
        TaskResultValue::Rejected { reason } => TaskResult::Failure(reason.clone()),
    }
}

/// Convert TaskResult to TaskResultValue (for retrieval)
fn from_task_result(result: &TaskResult) -> TaskResultValue {
    match result {
        TaskResult::Pending => TaskResultValue::Pending,
        TaskResult::Started => TaskResultValue::Started,
        TaskResult::Success(v) => TaskResultValue::Success(v.clone()),
        TaskResult::Failure(msg) => TaskResultValue::Failure {
            error: msg.clone(),
            traceback: None,
        },
        TaskResult::Revoked => TaskResultValue::Revoked,
        TaskResult::Retry(count) => TaskResultValue::Retry {
            attempt: *count,
            max_retries: 3, // Default, actual value would need to come from config
        },
    }
}

/// Convert TaskResult to TaskState
fn task_result_to_state(result: &TaskResult) -> TaskState {
    match result {
        TaskResult::Pending => TaskState::Pending,
        TaskResult::Started => TaskState::Running,
        TaskResult::Success(v) => {
            // Serialize to bytes for TaskState::Succeeded
            let bytes = serde_json::to_vec(v).unwrap_or_default();
            TaskState::Succeeded(bytes)
        }
        TaskResult::Failure(msg) => TaskState::Failed(msg.clone()),
        TaskResult::Revoked => TaskState::Failed("Task revoked".to_string()),
        TaskResult::Retry(_) => TaskState::Running, // Retrying is still running
    }
}

#[async_trait]
impl ResultStore for RedisResultBackend {
    /// Record a task outcome.
    ///
    /// This is a read-modify-write: the existing record is loaded first and
    /// only `result` (plus `completed_at` for terminal states) is changed, so
    /// `task_name`, `worker`, `started_at`, `created_at`, `tags`, `metadata`
    /// and friends survive. Writing a freshly constructed `TaskMeta` here used
    /// to wipe every one of those fields — and, with an empty `task_name`, it
    /// also made per-task-type TTL configuration unreachable.
    ///
    /// The clones below are shallow: connection, cache, metrics and compression
    /// statistics all live behind `Arc`, so statistics recorded through this
    /// adapter land on the shared counters.
    async fn store_result(
        &self,
        task_id: TaskId,
        result: TaskResultValue,
    ) -> celers_core::Result<()> {
        let task_result = to_task_result(&result);
        let mut backend = self.clone();

        // Read through Redis rather than the cache: a task that is still
        // running is deliberately not cached.
        let existing = backend
            .get_result_uncached(task_id)
            .await
            .map_err(|e| celers_core::CelersError::Other(format!("Redis error: {}", e)))?;

        let meta = match existing {
            Some(mut meta) => {
                let is_terminal = task_result.is_terminal();
                meta.result = task_result;
                if is_terminal && meta.completed_at.is_none() {
                    meta.completed_at = Some(chrono::Utc::now());
                }
                meta
            }
            None => {
                // Nothing recorded yet: the task name is genuinely unknown at
                // this layer, so the default TTL applies.
                let mut meta = TaskMeta::new(task_id, String::new());
                let is_terminal = task_result.is_terminal();
                meta.result = task_result;
                if is_terminal {
                    meta.completed_at = Some(chrono::Utc::now());
                }
                meta
            }
        };

        <RedisResultBackend as LocalResultBackend>::store_result(&mut backend, task_id, &meta)
            .await
            .map_err(|e| celers_core::CelersError::Other(format!("Redis error: {}", e)))
    }

    async fn get_result(&self, task_id: TaskId) -> celers_core::Result<Option<TaskResultValue>> {
        let mut backend = self.clone();
        match <RedisResultBackend as LocalResultBackend>::get_result(&mut backend, task_id).await {
            Ok(Some(meta)) => Ok(Some(from_task_result(&meta.result))),
            Ok(None) => Ok(None),
            Err(e) => Err(celers_core::CelersError::Other(format!(
                "Redis error: {}",
                e
            ))),
        }
    }

    async fn get_state(&self, task_id: TaskId) -> celers_core::Result<TaskState> {
        let mut backend = self.clone();
        match <RedisResultBackend as LocalResultBackend>::get_result(&mut backend, task_id).await {
            Ok(Some(meta)) => Ok(task_result_to_state(&meta.result)),
            Ok(None) => Ok(TaskState::Pending),
            Err(e) => Err(celers_core::CelersError::Other(format!(
                "Redis error: {}",
                e
            ))),
        }
    }

    async fn forget(&self, task_id: TaskId) -> celers_core::Result<()> {
        let mut backend = self.clone();
        <RedisResultBackend as LocalResultBackend>::delete_result(&mut backend, task_id)
            .await
            .map_err(|e| celers_core::CelersError::Other(format!("Redis error: {}", e)))
    }

    async fn has_result(&self, task_id: TaskId) -> celers_core::Result<bool> {
        // `EXISTS` avoids fetching (and decrypting, decompressing, reassembling)
        // a payload we are going to throw away.
        let mut backend = self.clone();
        backend
            .task_exists(task_id)
            .await
            .map_err(|e| celers_core::CelersError::Other(format!("Redis error: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_task_result_conversion() {
        // Test Success conversion
        let value = TaskResultValue::Success(json!({"result": 42}));
        let result = to_task_result(&value);
        assert!(matches!(result, TaskResult::Success(_)));

        let back = from_task_result(&result);
        assert!(matches!(back, TaskResultValue::Success(_)));
    }

    #[test]
    fn test_failure_conversion() {
        let value = TaskResultValue::Failure {
            error: "Test error".to_string(),
            traceback: Some("Stack trace".to_string()),
        };
        let result = to_task_result(&value);
        assert!(matches!(result, TaskResult::Failure(_)));

        if let TaskResult::Failure(msg) = result {
            assert_eq!(msg, "Test error");
        }
    }

    #[test]
    fn test_state_conversion() {
        let result = TaskResult::Success(json!({"value": 123}));
        let state = task_result_to_state(&result);
        assert!(matches!(state, TaskState::Succeeded(_)));

        let result = TaskResult::Failure("error".to_string());
        let state = task_result_to_state(&result);
        assert!(matches!(state, TaskState::Failed(_)));
    }

    #[test]
    fn test_retry_conversion() {
        let value = TaskResultValue::Retry {
            attempt: 2,
            max_retries: 5,
        };
        let result = to_task_result(&value);
        assert!(matches!(result, TaskResult::Retry(2)));
    }

    #[test]
    fn test_revoked_conversion() {
        let value = TaskResultValue::Revoked;
        let result = to_task_result(&value);
        assert!(matches!(result, TaskResult::Revoked));

        let back = from_task_result(&result);
        assert!(matches!(back, TaskResultValue::Revoked));
    }
}
