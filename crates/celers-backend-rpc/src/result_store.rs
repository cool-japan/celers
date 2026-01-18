//! ResultStore implementation for gRPC/RPC backend
//!
//! This module provides an adapter between the AsyncResult API (ResultStore trait)
//! and the gRPC result backend implementation.

use crate::{GrpcResultBackend, TaskMeta, TaskResult};
use async_trait::async_trait;
use celers_backend_redis::ResultBackend as LocalResultBackend;
use celers_core::result::{ResultStore, TaskResultValue};
use celers_core::{TaskId, TaskState};

/// Convert TaskResultValue to TaskResult (for storage)
fn to_task_result(value: &TaskResultValue) -> TaskResult {
    match value {
        TaskResultValue::Pending => TaskResult::Pending,
        TaskResultValue::Received => TaskResult::Pending,
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
            max_retries: 3,
        },
    }
}

/// Convert TaskResult to TaskState
fn task_result_to_state(result: &TaskResult) -> TaskState {
    match result {
        TaskResult::Pending => TaskState::Pending,
        TaskResult::Started => TaskState::Running,
        TaskResult::Success(v) => {
            let bytes = serde_json::to_vec(v).unwrap_or_default();
            TaskState::Succeeded(bytes)
        }
        TaskResult::Failure(msg) => TaskState::Failed(msg.clone()),
        TaskResult::Revoked => TaskState::Failed("Task revoked".to_string()),
        TaskResult::Retry(_) => TaskState::Running,
    }
}

#[async_trait]
impl ResultStore for GrpcResultBackend {
    async fn store_result(
        &self,
        task_id: TaskId,
        result: TaskResultValue,
    ) -> celers_core::Result<()> {
        let task_result = to_task_result(&result);
        let mut meta = TaskMeta::new(task_id, String::new());
        meta.result = task_result;

        let mut backend = self.clone();
        <GrpcResultBackend as LocalResultBackend>::store_result(&mut backend, task_id, &meta)
            .await
            .map_err(|e| celers_core::CelersError::Other(format!("gRPC error: {}", e)))
    }

    async fn get_result(&self, task_id: TaskId) -> celers_core::Result<Option<TaskResultValue>> {
        let mut backend = self.clone();
        match <GrpcResultBackend as LocalResultBackend>::get_result(&mut backend, task_id).await {
            Ok(Some(meta)) => Ok(Some(from_task_result(&meta.result))),
            Ok(None) => Ok(None),
            Err(e) => Err(celers_core::CelersError::Other(format!(
                "gRPC error: {}",
                e
            ))),
        }
    }

    async fn get_state(&self, task_id: TaskId) -> celers_core::Result<TaskState> {
        let mut backend = self.clone();
        match <GrpcResultBackend as LocalResultBackend>::get_result(&mut backend, task_id).await {
            Ok(Some(meta)) => Ok(task_result_to_state(&meta.result)),
            Ok(None) => Ok(TaskState::Pending),
            Err(e) => Err(celers_core::CelersError::Other(format!(
                "gRPC error: {}",
                e
            ))),
        }
    }

    async fn forget(&self, task_id: TaskId) -> celers_core::Result<()> {
        let mut backend = self.clone();
        <GrpcResultBackend as LocalResultBackend>::delete_result(&mut backend, task_id)
            .await
            .map_err(|e| celers_core::CelersError::Other(format!("gRPC error: {}", e)))
    }

    async fn has_result(&self, task_id: TaskId) -> celers_core::Result<bool> {
        let mut backend = self.clone();
        match <GrpcResultBackend as LocalResultBackend>::get_result(&mut backend, task_id).await {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => Err(celers_core::CelersError::Other(format!(
                "gRPC error: {}",
                e
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_task_result_conversion() {
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
}
