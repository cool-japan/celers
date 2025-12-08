//! gRPC/RPC result backend for CeleRS
//!
//! This crate provides gRPC-based result storage for distributed microservices architectures.
//!
//! # Features
//!
//! - gRPC client for remote result storage
//! - Service mesh compatible
//! - Real-time result streaming (future)
//! - Distributed result storage
//! - Load balancing ready
//!
//! # Example
//!
//! ```ignore
//! use celers_backend_rpc::GrpcResultBackend;
//! use celers_backend_redis::ResultBackend;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut backend = GrpcResultBackend::connect("http://localhost:50051").await?;
//!
//! // Store result
//! let meta = TaskMeta::new(task_id, "my_task".to_string());
//! backend.store_result(task_id, &meta).await?;
//! # Ok(())
//! # }
//! ```

pub mod result_store;

use async_trait::async_trait;
pub use celers_backend_redis::{
    BackendError, ChordState, Result, ResultBackend, TaskMeta, TaskResult,
};
use chrono::{TimeZone, Utc};
use std::time::Duration;
use tonic::transport::Channel;
use uuid::Uuid;

// Include generated protobuf code
pub mod proto {
    tonic::include_proto!("result_backend");
}

use proto::{
    result_backend_service_client::ResultBackendServiceClient, ChordCompleteTaskRequest,
    ChordGetStateRequest, ChordInitRequest, DeleteResultRequest, GetResultRequest,
    SetExpirationRequest, StoreResultRequest, TaskResultState,
};

/// gRPC result backend client
#[derive(Clone)]
pub struct GrpcResultBackend {
    client: ResultBackendServiceClient<Channel>,
}

impl GrpcResultBackend {
    /// Connect to gRPC result backend service
    ///
    /// # Arguments
    /// * `endpoint` - gRPC server endpoint (e.g., "http://localhost:50051")
    pub async fn connect(endpoint: &str) -> Result<Self> {
        let client = ResultBackendServiceClient::connect(endpoint.to_string())
            .await
            .map_err(|e| {
                BackendError::Connection(format!("Failed to connect to gRPC server: {}", e))
            })?;

        Ok(Self { client })
    }

    /// Connect with custom channel
    pub fn from_channel(channel: Channel) -> Self {
        let client = ResultBackendServiceClient::new(channel);
        Self { client }
    }

    /// Convert TaskMeta to proto TaskMeta
    fn to_proto_meta(&self, meta: &TaskMeta) -> proto::TaskMeta {
        let (result_state, result_data, error_message, retry_count) = match &meta.result {
            TaskResult::Pending => (TaskResultState::Pending, None, None, None),
            TaskResult::Started => (TaskResultState::Started, None, None, None),
            TaskResult::Success(data) => {
                let json_str = serde_json::to_string(data).ok();
                (TaskResultState::Success, json_str, None, None)
            }
            TaskResult::Failure(err) => (TaskResultState::Failure, None, Some(err.clone()), None),
            TaskResult::Revoked => (TaskResultState::Revoked, None, None, None),
            TaskResult::Retry(count) => (TaskResultState::Retry, None, None, Some(*count)),
        };

        proto::TaskMeta {
            task_id: meta.task_id.to_string(),
            task_name: meta.task_name.clone(),
            result_state: result_state as i32,
            result_data,
            error_message,
            retry_count,
            created_at: meta.created_at.timestamp(),
            started_at: meta.started_at.map(|dt| dt.timestamp()),
            completed_at: meta.completed_at.map(|dt| dt.timestamp()),
            worker: meta.worker.clone(),
        }
    }

    /// Convert proto TaskMeta to TaskMeta
    fn from_proto_meta(proto_meta: proto::TaskMeta) -> Result<TaskMeta> {
        let result_state = TaskResultState::try_from(proto_meta.result_state)
            .map_err(|_| BackendError::Serialization("Invalid result state".to_string()))?;

        let result = match result_state {
            TaskResultState::Pending => TaskResult::Pending,
            TaskResultState::Started => TaskResult::Started,
            TaskResultState::Success => {
                let data = proto_meta
                    .result_data
                    .and_then(|s| serde_json::from_str(&s).ok())
                    .unwrap_or(serde_json::Value::Null);
                TaskResult::Success(data)
            }
            TaskResultState::Failure => {
                TaskResult::Failure(proto_meta.error_message.unwrap_or_default())
            }
            TaskResultState::Revoked => TaskResult::Revoked,
            TaskResultState::Retry => TaskResult::Retry(proto_meta.retry_count.unwrap_or(0)),
        };

        let task_id = Uuid::parse_str(&proto_meta.task_id)
            .map_err(|e| BackendError::Serialization(format!("Invalid UUID: {}", e)))?;

        let created_at = Utc
            .timestamp_opt(proto_meta.created_at, 0)
            .single()
            .ok_or_else(|| {
                BackendError::Serialization("Invalid created_at timestamp".to_string())
            })?;

        let started_at = proto_meta
            .started_at
            .and_then(|ts| Utc.timestamp_opt(ts, 0).single());

        let completed_at = proto_meta
            .completed_at
            .and_then(|ts| Utc.timestamp_opt(ts, 0).single());

        Ok(TaskMeta {
            task_id,
            task_name: proto_meta.task_name,
            result,
            created_at,
            started_at,
            completed_at,
            worker: proto_meta.worker,
            progress: None,
        })
    }

    /// Convert ChordState to proto ChordState
    fn to_proto_chord(&self, state: &ChordState) -> proto::ChordState {
        proto::ChordState {
            chord_id: state.chord_id.to_string(),
            total: state.total as u32,
            completed: state.completed as u32,
            callback: state.callback.clone(),
            task_ids: state.task_ids.iter().map(|id| id.to_string()).collect(),
            created_at: state.created_at.timestamp(),
            timeout_seconds: state.timeout.map(|d| d.as_secs()),
            cancelled: state.cancelled,
            cancellation_reason: state.cancellation_reason.clone(),
        }
    }

    /// Convert proto ChordState to ChordState
    fn from_proto_chord(proto_state: proto::ChordState) -> Result<ChordState> {
        let chord_id = Uuid::parse_str(&proto_state.chord_id)
            .map_err(|e| BackendError::Serialization(format!("Invalid chord UUID: {}", e)))?;

        let task_ids: Result<Vec<Uuid>> = proto_state
            .task_ids
            .iter()
            .map(|s| {
                Uuid::parse_str(s)
                    .map_err(|e| BackendError::Serialization(format!("Invalid task UUID: {}", e)))
            })
            .collect();

        Ok(ChordState {
            chord_id,
            total: proto_state.total as usize,
            completed: proto_state.completed as usize,
            callback: proto_state.callback,
            task_ids: task_ids?,
            created_at: Utc
                .timestamp_opt(proto_state.created_at, 0)
                .single()
                .ok_or_else(|| BackendError::Serialization("Invalid timestamp".to_string()))?,
            timeout: proto_state.timeout_seconds.map(Duration::from_secs),
            cancelled: proto_state.cancelled,
            cancellation_reason: proto_state.cancellation_reason,
        })
    }
}

#[async_trait]
impl ResultBackend for GrpcResultBackend {
    async fn store_result(&mut self, task_id: Uuid, meta: &TaskMeta) -> Result<()> {
        let request = tonic::Request::new(StoreResultRequest {
            task_id: task_id.to_string(),
            meta: Some(self.to_proto_meta(meta)),
        });

        self.client
            .store_result(request)
            .await
            .map_err(|e| BackendError::Connection(format!("gRPC error: {}", e)))?;

        Ok(())
    }

    async fn get_result(&mut self, task_id: Uuid) -> Result<Option<TaskMeta>> {
        let request = tonic::Request::new(GetResultRequest {
            task_id: task_id.to_string(),
        });

        let response = self
            .client
            .get_result(request)
            .await
            .map_err(|e| BackendError::Connection(format!("gRPC error: {}", e)))?;

        match response.into_inner().meta {
            Some(proto_meta) => Ok(Some(Self::from_proto_meta(proto_meta)?)),
            None => Ok(None),
        }
    }

    async fn delete_result(&mut self, task_id: Uuid) -> Result<()> {
        let request = tonic::Request::new(DeleteResultRequest {
            task_id: task_id.to_string(),
        });

        self.client
            .delete_result(request)
            .await
            .map_err(|e| BackendError::Connection(format!("gRPC error: {}", e)))?;

        Ok(())
    }

    async fn set_expiration(&mut self, task_id: Uuid, ttl: Duration) -> Result<()> {
        let request = tonic::Request::new(SetExpirationRequest {
            task_id: task_id.to_string(),
            ttl_seconds: ttl.as_secs(),
        });

        self.client
            .set_expiration(request)
            .await
            .map_err(|e| BackendError::Connection(format!("gRPC error: {}", e)))?;

        Ok(())
    }

    async fn chord_init(&mut self, state: ChordState) -> Result<()> {
        let request = tonic::Request::new(ChordInitRequest {
            state: Some(self.to_proto_chord(&state)),
        });

        self.client
            .chord_init(request)
            .await
            .map_err(|e| BackendError::Connection(format!("gRPC error: {}", e)))?;

        Ok(())
    }

    async fn chord_complete_task(&mut self, chord_id: Uuid) -> Result<usize> {
        let request = tonic::Request::new(ChordCompleteTaskRequest {
            chord_id: chord_id.to_string(),
        });

        let response = self
            .client
            .chord_complete_task(request)
            .await
            .map_err(|e| BackendError::Connection(format!("gRPC error: {}", e)))?;

        Ok(response.into_inner().completed_count as usize)
    }

    async fn chord_get_state(&mut self, chord_id: Uuid) -> Result<Option<ChordState>> {
        let request = tonic::Request::new(ChordGetStateRequest {
            chord_id: chord_id.to_string(),
        });

        let response = self
            .client
            .chord_get_state(request)
            .await
            .map_err(|e| BackendError::Connection(format!("gRPC error: {}", e)))?;

        match response.into_inner().state {
            Some(proto_state) => Ok(Some(Self::from_proto_chord(proto_state)?)),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use uuid::Uuid;

    fn create_test_meta() -> TaskMeta {
        TaskMeta {
            task_id: Uuid::new_v4(),
            task_name: "test_task".to_string(),
            result: TaskResult::Pending,
            created_at: Utc::now(),
            started_at: None,
            completed_at: None,
            worker: None,
            progress: None,
        }
    }

    fn create_test_chord_state() -> ChordState {
        ChordState {
            chord_id: Uuid::new_v4(),
            total: 5,
            completed: 0,
            callback: Some("callback_task".to_string()),
            task_ids: vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()],
        }
    }

    // Helper to create a dummy backend for testing conversion methods
    // Note: This doesn't actually connect to a server
    fn create_dummy_backend() -> GrpcResultBackend {
        // Create an endpoint but don't connect
        let channel =
            tonic::transport::Endpoint::from_static("http://localhost:50051").connect_lazy();
        GrpcResultBackend::from_channel(channel)
    }

    #[tokio::test]
    async fn test_proto_meta_pending_conversion() {
        let backend = create_dummy_backend();

        let meta = create_test_meta();
        let task_id = meta.task_id;

        let proto_meta = backend.to_proto_meta(&meta);
        assert_eq!(proto_meta.task_id, task_id.to_string());
        assert_eq!(proto_meta.task_name, "test_task");
        assert_eq!(proto_meta.result_state, TaskResultState::Pending as i32);

        let converted = GrpcResultBackend::from_proto_meta(proto_meta).unwrap();
        assert_eq!(converted.task_id, task_id);
        assert_eq!(converted.task_name, "test_task");
        assert!(matches!(converted.result, TaskResult::Pending));
    }

    #[tokio::test]
    async fn test_proto_meta_success_conversion() {
        let backend = create_dummy_backend();

        let mut meta = create_test_meta();
        meta.result = TaskResult::Success(serde_json::json!({"result": 42}));

        let proto_meta = backend.to_proto_meta(&meta);
        assert_eq!(proto_meta.result_state, TaskResultState::Success as i32);
        assert!(proto_meta.result_data.is_some());

        let converted = GrpcResultBackend::from_proto_meta(proto_meta).unwrap();
        match converted.result {
            TaskResult::Success(data) => {
                assert_eq!(data["result"], 42);
            }
            _ => panic!("Expected Success result"),
        }
    }

    #[tokio::test]
    async fn test_proto_meta_failure_conversion() {
        let backend = create_dummy_backend();

        let mut meta = create_test_meta();
        meta.result = TaskResult::Failure("task failed".to_string());

        let proto_meta = backend.to_proto_meta(&meta);
        assert_eq!(proto_meta.result_state, TaskResultState::Failure as i32);
        assert_eq!(proto_meta.error_message, Some("task failed".to_string()));

        let converted = GrpcResultBackend::from_proto_meta(proto_meta).unwrap();
        match converted.result {
            TaskResult::Failure(msg) => assert_eq!(msg, "task failed"),
            _ => panic!("Expected Failure result"),
        }
    }

    #[tokio::test]
    async fn test_proto_meta_retry_conversion() {
        let backend = create_dummy_backend();

        let mut meta = create_test_meta();
        meta.result = TaskResult::Retry(3);

        let proto_meta = backend.to_proto_meta(&meta);
        assert_eq!(proto_meta.result_state, TaskResultState::Retry as i32);
        assert_eq!(proto_meta.retry_count, Some(3));

        let converted = GrpcResultBackend::from_proto_meta(proto_meta).unwrap();
        match converted.result {
            TaskResult::Retry(count) => assert_eq!(count, 3),
            _ => panic!("Expected Retry result"),
        }
    }

    #[tokio::test]
    async fn test_proto_chord_conversion() {
        let backend = create_dummy_backend();

        let chord_state = create_test_chord_state();
        let chord_id = chord_state.chord_id;

        let proto_chord = backend.to_proto_chord(&chord_state);
        assert_eq!(proto_chord.chord_id, chord_id.to_string());
        assert_eq!(proto_chord.total, 5);
        assert_eq!(proto_chord.completed, 0);
        assert_eq!(proto_chord.callback, Some("callback_task".to_string()));
        assert_eq!(proto_chord.task_ids.len(), 3);

        let converted = GrpcResultBackend::from_proto_chord(proto_chord).unwrap();
        assert_eq!(converted.chord_id, chord_id);
        assert_eq!(converted.total, 5);
        assert_eq!(converted.completed, 0);
        assert_eq!(converted.callback, Some("callback_task".to_string()));
        assert_eq!(converted.task_ids.len(), 3);
    }

    #[tokio::test]
    #[ignore] // Requires gRPC server running
    async fn test_grpc_backend_connection() {
        let backend = GrpcResultBackend::connect("http://localhost:50051").await;
        // Will fail if server not running, but tests compilation
        let _ = backend;
    }
}
