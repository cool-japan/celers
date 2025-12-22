//! Task result backend for storing and retrieving task execution results
//!
//! Provides:
//! - Task result storage in Redis with configurable TTL
//! - Result retrieval by task ID
//! - Result metadata (status, timestamp, duration)
//! - Automatic cleanup of expired results
//! - Compression for large results
//!
//! # Example
//!
//! ```rust,no_run
//! use celers_broker_redis::result_backend::{ResultBackend, TaskResult, TaskStatus};
//! use uuid::Uuid;
//!
//! # async fn example() -> celers_core::Result<()> {
//! let backend = ResultBackend::new("redis://localhost:6379", Some(3600))?;
//!
//! let task_id = Uuid::new_v4();
//!
//! // Store a successful result
//! backend.store_result(
//!     &task_id,
//!     TaskStatus::Success,
//!     Some(b"result data".to_vec()),
//!     None,
//! ).await?;
//!
//! // Retrieve the result
//! if let Some(result) = backend.get_result(&task_id).await? {
//!     println!("Task status: {:?}", result.status);
//!     if let Some(data) = result.result {
//!         println!("Result: {:?}", data);
//!     }
//! }
//!
//! # Ok(())
//! # }
//! ```

use celers_core::{CelersError, Result, TaskId};
use redis::{AsyncCommands, Client};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

/// Task execution status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    /// Task is pending execution
    Pending,
    /// Task is currently executing
    Started,
    /// Task completed successfully
    Success,
    /// Task failed with error
    Failure,
    /// Task was revoked/cancelled
    Revoked,
    /// Task retry scheduled
    Retry,
}

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskStatus::Pending => write!(f, "PENDING"),
            TaskStatus::Started => write!(f, "STARTED"),
            TaskStatus::Success => write!(f, "SUCCESS"),
            TaskStatus::Failure => write!(f, "FAILURE"),
            TaskStatus::Revoked => write!(f, "REVOKED"),
            TaskStatus::Retry => write!(f, "RETRY"),
        }
    }
}

/// Task result metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResult {
    /// Task ID
    pub task_id: TaskId,
    /// Task execution status
    pub status: TaskStatus,
    /// Result data (if any)
    pub result: Option<Vec<u8>>,
    /// Error message (if failed)
    pub error: Option<String>,
    /// Timestamp when result was stored (Unix timestamp)
    pub timestamp: u64,
    /// Execution duration in milliseconds (if completed)
    pub duration_ms: Option<u64>,
    /// Task metadata (custom key-value pairs)
    pub metadata: std::collections::HashMap<String, String>,
}

impl TaskResult {
    /// Create a new task result
    pub fn new(
        task_id: TaskId,
        status: TaskStatus,
        result: Option<Vec<u8>>,
        error: Option<String>,
    ) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            task_id,
            status,
            result,
            error,
            timestamp,
            duration_ms: None,
            metadata: std::collections::HashMap::new(),
        }
    }

    /// Set execution duration
    pub fn with_duration(mut self, duration_ms: u64) -> Self {
        self.duration_ms = Some(duration_ms);
        self
    }

    /// Add custom metadata
    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }

    /// Check if task succeeded
    pub fn is_success(&self) -> bool {
        self.status == TaskStatus::Success
    }

    /// Check if task failed
    pub fn is_failure(&self) -> bool {
        self.status == TaskStatus::Failure
    }

    /// Check if task is complete (success or failure)
    pub fn is_complete(&self) -> bool {
        matches!(
            self.status,
            TaskStatus::Success | TaskStatus::Failure | TaskStatus::Revoked
        )
    }
}

/// Result backend configuration
#[derive(Debug, Clone)]
pub struct ResultBackendConfig {
    /// Default TTL for results in seconds (None = no expiration)
    pub default_ttl: Option<u64>,
    /// Key prefix for result storage
    pub key_prefix: String,
    /// Enable compression for large results (> threshold bytes)
    pub compression_threshold: Option<usize>,
}

impl Default for ResultBackendConfig {
    fn default() -> Self {
        Self {
            default_ttl: Some(3600), // 1 hour
            key_prefix: "celery-task-meta-".to_string(),
            compression_threshold: Some(1024), // 1KB
        }
    }
}

impl ResultBackendConfig {
    /// Create a new configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set default TTL
    pub fn with_ttl(mut self, ttl_secs: u64) -> Self {
        self.default_ttl = Some(ttl_secs);
        self
    }

    /// Disable TTL (results never expire)
    pub fn without_ttl(mut self) -> Self {
        self.default_ttl = None;
        self
    }

    /// Set key prefix
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.key_prefix = prefix.into();
        self
    }

    /// Set compression threshold
    pub fn with_compression_threshold(mut self, threshold: usize) -> Self {
        self.compression_threshold = Some(threshold);
        self
    }

    /// Disable compression
    pub fn without_compression(mut self) -> Self {
        self.compression_threshold = None;
        self
    }
}

/// Redis-based result backend
pub struct ResultBackend {
    client: Client,
    config: ResultBackendConfig,
}

impl ResultBackend {
    /// Create a new result backend with default TTL (1 hour)
    pub fn new(redis_url: &str, default_ttl: Option<u64>) -> Result<Self> {
        let client = Client::open(redis_url)
            .map_err(|e| CelersError::Broker(format!("Failed to connect to Redis: {}", e)))?;

        let config = ResultBackendConfig {
            default_ttl,
            ..Default::default()
        };

        Ok(Self { client, config })
    }

    /// Create a new result backend with custom configuration
    pub fn with_config(redis_url: &str, config: ResultBackendConfig) -> Result<Self> {
        let client = Client::open(redis_url)
            .map_err(|e| CelersError::Broker(format!("Failed to connect to Redis: {}", e)))?;

        Ok(Self { client, config })
    }

    /// Store a task result
    pub async fn store_result(
        &self,
        task_id: &TaskId,
        status: TaskStatus,
        result: Option<Vec<u8>>,
        error: Option<String>,
    ) -> Result<()> {
        let task_result = TaskResult::new(*task_id, status, result, error);
        self.store_task_result(&task_result).await
    }

    /// Store a task result with full metadata
    pub async fn store_task_result(&self, task_result: &TaskResult) -> Result<()> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let key = self.result_key(&task_result.task_id);

        // Serialize result
        let mut data = serde_json::to_vec(task_result)
            .map_err(|e| CelersError::Serialization(e.to_string()))?;

        // Compress if needed
        if let Some(threshold) = self.config.compression_threshold {
            if data.len() > threshold {
                data = self.compress(&data)?;
                debug!(
                    "Compressed result for task {} ({} bytes)",
                    task_result.task_id,
                    data.len()
                );
            }
        }

        // Store with TTL
        if let Some(ttl) = self.config.default_ttl {
            conn.set_ex::<_, _, ()>(&key, data, ttl)
                .await
                .map_err(|e| CelersError::Broker(format!("Failed to store result: {}", e)))?;
        } else {
            conn.set::<_, _, ()>(&key, data)
                .await
                .map_err(|e| CelersError::Broker(format!("Failed to store result: {}", e)))?;
        }

        debug!(
            "Stored result for task {} with status {}",
            task_result.task_id, task_result.status
        );

        Ok(())
    }

    /// Get a task result by ID
    pub async fn get_result(&self, task_id: &TaskId) -> Result<Option<TaskResult>> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let key = self.result_key(task_id);

        let data: Option<Vec<u8>> = conn
            .get(&key)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get result: {}", e)))?;

        if let Some(mut data) = data {
            // Try to decompress
            if self.is_compressed(&data) {
                data = self.decompress(&data)?;
                debug!("Decompressed result for task {}", task_id);
            }

            let task_result: TaskResult = serde_json::from_slice(&data)
                .map_err(|e| CelersError::Deserialization(e.to_string()))?;

            Ok(Some(task_result))
        } else {
            Ok(None)
        }
    }

    /// Delete a task result
    pub async fn delete_result(&self, task_id: &TaskId) -> Result<bool> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let key = self.result_key(task_id);

        let deleted: i32 = conn
            .del(&key)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to delete result: {}", e)))?;

        Ok(deleted > 0)
    }

    /// Wait for task completion (polling with timeout)
    pub async fn wait_for_result(
        &self,
        task_id: &TaskId,
        timeout_secs: u64,
        poll_interval_ms: u64,
    ) -> Result<Option<TaskResult>> {
        let start = SystemTime::now();
        let timeout = std::time::Duration::from_secs(timeout_secs);

        loop {
            if let Some(result) = self.get_result(task_id).await? {
                if result.is_complete() {
                    return Ok(Some(result));
                }
            }

            if start.elapsed().unwrap() >= timeout {
                warn!("Timeout waiting for task {} result", task_id);
                return Ok(None);
            }

            tokio::time::sleep(std::time::Duration::from_millis(poll_interval_ms)).await;
        }
    }

    /// Update task status
    pub async fn update_status(&self, task_id: &TaskId, status: TaskStatus) -> Result<()> {
        if let Some(mut result) = self.get_result(task_id).await? {
            result.status = status;
            result.timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            self.store_task_result(&result).await?;
        } else {
            // Create new result with status
            self.store_result(task_id, status, None, None).await?;
        }
        Ok(())
    }

    /// Get multiple results by IDs
    pub async fn get_results(&self, task_ids: &[TaskId]) -> Result<Vec<Option<TaskResult>>> {
        let mut results = Vec::with_capacity(task_ids.len());
        for task_id in task_ids {
            results.push(self.get_result(task_id).await?);
        }
        Ok(results)
    }

    /// Count stored results
    pub async fn count_results(&self) -> Result<usize> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let pattern = format!("{}*", self.config.key_prefix);
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(&pattern)
            .query_async(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to count results: {}", e)))?;

        Ok(keys.len())
    }

    /// Clear all results
    pub async fn clear_all(&self) -> Result<usize> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let pattern = format!("{}*", self.config.key_prefix);
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(&pattern)
            .query_async(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get result keys: {}", e)))?;

        if keys.is_empty() {
            return Ok(0);
        }

        let count = keys.len();
        conn.del::<_, ()>(keys)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to clear results: {}", e)))?;

        info!("Cleared {} task results", count);
        Ok(count)
    }

    /// Generate result key
    fn result_key(&self, task_id: &TaskId) -> String {
        format!("{}{}", self.config.key_prefix, task_id)
    }

    /// Compress data using gzip
    fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder
            .write_all(data)
            .map_err(|e| CelersError::Other(format!("Compression failed: {}", e)))?;
        encoder
            .finish()
            .map_err(|e| CelersError::Other(format!("Compression failed: {}", e)))
    }

    /// Decompress data using gzip
    fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        use flate2::read::GzDecoder;
        use std::io::Read;

        let mut decoder = GzDecoder::new(data);
        let mut decompressed = Vec::new();
        decoder
            .read_to_end(&mut decompressed)
            .map_err(|e| CelersError::Other(format!("Decompression failed: {}", e)))?;
        Ok(decompressed)
    }

    /// Check if data is compressed (gzip magic bytes)
    fn is_compressed(&self, data: &[u8]) -> bool {
        data.len() >= 2 && data[0] == 0x1f && data[1] == 0x8b
    }

    /// Get configuration
    pub fn config(&self) -> &ResultBackendConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_status_display() {
        assert_eq!(TaskStatus::Pending.to_string(), "PENDING");
        assert_eq!(TaskStatus::Started.to_string(), "STARTED");
        assert_eq!(TaskStatus::Success.to_string(), "SUCCESS");
        assert_eq!(TaskStatus::Failure.to_string(), "FAILURE");
        assert_eq!(TaskStatus::Revoked.to_string(), "REVOKED");
        assert_eq!(TaskStatus::Retry.to_string(), "RETRY");
    }

    #[test]
    fn test_task_result_creation() {
        use uuid::Uuid;
        let task_id = Uuid::new_v4();
        let result = TaskResult::new(
            task_id,
            TaskStatus::Success,
            Some(b"test result".to_vec()),
            None,
        );

        assert_eq!(result.task_id, task_id);
        assert_eq!(result.status, TaskStatus::Success);
        assert!(result.result.is_some());
        assert!(result.error.is_none());
        assert!(result.is_success());
        assert!(!result.is_failure());
        assert!(result.is_complete());
    }

    #[test]
    fn test_task_result_with_duration() {
        use uuid::Uuid;
        let task_id = Uuid::new_v4();
        let result = TaskResult::new(task_id, TaskStatus::Success, None, None).with_duration(1500);

        assert_eq!(result.duration_ms, Some(1500));
    }

    #[test]
    fn test_task_result_with_metadata() {
        use uuid::Uuid;
        let task_id = Uuid::new_v4();
        let result = TaskResult::new(task_id, TaskStatus::Success, None, None)
            .with_metadata("worker".to_string(), "worker-1".to_string())
            .with_metadata("queue".to_string(), "default".to_string());

        assert_eq!(result.metadata.len(), 2);
        assert_eq!(result.metadata.get("worker"), Some(&"worker-1".to_string()));
        assert_eq!(result.metadata.get("queue"), Some(&"default".to_string()));
    }

    #[test]
    fn test_task_result_is_complete() {
        use uuid::Uuid;
        let task_id = Uuid::new_v4();

        let pending = TaskResult::new(task_id, TaskStatus::Pending, None, None);
        assert!(!pending.is_complete());

        let started = TaskResult::new(task_id, TaskStatus::Started, None, None);
        assert!(!started.is_complete());

        let success = TaskResult::new(task_id, TaskStatus::Success, None, None);
        assert!(success.is_complete());

        let failure = TaskResult::new(
            task_id,
            TaskStatus::Failure,
            None,
            Some("error".to_string()),
        );
        assert!(failure.is_complete());

        let revoked = TaskResult::new(task_id, TaskStatus::Revoked, None, None);
        assert!(revoked.is_complete());
    }

    #[test]
    fn test_result_backend_config_default() {
        let config = ResultBackendConfig::default();
        assert_eq!(config.default_ttl, Some(3600));
        assert_eq!(config.key_prefix, "celery-task-meta-");
        assert_eq!(config.compression_threshold, Some(1024));
    }

    #[test]
    fn test_result_backend_config_builder() {
        let config = ResultBackendConfig::new()
            .with_ttl(7200)
            .with_prefix("custom-")
            .with_compression_threshold(2048);

        assert_eq!(config.default_ttl, Some(7200));
        assert_eq!(config.key_prefix, "custom-");
        assert_eq!(config.compression_threshold, Some(2048));
    }

    #[test]
    fn test_result_backend_config_without_ttl() {
        let config = ResultBackendConfig::new().without_ttl();
        assert_eq!(config.default_ttl, None);
    }

    #[test]
    fn test_result_backend_config_without_compression() {
        let config = ResultBackendConfig::new().without_compression();
        assert_eq!(config.compression_threshold, None);
    }

    #[test]
    fn test_result_backend_creation() {
        let backend = ResultBackend::new("redis://localhost:6379", Some(3600));
        assert!(backend.is_ok());
    }

    #[test]
    fn test_result_backend_with_config() {
        let config = ResultBackendConfig::new().with_ttl(7200);
        let backend = ResultBackend::with_config("redis://localhost:6379", config);
        assert!(backend.is_ok());
    }

    #[test]
    fn test_result_key_generation() {
        use uuid::Uuid;
        let backend = ResultBackend::new("redis://localhost:6379", Some(3600)).unwrap();
        let task_id = Uuid::new_v4();
        let key = backend.result_key(&task_id);
        assert!(key.starts_with("celery-task-meta-"));
        assert!(key.contains(&task_id.to_string()));
    }
}
