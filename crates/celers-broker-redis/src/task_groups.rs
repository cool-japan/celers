//! Task groups for coordinating and tracking related tasks
//!
//! Provides:
//! - Group task execution and tracking
//! - Barrier synchronization (wait for all tasks)
//! - Group result aggregation
//! - Group cancellation
//! - Group timeout and expiration
//!
//! # Example
//!
//! ```rust,no_run
//! use celers_broker_redis::task_groups::{TaskGroup, GroupConfig};
//! use uuid::Uuid;
//!
//! # async fn example() -> celers_core::Result<()> {
//! let config = GroupConfig::new().with_timeout(60);
//! let group = TaskGroup::new("redis://localhost:6379", "process-batch", config)?;
//!
//! // Add tasks to group
//! let task1 = Uuid::new_v4();
//! let task2 = Uuid::new_v4();
//! group.add_task(&task1).await?;
//! group.add_task(&task2).await?;
//!
//! // Wait for all tasks to complete
//! let results = group.wait_all(30).await?;
//! println!("Completed: {}/{}", results.len(), group.size().await?);
//!
//! # Ok(())
//! # }
//! ```

use celers_core::{CelersError, Result, TaskId};
use redis::{AsyncCommands, Client};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, info, warn};

/// Task group status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupStatus {
    /// Group is active and accepting tasks
    Active,
    /// Group is completed (all tasks done)
    Completed,
    /// Group is cancelled
    Cancelled,
    /// Group is expired
    Expired,
}

impl std::fmt::Display for GroupStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GroupStatus::Active => write!(f, "ACTIVE"),
            GroupStatus::Completed => write!(f, "COMPLETED"),
            GroupStatus::Cancelled => write!(f, "CANCELLED"),
            GroupStatus::Expired => write!(f, "EXPIRED"),
        }
    }
}

/// Task group metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupMetadata {
    /// Group ID
    pub group_id: String,
    /// Group status
    pub status: GroupStatus,
    /// Creation timestamp
    pub created_at: u64,
    /// Completion timestamp (if completed)
    pub completed_at: Option<u64>,
    /// Total tasks in group
    pub total_tasks: usize,
    /// Completed tasks count
    pub completed_tasks: usize,
    /// Failed tasks count
    pub failed_tasks: usize,
}

impl GroupMetadata {
    /// Create new group metadata
    pub fn new(group_id: String) -> Self {
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after UNIX epoch")
            .as_secs();

        Self {
            group_id,
            status: GroupStatus::Active,
            created_at,
            completed_at: None,
            total_tasks: 0,
            completed_tasks: 0,
            failed_tasks: 0,
        }
    }

    /// Check if group is complete
    pub fn is_complete(&self) -> bool {
        self.status == GroupStatus::Completed
    }

    /// Get success rate (0.0 to 1.0)
    pub fn success_rate(&self) -> f64 {
        if self.total_tasks == 0 {
            0.0
        } else {
            self.completed_tasks as f64 / self.total_tasks as f64
        }
    }

    /// Get duration in seconds (if completed)
    pub fn duration_secs(&self) -> Option<u64> {
        self.completed_at
            .map(|completed| completed.saturating_sub(self.created_at))
    }
}

/// Group configuration
#[derive(Debug, Clone)]
pub struct GroupConfig {
    /// Group timeout in seconds (None = no timeout)
    pub timeout_secs: Option<u64>,
    /// Key prefix for group storage
    pub key_prefix: String,
    /// Auto-cleanup after completion (seconds)
    pub cleanup_after_secs: Option<u64>,
}

impl Default for GroupConfig {
    fn default() -> Self {
        Self {
            timeout_secs: Some(3600), // 1 hour
            key_prefix: "celery-group:".to_string(),
            cleanup_after_secs: Some(86400), // 24 hours
        }
    }
}

impl GroupConfig {
    /// Create a new group configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set timeout
    pub fn with_timeout(mut self, timeout_secs: u64) -> Self {
        self.timeout_secs = Some(timeout_secs);
        self
    }

    /// Disable timeout
    pub fn without_timeout(mut self) -> Self {
        self.timeout_secs = None;
        self
    }

    /// Set key prefix
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.key_prefix = prefix.into();
        self
    }

    /// Set auto-cleanup delay
    pub fn with_cleanup_after(mut self, secs: u64) -> Self {
        self.cleanup_after_secs = Some(secs);
        self
    }

    /// Disable auto-cleanup
    pub fn without_cleanup(mut self) -> Self {
        self.cleanup_after_secs = None;
        self
    }
}

/// Task group for coordinating related tasks
pub struct TaskGroup {
    client: Client,
    group_id: String,
    config: GroupConfig,
}

impl TaskGroup {
    /// Create a new task group
    pub fn new(redis_url: &str, group_id: &str, config: GroupConfig) -> Result<Self> {
        let client = Client::open(redis_url)
            .map_err(|e| CelersError::Broker(format!("Failed to connect to Redis: {}", e)))?;

        Ok(Self {
            client,
            group_id: group_id.to_string(),
            config,
        })
    }

    /// Initialize the group
    pub async fn init(&self) -> Result<()> {
        let metadata = GroupMetadata::new(self.group_id.clone());
        self.store_metadata(&metadata).await?;

        debug!("Initialized task group '{}'", self.group_id);
        Ok(())
    }

    /// Add a task to the group
    pub async fn add_task(&self, task_id: &TaskId) -> Result<()> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let tasks_key = self.tasks_key();

        // Add task ID to group's task set
        conn.sadd::<_, _, ()>(&tasks_key, task_id.to_string())
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to add task to group: {}", e)))?;

        // Update metadata
        let mut metadata = self
            .get_metadata()
            .await?
            .unwrap_or_else(|| GroupMetadata::new(self.group_id.clone()));
        metadata.total_tasks += 1;
        self.store_metadata(&metadata).await?;

        debug!("Added task {} to group '{}'", task_id, self.group_id);
        Ok(())
    }

    /// Remove a task from the group
    pub async fn remove_task(&self, task_id: &TaskId) -> Result<bool> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let tasks_key = self.tasks_key();

        let removed: i32 = conn
            .srem(&tasks_key, task_id.to_string())
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to remove task from group: {}", e)))?;

        if removed > 0 {
            // Update metadata
            if let Some(mut metadata) = self.get_metadata().await? {
                metadata.total_tasks = metadata.total_tasks.saturating_sub(1);
                self.store_metadata(&metadata).await?;
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Mark a task as completed
    pub async fn mark_completed(&self, task_id: &TaskId, success: bool) -> Result<()> {
        let mut metadata = self
            .get_metadata()
            .await?
            .ok_or_else(|| CelersError::Broker(format!("Group '{}' not found", self.group_id)))?;

        if success {
            metadata.completed_tasks += 1;
        } else {
            metadata.failed_tasks += 1;
        }

        // Check if all tasks are done
        let total_done = metadata.completed_tasks + metadata.failed_tasks;
        if total_done >= metadata.total_tasks && metadata.total_tasks > 0 {
            metadata.status = GroupStatus::Completed;
            metadata.completed_at = Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("system time should be after UNIX epoch")
                    .as_secs(),
            );
            info!("Task group '{}' completed", self.group_id);
        }

        self.store_metadata(&metadata).await?;

        debug!(
            "Marked task {} as {} in group '{}'",
            task_id,
            if success { "completed" } else { "failed" },
            self.group_id
        );

        Ok(())
    }

    /// Get all task IDs in the group
    pub async fn get_tasks(&self) -> Result<Vec<TaskId>> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let tasks_key = self.tasks_key();

        let task_strings: Vec<String> = conn
            .smembers(&tasks_key)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get group tasks: {}", e)))?;

        let task_ids: Result<Vec<TaskId>> = task_strings
            .into_iter()
            .map(|s| {
                s.parse()
                    .map_err(|e| CelersError::Deserialization(format!("Invalid task ID: {}", e)))
            })
            .collect();

        task_ids
    }

    /// Get group size (number of tasks)
    pub async fn size(&self) -> Result<usize> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let tasks_key = self.tasks_key();

        let size: usize = conn
            .scard(&tasks_key)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get group size: {}", e)))?;

        Ok(size)
    }

    /// Wait for all tasks to complete
    pub async fn wait_all(&self, timeout_secs: u64) -> Result<Vec<TaskId>> {
        let start = SystemTime::now();
        let timeout = Duration::from_secs(timeout_secs);

        loop {
            let metadata = self.get_metadata().await?.ok_or_else(|| {
                CelersError::Broker(format!("Group '{}' not found", self.group_id))
            })?;

            if metadata.is_complete() {
                return self.get_tasks().await;
            }

            if start
                .elapsed()
                .expect("system time should not go backwards")
                >= timeout
            {
                warn!("Timeout waiting for group '{}' completion", self.group_id);
                return Err(CelersError::Other(
                    "Timeout waiting for group completion".to_string(),
                ));
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Cancel all tasks in the group
    pub async fn cancel(&self) -> Result<()> {
        let mut metadata = self
            .get_metadata()
            .await?
            .ok_or_else(|| CelersError::Broker(format!("Group '{}' not found", self.group_id)))?;

        metadata.status = GroupStatus::Cancelled;
        self.store_metadata(&metadata).await?;

        info!("Cancelled task group '{}'", self.group_id);
        Ok(())
    }

    /// Check if group is cancelled
    pub async fn is_cancelled(&self) -> Result<bool> {
        if let Some(metadata) = self.get_metadata().await? {
            Ok(metadata.status == GroupStatus::Cancelled)
        } else {
            Ok(false)
        }
    }

    /// Get group metadata
    pub async fn get_metadata(&self) -> Result<Option<GroupMetadata>> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let meta_key = self.metadata_key();

        let data: Option<Vec<u8>> = conn
            .get(&meta_key)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get group metadata: {}", e)))?;

        if let Some(data) = data {
            let metadata: GroupMetadata = serde_json::from_slice(&data)
                .map_err(|e| CelersError::Deserialization(e.to_string()))?;
            Ok(Some(metadata))
        } else {
            Ok(None)
        }
    }

    /// Store group metadata
    async fn store_metadata(&self, metadata: &GroupMetadata) -> Result<()> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let meta_key = self.metadata_key();
        let data =
            serde_json::to_vec(metadata).map_err(|e| CelersError::Serialization(e.to_string()))?;

        if let Some(ttl) = self.config.cleanup_after_secs {
            conn.set_ex::<_, _, ()>(&meta_key, data, ttl)
                .await
                .map_err(|e| CelersError::Broker(format!("Failed to store metadata: {}", e)))?;
        } else {
            conn.set::<_, _, ()>(&meta_key, data)
                .await
                .map_err(|e| CelersError::Broker(format!("Failed to store metadata: {}", e)))?;
        }

        Ok(())
    }

    /// Delete the group
    pub async fn delete(&self) -> Result<()> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let keys = vec![self.metadata_key(), self.tasks_key()];

        conn.del::<_, ()>(keys)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to delete group: {}", e)))?;

        info!("Deleted task group '{}'", self.group_id);
        Ok(())
    }

    /// Generate metadata key
    fn metadata_key(&self) -> String {
        format!("{}{}:meta", self.config.key_prefix, self.group_id)
    }

    /// Generate tasks set key
    fn tasks_key(&self) -> String {
        format!("{}{}:tasks", self.config.key_prefix, self.group_id)
    }

    /// Get group ID
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Get configuration
    pub fn config(&self) -> &GroupConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_group_status_display() {
        assert_eq!(GroupStatus::Active.to_string(), "ACTIVE");
        assert_eq!(GroupStatus::Completed.to_string(), "COMPLETED");
        assert_eq!(GroupStatus::Cancelled.to_string(), "CANCELLED");
        assert_eq!(GroupStatus::Expired.to_string(), "EXPIRED");
    }

    #[test]
    fn test_group_metadata_creation() {
        let metadata = GroupMetadata::new("test-group".to_string());
        assert_eq!(metadata.group_id, "test-group");
        assert_eq!(metadata.status, GroupStatus::Active);
        assert_eq!(metadata.total_tasks, 0);
        assert_eq!(metadata.completed_tasks, 0);
        assert_eq!(metadata.failed_tasks, 0);
        assert!(!metadata.is_complete());
    }

    #[test]
    fn test_group_metadata_success_rate() {
        let mut metadata = GroupMetadata::new("test-group".to_string());
        metadata.total_tasks = 10;
        metadata.completed_tasks = 7;
        metadata.failed_tasks = 3;

        assert_eq!(metadata.success_rate(), 0.7);
    }

    #[test]
    fn test_group_metadata_success_rate_zero_tasks() {
        let metadata = GroupMetadata::new("test-group".to_string());
        assert_eq!(metadata.success_rate(), 0.0);
    }

    #[test]
    fn test_group_config_default() {
        let config = GroupConfig::default();
        assert_eq!(config.timeout_secs, Some(3600));
        assert_eq!(config.key_prefix, "celery-group:");
        assert_eq!(config.cleanup_after_secs, Some(86400));
    }

    #[test]
    fn test_group_config_builder() {
        let config = GroupConfig::new()
            .with_timeout(7200)
            .with_prefix("custom:")
            .with_cleanup_after(3600);

        assert_eq!(config.timeout_secs, Some(7200));
        assert_eq!(config.key_prefix, "custom:");
        assert_eq!(config.cleanup_after_secs, Some(3600));
    }

    #[test]
    fn test_group_config_without_timeout() {
        let config = GroupConfig::new().without_timeout();
        assert_eq!(config.timeout_secs, None);
    }

    #[test]
    fn test_group_config_without_cleanup() {
        let config = GroupConfig::new().without_cleanup();
        assert_eq!(config.cleanup_after_secs, None);
    }

    #[test]
    fn test_task_group_creation() {
        let config = GroupConfig::new();
        let group = TaskGroup::new("redis://localhost:6379", "test-group", config);
        assert!(group.is_ok());

        let group = group.unwrap();
        assert_eq!(group.group_id(), "test-group");
    }

    #[test]
    fn test_task_group_key_generation() {
        let config = GroupConfig::new();
        let group = TaskGroup::new("redis://localhost:6379", "my-group", config).unwrap();

        let meta_key = group.metadata_key();
        let tasks_key = group.tasks_key();

        assert_eq!(meta_key, "celery-group:my-group:meta");
        assert_eq!(tasks_key, "celery-group:my-group:tasks");
    }

    #[test]
    fn test_task_group_key_generation_custom_prefix() {
        let config = GroupConfig::new().with_prefix("app:");
        let group = TaskGroup::new("redis://localhost:6379", "my-group", config).unwrap();

        let meta_key = group.metadata_key();
        let tasks_key = group.tasks_key();

        assert_eq!(meta_key, "app:my-group:meta");
        assert_eq!(tasks_key, "app:my-group:tasks");
    }
}
