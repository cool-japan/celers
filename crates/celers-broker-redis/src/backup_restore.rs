//! Queue backup and restore utilities
//!
//! Provides mechanisms to backup and restore queue state:
//! - Export queue contents to JSON
//! - Import queue contents from JSON
//! - Snapshot creation with metadata
//! - Queue migration between instances

use celers_core::{CelersError, Result, SerializedTask};
use chrono::{DateTime, Utc};
use redis::{AsyncCommands, Client};
use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::fs;
use tracing::{debug, info};

use crate::QueueMode;

/// Queue snapshot containing all tasks and metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueSnapshot {
    /// Timestamp when snapshot was created
    pub created_at: DateTime<Utc>,
    /// Queue name
    pub queue_name: String,
    /// Queue mode (FIFO or Priority)
    pub mode: QueueMode,
    /// Main queue tasks
    pub main_queue: Vec<SerializedTask>,
    /// Processing queue tasks
    pub processing_queue: Vec<SerializedTask>,
    /// DLQ tasks
    pub dlq: Vec<SerializedTask>,
    /// Delayed queue tasks with execution times
    pub delayed_queue: Vec<(SerializedTask, i64)>,
    /// Total task count
    pub total_tasks: usize,
}

impl QueueSnapshot {
    /// Get a summary of the snapshot
    pub fn summary(&self) -> String {
        format!(
            "Snapshot of '{}' ({}): {} tasks (main: {}, processing: {}, dlq: {}, delayed: {})",
            self.queue_name,
            self.mode,
            self.total_tasks,
            self.main_queue.len(),
            self.processing_queue.len(),
            self.dlq.len(),
            self.delayed_queue.len()
        )
    }
}

/// Queue backup and restore manager
pub struct BackupManager {
    client: Client,
    queue_name: String,
    processing_queue: String,
    dlq_name: String,
    delayed_queue: String,
    mode: QueueMode,
}

impl BackupManager {
    /// Create a new backup manager
    pub fn new(
        client: Client,
        queue_name: String,
        processing_queue: String,
        dlq_name: String,
        delayed_queue: String,
        mode: QueueMode,
    ) -> Self {
        Self {
            client,
            queue_name,
            processing_queue,
            dlq_name,
            delayed_queue,
            mode,
        }
    }

    /// Create a snapshot of the current queue state
    pub async fn create_snapshot(&self) -> Result<QueueSnapshot> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        // Get main queue tasks
        let main_items: Vec<String> = if self.mode == QueueMode::Priority {
            conn.zrange(&self.queue_name, 0, -1)
                .await
                .map_err(|e| CelersError::Broker(format!("Failed to get main queue: {}", e)))?
        } else {
            conn.lrange(&self.queue_name, 0, -1)
                .await
                .map_err(|e| CelersError::Broker(format!("Failed to get main queue: {}", e)))?
        };

        let main_queue: Vec<SerializedTask> = main_items
            .iter()
            .filter_map(|s| serde_json::from_str(s).ok())
            .collect();

        // Get processing queue tasks
        let processing_items: Vec<String> = conn
            .lrange(&self.processing_queue, 0, -1)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get processing queue: {}", e)))?;

        let processing_queue: Vec<SerializedTask> = processing_items
            .iter()
            .filter_map(|s| serde_json::from_str(s).ok())
            .collect();

        // Get DLQ tasks
        let dlq_items: Vec<String> = conn
            .lrange(&self.dlq_name, 0, -1)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get DLQ: {}", e)))?;

        let dlq: Vec<SerializedTask> = dlq_items
            .iter()
            .filter_map(|s| serde_json::from_str(s).ok())
            .collect();

        // Get delayed queue tasks
        let delayed_items: Vec<(String, f64)> = conn
            .zrange_withscores(&self.delayed_queue, 0, -1)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get delayed queue: {}", e)))?;

        let delayed_queue: Vec<(SerializedTask, i64)> = delayed_items
            .iter()
            .filter_map(|(s, score)| {
                serde_json::from_str(s)
                    .ok()
                    .map(|task| (task, *score as i64))
            })
            .collect();

        let total_tasks =
            main_queue.len() + processing_queue.len() + dlq.len() + delayed_queue.len();

        let snapshot = QueueSnapshot {
            created_at: Utc::now(),
            queue_name: self.queue_name.clone(),
            mode: self.mode,
            main_queue,
            processing_queue,
            dlq,
            delayed_queue,
            total_tasks,
        };

        info!("Created snapshot: {}", snapshot.summary());
        Ok(snapshot)
    }

    /// Export snapshot to JSON file
    pub async fn export_to_file(&self, path: &Path) -> Result<QueueSnapshot> {
        let snapshot = self.create_snapshot().await?;

        let json = serde_json::to_string_pretty(&snapshot)
            .map_err(|e| CelersError::Serialization(e.to_string()))?;

        fs::write(path, json)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to write file: {}", e)))?;

        info!("Exported snapshot to {:?}", path);
        Ok(snapshot)
    }

    /// Import snapshot from JSON file
    pub async fn import_from_file(&self, path: &Path) -> Result<QueueSnapshot> {
        let json = fs::read_to_string(path)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to read file: {}", e)))?;

        let snapshot: QueueSnapshot =
            serde_json::from_str(&json).map_err(|e| CelersError::Deserialization(e.to_string()))?;

        info!("Imported snapshot from {:?}", path);
        Ok(snapshot)
    }

    /// Restore queue state from snapshot
    ///
    /// # Arguments
    /// * `snapshot` - The snapshot to restore from
    /// * `clear_existing` - If true, clears existing queue contents before restoring
    pub async fn restore_from_snapshot(
        &self,
        snapshot: &QueueSnapshot,
        clear_existing: bool,
    ) -> Result<usize> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        // Clear existing queues if requested
        if clear_existing {
            let _: () = conn
                .del(&[
                    &self.queue_name,
                    &self.processing_queue,
                    &self.dlq_name,
                    &self.delayed_queue,
                ])
                .await
                .map_err(|e| CelersError::Broker(format!("Failed to clear queues: {}", e)))?;

            debug!("Cleared existing queue contents");
        }

        let mut restored_count = 0;
        let mut pipe = redis::pipe();

        // Restore main queue
        for task in &snapshot.main_queue {
            if let Ok(serialized) = serde_json::to_string(task) {
                match self.mode {
                    QueueMode::Fifo => {
                        pipe.rpush(&self.queue_name, &serialized);
                    }
                    QueueMode::Priority => {
                        let score = -(task.metadata.priority as f64);
                        pipe.zadd(&self.queue_name, &serialized, score);
                    }
                }
                restored_count += 1;
            }
        }

        // Restore processing queue
        for task in &snapshot.processing_queue {
            if let Ok(serialized) = serde_json::to_string(task) {
                pipe.lpush(&self.processing_queue, &serialized);
                restored_count += 1;
            }
        }

        // Restore DLQ
        for task in &snapshot.dlq {
            if let Ok(serialized) = serde_json::to_string(task) {
                pipe.lpush(&self.dlq_name, &serialized);
                restored_count += 1;
            }
        }

        // Restore delayed queue
        for (task, execute_at) in &snapshot.delayed_queue {
            if let Ok(serialized) = serde_json::to_string(task) {
                pipe.zadd(&self.delayed_queue, &serialized, *execute_at as f64);
                restored_count += 1;
            }
        }

        // Execute all restore operations
        pipe.query_async::<redis::Value>(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to restore queue: {}", e)))?;

        info!(
            "Restored {} tasks from snapshot (created at {})",
            restored_count, snapshot.created_at
        );

        Ok(restored_count)
    }

    /// Migrate queue to another Redis instance
    ///
    /// # Arguments
    /// * `target_url` - Redis URL of the target instance
    /// * `target_queue_name` - Queue name on the target instance (optional, uses same name if None)
    pub async fn migrate_to(
        &self,
        target_url: &str,
        target_queue_name: Option<&str>,
    ) -> Result<usize> {
        // Create snapshot from current instance
        let snapshot = self.create_snapshot().await?;

        // Connect to target instance
        let target_client = Client::open(target_url)
            .map_err(|e| CelersError::Broker(format!("Failed to connect to target: {}", e)))?;

        let target_name = target_queue_name.unwrap_or(&self.queue_name);
        let target_manager = BackupManager::new(
            target_client,
            target_name.to_string(),
            format!("{}:processing", target_name),
            format!("{}:dlq", target_name),
            format!("{}:delayed", target_name),
            self.mode,
        );

        // Restore to target instance
        let count = target_manager
            .restore_from_snapshot(&snapshot, false)
            .await?;

        info!(
            "Migrated {} tasks to {} (queue: {})",
            count, target_url, target_name
        );

        Ok(count)
    }

    /// Compare current queue state with a snapshot
    pub async fn compare_with_snapshot(
        &self,
        snapshot: &QueueSnapshot,
    ) -> Result<SnapshotComparison> {
        let current = self.create_snapshot().await?;

        let comparison = SnapshotComparison {
            main_queue_diff: (current.main_queue.len() as i64) - (snapshot.main_queue.len() as i64),
            processing_queue_diff: (current.processing_queue.len() as i64)
                - (snapshot.processing_queue.len() as i64),
            dlq_diff: (current.dlq.len() as i64) - (snapshot.dlq.len() as i64),
            delayed_queue_diff: (current.delayed_queue.len() as i64)
                - (snapshot.delayed_queue.len() as i64),
            total_diff: (current.total_tasks as i64) - (snapshot.total_tasks as i64),
        };

        Ok(comparison)
    }
}

/// Snapshot comparison result
#[derive(Debug, Clone)]
pub struct SnapshotComparison {
    pub main_queue_diff: i64,
    pub processing_queue_diff: i64,
    pub dlq_diff: i64,
    pub delayed_queue_diff: i64,
    pub total_diff: i64,
}

impl SnapshotComparison {
    /// Check if queues are identical
    pub fn is_identical(&self) -> bool {
        self.total_diff == 0
    }

    /// Get a summary of the differences
    pub fn summary(&self) -> String {
        format!(
            "Total: {:+}, Main: {:+}, Processing: {:+}, DLQ: {:+}, Delayed: {:+}",
            self.total_diff,
            self.main_queue_diff,
            self.processing_queue_diff,
            self.dlq_diff,
            self.delayed_queue_diff
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backup_manager_creation() {
        let client = Client::open("redis://localhost:6379").unwrap();
        let manager = BackupManager::new(
            client,
            "queue".to_string(),
            "queue:processing".to_string(),
            "queue:dlq".to_string(),
            "queue:delayed".to_string(),
            QueueMode::Fifo,
        );
        assert_eq!(manager.queue_name, "queue");
    }

    #[test]
    fn test_snapshot_comparison_identical() {
        let comp = SnapshotComparison {
            main_queue_diff: 0,
            processing_queue_diff: 0,
            dlq_diff: 0,
            delayed_queue_diff: 0,
            total_diff: 0,
        };
        assert!(comp.is_identical());
    }

    #[test]
    fn test_snapshot_comparison_different() {
        let comp = SnapshotComparison {
            main_queue_diff: 5,
            processing_queue_diff: 0,
            dlq_diff: 0,
            delayed_queue_diff: 0,
            total_diff: 5,
        };
        assert!(!comp.is_identical());
    }
}
