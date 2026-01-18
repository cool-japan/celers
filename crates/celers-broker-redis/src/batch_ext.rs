//! Extended batch operations with conditional processing and filtering
//!
//! Provides advanced batch operations beyond the standard broker interface:
//! - Conditional dequeue with custom filters
//! - Batch operations with priority ranges
//! - Selective task processing

use celers_core::{BrokerMessage, CelersError, Result, SerializedTask, TaskId};
use redis::{AsyncCommands, Client};
use std::sync::Arc;
use tracing::{debug, warn};

use crate::QueueMode;

/// Task filter function type
pub type TaskFilter = Arc<dyn Fn(&SerializedTask) -> bool + Send + Sync>;

/// Extended batch operations for Redis broker
pub struct BatchOperations {
    client: Client,
    queue_name: String,
    processing_queue: String,
    mode: QueueMode,
}

impl BatchOperations {
    /// Create new batch operations handler
    pub fn new(client: Client, queue_name: String, mode: QueueMode) -> Self {
        let processing_queue = format!("{}:processing", queue_name);
        Self {
            client,
            queue_name,
            processing_queue,
            mode,
        }
    }

    /// Dequeue batch with custom filter
    ///
    /// Only dequeues tasks that pass the filter function.
    /// This allows selective processing based on task properties.
    ///
    /// # Arguments
    /// * `count` - Maximum number of tasks to dequeue
    /// * `filter` - Function to determine if a task should be dequeued
    ///
    /// # Example
    /// ```rust,no_run
    /// use celers_broker_redis::batch_ext::{BatchOperations, TaskFilter};
    /// use std::sync::Arc;
    ///
    /// # async fn example() -> celers_core::Result<()> {
    /// # let batch_ops: BatchOperations = todo!();
    /// // Only dequeue high-priority tasks
    /// let filter: TaskFilter = Arc::new(|task| task.metadata.priority >= 5);
    /// let messages = batch_ops.dequeue_batch_filtered(10, filter).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn dequeue_batch_filtered(
        &self,
        count: usize,
        filter: TaskFilter,
    ) -> Result<Vec<BrokerMessage>> {
        if count == 0 {
            return Ok(Vec::new());
        }

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let mut messages = Vec::new();
        let mut checked = 0;
        let max_checks = count * 3; // Check up to 3x count to find matching tasks

        match self.mode {
            QueueMode::Fifo => {
                while messages.len() < count && checked < max_checks {
                    let data: Option<String> =
                        conn.rpop(&self.queue_name, None).await.map_err(|e| {
                            CelersError::Broker(format!("Failed to dequeue task: {}", e))
                        })?;

                    if let Some(serialized) = data {
                        checked += 1;
                        let task: SerializedTask = serde_json::from_str(&serialized)
                            .map_err(|e| CelersError::Deserialization(e.to_string()))?;

                        if filter(&task) {
                            // Move to processing queue
                            conn.lpush::<_, _, ()>(&self.processing_queue, &serialized)
                                .await
                                .map_err(|e| {
                                    CelersError::Broker(format!(
                                        "Failed to move to processing: {}",
                                        e
                                    ))
                                })?;

                            messages.push(BrokerMessage {
                                task,
                                receipt_handle: Some(serialized),
                            });
                        } else {
                            // Put back at the front if it doesn't match
                            conn.lpush::<_, _, ()>(&self.queue_name, &serialized)
                                .await
                                .map_err(|e| {
                                    CelersError::Broker(format!("Failed to requeue task: {}", e))
                                })?;
                        }
                    } else {
                        break; // Queue is empty
                    }
                }
            }
            QueueMode::Priority => {
                // For priority queue, we need to peek and filter
                let items: Vec<(String, f64)> = conn
                    .zpopmin(&self.queue_name, count as isize * 2)
                    .await
                    .map_err(|e| CelersError::Broker(format!("Failed to dequeue batch: {}", e)))?;

                for (data, score) in items {
                    if messages.len() >= count {
                        // Put remaining back
                        conn.zadd::<_, _, _, ()>(&self.queue_name, &data, score)
                            .await
                            .map_err(|e| {
                                CelersError::Broker(format!("Failed to requeue task: {}", e))
                            })?;
                        continue;
                    }

                    let task: SerializedTask = serde_json::from_str(&data)
                        .map_err(|e| CelersError::Deserialization(e.to_string()))?;

                    if filter(&task) {
                        // Move to processing queue
                        conn.lpush::<_, _, ()>(&self.processing_queue, &data)
                            .await
                            .map_err(|e| {
                                CelersError::Broker(format!("Failed to move to processing: {}", e))
                            })?;

                        messages.push(BrokerMessage {
                            task,
                            receipt_handle: Some(data),
                        });
                    } else {
                        // Put back if it doesn't match
                        conn.zadd::<_, _, _, ()>(&self.queue_name, &data, score)
                            .await
                            .map_err(|e| {
                                CelersError::Broker(format!("Failed to requeue task: {}", e))
                            })?;
                    }
                }
            }
        }

        debug!(
            "Dequeued {} filtered tasks from {} candidates",
            messages.len(),
            checked
        );

        Ok(messages)
    }

    /// Dequeue tasks within a priority range (Priority mode only)
    ///
    /// # Arguments
    /// * `count` - Maximum number of tasks to dequeue
    /// * `min_priority` - Minimum priority (inclusive)
    /// * `max_priority` - Maximum priority (inclusive)
    pub async fn dequeue_batch_by_priority_range(
        &self,
        count: usize,
        min_priority: i32,
        max_priority: i32,
    ) -> Result<Vec<BrokerMessage>> {
        if self.mode != QueueMode::Priority {
            return Err(CelersError::Broker(
                "Priority range dequeue only works in Priority mode".to_string(),
            ));
        }

        let filter: TaskFilter = Arc::new(move |task| {
            task.metadata.priority >= min_priority && task.metadata.priority <= max_priority
        });

        self.dequeue_batch_filtered(count, filter).await
    }

    /// Dequeue tasks matching a specific task name pattern
    ///
    /// # Arguments
    /// * `count` - Maximum number of tasks to dequeue
    /// * `name_pattern` - Task name pattern to match (exact match)
    pub async fn dequeue_batch_by_name(
        &self,
        count: usize,
        name_pattern: &str,
    ) -> Result<Vec<BrokerMessage>> {
        let pattern = name_pattern.to_string();
        let filter: TaskFilter = Arc::new(move |task| task.metadata.name == pattern);

        self.dequeue_batch_filtered(count, filter).await
    }

    /// Reject multiple tasks at once with individual requeue decisions
    ///
    /// # Arguments
    /// * `rejections` - List of (TaskId, receipt_handle, should_requeue)
    pub async fn reject_batch(
        &self,
        rejections: &[(TaskId, Option<String>, bool)],
    ) -> Result<usize> {
        if rejections.is_empty() {
            return Ok(0);
        }

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let mut pipe = redis::pipe();
        let mut rejected_count = 0;

        for (task_id, receipt_handle, requeue) in rejections {
            if let Some(handle) = receipt_handle {
                // Remove from processing queue
                pipe.lrem(&self.processing_queue, 1, handle);
                rejected_count += 1;

                if *requeue {
                    // Parse task to re-enqueue with updated retry count
                    if let Ok(mut task) = serde_json::from_str::<SerializedTask>(handle) {
                        let retry_count = match task.metadata.state {
                            celers_core::TaskState::Retrying(count) => count + 1,
                            _ => 1,
                        };
                        task.metadata.state = celers_core::TaskState::Retrying(retry_count);

                        if let Ok(serialized) = serde_json::to_string(&task) {
                            match self.mode {
                                QueueMode::Fifo => {
                                    pipe.rpush(&self.queue_name, &serialized);
                                }
                                QueueMode::Priority => {
                                    let score = -(task.metadata.priority as f64);
                                    pipe.zadd(&self.queue_name, &serialized, score);
                                }
                            }
                        }
                    }
                }
            } else {
                warn!("No receipt handle for task {}, skipping reject", task_id);
            }
        }

        if rejected_count > 0 {
            pipe.query_async::<redis::Value>(&mut conn)
                .await
                .map_err(|e| CelersError::Broker(format!("Failed to reject batch: {}", e)))?;

            debug!("Rejected batch of {} tasks", rejected_count);
        }

        Ok(rejected_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_operations_creation() {
        let client = Client::open("redis://localhost:6379").unwrap();
        let batch_ops = BatchOperations::new(client, "test_queue".to_string(), QueueMode::Fifo);
        assert_eq!(batch_ops.queue_name, "test_queue");
        assert_eq!(batch_ops.processing_queue, "test_queue:processing");
    }

    #[test]
    fn test_task_filter() {
        let filter: TaskFilter = Arc::new(|task| task.metadata.priority >= 5);

        let mut metadata = celers_core::TaskMetadata::new("test".to_string());
        metadata.priority = 7;
        let task = SerializedTask {
            metadata,
            payload: vec![],
        };

        assert!(filter(&task));

        let mut metadata2 = celers_core::TaskMetadata::new("test".to_string());
        metadata2.priority = 3;
        let task2 = SerializedTask {
            metadata: metadata2,
            payload: vec![],
        };

        assert!(!filter(&task2));
    }
}
