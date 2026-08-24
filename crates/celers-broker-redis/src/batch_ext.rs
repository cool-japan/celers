//! Extended batch operations with conditional processing and filtering
//!
//! Provides advanced batch operations beyond the standard broker interface:
//! - Conditional dequeue with custom filters
//! - Batch operations with priority ranges
//! - Selective task processing

use celers_core::{BrokerMessage, CelersError, Result, SerializedTask, TaskId};
use redis::{aio::ConnectionManager, AsyncCommands, Client};
use std::sync::Arc;
use tokio::sync::OnceCell;
use tracing::{debug, warn};

use crate::{QueueKeys, QueueMode};

/// Task filter function type
pub type TaskFilter = Arc<dyn Fn(&SerializedTask) -> bool + Send + Sync>;

/// Extended batch operations for Redis broker
pub struct BatchOperations {
    client: Client,
    /// Long-lived multiplexed connection, shared by every call.
    conn: OnceCell<ConnectionManager>,
    keys: QueueKeys,
    queue_name: String,
    processing_queue: String,
    mode: QueueMode,
    visibility_timeout_secs: u64,
}

impl BatchOperations {
    /// Create new batch operations handler
    pub fn new(client: Client, queue_name: String, mode: QueueMode) -> Self {
        let processing_queue = format!("{}:processing", queue_name);
        Self {
            client,
            conn: OnceCell::new(),
            keys: QueueKeys::new(&queue_name),
            queue_name,
            processing_queue,
            mode,
            visibility_timeout_secs: 300,
        }
    }

    /// Reuse an existing connection manager instead of opening one lazily.
    pub fn with_connection_manager(self, manager: ConnectionManager) -> Self {
        let conn = OnceCell::new();
        // Only fails if the cell is already initialised, which it is not.
        let _ = conn.set(manager);
        Self { conn, ..self }
    }

    /// Set the visibility timeout recorded for messages this handler
    /// delivers (default: 300 seconds).
    pub fn with_visibility_timeout(mut self, timeout_secs: u64) -> Self {
        self.visibility_timeout_secs = timeout_secs;
        self
    }

    /// Stage a delivered message as in-flight.
    ///
    /// Writes both structures the broker's reaper reads: the processing list
    /// (the message itself) and the unacked set (its visibility deadline).
    /// Without the deadline these messages would sit in the processing list
    /// forever if the worker died.
    async fn stage_in_flight(&self, conn: &mut ConnectionManager, message: &str) -> Result<()> {
        let deadline = crate::now_secs() + self.visibility_timeout_secs;

        let mut pipe = redis::pipe();
        pipe.atomic();
        pipe.lpush(&self.processing_queue, message);
        pipe.zadd(&self.keys.unacked, message, deadline);

        pipe.query_async::<redis::Value>(conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to move to processing: {}", e)))?;

        Ok(())
    }

    /// Get the shared connection, establishing it on first use.
    async fn connection(&self) -> Result<ConnectionManager> {
        self.conn
            .get_or_try_init(|| async {
                self.client
                    .get_connection_manager()
                    .await
                    .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))
            })
            .await
            .cloned()
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

        let mut conn = self.connection().await?;

        let mut messages = Vec::new();
        let mut checked = 0;
        let max_checks = count * 3; // Check up to 3x count to find matching tasks

        match self.mode {
            QueueMode::Fifo => {
                while messages.len() < count && checked < max_checks {
                    // The tail holds the oldest message: producers push to
                    // the head, so `RPOP` is the FIFO end.
                    let data: Option<String> =
                        conn.rpop(&self.queue_name, None).await.map_err(|e| {
                            CelersError::Broker(format!("Failed to dequeue task: {}", e))
                        })?;

                    if let Some(serialized) = data {
                        checked += 1;
                        let task: SerializedTask = serde_json::from_str(&serialized)
                            .map_err(|e| CelersError::Deserialization(e.to_string()))?;

                        if filter(&task) {
                            // Stage as in-flight: the processing list holds
                            // the message, the unacked set its deadline, so
                            // the reaper can recover it if this worker dies.
                            self.stage_in_flight(&mut conn, &serialized).await?;

                            messages.push(BrokerMessage {
                                task,
                                receipt_handle: Some(serialized),
                            });
                        } else {
                            // Put it back at the *back* of the queue (the
                            // head), so this scan makes progress instead of
                            // popping the same message forever.
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
                        // Stage as in-flight (message + visibility deadline)
                        self.stage_in_flight(&mut conn, &data).await?;

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

        let mut conn = self.connection().await?;

        let mut pipe = redis::pipe();
        // MULTI/EXEC: a half-applied rejection batch would leave tasks
        // in-flight with no owner.
        pipe.atomic();
        let mut rejected_count = 0;

        for (task_id, receipt_handle, requeue) in rejections {
            if let Some(handle) = receipt_handle {
                // Clear both in-flight structures
                pipe.lrem(&self.processing_queue, 1, handle);
                pipe.zrem(&self.keys.unacked, handle);
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
                                    // Head-push: the retried task goes to the
                                    // *back* of the delivery order rather
                                    // than jumping ahead of waiting work.
                                    pipe.lpush(&self.queue_name, &serialized);
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
