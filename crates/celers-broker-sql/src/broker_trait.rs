//! Broker trait implementation for MysqlBroker
//!
//! Implements the core `Broker` trait from celers-core.

use crate::broker_core::MysqlBroker;
use crate::row_ext::RowExt;
use async_trait::async_trait;
use celers_core::{Broker, BrokerMessage, CelersError, Result, SerializedTask, TaskId};
use oxisql_core::Connection;
use serde_json::json;
use std::sync::atomic::Ordering;
use uuid::Uuid;

#[cfg(feature = "metrics")]
use celers_metrics::{TASKS_ENQUEUED_BY_TYPE, TASKS_ENQUEUED_TOTAL};

#[async_trait]
impl Broker for MysqlBroker {
    async fn enqueue(&self, task: SerializedTask) -> Result<TaskId> {
        let task_id = task.metadata.id;
        let mut db_metadata = json!({
            "queue": self.queue_name,
            "enqueued_at": chrono::Utc::now().to_rfc3339(),
        });

        // Merge task metadata if present
        if let Ok(task_meta) = serde_json::to_value(&task.metadata) {
            if let Some(obj) = db_metadata.as_object_mut() {
                if let Some(meta_obj) = task_meta.as_object() {
                    for (k, v) in meta_obj {
                        obj.insert(k.clone(), v.clone());
                    }
                }
            }
        }
        let db_metadata_str =
            serde_json::to_string(&db_metadata).unwrap_or_else(|_| "{}".to_string());

        self.connection()
            .execute(
                r#"
                INSERT INTO celers_tasks
                    (id, task_name, payload, state, priority, max_retries, metadata, created_at, scheduled_at)
                VALUES (?, ?, ?, 'pending', ?, ?, ?, NOW(), NOW())
                "#,
                &[
                    &task_id.to_string(),
                    &task.metadata.name,
                    &task.payload,
                    &task.metadata.priority,
                    &(task.metadata.max_retries as i32),
                    &db_metadata_str,
                ],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to enqueue task: {}", e)))?;

        self.enqueue_count.fetch_add(1, Ordering::Relaxed);

        #[cfg(feature = "metrics")]
        {
            TASKS_ENQUEUED_TOTAL.inc();
            TASKS_ENQUEUED_BY_TYPE
                .with_label_values(&[&task.metadata.name])
                .inc();
        }

        Ok(task_id)
    }

    async fn dequeue(&self) -> Result<Option<BrokerMessage>> {
        // Check if queue is paused
        if self.paused.load(Ordering::SeqCst) {
            return Ok(None);
        }

        // Use FOR UPDATE SKIP LOCKED to atomically claim a task
        // This is the magic that makes distributed workers work without contention
        let mut tx = self
            .connection()
            .transaction()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        let rows = tx
            .query(
                r#"
                SELECT id, task_name, payload, retry_count
                FROM celers_tasks
                WHERE state = 'pending'
                  AND scheduled_at <= NOW()
                ORDER BY priority DESC, created_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
                "#,
                &[],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to dequeue task: {}", e)))?;

        if let Some(row) = rows.into_iter().next() {
            let task_id_str: String = row
                .col("id")
                .map_err(|e| CelersError::Other(format!("Failed to dequeue task: {e}")))?;
            let _task_id = Uuid::parse_str(&task_id_str)
                .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?;
            let task_name: String = row
                .col("task_name")
                .map_err(|e| CelersError::Other(format!("Failed to dequeue task: {e}")))?;
            let payload: Vec<u8> = row
                .col("payload")
                .map_err(|e| CelersError::Other(format!("Failed to dequeue task: {e}")))?;
            let retry_count: i32 = row
                .col("retry_count")
                .map_err(|e| CelersError::Other(format!("Failed to dequeue task: {e}")))?;

            // Mark as processing
            tx.execute(
                r#"
                UPDATE celers_tasks
                SET state = 'processing',
                    started_at = NOW(),
                    retry_count = retry_count + 1
                WHERE id = ?
                "#,
                &[&task_id_str],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to mark task as processing: {}", e)))?;

            tx.commit()
                .await
                .map_err(|e| CelersError::Other(format!("Failed to commit transaction: {}", e)))?;

            Ok(Some(BrokerMessage {
                task: SerializedTask::new(task_name, payload),
                receipt_handle: Some(retry_count.to_string()),
            }))
        } else {
            tx.rollback().await.map_err(|e| {
                CelersError::Other(format!("Failed to rollback transaction: {}", e))
            })?;
            Ok(None)
        }
    }

    async fn ack(&self, task_id: &TaskId, _receipt_handle: Option<&str>) -> Result<()> {
        self.connection()
            .execute(
                r#"
                UPDATE celers_tasks
                SET state = 'completed',
                    completed_at = NOW()
                WHERE id = ?
                "#,
                &[&task_id.to_string()],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to ack task: {}", e)))?;

        // Optionally delete completed tasks after a retention period
        // For now, we keep them for auditing

        Ok(())
    }

    async fn reject(
        &self,
        task_id: &TaskId,
        _receipt_handle: Option<&str>,
        requeue: bool,
    ) -> Result<()> {
        if requeue {
            // Check if task has exceeded max retries
            let rows = self
                .connection()
                .query(
                    r#"
                    SELECT retry_count, max_retries
                    FROM celers_tasks
                    WHERE id = ?
                    "#,
                    &[&task_id.to_string()],
                )
                .await
                .map_err(|e| CelersError::Other(format!("Failed to fetch task: {}", e)))?;
            let row = rows
                .into_iter()
                .next()
                .ok_or_else(|| CelersError::Other(format!("Task {task_id} not found")))?;

            let retry_count: i32 = row
                .col("retry_count")
                .map_err(|e| CelersError::Other(format!("Failed to fetch task: {e}")))?;
            let max_retries: i32 = row
                .col("max_retries")
                .map_err(|e| CelersError::Other(format!("Failed to fetch task: {e}")))?;

            if retry_count >= max_retries {
                // Move to DLQ
                self.move_to_dlq(task_id).await?;
            } else {
                // Requeue with exponential backoff
                let backoff_seconds = 2_i64.pow(retry_count as u32).min(3600); // Max 1 hour

                self.connection()
                    .execute(
                        r#"
                        UPDATE celers_tasks
                        SET state = 'pending',
                            scheduled_at = DATE_ADD(NOW(), INTERVAL ? SECOND),
                            started_at = NULL,
                            worker_id = NULL
                        WHERE id = ?
                        "#,
                        &[&backoff_seconds, &task_id.to_string()],
                    )
                    .await
                    .map_err(|e| CelersError::Other(format!("Failed to requeue task: {}", e)))?;
            }
        } else {
            // Mark as failed permanently
            self.connection()
                .execute(
                    r#"
                    UPDATE celers_tasks
                    SET state = 'failed',
                        completed_at = NOW()
                    WHERE id = ?
                    "#,
                    &[&task_id.to_string()],
                )
                .await
                .map_err(|e| CelersError::Other(format!("Failed to mark task as failed: {}", e)))?;
        }

        Ok(())
    }

    async fn queue_size(&self) -> Result<usize> {
        let rows = self
            .connection()
            .query(
                r#"
                SELECT COUNT(*) as count
                FROM celers_tasks
                WHERE state = 'pending'
                "#,
                &[],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get queue size: {}", e)))?;
        let row = rows
            .into_iter()
            .next()
            .ok_or_else(|| CelersError::Other("queue_size: query returned no rows".into()))?;

        let count: i64 = row
            .col("count")
            .map_err(|e| CelersError::Other(format!("Failed to get queue size: {e}")))?;
        Ok(count as usize)
    }

    async fn cancel(&self, task_id: &TaskId) -> Result<bool> {
        let affected = self
            .connection()
            .execute(
                r#"
                UPDATE celers_tasks
                SET state = 'cancelled',
                    completed_at = NOW()
                WHERE id = ? AND state IN ('pending', 'processing')
                "#,
                &[&task_id.to_string()],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to cancel task: {}", e)))?;

        Ok(affected > 0)
    }

    /// Schedule a task for execution at a specific Unix timestamp (seconds)
    async fn enqueue_at(&self, task: SerializedTask, execute_at: i64) -> Result<TaskId> {
        let task_id = task.metadata.id;
        let mut db_metadata = json!({
            "queue": self.queue_name,
            "enqueued_at": chrono::Utc::now().to_rfc3339(),
            "scheduled_for": execute_at,
        });

        // Merge task metadata if present
        if let Ok(task_meta) = serde_json::to_value(&task.metadata) {
            if let Some(obj) = db_metadata.as_object_mut() {
                if let Some(meta_obj) = task_meta.as_object() {
                    for (k, v) in meta_obj {
                        obj.insert(k.clone(), v.clone());
                    }
                }
            }
        }
        let db_metadata_str =
            serde_json::to_string(&db_metadata).unwrap_or_else(|_| "{}".to_string());

        // Convert Unix timestamp to MySQL DATETIME text form — see
        // `row_ext.rs`'s "DateTime<Utc> parameter convention (MySQL)"
        // section (`%Y-%m-%d %H:%M:%S`, no `T`/timezone suffix).
        let scheduled_at = chrono::DateTime::from_timestamp(execute_at, 0)
            .ok_or_else(|| CelersError::Other("Invalid timestamp".to_string()))?
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();

        self.connection()
            .execute(
                r#"
                INSERT INTO celers_tasks
                    (id, task_name, payload, state, priority, max_retries, metadata, created_at, scheduled_at)
                VALUES (?, ?, ?, 'pending', ?, ?, ?, NOW(), ?)
                "#,
                &[
                    &task_id.to_string(),
                    &task.metadata.name,
                    &task.payload,
                    &task.metadata.priority,
                    &(task.metadata.max_retries as i32),
                    &db_metadata_str,
                    &scheduled_at,
                ],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to enqueue delayed task: {}", e)))?;

        #[cfg(feature = "metrics")]
        {
            TASKS_ENQUEUED_TOTAL.inc();
            TASKS_ENQUEUED_BY_TYPE
                .with_label_values(&[&task.metadata.name])
                .inc();
        }

        Ok(task_id)
    }

    /// Schedule a task for execution after a delay (seconds)
    async fn enqueue_after(&self, task: SerializedTask, delay_secs: u64) -> Result<TaskId> {
        let task_id = task.metadata.id;
        let mut db_metadata = json!({
            "queue": self.queue_name,
            "enqueued_at": chrono::Utc::now().to_rfc3339(),
            "delay_seconds": delay_secs,
        });

        // Merge task metadata if present
        if let Ok(task_meta) = serde_json::to_value(&task.metadata) {
            if let Some(obj) = db_metadata.as_object_mut() {
                if let Some(meta_obj) = task_meta.as_object() {
                    for (k, v) in meta_obj {
                        obj.insert(k.clone(), v.clone());
                    }
                }
            }
        }
        let db_metadata_str =
            serde_json::to_string(&db_metadata).unwrap_or_else(|_| "{}".to_string());

        self.connection()
            .execute(
                r#"
                INSERT INTO celers_tasks
                    (id, task_name, payload, state, priority, max_retries, metadata, created_at, scheduled_at)
                VALUES (?, ?, ?, 'pending', ?, ?, ?, NOW(), DATE_ADD(NOW(), INTERVAL ? SECOND))
                "#,
                &[
                    &task_id.to_string(),
                    &task.metadata.name,
                    &task.payload,
                    &task.metadata.priority,
                    &(task.metadata.max_retries as i32),
                    &db_metadata_str,
                    &(delay_secs as i64),
                ],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to enqueue delayed task: {}", e)))?;

        #[cfg(feature = "metrics")]
        {
            TASKS_ENQUEUED_TOTAL.inc();
            TASKS_ENQUEUED_BY_TYPE
                .with_label_values(&[&task.metadata.name])
                .inc();
        }

        Ok(task_id)
    }

    // ========== Batch Operations (optimized overrides) ==========

    /// Optimized batch enqueue using a single transaction
    async fn enqueue_batch(&self, tasks: Vec<SerializedTask>) -> Result<Vec<TaskId>> {
        self.enqueue_batch_impl(tasks).await
    }

    /// Optimized batch dequeue using a single transaction with FOR UPDATE SKIP LOCKED
    async fn dequeue_batch(&self, count: usize) -> Result<Vec<BrokerMessage>> {
        self.dequeue_batch_impl(count).await
    }

    /// Optimized batch ack using a single query with IN clause
    async fn ack_batch(&self, tasks: &[(TaskId, Option<String>)]) -> Result<()> {
        if tasks.is_empty() {
            return Ok(());
        }

        let task_ids: Vec<String> = tasks.iter().map(|(id, _)| id.to_string()).collect();

        let placeholders = task_ids.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let query_str = format!(
            r#"
            UPDATE celers_tasks
            SET state = 'completed',
                completed_at = NOW()
            WHERE id IN ({})
            "#,
            placeholders
        );

        let param_refs: Vec<&dyn oxisql_core::ToSqlValue> = task_ids
            .iter()
            .map(|s| s as &dyn oxisql_core::ToSqlValue)
            .collect();

        self.connection()
            .execute(&query_str, &param_refs)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to batch ack tasks: {}", e)))?;

        Ok(())
    }
}
