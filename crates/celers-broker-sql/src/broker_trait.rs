//! Broker trait implementation for MysqlBroker
//!
//! Implements the core `Broker` trait from celers-core.

use crate::backoff::retry_backoff_seconds;
use crate::broker_core::MysqlBroker;
use crate::broker_dequeue::{claim_pending_rows, mark_rows_processing};
use crate::row_ext::RowExt;
use crate::task_row::resolve_row_id;
use async_trait::async_trait;
use celers_core::{Broker, BrokerMessage, CelersError, Result, SerializedTask, TaskId};
use oxisql_core::Connection;
use serde_json::json;
use std::sync::atomic::Ordering;

#[cfg(feature = "metrics")]
use celers_metrics::{TASKS_ENQUEUED_BY_TYPE, TASKS_ENQUEUED_TOTAL};

#[async_trait]
impl Broker for MysqlBroker {
    async fn enqueue(&self, task: SerializedTask) -> Result<TaskId> {
        let task_id = task.metadata.id;
        let db_metadata_str = self.build_task_metadata_document(&task, json!({}))?;

        self.connection()
            .execute(
                r#"
                INSERT INTO celers_tasks
                    (id, queue_name, task_name, payload, state, priority, max_retries, metadata, created_at, scheduled_at)
                VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, NOW(), NOW())
                "#,
                &[
                    &task_id.to_string(),
                    &self.queue_name,
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

    /// Claim the highest-priority ready task from this broker's logical queue.
    ///
    /// Uses `FOR UPDATE ... SKIP LOCKED` so any number of workers can claim
    /// concurrently without contending. The returned message carries the
    /// task's *persisted* metadata — in particular its real database id, so
    /// the subsequent `ack`/`reject` targets the right row.
    async fn dequeue(&self) -> Result<Option<BrokerMessage>> {
        // Check if queue is paused
        if self.paused.load(Ordering::SeqCst) {
            return Ok(None);
        }

        let mut tx = self
            .connection()
            .transaction()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        let claimed = claim_pending_rows(&mut *tx, &self.queue_name, None).await?;

        let Some((row_id, message)) = claimed.into_iter().next() else {
            tx.rollback().await.map_err(|e| {
                CelersError::Other(format!("Failed to rollback transaction: {}", e))
            })?;
            return Ok(None);
        };

        // `BeforeDequeue` runs while the row is still locked, so a hook that
        // rejects the task rolls the claim back and leaves it pending for
        // another worker rather than losing it.
        if let Err(hook_error) = self.fire_before_dequeue(&message.task).await {
            let _ = tx.rollback().await;
            return Err(hook_error);
        }

        mark_rows_processing(&mut *tx, &[row_id.to_string()], None).await?;

        tx.commit()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to commit transaction: {}", e)))?;

        self.fire_after_dequeue(&message.task).await?;

        Ok(Some(message))
    }

    async fn ack(&self, task_id: &TaskId, receipt_handle: Option<&str>) -> Result<()> {
        let row_id = resolve_row_id(task_id, receipt_handle);

        let hook_task = self.load_task_for_ack_hooks(&row_id).await?;
        if let Some(task) = hook_task.as_ref() {
            self.fire_before_ack(task).await?;
        }

        let affected = self
            .connection()
            .execute(
                r#"
                UPDATE celers_tasks
                SET state = 'completed',
                    completed_at = NOW()
                WHERE id = ?
                "#,
                &[&row_id],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to ack task: {}", e)))?;

        // A zero-row ack means the id never matched a row. That used to be
        // completely silent, which is how the "task acked but stayed
        // processing forever" failure mode hid.
        if affected == 0 {
            tracing::warn!(
                task_id = %task_id,
                row_id = %row_id,
                queue = %self.queue_name,
                "ack matched no task row; the task may have already been \
                 archived, moved to the DLQ, or acked twice"
            );
        }

        // Optionally delete completed tasks after a retention period
        // For now, we keep them for auditing

        if let Some(task) = hook_task.as_ref() {
            self.fire_after_ack(task).await?;
        }

        Ok(())
    }

    async fn reject(
        &self,
        task_id: &TaskId,
        receipt_handle: Option<&str>,
        requeue: bool,
    ) -> Result<()> {
        let row_id = resolve_row_id(task_id, receipt_handle);

        // Load before any state change: `move_to_dlq` deletes the row, so a
        // post-hoc load would find nothing to hand the hooks.
        let hook_task = self.load_task_for_reject_hooks(&row_id).await?;
        if let Some(task) = hook_task.as_ref() {
            self.fire_before_reject(task).await?;
        }

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
                    &[&row_id],
                )
                .await
                .map_err(|e| CelersError::Other(format!("Failed to fetch task: {}", e)))?;
            let row = rows
                .into_iter()
                .next()
                .ok_or_else(|| CelersError::TaskNotFound(task_id.to_string()))?;

            let retry_count: i32 = row
                .col("retry_count")
                .map_err(|e| CelersError::Other(format!("Failed to fetch task: {e}")))?;
            let max_retries: i32 = row
                .col("max_retries")
                .map_err(|e| CelersError::Other(format!("Failed to fetch task: {e}")))?;

            if retry_count >= max_retries {
                // Move to DLQ
                self.move_to_dlq_by_row_id(&row_id).await?;
            } else {
                // Requeue with exponential backoff. The delay is computed by
                // `retry_backoff_seconds`, which clamps the exponent before
                // shifting — the previous `2_i64.pow(retry_count as u32)`
                // panicked outright for a large or negative `retry_count`.
                let backoff_seconds = retry_backoff_seconds(retry_count);

                let affected = self
                    .connection()
                    .execute(
                        r#"
                        UPDATE celers_tasks
                        SET state = 'pending',
                            scheduled_at = DATE_ADD(NOW(), INTERVAL ? SECOND),
                            started_at = NULL,
                            worker_id = NULL
                        WHERE id = ?
                        "#,
                        &[&backoff_seconds, &row_id],
                    )
                    .await
                    .map_err(|e| CelersError::Other(format!("Failed to requeue task: {}", e)))?;

                if affected == 0 {
                    tracing::warn!(
                        task_id = %task_id,
                        row_id = %row_id,
                        "reject(requeue) matched no task row"
                    );
                }
            }
        } else {
            // Mark as failed permanently
            let affected = self
                .connection()
                .execute(
                    r#"
                    UPDATE celers_tasks
                    SET state = 'failed',
                        completed_at = NOW()
                    WHERE id = ?
                    "#,
                    &[&row_id],
                )
                .await
                .map_err(|e| CelersError::Other(format!("Failed to mark task as failed: {}", e)))?;

            if affected == 0 {
                tracing::warn!(
                    task_id = %task_id,
                    row_id = %row_id,
                    "reject matched no task row"
                );
            }
        }

        if let Some(task) = hook_task.as_ref() {
            self.fire_after_reject(task).await?;
        }

        Ok(())
    }

    /// Number of ready tasks in *this broker's* logical queue.
    async fn queue_size(&self) -> Result<usize> {
        let rows = self
            .connection()
            .query(
                r#"
                SELECT COUNT(*) as count
                FROM celers_tasks
                WHERE queue_name = ?
                  AND state = 'pending'
                "#,
                &[&self.queue_name],
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
        Ok(count.max(0) as usize)
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
        let db_metadata_str =
            self.build_task_metadata_document(&task, json!({ "scheduled_for": execute_at }))?;

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
                    (id, queue_name, task_name, payload, state, priority, max_retries, metadata, created_at, scheduled_at)
                VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, NOW(), ?)
                "#,
                &[
                    &task_id.to_string(),
                    &self.queue_name,
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
        let db_metadata_str =
            self.build_task_metadata_document(&task, json!({ "delay_seconds": delay_secs }))?;

        self.connection()
            .execute(
                r#"
                INSERT INTO celers_tasks
                    (id, queue_name, task_name, payload, state, priority, max_retries, metadata, created_at, scheduled_at)
                VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, NOW(), DATE_ADD(NOW(), INTERVAL ? SECOND))
                "#,
                &[
                    &task_id.to_string(),
                    &self.queue_name,
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

        let row_ids: Vec<String> = tasks
            .iter()
            .map(|(id, handle)| resolve_row_id(id, handle.as_deref()))
            .collect();

        let placeholders = row_ids.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let query_str = format!(
            r#"
            UPDATE celers_tasks
            SET state = 'completed',
                completed_at = NOW()
            WHERE id IN ({})
            "#,
            placeholders
        );

        let param_refs: Vec<&dyn oxisql_core::ToSqlValue> = row_ids
            .iter()
            .map(|s| s as &dyn oxisql_core::ToSqlValue)
            .collect();

        let affected = self
            .connection()
            .execute(&query_str, &param_refs)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to batch ack tasks: {}", e)))?;

        if affected < row_ids.len() as u64 {
            tracing::warn!(
                requested = row_ids.len(),
                affected,
                queue = %self.queue_name,
                "batch ack updated fewer rows than requested"
            );
        }

        Ok(())
    }
}
