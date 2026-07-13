//! Task chain and batch reject operations
//!
//! Task chain enqueuing for sequential task execution and
//! batch reject operations.

use crate::broker_core::MysqlBroker;
use crate::row_ext::RowExt;
use crate::types::TaskChain;
use celers_core::{Broker, CelersError, Result, TaskId};
use oxisql_core::Connection;

impl MysqlBroker {
    // ========== Task Chain Support ==========

    /// Enqueue a task chain where tasks execute in sequence
    ///
    /// Each task in the chain will be scheduled to execute after the previous task
    /// completes (with optional delay between tasks).
    ///
    /// # Arguments
    /// * `chain` - Task chain to enqueue
    ///
    /// # Returns
    /// Vector of task IDs in the same order as the chain
    ///
    /// # Example
    /// ```rust,ignore
    /// let chain = TaskChain::new()
    ///     .then(task1)
    ///     .then(task2)
    ///     .then(task3)
    ///     .with_delay(5); // 5 seconds between tasks
    ///
    /// let task_ids = broker.enqueue_chain(chain).await?;
    /// ```
    pub async fn enqueue_chain(&self, chain: TaskChain) -> Result<Vec<TaskId>> {
        if chain.tasks().is_empty() {
            return Ok(Vec::new());
        }

        let mut task_ids = Vec::with_capacity(chain.tasks().len());
        let base_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| CelersError::Other(format!("Failed to get system time: {}", e)))?
            .as_secs() as i64;

        for (idx, task) in chain.tasks().iter().enumerate() {
            let execute_at = if idx == 0 {
                // First task executes immediately
                base_time
            } else {
                // Subsequent tasks execute after delay
                let delay = chain.delay_between_secs().unwrap_or(0) * idx as u64;
                base_time + delay as i64
            };

            let task_id = self.enqueue_at(task.clone(), execute_at).await?;
            task_ids.push(task_id);
        }

        tracing::info!(
            chain_length = chain.tasks().len(),
            delay_secs = chain.delay_between_secs().unwrap_or(0),
            "Enqueued task chain"
        );

        Ok(task_ids)
    }

    /// Batch reject operation - reject multiple tasks at once
    ///
    /// This is more efficient than calling reject() for each task individually.
    ///
    /// # Arguments
    /// * `tasks` - Vector of (TaskId, receipt_handle, requeue) tuples
    ///
    /// # Returns
    /// Number of tasks successfully rejected
    pub async fn reject_batch(&self, tasks: &[(TaskId, Option<String>, bool)]) -> Result<u64> {
        if tasks.is_empty() {
            return Ok(0);
        }

        let mut tx = self
            .connection()
            .transaction()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        let mut rejected_count = 0u64;

        for (task_id, _receipt_handle, requeue) in tasks {
            if *requeue {
                // Check if task has exceeded max retries
                let rows = tx
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

                if let Some(row) = rows.into_iter().next() {
                    let retry_count: i32 = row
                        .col("retry_count")
                        .map_err(|e| CelersError::Other(format!("Failed to fetch task: {e}")))?;
                    let max_retries: i32 = row
                        .col("max_retries")
                        .map_err(|e| CelersError::Other(format!("Failed to fetch task: {e}")))?;

                    if retry_count >= max_retries {
                        // Move to DLQ — plain `CALL move_to_dlq(?)` with a `?`
                        // parameter, translated like any other MySQL statement.
                        tx.execute("CALL move_to_dlq(?)", &[&task_id.to_string()])
                            .await
                            .map_err(|e| {
                                CelersError::Other(format!("Failed to move task to DLQ: {}", e))
                            })?;
                    } else {
                        // Requeue with exponential backoff
                        let backoff_seconds = 2_i64.pow(retry_count as u32).min(3600); // Max 1 hour

                        tx.execute(
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
                        .map_err(|e| {
                            CelersError::Other(format!("Failed to requeue task: {}", e))
                        })?;
                    }
                    rejected_count += 1;
                }
            } else {
                // Mark as failed permanently
                let affected = tx
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
                    .map_err(|e| {
                        CelersError::Other(format!("Failed to mark task as failed: {}", e))
                    })?;

                rejected_count += affected;
            }
        }

        tx.commit()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to commit batch reject: {}", e)))?;

        Ok(rejected_count)
    }
}
