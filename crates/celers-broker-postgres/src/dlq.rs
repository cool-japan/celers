//! Dead Letter Queue operations and health/maintenance methods

use celers_core::{CelersError, Result, TaskId};
use chrono::Utc;
use std::time::Duration;
use uuid::Uuid;

use crate::row_ext::{uuid_from_row, uuid_param, RowExt};
use crate::types::{DlqTaskInfo, HealthStatus, PoolMetrics};
use crate::PostgresBroker;

impl PostgresBroker {
    // ========== DLQ Operations ==========

    /// List tasks in the dead letter queue
    pub async fn list_dlq(&self, limit: i64, offset: i64) -> Result<Vec<DlqTaskInfo>> {
        let rows = self
            .conn
            .query(
                r#"
            SELECT id, task_id, task_name, retry_count, error_message, failed_at
            FROM celers_dead_letter_queue
            WHERE queue_name = $1
            ORDER BY failed_at DESC
            LIMIT $2 OFFSET $3
            "#,
                &[&self.queue_name, &limit, &offset],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to list DLQ: {}", e)))?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in &rows {
            tasks.push(DlqTaskInfo {
                id: uuid_from_row(row, "id")
                    .map_err(|e| CelersError::Other(format!("Failed to read id: {}", e)))?,
                task_id: uuid_from_row(row, "task_id")
                    .map_err(|e| CelersError::Other(format!("Failed to read task_id: {}", e)))?,
                task_name: row
                    .col("task_name")
                    .map_err(|e| CelersError::Other(format!("Failed to read task_name: {}", e)))?,
                retry_count: row.col("retry_count").map_err(|e| {
                    CelersError::Other(format!("Failed to read retry_count: {}", e))
                })?,
                error_message: row.col("error_message").map_err(|e| {
                    CelersError::Other(format!("Failed to read error_message: {}", e))
                })?,
                failed_at: row
                    .col("failed_at")
                    .map_err(|e| CelersError::Other(format!("Failed to read failed_at: {}", e)))?,
            });
        }
        Ok(tasks)
    }

    /// Requeue a task from the dead letter queue
    ///
    /// This moves the task back to the main queue with reset retry count.
    pub async fn requeue_from_dlq(&self, dlq_id: &Uuid) -> Result<TaskId> {
        // Two-step on a pooled broker: check out a connection, then open the
        // transaction on it (the handle borrows `conn`, so the slot stays
        // reserved for the transaction's whole lifetime).
        let conn = self.connection().await?;
        let mut tx = conn
            .transaction()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        // Get task from DLQ
        let dlq_id_param = uuid_param(dlq_id);
        let rows = tx
            .query(
                r#"
            SELECT task_id, task_name, payload, metadata::text AS metadata
            FROM celers_dead_letter_queue
            WHERE id = $1::text::uuid AND queue_name = $2
            "#,
                &[&dlq_id_param, &self.queue_name],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to fetch DLQ task: {}", e)))?;

        let row = rows
            .into_iter()
            .next()
            .ok_or_else(|| CelersError::Other("DLQ task not found".to_string()))?;

        let task_id: Uuid = uuid_from_row(&row, "task_id")
            .map_err(|e| CelersError::Other(format!("Failed to read task_id: {}", e)))?;
        let task_name: String = row
            .col("task_name")
            .map_err(|e| CelersError::Other(format!("Failed to read task_name: {}", e)))?;
        let payload: Vec<u8> = row
            .col("payload")
            .map_err(|e| CelersError::Other(format!("Failed to read payload: {}", e)))?;
        // `metadata` is a nullable JSON column: read as `Option<String>`
        // directly (not `row_ext.rs`'s `json_from_row`, which collapses
        // SQL-NULL and JSON-null to the same `serde_json::Value::Null` —
        // here a genuinely absent metadata column must forward as SQL
        // `NULL` on the INSERT below, matching the original
        // `Option<serde_json::Value>` round-trip exactly). Mirrors
        // `results.rs`'s `task_result_json_column` rationale.
        let metadata_param: Option<String> = row
            .col("metadata")
            .map_err(|e| CelersError::Other(format!("Failed to read metadata: {}", e)))?;

        // Create new task in main queue
        let new_task_id = Uuid::new_v4();
        let new_task_id_param = uuid_param(&new_task_id);
        // `queue_name` is mandatory: `dequeue()` is queue-scoped, so a
        // requeued task written without it would land in the `default` queue
        // and never be delivered to this broker.
        tx.execute(
            r#"
            INSERT INTO celers_tasks
                (id, task_name, payload, state, priority, retry_count, max_retries, metadata, queue_name, created_at, scheduled_at)
            VALUES ($1::text::uuid, $2, $3, 'pending', 0, 0, 3, $4::text::jsonb, $5, NOW(), NOW())
            "#,
            &[
                &new_task_id_param,
                &task_name,
                &payload,
                &metadata_param,
                &self.queue_name,
            ],
        )
        .await
        .map_err(|e| CelersError::Other(format!("Failed to requeue task: {}", e)))?;

        // Delete from DLQ
        tx.execute(
            "DELETE FROM celers_dead_letter_queue WHERE id = $1::text::uuid",
            &[&dlq_id_param],
        )
        .await
        .map_err(|e| CelersError::Other(format!("Failed to delete from DLQ: {}", e)))?;

        tx.commit()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to commit requeue: {}", e)))?;

        tracing::info!(original_task_id = %task_id, new_task_id = %new_task_id, task_name = %task_name, "Requeued task from DLQ");

        Ok(new_task_id)
    }

    /// Purge (delete) a task from the dead letter queue
    pub async fn purge_dlq(&self, dlq_id: &Uuid) -> Result<bool> {
        let dlq_id_param = uuid_param(dlq_id);
        let affected = self
            .conn
            .execute(
                "DELETE FROM celers_dead_letter_queue WHERE id = $1::text::uuid AND queue_name = $2",
                &[&dlq_id_param, &self.queue_name],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to purge DLQ task: {}", e)))?;

        Ok(affected > 0)
    }

    /// Purge all tasks from the dead letter queue
    pub async fn purge_all_dlq(&self) -> Result<u64> {
        let affected = self
            .conn
            .execute(
                "DELETE FROM celers_dead_letter_queue WHERE queue_name = $1",
                &[&self.queue_name],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to purge all DLQ: {}", e)))?;

        tracing::info!(count = affected, "Purged all DLQ tasks");
        Ok(affected)
    }

    // ========== Health & Maintenance ==========

    /// Check database health
    ///
    /// `connection_pool_size`/`idle_connections` are read from the broker's
    /// real connection pool — see [`PostgresBroker::get_pool_metrics`].
    pub async fn check_health(&self) -> Result<HealthStatus> {
        // Test connection
        let rows = self
            .conn
            .query("SELECT version()", &[])
            .await
            .map_err(|e| CelersError::Other(format!("Health check failed: {}", e)))?;
        let row = rows.into_iter().next().ok_or_else(|| {
            CelersError::Other("Health check failed: no rows returned".to_string())
        })?;
        let version: String = row
            .col_idx(0)
            .map_err(|e| CelersError::Other(format!("Health check failed: {}", e)))?;

        // Get queue counts
        let stats = self.get_statistics().await?;

        let pool_metrics = self.get_pool_metrics();
        Ok(HealthStatus {
            healthy: true,
            connection_pool_size: pool_metrics.max_size,
            idle_connections: pool_metrics.idle,
            pending_tasks: stats.pending,
            processing_tasks: stats.processing,
            dlq_tasks: stats.dlq,
            database_version: version,
        })
    }

    /// Get detailed connection pool metrics
    ///
    /// This provides comprehensive statistics about the connection pool state,
    /// useful for monitoring and capacity planning.
    ///
    /// # Measured, not fabricated
    ///
    /// Every field is read from the broker's real connection pool
    /// ([`crate::pool::PgPool`]):
    ///
    /// * `max_size` — configured slot count (`max_connections`)
    /// * `size` — slots that currently hold an established connection
    ///   (connections are opened lazily, so this grows towards `max_size`
    ///   under load)
    /// * `in_use` — slots currently checked out by an operation
    /// * `idle` — established slots not checked out
    /// * `waiting` — tasks blocked waiting for a free slot
    ///
    /// An earlier version returned hardcoded zeros for all of these because
    /// the broker held a single `PgConnection` with no introspection API. The
    /// consequence was worse than a gap: `monitor_pool_health()` divided by
    /// `max_size == 0`, fell through every branch, and reported "Pool health
    /// is optimal" unconditionally — a permanently green signal for anyone
    /// wiring it into alerting.
    pub fn get_pool_metrics(&self) -> PoolMetrics {
        let snapshot = self.conn.snapshot();

        PoolMetrics {
            max_size: snapshot.max_size,
            size: snapshot.established,
            idle: snapshot.idle,
            in_use: snapshot.in_use,
            waiting: snapshot.waiting,
        }
    }

    /// Archive completed tasks older than the specified duration
    ///
    /// Returns the number of tasks archived (deleted).
    pub async fn archive_completed_tasks(&self, older_than: Duration) -> Result<u64> {
        let cutoff = Utc::now() - chrono::Duration::seconds(older_than.as_secs() as i64);

        // `cutoff` is a `DateTime<Utc>` parameter -> `.to_rfc3339()` bound
        // through a `$1::text::timestamptz` cast, same convention as
        // `results.rs`'s `archive_results`.
        let cutoff_param = cutoff.to_rfc3339();
        let affected = self
            .conn
            .execute(
                r#"
            DELETE FROM celers_tasks
            WHERE state IN ('completed', 'failed', 'cancelled')
              AND completed_at < $1::text::timestamptz
            "#,
                &[&cutoff_param],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to archive tasks: {}", e)))?;

        tracing::info!(count = affected, cutoff = %cutoff, "Archived completed tasks");
        Ok(affected)
    }

    /// Clean up stuck processing tasks (tasks that have been processing too long)
    ///
    /// This can happen if a worker crashes. Tasks are requeued with incremented retry count.
    pub async fn recover_stuck_tasks(&self, stuck_threshold: Duration) -> Result<u64> {
        let cutoff = Utc::now() - chrono::Duration::seconds(stuck_threshold.as_secs() as i64);

        // Same `DateTime<Utc>` -> `$1::text::timestamptz` convention as
        // `archive_completed_tasks` above.
        let cutoff_param = cutoff.to_rfc3339();
        let affected = self
            .conn
            .execute(
                r#"
            UPDATE celers_tasks
            SET state = 'pending',
                started_at = NULL,
                worker_id = NULL,
                error_message = 'Recovered from stuck processing state'
            WHERE state = 'processing'
              AND started_at < $1::text::timestamptz
            "#,
                &[&cutoff_param],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to recover stuck tasks: {}", e)))?;

        if affected > 0 {
            tracing::warn!(count = affected, "Recovered stuck processing tasks");
        }
        Ok(affected)
    }

    /// Purge all tasks (dangerous - use with caution)
    pub async fn purge_all(&self) -> Result<u64> {
        let affected = self
            .conn
            .execute("DELETE FROM celers_tasks", &[])
            .await
            .map_err(|e| CelersError::Other(format!("Failed to purge all tasks: {}", e)))?;

        tracing::warn!(count = affected, "Purged all tasks");
        Ok(affected)
    }
}
