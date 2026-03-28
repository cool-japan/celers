//! Task result storage operations

use celers_core::{CelersError, Result, TaskId};
use chrono::Utc;
use sqlx::Row;
use std::time::Duration;

use crate::types::{TaskResult, TaskResultStatus};
use crate::PostgresBroker;

impl PostgresBroker {
    // ========== Task Result Storage ==========

    /// Store a task result in the database
    ///
    /// This creates or updates the result for a given task ID.
    #[allow(clippy::too_many_arguments)]
    pub async fn store_result(
        &self,
        task_id: &TaskId,
        task_name: &str,
        status: TaskResultStatus,
        result: Option<serde_json::Value>,
        error: Option<&str>,
        traceback: Option<&str>,
        runtime_ms: Option<i64>,
    ) -> Result<()> {
        let completed_at = match status {
            TaskResultStatus::Success | TaskResultStatus::Failure | TaskResultStatus::Revoked => {
                Some(Utc::now())
            }
            _ => None,
        };

        sqlx::query(
            r#"
            INSERT INTO celers_task_results
                (task_id, task_name, status, result, error, traceback, created_at, completed_at, runtime_ms)
            VALUES ($1, $2, $3, $4, $5, $6, NOW(), $7, $8)
            ON CONFLICT (task_id) DO UPDATE SET
                status = EXCLUDED.status,
                result = EXCLUDED.result,
                error = EXCLUDED.error,
                traceback = EXCLUDED.traceback,
                completed_at = EXCLUDED.completed_at,
                runtime_ms = EXCLUDED.runtime_ms
            "#,
        )
        .bind(task_id)
        .bind(task_name)
        .bind(status.to_string())
        .bind(result)
        .bind(error)
        .bind(traceback)
        .bind(completed_at)
        .bind(runtime_ms)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to store result: {}", e)))?;

        Ok(())
    }

    /// Get a task result from the database
    pub async fn get_result(&self, task_id: &TaskId) -> Result<Option<TaskResult>> {
        let row = sqlx::query(
            r#"
            SELECT task_id, task_name, status, result, error, traceback,
                   created_at, completed_at, runtime_ms
            FROM celers_task_results
            WHERE task_id = $1
            "#,
        )
        .bind(task_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get result: {}", e)))?;

        match row {
            Some(row) => {
                let status_str: String = row.get("status");
                Ok(Some(TaskResult {
                    task_id: row.get("task_id"),
                    task_name: row.get("task_name"),
                    status: status_str.parse()?,
                    result: row.get("result"),
                    error: row.get("error"),
                    traceback: row.get("traceback"),
                    created_at: row.get("created_at"),
                    completed_at: row.get("completed_at"),
                    runtime_ms: row.get("runtime_ms"),
                }))
            }
            None => Ok(None),
        }
    }

    /// Delete a task result from the database
    pub async fn delete_result(&self, task_id: &TaskId) -> Result<bool> {
        let result = sqlx::query("DELETE FROM celers_task_results WHERE task_id = $1")
            .bind(task_id)
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to delete result: {}", e)))?;

        Ok(result.rows_affected() > 0)
    }

    /// Archive old task results
    ///
    /// Deletes results older than the specified duration.
    pub async fn archive_results(&self, older_than: Duration) -> Result<u64> {
        let cutoff = Utc::now() - chrono::Duration::seconds(older_than.as_secs() as i64);

        let result = sqlx::query(
            r#"
            DELETE FROM celers_task_results
            WHERE completed_at < $1
            "#,
        )
        .bind(cutoff)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to archive results: {}", e)))?;

        tracing::info!(count = result.rows_affected(), cutoff = %cutoff, "Archived old results");
        Ok(result.rows_affected())
    }

    /// Get multiple task results in a single query
    ///
    /// Efficiently retrieves results for multiple tasks at once.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    /// use uuid::Uuid;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let task_ids = vec![
    ///     Uuid::new_v4(),
    ///     Uuid::new_v4(),
    ///     Uuid::new_v4(),
    /// ];
    ///
    /// let results = broker.get_results_batch(&task_ids).await?;
    /// println!("Retrieved {} results", results.len());
    ///
    /// for result in results {
    ///     println!("Task {}: {:?}", result.task_id, result.status);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_results_batch(&self, task_ids: &[TaskId]) -> Result<Vec<TaskResult>> {
        if task_ids.is_empty() {
            return Ok(Vec::new());
        }

        let rows = sqlx::query(
            r#"
            SELECT task_id, task_name, status, result, error, traceback,
                   runtime_ms, created_at, completed_at
            FROM celers_task_results
            WHERE task_id = ANY($1)
            ORDER BY created_at DESC
            "#,
        )
        .bind(task_ids)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get results batch: {}", e)))?;

        let mut results = Vec::with_capacity(rows.len());
        for row in rows {
            results.push(TaskResult {
                task_id: row.get("task_id"),
                task_name: row.get("task_name"),
                status: row
                    .get::<String, _>("status")
                    .parse()
                    .unwrap_or(TaskResultStatus::Pending),
                result: row.get("result"),
                error: row.get("error"),
                traceback: row.get("traceback"),
                runtime_ms: row.get("runtime_ms"),
                created_at: row.get("created_at"),
                completed_at: row.get("completed_at"),
            });
        }

        tracing::debug!(count = results.len(), "Retrieved batch of task results");
        Ok(results)
    }

    /// Delete multiple task results in a single transaction
    ///
    /// Efficiently removes results for multiple tasks at once.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    /// use uuid::Uuid;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let task_ids = vec![
    ///     Uuid::new_v4(),
    ///     Uuid::new_v4(),
    ///     Uuid::new_v4(),
    /// ];
    ///
    /// let deleted = broker.delete_results_batch(&task_ids).await?;
    /// println!("Deleted {} results", deleted);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_results_batch(&self, task_ids: &[TaskId]) -> Result<u64> {
        if task_ids.is_empty() {
            return Ok(0);
        }

        let result = sqlx::query(
            r#"
            DELETE FROM celers_task_results
            WHERE task_id = ANY($1)
            "#,
        )
        .bind(task_ids)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to delete results batch: {}", e)))?;

        let deleted = result.rows_affected();
        tracing::info!(count = deleted, "Deleted batch of task results");
        Ok(deleted)
    }
}
