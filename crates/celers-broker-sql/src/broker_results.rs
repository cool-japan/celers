//! Task result storage for `MysqlBroker`.
//!
//! Extracted from `broker_core.rs` (which exceeded the 2000-line limit) along
//! the file's own `// ========== Task Result Storage ==========` section
//! boundary. Reads and writes `celers_broker_results` (created by migration
//! `010_broker_results.sql`).

use crate::broker_core::MysqlBroker;
use crate::row_ext::RowExt;
use crate::types::*;
use celers_core::{CelersError, Result, TaskId};
use chrono::Utc;
use oxisql_core::Connection;
use std::time::Duration;
use uuid::Uuid;

impl MysqlBroker {
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
                Some(Utc::now().format("%Y-%m-%d %H:%M:%S%.6f").to_string())
            }
            _ => None,
        };
        // Never fall back to the literal string "null" here: that turns a
        // failed serialization into a row that looks like a task which
        // successfully returned `null`, silently losing the real result.
        let result_str = result
            .map(|v| {
                serde_json::to_string(&v).map_err(|e| {
                    CelersError::Serialization(format!(
                        "Failed to serialize result for task {task_id}: {e}"
                    ))
                })
            })
            .transpose()?;
        let status_str = status.to_string();

        // MySQL uses INSERT ... ON DUPLICATE KEY UPDATE instead of ON CONFLICT
        self.conn
            .execute(
                r#"
                INSERT INTO celers_broker_results
                    (task_id, task_name, status, result, error, traceback, created_at, completed_at, runtime_ms)
                VALUES (?, ?, ?, ?, ?, ?, NOW(), ?, ?)
                ON DUPLICATE KEY UPDATE
                    status = VALUES(status),
                    result = VALUES(result),
                    error = VALUES(error),
                    traceback = VALUES(traceback),
                    completed_at = VALUES(completed_at),
                    runtime_ms = VALUES(runtime_ms)
                "#,
                &[
                    &task_id.to_string(),
                    &task_name,
                    &status_str,
                    &result_str,
                    &error,
                    &traceback,
                    &completed_at,
                    &runtime_ms,
                ],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to store result: {}", e)))?;

        Ok(())
    }

    /// Get a task result from the database
    pub async fn get_result(&self, task_id: &TaskId) -> Result<Option<TaskResult>> {
        let rows = self
            .conn
            .query(
                r#"
                SELECT task_id, task_name, status, result, error, traceback,
                       created_at, completed_at, runtime_ms
                FROM celers_broker_results
                WHERE task_id = ?
                "#,
                &[&task_id.to_string()],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get result: {}", e)))?;

        match rows.into_iter().next() {
            Some(row) => {
                let task_id_str: String = row
                    .col("task_id")
                    .map_err(|e| CelersError::Other(format!("Failed to get result: {e}")))?;
                let status_str: String = row
                    .col("status")
                    .map_err(|e| CelersError::Other(format!("Failed to get result: {e}")))?;
                // `result`, `error` and `traceback` are `LONGTEXT`/`TEXT`, and
                // MySQL sends those over the same wire type as `BLOB`, so
                // `oxisql-mysql` hands them back as `Value::Blob` — which
                // `col::<Option<String>>` rejects with `type mismatch:
                // expected Text, got Blob` on every row that is not `NULL`.
                // See `row_ext::opt_text_from_row`.
                let result_str = crate::row_ext::opt_text_from_row(&row, "result")
                    .map_err(|e| CelersError::Other(format!("Failed to get result: {e}")))?;
                Ok(Some(TaskResult {
                    task_id: Uuid::parse_str(&task_id_str)
                        .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?,
                    task_name: row
                        .col("task_name")
                        .map_err(|e| CelersError::Other(format!("Failed to get result: {e}")))?,
                    status: status_str.parse()?,
                    result: result_str.and_then(|s| serde_json::from_str(&s).ok()),
                    error: crate::row_ext::opt_text_from_row(&row, "error")
                        .map_err(|e| CelersError::Other(format!("Failed to get result: {e}")))?,
                    traceback: crate::row_ext::opt_text_from_row(&row, "traceback")
                        .map_err(|e| CelersError::Other(format!("Failed to get result: {e}")))?,
                    created_at: row
                        .col("created_at")
                        .map_err(|e| CelersError::Other(format!("Failed to get result: {e}")))?,
                    completed_at: row
                        .col("completed_at")
                        .map_err(|e| CelersError::Other(format!("Failed to get result: {e}")))?,
                    runtime_ms: row
                        .col("runtime_ms")
                        .map_err(|e| CelersError::Other(format!("Failed to get result: {e}")))?,
                }))
            }
            None => Ok(None),
        }
    }

    /// Delete a task result from the database
    pub async fn delete_result(&self, task_id: &TaskId) -> Result<bool> {
        let affected = self
            .conn
            .execute(
                "DELETE FROM celers_broker_results WHERE task_id = ?",
                &[&task_id.to_string()],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to delete result: {}", e)))?;

        Ok(affected > 0)
    }

    /// Archive old task results
    ///
    /// Deletes results older than the specified duration.
    pub async fn archive_results(&self, older_than: Duration) -> Result<u64> {
        let cutoff = Utc::now() - chrono::Duration::seconds(older_than.as_secs() as i64);
        let cutoff_str = cutoff.format("%Y-%m-%d %H:%M:%S%.6f").to_string();

        let affected = self
            .conn
            .execute(
                r#"
                DELETE FROM celers_broker_results
                WHERE completed_at < ?
                "#,
                &[&cutoff_str],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to archive results: {}", e)))?;

        tracing::info!(count = affected, cutoff = %cutoff, "Archived old results");
        Ok(affected)
    }
}
