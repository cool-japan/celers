//! Task result storage operations, backed by `celers_broker_results`.
//!
//! # Three result tables, and which one is which
//!
//! A PostgreSQL server running CeleRS can carry three similarly named result
//! tables, migrated by three different code paths. Getting them confused is
//! what made every function in this module unreachable until migration
//! `009_broker_results.sql`, so the split is stated here as well as in that
//! file's header:
//!
//! | Table | Owner | Shape |
//! |---|---|---|
//! | `celers_broker_results` | **this module** (migration `009_broker_results.sql`) | `status`, `result` JSONB, `error`, `traceback`, `runtime_ms` |
//! | `celers_task_results` | `celers-backend-db`'s `PostgresResultBackend` (`001_init_postgres.sql`) | `result_state`, `result_data`, `retry_count`, `worker`, `expires_at`, `extra` |
//! | `celers_results` | this crate's migration `002_results.sql`, for schema compatibility with `celers-broker-sql` | `result` BYTEA, `error_message`, `state`, `expires_at` |
//!
//! Nothing in this crate reads or writes either of the other two:
//! `celers_results` has no Rust caller here at all, and
//! `celers_task_results` belongs to the *result backend*, which auto-migrates
//! it onto the same database that this broker migrates. This module's
//! statements used to name `celers_task_results`, which meant they collided
//! head-on with that backend's incompatible schema — the identical collision
//! `celers-broker-sql` already resolved on MySQL by renaming its own store to
//! `celers_broker_results`. The two backends now agree on both names.

use celers_core::{CelersError, Result, TaskId};
use chrono::Utc;
use oxisql_core::ToSqlValue;
use std::time::Duration;

use crate::row_ext::{json_param, uuid_from_row, uuid_param, RowExt};
use crate::types::{TaskResult, TaskResultStatus};
use crate::PostgresBroker;

/// Read the nullable `result` column as `Option<serde_json::Value>`,
/// distinguishing a true SQL `NULL` (-> `None`) from a stored JSON literal
/// `null` (-> `Some(serde_json::Value::Null)`).
///
/// This is deliberately NOT `row_ext.rs`'s [`crate::row_ext::json_from_row`]
/// helper: that helper collapses both cases to `serde_json::Value::Null`
/// (documented there as "matching the common 'absent JSON column'
/// convention"), which is the right default for most JSON columns in this
/// crate but loses information here — the original sqlx code's
/// `row.get::<Option<serde_json::Value>, _>("result")` (via
/// `sqlx::types::Json`) preserves the SQL-NULL/JSON-null distinction, and
/// `store_result` below can genuinely bind either (a caller passing
/// `Some(serde_json::Value::Null)` stores the JSON text `"null"`, not a SQL
/// `NULL`). Reading the raw `Option<String>` directly and only treating an
/// *absent* column as `None` preserves that exact original behavior.
fn task_result_json_column(row: &oxisql_core::Row, col: &str) -> Result<Option<serde_json::Value>> {
    let raw: Option<String> = row
        .col(col)
        .map_err(|e| CelersError::Other(format!("Failed to read {col}: {e}")))?;
    raw.map(|s| serde_json::from_str(&s))
        .transpose()
        .map_err(|e| CelersError::Other(format!("invalid JSON in column '{col}': {e}")))
}

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

        // `completed_at` is `Option<DateTime<Utc>>` bound as a query
        // parameter — see `row_ext.rs`'s `DateTime<Utc>` parameter
        // convention doc comment for why this is bound as an RFC3339 string
        // through a `$7::text::timestamptz` cast rather than `.timestamp()`
        // (`i64`). `Option<String>` is `ToSqlValue` (blanket impl over
        // `Option<T: ToSqlValue>`), and a `NULL` `$7` still round-trips
        // correctly through the same cast: `NULL::text::timestamptz` is
        // `NULL`, exactly matching sqlx's `.bind(completed_at)` behavior for
        // `None`.
        //
        // `result` (JSONB, nullable) -> `json_param` wrapped in `Option` so a
        // `None` result binds SQL `NULL` rather than the literal string
        // `"null"`, bound through the statement's `$4::text::jsonb` cast
        // (`NULL::text::jsonb` is still `NULL`).
        //
        // `task_id` targets a `UUID` column and therefore goes through the
        // matching `$1::text::uuid` cast — see `row_ext.rs`'s `uuid_param`
        // for why a bare `$1` there is rejected by the server.
        let task_id_param = uuid_param(task_id);
        let status_param = status.to_string();
        let result_param: Option<String> = result.as_ref().map(json_param);
        let completed_at_param: Option<String> = completed_at.map(|dt| dt.to_rfc3339());
        self.conn
            .execute(
                r#"
            INSERT INTO celers_broker_results
                (task_id, task_name, status, result, error, traceback, created_at, completed_at, runtime_ms)
            VALUES ($1::text::uuid, $2, $3, $4::text::jsonb, $5, $6, NOW(), $7::text::timestamptz, $8)
            ON CONFLICT (task_id) DO UPDATE SET
                status = EXCLUDED.status,
                result = EXCLUDED.result,
                error = EXCLUDED.error,
                traceback = EXCLUDED.traceback,
                completed_at = EXCLUDED.completed_at,
                runtime_ms = EXCLUDED.runtime_ms
            "#,
                &[
                    &task_id_param,
                    &task_name,
                    &status_param,
                    &result_param,
                    &error,
                    &traceback,
                    &completed_at_param,
                    &runtime_ms,
                ],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to store result: {}", e)))?;

        Ok(())
    }

    /// Get a task result from the database
    pub async fn get_result(&self, task_id: &TaskId) -> Result<Option<TaskResult>> {
        let task_id_param = uuid_param(task_id);
        let rows = self
            .conn
            .query(
                r#"
            SELECT task_id, task_name, status, result::text AS result, error, traceback,
                   created_at, completed_at, runtime_ms
            FROM celers_broker_results
            WHERE task_id = $1::text::uuid
            "#,
                &[&task_id_param],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get result: {}", e)))?;

        match rows.into_iter().next() {
            Some(row) => {
                let status_str: String = row
                    .col("status")
                    .map_err(|e| CelersError::Other(format!("Failed to read status: {}", e)))?;
                Ok(Some(TaskResult {
                    task_id: uuid_from_row(&row, "task_id").map_err(|e| {
                        CelersError::Other(format!("Failed to read task_id: {}", e))
                    })?,
                    task_name: row.col("task_name").map_err(|e| {
                        CelersError::Other(format!("Failed to read task_name: {}", e))
                    })?,
                    status: status_str.parse()?,
                    result: task_result_json_column(&row, "result")?,
                    error: row
                        .col("error")
                        .map_err(|e| CelersError::Other(format!("Failed to read error: {}", e)))?,
                    traceback: row.col("traceback").map_err(|e| {
                        CelersError::Other(format!("Failed to read traceback: {}", e))
                    })?,
                    created_at: row.col("created_at").map_err(|e| {
                        CelersError::Other(format!("Failed to read created_at: {}", e))
                    })?,
                    completed_at: row.col("completed_at").map_err(|e| {
                        CelersError::Other(format!("Failed to read completed_at: {}", e))
                    })?,
                    runtime_ms: row.col("runtime_ms").map_err(|e| {
                        CelersError::Other(format!("Failed to read runtime_ms: {}", e))
                    })?,
                }))
            }
            None => Ok(None),
        }
    }

    /// Delete a task result from the database
    pub async fn delete_result(&self, task_id: &TaskId) -> Result<bool> {
        let task_id_param = uuid_param(task_id);
        let rows_affected = self
            .conn
            .execute(
                "DELETE FROM celers_broker_results WHERE task_id = $1::text::uuid",
                &[&task_id_param],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to delete result: {}", e)))?;

        Ok(rows_affected > 0)
    }

    /// Archive old task results
    ///
    /// Deletes results older than the specified duration.
    pub async fn archive_results(&self, older_than: Duration) -> Result<u64> {
        let cutoff = Utc::now() - chrono::Duration::seconds(older_than.as_secs() as i64);

        // `cutoff` is a `DateTime<Utc>` bound as a query parameter — same
        // `$1::text::timestamptz` convention as `store_result` above.
        let cutoff_param = cutoff.to_rfc3339();
        let rows_affected = self
            .conn
            .execute(
                r#"
            DELETE FROM celers_broker_results
            WHERE completed_at < $1::text::timestamptz
            "#,
                &[&cutoff_param],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to archive results: {}", e)))?;

        tracing::info!(count = rows_affected, cutoff = %cutoff, "Archived old results");
        Ok(rows_affected)
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

        // `WHERE task_id = ANY($1)` -> oxisql has no array/slice `ToSqlValue`,
        // rewritten to a dynamically sized `IN ($1, .., $N)` placeholder
        // list, one `$n` per task id, each bound individually. Only the
        // *count* of placeholders is generated from `task_ids.len()` — no
        // value is ever spliced into the SQL text, so this remains fully
        // injection-safe. This is the same rewrite pattern as
        // `broker_trait.rs`'s `dequeue_batch`/`ack_batch`.
        let placeholders = crate::sql::uuid_in_clause(1, task_ids.len());
        let query_str = format!(
            r#"
            SELECT task_id, task_name, status, result::text AS result, error, traceback,
                   runtime_ms, created_at, completed_at
            FROM celers_broker_results
            WHERE task_id IN ({})
            ORDER BY created_at DESC
            "#,
            placeholders
        );
        let task_id_params: Vec<oxisql_core::Value> = task_ids.iter().map(uuid_param).collect();
        let param_refs: Vec<&dyn ToSqlValue> = task_id_params
            .iter()
            .map(|p| p as &dyn ToSqlValue)
            .collect();

        let rows = self
            .conn
            .query(&query_str, &param_refs)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get results batch: {}", e)))?;

        let mut results = Vec::with_capacity(rows.len());
        for row in &rows {
            results.push(TaskResult {
                task_id: uuid_from_row(row, "task_id")
                    .map_err(|e| CelersError::Other(format!("Failed to read task_id: {}", e)))?,
                task_name: row
                    .col("task_name")
                    .map_err(|e| CelersError::Other(format!("Failed to read task_name: {}", e)))?,
                status: row
                    .col::<String>("status")
                    .map_err(|e| CelersError::Other(format!("Failed to read status: {}", e)))?
                    .parse()
                    .unwrap_or(TaskResultStatus::Pending),
                result: task_result_json_column(row, "result")?,
                error: row
                    .col("error")
                    .map_err(|e| CelersError::Other(format!("Failed to read error: {}", e)))?,
                traceback: row
                    .col("traceback")
                    .map_err(|e| CelersError::Other(format!("Failed to read traceback: {}", e)))?,
                runtime_ms: row
                    .col("runtime_ms")
                    .map_err(|e| CelersError::Other(format!("Failed to read runtime_ms: {}", e)))?,
                created_at: row
                    .col("created_at")
                    .map_err(|e| CelersError::Other(format!("Failed to read created_at: {}", e)))?,
                completed_at: row.col("completed_at").map_err(|e| {
                    CelersError::Other(format!("Failed to read completed_at: {}", e))
                })?,
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

        // Same `ANY($1)` -> `IN ($1, .., $N)` rewrite as `get_results_batch`
        // above.
        let placeholders = crate::sql::uuid_in_clause(1, task_ids.len());
        let query_str = format!(
            r#"
            DELETE FROM celers_broker_results
            WHERE task_id IN ({})
            "#,
            placeholders
        );
        let task_id_params: Vec<oxisql_core::Value> = task_ids.iter().map(uuid_param).collect();
        let param_refs: Vec<&dyn ToSqlValue> = task_id_params
            .iter()
            .map(|p| p as &dyn ToSqlValue)
            .collect();

        let deleted = self
            .conn
            .execute(&query_str, &param_refs)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to delete results batch: {}", e)))?;

        tracing::info!(count = deleted, "Deleted batch of task results");
        Ok(deleted)
    }
}
