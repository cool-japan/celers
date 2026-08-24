//! Queue control and task inspection operations

use celers_core::{CelersError, Result, TaskId};
use oxisql_core::ToSqlValue;
use std::sync::atomic::Ordering;

use crate::row_ext::{json_param, uuid_from_row, uuid_param, RowExt};
use crate::types::{DbTaskState, QueueStatistics, TaskInfo};
use crate::PostgresBroker;

impl PostgresBroker {
    // ========== Queue Control ==========

    /// Pause the queue (dequeue will return None while paused)
    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
        tracing::info!(queue = %self.queue_name, "Queue paused");
    }

    /// Resume the queue
    pub fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
        tracing::info!(queue = %self.queue_name, "Queue resumed");
    }

    /// Check if the queue is paused
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    // ========== Task Inspection ==========

    /// Map a `TaskInfo`-shaped row. Shared by every method in this file that
    /// selects the same 12-column `TaskInfo` projection, so the
    /// UUID/DateTime/Option handling is written exactly once.
    fn row_to_task_info(row: &oxisql_core::Row) -> Result<TaskInfo> {
        let state_str: String = row
            .col("state")
            .map_err(|e| CelersError::Other(format!("Failed to read state: {}", e)))?;
        Ok(TaskInfo {
            id: uuid_from_row(row, "id")
                .map_err(|e| CelersError::Other(format!("Failed to read id: {}", e)))?,
            task_name: row
                .col("task_name")
                .map_err(|e| CelersError::Other(format!("Failed to read task_name: {}", e)))?,
            state: state_str.parse()?,
            priority: row
                .col("priority")
                .map_err(|e| CelersError::Other(format!("Failed to read priority: {}", e)))?,
            retry_count: row
                .col("retry_count")
                .map_err(|e| CelersError::Other(format!("Failed to read retry_count: {}", e)))?,
            max_retries: row
                .col("max_retries")
                .map_err(|e| CelersError::Other(format!("Failed to read max_retries: {}", e)))?,
            created_at: row
                .col("created_at")
                .map_err(|e| CelersError::Other(format!("Failed to read created_at: {}", e)))?,
            scheduled_at: row
                .col("scheduled_at")
                .map_err(|e| CelersError::Other(format!("Failed to read scheduled_at: {}", e)))?,
            started_at: row
                .col("started_at")
                .map_err(|e| CelersError::Other(format!("Failed to read started_at: {}", e)))?,
            completed_at: row
                .col("completed_at")
                .map_err(|e| CelersError::Other(format!("Failed to read completed_at: {}", e)))?,
            worker_id: row
                .col("worker_id")
                .map_err(|e| CelersError::Other(format!("Failed to read worker_id: {}", e)))?,
            error_message: row
                .col("error_message")
                .map_err(|e| CelersError::Other(format!("Failed to read error_message: {}", e)))?,
        })
    }

    /// Get detailed information about a specific task
    pub async fn get_task(&self, task_id: &TaskId) -> Result<Option<TaskInfo>> {
        let task_id_param = uuid_param(task_id);
        let rows = self
            .conn
            .query(
                r#"
            SELECT id, task_name, state, priority, retry_count, max_retries,
                   created_at, scheduled_at, started_at, completed_at, worker_id, error_message
            FROM celers_tasks
            WHERE id = $1
            "#,
                &[&task_id_param],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get task: {}", e)))?;

        match rows.into_iter().next() {
            Some(row) => Ok(Some(Self::row_to_task_info(&row)?)),
            None => Ok(None),
        }
    }

    /// List tasks by state with pagination
    pub async fn list_tasks(
        &self,
        state: Option<DbTaskState>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<TaskInfo>> {
        let rows = match state {
            Some(s) => {
                let state_param = s.to_string();
                self.conn
                    .query(
                        r#"
                    SELECT id, task_name, state, priority, retry_count, max_retries,
                           created_at, scheduled_at, started_at, completed_at, worker_id, error_message
                    FROM celers_tasks
                    WHERE state = $1
                    ORDER BY created_at DESC
                    LIMIT $2 OFFSET $3
                    "#,
                        &[&state_param, &limit, &offset],
                    )
                    .await
            }
            None => {
                self.conn
                    .query(
                        r#"
                    SELECT id, task_name, state, priority, retry_count, max_retries,
                           created_at, scheduled_at, started_at, completed_at, worker_id, error_message
                    FROM celers_tasks
                    ORDER BY created_at DESC
                    LIMIT $1 OFFSET $2
                    "#,
                        &[&limit, &offset],
                    )
                    .await
            }
        }
        .map_err(|e| CelersError::Other(format!("Failed to list tasks: {}", e)))?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in &rows {
            tasks.push(Self::row_to_task_info(row)?);
        }
        Ok(tasks)
    }

    /// Find tasks by metadata JSON path query
    ///
    /// Uses PostgreSQL's JSONB operators to query tasks by metadata.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Find tasks where metadata.user_id = 123
    /// let tasks = broker.find_tasks_by_metadata("user_id", &json!(123), 10, 0).await?;
    ///
    /// // Find tasks where metadata.priority = "high"
    /// let tasks = broker.find_tasks_by_metadata("priority", &json!("high"), 10, 0).await?;
    /// ```
    pub async fn find_tasks_by_metadata(
        &self,
        json_path: &str,
        value: &serde_json::Value,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<TaskInfo>> {
        let value_param = json_param(value);
        let rows = self
            .conn
            .query(
                r#"
            SELECT id, task_name, state, priority, retry_count, max_retries,
                   created_at, scheduled_at, started_at, completed_at, worker_id, error_message
            FROM celers_tasks
            WHERE queue_name = $1
              AND metadata->$2 = $3::text::jsonb
            ORDER BY created_at DESC
            LIMIT $4 OFFSET $5
            "#,
                &[&self.queue_name, &json_path, &value_param, &limit, &offset],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to find tasks by metadata: {}", e)))?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in &rows {
            tasks.push(Self::row_to_task_info(row)?);
        }
        Ok(tasks)
    }

    /// Count tasks matching metadata criteria
    ///
    /// # Examples
    ///
    /// ```ignore
    /// // Count tasks where metadata.user_id = 123
    /// let count = broker.count_tasks_by_metadata("user_id", &json!(123)).await?;
    /// ```
    pub async fn count_tasks_by_metadata(
        &self,
        json_path: &str,
        value: &serde_json::Value,
    ) -> Result<i64> {
        let value_param = json_param(value);
        let rows = self
            .conn
            .query(
                r#"
            SELECT COUNT(*) as count
            FROM celers_tasks
            WHERE queue_name = $1
              AND metadata->$2 = $3::text::jsonb
            "#,
                &[&self.queue_name, &json_path, &value_param],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to count tasks by metadata: {}", e)))?;

        // .fetch_one (via query_scalar in the original): error if no row.
        let row = rows.into_iter().next().ok_or_else(|| {
            CelersError::Other("Failed to count tasks by metadata: no rows returned".to_string())
        })?;
        row.col("count")
            .map_err(|e| CelersError::Other(format!("Failed to read count: {}", e)))
    }

    /// Find tasks by task name with pagination
    ///
    /// This is useful for monitoring specific task types.
    pub async fn find_tasks_by_name(
        &self,
        task_name: &str,
        state: Option<DbTaskState>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<TaskInfo>> {
        let rows = match state {
            Some(s) => {
                let state_param = s.to_string();
                self.conn
                    .query(
                        r#"
                    SELECT id, task_name, state, priority, retry_count, max_retries,
                           created_at, scheduled_at, started_at, completed_at, worker_id, error_message
                    FROM celers_tasks
                    WHERE task_name = $1 AND state = $2
                    ORDER BY created_at DESC
                    LIMIT $3 OFFSET $4
                    "#,
                        &[&task_name, &state_param, &limit, &offset],
                    )
                    .await
            }
            None => {
                self.conn
                    .query(
                        r#"
                    SELECT id, task_name, state, priority, retry_count, max_retries,
                           created_at, scheduled_at, started_at, completed_at, worker_id, error_message
                    FROM celers_tasks
                    WHERE task_name = $1
                    ORDER BY created_at DESC
                    LIMIT $2 OFFSET $3
                    "#,
                        &[&task_name, &limit, &offset],
                    )
                    .await
            }
        }
        .map_err(|e| CelersError::Other(format!("Failed to find tasks by name: {}", e)))?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in &rows {
            tasks.push(Self::row_to_task_info(row)?);
        }
        Ok(tasks)
    }

    /// Search tasks using advanced JSONB query with JSONPath
    ///
    /// Enables complex JSON queries using PostgreSQL's JSONPath operators.
    /// Supports nested paths, array indexing, and complex filters.
    ///
    /// # Arguments
    ///
    /// * `jsonpath` - JSONPath expression (e.g., "$.user.id", "$.tags\[*\]")
    /// * `value` - Value to match
    /// * `limit` - Maximum number of results
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    /// use serde_json::json;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Find tasks with nested metadata: user.department = "engineering"
    /// let tasks = broker.search_tasks_by_jsonpath(
    ///     "$.user.department",
    ///     &json!("engineering"),
    ///     100
    /// ).await?;
    /// println!("Found {} engineering tasks", tasks.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn search_tasks_by_jsonpath(
        &self,
        jsonpath: &str,
        value: &serde_json::Value,
        limit: i64,
    ) -> Result<Vec<TaskInfo>> {
        let jsonpath_expr = format!("$.{}", jsonpath.trim_start_matches("$."));
        let jsonpath_field = jsonpath.trim_start_matches("$.").to_string();
        let value_str = value.as_str().unwrap_or("").to_string();
        let rows = self
            .conn
            .query(
                r#"
            SELECT id, task_name, state, priority, retry_count, max_retries,
                   created_at, scheduled_at, started_at, completed_at, worker_id, error_message
            FROM celers_tasks
            WHERE queue_name = $1
              AND jsonb_path_exists(metadata, $2::jsonpath)
              AND metadata #>> $3 = $4
            ORDER BY created_at DESC
            LIMIT $5
            "#,
                &[
                    &self.queue_name,
                    &jsonpath_expr,
                    &jsonpath_field,
                    &value_str,
                    &limit,
                ],
            )
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to search tasks by JSONPath: {}", e))
            })?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in &rows {
            tasks.push(TaskInfo {
                id: uuid_from_row(row, "id")
                    .map_err(|e| CelersError::Other(format!("Failed to read id: {}", e)))?,
                task_name: row
                    .col("task_name")
                    .map_err(|e| CelersError::Other(format!("Failed to read task_name: {}", e)))?,
                state: row
                    .col::<String>("state")
                    .map_err(|e| CelersError::Other(format!("Failed to read state: {}", e)))?
                    .parse()
                    .unwrap_or(DbTaskState::Pending),
                priority: row
                    .col("priority")
                    .map_err(|e| CelersError::Other(format!("Failed to read priority: {}", e)))?,
                retry_count: row.col("retry_count").map_err(|e| {
                    CelersError::Other(format!("Failed to read retry_count: {}", e))
                })?,
                max_retries: row.col("max_retries").map_err(|e| {
                    CelersError::Other(format!("Failed to read max_retries: {}", e))
                })?,
                created_at: row
                    .col("created_at")
                    .map_err(|e| CelersError::Other(format!("Failed to read created_at: {}", e)))?,
                scheduled_at: row.col("scheduled_at").map_err(|e| {
                    CelersError::Other(format!("Failed to read scheduled_at: {}", e))
                })?,
                started_at: row
                    .col("started_at")
                    .map_err(|e| CelersError::Other(format!("Failed to read started_at: {}", e)))?,
                completed_at: row.col("completed_at").map_err(|e| {
                    CelersError::Other(format!("Failed to read completed_at: {}", e))
                })?,
                worker_id: row
                    .col("worker_id")
                    .map_err(|e| CelersError::Other(format!("Failed to read worker_id: {}", e)))?,
                error_message: row.col("error_message").map_err(|e| {
                    CelersError::Other(format!("Failed to read error_message: {}", e))
                })?,
            });
        }

        tracing::debug!(count = tasks.len(), jsonpath, "Searched tasks by JSONPath");
        Ok(tasks)
    }

    /// Find tasks where metadata contains specific key-value pairs (multiple filters)
    ///
    /// Applies multiple metadata filters with AND logic.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    /// use serde_json::json;
    /// use std::collections::HashMap;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let mut filters = HashMap::new();
    /// filters.insert("user_id".to_string(), json!(123));
    /// filters.insert("status".to_string(), json!("active"));
    ///
    /// let tasks = broker.find_tasks_by_metadata_filters(&filters, 50).await?;
    /// println!("Found {} matching tasks", tasks.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn find_tasks_by_metadata_filters(
        &self,
        filters: &std::collections::HashMap<String, serde_json::Value>,
        limit: i64,
    ) -> Result<Vec<TaskInfo>> {
        if filters.is_empty() {
            return Ok(Vec::new());
        }

        // Collect once so clause-building order and bind order are provably
        // derived from the same data (never re-iterate `filters.keys()`).
        let filter_pairs: Vec<(&String, &serde_json::Value)> = filters.iter().collect();

        // Every filter contributes TWO binds: the JSON key via `metadata->$n`
        // and the value via `$n+1`. Nothing is spliced as text, so the
        // assembled string below contains only `$n` placeholders. Byte-for-byte
        // preserved from the pre-migration `sqlx::AssertSqlSafe` version;
        // oxisql's `execute`/`query` take `&str` directly so `AssertSqlSafe`
        // simply drops away.
        let mut where_clauses = Vec::with_capacity(filter_pairs.len());
        let mut bind_idx = 2; // $1 is queue_name
        for _ in &filter_pairs {
            // The JSON value is bound as text and cast server-side: a
            // `String` parameter's binary wire encoding is raw UTF-8, which is
            // not a valid binary `jsonb` payload (that format carries a
            // 1-byte version header).
            where_clauses.push(format!(
                "metadata->${} = ${}::text::jsonb",
                bind_idx,
                bind_idx + 1
            ));
            bind_idx += 2;
        }

        let query_str = format!(
            r#"
            SELECT id, task_name, state, priority, retry_count, max_retries,
                   created_at, scheduled_at, started_at, completed_at, worker_id, error_message
            FROM celers_tasks
            WHERE queue_name = $1
              AND {}
            ORDER BY created_at DESC
            LIMIT ${}
            "#,
            where_clauses.join(" AND "),
            bind_idx
        );

        // Build the owned param values first (String for the queue name and
        // every JSON key/value, matching what `json_param` produces), then a
        // `Vec<&dyn ToSqlValue>` of references into them — mirrors the
        // dynamic-placeholder-count array-binding rewrite pattern used in
        // `broker_trait.rs`'s `dequeue_batch`/`ack_batch`.
        let mut owned_params: Vec<String> = Vec::with_capacity(1 + filter_pairs.len() * 2);
        owned_params.push(self.queue_name.clone());
        for (key, value) in &filter_pairs {
            owned_params.push((*key).clone());
            owned_params.push(json_param(value));
        }
        let mut param_refs: Vec<&dyn ToSqlValue> =
            owned_params.iter().map(|p| p as &dyn ToSqlValue).collect();
        param_refs.push(&limit);

        let rows = self
            .conn
            .query(&query_str, &param_refs)
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to find tasks by metadata filters: {}", e))
            })?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in &rows {
            tasks.push(TaskInfo {
                id: uuid_from_row(row, "id")
                    .map_err(|e| CelersError::Other(format!("Failed to read id: {}", e)))?,
                task_name: row
                    .col("task_name")
                    .map_err(|e| CelersError::Other(format!("Failed to read task_name: {}", e)))?,
                state: row
                    .col::<String>("state")
                    .map_err(|e| CelersError::Other(format!("Failed to read state: {}", e)))?
                    .parse()
                    .unwrap_or(DbTaskState::Pending),
                priority: row
                    .col("priority")
                    .map_err(|e| CelersError::Other(format!("Failed to read priority: {}", e)))?,
                retry_count: row.col("retry_count").map_err(|e| {
                    CelersError::Other(format!("Failed to read retry_count: {}", e))
                })?,
                max_retries: row.col("max_retries").map_err(|e| {
                    CelersError::Other(format!("Failed to read max_retries: {}", e))
                })?,
                created_at: row
                    .col("created_at")
                    .map_err(|e| CelersError::Other(format!("Failed to read created_at: {}", e)))?,
                scheduled_at: row.col("scheduled_at").map_err(|e| {
                    CelersError::Other(format!("Failed to read scheduled_at: {}", e))
                })?,
                started_at: row
                    .col("started_at")
                    .map_err(|e| CelersError::Other(format!("Failed to read started_at: {}", e)))?,
                completed_at: row.col("completed_at").map_err(|e| {
                    CelersError::Other(format!("Failed to read completed_at: {}", e))
                })?,
                worker_id: row
                    .col("worker_id")
                    .map_err(|e| CelersError::Other(format!("Failed to read worker_id: {}", e)))?,
                error_message: row.col("error_message").map_err(|e| {
                    CelersError::Other(format!("Failed to read error_message: {}", e))
                })?,
            });
        }

        tracing::debug!(
            count = tasks.len(),
            filter_count = filters.len(),
            "Found tasks by metadata filters"
        );
        Ok(tasks)
    }

    /// Get queue statistics
    pub async fn get_statistics(&self) -> Result<QueueStatistics> {
        let rows = self
            .conn
            .query(
                r#"
            SELECT
                COUNT(*) FILTER (WHERE state = 'pending') as pending,
                COUNT(*) FILTER (WHERE state = 'processing') as processing,
                COUNT(*) FILTER (WHERE state = 'completed') as completed,
                COUNT(*) FILTER (WHERE state = 'failed') as failed,
                COUNT(*) FILTER (WHERE state = 'cancelled') as cancelled,
                COUNT(*) as total
            FROM celers_tasks
            WHERE queue_name = $1
            "#,
                &[&self.queue_name],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get statistics: {}", e)))?;

        let row = rows.into_iter().next().ok_or_else(|| {
            CelersError::Other("Failed to get statistics: no rows returned".to_string())
        })?;

        let dlq_rows = self
            .conn
            .query(
                "SELECT COUNT(*) FROM celers_dead_letter_queue WHERE queue_name = $1",
                &[&self.queue_name],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get DLQ count: {}", e)))?;
        let dlq_row = dlq_rows.into_iter().next().ok_or_else(|| {
            CelersError::Other("Failed to get DLQ count: no rows returned".to_string())
        })?;
        // Unaliased `COUNT(*)` (no `AS` clause): use positional access per
        // the RowExt convention documented in `row_ext.rs` (the driver
        // assigns an implicit column name — `"count"` here, since Postgres
        // does special-case a single unadorned aggregate/function call as
        // its own name — but `col_idx` is used to avoid relying on that
        // guess, matching how `find_tasks_by_metadata`'s sibling
        // `count_tasks_by_metadata` above uses the aliased form instead).
        let dlq_count: i64 = dlq_row
            .col_idx(0)
            .map_err(|e| CelersError::Other(format!("Failed to read DLQ count: {}", e)))?;

        Ok(QueueStatistics {
            pending: row
                .col("pending")
                .map_err(|e| CelersError::Other(format!("Failed to read pending: {}", e)))?,
            processing: row
                .col("processing")
                .map_err(|e| CelersError::Other(format!("Failed to read processing: {}", e)))?,
            completed: row
                .col("completed")
                .map_err(|e| CelersError::Other(format!("Failed to read completed: {}", e)))?,
            failed: row
                .col("failed")
                .map_err(|e| CelersError::Other(format!("Failed to read failed: {}", e)))?,
            cancelled: row
                .col("cancelled")
                .map_err(|e| CelersError::Other(format!("Failed to read cancelled: {}", e)))?,
            dlq: dlq_count,
            total: row
                .col("total")
                .map_err(|e| CelersError::Other(format!("Failed to read total: {}", e)))?,
        })
    }
}
