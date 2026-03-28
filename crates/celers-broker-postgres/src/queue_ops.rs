//! Queue control and task inspection operations

use celers_core::{CelersError, Result, TaskId};
use sqlx::Row;
use std::sync::atomic::Ordering;

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

    /// Get detailed information about a specific task
    pub async fn get_task(&self, task_id: &TaskId) -> Result<Option<TaskInfo>> {
        let row = sqlx::query(
            r#"
            SELECT id, task_name, state, priority, retry_count, max_retries,
                   created_at, scheduled_at, started_at, completed_at, worker_id, error_message
            FROM celers_tasks
            WHERE id = $1
            "#,
        )
        .bind(task_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get task: {}", e)))?;

        match row {
            Some(row) => {
                let state_str: String = row.get("state");
                Ok(Some(TaskInfo {
                    id: row.get("id"),
                    task_name: row.get("task_name"),
                    state: state_str.parse()?,
                    priority: row.get("priority"),
                    retry_count: row.get("retry_count"),
                    max_retries: row.get("max_retries"),
                    created_at: row.get("created_at"),
                    scheduled_at: row.get("scheduled_at"),
                    started_at: row.get("started_at"),
                    completed_at: row.get("completed_at"),
                    worker_id: row.get("worker_id"),
                    error_message: row.get("error_message"),
                }))
            }
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
                sqlx::query(
                    r#"
                    SELECT id, task_name, state, priority, retry_count, max_retries,
                           created_at, scheduled_at, started_at, completed_at, worker_id, error_message
                    FROM celers_tasks
                    WHERE state = $1
                    ORDER BY created_at DESC
                    LIMIT $2 OFFSET $3
                    "#,
                )
                .bind(s.to_string())
                .bind(limit)
                .bind(offset)
                .fetch_all(&self.pool)
                .await
            }
            None => {
                sqlx::query(
                    r#"
                    SELECT id, task_name, state, priority, retry_count, max_retries,
                           created_at, scheduled_at, started_at, completed_at, worker_id, error_message
                    FROM celers_tasks
                    ORDER BY created_at DESC
                    LIMIT $1 OFFSET $2
                    "#,
                )
                .bind(limit)
                .bind(offset)
                .fetch_all(&self.pool)
                .await
            }
        }
        .map_err(|e| CelersError::Other(format!("Failed to list tasks: {}", e)))?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            let state_str: String = row.get("state");
            tasks.push(TaskInfo {
                id: row.get("id"),
                task_name: row.get("task_name"),
                state: state_str.parse()?,
                priority: row.get("priority"),
                retry_count: row.get("retry_count"),
                max_retries: row.get("max_retries"),
                created_at: row.get("created_at"),
                scheduled_at: row.get("scheduled_at"),
                started_at: row.get("started_at"),
                completed_at: row.get("completed_at"),
                worker_id: row.get("worker_id"),
                error_message: row.get("error_message"),
            });
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
        let rows = sqlx::query(
            r#"
            SELECT id, task_name, state, priority, retry_count, max_retries,
                   created_at, scheduled_at, started_at, completed_at, worker_id, error_message
            FROM celers_tasks
            WHERE metadata->$1 = $2
            ORDER BY created_at DESC
            LIMIT $3 OFFSET $4
            "#,
        )
        .bind(json_path)
        .bind(value)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to find tasks by metadata: {}", e)))?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            let state_str: String = row.get("state");
            tasks.push(TaskInfo {
                id: row.get("id"),
                task_name: row.get("task_name"),
                state: state_str.parse()?,
                priority: row.get("priority"),
                retry_count: row.get("retry_count"),
                max_retries: row.get("max_retries"),
                created_at: row.get("created_at"),
                scheduled_at: row.get("scheduled_at"),
                started_at: row.get("started_at"),
                completed_at: row.get("completed_at"),
                worker_id: row.get("worker_id"),
                error_message: row.get("error_message"),
            });
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
        let count: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM celers_tasks
            WHERE metadata->$1 = $2
            "#,
        )
        .bind(json_path)
        .bind(value)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to count tasks by metadata: {}", e)))?;

        Ok(count)
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
                sqlx::query(
                    r#"
                    SELECT id, task_name, state, priority, retry_count, max_retries,
                           created_at, scheduled_at, started_at, completed_at, worker_id, error_message
                    FROM celers_tasks
                    WHERE task_name = $1 AND state = $2
                    ORDER BY created_at DESC
                    LIMIT $3 OFFSET $4
                    "#,
                )
                .bind(task_name)
                .bind(s.to_string())
                .bind(limit)
                .bind(offset)
                .fetch_all(&self.pool)
                .await
            }
            None => {
                sqlx::query(
                    r#"
                    SELECT id, task_name, state, priority, retry_count, max_retries,
                           created_at, scheduled_at, started_at, completed_at, worker_id, error_message
                    FROM celers_tasks
                    WHERE task_name = $1
                    ORDER BY created_at DESC
                    LIMIT $2 OFFSET $3
                    "#,
                )
                .bind(task_name)
                .bind(limit)
                .bind(offset)
                .fetch_all(&self.pool)
                .await
            }
        }
        .map_err(|e| CelersError::Other(format!("Failed to find tasks by name: {}", e)))?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            let state_str: String = row.get("state");
            tasks.push(TaskInfo {
                id: row.get("id"),
                task_name: row.get("task_name"),
                state: state_str.parse()?,
                priority: row.get("priority"),
                retry_count: row.get("retry_count"),
                max_retries: row.get("max_retries"),
                created_at: row.get("created_at"),
                scheduled_at: row.get("scheduled_at"),
                started_at: row.get("started_at"),
                completed_at: row.get("completed_at"),
                worker_id: row.get("worker_id"),
                error_message: row.get("error_message"),
            });
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
        let rows = sqlx::query(
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
        )
        .bind(&self.queue_name)
        .bind(format!("$.{}", jsonpath.trim_start_matches("$.")))
        .bind([jsonpath.trim_start_matches("$.")])
        .bind(value.as_str().unwrap_or(""))
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to search tasks by JSONPath: {}", e)))?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            tasks.push(TaskInfo {
                id: row.get("id"),
                task_name: row.get("task_name"),
                state: row
                    .get::<String, _>("state")
                    .parse()
                    .unwrap_or(DbTaskState::Pending),
                priority: row.get("priority"),
                retry_count: row.get("retry_count"),
                max_retries: row.get("max_retries"),
                created_at: row.get("created_at"),
                scheduled_at: row.get("scheduled_at"),
                started_at: row.get("started_at"),
                completed_at: row.get("completed_at"),
                worker_id: row.get("worker_id"),
                error_message: row.get("error_message"),
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

        // Build dynamic WHERE clause
        let mut where_clauses = Vec::new();
        let mut bind_idx = 2; // Start at 2 because $1 is queue_name

        for key in filters.keys() {
            where_clauses.push(format!("metadata->'{}' = ${}", key, bind_idx));
            bind_idx += 1;
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

        let mut query = sqlx::query(&query_str).bind(&self.queue_name);

        // Bind all filter values in order
        for key in filters.keys() {
            query = query.bind(&filters[key]);
        }

        query = query.bind(limit);

        let rows = query.fetch_all(&self.pool).await.map_err(|e| {
            CelersError::Other(format!("Failed to find tasks by metadata filters: {}", e))
        })?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            tasks.push(TaskInfo {
                id: row.get("id"),
                task_name: row.get("task_name"),
                state: row
                    .get::<String, _>("state")
                    .parse()
                    .unwrap_or(DbTaskState::Pending),
                priority: row.get("priority"),
                retry_count: row.get("retry_count"),
                max_retries: row.get("max_retries"),
                created_at: row.get("created_at"),
                scheduled_at: row.get("scheduled_at"),
                started_at: row.get("started_at"),
                completed_at: row.get("completed_at"),
                worker_id: row.get("worker_id"),
                error_message: row.get("error_message"),
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
        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*) FILTER (WHERE state = 'pending') as pending,
                COUNT(*) FILTER (WHERE state = 'processing') as processing,
                COUNT(*) FILTER (WHERE state = 'completed') as completed,
                COUNT(*) FILTER (WHERE state = 'failed') as failed,
                COUNT(*) FILTER (WHERE state = 'cancelled') as cancelled,
                COUNT(*) as total
            FROM celers_tasks
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get statistics: {}", e)))?;

        let dlq_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM celers_dead_letter_queue")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get DLQ count: {}", e)))?;

        Ok(QueueStatistics {
            pending: row.get("pending"),
            processing: row.get("processing"),
            completed: row.get("completed"),
            failed: row.get("failed"),
            cancelled: row.get("cancelled"),
            dlq: dlq_count,
            total: row.get("total"),
        })
    }
}
