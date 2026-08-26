//! Resilience and advanced features for MysqlBroker
//!
//! Retry policies, recurring tasks, data export/import,
//! circuit breaker operations, and idempotency support.

use crate::broker_core::MysqlBroker;
use crate::circuit_breaker::{
    CircuitBreakerState, CircuitBreakerStats, IdempotencyRecord, IdempotencyStats,
};
use crate::row_ext::RowExt;
use crate::types::*;
use celers_core::{Broker, CelersError, Result, SerializedTask, TaskId};
use chrono::{DateTime, Utc};
use oxisql_core::Connection;
use serde_json::json;
use uuid::Uuid;

#[cfg(feature = "metrics")]
use celers_metrics::{TASKS_ENQUEUED_BY_TYPE, TASKS_ENQUEUED_TOTAL};

impl MysqlBroker {
    /// Enqueue a task with advanced retry policy
    ///
    /// Enqueues a task with custom retry behavior using exponential backoff, jitter, etc.
    ///
    /// # Arguments
    /// * `task` - Task to enqueue
    /// * `retry_policy` - Custom retry policy configuration
    ///
    /// # Returns
    /// Task ID
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::{MysqlBroker, RetryPolicy, RetryStrategy};
    /// # use celers_core::SerializedTask;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    ///
    /// let policy = RetryPolicy {
    ///     max_retries: 5,
    ///     strategy: RetryStrategy::ExponentialWithJitter {
    ///         base_delay_secs: 2,
    ///         multiplier: 2.0,
    ///         max_delay_secs: 600,
    ///     },
    /// };
    ///
    /// let task = SerializedTask::new("important_task".to_string(), vec![1, 2, 3]);
    /// let task_id = broker.enqueue_with_retry_policy(task, policy).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn enqueue_with_retry_policy(
        &self,
        task: SerializedTask,
        retry_policy: RetryPolicy,
    ) -> Result<TaskId> {
        let task_id = task.metadata.id;
        let retry_policy_json = serde_json::to_value(&retry_policy)
            .map_err(|e| CelersError::Other(format!("Failed to serialize retry policy: {}", e)))?;

        let metadata_str =
            self.build_task_metadata_document(&task, json!({ "retry_policy": retry_policy_json }))?;

        self.connection()
            .execute(
                r#"
                INSERT INTO celers_tasks
                    (id, queue_name, task_name, payload, state, priority, max_retries, metadata, created_at, scheduled_at)
                VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, NOW(), NOW())
                "#,
                &[
                    &task_id.to_string(),
                    &self.queue_name(),
                    &task.metadata.name,
                    &task.payload,
                    &task.metadata.priority,
                    &(retry_policy.max_retries as i32),
                    &metadata_str,
                ],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to enqueue task: {}", e)))?;

        #[cfg(feature = "metrics")]
        {
            TASKS_ENQUEUED_TOTAL.inc();
            TASKS_ENQUEUED_BY_TYPE
                .with_label_values(&[&task.metadata.name])
                .inc();
        }

        tracing::debug!(task_id = %task_id, task_name = %task.metadata.name, "Task enqueued with custom retry policy");
        Ok(task_id)
    }

    /// Reject a task with advanced retry scheduling
    ///
    /// Rejects a task and schedules retry based on the custom retry policy if present.
    ///
    /// # Arguments
    /// * `task_id` - Task ID to reject
    /// * `error_message` - Optional error message
    /// * `requeue` - Whether to requeue for retry
    ///
    /// # Returns
    /// true if task was rejected successfully
    pub async fn reject_with_retry_policy(
        &self,
        task_id: &TaskId,
        error_message: Option<String>,
        requeue: bool,
    ) -> Result<bool> {
        // Get current task info including retry policy
        let task_info = self.get_task(task_id).await?;
        let task_info = match task_info {
            Some(info) => info,
            None => return Ok(false),
        };

        if !requeue || task_info.retry_count >= task_info.max_retries {
            // Move to DLQ or mark as failed
            self.reject(task_id, None, false).await?;
            return Ok(true);
        }

        // Extract retry policy from metadata
        let retry_delay_secs = if let Ok(Some(task)) = self.get_task(task_id).await {
            // Try to get retry policy from metadata
            let metadata_str: Option<String> = self
                .connection()
                .query(
                    "SELECT metadata FROM celers_tasks WHERE id = ?",
                    &[&task_id.to_string()],
                )
                .await
                .ok()
                .and_then(|rows| rows.into_iter().next())
                .and_then(|row| row.col::<String>("metadata").ok());

            if let Some(meta_str) = metadata_str {
                if let Ok(meta) = serde_json::from_str::<serde_json::Value>(&meta_str) {
                    if let Some(policy_value) = meta.get("retry_policy") {
                        if let Ok(policy) =
                            serde_json::from_value::<RetryPolicy>(policy_value.clone())
                        {
                            policy.strategy.calculate_delay(task.retry_count as u32)
                        } else {
                            60 // Default 1 minute
                        }
                    } else {
                        60
                    }
                } else {
                    60
                }
            } else {
                60
            }
        } else {
            60
        };

        // Update task with retry scheduling
        let affected = self
            .connection()
            .execute(
                r#"
                UPDATE celers_tasks
                SET state = 'pending',
                    retry_count = retry_count + 1,
                    error_message = ?,
                    scheduled_at = DATE_ADD(NOW(), INTERVAL ? SECOND),
                    started_at = NULL
                WHERE id = ? AND state = 'processing'
                "#,
                &[
                    &error_message,
                    &(retry_delay_secs as i64),
                    &task_id.to_string(),
                ],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to reject task: {}", e)))?;

        Ok(affected > 0)
    }

    /// Register a recurring task
    ///
    /// Registers a task to be executed on a recurring schedule.
    /// The task will be automatically enqueued when it's due.
    ///
    /// # Arguments
    /// * `config` - Recurring task configuration
    ///
    /// # Returns
    /// ID of the recurring task configuration
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::{MysqlBroker, RecurringTaskConfig, RecurringSchedule};
    /// # use chrono::Utc;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    ///
    /// let config = RecurringTaskConfig {
    ///     task_name: "daily_cleanup".to_string(),
    ///     schedule: RecurringSchedule::EveryDays(1, 2, 0), // Every day at 2:00 AM
    ///     payload: vec![],
    ///     priority: 5,
    ///     enabled: true,
    ///     last_run: None,
    ///     next_run: Utc::now(),
    /// };
    ///
    /// let config_id = broker.register_recurring_task(config).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn register_recurring_task(&self, config: RecurringTaskConfig) -> Result<String> {
        let config_id = Uuid::new_v4().to_string();
        let config_json = serde_json::to_string(&config)
            .map_err(|e| CelersError::Other(format!("Failed to serialize config: {}", e)))?;
        let recurring_name = format!("__recurring__{}", config.task_name);

        self.connection()
            .execute(
                r#"
                INSERT INTO celers_broker_results
                    (task_id, task_name, status, result, created_at)
                VALUES (?, ?, 'PENDING', ?, NOW())
                ON DUPLICATE KEY UPDATE
                    result = VALUES(result),
                    created_at = NOW()
                "#,
                &[&config_id, &recurring_name, &config_json],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to register recurring task: {}", e)))?;

        tracing::info!(
            config_id = config_id,
            task_name = config.task_name,
            "Recurring task registered"
        );
        Ok(config_id)
    }

    /// Process due recurring tasks
    ///
    /// Checks for recurring tasks that are due and enqueues them.
    /// Should be called periodically (e.g., every minute) by a scheduler.
    ///
    /// # Distributed safety
    ///
    /// Safe to run from any number of scheduler ("beat") processes at once:
    /// each due configuration is **claimed before it is enqueued** with a
    /// compare-and-swap that swaps the whole stored JSON document for the one
    /// carrying the advanced `next_run`, conditional on the document still
    /// being byte-identical to what this process read
    /// (`sql_text::RECURRING_CLAIM_SQL`). Exactly one process
    /// observes `rows_affected == 1` and enqueues; the rest skip.
    ///
    /// This previously used a plain `SELECT` with no claim of any kind, so
    /// every scheduler instance enqueued every due task, and the write-back
    /// that would have closed the window was `let _ = ...` — a failed UPDATE
    /// left `next_run` in the past and re-enqueued the task on every
    /// subsequent tick forever.
    ///
    /// Claiming before enqueuing means a failed enqueue skips one period
    /// rather than duplicating the task; the failure is logged at error level.
    ///
    /// # Returns
    /// Number of tasks enqueued
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    ///
    /// // Call this periodically (e.g., in a background loop)
    /// let enqueued = broker.process_recurring_tasks().await?;
    /// println!("Enqueued {} recurring tasks", enqueued);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn process_recurring_tasks(&self) -> Result<u64> {
        // Get all recurring task configurations
        let rows = self
            .connection()
            .query(
                r#"
                SELECT task_id, result
                FROM celers_broker_results
                WHERE task_name LIKE '__recurring__%'
                  AND status = 'PENDING'
                "#,
                &[],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to fetch recurring tasks: {}", e)))?;

        let mut enqueued = 0u64;
        let now = Utc::now();

        for row in rows {
            let config_id: String = row
                .col("task_id")
                .map_err(|e| CelersError::Other(format!("Failed to fetch recurring tasks: {e}")))?;
            // `celers_results.result` is a MEDIUMBLOB holding JSON text, and
            // MySQL BLOB columns arrive as `Value::Blob`, which
            // `col::<String>` rejects. See `row_ext::text_from_row`.
            let config_json = crate::row_ext::text_from_row(&row, "result")
                .map_err(|e| CelersError::Other(format!("Failed to fetch recurring tasks: {e}")))?;

            let mut config: RecurringTaskConfig =
                serde_json::from_str(&config_json).map_err(|e| {
                    CelersError::Other(format!("Failed to parse recurring config: {}", e))
                })?;

            // Check if task is due
            if !config.enabled || config.next_run > now {
                continue;
            }

            // Advance the schedule, then claim the configuration by swapping
            // the stored document for the advanced one. Only the scheduler
            // whose swap actually lands may enqueue.
            config.last_run = Some(now);
            config.next_run = config.schedule.next_run_from(now);
            let updated_json = serde_json::to_string(&config).map_err(|e| {
                CelersError::Serialization(format!(
                    "Failed to serialize recurring config {config_id}: {e}"
                ))
            })?;

            let claimed = self
                .connection()
                .execute(
                    crate::sql_text::RECURRING_CLAIM_SQL,
                    &[&updated_json, &config_id, &config_json],
                )
                .await
                .map_err(|e| {
                    CelersError::Other(format!("Failed to claim recurring task {config_id}: {e}"))
                })?;

            if claimed == 0 {
                tracing::debug!(
                    config_id = config_id,
                    task_name = config.task_name,
                    "Recurring task already claimed by another scheduler instance"
                );
                continue;
            }

            let task = SerializedTask::new(config.task_name.clone(), config.payload.clone());
            match self.enqueue(task).await {
                Ok(_) => {
                    enqueued += 1;
                    tracing::debug!(
                        config_id = config_id,
                        task_name = config.task_name,
                        "Recurring task enqueued"
                    );
                }
                Err(e) => {
                    // The claim already advanced `next_run`, so this period is
                    // skipped rather than retried in a tight loop. Loud on
                    // purpose: this is a dropped scheduled execution.
                    tracing::error!(
                        config_id = config_id,
                        task_name = config.task_name,
                        next_run = %config.next_run,
                        error = %e,
                        "Claimed a due recurring task but failed to enqueue it; \
                         this occurrence is skipped"
                    );
                }
            }
        }

        Ok(enqueued)
    }

    /// List all recurring task configurations
    ///
    /// # Returns
    /// List of recurring task configurations
    pub async fn list_recurring_tasks(&self) -> Result<Vec<(String, RecurringTaskConfig)>> {
        let rows = self
            .connection()
            .query(
                r#"
                SELECT task_id, result
                FROM celers_broker_results
                WHERE task_name LIKE '__recurring__%'
                  AND status = 'PENDING'
                "#,
                &[],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to fetch recurring tasks: {}", e)))?;

        let mut configs = Vec::new();
        for row in rows {
            let config_id: String = row
                .col("task_id")
                .map_err(|e| CelersError::Other(format!("Failed to fetch recurring tasks: {e}")))?;
            // `celers_results.result` is a MEDIUMBLOB holding JSON text, and
            // MySQL BLOB columns arrive as `Value::Blob`, which
            // `col::<String>` rejects. See `row_ext::text_from_row`.
            let config_json = crate::row_ext::text_from_row(&row, "result")
                .map_err(|e| CelersError::Other(format!("Failed to fetch recurring tasks: {e}")))?;

            if let Ok(config) = serde_json::from_str::<RecurringTaskConfig>(&config_json) {
                configs.push((config_id, config));
            }
        }

        Ok(configs)
    }

    /// Delete a recurring task configuration
    ///
    /// # Arguments
    /// * `config_id` - ID of the recurring task configuration
    ///
    /// # Returns
    /// true if configuration was deleted
    pub async fn delete_recurring_task(&self, config_id: &str) -> Result<bool> {
        let affected = self
            .connection()
            .execute(
                r#"
                DELETE FROM celers_broker_results
                WHERE task_id = ? AND task_name LIKE '__recurring__%'
                "#,
                &[&config_id],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to delete recurring task: {}", e)))?;

        Ok(affected > 0)
    }

    /// Export tasks to JSON format for backup or migration
    ///
    /// Exports all tasks in the specified state to a JSON array.
    /// This is useful for backing up tasks or migrating between databases.
    ///
    /// # Arguments
    /// * `state` - Optional state filter (None exports all states)
    /// * `limit` - Maximum number of tasks to export (None exports all)
    ///
    /// # Returns
    /// JSON string containing the task data
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::{MysqlBroker, DbTaskState};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    ///
    /// // Export all pending tasks
    /// let json = broker.export_tasks(Some(DbTaskState::Pending), Some(1000)).await?;
    /// std::fs::write("pending_tasks.json", json)?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn export_tasks(
        &self,
        state: Option<DbTaskState>,
        limit: Option<i64>,
    ) -> Result<String> {
        let mut query = String::from(
            r#"
            SELECT id, task_name, payload, state, priority, retry_count, max_retries,
                   created_at, scheduled_at, started_at, completed_at, worker_id, error_message, metadata
            FROM celers_tasks
            "#,
        );

        if state.is_some() {
            query.push_str(" WHERE state = ?");
        }

        query.push_str(" ORDER BY created_at ASC");

        if limit.is_some() {
            query.push_str(" LIMIT ?");
        }

        // Placeholder order matches param push order below — only the `?`
        // *count* (never a value) was conditionally appended to `query`.
        let state_str = state.as_ref().map(|s| s.to_string());
        let mut param_refs: Vec<&dyn oxisql_core::ToSqlValue> = Vec::new();
        if let Some(s) = &state_str {
            param_refs.push(s);
        }
        if let Some(l) = &limit {
            param_refs.push(l);
        }

        let rows = self
            .connection()
            .query(&query, &param_refs)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to export tasks: {}", e)))?;

        let mut tasks = Vec::new();
        for row in rows {
            let task = serde_json::json!({
                "id": row.col::<String>("id").map_err(|e| CelersError::Other(format!("Failed to export tasks: {e}")))?,
                "task_name": row.col::<String>("task_name").map_err(|e| CelersError::Other(format!("Failed to export tasks: {e}")))?,
                "payload": row.col::<Vec<u8>>("payload").map_err(|e| CelersError::Other(format!("Failed to export tasks: {e}")))?,
                "state": row.col::<String>("state").map_err(|e| CelersError::Other(format!("Failed to export tasks: {e}")))?,
                "priority": row.col::<i32>("priority").map_err(|e| CelersError::Other(format!("Failed to export tasks: {e}")))?,
                "retry_count": row.col::<i32>("retry_count").map_err(|e| CelersError::Other(format!("Failed to export tasks: {e}")))?,
                "max_retries": row.col::<i32>("max_retries").map_err(|e| CelersError::Other(format!("Failed to export tasks: {e}")))?,
                "created_at": row.col::<DateTime<Utc>>("created_at").map_err(|e| CelersError::Other(format!("Failed to export tasks: {e}")))?,
                "scheduled_at": row.col::<DateTime<Utc>>("scheduled_at").map_err(|e| CelersError::Other(format!("Failed to export tasks: {e}")))?,
                "started_at": row.col::<Option<DateTime<Utc>>>("started_at").map_err(|e| CelersError::Other(format!("Failed to export tasks: {e}")))?,
                "completed_at": row.col::<Option<DateTime<Utc>>>("completed_at").map_err(|e| CelersError::Other(format!("Failed to export tasks: {e}")))?,
                "worker_id": row.col::<Option<String>>("worker_id").map_err(|e| CelersError::Other(format!("Failed to export tasks: {e}")))?,
                "error_message": row.col::<Option<String>>("error_message").map_err(|e| CelersError::Other(format!("Failed to export tasks: {e}")))?,
                "metadata": row.col::<String>("metadata").map_err(|e| CelersError::Other(format!("Failed to export tasks: {e}")))?,
            });
            tasks.push(task);
        }

        serde_json::to_string_pretty(&tasks)
            .map_err(|e| CelersError::Other(format!("Failed to serialize tasks: {}", e)))
    }

    /// Import tasks from JSON format
    ///
    /// Imports tasks from a JSON array (typically exported via export_tasks).
    /// This is useful for restoring tasks from backup or migrating between databases.
    ///
    /// # Arguments
    /// * `json_data` - JSON string containing task data
    /// * `skip_existing` - If true, skip tasks that already exist (by ID)
    ///
    /// # Returns
    /// Number of tasks imported
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    ///
    /// // Import tasks from backup
    /// let json = std::fs::read_to_string("pending_tasks.json")?;
    /// let imported = broker.import_tasks(&json, true).await?;
    /// println!("Imported {} tasks", imported);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn import_tasks(&self, json_data: &str, skip_existing: bool) -> Result<u64> {
        let tasks: Vec<serde_json::Value> = serde_json::from_str(json_data)
            .map_err(|e| CelersError::Other(format!("Failed to parse JSON: {}", e)))?;

        let mut imported = 0u64;

        for task in tasks {
            let id = task["id"]
                .as_str()
                .ok_or_else(|| CelersError::Other("Missing task id".to_string()))?;

            // Check if task already exists
            if skip_existing {
                let exists_rows = self
                    .connection()
                    .query(
                        "SELECT COUNT(*) AS c FROM celers_tasks WHERE id = ?",
                        &[&id],
                    )
                    .await
                    .map_err(|e| {
                        CelersError::Other(format!("Failed to check task existence: {}", e))
                    })?;
                let exists: i64 = exists_rows
                    .first()
                    .map(|r| r.col("c"))
                    .transpose()
                    .map_err(|e| {
                        CelersError::Other(format!("Failed to check task existence: {e}"))
                    })?
                    .unwrap_or(0);

                if exists > 0 {
                    tracing::debug!(task_id = id, "Skipping existing task");
                    continue;
                }
            }

            // Insert task
            let payload: Vec<u8> = task["payload"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_u64().map(|n| n as u8))
                        .collect::<Vec<u8>>()
                })
                .unwrap_or_default();
            let task_name = task["task_name"].as_str().unwrap_or("");
            let state = task["state"].as_str().unwrap_or("pending");
            let priority = task["priority"].as_i64().unwrap_or(0) as i32;
            let retry_count = task["retry_count"].as_i64().unwrap_or(0) as i32;
            let max_retries = task["max_retries"].as_i64().unwrap_or(3) as i32;
            // MySQL DATETIME/TIMESTAMP parameter convention — see
            // `row_ext.rs`'s "DateTime<Utc> parameter convention (MySQL)"
            // section. Source JSON stores RFC3339 (from a prior
            // `export_tasks` call, or hand-authored); parse then reformat.
            let created_at = task["created_at"]
                .as_str()
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(Utc::now)
                .format("%Y-%m-%d %H:%M:%S%.6f")
                .to_string();
            let scheduled_at = task["scheduled_at"]
                .as_str()
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(Utc::now)
                .format("%Y-%m-%d %H:%M:%S%.6f")
                .to_string();
            let started_at = task["started_at"]
                .as_str()
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| {
                    dt.with_timezone(&Utc)
                        .format("%Y-%m-%d %H:%M:%S%.6f")
                        .to_string()
                });
            let completed_at = task["completed_at"]
                .as_str()
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| {
                    dt.with_timezone(&Utc)
                        .format("%Y-%m-%d %H:%M:%S%.6f")
                        .to_string()
                });
            let worker_id = task["worker_id"].as_str();
            let error_message = task["error_message"].as_str();
            let metadata = task["metadata"].as_str().unwrap_or("{}");

            let result = self
                .connection()
                .execute(
                    r#"
                    INSERT INTO celers_tasks
                        (id, queue_name, task_name, payload, state, priority, retry_count, max_retries,
                         created_at, scheduled_at, started_at, completed_at, worker_id, error_message, metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    "#,
                    &[
                        &id,
                        &self.queue_name(),
                        &task_name,
                        &payload,
                        &state,
                        &priority,
                        &retry_count,
                        &max_retries,
                        &created_at,
                        &scheduled_at,
                        &started_at,
                        &completed_at,
                        &worker_id,
                        &error_message,
                        &metadata,
                    ],
                )
                .await;

            match result {
                Ok(_) => {
                    imported += 1;
                    tracing::debug!(task_id = id, "Imported task");
                }
                Err(e) => {
                    if !skip_existing {
                        return Err(CelersError::Other(format!(
                            "Failed to import task {}: {}",
                            id, e
                        )));
                    }
                    tracing::warn!(task_id = id, error = %e, "Failed to import task, skipping");
                }
            }
        }

        tracing::info!(imported = imported, "Task import completed");
        Ok(imported)
    }

    /// Export DLQ entries to JSON format
    ///
    /// Exports dead letter queue entries for backup or analysis.
    ///
    /// # Arguments
    /// * `limit` - Maximum number of entries to export (None exports all)
    ///
    /// # Returns
    /// JSON string containing DLQ data
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    ///
    /// // Export DLQ for analysis
    /// let json = broker.export_dlq(Some(100)).await?;
    /// std::fs::write("dlq_entries.json", json)?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn export_dlq(&self, limit: Option<i64>) -> Result<String> {
        let mut query = String::from(
            r#"
            SELECT id, task_id, task_name, payload, retry_count, error_message, failed_at, metadata
            FROM celers_dead_letter_queue
            ORDER BY failed_at DESC
            "#,
        );

        if limit.is_some() {
            query.push_str(" LIMIT ?");
        }

        let mut param_refs: Vec<&dyn oxisql_core::ToSqlValue> = Vec::new();
        if let Some(l) = &limit {
            param_refs.push(l);
        }

        let rows = self
            .connection()
            .query(&query, &param_refs)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to export DLQ: {}", e)))?;

        let mut dlq_entries = Vec::new();
        for row in rows {
            let entry = serde_json::json!({
                "id": row.col::<String>("id").map_err(|e| CelersError::Other(format!("Failed to export DLQ: {e}")))?,
                "task_id": row.col::<String>("task_id").map_err(|e| CelersError::Other(format!("Failed to export DLQ: {e}")))?,
                "task_name": row.col::<String>("task_name").map_err(|e| CelersError::Other(format!("Failed to export DLQ: {e}")))?,
                "payload": row.col::<Vec<u8>>("payload").map_err(|e| CelersError::Other(format!("Failed to export DLQ: {e}")))?,
                "retry_count": row.col::<i32>("retry_count").map_err(|e| CelersError::Other(format!("Failed to export DLQ: {e}")))?,
                "error_message": row.col::<Option<String>>("error_message").map_err(|e| CelersError::Other(format!("Failed to export DLQ: {e}")))?,
                "failed_at": row.col::<DateTime<Utc>>("failed_at").map_err(|e| CelersError::Other(format!("Failed to export DLQ: {e}")))?,
                "metadata": row.col::<String>("metadata").map_err(|e| CelersError::Other(format!("Failed to export DLQ: {e}")))?,
            });
            dlq_entries.push(entry);
        }

        serde_json::to_string_pretty(&dlq_entries)
            .map_err(|e| CelersError::Other(format!("Failed to serialize DLQ: {}", e)))
    }

    /// Get circuit breaker statistics
    ///
    /// Returns the current state and statistics of the circuit breaker.
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    ///
    /// let stats = broker.get_circuit_breaker_stats();
    /// println!("Circuit breaker state: {:?}", stats.state);
    /// println!("Failures: {}, Successes: {}", stats.failure_count, stats.success_count);
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_circuit_breaker_stats(&self) -> CircuitBreakerStats {
        let cb = self
            .circuit_breaker
            .read()
            .expect("lock should not be poisoned");
        CircuitBreakerStats {
            state: cb.state,
            failure_count: cb.failure_count,
            success_count: cb.success_count,
            last_failure_time: cb.last_failure_time,
            last_state_change: cb.last_state_change,
        }
    }

    /// Reset the circuit breaker to closed state
    ///
    /// Manually resets the circuit breaker, clearing all failure counts and
    /// transitioning to the Closed state.
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    ///
    /// // Manually reset after fixing issues
    /// broker.reset_circuit_breaker();
    /// println!("Circuit breaker reset to closed state");
    /// # Ok(())
    /// # }
    /// ```
    pub fn reset_circuit_breaker(&self) {
        let mut cb = self
            .circuit_breaker
            .write()
            .expect("lock should not be poisoned");
        cb.state = CircuitBreakerState::Closed;
        cb.failure_count = 0;
        cb.success_count = 0;
        cb.last_failure_time = None;
        cb.last_state_change = Utc::now();
        tracing::info!("Circuit breaker manually reset to Closed state");
    }

    /// Record a successful operation
    ///
    /// This is called internally after successful database operations.
    /// In HalfOpen state, enough successes will close the circuit.
    fn record_success(&self) {
        let mut cb = self
            .circuit_breaker
            .write()
            .expect("lock should not be poisoned");

        match cb.state {
            CircuitBreakerState::HalfOpen => {
                cb.success_count += 1;
                if cb.success_count >= cb.config.success_threshold {
                    cb.state = CircuitBreakerState::Closed;
                    cb.failure_count = 0;
                    cb.success_count = 0;
                    cb.last_state_change = Utc::now();
                    tracing::info!(
                        "Circuit breaker transitioned to Closed after successful recovery"
                    );
                }
            }
            CircuitBreakerState::Closed => {
                // Reset failure count on success in closed state
                cb.failure_count = 0;
            }
            CircuitBreakerState::Open => {
                // Ignore successes in open state
            }
        }
    }

    /// Record a failed operation
    ///
    /// This is called internally after failed database operations.
    /// Enough failures will open the circuit.
    pub(crate) fn record_failure(&self) {
        let mut cb = self
            .circuit_breaker
            .write()
            .expect("lock should not be poisoned");
        cb.failure_count += 1;
        cb.last_failure_time = Some(Utc::now());

        match cb.state {
            CircuitBreakerState::Closed => {
                if cb.failure_count >= cb.config.failure_threshold {
                    cb.state = CircuitBreakerState::Open;
                    cb.last_state_change = Utc::now();
                    tracing::warn!(
                        failure_count = cb.failure_count,
                        "Circuit breaker opened due to consecutive failures"
                    );
                }
            }
            CircuitBreakerState::HalfOpen => {
                // Any failure in half-open immediately opens the circuit
                cb.state = CircuitBreakerState::Open;
                cb.success_count = 0;
                cb.last_state_change = Utc::now();
                tracing::warn!("Circuit breaker reopened after failure in HalfOpen state");
            }
            CircuitBreakerState::Open => {
                // Already open, just increment counter
            }
        }
    }

    /// Check if the circuit breaker allows the operation
    ///
    /// Returns Ok(()) if operation is allowed, Err if circuit is open.
    /// Automatically transitions from Open to HalfOpen after timeout.
    fn check_circuit(&self) -> Result<()> {
        let mut cb = self
            .circuit_breaker
            .write()
            .expect("lock should not be poisoned");

        match cb.state {
            CircuitBreakerState::Closed | CircuitBreakerState::HalfOpen => Ok(()),
            CircuitBreakerState::Open => {
                // Check if timeout has elapsed
                let elapsed = Utc::now()
                    .signed_duration_since(cb.last_state_change)
                    .num_seconds();

                if elapsed >= cb.config.timeout_secs as i64 {
                    // Transition to half-open
                    cb.state = CircuitBreakerState::HalfOpen;
                    cb.success_count = 0;
                    cb.last_state_change = Utc::now();
                    tracing::info!("Circuit breaker transitioned to HalfOpen, testing recovery");
                    Ok(())
                } else {
                    Err(CelersError::Other(format!(
                        "Circuit breaker is open (will retry in {} seconds)",
                        cb.config.timeout_secs as i64 - elapsed
                    )))
                }
            }
        }
    }

    /// Execute a database operation with circuit breaker protection
    ///
    /// Wraps a database operation with circuit breaker logic.
    /// If the circuit is open, the operation is rejected.
    /// Successful/failed operations update the circuit breaker state.
    ///
    /// # Arguments
    /// * `operation` - Async function to execute
    ///
    /// # Returns
    /// Result of the operation or circuit breaker error
    pub async fn with_circuit_breaker<F, T, Fut>(&self, operation: F) -> Result<T>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        // Check if circuit allows operation
        self.check_circuit()?;

        // Execute operation
        match operation().await {
            Ok(result) => {
                self.record_success();
                Ok(result)
            }
            Err(e) => {
                self.record_failure();
                Err(e)
            }
        }
    }

    /// Enqueue a task with idempotency key for duplicate prevention
    ///
    /// This method ensures that tasks with the same idempotency key are only
    /// enqueued once within the configured TTL window. If a task with the same
    /// key already exists, it returns the existing task ID instead of creating
    /// a new task.
    ///
    /// Critical for preventing duplicate operations in distributed systems:
    /// - Financial transactions (same payment ID)
    /// - Email/notification sending (same message ID)
    /// - External API calls (same request ID)
    ///
    /// # Arguments
    /// * `task` - Task to enqueue
    /// * `idempotency_key` - Unique key for deduplication (e.g., request ID)
    /// * `ttl_secs` - Time-to-live for the idempotency record (in seconds)
    /// * `metadata` - Optional metadata for the idempotency record
    ///
    /// # Returns
    /// Task ID (either newly created or existing)
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # use celers_core::SerializedTask;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    ///
    /// let task = SerializedTask::new("send_payment".to_string(), vec![1, 2, 3]);
    /// let task_id = broker.enqueue_with_idempotency(
    ///     task,
    ///     "payment-12345",
    ///     86400, // 24 hour TTL
    ///     None
    /// ).await?;
    ///
    /// // Attempting to enqueue again with same key returns the same task_id
    /// let task2 = SerializedTask::new("send_payment".to_string(), vec![1, 2, 3]);
    /// let task_id2 = broker.enqueue_with_idempotency(
    ///     task2,
    ///     "payment-12345",
    ///     86400,
    ///     None
    /// ).await?;
    /// assert_eq!(task_id, task_id2); // Same task ID!
    /// # Ok(())
    /// # }
    /// ```
    pub async fn enqueue_with_idempotency(
        &self,
        task: SerializedTask,
        idempotency_key: &str,
        ttl_secs: u64,
        metadata: Option<serde_json::Value>,
    ) -> Result<TaskId> {
        // First, check if an idempotency record already exists for this key
        let existing_rows = self
            .connection()
            .query(
                r#"
                SELECT task_id
                FROM celers_task_idempotency
                WHERE idempotency_key = ?
                  AND task_name = ?
                  AND expires_at > NOW()
                "#,
                &[&idempotency_key, &task.metadata.name],
            )
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to check idempotency record: {}", e))
            })?;

        if let Some(row) = existing_rows.into_iter().next() {
            let task_id_str: String = row.col("task_id").map_err(|e| {
                CelersError::Other(format!("Failed to check idempotency record: {e}"))
            })?;
            // Idempotency record exists, return existing task ID
            let task_id = Uuid::parse_str(&task_id_str)
                .map_err(|e| CelersError::Other(format!("Invalid task UUID: {}", e)))?;

            tracing::debug!(
                idempotency_key = %idempotency_key,
                task_id = %task_id,
                "Duplicate task detected, returning existing task ID"
            );

            return Ok(task_id);
        }

        // No existing record, create new task
        let task_id = Uuid::new_v4();
        let idempotency_id = Uuid::new_v4();

        // This path mints its own row id rather than using `task.metadata.id`,
        // so the metadata document must record the id actually stored —
        // otherwise the dequeue side would rebuild the task around a
        // different id than the row it came from.
        let mut stored_task = task.clone();
        stored_task.metadata.id = task_id;
        let idempotent_metadata_str = self.build_task_metadata_document(
            &stored_task,
            json!({ "idempotency_key": idempotency_key }),
        )?;

        // Begin transaction to ensure atomicity
        let mut tx = self
            .connection()
            .transaction()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        // Insert the task
        tx.execute(
            r#"
            INSERT INTO celers_tasks
                (id, queue_name, task_name, payload, state, priority, max_retries, metadata, created_at, scheduled_at)
            VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, NOW(), NOW())
            "#,
            &[
                &task_id.to_string(),
                &self.queue_name(),
                &task.metadata.name,
                &task.payload,
                &task.metadata.priority,
                &(task.metadata.max_retries as i32),
                &idempotent_metadata_str,
            ],
        )
        .await
        .map_err(|e| CelersError::Other(format!("Failed to enqueue task: {}", e)))?;

        // Insert idempotency record
        let metadata_str = metadata
            .map(|m| {
                serde_json::to_string(&m).map_err(|e| {
                    CelersError::Serialization(format!(
                        "Failed to serialize idempotency metadata: {e}"
                    ))
                })
            })
            .transpose()?;
        tx.execute(
            r#"
            INSERT INTO celers_task_idempotency
                (id, idempotency_key, task_name, task_id, created_at, expires_at, metadata)
            VALUES (?, ?, ?, ?, NOW(), DATE_ADD(NOW(), INTERVAL ? SECOND), ?)
            "#,
            &[
                &idempotency_id.to_string(),
                &idempotency_key,
                &task.metadata.name,
                &task_id.to_string(),
                &(ttl_secs as i64),
                &metadata_str,
            ],
        )
        .await
        .map_err(|e| CelersError::Other(format!("Failed to insert idempotency record: {}", e)))?;

        // Commit transaction
        tx.commit()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to commit transaction: {}", e)))?;

        tracing::info!(
            task_id = %task_id,
            idempotency_key = %idempotency_key,
            ttl_secs = ttl_secs,
            "Task enqueued with idempotency key"
        );

        #[cfg(feature = "metrics")]
        {
            TASKS_ENQUEUED_TOTAL.inc();
            TASKS_ENQUEUED_BY_TYPE
                .with_label_values(&[&task.metadata.name])
                .inc();
        }

        Ok(task_id)
    }

    /// Get an idempotency record by key and task name
    ///
    /// # Arguments
    /// * `idempotency_key` - The idempotency key to look up
    /// * `task_name` - The task name to scope the lookup
    ///
    /// # Returns
    /// The idempotency record if found
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    ///
    /// if let Some(record) = broker.get_idempotency_record("payment-12345", "send_payment").await? {
    ///     println!("Found existing task: {}", record.task_id);
    ///     println!("Expires at: {}", record.expires_at);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[allow(clippy::type_complexity)]
    pub async fn get_idempotency_record(
        &self,
        idempotency_key: &str,
        task_name: &str,
    ) -> Result<Option<IdempotencyRecord>> {
        let rows = self
            .connection()
            .query(
                r#"
                SELECT id, idempotency_key, task_name, task_id, created_at, expires_at, metadata
                FROM celers_task_idempotency
                WHERE idempotency_key = ?
                  AND task_name = ?
                ORDER BY created_at DESC
                LIMIT 1
                "#,
                &[&idempotency_key, &task_name],
            )
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to fetch idempotency record: {}", e))
            })?;

        if let Some(row) = rows.into_iter().next() {
            let id: String = row.col("id").map_err(|e| {
                CelersError::Other(format!("Failed to fetch idempotency record: {e}"))
            })?;
            let key: String = row.col("idempotency_key").map_err(|e| {
                CelersError::Other(format!("Failed to fetch idempotency record: {e}"))
            })?;
            let task_name: String = row.col("task_name").map_err(|e| {
                CelersError::Other(format!("Failed to fetch idempotency record: {e}"))
            })?;
            let task_id: String = row.col("task_id").map_err(|e| {
                CelersError::Other(format!("Failed to fetch idempotency record: {e}"))
            })?;
            let created_at: DateTime<Utc> = row.col("created_at").map_err(|e| {
                CelersError::Other(format!("Failed to fetch idempotency record: {e}"))
            })?;
            let expires_at: DateTime<Utc> = row.col("expires_at").map_err(|e| {
                CelersError::Other(format!("Failed to fetch idempotency record: {e}"))
            })?;
            let metadata: Option<String> = row.col("metadata").map_err(|e| {
                CelersError::Other(format!("Failed to fetch idempotency record: {e}"))
            })?;

            Ok(Some(IdempotencyRecord {
                id: Uuid::parse_str(&id)
                    .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?,
                idempotency_key: key,
                task_name,
                task_id: Uuid::parse_str(&task_id)
                    .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?,
                created_at,
                expires_at,
                metadata: metadata.and_then(|m| serde_json::from_str(&m).ok()),
            }))
        } else {
            Ok(None)
        }
    }

    /// Cleanup expired idempotency keys
    ///
    /// Removes idempotency records that have exceeded their TTL.
    /// Should be called periodically to prevent unbounded table growth.
    ///
    /// # Returns
    /// Number of records deleted
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    ///
    /// let deleted = broker.cleanup_expired_idempotency_keys().await?;
    /// println!("Cleaned up {} expired idempotency keys", deleted);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn cleanup_expired_idempotency_keys(&self) -> Result<u64> {
        let deleted = self
            .connection()
            .execute(
                r#"
                DELETE FROM celers_task_idempotency
                WHERE expires_at <= NOW()
                "#,
                &[],
            )
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to cleanup expired idempotency keys: {}", e))
            })?;

        if deleted > 0 {
            tracing::info!(count = deleted, "Cleaned up expired idempotency keys");
        }

        Ok(deleted)
    }

    /// Get idempotency statistics by task name
    ///
    /// Returns statistics about idempotency key usage for each task type.
    ///
    /// # Returns
    /// Vector of idempotency statistics per task type
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    ///
    /// let stats = broker.get_idempotency_statistics().await?;
    /// for stat in stats {
    ///     println!("{}: {} total keys ({} active, {} expired)",
    ///         stat.task_name, stat.total_keys, stat.active_keys, stat.expired_keys);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[allow(clippy::type_complexity)]
    pub async fn get_idempotency_statistics(&self) -> Result<Vec<IdempotencyStats>> {
        let rows = self
            .connection()
            .query(
                r#"
                SELECT
                    task_name,
                    COUNT(*) as total_keys,
                    COUNT(DISTINCT idempotency_key) as unique_keys,
                    SUM(CASE WHEN expires_at > NOW() THEN 1 ELSE 0 END) as active_keys,
                    SUM(CASE WHEN expires_at <= NOW() THEN 1 ELSE 0 END) as expired_keys,
                    MIN(created_at) as oldest_key,
                    MAX(created_at) as newest_key
                FROM celers_task_idempotency
                GROUP BY task_name
                ORDER BY task_name
                "#,
                &[],
            )
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to fetch idempotency statistics: {}", e))
            })?;

        rows.into_iter()
            .map(|row| {
                let task_name: String = row.col("task_name").map_err(|e| {
                    CelersError::Other(format!("Failed to fetch idempotency statistics: {e}"))
                })?;
                let total_keys: i64 = row.col("total_keys").map_err(|e| {
                    CelersError::Other(format!("Failed to fetch idempotency statistics: {e}"))
                })?;
                let unique_keys: i64 = row.col("unique_keys").map_err(|e| {
                    CelersError::Other(format!("Failed to fetch idempotency statistics: {e}"))
                })?;
                // Plain COUNT(*)/SUM(0-or-1) aggregates are I64, not
                // DECIMAL, on MySQL — read directly as Option<i64> rather
                // than through the Decimal-text-parse path used for AVG().
                let active_keys: Option<i64> = row.col("active_keys").map_err(|e| {
                    CelersError::Other(format!("Failed to fetch idempotency statistics: {e}"))
                })?;
                let expired_keys: Option<i64> = row.col("expired_keys").map_err(|e| {
                    CelersError::Other(format!("Failed to fetch idempotency statistics: {e}"))
                })?;
                let oldest_key: Option<DateTime<Utc>> = row.col("oldest_key").map_err(|e| {
                    CelersError::Other(format!("Failed to fetch idempotency statistics: {e}"))
                })?;
                let newest_key: Option<DateTime<Utc>> = row.col("newest_key").map_err(|e| {
                    CelersError::Other(format!("Failed to fetch idempotency statistics: {e}"))
                })?;

                Ok(IdempotencyStats {
                    task_name,
                    total_keys,
                    unique_keys,
                    active_keys: active_keys.unwrap_or(0),
                    expired_keys: expired_keys.unwrap_or(0),
                    oldest_key,
                    newest_key,
                })
            })
            .collect()
    }
}
