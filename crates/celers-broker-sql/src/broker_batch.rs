//! Batch operations, worker management, and task groups
//!
//! Batch result storage, drain mode, worker heartbeats,
//! task grouping, connection health, and DLQ replay operations.

use crate::broker_core::MysqlBroker;
use crate::types::*;
use celers_core::{CelersError, Result, SerializedTask, TaskId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

/// Slow query information from performance_schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlowQueryInfo {
    pub query_text: String,
    pub execution_count: i64,
    pub avg_time_ms: f64,
    pub max_time_ms: f64,
    pub total_time_ms: f64,
}

/// Worker heartbeat information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerHeartbeat {
    pub worker_id: String,
    pub last_heartbeat: DateTime<Utc>,
    pub status: WorkerStatus,
    pub task_count: i64,
    pub capabilities: Option<serde_json::Value>,
}

/// Worker status
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WorkerStatus {
    Active,
    Idle,
    Busy,
    Offline,
}

impl std::fmt::Display for WorkerStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkerStatus::Active => write!(f, "active"),
            WorkerStatus::Idle => write!(f, "idle"),
            WorkerStatus::Busy => write!(f, "busy"),
            WorkerStatus::Offline => write!(f, "offline"),
        }
    }
}

/// Task group information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskGroup {
    pub group_id: String,
    pub task_ids: Vec<Uuid>,
    pub created_at: DateTime<Utc>,
    pub metadata: Option<serde_json::Value>,
}

/// Task group status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskGroupStatus {
    pub group_id: String,
    pub total_tasks: i64,
    pub pending_tasks: i64,
    pub processing_tasks: i64,
    pub completed_tasks: i64,
    pub failed_tasks: i64,
    pub cancelled_tasks: i64,
}

/// Batch result storage input
#[derive(Debug, Clone)]
pub struct BatchResultInput {
    pub task_id: Uuid,
    pub task_name: String,
    pub status: TaskResultStatus,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
    pub traceback: Option<String>,
    pub runtime_ms: Option<i64>,
}

impl MysqlBroker {
    /// Store multiple task results in a single transaction for efficiency
    ///
    /// # Arguments
    ///
    /// * `results` - Vector of result inputs to store
    ///
    /// # Returns
    ///
    /// Number of results successfully stored
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use celers_broker_sql::{MysqlBroker, BatchResultInput, TaskResultStatus};
    /// # use uuid::Uuid;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = MysqlBroker::new("mysql://localhost/test").await?;
    /// let results = vec![
    ///     BatchResultInput {
    ///         task_id: Uuid::new_v4(),
    ///         task_name: "task1".to_string(),
    ///         status: TaskResultStatus::Success,
    ///         result: Some(serde_json::json!({"value": 42})),
    ///         error: None,
    ///         traceback: None,
    ///         runtime_ms: Some(100),
    ///     },
    ///     BatchResultInput {
    ///         task_id: Uuid::new_v4(),
    ///         task_name: "task2".to_string(),
    ///         status: TaskResultStatus::Success,
    ///         result: Some(serde_json::json!({"value": 24})),
    ///         error: None,
    ///         traceback: None,
    ///         runtime_ms: Some(150),
    ///     },
    /// ];
    ///
    /// let stored = broker.store_result_batch(&results).await?;
    /// println!("Stored {} results", stored);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn store_result_batch(&self, results: &[BatchResultInput]) -> Result<u64> {
        if results.is_empty() {
            return Ok(0);
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        let mut stored = 0u64;
        for result in results {
            let result_json = result
                .result
                .as_ref()
                .map(|v| serde_json::to_string(v).unwrap_or_else(|_| "null".to_string()))
                .unwrap_or_else(|| "null".to_string());

            let rows_affected = sqlx::query(
                r#"
                INSERT INTO celers_task_results
                    (task_id, task_name, status, result, error, traceback, runtime_ms, created_at, completed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, NOW(), NOW())
                ON DUPLICATE KEY UPDATE
                    task_name = VALUES(task_name),
                    status = VALUES(status),
                    result = VALUES(result),
                    error = VALUES(error),
                    traceback = VALUES(traceback),
                    runtime_ms = VALUES(runtime_ms),
                    completed_at = NOW()
                "#,
            )
            .bind(result.task_id.to_string())
            .bind(&result.task_name)
            .bind(result.status.to_string())
            .bind(result_json)
            .bind(&result.error)
            .bind(&result.traceback)
            .bind(result.runtime_ms)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to store result for task {}: {}", result.task_id, e))
            })?
            .rows_affected();

            stored += rows_affected;
        }

        tx.commit()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to commit transaction: {}", e)))?;

        Ok(stored)
    }

    /// Get multiple task results in a single query
    ///
    /// # Arguments
    ///
    /// * `task_ids` - Vector of task IDs to retrieve results for
    ///
    /// # Returns
    ///
    /// Vector of task results found
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # use uuid::Uuid;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = MysqlBroker::new("mysql://localhost/test").await?;
    /// let task_ids = vec![Uuid::new_v4(), Uuid::new_v4()];
    /// let results = broker.get_result_batch(&task_ids).await?;
    /// println!("Retrieved {} results", results.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_result_batch(&self, task_ids: &[Uuid]) -> Result<Vec<TaskResult>> {
        if task_ids.is_empty() {
            return Ok(Vec::new());
        }

        let placeholders = task_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let query_str = format!(
            r#"
            SELECT task_id, task_name, status, result, error, traceback, created_at, completed_at, runtime_ms
            FROM celers_task_results
            WHERE task_id IN ({})
            "#,
            placeholders
        );

        let mut query = sqlx::query_as::<
            _,
            (
                String,
                String,
                String,
                String,
                Option<String>,
                Option<String>,
                DateTime<Utc>,
                Option<DateTime<Utc>>,
                Option<i64>,
            ),
        >(&query_str);
        for task_id in task_ids {
            query = query.bind(task_id.to_string());
        }

        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to fetch results: {}", e)))?;

        rows.into_iter()
            .map(
                |(
                    task_id,
                    task_name,
                    status,
                    result,
                    error,
                    traceback,
                    created_at,
                    completed_at,
                    runtime_ms,
                )| {
                    Ok(TaskResult {
                        task_id: Uuid::parse_str(&task_id)
                            .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?,
                        task_name,
                        status: status.parse()?,
                        result: serde_json::from_str(&result).ok(),
                        error,
                        traceback,
                        created_at,
                        completed_at,
                        runtime_ms,
                    })
                },
            )
            .collect()
    }

    /// Enable drain mode - prevents new tasks from being enqueued while allowing processing of existing tasks
    ///
    /// This is useful for graceful shutdown scenarios where you want to:
    /// 1. Stop accepting new work
    /// 2. Allow workers to finish current tasks
    /// 3. Drain the queue before shutting down
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = MysqlBroker::new("mysql://localhost/test").await?;
    /// // Enable drain mode
    /// broker.enable_drain_mode().await?;
    ///
    /// // Check drain status
    /// let is_draining = broker.is_drain_mode().await?;
    /// println!("Drain mode: {}", is_draining);
    ///
    /// // Disable drain mode when ready
    /// broker.disable_drain_mode().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn enable_drain_mode(&self) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO celers_queue_config (queue_name, config_key, config_value, updated_at)
            VALUES (?, 'drain_mode', 'true', NOW())
            ON DUPLICATE KEY UPDATE config_value = 'true', updated_at = NOW()
            "#,
        )
        .bind(&self.queue_name)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to enable drain mode: {}", e)))?;

        Ok(())
    }

    /// Disable drain mode - allows new tasks to be enqueued again
    pub async fn disable_drain_mode(&self) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO celers_queue_config (queue_name, config_key, config_value, updated_at)
            VALUES (?, 'drain_mode', 'false', NOW())
            ON DUPLICATE KEY UPDATE config_value = 'false', updated_at = NOW()
            "#,
        )
        .bind(&self.queue_name)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to disable drain mode: {}", e)))?;

        Ok(())
    }

    /// Check if drain mode is enabled
    pub async fn is_drain_mode(&self) -> Result<bool> {
        let row: Option<(String,)> = sqlx::query_as(
            r#"
            SELECT config_value
            FROM celers_queue_config
            WHERE queue_name = ? AND config_key = 'drain_mode'
            "#,
        )
        .bind(&self.queue_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to check drain mode: {}", e)))?;

        Ok(row.map(|(val,)| val == "true").unwrap_or(false))
    }

    /// Register a worker and update its heartbeat
    ///
    /// # Arguments
    ///
    /// * `worker_id` - Unique identifier for the worker
    /// * `status` - Current worker status
    /// * `capabilities` - Optional JSON object describing worker capabilities
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use celers_broker_sql::{MysqlBroker, WorkerStatus};
    /// # use serde_json::json;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = MysqlBroker::new("mysql://localhost/test").await?;
    /// broker.register_worker(
    ///     "worker-1",
    ///     WorkerStatus::Active,
    ///     Some(json!({"cpu_cores": 4, "memory_gb": 8}))
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn register_worker(
        &self,
        worker_id: &str,
        status: WorkerStatus,
        capabilities: Option<serde_json::Value>,
    ) -> Result<()> {
        let capabilities_json = capabilities
            .as_ref()
            .map(|v| serde_json::to_string(v).unwrap_or_else(|_| "null".to_string()))
            .unwrap_or_else(|| "null".to_string());

        sqlx::query(
            r#"
            INSERT INTO celers_worker_heartbeat
                (worker_id, queue_name, last_heartbeat, status, capabilities, task_count, updated_at)
            VALUES (?, ?, NOW(), ?, ?, 0, NOW())
            ON DUPLICATE KEY UPDATE
                last_heartbeat = NOW(),
                status = VALUES(status),
                capabilities = VALUES(capabilities),
                updated_at = NOW()
            "#,
        )
        .bind(worker_id)
        .bind(&self.queue_name)
        .bind(status.to_string())
        .bind(capabilities_json)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            CelersError::Other(format!("Failed to register worker: {}", e))
        })?;

        Ok(())
    }

    /// Update worker heartbeat to indicate it's still alive
    pub async fn update_worker_heartbeat(
        &self,
        worker_id: &str,
        status: WorkerStatus,
    ) -> Result<()> {
        let rows_affected = sqlx::query(
            r#"
            UPDATE celers_worker_heartbeat
            SET last_heartbeat = NOW(), status = ?, updated_at = NOW()
            WHERE worker_id = ? AND queue_name = ?
            "#,
        )
        .bind(status.to_string())
        .bind(worker_id)
        .bind(&self.queue_name)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to update worker heartbeat: {}", e)))?
        .rows_affected();

        if rows_affected == 0 {
            return Err(CelersError::Other(format!(
                "Worker {} not found",
                worker_id
            )));
        }

        Ok(())
    }

    /// Get heartbeat information for all workers
    ///
    /// # Arguments
    ///
    /// * `stale_threshold_secs` - Seconds after which a worker is considered stale/offline
    ///
    /// # Returns
    ///
    /// Vector of worker heartbeat information
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = MysqlBroker::new("mysql://localhost/test").await?;
    /// let workers = broker.get_all_worker_heartbeats(60).await?;
    /// for worker in workers {
    ///     println!("Worker {} status: {}", worker.worker_id, worker.status);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_all_worker_heartbeats(
        &self,
        stale_threshold_secs: i64,
    ) -> Result<Vec<WorkerHeartbeat>> {
        let rows: Vec<(String, DateTime<Utc>, String, i64, String)> = sqlx::query_as(
            r#"
            SELECT
                worker_id,
                last_heartbeat,
                CASE
                    WHEN TIMESTAMPDIFF(SECOND, last_heartbeat, NOW()) > ? THEN 'offline'
                    ELSE status
                END as status,
                task_count,
                COALESCE(capabilities, 'null') as capabilities
            FROM celers_worker_heartbeat
            WHERE queue_name = ?
            ORDER BY last_heartbeat DESC
            "#,
        )
        .bind(stale_threshold_secs)
        .bind(&self.queue_name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to fetch worker heartbeats: {}", e)))?;

        rows.into_iter()
            .map(
                |(worker_id, last_heartbeat, status, task_count, capabilities)| {
                    let status = match status.as_str() {
                        "active" => WorkerStatus::Active,
                        "idle" => WorkerStatus::Idle,
                        "busy" => WorkerStatus::Busy,
                        _ => WorkerStatus::Offline,
                    };
                    let capabilities = serde_json::from_str(&capabilities).ok();
                    Ok(WorkerHeartbeat {
                        worker_id,
                        last_heartbeat,
                        status,
                        task_count,
                        capabilities,
                    })
                },
            )
            .collect()
    }

    /// Enqueue a group of related tasks
    ///
    /// # Arguments
    ///
    /// * `group_id` - Unique identifier for the task group
    /// * `tasks` - Vector of tasks to enqueue
    /// * `metadata` - Optional metadata for the group
    ///
    /// # Returns
    ///
    /// Vector of task IDs that were enqueued
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # use celers_core::SerializedTask;
    /// # use serde_json::json;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = MysqlBroker::new("mysql://localhost/test").await?;
    /// # let task1 = SerializedTask::new("test".to_string(), vec![])
    /// #     .with_priority(0)
    /// #     .with_max_retries(3);
    /// # let task2 = SerializedTask::new("test".to_string(), vec![])
    /// #     .with_priority(0)
    /// #     .with_max_retries(3);
    /// let task_ids = broker.enqueue_group(
    ///     "batch-123",
    ///     vec![task1, task2],
    ///     Some(json!({"batch_type": "data_processing"}))
    /// ).await?;
    /// println!("Enqueued group with {} tasks", task_ids.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn enqueue_group(
        &self,
        group_id: &str,
        tasks: Vec<SerializedTask>,
        metadata: Option<serde_json::Value>,
    ) -> Result<Vec<TaskId>> {
        if tasks.is_empty() {
            return Ok(Vec::new());
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        let mut task_ids = Vec::new();

        for task in tasks {
            let task_id = Uuid::new_v4();

            sqlx::query(
                r#"
                INSERT INTO celers_tasks
                    (id, task_name, payload, state, priority, retry_count, max_retries, created_at, scheduled_at, metadata)
                VALUES (?, ?, ?, 'pending', ?, 0, ?, NOW(), NOW(), ?)
                "#,
            )
            .bind(task_id.to_string())
            .bind(&task.metadata.name)
            .bind(&task.payload)
            .bind(task.metadata.priority)
            .bind(task.metadata.max_retries as i32)
            .bind(serde_json::to_string(&json!({"group_id": group_id})).unwrap_or_else(|_| "{}".to_string()))
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to insert task: {}", e))
            })?;

            task_ids.push(task_id);
        }

        // Store group metadata
        let metadata_json = metadata
            .as_ref()
            .map(|v| serde_json::to_string(v).unwrap_or_else(|_| "null".to_string()))
            .unwrap_or_else(|| "null".to_string());

        sqlx::query(
            r#"
            INSERT INTO celers_task_groups
                (group_id, queue_name, task_count, created_at, metadata)
            VALUES (?, ?, ?, NOW(), ?)
            "#,
        )
        .bind(group_id)
        .bind(&self.queue_name)
        .bind(task_ids.len() as i64)
        .bind(metadata_json)
        .execute(&mut *tx)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to insert task group: {}", e)))?;

        tx.commit()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to commit transaction: {}", e)))?;

        Ok(task_ids)
    }

    /// Get status of a task group
    ///
    /// # Arguments
    ///
    /// * `group_id` - The task group identifier
    ///
    /// # Returns
    ///
    /// Task group status with counts by state
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = MysqlBroker::new("mysql://localhost/test").await?;
    /// let status = broker.get_group_status("batch-123").await?;
    /// println!("Group has {} completed tasks out of {} total",
    ///     status.completed_tasks, status.total_tasks);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_group_status(&self, group_id: &str) -> Result<TaskGroupStatus> {
        let row: (i64, i64, i64, i64, i64, i64) = sqlx::query_as(
            r#"
            SELECT
                COUNT(*) as total_tasks,
                SUM(CASE WHEN state = 'pending' THEN 1 ELSE 0 END) as pending_tasks,
                SUM(CASE WHEN state = 'processing' THEN 1 ELSE 0 END) as processing_tasks,
                SUM(CASE WHEN state = 'completed' THEN 1 ELSE 0 END) as completed_tasks,
                SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END) as failed_tasks,
                SUM(CASE WHEN state = 'cancelled' THEN 1 ELSE 0 END) as cancelled_tasks
            FROM celers_tasks
            WHERE JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.group_id')) = ?
            "#,
        )
        .bind(group_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to fetch group status: {}", e)))?;

        Ok(TaskGroupStatus {
            group_id: group_id.to_string(),
            total_tasks: row.0,
            pending_tasks: row.1,
            processing_tasks: row.2,
            completed_tasks: row.3,
            failed_tasks: row.4,
            cancelled_tasks: row.5,
        })
    }

    /// Check if connection pool is healthy and can handle current load
    ///
    /// Performs a comprehensive health check of the connection pool including:
    /// - Connection availability (can acquire a connection)
    /// - Pool utilization (not over-utilized)
    /// - Database responsiveness (simple query performance)
    ///
    /// # Returns
    ///
    /// `Ok(true)` if healthy, `Ok(false)` if degraded, `Err` if critical failure
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    ///
    /// match broker.check_connection_health().await {
    ///     Ok(true) => println!("Connection pool is healthy"),
    ///     Ok(false) => println!("Connection pool is degraded"),
    ///     Err(e) => println!("Connection pool has critical issues: {}", e),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn check_connection_health(&self) -> Result<bool> {
        // Try to acquire a connection with timeout
        let start = std::time::Instant::now();
        let conn_result =
            tokio::time::timeout(std::time::Duration::from_secs(5), self.pool.acquire()).await;

        match conn_result {
            Err(_) => {
                tracing::error!("Connection pool timeout: failed to acquire connection within 5s");
                Err(CelersError::Other(
                    "Connection pool exhausted: timeout acquiring connection".to_string(),
                ))
            }
            Ok(Err(e)) => {
                tracing::error!(error = %e, "Connection pool error");
                Err(CelersError::Other(format!("Connection pool error: {}", e)))
            }
            Ok(Ok(mut conn)) => {
                let acquire_time = start.elapsed();

                // Warn if connection acquisition took too long
                if acquire_time > std::time::Duration::from_secs(1) {
                    tracing::warn!(
                        acquire_time_ms = acquire_time.as_millis(),
                        "Slow connection acquisition indicates pool pressure"
                    );
                }

                // Test database responsiveness with simple query
                let query_start = std::time::Instant::now();
                let result = sqlx::query_scalar::<_, i64>("SELECT 1")
                    .fetch_one(&mut *conn)
                    .await;

                match result {
                    Err(e) => {
                        tracing::error!(error = %e, "Database health check query failed");
                        Err(CelersError::Other(format!("Database unresponsive: {}", e)))
                    }
                    Ok(_) => {
                        let query_time = query_start.elapsed();

                        // Warn if database is responding slowly
                        if query_time > std::time::Duration::from_millis(100) {
                            tracing::warn!(
                                query_time_ms = query_time.as_millis(),
                                "Slow database response indicates potential issues"
                            );
                            return Ok(false); // Degraded
                        }

                        // Check pool utilization
                        let pool_size = self.pool.size();
                        let idle_conns = self.pool.num_idle() as u32;
                        let active_conns = pool_size.saturating_sub(idle_conns);
                        let utilization = (active_conns as f64 / pool_size as f64) * 100.0;

                        if utilization > 90.0 {
                            tracing::warn!(
                                utilization_percent = utilization,
                                pool_size = pool_size,
                                active = active_conns,
                                idle = idle_conns,
                                "High connection pool utilization"
                            );
                            return Ok(false); // Degraded
                        }

                        tracing::debug!(
                            acquire_time_ms = acquire_time.as_millis(),
                            query_time_ms = query_time.as_millis(),
                            utilization_percent = utilization,
                            "Connection pool health check passed"
                        );

                        Ok(true) // Healthy
                    }
                }
            }
        }
    }

    /// Batch replay tasks from DLQ with filtering
    ///
    /// Requeues multiple tasks from the dead letter queue based on filter criteria.
    /// This is useful for recovering from systematic failures or replaying tasks
    /// after fixing bugs.
    ///
    /// # Arguments
    ///
    /// * `task_name_filter` - Optional task name pattern to match (None = all tasks)
    /// * `min_retry_count` - Minimum retry count to include (for filtering partial failures)
    /// * `limit` - Maximum number of tasks to replay
    ///
    /// # Returns
    ///
    /// Number of tasks successfully requeued from DLQ
    ///
    /// # Examples
    ///
    /// ```
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = MysqlBroker::new("mysql://localhost/test").await?;
    /// // Replay all tasks with "payment" in the name that failed with 3+ retries
    /// let count = broker.replay_dlq_batch(Some("payment"), Some(3), 100).await?;
    /// println!("Replayed {} payment tasks from DLQ", count);
    ///
    /// // Replay all failed tasks (no filter)
    /// let count = broker.replay_dlq_batch(None, None, 1000).await?;
    /// println!("Replayed {} tasks from DLQ", count);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn replay_dlq_batch(
        &self,
        task_name_filter: Option<&str>,
        min_retry_count: Option<i32>,
        limit: i64,
    ) -> Result<u64> {
        let mut query = String::from("SELECT id FROM celers_dead_letter_queue WHERE 1=1");

        if task_name_filter.is_some() {
            query.push_str(" AND task_name LIKE ?");
        }

        if min_retry_count.is_some() {
            query.push_str(" AND retry_count >= ?");
        }

        query.push_str(" ORDER BY failed_at ASC LIMIT ?");

        let mut q = sqlx::query_scalar::<_, String>(&query);

        if let Some(filter) = task_name_filter {
            q = q.bind(format!("%{}%", filter));
        }

        if let Some(min_retries) = min_retry_count {
            q = q.bind(min_retries);
        }

        q = q.bind(limit);

        let dlq_ids = q
            .fetch_all(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to fetch DLQ IDs: {}", e)))?;

        let mut replayed = 0u64;
        for dlq_id in dlq_ids {
            match Uuid::parse_str(&dlq_id) {
                Ok(id) => {
                    if self.requeue_from_dlq(&id).await.is_ok() {
                        replayed += 1;
                    }
                }
                Err(e) => {
                    tracing::warn!(dlq_id = %dlq_id, error = %e, "Failed to parse DLQ ID");
                }
            }
        }

        tracing::info!(
            replayed = replayed,
            task_filter = ?task_name_filter,
            min_retries = ?min_retry_count,
            "Batch replay from DLQ completed"
        );

        Ok(replayed)
    }
}
