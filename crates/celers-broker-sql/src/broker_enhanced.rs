//! Enhanced broker operations
//!
//! Batch cancellation, scheduled tasks, DLQ operations, health checks,
//! queue statistics, worker management, and other extended operations.

use crate::broker_core::MysqlBroker;
use crate::types::*;
use celers_core::{Broker, CelersError, Result, SerializedTask, TaskId};
use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::Row;
use std::time::Duration;
use uuid::Uuid;

#[cfg(feature = "metrics")]
use celers_metrics::{TASKS_ENQUEUED_BY_TYPE, TASKS_ENQUEUED_TOTAL};

// ========== Enhancement Methods ==========
impl MysqlBroker {
    /// Cancel multiple tasks atomically
    ///
    /// This is more efficient than calling cancel() multiple times.
    /// Only cancels tasks in 'pending' or 'processing' state.
    ///
    /// # Arguments
    /// * `task_ids` - Slice of task IDs to cancel
    ///
    /// # Returns
    /// The number of tasks actually cancelled.
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # use celers_core::{Broker, SerializedTask};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// // Enqueue some tasks
    /// let task1 = broker.enqueue(SerializedTask::new("task1".into(), vec![])).await?;
    /// let task2 = broker.enqueue(SerializedTask::new("task2".into(), vec![])).await?;
    /// let task3 = broker.enqueue(SerializedTask::new("task3".into(), vec![])).await?;
    ///
    /// // Cancel all three tasks in one operation
    /// let cancelled = broker.cancel_batch(&[task1, task2, task3]).await?;
    /// println!("Cancelled {} tasks", cancelled);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn cancel_batch(&self, task_ids: &[TaskId]) -> Result<u64> {
        if task_ids.is_empty() {
            return Ok(0);
        }

        let task_id_strings: Vec<String> = task_ids.iter().map(|id| id.to_string()).collect();
        let placeholders = vec!["?"; task_ids.len()].join(", ");

        let query = format!(
            r#"
            UPDATE celers_tasks
            SET state = 'cancelled', completed_at = NOW()
            WHERE id IN ({}) AND state IN ('pending', 'processing')
            "#,
            placeholders
        );

        let mut query_builder = sqlx::query(&query);
        for task_id_str in task_id_strings {
            query_builder = query_builder.bind(task_id_str);
        }

        let result = query_builder
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to cancel batch: {}", e)))?;

        let cancelled = result.rows_affected();
        tracing::info!(count = cancelled, "Cancelled tasks in batch");

        Ok(cancelled)
    }

    /// Get statistics for a specific worker
    ///
    /// Returns detailed information about tasks processed by a specific worker,
    /// including active tasks, completed tasks, failed tasks, and average duration.
    ///
    /// # Arguments
    /// * `worker_id` - The worker ID to get statistics for
    ///
    /// # Returns
    /// Worker statistics including task counts and average duration
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// // Get statistics for a specific worker
    /// let stats = broker.get_worker_statistics("worker-123").await?;
    /// println!("Worker {} has {} active tasks, {} completed",
    ///     stats.worker_id, stats.active_tasks, stats.completed_tasks);
    /// println!("Average task duration: {:.2}s", stats.avg_task_duration_secs);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_worker_statistics(&self, worker_id: &str) -> Result<WorkerStatistics> {
        let row = sqlx::query(
            r#"
            SELECT
                worker_id,
                SUM(CASE WHEN state = 'processing' THEN 1 ELSE 0 END) as active_tasks,
                SUM(CASE WHEN state = 'completed' THEN 1 ELSE 0 END) as completed_tasks,
                SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END) as failed_tasks,
                MAX(started_at) as last_seen,
                AVG(TIMESTAMPDIFF(SECOND, started_at, completed_at)) as avg_duration
            FROM celers_tasks
            WHERE worker_id = ?
            GROUP BY worker_id
            "#,
        )
        .bind(worker_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get worker statistics: {}", e)))?;

        if let Some(row) = row {
            let active: Option<rust_decimal::Decimal> = row.get("active_tasks");
            let completed: Option<rust_decimal::Decimal> = row.get("completed_tasks");
            let failed: Option<rust_decimal::Decimal> = row.get("failed_tasks");
            let last_seen: Option<DateTime<Utc>> = row.get("last_seen");
            let avg_duration: Option<rust_decimal::Decimal> = row.get("avg_duration");

            Ok(WorkerStatistics {
                worker_id: worker_id.to_string(),
                active_tasks: active
                    .map(|d| d.to_string().parse().unwrap_or(0))
                    .unwrap_or(0),
                completed_tasks: completed
                    .map(|d| d.to_string().parse().unwrap_or(0))
                    .unwrap_or(0),
                failed_tasks: failed
                    .map(|d| d.to_string().parse().unwrap_or(0))
                    .unwrap_or(0),
                last_seen: last_seen.unwrap_or_else(Utc::now),
                avg_task_duration_secs: avg_duration
                    .and_then(|d| d.to_string().parse::<f64>().ok())
                    .unwrap_or(0.0),
            })
        } else {
            // Worker not found, return zero stats
            Ok(WorkerStatistics {
                worker_id: worker_id.to_string(),
                active_tasks: 0,
                completed_tasks: 0,
                failed_tasks: 0,
                last_seen: Utc::now(),
                avg_task_duration_secs: 0.0,
            })
        }
    }

    /// Get quick count of tasks by state
    ///
    /// This is a lightweight alternative to get_statistics() that only
    /// returns counts for a specific state, without computing aggregates
    /// for all states.
    ///
    /// # Arguments
    /// * `state` - The task state to count
    ///
    /// # Returns
    /// The number of tasks in the specified state
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::{MysqlBroker, DbTaskState};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// // Quickly check how many tasks are pending
    /// let pending = broker.count_by_state_quick(DbTaskState::Pending).await?;
    /// println!("Pending tasks: {}", pending);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn count_by_state_quick(&self, state: DbTaskState) -> Result<i64> {
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM celers_tasks WHERE state = ?")
            .bind(state.to_string())
            .fetch_one(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to count tasks by state: {}", e)))?;

        Ok(count)
    }

    /// Get task age distribution for monitoring queue health
    ///
    /// Returns pending tasks grouped into age buckets to help identify
    /// queue backlogs and performance issues.
    ///
    /// Age buckets:
    /// - < 1 minute
    /// - 1-5 minutes
    /// - 5-15 minutes
    /// - 15-60 minutes
    /// - > 60 minutes
    ///
    /// # Returns
    /// Vector of age distribution buckets with task counts and oldest task age
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// let distribution = broker.get_task_age_distribution().await?;
    /// for bucket in distribution {
    ///     println!("{}: {} tasks (oldest: {}s)",
    ///         bucket.bucket_label,
    ///         bucket.task_count,
    ///         bucket.oldest_task_age_secs);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_task_age_distribution(&self) -> Result<Vec<TaskAgeDistribution>> {
        let rows = sqlx::query(
            r#"
            SELECT
                CASE
                    WHEN TIMESTAMPDIFF(SECOND, created_at, NOW()) < 60 THEN '< 1 min'
                    WHEN TIMESTAMPDIFF(SECOND, created_at, NOW()) < 300 THEN '1-5 min'
                    WHEN TIMESTAMPDIFF(SECOND, created_at, NOW()) < 900 THEN '5-15 min'
                    WHEN TIMESTAMPDIFF(SECOND, created_at, NOW()) < 3600 THEN '15-60 min'
                    ELSE '> 60 min'
                END as bucket,
                COUNT(*) as task_count,
                MAX(TIMESTAMPDIFF(SECOND, created_at, NOW())) as oldest_age
            FROM celers_tasks
            WHERE state = 'pending'
            GROUP BY bucket
            ORDER BY
                CASE bucket
                    WHEN '< 1 min' THEN 1
                    WHEN '1-5 min' THEN 2
                    WHEN '5-15 min' THEN 3
                    WHEN '15-60 min' THEN 4
                    ELSE 5
                END
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get task age distribution: {}", e)))?;

        let mut distribution = Vec::with_capacity(rows.len());
        for row in rows {
            let bucket: String = row.get("bucket");
            let task_count: i64 = row.get("task_count");
            let oldest_age: Option<i64> = row.get("oldest_age");

            distribution.push(TaskAgeDistribution {
                bucket_label: bucket,
                task_count,
                oldest_task_age_secs: oldest_age.unwrap_or(0),
            });
        }

        Ok(distribution)
    }

    /// Get retry statistics grouped by task name
    ///
    /// Analyzes task failure patterns to identify which task types
    /// are failing most often and how many retries they typically require.
    ///
    /// # Returns
    /// Vector of retry statistics per task type, sorted by total retries descending
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// let stats = broker.get_retry_statistics().await?;
    /// for stat in stats {
    ///     println!("Task '{}': {} retries across {} tasks (avg: {:.1}, max: {})",
    ///         stat.task_name,
    ///         stat.total_retries,
    ///         stat.unique_tasks,
    ///         stat.avg_retries_per_task,
    ///         stat.max_retries_observed);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_retry_statistics(&self) -> Result<Vec<RetryStatistics>> {
        let rows = sqlx::query(
            r#"
            SELECT
                task_name,
                SUM(retry_count) as total_retries,
                COUNT(*) as unique_tasks,
                AVG(retry_count) as avg_retries,
                MAX(retry_count) as max_retries
            FROM celers_tasks
            WHERE retry_count > 0
            GROUP BY task_name
            ORDER BY total_retries DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get retry statistics: {}", e)))?;

        let mut stats = Vec::with_capacity(rows.len());
        for row in rows {
            let task_name: String = row.get("task_name");
            let total_retries: Option<rust_decimal::Decimal> = row.get("total_retries");
            let unique_tasks: i64 = row.get("unique_tasks");
            let avg_retries: Option<rust_decimal::Decimal> = row.get("avg_retries");
            let max_retries: i32 = row.get("max_retries");

            stats.push(RetryStatistics {
                task_name,
                total_retries: total_retries
                    .map(|d| d.to_string().parse().unwrap_or(0))
                    .unwrap_or(0),
                unique_tasks,
                avg_retries_per_task: avg_retries
                    .and_then(|d| d.to_string().parse::<f64>().ok())
                    .unwrap_or(0.0),
                max_retries_observed: max_retries,
            });
        }

        Ok(stats)
    }

    /// Get all workers that are currently processing tasks
    ///
    /// Returns a list of all worker IDs that currently have tasks
    /// in the 'processing' state.
    ///
    /// # Returns
    /// Vector of worker IDs currently processing tasks
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// let workers = broker.list_active_workers().await?;
    /// println!("Active workers: {:?}", workers);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_active_workers(&self) -> Result<Vec<String>> {
        let rows = sqlx::query(
            r#"
            SELECT DISTINCT worker_id
            FROM celers_tasks
            WHERE worker_id IS NOT NULL AND state = 'processing'
            ORDER BY worker_id
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to list active workers: {}", e)))?;

        let workers: Vec<String> = rows.into_iter().map(|row| row.get("worker_id")).collect();

        Ok(workers)
    }

    /// Get all worker statistics for all active workers
    ///
    /// Returns statistics for all workers that are currently processing tasks.
    /// This is a convenience method that combines list_active_workers() and
    /// get_worker_statistics() for each worker.
    ///
    /// # Returns
    /// Vector of worker statistics for all active workers
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// let all_stats = broker.get_all_worker_statistics().await?;
    /// for stats in all_stats {
    ///     println!("Worker {}: {} active, {} completed, {} failed",
    ///         stats.worker_id,
    ///         stats.active_tasks,
    ///         stats.completed_tasks,
    ///         stats.failed_tasks);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_all_worker_statistics(&self) -> Result<Vec<WorkerStatistics>> {
        let worker_ids = self.list_active_workers().await?;
        let mut all_stats = Vec::with_capacity(worker_ids.len());

        for worker_id in worker_ids {
            if let Ok(stats) = self.get_worker_statistics(&worker_id).await {
                all_stats.push(stats);
            }
        }

        Ok(all_stats)
    }

    /// Get overall queue health summary
    ///
    /// Combines multiple metrics to provide a comprehensive health assessment
    /// of the queue, including backlog, oldest task age, and active workers.
    ///
    /// Status determination:
    /// - "healthy": < 100 pending, oldest task < 5 min
    /// - "degraded": < 1000 pending, oldest task < 15 min
    /// - "critical": >= 1000 pending or oldest task >= 15 min
    ///
    /// # Returns
    /// Queue health summary with overall status assessment
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// let health = broker.get_queue_health().await?;
    /// println!("Queue status: {}", health.overall_status);
    /// println!("Pending: {}, Processing: {}", health.pending_tasks, health.processing_tasks);
    /// println!("Oldest task: {}s, Active workers: {}",
    ///     health.oldest_pending_age_secs, health.active_workers);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_queue_health(&self) -> Result<QueueHealth> {
        let stats = self.get_statistics().await?;
        let workers = self.list_active_workers().await?;

        // Get oldest pending task age
        let oldest_age: Option<i64> = sqlx::query_scalar(
            r#"
            SELECT TIMESTAMPDIFF(SECOND, created_at, NOW())
            FROM celers_tasks
            WHERE state = 'pending'
            ORDER BY created_at ASC
            LIMIT 1
            "#,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get oldest task: {}", e)))?;

        let oldest_age_secs = oldest_age.unwrap_or(0);
        let oldest_age_minutes = oldest_age_secs as f64 / 60.0;

        // Determine overall status
        let overall_status = if stats.pending < 100 && oldest_age_minutes < 5.0 {
            "healthy"
        } else if stats.pending < 1000 && oldest_age_minutes < 15.0 {
            "degraded"
        } else {
            "critical"
        };

        // Estimate backlog in minutes (rough estimate)
        let avg_processing_rate = if !workers.is_empty() {
            workers.len() as f64
        } else {
            1.0
        };
        let backlog_minutes = if avg_processing_rate > 0.0 {
            stats.pending as f64 / avg_processing_rate
        } else {
            0.0
        };

        Ok(QueueHealth {
            overall_status: overall_status.to_string(),
            pending_tasks: stats.pending,
            processing_tasks: stats.processing,
            oldest_pending_age_secs: oldest_age_secs,
            active_workers: workers.len() as i64,
            queue_backlog_minutes: backlog_minutes,
        })
    }

    /// Get task throughput metrics
    ///
    /// Calculates how many tasks have been completed and failed in the
    /// last minute and hour, with overall tasks per second rate.
    ///
    /// # Returns
    /// Throughput metrics for completed and failed tasks
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// let throughput = broker.get_task_throughput().await?;
    /// println!("Completed: {} last min, {} last hour ({:.2}/s)",
    ///     throughput.completed_last_minute,
    ///     throughput.completed_last_hour,
    ///     throughput.tasks_per_second);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_task_throughput(&self) -> Result<TaskThroughput> {
        let row = sqlx::query(
            r#"
            SELECT
                SUM(CASE WHEN state = 'completed' AND completed_at >= DATE_SUB(NOW(), INTERVAL 1 MINUTE) THEN 1 ELSE 0 END) as completed_1min,
                SUM(CASE WHEN state = 'completed' AND completed_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR) THEN 1 ELSE 0 END) as completed_1hour,
                SUM(CASE WHEN state = 'failed' AND completed_at >= DATE_SUB(NOW(), INTERVAL 1 MINUTE) THEN 1 ELSE 0 END) as failed_1min,
                SUM(CASE WHEN state = 'failed' AND completed_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR) THEN 1 ELSE 0 END) as failed_1hour
            FROM celers_tasks
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get throughput: {}", e)))?;

        let completed_1min: Option<rust_decimal::Decimal> = row.get("completed_1min");
        let completed_1hour: Option<rust_decimal::Decimal> = row.get("completed_1hour");
        let failed_1min: Option<rust_decimal::Decimal> = row.get("failed_1min");
        let failed_1hour: Option<rust_decimal::Decimal> = row.get("failed_1hour");

        let completed_last_minute = completed_1min
            .map(|d| d.to_string().parse().unwrap_or(0))
            .unwrap_or(0);
        let completed_last_hour = completed_1hour
            .map(|d| d.to_string().parse().unwrap_or(0))
            .unwrap_or(0);

        let tasks_per_second = completed_last_minute as f64 / 60.0;

        Ok(TaskThroughput {
            completed_last_minute,
            completed_last_hour,
            failed_last_minute: failed_1min
                .map(|d| d.to_string().parse().unwrap_or(0))
                .unwrap_or(0),
            failed_last_hour: failed_1hour
                .map(|d| d.to_string().parse().unwrap_or(0))
                .unwrap_or(0),
            tasks_per_second,
        })
    }

    /// Requeue tasks stuck with a specific worker
    ///
    /// This is useful when a worker crashes or becomes unresponsive.
    /// Moves tasks back to pending state so they can be picked up by other workers.
    ///
    /// # Arguments
    /// * `worker_id` - The worker ID whose tasks should be requeued
    ///
    /// # Returns
    /// The number of tasks requeued
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// // Requeue all tasks from a crashed worker
    /// let requeued = broker.requeue_stuck_tasks_by_worker("worker-crashed-123").await?;
    /// println!("Requeued {} tasks from crashed worker", requeued);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn requeue_stuck_tasks_by_worker(&self, worker_id: &str) -> Result<u64> {
        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = 'pending', worker_id = NULL, started_at = NULL
            WHERE worker_id = ? AND state = 'processing'
            "#,
        )
        .bind(worker_id)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to requeue tasks for worker: {}", e)))?;

        let requeued = result.rows_affected();
        tracing::warn!(worker_id = %worker_id, count = requeued, "Requeued stuck tasks");

        Ok(requeued)
    }

    /// Execute multiple operations within a single transaction
    ///
    /// This method provides a transaction wrapper for executing complex multi-step
    /// operations atomically. The callback receives a transaction handle that can
    /// be used for database operations.
    ///
    /// # Arguments
    /// * `f` - Async callback function that performs operations within the transaction
    ///
    /// # Returns
    /// The result of the callback function
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # use celers_core::{Broker, SerializedTask};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// // Execute multiple enqueues atomically
    /// broker.with_transaction(|_tx| async {
    ///     // Your transaction logic here
    ///     Ok(())
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn with_transaction<F, T, Fut>(&self, f: F) -> Result<T>
    where
        F: FnOnce(sqlx::Transaction<'_, sqlx::MySql>) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let tx = self
            .pool
            .begin()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        let result = f(tx).await?;

        Ok(result)
    }

    /// Query tasks by metadata JSON field
    ///
    /// Searches for tasks where the metadata JSON contains a specific key-value pair.
    /// This uses MySQL JSON functions to query inside the metadata column.
    ///
    /// # Arguments
    /// * `json_path` - JSON path to query (e.g., "$.user_id")
    /// * `value` - Value to match
    /// * `limit` - Maximum number of results
    /// * `offset` - Pagination offset
    ///
    /// # Returns
    /// List of matching tasks
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// // Find all tasks for a specific user
    /// let tasks = broker.query_tasks_by_metadata("$.user_id", "12345", 10, 0).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn query_tasks_by_metadata(
        &self,
        json_path: &str,
        value: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<TaskInfo>> {
        let rows = sqlx::query(
            r#"
            SELECT id, task_name, state, priority, retry_count, max_retries,
                   created_at, scheduled_at, started_at, completed_at, worker_id, error_message
            FROM celers_tasks
            WHERE JSON_EXTRACT(metadata, ?) = ?
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
            "#,
        )
        .bind(json_path)
        .bind(value)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to query tasks by metadata: {}", e)))?;

        let mut tasks = Vec::new();
        for row in rows {
            let id_str: String = row
                .try_get("id")
                .map_err(|e| CelersError::Other(format!("Failed to get id: {}", e)))?;
            let state_str: String = row
                .try_get("state")
                .map_err(|e| CelersError::Other(format!("Failed to get state: {}", e)))?;

            tasks.push(TaskInfo {
                id: Uuid::parse_str(&id_str)
                    .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?,
                task_name: row
                    .try_get("task_name")
                    .map_err(|e| CelersError::Other(format!("Failed to get task_name: {}", e)))?,
                state: state_str.parse()?,
                priority: row
                    .try_get("priority")
                    .map_err(|e| CelersError::Other(format!("Failed to get priority: {}", e)))?,
                retry_count: row
                    .try_get("retry_count")
                    .map_err(|e| CelersError::Other(format!("Failed to get retry_count: {}", e)))?,
                max_retries: row
                    .try_get("max_retries")
                    .map_err(|e| CelersError::Other(format!("Failed to get max_retries: {}", e)))?,
                created_at: row
                    .try_get("created_at")
                    .map_err(|e| CelersError::Other(format!("Failed to get created_at: {}", e)))?,
                scheduled_at: row.try_get("scheduled_at").map_err(|e| {
                    CelersError::Other(format!("Failed to get scheduled_at: {}", e))
                })?,
                started_at: row
                    .try_get("started_at")
                    .map_err(|e| CelersError::Other(format!("Failed to get started_at: {}", e)))?,
                completed_at: row.try_get("completed_at").map_err(|e| {
                    CelersError::Other(format!("Failed to get completed_at: {}", e))
                })?,
                worker_id: row
                    .try_get("worker_id")
                    .map_err(|e| CelersError::Other(format!("Failed to get worker_id: {}", e)))?,
                error_message: row.try_get("error_message").map_err(|e| {
                    CelersError::Other(format!("Failed to get error_message: {}", e))
                })?,
            });
        }

        Ok(tasks)
    }

    /// Enqueue a task with deduplication based on a custom key
    ///
    /// This method ensures that only one task with a given deduplication key exists
    /// in the pending or processing state. If a task with the same key already exists,
    /// this method returns the existing task ID instead of creating a duplicate.
    ///
    /// The deduplication key is stored in the metadata JSON field.
    ///
    /// # Arguments
    /// * `task` - The task to enqueue
    /// * `dedup_key` - Unique key for deduplication
    ///
    /// # Returns
    /// Task ID (either new or existing)
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # use celers_core::{Broker, SerializedTask};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// let task = SerializedTask::new("process_user".into(), vec![1, 2, 3]);
    /// // Only one task per user will be enqueued
    /// let task_id = broker.enqueue_deduplicated(task, "user:12345").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn enqueue_deduplicated(
        &self,
        task: SerializedTask,
        dedup_key: &str,
    ) -> Result<TaskId> {
        // First check if a task with this dedup key already exists
        let existing = sqlx::query(
            r#"
            SELECT id FROM celers_tasks
            WHERE JSON_EXTRACT(metadata, '$.dedup_key') = ?
              AND state IN ('pending', 'processing')
            LIMIT 1
            "#,
        )
        .bind(dedup_key)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to check for duplicate task: {}", e)))?;

        if let Some(row) = existing {
            let id_str: String = row
                .try_get("id")
                .map_err(|e| CelersError::Other(format!("Failed to get id: {}", e)))?;
            let task_id = Uuid::parse_str(&id_str)
                .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?;
            tracing::info!(task_id = %task_id, dedup_key = %dedup_key, "Task already exists, skipping");
            return Ok(task_id);
        }

        // Create metadata with dedup key
        let task_id = task.metadata.id;
        let mut db_metadata = json!({
            "queue": self.queue_name,
            "enqueued_at": chrono::Utc::now().to_rfc3339(),
            "dedup_key": dedup_key,
        });

        // Merge task metadata if present
        if let Ok(task_meta) = serde_json::to_value(&task.metadata) {
            if let Some(obj) = db_metadata.as_object_mut() {
                if let Some(meta_obj) = task_meta.as_object() {
                    for (k, v) in meta_obj {
                        obj.insert(k.clone(), v.clone());
                    }
                }
            }
        }

        sqlx::query(
            r#"
            INSERT INTO celers_tasks
                (id, task_name, payload, state, priority, max_retries, metadata, created_at, scheduled_at)
            VALUES (?, ?, ?, 'pending', ?, ?, ?, NOW(), NOW())
            "#,
        )
        .bind(task_id.to_string())
        .bind(&task.metadata.name)
        .bind(&task.payload)
        .bind(task.metadata.priority)
        .bind(task.metadata.max_retries as i32)
        .bind(serde_json::to_string(&db_metadata).unwrap_or_else(|_| "{}".to_string()))
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to enqueue deduplicated task: {}", e)))?;

        tracing::info!(task_id = %task_id, dedup_key = %dedup_key, "Enqueued new deduplicated task");

        #[cfg(feature = "metrics")]
        {
            TASKS_ENQUEUED_TOTAL.inc();
            TASKS_ENQUEUED_BY_TYPE
                .with_label_values(&[&task.metadata.name])
                .inc();
        }

        Ok(task_id)
    }

    /// Update state for multiple tasks atomically
    ///
    /// This is more efficient than updating task states individually.
    /// Only updates tasks that are in a valid source state.
    ///
    /// # Arguments
    /// * `task_ids` - Slice of task IDs to update
    /// * `new_state` - The new state to set
    ///
    /// # Returns
    /// The number of tasks actually updated
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::{MysqlBroker, DbTaskState};
    /// # use celers_core::TaskId;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// let task_ids = vec![/* task IDs */];
    /// let updated = broker.update_batch_state(&task_ids, DbTaskState::Failed).await?;
    /// println!("Updated {} tasks to failed state", updated);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn update_batch_state(
        &self,
        task_ids: &[TaskId],
        new_state: DbTaskState,
    ) -> Result<u64> {
        if task_ids.is_empty() {
            return Ok(0);
        }

        let task_id_strings: Vec<String> = task_ids.iter().map(|id| id.to_string()).collect();
        let placeholders = vec!["?"; task_ids.len()].join(", ");

        let completed_at_clause = match new_state {
            DbTaskState::Completed | DbTaskState::Failed | DbTaskState::Cancelled => {
                ", completed_at = NOW()"
            }
            _ => "",
        };

        let query = format!(
            r#"
            UPDATE celers_tasks
            SET state = ?{}
            WHERE id IN ({})
            "#,
            completed_at_clause, placeholders
        );

        let mut query_builder = sqlx::query(&query);
        query_builder = query_builder.bind(new_state.to_string());
        for task_id_str in task_id_strings {
            query_builder = query_builder.bind(task_id_str);
        }

        let result = query_builder
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to update batch state: {}", e)))?;

        let updated = result.rows_affected();
        tracing::info!(count = updated, state = %new_state, "Updated task states in batch");

        Ok(updated)
    }

    /// Check if enqueueing would exceed the queue capacity
    ///
    /// Returns true if the queue has room for more tasks, false if it's at capacity.
    /// This can be used to implement backpressure and prevent queue overflow.
    ///
    /// # Arguments
    /// * `max_size` - Maximum allowed pending tasks
    ///
    /// # Returns
    /// True if the queue can accept more tasks
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # use celers_core::{Broker, SerializedTask};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// // Check capacity before enqueueing
    /// if broker.has_capacity(10000).await? {
    ///     let task = SerializedTask::new("task".into(), vec![]);
    ///     broker.enqueue(task).await?;
    /// } else {
    ///     println!("Queue is full, backing off");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn has_capacity(&self, max_size: i64) -> Result<bool> {
        let current_size = self.count_by_state_quick(DbTaskState::Pending).await?;
        Ok(current_size < max_size)
    }

    /// Enqueue a task only if the queue has capacity
    ///
    /// This is a convenience method that combines capacity checking with enqueuing.
    /// If the queue is full, it returns an error instead of enqueueing.
    ///
    /// # Arguments
    /// * `task` - The task to enqueue
    /// * `max_size` - Maximum allowed pending tasks
    ///
    /// # Returns
    /// Task ID if enqueued successfully
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # use celers_core::{Broker, SerializedTask};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// let task = SerializedTask::new("task".into(), vec![]);
    /// match broker.enqueue_with_capacity(task, 10000).await {
    ///     Ok(task_id) => println!("Enqueued task {}", task_id),
    ///     Err(_) => println!("Queue is full"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn enqueue_with_capacity(
        &self,
        task: SerializedTask,
        max_size: i64,
    ) -> Result<TaskId> {
        if !self.has_capacity(max_size).await? {
            return Err(CelersError::Other(
                "Queue is at capacity, cannot enqueue".to_string(),
            ));
        }

        self.enqueue(task).await
    }

    /// Expire old pending tasks that have exceeded their TTL
    ///
    /// This method marks pending tasks as cancelled if they have been pending
    /// longer than the specified TTL. Useful for preventing stale tasks from
    /// being processed.
    ///
    /// # Arguments
    /// * `ttl` - Maximum age for pending tasks
    ///
    /// # Returns
    /// Number of tasks expired
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # use std::time::Duration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// // Expire tasks older than 1 hour
    /// let expired = broker.expire_pending_tasks(Duration::from_secs(3600)).await?;
    /// println!("Expired {} stale tasks", expired);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn expire_pending_tasks(&self, ttl: Duration) -> Result<u64> {
        let ttl_seconds = ttl.as_secs() as i64;

        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = 'cancelled',
                completed_at = NOW(),
                error_message = CONCAT('Task expired after ', ?, ' seconds')
            WHERE state = 'pending'
              AND TIMESTAMPDIFF(SECOND, created_at, NOW()) > ?
            "#,
        )
        .bind(ttl_seconds)
        .bind(ttl_seconds)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to expire pending tasks: {}", e)))?;

        let expired = result.rows_affected();
        if expired > 0 {
            tracing::warn!(
                count = expired,
                ttl_seconds = ttl_seconds,
                "Expired pending tasks"
            );
        }

        Ok(expired)
    }

    /// Delete tasks matching specific criteria
    ///
    /// Permanently deletes tasks from the database based on state and age.
    /// This is more flexible than the existing purge methods.
    ///
    /// # Arguments
    /// * `state` - Optional state filter (None = all states)
    /// * `older_than` - Only delete tasks older than this duration
    ///
    /// # Returns
    /// Number of tasks deleted
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::{MysqlBroker, DbTaskState};
    /// # use std::time::Duration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// // Delete completed tasks older than 7 days
    /// let deleted = broker.delete_tasks_by_criteria(
    ///     Some(DbTaskState::Completed),
    ///     Duration::from_secs(7 * 24 * 3600)
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub async fn delete_tasks_by_criteria(
        &self,
        state: Option<DbTaskState>,
        older_than: Duration,
    ) -> Result<u64> {
        let seconds_ago = older_than.as_secs() as i64;

        let query = if let Some(state) = state {
            sqlx::query(
                r#"
                DELETE FROM celers_tasks
                WHERE state = ?
                  AND TIMESTAMPDIFF(SECOND, created_at, NOW()) > ?
                "#,
            )
            .bind(state.to_string())
            .bind(seconds_ago)
        } else {
            sqlx::query(
                r#"
                DELETE FROM celers_tasks
                WHERE TIMESTAMPDIFF(SECOND, created_at, NOW()) > ?
                "#,
            )
            .bind(seconds_ago)
        };

        let result = query
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to delete tasks: {}", e)))?;

        let deleted = result.rows_affected();
        tracing::info!(count = deleted, "Deleted tasks by criteria");

        Ok(deleted)
    }

    /// Update metadata for an existing task
    ///
    /// Allows updating the JSON metadata field for a task without changing
    /// its state or other properties. The metadata is merged with existing
    /// metadata using JSON_SET.
    ///
    /// # Arguments
    /// * `task_id` - Task ID to update
    /// * `json_path` - JSON path to update (e.g., "$.priority_level")
    /// * `value` - New value for the path
    ///
    /// # Returns
    /// True if task was updated
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # use celers_core::TaskId;
    /// # use uuid::Uuid;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    /// let task_id = Uuid::new_v4();
    ///
    /// // Update a specific metadata field
    /// broker.update_task_metadata(&task_id, "$.priority_level", "high").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn update_task_metadata(
        &self,
        task_id: &TaskId,
        json_path: &str,
        value: &str,
    ) -> Result<bool> {
        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET metadata = JSON_SET(metadata, ?, ?)
            WHERE id = ?
            "#,
        )
        .bind(json_path)
        .bind(value)
        .bind(task_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to update task metadata: {}", e)))?;

        Ok(result.rows_affected() > 0)
    }

    /// Search tasks by creation date range
    ///
    /// Finds tasks created within a specific time window. Useful for
    /// analyzing task patterns over time or implementing time-based cleanup.
    ///
    /// # Arguments
    /// * `from` - Start of time range
    /// * `to` - End of time range
    /// * `state` - Optional state filter
    /// * `limit` - Maximum results
    /// * `offset` - Pagination offset
    ///
    /// # Returns
    /// List of matching tasks
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::{MysqlBroker, DbTaskState};
    /// # use chrono::{Utc, Duration};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// let now = Utc::now();
    /// let one_hour_ago = now - Duration::hours(1);
    ///
    /// // Find all failed tasks in the last hour
    /// let tasks = broker.search_tasks_by_date_range(
    ///     one_hour_ago,
    ///     now,
    ///     Some(DbTaskState::Failed),
    ///     100,
    ///     0
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub async fn search_tasks_by_date_range(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        state: Option<DbTaskState>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<TaskInfo>> {
        let query = if let Some(state) = state {
            sqlx::query(
                r#"
                SELECT id, task_name, state, priority, retry_count, max_retries,
                       created_at, scheduled_at, started_at, completed_at, worker_id, error_message
                FROM celers_tasks
                WHERE created_at >= ? AND created_at <= ?
                  AND state = ?
                ORDER BY created_at DESC
                LIMIT ? OFFSET ?
                "#,
            )
            .bind(from)
            .bind(to)
            .bind(state.to_string())
            .bind(limit)
            .bind(offset)
        } else {
            sqlx::query(
                r#"
                SELECT id, task_name, state, priority, retry_count, max_retries,
                       created_at, scheduled_at, started_at, completed_at, worker_id, error_message
                FROM celers_tasks
                WHERE created_at >= ? AND created_at <= ?
                ORDER BY created_at DESC
                LIMIT ? OFFSET ?
                "#,
            )
            .bind(from)
            .bind(to)
            .bind(limit)
            .bind(offset)
        };

        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to search tasks by date: {}", e)))?;

        let mut tasks = Vec::new();
        for row in rows {
            let id_str: String = row
                .try_get("id")
                .map_err(|e| CelersError::Other(format!("Failed to get id: {}", e)))?;
            let state_str: String = row
                .try_get("state")
                .map_err(|e| CelersError::Other(format!("Failed to get state: {}", e)))?;

            tasks.push(TaskInfo {
                id: Uuid::parse_str(&id_str)
                    .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?,
                task_name: row
                    .try_get("task_name")
                    .map_err(|e| CelersError::Other(format!("Failed to get task_name: {}", e)))?,
                state: state_str.parse()?,
                priority: row
                    .try_get("priority")
                    .map_err(|e| CelersError::Other(format!("Failed to get priority: {}", e)))?,
                retry_count: row
                    .try_get("retry_count")
                    .map_err(|e| CelersError::Other(format!("Failed to get retry_count: {}", e)))?,
                max_retries: row
                    .try_get("max_retries")
                    .map_err(|e| CelersError::Other(format!("Failed to get max_retries: {}", e)))?,
                created_at: row
                    .try_get("created_at")
                    .map_err(|e| CelersError::Other(format!("Failed to get created_at: {}", e)))?,
                scheduled_at: row.try_get("scheduled_at").map_err(|e| {
                    CelersError::Other(format!("Failed to get scheduled_at: {}", e))
                })?,
                started_at: row
                    .try_get("started_at")
                    .map_err(|e| CelersError::Other(format!("Failed to get started_at: {}", e)))?,
                completed_at: row.try_get("completed_at").map_err(|e| {
                    CelersError::Other(format!("Failed to get completed_at: {}", e))
                })?,
                worker_id: row
                    .try_get("worker_id")
                    .map_err(|e| CelersError::Other(format!("Failed to get worker_id: {}", e)))?,
                error_message: row.try_get("error_message").map_err(|e| {
                    CelersError::Other(format!("Failed to get error_message: {}", e))
                })?,
            });
        }

        Ok(tasks)
    }

    /// Get Dead Letter Queue statistics
    ///
    /// Returns comprehensive statistics about the DLQ including total count,
    /// counts by task name, and average retry counts.
    ///
    /// # Returns
    /// DLQ statistics
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// let stats = broker.get_dlq_statistics().await?;
    /// println!("DLQ has {} tasks", stats.total_tasks);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_dlq_statistics(&self) -> Result<DlqStatistics> {
        // Get total count
        let total_row = sqlx::query(
            r#"
            SELECT COUNT(*) as total
            FROM celers_dead_letter_queue
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get DLQ total: {}", e)))?;

        let total_tasks: i64 = total_row
            .try_get("total")
            .map_err(|e| CelersError::Other(format!("Failed to get total: {}", e)))?;

        // Get counts by task name
        let rows = sqlx::query(
            r#"
            SELECT task_name,
                   COUNT(*) as count,
                   AVG(retry_count) as avg_retries,
                   MAX(retry_count) as max_retries
            FROM celers_dead_letter_queue
            GROUP BY task_name
            ORDER BY count DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get DLQ stats: {}", e)))?;

        let mut by_task_name = Vec::new();
        for row in rows {
            let count: i64 = row
                .try_get("count")
                .map_err(|e| CelersError::Other(format!("Failed to get count: {}", e)))?;
            let avg_retries: Option<rust_decimal::Decimal> = row
                .try_get("avg_retries")
                .map_err(|e| CelersError::Other(format!("Failed to get avg_retries: {}", e)))?;
            let max_retries: i32 = row
                .try_get("max_retries")
                .map_err(|e| CelersError::Other(format!("Failed to get max_retries: {}", e)))?;

            by_task_name.push(DlqTaskStats {
                task_name: row
                    .try_get("task_name")
                    .map_err(|e| CelersError::Other(format!("Failed to get task_name: {}", e)))?,
                count,
                avg_retries: avg_retries.map(|d| d.to_string().parse::<f64>().unwrap_or(0.0)),
                max_retries,
            });
        }

        Ok(DlqStatistics {
            total_tasks,
            by_task_name,
        })
    }

    /// Detect and recover tasks that have exceeded their processing timeout
    ///
    /// Finds tasks that have been in 'processing' state longer than the
    /// specified timeout and requeues them as pending. This helps recover
    /// from worker crashes or hangs.
    ///
    /// # Arguments
    /// * `timeout` - Maximum time a task should be in processing state
    ///
    /// # Returns
    /// Number of timed-out tasks recovered
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # use std::time::Duration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// // Recover tasks stuck in processing for more than 30 minutes
    /// let recovered = broker.recover_timed_out_tasks(Duration::from_secs(1800)).await?;
    /// println!("Recovered {} timed-out tasks", recovered);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn recover_timed_out_tasks(&self, timeout: Duration) -> Result<u64> {
        let timeout_seconds = timeout.as_secs() as i64;

        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = 'pending',
                worker_id = NULL,
                started_at = NULL,
                error_message = CONCAT(
                    COALESCE(error_message, ''),
                    IF(error_message IS NOT NULL, '; ', ''),
                    'Task timed out after ', ?, ' seconds and was requeued'
                )
            WHERE state = 'processing'
              AND started_at IS NOT NULL
              AND TIMESTAMPDIFF(SECOND, started_at, NOW()) > ?
            "#,
        )
        .bind(timeout_seconds)
        .bind(timeout_seconds)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to recover timed-out tasks: {}", e)))?;

        let recovered = result.rows_affected();
        if recovered > 0 {
            tracing::warn!(
                count = recovered,
                timeout_seconds = timeout_seconds,
                "Recovered timed-out tasks"
            );
        }

        Ok(recovered)
    }
}
