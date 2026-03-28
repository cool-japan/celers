//! Task analytics, search, estimates, and performance baselines

use celers_core::{CelersError, Result};
use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::Row;
use uuid::Uuid;

use crate::types::{
    DbTaskState, PriorityStrategy, PriorityStrategyResult, StateTransitionStats, TaskInfo,
    TaskLifecycle, TaskResult,
};
use crate::PostgresBroker;

impl PostgresBroker {
    /// Store multiple task results in a single batch operation
    ///
    /// More efficient than calling `store_result()` multiple times.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::{PostgresBroker, TaskResult, TaskResultStatus};
    /// use uuid::Uuid;
    /// use chrono::Utc;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let results = vec![
    ///     TaskResult {
    ///         task_id: Uuid::new_v4(),
    ///         task_name: "task1".to_string(),
    ///         status: TaskResultStatus::Success,
    ///         result: Some(serde_json::json!({"value": 42})),
    ///         error: None,
    ///         traceback: None,
    ///         created_at: Utc::now(),
    ///         completed_at: Some(Utc::now()),
    ///         runtime_ms: Some(100),
    ///     },
    ///     TaskResult {
    ///         task_id: Uuid::new_v4(),
    ///         task_name: "task2".to_string(),
    ///         status: TaskResultStatus::Success,
    ///         result: Some(serde_json::json!({"value": 100})),
    ///         error: None,
    ///         traceback: None,
    ///         created_at: Utc::now(),
    ///         completed_at: Some(Utc::now()),
    ///         runtime_ms: Some(200),
    ///     },
    /// ];
    ///
    /// broker.store_results_batch(&results).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn store_results_batch(&self, results: &[TaskResult]) -> Result<i64> {
        if results.is_empty() {
            return Ok(0);
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        let mut stored = 0i64;
        for result in results {
            sqlx::query(
                r#"
                INSERT INTO celers_task_results (task_id, status, result, error, traceback)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (task_id) DO UPDATE
                SET status = EXCLUDED.status,
                    result = EXCLUDED.result,
                    error = EXCLUDED.error,
                    traceback = EXCLUDED.traceback,
                    updated_at = NOW()
                "#,
            )
            .bind(result.task_id)
            .bind(result.status.to_string())
            .bind(&result.result)
            .bind(&result.error)
            .bind(&result.traceback)
            .execute(&mut *tx)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to store result: {}", e)))?;

            stored += 1;
        }

        tx.commit()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to commit transaction: {}", e)))?;

        tracing::info!(count = stored, "Stored batch of task results");
        Ok(stored)
    }

    /// Find tasks that failed with errors matching a pattern
    ///
    /// Uses PostgreSQL pattern matching (LIKE) to find tasks with specific error messages.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Find all tasks that failed with connection errors
    /// let tasks = broker.find_tasks_by_error("%connection%", 100).await?;
    /// println!("Found {} tasks with connection errors", tasks.len());
    ///
    /// // Find specific error
    /// let tasks = broker.find_tasks_by_error("%timeout%", 50).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn find_tasks_by_error(
        &self,
        error_pattern: &str,
        limit: i64,
    ) -> Result<Vec<TaskInfo>> {
        let rows = sqlx::query(
            r#"
            SELECT id, task_name, state, priority, retry_count, max_retries,
                   created_at, scheduled_at, started_at, completed_at, worker_id, error_message
            FROM celers_tasks
            WHERE queue_name = $1
              AND state IN ('failed', 'cancelled')
              AND error_message LIKE $2
            ORDER BY created_at DESC
            LIMIT $3
            "#,
        )
        .bind(&self.queue_name)
        .bind(error_pattern)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to find tasks by error: {}", e)))?;

        let tasks: Vec<TaskInfo> = rows
            .iter()
            .map(|row| TaskInfo {
                id: row.get("id"),
                task_name: row.get("task_name"),
                state: row
                    .get::<String, _>("state")
                    .parse()
                    .unwrap_or(DbTaskState::Failed),
                priority: row.get("priority"),
                retry_count: row.get("retry_count"),
                max_retries: row.get("max_retries"),
                created_at: row.get("created_at"),
                scheduled_at: row.get("scheduled_at"),
                started_at: row.get("started_at"),
                completed_at: row.get("completed_at"),
                worker_id: row.get("worker_id"),
                error_message: row.get("error_message"),
            })
            .collect();

        Ok(tasks)
    }

    /// Estimate wait time for a pending task based on current throughput
    ///
    /// Calculates expected wait time by analyzing recent task completion rate
    /// and current queue depth.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let wait_secs = broker.estimate_wait_time().await?;
    /// if let Some(wait) = wait_secs {
    ///     println!("Estimated wait time: {} seconds", wait);
    /// } else {
    ///     println!("Not enough data to estimate wait time");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn estimate_wait_time(&self) -> Result<Option<i64>> {
        // Get completed tasks in last hour
        let completed_last_hour: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM celers_tasks
            WHERE queue_name = $1
              AND state = 'completed'
              AND completed_at > NOW() - INTERVAL '1 hour'
            "#,
        )
        .bind(&self.queue_name)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get completion rate: {}", e)))?;

        if completed_last_hour == 0 {
            return Ok(None);
        }

        // Get current pending count
        let pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM celers_tasks WHERE queue_name = $1 AND state = 'pending'",
        )
        .bind(&self.queue_name)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get pending count: {}", e)))?;

        // Calculate tasks per second
        let tasks_per_second = completed_last_hour as f64 / 3600.0;

        if tasks_per_second > 0.0 {
            let estimated_wait = (pending as f64 / tasks_per_second) as i64;
            Ok(Some(estimated_wait))
        } else {
            Ok(None)
        }
    }

    /// Get worker performance statistics
    ///
    /// Returns statistics about worker performance including task counts,
    /// average processing times, and success rates per worker.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let worker_stats = broker.get_worker_stats(10).await?;
    /// for (worker_id, processed, avg_time_ms, success_rate) in worker_stats {
    ///     println!(
    ///         "Worker {}: {} tasks, {:.0}ms avg, {:.1}% success",
    ///         worker_id.unwrap_or_else(|| "unknown".to_string()),
    ///         processed,
    ///         avg_time_ms.unwrap_or(0.0),
    ///         success_rate.unwrap_or(0.0) * 100.0
    ///     );
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_worker_stats(
        &self,
        limit: i64,
    ) -> Result<Vec<(Option<String>, i64, Option<f64>, Option<f64>)>> {
        let rows = sqlx::query(
            r#"
            SELECT
                worker_id,
                COUNT(*) as processed,
                AVG(EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000) as avg_time_ms,
                SUM(CASE WHEN state = 'completed' THEN 1 ELSE 0 END)::FLOAT / COUNT(*) as success_rate
            FROM celers_tasks
            WHERE queue_name = $1
              AND worker_id IS NOT NULL
              AND state IN ('completed', 'failed')
              AND completed_at > NOW() - INTERVAL '24 hours'
            GROUP BY worker_id
            ORDER BY processed DESC
            LIMIT $2
            "#,
        )
        .bind(&self.queue_name)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get worker stats: {}", e)))?;

        let stats = rows
            .iter()
            .map(|row| {
                let worker_id: Option<String> = row.get("worker_id");
                let processed: i64 = row.get("processed");
                let avg_time_ms: Option<f64> = row.get("avg_time_ms");
                let success_rate: Option<f64> = row.get("success_rate");
                (worker_id, processed, avg_time_ms, success_rate)
            })
            .collect();

        Ok(stats)
    }

    /// Get task age distribution
    ///
    /// Returns histogram buckets showing how many tasks fall into different age ranges.
    /// Useful for monitoring queue latency and identifying bottlenecks.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let distribution = broker.get_task_age_distribution().await?;
    /// println!("< 1 min: {} tasks", distribution.0);
    /// println!("1-5 min: {} tasks", distribution.1);
    /// println!("5-15 min: {} tasks", distribution.2);
    /// println!("15-60 min: {} tasks", distribution.3);
    /// println!("> 1 hour: {} tasks", distribution.4);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_task_age_distribution(&self) -> Result<(i64, i64, i64, i64, i64)> {
        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*) FILTER (WHERE age_secs < 60) as under_1min,
                COUNT(*) FILTER (WHERE age_secs >= 60 AND age_secs < 300) as between_1_5min,
                COUNT(*) FILTER (WHERE age_secs >= 300 AND age_secs < 900) as between_5_15min,
                COUNT(*) FILTER (WHERE age_secs >= 900 AND age_secs < 3600) as between_15_60min,
                COUNT(*) FILTER (WHERE age_secs >= 3600) as over_1hour
            FROM (
                SELECT EXTRACT(EPOCH FROM (NOW() - created_at)) as age_secs
                FROM celers_tasks
                WHERE queue_name = $1 AND state = 'pending'
            ) as ages
            "#,
        )
        .bind(&self.queue_name)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get task age distribution: {}", e)))?;

        Ok((
            row.get("under_1min"),
            row.get("between_1_5min"),
            row.get("between_5_15min"),
            row.get("between_15_60min"),
            row.get("over_1hour"),
        ))
    }

    /// Clone or copy tasks from another queue
    ///
    /// Copies pending tasks from a source queue to this queue.
    /// Useful for queue migration, load balancing, or disaster recovery.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Copy up to 100 tasks from source_queue
    /// let copied = broker.copy_tasks_from_queue("source_queue", 100).await?;
    /// println!("Copied {} tasks", copied);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn copy_tasks_from_queue(&self, source_queue: &str, limit: i64) -> Result<i64> {
        let result = sqlx::query(
            r#"
            INSERT INTO celers_tasks
                (task_name, payload, queue_name, state, priority, retry_count, max_retries,
                 timeout_secs, scheduled_at, metadata)
            SELECT
                task_name, payload, $1 as queue_name, 'pending' as state, priority,
                0 as retry_count, max_retries, timeout_secs, scheduled_at, metadata
            FROM celers_tasks
            WHERE queue_name = $2
              AND state = 'pending'
            LIMIT $3
            "#,
        )
        .bind(&self.queue_name)
        .bind(source_queue)
        .bind(limit)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to copy tasks: {}", e)))?;

        let copied = result.rows_affected() as i64;

        tracing::info!(
            source = source_queue,
            destination = %self.queue_name,
            count = copied,
            "Copied tasks between queues"
        );

        Ok(copied)
    }

    /// Move tasks from another queue to this queue
    ///
    /// Transfers pending tasks from a source queue to this queue by updating their queue_name.
    /// More efficient than copying as it doesn't create new rows.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Move up to 50 tasks from old_queue
    /// let moved = broker.move_tasks_from_queue("old_queue", 50).await?;
    /// println!("Moved {} tasks", moved);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn move_tasks_from_queue(&self, source_queue: &str, limit: i64) -> Result<i64> {
        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET queue_name = $1
            WHERE id IN (
                SELECT id FROM celers_tasks
                WHERE queue_name = $2 AND state = 'pending'
                LIMIT $3
            )
            "#,
        )
        .bind(&self.queue_name)
        .bind(source_queue)
        .bind(limit)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to move tasks: {}", e)))?;

        let moved = result.rows_affected() as i64;

        tracing::info!(
            source = source_queue,
            destination = %self.queue_name,
            count = moved,
            "Moved tasks between queues"
        );

        Ok(moved)
    }

    /// Get breakdown of tasks by hour of creation
    ///
    /// Returns task counts grouped by hour for the last 24 hours.
    /// Useful for understanding task creation patterns and peak times.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let hourly = broker.get_hourly_task_counts().await?;
    /// for (hour, count) in hourly {
    ///     println!("Hour {}: {} tasks", hour, count);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_hourly_task_counts(&self) -> Result<Vec<(i32, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT
                EXTRACT(HOUR FROM created_at)::INTEGER as hour,
                COUNT(*) as count
            FROM celers_tasks
            WHERE queue_name = $1
              AND created_at > NOW() - INTERVAL '24 hours'
            GROUP BY hour
            ORDER BY hour
            "#,
        )
        .bind(&self.queue_name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get hourly task counts: {}", e)))?;

        let hourly = rows
            .iter()
            .map(|row| {
                let hour: i32 = row.get("hour");
                let count: i64 = row.get("count");
                (hour, count)
            })
            .collect();

        Ok(hourly)
    }

    /// Replay/rerun completed or failed tasks
    ///
    /// Creates new pending tasks by cloning completed or failed tasks.
    /// Useful for retrying batches of tasks or debugging issues.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::{PostgresBroker, DbTaskState};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Replay all failed tasks from the last hour
    /// let replayed = broker.replay_tasks(DbTaskState::Failed, 100).await?;
    /// println!("Replayed {} failed tasks", replayed);
    ///
    /// // Rerun successful tasks for testing
    /// let rerun = broker.replay_tasks(DbTaskState::Completed, 10).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn replay_tasks(&self, source_state: DbTaskState, limit: i64) -> Result<i64> {
        let result = sqlx::query(
            r#"
            INSERT INTO celers_tasks
                (task_name, payload, queue_name, state, priority, retry_count, max_retries,
                 timeout_secs, scheduled_at, metadata)
            SELECT
                task_name, payload, queue_name, 'pending' as state, priority,
                0 as retry_count, max_retries, timeout_secs, NOW() as scheduled_at, metadata
            FROM celers_tasks
            WHERE queue_name = $1
              AND state = $2
            ORDER BY created_at DESC
            LIMIT $3
            "#,
        )
        .bind(&self.queue_name)
        .bind(source_state.to_string())
        .bind(limit)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to replay tasks: {}", e)))?;

        let replayed = result.rows_affected() as i64;

        tracing::info!(
            state = %source_state,
            count = replayed,
            "Replayed tasks"
        );

        Ok(replayed)
    }

    /// Calculate composite queue health score (0-100)
    ///
    /// Returns a health score based on multiple factors:
    /// - Queue depth (pending tasks)
    /// - Processing efficiency (tasks in progress vs pending)
    /// - DLQ ratio (failed tasks)
    /// - Task age (how long tasks have been waiting)
    ///
    /// Score interpretation:
    /// - 90-100: Excellent
    /// - 70-89: Good
    /// - 50-69: Fair
    /// - 30-49: Poor
    /// - 0-29: Critical
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let score = broker.calculate_queue_health_score().await?;
    /// match score {
    ///     90..=100 => println!("Queue health: Excellent ({})", score),
    ///     70..=89 => println!("Queue health: Good ({})", score),
    ///     50..=69 => println!("Queue health: Fair ({})", score),
    ///     30..=49 => println!("Queue health: Poor ({})", score),
    ///     _ => println!("Queue health: Critical ({})", score),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn calculate_queue_health_score(&self) -> Result<i32> {
        let stats = self.get_statistics().await?;

        let mut score = 100;

        // Deduct points for high pending count (max -30 points)
        if stats.pending > 1000 {
            score -= 30;
        } else if stats.pending > 500 {
            score -= 20;
        } else if stats.pending > 100 {
            score -= 10;
        }

        // Deduct points for DLQ ratio (max -25 points)
        let total_tasks = stats.total.max(1);
        let dlq_ratio = (stats.dlq as f64 / total_tasks as f64) * 100.0;
        if dlq_ratio > 10.0 {
            score -= 25;
        } else if dlq_ratio > 5.0 {
            score -= 15;
        } else if dlq_ratio > 1.0 {
            score -= 5;
        }

        // Deduct points for stuck tasks (max -20 points)
        if stats.processing > stats.pending && stats.pending > 0 {
            score -= 20;
        } else if stats.processing > stats.pending / 2 && stats.pending > 0 {
            score -= 10;
        }

        // Deduct points for old pending tasks (max -25 points)
        if let Ok(Some(oldest_age)) = self.oldest_pending_age_secs().await {
            if oldest_age > 3600 {
                score -= 25;
            } else if oldest_age > 1800 {
                score -= 15;
            } else if oldest_age > 600 {
                score -= 10;
            }
        }

        Ok(score.max(0))
    }

    /// Get auto-scaling recommendations based on queue metrics
    ///
    /// Analyzes queue metrics and provides worker scaling recommendations.
    /// Returns (recommended_workers, reason).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let (recommended, reason) = broker.get_autoscaling_recommendation(5).await?;
    /// println!("Current workers: 5, Recommended: {}, Reason: {}", recommended, reason);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_autoscaling_recommendation(
        &self,
        current_workers: i32,
    ) -> Result<(i32, String)> {
        let stats = self.get_statistics().await?;
        let wait_time = self.estimate_wait_time().await?;

        // If queue is empty or very small, scale down
        if stats.pending < 10 {
            let recommended = (current_workers / 2).max(1);
            return Ok((recommended, "Low queue depth - scale down".to_string()));
        }

        // If wait time is very high, scale up aggressively
        if let Some(wait_secs) = wait_time {
            if wait_secs > 3600 {
                let recommended = (current_workers * 2).min(50);
                return Ok((
                    recommended,
                    format!("High wait time ({}s) - scale up aggressively", wait_secs),
                ));
            } else if wait_secs > 600 {
                let recommended = (current_workers + (current_workers / 2)).min(50);
                return Ok((
                    recommended,
                    format!("Moderate wait time ({}s) - scale up", wait_secs),
                ));
            }
        }

        // If processing rate is low compared to pending, scale up
        if stats.processing < stats.pending / 10 && stats.pending > 100 {
            let recommended = (current_workers + (current_workers / 3)).min(50);
            return Ok((
                recommended,
                "Low processing rate vs pending - scale up".to_string(),
            ));
        }

        // If DLQ is growing, might need more workers or there's an issue
        let dlq_ratio = (stats.dlq as f64 / stats.total.max(1) as f64) * 100.0;
        if dlq_ratio > 20.0 {
            return Ok((
                current_workers,
                "High DLQ ratio - investigate errors before scaling".to_string(),
            ));
        }

        // Everything looks good
        Ok((
            current_workers,
            "Queue metrics healthy - maintain current workers".to_string(),
        ))
    }

    /// Sample random tasks for monitoring without affecting processing
    ///
    /// Returns a random sample of tasks in a specific state for analysis.
    /// Does not modify task state.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::{PostgresBroker, DbTaskState};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Sample 10 random pending tasks for analysis
    /// let samples = broker.sample_tasks(DbTaskState::Pending, 10).await?;
    /// for task in samples {
    ///     println!("Task: {} (age: {:?})", task.task_name, task.created_at);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn sample_tasks(
        &self,
        state: DbTaskState,
        sample_size: i64,
    ) -> Result<Vec<TaskInfo>> {
        let rows = sqlx::query(
            r#"
            SELECT id, task_name, state, priority, retry_count, max_retries,
                   created_at, scheduled_at, started_at, completed_at, worker_id, error_message
            FROM celers_tasks
            WHERE queue_name = $1 AND state = $2
            ORDER BY RANDOM()
            LIMIT $3
            "#,
        )
        .bind(&self.queue_name)
        .bind(state.to_string())
        .bind(sample_size)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to sample tasks: {}", e)))?;

        let tasks: Vec<TaskInfo> = rows
            .iter()
            .map(|row| TaskInfo {
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
            })
            .collect();

        Ok(tasks)
    }

    /// Get aggregated statistics from task metadata
    ///
    /// Allows custom aggregations on JSONB metadata fields.
    /// Returns task counts grouped by a metadata key.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Count tasks by region
    /// let by_region = broker.aggregate_by_metadata("region", 10).await?;
    /// for (region, count) in by_region {
    ///     println!("Region {}: {} tasks", region.unwrap_or_else(|| "unknown".to_string()), count);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn aggregate_by_metadata(
        &self,
        key: &str,
        limit: i64,
    ) -> Result<Vec<(Option<String>, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT
                metadata->$2 as value,
                COUNT(*) as count
            FROM celers_tasks
            WHERE queue_name = $1
              AND metadata IS NOT NULL
              AND metadata ? $2
            GROUP BY value
            ORDER BY count DESC
            LIMIT $3
            "#,
        )
        .bind(&self.queue_name)
        .bind(key)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to aggregate metadata: {}", e)))?;

        let aggregated = rows
            .iter()
            .map(|row| {
                let value: Option<serde_json::Value> = row.get("value");
                let value_str = value.map(|v| v.to_string().trim_matches('"').to_string());
                let count: i64 = row.get("count");
                (value_str, count)
            })
            .collect();

        Ok(aggregated)
    }

    /// Store a performance baseline for comparison
    ///
    /// Stores current queue metrics as a named baseline for future comparison.
    /// Useful for capacity planning and performance regression detection.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Store baseline after optimization
    /// broker.store_performance_baseline("after_optimization").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn store_performance_baseline(&self, baseline_name: &str) -> Result<()> {
        let stats = self.get_statistics().await?;
        let throughput = self.get_throughput_stats().await?;

        let baseline_data = json!({
            "name": baseline_name,
            "timestamp": Utc::now(),
            "queue_name": self.queue_name,
            "stats": {
                "pending": stats.pending,
                "processing": stats.processing,
                "completed": stats.completed,
                "failed": stats.failed,
                "dlq": stats.dlq,
            },
            "throughput": {
                "tasks_per_hour": throughput.0,
                "tasks_per_day": throughput.1,
            }
        });

        // Store in metadata of a special marker task
        sqlx::query(
            r#"
            INSERT INTO celers_tasks
                (task_name, payload, queue_name, state, metadata)
            VALUES ('__baseline__', '[]'::jsonb, $1, 'completed', $2)
            "#,
        )
        .bind(&self.queue_name)
        .bind(baseline_data)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to store baseline: {}", e)))?;

        tracing::info!(
            baseline = baseline_name,
            queue = %self.queue_name,
            "Stored performance baseline"
        );

        Ok(())
    }

    /// Compare current metrics against a stored baseline
    ///
    /// Returns percentage differences between current metrics and a baseline.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// if let Some(comparison) = broker.compare_to_baseline("after_optimization").await? {
    ///     println!("Throughput change: {:.1}%", comparison.get("throughput_change").unwrap_or(&0.0));
    ///     println!("DLQ change: {:.1}%", comparison.get("dlq_change").unwrap_or(&0.0));
    /// } else {
    ///     println!("Baseline not found");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn compare_to_baseline(
        &self,
        baseline_name: &str,
    ) -> Result<Option<std::collections::HashMap<String, f64>>> {
        // Fetch baseline
        let baseline_row = sqlx::query(
            r#"
            SELECT metadata
            FROM celers_tasks
            WHERE queue_name = $1
              AND task_name = '__baseline__'
              AND metadata->>'name' = $2
            ORDER BY created_at DESC
            LIMIT 1
            "#,
        )
        .bind(&self.queue_name)
        .bind(baseline_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to fetch baseline: {}", e)))?;

        if baseline_row.is_none() {
            return Ok(None);
        }

        let baseline_meta: serde_json::Value = baseline_row
            .expect("baseline row checked to be Some")
            .get("metadata");

        // Get current metrics
        let current_stats = self.get_statistics().await?;
        let current_throughput = self.get_throughput_stats().await?;

        // Extract baseline values
        let baseline_dlq = baseline_meta["stats"]["dlq"].as_i64().unwrap_or(1) as f64;
        let baseline_throughput = baseline_meta["throughput"]["tasks_per_hour"]
            .as_f64()
            .unwrap_or(1.0);

        // Calculate percentage changes
        let mut comparison = std::collections::HashMap::new();

        let dlq_change =
            ((current_stats.dlq as f64 - baseline_dlq) / baseline_dlq.max(1.0)) * 100.0;
        let throughput_change = ((current_throughput.0 as f64 - baseline_throughput)
            / baseline_throughput.max(1.0))
            * 100.0;

        comparison.insert("dlq_change".to_string(), dlq_change);
        comparison.insert("throughput_change".to_string(), throughput_change);
        comparison.insert("current_pending".to_string(), current_stats.pending as f64);
        comparison.insert("current_dlq".to_string(), current_stats.dlq as f64);

        Ok(Some(comparison))
    }

    /// Get distinct task names in the queue
    ///
    /// Returns all unique task names currently in the queue.
    /// Useful for discovering task types and monitoring task diversity.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let task_types = broker.get_distinct_task_names().await?;
    /// println!("Task types in queue: {}", task_types.len());
    /// for task_name in task_types {
    ///     println!("  - {}", task_name);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_distinct_task_names(&self) -> Result<Vec<String>> {
        let rows = sqlx::query(
            r#"
            SELECT DISTINCT task_name
            FROM celers_tasks
            WHERE queue_name = $1
            ORDER BY task_name
            "#,
        )
        .bind(&self.queue_name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get distinct task names: {}", e)))?;

        let task_names = rows.iter().map(|row| row.get("task_name")).collect();

        Ok(task_names)
    }

    /// Get task count breakdown by task name
    ///
    /// Returns counts of tasks grouped by task name and state.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let breakdown = broker.get_task_breakdown_by_name(20).await?;
    /// for (task_name, pending, processing, completed, failed) in breakdown {
    ///     println!("{}: {} pending, {} processing, {} completed, {} failed",
    ///              task_name, pending, processing, completed, failed);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_task_breakdown_by_name(
        &self,
        limit: i64,
    ) -> Result<Vec<(String, i64, i64, i64, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT
                task_name,
                COUNT(*) FILTER (WHERE state = 'pending') as pending,
                COUNT(*) FILTER (WHERE state = 'processing') as processing,
                COUNT(*) FILTER (WHERE state = 'completed') as completed,
                COUNT(*) FILTER (WHERE state = 'failed') as failed
            FROM celers_tasks
            WHERE queue_name = $1
            GROUP BY task_name
            ORDER BY COUNT(*) DESC
            LIMIT $2
            "#,
        )
        .bind(&self.queue_name)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get task breakdown: {}", e)))?;

        let breakdown = rows
            .iter()
            .map(|row| {
                let task_name: String = row.get("task_name");
                let pending: i64 = row.get("pending");
                let processing: i64 = row.get("processing");
                let completed: i64 = row.get("completed");
                let failed: i64 = row.get("failed");
                (task_name, pending, processing, completed, failed)
            })
            .collect();

        Ok(breakdown)
    }

    /// Get task state transition history
    ///
    /// Returns a history of state transitions for tasks, showing how tasks move
    /// through different states over time. Useful for debugging and understanding
    /// task lifecycle patterns.
    ///
    /// # Arguments
    ///
    /// * `task_id` - Optional task ID to filter (None for all tasks)
    /// * `hours` - Number of hours to look back
    /// * `limit` - Maximum number of records to return
    ///
    /// # Returns
    ///
    /// Vector of tuples: (task_id, task_name, from_state, to_state, transition_time, duration_ms)
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Get state transitions for the last 24 hours
    /// let transitions = broker.get_state_transition_history(None, 24, 100).await?;
    /// for (task_id, task_name, from_state, to_state, transition_time, duration_ms) in transitions {
    ///     println!("{} ({}): {} -> {} in {}ms",
    ///              task_id, task_name, from_state, to_state, duration_ms.unwrap_or(0));
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_state_transition_history(
        &self,
        task_id: Option<Uuid>,
        hours: i64,
        limit: i64,
    ) -> Result<Vec<(Uuid, String, String, String, DateTime<Utc>, Option<i64>)>> {
        let query = if task_id.is_some() {
            r#"
            SELECT
                id,
                task_name,
                COALESCE(LAG(state) OVER (PARTITION BY id ORDER BY updated_at), 'created') as from_state,
                state as to_state,
                updated_at,
                EXTRACT(EPOCH FROM (updated_at - LAG(updated_at) OVER (PARTITION BY id ORDER BY updated_at))) * 1000 as duration_ms
            FROM celers_tasks
            WHERE queue_name = $1
              AND id = $2
              AND updated_at >= NOW() - INTERVAL '1 hour' * $3
            ORDER BY updated_at DESC
            LIMIT $4
            "#
        } else {
            r#"
            SELECT
                id,
                task_name,
                COALESCE(LAG(state) OVER (PARTITION BY id ORDER BY updated_at), 'created') as from_state,
                state as to_state,
                updated_at,
                EXTRACT(EPOCH FROM (updated_at - LAG(updated_at) OVER (PARTITION BY id ORDER BY updated_at))) * 1000 as duration_ms
            FROM celers_tasks
            WHERE queue_name = $1
              AND updated_at >= NOW() - INTERVAL '1 hour' * $2
            ORDER BY updated_at DESC
            LIMIT $3
            "#
        };

        let rows = if let Some(tid) = task_id {
            sqlx::query(query)
                .bind(&self.queue_name)
                .bind(tid)
                .bind(hours)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
        } else {
            sqlx::query(query)
                .bind(&self.queue_name)
                .bind(hours)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
        }
        .map_err(|e| {
            CelersError::Other(format!("Failed to get state transition history: {}", e))
        })?;

        let transitions = rows
            .iter()
            .map(|row| {
                let id: Uuid = row.get("id");
                let task_name: String = row.get("task_name");
                let from_state: String = row.get("from_state");
                let to_state: String = row.get("to_state");
                let transition_time: DateTime<Utc> = row.get("updated_at");
                let duration_ms: Option<f64> = row.try_get("duration_ms").ok();
                let duration_ms = duration_ms.map(|d| d as i64);
                (
                    id,
                    task_name,
                    from_state,
                    to_state,
                    transition_time,
                    duration_ms,
                )
            })
            .collect();

        Ok(transitions)
    }

    /// Get comprehensive lifecycle information for a task
    ///
    /// Returns detailed lifecycle metrics including time spent in each state,
    /// total retry attempts, and full timeline.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    /// use uuid::Uuid;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let task_id = Uuid::new_v4();
    ///
    /// if let Some(lifecycle) = broker.get_task_lifecycle(&task_id).await? {
    ///     println!("Task lifecycle:");
    ///     println!("  Total lifetime: {} seconds", lifecycle.total_lifetime_secs);
    ///     println!("  Time pending: {} seconds", lifecycle.time_pending_secs.unwrap_or(0));
    ///     println!("  Time processing: {} seconds", lifecycle.time_processing_secs.unwrap_or(0));
    ///     println!("  Retries: {}", lifecycle.retry_count);
    ///     println!("  Current state: {}", lifecycle.current_state);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_task_lifecycle(&self, task_id: &Uuid) -> Result<Option<TaskLifecycle>> {
        let row = sqlx::query(
            r#"
            SELECT
                id,
                task_name,
                state,
                created_at,
                started_at,
                completed_at,
                retry_count,
                EXTRACT(EPOCH FROM (COALESCE(completed_at, NOW()) - created_at)) as total_lifetime_secs,
                EXTRACT(EPOCH FROM (COALESCE(started_at, completed_at, NOW()) - created_at)) as time_pending_secs,
                EXTRACT(EPOCH FROM (COALESCE(completed_at, NOW()) - COALESCE(started_at, created_at))) as time_processing_secs,
                error_message
            FROM celers_tasks
            WHERE queue_name = $1 AND id = $2
            "#,
        )
        .bind(&self.queue_name)
        .bind(task_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get task lifecycle: {}", e)))?;

        if let Some(row) = row {
            let lifecycle = TaskLifecycle {
                task_id: row.get("id"),
                task_name: row.get("task_name"),
                current_state: row.get::<String, _>("state"),
                created_at: row.get("created_at"),
                started_at: row.try_get("started_at").ok(),
                completed_at: row.try_get("completed_at").ok(),
                retry_count: row.get("retry_count"),
                total_lifetime_secs: row.get::<f64, _>("total_lifetime_secs") as i64,
                time_pending_secs: row
                    .try_get::<f64, _>("time_pending_secs")
                    .ok()
                    .map(|v| v as i64),
                time_processing_secs: row
                    .try_get::<f64, _>("time_processing_secs")
                    .ok()
                    .map(|v| v as i64),
                error_message: row.try_get("error_message").ok(),
            };
            Ok(Some(lifecycle))
        } else {
            Ok(None)
        }
    }

    /// Detect tasks with abnormal state durations
    ///
    /// Identifies tasks that have been in a particular state for longer than expected,
    /// which may indicate issues requiring attention.
    ///
    /// # Arguments
    ///
    /// * `state` - State to check (e.g., "processing", "pending")
    /// * `threshold_secs` - Duration threshold in seconds
    /// * `limit` - Maximum number of tasks to return
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Find tasks stuck in processing for more than 1 hour
    /// let stuck_tasks = broker.detect_abnormal_state_duration("processing", 3600, 50).await?;
    /// for (task_id, task_name, duration_secs, retry_count) in stuck_tasks {
    ///     println!("Task {} stuck in processing for {} seconds (retries: {})",
    ///              task_id, duration_secs, retry_count);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn detect_abnormal_state_duration(
        &self,
        state: &str,
        threshold_secs: i64,
        limit: i64,
    ) -> Result<Vec<(Uuid, String, i64, i32)>> {
        let query = match state.to_lowercase().as_str() {
            "processing" => {
                r#"
                SELECT
                    id,
                    task_name,
                    EXTRACT(EPOCH FROM (NOW() - COALESCE(started_at, updated_at)))::BIGINT as duration_secs,
                    retry_count
                FROM celers_tasks
                WHERE queue_name = $1
                  AND state = 'processing'
                  AND EXTRACT(EPOCH FROM (NOW() - COALESCE(started_at, updated_at))) > $2
                ORDER BY duration_secs DESC
                LIMIT $3
                "#
            }
            "pending" => {
                r#"
                SELECT
                    id,
                    task_name,
                    EXTRACT(EPOCH FROM (NOW() - created_at))::BIGINT as duration_secs,
                    retry_count
                FROM celers_tasks
                WHERE queue_name = $1
                  AND state = 'pending'
                  AND EXTRACT(EPOCH FROM (NOW() - created_at)) > $2
                ORDER BY duration_secs DESC
                LIMIT $3
                "#
            }
            _ => {
                return Err(CelersError::Other(format!(
                    "Unsupported state for abnormal duration detection: {}",
                    state
                )));
            }
        };

        let rows = sqlx::query(query)
            .bind(&self.queue_name)
            .bind(threshold_secs)
            .bind(limit)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to detect abnormal state duration: {}", e))
            })?;

        let tasks = rows
            .iter()
            .map(|row| {
                let id: Uuid = row.get("id");
                let task_name: String = row.get("task_name");
                let duration_secs: i64 = row.get("duration_secs");
                let retry_count: i32 = row.get("retry_count");
                (id, task_name, duration_secs, retry_count)
            })
            .collect();

        Ok(tasks)
    }

    /// Get state transition statistics
    ///
    /// Returns statistics about how tasks transition between states,
    /// including average time spent in each state and transition counts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let stats = broker.get_state_transition_stats(24).await?;
    /// println!("Avg time pending: {} seconds", stats.avg_time_pending_secs);
    /// println!("Avg time processing: {} seconds", stats.avg_time_processing_secs);
    /// println!("Success rate: {:.2}%", stats.success_rate * 100.0);
    /// println!("Pending -> Processing: {} tasks", stats.pending_to_processing_count);
    /// println!("Processing -> Completed: {} tasks", stats.processing_to_completed_count);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_state_transition_stats(&self, hours: i64) -> Result<StateTransitionStats> {
        let row = sqlx::query(
            r#"
            WITH recent_tasks AS (
                SELECT *
                FROM celers_tasks
                WHERE queue_name = $1
                  AND created_at >= NOW() - INTERVAL '1 hour' * $2
            )
            SELECT
                AVG(EXTRACT(EPOCH FROM (COALESCE(started_at, NOW()) - created_at))) as avg_time_pending_secs,
                AVG(EXTRACT(EPOCH FROM (COALESCE(completed_at, NOW()) - COALESCE(started_at, created_at)))) as avg_time_processing_secs,
                COUNT(*) FILTER (WHERE state = 'completed' OR state = 'failed') as total_finished,
                COUNT(*) FILTER (WHERE state = 'completed') as completed_count,
                COUNT(*) FILTER (WHERE state = 'failed') as failed_count,
                COUNT(*) FILTER (WHERE started_at IS NOT NULL) as pending_to_processing_count,
                COUNT(*) FILTER (WHERE completed_at IS NOT NULL) as processing_to_completed_count,
                COUNT(*) FILTER (WHERE state = 'cancelled') as cancelled_count
            FROM recent_tasks
            "#,
        )
        .bind(&self.queue_name)
        .bind(hours)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get state transition stats: {}", e)))?;

        let avg_time_pending_secs: Option<f64> = row.try_get("avg_time_pending_secs").ok();
        let avg_time_processing_secs: Option<f64> = row.try_get("avg_time_processing_secs").ok();
        let total_finished: i64 = row.get("total_finished");
        let completed_count: i64 = row.get("completed_count");
        let failed_count: i64 = row.get("failed_count");
        let pending_to_processing_count: i64 = row.get("pending_to_processing_count");
        let processing_to_completed_count: i64 = row.get("processing_to_completed_count");
        let cancelled_count: i64 = row.get("cancelled_count");

        let success_rate = if total_finished > 0 {
            completed_count as f64 / total_finished as f64
        } else {
            0.0
        };

        Ok(StateTransitionStats {
            avg_time_pending_secs: avg_time_pending_secs.unwrap_or(0.0),
            avg_time_processing_secs: avg_time_processing_secs.unwrap_or(0.0),
            success_rate,
            pending_to_processing_count,
            processing_to_completed_count,
            completed_count,
            failed_count,
            cancelled_count,
        })
    }

    /// Auto-adjust task priorities based on age
    ///
    /// Automatically increases priority for tasks that have been pending for longer
    /// than specified thresholds. Useful for ensuring old tasks don't get starved.
    ///
    /// # Arguments
    ///
    /// * `age_threshold_secs` - Tasks older than this get priority boost
    /// * `priority_increment` - Amount to increase priority by
    ///
    /// # Returns
    ///
    /// Number of tasks whose priority was adjusted
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Boost priority of tasks pending for more than 1 hour by 10 points
    /// let adjusted = broker.auto_adjust_priority_by_age(3600, 10).await?;
    /// println!("Adjusted priority for {} tasks", adjusted);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn auto_adjust_priority_by_age(
        &self,
        age_threshold_secs: i64,
        priority_increment: i32,
    ) -> Result<i64> {
        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET priority = priority + $3
            WHERE queue_name = $1
              AND state = 'pending'
              AND EXTRACT(EPOCH FROM (NOW() - created_at)) > $2
            "#,
        )
        .bind(&self.queue_name)
        .bind(age_threshold_secs)
        .bind(priority_increment)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to auto-adjust priority by age: {}", e)))?;

        let count = result.rows_affected() as i64;
        tracing::info!(
            count = count,
            age_threshold_secs = age_threshold_secs,
            priority_increment = priority_increment,
            "Auto-adjusted task priorities by age"
        );

        Ok(count)
    }

    /// Auto-adjust task priorities based on retry count
    ///
    /// Increases priority for tasks that have been retried multiple times,
    /// helping them get processed sooner to avoid repeated failures.
    ///
    /// # Arguments
    ///
    /// * `retry_threshold` - Tasks with this many retries or more get priority boost
    /// * `priority_increment` - Amount to increase priority by
    ///
    /// # Returns
    ///
    /// Number of tasks whose priority was adjusted
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Boost priority of tasks that have been retried 3+ times by 20 points
    /// let adjusted = broker.auto_adjust_priority_by_retries(3, 20).await?;
    /// println!("Adjusted priority for {} frequently-retried tasks", adjusted);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn auto_adjust_priority_by_retries(
        &self,
        retry_threshold: i32,
        priority_increment: i32,
    ) -> Result<i64> {
        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET priority = priority + $3
            WHERE queue_name = $1
              AND state IN ('pending', 'processing')
              AND retry_count >= $2
            "#,
        )
        .bind(&self.queue_name)
        .bind(retry_threshold)
        .bind(priority_increment)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            CelersError::Other(format!("Failed to auto-adjust priority by retries: {}", e))
        })?;

        let count = result.rows_affected() as i64;
        tracing::info!(
            count = count,
            retry_threshold = retry_threshold,
            priority_increment = priority_increment,
            "Auto-adjusted task priorities by retry count"
        );

        Ok(count)
    }

    /// Apply dynamic priority strategy
    ///
    /// Applies a comprehensive priority adjustment strategy that considers multiple
    /// factors: task age, retry count, and task type importance.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    /// use celers_broker_postgres::PriorityStrategy;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let strategy = PriorityStrategy {
    ///     age_threshold_secs: 1800,  // 30 minutes
    ///     age_priority_boost: 5,
    ///     retry_threshold: 2,
    ///     retry_priority_boost: 10,
    ///     task_type_boosts: vec![
    ///         ("critical_task".to_string(), 50),
    ///         ("high_priority_task".to_string(), 25),
    ///     ],
    /// };
    ///
    /// let result = broker.apply_priority_strategy(&strategy).await?;
    /// println!("Age-based adjustments: {}", result.age_adjusted_count);
    /// println!("Retry-based adjustments: {}", result.retry_adjusted_count);
    /// println!("Type-based adjustments: {}", result.type_adjusted_count);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn apply_priority_strategy(
        &self,
        strategy: &PriorityStrategy,
    ) -> Result<PriorityStrategyResult> {
        let mut age_adjusted_count = 0;
        let mut retry_adjusted_count = 0;
        let mut type_adjusted_count = 0;

        // Apply age-based priority boost
        if strategy.age_threshold_secs > 0 && strategy.age_priority_boost != 0 {
            age_adjusted_count = self
                .auto_adjust_priority_by_age(
                    strategy.age_threshold_secs,
                    strategy.age_priority_boost,
                )
                .await?;
        }

        // Apply retry-based priority boost
        if strategy.retry_threshold > 0 && strategy.retry_priority_boost != 0 {
            retry_adjusted_count = self
                .auto_adjust_priority_by_retries(
                    strategy.retry_threshold,
                    strategy.retry_priority_boost,
                )
                .await?;
        }

        // Apply task type-specific priority boosts
        for (task_name, priority_boost) in &strategy.task_type_boosts {
            let result = sqlx::query(
                r#"
                UPDATE celers_tasks
                SET priority = priority + $3
                WHERE queue_name = $1
                  AND task_name = $2
                  AND state = 'pending'
                "#,
            )
            .bind(&self.queue_name)
            .bind(task_name)
            .bind(priority_boost)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to apply task type priority boost: {}", e))
            })?;

            type_adjusted_count += result.rows_affected() as i64;
        }

        tracing::info!(
            age_adjusted = age_adjusted_count,
            retry_adjusted = retry_adjusted_count,
            type_adjusted = type_adjusted_count,
            "Applied priority strategy"
        );

        Ok(PriorityStrategyResult {
            age_adjusted_count,
            retry_adjusted_count,
            type_adjusted_count,
            total_adjusted: age_adjusted_count + retry_adjusted_count + type_adjusted_count,
        })
    }
}
