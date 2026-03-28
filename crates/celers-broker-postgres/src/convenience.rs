//! Convenience helper methods for common patterns

use celers_core::{Broker, CelersError, Result, SerializedTask, TaskId};
use sqlx::Row;
use std::time::Duration;

use crate::types::{DbTaskState, DlqTaskInfo, TaskInfo};
use crate::PostgresBroker;

/// Convenience helper methods for common patterns
impl PostgresBroker {
    /// Enqueue multiple tasks with the same configuration
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # use celers_core::Broker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let task_ids = broker.enqueue_many(
    ///     "process_user",
    ///     vec![vec![1], vec![2], vec![3]],  // payloads
    ///     Some(5),  // priority
    ///     Some(3),  // max_retries
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn enqueue_many(
        &self,
        task_name: &str,
        payloads: Vec<Vec<u8>>,
        priority: Option<i32>,
        max_retries: Option<u32>,
    ) -> Result<Vec<TaskId>> {
        let mut tasks = Vec::new();

        for payload in payloads {
            let mut task = SerializedTask::new(task_name.to_string(), payload);
            if let Some(p) = priority {
                task.metadata.priority = p;
            }
            if let Some(r) = max_retries {
                task.metadata.max_retries = r;
            }
            tasks.push(task);
        }

        self.enqueue_batch(tasks).await
    }

    /// Process a single task with automatic ack/reject
    ///
    /// Returns Some((task, ack_fn, reject_fn)) if a task is available, None otherwise
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// if let Some((task, ack, reject)) = broker.dequeue_with_handlers().await? {
    ///     match process_task(&task).await {
    ///         Ok(_) => ack().await?,
    ///         Err(e) => reject(&e.to_string()).await?,
    ///     }
    /// }
    /// # async fn process_task(_: &celers_core::SerializedTask) -> Result<(), Box<dyn std::error::Error>> { Ok(()) }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn dequeue_with_handlers(
        &self,
    ) -> Result<
        Option<(
            SerializedTask,
            Box<
                dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
                    + Send,
            >,
            Box<
                dyn Fn(
                        &str,
                    )
                        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
                    + Send,
            >,
        )>,
    > {
        if let Some(msg) = self.dequeue().await? {
            let task_id = msg.task.metadata.id;
            let receipt = msg.receipt_handle.clone();
            let broker1 = self.pool.clone();
            let broker2 = self.pool.clone();

            let ack_fn = Box::new(move || {
                let pool = broker1.clone();
                let id = task_id;
                let _receipt_clone = receipt.clone();
                Box::pin(async move {
                    sqlx::query("UPDATE celers_tasks SET state = 'completed', completed_at = NOW() WHERE id = $1")
                        .bind(id)
                        .execute(&pool)
                        .await
                        .map_err(|e| CelersError::Other(format!("Ack failed: {}", e)))?;
                    Ok(())
                })
                    as std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
            });

            let reject_fn = Box::new(move |_error: &str| {
                let pool = broker2.clone();
                let id = task_id;
                Box::pin(async move {
                    sqlx::query(
                        "UPDATE celers_tasks SET retry_count = retry_count + 1 WHERE id = $1",
                    )
                    .bind(id)
                    .execute(&pool)
                    .await
                    .map_err(|e| CelersError::Other(format!("Reject failed: {}", e)))?;
                    Ok(())
                })
                    as std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
            });

            Ok(Some((msg.task, ack_fn, reject_fn)))
        } else {
            Ok(None)
        }
    }

    /// Get a summary of queue health in a single call
    ///
    /// Returns (pending, processing, dlq, oldest_task_age_secs, success_rate)
    pub async fn get_queue_health_summary(&self) -> Result<(i64, i64, i64, Option<i64>, f64)> {
        let stats = self.get_statistics().await?;
        let oldest = self.oldest_pending_age_secs().await.unwrap_or(None);
        let success = self.success_rate().await.unwrap_or(0.0);

        Ok((stats.pending, stats.processing, stats.dlq, oldest, success))
    }

    /// Purge all completed tasks older than specified age
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # use std::time::Duration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// // Purge completed tasks older than 7 days
    /// let purged = broker.purge_old_completed(Duration::from_secs(7 * 24 * 3600)).await?;
    /// println!("Purged {} old completed tasks", purged);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn purge_old_completed(&self, older_than: Duration) -> Result<i64> {
        let cutoff = chrono::Utc::now()
            - chrono::Duration::from_std(older_than)
                .map_err(|e| CelersError::Other(format!("Invalid duration: {}", e)))?;

        let result =
            sqlx::query("DELETE FROM celers_tasks WHERE state = 'completed' AND completed_at < $1")
                .bind(cutoff)
                .execute(&self.pool)
                .await
                .map_err(|e| CelersError::Other(format!("Purge failed: {}", e)))?;

        Ok(result.rows_affected() as i64)
    }

    /// Cancel all tasks matching a specific task name
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let cancelled = broker.cancel_by_name("slow_task").await?;
    /// println!("Cancelled {} tasks", cancelled);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn cancel_by_name(&self, task_name: &str) -> Result<i64> {
        let result = sqlx::query(
            "UPDATE celers_tasks
             SET state = 'cancelled', completed_at = NOW()
             WHERE task_name = $1 AND state IN ('pending', 'processing')",
        )
        .bind(task_name)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Cancel failed: {}", e)))?;

        Ok(result.rows_affected() as i64)
    }

    /// Wait for a specific task to complete (with timeout)
    ///
    /// Returns true if task completed, false if timeout reached
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # use celers_core::Broker;
    /// # use std::time::Duration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// # let task = celers_core::SerializedTask::new("test".to_string(), vec![]);
    /// let task_id = broker.enqueue(task).await?;
    ///
    /// // Wait up to 30 seconds for completion
    /// if broker.wait_for_completion(&task_id, Duration::from_secs(30)).await? {
    ///     println!("Task completed!");
    /// } else {
    ///     println!("Task timed out");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn wait_for_completion(&self, task_id: &TaskId, timeout: Duration) -> Result<bool> {
        let start = std::time::Instant::now();
        let poll_interval = Duration::from_millis(100);

        while start.elapsed() < timeout {
            if let Some(task_info) = self.get_task(task_id).await? {
                match task_info.state {
                    DbTaskState::Completed | DbTaskState::Failed | DbTaskState::Cancelled => {
                        return Ok(true);
                    }
                    _ => {}
                }
            }

            tokio::time::sleep(poll_interval).await;
        }

        Ok(false)
    }

    /// Get count of tasks in each state as a HashMap
    pub async fn get_state_counts(&self) -> Result<std::collections::HashMap<String, i64>> {
        let stats = self.get_statistics().await?;

        let mut counts = std::collections::HashMap::new();
        counts.insert("pending".to_string(), stats.pending);
        counts.insert("processing".to_string(), stats.processing);
        counts.insert("completed".to_string(), stats.completed);
        counts.insert("failed".to_string(), stats.failed);
        counts.insert("cancelled".to_string(), stats.cancelled);
        counts.insert("dlq".to_string(), stats.dlq);

        Ok(counts)
    }

    /// Retry all tasks currently in the DLQ
    ///
    /// Returns the number of tasks requeued
    pub async fn retry_all_dlq(&self) -> Result<i64> {
        let dlq_tasks = self.list_dlq(1000, 0).await?;
        let mut count = 0;

        for dlq_task in dlq_tasks {
            if self.requeue_from_dlq(&dlq_task.id).await.is_ok() {
                count += 1;
            }
        }

        Ok(count)
    }

    /// Find tasks within a specific priority range
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// // Find high-priority tasks (priority >= 5)
    /// let high_priority_tasks = broker.find_tasks_by_priority_range(5, 10, 100).await?;
    /// println!("Found {} high-priority tasks", high_priority_tasks.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn find_tasks_by_priority_range(
        &self,
        min_priority: i32,
        max_priority: i32,
        limit: i64,
    ) -> Result<Vec<TaskInfo>> {
        let rows = sqlx::query(&format!(
            "SELECT id, task_name, state, priority, retry_count, max_retries,
                        created_at, scheduled_at, started_at, completed_at, worker_id, error_message
                 FROM {}
                 WHERE priority BETWEEN $1 AND $2 AND state = 'pending'
                 ORDER BY priority DESC, created_at ASC
                 LIMIT $3",
            self.queue_name
        ))
        .bind(min_priority)
        .bind(max_priority)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to find tasks by priority: {}", e)))?;

        let mut tasks = Vec::new();
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

    /// Cancel all pending tasks older than specified age
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # use std::time::Duration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// // Cancel pending tasks older than 24 hours
    /// let cancelled = broker.cancel_old_pending(Duration::from_secs(24 * 3600)).await?;
    /// println!("Cancelled {} old pending tasks", cancelled);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn cancel_old_pending(&self, older_than: Duration) -> Result<i64> {
        let cutoff = chrono::Utc::now()
            - chrono::Duration::from_std(older_than)
                .map_err(|e| CelersError::Other(format!("Invalid duration: {}", e)))?;

        let result = sqlx::query(&format!(
            "UPDATE {} SET state = 'cancelled', completed_at = NOW()
                 WHERE state = 'pending' AND created_at < $1",
            self.queue_name
        ))
        .bind(cutoff)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Cancel old pending failed: {}", e)))?;

        Ok(result.rows_affected() as i64)
    }

    /// Batch cancel multiple tasks by their IDs
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # use uuid::Uuid;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let task_ids = vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
    /// let cancelled = broker.batch_cancel(&task_ids).await?;
    /// println!("Cancelled {} tasks", cancelled);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn batch_cancel(&self, task_ids: &[TaskId]) -> Result<i64> {
        if task_ids.is_empty() {
            return Ok(0);
        }

        let result = sqlx::query(&format!(
            "UPDATE {} SET state = 'cancelled', completed_at = NOW()
                 WHERE id = ANY($1) AND state IN ('pending', 'processing')",
            self.queue_name
        ))
        .bind(task_ids)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Batch cancel failed: {}", e)))?;

        Ok(result.rows_affected() as i64)
    }

    /// Find tasks that have been processing for longer than the threshold
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # use std::time::Duration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// // Find tasks processing for more than 1 hour
    /// let stuck_tasks = broker.find_stuck_tasks(Duration::from_secs(3600)).await?;
    /// println!("Found {} stuck tasks", stuck_tasks.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn find_stuck_tasks(&self, threshold: Duration) -> Result<Vec<TaskInfo>> {
        let cutoff = chrono::Utc::now()
            - chrono::Duration::from_std(threshold)
                .map_err(|e| CelersError::Other(format!("Invalid duration: {}", e)))?;

        let rows = sqlx::query(&format!(
            "SELECT id, task_name, state, priority, retry_count, max_retries,
                        created_at, scheduled_at, started_at, completed_at, worker_id, error_message
                 FROM {}
                 WHERE state = 'processing' AND started_at < $1
                 ORDER BY started_at ASC",
            self.queue_name
        ))
        .bind(cutoff)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to find stuck tasks: {}", e)))?;

        let mut tasks = Vec::new();
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

    /// Automatically requeue stuck tasks (processing too long)
    ///
    /// Returns the number of tasks requeued
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # use std::time::Duration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// // Requeue tasks stuck for more than 2 hours
    /// let requeued = broker.requeue_stuck_tasks(Duration::from_secs(2 * 3600)).await?;
    /// println!("Requeued {} stuck tasks", requeued);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn requeue_stuck_tasks(&self, threshold: Duration) -> Result<i64> {
        let cutoff = chrono::Utc::now()
            - chrono::Duration::from_std(threshold)
                .map_err(|e| CelersError::Other(format!("Invalid duration: {}", e)))?;

        let result = sqlx::query(&format!(
            "UPDATE {} SET state = 'pending', started_at = NULL, worker_id = NULL
                 WHERE state = 'processing' AND started_at < $1",
            self.queue_name
        ))
        .bind(cutoff)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Requeue stuck tasks failed: {}", e)))?;

        Ok(result.rows_affected() as i64)
    }

    /// Get queue depth grouped by priority level
    ///
    /// Returns a HashMap mapping priority to task count
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let depth_by_priority = broker.get_queue_depth_by_priority().await?;
    /// for (priority, count) in depth_by_priority {
    ///     println!("Priority {}: {} tasks", priority, count);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_queue_depth_by_priority(&self) -> Result<std::collections::HashMap<i32, i64>> {
        let rows = sqlx::query(&format!(
            "SELECT priority, COUNT(*) as count
                 FROM {}
                 WHERE state = 'pending'
                 GROUP BY priority
                 ORDER BY priority DESC",
            self.queue_name
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get queue depth: {}", e)))?;

        let mut depth_map = std::collections::HashMap::new();
        for row in rows {
            let priority: i32 = row.get("priority");
            let count: i64 = row.get("count");
            depth_map.insert(priority, count);
        }

        Ok(depth_map)
    }

    /// Get throughput statistics (completed tasks per time period)
    ///
    /// Returns (tasks_last_hour, tasks_last_day, avg_tasks_per_hour)
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let (last_hour, last_day, avg_per_hour) = broker.get_throughput_stats().await?;
    /// println!("Throughput: {} tasks/hour (last hour: {}, last day: {})",
    ///          avg_per_hour, last_hour, last_day);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_throughput_stats(&self) -> Result<(i64, i64, f64)> {
        let one_hour_ago = chrono::Utc::now() - chrono::Duration::hours(1);
        let one_day_ago = chrono::Utc::now() - chrono::Duration::days(1);

        // Tasks completed in last hour
        let last_hour: i64 = sqlx::query_scalar(&format!(
            "SELECT COUNT(*) FROM {} WHERE state = 'completed' AND completed_at > $1",
            self.queue_name
        ))
        .bind(one_hour_ago)
        .fetch_one(&self.pool)
        .await
        .unwrap_or(0);

        // Tasks completed in last day
        let last_day: i64 = sqlx::query_scalar(&format!(
            "SELECT COUNT(*) FROM {} WHERE state = 'completed' AND completed_at > $1",
            self.queue_name
        ))
        .bind(one_day_ago)
        .fetch_one(&self.pool)
        .await
        .unwrap_or(0);

        // Average per hour over last day
        let avg_per_hour = if last_day > 0 {
            last_day as f64 / 24.0
        } else {
            0.0
        };

        Ok((last_hour, last_day, avg_per_hour))
    }

    /// Get average task duration by task name
    ///
    /// Returns HashMap of task name to average duration in milliseconds
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let avg_durations = broker.get_avg_task_duration_by_name().await?;
    /// for (task_name, duration_ms) in avg_durations {
    ///     println!("{}: {:.2}ms average", task_name, duration_ms);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_avg_task_duration_by_name(
        &self,
    ) -> Result<std::collections::HashMap<String, f64>> {
        let rows = sqlx::query(
            &format!(
                "SELECT task_name,
                        AVG(EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000) as avg_duration_ms
                 FROM {}
                 WHERE state = 'completed' AND started_at IS NOT NULL AND completed_at IS NOT NULL
                 GROUP BY task_name
                 ORDER BY avg_duration_ms DESC",
                self.queue_name
            )
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get avg duration: {}", e)))?;

        let mut duration_map = std::collections::HashMap::new();
        for row in rows {
            let task_name: String = row.get("task_name");
            let avg_duration: Option<f64> = row.get("avg_duration_ms");
            if let Some(duration) = avg_duration {
                duration_map.insert(task_name, duration);
            }
        }

        Ok(duration_map)
    }

    // ========== Task TTL (Time To Live) ==========

    /// Set TTL (Time To Live) for tasks by task name
    ///
    /// Tasks older than the specified TTL will be automatically expired and moved to cancelled state.
    /// This is useful for preventing old tasks from being processed when they're no longer relevant.
    ///
    /// # Arguments
    ///
    /// * `task_name` - Name of the task type to expire
    /// * `ttl_secs` - TTL in seconds (tasks older than this will be expired)
    ///
    /// # Returns
    ///
    /// Number of tasks that were expired
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Expire pending tasks older than 1 hour for "email_notifications" task type
    /// let expired = broker.expire_tasks_by_ttl("email_notifications", 3600).await?;
    /// println!("Expired {} old tasks", expired);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn expire_tasks_by_ttl(&self, task_name: &str, ttl_secs: i64) -> Result<i64> {
        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = 'cancelled',
                completed_at = NOW(),
                error_message = 'Task expired due to TTL'
            WHERE task_name = $1
              AND queue_name = $2
              AND state IN ('pending', 'processing')
              AND created_at < NOW() - INTERVAL '1 second' * $3
            "#,
        )
        .bind(task_name)
        .bind(&self.queue_name)
        .bind(ttl_secs)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to expire tasks: {}", e)))?;

        let expired = result.rows_affected() as i64;

        if expired > 0 {
            tracing::info!(
                task_name = task_name,
                ttl_secs = ttl_secs,
                expired = expired,
                "Expired tasks due to TTL"
            );
        }

        Ok(expired)
    }

    /// Expire all pending tasks older than specified TTL
    ///
    /// This is a global TTL that applies to all task types.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Expire all pending tasks older than 24 hours
    /// let expired = broker.expire_all_tasks_by_ttl(86400).await?;
    /// println!("Expired {} old tasks", expired);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn expire_all_tasks_by_ttl(&self, ttl_secs: i64) -> Result<i64> {
        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = 'cancelled',
                completed_at = NOW(),
                error_message = 'Task expired due to TTL'
            WHERE queue_name = $1
              AND state IN ('pending', 'processing')
              AND created_at < NOW() - INTERVAL '1 second' * $2
            "#,
        )
        .bind(&self.queue_name)
        .bind(ttl_secs)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to expire all tasks: {}", e)))?;

        let expired = result.rows_affected() as i64;

        if expired > 0 {
            tracing::info!(
                ttl_secs = ttl_secs,
                expired = expired,
                "Expired all tasks due to TTL"
            );
        }

        Ok(expired)
    }

    // ========== PostgreSQL Advisory Locks ==========

    /// Acquire PostgreSQL advisory lock for exclusive task processing
    ///
    /// Advisory locks provide application-level distributed locking using PostgreSQL's
    /// advisory lock mechanism. This is useful when you need to ensure only one worker
    /// processes tasks of a specific type at a time.
    ///
    /// The lock is automatically released when the connection is closed or when
    /// `release_advisory_lock` is called.
    ///
    /// # Arguments
    ///
    /// * `lock_id` - Numeric lock ID (must be i64)
    ///
    /// # Returns
    ///
    /// `true` if lock was acquired, `false` if already locked by another session
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Try to acquire lock for critical section
    /// let lock_id = 12345i64;
    /// if broker.try_advisory_lock(lock_id).await? {
    ///     // Process critical task
    ///     println!("Lock acquired, processing...");
    ///
    ///     // Release lock when done
    ///     broker.release_advisory_lock(lock_id).await?;
    /// } else {
    ///     println!("Lock held by another worker");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn try_advisory_lock(&self, lock_id: i64) -> Result<bool> {
        let row = sqlx::query("SELECT pg_try_advisory_lock($1) as locked")
            .bind(lock_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to acquire advisory lock: {}", e)))?;

        let locked: bool = row.get("locked");

        if locked {
            tracing::debug!(lock_id = lock_id, "Advisory lock acquired");
        } else {
            tracing::debug!(lock_id = lock_id, "Advisory lock already held");
        }

        Ok(locked)
    }

    /// Release PostgreSQL advisory lock
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// broker.release_advisory_lock(12345).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn release_advisory_lock(&self, lock_id: i64) -> Result<bool> {
        let row = sqlx::query("SELECT pg_advisory_unlock($1) as unlocked")
            .bind(lock_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to release advisory lock: {}", e)))?;

        let unlocked: bool = row.get("unlocked");

        if unlocked {
            tracing::debug!(lock_id = lock_id, "Advisory lock released");
        } else {
            tracing::warn!(lock_id = lock_id, "Advisory lock was not held");
        }

        Ok(unlocked)
    }

    /// Acquire blocking advisory lock (waits until available)
    ///
    /// Unlike `try_advisory_lock`, this method will block until the lock becomes available.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Wait for lock to become available
    /// broker.advisory_lock(12345).await?;
    /// // Process critical section
    /// broker.release_advisory_lock(12345).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn advisory_lock(&self, lock_id: i64) -> Result<()> {
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(lock_id)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to acquire blocking advisory lock: {}", e))
            })?;

        tracing::debug!(lock_id = lock_id, "Blocking advisory lock acquired");

        Ok(())
    }

    /// Check if an advisory lock is currently held
    ///
    /// Note: This checks across all sessions, not just the current one.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let is_locked = broker.is_advisory_lock_held(12345).await?;
    /// println!("Lock held: {}", is_locked);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn is_advisory_lock_held(&self, lock_id: i64) -> Result<bool> {
        let row = sqlx::query(
            r#"
            SELECT COUNT(*) > 0 as held
            FROM pg_locks
            WHERE locktype = 'advisory'
              AND objid = $1
            "#,
        )
        .bind(lock_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to check advisory lock: {}", e)))?;

        let held: bool = row.get("held");
        Ok(held)
    }

    // ========== Task Performance Analytics ==========

    /// Get task performance percentiles for a specific task type
    ///
    /// Returns performance percentiles (p50, p95, p99) for task execution times.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let (p50, p95, p99) = broker.get_task_percentiles("send_email").await?;
    /// println!("p50: {}ms, p95: {}ms, p99: {}ms", p50, p95, p99);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_task_percentiles(&self, task_name: &str) -> Result<(f64, f64, f64)> {
        let row = sqlx::query(
            r#"
            SELECT
                PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY
                    EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000
                ) as p50,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY
                    EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000
                ) as p95,
                PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY
                    EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000
                ) as p99
            FROM celers_tasks
            WHERE task_name = $1
              AND queue_name = $2
              AND state = 'completed'
              AND started_at IS NOT NULL
              AND completed_at IS NOT NULL
              AND completed_at > started_at
            "#,
        )
        .bind(task_name)
        .bind(&self.queue_name)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get task percentiles: {}", e)))?;

        let p50: Option<f64> = row.get("p50");
        let p95: Option<f64> = row.get("p95");
        let p99: Option<f64> = row.get("p99");

        Ok((p50.unwrap_or(0.0), p95.unwrap_or(0.0), p99.unwrap_or(0.0)))
    }

    /// Get slowest tasks in the queue
    ///
    /// Returns the slowest N tasks based on execution time.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let slow_tasks = broker.get_slowest_tasks(10).await?;
    /// for task in slow_tasks {
    ///     if let (Some(started), Some(completed)) = (task.started_at, task.completed_at) {
    ///         let duration_ms = (completed - started).num_milliseconds();
    ///         println!("{}: {}ms", task.task_name, duration_ms);
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_slowest_tasks(&self, limit: i64) -> Result<Vec<TaskInfo>> {
        let rows = sqlx::query(
            r#"
            SELECT
                id, task_name, state, priority, retry_count, max_retries,
                created_at, scheduled_at, started_at, completed_at,
                worker_id, error_message,
                EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000 as runtime_ms
            FROM celers_tasks
            WHERE queue_name = $1
              AND state = 'completed'
              AND started_at IS NOT NULL
              AND completed_at IS NOT NULL
              AND completed_at > started_at
            ORDER BY (completed_at - started_at) DESC
            LIMIT $2
            "#,
        )
        .bind(&self.queue_name)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get slowest tasks: {}", e)))?;

        let tasks: Result<Vec<TaskInfo>> = rows
            .iter()
            .map(|row| {
                let state_str: String = row.get("state");
                Ok(TaskInfo {
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
                })
            })
            .collect();

        tasks
    }

    // ========== Rate Limiting ==========

    /// Get task processing rate for a specific task type
    ///
    /// Returns the number of tasks processed in the last N seconds.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Get tasks processed in last 60 seconds
    /// let rate = broker.get_task_rate("send_email", 60).await?;
    /// println!("Processed {} send_email tasks in last 60 seconds", rate);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_task_rate(&self, task_name: &str, window_secs: i64) -> Result<i64> {
        let row = sqlx::query(
            r#"
            SELECT COUNT(*) as count
            FROM celers_tasks
            WHERE task_name = $1
              AND queue_name = $2
              AND state = 'completed'
              AND completed_at > NOW() - INTERVAL '1 second' * $3
            "#,
        )
        .bind(task_name)
        .bind(&self.queue_name)
        .bind(window_secs)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get task rate: {}", e)))?;

        let count: i64 = row.get("count");
        Ok(count)
    }

    /// Check if task processing rate exceeds limit
    ///
    /// Returns true if the rate limit is exceeded.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Check if more than 100 emails sent in last 60 seconds
    /// if broker.is_rate_limited("send_email", 100, 60).await? {
    ///     println!("Rate limit exceeded, throttling...");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn is_rate_limited(
        &self,
        task_name: &str,
        max_count: i64,
        window_secs: i64,
    ) -> Result<bool> {
        let current_rate = self.get_task_rate(task_name, window_secs).await?;
        Ok(current_rate >= max_count)
    }

    // ========== Dynamic Priority Management ==========

    /// Boost priority of pending tasks by task name
    ///
    /// Increases the priority of all pending tasks of a specific type by the specified amount.
    /// Higher priority values mean higher priority (will be dequeued first).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Boost priority of critical alerts by 100
    /// let boosted = broker.boost_task_priority("critical_alert", 100).await?;
    /// println!("Boosted {} critical alert tasks", boosted);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn boost_task_priority(&self, task_name: &str, boost_amount: i32) -> Result<i64> {
        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET priority = priority + $1
            WHERE task_name = $2
              AND queue_name = $3
              AND state = 'pending'
            "#,
        )
        .bind(boost_amount)
        .bind(task_name)
        .bind(&self.queue_name)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to boost task priority: {}", e)))?;

        let boosted = result.rows_affected() as i64;

        if boosted > 0 {
            tracing::info!(
                task_name = task_name,
                boost_amount = boost_amount,
                boosted = boosted,
                "Boosted task priority"
            );
        }

        Ok(boosted)
    }

    /// Set absolute priority for specific tasks
    ///
    /// Sets the priority to an absolute value for tasks matching the criteria.
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
    /// let task_ids = vec![Uuid::new_v4(), Uuid::new_v4()];
    /// let updated = broker.set_task_priority(&task_ids, 999).await?;
    /// println!("Set {} tasks to highest priority", updated);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn set_task_priority(
        &self,
        task_ids: &[uuid::Uuid],
        new_priority: i32,
    ) -> Result<i64> {
        if task_ids.is_empty() {
            return Ok(0);
        }

        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET priority = $1
            WHERE id = ANY($2)
              AND queue_name = $3
              AND state = 'pending'
            "#,
        )
        .bind(new_priority)
        .bind(task_ids)
        .bind(&self.queue_name)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to set task priority: {}", e)))?;

        let updated = result.rows_affected() as i64;

        if updated > 0 {
            tracing::info!(
                count = updated,
                new_priority = new_priority,
                "Updated task priorities"
            );
        }

        Ok(updated)
    }

    // ========== Enhanced DLQ Analytics ==========

    /// Get DLQ task statistics grouped by task name
    ///
    /// Returns a map of task names to their DLQ counts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let stats = broker.get_dlq_stats_by_task().await?;
    /// for (task_name, count) in stats {
    ///     println!("{}: {} failed tasks", task_name, count);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_dlq_stats_by_task(&self) -> Result<std::collections::HashMap<String, i64>> {
        let rows = sqlx::query(
            r#"
            SELECT task_name, COUNT(*) as count
            FROM celers_dlq
            WHERE queue_name = $1
            GROUP BY task_name
            ORDER BY count DESC
            "#,
        )
        .bind(&self.queue_name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get DLQ stats: {}", e)))?;

        let mut stats = std::collections::HashMap::new();
        for row in rows {
            let task_name: String = row.get("task_name");
            let count: i64 = row.get("count");
            stats.insert(task_name, count);
        }

        Ok(stats)
    }

    /// Get most common error messages from DLQ
    ///
    /// Returns the top N most common error messages with their counts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let errors = broker.get_dlq_error_patterns(10).await?;
    /// for (error_msg, count) in errors {
    ///     println!("{} occurrences: {}", count, error_msg.unwrap_or_else(|| "Unknown".to_string()));
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_dlq_error_patterns(&self, limit: i64) -> Result<Vec<(Option<String>, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT error_message, COUNT(*) as count
            FROM celers_dlq
            WHERE queue_name = $1
            GROUP BY error_message
            ORDER BY count DESC
            LIMIT $2
            "#,
        )
        .bind(&self.queue_name)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get DLQ error patterns: {}", e)))?;

        let patterns: Vec<(Option<String>, i64)> = rows
            .iter()
            .map(|row| {
                let error_message: Option<String> = row.get("error_message");
                let count: i64 = row.get("count");
                (error_message, count)
            })
            .collect();

        Ok(patterns)
    }

    /// Get DLQ tasks that failed recently
    ///
    /// Returns DLQ tasks that failed within the specified time window.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Get tasks that failed in last hour
    /// let recent_failures = broker.get_recent_dlq_tasks(3600).await?;
    /// println!("Recent failures: {}", recent_failures.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_recent_dlq_tasks(&self, window_secs: i64) -> Result<Vec<DlqTaskInfo>> {
        let rows = sqlx::query(
            r#"
            SELECT id, task_id, task_name, retry_count, error_message, failed_at
            FROM celers_dlq
            WHERE queue_name = $1
              AND failed_at > NOW() - INTERVAL '1 second' * $2
            ORDER BY failed_at DESC
            "#,
        )
        .bind(&self.queue_name)
        .bind(window_secs)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get recent DLQ tasks: {}", e)))?;

        let tasks: Vec<DlqTaskInfo> = rows
            .iter()
            .map(|row| DlqTaskInfo {
                id: row.get("id"),
                task_id: row.get("task_id"),
                task_name: row.get("task_name"),
                retry_count: row.get("retry_count"),
                error_message: row.get("error_message"),
                failed_at: row.get("failed_at"),
            })
            .collect();

        Ok(tasks)
    }

    // ========== Task Cancellation with Reasons ==========

    /// Cancel task with a specific reason
    ///
    /// Cancels a task and records the cancellation reason in the error_message field.
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
    /// let task_id = Uuid::new_v4();
    /// broker.cancel_with_reason(&task_id, "User requested cancellation").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn cancel_with_reason(&self, task_id: &uuid::Uuid, reason: &str) -> Result<()> {
        sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = 'cancelled',
                completed_at = NOW(),
                error_message = $1
            WHERE id = $2
              AND queue_name = $3
              AND state IN ('pending', 'processing')
            "#,
        )
        .bind(format!("Cancelled: {}", reason))
        .bind(task_id)
        .bind(&self.queue_name)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to cancel task with reason: {}", e)))?;

        tracing::info!(
            task_id = %task_id,
            reason = reason,
            "Task cancelled with reason"
        );

        Ok(())
    }

    /// Cancel multiple tasks with a reason
    ///
    /// Batch cancellation with a specified reason.
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
    /// let task_ids = vec![Uuid::new_v4(), Uuid::new_v4()];
    /// let cancelled = broker.cancel_batch_with_reason(&task_ids, "System shutdown").await?;
    /// println!("Cancelled {} tasks", cancelled);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn cancel_batch_with_reason(
        &self,
        task_ids: &[uuid::Uuid],
        reason: &str,
    ) -> Result<i64> {
        if task_ids.is_empty() {
            return Ok(0);
        }

        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = 'cancelled',
                completed_at = NOW(),
                error_message = $1
            WHERE id = ANY($2)
              AND queue_name = $3
              AND state IN ('pending', 'processing')
            "#,
        )
        .bind(format!("Cancelled: {}", reason))
        .bind(task_ids)
        .bind(&self.queue_name)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to cancel tasks with reason: {}", e)))?;

        let cancelled = result.rows_affected() as i64;

        if cancelled > 0 {
            tracing::info!(
                count = cancelled,
                reason = reason,
                "Tasks cancelled with reason"
            );
        }

        Ok(cancelled)
    }

    /// Get cancellation reasons for cancelled tasks
    ///
    /// Returns a breakdown of cancellation reasons and their counts.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let reasons = broker.get_cancellation_reasons(10).await?;
    /// for (reason, count) in reasons {
    ///     println!("{}: {} cancellations", reason.unwrap_or_else(|| "Unknown".to_string()), count);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_cancellation_reasons(&self, limit: i64) -> Result<Vec<(Option<String>, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT error_message, COUNT(*) as count
            FROM celers_tasks
            WHERE queue_name = $1
              AND state = 'cancelled'
              AND error_message IS NOT NULL
            GROUP BY error_message
            ORDER BY count DESC
            LIMIT $2
            "#,
        )
        .bind(&self.queue_name)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get cancellation reasons: {}", e)))?;

        let reasons: Vec<(Option<String>, i64)> = rows
            .iter()
            .map(|row| {
                let reason: Option<String> = row.get("error_message");
                let count: i64 = row.get("count");
                (reason, count)
            })
            .collect();

        Ok(reasons)
    }
}
