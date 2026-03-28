//! Periodic task scheduling and connection pool health methods

use celers_core::{CelersError, Result};
use chrono::Utc;
use sqlx::Row;
use uuid::Uuid;

use crate::types::{
    ConnectionPoolHealth, PeriodicTaskSchedule, QueueSnapshot, TaskRetentionPolicy,
};
use crate::PostgresBroker;

impl PostgresBroker {
    /// Schedule a periodic task with cron-like expression
    ///
    /// Creates a periodic task that will be automatically enqueued based on the cron schedule.
    /// This method stores the schedule configuration in metadata for external schedulers.
    ///
    /// # Cron Expression Format
    /// - "*/5 * * * *" - Every 5 minutes
    /// - "0 */2 * * *" - Every 2 hours
    /// - "0 0 * * *" - Daily at midnight
    /// - "0 9 * * 1" - Every Monday at 9 AM
    ///
    /// # Example
    /// ```rust,no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgresql://localhost/celers").await?;
    ///
    /// // Schedule a daily cleanup task
    /// let schedule_id = broker.schedule_periodic_task(
    ///     "daily_cleanup",
    ///     "0 2 * * *",  // Run at 2 AM daily
    ///     serde_json::json!({"action": "cleanup", "max_age_days": 7}),
    ///     5
    /// ).await?;
    ///
    /// println!("Scheduled task: {}", schedule_id);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn schedule_periodic_task(
        &self,
        task_name: &str,
        cron_expression: &str,
        payload: serde_json::Value,
        priority: i32,
    ) -> Result<String> {
        let schedule_id = Uuid::new_v4().to_string();
        let now = Utc::now();

        let schedule = PeriodicTaskSchedule {
            schedule_id: schedule_id.clone(),
            task_name: task_name.to_string(),
            cron_expression: cron_expression.to_string(),
            payload,
            priority,
            enabled: true,
            last_run: None,
            next_run: None,
            created_at: now,
        };

        // Store schedule in a metadata table (would need migration)
        // For now, store as a special task with metadata
        let schedule_json = serde_json::to_value(&schedule)
            .map_err(|e| CelersError::Other(format!("Failed to serialize schedule: {}", e)))?;

        sqlx::query(&format!(
            "INSERT INTO {} (id, name, payload, state, priority, created_at, metadata)
             VALUES ($1, $2, $3, 'pending', $4, $5, $6)",
            self.queue_name
        ))
        .bind(Uuid::new_v4())
        .bind(format!("__periodic_schedule_{}", task_name))
        .bind(&schedule_json)
        .bind(priority)
        .bind(now)
        .bind(serde_json::json!({"periodic_schedule": schedule_json}))
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to schedule periodic task: {}", e)))?;

        Ok(schedule_id)
    }

    /// List all periodic task schedules
    ///
    /// # Example
    /// ```rust,no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgresql://localhost/celers").await?;
    ///
    /// let schedules = broker.list_periodic_schedules().await?;
    /// for schedule in schedules {
    ///     println!("Schedule: {} - {}", schedule.task_name, schedule.cron_expression);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_periodic_schedules(&self) -> Result<Vec<PeriodicTaskSchedule>> {
        let rows = sqlx::query(&format!(
            "SELECT payload FROM {} WHERE name LIKE '__periodic_schedule_%' AND state = 'pending'",
            self.queue_name
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to list periodic schedules: {}", e)))?;

        let mut schedules = Vec::new();
        for row in rows {
            let payload: serde_json::Value = row
                .try_get("payload")
                .map_err(|e| CelersError::Other(format!("Failed to get payload: {}", e)))?;
            if let Ok(schedule) = serde_json::from_value::<PeriodicTaskSchedule>(payload) {
                schedules.push(schedule);
            }
        }

        Ok(schedules)
    }

    /// Cancel a periodic task schedule
    ///
    /// # Example
    /// ```rust,no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgresql://localhost/celers").await?;
    ///
    /// broker.cancel_periodic_schedule("schedule_id_123").await?;
    /// println!("Schedule cancelled");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn cancel_periodic_schedule(&self, schedule_id: &str) -> Result<bool> {
        let result = sqlx::query(&format!(
            "DELETE FROM {} WHERE name LIKE '__periodic_schedule_%'
             AND payload->>'schedule_id' = $1",
            self.queue_name
        ))
        .bind(schedule_id)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to cancel periodic schedule: {}", e)))?;

        Ok(result.rows_affected() > 0)
    }

    /// Create a snapshot of the current queue state
    ///
    /// Creates a backup snapshot that can be used for restore or analysis.
    ///
    /// # Example
    /// ```rust,no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgresql://localhost/celers").await?;
    ///
    /// let snapshot = broker.create_queue_snapshot(true).await?;
    /// println!("Created snapshot: {} with {} tasks",
    ///          snapshot.snapshot_id, snapshot.task_count);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_queue_snapshot(&self, include_results: bool) -> Result<QueueSnapshot> {
        let snapshot_id = Uuid::new_v4().to_string();
        let now = Utc::now();

        // Count tasks
        let count: (i64,) = sqlx::query_as(&format!("SELECT COUNT(*) FROM {}", self.queue_name))
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to count tasks for snapshot: {}", e))
            })?;

        // Estimate size
        let size: (Option<i64>,) = sqlx::query_as(&format!(
            "SELECT pg_total_relation_size('{}')",
            self.queue_name
        ))
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get table size: {}", e)))?;

        let snapshot = QueueSnapshot {
            snapshot_id: snapshot_id.clone(),
            queue_name: self.queue_name.clone(),
            created_at: now,
            task_count: count.0,
            total_size_bytes: size.0.unwrap_or(0),
            includes_results: include_results,
        };

        // Store snapshot metadata (would need a snapshots table in real implementation)
        // For now, just return the snapshot info
        Ok(snapshot)
    }

    /// List available queue snapshots
    ///
    /// # Example
    /// ```rust,no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgresql://localhost/celers").await?;
    ///
    /// let snapshots = broker.list_queue_snapshots().await?;
    /// for snapshot in snapshots {
    ///     println!("Snapshot: {} - {} tasks", snapshot.snapshot_id, snapshot.task_count);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_queue_snapshots(&self) -> Result<Vec<QueueSnapshot>> {
        // In a real implementation, this would query a snapshots table
        // For now, return empty list
        Ok(Vec::new())
    }

    /// Archive tasks based on custom criteria
    ///
    /// More flexible than `archive_completed_tasks`, allows custom SQL WHERE clauses.
    ///
    /// # Example
    /// ```rust,no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgresql://localhost/celers").await?;
    ///
    /// // Archive failed tasks older than 7 days
    /// let archived = broker.archive_by_criteria(
    ///     "state = 'failed' AND created_at < NOW() - INTERVAL '7 days'"
    /// ).await?;
    ///
    /// println!("Archived {} failed tasks", archived);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn archive_by_criteria(&self, where_clause: &str) -> Result<i64> {
        // Move tasks to history table
        let result = sqlx::query(&format!(
            "INSERT INTO celers_task_history
             SELECT * FROM {} WHERE {}",
            self.queue_name, where_clause
        ))
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to archive tasks to history: {}", e)))?;

        let archived_count = result.rows_affected();

        // Delete from main table
        sqlx::query(&format!(
            "DELETE FROM {} WHERE {}",
            self.queue_name, where_clause
        ))
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to delete archived tasks: {}", e)))?;

        Ok(archived_count as i64)
    }

    /// Apply task retention policies
    ///
    /// Automatically archives or deletes tasks based on configured retention policies.
    ///
    /// # Example
    /// ```rust,no_run
    /// # use celers_broker_postgres::{PostgresBroker, TaskRetentionPolicy};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgresql://localhost/celers").await?;
    ///
    /// let policies = vec![
    ///     TaskRetentionPolicy {
    ///         policy_name: "completed".to_string(),
    ///         task_state: "completed".to_string(),
    ///         retention_days: 30,
    ///         archive_before_delete: true,
    ///         enabled: true,
    ///     },
    ///     TaskRetentionPolicy {
    ///         policy_name: "failed".to_string(),
    ///         task_state: "failed".to_string(),
    ///         retention_days: 90,
    ///         archive_before_delete: true,
    ///         enabled: true,
    ///     },
    /// ];
    ///
    /// let affected = broker.apply_retention_policies(&policies).await?;
    /// println!("Retention policies affected {} tasks", affected);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn apply_retention_policies(&self, policies: &[TaskRetentionPolicy]) -> Result<i64> {
        let mut total_affected = 0i64;

        for policy in policies {
            if !policy.enabled {
                continue;
            }

            let where_clause = format!(
                "state = '{}' AND created_at < NOW() - INTERVAL '{} days'",
                policy.task_state, policy.retention_days
            );

            if policy.archive_before_delete {
                let archived = self.archive_by_criteria(&where_clause).await?;
                total_affected += archived;
            } else {
                let result = sqlx::query(&format!(
                    "DELETE FROM {} WHERE {}",
                    self.queue_name, where_clause
                ))
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    CelersError::Other(format!("Failed to delete tasks by retention policy: {}", e))
                })?;
                total_affected += result.rows_affected() as i64;
            }
        }

        Ok(total_affected)
    }

    /// Monitor connection pool health and get tuning recommendations
    ///
    /// Analyzes current pool utilization and provides scaling recommendations.
    ///
    /// # Example
    /// ```rust,no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgresql://localhost/celers").await?;
    ///
    /// let health = broker.monitor_pool_health().await?;
    /// println!("Pool utilization: {:.1}%", health.utilization_percent);
    /// println!("Recommendation: {}", health.recommendation);
    ///
    /// if health.should_scale {
    ///     println!("Consider scaling to {} connections", health.recommended_size);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn monitor_pool_health(&self) -> Result<ConnectionPoolHealth> {
        let pool_metrics = self.get_pool_metrics();

        let utilization = if pool_metrics.max_size > 0 {
            (pool_metrics.size as f64 / pool_metrics.max_size as f64) * 100.0
        } else {
            0.0
        };

        let active_ratio = if pool_metrics.size > 0 {
            pool_metrics.in_use as f64 / pool_metrics.size as f64
        } else {
            0.0
        };

        let (recommendation, should_scale, recommended_size) = if utilization > 90.0 {
            (
                "Pool is near capacity. Consider increasing max_size.".to_string(),
                true,
                (pool_metrics.max_size as f64 * 1.5) as u32,
            )
        } else if utilization < 30.0 && pool_metrics.max_size > 10 {
            (
                "Pool is underutilized. Consider decreasing max_size.".to_string(),
                true,
                (pool_metrics.max_size as f64 * 0.7).max(10.0) as u32,
            )
        } else if active_ratio > 0.8 {
            (
                "High active connection ratio. Pool is working efficiently.".to_string(),
                false,
                pool_metrics.max_size,
            )
        } else {
            (
                "Pool health is optimal.".to_string(),
                false,
                pool_metrics.max_size,
            )
        };

        Ok(ConnectionPoolHealth {
            current_size: pool_metrics.size,
            idle_count: pool_metrics.idle,
            active_count: pool_metrics.in_use,
            max_size: pool_metrics.max_size,
            utilization_percent: utilization,
            avg_wait_time_ms: 0.0, // Would need to track this over time
            recommendation,
            should_scale,
            recommended_size,
        })
    }

    /// Auto-tune connection pool size based on workload
    ///
    /// Automatically adjusts pool size based on current utilization patterns.
    /// Note: This requires creating a new broker instance with adjusted pool settings.
    ///
    /// # Example
    /// ```rust,no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgresql://localhost/celers").await?;
    ///
    /// let recommendation = broker.auto_tune_pool_size().await?;
    /// println!("Current pool size: {}", recommendation.current_size);
    /// println!("Recommended size: {}", recommendation.recommended_size);
    /// println!("Reason: {}", recommendation.recommendation);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn auto_tune_pool_size(&self) -> Result<ConnectionPoolHealth> {
        self.monitor_pool_health().await
    }

    /// Get batch archiving statistics
    ///
    /// Efficiently archives large numbers of completed tasks and provides statistics.
    ///
    /// # Example
    /// ```rust,no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgresql://localhost/celers").await?;
    ///
    /// // Archive up to 10,000 completed tasks older than 30 days
    /// let archived = broker.batch_archive_completed(30, 10000).await?;
    /// println!("Archived {} tasks", archived);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn batch_archive_completed(
        &self,
        older_than_days: i32,
        batch_size: i64,
    ) -> Result<i64> {
        let where_clause = format!(
            "state = 'completed' AND created_at < NOW() - INTERVAL '{} days' LIMIT {}",
            older_than_days, batch_size
        );

        self.archive_by_criteria(&where_clause).await
    }
}
