//! Advanced operations: forecasting, task groups, tags, queue health checks

use celers_core::{CelersError, Result};
use chrono::Utc;
use sqlx::Row;
use uuid::Uuid;

use crate::types::{
    DbTaskState, QueueCapacityAnalysis, QueueForecast, QueueHealthCheck, QueueHealthThresholds,
    QueueTrendAnalysis, TaskCompletionEstimate, TaskGroupStatus, TaskInfo, TaskSearchFilter,
};
use crate::PostgresBroker;

impl PostgresBroker {
    /// Rebalance queue priorities
    ///
    /// Normalizes task priorities to prevent priority inflation over time.
    /// Can use either percentile-based normalization or range compression.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Rebalance priorities to 0-100 range
    /// let rebalanced = broker.rebalance_queue_priorities(0, 100).await?;
    /// println!("Rebalanced {} tasks", rebalanced);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn rebalance_queue_priorities(
        &self,
        min_priority: i32,
        max_priority: i32,
    ) -> Result<i64> {
        // First, get the current min and max priorities
        let row = sqlx::query(
            r#"
            SELECT
                COALESCE(MIN(priority), 0) as current_min,
                COALESCE(MAX(priority), 100) as current_max
            FROM celers_tasks
            WHERE queue_name = $1
              AND state = 'pending'
            "#,
        )
        .bind(&self.queue_name)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get priority range: {}", e)))?;

        let current_min: i32 = row.get("current_min");
        let current_max: i32 = row.get("current_max");

        if current_min == current_max {
            // All tasks have same priority, just set them to middle of range
            let mid_priority = (min_priority + max_priority) / 2;
            let result = sqlx::query(
                r#"
                UPDATE celers_tasks
                SET priority = $2
                WHERE queue_name = $1
                  AND state = 'pending'
                "#,
            )
            .bind(&self.queue_name)
            .bind(mid_priority)
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to rebalance priorities: {}", e)))?;

            return Ok(result.rows_affected() as i64);
        }

        // Normalize priorities to new range
        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET priority = $3 + (
                (priority - $4)::FLOAT / ($5 - $4)::FLOAT * ($6 - $3)::FLOAT
            )::INTEGER
            WHERE queue_name = $1
              AND state = 'pending'
            "#,
        )
        .bind(&self.queue_name)
        .bind(min_priority) // $2 (not used in UPDATE directly)
        .bind(min_priority) // $3
        .bind(current_min) // $4
        .bind(current_max) // $5
        .bind(max_priority) // $6
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to rebalance priorities: {}", e)))?;

        let count = result.rows_affected() as i64;
        tracing::info!(
            count = count,
            old_range = format!("{}-{}", current_min, current_max),
            new_range = format!("{}-{}", min_priority, max_priority),
            "Rebalanced queue priorities"
        );

        Ok(count)
    }

    /// Forecast queue depth
    ///
    /// Predicts future queue depth based on current trends in task arrival
    /// and processing rates. Useful for capacity planning and alerting.
    ///
    /// # Arguments
    ///
    /// * `forecast_hours` - Number of hours to forecast into the future
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let forecast = broker.forecast_queue_depth(4).await?;
    /// println!("Current queue depth: {}", forecast.current_depth);
    /// println!("Predicted depth in 4 hours: {}", forecast.predicted_depth);
    /// println!("Trend: {}", forecast.trend);
    /// println!("Confidence: {:.2}%", forecast.confidence * 100.0);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn forecast_queue_depth(&self, forecast_hours: i64) -> Result<QueueForecast> {
        // Get historical data for the last 24 hours
        let row = sqlx::query(
            r#"
            WITH hourly_stats AS (
                SELECT
                    date_trunc('hour', created_at) as hour,
                    COUNT(*) as tasks_created,
                    COUNT(*) FILTER (WHERE state = 'completed') as tasks_completed
                FROM celers_tasks
                WHERE queue_name = $1
                  AND created_at >= NOW() - INTERVAL '24 hours'
                GROUP BY date_trunc('hour', created_at)
                ORDER BY hour DESC
                LIMIT 24
            ),
            current_state AS (
                SELECT
                    COUNT(*) FILTER (WHERE state = 'pending') as pending,
                    COUNT(*) FILTER (WHERE state = 'processing') as processing
                FROM celers_tasks
                WHERE queue_name = $1
            )
            SELECT
                COALESCE(AVG(tasks_created), 0) as avg_arrival_rate,
                COALESCE(AVG(tasks_completed), 0) as avg_completion_rate,
                COALESCE(STDDEV(tasks_created), 0) as arrival_stddev,
                (SELECT pending FROM current_state) as current_pending,
                (SELECT processing FROM current_state) as current_processing
            FROM hourly_stats
            "#,
        )
        .bind(&self.queue_name)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to forecast queue depth: {}", e)))?;

        let avg_arrival_rate: f64 = row.get("avg_arrival_rate");
        let avg_completion_rate: f64 = row.get("avg_completion_rate");
        let arrival_stddev: f64 = row.get("arrival_stddev");
        let current_pending: i64 = row.get("current_pending");
        let current_processing: i64 = row.get("current_processing");

        let current_depth = current_pending + current_processing;
        let net_rate = avg_arrival_rate - avg_completion_rate;
        let predicted_depth = current_depth + (net_rate * forecast_hours as f64) as i64;
        let predicted_depth = predicted_depth.max(0); // Can't be negative

        // Determine trend
        let trend = if net_rate > 1.0 {
            "growing".to_string()
        } else if net_rate < -1.0 {
            "shrinking".to_string()
        } else {
            "stable".to_string()
        };

        // Calculate confidence (higher stddev = lower confidence)
        let confidence = if avg_arrival_rate > 0.0 {
            (1.0 - (arrival_stddev / avg_arrival_rate).min(1.0)).max(0.0)
        } else {
            0.5
        };

        Ok(QueueForecast {
            current_depth,
            predicted_depth,
            forecast_hours,
            avg_arrival_rate,
            avg_completion_rate,
            net_rate,
            trend,
            confidence,
        })
    }

    /// Get queue trend analysis
    ///
    /// Analyzes queue metrics over time to identify trends and patterns.
    /// Provides insights into queue health and potential issues.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let trend = broker.get_queue_trend_analysis(24).await?;
    /// println!("Queue trend over last 24 hours:");
    /// println!("  Peak pending: {}", trend.peak_pending);
    /// println!("  Average pending: {:.1}", trend.avg_pending);
    /// println!("  Peak hour: {}", trend.peak_hour);
    /// println!("  Task velocity: {:.1} tasks/hour", trend.task_velocity);
    /// println!("  Success rate: {:.2}%", trend.success_rate * 100.0);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_queue_trend_analysis(&self, hours: i64) -> Result<QueueTrendAnalysis> {
        let row = sqlx::query(
            r#"
            WITH hourly_stats AS (
                SELECT
                    EXTRACT(HOUR FROM created_at)::INTEGER as hour,
                    COUNT(*) FILTER (WHERE state = 'pending') as pending,
                    COUNT(*) FILTER (WHERE state = 'completed') as completed,
                    COUNT(*) FILTER (WHERE state = 'failed') as failed,
                    COUNT(*) as total
                FROM celers_tasks
                WHERE queue_name = $1
                  AND created_at >= NOW() - INTERVAL '1 hour' * $2
                GROUP BY EXTRACT(HOUR FROM created_at)
            ),
            aggregated AS (
                SELECT
                    MAX(pending) as peak_pending,
                    AVG(pending) as avg_pending,
                    SUM(total) as total_tasks,
                    SUM(completed) as total_completed,
                    SUM(failed) as total_failed
                FROM hourly_stats
            ),
            peak_hour_calc AS (
                SELECT hour
                FROM hourly_stats
                ORDER BY total DESC
                LIMIT 1
            )
            SELECT
                a.peak_pending,
                a.avg_pending,
                a.total_tasks,
                a.total_completed,
                a.total_failed,
                COALESCE(p.hour, 0) as peak_hour
            FROM aggregated a
            CROSS JOIN peak_hour_calc p
            "#,
        )
        .bind(&self.queue_name)
        .bind(hours)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get queue trend analysis: {}", e)))?;

        let peak_pending: i64 = row.get("peak_pending");
        let avg_pending: f64 = row.try_get("avg_pending").unwrap_or(0.0);
        let total_tasks: i64 = row.get("total_tasks");
        let total_completed: i64 = row.get("total_completed");
        let total_failed: i64 = row.get("total_failed");
        let peak_hour: i32 = row.get("peak_hour");

        let task_velocity = total_tasks as f64 / hours as f64;
        let success_rate = if total_completed + total_failed > 0 {
            total_completed as f64 / (total_completed + total_failed) as f64
        } else {
            0.0
        };

        Ok(QueueTrendAnalysis {
            hours_analyzed: hours,
            peak_pending,
            avg_pending,
            peak_hour,
            total_tasks,
            task_velocity,
            success_rate,
            total_completed,
            total_failed,
        })
    }

    /// Estimate task completion time
    ///
    /// Estimates when a specific task will be completed based on current
    /// queue depth, processing rate, and task priority.
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
    /// if let Some(estimate) = broker.estimate_task_completion_time(&task_id).await? {
    ///     println!("Task estimated to complete in {} seconds", estimate.estimated_wait_secs);
    ///     println!("Confidence: {:.2}%", estimate.confidence * 100.0);
    ///     println!("Tasks ahead in queue: {}", estimate.tasks_ahead);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn estimate_task_completion_time(
        &self,
        task_id: &Uuid,
    ) -> Result<Option<TaskCompletionEstimate>> {
        // First check if task exists and is not already completed
        let task_row = sqlx::query(
            r#"
            SELECT state, priority
            FROM celers_tasks
            WHERE queue_name = $1 AND id = $2
            "#,
        )
        .bind(&self.queue_name)
        .bind(task_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to find task: {}", e)))?;

        let (state, priority) = match task_row {
            Some(row) => {
                let state: String = row.get("state");
                let priority: i32 = row.get("priority");
                (state, priority)
            }
            None => return Ok(None),
        };

        // If task is already completed or failed, return None
        if state == "completed" || state == "failed" || state == "cancelled" {
            return Ok(None);
        }

        // Get statistics for estimation
        let row = sqlx::query(
            r#"
            WITH recent_completions AS (
                SELECT
                    EXTRACT(EPOCH FROM (completed_at - started_at)) as duration_secs
                FROM celers_tasks
                WHERE queue_name = $1
                  AND state = 'completed'
                  AND completed_at IS NOT NULL
                  AND started_at IS NOT NULL
                  AND completed_at >= NOW() - INTERVAL '1 hour'
                LIMIT 100
            ),
            queue_stats AS (
                SELECT
                    COUNT(*) FILTER (WHERE state = 'pending' AND priority >= $3) as tasks_ahead,
                    COUNT(*) FILTER (WHERE state = 'processing') as currently_processing
                FROM celers_tasks
                WHERE queue_name = $1
            )
            SELECT
                COALESCE(AVG(duration_secs), 60) as avg_task_duration_secs,
                COALESCE(STDDEV(duration_secs), 30) as duration_stddev,
                (SELECT tasks_ahead FROM queue_stats) as tasks_ahead,
                (SELECT currently_processing FROM queue_stats) as currently_processing
            FROM recent_completions
            "#,
        )
        .bind(&self.queue_name)
        .bind(task_id)
        .bind(priority)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to estimate completion time: {}", e)))?;

        let avg_task_duration_secs: f64 = row.get("avg_task_duration_secs");
        let duration_stddev: f64 = row.get("duration_stddev");
        let tasks_ahead: i64 = row.get("tasks_ahead");
        let currently_processing: i64 = row.get("currently_processing");

        // Estimate wait time based on queue position and average task duration
        let worker_count = currently_processing.max(1); // Assume at least 1 worker
        let estimated_wait_secs =
            (tasks_ahead as f64 / worker_count as f64 * avg_task_duration_secs) as i64;

        // Calculate confidence (lower stddev = higher confidence)
        let confidence = if avg_task_duration_secs > 0.0 {
            (1.0 - (duration_stddev / avg_task_duration_secs).min(1.0)).max(0.0)
        } else {
            0.5
        };

        Ok(Some(TaskCompletionEstimate {
            task_id: *task_id,
            estimated_wait_secs,
            tasks_ahead,
            avg_task_duration_secs: avg_task_duration_secs as i64,
            confidence,
        }))
    }

    /// Get queue capacity analysis
    ///
    /// Analyzes current queue capacity and provides recommendations for
    /// scaling workers or adjusting processing parameters.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let capacity = broker.get_queue_capacity_analysis().await?;
    /// println!("Current utilization: {:.2}%", capacity.utilization_percent);
    /// println!("Estimated worker count: {}", capacity.estimated_worker_count);
    /// println!("Recommendation: {}", capacity.recommendation);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_queue_capacity_analysis(&self) -> Result<QueueCapacityAnalysis> {
        let row = sqlx::query(
            r#"
            WITH recent_metrics AS (
                SELECT
                    COUNT(*) FILTER (WHERE state = 'pending') as pending,
                    COUNT(*) FILTER (WHERE state = 'processing') as processing,
                    COUNT(*) FILTER (WHERE state = 'completed' AND completed_at >= NOW() - INTERVAL '1 hour') as completed_last_hour,
                    EXTRACT(EPOCH FROM AVG(completed_at - started_at))::BIGINT FILTER (WHERE state = 'completed' AND completed_at >= NOW() - INTERVAL '1 hour') as avg_duration_secs
                FROM celers_tasks
                WHERE queue_name = $1
            )
            SELECT
                pending,
                processing,
                completed_last_hour,
                COALESCE(avg_duration_secs, 60) as avg_duration_secs
            FROM recent_metrics
            "#,
        )
        .bind(&self.queue_name)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to analyze queue capacity: {}", e)))?;

        let pending: i64 = row.get("pending");
        let processing: i64 = row.get("processing");
        let completed_last_hour: i64 = row.get("completed_last_hour");
        let avg_duration_secs: i64 = row.get("avg_duration_secs");

        let estimated_worker_count = processing;
        let throughput_per_hour = if estimated_worker_count > 0 {
            (3600 / avg_duration_secs.max(1)) * estimated_worker_count
        } else {
            0
        };

        // Calculate utilization
        let utilization_percent = if throughput_per_hour > 0 {
            ((completed_last_hour as f64 / throughput_per_hour as f64) * 100.0).min(100.0)
        } else {
            0.0
        };

        // Generate recommendation
        let recommendation = if utilization_percent > 90.0 {
            "High utilization - consider adding more workers".to_string()
        } else if utilization_percent > 70.0 {
            "Moderate utilization - monitor for increases".to_string()
        } else if utilization_percent < 30.0 && estimated_worker_count > 1 {
            "Low utilization - consider reducing workers".to_string()
        } else {
            "Utilization is within normal range".to_string()
        };

        Ok(QueueCapacityAnalysis {
            pending_tasks: pending,
            processing_tasks: processing,
            estimated_worker_count,
            throughput_per_hour,
            utilization_percent,
            avg_task_duration_secs: avg_duration_secs,
            recommendation,
        })
    }

    /// Advanced task search with multiple filters
    ///
    /// Searches for tasks matching multiple criteria including state, priority range,
    /// time range, task name pattern, and metadata filters.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::{PostgresBroker, TaskSearchFilter};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let filter = TaskSearchFilter {
    ///     states: Some(vec!["pending".to_string(), "processing".to_string()]),
    ///     priority_min: Some(50),
    ///     priority_max: Some(100),
    ///     created_after_secs: Some(3600), // Last hour
    ///     task_name_pattern: Some("%import%".to_string()),
    ///     metadata_filters: vec![("priority_level".to_string(), "high".to_string())],
    ///     limit: 100,
    /// };
    ///
    /// let tasks = broker.search_tasks(&filter).await?;
    /// println!("Found {} tasks matching criteria", tasks.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn search_tasks(&self, filter: &TaskSearchFilter) -> Result<Vec<TaskInfo>> {
        let mut query = String::from(
            r#"
            SELECT
                id, task_name, state, priority, retry_count, max_retries,
                created_at, scheduled_at, started_at, completed_at, worker_id, error_message
            FROM celers_tasks
            WHERE queue_name = $1
            "#,
        );

        let mut param_count = 1;
        let mut conditions = Vec::new();

        // State filter
        if let Some(ref states) = filter.states {
            if !states.is_empty() {
                param_count += 1;
                conditions.push(format!("state = ANY(${})", param_count));
            }
        }

        // Priority range
        if filter.priority_min.is_some() {
            param_count += 1;
            conditions.push(format!("priority >= ${}", param_count));
        }
        if filter.priority_max.is_some() {
            param_count += 1;
            conditions.push(format!("priority <= ${}", param_count));
        }

        // Time range
        if filter.created_after_secs.is_some() {
            param_count += 1;
            conditions.push(format!(
                "created_at >= NOW() - INTERVAL '1 second' * ${}",
                param_count
            ));
        }

        // Task name pattern
        if filter.task_name_pattern.is_some() {
            param_count += 1;
            conditions.push(format!("task_name LIKE ${}", param_count));
        }

        // Metadata filters
        for _ in &filter.metadata_filters {
            param_count += 1;
            let key_param = param_count;
            param_count += 1;
            let value_param = param_count;
            conditions.push(format!("(metadata->>${} = ${})", key_param, value_param));
        }

        if !conditions.is_empty() {
            query.push_str(" AND ");
            query.push_str(&conditions.join(" AND "));
        }

        query.push_str(" ORDER BY priority DESC, created_at ASC LIMIT $");
        param_count += 1;
        query.push_str(&param_count.to_string());

        // Build query dynamically
        let mut sql_query = sqlx::query(&query).bind(&self.queue_name);

        if let Some(ref states) = filter.states {
            if !states.is_empty() {
                sql_query = sql_query.bind(states);
            }
        }
        if let Some(min_priority) = filter.priority_min {
            sql_query = sql_query.bind(min_priority);
        }
        if let Some(max_priority) = filter.priority_max {
            sql_query = sql_query.bind(max_priority);
        }
        if let Some(created_after_secs) = filter.created_after_secs {
            sql_query = sql_query.bind(created_after_secs);
        }
        if let Some(ref pattern) = filter.task_name_pattern {
            sql_query = sql_query.bind(pattern);
        }
        for (key, value) in &filter.metadata_filters {
            sql_query = sql_query.bind(key).bind(value);
        }
        sql_query = sql_query.bind(filter.limit);

        let rows = sql_query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to search tasks: {}", e)))?;

        let tasks = rows
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
                started_at: row.try_get("started_at").ok(),
                completed_at: row.try_get("completed_at").ok(),
                worker_id: row.try_get("worker_id").ok(),
                error_message: row.try_get("error_message").ok(),
            })
            .collect();

        Ok(tasks)
    }

    /// Find tasks by complex criteria
    ///
    /// More flexible task search supporting OR conditions, partial matches,
    /// and sorting options.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Find high-priority or old pending tasks
    /// let tasks = broker.find_tasks_by_complex_criteria(
    ///     Some(vec!["pending".to_string()]),
    ///     Some("priority > 80 OR EXTRACT(EPOCH FROM (NOW() - created_at)) > 3600"),
    ///     "priority DESC, created_at ASC",
    ///     50
    /// ).await?;
    ///
    /// println!("Found {} urgent tasks", tasks.len());
    /// # Ok(())
    /// # }
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub async fn find_tasks_by_complex_criteria(
        &self,
        states: Option<Vec<String>>,
        custom_where_clause: Option<&str>,
        order_by: &str,
        limit: i64,
    ) -> Result<Vec<TaskInfo>> {
        let mut query = String::from(
            r#"
            SELECT
                id, task_name, state, priority, retry_count, max_retries,
                created_at, scheduled_at, started_at, completed_at, worker_id, error_message
            FROM celers_tasks
            WHERE queue_name = $1
            "#,
        );

        let mut param_count = 1;

        if let Some(ref state_list) = states {
            if !state_list.is_empty() {
                param_count += 1;
                query.push_str(&format!(" AND state = ANY(${})", param_count));
            }
        }

        if let Some(custom_clause) = custom_where_clause {
            query.push_str(&format!(" AND ({})", custom_clause));
        }

        // Sanitize order_by to prevent SQL injection (allow only safe characters)
        let safe_order_by = order_by
            .chars()
            .filter(|c| {
                c.is_alphanumeric() || *c == '_' || *c == ' ' || *c == ',' || *c == '(' || *c == ')'
            })
            .collect::<String>();

        query.push_str(&format!(
            " ORDER BY {} LIMIT ${}",
            safe_order_by,
            param_count + 1
        ));

        let mut sql_query = sqlx::query(&query).bind(&self.queue_name);

        if let Some(ref state_list) = states {
            if !state_list.is_empty() {
                sql_query = sql_query.bind(state_list);
            }
        }
        sql_query = sql_query.bind(limit);

        let rows = sql_query.fetch_all(&self.pool).await.map_err(|e| {
            CelersError::Other(format!("Failed to find tasks by complex criteria: {}", e))
        })?;

        let tasks = rows
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
                started_at: row.try_get("started_at").ok(),
                completed_at: row.try_get("completed_at").ok(),
                worker_id: row.try_get("worker_id").ok(),
                error_message: row.try_get("error_message").ok(),
            })
            .collect();

        Ok(tasks)
    }

    /// Count tasks matching search filter
    ///
    /// Returns the count of tasks matching the specified filter without retrieving
    /// the full task data. Useful for pagination and reporting.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::{PostgresBroker, TaskSearchFilter};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let filter = TaskSearchFilter {
    ///     states: Some(vec!["pending".to_string()]),
    ///     priority_min: Some(70),
    ///     priority_max: None,
    ///     created_after_secs: None,
    ///     task_name_pattern: None,
    ///     metadata_filters: vec![],
    ///     limit: 1000,
    /// };
    ///
    /// let count = broker.count_tasks_matching(&filter).await?;
    /// println!("Total high-priority pending tasks: {}", count);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn count_tasks_matching(&self, filter: &TaskSearchFilter) -> Result<i64> {
        let mut query = String::from(
            r#"
            SELECT COUNT(*) as count
            FROM celers_tasks
            WHERE queue_name = $1
            "#,
        );

        let mut param_count = 1;
        let mut conditions = Vec::new();

        if let Some(ref states) = filter.states {
            if !states.is_empty() {
                param_count += 1;
                conditions.push(format!("state = ANY(${})", param_count));
            }
        }

        if filter.priority_min.is_some() {
            param_count += 1;
            conditions.push(format!("priority >= ${}", param_count));
        }
        if filter.priority_max.is_some() {
            param_count += 1;
            conditions.push(format!("priority <= ${}", param_count));
        }

        if filter.created_after_secs.is_some() {
            param_count += 1;
            conditions.push(format!(
                "created_at >= NOW() - INTERVAL '1 second' * ${}",
                param_count
            ));
        }

        if filter.task_name_pattern.is_some() {
            param_count += 1;
            conditions.push(format!("task_name LIKE ${}", param_count));
        }

        for _ in &filter.metadata_filters {
            param_count += 1;
            let key_param = param_count;
            param_count += 1;
            let value_param = param_count;
            conditions.push(format!("(metadata->>${} = ${})", key_param, value_param));
        }

        if !conditions.is_empty() {
            query.push_str(" AND ");
            query.push_str(&conditions.join(" AND "));
        }

        let mut sql_query = sqlx::query(&query).bind(&self.queue_name);

        if let Some(ref states) = filter.states {
            if !states.is_empty() {
                sql_query = sql_query.bind(states);
            }
        }
        if let Some(min_priority) = filter.priority_min {
            sql_query = sql_query.bind(min_priority);
        }
        if let Some(max_priority) = filter.priority_max {
            sql_query = sql_query.bind(max_priority);
        }
        if let Some(created_after_secs) = filter.created_after_secs {
            sql_query = sql_query.bind(created_after_secs);
        }
        if let Some(ref pattern) = filter.task_name_pattern {
            sql_query = sql_query.bind(pattern);
        }
        for (key, value) in &filter.metadata_filters {
            sql_query = sql_query.bind(key).bind(value);
        }

        let row = sql_query
            .fetch_one(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to count tasks: {}", e)))?;

        let count: i64 = row.get("count");
        Ok(count)
    }

    /// Create task group
    ///
    /// Groups related tasks together for tracking and management.
    /// Useful for batch jobs, workflows, or logically related operations.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Create a task group for a batch import job
    /// let group_id = broker.create_task_group(
    ///     "data_import_batch_2024_01",
    ///     Some("Import customer data from CSV files")
    /// ).await?;
    ///
    /// println!("Created task group: {}", group_id);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_task_group(
        &self,
        group_name: &str,
        _description: Option<&str>,
    ) -> Result<String> {
        let group_id = Uuid::new_v4().to_string();

        // Store group metadata in a dedicated metadata field or separate tracking
        // For now, we'll just return the group_id and rely on metadata tagging
        tracing::info!(group_id = %group_id, group_name = %group_name, "Created task group");
        Ok(group_id)
    }

    /// Add tasks to group
    ///
    /// Associates multiple tasks with a task group for coordinated tracking.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    /// use uuid::Uuid;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let group_id = "my-task-group-id";
    /// let task_ids = vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
    ///
    /// let added = broker.add_tasks_to_group(group_id, &task_ids).await?;
    /// println!("Added {} tasks to group", added);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn add_tasks_to_group(&self, group_id: &str, task_ids: &[Uuid]) -> Result<i64> {
        if task_ids.is_empty() {
            return Ok(0);
        }

        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET metadata = jsonb_set(
                COALESCE(metadata, '{}'::jsonb),
                '{task_group_id}',
                to_jsonb($1::text)
            )
            WHERE queue_name = $2 AND id = ANY($3)
            "#,
        )
        .bind(group_id)
        .bind(&self.queue_name)
        .bind(task_ids)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to add tasks to group: {}", e)))?;

        let count = result.rows_affected() as i64;
        tracing::info!(group_id = %group_id, task_count = count, "Added tasks to group");
        Ok(count)
    }

    /// Get task group status
    ///
    /// Retrieves comprehensive status information for a task group including
    /// task counts by state, completion percentage, and timing information.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let group_id = "my-task-group-id";
    ///
    /// if let Some(status) = broker.get_task_group_status(group_id).await? {
    ///     println!("Group: {}", status.group_name);
    ///     println!("Progress: {:.1}%", status.completion_percentage);
    ///     println!("Pending: {}, Processing: {}, Completed: {}",
    ///              status.pending_count, status.processing_count, status.completed_count);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_task_group_status(&self, group_id: &str) -> Result<Option<TaskGroupStatus>> {
        let row = sqlx::query(
            r#"
            WITH group_tasks AS (
                SELECT
                    state,
                    created_at,
                    completed_at
                FROM celers_tasks
                WHERE queue_name = $1
                  AND metadata->>'task_group_id' = $2
            )
            SELECT
                COUNT(*) as total_tasks,
                COUNT(*) FILTER (WHERE state = 'pending') as pending_count,
                COUNT(*) FILTER (WHERE state = 'processing') as processing_count,
                COUNT(*) FILTER (WHERE state = 'completed') as completed_count,
                COUNT(*) FILTER (WHERE state = 'failed') as failed_count,
                COUNT(*) FILTER (WHERE state = 'cancelled') as cancelled_count,
                MIN(created_at) as first_task_created,
                MAX(completed_at) as last_task_completed
            FROM group_tasks
            "#,
        )
        .bind(&self.queue_name)
        .bind(group_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get task group status: {}", e)))?;

        if let Some(row) = row {
            let total_tasks: i64 = row.get("total_tasks");

            if total_tasks == 0 {
                return Ok(None);
            }

            let pending_count: i64 = row.get("pending_count");
            let processing_count: i64 = row.get("processing_count");
            let completed_count: i64 = row.get("completed_count");
            let failed_count: i64 = row.get("failed_count");
            let cancelled_count: i64 = row.get("cancelled_count");

            let completion_percentage = if total_tasks > 0 {
                (completed_count as f64 / total_tasks as f64) * 100.0
            } else {
                0.0
            };

            Ok(Some(TaskGroupStatus {
                group_id: group_id.to_string(),
                group_name: group_id.to_string(),
                total_tasks,
                pending_count,
                processing_count,
                completed_count,
                failed_count,
                cancelled_count,
                completion_percentage,
                first_task_created: row.try_get("first_task_created").ok(),
                last_task_completed: row.try_get("last_task_completed").ok(),
            }))
        } else {
            Ok(None)
        }
    }

    /// Get all tasks in a group
    ///
    /// Retrieves all task IDs belonging to a specific task group.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let group_id = "my-task-group-id";
    ///
    /// let tasks = broker.get_tasks_in_group(group_id, None).await?;
    /// println!("Group contains {} tasks", tasks.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_tasks_in_group(
        &self,
        group_id: &str,
        state_filter: Option<&str>,
    ) -> Result<Vec<TaskInfo>> {
        let query = if state_filter.is_some() {
            r#"
            SELECT
                id, task_name, state, priority, retry_count, max_retries,
                created_at, scheduled_at, started_at, completed_at, worker_id, error_message
            FROM celers_tasks
            WHERE queue_name = $1
              AND metadata->>'task_group_id' = $2
              AND state = $3
            ORDER BY created_at ASC
            "#
        } else {
            r#"
            SELECT
                id, task_name, state, priority, retry_count, max_retries,
                created_at, scheduled_at, started_at, completed_at, worker_id, error_message
            FROM celers_tasks
            WHERE queue_name = $1
              AND metadata->>'task_group_id' = $2
            ORDER BY created_at ASC
            "#
        };

        let rows = if let Some(state) = state_filter {
            sqlx::query(query)
                .bind(&self.queue_name)
                .bind(group_id)
                .bind(state)
                .fetch_all(&self.pool)
                .await
        } else {
            sqlx::query(query)
                .bind(&self.queue_name)
                .bind(group_id)
                .fetch_all(&self.pool)
                .await
        }
        .map_err(|e| CelersError::Other(format!("Failed to get tasks in group: {}", e)))?;

        let tasks = rows
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
                started_at: row.try_get("started_at").ok(),
                completed_at: row.try_get("completed_at").ok(),
                worker_id: row.try_get("worker_id").ok(),
                error_message: row.try_get("error_message").ok(),
            })
            .collect();

        Ok(tasks)
    }

    /// Cancel all tasks in a group
    ///
    /// Cancels all pending and processing tasks in a task group.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let group_id = "my-task-group-id";
    ///
    /// let cancelled = broker.cancel_task_group(group_id, "Job cancelled by user").await?;
    /// println!("Cancelled {} tasks", cancelled);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn cancel_task_group(&self, group_id: &str, reason: &str) -> Result<i64> {
        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = 'cancelled',
                error_message = $3,
                completed_at = NOW()
            WHERE queue_name = $1
              AND metadata->>'task_group_id' = $2
              AND state IN ('pending', 'processing')
            "#,
        )
        .bind(&self.queue_name)
        .bind(group_id)
        .bind(reason)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to cancel task group: {}", e)))?;

        let count = result.rows_affected() as i64;
        tracing::info!(group_id = %group_id, cancelled = count, "Cancelled task group");
        Ok(count)
    }

    /// Add tags to tasks
    ///
    /// Tags tasks with one or more labels for flexible categorization and filtering.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    /// use uuid::Uuid;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let task_ids = vec![Uuid::new_v4(), Uuid::new_v4()];
    /// let tags = vec!["urgent", "customer-facing", "production"];
    ///
    /// let tagged = broker.tag_tasks(&task_ids, &tags).await?;
    /// println!("Tagged {} tasks", tagged);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn tag_tasks(&self, task_ids: &[Uuid], tags: &[&str]) -> Result<i64> {
        if task_ids.is_empty() || tags.is_empty() {
            return Ok(0);
        }

        let tags_json = serde_json::to_value(tags)
            .map_err(|e| CelersError::Other(format!("Failed to serialize tags: {}", e)))?;

        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET metadata = jsonb_set(
                COALESCE(metadata, '{}'::jsonb),
                '{tags}',
                COALESCE(metadata->'tags', '[]'::jsonb) || $1::jsonb
            )
            WHERE queue_name = $2 AND id = ANY($3)
            "#,
        )
        .bind(&tags_json)
        .bind(&self.queue_name)
        .bind(task_ids)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to tag tasks: {}", e)))?;

        let count = result.rows_affected() as i64;
        tracing::info!(task_count = count, tags = ?tags, "Tagged tasks");
        Ok(count)
    }

    /// Find tasks by tag
    ///
    /// Retrieves tasks that have been tagged with a specific label.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// // Find all urgent tasks
    /// let urgent_tasks = broker.find_tasks_by_tag("urgent", Some("pending"), 100).await?;
    /// println!("Found {} urgent pending tasks", urgent_tasks.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn find_tasks_by_tag(
        &self,
        tag: &str,
        state_filter: Option<&str>,
        limit: i64,
    ) -> Result<Vec<TaskInfo>> {
        let query = if state_filter.is_some() {
            r#"
            SELECT
                id, task_name, state, priority, retry_count, max_retries,
                created_at, scheduled_at, started_at, completed_at, worker_id, error_message
            FROM celers_tasks
            WHERE queue_name = $1
              AND metadata->'tags' ? $2
              AND state = $3
            ORDER BY priority DESC, created_at ASC
            LIMIT $4
            "#
        } else {
            r#"
            SELECT
                id, task_name, state, priority, retry_count, max_retries,
                created_at, scheduled_at, started_at, completed_at, worker_id, error_message
            FROM celers_tasks
            WHERE queue_name = $1
              AND metadata->'tags' ? $2
            ORDER BY priority DESC, created_at ASC
            LIMIT $3
            "#
        };

        let rows = if let Some(state) = state_filter {
            sqlx::query(query)
                .bind(&self.queue_name)
                .bind(tag)
                .bind(state)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
        } else {
            sqlx::query(query)
                .bind(&self.queue_name)
                .bind(tag)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
        }
        .map_err(|e| CelersError::Other(format!("Failed to find tasks by tag: {}", e)))?;

        let tasks = rows
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
                started_at: row.try_get("started_at").ok(),
                completed_at: row.try_get("completed_at").ok(),
                worker_id: row.try_get("worker_id").ok(),
                error_message: row.try_get("error_message").ok(),
            })
            .collect();

        Ok(tasks)
    }

    /// Get all distinct tags in use
    ///
    /// Returns a list of all unique tags currently assigned to tasks.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let tags = broker.get_all_tags().await?;
    /// println!("Tags in use: {:?}", tags);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_all_tags(&self) -> Result<Vec<String>> {
        let rows = sqlx::query(
            r#"
            SELECT DISTINCT jsonb_array_elements_text(metadata->'tags') as tag
            FROM celers_tasks
            WHERE queue_name = $1
              AND metadata ? 'tags'
            ORDER BY tag
            "#,
        )
        .bind(&self.queue_name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get tags: {}", e)))?;

        let tags = rows.iter().map(|row| row.get("tag")).collect();
        Ok(tags)
    }

    /// Get tag statistics
    ///
    /// Returns counts of tasks for each tag, useful for understanding tag distribution.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let stats = broker.get_tag_statistics().await?;
    /// for (tag, count) in stats {
    ///     println!("{}: {} tasks", tag, count);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_tag_statistics(&self) -> Result<Vec<(String, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT
                jsonb_array_elements_text(metadata->'tags') as tag,
                COUNT(*) as task_count
            FROM celers_tasks
            WHERE queue_name = $1
              AND metadata ? 'tags'
            GROUP BY tag
            ORDER BY task_count DESC, tag
            "#,
        )
        .bind(&self.queue_name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get tag statistics: {}", e)))?;

        let stats = rows
            .iter()
            .map(|row| {
                let tag: String = row.get("tag");
                let count: i64 = row.get("task_count");
                (tag, count)
            })
            .collect();

        Ok(stats)
    }

    /// Check queue health with thresholds
    ///
    /// Performs comprehensive queue health check and returns issues/warnings based on
    /// configurable thresholds. Useful for automated alerting and monitoring.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::{PostgresBroker, QueueHealthThresholds};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let thresholds = QueueHealthThresholds {
    ///     max_pending_tasks: 1000,
    ///     max_processing_tasks: 100,
    ///     max_dlq_tasks: 50,
    ///     max_oldest_pending_age_secs: 3600,  // 1 hour
    ///     min_success_rate: 0.95,  // 95%
    /// };
    ///
    /// let health = broker.check_queue_health_with_thresholds(&thresholds).await?;
    /// if !health.is_healthy {
    ///     println!("Queue health issues:");
    ///     for issue in &health.issues {
    ///         println!("  - {}", issue);
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn check_queue_health_with_thresholds(
        &self,
        thresholds: &QueueHealthThresholds,
    ) -> Result<QueueHealthCheck> {
        let stats = self.get_statistics().await?;
        let oldest_pending = self.oldest_pending_age_secs().await?;
        let success_rate = self.success_rate().await?;

        let mut issues = Vec::new();
        let mut warnings = Vec::new();

        // Check pending queue depth
        if stats.pending > thresholds.max_pending_tasks {
            issues.push(format!(
                "Pending queue depth ({}) exceeds threshold ({})",
                stats.pending, thresholds.max_pending_tasks
            ));
        } else if stats.pending > (thresholds.max_pending_tasks as f64 * 0.8) as i64 {
            warnings.push(format!(
                "Pending queue depth ({}) approaching threshold ({})",
                stats.pending, thresholds.max_pending_tasks
            ));
        }

        // Check processing queue depth
        if stats.processing > thresholds.max_processing_tasks {
            issues.push(format!(
                "Processing tasks ({}) exceed threshold ({})",
                stats.processing, thresholds.max_processing_tasks
            ));
        }

        // Check DLQ size
        if stats.dlq > thresholds.max_dlq_tasks {
            issues.push(format!(
                "DLQ size ({}) exceeds threshold ({})",
                stats.dlq, thresholds.max_dlq_tasks
            ));
        } else if stats.dlq > (thresholds.max_dlq_tasks as f64 * 0.5) as i64 {
            warnings.push(format!(
                "DLQ size ({}) increasing, consider investigating failures",
                stats.dlq
            ));
        }

        // Check oldest pending task age
        if let Some(age) = oldest_pending {
            if age > thresholds.max_oldest_pending_age_secs {
                issues.push(format!(
                    "Oldest pending task age ({} seconds) exceeds threshold ({} seconds)",
                    age, thresholds.max_oldest_pending_age_secs
                ));
            }
        }

        // Check success rate
        if success_rate < thresholds.min_success_rate {
            issues.push(format!(
                "Success rate ({:.2}%) below threshold ({:.2}%)",
                success_rate * 100.0,
                thresholds.min_success_rate * 100.0
            ));
        }

        let is_healthy = issues.is_empty();

        Ok(QueueHealthCheck {
            is_healthy,
            issues,
            warnings,
            stats,
            oldest_pending_age_secs: oldest_pending,
            success_rate,
            checked_at: Utc::now(),
        })
    }

    /// Get queue performance score
    ///
    /// Calculates a performance score (0.0-1.0) based on multiple health factors.
    /// Useful for dashboards and trending.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let score = broker.get_queue_performance_score().await?;
    /// println!("Queue performance score: {:.2}/1.00", score);
    ///
    /// if score < 0.7 {
    ///     println!("Warning: Queue performance degraded!");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_queue_performance_score(&self) -> Result<f64> {
        let stats = self.get_statistics().await?;
        let success_rate = self.success_rate().await?;
        let oldest_pending = self.oldest_pending_age_secs().await?;

        let mut score = 1.0;

        // Factor 1: Success rate (30% weight)
        score *= 0.7 + (success_rate * 0.3);

        // Factor 2: Queue depth health (30% weight)
        let total_active = stats.pending + stats.processing;
        let queue_health_factor = if total_active < 100 {
            1.0
        } else if total_active < 1000 {
            0.9
        } else if total_active < 5000 {
            0.7
        } else {
            0.5
        };
        score *= 0.7 + (queue_health_factor * 0.3);

        // Factor 3: DLQ size (20% weight)
        let dlq_factor = if stats.dlq == 0 {
            1.0
        } else if stats.dlq < 10 {
            0.9
        } else if stats.dlq < 50 {
            0.7
        } else {
            0.5
        };
        score *= 0.8 + (dlq_factor * 0.2);

        // Factor 4: Oldest pending task age (20% weight)
        let age_factor = if let Some(age) = oldest_pending {
            if age < 60 {
                1.0 // < 1 minute
            } else if age < 300 {
                0.9 // < 5 minutes
            } else if age < 1800 {
                0.7 // < 30 minutes
            } else {
                0.5 // > 30 minutes
            }
        } else {
            1.0
        };
        score *= 0.8 + (age_factor * 0.2);

        Ok(score.clamp(0.0, 1.0))
    }
}
