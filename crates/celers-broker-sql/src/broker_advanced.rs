//! Advanced broker operations (part 1)
//!
//! Prometheus metrics updates, DLQ retention, optimal batch sizing,
//! pool health, vacuum/analyze, slow queries, priority aging,
//! task progress tracking, rate limiting, deduplication, and cascade cancel.

use crate::broker_batch::SlowQueryInfo;
use crate::broker_core::MysqlBroker;
use crate::types::*;
use celers_core::{CelersError, Result, SerializedTask, TaskId};
use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::Row;
use std::time::Duration;
use uuid::Uuid;

#[cfg(feature = "metrics")]
use celers_metrics::{DLQ_SIZE, PROCESSING_QUEUE_SIZE, QUEUE_SIZE};

impl MysqlBroker {
    /// Update Prometheus metrics gauges for queue sizes
    ///
    /// This should be called periodically (e.g., every few seconds) to keep
    /// metrics up to date. Not part of the Broker trait, but useful for monitoring.
    #[cfg(feature = "metrics")]
    pub async fn update_metrics(&self) -> Result<()> {
        // Get pending tasks count
        let pending_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM celers_tasks WHERE state = 'pending'")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| CelersError::Other(format!("Failed to get pending count: {}", e)))?;

        // Get processing tasks count
        let processing_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM celers_tasks WHERE state = 'processing'")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| {
                    CelersError::Other(format!("Failed to get processing count: {}", e))
                })?;

        // Get DLQ count
        let dlq_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM celers_dead_letter_queue")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get DLQ count: {}", e)))?;

        // Update gauges
        QUEUE_SIZE.set(pending_count as f64);
        PROCESSING_QUEUE_SIZE.set(processing_count as f64);
        DLQ_SIZE.set(dlq_count as f64);

        Ok(())
    }

    /// Apply DLQ retention policy - delete old DLQ entries based on age
    ///
    /// This helps prevent unbounded DLQ growth by removing entries older than the specified retention period.
    /// Useful for production systems where DLQ entries are monitored but eventually need cleanup.
    ///
    /// # Arguments
    /// * `retention_period` - Duration after which DLQ entries should be deleted
    ///
    /// # Returns
    /// Number of DLQ entries deleted
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # use std::time::Duration;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    ///
    /// // Delete DLQ entries older than 30 days
    /// let deleted = broker.apply_dlq_retention(Duration::from_secs(30 * 24 * 3600)).await?;
    /// println!("Deleted {} old DLQ entries", deleted);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn apply_dlq_retention(&self, retention_period: Duration) -> Result<u64> {
        let retention_seconds = retention_period.as_secs() as i64;

        // Validate retention period (warn if too short to prevent accidental deletion)
        if retention_seconds < 3600 {
            return Err(CelersError::Other(
                "DLQ retention period must be at least 1 hour to prevent accidental deletion"
                    .to_string(),
            ));
        }
        if retention_seconds < 86400 {
            tracing::warn!(
                retention_hours = retention_seconds / 3600,
                "DLQ retention period is less than 24 hours"
            );
        }

        let result = sqlx::query(
            r#"
            DELETE FROM celers_dead_letter_queue
            WHERE TIMESTAMPDIFF(SECOND, failed_at, NOW()) > ?
            "#,
        )
        .bind(retention_seconds)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to apply DLQ retention: {}", e)))?;

        let deleted = result.rows_affected();
        if deleted > 0 {
            tracing::info!(
                count = deleted,
                retention_days = retention_seconds / 86400,
                "Applied DLQ retention policy"
            );
        }

        Ok(deleted)
    }

    /// Calculate optimal batch size based on current queue depth and load
    ///
    /// This implements an adaptive batch sizing strategy:
    /// - Small batches (1-5) when queue is nearly empty to reduce latency
    /// - Medium batches (10-50) for moderate load
    /// - Large batches (50-200) for high load to maximize throughput
    ///
    /// # Arguments
    /// * `max_batch_size` - Maximum batch size to return (default: 200)
    ///
    /// # Returns
    /// Recommended batch size based on current queue state
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # use celers_core::Broker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    ///
    /// // Get adaptive batch size
    /// let batch_size = broker.get_optimal_batch_size(Some(100)).await?;
    /// let messages = broker.dequeue_batch(batch_size as usize).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_optimal_batch_size(&self, max_batch_size: Option<i64>) -> Result<i64> {
        // Validate max_batch_size if provided
        if let Some(max) = max_batch_size {
            if max <= 0 {
                return Err(CelersError::Other(
                    "max_batch_size must be positive".to_string(),
                ));
            }
            if max > 10000 {
                tracing::warn!(
                    max_batch_size = max,
                    "Very large max_batch_size may impact performance"
                );
            }
        }

        let max_size = max_batch_size.unwrap_or(200);

        // Get current pending task count
        let pending: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM celers_tasks WHERE state = 'pending' AND scheduled_at <= NOW()",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get pending count: {}", e)))?;

        // Adaptive batch sizing based on queue depth
        let optimal_size = if pending < 10 {
            // Small queue - use small batches to reduce latency
            std::cmp::min(pending.max(1), 5)
        } else if pending < 100 {
            // Medium queue - balance latency and throughput
            std::cmp::min(pending / 2, 50)
        } else {
            // Large queue - maximize throughput
            std::cmp::min(pending / 4, max_size)
        };

        Ok(optimal_size.max(1))
    }

    /// Get connection pool health status with detailed metrics
    ///
    /// Returns comprehensive connection pool metrics including:
    /// - Pool size and utilization
    /// - Active vs idle connections
    /// - Connection wait times (if available)
    /// - Pool pressure indicators
    ///
    /// # Returns
    /// Detailed connection diagnostics including health status
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    ///
    /// let health = broker.get_pool_health().await?;
    /// if health.pool_utilization_percent > 80.0 {
    ///     println!("Warning: Connection pool utilization is high!");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_pool_health(&self) -> Result<ConnectionDiagnostics> {
        let total = self.pool.size();
        let idle = self.pool.num_idle() as u32;
        let active = total - idle;
        let max = self.pool.options().get_max_connections();

        let utilization = if max > 0 {
            (total as f64 / max as f64) * 100.0
        } else {
            0.0
        };

        Ok(ConnectionDiagnostics {
            total_connections: total,
            idle_connections: idle,
            active_connections: active,
            max_connections: max,
            connection_wait_time_ms: None, // MySQL driver doesn't expose this
            pool_utilization_percent: utilization,
        })
    }

    /// Compress task payload using DEFLATE compression
    ///
    /// This can significantly reduce storage and network overhead for large task payloads.
    /// Compression is applied transparently and decompression happens automatically during dequeue.
    ///
    /// # Arguments
    /// * `payload` - Raw task payload bytes
    ///
    /// # Returns
    /// Compressed payload bytes
    ///
    /// Note: Only use compression for payloads larger than ~1KB, as small payloads may
    /// actually grow due to compression overhead.
    #[allow(dead_code)]
    fn compress_payload(payload: &[u8]) -> Result<Vec<u8>> {
        // Only compress if payload is larger than 1KB
        if payload.len() < 1024 {
            return Ok(payload.to_vec());
        }

        oxiarc_deflate::deflate(payload, 1)
            .map_err(|e| CelersError::Other(format!("Compression failed: {}", e)))
    }

    /// Decompress task payload
    ///
    /// # Arguments
    /// * `compressed` - Compressed payload bytes
    ///
    /// # Returns
    /// Decompressed payload bytes
    #[allow(dead_code)]
    fn decompress_payload(compressed: &[u8]) -> Result<Vec<u8>> {
        oxiarc_deflate::inflate(compressed)
            .map_err(|e| CelersError::Other(format!("Decompression failed: {}", e)))
    }

    /// Vacuum analyze all CeleRS tables for optimal query performance
    ///
    /// This operation is similar to PostgreSQL's VACUUM ANALYZE but uses MySQL-specific
    /// optimizations (OPTIMIZE TABLE + ANALYZE TABLE). It:
    /// - Reclaims storage from deleted rows
    /// - Updates table statistics for better query planning
    /// - Defragments table data
    ///
    /// Should be run periodically (e.g., weekly) on production systems.
    ///
    /// # Returns
    /// Number of tables optimized
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    ///
    /// // Run maintenance
    /// let tables_optimized = broker.vacuum_analyze().await?;
    /// println!("Optimized {} tables", tables_optimized);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn vacuum_analyze(&self) -> Result<u64> {
        let tables = vec![
            "celers_tasks",
            "celers_dead_letter_queue",
            "celers_task_history",
            "celers_task_results",
        ];

        let mut optimized = 0u64;

        for table in &tables {
            // OPTIMIZE TABLE
            sqlx::query(&format!("OPTIMIZE TABLE {}", table))
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    CelersError::Other(format!("Failed to optimize table {}: {}", table, e))
                })?;

            // ANALYZE TABLE
            sqlx::query(&format!("ANALYZE TABLE {}", table))
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    CelersError::Other(format!("Failed to analyze table {}: {}", table, e))
                })?;

            optimized += 1;
        }

        tracing::info!(tables_count = optimized, "Completed vacuum analyze");
        Ok(optimized)
    }

    /// Get slow query log entries related to CeleRS tables
    ///
    /// Returns queries that exceeded a certain threshold from MySQL slow query log.
    /// Requires slow query log to be enabled in MySQL configuration.
    ///
    /// # Arguments
    /// * `limit` - Maximum number of slow queries to return
    ///
    /// # Returns
    /// List of slow query information (query text, execution time, etc.)
    ///
    /// Note: This requires MySQL slow_query_log to be enabled and accessible.
    pub async fn get_slow_queries(&self, limit: i64) -> Result<Vec<SlowQueryInfo>> {
        // Check if performance_schema is enabled
        let ps_enabled: String = sqlx::query_scalar("SELECT @@performance_schema")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to check performance_schema: {}", e))
            })?;

        if ps_enabled != "1" {
            return Ok(Vec::new());
        }

        // Query from events_statements_summary_by_digest
        let rows = sqlx::query(
            r#"
            SELECT
                DIGEST_TEXT as query_text,
                COUNT_STAR as execution_count,
                AVG_TIMER_WAIT / 1000000000 as avg_time_ms,
                MAX_TIMER_WAIT / 1000000000 as max_time_ms,
                SUM_TIMER_WAIT / 1000000000 as total_time_ms
            FROM performance_schema.events_statements_summary_by_digest
            WHERE DIGEST_TEXT LIKE '%celers_%'
            ORDER BY SUM_TIMER_WAIT DESC
            LIMIT ?
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to query slow queries: {}", e)))?;

        let mut slow_queries = Vec::new();
        for row in rows {
            slow_queries.push(SlowQueryInfo {
                query_text: row.try_get("query_text").unwrap_or_default(),
                execution_count: row.try_get("execution_count").unwrap_or(0),
                avg_time_ms: row.try_get("avg_time_ms").unwrap_or(0.0),
                max_time_ms: row.try_get("max_time_ms").unwrap_or(0.0),
                total_time_ms: row.try_get("total_time_ms").unwrap_or(0.0),
            });
        }

        Ok(slow_queries)
    }

    /// Apply priority aging to prevent task starvation
    ///
    /// Increases the priority of tasks that have been pending for a long time.
    /// This prevents low-priority tasks from being starved by a continuous stream
    /// of high-priority tasks.
    ///
    /// # Arguments
    /// * `age_threshold_secs` - Tasks older than this will have their priority increased
    /// * `priority_boost` - Amount to add to the priority (default: 10)
    ///
    /// # Returns
    /// Number of tasks whose priority was increased
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    ///
    /// // Boost priority for tasks pending more than 5 minutes
    /// let boosted = broker.apply_priority_aging(300, 10).await?;
    /// println!("Boosted priority for {} old tasks", boosted);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn apply_priority_aging(
        &self,
        age_threshold_secs: i64,
        priority_boost: i32,
    ) -> Result<u64> {
        // Validate parameters
        if age_threshold_secs <= 0 {
            return Err(CelersError::Other(
                "age_threshold_secs must be positive".to_string(),
            ));
        }
        if priority_boost <= 0 {
            return Err(CelersError::Other(
                "priority_boost must be positive".to_string(),
            ));
        }
        if priority_boost > 100 {
            tracing::warn!(
                priority_boost = priority_boost,
                "Large priority boost may cause priority inversion"
            );
        }

        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET priority = priority + ?
            WHERE state = 'pending'
              AND TIMESTAMPDIFF(SECOND, created_at, NOW()) > ?
              AND priority < 1000
            "#,
        )
        .bind(priority_boost)
        .bind(age_threshold_secs)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to apply priority aging: {}", e)))?;

        let updated = result.rows_affected();
        if updated > 0 {
            tracing::info!(
                count = updated,
                age_threshold_secs = age_threshold_secs,
                priority_boost = priority_boost,
                "Applied priority aging"
            );
        }

        Ok(updated)
    }

    /// Update task progress for long-running tasks
    ///
    /// Allows workers to report progress on long-running tasks. This is stored
    /// in the task metadata as JSON and can be queried later.
    ///
    /// # Arguments
    /// * `task_id` - Task ID to update
    /// * `progress_percent` - Progress percentage (0.0 - 100.0)
    /// * `current_step` - Optional description of current step
    ///
    /// # Returns
    /// True if task was updated, false if not found
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # use uuid::Uuid;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    /// let task_id = Uuid::new_v4();
    ///
    /// // Update progress to 50%
    /// broker.update_task_progress(&task_id, 50.0, Some("Processing chunk 5/10")).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn update_task_progress(
        &self,
        task_id: &TaskId,
        progress_percent: f64,
        current_step: Option<&str>,
    ) -> Result<bool> {
        // Validate progress_percent is in valid range
        if !(0.0..=100.0).contains(&progress_percent) {
            return Err(CelersError::Other(format!(
                "progress_percent must be between 0.0 and 100.0, got {}",
                progress_percent
            )));
        }

        let progress_json = serde_json::json!({
            "progress_percent": progress_percent,
            "current_step": current_step,
            "updated_at": chrono::Utc::now().to_rfc3339(),
        });

        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET metadata = JSON_SET(
                metadata,
                '$.progress', ?
            )
            WHERE id = ? AND state = 'processing'
            "#,
        )
        .bind(serde_json::to_string(&progress_json).unwrap_or_else(|_| "{}".to_string()))
        .bind(task_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to update task progress: {}", e)))?;

        Ok(result.rows_affected() > 0)
    }

    /// Get task progress for a specific task
    ///
    /// # Arguments
    /// * `task_id` - Task ID to query
    ///
    /// # Returns
    /// Task progress information if available
    pub async fn get_task_progress(&self, task_id: &TaskId) -> Result<Option<TaskProgress>> {
        let row = sqlx::query(
            r#"
            SELECT
                id,
                JSON_EXTRACT(metadata, '$.progress.progress_percent') as progress_percent,
                JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.progress.current_step')) as current_step,
                JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.progress.updated_at')) as updated_at
            FROM celers_tasks
            WHERE id = ?
            "#,
        )
        .bind(task_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get task progress: {}", e)))?;

        if let Some(row) = row {
            let progress_percent: Option<f64> = row.try_get("progress_percent").ok();
            let current_step: Option<String> = row.try_get("current_step").ok();
            let updated_at_str: Option<String> = row.try_get("updated_at").ok();

            if let Some(percent) = progress_percent {
                let updated_at = updated_at_str
                    .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_else(Utc::now);

                return Ok(Some(TaskProgress {
                    task_id: *task_id,
                    progress_percent: percent,
                    current_step,
                    total_steps: None,
                    updated_at,
                }));
            }
        }

        Ok(None)
    }

    /// Check rate limit for a specific task type
    ///
    /// Returns current execution rate and whether the limit is exceeded.
    ///
    /// # Arguments
    /// * `task_name` - Task type to check
    /// * `max_per_minute` - Maximum tasks per minute allowed
    ///
    /// # Returns
    /// Rate limit status including current rate and whether limit is exceeded
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    ///
    /// // Check if we can execute more "expensive_task" (limit: 100/min)
    /// let status = broker.check_rate_limit("expensive_task", 100).await?;
    /// if status.limit_exceeded {
    ///     println!("Rate limit exceeded, backing off...");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn check_rate_limit(
        &self,
        task_name: &str,
        max_per_minute: i64,
    ) -> Result<RateLimitStatus> {
        // Count completed tasks in the last minute
        let completed_last_minute: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM celers_tasks
            WHERE task_name = ?
              AND state = 'completed'
              AND completed_at >= DATE_SUB(NOW(), INTERVAL 1 MINUTE)
            "#,
        )
        .bind(task_name)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to check rate limit: {}", e)))?;

        // Count for last hour
        let completed_last_hour: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM celers_tasks
            WHERE task_name = ?
              AND state = 'completed'
              AND completed_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
            "#,
        )
        .bind(task_name)
        .fetch_one(&self.pool)
        .await
        .unwrap_or(0);

        let per_second = completed_last_minute as f64 / 60.0;
        let limit_exceeded = completed_last_minute >= max_per_minute;

        Ok(RateLimitStatus {
            task_name: task_name.to_string(),
            current_per_second: per_second,
            current_per_minute: completed_last_minute,
            current_per_hour: completed_last_hour,
            limit_exceeded,
        })
    }

    /// Deduplicate tasks within a time window
    ///
    /// Prevents duplicate tasks from being enqueued if a matching task exists
    /// within the specified time window.
    ///
    /// # Arguments
    /// * `task` - Task to enqueue
    /// * `dedup_key` - Deduplication key
    /// * `window_secs` - Time window in seconds to check for duplicates
    ///
    /// # Returns
    /// TaskId - Either the existing task ID or a new task ID
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # use celers_core::SerializedTask;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    /// let task = SerializedTask::new("process_order".to_string(), vec![1, 2, 3]);
    ///
    /// // Only enqueue if no matching task in last 5 minutes
    /// let task_id = broker.enqueue_deduplicated_window(task, "order-123", 300).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn enqueue_deduplicated_window(
        &self,
        task: SerializedTask,
        dedup_key: &str,
        window_secs: i64,
    ) -> Result<TaskId> {
        // Check for existing task within window
        let existing: Option<String> = sqlx::query_scalar(
            r#"
            SELECT id
            FROM celers_tasks
            WHERE JSON_EXTRACT(metadata, '$.dedup_key') = ?
              AND created_at >= DATE_SUB(NOW(), INTERVAL ? SECOND)
              AND state IN ('pending', 'processing')
            LIMIT 1
            "#,
        )
        .bind(dedup_key)
        .bind(window_secs)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to check for duplicates: {}", e)))?;

        if let Some(id_str) = existing {
            // Return existing task ID
            let task_id = Uuid::parse_str(&id_str)
                .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?;
            tracing::debug!(
                task_id = %task_id,
                dedup_key = dedup_key,
                "Found duplicate task within window"
            );
            return Ok(task_id);
        }

        // No duplicate found, enqueue new task with dedup_key
        let task_id = task.metadata.id;
        let mut db_metadata = json!({
            "queue": self.queue_name,
            "enqueued_at": chrono::Utc::now().to_rfc3339(),
            "dedup_key": dedup_key,
        });

        // Merge task metadata
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
        .map_err(|e| CelersError::Other(format!("Failed to enqueue task: {}", e)))?;

        Ok(task_id)
    }

    /// Cascade cancel - cancel a task and all its dependent tasks
    ///
    /// When a task is cancelled, this will also cancel any tasks that depend on it
    /// (identified by metadata relationships).
    ///
    /// # Arguments
    /// * `task_id` - Parent task ID to cancel
    ///
    /// # Returns
    /// Number of tasks cancelled (including the parent)
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # use uuid::Uuid;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = MysqlBroker::new("mysql://localhost/db").await?;
    /// let parent_id = Uuid::new_v4();
    ///
    /// // Cancel task and all dependent tasks
    /// let cancelled = broker.cancel_cascade(&parent_id).await?;
    /// println!("Cancelled {} tasks (including dependents)", cancelled);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn cancel_cascade(&self, task_id: &TaskId) -> Result<u64> {
        // First cancel the parent task
        let parent_result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = 'cancelled',
                completed_at = NOW()
            WHERE id = ?
              AND state IN ('pending', 'processing')
            "#,
        )
        .bind(task_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to cancel parent task: {}", e)))?;

        let mut total_cancelled = parent_result.rows_affected();

        // Cancel dependent tasks (those with parent_task_id in metadata)
        let dependent_result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = 'cancelled',
                completed_at = NOW(),
                error_message = CONCAT(
                    COALESCE(error_message, ''),
                    'Cancelled due to parent task cancellation'
                )
            WHERE JSON_EXTRACT(metadata, '$.parent_task_id') = ?
              AND state IN ('pending', 'processing')
            "#,
        )
        .bind(task_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to cancel dependent tasks: {}", e)))?;

        total_cancelled += dependent_result.rows_affected();

        if total_cancelled > 0 {
            tracing::info!(
                parent_task_id = %task_id,
                total_cancelled = total_cancelled,
                "Cascade cancelled tasks"
            );
        }

        Ok(total_cancelled)
    }
}
