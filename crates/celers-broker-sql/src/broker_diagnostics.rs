//! Diagnostics, profiling, and statistics for MysqlBroker
//!
//! Load generation, migration verification, query performance profiling,
//! connection pool warmup, latency stats, priority queue stats,
//! execution stats, queue saturation, and task state transitions.

use crate::broker_batch::BatchResultInput;
use crate::broker_core::MysqlBroker;
use crate::stats_types::*;
use celers_core::{Broker, CelersError, Result, SerializedTask, TaskId};
use chrono::Utc;
use sqlx::Row;
use uuid::Uuid;

impl MysqlBroker {
    /// Generate synthetic load for performance testing
    ///
    /// Creates a specified number of test tasks with configurable properties.
    /// Useful for load testing, performance benchmarking, and capacity planning.
    ///
    /// # Arguments
    ///
    /// * `task_count` - Number of tasks to generate
    /// * `task_name` - Name for generated tasks
    /// * `payload_size_bytes` - Size of payload for each task (filled with random data)
    /// * `priority_range` - Optional priority range (min, max) for random priorities
    ///
    /// # Returns
    ///
    /// Vector of generated task IDs
    ///
    /// # Examples
    ///
    /// ```
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = MysqlBroker::new("mysql://localhost/test").await?;
    /// // Generate 1000 test tasks with 1KB payload, random priorities 1-10
    /// let task_ids = broker.generate_load(
    ///     1000,
    ///     "load_test",
    ///     1024,
    ///     Some((1, 10))
    /// ).await?;
    /// println!("Generated {} test tasks", task_ids.len());
    ///
    /// // Generate 500 high-priority tasks with 512-byte payload
    /// let task_ids = broker.generate_load(
    ///     500,
    ///     "stress_test",
    ///     512,
    ///     Some((8, 10))
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn generate_load(
        &self,
        task_count: usize,
        task_name: &str,
        payload_size_bytes: usize,
        priority_range: Option<(i32, i32)>,
    ) -> Result<Vec<Uuid>> {
        use rand::RngExt;

        let mut tasks = Vec::with_capacity(task_count);
        let mut rng = rand::rng();

        for _i in 0..task_count {
            // Generate random payload
            let payload: Vec<u8> = (0..payload_size_bytes)
                .map(|_| rng.random::<u8>())
                .collect();

            let mut task = SerializedTask::new(task_name.to_string(), payload);

            // Set random priority if range specified
            if let Some((min_prio, max_prio)) = priority_range {
                task.metadata.priority = rng.random_range(min_prio..=max_prio);
            }

            tasks.push(task);
        }

        let task_ids = self.enqueue_batch(tasks).await?;

        tracing::info!(
            count = task_count,
            task_name = task_name,
            payload_size = payload_size_bytes,
            "Generated synthetic load"
        );

        Ok(task_ids)
    }

    /// Verify migration integrity
    ///
    /// Checks that all required migrations have been applied and that the
    /// database schema matches expectations. This is useful for deployment
    /// verification and troubleshooting.
    ///
    /// # Returns
    ///
    /// Result containing migration verification status
    ///
    /// # Examples
    ///
    /// ```
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = MysqlBroker::new("mysql://localhost/test").await?;
    /// match broker.verify_migrations().await {
    ///     Ok(report) => {
    ///         println!("Migrations verified: {} applied, {} missing",
    ///             report.applied_count, report.missing_count);
    ///         if report.is_complete {
    ///             println!("All migrations applied successfully");
    ///         }
    ///     }
    ///     Err(e) => eprintln!("Migration verification failed: {}", e),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn verify_migrations(&self) -> Result<MigrationVerification> {
        // Check if migrations table exists
        let table_exists = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM information_schema.tables
             WHERE table_schema = DATABASE()
             AND table_name = 'celers_migrations'",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to check migrations table: {}", e)))?;

        if table_exists == 0 {
            return Ok(MigrationVerification {
                is_complete: false,
                applied_count: 0,
                missing_count: 8, // Total number of migrations
                applied_migrations: vec![],
                missing_migrations: vec![
                    "001_init.sql".to_string(),
                    "002_results.sql".to_string(),
                    "003_performance_indexes.sql".to_string(),
                    "004_partitioning_guide.sql".to_string(),
                    "005_uuid_optimization.sql".to_string(),
                    "006_idempotency.sql".to_string(),
                    "007_workflow.sql".to_string(),
                    "008_production_features.sql".to_string(),
                ],
                schema_valid: false,
            });
        }

        // Get applied migrations
        let applied: Vec<String> =
            sqlx::query_scalar("SELECT version FROM celers_migrations ORDER BY version")
                .fetch_all(&self.pool)
                .await
                .map_err(|e| {
                    CelersError::Other(format!("Failed to fetch applied migrations: {}", e))
                })?;

        let expected = [
            "001_init.sql",
            "002_results.sql",
            "003_performance_indexes.sql",
            "006_idempotency.sql",
            "008_production_features.sql",
        ];

        let missing: Vec<String> = expected
            .iter()
            .filter(|&v| !applied.contains(&v.to_string()))
            .map(|s| s.to_string())
            .collect();

        // Check core tables exist
        let core_tables = vec![
            "celers_tasks",
            "celers_dead_letter_queue",
            "celers_task_results",
            "celers_task_idempotency",
            "celers_queue_config",
            "celers_worker_heartbeat",
            "celers_task_groups",
        ];

        let mut schema_valid = true;
        for table in &core_tables {
            let exists = sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM information_schema.tables
                 WHERE table_schema = DATABASE() AND table_name = ?",
            )
            .bind(table)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to check table {}: {}", table, e)))?;

            if exists == 0 {
                schema_valid = false;
                tracing::warn!(table = table, "Core table missing");
            }
        }

        Ok(MigrationVerification {
            is_complete: missing.is_empty() && schema_valid,
            applied_count: applied.len(),
            missing_count: missing.len(),
            applied_migrations: applied,
            missing_migrations: missing,
            schema_valid,
        })
    }

    /// Profile query performance and identify slow operations
    ///
    /// Analyzes recent query performance from MySQL performance_schema and
    /// identifies potential bottlenecks. Requires performance_schema to be enabled.
    ///
    /// # Arguments
    ///
    /// * `min_execution_time_ms` - Minimum execution time to report (filters fast queries)
    /// * `limit` - Maximum number of slow queries to return
    ///
    /// # Returns
    ///
    /// Vector of query performance profiles
    ///
    /// # Examples
    ///
    /// ```
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = MysqlBroker::new("mysql://localhost/test").await?;
    /// // Find queries taking more than 100ms
    /// let slow_queries = broker.profile_query_performance(100.0, 10).await?;
    /// for query in slow_queries {
    ///     println!("Slow query: {} ({}ms, {} calls)",
    ///         query.query_digest,
    ///         query.avg_execution_time_ms,
    ///         query.execution_count);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn profile_query_performance(
        &self,
        min_execution_time_ms: f64,
        limit: i64,
    ) -> Result<Vec<QueryPerformanceProfile>> {
        let rows = sqlx::query(
            "SELECT
                DIGEST_TEXT as query_digest,
                COUNT_STAR as execution_count,
                AVG_TIMER_WAIT / 1000000000000 as avg_execution_time_ms,
                SUM_ROWS_EXAMINED as total_rows_examined,
                SUM_ROWS_SENT as total_rows_sent,
                SUM_NO_INDEX_USED as no_index_used_count,
                SUM_NO_GOOD_INDEX_USED as no_good_index_used_count
             FROM performance_schema.events_statements_summary_by_digest
             WHERE DIGEST_TEXT IS NOT NULL
               AND SCHEMA_NAME = DATABASE()
               AND AVG_TIMER_WAIT / 1000000000000 >= ?
             ORDER BY AVG_TIMER_WAIT DESC
             LIMIT ?",
        )
        .bind(min_execution_time_ms)
        .bind(limit)
        .fetch_all(&self.pool)
        .await;

        let rows = match rows {
            Ok(r) => r,
            Err(e) => {
                // performance_schema might not be enabled
                tracing::warn!(error = %e, "Failed to query performance_schema");
                return Ok(vec![]);
            }
        };

        let mut profiles = Vec::new();
        for row in rows {
            let query_digest: String = row.try_get("query_digest").unwrap_or_default();
            let execution_count: i64 = row.try_get("execution_count").unwrap_or(0);
            let avg_time: rust_decimal::Decimal =
                row.try_get("avg_execution_time_ms").unwrap_or_default();
            let rows_examined: i64 = row.try_get("total_rows_examined").unwrap_or(0);
            let rows_sent: i64 = row.try_get("total_rows_sent").unwrap_or(0);
            let no_index: i64 = row.try_get("no_index_used_count").unwrap_or(0);
            let no_good_index: i64 = row.try_get("no_good_index_used_count").unwrap_or(0);

            if query_digest.is_empty() {
                continue; // Skip rows with empty query digest
            }

            profiles.push(QueryPerformanceProfile {
                query_digest,
                execution_count,
                avg_execution_time_ms: avg_time.to_string().parse().unwrap_or(0.0),
                total_rows_examined: rows_examined,
                total_rows_sent: rows_sent,
                no_index_used_count: no_index,
                no_good_index_used_count: no_good_index,
                needs_optimization: no_index > 0 || no_good_index > 0,
            });
        }

        Ok(profiles)
    }

    /// Acknowledge multiple tasks and store their results atomically in a single transaction
    ///
    /// This is more efficient than calling ack() and store_result() separately for each task.
    ///
    /// # Arguments
    ///
    /// * `tasks_with_results` - Vector of (task_id, receipt_handle, result_input) tuples
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use celers_broker_sql::{MysqlBroker, BatchResultInput, TaskResultStatus};
    /// # use uuid::Uuid;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = MysqlBroker::new("mysql://localhost/test").await?;
    /// let tasks_with_results = vec![
    ///     (
    ///         Uuid::new_v4(),
    ///         Some("receipt1".to_string()),
    ///         BatchResultInput {
    ///             task_id: Uuid::new_v4(),
    ///             task_name: "task1".to_string(),
    ///             status: TaskResultStatus::Success,
    ///             result: Some(serde_json::json!({"value": 42})),
    ///             error: None,
    ///             traceback: None,
    ///             runtime_ms: Some(100),
    ///         },
    ///     ),
    /// ];
    ///
    /// broker.ack_batch_with_results(&tasks_with_results).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[allow(dead_code)]
    pub async fn ack_batch_with_results(
        &self,
        tasks_with_results: &[(TaskId, Option<String>, BatchResultInput)],
    ) -> Result<()> {
        if tasks_with_results.is_empty() {
            return Ok(());
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        // Acknowledge all tasks
        for (task_id, _receipt_handle, _) in tasks_with_results {
            sqlx::query(
                "UPDATE celers_tasks
                 SET state = 'completed', completed_at = NOW()
                 WHERE id = ? AND state = 'processing'",
            )
            .bind(task_id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to acknowledge task {}: {}", task_id, e))
            })?;
        }

        // Store all results
        for (_, _, result) in tasks_with_results {
            let result_json = result
                .result
                .as_ref()
                .map(|v| serde_json::to_string(v).unwrap_or_else(|_| "null".to_string()))
                .unwrap_or_else(|| "null".to_string());

            sqlx::query(
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
            })?;
        }

        tx.commit()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to commit transaction: {}", e)))?;

        tracing::info!(
            count = tasks_with_results.len(),
            "Acknowledged tasks with results"
        );
        Ok(())
    }

    /// Pre-warm the connection pool by establishing minimum connections
    ///
    /// This reduces cold start latency by ensuring connections are available
    /// before the first query. Useful for production deployments.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = MysqlBroker::new("mysql://localhost/test").await?;
    /// // Pre-warm connections before starting workers
    /// broker.warmup_connection_pool().await?;
    /// println!("Connection pool is ready");
    /// # Ok(())
    /// # }
    /// ```
    #[allow(dead_code)]
    pub async fn warmup_connection_pool(&self) -> Result<()> {
        // Get pool configuration
        let pool_options = self.pool.options();
        let min_connections = pool_options.get_min_connections();

        tracing::info!(
            min_connections = min_connections,
            "Warming up connection pool"
        );

        // Execute simple queries to establish connections
        for i in 0..min_connections {
            let _ = sqlx::query("SELECT 1")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| {
                    CelersError::Other(format!("Failed to warm up connection {}: {}", i, e))
                })?;
        }

        tracing::info!("Connection pool warmup complete");
        Ok(())
    }

    /// Get task latency statistics (time from enqueue to dequeue)
    ///
    /// Provides insights into how long tasks wait in the queue before processing.
    /// Useful for SLA monitoring and capacity planning.
    ///
    /// # Returns
    ///
    /// TaskLatencyStats with min, max, avg latency in seconds
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = MysqlBroker::new("mysql://localhost/test").await?;
    /// let stats = broker.get_task_latency_stats().await?;
    /// println!("Average queue latency: {:.2}s", stats.avg_latency_secs);
    /// println!("Max queue latency: {:.2}s", stats.max_latency_secs);
    /// # Ok(())
    /// # }
    /// ```
    #[allow(dead_code)]
    pub async fn get_task_latency_stats(&self) -> Result<TaskLatencyStats> {
        let row = sqlx::query(
            "SELECT
                COUNT(*) as task_count,
                MIN(TIMESTAMPDIFF(SECOND, created_at, started_at)) as min_latency,
                MAX(TIMESTAMPDIFF(SECOND, created_at, started_at)) as max_latency,
                AVG(TIMESTAMPDIFF(SECOND, created_at, started_at)) as avg_latency,
                STDDEV(TIMESTAMPDIFF(SECOND, created_at, started_at)) as stddev_latency
             FROM celers_tasks
             WHERE state IN ('processing', 'completed')
               AND started_at IS NOT NULL",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get task latency stats: {}", e)))?;

        let task_count: i64 = row.try_get("task_count").unwrap_or(0);
        let min_latency: Option<i64> = row.try_get("min_latency").ok();
        let max_latency: Option<i64> = row.try_get("max_latency").ok();
        let avg_latency: Option<rust_decimal::Decimal> = row.try_get("avg_latency").ok();
        let stddev_latency: Option<rust_decimal::Decimal> = row.try_get("stddev_latency").ok();

        Ok(TaskLatencyStats {
            task_count,
            min_latency_secs: min_latency.unwrap_or(0) as f64,
            max_latency_secs: max_latency.unwrap_or(0) as f64,
            avg_latency_secs: avg_latency
                .map(|d| d.to_string().parse().unwrap_or(0.0))
                .unwrap_or(0.0),
            stddev_latency_secs: stddev_latency
                .map(|d| d.to_string().parse().unwrap_or(0.0))
                .unwrap_or(0.0),
        })
    }

    /// Get statistics broken down by priority level
    ///
    /// Provides insights into task distribution across priority levels.
    /// Useful for tuning priority-based scheduling.
    ///
    /// # Returns
    ///
    /// Vector of PriorityQueueStats sorted by priority (highest first)
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = MysqlBroker::new("mysql://localhost/test").await?;
    /// let stats = broker.get_priority_queue_stats().await?;
    /// for stat in stats {
    ///     println!("Priority {}: {} pending, {} processing",
    ///         stat.priority, stat.pending_count, stat.processing_count);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[allow(dead_code)]
    pub async fn get_priority_queue_stats(&self) -> Result<Vec<PriorityQueueStats>> {
        let rows = sqlx::query(
            "SELECT
                priority,
                SUM(CASE WHEN state = 'pending' THEN 1 ELSE 0 END) as pending_count,
                SUM(CASE WHEN state = 'processing' THEN 1 ELSE 0 END) as processing_count,
                SUM(CASE WHEN state = 'completed' THEN 1 ELSE 0 END) as completed_count,
                SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END) as failed_count,
                AVG(CASE WHEN started_at IS NOT NULL
                    THEN TIMESTAMPDIFF(SECOND, created_at, started_at)
                    ELSE NULL END) as avg_wait_time_secs
             FROM celers_tasks
             GROUP BY priority
             ORDER BY priority DESC",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get priority queue stats: {}", e)))?;

        let mut stats = Vec::new();
        for row in rows {
            let priority: i32 = row.try_get("priority").unwrap_or(0);
            let pending_count: i64 = row.try_get("pending_count").unwrap_or(0);
            let processing_count: i64 = row.try_get("processing_count").unwrap_or(0);
            let completed_count: i64 = row.try_get("completed_count").unwrap_or(0);
            let failed_count: i64 = row.try_get("failed_count").unwrap_or(0);
            let avg_wait_time: Option<rust_decimal::Decimal> =
                row.try_get("avg_wait_time_secs").ok();

            stats.push(PriorityQueueStats {
                priority,
                pending_count,
                processing_count,
                completed_count,
                failed_count,
                avg_wait_time_secs: avg_wait_time
                    .map(|d| d.to_string().parse().unwrap_or(0.0))
                    .unwrap_or(0.0),
            });
        }

        Ok(stats)
    }

    /// Get task execution time statistics (time from start to completion)
    ///
    /// Measures how long tasks take to execute, complementing latency statistics.
    /// Useful for identifying slow tasks and optimizing task implementation.
    ///
    /// # Returns
    ///
    /// TaskExecutionStats with min, max, avg execution time in seconds
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = MysqlBroker::new("mysql://localhost/test").await?;
    /// let stats = broker.get_task_execution_stats().await?;
    /// println!("Average execution time: {:.2}s", stats.avg_execution_secs);
    /// println!("P95 execution time: {:.2}s", stats.p95_execution_secs);
    /// # Ok(())
    /// # }
    /// ```
    #[allow(dead_code)]
    pub async fn get_task_execution_stats(&self) -> Result<TaskExecutionStats> {
        let row = sqlx::query(
            "SELECT
                COUNT(*) as task_count,
                MIN(TIMESTAMPDIFF(SECOND, started_at, completed_at)) as min_execution,
                MAX(TIMESTAMPDIFF(SECOND, started_at, completed_at)) as max_execution,
                AVG(TIMESTAMPDIFF(SECOND, started_at, completed_at)) as avg_execution,
                STDDEV(TIMESTAMPDIFF(SECOND, started_at, completed_at)) as stddev_execution
             FROM celers_tasks
             WHERE state = 'completed'
               AND started_at IS NOT NULL
               AND completed_at IS NOT NULL",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get task execution stats: {}", e)))?;

        let task_count: i64 = row.try_get("task_count").unwrap_or(0);
        let min_execution: Option<i64> = row.try_get("min_execution").ok();
        let max_execution: Option<i64> = row.try_get("max_execution").ok();
        let avg_execution: Option<rust_decimal::Decimal> = row.try_get("avg_execution").ok();
        let stddev_execution: Option<rust_decimal::Decimal> = row.try_get("stddev_execution").ok();

        // Calculate approximate P95 using ORDER BY LIMIT approach
        let p95_row = sqlx::query(
            "SELECT TIMESTAMPDIFF(SECOND, started_at, completed_at) as execution_time
             FROM celers_tasks
             WHERE state = 'completed'
               AND started_at IS NOT NULL
               AND completed_at IS NOT NULL
             ORDER BY execution_time DESC
             LIMIT 1 OFFSET ?",
        )
        .bind((task_count as f64 * 0.05).ceil() as i64)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to calculate P95: {}", e)))?;

        let p95_execution = p95_row
            .and_then(|r| r.try_get::<i64, _>("execution_time").ok())
            .unwrap_or(0);

        Ok(TaskExecutionStats {
            task_count,
            min_execution_secs: min_execution.unwrap_or(0) as f64,
            max_execution_secs: max_execution.unwrap_or(0) as f64,
            avg_execution_secs: avg_execution
                .map(|d| d.to_string().parse().unwrap_or(0.0))
                .unwrap_or(0.0),
            stddev_execution_secs: stddev_execution
                .map(|d| d.to_string().parse().unwrap_or(0.0))
                .unwrap_or(0.0),
            p95_execution_secs: p95_execution as f64,
        })
    }

    /// Monitor queue saturation and capacity utilization
    ///
    /// Detects when queue is approaching capacity limits based on pending tasks,
    /// processing rate, and historical patterns. Essential for auto-scaling decisions.
    ///
    /// # Arguments
    ///
    /// * `capacity_threshold` - Maximum recommended pending tasks (e.g., 10000)
    ///
    /// # Returns
    ///
    /// QueueSaturation with utilization percentage and status
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = MysqlBroker::new("mysql://localhost/test").await?;
    /// let saturation = broker.get_queue_saturation(10000).await?;
    /// if saturation.is_saturated {
    ///     println!("WARNING: Queue is {}% saturated!", saturation.utilization_percent);
    ///     println!("Consider scaling up workers");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[allow(dead_code)]
    pub async fn get_queue_saturation(&self, capacity_threshold: i64) -> Result<QueueSaturation> {
        if capacity_threshold <= 0 {
            return Err(CelersError::Other(
                "Capacity threshold must be positive".to_string(),
            ));
        }

        let row = sqlx::query(
            "SELECT
                SUM(CASE WHEN state = 'pending' THEN 1 ELSE 0 END) as pending_count,
                SUM(CASE WHEN state = 'processing' THEN 1 ELSE 0 END) as processing_count,
                COUNT(*) as total_tasks
             FROM celers_tasks",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get queue saturation: {}", e)))?;

        let pending_count: i64 = row.try_get("pending_count").unwrap_or(0);
        let processing_count: i64 = row.try_get("processing_count").unwrap_or(0);
        let total_tasks: i64 = row.try_get("total_tasks").unwrap_or(0);

        let utilization_percent =
            (pending_count as f64 / capacity_threshold as f64 * 100.0).min(100.0);
        let is_saturated = pending_count >= (capacity_threshold as f64 * 0.8) as i64; // 80% threshold
        let is_critical = pending_count >= (capacity_threshold as f64 * 0.95) as i64; // 95% threshold

        let status = if is_critical {
            "critical".to_string()
        } else if is_saturated {
            "warning".to_string()
        } else {
            "healthy".to_string()
        };

        Ok(QueueSaturation {
            pending_count,
            processing_count,
            total_tasks,
            capacity_threshold,
            utilization_percent,
            is_saturated,
            is_critical,
            status,
        })
    }

    /// Get task latency percentiles (P50, P95, P99) for SLA monitoring
    ///
    /// Provides detailed percentile analysis of queue latency, essential for
    /// tracking SLA compliance and identifying performance degradation.
    ///
    /// # Returns
    ///
    /// TaskLatencyPercentiles with P50, P95, P99 values in seconds
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = MysqlBroker::new("mysql://localhost/test").await?;
    /// let percentiles = broker.get_task_latency_percentiles().await?;
    /// println!("P50 latency: {:.2}s", percentiles.p50_latency_secs);
    /// println!("P95 latency: {:.2}s", percentiles.p95_latency_secs);
    /// println!("P99 latency: {:.2}s", percentiles.p99_latency_secs);
    /// # Ok(())
    /// # }
    /// ```
    #[allow(dead_code)]
    pub async fn get_task_latency_percentiles(&self) -> Result<TaskLatencyPercentiles> {
        // Get total count
        let count_row = sqlx::query(
            "SELECT COUNT(*) as task_count
             FROM celers_tasks
             WHERE state IN ('processing', 'completed')
               AND started_at IS NOT NULL",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to count tasks: {}", e)))?;

        let task_count: i64 = count_row.try_get("task_count").unwrap_or(0);

        if task_count == 0 {
            return Ok(TaskLatencyPercentiles {
                task_count: 0,
                p50_latency_secs: 0.0,
                p95_latency_secs: 0.0,
                p99_latency_secs: 0.0,
            });
        }

        // Calculate P50 (median)
        let p50_offset = (task_count as f64 * 0.5) as i64;
        let p50_row = sqlx::query(
            "SELECT TIMESTAMPDIFF(SECOND, created_at, started_at) as latency
             FROM celers_tasks
             WHERE state IN ('processing', 'completed')
               AND started_at IS NOT NULL
             ORDER BY latency
             LIMIT 1 OFFSET ?",
        )
        .bind(p50_offset)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to calculate P50: {}", e)))?;

        let p50_latency = p50_row
            .and_then(|r| r.try_get::<i64, _>("latency").ok())
            .unwrap_or(0);

        // Calculate P95
        let p95_offset = (task_count as f64 * 0.95) as i64;
        let p95_row = sqlx::query(
            "SELECT TIMESTAMPDIFF(SECOND, created_at, started_at) as latency
             FROM celers_tasks
             WHERE state IN ('processing', 'completed')
               AND started_at IS NOT NULL
             ORDER BY latency
             LIMIT 1 OFFSET ?",
        )
        .bind(p95_offset)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to calculate P95: {}", e)))?;

        let p95_latency = p95_row
            .and_then(|r| r.try_get::<i64, _>("latency").ok())
            .unwrap_or(0);

        // Calculate P99
        let p99_offset = (task_count as f64 * 0.99) as i64;
        let p99_row = sqlx::query(
            "SELECT TIMESTAMPDIFF(SECOND, created_at, started_at) as latency
             FROM celers_tasks
             WHERE state IN ('processing', 'completed')
               AND started_at IS NOT NULL
             ORDER BY latency
             LIMIT 1 OFFSET ?",
        )
        .bind(p99_offset)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to calculate P99: {}", e)))?;

        let p99_latency = p99_row
            .and_then(|r| r.try_get::<i64, _>("latency").ok())
            .unwrap_or(0);

        Ok(TaskLatencyPercentiles {
            task_count,
            p50_latency_secs: p50_latency as f64,
            p95_latency_secs: p95_latency as f64,
            p99_latency_secs: p99_latency as f64,
        })
    }

    /// Track task state transitions for debugging and analysis
    ///
    /// Records when tasks move between states, useful for debugging stuck tasks,
    /// analyzing processing patterns, and detecting anomalies.
    ///
    /// # Arguments
    ///
    /// * `task_id` - Task to get transition history for
    ///
    /// # Returns
    ///
    /// Vector of TaskStateTransition records
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # use uuid::Uuid;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = MysqlBroker::new("mysql://localhost/test").await?;
    /// # let task_id = Uuid::new_v4();
    /// let transitions = broker.get_task_state_transitions(&task_id).await?;
    /// for transition in transitions {
    ///     println!("{:?} -> {:?} at {}",
    ///         transition.from_state, transition.to_state, transition.transitioned_at);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[allow(dead_code)]
    pub async fn get_task_state_transitions(
        &self,
        task_id: &TaskId,
    ) -> Result<Vec<TaskStateTransition>> {
        // This requires the task_history table to track transitions
        // We'll infer transitions from timestamp fields
        let row = sqlx::query(
            "SELECT state, created_at, started_at, completed_at
             FROM celers_tasks
             WHERE id = ?",
        )
        .bind(task_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get task state transitions: {}", e)))?;

        let Some(row) = row else {
            return Ok(vec![]);
        };

        let current_state: String = row.try_get("state").unwrap_or_default();
        let created_at: chrono::DateTime<Utc> =
            row.try_get("created_at").unwrap_or_else(|_| Utc::now());
        let started_at: Option<chrono::DateTime<Utc>> = row.try_get("started_at").ok();
        let completed_at: Option<chrono::DateTime<Utc>> = row.try_get("completed_at").ok();

        let mut transitions = Vec::new();

        // Infer transitions based on timestamps
        transitions.push(TaskStateTransition {
            task_id: *task_id,
            from_state: None,
            to_state: "pending".to_string(),
            transitioned_at: created_at,
        });

        if let Some(started) = started_at {
            transitions.push(TaskStateTransition {
                task_id: *task_id,
                from_state: Some("pending".to_string()),
                to_state: "processing".to_string(),
                transitioned_at: started,
            });
        }

        if let Some(completed) = completed_at {
            transitions.push(TaskStateTransition {
                task_id: *task_id,
                from_state: Some("processing".to_string()),
                to_state: current_state,
                transitioned_at: completed,
            });
        }

        Ok(transitions)
    }
}
