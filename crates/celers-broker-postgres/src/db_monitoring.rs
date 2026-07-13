//! Database monitoring operations (table sizes, index usage, etc.)

use celers_core::{CelersError, Result};
use oxisql_core::Connection;
use std::sync::Arc;
use std::time::Duration;

use crate::row_ext::RowExt;
use crate::types::{DbTaskState, IndexUsageInfo, TableSizeInfo};
use crate::PostgresBroker;

/// Map an `IndexUsageInfo`-shaped row. Shared by [`PostgresBroker::get_index_usage`]
/// and [`PostgresBroker::get_unused_indexes`], which select the same 7-column
/// projection with only a differing `WHERE`/`ORDER BY`.
fn row_to_index_usage_info(row: &oxisql_core::Row) -> Result<IndexUsageInfo> {
    Ok(IndexUsageInfo {
        index_name: row
            .col("index_name")
            .map_err(|e| CelersError::Other(format!("Failed to read index_name: {}", e)))?,
        table_name: row
            .col("table_name")
            .map_err(|e| CelersError::Other(format!("Failed to read table_name: {}", e)))?,
        index_scans: row
            .col("index_scans")
            .map_err(|e| CelersError::Other(format!("Failed to read index_scans: {}", e)))?,
        tuples_read: row
            .col("tuples_read")
            .map_err(|e| CelersError::Other(format!("Failed to read tuples_read: {}", e)))?,
        tuples_fetched: row
            .col("tuples_fetched")
            .map_err(|e| CelersError::Other(format!("Failed to read tuples_fetched: {}", e)))?,
        index_size_bytes: row
            .col("index_size_bytes")
            .map_err(|e| CelersError::Other(format!("Failed to read index_size_bytes: {}", e)))?,
        index_size_pretty: row
            .col("index_size_pretty")
            .map_err(|e| CelersError::Other(format!("Failed to read index_size_pretty: {}", e)))?,
    })
}

impl PostgresBroker {
    // ========== Database Monitoring ==========

    /// Get table size information for CeleRS tables
    pub async fn get_table_sizes(&self) -> Result<Vec<TableSizeInfo>> {
        let rows = self
            .conn
            .query(
                r#"
            SELECT
                relname as table_name,
                n_live_tup as row_count,
                pg_total_relation_size(relid) as total_size_bytes,
                pg_table_size(relid) as table_size_bytes,
                pg_indexes_size(relid) as index_size_bytes,
                pg_size_pretty(pg_total_relation_size(relid)) as total_size_pretty
            FROM pg_stat_user_tables
            WHERE relname LIKE 'celers_%'
            ORDER BY pg_total_relation_size(relid) DESC
            "#,
                &[],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get table sizes: {}", e)))?;

        let mut tables = Vec::with_capacity(rows.len());
        for row in &rows {
            tables.push(TableSizeInfo {
                table_name: row
                    .col("table_name")
                    .map_err(|e| CelersError::Other(format!("Failed to read table_name: {}", e)))?,
                row_count: row
                    .col("row_count")
                    .map_err(|e| CelersError::Other(format!("Failed to read row_count: {}", e)))?,
                total_size_bytes: row.col("total_size_bytes").map_err(|e| {
                    CelersError::Other(format!("Failed to read total_size_bytes: {}", e))
                })?,
                table_size_bytes: row.col("table_size_bytes").map_err(|e| {
                    CelersError::Other(format!("Failed to read table_size_bytes: {}", e))
                })?,
                index_size_bytes: row.col("index_size_bytes").map_err(|e| {
                    CelersError::Other(format!("Failed to read index_size_bytes: {}", e))
                })?,
                total_size_pretty: row.col("total_size_pretty").map_err(|e| {
                    CelersError::Other(format!("Failed to read total_size_pretty: {}", e))
                })?,
            });
        }
        Ok(tables)
    }

    /// Get index usage statistics for CeleRS tables
    pub async fn get_index_usage(&self) -> Result<Vec<IndexUsageInfo>> {
        let rows = self
            .conn
            .query(
                r#"
            SELECT
                indexrelname as index_name,
                relname as table_name,
                idx_scan as index_scans,
                idx_tup_read as tuples_read,
                idx_tup_fetch as tuples_fetched,
                pg_relation_size(indexrelid) as index_size_bytes,
                pg_size_pretty(pg_relation_size(indexrelid)) as index_size_pretty
            FROM pg_stat_user_indexes
            WHERE relname LIKE 'celers_%'
            ORDER BY idx_scan DESC
            "#,
                &[],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get index usage: {}", e)))?;

        let mut indexes = Vec::with_capacity(rows.len());
        for row in &rows {
            indexes.push(row_to_index_usage_info(row)?);
        }
        Ok(indexes)
    }

    /// Get unused indexes (indexes that have never been scanned)
    ///
    /// This can help identify indexes that can be safely dropped.
    pub async fn get_unused_indexes(&self) -> Result<Vec<IndexUsageInfo>> {
        let rows = self
            .conn
            .query(
                r#"
            SELECT
                indexrelname as index_name,
                relname as table_name,
                idx_scan as index_scans,
                idx_tup_read as tuples_read,
                idx_tup_fetch as tuples_fetched,
                pg_relation_size(indexrelid) as index_size_bytes,
                pg_size_pretty(pg_relation_size(indexrelid)) as index_size_pretty
            FROM pg_stat_user_indexes
            WHERE relname LIKE 'celers_%'
              AND idx_scan = 0
            ORDER BY pg_relation_size(indexrelid) DESC
            "#,
                &[],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get unused indexes: {}", e)))?;

        let mut indexes = Vec::with_capacity(rows.len());
        for row in &rows {
            indexes.push(row_to_index_usage_info(row)?);
        }
        Ok(indexes)
    }

    /// Analyze tables to update statistics for query planner
    ///
    /// This should be run periodically for optimal query performance.
    pub async fn analyze_tables(&self) -> Result<()> {
        self.conn
            .execute("ANALYZE celers_tasks", &[])
            .await
            .map_err(|e| CelersError::Other(format!("Failed to analyze celers_tasks: {}", e)))?;

        self.conn
            .execute("ANALYZE celers_dead_letter_queue", &[])
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to analyze celers_dead_letter_queue: {}", e))
            })?;

        self.conn
            .execute("ANALYZE celers_task_results", &[])
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to analyze celers_task_results: {}", e))
            })?;

        tracing::info!("Analyzed all CeleRS tables");
        Ok(())
    }

    /// Manually vacuum CeleRS tables to reclaim space
    ///
    /// This is useful for high-churn queues. For most cases, PostgreSQL's
    /// autovacuum is sufficient.
    pub async fn vacuum_tables(&self) -> Result<()> {
        self.conn
            .execute("VACUUM ANALYZE celers_tasks", &[])
            .await
            .map_err(|e| CelersError::Other(format!("Failed to vacuum celers_tasks: {}", e)))?;

        self.conn
            .execute("VACUUM ANALYZE celers_dead_letter_queue", &[])
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to vacuum celers_dead_letter_queue: {}", e))
            })?;

        self.conn
            .execute("VACUUM ANALYZE celers_task_results", &[])
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to vacuum celers_task_results: {}", e))
            })?;

        tracing::info!("Vacuumed all CeleRS tables");
        Ok(())
    }

    /// Start a background maintenance task that periodically runs VACUUM and ANALYZE
    ///
    /// This spawns a tokio task that runs maintenance operations at the specified interval.
    /// Returns a handle that can be used to stop the maintenance task.
    ///
    /// # Arguments
    /// * `interval` - Duration between maintenance runs (e.g., Duration::from_secs(3600) for hourly)
    /// * `vacuum` - Whether to run VACUUM (can be expensive, consider running less frequently)
    /// * `analyze` - Whether to run ANALYZE (lightweight, recommended)
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Run maintenance every hour (ANALYZE only)
    /// let handle = broker.start_maintenance_scheduler(
    ///     Duration::from_secs(3600),
    ///     false, // Don't VACUUM (let autovacuum handle it)
    ///     true,  // Do ANALYZE
    /// );
    ///
    /// // Stop the maintenance task later
    /// handle.abort();
    /// ```
    pub fn start_maintenance_scheduler(
        self: Arc<Self>,
        interval: Duration,
        vacuum: bool,
        analyze: bool,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval_timer.tick().await;

                tracing::debug!("Running scheduled maintenance task");

                // Run maintenance operations
                if vacuum {
                    if let Err(e) = self.vacuum_tables().await {
                        tracing::error!(error = %e, "Failed to vacuum tables during maintenance");
                    }
                } else if analyze {
                    // Just run ANALYZE without VACUUM (much faster)
                    if let Err(e) = self.analyze_tables().await {
                        tracing::error!(error = %e, "Failed to analyze tables during maintenance");
                    }
                }

                // Also archive old completed tasks (older than 7 days by default)
                let archive_threshold = Duration::from_secs(7 * 24 * 3600);
                match self.archive_completed_tasks(archive_threshold).await {
                    Ok(count) => {
                        if count > 0 {
                            tracing::info!(count = count, "Archived old completed tasks");
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "Failed to archive tasks during maintenance");
                    }
                }

                // Archive old results (older than 30 days)
                let results_threshold = Duration::from_secs(30 * 24 * 3600);
                match self.archive_results(results_threshold).await {
                    Ok(count) => {
                        if count > 0 {
                            tracing::info!(count = count, "Archived old task results");
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "Failed to archive results during maintenance");
                    }
                }

                // Check for stuck tasks (processing for more than 1 hour)
                let stuck_threshold = Duration::from_secs(3600);
                match self.recover_stuck_tasks(stuck_threshold).await {
                    Ok(count) => {
                        if count > 0 {
                            tracing::warn!(count = count, "Recovered stuck processing tasks");
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "Failed to recover stuck tasks");
                    }
                }
            }
        })
    }

    /// Count tasks by state
    ///
    /// Returns the number of tasks in the specified state.
    pub async fn count_by_state(&self, state: DbTaskState) -> Result<i64> {
        let state_param = state.to_string();
        let rows = self
            .conn
            .query(
                "SELECT COUNT(*) FROM celers_tasks WHERE state = $1",
                &[&state_param],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to count tasks by state: {}", e)))?;
        let row = rows.into_iter().next().ok_or_else(|| {
            CelersError::Other("Failed to count tasks by state: no rows returned".to_string())
        })?;
        // Unaliased `COUNT(*)`: positional access per the `RowExt`
        // convention (see `row_ext.rs`).
        row.col_idx(0)
            .map_err(|e| CelersError::Other(format!("Failed to read count: {}", e)))
    }

    /// Get the number of tasks scheduled for future execution
    ///
    /// Returns the count of tasks that are pending but scheduled for a future time.
    pub async fn count_scheduled(&self) -> Result<i64> {
        let rows = self
            .conn
            .query(
                "SELECT COUNT(*) FROM celers_tasks WHERE state = 'pending' AND scheduled_at > NOW()",
                &[],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to count scheduled tasks: {}", e)))?;
        let row = rows.into_iter().next().ok_or_else(|| {
            CelersError::Other("Failed to count scheduled tasks: no rows returned".to_string())
        })?;
        row.col_idx(0)
            .map_err(|e| CelersError::Other(format!("Failed to read count: {}", e)))
    }

    /// Cancel all pending tasks
    ///
    /// Returns the number of tasks cancelled.
    pub async fn cancel_all_pending(&self) -> Result<u64> {
        let affected = self
            .conn
            .execute(
                r#"
            UPDATE celers_tasks
            SET state = 'cancelled',
                completed_at = NOW()
            WHERE state = 'pending'
            "#,
                &[],
            )
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to cancel all pending tasks: {}", e))
            })?;

        tracing::warn!(count = affected, "Cancelled all pending tasks");
        Ok(affected)
    }

    /// Test database connectivity
    ///
    /// Performs a simple query to verify the connection is working.
    /// Returns true if the connection is healthy.
    pub async fn test_connection(&self) -> Result<bool> {
        let rows = self
            .conn
            .query("SELECT 1", &[])
            .await
            .map_err(|e| CelersError::Other(format!("Connection test failed: {}", e)))?;
        let row = rows.into_iter().next().ok_or_else(|| {
            CelersError::Other("Connection test failed: no rows returned".to_string())
        })?;
        let result: i32 = row
            .col_idx(0)
            .map_err(|e| CelersError::Other(format!("Connection test failed: {}", e)))?;

        Ok(result == 1)
    }

    /// Get the age (in seconds) of the oldest pending task
    ///
    /// Returns None if there are no pending tasks.
    pub async fn oldest_pending_age_secs(&self) -> Result<Option<i64>> {
        let rows = self
            .conn
            .query(
                r#"
            SELECT EXTRACT(EPOCH FROM (NOW() - created_at))::BIGINT
            FROM celers_tasks
            WHERE state = 'pending'
            ORDER BY created_at ASC
            LIMIT 1
            "#,
                &[],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get oldest pending age: {}", e)))?;

        match rows.into_iter().next() {
            Some(row) => row.col_idx(0).map_err(|e| {
                CelersError::Other(format!("Failed to read oldest pending age: {}", e))
            }),
            None => Ok(None),
        }
    }

    /// Get the age (in seconds) of the oldest processing task
    ///
    /// This is useful for detecting stuck tasks. Returns None if there are no processing tasks.
    pub async fn oldest_processing_age_secs(&self) -> Result<Option<i64>> {
        let rows = self
            .conn
            .query(
                r#"
            SELECT EXTRACT(EPOCH FROM (NOW() - started_at))::BIGINT
            FROM celers_tasks
            WHERE state = 'processing' AND started_at IS NOT NULL
            ORDER BY started_at ASC
            LIMIT 1
            "#,
                &[],
            )
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to get oldest processing age: {}", e))
            })?;

        match rows.into_iter().next() {
            Some(row) => row.col_idx(0).map_err(|e| {
                CelersError::Other(format!("Failed to read oldest processing age: {}", e))
            }),
            None => Ok(None),
        }
    }

    /// Get average task processing time for completed tasks (in milliseconds)
    ///
    /// Calculates the average time between started_at and completed_at for recently completed tasks.
    /// Uses the last 1000 completed tasks by default.
    pub async fn avg_processing_time_ms(&self) -> Result<Option<f64>> {
        let rows = self
            .conn
            .query(
                r#"
            SELECT AVG(EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000)
            FROM (
                SELECT started_at, completed_at
                FROM celers_tasks
                WHERE state = 'completed'
                  AND started_at IS NOT NULL
                  AND completed_at IS NOT NULL
                ORDER BY completed_at DESC
                LIMIT 1000
            ) recent_tasks
            "#,
                &[],
            )
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to calculate avg processing time: {}", e))
            })?;

        match rows.into_iter().next() {
            Some(row) => row.col_idx(0).map_err(|e| {
                CelersError::Other(format!("Failed to read avg processing time: {}", e))
            }),
            None => Ok(None),
        }
    }

    /// Get the retry rate (percentage of tasks that have been retried at least once)
    ///
    /// Returns a value between 0.0 and 100.0.
    pub async fn retry_rate(&self) -> Result<f64> {
        let rows = self
            .conn
            .query(
                r#"
            SELECT
                CASE WHEN COUNT(*) > 0
                THEN (COUNT(*) FILTER (WHERE retry_count > 0)::FLOAT / COUNT(*)::FLOAT * 100.0)
                ELSE 0.0
                END
            FROM celers_tasks
            WHERE state IN ('completed', 'failed', 'processing')
            "#,
                &[],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to calculate retry rate: {}", e)))?;
        let row = rows.into_iter().next().ok_or_else(|| {
            CelersError::Other("Failed to calculate retry rate: no rows returned".to_string())
        })?;
        let rate: Option<f64> = row
            .col_idx(0)
            .map_err(|e| CelersError::Other(format!("Failed to read retry rate: {}", e)))?;

        Ok(rate.unwrap_or(0.0))
    }

    /// Get success rate (percentage of completed vs failed tasks)
    ///
    /// Returns a value between 0.0 and 100.0.
    pub async fn success_rate(&self) -> Result<f64> {
        let rows = self
            .conn
            .query(
                r#"
            SELECT
                CASE WHEN COUNT(*) > 0
                THEN (COUNT(*) FILTER (WHERE state = 'completed')::FLOAT / COUNT(*)::FLOAT * 100.0)
                ELSE 0.0
                END
            FROM celers_tasks
            WHERE state IN ('completed', 'failed')
            "#,
                &[],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to calculate success rate: {}", e)))?;
        let row = rows.into_iter().next().ok_or_else(|| {
            CelersError::Other("Failed to calculate success rate: no rows returned".to_string())
        })?;
        let rate: Option<f64> = row
            .col_idx(0)
            .map_err(|e| CelersError::Other(format!("Failed to read success rate: {}", e)))?;

        Ok(rate.unwrap_or(0.0))
    }
}
