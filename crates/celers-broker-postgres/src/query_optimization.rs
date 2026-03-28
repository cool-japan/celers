//! Query optimization and performance analysis methods

use celers_core::{CelersError, Result};
use chrono::{DateTime, Utc};
use sqlx::Row;

use crate::PostgresBroker;

// Query Optimization Methods
impl PostgresBroker {
    /// Analyze query performance for the dequeue operation
    ///
    /// Returns EXPLAIN ANALYZE output for the main dequeue query.
    /// Useful for understanding query performance and identifying bottlenecks.
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let explain_output = broker.explain_dequeue_query().await?;
    /// println!("Query plan:\n{}", explain_output);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn explain_dequeue_query(&self) -> Result<String> {
        let explain_query = format!(
            r#"
            EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
            SELECT id, task_name, payload, retry_count, max_retries, created_at
            FROM {}
            WHERE state = 'pending' AND scheduled_at <= NOW()
            ORDER BY priority DESC, created_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            "#,
            self.queue_name
        );

        let rows = sqlx::query_scalar::<_, String>(&explain_query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to explain query: {}", e)))?;

        Ok(rows.join("\n"))
    }

    /// Get query statistics for tasks table
    ///
    /// Returns statistics about table scans, index usage, etc.
    /// Useful for monitoring query performance over time.
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let stats = broker.get_query_stats().await?;
    /// println!("Query statistics:\n{}", stats);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_query_stats(&self) -> Result<String> {
        let query = format!(
            r#"
            SELECT
                schemaname,
                relname,
                seq_scan,
                seq_tup_read,
                idx_scan,
                idx_tup_fetch,
                n_tup_ins,
                n_tup_upd,
                n_tup_del,
                n_live_tup,
                n_dead_tup,
                last_vacuum,
                last_autovacuum,
                last_analyze,
                last_autoanalyze
            FROM pg_stat_user_tables
            WHERE relname = '{}'
            "#,
            self.queue_name.trim_start_matches("public.")
        );

        let row = sqlx::query(&query)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get query stats: {}", e)))?;

        let stats = format!(
            "Table: {}.{}\n\
             Sequential Scans: {}\n\
             Sequential Tuples Read: {}\n\
             Index Scans: {}\n\
             Index Tuples Fetched: {}\n\
             Tuples Inserted: {}\n\
             Tuples Updated: {}\n\
             Tuples Deleted: {}\n\
             Live Tuples: {}\n\
             Dead Tuples: {}\n\
             Last Vacuum: {:?}\n\
             Last Autovacuum: {:?}\n\
             Last Analyze: {:?}\n\
             Last Autoanalyze: {:?}",
            row.get::<String, _>("schemaname"),
            row.get::<String, _>("relname"),
            row.get::<i64, _>("seq_scan"),
            row.get::<i64, _>("seq_tup_read"),
            row.get::<Option<i64>, _>("idx_scan").unwrap_or(0),
            row.get::<Option<i64>, _>("idx_tup_fetch").unwrap_or(0),
            row.get::<i64, _>("n_tup_ins"),
            row.get::<i64, _>("n_tup_upd"),
            row.get::<i64, _>("n_tup_del"),
            row.get::<i64, _>("n_live_tup"),
            row.get::<i64, _>("n_dead_tup"),
            row.get::<Option<DateTime<Utc>>, _>("last_vacuum"),
            row.get::<Option<DateTime<Utc>>, _>("last_autovacuum"),
            row.get::<Option<DateTime<Utc>>, _>("last_analyze"),
            row.get::<Option<DateTime<Utc>>, _>("last_autoanalyze")
        );

        Ok(stats)
    }

    /// Set query optimization hints for PostgreSQL
    ///
    /// Configures session-level query optimization settings.
    /// These settings only affect the current connection.
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// // Enable parallel query execution
    /// broker.set_query_hints(true, 4).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn set_query_hints(
        &self,
        enable_parallel: bool,
        max_parallel_workers: i32,
    ) -> Result<()> {
        if enable_parallel {
            sqlx::query(&format!(
                "SET max_parallel_workers_per_gather = {}",
                max_parallel_workers
            ))
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to set query hints: {}", e)))?;

            sqlx::query("SET parallel_setup_cost = 100")
                .execute(&self.pool)
                .await
                .map_err(|e| CelersError::Other(format!("Failed to set query hints: {}", e)))?;

            sqlx::query("SET parallel_tuple_cost = 0.01")
                .execute(&self.pool)
                .await
                .map_err(|e| CelersError::Other(format!("Failed to set query hints: {}", e)))?;
        } else {
            sqlx::query("SET max_parallel_workers_per_gather = 0")
                .execute(&self.pool)
                .await
                .map_err(|e| CelersError::Other(format!("Failed to set query hints: {}", e)))?;
        }

        Ok(())
    }

    /// Get connection pool configuration recommendations
    ///
    /// Analyzes current workload and returns recommended pool settings.
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let recommendations = broker.get_pool_recommendations().await?;
    /// println!("{}", recommendations);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_pool_recommendations(&self) -> Result<String> {
        let metrics = self.get_pool_metrics();
        let stats = self.get_statistics().await?;

        let utilization = if metrics.max_size > 0 {
            (metrics.in_use as f64 / metrics.max_size as f64) * 100.0
        } else {
            0.0
        };

        let recommendations = format!(
            "Connection Pool Recommendations:\n\
             \n\
             Current Configuration:\n\
             - Max Size: {}\n\
             - Current Size: {}\n\
             - In Use: {} ({}% utilization)\n\
             - Idle: {}\n\
             \n\
             Workload Analysis:\n\
             - Pending Tasks: {}\n\
             - Processing Tasks: {}\n\
             - Total Tasks: {}\n\
             \n\
             Recommendations:\n\
             {}",
            metrics.max_size,
            metrics.size,
            metrics.in_use,
            utilization as i32,
            metrics.idle,
            stats.pending,
            stats.processing,
            stats.total,
            if utilization > 80.0 {
                "⚠️  High pool utilization! Consider increasing max_connections.\n\
                 - Recommended: Increase pool size to handle peak load\n\
                 - Current bottleneck: Connection pool exhaustion"
            } else if utilization < 20.0 && metrics.max_size > 10 {
                "✓ Pool is underutilized. You may reduce max_connections to save resources.\n\
                 - Recommended: Reduce pool size to 50-60% of current\n\
                 - Benefit: Lower memory usage and connection overhead"
            } else {
                "✓ Pool utilization is optimal (20-80% range).\n\
                 - Current configuration is well-tuned for your workload"
            }
        );

        Ok(recommendations)
    }
}
