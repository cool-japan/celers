//! Query optimization and performance analysis methods

use celers_core::{CelersError, Result};
use chrono::{DateTime, Utc};

use crate::row_ext::RowExt;
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
        // The queue label is a bound predicate on the real `queue_name`
        // column, not a table name: the statement below mirrors the shape of
        // the claim sub-select in `sql::claim_one_sql()` so the plan it
        // reports is the plan the hot path actually gets.
        let explain_query = r#"
            EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
            SELECT id, task_name, payload, retry_count, max_retries, created_at
            FROM celers_tasks
            WHERE queue_name = $1
              AND state = 'pending' AND scheduled_at <= NOW()
            ORDER BY priority DESC, created_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
            "#;

        let rows = self
            .conn
            .query(explain_query, &[&self.queue_name])
            .await
            .map_err(|e| CelersError::Other(format!("Failed to explain query: {}", e)))?;

        let mut lines = Vec::with_capacity(rows.len());
        for row in &rows {
            let line: String = row
                .col_idx(0)
                .map_err(|e| CelersError::Other(format!("Failed to read explain line: {}", e)))?;
            lines.push(line);
        }

        Ok(lines.join("\n"))
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
        // `pg_stat_user_tables` is per-TABLE, and the broker's tasks all live
        // in `celers_tasks` regardless of their logical queue label — the
        // previous `relname = '<queue_name>'` filter matched nothing.
        let query = r#"
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
            WHERE relname = 'celers_tasks'
            "#;

        let rows = self
            .conn
            .query(query, &[])
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get query stats: {}", e)))?;
        let row = rows.into_iter().next().ok_or_else(|| {
            CelersError::Other("Failed to get query stats: no rows returned".to_string())
        })?;

        let idx_scan: Option<i64> = row
            .col("idx_scan")
            .map_err(|e| CelersError::Other(format!("Failed to read idx_scan: {}", e)))?;
        let idx_tup_fetch: Option<i64> = row
            .col("idx_tup_fetch")
            .map_err(|e| CelersError::Other(format!("Failed to read idx_tup_fetch: {}", e)))?;
        let last_vacuum: Option<DateTime<Utc>> = row
            .col("last_vacuum")
            .map_err(|e| CelersError::Other(format!("Failed to read last_vacuum: {}", e)))?;
        let last_autovacuum: Option<DateTime<Utc>> = row
            .col("last_autovacuum")
            .map_err(|e| CelersError::Other(format!("Failed to read last_autovacuum: {}", e)))?;
        let last_analyze: Option<DateTime<Utc>> = row
            .col("last_analyze")
            .map_err(|e| CelersError::Other(format!("Failed to read last_analyze: {}", e)))?;
        let last_autoanalyze: Option<DateTime<Utc>> = row
            .col("last_autoanalyze")
            .map_err(|e| CelersError::Other(format!("Failed to read last_autoanalyze: {}", e)))?;

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
            row.col::<String>("schemaname")
                .map_err(|e| CelersError::Other(format!("Failed to read schemaname: {}", e)))?,
            row.col::<String>("relname")
                .map_err(|e| CelersError::Other(format!("Failed to read relname: {}", e)))?,
            row.col::<i64>("seq_scan")
                .map_err(|e| CelersError::Other(format!("Failed to read seq_scan: {}", e)))?,
            row.col::<i64>("seq_tup_read")
                .map_err(|e| CelersError::Other(format!("Failed to read seq_tup_read: {}", e)))?,
            idx_scan.unwrap_or(0),
            idx_tup_fetch.unwrap_or(0),
            row.col::<i64>("n_tup_ins")
                .map_err(|e| CelersError::Other(format!("Failed to read n_tup_ins: {}", e)))?,
            row.col::<i64>("n_tup_upd")
                .map_err(|e| CelersError::Other(format!("Failed to read n_tup_upd: {}", e)))?,
            row.col::<i64>("n_tup_del")
                .map_err(|e| CelersError::Other(format!("Failed to read n_tup_del: {}", e)))?,
            row.col::<i64>("n_live_tup")
                .map_err(|e| CelersError::Other(format!("Failed to read n_live_tup: {}", e)))?,
            row.col::<i64>("n_dead_tup")
                .map_err(|e| CelersError::Other(format!("Failed to read n_dead_tup: {}", e)))?,
            last_vacuum,
            last_autovacuum,
            last_analyze,
            last_autoanalyze
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
            // Postgres `SET` statements do not accept `$n` bind parameters
            // on the right-hand side, so `max_parallel_workers` (a typed
            // `i32`, not attacker-controlled text) must still be
            // interpolated as literal SQL text here — same as the
            // pre-migration `sqlx::AssertSqlSafe` version, minus the
            // now-dropped wrapper.
            let set_workers_query = format!(
                "SET max_parallel_workers_per_gather = {}",
                max_parallel_workers
            );
            self.conn
                .execute(&set_workers_query, &[])
                .await
                .map_err(|e| CelersError::Other(format!("Failed to set query hints: {}", e)))?;

            self.conn
                .execute("SET parallel_setup_cost = 100", &[])
                .await
                .map_err(|e| CelersError::Other(format!("Failed to set query hints: {}", e)))?;

            self.conn
                .execute("SET parallel_tuple_cost = 0.01", &[])
                .await
                .map_err(|e| CelersError::Other(format!("Failed to set query hints: {}", e)))?;
        } else {
            self.conn
                .execute("SET max_parallel_workers_per_gather = 0", &[])
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
