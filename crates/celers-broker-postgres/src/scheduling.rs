//! Periodic task scheduling and connection pool health methods

use celers_core::{CelersError, Result};
use chrono::Utc;
use uuid::Uuid;

use crate::row_ext::{json_from_row, json_param, RowExt};
use crate::types::{
    ConnectionPoolHealth, DbTaskState, PeriodicTaskSchedule, QueueSnapshot, TaskRetentionPolicy,
};
use crate::PostgresBroker;

/// Validate that `s` is safe to interpolate into SQL as a bare identifier
/// (e.g. a table name), since an identifier cannot be bound as a parameter.
/// Only ASCII letters, digits, and underscores are permitted, and the first
/// character must not be a digit — i.e. `^[A-Za-z_][A-Za-z0-9_]*$`.
///
/// Retained (with its tests) as the guard any future identifier splice must
/// pass, but currently unused: no statement in this crate interpolates an
/// identifier any more. The queue label — the one value that used to be
/// spliced as a table name — is now a bound predicate against the real
/// `queue_name` column, and is separately validated at construction by
/// [`crate::sql::validate_queue_name`], whose character class also admits
/// `-` (legal in a queue label, illegal in a bare SQL identifier).
#[allow(dead_code)]
pub(crate) fn validate_sql_identifier(s: &str) -> Result<()> {
    let starts_ok = matches!(s.chars().next(), Some(c) if c.is_ascii_alphabetic() || c == '_');
    let rest_ok = s
        .chars()
        .skip(1)
        .all(|c| c.is_ascii_alphanumeric() || c == '_');
    if starts_ok && rest_ok {
        Ok(())
    } else {
        Err(CelersError::Other(format!(
            "Invalid SQL identifier {:?}: must match ^[A-Za-z_][A-Za-z0-9_]*$",
            s
        )))
    }
}

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

        // Schedules live in their own table (`celers_periodic_schedules`,
        // migration 007), not as rows in the dispatch table. Smuggling them
        // into `celers_tasks` as `state = 'pending'` rows meant a worker
        // would claim the schedule as if it were a task, fail to resolve a
        // task named `__periodic_schedule_<x>`, flip its state, and the
        // schedule would silently vanish from `list_periodic_schedules`.
        let payload_param = json_param(&payload);
        let created_at_param = now.to_rfc3339();
        self.conn
            .execute(
                "INSERT INTO celers_periodic_schedules \
                 (schedule_id, queue_name, task_name, cron_expression, payload, priority, \
                  enabled, created_at) \
                 VALUES ($1, $2, $3, $4, $5::text::jsonb, $6, TRUE, $7::text::timestamptz)",
                &[
                    &schedule_id,
                    &self.queue_name,
                    &task_name,
                    &cron_expression,
                    &payload_param,
                    &priority,
                    &created_at_param,
                ],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to schedule periodic task: {}", e)))?;

        tracing::info!(
            schedule_id = %schedule_id,
            queue = %self.queue_name,
            task_name = %task_name,
            cron = %cron_expression,
            "Stored periodic task schedule"
        );

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
        let rows = self
            .conn
            .query(
                "SELECT schedule_id, task_name, cron_expression, payload::text AS payload, \
                        priority, enabled, last_run, next_run, created_at \
                   FROM celers_periodic_schedules \
                  WHERE queue_name = $1 \
                  ORDER BY created_at ASC",
                &[&self.queue_name],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to list periodic schedules: {}", e)))?;

        let mut schedules = Vec::with_capacity(rows.len());
        for row in &rows {
            schedules.push(PeriodicTaskSchedule {
                schedule_id: row.col("schedule_id").map_err(|e| {
                    CelersError::Other(format!("Failed to read schedule_id: {}", e))
                })?,
                task_name: row
                    .col("task_name")
                    .map_err(|e| CelersError::Other(format!("Failed to read task_name: {}", e)))?,
                cron_expression: row.col("cron_expression").map_err(|e| {
                    CelersError::Other(format!("Failed to read cron_expression: {}", e))
                })?,
                payload: json_from_row(row, "payload")
                    .map_err(|e| CelersError::Other(format!("Failed to read payload: {}", e)))?,
                priority: row
                    .col("priority")
                    .map_err(|e| CelersError::Other(format!("Failed to read priority: {}", e)))?,
                enabled: row
                    .col("enabled")
                    .map_err(|e| CelersError::Other(format!("Failed to read enabled: {}", e)))?,
                last_run: row
                    .col("last_run")
                    .map_err(|e| CelersError::Other(format!("Failed to read last_run: {}", e)))?,
                next_run: row
                    .col("next_run")
                    .map_err(|e| CelersError::Other(format!("Failed to read next_run: {}", e)))?,
                created_at: row
                    .col("created_at")
                    .map_err(|e| CelersError::Other(format!("Failed to read created_at: {}", e)))?,
            });
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
        let affected = self
            .conn
            .execute(
                "DELETE FROM celers_periodic_schedules \
                  WHERE schedule_id = $1 AND queue_name = $2",
                &[&schedule_id, &self.queue_name],
            )
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to cancel periodic schedule: {}", e))
            })?;

        Ok(affected > 0)
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

        // Count this queue's tasks. (The queue label is a bound predicate on
        // the real `queue_name` column — it was previously spliced into the
        // FROM clause as if it named a table.)
        let count_query = "SELECT COUNT(*) FROM celers_tasks WHERE queue_name = $1";
        let count_rows = self
            .conn
            .query(count_query, &[&self.queue_name])
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to count tasks for snapshot: {}", e))
            })?;
        let count_row = count_rows.into_iter().next().ok_or_else(|| {
            CelersError::Other("Failed to count tasks for snapshot: no rows returned".to_string())
        })?;
        let task_count: i64 = count_row
            .col_idx(0)
            .map_err(|e| CelersError::Other(format!("Failed to read task count: {}", e)))?;

        // Estimate size. The relation name is a fixed literal now: the old
        // `pg_total_relation_size('{queue_name}')` form spliced a
        // caller-supplied label *inside a quoted string literal*, so a single
        // apostrophe in the label would break out of the literal and append
        // arbitrary SQL.
        let size_query = "SELECT pg_total_relation_size('celers_tasks')";
        let size_rows = self
            .conn
            .query(size_query, &[])
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get table size: {}", e)))?;
        let size_row = size_rows.into_iter().next().ok_or_else(|| {
            CelersError::Other("Failed to get table size: no rows returned".to_string())
        })?;
        let total_size_bytes: Option<i64> = size_row
            .col_idx(0)
            .map_err(|e| CelersError::Other(format!("Failed to read table size: {}", e)))?;

        let snapshot = QueueSnapshot {
            snapshot_id: snapshot_id.clone(),
            queue_name: self.queue_name.clone(),
            created_at: now,
            task_count,
            total_size_bytes: total_size_bytes.unwrap_or(0),
            includes_results: include_results,
        };

        // Persist snapshot metadata. `created_at` is a `DateTime<Utc>`
        // parameter -> `.to_rfc3339()` bound through a
        // `$3::text::timestamptz` cast, per `row_ext.rs`'s convention.
        let created_at_param = snapshot.created_at.to_rfc3339();
        self.conn
            .execute(
                "INSERT INTO celers_queue_snapshots \
             (snapshot_id, queue_name, created_at, task_count, total_size_bytes, includes_results) \
             VALUES ($1, $2, $3::text::timestamptz, $4, $5, $6)",
                &[
                    &snapshot.snapshot_id,
                    &snapshot.queue_name,
                    &created_at_param,
                    &snapshot.task_count,
                    &snapshot.total_size_bytes,
                    &snapshot.includes_results,
                ],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to store snapshot metadata: {}", e)))?;

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
        let rows = self
            .conn
            .query(
                "SELECT snapshot_id, queue_name, created_at, task_count, total_size_bytes, \
             includes_results \
             FROM celers_queue_snapshots \
             WHERE queue_name = $1 \
             ORDER BY created_at DESC",
                &[&self.queue_name],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to list snapshots: {}", e)))?;

        let mut snapshots = Vec::with_capacity(rows.len());
        for row in &rows {
            snapshots.push(QueueSnapshot {
                snapshot_id: row.col("snapshot_id").map_err(|e| {
                    CelersError::Other(format!("Failed to read snapshot_id: {}", e))
                })?,
                queue_name: row
                    .col("queue_name")
                    .map_err(|e| CelersError::Other(format!("Failed to read queue_name: {}", e)))?,
                created_at: row
                    .col("created_at")
                    .map_err(|e| CelersError::Other(format!("Failed to read created_at: {}", e)))?,
                task_count: row
                    .col("task_count")
                    .map_err(|e| CelersError::Other(format!("Failed to read task_count: {}", e)))?,
                total_size_bytes: row.col("total_size_bytes").map_err(|e| {
                    CelersError::Other(format!("Failed to read total_size_bytes: {}", e))
                })?,
                includes_results: row.col("includes_results").map_err(|e| {
                    CelersError::Other(format!("Failed to read includes_results: {}", e))
                })?,
            });
        }
        Ok(snapshots)
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
    ///
    /// # Security
    ///
    /// `where_clause` is interpolated directly into the generated SQL as a
    /// raw predicate fragment — it cannot be a bind parameter because it is
    /// a whole piece of SQL syntax, not a single value. Callers MUST pass
    /// only trusted, non-attacker-derived strings (e.g. a hardcoded literal,
    /// or a value assembled exclusively from internally-validated,
    /// closed-vocabulary pieces, the way `apply_retention_policies` does).
    /// Never forward user-supplied or otherwise untrusted input as
    /// `where_clause`. This method validates `self.queue_name` (the table
    /// name) but has no way to validate the semantic safety of an arbitrary
    /// predicate string.
    pub async fn archive_by_criteria(&self, where_clause: &str) -> Result<i64> {
        // The INSERT projects explicit columns: `celers_task_history` has six
        // columns while `celers_tasks` has sixteen, so the historical
        // `INSERT INTO celers_task_history SELECT * FROM ...` form could never
        // execute — PostgreSQL rejected it on arity before it ever reached the
        // data. Both statements are queue-scoped through a bound `$1` and run
        // inside one transaction, so a failure between them can no longer
        // duplicate rows into the history table on retry.
        let conn = self.connection().await?;
        let mut tx = conn.transaction().await.map_err(|e| {
            CelersError::Other(format!("Failed to begin archive transaction: {}", e))
        })?;

        let archive_query = crate::sql::archive_insert_sql(where_clause);
        let archived_count = tx
            .execute(&archive_query, &[&self.queue_name])
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to archive tasks to history: {}", e))
            })?;

        let delete_query = crate::sql::archive_delete_sql(where_clause);
        tx.execute(&delete_query, &[&self.queue_name])
            .await
            .map_err(|e| CelersError::Other(format!("Failed to delete archived tasks: {}", e)))?;

        tx.commit()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to commit archive: {}", e)))?;

        Ok(i64::try_from(archived_count).unwrap_or(i64::MAX))
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

            // Closed-vocabulary validation: only a genuine DbTaskState
            // variant (rendered via its canonical Display string) can reach
            // the SQL text below, regardless of what `policy.task_state`
            // originally contained.
            let validated_state: DbTaskState = policy.task_state.parse()?;

            let where_clause = format!(
                "state = '{}' AND created_at < NOW() - INTERVAL '{} days'",
                validated_state, policy.retention_days
            );

            if policy.archive_before_delete {
                let archived = self.archive_by_criteria(&where_clause).await?;
                total_affected += archived;
            } else {
                let delete_query = crate::sql::archive_delete_sql(&where_clause);
                let affected = self
                    .conn
                    .execute(&delete_query, &[&self.queue_name])
                    .await
                    .map_err(|e| {
                        CelersError::Other(format!(
                            "Failed to delete tasks by retention policy: {}",
                            e
                        ))
                    })?;
                total_affected += i64::try_from(affected).unwrap_or(i64::MAX);
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
        let snapshot = self.conn.snapshot();

        // Utilisation is "how much of the configured pool is checked out right
        // now", which is the number an operator wants when deciding whether to
        // grow the pool. (It used to divide by a hardcoded `max_size = 0`,
        // so it was always 0.0 and the recommendation was always "optimal".)
        let utilization = if pool_metrics.max_size > 0 {
            (pool_metrics.in_use as f64 / pool_metrics.max_size as f64) * 100.0
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
            // Measured: the pool keeps an EWMA of how long each checkout
            // waited for a free slot.
            avg_wait_time_ms: snapshot.avg_wait_us as f64 / 1000.0,
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
        // The batch bound is expressed as `id IN (SELECT ... LIMIT n)`, not as
        // a trailing `LIMIT n` on the predicate: the fragment also lands in a
        // `DELETE`, and PostgreSQL's `DELETE` has no `LIMIT` clause — the old
        // form was a syntax error waiting to happen.
        let where_clause = crate::sql::completed_batch_predicate(older_than_days, batch_size);
        self.archive_by_criteria(&where_clause).await
    }
}

#[cfg(test)]
mod tests {
    use super::validate_sql_identifier;

    #[test]
    fn validate_sql_identifier_accepts_valid_identifiers() {
        assert!(validate_sql_identifier("tasks").is_ok());
        assert!(validate_sql_identifier("_tasks").is_ok());
        assert!(validate_sql_identifier("tasks_2").is_ok());
        assert!(validate_sql_identifier("Tasks_Table").is_ok());
        assert!(validate_sql_identifier("celers_default_queue").is_ok());
    }

    #[test]
    fn validate_sql_identifier_rejects_invalid_identifiers() {
        assert!(validate_sql_identifier("").is_err());
        assert!(validate_sql_identifier("2tasks").is_err());
        assert!(validate_sql_identifier("tasks-table").is_err());
        assert!(validate_sql_identifier("tasks.table").is_err());
        assert!(validate_sql_identifier("tasks; DROP TABLE users;--").is_err());
        assert!(validate_sql_identifier("tasks WHERE 1=1").is_err());
        assert!(validate_sql_identifier("tasks'").is_err());
    }
}
