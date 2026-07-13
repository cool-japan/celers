//! Analytics module for database result backends
//!
//! Provides aggregate query helpers for task success/failure rates,
//! duration percentiles, per-worker throughput, result storage sizing,
//! and chord completion rates.  Both [`PostgresAnalytics`] and
//! [`MysqlAnalytics`] expose the same five async methods but issue
//! database-appropriate SQL internally.

use crate::row_ext::RowExt;
use celers_backend_redis::BackendError;
use oxisql_core::Connection;
use std::collections::HashMap;
use std::time::Duration;

// ──────────────────────────────────────────────────────────────────
// Shared result types
// ──────────────────────────────────────────────────────────────────

/// Aggregated task statistics over a time window.
#[derive(Debug, Clone)]
pub struct TaskStats {
    /// Total tasks seen in the window (all states).
    pub total_count: u64,
    /// Tasks that reached the `success` state.
    pub success_count: u64,
    /// Tasks that reached the `failure` state.
    pub failure_count: u64,
    /// Tasks that reached the `retry` state.
    pub retry_count: u64,
    /// Tasks still in the `pending` state.
    pub pending_count: u64,
    /// `success_count / total_count`, or `0.0` when `total_count == 0`.
    pub success_rate: f64,
    /// `failure_count / total_count`, or `0.0` when `total_count == 0`.
    pub failure_rate: f64,
}

impl TaskStats {
    /// Build a `TaskStats` from raw counts; rates are computed automatically.
    pub fn from_counts(
        total_count: u64,
        success_count: u64,
        failure_count: u64,
        retry_count: u64,
        pending_count: u64,
    ) -> Self {
        let success_rate = if total_count > 0 {
            success_count as f64 / total_count as f64
        } else {
            0.0
        };
        let failure_rate = if total_count > 0 {
            failure_count as f64 / total_count as f64
        } else {
            0.0
        };
        Self {
            total_count,
            success_count,
            failure_count,
            retry_count,
            pending_count,
            success_rate,
            failure_rate,
        }
    }
}

/// Task duration percentiles computed from `started_at`/`completed_at`.
///
/// All fields are `None` when no completed tasks exist in the window.
#[derive(Debug, Clone)]
pub struct PercentileLatencies {
    /// Arithmetic mean duration.
    pub mean: Option<Duration>,
    /// 50th-percentile (median) duration.
    pub p50: Option<Duration>,
    /// 95th-percentile duration.
    pub p95: Option<Duration>,
    /// 99th-percentile duration.
    pub p99: Option<Duration>,
    /// Minimum observed duration.
    pub min: Option<Duration>,
    /// Maximum observed duration.
    pub max: Option<Duration>,
}

impl PercentileLatencies {
    /// All-`None` instance representing an empty measurement window.
    pub fn empty() -> Self {
        Self {
            mean: None,
            p50: None,
            p95: None,
            p99: None,
            min: None,
            max: None,
        }
    }

    /// Convert an optional seconds value into an optional [`Duration`].
    fn secs_to_duration(secs: Option<f64>) -> Option<Duration> {
        secs.filter(|&s| s >= 0.0).map(Duration::from_secs_f64)
    }
}

/// Per-worker throughput statistics.
#[derive(Debug, Clone)]
pub struct WorkerStat {
    /// Worker identifier string.
    pub worker: String,
    /// Total tasks attributed to this worker in the window.
    pub total_tasks: u64,
    /// Tasks completed successfully.
    pub success_tasks: u64,
    /// Tasks that failed.
    pub failure_tasks: u64,
    /// Mean duration in seconds for completed tasks (`None` if none completed).
    pub avg_duration_secs: Option<f64>,
    /// Estimated tasks processed per hour, derived from window size and count.
    pub tasks_per_hour: f64,
}

/// Storage usage snapshot for the results tables.
#[derive(Debug, Clone)]
pub struct StorageStats {
    /// Total row count across all states.
    pub total_rows: u64,
    /// Row count broken down by `result_state`.
    pub rows_by_state: HashMap<String, u64>,
    /// Rough byte estimate for all stored `result_data` values.
    ///
    /// PostgreSQL: `SUM(octet_length(result_data::text))`.
    /// MySQL: `SUM(LENGTH(result_data))`.
    pub estimated_result_bytes: u64,
    /// Chords that have not yet reached `completed >= total`.
    pub active_chords: u64,
    /// Chords where `completed >= total AND total > 0`.
    pub completed_chords: u64,
}

// ──────────────────────────────────────────────────────────────────
// Internal helpers
// ──────────────────────────────────────────────────────────────────

/// Format a [`Duration`] as a PostgreSQL interval literal, e.g. `"3600 seconds"`.
fn pg_interval_secs(window: Duration) -> String {
    format!("{} seconds", window.as_secs())
}

// ──────────────────────────────────────────────────────────────────
// PostgresAnalytics
// ──────────────────────────────────────────────────────────────────

/// Analytics queries for a PostgreSQL result backend.
#[derive(Clone)]
pub struct PostgresAnalytics {
    conn: oxisql_postgres::PgConnection,
}

// Manual `Debug` impl: `oxisql_postgres::PgConnection` does not implement
// `Debug` (unlike the previous `sqlx::PgPool`, which did), so `#[derive(Debug)]`
// no longer applies directly. The connection handle carries no fields
// meaningful to print (it wraps an `Arc<Mutex<tokio_postgres::Client>>`), so
// this opaque placeholder preserves the type's `Debug` bound for any
// generic code (`{:?}`-formatting callers, `assert_debug_snapshot!`, etc.)
// without attempting to print connection internals.
impl std::fmt::Debug for PostgresAnalytics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresAnalytics")
            .field("conn", &"PgConnection { .. }")
            .finish()
    }
}

impl PostgresAnalytics {
    /// Create a new analytics helper bound to `conn`.
    pub fn new(conn: oxisql_postgres::PgConnection) -> Self {
        Self { conn }
    }

    /// Return aggregate success/failure/retry/pending counts for tasks created
    /// within `since` duration of now.  Optionally filter to a single task name.
    pub async fn task_stats(
        &self,
        since: Duration,
        task_name: Option<&str>,
    ) -> Result<TaskStats, BackendError> {
        let interval = pg_interval_secs(since);

        let rows = self
            .conn
            .query(
                r#"
                SELECT
                    COUNT(*)                                               AS total_count,
                    COUNT(*) FILTER (WHERE result_state = 'success')      AS success_count,
                    COUNT(*) FILTER (WHERE result_state = 'failure')      AS failure_count,
                    COUNT(*) FILTER (WHERE result_state = 'retry')        AS retry_count,
                    COUNT(*) FILTER (WHERE result_state = 'pending')      AS pending_count
                FROM celers_task_results
                WHERE created_at >= NOW() - $1::interval
                  AND ($2::text IS NULL OR task_name = $2)
                "#,
                &[&interval, &task_name],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("task_stats query failed: {}", e)))?;
        let row = rows.into_iter().next().ok_or_else(|| {
            BackendError::Connection("task_stats query returned no rows".to_string())
        })?;

        let total: i64 = row
            .col("total_count")
            .map_err(|e| BackendError::Connection(format!("task_stats query failed: {e}")))?;
        let success: i64 = row
            .col("success_count")
            .map_err(|e| BackendError::Connection(format!("task_stats query failed: {e}")))?;
        let failure: i64 = row
            .col("failure_count")
            .map_err(|e| BackendError::Connection(format!("task_stats query failed: {e}")))?;
        let retry: i64 = row
            .col("retry_count")
            .map_err(|e| BackendError::Connection(format!("task_stats query failed: {e}")))?;
        let pending: i64 = row
            .col("pending_count")
            .map_err(|e| BackendError::Connection(format!("task_stats query failed: {e}")))?;

        Ok(TaskStats::from_counts(
            total as u64,
            success as u64,
            failure as u64,
            retry as u64,
            pending as u64,
        ))
    }

    /// Compute duration percentiles (p50, p95, p99, mean, min, max) for tasks
    /// that completed within `since`.  Optionally filter to a single task name.
    ///
    /// Returns [`PercentileLatencies::empty`] when no qualifying rows exist.
    pub async fn percentile_latencies(
        &self,
        since: Duration,
        task_name: Option<&str>,
    ) -> Result<PercentileLatencies, BackendError> {
        let interval = pg_interval_secs(since);

        let rows = self
            .conn
            .query(
                r#"
                SELECT
                    PERCENTILE_CONT(0.5)  WITHIN GROUP (ORDER BY
                        EXTRACT(EPOCH FROM (completed_at - started_at)))  AS p50,
                    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY
                        EXTRACT(EPOCH FROM (completed_at - started_at)))  AS p95,
                    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY
                        EXTRACT(EPOCH FROM (completed_at - started_at)))  AS p99,
                    AVG(EXTRACT(EPOCH FROM (completed_at - started_at)))  AS mean_secs,
                    MIN(EXTRACT(EPOCH FROM (completed_at - started_at)))  AS min_secs,
                    MAX(EXTRACT(EPOCH FROM (completed_at - started_at)))  AS max_secs
                FROM celers_task_results
                WHERE completed_at IS NOT NULL
                  AND started_at   IS NOT NULL
                  AND created_at  >= NOW() - $1::interval
                  AND ($2::text IS NULL OR task_name = $2)
                  AND result_state IN ('success', 'failure')
                "#,
                &[&interval, &task_name],
            )
            .await
            .map_err(|e| {
                BackendError::Connection(format!("percentile_latencies query failed: {}", e))
            })?;
        let row = rows.into_iter().next().ok_or_else(|| {
            BackendError::Connection("percentile_latencies query returned no rows".to_string())
        })?;

        let p50: Option<f64> = row.col("p50").map_err(|e| {
            BackendError::Connection(format!("percentile_latencies query failed: {e}"))
        })?;
        let p95: Option<f64> = row.col("p95").map_err(|e| {
            BackendError::Connection(format!("percentile_latencies query failed: {e}"))
        })?;
        let p99: Option<f64> = row.col("p99").map_err(|e| {
            BackendError::Connection(format!("percentile_latencies query failed: {e}"))
        })?;
        let mean: Option<f64> = row.col("mean_secs").map_err(|e| {
            BackendError::Connection(format!("percentile_latencies query failed: {e}"))
        })?;
        let min: Option<f64> = row.col("min_secs").map_err(|e| {
            BackendError::Connection(format!("percentile_latencies query failed: {e}"))
        })?;
        let max: Option<f64> = row.col("max_secs").map_err(|e| {
            BackendError::Connection(format!("percentile_latencies query failed: {e}"))
        })?;

        Ok(PercentileLatencies {
            mean: PercentileLatencies::secs_to_duration(mean),
            p50: PercentileLatencies::secs_to_duration(p50),
            p95: PercentileLatencies::secs_to_duration(p95),
            p99: PercentileLatencies::secs_to_duration(p99),
            min: PercentileLatencies::secs_to_duration(min),
            max: PercentileLatencies::secs_to_duration(max),
        })
    }

    /// Compute per-worker throughput and success/failure counts for tasks created
    /// within `since`.  Workers with a `NULL` worker column are excluded.
    pub async fn worker_stats(&self, since: Duration) -> Result<Vec<WorkerStat>, BackendError> {
        let interval = pg_interval_secs(since);
        let window_hours = since.as_secs_f64() / 3600.0;

        let rows = self
            .conn
            .query(
                r#"
                SELECT
                    worker,
                    COUNT(*)                                              AS total_tasks,
                    COUNT(*) FILTER (WHERE result_state = 'success')      AS success_tasks,
                    COUNT(*) FILTER (WHERE result_state = 'failure')      AS failure_tasks,
                    AVG(EXTRACT(EPOCH FROM (completed_at - started_at)))
                        FILTER (WHERE completed_at IS NOT NULL
                                  AND started_at   IS NOT NULL)           AS avg_duration_secs
                FROM celers_task_results
                WHERE worker    IS NOT NULL
                  AND created_at >= NOW() - $1::interval
                GROUP BY worker
                ORDER BY total_tasks DESC
                "#,
                &[&interval],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("worker_stats query failed: {}", e)))?;

        let stats = rows
            .into_iter()
            .map(|row| {
                let total: i64 = row.col("total_tasks")?;
                let success: i64 = row.col("success_tasks")?;
                let failure: i64 = row.col("failure_tasks")?;
                let avg_dur: Option<f64> = row.col("avg_duration_secs")?;
                let tasks_per_hour = if window_hours > 0.0 {
                    total as f64 / window_hours
                } else {
                    0.0
                };
                Ok(WorkerStat {
                    worker: row.col("worker")?,
                    total_tasks: total as u64,
                    success_tasks: success as u64,
                    failure_tasks: failure as u64,
                    avg_duration_secs: avg_dur,
                    tasks_per_hour,
                })
            })
            .collect::<std::result::Result<Vec<WorkerStat>, oxisql_core::OxiSqlError>>()
            .map_err(|e| {
                BackendError::Connection(format!("worker_stats row decode failed: {e}"))
            })?;

        Ok(stats)
    }

    /// Snapshot current storage utilisation: row counts per state, estimated
    /// byte usage of `result_data`, and chord completion counters.
    pub async fn storage_stats(&self) -> Result<StorageStats, BackendError> {
        // Per-state row counts and data size
        let rows = self
            .conn
            .query(
                r#"
                SELECT
                    result_state,
                    COUNT(*)                                                      AS row_count,
                    COALESCE(SUM(octet_length(result_data::text)), 0)::bigint     AS data_bytes
                FROM celers_task_results
                GROUP BY result_state
                "#,
                &[],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("storage_stats query failed: {}", e)))?;

        let mut rows_by_state: HashMap<String, u64> = HashMap::new();
        let mut total_rows: u64 = 0;
        let mut estimated_result_bytes: u64 = 0;

        for row in rows {
            let state: String = row.col("result_state").map_err(|e| {
                BackendError::Connection(format!("storage_stats query failed: {e}"))
            })?;
            let count: i64 = row.col("row_count").map_err(|e| {
                BackendError::Connection(format!("storage_stats query failed: {e}"))
            })?;
            let bytes: i64 = row.col("data_bytes").map_err(|e| {
                BackendError::Connection(format!("storage_stats query failed: {e}"))
            })?;
            rows_by_state.insert(state, count as u64);
            total_rows += count as u64;
            estimated_result_bytes += bytes as u64;
        }

        // Chord completion counters
        let chord_rows = self
            .conn
            .query(
                r#"
                SELECT
                    COUNT(*) FILTER (WHERE completed >= total AND total > 0)  AS completed_chords,
                    COUNT(*) FILTER (WHERE completed  < total OR  total = 0)  AS active_chords
                FROM celers_chord_state
                "#,
                &[],
            )
            .await
            .map_err(|e| {
                BackendError::Connection(format!("chord storage_stats query failed: {}", e))
            })?;
        let chord_row = chord_rows.into_iter().next().ok_or_else(|| {
            BackendError::Connection("chord storage_stats query returned no rows".to_string())
        })?;

        let completed_chords: i64 = chord_row.col("completed_chords").map_err(|e| {
            BackendError::Connection(format!("chord storage_stats query failed: {e}"))
        })?;
        let active_chords: i64 = chord_row.col("active_chords").map_err(|e| {
            BackendError::Connection(format!("chord storage_stats query failed: {e}"))
        })?;

        Ok(StorageStats {
            total_rows,
            rows_by_state,
            estimated_result_bytes,
            active_chords: active_chords as u64,
            completed_chords: completed_chords as u64,
        })
    }

    /// Compute the fraction of chords (created within `since`) where all tasks
    /// have completed.  Returns `0.0` when no chords exist in the window.
    pub async fn chord_completion_rate(&self, since: Duration) -> Result<f64, BackendError> {
        let interval = pg_interval_secs(since);

        let rows = self
            .conn
            .query(
                r#"
                SELECT COALESCE(
                    SUM(CASE WHEN completed >= total AND total > 0 THEN 1 ELSE 0 END)::float
                        / NULLIF(COUNT(*), 0),
                    0.0
                ) AS rate
                FROM celers_chord_state
                WHERE created_at >= NOW() - $1::interval
                "#,
                &[&interval],
            )
            .await
            .map_err(|e| {
                BackendError::Connection(format!("chord_completion_rate query failed: {}", e))
            })?;
        let row = rows.into_iter().next().ok_or_else(|| {
            BackendError::Connection("chord_completion_rate query returned no rows".to_string())
        })?;

        let rate: f64 = row.col("rate").map_err(|e| {
            BackendError::Connection(format!("chord_completion_rate query failed: {e}"))
        })?;
        Ok(rate)
    }
}

// ──────────────────────────────────────────────────────────────────
// MysqlAnalytics
// ──────────────────────────────────────────────────────────────────

/// Analytics queries for a MySQL result backend.
#[derive(Clone)]
pub struct MysqlAnalytics {
    conn: oxisql_mysql::MyConnection,
}

// Manual `Debug` impl: `oxisql_mysql::MyConnection` does not implement
// `Debug` (unlike the previous `sqlx::MySqlPool`, which did). See
// `PostgresAnalytics`'s `Debug` impl above for the full rationale.
impl std::fmt::Debug for MysqlAnalytics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MysqlAnalytics")
            .field("conn", &"MyConnection { .. }")
            .finish()
    }
}

impl MysqlAnalytics {
    /// Create a new analytics helper bound to `conn`.
    pub fn new(conn: oxisql_mysql::MyConnection) -> Self {
        Self { conn }
    }

    /// Return aggregate success/failure/retry/pending counts for tasks created
    /// within `since` duration of now.  Optionally filter to a single task name.
    pub async fn task_stats(
        &self,
        since: Duration,
        task_name: Option<&str>,
    ) -> Result<TaskStats, BackendError> {
        let secs = since.as_secs();

        // MySQL does not support a generic placeholder for the INTERVAL literal,
        // so we format it directly (the value is a u64 — no injection risk).
        let sql = format!(
            r#"
            SELECT
                COUNT(*)                                                             AS total_count,
                SUM(CASE WHEN result_state = 'success' THEN 1 ELSE 0 END)           AS success_count,
                SUM(CASE WHEN result_state = 'failure' THEN 1 ELSE 0 END)           AS failure_count,
                SUM(CASE WHEN result_state = 'retry'   THEN 1 ELSE 0 END)           AS retry_count,
                SUM(CASE WHEN result_state = 'pending' THEN 1 ELSE 0 END)           AS pending_count
            FROM celers_task_results
            WHERE created_at >= NOW() - INTERVAL {secs} SECOND
              AND (? IS NULL OR task_name = ?)
            "#,
        );

        let rows = self
            .conn
            .query(&sql, &[&task_name, &task_name])
            .await
            .map_err(|e| BackendError::Connection(format!("task_stats query failed: {}", e)))?;
        let row = rows.into_iter().next().ok_or_else(|| {
            BackendError::Connection("task_stats query returned no rows".to_string())
        })?;

        let total: i64 = row
            .col("total_count")
            .map_err(|e| BackendError::Connection(format!("task_stats query failed: {e}")))?;
        let success: i64 = row
            .col("success_count")
            .map_err(|e| BackendError::Connection(format!("task_stats query failed: {e}")))?;
        let failure: i64 = row
            .col("failure_count")
            .map_err(|e| BackendError::Connection(format!("task_stats query failed: {e}")))?;
        let retry: i64 = row
            .col("retry_count")
            .map_err(|e| BackendError::Connection(format!("task_stats query failed: {e}")))?;
        let pending: i64 = row
            .col("pending_count")
            .map_err(|e| BackendError::Connection(format!("task_stats query failed: {e}")))?;

        Ok(TaskStats::from_counts(
            total as u64,
            success as u64,
            failure as u64,
            retry as u64,
            pending as u64,
        ))
    }

    /// Compute duration percentiles for tasks that completed within `since`.
    ///
    /// MySQL 5.7/8.0 lack aggregate `PERCENTILE_CONT`, so p50/p95/p99 are
    /// derived from `ORDER BY … LIMIT 1 OFFSET k` sub-queries.
    pub async fn percentile_latencies(
        &self,
        since: Duration,
        task_name: Option<&str>,
    ) -> Result<PercentileLatencies, BackendError> {
        let secs = since.as_secs();

        // Step 1: count eligible rows, mean, min, max
        let count_sql = format!(
            r#"
            SELECT
                COUNT(*)                                                                     AS cnt,
                AVG(TIMESTAMPDIFF(MICROSECOND, started_at, completed_at)) / 1000000.0       AS mean_secs,
                MIN(TIMESTAMPDIFF(MICROSECOND, started_at, completed_at)) / 1000000.0       AS min_secs,
                MAX(TIMESTAMPDIFF(MICROSECOND, started_at, completed_at)) / 1000000.0       AS max_secs
            FROM celers_task_results
            WHERE completed_at IS NOT NULL
              AND started_at   IS NOT NULL
              AND created_at  >= NOW() - INTERVAL {secs} SECOND
              AND result_state IN ('success', 'failure')
              AND (? IS NULL OR task_name = ?)
            "#
        );

        let count_rows = self
            .conn
            .query(&count_sql, &[&task_name, &task_name])
            .await
            .map_err(|e| {
                BackendError::Connection(format!("percentile_latencies count query failed: {}", e))
            })?;
        let count_row = count_rows.into_iter().next().ok_or_else(|| {
            BackendError::Connection(
                "percentile_latencies count query returned no rows".to_string(),
            )
        })?;

        let cnt: i64 = count_row.col("cnt").map_err(|e| {
            BackendError::Connection(format!("percentile_latencies count query failed: {e}"))
        })?;

        if cnt == 0 {
            return Ok(PercentileLatencies::empty());
        }

        let mean: Option<f64> = count_row.col("mean_secs").map_err(|e| {
            BackendError::Connection(format!("percentile_latencies count query failed: {e}"))
        })?;
        let min: Option<f64> = count_row.col("min_secs").map_err(|e| {
            BackendError::Connection(format!("percentile_latencies count query failed: {e}"))
        })?;
        let max: Option<f64> = count_row.col("max_secs").map_err(|e| {
            BackendError::Connection(format!("percentile_latencies count query failed: {e}"))
        })?;

        // Step 2: fetch each percentile via LIMIT 1 OFFSET
        let fetch_percentile = |fraction: f64| {
            let offset = (fraction * cnt as f64).floor() as i64;
            let sql = format!(
                r#"
                SELECT TIMESTAMPDIFF(MICROSECOND, started_at, completed_at) / 1000000.0 AS dur_secs
                FROM celers_task_results
                WHERE completed_at IS NOT NULL
                  AND started_at   IS NOT NULL
                  AND created_at  >= NOW() - INTERVAL {secs} SECOND
                  AND result_state IN ('success', 'failure')
                  AND (? IS NULL OR task_name = ?)
                ORDER BY TIMESTAMPDIFF(MICROSECOND, started_at, completed_at)
                LIMIT 1 OFFSET {offset}
                "#
            );
            (sql, task_name)
        };

        let (p50_sql, tn) = fetch_percentile(0.50);
        let p50_rows =
            self.conn.query(&p50_sql, &[&tn, &tn]).await.map_err(|e| {
                BackendError::Connection(format!("percentile p50 query failed: {}", e))
            })?;
        let p50: Option<f64> = p50_rows
            .into_iter()
            .next()
            .map(|r| r.col("dur_secs"))
            .transpose()
            .map_err(|e| BackendError::Connection(format!("percentile p50 query failed: {e}")))?;

        let (p95_sql, tn) = fetch_percentile(0.95);
        let p95_rows =
            self.conn.query(&p95_sql, &[&tn, &tn]).await.map_err(|e| {
                BackendError::Connection(format!("percentile p95 query failed: {}", e))
            })?;
        let p95: Option<f64> = p95_rows
            .into_iter()
            .next()
            .map(|r| r.col("dur_secs"))
            .transpose()
            .map_err(|e| BackendError::Connection(format!("percentile p95 query failed: {e}")))?;

        let (p99_sql, tn) = fetch_percentile(0.99);
        let p99_rows =
            self.conn.query(&p99_sql, &[&tn, &tn]).await.map_err(|e| {
                BackendError::Connection(format!("percentile p99 query failed: {}", e))
            })?;
        let p99: Option<f64> = p99_rows
            .into_iter()
            .next()
            .map(|r| r.col("dur_secs"))
            .transpose()
            .map_err(|e| BackendError::Connection(format!("percentile p99 query failed: {e}")))?;

        Ok(PercentileLatencies {
            mean: PercentileLatencies::secs_to_duration(mean),
            p50: PercentileLatencies::secs_to_duration(p50),
            p95: PercentileLatencies::secs_to_duration(p95),
            p99: PercentileLatencies::secs_to_duration(p99),
            min: PercentileLatencies::secs_to_duration(min),
            max: PercentileLatencies::secs_to_duration(max),
        })
    }

    /// Compute per-worker throughput and success/failure counts for tasks created
    /// within `since`.  Workers with a `NULL` worker column are excluded.
    pub async fn worker_stats(&self, since: Duration) -> Result<Vec<WorkerStat>, BackendError> {
        let secs = since.as_secs();
        let window_hours = since.as_secs_f64() / 3600.0;

        let sql = format!(
            r#"
            SELECT
                worker,
                COUNT(*)                                                                           AS total_tasks,
                SUM(CASE WHEN result_state = 'success' THEN 1 ELSE 0 END)                         AS success_tasks,
                SUM(CASE WHEN result_state = 'failure' THEN 1 ELSE 0 END)                         AS failure_tasks,
                AVG(
                    CASE WHEN completed_at IS NOT NULL AND started_at IS NOT NULL
                         THEN TIMESTAMPDIFF(MICROSECOND, started_at, completed_at) / 1000000.0
                    END
                )                                                                                  AS avg_duration_secs
            FROM celers_task_results
            WHERE worker    IS NOT NULL
              AND created_at >= NOW() - INTERVAL {secs} SECOND
            GROUP BY worker
            ORDER BY total_tasks DESC
            "#
        );

        let rows =
            self.conn.query(&sql, &[]).await.map_err(|e| {
                BackendError::Connection(format!("worker_stats query failed: {}", e))
            })?;

        let stats = rows
            .into_iter()
            .map(|row| {
                let total: i64 = row.col("total_tasks")?;
                let success: i64 = row.col("success_tasks")?;
                let failure: i64 = row.col("failure_tasks")?;
                let avg_dur: Option<f64> = row.col("avg_duration_secs")?;
                let tasks_per_hour = if window_hours > 0.0 {
                    total as f64 / window_hours
                } else {
                    0.0
                };
                Ok(WorkerStat {
                    worker: row.col("worker")?,
                    total_tasks: total as u64,
                    success_tasks: success as u64,
                    failure_tasks: failure as u64,
                    avg_duration_secs: avg_dur,
                    tasks_per_hour,
                })
            })
            .collect::<std::result::Result<Vec<WorkerStat>, oxisql_core::OxiSqlError>>()
            .map_err(|e| {
                BackendError::Connection(format!("worker_stats row decode failed: {e}"))
            })?;

        Ok(stats)
    }

    /// Snapshot current storage utilisation: row counts per state, estimated
    /// byte usage of `result_data`, and chord completion counters.
    pub async fn storage_stats(&self) -> Result<StorageStats, BackendError> {
        let rows = self
            .conn
            .query(
                r#"
                SELECT
                    result_state,
                    COUNT(*)                                         AS row_count,
                    COALESCE(SUM(LENGTH(result_data)), 0)            AS data_bytes
                FROM celers_task_results
                GROUP BY result_state
                "#,
                &[],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("storage_stats query failed: {}", e)))?;

        let mut rows_by_state: HashMap<String, u64> = HashMap::new();
        let mut total_rows: u64 = 0;
        let mut estimated_result_bytes: u64 = 0;

        for row in rows {
            let state: String = row.col("result_state").map_err(|e| {
                BackendError::Connection(format!("storage_stats query failed: {e}"))
            })?;
            let count: i64 = row.col("row_count").map_err(|e| {
                BackendError::Connection(format!("storage_stats query failed: {e}"))
            })?;
            let bytes: i64 = row.col("data_bytes").map_err(|e| {
                BackendError::Connection(format!("storage_stats query failed: {e}"))
            })?;
            rows_by_state.insert(state, count as u64);
            total_rows += count as u64;
            estimated_result_bytes += bytes as u64;
        }

        let chord_rows = self
            .conn
            .query(
                r#"
                SELECT
                    SUM(CASE WHEN completed >= total AND total > 0 THEN 1 ELSE 0 END) AS completed_chords,
                    SUM(CASE WHEN completed  < total OR  total = 0 THEN 1 ELSE 0 END) AS active_chords
                FROM celers_chord_state
                "#,
                &[],
            )
            .await
            .map_err(|e| {
                BackendError::Connection(format!("chord storage_stats query failed: {}", e))
            })?;
        let chord_row = chord_rows.into_iter().next().ok_or_else(|| {
            BackendError::Connection("chord storage_stats query returned no rows".to_string())
        })?;

        let completed_chords: Option<i64> = chord_row.col("completed_chords").map_err(|e| {
            BackendError::Connection(format!("chord storage_stats query failed: {e}"))
        })?;
        let active_chords: Option<i64> = chord_row.col("active_chords").map_err(|e| {
            BackendError::Connection(format!("chord storage_stats query failed: {e}"))
        })?;

        Ok(StorageStats {
            total_rows,
            rows_by_state,
            estimated_result_bytes,
            active_chords: active_chords.unwrap_or(0) as u64,
            completed_chords: completed_chords.unwrap_or(0) as u64,
        })
    }

    /// Compute the fraction of chords (created within `since`) where all tasks
    /// have completed.  Returns `0.0` when no chords exist in the window.
    pub async fn chord_completion_rate(&self, since: Duration) -> Result<f64, BackendError> {
        let secs = since.as_secs();
        let sql = format!(
            r#"
            SELECT COALESCE(
                SUM(CASE WHEN completed >= total AND total > 0 THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(*), 0),
                0.0
            ) AS rate
            FROM celers_chord_state
            WHERE created_at >= NOW() - INTERVAL {secs} SECOND
            "#
        );

        let rows = self.conn.query(&sql, &[]).await.map_err(|e| {
            BackendError::Connection(format!("chord_completion_rate query failed: {}", e))
        })?;
        let row = rows.into_iter().next().ok_or_else(|| {
            BackendError::Connection("chord_completion_rate query returned no rows".to_string())
        })?;

        let rate: Option<f64> = row.col("rate").map_err(|e| {
            BackendError::Connection(format!("chord_completion_rate query failed: {e}"))
        })?;
        Ok(rate.unwrap_or(0.0))
    }
}

// ──────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── TaskStats ────────────────────────────────────────────────

    #[test]
    fn test_task_stats_rates_all_success() {
        let stats = TaskStats::from_counts(100, 100, 0, 0, 0);
        assert_eq!(stats.total_count, 100);
        assert_eq!(stats.success_count, 100);
        assert!((stats.success_rate - 1.0).abs() < f64::EPSILON);
        assert!(stats.failure_rate.abs() < f64::EPSILON);
    }

    #[test]
    fn test_task_stats_rates_mixed() {
        let stats = TaskStats::from_counts(200, 150, 40, 5, 5);
        assert_eq!(stats.total_count, 200);
        assert!((stats.success_rate - 0.75).abs() < 1e-9);
        assert!((stats.failure_rate - 0.20).abs() < 1e-9);
    }

    #[test]
    fn test_task_stats_zero_total() {
        let stats = TaskStats::from_counts(0, 0, 0, 0, 0);
        assert_eq!(stats.success_rate, 0.0);
        assert_eq!(stats.failure_rate, 0.0);
    }

    #[test]
    fn test_task_stats_all_failure() {
        let stats = TaskStats::from_counts(50, 0, 50, 0, 0);
        assert_eq!(stats.failure_count, 50);
        assert!((stats.failure_rate - 1.0).abs() < f64::EPSILON);
        assert!(stats.success_rate.abs() < f64::EPSILON);
    }

    // ── WorkerStat ───────────────────────────────────────────────

    #[test]
    fn test_worker_stat_construction() {
        let ws = WorkerStat {
            worker: "worker-1@host".to_string(),
            total_tasks: 1000,
            success_tasks: 980,
            failure_tasks: 20,
            avg_duration_secs: Some(0.350),
            tasks_per_hour: 250.0,
        };
        assert_eq!(ws.worker, "worker-1@host");
        assert_eq!(ws.total_tasks, 1000);
        assert_eq!(ws.success_tasks + ws.failure_tasks, 1000);
        assert!(ws.avg_duration_secs.is_some());
        assert!((ws.tasks_per_hour - 250.0).abs() < f64::EPSILON);
    }

    // ── StorageStats ─────────────────────────────────────────────

    #[test]
    fn test_storage_stats_construction() {
        let mut rows_by_state = HashMap::new();
        rows_by_state.insert("success".to_string(), 800_u64);
        rows_by_state.insert("failure".to_string(), 100_u64);
        rows_by_state.insert("pending".to_string(), 50_u64);

        let stats = StorageStats {
            total_rows: 950,
            rows_by_state,
            estimated_result_bytes: 1_048_576,
            active_chords: 3,
            completed_chords: 47,
        };

        assert_eq!(stats.total_rows, 950);
        assert_eq!(*stats.rows_by_state.get("success").unwrap(), 800);
        assert_eq!(stats.estimated_result_bytes, 1_048_576);
        assert_eq!(stats.active_chords + stats.completed_chords, 50);
    }

    // ── PercentileLatencies ──────────────────────────────────────

    #[test]
    fn test_percentile_latencies_empty() {
        let pl = PercentileLatencies::empty();
        assert!(pl.mean.is_none());
        assert!(pl.p50.is_none());
        assert!(pl.p95.is_none());
        assert!(pl.p99.is_none());
        assert!(pl.min.is_none());
        assert!(pl.max.is_none());
    }

    #[test]
    fn test_percentile_latencies_from_secs() {
        let pl = PercentileLatencies {
            mean: PercentileLatencies::secs_to_duration(Some(1.5)),
            p50: PercentileLatencies::secs_to_duration(Some(1.0)),
            p95: PercentileLatencies::secs_to_duration(Some(3.0)),
            p99: PercentileLatencies::secs_to_duration(Some(5.0)),
            min: PercentileLatencies::secs_to_duration(Some(0.1)),
            max: PercentileLatencies::secs_to_duration(Some(10.0)),
        };
        assert_eq!(pl.p50, Some(Duration::from_secs_f64(1.0)));
        assert_eq!(pl.max, Some(Duration::from_secs_f64(10.0)));
    }

    #[test]
    fn test_percentile_latencies_negative_secs_becomes_none() {
        // Negative durations (data anomaly) must not panic; they map to None.
        let result = PercentileLatencies::secs_to_duration(Some(-0.5));
        assert!(result.is_none());
    }

    // ── pg_interval_secs helper ──────────────────────────────────

    #[test]
    fn test_pg_interval_secs_formatting() {
        let s = pg_interval_secs(Duration::from_secs(3600));
        assert_eq!(s, "3600 seconds");

        let s2 = pg_interval_secs(Duration::from_secs(86400));
        assert_eq!(s2, "86400 seconds");
    }

    // ── DB integration tests (require live DB, marked #[ignore]) ──

    #[tokio::test]
    #[ignore] // Requires PostgreSQL running
    async fn test_pg_task_stats_live() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost/celers_test".to_string());
        let conn = oxisql_postgres::PgConnection::connect(&url, oxisql_postgres::TlsMode::Disabled)
            .await
            .expect("connect to live PostgreSQL");
        let analytics = PostgresAnalytics::new(conn);
        let stats = analytics
            .task_stats(Duration::from_secs(3600), None)
            .await
            .expect("task_stats query");
        // Just verify we get a valid (possibly empty) result without panic.
        assert!(stats.success_rate >= 0.0 && stats.success_rate <= 1.0);
    }

    #[tokio::test]
    #[ignore] // Requires PostgreSQL running
    async fn test_pg_storage_stats_live() {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost/celers_test".to_string());
        let conn = oxisql_postgres::PgConnection::connect(&url, oxisql_postgres::TlsMode::Disabled)
            .await
            .expect("connect to live PostgreSQL");
        let analytics = PostgresAnalytics::new(conn);
        let stats = analytics
            .storage_stats()
            .await
            .expect("storage_stats query");
        // Row counts are non-negative by definition.
        let _ = stats.total_rows;
    }
}
