//! Database result backend for CeleRS
//!
//! This crate provides PostgreSQL and MySQL-based storage for task results and workflow state.
//!
//! # Features
//!
//! - Task result storage with expiration
//! - Chord state management (barrier synchronization)
//! - Atomic counter operations
//! - SQL-based result queries and analytics
//! - Support for both PostgreSQL and MySQL, independently selectable via the
//!   `postgres`/`mysql` Cargo features (both enabled by default)
//!
//! # Example
//!
//! ```ignore
//! use celers_backend_db::PostgresResultBackend;
//! use celers_backend_redis::ResultBackend;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut backend = PostgresResultBackend::new("postgres://localhost/celers").await?;
//! backend.migrate().await?;
//!
//! // Store task result
//! let meta = TaskMeta::new(task_id, "my_task".to_string());
//! backend.store_result(task_id, &meta).await?;
//! # Ok(())
//! # }
//! ```

pub mod analytics;
#[cfg(feature = "postgres")]
pub mod event_persistence;
#[cfg(all(feature = "distributed-locks", feature = "postgres"))]
pub mod lock;
#[cfg(feature = "postgres")]
mod pg_pool;
#[cfg(any(feature = "postgres", feature = "mysql"))]
pub mod result_store;
mod row_ext;
#[cfg(feature = "mysql")]
mod sql_split;
#[cfg(any(feature = "postgres", feature = "mysql"))]
mod task_meta_extra;
mod tls_mode;

#[cfg(feature = "mysql")]
pub use analytics::MysqlAnalytics;
#[cfg(feature = "postgres")]
pub use analytics::PostgresAnalytics;
pub use analytics::{PercentileLatencies, StorageStats, TaskStats, WorkerStat};
#[cfg(feature = "postgres")]
pub use event_persistence::{DbEventPersister, DbEventPersisterConfig};
#[cfg(feature = "postgres")]
pub use pg_pool::{PgConnPool, DEFAULT_POOL_SIZE};

#[cfg(any(feature = "postgres", feature = "mysql"))]
use async_trait::async_trait;
pub use celers_backend_redis::{
    BackendError, ChordState, ProgressInfo, Result, ResultBackend, TaskMeta, TaskResult,
    TaskTtlConfig,
};
// Only reached from `default_ttl_config` (postgres/mysql constructors), not
// re-exported: `RedisResultBackend::new`'s own default lives behind this
// same path, so importing it here — rather than repeating the bare
// `Duration::from_secs(86400)` literal it expands to — keeps both backends'
// "24 hours" defined in exactly one place.
#[cfg(any(feature = "postgres", feature = "mysql"))]
use celers_backend_redis::ttl;
// `DateTime` (the bare type name) is only spelled explicitly in MySQL's
// `get_result` (`row.col::<DateTime<Utc>>(..)`) — Postgres's equivalent read
// relies on type inference and never names `DateTime` directly, so a
// postgres-only build would otherwise flag it unused.
#[cfg(feature = "mysql")]
use chrono::DateTime;
#[cfg(any(feature = "postgres", feature = "mysql"))]
use chrono::Utc;
#[cfg(any(feature = "postgres", feature = "mysql"))]
use oxisql_core::{Connection, ToSqlValue};
#[cfg(feature = "postgres")]
use row_ext::uuid_from_row;
#[cfg(any(feature = "postgres", feature = "mysql"))]
use row_ext::RowExt;
#[cfg(feature = "postgres")]
use row_ext::{json_from_row, json_param};
#[cfg(any(feature = "postgres", feature = "mysql"))]
use serde_json::json;
#[cfg(any(feature = "postgres", feature = "mysql"))]
use std::time::Duration;
#[cfg(any(feature = "postgres", feature = "mysql"))]
use task_meta_extra::TaskMetaExtra;
#[cfg(any(feature = "postgres", feature = "mysql"))]
use uuid::Uuid;

/// Decode a stored `result_state` string (plus its accompanying columns)
/// into a [`TaskResult`].
///
/// Shared by every DB read path (Postgres/MySQL, single/batch) so the
/// mapping is defined exactly once. Unlike the ad-hoc `match` this replaces,
/// an unrecognised state is a hard [`BackendError::Serialization`] rather
/// than a silent `TaskResult::Pending` — a schema/code vocabulary drift
/// (a newer writer, a manual data fix, a partially-applied migration, a
/// mixed-version rolling deploy) must be loud, not indistinguishable from a
/// task that simply hasn't run yet: a caller polling `is_task_complete` on a
/// silently-mispapped row would otherwise wait forever, and it would count in
/// neither the success nor the failure bucket of `TaskStats`.
///
/// # Errors
///
/// Returns [`BackendError::Serialization`] if `state` is not one of
/// `"pending"`/`"started"`/`"success"`/`"failure"`/`"revoked"`/`"retry"`.
#[cfg(any(feature = "postgres", feature = "mysql"))]
fn decode_result_state(
    task_id: Uuid,
    state: &str,
    result_data: Option<serde_json::Value>,
    error_message: Option<String>,
    retry_count: Option<i32>,
) -> Result<TaskResult> {
    Ok(match state {
        "pending" => TaskResult::Pending,
        "started" => TaskResult::Started,
        "success" => TaskResult::Success(result_data.unwrap_or(json!(null))),
        // A NULL error_message on a `failure` row is a genuine anomaly (the
        // failure path should always record a reason), but it must not
        // silently downgrade to an empty string that looks like "no error" —
        // surface it as an explicit placeholder instead.
        "failure" => TaskResult::Failure(
            error_message.unwrap_or_else(|| "unknown error (error_message was NULL)".to_string()),
        ),
        "revoked" => TaskResult::Revoked,
        "retry" => TaskResult::Retry(retry_count.unwrap_or(0) as u32),
        other => {
            return Err(BackendError::Serialization(format!(
                "unknown result_state {other:?} for task {task_id}"
            )))
        }
    })
}

/// The TTL configuration every `*ResultBackend::new`/`with_pool_size`
/// constructor installs: results expire after [`ttl::SUCCESS`] (24 hours),
/// matching [`RedisResultBackend`](celers_backend_redis::RedisResultBackend)'s
/// own `new`-time default and Celery's `result_expires`.
///
/// Without a default TTL, `store_result`'s `expires_at` column is left NULL
/// for every task type that has no explicit `set_task_ttl`/`with_ttl_config`
/// override (see `ttl_expires_at_param`), and `cleanup_expired_results()`
/// only ever deletes rows `WHERE expires_at IS NOT NULL` — so with no
/// default, cleanup silently collects nothing and `celers_task_results`
/// grows without bound. Shared by every constructor (rather than each
/// repeating `TaskTtlConfig::with_default(ttl::SUCCESS)`) so the "24 hours"
/// is defined exactly once and is independently unit-testable without a
/// live database connection.
///
/// Callers that genuinely want permanent, never-expiring results can still
/// opt out with `.with_ttl_config(TaskTtlConfig::new())` after construction.
#[cfg(any(feature = "postgres", feature = "mysql"))]
fn default_ttl_config() -> TaskTtlConfig {
    TaskTtlConfig::with_default(ttl::SUCCESS)
}

// ══════════════════════════════════════════════════════════════════════════
// PostgreSQL backend
// ══════════════════════════════════════════════════════════════════════════

/// PostgreSQL result backend implementation
#[cfg(feature = "postgres")]
#[derive(Clone)]
pub struct PostgresResultBackend {
    conn: PgConnPool,
    ttl_config: TaskTtlConfig,
}

#[cfg(feature = "postgres")]
impl PostgresResultBackend {
    /// Create a new PostgreSQL result backend backed by a pool of
    /// [`DEFAULT_POOL_SIZE`] independent connections.
    ///
    /// Results expire after [`ttl::SUCCESS`] (24 hours) by default, matching
    /// `RedisResultBackend::new` and Celery's `result_expires` — see
    /// `default_ttl_config`. Use [`Self::with_ttl_config`]`(TaskTtlConfig::new())`
    /// for permanent results, or `with_ttl_config` with a populated
    /// [`TaskTtlConfig`] for per-task-type expiry.
    ///
    /// # Arguments
    /// * `database_url` - PostgreSQL connection string (e.g., "postgres://user:pass@localhost/db")
    pub async fn new(database_url: &str) -> Result<Self> {
        Self::with_pool_size(database_url, DEFAULT_POOL_SIZE).await
    }

    /// Create a new PostgreSQL result backend with an explicit connection
    /// pool size (see [`PgConnPool`] for the pooling/reconnect behavior this
    /// provides over a single shared connection).
    ///
    /// Installs the same 24-hour default TTL as [`Self::new`] (which calls
    /// this with [`DEFAULT_POOL_SIZE`]) — see its doc comment.
    pub async fn with_pool_size(database_url: &str, pool_size: usize) -> Result<Self> {
        let tls = tls_mode::pg_tls_mode_for_url(database_url)
            .map_err(|e| BackendError::Connection(format!("Failed to resolve TLS mode: {e}")))?;
        let conn = PgConnPool::connect(database_url, tls, pool_size, Duration::from_secs(5))
            .await
            .map_err(|e| {
                BackendError::Connection(format!("Failed to connect to database: {}", e))
            })?;

        Ok(Self {
            conn,
            ttl_config: default_ttl_config(),
        })
    }

    /// Configure per-task-type TTL
    pub fn with_ttl_config(mut self, config: TaskTtlConfig) -> Self {
        self.ttl_config = config;
        self
    }

    /// Get the TTL configuration
    pub fn ttl_config(&self) -> &TaskTtlConfig {
        &self.ttl_config
    }

    /// Get a mutable reference to the TTL configuration
    pub fn ttl_config_mut(&mut self) -> &mut TaskTtlConfig {
        &mut self.ttl_config
    }

    /// Run database migrations
    pub async fn migrate(&self) -> Result<()> {
        let migration_sql = include_str!("../migrations/001_init_postgres.sql");

        // `execute_batch` (simple-query protocol, `batch_execute` under the
        // hood) is used rather than `execute` (extended/prepared-statement
        // protocol) because the migration file contains multiple `;`-
        // separated DDL statements in one string — the extended protocol
        // `oxisql_postgres::Connection::execute`/`query` use rejects
        // multi-statement text, matching the same constraint the
        // pre-migration `sqlx::query(...).execute(...)` call relied on
        // sqlx's own simple-query fallback for.
        self.conn
            .execute_batch(migration_sql)
            .await
            .map_err(|e| BackendError::Connection(format!("Migration failed: {}", e)))?;

        Ok(())
    }

    /// Get the underlying connection pool.
    pub fn connection(&self) -> &PgConnPool {
        &self.conn
    }

    /// Return an analytics helper bound to the same connection pool.
    pub fn analytics(&self) -> PostgresAnalytics {
        PostgresAnalytics::new(self.conn.clone())
    }

    /// Check whether the backend can currently reach the database.
    ///
    /// Issues a trivial `SELECT 1` against the connection pool (benefiting
    /// from [`PgConnPool::query`]'s reconnect-and-retry-once-on-connection-
    /// loss policy) and reports whether it succeeded. Mirrors
    /// `celers_backend_redis::RedisResultBackend::health_check`'s contract
    /// for use in the same monitoring/readiness-probe role: `Ok(true)` means
    /// healthy, `Err(_)` means a connection or other error occurred.
    pub async fn health_check(&self) -> Result<bool> {
        match self.conn.query("SELECT 1", &[]).await {
            Ok(rows) => Ok(!rows.is_empty()),
            Err(e) => Err(BackendError::Connection(format!(
                "health_check query failed: {}",
                e
            ))),
        }
    }

    /// Clean up expired results (returns number of deleted rows)
    pub async fn cleanup_expired(&self) -> Result<usize> {
        let rows = self
            .conn
            .query("SELECT cleanup_expired_results()", &[])
            .await
            .map_err(|e| {
                BackendError::Connection(format!("Failed to cleanup expired results: {}", e))
            })?;
        let row = rows.into_iter().next().ok_or_else(|| {
            BackendError::Connection("cleanup_expired_results() returned no rows".to_string())
        })?;

        let count: i64 = row.col_idx(0).map_err(|e| {
            BackendError::Connection(format!("Failed to read cleanup_expired count: {e}"))
        })?;
        Ok(count as usize)
    }

    /// Spawn a background task that calls [`PostgresResultBackend::cleanup_expired`]
    /// on `interval` forever, logging (rather than propagating) any error so
    /// one failed cleanup pass never kills the scheduler.
    ///
    /// Purely opt-in: nothing calls this automatically. Wire it in from
    /// application start-up if periodic expiry cleanup is desired — this
    /// crate is a library and must not spawn background work the caller
    /// didn't ask for.
    pub fn spawn_periodic_cleanup(&self, interval: Duration) -> tokio::task::JoinHandle<()> {
        let backend = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // The first tick fires immediately; skip it so the first real
            // cleanup happens after one full `interval`, not at t=0.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                match backend.cleanup_expired().await {
                    Ok(n) if n > 0 => tracing::debug!(deleted = n, "cleaned up expired results"),
                    Ok(_) => {}
                    Err(e) => tracing::error!(error = %e, "periodic cleanup_expired failed"),
                }
            }
        })
    }

    /// Serialize the extended [`TaskMeta`] fields (progress, tags, metadata,
    /// version, ...) into the JSON text stored in the `extra` column.
    fn extra_param(meta: &TaskMeta) -> Result<String> {
        TaskMetaExtra::from_meta(meta)
            .to_json_string()
            .map_err(|e| BackendError::Serialization(format!("Failed to serialize extra: {e}")))
    }

    /// Parse the `extra` column's text (if present) and overlay it onto `meta`.
    fn apply_extra(meta: &mut TaskMeta, raw: Option<&str>) -> Result<()> {
        let extra = TaskMetaExtra::from_column(raw)
            .map_err(|e| BackendError::Serialization(format!("Failed to parse extra: {e}")))?;
        extra.apply_to(meta);
        Ok(())
    }

    /// Compute the `expires_at` parameter for `store_result`'s INSERT from
    /// the per-task-type TTL config: `Some(rfc3339 string)` when a TTL is
    /// configured for `task_name`, `None` otherwise.
    ///
    /// Folding this into the initial INSERT (see `store_result`) rather than
    /// a second, separate `set_expiration` UPDATE afterward removes a
    /// round-trip on the hottest write path and closes the partial-write
    /// window where a row existed with no `expires_at` if that second,
    /// independent statement failed after the first had already committed.
    fn ttl_expires_at_param(ttl_config: &TaskTtlConfig, task_name: &str) -> Result<Option<String>> {
        ttl_config
            .get_ttl(task_name)
            .map(|ttl| {
                let expires_at = Utc::now()
                    + chrono::Duration::from_std(ttl).map_err(|e| {
                        BackendError::Serialization(format!("Invalid TTL duration: {}", e))
                    })?;
                Ok(expires_at.to_rfc3339())
            })
            .transpose()
    }
}

#[cfg(feature = "postgres")]
#[async_trait]
impl ResultBackend for PostgresResultBackend {
    async fn store_result(&mut self, task_id: Uuid, meta: &TaskMeta) -> Result<()> {
        let (result_state, result_data, error_message, retry_count) = match &meta.result {
            TaskResult::Pending => ("pending", None, None, None),
            TaskResult::Started => ("started", None, None, None),
            TaskResult::Success(data) => ("success", Some(data.clone()), None, None),
            TaskResult::Failure(err) => ("failure", None, Some(err.clone()), None),
            TaskResult::Revoked => ("revoked", None, None, None),
            TaskResult::Retry(count) => ("retry", None, None, Some(*count as i32)),
        };

        let created_at_param = meta.created_at.to_rfc3339();
        let started_at_param = meta.started_at.map(|dt| dt.to_rfc3339());
        let completed_at_param = meta.completed_at.map(|dt| dt.to_rfc3339());
        let extra_param = Self::extra_param(meta)?;
        let expires_at_param = Self::ttl_expires_at_param(&self.ttl_config, &meta.task_name)?;

        self.conn
            .execute(
                r#"
                INSERT INTO celers_task_results
                    (task_id, task_name, result_state, result_data, error_message, retry_count,
                     created_at, started_at, completed_at, worker, extra, expires_at)
                VALUES ($1::text::uuid, $2, $3, $4, $5, $6,
                        $7::text::timestamptz, $8::text::timestamptz, $9::text::timestamptz, $10, $11,
                        $12::text::timestamptz)
                ON CONFLICT (task_id) DO UPDATE SET
                    result_state = EXCLUDED.result_state,
                    result_data = EXCLUDED.result_data,
                    error_message = EXCLUDED.error_message,
                    retry_count = EXCLUDED.retry_count,
                    started_at = EXCLUDED.started_at,
                    completed_at = EXCLUDED.completed_at,
                    worker = EXCLUDED.worker,
                    extra = EXCLUDED.extra,
                    -- Only overwrite an existing expires_at when THIS store
                    -- configured a TTL (EXCLUDED.expires_at IS NOT NULL);
                    -- otherwise preserve whatever was already there. Matches
                    -- the pre-fold behavior, where a separate set_expiration
                    -- UPDATE ran (and unconditionally overwrote) only when a
                    -- TTL was configured for this task_name.
                    expires_at = COALESCE(EXCLUDED.expires_at, celers_task_results.expires_at)
                "#,
                &[
                    &task_id.to_string(),
                    &meta.task_name,
                    &result_state,
                    &result_data.map(|v| json_param(&v)),
                    &error_message,
                    &retry_count,
                    &created_at_param,
                    &started_at_param,
                    &completed_at_param,
                    &meta.worker,
                    &extra_param,
                    &expires_at_param,
                ],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to store result: {}", e)))?;

        Ok(())
    }

    async fn get_result(&mut self, task_id: Uuid) -> Result<Option<TaskMeta>> {
        let rows = self
            .conn
            .query(
                r#"
                SELECT task_id, task_name, result_state, result_data, error_message,
                       retry_count, created_at, started_at, completed_at, worker, extra
                FROM celers_task_results
                WHERE task_id = $1::text::uuid
                "#,
                &[&task_id.to_string()],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to get result: {}", e)))?;

        match rows.into_iter().next() {
            Some(row) => {
                let result_state: String = row
                    .col("result_state")
                    .map_err(|e| BackendError::Connection(format!("Failed to get result: {e}")))?;
                let result_data = json_from_row(&row, "result_data")
                    .map_err(|e| BackendError::Connection(format!("Failed to get result: {e}")))?;
                let result_data = if result_data.is_null() {
                    None
                } else {
                    Some(result_data)
                };
                let error_message: Option<String> = row
                    .col("error_message")
                    .map_err(|e| BackendError::Connection(format!("Failed to get result: {e}")))?;
                let retry_count: Option<i32> = row
                    .col("retry_count")
                    .map_err(|e| BackendError::Connection(format!("Failed to get result: {e}")))?;
                let extra_raw: Option<String> = row
                    .col("extra")
                    .map_err(|e| BackendError::Connection(format!("Failed to get result: {e}")))?;

                let result = decode_result_state(
                    task_id,
                    &result_state,
                    result_data,
                    error_message,
                    retry_count,
                )?;

                let mut meta = TaskMeta {
                    task_id: uuid_from_row(&row, "task_id").map_err(|e| {
                        BackendError::Connection(format!("Failed to get result: {e}"))
                    })?,
                    task_name: row.col("task_name").map_err(|e| {
                        BackendError::Connection(format!("Failed to get result: {e}"))
                    })?,
                    result,
                    created_at: row.col("created_at").map_err(|e| {
                        BackendError::Connection(format!("Failed to get result: {e}"))
                    })?,
                    started_at: row.col("started_at").map_err(|e| {
                        BackendError::Connection(format!("Failed to get result: {e}"))
                    })?,
                    completed_at: row.col("completed_at").map_err(|e| {
                        BackendError::Connection(format!("Failed to get result: {e}"))
                    })?,
                    worker: row.col("worker").map_err(|e| {
                        BackendError::Connection(format!("Failed to get result: {e}"))
                    })?,
                    progress: None,
                    version: 0,
                    tags: Vec::new(),
                    metadata: std::collections::HashMap::new(),
                    worker_hostname: None,
                    runtime_ms: None,
                    memory_bytes: None,
                    retries: None,
                    queue: None,
                };
                Self::apply_extra(&mut meta, extra_raw.as_deref())?;

                Ok(Some(meta))
            }
            None => Ok(None),
        }
    }

    async fn delete_result(&mut self, task_id: Uuid) -> Result<()> {
        self.conn
            .execute(
                "DELETE FROM celers_task_results WHERE task_id = $1::text::uuid",
                &[&task_id.to_string()],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to delete result: {}", e)))?;

        Ok(())
    }

    async fn set_expiration(&mut self, task_id: Uuid, ttl: Duration) -> Result<()> {
        let expires_at = Utc::now()
            + chrono::Duration::from_std(ttl)
                .map_err(|e| BackendError::Serialization(format!("Invalid TTL duration: {}", e)))?;
        let expires_at_param = expires_at.to_rfc3339();

        self.conn
            .execute(
                "UPDATE celers_task_results SET expires_at = $1::text::timestamptz WHERE task_id = $2::text::uuid",
                &[&expires_at_param, &task_id.to_string()],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to set expiration: {}", e)))?;

        Ok(())
    }

    // `chord_init` is documented on the trait as a *create-or-reset*
    // primitive that always zeroes the completion counter — including when
    // `chord_id` already exists (the `chord_retry` path: same chord id,
    // freshly re-dispatched header tasks, counter must start back at 0 or
    // the barrier is already "complete" before any of the retried tasks
    // report in and the callback fires immediately). The ON CONFLICT branch
    // below previously omitted `completed` from the SET list entirely,
    // leaving a pre-existing row's counter untouched on conflict — correct
    // for a plain no-op re-init, but wrong for a reset, which is exactly
    // when this branch is taken. `completed = 0` (a literal, not
    // `EXCLUDED.completed`, since `state.completed` is never part of the
    // INSERT's own VALUES either — it's hardcoded to 0 there too) makes
    // this branch match the same reset semantics on both paths.
    //
    // Callers that want to persist a mutated state (cancellation, a new
    // callback, an updated timeout) WITHOUT losing in-flight progress must
    // use `chord_update_state` instead, which is the same upsert minus the
    // `completed` column entirely.
    async fn chord_init(&mut self, state: ChordState) -> Result<()> {
        let task_ids = serde_json::to_value(&state.task_ids)
            .map_err(|e| BackendError::Serialization(e.to_string()))?;
        let created_at_param = state.created_at.to_rfc3339();
        let timeout_secs_param = state.timeout.map(|d| d.as_secs() as i64);

        self.conn
            .execute(
                r#"
                INSERT INTO celers_chord_state (chord_id, total, completed, callback, task_ids, created_at, timeout_seconds, cancelled, cancellation_reason)
                VALUES ($1::text::uuid, $2, 0, $3, $4, $5::text::timestamptz, $6, $7, $8)
                ON CONFLICT (chord_id) DO UPDATE SET
                    total = EXCLUDED.total,
                    completed = 0,
                    callback = EXCLUDED.callback,
                    task_ids = EXCLUDED.task_ids,
                    timeout_seconds = EXCLUDED.timeout_seconds,
                    cancelled = EXCLUDED.cancelled,
                    cancellation_reason = EXCLUDED.cancellation_reason
                "#,
                &[
                    &state.chord_id.to_string(),
                    &(state.total as i64),
                    &state.callback,
                    &json_param(&task_ids),
                    &created_at_param,
                    &timeout_secs_param,
                    &state.cancelled,
                    &state.cancellation_reason,
                ],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to init chord: {}", e)))?;

        Ok(())
    }

    // The `chord_init` upsert minus `completed`, so persisting a state
    // mutation (cancellation, a new callback/timeout — see `chord_cancel`'s
    // default implementation, which reads-modify-writes through this
    // method) never resets tasks that already completed. `completed` is
    // absent from every clause here: on the INSERT branch (row genuinely
    // never existed) the column is left out of the column/VALUES lists
    // entirely so it takes the schema's own `DEFAULT 0` — the honest
    // starting value for a counter with nothing to preserve — and on the
    // ON CONFLICT branch it is simply not part of the SET list, so
    // Postgres leaves the existing value untouched. `created_at` is
    // likewise only ever written on the INSERT branch, matching
    // `chord_init`'s own behavior of never rewriting it on conflict.
    async fn chord_update_state(&mut self, state: ChordState) -> Result<()> {
        let task_ids = serde_json::to_value(&state.task_ids)
            .map_err(|e| BackendError::Serialization(e.to_string()))?;
        let created_at_param = state.created_at.to_rfc3339();
        let timeout_secs_param = state.timeout.map(|d| d.as_secs() as i64);

        self.conn
            .execute(
                r#"
                INSERT INTO celers_chord_state (chord_id, total, callback, task_ids, created_at, timeout_seconds, cancelled, cancellation_reason)
                VALUES ($1::text::uuid, $2, $3, $4, $5::text::timestamptz, $6, $7, $8)
                ON CONFLICT (chord_id) DO UPDATE SET
                    total = EXCLUDED.total,
                    callback = EXCLUDED.callback,
                    task_ids = EXCLUDED.task_ids,
                    timeout_seconds = EXCLUDED.timeout_seconds,
                    cancelled = EXCLUDED.cancelled,
                    cancellation_reason = EXCLUDED.cancellation_reason
                "#,
                &[
                    &state.chord_id.to_string(),
                    &(state.total as i64),
                    &state.callback,
                    &json_param(&task_ids),
                    &created_at_param,
                    &timeout_secs_param,
                    &state.cancelled,
                    &state.cancellation_reason,
                ],
            )
            .await
            .map_err(|e| {
                BackendError::Connection(format!("Failed to update chord state: {}", e))
            })?;

        Ok(())
    }

    async fn chord_complete_task(&mut self, chord_id: Uuid) -> Result<usize> {
        let rows = self
            .conn
            .query(
                "SELECT chord_increment_counter($1::text::uuid)",
                &[&chord_id.to_string()],
            )
            .await
            .map_err(|e| {
                BackendError::Connection(format!("Failed to increment chord counter: {}", e))
            })?;
        let row = rows.into_iter().next().ok_or_else(|| {
            BackendError::Connection("chord_increment_counter() returned no rows".to_string())
        })?;

        let count: i64 = row
            .col_idx(0)
            .map_err(|e| BackendError::Connection(format!("Failed to read chord counter: {e}")))?;
        Ok(count as usize)
    }

    async fn chord_get_state(&mut self, chord_id: Uuid) -> Result<Option<ChordState>> {
        let rows = self
            .conn
            .query(
                r#"
                SELECT chord_id, total, completed, callback, task_ids, created_at, timeout_seconds, cancelled, cancellation_reason
                FROM celers_chord_state
                WHERE chord_id = $1::text::uuid
                "#,
                &[&chord_id.to_string()],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to get chord state: {}", e)))?;

        match rows.into_iter().next() {
            Some(row) => {
                let task_ids_json = json_from_row(&row, "task_ids").map_err(|e| {
                    BackendError::Connection(format!("Failed to get chord state: {e}"))
                })?;
                let task_ids: Vec<Uuid> = serde_json::from_value(task_ids_json)
                    .map_err(|e| BackendError::Serialization(e.to_string()))?;

                let total: i64 = row.col("total").map_err(|e| {
                    BackendError::Connection(format!("Failed to get chord state: {e}"))
                })?;
                let completed: i64 = row.col("completed").map_err(|e| {
                    BackendError::Connection(format!("Failed to get chord state: {e}"))
                })?;
                let timeout_secs: Option<i64> = row.col("timeout_seconds").map_err(|e| {
                    BackendError::Connection(format!("Failed to get chord state: {e}"))
                })?;

                let state = ChordState {
                    chord_id: uuid_from_row(&row, "chord_id").map_err(|e| {
                        BackendError::Connection(format!("Failed to get chord state: {e}"))
                    })?,
                    total: total as usize,
                    completed: completed as usize,
                    callback: row.col("callback").map_err(|e| {
                        BackendError::Connection(format!("Failed to get chord state: {e}"))
                    })?,
                    // `celers_chord_state` has no column for this field —
                    // the same gap `retry_count`/`max_retries` below already
                    // accept. The Redis backend persists the whole
                    // `ChordState` as one serialized blob, so a new struct
                    // field round-trips for free there; this SQL backend
                    // maps one column per field explicitly and was never
                    // extended when the field was added. Needs a schema
                    // migration (a `callback_on_success_link TEXT` column on
                    // `celers_chord_state`) plus INSERT/SELECT wiring here —
                    // out of scope for this pass (migrations aren't owned by
                    // it); tracked as a followup.
                    callback_on_success_link: None,
                    task_ids,
                    created_at: row.col("created_at").map_err(|e| {
                        BackendError::Connection(format!("Failed to get chord state: {e}"))
                    })?,
                    timeout: timeout_secs.map(|s| std::time::Duration::from_secs(s as u64)),
                    cancelled: row.col("cancelled").map_err(|e| {
                        BackendError::Connection(format!("Failed to get chord state: {e}"))
                    })?,
                    cancellation_reason: row.col("cancellation_reason").map_err(|e| {
                        BackendError::Connection(format!("Failed to get chord state: {e}"))
                    })?,
                    retry_count: 0,
                    max_retries: None,
                };

                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    // Batch operations using transactions for atomic multi-row operations

    async fn store_results_batch(&mut self, results: &[(Uuid, TaskMeta)]) -> Result<()> {
        if results.is_empty() {
            return Ok(());
        }

        // A transaction must stay pinned to one physical connection for its
        // whole lifetime, so it is obtained via `PgConnPool::get()` (an
        // owned, round-robin-picked connection) rather than through the
        // pool's own `execute`/`query`, which pick a (possibly different)
        // connection per call. See `pg_pool.rs`'s module doc for why.
        let picked = self.conn.get().await;
        let mut tx = picked
            .transaction()
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to begin transaction: {}", e)))?;

        for (task_id, meta) in results {
            let (result_state, result_data, error_message, retry_count) = match &meta.result {
                TaskResult::Pending => ("pending", None, None, None),
                TaskResult::Started => ("started", None, None, None),
                TaskResult::Success(data) => ("success", Some(data.clone()), None, None),
                TaskResult::Failure(err) => ("failure", None, Some(err.clone()), None),
                TaskResult::Revoked => ("revoked", None, None, None),
                TaskResult::Retry(count) => ("retry", None, None, Some(*count as i32)),
            };
            let created_at_param = meta.created_at.to_rfc3339();
            let started_at_param = meta.started_at.map(|dt| dt.to_rfc3339());
            let completed_at_param = meta.completed_at.map(|dt| dt.to_rfc3339());
            let extra_param = Self::extra_param(meta)?;

            tx.execute(
                r#"
                INSERT INTO celers_task_results
                    (task_id, task_name, result_state, result_data, error_message, retry_count,
                     created_at, started_at, completed_at, worker, extra)
                VALUES ($1::text::uuid, $2, $3, $4, $5, $6,
                        $7::text::timestamptz, $8::text::timestamptz, $9::text::timestamptz, $10, $11)
                ON CONFLICT (task_id) DO UPDATE SET
                    result_state = EXCLUDED.result_state,
                    result_data = EXCLUDED.result_data,
                    error_message = EXCLUDED.error_message,
                    retry_count = EXCLUDED.retry_count,
                    started_at = EXCLUDED.started_at,
                    completed_at = EXCLUDED.completed_at,
                    worker = EXCLUDED.worker,
                    extra = EXCLUDED.extra
                "#,
                &[
                    &task_id.to_string(),
                    &meta.task_name,
                    &result_state,
                    &result_data.map(|v| json_param(&v)),
                    &error_message,
                    &retry_count,
                    &created_at_param,
                    &started_at_param,
                    &completed_at_param,
                    &meta.worker,
                    &extra_param,
                ],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to store result: {}", e)))?;
        }

        tx.commit().await.map_err(|e| {
            BackendError::Connection(format!("Failed to commit transaction: {}", e))
        })?;

        Ok(())
    }

    async fn get_results_batch(&mut self, task_ids: &[Uuid]) -> Result<Vec<Option<TaskMeta>>> {
        if task_ids.is_empty() {
            return Ok(Vec::new());
        }

        // oxisql-core has no `ToSqlValue` impl for `Vec<Uuid>` (or any
        // non-`Vec<u8>` `Vec<T>` — verified exhaustively against
        // oxisql-core's `traits.rs`), and building a `Value::TypedArray`
        // literal to bind against `= ANY($1)` would hit the exact same
        // Postgres binary/text wire-format hazard documented in
        // `row_ext.rs` for UUID scalars and `DateTime<Utc>` above. A
        // dynamically-built `IN (...)` with one `$n::text::uuid` placeholder
        // per element sidesteps the array-binding hazard entirely — every
        // value still goes through a parameter placeholder (bound as plain
        // `String`, cast to `uuid` server-side), only the *number* of
        // placeholders (a count, not a value) is spliced into the SQL text.
        let placeholders: String = (1..=task_ids.len())
            .map(|i| format!("${i}::text::uuid"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            r#"
            SELECT task_id, task_name, result_state, result_data, error_message,
                   retry_count, created_at, started_at, completed_at, worker, extra
            FROM celers_task_results
            WHERE task_id IN ({placeholders})
            "#
        );
        let params: Vec<String> = task_ids.iter().map(|id| id.to_string()).collect();
        let param_refs: Vec<&dyn ToSqlValue> =
            params.iter().map(|v| v as &dyn ToSqlValue).collect();

        let rows = self
            .conn
            .query(&sql, &param_refs)
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to get results: {}", e)))?;

        // Create a HashMap for O(1) lookup
        let mut results_map = std::collections::HashMap::new();
        for row in rows {
            let task_id = uuid_from_row(&row, "task_id")
                .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?;
            let result_state: String = row
                .col("result_state")
                .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?;
            let result_data = json_from_row(&row, "result_data")
                .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?;
            let result_data = if result_data.is_null() {
                None
            } else {
                Some(result_data)
            };
            let error_message: Option<String> = row
                .col("error_message")
                .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?;
            let retry_count: Option<i32> = row
                .col("retry_count")
                .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?;
            let extra_raw: Option<String> = row
                .col("extra")
                .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?;

            let result = decode_result_state(
                task_id,
                &result_state,
                result_data,
                error_message,
                retry_count,
            )?;

            let mut meta = TaskMeta {
                task_id,
                task_name: row
                    .col("task_name")
                    .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?,
                result,
                created_at: row
                    .col("created_at")
                    .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?,
                started_at: row
                    .col("started_at")
                    .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?,
                completed_at: row
                    .col("completed_at")
                    .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?,
                worker: row
                    .col("worker")
                    .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?,
                progress: None,
                version: 0,
                tags: Vec::new(),
                metadata: std::collections::HashMap::new(),
                worker_hostname: None,
                runtime_ms: None,
                memory_bytes: None,
                retries: None,
                queue: None,
            };
            Self::apply_extra(&mut meta, extra_raw.as_deref())?;

            results_map.insert(task_id, meta);
        }

        // Return results in the same order as input task_ids
        Ok(task_ids
            .iter()
            .map(|id| results_map.get(id).cloned())
            .collect())
    }

    async fn delete_results_batch(&mut self, task_ids: &[Uuid]) -> Result<()> {
        if task_ids.is_empty() {
            return Ok(());
        }

        // See the comment in `get_results_batch` for why a dynamically-built
        // `IN (...)` with one `$n::text::uuid` placeholder per element is
        // used instead of `= ANY($1)`.
        let placeholders: String = (1..=task_ids.len())
            .map(|i| format!("${i}::text::uuid"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("DELETE FROM celers_task_results WHERE task_id IN ({placeholders})");
        let params: Vec<String> = task_ids.iter().map(|id| id.to_string()).collect();
        let param_refs: Vec<&dyn ToSqlValue> =
            params.iter().map(|v| v as &dyn ToSqlValue).collect();

        self.conn
            .execute(&sql, &param_refs)
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to delete results: {}", e)))?;

        Ok(())
    }
}

// ══════════════════════════════════════════════════════════════════════════
// MySQL backend
// ══════════════════════════════════════════════════════════════════════════

/// MySQL result backend implementation
#[cfg(feature = "mysql")]
#[derive(Clone)]
pub struct MysqlResultBackend {
    conn: oxisql_mysql::MyConnection,
    ttl_config: TaskTtlConfig,
}

#[cfg(feature = "mysql")]
impl MysqlResultBackend {
    /// Create a new MySQL result backend
    ///
    /// # Arguments
    /// * `database_url` - MySQL connection string (e.g., "mysql://user:pass@localhost/db")
    ///
    /// `oxisql_mysql::MyConnection` is backed by a real `mysql_async::Pool`
    /// (confirmed against its own doc comments: "callers can share a
    /// `MyConnection` across async tasks without additional locking"), so
    /// unlike the Postgres backend this already pools connections and
    /// transparently discards/replaces a broken one on next checkout — no
    /// bespoke pooling wrapper is needed here. See [`Self::with_pool_size`]
    /// to configure the pool's connection limits explicitly.
    ///
    /// Results expire after [`ttl::SUCCESS`] (24 hours) by default, matching
    /// `RedisResultBackend::new` and Celery's `result_expires` — see
    /// `default_ttl_config`. Use [`Self::with_ttl_config`]`(TaskTtlConfig::new())`
    /// for permanent results, or `with_ttl_config` with a populated
    /// [`TaskTtlConfig`] for per-task-type expiry.
    pub async fn new(database_url: &str) -> Result<Self> {
        let tls = tls_mode::mysql_tls_mode_for_url(database_url)
            .map_err(|e| BackendError::Connection(format!("Failed to resolve TLS mode: {e}")))?;
        let conn = oxisql_mysql::MyConnection::connect(database_url, tls)
            .await
            .map_err(|e| {
                BackendError::Connection(format!("Failed to connect to database: {}", e))
            })?;

        Ok(Self {
            conn,
            ttl_config: default_ttl_config(),
        })
    }

    /// Create a new MySQL result backend with an explicit pool connection
    /// limit (`mysql_async`'s default is 10 when unset).
    ///
    /// Installs the same 24-hour default TTL as [`Self::new`] — see its doc
    /// comment.
    pub async fn with_pool_size(database_url: &str, pool_max: usize) -> Result<Self> {
        let tls = tls_mode::mysql_tls_mode_for_url(database_url)
            .map_err(|e| BackendError::Connection(format!("Failed to resolve TLS mode: {e}")))?;
        let url = url::Url::parse(database_url)
            .map_err(|e| BackendError::Connection(format!("Invalid database URL: {e}")))?;
        let conn = oxisql_mysql::MyConnectionBuilder::new()
            .host(url.host_str().unwrap_or("localhost"))
            .port(url.port().unwrap_or(3306))
            .user(url.username())
            .password(url.password().unwrap_or(""))
            .dbname(url.path().trim_start_matches('/'))
            .pool_max(pool_max.max(1))
            .tls_mode(tls)
            .connect()
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to connect to database: {e}")))?;

        Ok(Self {
            conn,
            ttl_config: default_ttl_config(),
        })
    }

    /// Configure per-task-type TTL
    pub fn with_ttl_config(mut self, config: TaskTtlConfig) -> Self {
        self.ttl_config = config;
        self
    }

    /// Get the TTL configuration
    pub fn ttl_config(&self) -> &TaskTtlConfig {
        &self.ttl_config
    }

    /// Get a mutable reference to the TTL configuration
    pub fn ttl_config_mut(&mut self) -> &mut TaskTtlConfig {
        &mut self.ttl_config
    }

    /// Run database migrations
    pub async fn migrate(&self) -> Result<()> {
        let migration_sql = include_str!("../migrations/001_init_mysql.sql");

        // Split on the `DELIMITER //` marker: everything before it is
        // ordinary `;`-terminated DDL, everything after is one or more
        // stored-procedure bodies (each internally `;`-terminated between
        // `DELIMITER //` and `DELIMITER ;`, executed as a single statement).
        let sections: Vec<&str> = migration_sql.split("DELIMITER //").collect();

        // Execute the main DDL section via a real statement splitter that
        // respects string/identifier quoting and comments (see
        // `sql_split.rs`) instead of a naive `str::split(';')`, which both
        // breaks on any `;` inside a string/identifier and can silently
        // *drop* a statement that follows a comment line within the same
        // fragment.
        if let Some(main_sql) = sections.first() {
            for statement in sql_split::split_sql_statements(main_sql) {
                self.conn
                    .execute(&statement, &[])
                    .await
                    .map_err(|e| BackendError::Connection(format!("Migration failed: {}", e)))?;
            }
        }

        // Execute stored procedures: each section between `DELIMITER //` and
        // the next `DELIMITER ;` is one procedure body, executed whole (its
        // internal `;`s are part of the procedure's `BEGIN...END` block, not
        // statement separators at this level).
        for &proc_section in sections.iter().skip(1) {
            if let Some(proc_sql) = proc_section.split("DELIMITER ;").next() {
                let trimmed = proc_sql.trim();
                if !trimmed.is_empty() {
                    self.conn.execute(trimmed, &[]).await.map_err(|e| {
                        BackendError::Connection(format!("Stored procedure creation failed: {}", e))
                    })?;
                }
            }
        }

        // Backward-compatible column add for databases migrated before the
        // `extra` column existed. A fresh install already has it (via the
        // CREATE TABLE above); MySQL 5.7 has no `ADD COLUMN IF NOT EXISTS`
        // (that syntax needs 8.0.29+/newer MariaDB), so portably checking
        // `information_schema.columns` first and only running the `ALTER`
        // when the column is actually missing is what keeps `migrate()` safe
        // to call repeatedly across MySQL/MariaDB versions.
        let has_extra_column = self
            .conn
            .query(
                "SELECT 1 FROM information_schema.columns \
                 WHERE table_schema = DATABASE() \
                   AND table_name = 'celers_task_results' \
                   AND column_name = 'extra'",
                &[],
            )
            .await
            .map_err(|e| {
                BackendError::Connection(format!("Failed to check for extra column: {}", e))
            })?;
        if has_extra_column.is_empty() {
            self.conn
                .execute("ALTER TABLE celers_task_results ADD COLUMN extra JSON", &[])
                .await
                .map_err(|e| {
                    BackendError::Connection(format!("Failed to add extra column: {}", e))
                })?;
        }

        Ok(())
    }

    /// Return an analytics helper bound to the same connection.
    pub fn analytics(&self) -> MysqlAnalytics {
        MysqlAnalytics::new(self.conn.clone())
    }

    /// Check whether the backend can currently reach the database.
    ///
    /// Issues a trivial `SELECT 1` (benefiting from the underlying
    /// `mysql_async::Pool`'s own checkout/discard-broken-connection
    /// behavior — see [`MysqlResultBackend::new`]'s doc comment) and reports
    /// whether it succeeded. Mirrors
    /// `celers_backend_redis::RedisResultBackend::health_check`'s contract
    /// for use in the same monitoring/readiness-probe role: `Ok(true)` means
    /// healthy, `Err(_)` means a connection or other error occurred.
    pub async fn health_check(&self) -> Result<bool> {
        match self.conn.query("SELECT 1", &[]).await {
            Ok(rows) => Ok(!rows.is_empty()),
            Err(e) => Err(BackendError::Connection(format!(
                "health_check query failed: {}",
                e
            ))),
        }
    }

    /// Get the underlying connection
    pub fn connection(&self) -> &oxisql_mysql::MyConnection {
        &self.conn
    }

    /// Serialize the extended [`TaskMeta`] fields (progress, tags, metadata,
    /// version, ...) into the JSON text stored in the `extra` column.
    fn extra_param(meta: &TaskMeta) -> Result<String> {
        TaskMetaExtra::from_meta(meta)
            .to_json_string()
            .map_err(|e| BackendError::Serialization(format!("Failed to serialize extra: {e}")))
    }

    /// Parse the `extra` column's text (if present) and overlay it onto `meta`.
    fn apply_extra(meta: &mut TaskMeta, raw: Option<&str>) -> Result<()> {
        let extra = TaskMetaExtra::from_column(raw)
            .map_err(|e| BackendError::Serialization(format!("Failed to parse extra: {e}")))?;
        extra.apply_to(meta);
        Ok(())
    }

    /// Compute the `expires_at` parameter for `store_result`'s INSERT from
    /// the per-task-type TTL config: `Some(MySQL DATETIME string)` when a TTL
    /// is configured for `task_name`, `None` otherwise. See
    /// `PostgresResultBackend::ttl_expires_at_param` for why this is folded
    /// into the initial INSERT rather than a second `set_expiration` UPDATE.
    fn ttl_expires_at_param(ttl_config: &TaskTtlConfig, task_name: &str) -> Result<Option<String>> {
        ttl_config
            .get_ttl(task_name)
            .map(|ttl| {
                let expires_at = Utc::now()
                    + chrono::Duration::from_std(ttl).map_err(|e| {
                        BackendError::Serialization(format!("Invalid TTL duration: {}", e))
                    })?;
                Ok(expires_at.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
            })
            .transpose()
    }
}

#[cfg(feature = "mysql")]
#[async_trait]
impl ResultBackend for MysqlResultBackend {
    async fn store_result(&mut self, task_id: Uuid, meta: &TaskMeta) -> Result<()> {
        let (result_state, result_data, error_message, retry_count) = match &meta.result {
            TaskResult::Pending => ("pending", None, None, None),
            TaskResult::Started => ("started", None, None, None),
            TaskResult::Success(data) => ("success", Some(data.clone()), None, None),
            TaskResult::Failure(err) => ("failure", None, Some(err.clone()), None),
            TaskResult::Revoked => ("revoked", None, None, None),
            TaskResult::Retry(count) => ("retry", None, None, Some(*count as i32)),
        };

        let result_data_str =
            result_data.map(|v| serde_json::to_string(&v).unwrap_or_else(|_| "null".to_string()));
        // MySQL DATETIME/TIMESTAMP grammar convention — see `row_ext.rs`'s
        // "DateTime<Utc> parameter convention (MySQL)" section for why
        // `.to_rfc3339()` is unsafe here and this format is the verified
        // MySQL-server-accepted one.
        let created_at_param = meta.created_at.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
        let started_at_param = meta
            .started_at
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string());
        let completed_at_param = meta
            .completed_at
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string());
        let extra_param = Self::extra_param(meta)?;
        let expires_at_param = Self::ttl_expires_at_param(&self.ttl_config, &meta.task_name)?;

        self.conn
            .execute(
                r#"
                INSERT INTO celers_task_results
                    (task_id, task_name, result_state, result_data, error_message, retry_count,
                     created_at, started_at, completed_at, worker, extra, expires_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    result_state = VALUES(result_state),
                    result_data = VALUES(result_data),
                    error_message = VALUES(error_message),
                    retry_count = VALUES(retry_count),
                    started_at = VALUES(started_at),
                    completed_at = VALUES(completed_at),
                    worker = VALUES(worker),
                    extra = VALUES(extra),
                    -- See PostgresResultBackend::store_result's ON CONFLICT
                    -- clause: only overwrite an existing expires_at when
                    -- THIS store configured a TTL, otherwise preserve it.
                    expires_at = COALESCE(VALUES(expires_at), expires_at)
                "#,
                &[
                    &task_id.to_string(),
                    &meta.task_name,
                    &result_state,
                    &result_data_str,
                    &error_message,
                    &retry_count,
                    &created_at_param,
                    &started_at_param,
                    &completed_at_param,
                    &meta.worker,
                    &extra_param,
                    &expires_at_param,
                ],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to store result: {}", e)))?;

        Ok(())
    }

    async fn get_result(&mut self, task_id: Uuid) -> Result<Option<TaskMeta>> {
        let rows = self
            .conn
            .query(
                r#"
                SELECT task_id, task_name, result_state, result_data, error_message,
                       retry_count, created_at, started_at, completed_at, worker, extra
                FROM celers_task_results
                WHERE task_id = ?
                "#,
                &[&task_id.to_string()],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to get result: {}", e)))?;

        match rows.into_iter().next() {
            Some(row) => {
                let task_id_str: String = row
                    .col("task_id")
                    .map_err(|e| BackendError::Connection(format!("Failed to get result: {e}")))?;
                let result_state: String = row
                    .col("result_state")
                    .map_err(|e| BackendError::Connection(format!("Failed to get result: {e}")))?;
                let result_data_str: Option<String> = row
                    .col("result_data")
                    .map_err(|e| BackendError::Connection(format!("Failed to get result: {e}")))?;
                let error_message: Option<String> = row
                    .col("error_message")
                    .map_err(|e| BackendError::Connection(format!("Failed to get result: {e}")))?;
                let retry_count: Option<i32> = row
                    .col("retry_count")
                    .map_err(|e| BackendError::Connection(format!("Failed to get result: {e}")))?;
                let extra_raw: Option<String> = row
                    .col("extra")
                    .map_err(|e| BackendError::Connection(format!("Failed to get result: {e}")))?;

                let result_data = result_data_str.and_then(|s| serde_json::from_str(&s).ok());

                let parsed_task_id = Uuid::parse_str(&task_id_str)
                    .map_err(|e| BackendError::Serialization(e.to_string()))?;
                let result = decode_result_state(
                    parsed_task_id,
                    &result_state,
                    result_data,
                    error_message,
                    retry_count,
                )?;

                let mut meta = TaskMeta {
                    task_id: parsed_task_id,
                    task_name: row.col("task_name").map_err(|e| {
                        BackendError::Connection(format!("Failed to get result: {e}"))
                    })?,
                    result,
                    created_at: row.col::<DateTime<Utc>>("created_at").map_err(|e| {
                        BackendError::Connection(format!("Failed to get result: {e}"))
                    })?,
                    started_at: row.col("started_at").map_err(|e| {
                        BackendError::Connection(format!("Failed to get result: {e}"))
                    })?,
                    completed_at: row.col("completed_at").map_err(|e| {
                        BackendError::Connection(format!("Failed to get result: {e}"))
                    })?,
                    worker: row.col("worker").map_err(|e| {
                        BackendError::Connection(format!("Failed to get result: {e}"))
                    })?,
                    progress: None,
                    version: 0,
                    tags: Vec::new(),
                    metadata: std::collections::HashMap::new(),
                    worker_hostname: None,
                    runtime_ms: None,
                    memory_bytes: None,
                    retries: None,
                    queue: None,
                };
                Self::apply_extra(&mut meta, extra_raw.as_deref())?;

                Ok(Some(meta))
            }
            None => Ok(None),
        }
    }

    async fn delete_result(&mut self, task_id: Uuid) -> Result<()> {
        self.conn
            .execute(
                "DELETE FROM celers_task_results WHERE task_id = ?",
                &[&task_id.to_string()],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to delete result: {}", e)))?;

        Ok(())
    }

    async fn set_expiration(&mut self, task_id: Uuid, ttl: Duration) -> Result<()> {
        let expires_at = Utc::now()
            + chrono::Duration::from_std(ttl)
                .map_err(|e| BackendError::Serialization(format!("Invalid TTL duration: {}", e)))?;
        let expires_at_param = expires_at.format("%Y-%m-%d %H:%M:%S%.6f").to_string();

        self.conn
            .execute(
                "UPDATE celers_task_results SET expires_at = ? WHERE task_id = ?",
                &[&expires_at_param, &task_id.to_string()],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to set expiration: {}", e)))?;

        Ok(())
    }

    // See the identical comment on
    // `PostgresResultBackend::chord_init`: this is a *create-or-reset*
    // primitive that must zero `completed` even on conflict, or a
    // `chord_retry` of an already-terminal chord leaves the counter at or
    // above `total` and the callback fires before any retried task reports
    // in. `completed = 0` (a literal, matching the INSERT branch's own
    // hardcoded `0`, not `VALUES(completed)`) makes the DUPLICATE KEY branch
    // agree with the INSERT branch. Callers that want to persist a state
    // mutation without losing progress must use `chord_update_state`
    // instead.
    async fn chord_init(&mut self, state: ChordState) -> Result<()> {
        let task_ids = serde_json::to_string(&state.task_ids)
            .map_err(|e| BackendError::Serialization(e.to_string()))?;

        let created_at_param = state.created_at.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
        let timeout_secs_param = state.timeout.map(|d| d.as_secs() as i64);

        self.conn
            .execute(
                r#"
                INSERT INTO celers_chord_state (chord_id, total, completed, callback, task_ids, created_at, timeout_seconds, cancelled, cancellation_reason)
                VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    total = VALUES(total),
                    completed = 0,
                    callback = VALUES(callback),
                    task_ids = VALUES(task_ids),
                    timeout_seconds = VALUES(timeout_seconds),
                    cancelled = VALUES(cancelled),
                    cancellation_reason = VALUES(cancellation_reason)
                "#,
                &[
                    &state.chord_id.to_string(),
                    &(state.total as i64),
                    &state.callback,
                    &task_ids,
                    &created_at_param,
                    &timeout_secs_param,
                    &state.cancelled,
                    &state.cancellation_reason,
                ],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to init chord: {}", e)))?;

        Ok(())
    }

    // The `chord_init` upsert minus `completed`: see
    // `PostgresResultBackend::chord_update_state` for why the column is
    // absent from every clause rather than pinned to a value — omitted from
    // the INSERT branch's column/VALUES lists (the fresh-row case takes the
    // schema's own `DEFAULT 0`) and from the DUPLICATE KEY branch's SET
    // list (the existing-row case leaves it untouched, preserving in-flight
    // progress). Used by `chord_cancel`'s default implementation so
    // cancelling a chord never un-completes tasks that already reported in.
    async fn chord_update_state(&mut self, state: ChordState) -> Result<()> {
        let task_ids = serde_json::to_string(&state.task_ids)
            .map_err(|e| BackendError::Serialization(e.to_string()))?;

        let created_at_param = state.created_at.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
        let timeout_secs_param = state.timeout.map(|d| d.as_secs() as i64);

        self.conn
            .execute(
                r#"
                INSERT INTO celers_chord_state (chord_id, total, callback, task_ids, created_at, timeout_seconds, cancelled, cancellation_reason)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    total = VALUES(total),
                    callback = VALUES(callback),
                    task_ids = VALUES(task_ids),
                    timeout_seconds = VALUES(timeout_seconds),
                    cancelled = VALUES(cancelled),
                    cancellation_reason = VALUES(cancellation_reason)
                "#,
                &[
                    &state.chord_id.to_string(),
                    &(state.total as i64),
                    &state.callback,
                    &task_ids,
                    &created_at_param,
                    &timeout_secs_param,
                    &state.cancelled,
                    &state.cancellation_reason,
                ],
            )
            .await
            .map_err(|e| {
                BackendError::Connection(format!("Failed to update chord state: {}", e))
            })?;

        Ok(())
    }

    async fn chord_complete_task(&mut self, chord_id: Uuid) -> Result<usize> {
        // Atomic increment-and-read: both statements MUST run on the same
        // physical connection, so this uses an explicit transaction (which
        // `MyTransaction` pins to one connection borrowed from the pool for
        // its whole lifetime — see `oxisql-mysql`'s `connection.rs` doc
        // comment) rather than `self.conn.execute`/`.query`, each of which
        // independently checks a connection out of `mysql_async::Pool` and
        // could land on two different physical connections.
        //
        // `LAST_INSERT_ID(expr)` is MySQL's own documented idiom for a
        // session-scoped atomic increment-and-read (see the MySQL Reference
        // Manual's "Obtaining the Unique ID": `UPDATE sequence SET
        // id=LAST_INSERT_ID(id+1); SELECT LAST_INSERT_ID();`): the UPDATE's
        // row-level lock serializes concurrent increments on the same row,
        // and `LAST_INSERT_ID()` returns exactly the value *this session's*
        // UPDATE computed, immune to another connection's concurrent
        // increment landing in between (unlike the previous separate
        // UPDATE-then-SELECT, where two concurrent callers could each read
        // back the OTHER's incremented value and both observe the terminal
        // count — firing the chord callback twice).
        let mut tx =
            self.conn.transaction().await.map_err(|e| {
                BackendError::Connection(format!("Failed to begin transaction: {}", e))
            })?;

        let affected = tx
            .execute(
                "UPDATE celers_chord_state SET completed = LAST_INSERT_ID(completed + 1) WHERE chord_id = ?",
                &[&chord_id.to_string()],
            )
            .await
            .map_err(|e| {
                BackendError::Connection(format!("Failed to increment chord counter: {}", e))
            })?;

        if affected == 0 {
            // No such chord row: roll back (nothing to commit) and report a
            // meaningful not-found error rather than the previous behavior
            // of a generic "chord counter query returned no rows" — same
            // spirit as the caller-visible error identifying the missing id.
            tx.rollback().await.map_err(|e| {
                BackendError::Connection(format!("Failed to roll back transaction: {}", e))
            })?;
            return Err(BackendError::NotFound(chord_id));
        }

        let rows = tx
            .query("SELECT LAST_INSERT_ID() AS completed", &[])
            .await
            .map_err(|e| {
                BackendError::Connection(format!("Failed to read chord counter: {}", e))
            })?;
        let row = rows.into_iter().next().ok_or_else(|| {
            BackendError::Connection("LAST_INSERT_ID() query returned no rows".to_string())
        })?;
        let count: i64 = row
            .col("completed")
            .map_err(|e| BackendError::Connection(format!("Failed to read chord counter: {e}")))?;

        tx.commit().await.map_err(|e| {
            BackendError::Connection(format!("Failed to commit transaction: {}", e))
        })?;

        Ok(count as usize)
    }

    async fn chord_get_state(&mut self, chord_id: Uuid) -> Result<Option<ChordState>> {
        let rows = self
            .conn
            .query(
                r#"
                SELECT chord_id, total, completed, callback, task_ids, created_at, timeout_seconds, cancelled, cancellation_reason
                FROM celers_chord_state
                WHERE chord_id = ?
                "#,
                &[&chord_id.to_string()],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to get chord state: {}", e)))?;

        match rows.into_iter().next() {
            Some(row) => {
                let chord_id_str: String = row.col("chord_id").map_err(|e| {
                    BackendError::Connection(format!("Failed to get chord state: {e}"))
                })?;
                let task_ids_str: String = row.col("task_ids").map_err(|e| {
                    BackendError::Connection(format!("Failed to get chord state: {e}"))
                })?;
                let task_ids: Vec<Uuid> = serde_json::from_str(&task_ids_str)
                    .map_err(|e| BackendError::Serialization(e.to_string()))?;

                let total: i64 = row.col("total").map_err(|e| {
                    BackendError::Connection(format!("Failed to get chord state: {e}"))
                })?;
                let completed: i64 = row.col("completed").map_err(|e| {
                    BackendError::Connection(format!("Failed to get chord state: {e}"))
                })?;
                let timeout_secs: Option<i64> = row.col("timeout_seconds").map_err(|e| {
                    BackendError::Connection(format!("Failed to get chord state: {e}"))
                })?;

                let state = ChordState {
                    chord_id: Uuid::parse_str(&chord_id_str)
                        .map_err(|e| BackendError::Serialization(e.to_string()))?,
                    total: total as usize,
                    completed: completed as usize,
                    callback: row.col("callback").map_err(|e| {
                        BackendError::Connection(format!("Failed to get chord state: {e}"))
                    })?,
                    // See the identical gap called out in
                    // `PostgresResultBackend::chord_get_state`: no column
                    // for this field on `celers_chord_state`, same as
                    // `retry_count`/`max_retries` below.
                    callback_on_success_link: None,
                    task_ids,
                    created_at: row.col("created_at").map_err(|e| {
                        BackendError::Connection(format!("Failed to get chord state: {e}"))
                    })?,
                    timeout: timeout_secs.map(|s| std::time::Duration::from_secs(s as u64)),
                    cancelled: row.col("cancelled").map_err(|e| {
                        BackendError::Connection(format!("Failed to get chord state: {e}"))
                    })?,
                    cancellation_reason: row.col("cancellation_reason").map_err(|e| {
                        BackendError::Connection(format!("Failed to get chord state: {e}"))
                    })?,
                    retry_count: 0,
                    max_retries: None,
                };

                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    // Batch operations using transactions for atomic multi-row operations

    async fn store_results_batch(&mut self, results: &[(Uuid, TaskMeta)]) -> Result<()> {
        if results.is_empty() {
            return Ok(());
        }

        let mut tx =
            self.conn.transaction().await.map_err(|e| {
                BackendError::Connection(format!("Failed to begin transaction: {}", e))
            })?;

        for (task_id, meta) in results {
            let (result_state, result_data, error_message, retry_count) = match &meta.result {
                TaskResult::Pending => ("pending", None, None, None),
                TaskResult::Started => ("started", None, None, None),
                TaskResult::Success(data) => ("success", Some(data.clone()), None, None),
                TaskResult::Failure(err) => ("failure", None, Some(err.clone()), None),
                TaskResult::Revoked => ("revoked", None, None, None),
                TaskResult::Retry(count) => ("retry", None, None, Some(*count as i32)),
            };
            let result_data_str = result_data
                .map(|v| serde_json::to_string(&v).unwrap_or_else(|_| "null".to_string()));
            let created_at_param = meta.created_at.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
            let started_at_param = meta
                .started_at
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string());
            let completed_at_param = meta
                .completed_at
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string());
            let extra_param = Self::extra_param(meta)?;

            tx.execute(
                r#"
                INSERT INTO celers_task_results
                    (task_id, task_name, result_state, result_data, error_message, retry_count,
                     created_at, started_at, completed_at, worker, extra)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    result_state = VALUES(result_state),
                    result_data = VALUES(result_data),
                    error_message = VALUES(error_message),
                    retry_count = VALUES(retry_count),
                    started_at = VALUES(started_at),
                    completed_at = VALUES(completed_at),
                    worker = VALUES(worker),
                    extra = VALUES(extra)
                "#,
                &[
                    &task_id.to_string(),
                    &meta.task_name,
                    &result_state,
                    &result_data_str,
                    &error_message,
                    &retry_count,
                    &created_at_param,
                    &started_at_param,
                    &completed_at_param,
                    &meta.worker,
                    &extra_param,
                ],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to store result: {}", e)))?;
        }

        tx.commit().await.map_err(|e| {
            BackendError::Connection(format!("Failed to commit transaction: {}", e))
        })?;

        Ok(())
    }

    async fn get_results_batch(&mut self, task_ids: &[Uuid]) -> Result<Vec<Option<TaskMeta>>> {
        if task_ids.is_empty() {
            return Ok(Vec::new());
        }

        // MySQL requires IN clause with placeholders
        let placeholders = task_ids.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let query_str = format!(
            r#"
            SELECT task_id, task_name, result_state, result_data, error_message,
                   retry_count, created_at, started_at, completed_at, worker, extra
            FROM celers_task_results
            WHERE task_id IN ({})
            "#,
            placeholders
        );
        // oxisql_mysql::MyConnection::execute/query take `&str` directly —
        // sqlx's `AssertSqlSafe` opt-out wrapper has no equivalent (and none
        // is needed): only the placeholder *count* (never a value) was
        // spliced into `query_str` above, matching the exact same
        // static-fragment-only discipline `sqlx::AssertSqlSafe` was
        // previously asserting.
        let id_params: Vec<String> = task_ids.iter().map(|id| id.to_string()).collect();
        let param_refs: Vec<&dyn ToSqlValue> =
            id_params.iter().map(|s| s as &dyn ToSqlValue).collect();

        let rows = self
            .conn
            .query(&query_str, &param_refs)
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to get results: {}", e)))?;

        // Create a HashMap for O(1) lookup
        let mut results_map = std::collections::HashMap::new();
        for row in rows {
            let task_id_str: String = row
                .col("task_id")
                .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?;
            let task_id = Uuid::parse_str(&task_id_str)
                .map_err(|e| BackendError::Serialization(e.to_string()))?;
            let result_state: String = row
                .col("result_state")
                .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?;
            let result_data_str: Option<String> = row
                .col("result_data")
                .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?;
            let result_data = result_data_str.and_then(|s| serde_json::from_str(&s).ok());
            let error_message: Option<String> = row
                .col("error_message")
                .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?;
            let retry_count: Option<i32> = row
                .col("retry_count")
                .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?;
            let extra_raw: Option<String> = row
                .col("extra")
                .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?;

            let result = decode_result_state(
                task_id,
                &result_state,
                result_data,
                error_message,
                retry_count,
            )?;

            let mut meta = TaskMeta {
                task_id,
                task_name: row
                    .col("task_name")
                    .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?,
                result,
                created_at: row
                    .col("created_at")
                    .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?,
                started_at: row
                    .col("started_at")
                    .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?,
                completed_at: row
                    .col("completed_at")
                    .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?,
                worker: row
                    .col("worker")
                    .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?,
                progress: None,
                version: 0,
                tags: Vec::new(),
                metadata: std::collections::HashMap::new(),
                worker_hostname: None,
                runtime_ms: None,
                memory_bytes: None,
                retries: None,
                queue: None,
            };
            Self::apply_extra(&mut meta, extra_raw.as_deref())?;

            results_map.insert(task_id, meta);
        }

        // Return results in the same order as input task_ids
        Ok(task_ids
            .iter()
            .map(|id| results_map.get(id).cloned())
            .collect())
    }

    async fn delete_results_batch(&mut self, task_ids: &[Uuid]) -> Result<()> {
        if task_ids.is_empty() {
            return Ok(());
        }

        // MySQL requires IN clause with placeholders
        let placeholders = task_ids.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let query_str = format!(
            "DELETE FROM celers_task_results WHERE task_id IN ({})",
            placeholders
        );
        let id_params: Vec<String> = task_ids.iter().map(|id| id.to_string()).collect();
        let param_refs: Vec<&dyn ToSqlValue> =
            id_params.iter().map(|s| s as &dyn ToSqlValue).collect();

        self.conn
            .execute(&query_str, &param_refs)
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to delete results: {}", e)))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "postgres")]
    #[tokio::test]
    #[ignore] // Requires PostgreSQL running
    async fn test_postgres_backend_creation() {
        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost/celers_test".to_string());

        let backend = PostgresResultBackend::new(&database_url).await;
        assert!(backend.is_ok());
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    #[ignore] // Requires PostgreSQL running
    async fn test_postgres_store_get_delete_roundtrip_uuid_binding() {
        // Regression test for the UUID binary-wire-format hazard: every
        // Postgres call site in this file binds `task_id.to_string()` with a
        // `$n::text::uuid` cast rather than a bare `Value::Uuid`. This
        // exercises every fixed call site transitively (store_result's
        // INSERT, get_result's SELECT, delete_result's DELETE) against a
        // live server, where the bug would surface as a connection/protocol
        // error rather than a Rust-level type error.
        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost/celers_test".to_string());
        let mut backend = PostgresResultBackend::new(&database_url)
            .await
            .expect("connect");
        backend.migrate().await.expect("migrate");

        let task_id = Uuid::new_v4();
        let mut meta = TaskMeta::new(task_id, "uuid_roundtrip_test".to_string());
        meta.result = TaskResult::Success(serde_json::json!({"ok": true}));
        meta.tags = vec!["t1".to_string()];
        meta.version = 3;

        backend.store_result(task_id, &meta).await.expect("store");
        let fetched = backend
            .get_result(task_id)
            .await
            .expect("get")
            .expect("row exists");
        assert_eq!(fetched.task_id, task_id);
        assert_eq!(fetched.tags, vec!["t1".to_string()]);
        assert_eq!(fetched.version, 3);
        assert!(matches!(fetched.result, TaskResult::Success(_)));

        backend.delete_result(task_id).await.expect("delete");
        assert!(backend
            .get_result(task_id)
            .await
            .expect("get after delete")
            .is_none());
    }

    /// Build a fresh, non-cancelled `ChordState` with `TOTAL` header tasks
    /// and no progress yet, shared by the Postgres/MySQL chord reset and
    /// update-state tests below.
    #[cfg(any(feature = "postgres", feature = "mysql"))]
    fn fresh_chord_state(chord_id: Uuid, total: usize) -> ChordState {
        ChordState {
            chord_id,
            total,
            completed: 0,
            callback: Some("noop".to_string()),
            callback_on_success_link: None,
            task_ids: (0..total).map(|_| Uuid::new_v4()).collect(),
            created_at: Utc::now(),
            timeout: None,
            cancelled: false,
            cancellation_reason: None,
            retry_count: 0,
            max_retries: None,
        }
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    #[ignore] // Requires PostgreSQL running
    async fn test_postgres_chord_init_resets_completed_counter_on_conflict() {
        // Regression test for the chord *reset* bug: `chord_init`'s
        // ON CONFLICT branch previously omitted `completed` from its SET
        // list, so re-running `chord_init` for the same `chord_id` — the
        // `chord_retry` path — left a terminal counter in place: the
        // barrier looked already-complete before any of the retried tasks
        // reported in, and the callback would fire immediately.
        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost/celers_test".to_string());
        let mut backend = PostgresResultBackend::new(&database_url)
            .await
            .expect("connect");
        backend.migrate().await.expect("migrate");

        const TOTAL: usize = 3;
        let chord_id = Uuid::new_v4();
        let state = fresh_chord_state(chord_id, TOTAL);
        backend.chord_init(state.clone()).await.expect("chord_init");

        for _ in 0..TOTAL {
            backend
                .chord_complete_task(chord_id)
                .await
                .expect("chord_complete_task");
        }
        let terminal = backend
            .chord_get_state(chord_id)
            .await
            .expect("get")
            .expect("state exists");
        assert_eq!(
            terminal.completed, TOTAL,
            "sanity check: counter must be terminal before the reset"
        );

        // Re-run chord_init for the SAME chord_id — this is what
        // `chord_retry` does. The counter must come back to 0, not just on
        // first insert.
        backend.chord_init(state).await.expect("chord_init (reset)");
        let reset = backend
            .chord_get_state(chord_id)
            .await
            .expect("get")
            .expect("state exists");
        assert_eq!(
            reset.completed, 0,
            "chord_init must reset the completion counter on conflict, matching its \
             documented create-or-reset contract, not leave a stale terminal count in place"
        );
    }

    #[cfg(feature = "postgres")]
    #[tokio::test]
    #[ignore] // Requires PostgreSQL running
    async fn test_postgres_chord_cancel_preserves_completed_counter() {
        // Regression test for `chord_update_state` (used by `chord_cancel`'s
        // default trait implementation): persisting a state mutation must
        // never reset tasks that already completed, unlike `chord_init`'s
        // create-or-reset semantics. Before `chord_update_state` was
        // overridden, this fell back to `chord_init`, which (once fixed to
        // reset `completed` on conflict) would have made cancellation
        // un-complete a chord's in-flight progress.
        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost/celers_test".to_string());
        let mut backend = PostgresResultBackend::new(&database_url)
            .await
            .expect("connect");
        backend.migrate().await.expect("migrate");

        const TOTAL: usize = 3;
        let chord_id = Uuid::new_v4();
        let state = fresh_chord_state(chord_id, TOTAL);
        let task_ids = state.task_ids.clone();
        backend.chord_init(state).await.expect("chord_init");
        backend
            .chord_complete_task(chord_id)
            .await
            .expect("chord_complete_task");

        backend
            .chord_cancel(chord_id, Some("test cancel".to_string()))
            .await
            .expect("chord_cancel");

        let after = backend
            .chord_get_state(chord_id)
            .await
            .expect("get")
            .expect("state exists");
        assert_eq!(
            after.completed, 1,
            "chord_update_state must not reset in-flight progress"
        );
        assert!(
            after.cancelled,
            "chord_cancel must mark the chord cancelled"
        );
        assert_eq!(after.cancellation_reason.as_deref(), Some("test cancel"));
        assert_eq!(
            after.callback.as_deref(),
            Some("noop"),
            "chord_update_state must preserve the callback, not just the counter"
        );
        assert_eq!(
            after.task_ids, task_ids,
            "chord_update_state must preserve task_ids"
        );
    }

    #[cfg(feature = "mysql")]
    #[tokio::test]
    #[ignore] // Requires MySQL running
    async fn test_mysql_backend_creation() {
        let database_url = std::env::var("MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@localhost/celers_test".to_string());

        let backend = MysqlResultBackend::new(&database_url).await;
        assert!(backend.is_ok());
    }

    #[cfg(feature = "mysql")]
    #[tokio::test]
    #[ignore] // Requires MySQL running
    async fn test_mysql_chord_complete_task_fires_callback_exactly_once_concurrently() {
        // Regression test for the non-atomic UPDATE-then-SELECT chord
        // counter: spawns `total` tasks all completing the SAME chord
        // concurrently and asserts exactly one of them observes
        // `count >= total` (the condition `celers-worker`'s `workflows.rs`
        // gates the chord callback dispatch on). Before the fix, the
        // separate UPDATE and SELECT statements let two concurrent callers
        // both read back the terminal count, firing the callback twice.
        let database_url = std::env::var("MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@localhost/celers_test".to_string());
        let backend = MysqlResultBackend::new(&database_url)
            .await
            .expect("connect");
        backend.migrate().await.expect("migrate");

        let chord_id = Uuid::new_v4();
        const TOTAL: usize = 8;
        let state = ChordState {
            chord_id,
            total: TOTAL,
            completed: 0,
            callback: Some("noop".to_string()),
            callback_on_success_link: None,
            task_ids: (0..TOTAL).map(|_| Uuid::new_v4()).collect(),
            created_at: Utc::now(),
            timeout: None,
            cancelled: false,
            cancellation_reason: None,
            retry_count: 0,
            max_retries: None,
        };
        let mut init_backend = backend.clone();
        init_backend.chord_init(state).await.expect("chord_init");

        let mut handles = Vec::with_capacity(TOTAL);
        for _ in 0..TOTAL {
            let mut worker_backend = backend.clone();
            handles.push(tokio::spawn(async move {
                worker_backend
                    .chord_complete_task(chord_id)
                    .await
                    .expect("chord_complete_task")
            }));
        }

        let mut terminal_observations = 0usize;
        for handle in handles {
            let count = handle.await.expect("task join");
            if count >= TOTAL {
                terminal_observations += 1;
            }
        }

        assert_eq!(
            terminal_observations, 1,
            "exactly one concurrent caller must observe the terminal chord count \
             (Celery chord semantics: the callback fires exactly once)"
        );
    }

    #[cfg(feature = "mysql")]
    #[tokio::test]
    #[ignore] // Requires MySQL running
    async fn test_mysql_chord_init_resets_completed_counter_on_conflict() {
        // MySQL counterpart of
        // `test_postgres_chord_init_resets_completed_counter_on_conflict` —
        // see its comment. The `ON DUPLICATE KEY UPDATE` branch had the same
        // gap as Postgres's `ON CONFLICT DO UPDATE`.
        let database_url = std::env::var("MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@localhost/celers_test".to_string());
        let mut backend = MysqlResultBackend::new(&database_url)
            .await
            .expect("connect");
        backend.migrate().await.expect("migrate");

        const TOTAL: usize = 3;
        let chord_id = Uuid::new_v4();
        let state = fresh_chord_state(chord_id, TOTAL);
        backend.chord_init(state.clone()).await.expect("chord_init");

        for _ in 0..TOTAL {
            backend
                .chord_complete_task(chord_id)
                .await
                .expect("chord_complete_task");
        }
        let terminal = backend
            .chord_get_state(chord_id)
            .await
            .expect("get")
            .expect("state exists");
        assert_eq!(
            terminal.completed, TOTAL,
            "sanity check: counter must be terminal before the reset"
        );

        backend.chord_init(state).await.expect("chord_init (reset)");
        let reset = backend
            .chord_get_state(chord_id)
            .await
            .expect("get")
            .expect("state exists");
        assert_eq!(
            reset.completed, 0,
            "chord_init must reset the completion counter on conflict, matching its \
             documented create-or-reset contract, not leave a stale terminal count in place"
        );
    }

    #[cfg(feature = "mysql")]
    #[tokio::test]
    #[ignore] // Requires MySQL running
    async fn test_mysql_chord_cancel_preserves_completed_counter() {
        // MySQL counterpart of
        // `test_postgres_chord_cancel_preserves_completed_counter` — see its
        // comment.
        let database_url = std::env::var("MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@localhost/celers_test".to_string());
        let mut backend = MysqlResultBackend::new(&database_url)
            .await
            .expect("connect");
        backend.migrate().await.expect("migrate");

        const TOTAL: usize = 3;
        let chord_id = Uuid::new_v4();
        let state = fresh_chord_state(chord_id, TOTAL);
        let task_ids = state.task_ids.clone();
        backend.chord_init(state).await.expect("chord_init");
        backend
            .chord_complete_task(chord_id)
            .await
            .expect("chord_complete_task");

        backend
            .chord_cancel(chord_id, Some("test cancel".to_string()))
            .await
            .expect("chord_cancel");

        let after = backend
            .chord_get_state(chord_id)
            .await
            .expect("get")
            .expect("state exists");
        assert_eq!(
            after.completed, 1,
            "chord_update_state must not reset in-flight progress"
        );
        assert!(
            after.cancelled,
            "chord_cancel must mark the chord cancelled"
        );
        assert_eq!(after.cancellation_reason.as_deref(), Some("test cancel"));
        assert_eq!(
            after.callback.as_deref(),
            Some("noop"),
            "chord_update_state must preserve the callback, not just the counter"
        );
        assert_eq!(
            after.task_ids, task_ids,
            "chord_update_state must preserve task_ids"
        );
    }

    #[test]
    fn decode_result_state_maps_every_known_state() {
        let task_id = Uuid::new_v4();
        assert!(matches!(
            decode_result_state(task_id, "pending", None, None, None).unwrap(),
            TaskResult::Pending
        ));
        assert!(matches!(
            decode_result_state(task_id, "started", None, None, None).unwrap(),
            TaskResult::Started
        ));
        assert!(matches!(
            decode_result_state(task_id, "success", Some(json!({"a":1})), None, None).unwrap(),
            TaskResult::Success(_)
        ));
        assert!(matches!(
            decode_result_state(task_id, "revoked", None, None, None).unwrap(),
            TaskResult::Revoked
        ));
        match decode_result_state(task_id, "retry", None, None, Some(3)).unwrap() {
            TaskResult::Retry(n) => assert_eq!(n, 3),
            other => panic!("expected Retry, got {other:?}"),
        }
    }

    #[test]
    fn decode_result_state_errors_on_unknown_state_instead_of_silently_pending() {
        let task_id = Uuid::new_v4();
        let err = decode_result_state(task_id, "some_future_state", None, None, None)
            .expect_err("unknown state must error, not silently map to Pending");
        assert!(matches!(err, BackendError::Serialization(_)));
        let msg = err.to_string();
        assert!(msg.contains("some_future_state"));
        assert!(msg.contains(&task_id.to_string()));
    }

    #[test]
    fn decode_result_state_failure_with_null_error_message_gets_explicit_placeholder() {
        let task_id = Uuid::new_v4();
        match decode_result_state(task_id, "failure", None, None, None).unwrap() {
            TaskResult::Failure(msg) => {
                assert!(
                    !msg.is_empty(),
                    "a NULL error_message must not silently become an empty-string failure reason"
                );
                assert!(msg.to_lowercase().contains("unknown"));
            }
            other => panic!("expected Failure, got {other:?}"),
        }
    }

    #[test]
    fn decode_result_state_failure_with_present_message_is_preserved() {
        let task_id = Uuid::new_v4();
        match decode_result_state(task_id, "failure", None, Some("boom".to_string()), None).unwrap()
        {
            TaskResult::Failure(msg) => assert_eq!(msg, "boom"),
            other => panic!("expected Failure, got {other:?}"),
        }
    }

    // ── ttl_expires_at_param: the TTL-folded-into-INSERT helper ─────────
    //
    // Regression coverage for folding `expires_at` into `store_result`'s
    // INSERT instead of a second, separate `set_expiration` UPDATE (see its
    // doc comment): these are pure, DB-free unit tests of the value/format
    // computed for the new `$12`/`?` parameter, independent of the live-DB
    // integration tests above.

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_ttl_expires_at_param_is_none_without_a_configured_ttl() {
        let ttl_config = TaskTtlConfig::new();
        let result = PostgresResultBackend::ttl_expires_at_param(&ttl_config, "untracked_task")
            .expect("no TTL configured must not error");
        assert!(
            result.is_none(),
            "no TTL configured must leave expires_at untouched (None), not force it to NULL/now"
        );
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_ttl_expires_at_param_is_a_future_rfc3339_timestamp_when_ttl_configured() {
        let mut ttl_config = TaskTtlConfig::new();
        ttl_config.set_task_ttl("ttl_task", Duration::from_secs(3600));
        let before = Utc::now();

        let raw = PostgresResultBackend::ttl_expires_at_param(&ttl_config, "ttl_task")
            .expect("TTL config must not error")
            .expect("a configured TTL must produce Some(..)");

        // Must parse as RFC3339 (the format `$12::text::timestamptz` expects
        // on the text side of the cast).
        let expires_at = chrono::DateTime::parse_from_rfc3339(&raw)
            .expect("must be a valid RFC3339 timestamp")
            .with_timezone(&Utc);
        assert!(expires_at > before, "expires_at must be in the future");
        assert!(
            expires_at <= before + chrono::Duration::seconds(3601),
            "expires_at must be ~= now + ttl, not something unrelated to the configured 3600s TTL"
        );
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_ttl_expires_at_param_falls_back_to_the_default_ttl() {
        let ttl_config = TaskTtlConfig::with_default(Duration::from_secs(60));
        let result =
            PostgresResultBackend::ttl_expires_at_param(&ttl_config, "any_task_name_at_all")
                .expect("default TTL must not error");
        assert!(
            result.is_some(),
            "a configured default TTL must apply to every task_name"
        );
    }

    #[cfg(feature = "mysql")]
    #[test]
    fn mysql_ttl_expires_at_param_is_none_without_a_configured_ttl() {
        let ttl_config = TaskTtlConfig::new();
        let result = MysqlResultBackend::ttl_expires_at_param(&ttl_config, "untracked_task")
            .expect("no TTL configured must not error");
        assert!(result.is_none());
    }

    #[cfg(feature = "mysql")]
    #[test]
    fn mysql_ttl_expires_at_param_matches_the_mysql_datetime_grammar_when_ttl_configured() {
        let mut ttl_config = TaskTtlConfig::new();
        ttl_config.set_task_ttl("ttl_task", Duration::from_secs(3600));

        let raw = MysqlResultBackend::ttl_expires_at_param(&ttl_config, "ttl_task")
            .expect("TTL config must not error")
            .expect("a configured TTL must produce Some(..)");

        // See row_ext.rs's MySQL DateTime<Utc> parameter convention: a space
        // separator, no 'T', no timezone suffix — never an RFC3339 string.
        assert!(
            !raw.contains('T'),
            "MySQL DATETIME grammar has no 'T' separator: {raw:?}"
        );
        assert!(
            !raw.contains('+') && !raw.contains('Z'),
            "MySQL DATETIME grammar has no timezone suffix: {raw:?}"
        );
        assert!(
            chrono::NaiveDateTime::parse_from_str(&raw, "%Y-%m-%d %H:%M:%S%.6f").is_ok(),
            "must match MySQL's own DATETIME text grammar: {raw:?}"
        );
    }

    // ── default_ttl_config: the constructors' 24h default ───────────────
    //
    // Pure, DB-free unit test: every `*ResultBackend::new`/`with_pool_size`
    // constructor installs this exact config (see their doc comments), but
    // exercising that live would require a real database connection. This
    // is what actually gets asserted; the constructors are trusted to call
    // the same shared function, which every one of them does.

    #[cfg(any(feature = "postgres", feature = "mysql"))]
    #[test]
    fn default_ttl_config_matches_redis_backends_24_hour_default() {
        let config = default_ttl_config();
        assert_eq!(
            config.default_ttl(),
            Some(Duration::from_secs(86400)),
            "the DB backends' default TTL must match RedisResultBackend::new's 24-hour \
             default (ttl::SUCCESS): without it, store_result never populates expires_at, \
             and cleanup_expired_results() — which only deletes rows WHERE expires_at IS NOT \
             NULL — silently collects nothing"
        );
    }
}
