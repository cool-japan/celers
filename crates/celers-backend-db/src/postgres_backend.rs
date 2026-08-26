//! PostgreSQL result backend implementation.
//!
//! Split out of `lib.rs` (which used to hold both the PostgreSQL and MySQL
//! backends plus their shared helpers in one 2300+ line file) purely for
//! file size — no API change. [`crate::decode_result_state`] and
//! [`crate::default_ttl_config`] stay in the crate root since both backends
//! share them.

use async_trait::async_trait;
use celers_backend_redis::{
    BackendError, ChordState, Result, ResultBackend, TaskMeta, TaskResult, TaskTtlConfig,
};
use chrono::Utc;
use oxisql_core::{Connection, ToSqlValue};
use std::time::Duration;
use uuid::Uuid;

use crate::analytics::PostgresAnalytics;
use crate::pg_pool::{PgConnPool, DEFAULT_POOL_SIZE};
use crate::result_compression::{self, CompressionConfig};
use crate::row_ext::{json_from_row, json_param, uuid_from_row, RowExt};
use crate::task_meta_extra::TaskMetaExtra;
use crate::tls_mode;
use crate::{decode_result_state, default_ttl_config};

/// PostgreSQL result backend implementation
#[derive(Clone)]
pub struct PostgresResultBackend {
    conn: PgConnPool,
    ttl_config: TaskTtlConfig,
    compression: CompressionConfig,
}

impl PostgresResultBackend {
    /// Create a new PostgreSQL result backend backed by a pool of
    /// [`DEFAULT_POOL_SIZE`] independent connections.
    ///
    /// Results expire after `ttl::SUCCESS` (24 hours) by default, matching
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
            compression: CompressionConfig::disabled(),
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

    /// Configure compression for the `result_data` column.
    ///
    /// Disabled by default — see [`CompressionConfig::disabled`] for why.
    /// Decompression on read is unconditional regardless of this setting,
    /// so turning compression off here never breaks reading a row a
    /// differently-configured writer compressed.
    pub fn with_compression(mut self, config: CompressionConfig) -> Self {
        self.compression = config;
        self
    }

    /// Get the compression configuration.
    pub fn compression_config(&self) -> &CompressionConfig {
        &self.compression
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
        //
        // The script is wrapped in the shared migration advisory lock so that
        // several workers auto-migrating at once serialize rather than racing
        // each other's `CREATE TABLE IF NOT EXISTS` — see `pg_ddl` for why
        // `IF NOT EXISTS` alone is not safe against concurrent creation.
        self.conn
            .execute_batch(&crate::pg_ddl::advisory_locked_migration(migration_sql))
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

// ── Statements that bind a JSONB parameter ─────────────────────────────────
//
// Hoisted out of the method bodies purely so `tests::every_jsonb_parameter_is
// _cast_from_text` can assert on them without a live database, mirroring the
// same guard `celers-broker-postgres`'s `sql.rs` keeps over its own
// statements.
//
// Every parameter that lands in a `JSONB` column MUST be written
// `$n::text::jsonb`. `json_param` hands over JSON *text*, and a bare `$n` in
// a JSONB position makes PostgreSQL infer the parameter type as `jsonb`,
// after which the client sends the string in the binary jsonb encoding whose
// leading byte has to be the version `0x01`. JSON text begins with `{`, `[`,
// `"`, a digit or `n`, so the server rejects the whole statement with
// `unsupported jsonb version number 123 / 91 / 110`. See `crate::pg_ddl`'s
// sibling note and `row_ext::json_param`'s documentation for the full
// explanation.

const SQL_STORE_RESULT: &str = r#"
                INSERT INTO celers_task_results
                    (task_id, task_name, result_state, result_data, error_message, retry_count,
                     created_at, started_at, completed_at, worker, extra, expires_at)
                VALUES ($1::text::uuid, $2, $3, $4::text::jsonb, $5, $6,
                        $7::text::timestamptz, $8::text::timestamptz, $9::text::timestamptz, $10,
                        $11::text::jsonb,
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
                "#;

const SQL_STORE_RESULTS_BATCH: &str = r#"
                INSERT INTO celers_task_results
                    (task_id, task_name, result_state, result_data, error_message, retry_count,
                     created_at, started_at, completed_at, worker, extra)
                VALUES ($1::text::uuid, $2, $3, $4::text::jsonb, $5, $6,
                        $7::text::timestamptz, $8::text::timestamptz, $9::text::timestamptz, $10,
                        $11::text::jsonb)
                ON CONFLICT (task_id) DO UPDATE SET
                    result_state = EXCLUDED.result_state,
                    result_data = EXCLUDED.result_data,
                    error_message = EXCLUDED.error_message,
                    retry_count = EXCLUDED.retry_count,
                    started_at = EXCLUDED.started_at,
                    completed_at = EXCLUDED.completed_at,
                    worker = EXCLUDED.worker,
                    extra = EXCLUDED.extra
                "#;

const SQL_CHORD_INIT: &str = r#"
                INSERT INTO celers_chord_state (chord_id, total, completed, callback, task_ids, created_at, timeout_seconds, cancelled, cancellation_reason)
                VALUES ($1::text::uuid, $2, 0, $3, $4::text::jsonb, $5::text::timestamptz, $6, $7, $8)
                ON CONFLICT (chord_id) DO UPDATE SET
                    total = EXCLUDED.total,
                    completed = 0,
                    callback = EXCLUDED.callback,
                    task_ids = EXCLUDED.task_ids,
                    timeout_seconds = EXCLUDED.timeout_seconds,
                    cancelled = EXCLUDED.cancelled,
                    cancellation_reason = EXCLUDED.cancellation_reason
                "#;

const SQL_CHORD_UPDATE_STATE: &str = r#"
                INSERT INTO celers_chord_state (chord_id, total, callback, task_ids, created_at, timeout_seconds, cancelled, cancellation_reason)
                VALUES ($1::text::uuid, $2, $3, $4::text::jsonb, $5::text::timestamptz, $6, $7, $8)
                ON CONFLICT (chord_id) DO UPDATE SET
                    total = EXCLUDED.total,
                    callback = EXCLUDED.callback,
                    task_ids = EXCLUDED.task_ids,
                    timeout_seconds = EXCLUDED.timeout_seconds,
                    cancelled = EXCLUDED.cancelled,
                    cancellation_reason = EXCLUDED.cancellation_reason
                "#;

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
        // `result_data` is JSONB: compressing it can only mean wrapping it
        // in the self-describing envelope `result_compression` builds, not
        // storing raw bytes — see that module's doc comment.
        let result_data = result_data
            .map(|v| result_compression::maybe_compress(&v, &self.compression))
            .transpose()?;

        let created_at_param = meta.created_at.to_rfc3339();
        let started_at_param = meta.started_at.map(|dt| dt.to_rfc3339());
        let completed_at_param = meta.completed_at.map(|dt| dt.to_rfc3339());
        let extra_param = Self::extra_param(meta)?;
        let expires_at_param = Self::ttl_expires_at_param(&self.ttl_config, &meta.task_name)?;

        self.conn
            .execute(
                SQL_STORE_RESULT,
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
                -- `result_data` and `extra` are JSONB and MUST be selected as
                -- `::text` (keeping the column name via AS, since they are
                -- read by name): oxisql-postgres converts JSON/JSONB by
                -- asking tokio-postgres for a String, which tokio-postgres
                -- only implements for text-ish types, so an uncast JSONB
                -- column fails in the driver with
                -- `error deserializing column N`. See row_ext::json_from_row.
                SELECT task_id, task_name, result_state,
                       result_data::text AS result_data, error_message,
                       retry_count, created_at, started_at, completed_at, worker,
                       extra::text AS extra
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
                    // Decompression is unconditional: a row a
                    // differently-configured writer compressed must still
                    // read correctly here regardless of this instance's own
                    // `self.compression`. A plain (never compressed) value
                    // passes through unchanged.
                    Some(result_compression::maybe_decompress(
                        result_data,
                        &self.compression,
                    )?)
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
                    ignored_error: None,
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
                SQL_CHORD_INIT,
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
                SQL_CHORD_UPDATE_STATE,
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
                -- `task_ids` is JSONB: selected as `::text` for the reason
                -- documented on the `get_result` query above.
                SELECT chord_id, total, completed, callback,
                       task_ids::text AS task_ids,
                       created_at, timeout_seconds, cancelled, cancellation_reason
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
            let result_data = result_data
                .map(|v| result_compression::maybe_compress(&v, &self.compression))
                .transpose()?;
            let created_at_param = meta.created_at.to_rfc3339();
            let started_at_param = meta.started_at.map(|dt| dt.to_rfc3339());
            let completed_at_param = meta.completed_at.map(|dt| dt.to_rfc3339());
            let extra_param = Self::extra_param(meta)?;

            tx.execute(
                SQL_STORE_RESULTS_BATCH,
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
            -- JSONB columns selected as `::text`; see the `get_result` query.
            SELECT task_id, task_name, result_state,
                   result_data::text AS result_data, error_message,
                   retry_count, created_at, started_at, completed_at, worker,
                   extra::text AS extra
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
                // See `get_result`: decompression is unconditional.
                Some(result_compression::maybe_decompress(
                    result_data,
                    &self.compression,
                )?)
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
                ignored_error: None,
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Every `$n` that lands in a `JSONB` column must be written
    /// `$n::text::jsonb`.
    ///
    /// This is the always-on guard for a bug that shipped: `result_data`,
    /// `extra` and `task_ids` were bound as bare `$n`, so PostgreSQL inferred
    /// the parameter type as `jsonb`, the client encoded the JSON *text*
    /// `json_param` produces as if it were binary jsonb, and the server
    /// rejected every write with `unsupported jsonb version number 123` (`{`)
    /// / `91` (`[`) / `110` (`n`). Storing any successful task result with a
    /// payload — the crate's primary job — failed outright against a live
    /// PostgreSQL.
    ///
    /// The live round-trip tests below *would* have caught it, but they are
    /// `#[ignore]`d and had never been run against a real server. This test
    /// needs no database, so it runs in every plain `cargo nextest run`.
    /// `celers-broker-postgres`'s `sql.rs` keeps the same guard over its own
    /// statements.
    #[test]
    fn every_jsonb_parameter_is_cast_from_text() {
        // (statement, const name, the JSONB placeholders it binds)
        let cases: &[(&str, &str, &[&str])] = &[
            (SQL_STORE_RESULT, "SQL_STORE_RESULT", &["$4", "$11"]),
            (
                SQL_STORE_RESULTS_BATCH,
                "SQL_STORE_RESULTS_BATCH",
                &["$4", "$11"],
            ),
            (SQL_CHORD_INIT, "SQL_CHORD_INIT", &["$4"]),
            (SQL_CHORD_UPDATE_STATE, "SQL_CHORD_UPDATE_STATE", &["$4"]),
        ];

        for (sql, name, jsonb_params) in cases {
            for param in *jsonb_params {
                let cast = format!("{param}::text::jsonb");
                assert!(
                    sql.contains(&cast),
                    "{name}: JSONB parameter {param} must be bound as `{cast}`, \
                     not as a bare `{param}` — a bare placeholder is sent in the \
                     binary jsonb encoding and the server rejects it with \
                     `unsupported jsonb version number`. Statement:\n{sql}"
                );
            }
        }
    }

    /// The casts above are only correct if the placeholders they name are the
    /// ones actually pointed at the JSONB columns, so pin the column order of
    /// each `INSERT` too. Renumbering a parameter without moving its cast
    /// would otherwise silently reintroduce the bug.
    #[test]
    fn jsonb_placeholders_match_the_insert_column_order() {
        // celers_task_results: (task_id, task_name, result_state, result_data,
        // error_message, retry_count, created_at, started_at, completed_at,
        // worker, extra, ...) — result_data is 4th, extra is 11th.
        for (sql, name) in [
            (SQL_STORE_RESULT, "SQL_STORE_RESULT"),
            (SQL_STORE_RESULTS_BATCH, "SQL_STORE_RESULTS_BATCH"),
        ] {
            assert!(
                sql.contains("result_state, result_data, error_message"),
                "{name}: result_data must stay the 4th column"
            );
            assert!(
                sql.contains("worker, extra"),
                "{name}: extra must stay the 11th column"
            );
        }

        // celers_chord_state: task_ids is the 5th column in chord_init (which
        // also writes `completed`) but the 4th VALUES placeholder, because
        // `completed` is written as the literal 0 rather than bound.
        assert!(
            SQL_CHORD_INIT.contains("callback, task_ids, created_at"),
            "SQL_CHORD_INIT: task_ids column position moved"
        );
        assert!(
            SQL_CHORD_INIT.contains("$2, 0, $3, $4::text::jsonb"),
            "SQL_CHORD_INIT: `completed` is the literal 0, so task_ids is $4"
        );
        assert!(
            SQL_CHORD_UPDATE_STATE.contains("callback, task_ids, created_at"),
            "SQL_CHORD_UPDATE_STATE: task_ids column position moved"
        );
    }

    #[tokio::test]
    #[ignore] // Requires PostgreSQL running
    async fn test_postgres_backend_creation() {
        let Some(database_url) = crate::test_env::postgres_url("test_postgres_backend_creation")
        else {
            return;
        };

        let backend = PostgresResultBackend::new(&database_url).await;
        assert!(backend.is_ok());
    }

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
        let Some(database_url) =
            crate::test_env::postgres_url("test_postgres_store_get_delete_roundtrip_uuid_binding")
        else {
            return;
        };
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

    #[tokio::test]
    #[ignore] // Requires PostgreSQL running
    async fn test_postgres_store_get_roundtrip_with_compression_enabled() {
        // End-to-end proof that a compressed `result_data` (a JSONB column)
        // round-trips through a real server: `store_result` must write a
        // valid JSON envelope (a raw compressed blob would be rejected by
        // the column's own JSONB validation), and `get_result` must decode
        // it back to the exact original value.
        let Some(database_url) = crate::test_env::postgres_url(
            "test_postgres_store_get_roundtrip_with_compression_enabled",
        ) else {
            return;
        };
        let mut backend = PostgresResultBackend::new(&database_url)
            .await
            .expect("connect")
            .with_compression(crate::result_compression::CompressionConfig::new(
                16, "zstd",
            ));
        backend.migrate().await.expect("migrate");

        let task_id = Uuid::new_v4();
        let large_value: Vec<serde_json::Value> = (0..512)
            .map(
                |i| serde_json::json!({"seq": i, "note": "same shape every time, compresses well"}),
            )
            .collect();
        let original = serde_json::json!({ "items": large_value });
        let mut meta = TaskMeta::new(task_id, "compression_roundtrip_test".to_string());
        meta.result = TaskResult::Success(original.clone());

        backend.store_result(task_id, &meta).await.expect("store");

        // The raw column value must be the small envelope, not the large
        // plain JSON -- proving compression actually happened, not just
        // that decode is lenient.
        let raw_rows = backend
            .connection()
            .query(
                "SELECT result_data::text FROM celers_task_results WHERE task_id = $1::text::uuid",
                &[&task_id.to_string()],
            )
            .await
            .expect("raw select");
        let raw_json: String = raw_rows[0].col_idx(0).expect("result_data column");
        assert!(
            raw_json.contains("__celers_compressed"),
            "the stored column value must be the compression envelope: {raw_json}"
        );
        assert!(
            raw_json.len() < 2000,
            "the envelope must be much smaller than the ~20KB plain payload: {} bytes",
            raw_json.len()
        );

        let fetched = backend
            .get_result(task_id)
            .await
            .expect("get")
            .expect("row exists");
        match fetched.result {
            TaskResult::Success(v) => assert_eq!(v, original),
            other => panic!("expected Success, got {other:?}"),
        }

        backend.delete_result(task_id).await.expect("cleanup");
    }

    #[tokio::test]
    #[ignore] // Requires PostgreSQL running
    async fn test_postgres_store_get_roundtrip_with_ignored_error() {
        // End-to-end proof that `TaskMeta::ignored_error` -- the marker
        // `celers_backend_db::result_store` uses to preserve
        // `TaskResultValue::Ignored`'s suppressed error text across the
        // `Success(null)` projection (see that module's doc comments) --
        // actually survives a real `extra` JSONB column round trip, not
        // just the in-memory `TaskMetaExtra` unit tests.
        let Some(database_url) =
            crate::test_env::postgres_url("test_postgres_store_get_roundtrip_with_ignored_error")
        else {
            return;
        };
        let mut backend = PostgresResultBackend::new(&database_url)
            .await
            .expect("connect");
        backend.migrate().await.expect("migrate");

        let task_id = Uuid::new_v4();
        let mut meta = TaskMeta::new(task_id, "ignored_error_roundtrip_test".to_string());
        meta.result = TaskResult::Success(serde_json::Value::Null);
        meta.ignored_error = Some("task failed but ignore_errors was set".to_string());

        backend.store_result(task_id, &meta).await.expect("store");

        // The raw `extra` column must actually carry the marker -- proving
        // it was persisted, not merely echoed back from the in-memory
        // `TaskMeta` this same process just built.
        let raw_rows = backend
            .connection()
            .query(
                "SELECT extra::text FROM celers_task_results WHERE task_id = $1::text::uuid",
                &[&task_id.to_string()],
            )
            .await
            .expect("raw select");
        let raw_extra: String = raw_rows[0].col_idx(0).expect("extra column");
        assert!(
            raw_extra.contains("task failed but ignore_errors was set"),
            "the extra column must persist ignored_error: {raw_extra}"
        );

        let fetched = backend
            .get_result(task_id)
            .await
            .expect("get")
            .expect("row exists");
        assert!(matches!(
            fetched.result,
            TaskResult::Success(serde_json::Value::Null)
        ));
        assert_eq!(
            fetched.ignored_error.as_deref(),
            Some("task failed but ignore_errors was set"),
            "ignored_error must round-trip through a real Postgres connection"
        );

        backend.delete_result(task_id).await.expect("cleanup");
    }

    /// Build a fresh, non-cancelled `ChordState` with `TOTAL` header tasks
    /// and no progress yet.
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

    #[tokio::test]
    #[ignore] // Requires PostgreSQL running
    async fn test_postgres_chord_init_resets_completed_counter_on_conflict() {
        // Regression test for the chord *reset* bug: `chord_init`'s
        // ON CONFLICT branch previously omitted `completed` from its SET
        // list, so re-running `chord_init` for the same `chord_id` — the
        // `chord_retry` path — left a terminal counter in place: the
        // barrier looked already-complete before any of the retried tasks
        // reported in, and the callback would fire immediately.
        let Some(database_url) = crate::test_env::postgres_url(
            "test_postgres_chord_init_resets_completed_counter_on_conflict",
        ) else {
            return;
        };
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
        let Some(database_url) =
            crate::test_env::postgres_url("test_postgres_chord_cancel_preserves_completed_counter")
        else {
            return;
        };
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

    // ── ttl_expires_at_param: the TTL-folded-into-INSERT helper ─────────
    //
    // Regression coverage for folding `expires_at` into `store_result`'s
    // INSERT instead of a second, separate `set_expiration` UPDATE (see its
    // doc comment): these are pure, DB-free unit tests of the value/format
    // computed for the new `$12` parameter, independent of the live-DB
    // integration tests above.

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
}
