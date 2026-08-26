//! Core MysqlBroker struct and primary implementation
//!
//! Contains the broker struct definition, constructors, and core
//! enqueue/dequeue/ack/reject operations. Schema migration — including the
//! two untracked upgrade steps that let this crate share a database with
//! `celers-backend-db` — lives in the crate-internal `broker_migrate` module.

use crate::circuit_breaker::{CircuitBreakerConfig, CircuitBreakerStateInternal};
use crate::mysql_error::with_deadlock_retry;
use crate::row_ext::RowExt;
use crate::tls_mode;
use crate::types::*;
use crate::workflow::TaskHooks;
use crate::{server_version, sql_text};
use celers_core::{CelersError, Result, SerializedTask, TaskId};
use chrono::Utc;
use oxisql_core::Connection;
use oxisql_mysql::MyConnection;
use serde_json::json;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use uuid::Uuid;

/// MySQL-based broker implementation using SKIP LOCKED
pub struct MysqlBroker {
    pub(crate) conn: MyConnection,
    /// The configured maximum connection-pool size this broker was
    /// constructed with (via [`PoolConfig::max_connections`]).
    ///
    /// `oxisql_mysql::MyConnection` does not expose a pool-introspection API
    /// (no `num_idle`/`size`/`options()` equivalent to `sqlx::MySqlPool`'s —
    /// confirmed by reading `oxisql-mysql` 0.3.2's `connection.rs` in full:
    /// the only pool-shaped knobs live on `MyConnectionBuilder`, which are
    /// write-only at connect time), so the diagnostics methods below
    /// (`get_connection_diagnostics`, `get_pool_health`) report this
    /// configured ceiling rather than a live idle/active connection count.
    pub(crate) configured_max_connections: u32,
    /// Logical queue name for multi-tenancy.
    ///
    /// This is a real, indexed `celers_tasks.queue_name` column (migration
    /// `009_queue_name.sql`), bound by every enqueue path in this crate and
    /// filtered on by every claim path (`dequeue`, `dequeue_batch`,
    /// `dequeue_with_worker_id`) as well as `queue_size`, `get_statistics`,
    /// `enqueue_deduplicated`, `enqueue_deduplicated_window` and
    /// `purge_terminal_tasks`. Two brokers pointed at the same database with
    /// different queue names therefore never see each other's tasks. It is
    /// NOT a table name — all queues share one set of tables.
    ///
    /// The read-only *diagnostics* family is scoped to the same column:
    /// `count_by_state_quick`, `get_task_age_distribution`,
    /// `get_retry_statistics`, `list_active_workers`,
    /// `get_worker_statistics`/`get_all_worker_statistics`, `get_queue_health`
    /// (and `has_capacity`, which is built on the first of them), plus
    /// `list_scheduled_tasks`/`count_scheduled_tasks`. These used to report
    /// database-wide numbers on a queue-scoped broker — the same class of bug
    /// `009_queue_name.sql` was written to fix for `queue_size` and
    /// `get_statistics`, and the reason a "pending" count could exceed what
    /// the same broker's `dequeue` would ever hand out.
    ///
    /// `enqueue_deduplicated`/`enqueue_deduplicated_window`'s duplicate
    /// lookup used to search `$.dedup_key` with no queue predicate: two
    /// brokers on different queues sharing a `dedup_key` would collide, and
    /// the second broker's call silently returned the first broker's task id
    /// instead of inserting anything into its own queue — a real task loss,
    /// not merely a cross-tenant read leak, since that id is invisible to
    /// the second broker's (queue-scoped) `dequeue`.
    ///
    /// The same label is *also* still written into the task metadata JSON
    /// document under `$.queue` for backwards compatibility with anything
    /// reading metadata directly; the column is authoritative.
    ///
    /// Deliberately queue-blind, because silently narrowing a destructive or
    /// operator-facing method would be worse than leaving it documented as
    /// database-wide: `purge_all`, `purge_by_state`, `purge_by_task_name`,
    /// `archive_completed_tasks`, `recover_stuck_tasks`, the operator-facing
    /// listings (`list_tasks`, `count_by_task_name`, `query_tasks_by_metadata`),
    /// and the DLQ inspection/purge helpers — the DLQ table has no
    /// `queue_name` column of its own. See `TODO.md`.
    pub(crate) queue_name: String,
    pub(crate) paused: AtomicBool,
    pub(crate) enqueue_count: AtomicU64,
    pub(crate) enqueue_window_start_ms: AtomicI64,
    pub(crate) circuit_breaker: Arc<RwLock<CircuitBreakerStateInternal>>,
    pub(crate) hooks: Arc<tokio::sync::RwLock<TaskHooks>>,
    /// How long a durable revocation record survives, in seconds — see
    /// `revocation.rs`. Mirrors `celers-broker-redis`'s
    /// `DEFAULT_REVOCATION_TTL_SECS`/`with_revocation_ttl`.
    pub(crate) revocation_ttl_secs: u64,
    /// How often [`crate::revocation::MysqlRevocationStream`] polls
    /// `celers_revoked_tasks` for new rows. MySQL has no LISTEN/NOTIFY
    /// equivalent, so this is the notification-latency knob — see
    /// `revocation.rs`.
    pub(crate) revocation_poll_interval_secs: u64,
}

/// Default lifetime of a durable revocation record: 24 hours, matching
/// `celers-broker-redis::DEFAULT_REVOCATION_TTL_SECS` and
/// `celers-broker-postgres::DEFAULT_REVOCATION_TTL_SECS`.
pub const DEFAULT_REVOCATION_TTL_SECS: u64 = 86_400;

/// Default poll interval for [`crate::revocation::MysqlRevocationStream`].
///
/// Every already-running task a `revoke(terminate = true)` call needs to
/// reach waits up to this long to learn about it (a *queued* task is refused
/// immediately, at dispatch, via `is_revoked()` against the same durable
/// row — this latency is specific to the fire-and-forget "abort a task that
/// is already executing" path). Two seconds is a deliberate compromise: short
/// enough that "revoke --terminate" feels responsive, long enough that an
/// idle worker fleet is not issuing a `SELECT` against this table five times
/// a second per worker.
pub const DEFAULT_REVOCATION_POLL_INTERVAL_SECS: u64 = 2;

impl MysqlBroker {
    /// Create a new MySQL broker
    ///
    /// # Arguments
    /// * `database_url` - MySQL connection string (e.g., "mysql://user:pass@localhost/db")
    /// * `queue_name` - Logical queue name for multi-tenancy (optional, defaults to "default")
    pub async fn new(database_url: &str) -> Result<Self> {
        Self::with_queue(database_url, "default").await
    }

    /// Create a new MySQL broker with a specific queue name
    pub async fn with_queue(database_url: &str, queue_name: &str) -> Result<Self> {
        Self::with_config(database_url, queue_name, PoolConfig::default()).await
    }

    /// Create a new MySQL broker with custom connection pool configuration
    ///
    /// Note: `oxisql_mysql::MyConnection::connect` does not currently accept
    /// per-call pool-sizing overrides the way `sqlx::MySqlPoolOptions` did
    /// (see the `configured_max_connections` field doc for why). `config` is
    /// still accepted (and `config.max_connections` recorded for diagnostics
    /// reporting) for source compatibility, but its `min_connections`,
    /// `acquire_timeout_secs`, `max_lifetime_secs`, and `idle_timeout_secs`
    /// knobs are not wired through to the underlying pool post-migration.
    ///
    /// # Errors
    ///
    /// Returns [`CelersError::Configuration`] when the connected server is
    /// older than **MySQL 8.0.1** / **MariaDB 10.6** — see
    /// `connect_checked`.
    pub async fn with_config(
        database_url: &str,
        queue_name: &str,
        config: PoolConfig,
    ) -> Result<Self> {
        let conn = Self::connect_checked(database_url).await?;

        Ok(Self {
            conn,
            configured_max_connections: config.max_connections,
            queue_name: queue_name.to_string(),
            paused: AtomicBool::new(false),
            enqueue_count: AtomicU64::new(0),
            enqueue_window_start_ms: AtomicI64::new(chrono::Utc::now().timestamp_millis()),
            circuit_breaker: Arc::new(RwLock::new(CircuitBreakerStateInternal::new(
                CircuitBreakerConfig::default(),
            ))),
            hooks: Arc::new(tokio::sync::RwLock::new(TaskHooks::new())),
            revocation_ttl_secs: DEFAULT_REVOCATION_TTL_SECS,
            revocation_poll_interval_secs: DEFAULT_REVOCATION_POLL_INTERVAL_SECS,
        })
    }

    /// Create a new MySQL broker with custom circuit breaker configuration
    pub async fn with_circuit_breaker_config(
        database_url: &str,
        queue_name: &str,
        pool_config: PoolConfig,
        circuit_breaker_config: CircuitBreakerConfig,
    ) -> Result<Self> {
        let conn = Self::connect_checked(database_url).await?;

        Ok(Self {
            conn,
            configured_max_connections: pool_config.max_connections,
            queue_name: queue_name.to_string(),
            paused: AtomicBool::new(false),
            enqueue_count: AtomicU64::new(0),
            enqueue_window_start_ms: AtomicI64::new(chrono::Utc::now().timestamp_millis()),
            circuit_breaker: Arc::new(RwLock::new(CircuitBreakerStateInternal::new(
                circuit_breaker_config,
            ))),
            hooks: Arc::new(tokio::sync::RwLock::new(TaskHooks::new())),
            revocation_ttl_secs: DEFAULT_REVOCATION_TTL_SECS,
            revocation_poll_interval_secs: DEFAULT_REVOCATION_POLL_INTERVAL_SECS,
        })
    }

    /// Connect, then verify the server can actually run this broker's claim
    /// spine.
    ///
    /// TLS mode is derived from `database_url`'s `ssl-mode`/`tls` query
    /// parameters (see `tls_mode.rs`): a URL with `ssl-mode=required` (or
    /// `tls=true`) gets a real TLS connection, matching the
    /// pre-`oxisql`-migration `sqlx` behavior. Absent/`ssl-mode=disabled`
    /// still resolves to `TlsMode::Disabled`, so plain-text callers are
    /// unaffected.
    ///
    /// Every dequeue in this crate uses `FOR UPDATE ... SKIP LOCKED`, which
    /// exists only on MySQL 8.0.1+ and MariaDB 10.6+. Without this probe an
    /// unsupported server produced a bare `ERROR 1064` on every dequeue, with
    /// nothing pointing at the server version as the cause. Both constructors
    /// route through here so the probe cannot be bypassed.
    ///
    /// A version string this parser does not recognise (proxies, forks) is
    /// logged and accepted rather than rejected — see
    /// [`crate::server_version`].
    async fn connect_checked(database_url: &str) -> Result<MyConnection> {
        let tls = tls_mode::mysql_tls_mode_for_url(database_url)
            .map_err(|e| CelersError::Other(format!("Failed to resolve TLS mode: {e}")))?;
        let conn = MyConnection::connect(database_url, tls)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to connect to database: {}", e)))?;

        let rows = conn
            .query("SELECT VERSION() AS v", &[])
            .await
            .map_err(|e| CelersError::Other(format!("Failed to read server version: {e}")))?;
        let reported: Option<String> = rows
            .first()
            .map(|row| row.col("v"))
            .transpose()
            .map_err(|e| CelersError::Other(format!("Failed to read server version: {e}")))?;

        match reported {
            Some(version) => server_version::check_skip_locked_support(&version)?,
            None => tracing::warn!(
                "SELECT VERSION() returned no rows; skipping the SKIP LOCKED capability check"
            ),
        }

        Ok(conn)
    }

    /// Get the underlying connection
    ///
    /// `MyConnection` is `Clone` and internally pool-backed by
    /// `mysql_async::Pool` (each `execute`/`query` call transparently checks
    /// out and returns a connection from that pool) — unlike
    /// `celers-broker-postgres`'s single-connection `PgConnection`, so this
    /// is a direct field accessor with no shared-single-connection
    /// concurrency caveat.
    pub fn connection(&self) -> &MyConnection {
        &self.conn
    }

    /// Override how long a durable revocation record survives.
    ///
    /// The default is [`DEFAULT_REVOCATION_TTL_SECS`]. Shorten it to exercise
    /// expiry in a test without waiting a day for a record to lapse.
    pub fn with_revocation_ttl(mut self, ttl_secs: u64) -> Self {
        self.revocation_ttl_secs = ttl_secs;
        self
    }

    /// The currently configured revocation TTL, in seconds.
    pub fn revocation_ttl(&self) -> u64 {
        self.revocation_ttl_secs
    }

    /// Override how often [`crate::revocation::MysqlRevocationStream`] polls
    /// for new revocations.
    ///
    /// The default is [`DEFAULT_REVOCATION_POLL_INTERVAL_SECS`]. Shorten it to
    /// keep a test's wait bounded.
    pub fn with_revocation_poll_interval(mut self, poll_interval_secs: u64) -> Self {
        self.revocation_poll_interval_secs = poll_interval_secs;
        self
    }

    /// The currently configured revocation poll interval, in seconds.
    pub fn revocation_poll_interval(&self) -> u64 {
        self.revocation_poll_interval_secs
    }

    /// The logical queue this broker enqueues into and claims from.
    ///
    /// Backed by the real, indexed `celers_tasks.queue_name` column, so two
    /// brokers sharing a database but configured with different queue names
    /// never see each other's tasks.
    pub fn queue_name(&self) -> &str {
        &self.queue_name
    }

    /// Build the JSON document stored in `celers_tasks.metadata`.
    ///
    /// The document is this broker's own labels (`queue`, `enqueued_at`)
    /// merged with the task's serialized [`celers_core::TaskMetadata`], with
    /// `extra` overlaid last so a caller-supplied label (`trace_context`,
    /// `dedup_key`, `retry_policy`, `scheduled_for`, ...) always survives.
    ///
    /// Serialization failures are propagated. Every enqueue path used to fall
    /// back to the literal `"{}"` on error and insert the row anyway, which
    /// silently discarded the queue label, the dedup key (defeating
    /// `enqueue_deduplicated` entirely), the trace context and every merged
    /// metadata field — with no error and no log.
    ///
    /// The dequeue side reads this document back to reconstruct the task's
    /// metadata (see [`crate::task_row::build_serialized_task`]), so a
    /// lobotomized document is a real data loss, not a cosmetic one.
    pub(crate) fn build_task_metadata_document(
        &self,
        task: &SerializedTask,
        extra: serde_json::Value,
    ) -> Result<String> {
        let mut document = json!({
            "queue": self.queue_name,
            "enqueued_at": Utc::now().to_rfc3339(),
        });

        let task_metadata = serde_json::to_value(&task.metadata).map_err(|e| {
            CelersError::Serialization(format!(
                "Failed to serialize metadata for task {}: {e}",
                task.metadata.id
            ))
        })?;

        if let Some(target) = document.as_object_mut() {
            for source in [&task_metadata, &extra] {
                if let Some(fields) = source.as_object() {
                    for (key, value) in fields {
                        target.insert(key.clone(), value.clone());
                    }
                }
            }
        }

        serde_json::to_string(&document).map_err(|e| {
            CelersError::Serialization(format!(
                "Failed to serialize metadata document for task {}: {e}",
                task.metadata.id
            ))
        })
    }

    // ========== Queue Control ==========

    /// Pause the queue (dequeue will return None while paused)
    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
        tracing::info!(queue = %self.queue_name, "Queue paused");
    }

    /// Resume the queue
    pub fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
        tracing::info!(queue = %self.queue_name, "Queue resumed");
    }

    /// Check if the queue is paused
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    // ========== Task Inspection ==========

    /// Get detailed information about a specific task
    pub async fn get_task(&self, task_id: &TaskId) -> Result<Option<TaskInfo>> {
        let rows = self
            .conn
            .query(
                r#"
                SELECT id, task_name, state, priority, retry_count, max_retries,
                       created_at, scheduled_at, started_at, completed_at, worker_id, error_message
                FROM celers_tasks
                WHERE id = ?
                "#,
                &[&task_id.to_string()],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get task: {}", e)))?;

        match rows.into_iter().next() {
            Some(row) => {
                let task_id_str: String = row
                    .col("id")
                    .map_err(|e| CelersError::Other(format!("Failed to get task: {e}")))?;
                let state_str: String = row
                    .col("state")
                    .map_err(|e| CelersError::Other(format!("Failed to get task: {e}")))?;
                Ok(Some(TaskInfo {
                    id: Uuid::parse_str(&task_id_str)
                        .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?,
                    task_name: row
                        .col("task_name")
                        .map_err(|e| CelersError::Other(format!("Failed to get task: {e}")))?,
                    state: state_str.parse()?,
                    priority: row
                        .col("priority")
                        .map_err(|e| CelersError::Other(format!("Failed to get task: {e}")))?,
                    retry_count: row
                        .col("retry_count")
                        .map_err(|e| CelersError::Other(format!("Failed to get task: {e}")))?,
                    max_retries: row
                        .col("max_retries")
                        .map_err(|e| CelersError::Other(format!("Failed to get task: {e}")))?,
                    created_at: row
                        .col("created_at")
                        .map_err(|e| CelersError::Other(format!("Failed to get task: {e}")))?,
                    scheduled_at: row
                        .col("scheduled_at")
                        .map_err(|e| CelersError::Other(format!("Failed to get task: {e}")))?,
                    started_at: row
                        .col("started_at")
                        .map_err(|e| CelersError::Other(format!("Failed to get task: {e}")))?,
                    completed_at: row
                        .col("completed_at")
                        .map_err(|e| CelersError::Other(format!("Failed to get task: {e}")))?,
                    worker_id: row
                        .col("worker_id")
                        .map_err(|e| CelersError::Other(format!("Failed to get task: {e}")))?,
                    error_message: row
                        .col("error_message")
                        .map_err(|e| CelersError::Other(format!("Failed to get task: {e}")))?,
                }))
            }
            None => Ok(None),
        }
    }

    /// List tasks by state with pagination
    pub async fn list_tasks(
        &self,
        state: Option<DbTaskState>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<TaskInfo>> {
        let rows = match state {
            Some(s) => {
                self.conn
                    .query(
                        r#"
                        SELECT id, task_name, state, priority, retry_count, max_retries,
                               created_at, scheduled_at, started_at, completed_at, worker_id, error_message
                        FROM celers_tasks
                        WHERE state = ?
                        ORDER BY created_at DESC
                        LIMIT ? OFFSET ?
                        "#,
                        &[&s.to_string(), &limit, &offset],
                    )
                    .await
            }
            None => {
                self.conn
                    .query(
                        r#"
                        SELECT id, task_name, state, priority, retry_count, max_retries,
                               created_at, scheduled_at, started_at, completed_at, worker_id, error_message
                        FROM celers_tasks
                        ORDER BY created_at DESC
                        LIMIT ? OFFSET ?
                        "#,
                        &[&limit, &offset],
                    )
                    .await
            }
        }
        .map_err(|e| CelersError::Other(format!("Failed to list tasks: {}", e)))?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            let task_id_str: String = row
                .col("id")
                .map_err(|e| CelersError::Other(format!("Failed to list tasks: {e}")))?;
            let state_str: String = row
                .col("state")
                .map_err(|e| CelersError::Other(format!("Failed to list tasks: {e}")))?;
            tasks.push(TaskInfo {
                id: Uuid::parse_str(&task_id_str)
                    .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?,
                task_name: row
                    .col("task_name")
                    .map_err(|e| CelersError::Other(format!("Failed to list tasks: {e}")))?,
                state: state_str.parse()?,
                priority: row
                    .col("priority")
                    .map_err(|e| CelersError::Other(format!("Failed to list tasks: {e}")))?,
                retry_count: row
                    .col("retry_count")
                    .map_err(|e| CelersError::Other(format!("Failed to list tasks: {e}")))?,
                max_retries: row
                    .col("max_retries")
                    .map_err(|e| CelersError::Other(format!("Failed to list tasks: {e}")))?,
                created_at: row
                    .col("created_at")
                    .map_err(|e| CelersError::Other(format!("Failed to list tasks: {e}")))?,
                scheduled_at: row
                    .col("scheduled_at")
                    .map_err(|e| CelersError::Other(format!("Failed to list tasks: {e}")))?,
                started_at: row
                    .col("started_at")
                    .map_err(|e| CelersError::Other(format!("Failed to list tasks: {e}")))?,
                completed_at: row
                    .col("completed_at")
                    .map_err(|e| CelersError::Other(format!("Failed to list tasks: {e}")))?,
                worker_id: row
                    .col("worker_id")
                    .map_err(|e| CelersError::Other(format!("Failed to list tasks: {e}")))?,
                error_message: row
                    .col("error_message")
                    .map_err(|e| CelersError::Other(format!("Failed to list tasks: {e}")))?,
            });
        }
        Ok(tasks)
    }

    /// Get queue statistics
    pub async fn get_statistics(&self) -> Result<QueueStatistics> {
        // MySQL doesn't have FILTER clause, so we use CASE WHEN
        let rows = self
            .conn
            .query(
                r#"
                SELECT
                    SUM(CASE WHEN state = 'pending' THEN 1 ELSE 0 END) as pending,
                    SUM(CASE WHEN state = 'processing' THEN 1 ELSE 0 END) as processing,
                    SUM(CASE WHEN state = 'completed' THEN 1 ELSE 0 END) as completed,
                    SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END) as failed,
                    SUM(CASE WHEN state = 'cancelled' THEN 1 ELSE 0 END) as cancelled,
                    COUNT(*) as total
                FROM celers_tasks
                WHERE queue_name = ?
                "#,
                &[&self.queue_name],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get statistics: {}", e)))?;
        let row = rows
            .into_iter()
            .next()
            .ok_or_else(|| CelersError::Other("get_statistics: query returned no rows".into()))?;

        let dlq_rows = self
            .conn
            .query("SELECT COUNT(*) AS c FROM celers_dead_letter_queue", &[])
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get DLQ count: {}", e)))?;
        let dlq_count: i64 = dlq_rows
            .first()
            .map(|r| r.col("c"))
            .transpose()
            .map_err(|e| CelersError::Other(format!("Failed to get DLQ count: {e}")))?
            .unwrap_or(0);

        // MySQL returns DECIMAL for SUM, decoded here as a decimal-text
        // string (see `row_ext.rs`: `Value::Decimal` is transparently
        // accepted by `FromValue for Option<String>`) and parsed the same
        // way the pre-migration `rust_decimal::Decimal::to_string().parse()`
        // round-trip did.
        let pending: Option<String> = row
            .col("pending")
            .map_err(|e| CelersError::Other(format!("Failed to get statistics: {e}")))?;
        let processing: Option<String> = row
            .col("processing")
            .map_err(|e| CelersError::Other(format!("Failed to get statistics: {e}")))?;
        let completed: Option<String> = row
            .col("completed")
            .map_err(|e| CelersError::Other(format!("Failed to get statistics: {e}")))?;
        let failed: Option<String> = row
            .col("failed")
            .map_err(|e| CelersError::Other(format!("Failed to get statistics: {e}")))?;
        let cancelled: Option<String> = row
            .col("cancelled")
            .map_err(|e| CelersError::Other(format!("Failed to get statistics: {e}")))?;
        let total: i64 = row
            .col("total")
            .map_err(|e| CelersError::Other(format!("Failed to get statistics: {e}")))?;

        Ok(QueueStatistics {
            pending: pending.and_then(|d| d.parse().ok()).unwrap_or(0),
            processing: processing.and_then(|d| d.parse().ok()).unwrap_or(0),
            completed: completed.and_then(|d| d.parse().ok()).unwrap_or(0),
            failed: failed.and_then(|d| d.parse().ok()).unwrap_or(0),
            cancelled: cancelled.and_then(|d| d.parse().ok()).unwrap_or(0),
            dlq: dlq_count,
            total,
        })
    }

    // ========== DLQ Operations ==========

    /// List tasks in the dead letter queue
    pub async fn list_dlq(&self, limit: i64, offset: i64) -> Result<Vec<DlqTaskInfo>> {
        let rows = self
            .conn
            .query(
                r#"
                SELECT id, task_id, task_name, retry_count, error_message, failed_at
                FROM celers_dead_letter_queue
                ORDER BY failed_at DESC
                LIMIT ? OFFSET ?
                "#,
                &[&limit, &offset],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to list DLQ: {}", e)))?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            let id_str: String = row
                .col("id")
                .map_err(|e| CelersError::Other(format!("Failed to list DLQ: {e}")))?;
            let task_id_str: String = row
                .col("task_id")
                .map_err(|e| CelersError::Other(format!("Failed to list DLQ: {e}")))?;
            tasks.push(DlqTaskInfo {
                id: Uuid::parse_str(&id_str)
                    .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?,
                task_id: Uuid::parse_str(&task_id_str)
                    .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?,
                task_name: row
                    .col("task_name")
                    .map_err(|e| CelersError::Other(format!("Failed to list DLQ: {e}")))?,
                retry_count: row
                    .col("retry_count")
                    .map_err(|e| CelersError::Other(format!("Failed to list DLQ: {e}")))?,
                error_message: row
                    .col("error_message")
                    .map_err(|e| CelersError::Other(format!("Failed to list DLQ: {e}")))?,
                failed_at: row
                    .col("failed_at")
                    .map_err(|e| CelersError::Other(format!("Failed to list DLQ: {e}")))?,
            });
        }
        Ok(tasks)
    }

    /// Requeue a task from the dead letter queue
    ///
    /// This moves the task back to the main queue with reset retry count.
    pub async fn requeue_from_dlq(&self, dlq_id: &Uuid) -> Result<TaskId> {
        let mut tx = self
            .conn
            .transaction()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        // Get task from DLQ
        let rows = tx
            .query(
                r#"
                SELECT task_id, task_name, payload, metadata
                FROM celers_dead_letter_queue
                WHERE id = ?
                "#,
                &[&dlq_id.to_string()],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to fetch DLQ task: {}", e)))?;

        let row = rows
            .into_iter()
            .next()
            .ok_or_else(|| CelersError::Other("DLQ task not found".to_string()))?;

        let task_id_str: String = row
            .col("task_id")
            .map_err(|e| CelersError::Other(format!("Failed to fetch DLQ task: {e}")))?;
        let task_id = Uuid::parse_str(&task_id_str)
            .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?;
        let task_name: String = row
            .col("task_name")
            .map_err(|e| CelersError::Other(format!("Failed to fetch DLQ task: {e}")))?;
        let payload: Vec<u8> = row
            .col("payload")
            .map_err(|e| CelersError::Other(format!("Failed to fetch DLQ task: {e}")))?;
        let metadata: Option<String> = row
            .col("metadata")
            .map_err(|e| CelersError::Other(format!("Failed to fetch DLQ task: {e}")))?;

        // Create new task in main queue
        let new_task_id = Uuid::new_v4();
        tx.execute(
            r#"
            INSERT INTO celers_tasks
                (id, queue_name, task_name, payload, state, priority, retry_count, max_retries, metadata, created_at, scheduled_at)
            VALUES (?, ?, ?, ?, 'pending', 0, 0, 3, ?, NOW(), NOW())
            "#,
            &[
                &new_task_id.to_string(),
                &self.queue_name,
                &task_name,
                &payload,
                &metadata,
            ],
        )
        .await
        .map_err(|e| CelersError::Other(format!("Failed to requeue task: {}", e)))?;

        // Delete from DLQ
        tx.execute(
            "DELETE FROM celers_dead_letter_queue WHERE id = ?",
            &[&dlq_id.to_string()],
        )
        .await
        .map_err(|e| CelersError::Other(format!("Failed to delete from DLQ: {}", e)))?;

        tx.commit()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to commit requeue: {}", e)))?;

        tracing::info!(original_task_id = %task_id, new_task_id = %new_task_id, task_name = %task_name, "Requeued task from DLQ");

        Ok(new_task_id)
    }

    /// Purge (delete) a task from the dead letter queue
    pub async fn purge_dlq(&self, dlq_id: &Uuid) -> Result<bool> {
        let affected = self
            .conn
            .execute(
                "DELETE FROM celers_dead_letter_queue WHERE id = ?",
                &[&dlq_id.to_string()],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to purge DLQ task: {}", e)))?;

        Ok(affected > 0)
    }

    /// Purge all tasks from the dead letter queue
    pub async fn purge_all_dlq(&self) -> Result<u64> {
        let affected = self
            .conn
            .execute("DELETE FROM celers_dead_letter_queue", &[])
            .await
            .map_err(|e| CelersError::Other(format!("Failed to purge all DLQ: {}", e)))?;

        tracing::info!(count = affected, "Purged all DLQ tasks");
        Ok(affected)
    }

    // ========== Health & Maintenance ==========

    /// Check database health
    pub async fn check_health(&self) -> Result<HealthStatus> {
        // Test connection and get MySQL version
        let rows = self
            .conn
            .query("SELECT VERSION() AS v", &[])
            .await
            .map_err(|e| CelersError::Other(format!("Health check failed: {}", e)))?;
        let version: String = rows
            .first()
            .map(|r| r.col("v"))
            .transpose()
            .map_err(|e| CelersError::Other(format!("Health check failed: {e}")))?
            .ok_or_else(|| CelersError::Other("Health check failed: no rows returned".into()))?;

        // Get queue counts
        let stats = self.get_statistics().await?;

        Ok(HealthStatus {
            healthy: true,
            connection_pool_size: self.configured_max_connections,
            idle_connections: 0,
            pending_tasks: stats.pending,
            processing_tasks: stats.processing,
            dlq_tasks: stats.dlq,
            database_version: version,
        })
    }

    /// Archive completed tasks older than the specified duration
    ///
    /// Returns the number of tasks archived (deleted).
    ///
    /// Retried on `ERROR 1213` — see [`Self::purge_all`] for why every bulk
    /// maintenance statement needs it.
    pub async fn archive_completed_tasks(&self, older_than: Duration) -> Result<u64> {
        let cutoff = Utc::now() - chrono::Duration::seconds(older_than.as_secs() as i64);
        // MySQL DATETIME/TIMESTAMP parameter convention — see `row_ext.rs`'s
        // "DateTime<Utc> parameter convention (MySQL)" section: bind
        // `.format("%Y-%m-%d %H:%M:%S%.6f")`, never `.to_rfc3339()`.
        let cutoff_str = cutoff.format("%Y-%m-%d %H:%M:%S%.6f").to_string();

        let affected = with_deadlock_retry("archive_completed_tasks", || async {
            self.conn
                .execute(
                    r#"
                DELETE FROM celers_tasks
                WHERE state IN ('completed', 'failed', 'cancelled')
                  AND completed_at < ?
                "#,
                    &[&cutoff_str],
                )
                .await
        })
        .await
        .map_err(|e| CelersError::Other(format!("Failed to archive tasks: {}", e)))?;

        tracing::info!(count = affected, cutoff = %cutoff, "Archived completed tasks");
        Ok(affected)
    }

    /// Clean up stuck processing tasks (tasks that have been processing too long)
    ///
    /// This can happen if a worker crashes. Tasks are requeued with incremented retry count.
    ///
    /// Retried on `ERROR 1213` — see [`Self::purge_all`] for why every bulk
    /// maintenance statement needs it.
    pub async fn recover_stuck_tasks(&self, stuck_threshold: Duration) -> Result<u64> {
        let cutoff = Utc::now() - chrono::Duration::seconds(stuck_threshold.as_secs() as i64);
        let cutoff_str = cutoff.format("%Y-%m-%d %H:%M:%S%.6f").to_string();

        let affected = with_deadlock_retry("recover_stuck_tasks", || async {
            self.conn
                .execute(
                    r#"
                UPDATE celers_tasks
                SET state = 'pending',
                    started_at = NULL,
                    worker_id = NULL,
                    error_message = 'Recovered from stuck processing state'
                WHERE state = 'processing'
                  AND started_at < ?
                "#,
                    &[&cutoff_str],
                )
                .await
        })
        .await
        .map_err(|e| CelersError::Other(format!("Failed to recover stuck tasks: {}", e)))?;

        if affected > 0 {
            tracing::warn!(count = affected, "Recovered stuck processing tasks");
        }
        Ok(affected)
    }

    /// Purge all tasks (dangerous - use with caution)
    ///
    /// # Deadlock retry
    ///
    /// This and its four siblings (`purge_by_state`, `purge_by_task_name`,
    /// `archive_completed_tasks`, `recover_stuck_tasks`) are the crate's
    /// *bulk* statements: one `DELETE`/`UPDATE` touching an unbounded number
    /// of rows and every secondary index on `celers_tasks`. That is precisely
    /// the shape most likely to form a lock cycle with the claim/ack/reject
    /// traffic running alongside it, and InnoDB resolves a cycle by rolling
    /// one transaction back with `ERROR 1213 (40001)`. Unretried, a single
    /// such rollback surfaced to the operator as a failed maintenance call
    /// even though nothing was wrong — the same spurious failure
    /// `with_deadlock_retry` was introduced for on the task-lifecycle
    /// paths. The whole statement is the retryable unit: the rollback is
    /// complete, so a restart is not a partial re-run.
    pub async fn purge_all(&self) -> Result<u64> {
        let affected = with_deadlock_retry("purge_all", || async {
            self.conn.execute("DELETE FROM celers_tasks", &[]).await
        })
        .await
        .map_err(|e| CelersError::Other(format!("Failed to purge all tasks: {}", e)))?;

        tracing::warn!(count = affected, "Purged all tasks");
        Ok(affected)
    }

    // ========== Database Monitoring ==========

    /// Get table size information for CeleRS tables
    pub async fn get_table_sizes(&self) -> Result<Vec<TableSizeInfo>> {
        let rows = self
            .conn
            .query(
                r#"
                SELECT
                    TABLE_NAME as table_name,
                    TABLE_ROWS as row_count,
                    DATA_LENGTH as data_size_bytes,
                    INDEX_LENGTH as index_size_bytes
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME LIKE 'celers_%'
                ORDER BY DATA_LENGTH DESC
                "#,
                &[],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get table sizes: {}", e)))?;

        let mut tables = Vec::with_capacity(rows.len());
        for row in rows {
            let row_count: Option<i64> = row
                .col("row_count")
                .map_err(|e| CelersError::Other(format!("Failed to get table sizes: {e}")))?;
            let data_size: Option<i64> = row
                .col("data_size_bytes")
                .map_err(|e| CelersError::Other(format!("Failed to get table sizes: {e}")))?;
            let index_size: Option<i64> = row
                .col("index_size_bytes")
                .map_err(|e| CelersError::Other(format!("Failed to get table sizes: {e}")))?;
            tables.push(TableSizeInfo {
                table_name: row
                    .col("table_name")
                    .map_err(|e| CelersError::Other(format!("Failed to get table sizes: {e}")))?,
                row_count: row_count.unwrap_or(0),
                data_size_bytes: data_size.unwrap_or(0),
                index_size_bytes: index_size.unwrap_or(0),
            });
        }
        Ok(tables)
    }

    /// Optimize CeleRS tables (MySQL-specific)
    ///
    /// This should be run periodically for optimal performance.
    pub async fn optimize_tables(&self) -> Result<()> {
        self.conn
            .execute("OPTIMIZE TABLE celers_tasks", &[])
            .await
            .map_err(|e| CelersError::Other(format!("Failed to optimize celers_tasks: {}", e)))?;

        self.conn
            .execute("OPTIMIZE TABLE celers_dead_letter_queue", &[])
            .await
            .map_err(|e| {
                CelersError::Other(format!(
                    "Failed to optimize celers_dead_letter_queue: {}",
                    e
                ))
            })?;

        self.conn
            .execute("OPTIMIZE TABLE celers_broker_results", &[])
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to optimize celers_broker_results: {}", e))
            })?;

        tracing::info!("Optimized all CeleRS tables");
        Ok(())
    }

    /// Analyze CeleRS tables for query optimization
    pub async fn analyze_tables(&self) -> Result<()> {
        self.conn
            .execute("ANALYZE TABLE celers_tasks", &[])
            .await
            .map_err(|e| CelersError::Other(format!("Failed to analyze celers_tasks: {}", e)))?;

        self.conn
            .execute("ANALYZE TABLE celers_dead_letter_queue", &[])
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to analyze celers_dead_letter_queue: {}", e))
            })?;

        self.conn
            .execute("ANALYZE TABLE celers_broker_results", &[])
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to analyze celers_broker_results: {}", e))
            })?;

        tracing::info!("Analyzed all CeleRS tables");
        Ok(())
    }

    // ========== Advanced Task Inspection ==========

    /// Get task counts grouped by task name
    ///
    /// Returns statistics for each unique task name including counts by state.
    pub async fn count_by_task_name(&self) -> Result<Vec<TaskNameCount>> {
        let rows = self
            .conn
            .query(
                r#"
                SELECT
                    task_name,
                    SUM(CASE WHEN state = 'pending' THEN 1 ELSE 0 END) as pending,
                    SUM(CASE WHEN state = 'processing' THEN 1 ELSE 0 END) as processing,
                    SUM(CASE WHEN state = 'completed' THEN 1 ELSE 0 END) as completed,
                    SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END) as failed,
                    COUNT(*) as total
                FROM celers_tasks
                GROUP BY task_name
                ORDER BY total DESC
                "#,
                &[],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to count by task name: {}", e)))?;

        let mut counts = Vec::with_capacity(rows.len());
        for row in rows {
            let pending: Option<String> = row
                .col("pending")
                .map_err(|e| CelersError::Other(format!("Failed to count by task name: {e}")))?;
            let processing: Option<String> = row
                .col("processing")
                .map_err(|e| CelersError::Other(format!("Failed to count by task name: {e}")))?;
            let completed: Option<String> = row
                .col("completed")
                .map_err(|e| CelersError::Other(format!("Failed to count by task name: {e}")))?;
            let failed: Option<String> = row
                .col("failed")
                .map_err(|e| CelersError::Other(format!("Failed to count by task name: {e}")))?;
            let total: i64 = row
                .col("total")
                .map_err(|e| CelersError::Other(format!("Failed to count by task name: {e}")))?;

            counts.push(TaskNameCount {
                task_name: row.col("task_name").map_err(|e| {
                    CelersError::Other(format!("Failed to count by task name: {e}"))
                })?,
                pending: pending.and_then(|d| d.parse().ok()).unwrap_or(0),
                processing: processing.and_then(|d| d.parse().ok()).unwrap_or(0),
                completed: completed.and_then(|d| d.parse().ok()).unwrap_or(0),
                failed: failed.and_then(|d| d.parse().ok()).unwrap_or(0),
                total,
            });
        }
        Ok(counts)
    }

    /// Get all currently processing tasks
    ///
    /// Useful for monitoring worker activity and detecting stuck tasks.
    pub async fn get_processing_tasks(&self, limit: i64, offset: i64) -> Result<Vec<TaskInfo>> {
        self.list_tasks(Some(DbTaskState::Processing), limit, offset)
            .await
    }

    /// Get tasks currently being processed by a specific worker
    pub async fn get_tasks_by_worker(&self, worker_id: &str) -> Result<Vec<TaskInfo>> {
        let rows = self
            .conn
            .query(
                r#"
                SELECT id, task_name, state, priority, retry_count, max_retries,
                       created_at, scheduled_at, started_at, completed_at, worker_id, error_message
                FROM celers_tasks
                WHERE worker_id = ?
                ORDER BY started_at DESC
                "#,
                &[&worker_id],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get tasks by worker: {}", e)))?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            let task_id_str: String = row
                .col("id")
                .map_err(|e| CelersError::Other(format!("Failed to get tasks by worker: {e}")))?;
            let state_str: String = row
                .col("state")
                .map_err(|e| CelersError::Other(format!("Failed to get tasks by worker: {e}")))?;
            tasks.push(TaskInfo {
                id: Uuid::parse_str(&task_id_str)
                    .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?,
                task_name: row.col("task_name").map_err(|e| {
                    CelersError::Other(format!("Failed to get tasks by worker: {e}"))
                })?,
                state: state_str.parse()?,
                priority: row.col("priority").map_err(|e| {
                    CelersError::Other(format!("Failed to get tasks by worker: {e}"))
                })?,
                retry_count: row.col("retry_count").map_err(|e| {
                    CelersError::Other(format!("Failed to get tasks by worker: {e}"))
                })?,
                max_retries: row.col("max_retries").map_err(|e| {
                    CelersError::Other(format!("Failed to get tasks by worker: {e}"))
                })?,
                created_at: row.col("created_at").map_err(|e| {
                    CelersError::Other(format!("Failed to get tasks by worker: {e}"))
                })?,
                scheduled_at: row.col("scheduled_at").map_err(|e| {
                    CelersError::Other(format!("Failed to get tasks by worker: {e}"))
                })?,
                started_at: row.col("started_at").map_err(|e| {
                    CelersError::Other(format!("Failed to get tasks by worker: {e}"))
                })?,
                completed_at: row.col("completed_at").map_err(|e| {
                    CelersError::Other(format!("Failed to get tasks by worker: {e}"))
                })?,
                worker_id: row.col("worker_id").map_err(|e| {
                    CelersError::Other(format!("Failed to get tasks by worker: {e}"))
                })?,
                error_message: row.col("error_message").map_err(|e| {
                    CelersError::Other(format!("Failed to get tasks by worker: {e}"))
                })?,
            });
        }
        Ok(tasks)
    }

    /// List scheduled tasks (tasks with scheduled_at in the future)
    ///
    /// Scoped to this broker's queue, like `queue_size` and `get_statistics`.
    pub async fn list_scheduled_tasks(
        &self,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<ScheduledTaskInfo>> {
        let rows = self
            .conn
            .query(
                r#"
                SELECT id, task_name, priority, scheduled_at, created_at,
                       TIMESTAMPDIFF(SECOND, NOW(), scheduled_at) as delay_remaining_secs
                FROM celers_tasks
                WHERE queue_name = ?
                  AND state = 'pending'
                  AND scheduled_at > NOW()
                ORDER BY scheduled_at ASC
                LIMIT ? OFFSET ?
                "#,
                &[&self.queue_name, &limit, &offset],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to list scheduled tasks: {}", e)))?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            let task_id_str: String = row
                .col("id")
                .map_err(|e| CelersError::Other(format!("Failed to list scheduled tasks: {e}")))?;
            let delay: Option<i64> = row
                .col("delay_remaining_secs")
                .map_err(|e| CelersError::Other(format!("Failed to list scheduled tasks: {e}")))?;
            tasks.push(ScheduledTaskInfo {
                id: Uuid::parse_str(&task_id_str)
                    .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?,
                task_name: row.col("task_name").map_err(|e| {
                    CelersError::Other(format!("Failed to list scheduled tasks: {e}"))
                })?,
                priority: row.col("priority").map_err(|e| {
                    CelersError::Other(format!("Failed to list scheduled tasks: {e}"))
                })?,
                scheduled_at: row.col("scheduled_at").map_err(|e| {
                    CelersError::Other(format!("Failed to list scheduled tasks: {e}"))
                })?,
                created_at: row.col("created_at").map_err(|e| {
                    CelersError::Other(format!("Failed to list scheduled tasks: {e}"))
                })?,
                delay_remaining_secs: delay.unwrap_or(0),
            });
        }
        Ok(tasks)
    }

    /// Count scheduled tasks (tasks with scheduled_at in the future)
    ///
    /// Scoped to this broker's queue, like [`Self::list_scheduled_tasks`].
    pub async fn count_scheduled_tasks(&self) -> Result<i64> {
        let rows = self
            .conn
            .query(
                r#"
                SELECT COUNT(*) AS c FROM celers_tasks
                WHERE queue_name = ? AND state = 'pending' AND scheduled_at > NOW()
                "#,
                &[&self.queue_name],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to count scheduled tasks: {}", e)))?;

        let count: i64 = rows
            .first()
            .map(|r| r.col("c"))
            .transpose()
            .map_err(|e| CelersError::Other(format!("Failed to count scheduled tasks: {e}")))?
            .unwrap_or(0);
        Ok(count)
    }

    // ========== Task Updates ==========

    /// Update the error message on a task
    ///
    /// Useful for recording error details during task execution.
    pub async fn update_error_message(
        &self,
        task_id: &TaskId,
        error_message: &str,
    ) -> Result<bool> {
        let affected = self
            .conn
            .execute(
                r#"
                UPDATE celers_tasks
                SET error_message = ?
                WHERE id = ?
                "#,
                &[&error_message, &task_id.to_string()],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to update error message: {}", e)))?;

        Ok(affected > 0)
    }

    /// Set the worker ID on a processing task
    ///
    /// This allows tracking which worker is processing which task.
    pub async fn set_worker_id(&self, task_id: &TaskId, worker_id: &str) -> Result<bool> {
        let affected = self
            .conn
            .execute(
                r#"
                UPDATE celers_tasks
                SET worker_id = ?
                WHERE id = ? AND state = 'processing'
                "#,
                &[&worker_id, &task_id.to_string()],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to set worker ID: {}", e)))?;

        Ok(affected > 0)
    }
    // ========== Selective Cleanup ==========

    /// Purge tasks by state
    ///
    /// Deletes all tasks with the specified state. Use with caution.
    ///
    /// Retried on `ERROR 1213` — see [`Self::purge_all`] for why every bulk
    /// maintenance statement needs it.
    pub async fn purge_by_state(&self, state: DbTaskState) -> Result<u64> {
        let state_str = state.to_string();
        let affected = with_deadlock_retry("purge_by_state", || async {
            self.conn
                .execute("DELETE FROM celers_tasks WHERE state = ?", &[&state_str])
                .await
        })
        .await
        .map_err(|e| CelersError::Other(format!("Failed to purge tasks by state: {}", e)))?;

        tracing::info!(state = %state, count = affected, "Purged tasks by state");
        Ok(affected)
    }

    /// Purge completed tasks only
    pub async fn purge_completed(&self) -> Result<u64> {
        self.purge_by_state(DbTaskState::Completed).await
    }

    /// Purge failed tasks only
    pub async fn purge_failed(&self) -> Result<u64> {
        self.purge_by_state(DbTaskState::Failed).await
    }

    /// Purge cancelled tasks only
    pub async fn purge_cancelled(&self) -> Result<u64> {
        self.purge_by_state(DbTaskState::Cancelled).await
    }

    /// Purge tasks by task name
    ///
    /// Deletes all tasks with the specified task name. Use with caution.
    ///
    /// Retried on `ERROR 1213` — see [`Self::purge_all`] for why every bulk
    /// maintenance statement needs it.
    pub async fn purge_by_task_name(&self, task_name: &str) -> Result<u64> {
        let affected = with_deadlock_retry("purge_by_task_name", || async {
            self.conn
                .execute(
                    "DELETE FROM celers_tasks WHERE task_name = ?",
                    &[&task_name],
                )
                .await
        })
        .await
        .map_err(|e| CelersError::Other(format!("Failed to purge tasks by name: {}", e)))?;

        tracing::info!(task_name = %task_name, count = affected, "Purged tasks by name");
        Ok(affected)
    }

    // ========== Migration Management ==========

    /// List all applied migrations
    pub async fn list_migrations(&self) -> Result<Vec<MigrationInfo>> {
        let rows = self
            .conn
            .query(
                r#"
                SELECT version, name, applied_at
                FROM celers_migrations
                ORDER BY applied_at ASC
                "#,
                &[],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to list migrations: {}", e)))?;

        let mut migrations = Vec::with_capacity(rows.len());
        for row in rows {
            migrations.push(MigrationInfo {
                version: row
                    .col("version")
                    .map_err(|e| CelersError::Other(format!("Failed to list migrations: {e}")))?,
                name: row
                    .col("name")
                    .map_err(|e| CelersError::Other(format!("Failed to list migrations: {e}")))?,
                applied_at: row
                    .col("applied_at")
                    .map_err(|e| CelersError::Other(format!("Failed to list migrations: {e}")))?,
            });
        }
        Ok(migrations)
    }

    // ========== Query Performance Tracking ==========

    /// Get query performance statistics from MySQL performance schema
    ///
    /// Note: This requires performance_schema to be enabled in MySQL configuration.
    pub async fn get_query_stats(&self) -> Result<Vec<QueryStats>> {
        let rows = self
            .conn
            .query(
                r#"
                SELECT
                    DIGEST_TEXT as query_name,
                    COUNT_STAR as execution_count,
                    SUM_TIMER_WAIT / 1000000000 as total_time_ms,
                    AVG_TIMER_WAIT / 1000000000 as avg_time_ms,
                    MIN_TIMER_WAIT / 1000000000 as min_time_ms,
                    MAX_TIMER_WAIT / 1000000000 as max_time_ms
                FROM performance_schema.events_statements_summary_by_digest
                WHERE SCHEMA_NAME = DATABASE()
                  AND DIGEST_TEXT LIKE '%celers%'
                ORDER BY SUM_TIMER_WAIT DESC
                LIMIT 50
                "#,
                &[],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get query stats: {}", e)))?;

        let mut stats = Vec::with_capacity(rows.len());
        for row in rows {
            let query_name: String = row
                .col("query_name")
                .map_err(|e| CelersError::Other(format!("Failed to get query stats: {e}")))?;
            let execution_count: i64 = row
                .col("execution_count")
                .map_err(|e| CelersError::Other(format!("Failed to get query stats: {e}")))?;
            let total_time: Option<String> = row
                .col("total_time_ms")
                .map_err(|e| CelersError::Other(format!("Failed to get query stats: {e}")))?;
            let avg_time: Option<String> = row
                .col("avg_time_ms")
                .map_err(|e| CelersError::Other(format!("Failed to get query stats: {e}")))?;
            let min_time: Option<String> = row
                .col("min_time_ms")
                .map_err(|e| CelersError::Other(format!("Failed to get query stats: {e}")))?;
            let max_time: Option<String> = row
                .col("max_time_ms")
                .map_err(|e| CelersError::Other(format!("Failed to get query stats: {e}")))?;

            stats.push(QueryStats {
                query_name,
                execution_count,
                total_time_ms: total_time.and_then(|d| d.parse().ok()).unwrap_or(0),
                avg_time_ms: avg_time.and_then(|d| d.parse().ok()).unwrap_or(0.0),
                min_time_ms: min_time.and_then(|d| d.parse().ok()).unwrap_or(0),
                max_time_ms: max_time.and_then(|d| d.parse().ok()).unwrap_or(0),
            });
        }
        Ok(stats)
    }

    /// Reset query performance statistics
    ///
    /// Clears the performance_schema statistics. Useful for benchmarking.
    pub async fn reset_query_stats(&self) -> Result<()> {
        self.conn
            .execute("CALL sys.ps_truncate_all_tables(FALSE)", &[])
            .await
            .map_err(|e| CelersError::Other(format!("Failed to reset query stats: {}", e)))?;

        tracing::info!("Reset query performance statistics");
        Ok(())
    }

    // ========== Index Usage and Query Optimization ==========

    /// Get index statistics for CeleRS tables
    ///
    /// Returns information about all indexes on CeleRS tables including cardinality
    /// and whether they are unique.
    pub async fn get_index_stats(&self) -> Result<Vec<IndexStats>> {
        let rows = self
            .conn
            .query(
                r#"
                SELECT
                    TABLE_NAME as table_name,
                    INDEX_NAME as index_name,
                    CARDINALITY as cardinality,
                    NON_UNIQUE as non_unique
                FROM information_schema.STATISTICS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME LIKE 'celers_%'
                ORDER BY TABLE_NAME, INDEX_NAME
                "#,
                &[],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get index stats: {}", e)))?;

        let mut stats = Vec::with_capacity(rows.len());
        for row in rows {
            let cardinality: Option<i64> = row
                .col("cardinality")
                .map_err(|e| CelersError::Other(format!("Failed to get index stats: {e}")))?;
            let non_unique: i32 = row
                .col("non_unique")
                .map_err(|e| CelersError::Other(format!("Failed to get index stats: {e}")))?;
            stats.push(IndexStats {
                table_name: row
                    .col("table_name")
                    .map_err(|e| CelersError::Other(format!("Failed to get index stats: {e}")))?,
                index_name: row
                    .col("index_name")
                    .map_err(|e| CelersError::Other(format!("Failed to get index stats: {e}")))?,
                cardinality: cardinality.unwrap_or(0),
                unique_values: non_unique == 0,
            });
        }
        Ok(stats)
    }

    /// Explain a query plan for the dequeue operation
    ///
    /// Returns the MySQL EXPLAIN output for the dequeue query.
    /// Useful for query optimization and performance tuning.
    ///
    /// Explains the *actual* statement `dequeue` runs (built by
    /// `sql_text::dequeue_candidate_sql`), not a hand-copied approximation of
    /// it — the previous copy had already drifted away from the real query's
    /// clause order and predicates.
    ///
    /// A claim is two statements (see the crate-internal `sql_text` module):
    /// this reports the
    /// plan of the *candidate scan*, which is the half whose plan is worth
    /// tuning — it is the one that picks an index and may filesort. The
    /// second half is a `FORCE INDEX (PRIMARY)` lookup on a bound id list, so
    /// its plan is fixed by construction and carries no tuning signal.
    pub async fn explain_dequeue(&self) -> Result<Vec<QueryPlan>> {
        let explain_sql = format!("EXPLAIN {}", sql_text::dequeue_candidate_sql());
        let candidate_limit = sql_text::claim_candidate_limit(1);
        let rows = self
            .conn
            .query(&explain_sql, &[&self.queue_name, &candidate_limit])
            .await
            .map_err(|e| CelersError::Other(format!("Failed to explain query: {}", e)))?;

        Self::rows_to_query_plans(rows)
    }

    /// Explain a custom query plan
    ///
    /// Returns the MySQL EXPLAIN output for any SELECT query.
    ///
    /// # Safety
    ///
    /// `query` is interpolated directly into the `EXPLAIN <query>` SQL text
    /// because EXPLAIN takes a whole statement, not a single bindable value,
    /// so there is no way to express this as a parameterized query. As
    /// defense-in-depth, this method rejects any input that (after
    /// trimming) does not case-insensitively start with `SELECT`, and
    /// rejects any input containing a `;` (blocking simple statement
    /// stacking). This guard reduces but does not eliminate risk; callers
    /// must still treat `query` as trusted, non-attacker-derived input.
    pub async fn explain_query(&self, query: &str) -> Result<Vec<QueryPlan>> {
        let trimmed = query.trim();
        let starts_with_select = trimmed
            .get(..6)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("SELECT"));
        if !starts_with_select {
            return Err(CelersError::Other(
                "explain_query: input must start with SELECT".to_string(),
            ));
        }
        if trimmed.contains(';') {
            return Err(CelersError::Other(
                "explain_query: input must not contain ';'".to_string(),
            ));
        }

        let explain_query = format!("EXPLAIN {}", query);
        let rows = self
            .conn
            .query(&explain_query, &[])
            .await
            .map_err(|e| CelersError::Other(format!("Failed to explain query: {}", e)))?;

        Self::rows_to_query_plans(rows)
    }

    /// Shared row-to-`QueryPlan` mapping for [`explain_dequeue`](Self::explain_dequeue)
    /// and [`explain_query`](Self::explain_query).
    fn rows_to_query_plans(rows: Vec<oxisql_core::Row>) -> Result<Vec<QueryPlan>> {
        let mut plans = Vec::with_capacity(rows.len());
        for row in rows {
            let rows_examined: Option<i64> = row.col("rows").ok();
            let filtered: Option<String> = row.col("filtered").ok();
            plans.push(QueryPlan {
                id: row
                    .col("id")
                    .map_err(|e| CelersError::Other(format!("Failed to explain query: {e}")))?,
                select_type: row
                    .col("select_type")
                    .map_err(|e| CelersError::Other(format!("Failed to explain query: {e}")))?,
                table: row.col("table").ok(),
                query_type: row.col("type").ok(),
                possible_keys: row.col("possible_keys").ok(),
                key_used: row.col("key").ok(),
                key_length: row.col("key_len").ok(),
                rows_examined,
                filtered: filtered.and_then(|d| d.parse().ok()),
                extra: row.col("Extra").ok(),
            });
        }
        Ok(plans)
    }

    /// Check if indexes are being used effectively
    ///
    /// Analyzes the explain plan for common queries and returns warnings
    /// if indexes are not being used properly.
    pub async fn check_index_usage(&self) -> Result<Vec<String>> {
        let mut warnings = Vec::new();

        // Check dequeue query
        let dequeue_plan = self.explain_dequeue().await?;
        for plan in dequeue_plan {
            if plan.key_used.is_none() {
                warnings.push(format!(
                    "Dequeue query on table {:?} is not using an index (full table scan)",
                    plan.table
                ));
            }
            if let Some(extra) = &plan.extra {
                if extra.contains("Using filesort") {
                    warnings.push(
                        "Dequeue query requires filesort - the \
                         idx_tasks_queue_dequeue composite index on \
                         (queue_name, state, scheduled_at, priority, created_at) \
                         from migration 009 should cover it; check that \
                         migrations are up to date and run ANALYZE TABLE"
                            .to_string(),
                    );
                }
            }
        }

        // Check index cardinality
        let index_stats = self.get_index_stats().await?;
        for stat in index_stats {
            if stat.cardinality == 0 && !stat.index_name.eq("PRIMARY") {
                warnings.push(format!(
                    "Index {} on table {} has zero cardinality - consider running ANALYZE TABLE",
                    stat.index_name, stat.table_name
                ));
            }
        }

        Ok(warnings)
    }

    // ========== Connection Diagnostics and Performance Monitoring ==========

    /// Get connection pool diagnostics
    ///
    /// Returns detailed information about the connection pool state.
    ///
    /// `oxisql_mysql::MyConnection` exposes no pool-introspection API (see
    /// the `configured_max_connections` field doc), so unlike the
    /// pre-migration `sqlx`-backed version this reports the configured
    /// ceiling with idle/active counts as unknown (`0`) rather than a live
    /// snapshot.
    pub fn get_connection_diagnostics(&self) -> ConnectionDiagnostics {
        ConnectionDiagnostics {
            total_connections: 0,
            idle_connections: 0,
            active_connections: 0,
            max_connections: self.configured_max_connections,
            connection_wait_time_ms: None, // MySQL doesn't expose this easily
            pool_utilization_percent: 0.0,
        }
    }

    /// Get comprehensive performance metrics snapshot
    ///
    /// This method collects various performance metrics including queue sizes,
    /// connection pool status, and database statistics.
    pub async fn get_performance_metrics(&self) -> Result<PerformanceMetrics> {
        let stats = self.get_statistics().await?;
        let conn_diag = self.get_connection_diagnostics();

        let now_ms = chrono::Utc::now().timestamp_millis();
        let window_start_ms = self.enqueue_window_start_ms.load(Ordering::Relaxed);
        let elapsed_ms = (now_ms - window_start_ms).max(1);
        let count = self.enqueue_count.swap(0, Ordering::Relaxed);
        // Reset window start to now for the next measurement period
        self.enqueue_window_start_ms
            .store(now_ms, Ordering::Relaxed);
        let tasks_per_second = (count as f64 / elapsed_ms as f64) * 1000.0;

        // Get average query times from performance schema (if available)
        let (avg_dequeue_ms, avg_enqueue_ms) = match self.get_query_stats().await {
            Ok(stats) => {
                let dequeue_stat = stats
                    .iter()
                    .find(|s| {
                        s.query_name.contains("SELECT") && s.query_name.contains("celers_tasks")
                    })
                    .map(|s| s.avg_time_ms)
                    .unwrap_or(0.0);

                let enqueue_stat = stats
                    .iter()
                    .find(|s| {
                        s.query_name.contains("INSERT") && s.query_name.contains("celers_tasks")
                    })
                    .map(|s| s.avg_time_ms)
                    .unwrap_or(0.0);

                (dequeue_stat, enqueue_stat)
            }
            Err(_) => (0.0, 0.0),
        };

        Ok(PerformanceMetrics {
            timestamp: Utc::now(),
            tasks_per_second,
            avg_dequeue_time_ms: avg_dequeue_ms,
            avg_enqueue_time_ms: avg_enqueue_ms,
            queue_depth: stats.pending,
            processing_tasks: stats.processing,
            dlq_size: stats.dlq,
            connection_pool: conn_diag,
        })
    }

    /// Check if the broker is healthy and ready to process tasks
    ///
    /// Returns true if:
    /// - Database connection is active
    /// - No critical errors detected
    ///
    /// Note: the pre-migration `sqlx`-backed version also checked live
    /// connection-pool idle/saturation state; `oxisql_mysql::MyConnection`
    /// exposes no equivalent introspection (see the
    /// `configured_max_connections` field doc), so that check is no longer
    /// possible and has been dropped from this method.
    pub async fn is_ready(&self) -> bool {
        self.conn.query("SELECT VERSION()", &[]).await.is_ok()
    }

    /// Get detailed database server variables
    ///
    /// Returns key MySQL server configuration variables that affect performance.
    pub async fn get_server_variables(&self) -> Result<std::collections::HashMap<String, String>> {
        let rows = self
            .conn
            .query(
                r#"
                SHOW VARIABLES WHERE Variable_name IN (
                    'max_connections',
                    'innodb_buffer_pool_size',
                    'innodb_log_file_size',
                    'query_cache_size',
                    'query_cache_type',
                    'innodb_flush_log_at_trx_commit',
                    'innodb_flush_method',
                    'binlog_format',
                    'expire_logs_days'
                )
                "#,
                &[],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get server variables: {}", e)))?;

        let mut variables = std::collections::HashMap::new();
        for row in rows {
            let var_name: String = row
                .col("Variable_name")
                .map_err(|e| CelersError::Other(format!("Failed to get server variables: {e}")))?;
            let var_value: String = row
                .col("Value")
                .map_err(|e| CelersError::Other(format!("Failed to get server variables: {e}")))?;
            variables.insert(var_name, var_value);
        }

        Ok(variables)
    }
}
