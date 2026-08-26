//! Core PostgresBroker struct and construction methods

use celers_core::{Broker, CelersError, Result, SerializedTask, TaskId};
use chrono::Utc;
use serde_json::json;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use crate::pool::{PgPool, PooledConnection, DEFAULT_POOL_SIZE};
use crate::row_ext::{json_from_row, json_param, uuid_param};
use crate::sql;
use crate::tls_mode;
use crate::types::{
    HookContext, RetentionConfig, RetryStrategy, TaskHook, TaskHooks, TraceContext,
};

#[cfg(feature = "metrics")]
use celers_metrics::{TASKS_ENQUEUED_BY_TYPE, TASKS_ENQUEUED_TOTAL};

/// Advisory-lock key serialising [`PostgresBroker::migrate`] across every
/// process pointed at one database.
///
/// PostgreSQL advisory locks live in a single flat `bigint` namespace shared
/// by every application on the server, so the value is a fixed, deliberately
/// unusual constant rather than a small integer that user code
/// ([`PostgresBroker::advisory_lock`] takes a caller-supplied `i64`) might
/// pick by accident: the ASCII bytes of `"celers"` followed by `0x0001`, the
/// "schema migration" slot.
pub const MIGRATION_ADVISORY_LOCK_ID: i64 = 0x6365_6c65_7273_0001;

/// PostgreSQL-based broker implementation using SKIP LOCKED
pub struct PostgresBroker {
    /// The connection pool backing every statement this crate runs.
    ///
    /// This used to be a single `oxisql_postgres::PgConnection`. That type is
    /// `Clone` but internally `Arc<Mutex<tokio_postgres::Client>>`, so every
    /// clone shared ONE connection: all database work in the process was
    /// serialised behind one mutex, and because
    /// `Connection::transaction()` takes an *owned* guard held until
    /// commit/rollback, a single `dequeue()` blocked every concurrent
    /// enqueue/ack/monitoring query for four network round trips. That
    /// defeated `FOR UPDATE SKIP LOCKED` within a process and made
    /// [`PostgresBroker::with_pool_config`]'s `max_connections` argument a
    /// no-op.
    ///
    /// [`PgPool`] owns `pool_size` independent connections and hands one out
    /// per operation, with per-slot broken-connection detection and
    /// reconnect-with-backoff, so a dropped connection no longer bricks the
    /// broker for the process lifetime. Its API is method-compatible with the
    /// old field (`execute`/`query`/`execute_batch`), with transactions taken
    /// through an explicit `acquire()` first.
    pub(crate) conn: PgPool,
    /// The connection string this broker was constructed with.
    ///
    /// Retained so `notifications.rs` can open its own dedicated
    /// `PgConnection` for LISTEN/NOTIFY (a long-lived LISTEN connection
    /// should not share the query connection, per Postgres best practice).
    pub(crate) database_url: String,
    /// Logical queue label for multi-tenancy.
    ///
    /// As of migration `007_queue_identity.sql` this is a REAL column on
    /// `celers_tasks` and `celers_dead_letter_queue` (backfilled from the
    /// legacy `metadata->>'queue'` label, which is still written for
    /// backwards compatibility). Every query in this crate scopes itself to it
    /// by binding it as a parameter — it is never spliced into SQL text as a
    /// table name, and it is validated at construction
    /// (`[A-Za-z0-9_-]{1,64}`) so it cannot carry SQL syntax anywhere.
    pub(crate) queue_name: String,
    pub(crate) paused: AtomicBool,
    pub(crate) retry_strategy: RetryStrategy,
    pub(crate) hooks: Arc<tokio::sync::RwLock<TaskHooks>>,
    /// How long a durable revocation record survives, in seconds — see
    /// `revocation.rs`. Mirrors `celers-broker-redis`'s
    /// `DEFAULT_REVOCATION_TTL_SECS`/`with_revocation_ttl`.
    pub(crate) revocation_ttl_secs: u64,
    /// Advisory locks currently held through the *deprecated*
    /// `try_advisory_lock` / `advisory_lock` / `release_advisory_lock` trio,
    /// keyed by lock id.
    ///
    /// `pg_advisory_lock` is **session**-scoped, so a lock taken on one
    /// pooled connection can only be released on that same connection. The
    /// trio's signatures carry nothing between the acquire call and the
    /// release call, so the connection has to be parked somewhere for the
    /// pair to be sound at all — that is what this map is. Each entry pins
    /// the [`PooledConnection`] whose session holds the lock, exactly the way
    /// [`crate::AdvisoryLockGuard`] does for the non-deprecated API, and
    /// `release_advisory_lock` takes the entry back out and unlocks on it.
    ///
    /// The guard API is preferred precisely because this map cannot express
    /// ownership: a caller that forgets to release pins one of the broker's
    /// pool slots for the broker's whole lifetime, whereas a dropped guard is
    /// at least noisy about it. See `advisory_lock.rs`.
    pub(crate) advisory_locks:
        Arc<tokio::sync::Mutex<std::collections::HashMap<i64, PooledConnection>>>,
}

/// Default lifetime of a durable revocation record: 24 hours, matching
/// `celers-broker-redis::DEFAULT_REVOCATION_TTL_SECS`. Long enough to outlive
/// any task's `max_retries` backoff window, short enough that the table does
/// not accumulate ids forever for tasks nobody re-enqueues under the same id.
pub const DEFAULT_REVOCATION_TTL_SECS: u64 = 86_400;

impl PostgresBroker {
    /// Create a new PostgreSQL broker
    ///
    /// # Arguments
    /// * `database_url` - PostgreSQL connection string (e.g., "postgres://user:pass@localhost/db")
    /// * `queue_name` - Logical queue name for multi-tenancy (optional, defaults to "default")
    pub async fn new(database_url: &str) -> Result<Self> {
        Self::with_queue(database_url, "default").await
    }

    /// Create a new PostgreSQL broker with a specific queue name
    ///
    /// Uses [`crate::pool::DEFAULT_POOL_SIZE`] connections; call
    /// [`PostgresBroker::with_pool_config`] to size the pool explicitly.
    ///
    /// # Errors
    ///
    /// Returns an error if `queue_name` is not a valid queue label
    /// (`[A-Za-z0-9_-]`, 1..=64 characters) or if the database is
    /// unreachable.
    pub async fn with_queue(database_url: &str, queue_name: &str) -> Result<Self> {
        Self::with_pool_config(database_url, queue_name, DEFAULT_POOL_SIZE, 30).await
    }

    /// Create a new PostgreSQL broker with custom pool configuration
    ///
    /// # Arguments
    /// * `database_url` - PostgreSQL connection string
    /// * `queue_name` - Logical queue name, `[A-Za-z0-9_-]{1,64}`
    /// * `max_connections` - Number of independent connections the broker
    ///   opens. This is now honoured: the broker keeps that many connection
    ///   slots and hands one out per operation, so concurrent `dequeue`s
    ///   really do run in parallel and `FOR UPDATE SKIP LOCKED` buys
    ///   in-process concurrency instead of only cross-process concurrency.
    ///   Clamped to `1..=`[`crate::pool::MAX_POOL_SIZE`]. One connection is
    ///   opened eagerly (so a bad URL still fails here); the rest are opened
    ///   on first use.
    /// * `acquire_timeout_secs` - Timeout applied to establishing a
    ///   connection (seconds)
    pub async fn with_pool_config(
        database_url: &str,
        queue_name: &str,
        max_connections: u32,
        acquire_timeout_secs: u64,
    ) -> Result<Self> {
        // Validate the queue label once, here, so that no downstream query
        // can ever be handed a name carrying SQL syntax — regardless of
        // whether the caller derived it from a tenant id, a header or a
        // config file.
        sql::validate_queue_name(queue_name).map_err(CelersError::Other)?;

        // TLS mode is derived from `database_url`'s `sslmode` query
        // parameter (see `tls_mode.rs`): a URL with `sslmode=require` (or
        // `verify-ca`/`verify-full`/`prefer`/`allow`) gets a real TLS
        // connection, matching the pre-`oxisql`-migration `sqlx` behavior.
        // Absent/`sslmode=disable` still resolves to `TlsMode::Disabled`,
        // so plain-text callers are unaffected.
        let tls_mode = tls_mode::pg_tls_mode_for_url(database_url)
            .map_err(|e| CelersError::Other(format!("Failed to resolve TLS mode: {}", e)))?;

        let conn = PgPool::connect(
            database_url,
            tls_mode,
            max_connections,
            Some(Duration::from_secs(acquire_timeout_secs)),
        )
        .await
        .map_err(|e| {
            CelersError::Other(format!("Failed to connect to database (oxisql): {}", e))
        })?;

        Ok(Self {
            conn,
            database_url: database_url.to_string(),
            queue_name: queue_name.to_string(),
            paused: AtomicBool::new(false),
            retry_strategy: RetryStrategy::default(),
            hooks: Arc::new(tokio::sync::RwLock::new(TaskHooks::new())),
            revocation_ttl_secs: DEFAULT_REVOCATION_TTL_SECS,
            advisory_locks: Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
        })
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

    /// Set the retry strategy for failed tasks
    ///
    /// This can be called on an existing broker instance to change the retry behavior.
    pub fn set_retry_strategy(&mut self, strategy: RetryStrategy) {
        self.retry_strategy = strategy;
        tracing::info!(strategy = ?strategy, "Updated retry strategy");
    }

    /// Get the current retry strategy
    pub fn retry_strategy(&self) -> RetryStrategy {
        self.retry_strategy
    }

    /// Add a lifecycle hook
    ///
    /// Lifecycle hooks allow you to inject custom logic at key points in task processing.
    /// Multiple hooks of the same type can be registered and will be executed in order.
    ///
    /// # Arguments
    /// * `hook` - The hook to add
    ///
    /// # Example
    /// ```no_run
    /// use celers_broker_postgres::{PostgresBroker, TaskHook, HookContext};
    /// use celers_core::{Result, SerializedTask};
    /// use std::sync::Arc;
    ///
    /// fn log_hook() -> Arc<dyn Fn(&HookContext, &SerializedTask) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> + Send + Sync> {
    ///     Arc::new(|ctx: &HookContext, task: &SerializedTask| {
    ///         let timestamp = ctx.timestamp;
    ///         let task_name = task.metadata.name.clone();
    ///         Box::pin(async move {
    ///             println!("Task {} enqueued at {}", task_name, timestamp);
    ///             Ok(())
    ///         })
    ///     })
    /// }
    ///
    /// # async fn example() -> Result<()> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// broker.add_hook(TaskHook::AfterEnqueue(log_hook())).await;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn add_hook(&self, hook: TaskHook) {
        let mut hooks = self.hooks.write().await;
        hooks.add(hook);
    }

    /// Clear all hooks of a specific type
    ///
    /// Removes all registered hooks, allowing you to reset hook behavior.
    pub async fn clear_hooks(&self) {
        let mut hooks = self.hooks.write().await;
        *hooks = TaskHooks::new();
    }

    /// Enqueue a task with distributed tracing context
    ///
    /// Adds W3C Trace Context to database metadata for end-to-end observability.
    ///
    /// # Arguments
    /// * `task` - The task to enqueue
    /// * `trace_ctx` - The trace context to attach
    ///
    /// # Returns
    /// The task ID
    ///
    /// # Example
    /// ```no_run
    /// use celers_broker_postgres::{PostgresBroker, TraceContext};
    /// use celers_core::SerializedTask;
    ///
    /// # async fn example() -> celers_core::Result<()> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let task = SerializedTask::new("my_task".to_string(), vec![1, 2, 3]);
    ///
    /// // Create trace context
    /// let trace_ctx = TraceContext::new(
    ///     "4bf92f3577b34da6a3ce929d0e0e4736",
    ///     "00f067aa0ba902b7"
    /// );
    ///
    /// // Enqueue with trace context
    /// broker.enqueue_with_trace_context(task, trace_ctx).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn enqueue_with_trace_context(
        &self,
        task: SerializedTask,
        trace_ctx: TraceContext,
    ) -> Result<TaskId> {
        let task_id = task.metadata.id;

        // Run before_enqueue hooks
        let hook_ctx = HookContext {
            queue_name: self.queue_name.clone(),
            task_id: Some(task_id),
            timestamp: Utc::now(),
            metadata: json!({}),
        };
        {
            let hooks = self.hooks.read().await;
            hooks.run_before_enqueue(&hook_ctx, &task).await?;
        }

        let mut db_metadata = json!({
            "queue": self.queue_name,
            "enqueued_at": chrono::Utc::now().to_rfc3339(),
            "trace_context": {
                "trace_id": trace_ctx.trace_id,
                "span_id": trace_ctx.span_id,
                "trace_flags": trace_ctx.trace_flags,
                "trace_state": trace_ctx.trace_state,
            }
        });

        // Merge task metadata if present
        if let Ok(task_meta) = serde_json::to_value(&task.metadata) {
            if let Some(obj) = db_metadata.as_object_mut() {
                if let Some(meta_obj) = task_meta.as_object() {
                    for (k, v) in meta_obj {
                        if k != "trace_context" {
                            // Don't override trace context
                            obj.insert(k.clone(), v.clone());
                        }
                    }
                }
            }
        }

        // Shares `sql::INSERT_TASK_NOW` with `broker_trait.rs`'s `enqueue()`
        // so the two can never drift apart: same column list (including the
        // real `queue_name` column, without which the task would be invisible
        // to this broker's queue-scoped `dequeue`), same `$6::text::jsonb`
        // cast for the metadata parameter.
        let task_id_param = uuid_param(&task_id);
        let metadata_param = json_param(&db_metadata);
        self.conn
            .execute(
                sql::INSERT_TASK_NOW,
                &[
                    &task_id_param,
                    &task.metadata.name,
                    &task.payload,
                    &task.metadata.priority,
                    &(task.metadata.max_retries as i32),
                    &metadata_param,
                    &self.queue_name,
                ],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to enqueue task with trace: {}", e)))?;

        #[cfg(feature = "metrics")]
        {
            TASKS_ENQUEUED_TOTAL.inc();
            TASKS_ENQUEUED_BY_TYPE
                .with_label_values(&[&task.metadata.name])
                .inc();
        }

        // Run after_enqueue hooks
        {
            let hooks = self.hooks.read().await;
            hooks.run_after_enqueue(&hook_ctx, &task).await?;
        }

        Ok(task_id)
    }

    /// Extract distributed tracing context from a task's database metadata
    ///
    /// Retrieves W3C Trace Context that was stored with the task.
    ///
    /// # Arguments
    /// * `task_id` - The task ID to extract trace context for
    ///
    /// # Returns
    /// The trace context if present, None otherwise
    ///
    /// # Example
    /// ```no_run
    /// use celers_broker_postgres::{PostgresBroker, TraceContext};
    /// use celers_core::Broker;
    ///
    /// # async fn example() -> celers_core::Result<()> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// if let Some(msg) = broker.dequeue().await? {
    ///     if let Some(trace_ctx) = broker.extract_trace_context(&msg.task.metadata.id).await? {
    ///         println!("Processing task in trace: {}", trace_ctx.trace_id);
    ///
    ///         // Create child span for nested operations
    ///         let child_span = trace_ctx.create_child_span();
    ///         println!("Child span: {}", child_span.span_id);
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn extract_trace_context(&self, task_id: &TaskId) -> Result<Option<TraceContext>> {
        let task_id_param = uuid_param(task_id);
        let rows = self
            .conn
            .query(
                r#"
            SELECT metadata::text AS metadata
            FROM celers_tasks
            WHERE id = $1::text::uuid
            "#,
                &[&task_id_param],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to fetch task metadata: {}", e)))?;

        if let Some(row) = rows.into_iter().next() {
            let metadata = json_from_row(&row, "metadata")
                .map_err(|e| CelersError::Other(format!("Failed to read metadata: {}", e)))?;
            if let Some(trace_value) = metadata.get("trace_context") {
                let trace_ctx: TraceContext =
                    serde_json::from_value(trace_value.clone()).map_err(|e| {
                        CelersError::Other(format!("Failed to deserialize trace context: {}", e))
                    })?;
                return Ok(Some(trace_ctx));
            }
        }
        Ok(None)
    }

    /// Enqueue a child task with trace context propagated from a parent task
    ///
    /// Creates a child span and enqueues the task with the propagated trace context.
    ///
    /// # Example
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    /// use celers_core::{Broker, SerializedTask};
    ///
    /// # async fn example() -> celers_core::Result<()> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// if let Some(msg) = broker.dequeue().await? {
    ///     // Create and enqueue child task with propagated trace
    ///     let child_task = SerializedTask::new("child_task".to_string(), vec![]);
    ///     broker.enqueue_with_parent_trace(&msg.task.metadata.id, child_task).await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn enqueue_with_parent_trace(
        &self,
        parent_task_id: &TaskId,
        child_task: SerializedTask,
    ) -> Result<TaskId> {
        if let Some(parent_ctx) = self.extract_trace_context(parent_task_id).await? {
            // Create child span
            let child_ctx = parent_ctx.create_child_span();
            self.enqueue_with_trace_context(child_task, child_ctx).await
        } else {
            // No trace context, enqueue normally
            self.enqueue(child_task).await
        }
    }

    /// Run database migrations
    ///
    /// Each migration file contains multiple `;`-separated DDL statements
    /// (and, in `001_init.sql`'s case, a `plpgsql` function body with
    /// internal semicolons of its own) in one string, so `execute_batch`
    /// (simple-query protocol, `batch_execute` under the hood) is used
    /// rather than `execute` (extended/prepared-statement protocol) —
    /// `oxisql_postgres::Connection::execute`/`query` reject multi-statement
    /// text, matching the same constraint the pre-migration
    /// `sqlx::query(...).execute(...)` call relied on sqlx's own
    /// simple-query fallback for. Mirrors `celers-backend-db`'s migration
    /// runner, the proven pattern for this exact situation.
    ///
    /// # Concurrency, and why the DDL is not simply replayed
    ///
    /// Every statement in the migration set is idempotent
    /// (`IF NOT EXISTS` / `OR REPLACE`), but idempotent is not the same as
    /// concurrency-safe. Replaying the set on every call — which is what this
    /// method used to do — breaks in three distinct ways once more than one
    /// caller exists, and a fleet of workers all calling `migrate()` on
    /// start-up (or a test suite whose cases each migrate a private queue) is
    /// exactly that:
    ///
    /// * two sessions running the same `CREATE OR REPLACE FUNCTION` race on
    ///   one `pg_proc` tuple — `ERROR: tuple concurrently updated`;
    /// * `CREATE INDEX IF NOT EXISTS` takes a `ShareLock` on `celers_tasks`
    ///   even when the index exists, against queue traffic already holding
    ///   `RowExclusiveLock` on it — `ERROR: deadlock detected`;
    /// * `007_queue_identity.sql` *replaces* `001_init.sql`'s `move_to_dlq()`
    ///   with one that carries `queue_name` across, so a replay briefly
    ///   reinstates the older definition and any task rejected inside that
    ///   window is filed under the `default` queue and lost.
    ///
    /// Two mechanisms remove all three. A **session-level advisory lock**
    /// ([`MIGRATION_ADVISORY_LOCK_ID`]), held on one pinned pooled
    /// connection, serialises concurrent callers; a **migration ledger**
    /// (`celers_schema_migrations`, see
    /// `migrations/000_schema_migrations.sql`) records each file as it is
    /// applied, inside the transaction that applies it, so every call after
    /// the first runs no DDL at all. The lock is released on the same
    /// connection whether the migrations succeeded or failed, and a process
    /// that dies mid-migration releases it implicitly when its session ends.
    ///
    /// A database migrated by an older build has no ledger rows: the first
    /// call after the upgrade replays the set once (harmlessly — every file
    /// is idempotent) and records it, and subsequent calls skip it.
    ///
    /// # The lock acquisition is bounded
    ///
    /// Waiting for the lock is capped at
    /// [`crate::DEFAULT_MIGRATION_LOCK_TIMEOUT`] (60 s); use
    /// [`migrate_with_lock_timeout`](Self::migrate_with_lock_timeout) to pick
    /// another deadline. This used to be an unbounded `pg_advisory_lock`,
    /// which is a statement the client cannot abandon once the server is
    /// waiting on it: a single session that took the migration lock and then
    /// stalled — a debugger paused on a breakpoint, a `BEGIN` nobody
    /// committed, a container frozen mid-`docker pause` — silently wedged
    /// every worker in the fleet at start-up, with no error, no log line and
    /// no timeout to hit. A bounded wait turns that into an actionable
    /// failure naming the lock id and pointing at `pg_locks`.
    pub async fn migrate(&self) -> Result<()> {
        self.migrate_with_lock_timeout(crate::advisory_lock::DEFAULT_MIGRATION_LOCK_TIMEOUT)
            .await
    }

    /// [`migrate`](Self::migrate) with an explicit deadline for acquiring the
    /// migration advisory lock.
    ///
    /// The deadline covers only the *lock acquisition*; the migrations
    /// themselves then run to completion. A zero deadline degrades to a
    /// single non-blocking attempt, which is the natural "migrate only if
    /// nobody else is" behaviour.
    ///
    /// # Errors
    ///
    /// Returns an error naming [`MIGRATION_ADVISORY_LOCK_ID`] and the elapsed
    /// wait if another session still holds the lock when `lock_timeout`
    /// expires, or whatever the migration set itself failed with.
    pub async fn migrate_with_lock_timeout(&self, lock_timeout: Duration) -> Result<()> {
        // One connection for the whole call: `pg_advisory_lock` is
        // *session*-scoped, so taking it on a pooled connection and releasing
        // it on whichever slot the pool hands out next would leak the lock
        // forever. Holding the guard pins the slot until this returns.
        let conn = self.connection().await?;

        crate::advisory_lock::lock_within(&conn, MIGRATION_ADVISORY_LOCK_ID, lock_timeout)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to lock for migration: {e}")))?;

        let outcome = Self::run_migration_set(&conn).await;

        // Release on the same connection, on both paths — a failed migration
        // must not leave every other caller blocked for the life of the
        // process.
        if let Err(e) = conn
            .execute(
                "SELECT pg_advisory_unlock($1)",
                &[&MIGRATION_ADVISORY_LOCK_ID],
            )
            .await
        {
            tracing::warn!(error = %e, "failed to release the migration advisory lock (non-fatal: it is released when this session ends)");
        }

        outcome
    }

    /// The migration statements themselves, run in order on one connection.
    ///
    /// Split out of [`migrate`](Self::migrate) so the advisory lock there
    /// wraps every path, including an early return from a failing migration.
    async fn run_migration_set(conn: &crate::pool::PooledConnection) -> Result<()> {
        let migrations: [(&str, &str); 9] = [
            ("001_init", include_str!("../migrations/001_init.sql")),
            ("002_results", include_str!("../migrations/002_results.sql")),
            (
                "004_deduplication",
                include_str!("../migrations/004_deduplication.sql"),
            ),
            (
                "005_snapshots",
                include_str!("../migrations/005_snapshots.sql"),
            ),
            (
                "006_deduplication_columns",
                include_str!("../migrations/006_deduplication_columns.sql"),
            ),
            // Queue identity (real `queue_name` column), delivery accounting
            // (`attempt_count`), idempotent DLQ promotion, and the periodic
            // schedule / task group tables.
            (
                "007_queue_identity",
                include_str!("../migrations/007_queue_identity.sql"),
            ),
            // Durable revoked-task set backing `Broker::revoke`/`is_revoked` —
            // see `revocation.rs`.
            (
                "008_revocation",
                include_str!("../migrations/008_revocation.sql"),
            ),
            // `celers_broker_results` — the broker's own result store, which
            // `results.rs` and `analytics.rs`'s `store_results_batch` have
            // always addressed and that no migration created, so every one of
            // those calls failed with `relation ... does not exist`. The name
            // is deliberately not `celers_task_results`: that belongs to
            // `celers-backend-db`'s result backend, which auto-migrates an
            // incompatible schema onto the same server. See the file header.
            (
                "009_broker_results",
                include_str!("../migrations/009_broker_results.sql"),
            ),
            // `celers_tasks.updated_at` — read by
            // `get_state_transition_history` and by
            // `detect_abnormal_state_duration("processing", ..)`, and now
            // maintained explicitly by every `UPDATE celers_tasks` in this
            // crate.
            (
                "010_task_updated_at",
                include_str!("../migrations/010_task_updated_at.sql"),
            ),
        ];

        // Bootstrap the ledger itself. This is the one statement that still
        // runs on every call; a `CREATE TABLE IF NOT EXISTS` on a table that
        // already exists touches no other relation, so it cannot deadlock
        // against live queue traffic the way the rest of the DDL can.
        conn.execute_batch(include_str!("../migrations/000_schema_migrations.sql"))
            .await
            .map_err(|e| {
                CelersError::Other(format!("Migration 000_schema_migrations failed: {e}"))
            })?;

        let applied = Self::applied_migrations(conn).await?;

        for (name, sql) in migrations {
            if applied.iter().any(|a| a == name) {
                tracing::debug!(migration = name, "migration already applied, skipping");
                continue;
            }
            Self::apply_migration(conn, name, sql).await?;
        }

        Ok(())
    }

    /// Names already recorded in `celers_schema_migrations`.
    async fn applied_migrations(conn: &crate::pool::PooledConnection) -> Result<Vec<String>> {
        let rows = conn
            .query("SELECT name FROM celers_schema_migrations", &[])
            .await
            .map_err(|e| CelersError::Other(format!("Failed to read migration ledger: {e}")))?;

        rows.iter()
            .map(|row| {
                crate::row_ext::RowExt::col::<String>(row, "name").map_err(|e| {
                    CelersError::Other(format!("Failed to read migration ledger name: {e}"))
                })
            })
            .collect()
    }

    /// Apply one migration file and record it, retrying a transient failure.
    ///
    /// The DDL in these files takes table-level locks (`CREATE INDEX` alone
    /// needs a `ShareLock` on `celers_tasks`), so applying them while the
    /// queue is serving traffic can lose a deadlock-detector coin toss —
    /// `ERROR: deadlock detected` — through no fault of the migration. Every
    /// file is idempotent, so simply trying again is both safe and the
    /// correct response; only a failure that survives every attempt is real.
    async fn apply_migration(
        conn: &crate::pool::PooledConnection,
        name: &str,
        sql: &str,
    ) -> Result<()> {
        const MAX_ATTEMPTS: u32 = 3;

        let mut last_error = None;
        for attempt in 1..=MAX_ATTEMPTS {
            match Self::apply_migration_once(conn, name, sql).await {
                Ok(()) => {
                    tracing::info!(migration = name, attempt, "applied migration");
                    return Ok(());
                }
                Err(e) => {
                    tracing::warn!(
                        migration = name,
                        attempt,
                        error = %e,
                        "migration attempt failed; retrying"
                    );
                    last_error = Some(e);
                    tokio::time::sleep(Duration::from_millis(50 * u64::from(attempt))).await;
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            CelersError::Other(format!("Migration {name} failed for an unknown reason"))
        }))
    }

    /// One attempt at applying `sql` and recording `name`, as a single
    /// transaction.
    ///
    /// PostgreSQL DDL is transactional, and that is load-bearing here rather
    /// than tidiness: `007_queue_identity.sql` replaces the `move_to_dlq()`
    /// that `001_init.sql` creates, and the older definition does not carry
    /// `queue_name` into the dead-letter queue. Applying the set inside one
    /// transaction per file — and rolling back a failure — means no other
    /// session ever observes a half-applied file.
    async fn apply_migration_once(
        conn: &crate::pool::PooledConnection,
        name: &str,
        sql: &str,
    ) -> Result<()> {
        conn.execute_batch("BEGIN")
            .await
            .map_err(|e| CelersError::Other(format!("Migration {name} failed to begin: {e}")))?;

        let applied = async {
            conn.execute_batch(sql)
                .await
                .map_err(|e| CelersError::Other(format!("Migration {name} failed: {e}")))?;
            conn.execute(
                "INSERT INTO celers_schema_migrations (name) VALUES ($1) \
                 ON CONFLICT (name) DO NOTHING",
                &[&name],
            )
            .await
            .map_err(|e| {
                CelersError::Other(format!("Migration {name} failed to record itself: {e}"))
            })?;
            Ok::<(), CelersError>(())
        }
        .await;

        match applied {
            Ok(()) => {
                conn.execute_batch("COMMIT").await.map(|_| ()).map_err(|e| {
                    CelersError::Other(format!("Migration {name} failed to commit: {e}"))
                })
            }
            Err(e) => {
                if let Err(rollback_error) = conn.execute_batch("ROLLBACK").await {
                    tracing::warn!(
                        migration = name,
                        error = %rollback_error,
                        "failed to roll back a failed migration"
                    );
                }
                Err(e)
            }
        }
    }

    /// Check out one pooled connection.
    ///
    /// Previously returned `&PgConnection`, which cannot survive the move to
    /// a real pool: there is no single connection to lend a reference to.
    /// The returned guard keeps its pool slot reserved until dropped, and is
    /// the entry point for running an explicit transaction:
    ///
    /// ```no_run
    /// # use celers_broker_postgres::PostgresBroker;
    /// # async fn example() -> celers_core::Result<()> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let conn = broker.connection().await?;
    /// let mut tx = conn
    ///     .transaction()
    ///     .await
    ///     .map_err(|e| celers_core::CelersError::Other(e.to_string()))?;
    /// tx.execute("SELECT 1", &[])
    ///     .await
    ///     .map_err(|e| celers_core::CelersError::Other(e.to_string()))?;
    /// tx.commit()
    ///     .await
    ///     .map_err(|e| celers_core::CelersError::Other(e.to_string()))?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connection(&self) -> Result<PooledConnection> {
        self.conn
            .acquire()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to acquire connection: {}", e)))
    }

    /// Number of connection slots this broker's pool was built with.
    #[must_use]
    pub fn pool_size(&self) -> u32 {
        self.conn.size()
    }

    /// Probe the database over a pooled connection.
    ///
    /// A slot whose connection has died is transparently reconnected by the
    /// pool, so a `true` here means the broker really can reach the database
    /// right now — supervisors can use this to distinguish "wedged" from
    /// "idle" without restarting the process.
    pub async fn health_check(&self) -> Result<()> {
        self.conn
            .ping()
            .await
            .map_err(|e| CelersError::Other(format!("Database health check failed: {}", e)))
    }

    /// Delete terminal tasks older than `retain_for`, in bounded chunks.
    ///
    /// `ack` intentionally leaves completed rows in `celers_tasks` for
    /// auditing, but `celers_tasks` is also the table every `dequeue` scans:
    /// without pruning it grows with lifetime throughput, and both
    /// `queue_size()` and the statistics queries degrade linearly. This is the
    /// manual form; [`PostgresBroker::spawn_retention_task`] runs it on a
    /// schedule.
    ///
    /// Returns the number of rows deleted. Each statement deletes at most
    /// `batch_size` rows (chosen through an `id IN (SELECT ... LIMIT n)`
    /// sub-select) so no single sweep holds long-lived row locks, and the
    /// loop stops early once a batch comes back short.
    pub async fn purge_terminal_tasks(
        &self,
        retain_for: Duration,
        batch_size: i64,
        max_batches: u32,
    ) -> Result<u64> {
        let batch_size = batch_size.clamp(1, 100_000);
        let age_secs = i64::try_from(retain_for.as_secs()).unwrap_or(i64::MAX);
        let statement = sql::purge_terminal_sql(batch_size);

        let mut deleted_total = 0u64;
        for _ in 0..max_batches {
            let deleted = self
                .conn
                .execute(&statement, &[&self.queue_name, &age_secs])
                .await
                .map_err(|e| {
                    CelersError::Other(format!("Failed to purge terminal tasks: {}", e))
                })?;
            deleted_total = deleted_total.saturating_add(deleted);
            if deleted < batch_size as u64 {
                break;
            }
        }

        if deleted_total > 0 {
            tracing::info!(
                queue = %self.queue_name,
                deleted = deleted_total,
                "Purged terminal tasks from the dispatch table"
            );
        }
        Ok(deleted_total)
    }

    /// Start a background retention sweep for this broker's queue.
    ///
    /// Deliberately opt-in rather than started from the constructor:
    /// deleting a deployment's audit history is not something a library may
    /// decide on its own. Drop the returned handle — or call
    /// [`tokio::task::JoinHandle::abort`] on it — to stop sweeping.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use celers_broker_postgres::{PostgresBroker, RetentionConfig};
    /// # async fn example() -> celers_core::Result<()> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let sweeper = broker.spawn_retention_task(RetentionConfig::default());
    /// // ... later ...
    /// sweeper.abort();
    /// # Ok(())
    /// # }
    /// ```
    pub fn spawn_retention_task(&self, config: RetentionConfig) -> tokio::task::JoinHandle<()> {
        // The task borrows nothing from `self`: the pool handle and queue
        // label are cloned, so the sweeper outlives this borrow and does not
        // force the broker into an `Arc`.
        let pool = self.conn.clone();
        let queue_name = self.queue_name.clone();
        let batch_size = config.batch_size.clamp(1, 100_000);
        let age_secs = i64::try_from(config.retain_for.as_secs()).unwrap_or(i64::MAX);
        let statement = sql::purge_terminal_sql(batch_size);

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(config.sweep_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                let mut deleted_total = 0u64;
                for _ in 0..config.max_batches_per_sweep {
                    match pool.execute(&statement, &[&queue_name, &age_secs]).await {
                        Ok(deleted) => {
                            deleted_total = deleted_total.saturating_add(deleted);
                            if deleted < batch_size as u64 {
                                break;
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                queue = %queue_name,
                                error = %e,
                                "Retention sweep failed; will retry on the next tick"
                            );
                            break;
                        }
                    }
                }
                if deleted_total > 0 {
                    tracing::info!(
                        queue = %queue_name,
                        deleted = deleted_total,
                        "Retention sweep pruned terminal tasks"
                    );
                }
            }
        })
    }

    /// Get the queue name
    pub fn queue_name(&self) -> &str {
        &self.queue_name
    }

    /// Move a task to the Dead Letter Queue
    ///
    /// Same trivial stored-function-call translation pattern as
    /// `celers-broker-sql`'s `MysqlBroker::move_to_dlq` (`CALL
    /// move_to_dlq(?)`): a plain `execute` with one bound `Uuid` parameter,
    /// here calling the Postgres `SELECT move_to_dlq($1::text::uuid)` function
    /// form (defined in `migrations/001_init.sql`) rather than MySQL's `CALL`.
    /// The function's parameter is declared `UUID`, so the argument needs the
    /// crate-wide `$n::text::uuid` cast exactly like a UUID column would — see
    /// [`crate::row_ext::uuid_param`].
    pub(crate) async fn move_to_dlq(&self, task_id: &TaskId) -> Result<()> {
        let task_id_param = uuid_param(task_id);
        self.conn
            .execute("SELECT move_to_dlq($1::text::uuid)", &[&task_id_param])
            .await
            .map_err(|e| CelersError::Other(format!("Failed to move task to DLQ: {}", e)))?;

        Ok(())
    }
}
