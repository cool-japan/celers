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
}

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
        })
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
            SELECT metadata
            FROM celers_tasks
            WHERE id = $1
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
    pub async fn migrate(&self) -> Result<()> {
        // Run initial schema migration
        let init_sql = include_str!("../migrations/001_init.sql");
        self.conn
            .execute_batch(init_sql)
            .await
            .map_err(|e| CelersError::Other(format!("Migration 001_init failed: {}", e)))?;

        // Run results table migration
        let results_sql = include_str!("../migrations/002_results.sql");
        self.conn
            .execute_batch(results_sql)
            .await
            .map_err(|e| CelersError::Other(format!("Migration 002_results failed: {}", e)))?;

        // Run deduplication table migration
        let dedup_sql = include_str!("../migrations/004_deduplication.sql");
        self.conn.execute_batch(dedup_sql).await.map_err(|e| {
            CelersError::Other(format!("Migration 004_deduplication failed: {}", e))
        })?;

        // Run snapshots table migration
        let snapshots_sql = include_str!("../migrations/005_snapshots.sql");
        self.conn
            .execute_batch(snapshots_sql)
            .await
            .map_err(|e| CelersError::Other(format!("Migration 005_snapshots failed: {}", e)))?;

        // Run deduplication schema reconciliation migration
        let dedup_columns_sql = include_str!("../migrations/006_deduplication_columns.sql");
        self.conn
            .execute_batch(dedup_columns_sql)
            .await
            .map_err(|e| {
                CelersError::Other(format!("Migration 006_deduplication_columns failed: {}", e))
            })?;

        // Queue identity (real `queue_name` column), delivery accounting
        // (`attempt_count`), idempotent DLQ promotion, and the periodic
        // schedule / task group tables.
        let queue_identity_sql = include_str!("../migrations/007_queue_identity.sql");
        self.conn
            .execute_batch(queue_identity_sql)
            .await
            .map_err(|e| {
                CelersError::Other(format!("Migration 007_queue_identity failed: {}", e))
            })?;

        Ok(())
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
    /// here calling the Postgres `SELECT move_to_dlq($1)` function form
    /// (defined in `migrations/001_init.sql`) rather than MySQL's `CALL`.
    pub(crate) async fn move_to_dlq(&self, task_id: &TaskId) -> Result<()> {
        let task_id_param = uuid_param(task_id);
        self.conn
            .execute("SELECT move_to_dlq($1)", &[&task_id_param])
            .await
            .map_err(|e| CelersError::Other(format!("Failed to move task to DLQ: {}", e)))?;

        Ok(())
    }
}
