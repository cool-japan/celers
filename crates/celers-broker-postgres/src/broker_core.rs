//! Core PostgresBroker struct and construction methods

use celers_core::{Broker, CelersError, Result, SerializedTask, TaskId};
use chrono::Utc;
use oxisql_core::Connection;
use oxisql_postgres::PgConnection;
use serde_json::json;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use crate::row_ext::{json_from_row, json_param, uuid_param};
use crate::tls_mode;
use crate::types::{HookContext, RetryStrategy, TaskHook, TaskHooks, TraceContext};

#[cfg(feature = "metrics")]
use celers_metrics::{TASKS_ENQUEUED_BY_TYPE, TASKS_ENQUEUED_TOTAL};

/// PostgreSQL-based broker implementation using SKIP LOCKED
pub struct PostgresBroker {
    /// The OxiSQL connection used throughout this crate's task-delivery hot
    /// path (`broker_trait.rs`'s `Broker` impl, `queue_ops.rs`, `results.rs`)
    /// and — as of this migration's final cleanup — this file's own
    /// internal migration-runner and `move_to_dlq` stored-function call too.
    ///
    /// This struct used to also carry a `pool: PgPool` field (the legacy
    /// `sqlx` connection pool) for exactly those two remaining call sites;
    /// it has been removed now that they are migrated, completing this
    /// crate's sqlx→oxisql migration and allowing the `sqlx` dependency to
    /// be dropped entirely.
    ///
    /// `oxisql_postgres::PgConnection` is `Clone` and internally
    /// `Arc<Mutex<tokio_postgres::Client>>` — cheap to clone, but unlike
    /// `sqlx::PgPool` all clones share ONE underlying connection rather than
    /// drawing from a pool of up to `max_connections`. This is a real
    /// behavioral change from the previous sqlx-based pool and is accepted
    /// for now per the migration plan as a deferred performance item.
    pub(crate) conn: PgConnection,
    /// The connection string this broker was constructed with.
    ///
    /// Retained so `notifications.rs` can open its own dedicated
    /// `PgConnection` for LISTEN/NOTIFY (a long-lived LISTEN connection
    /// should not share the query connection, per Postgres best practice).
    pub(crate) database_url: String,
    /// Logical queue label for multi-tenancy.
    ///
    /// This is stored as a JSON label inside `celers_tasks.metadata->>'queue'`
    /// at enqueue time (see `broker_trait.rs`'s `enqueue()`), NOT as a real
    /// column on `celers_tasks`, and NOT as a table name.
    ///
    /// Several peripheral APIs in this crate currently incorrectly assume one
    /// or the other (filtering `celers_tasks`/`celers_dead_letter_queue` on a
    /// nonexistent `queue_name` column, or interpolating this value as if it
    /// were a SQL table name via `format!("... FROM {} ...", self.queue_name)`).
    /// See `TODO.md` (`## queue_name schema drift`) for the full tracked list.
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
    pub async fn with_queue(database_url: &str, queue_name: &str) -> Result<Self> {
        // TLS mode is derived from `database_url`'s `sslmode` query
        // parameter (see `tls_mode.rs`): a URL with `sslmode=require` (or
        // `verify-ca`/`verify-full`/`prefer`/`allow`) gets a real TLS
        // connection, matching the pre-`oxisql`-migration `sqlx` behavior.
        // Absent/`sslmode=disable` still resolves to `TlsMode::Disabled`,
        // so plain-text callers are unaffected.
        let tls_mode = tls_mode::pg_tls_mode_for_url(database_url)
            .map_err(|e| CelersError::Other(format!("Failed to resolve TLS mode: {}", e)))?;
        let conn = PgConnection::connect(database_url, tls_mode)
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

    /// Create a new PostgreSQL broker with custom pool configuration
    ///
    /// # Arguments
    /// * `database_url` - PostgreSQL connection string
    /// * `queue_name` - Logical queue name
    /// * `max_connections` - Maximum number of connections in the pool. Kept
    ///   in the public signature for API compatibility, but unused now that
    ///   the legacy `sqlx::PgPool` construction has been removed:
    ///   `oxisql_postgres::PgConnection` wraps a single multiplexed
    ///   `tokio_postgres::Client` (see the doc comment on
    ///   `PostgresBroker::conn`), not a real connection pool, so there is
    ///   no pool size to configure.
    /// * `acquire_timeout_secs` - Timeout for acquiring a connection (seconds)
    pub async fn with_pool_config(
        database_url: &str,
        queue_name: &str,
        _max_connections: u32,
        acquire_timeout_secs: u64,
    ) -> Result<Self> {
        // See `with_queue` above for how the TLS mode is derived from
        // `database_url`'s `sslmode` query parameter.
        let tls_mode = tls_mode::pg_tls_mode_for_url(database_url)
            .map_err(|e| CelersError::Other(format!("Failed to resolve TLS mode: {}", e)))?;
        let conn = PgConnection::connect_with_timeout(
            database_url,
            tls_mode,
            Duration::from_secs(acquire_timeout_secs),
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

        // Byte-for-byte identical SQL text to the pre-migration sqlx version.
        // UUID -> uuid_param, JSON metadata -> json_param, everything else is
        // an already-primitive ToSqlValue (String, Vec<u8>, i32). Mirrors
        // `broker_trait.rs`'s `enqueue()`, this crate's proven pilot for this
        // exact INSERT shape.
        let task_id_param = uuid_param(&task_id);
        let metadata_param = json_param(&db_metadata);
        self.conn
            .execute(
                r#"
            INSERT INTO celers_tasks
                (id, task_name, payload, state, priority, max_retries, metadata, created_at, scheduled_at)
            VALUES ($1, $2, $3, 'pending', $4, $5, $6, NOW(), NOW())
            "#,
                &[
                    &task_id_param,
                    &task.metadata.name,
                    &task.payload,
                    &task.metadata.priority,
                    &(task.metadata.max_retries as i32),
                    &metadata_param,
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

        Ok(())
    }

    /// Get the underlying OxiSQL connection used by the migrated
    /// task-delivery hot path.
    ///
    /// Renamed from the previous `pool()` getter (which returned
    /// `&sqlx::PgPool`) as part of the sqlx→oxisql migration — this is a
    /// breaking change for any external caller of the old name. No callers
    /// of `.pool()` on `PostgresBroker` were found anywhere else in the
    /// `celers` workspace (grepped `crates/**/*.rs` for `.pool()`), so no
    /// other crate needed updating.
    pub fn connection(&self) -> &PgConnection {
        &self.conn
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
