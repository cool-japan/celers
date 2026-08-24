//! Distributed tracing and lifecycle hooks implementation for MysqlBroker
//!
//! Provides methods for enqueuing tasks with trace context and
//! managing task lifecycle hooks.

use crate::broker_core::MysqlBroker;
use crate::row_ext::RowExt;
use crate::tracing::TraceContext;
use crate::workflow::{HookContext, TaskHook};
use celers_core::{Broker, CelersError, Result, SerializedTask, TaskId};
use chrono::Utc;
use oxisql_core::Connection;
use serde_json::json;

#[cfg(feature = "metrics")]
use celers_metrics::{TASKS_ENQUEUED_BY_TYPE, TASKS_ENQUEUED_TOTAL};

// ========== Distributed Tracing & Lifecycle Hooks ==========
impl MysqlBroker {
    /// Enqueue a task with distributed tracing context
    ///
    /// Stores W3C Trace Context with the task for distributed tracing compatibility.
    /// Compatible with OpenTelemetry, Jaeger, Zipkin, and other tracing systems.
    ///
    /// # Arguments
    /// * `task` - The task to enqueue
    /// * `trace_ctx` - W3C Trace Context to propagate
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::{MysqlBroker, TraceContext};
    /// # use celers_core::{Broker, SerializedTask};
    /// # async fn example() -> celers_core::Result<()> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    ///
    /// // Create trace context (typically from incoming HTTP request)
    /// let trace_ctx = TraceContext::from_traceparent(
    ///     "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    /// )?;
    ///
    /// let task = SerializedTask::new("my_task".to_string(), vec![]);
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

        // oxisql-core has no `ToSqlValue` impl for `serde_json::Value`
        // itself (unlike sqlx's `json` feature, which allowed binding
        // `db_metadata` directly) — serialize to text first, matching the
        // `json_param` convention documented in `row_ext.rs`.
        //
        // `build_task_metadata_document` overlays the `extra` document last,
        // so the trace context can never be clobbered by a same-named field
        // inside the task's own metadata, and a serialization failure is
        // propagated instead of silently degrading the row to `{}`.
        let db_metadata_str = self.build_task_metadata_document(
            &task,
            json!({
                "trace_context": {
                    "trace_id": trace_ctx.trace_id,
                    "span_id": trace_ctx.span_id,
                    "trace_flags": trace_ctx.trace_flags,
                    "trace_state": trace_ctx.trace_state,
                }
            }),
        )?;

        self.connection()
            .execute(
                r#"
                INSERT INTO celers_tasks
                    (id, queue_name, task_name, payload, state, priority, max_retries, metadata, created_at, scheduled_at)
                VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, NOW(), NOW())
                "#,
                &[
                    &task_id.to_string(),
                    &self.queue_name,
                    &task.metadata.name,
                    &task.payload,
                    &task.metadata.priority,
                    &(task.metadata.max_retries as i32),
                    &db_metadata_str,
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

    /// Extract distributed tracing context from a task's metadata
    ///
    /// Retrieves W3C Trace Context that was stored with the task.
    ///
    /// # Arguments
    /// * `task_id` - The task ID to extract trace context from
    ///
    /// # Returns
    /// * `Some(TraceContext)` if trace context was found
    /// * `None` if no trace context was stored with the task
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # use celers_core::Broker;
    /// # async fn example() -> celers_core::Result<()> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
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
        let rows = self
            .connection()
            .query(
                r#"
                SELECT metadata
                FROM celers_tasks
                WHERE id = ?
                "#,
                &[&task_id.to_string()],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to fetch task metadata: {}", e)))?;

        if let Some(row) = rows.into_iter().next() {
            // `celers_tasks.metadata` is a MySQL `JSON` column, which
            // oxisql-mysql decodes to `Value::Json`/`Value::Text` (a text
            // representation) rather than a native `serde_json::Value` —
            // read as `String` then parse, matching the `json_from_row`
            // convention in `row_ext.rs`.
            let metadata_str: String = row
                .col("metadata")
                .map_err(|e| CelersError::Other(format!("Failed to fetch task metadata: {e}")))?;
            let metadata: serde_json::Value = serde_json::from_str(&metadata_str)
                .map_err(|e| CelersError::Other(format!("Failed to parse task metadata: {e}")))?;
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
    /// This maintains the distributed trace across task boundaries.
    ///
    /// # Arguments
    /// * `parent_task_id` - The parent task ID to propagate trace from
    /// * `child_task` - The child task to enqueue
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # use celers_core::{Broker, SerializedTask};
    /// # async fn example() -> celers_core::Result<()> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
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

    /// Add a task lifecycle hook
    ///
    /// Hooks allow you to inject custom logic at various points in the task lifecycle.
    /// Multiple hooks can be registered for each lifecycle event.
    ///
    /// # Available Hook Types
    /// * `BeforeEnqueue` - Before a task is enqueued
    /// * `AfterEnqueue` - After a task is successfully enqueued
    /// * `BeforeDequeue` - After a task row is locked, before it is claimed
    ///   (an error aborts the claim and leaves the task pending)
    /// * `AfterDequeue` - After a task is dequeued
    /// * `BeforeAck` - Before a task is acknowledged
    /// * `AfterAck` - After a task is acknowledged
    /// * `BeforeReject` - Before a task is rejected
    /// * `AfterReject` - After a task is rejected
    ///
    /// # Example
    /// ```
    /// # use celers_broker_sql::{MysqlBroker, TaskHook, HookContext};
    /// # use celers_core::{Broker, SerializedTask};
    /// # use std::sync::Arc;
    /// # async fn example() -> celers_core::Result<()> {
    /// # let broker = MysqlBroker::new("mysql://localhost/test").await?;
    /// # broker.migrate().await?;
    /// // Add a logging hook for enqueue
    /// broker.add_hook(TaskHook::BeforeEnqueue(Arc::new(|_ctx, task| {
    ///     let task_name = task.metadata.name.clone();
    ///     Box::pin(async move {
    ///         println!("Enqueueing task: {}", task_name);
    ///         Ok(())
    ///     })
    /// }))).await;
    ///
    /// // Add validation hook
    /// broker.add_hook(TaskHook::BeforeEnqueue(Arc::new(|_ctx, task| {
    ///     let is_empty = task.payload.is_empty();
    ///     Box::pin(async move {
    ///         if is_empty {
    ///             return Err(celers_core::CelersError::Other(
    ///                 "Task payload cannot be empty".to_string()
    ///             ));
    ///         }
    ///         Ok(())
    ///     })
    /// }))).await;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn add_hook(&self, hook: TaskHook) {
        let mut hooks = self.hooks.write().await;
        hooks.add(hook);
    }

    /// Clear all registered lifecycle hooks
    ///
    /// Removes all hooks that were previously registered with `add_hook`.
    ///
    /// # Example
    /// ```no_run
    /// # use celers_broker_sql::MysqlBroker;
    /// # async fn example() -> celers_core::Result<()> {
    /// let broker = MysqlBroker::new("mysql://localhost/celers").await?;
    /// // ... add hooks ...
    /// broker.clear_hooks().await;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn clear_hooks(&self) {
        let mut hooks = self.hooks.write().await;
        hooks.clear();
    }

    // ========== Hook dispatch ==========
    //
    // Every `TaskHook` variant `add_hook` accepts is dispatched from one of
    // the helpers below. Registering an `AfterAck` (or `BeforeDequeue`,
    // `AfterDequeue`, `BeforeAck`, `BeforeReject`, `AfterReject`) hook used
    // to be a silent no-op: the closure was stored and never read, so
    // auditing, metrics and workflow-chaining hooks simply never ran.

    /// Build the context handed to every lifecycle hook.
    fn hook_context(&self, task: &SerializedTask) -> HookContext {
        HookContext {
            queue_name: self.queue_name.clone(),
            task_id: Some(task.metadata.id),
            timestamp: Utc::now(),
            metadata: json!({}),
        }
    }

    /// Run `BeforeDequeue` hooks for a locked-but-not-yet-claimed task.
    pub(crate) async fn fire_before_dequeue(&self, task: &SerializedTask) -> Result<()> {
        let hooks = self.hooks.read().await;
        if hooks.before_dequeue.is_empty() {
            return Ok(());
        }
        let ctx = self.hook_context(task);
        hooks.run_before_dequeue(&ctx, task).await
    }

    /// Run `AfterDequeue` hooks for a committed claim.
    pub(crate) async fn fire_after_dequeue(&self, task: &SerializedTask) -> Result<()> {
        let hooks = self.hooks.read().await;
        if hooks.after_dequeue.is_empty() {
            return Ok(());
        }
        let ctx = self.hook_context(task);
        hooks.run_after_dequeue(&ctx, task).await
    }

    /// Run `BeforeAck` hooks.
    pub(crate) async fn fire_before_ack(&self, task: &SerializedTask) -> Result<()> {
        let hooks = self.hooks.read().await;
        if hooks.before_ack.is_empty() {
            return Ok(());
        }
        let ctx = self.hook_context(task);
        hooks.run_before_ack(&ctx, task).await
    }

    /// Run `AfterAck` hooks.
    pub(crate) async fn fire_after_ack(&self, task: &SerializedTask) -> Result<()> {
        let hooks = self.hooks.read().await;
        if hooks.after_ack.is_empty() {
            return Ok(());
        }
        let ctx = self.hook_context(task);
        hooks.run_after_ack(&ctx, task).await
    }

    /// Run `BeforeReject` hooks.
    pub(crate) async fn fire_before_reject(&self, task: &SerializedTask) -> Result<()> {
        let hooks = self.hooks.read().await;
        if hooks.before_reject.is_empty() {
            return Ok(());
        }
        let ctx = self.hook_context(task);
        hooks.run_before_reject(&ctx, task).await
    }

    /// Run `AfterReject` hooks.
    pub(crate) async fn fire_after_reject(&self, task: &SerializedTask) -> Result<()> {
        let hooks = self.hooks.read().await;
        if hooks.after_reject.is_empty() {
            return Ok(());
        }
        let ctx = self.hook_context(task);
        hooks.run_after_reject(&ctx, task).await
    }

    /// Load the task an ack is about, but only when an ack hook is registered.
    ///
    /// `Broker::ack` receives an id, not a task, so the hooks' `&SerializedTask`
    /// argument has to be read back from the row. The registration check keeps
    /// that extra `SELECT` off the hot path for the overwhelmingly common case
    /// of no ack hooks at all.
    pub(crate) async fn load_task_for_ack_hooks(
        &self,
        row_id: &str,
    ) -> Result<Option<SerializedTask>> {
        let needed = {
            let hooks = self.hooks.read().await;
            !hooks.before_ack.is_empty() || !hooks.after_ack.is_empty()
        };
        if !needed {
            return Ok(None);
        }
        self.load_task_by_row_id(row_id).await
    }

    /// Load the task a reject is about, but only when a reject hook is
    /// registered.
    ///
    /// Always called *before* the state change: exceeding `max_retries` moves
    /// the task to the DLQ, which deletes the row, so a later load would find
    /// nothing.
    pub(crate) async fn load_task_for_reject_hooks(
        &self,
        row_id: &str,
    ) -> Result<Option<SerializedTask>> {
        let needed = {
            let hooks = self.hooks.read().await;
            !hooks.before_reject.is_empty() || !hooks.after_reject.is_empty()
        };
        if !needed {
            return Ok(None);
        }
        self.load_task_by_row_id(row_id).await
    }

    /// Rebuild a [`SerializedTask`] from its `celers_tasks` row.
    async fn load_task_by_row_id(&self, row_id: &str) -> Result<Option<SerializedTask>> {
        let sql = format!(
            "SELECT {} FROM celers_tasks WHERE id = ?",
            crate::sql_text::DEQUEUE_COLUMNS
        );
        let rows = self
            .connection()
            .query(&sql, &[&row_id])
            .await
            .map_err(|e| CelersError::Other(format!("Failed to load task for hooks: {e}")))?;

        rows.first()
            .map(|row| {
                crate::task_row::read_dequeued_row(row)
                    .map(|dequeued| crate::task_row::build_serialized_task(&dequeued))
            })
            .transpose()
    }
}
