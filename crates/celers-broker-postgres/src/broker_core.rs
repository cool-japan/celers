//! Core PostgresBroker struct and construction methods

use celers_core::{Broker, CelersError, Result, SerializedTask, TaskId};
use chrono::Utc;
use serde_json::json;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use crate::types::{HookContext, RetryStrategy, TaskHook, TaskHooks, TraceContext};

#[cfg(feature = "metrics")]
use celers_metrics::{TASKS_ENQUEUED_BY_TYPE, TASKS_ENQUEUED_TOTAL};

/// PostgreSQL-based broker implementation using SKIP LOCKED
pub struct PostgresBroker {
    pub(crate) pool: PgPool,
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
        let pool = PgPoolOptions::new()
            .max_connections(20)
            .acquire_timeout(Duration::from_secs(5))
            .connect(database_url)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to connect to database: {}", e)))?;

        Ok(Self {
            pool,
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
    /// * `max_connections` - Maximum number of connections in the pool
    /// * `acquire_timeout_secs` - Timeout for acquiring a connection (seconds)
    pub async fn with_pool_config(
        database_url: &str,
        queue_name: &str,
        max_connections: u32,
        acquire_timeout_secs: u64,
    ) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(acquire_timeout_secs))
            .connect(database_url)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to connect to database: {}", e)))?;

        Ok(Self {
            pool,
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

        sqlx::query(
            r#"
            INSERT INTO celers_tasks
                (id, task_name, payload, state, priority, max_retries, metadata, created_at, scheduled_at)
            VALUES ($1, $2, $3, 'pending', $4, $5, $6, NOW(), NOW())
            "#,
        )
        .bind(task_id)
        .bind(&task.metadata.name)
        .bind(&task.payload)
        .bind(task.metadata.priority)
        .bind(task.metadata.max_retries as i32)
        .bind(db_metadata)
        .execute(&self.pool)
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
        let row = sqlx::query(
            r#"
            SELECT metadata
            FROM celers_tasks
            WHERE id = $1
            "#,
        )
        .bind(task_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to fetch task metadata: {}", e)))?;

        if let Some(row) = row {
            let metadata: serde_json::Value = row.get("metadata");
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
    pub async fn migrate(&self) -> Result<()> {
        // Run initial schema migration
        let init_sql = include_str!("../migrations/001_init.sql");
        sqlx::query(init_sql)
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Migration 001_init failed: {}", e)))?;

        // Run results table migration
        let results_sql = include_str!("../migrations/002_results.sql");
        sqlx::query(results_sql)
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Migration 002_results failed: {}", e)))?;

        // Run deduplication table migration
        let dedup_sql = include_str!("../migrations/004_deduplication.sql");
        sqlx::query(dedup_sql)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                CelersError::Other(format!("Migration 004_deduplication failed: {}", e))
            })?;

        Ok(())
    }

    /// Get the underlying connection pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Get the queue name
    pub fn queue_name(&self) -> &str {
        &self.queue_name
    }

    /// Move a task to the Dead Letter Queue
    pub(crate) async fn move_to_dlq(&self, task_id: &TaskId) -> Result<()> {
        sqlx::query("SELECT move_to_dlq($1)")
            .bind(task_id)
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to move task to DLQ: {}", e)))?;

        Ok(())
    }
}
