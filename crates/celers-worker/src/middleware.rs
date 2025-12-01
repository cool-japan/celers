//! Worker middleware system
//!
//! Provides a middleware stack for task execution, inspired by
//! Python Celery's Bootsteps and middleware patterns.
//!
//! # Middleware Pipeline
//!
//! ```text
//! Task Received
//!   ↓
//! [Tracing Middleware] ────→ OpenTelemetry Span
//!   ↓
//! [Metrics Middleware] ────→ Prometheus Histogram
//!   ↓
//! [Error Handler] ─────────→ Sentry Integration
//!   ↓
//! [User Task Execution]
//!   ↓
//! [Result Backend]
//!   ↓
//! [ACK to Broker]
//! ```

use async_trait::async_trait;
use std::fmt;

/// Task execution context
#[derive(Debug, Clone)]
pub struct TaskContext {
    /// Task ID
    pub task_id: String,

    /// Task name
    pub task_name: String,

    /// Retry count
    pub retry_count: u32,

    /// Worker name
    pub worker_name: String,

    /// Additional metadata
    pub metadata: std::collections::HashMap<String, String>,
}

impl TaskContext {
    pub fn new(task_id: String, task_name: String) -> Self {
        Self {
            task_id,
            task_name,
            retry_count: 0,
            worker_name: "worker-1".to_string(),
            metadata: std::collections::HashMap::new(),
        }
    }
}

/// Middleware result
pub type MiddlewareResult<T> = Result<T, MiddlewareError>;

/// Middleware errors
#[derive(Debug)]
pub enum MiddlewareError {
    /// Task should be retried
    Retry(String),

    /// Task failed permanently
    Failed(String),

    /// Task was cancelled
    Cancelled,

    /// Middleware error (doesn't fail the task)
    Soft(String),
}

impl fmt::Display for MiddlewareError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MiddlewareError::Retry(msg) => write!(f, "Retry: {}", msg),
            MiddlewareError::Failed(msg) => write!(f, "Failed: {}", msg),
            MiddlewareError::Cancelled => write!(f, "Cancelled"),
            MiddlewareError::Soft(msg) => write!(f, "Soft error: {}", msg),
        }
    }
}

impl std::error::Error for MiddlewareError {}

/// Middleware trait
#[async_trait]
pub trait Middleware: Send + Sync {
    /// Called before task execution
    async fn before_task(&self, ctx: &mut TaskContext) -> MiddlewareResult<()> {
        let _ = ctx;
        Ok(())
    }

    /// Called after successful task execution
    async fn after_task(
        &self,
        ctx: &TaskContext,
        result: &serde_json::Value,
    ) -> MiddlewareResult<()> {
        let _ = (ctx, result);
        Ok(())
    }

    /// Called on task failure
    async fn on_error(&self, ctx: &TaskContext, error: &str) -> MiddlewareResult<()> {
        let _ = (ctx, error);
        Ok(())
    }

    /// Called on task retry
    async fn on_retry(&self, ctx: &TaskContext, retry_count: u32) -> MiddlewareResult<()> {
        let _ = (ctx, retry_count);
        Ok(())
    }

    /// Middleware name (for logging)
    fn name(&self) -> &str {
        "middleware"
    }
}

/// Tracing middleware (OpenTelemetry spans)
pub struct TracingMiddleware {
    enabled: bool,
}

impl TracingMiddleware {
    pub fn new(enabled: bool) -> Self {
        Self { enabled }
    }
}

#[async_trait]
impl Middleware for TracingMiddleware {
    async fn before_task(&self, ctx: &mut TaskContext) -> MiddlewareResult<()> {
        if self.enabled {
            tracing::info!(
                task_id = %ctx.task_id,
                task_name = %ctx.task_name,
                "Task started"
            );
        }
        Ok(())
    }

    async fn after_task(
        &self,
        ctx: &TaskContext,
        _result: &serde_json::Value,
    ) -> MiddlewareResult<()> {
        if self.enabled {
            tracing::info!(
                task_id = %ctx.task_id,
                task_name = %ctx.task_name,
                "Task completed successfully"
            );
        }
        Ok(())
    }

    async fn on_error(&self, ctx: &TaskContext, error: &str) -> MiddlewareResult<()> {
        if self.enabled {
            tracing::error!(
                task_id = %ctx.task_id,
                task_name = %ctx.task_name,
                error = error,
                "Task failed"
            );
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "tracing"
    }
}

/// Metrics middleware (Prometheus)
#[cfg(feature = "metrics")]
pub struct MetricsMiddleware;

#[cfg(feature = "metrics")]
#[async_trait]
impl Middleware for MetricsMiddleware {
    async fn before_task(&self, ctx: &mut TaskContext) -> MiddlewareResult<()> {
        ctx.metadata.insert(
            "start_time".to_string(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                .to_string(),
        );
        Ok(())
    }

    async fn after_task(
        &self,
        ctx: &TaskContext,
        _result: &serde_json::Value,
    ) -> MiddlewareResult<()> {
        use celers_metrics::{
            TASKS_COMPLETED_BY_TYPE, TASKS_COMPLETED_TOTAL, TASK_EXECUTION_TIME,
            TASK_EXECUTION_TIME_BY_TYPE,
        };

        TASKS_COMPLETED_TOTAL.inc();

        // Track per-task-type metrics
        TASKS_COMPLETED_BY_TYPE
            .with_label_values(&[&ctx.task_name])
            .inc();

        if let Some(start_time) = ctx.metadata.get("start_time") {
            if let Ok(start) = start_time.parse::<u64>() {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                let duration = now - start;
                TASK_EXECUTION_TIME.observe(duration as f64);

                // Track per-task-type execution time
                TASK_EXECUTION_TIME_BY_TYPE
                    .with_label_values(&[&ctx.task_name])
                    .observe(duration as f64);
            }
        }

        Ok(())
    }

    async fn on_error(&self, ctx: &TaskContext, _error: &str) -> MiddlewareResult<()> {
        use celers_metrics::{TASKS_FAILED_BY_TYPE, TASKS_FAILED_TOTAL};

        TASKS_FAILED_TOTAL.inc();

        // Track per-task-type failures
        TASKS_FAILED_BY_TYPE
            .with_label_values(&[&ctx.task_name])
            .inc();

        Ok(())
    }

    fn name(&self) -> &str {
        "metrics"
    }
}

/// Middleware stack
pub struct MiddlewareStack {
    middlewares: Vec<Box<dyn Middleware>>,
}

impl MiddlewareStack {
    pub fn new() -> Self {
        Self {
            middlewares: Vec::new(),
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn add<M: Middleware + 'static>(mut self, middleware: M) -> Self {
        self.middlewares.push(Box::new(middleware));
        self
    }

    pub async fn before_task(&self, ctx: &mut TaskContext) -> MiddlewareResult<()> {
        for middleware in &self.middlewares {
            middleware.before_task(ctx).await?;
        }
        Ok(())
    }

    pub async fn after_task(
        &self,
        ctx: &TaskContext,
        result: &serde_json::Value,
    ) -> MiddlewareResult<()> {
        for middleware in &self.middlewares {
            middleware.after_task(ctx, result).await?;
        }
        Ok(())
    }

    pub async fn on_error(&self, ctx: &TaskContext, error: &str) -> MiddlewareResult<()> {
        for middleware in &self.middlewares {
            // Don't fail on middleware errors
            if let Err(e) = middleware.on_error(ctx, error).await {
                tracing::warn!(
                    middleware = middleware.name(),
                    error = %e,
                    "Middleware error during on_error"
                );
            }
        }
        Ok(())
    }

    pub async fn on_retry(&self, ctx: &TaskContext, retry_count: u32) -> MiddlewareResult<()> {
        for middleware in &self.middlewares {
            middleware.on_retry(ctx, retry_count).await?;
        }
        Ok(())
    }
}

impl Default for MiddlewareStack {
    fn default() -> Self {
        let mut stack = Self::new();

        // Add default middlewares
        stack = stack.add(TracingMiddleware::new(true));

        #[cfg(feature = "metrics")]
        {
            stack = stack.add(MetricsMiddleware);
        }

        stack
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_middleware_stack() {
        let stack = MiddlewareStack::default();
        let mut ctx = TaskContext::new("test-123".to_string(), "test_task".to_string());

        // Should not error
        stack.before_task(&mut ctx).await.unwrap();
        stack
            .after_task(&ctx, &serde_json::json!({"result": "ok"}))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_tracing_middleware() {
        let middleware = TracingMiddleware::new(true);
        let mut ctx = TaskContext::new("test-456".to_string(), "test_task".to_string());

        middleware.before_task(&mut ctx).await.unwrap();
        middleware
            .after_task(&ctx, &serde_json::json!(null))
            .await
            .unwrap();
    }
}
