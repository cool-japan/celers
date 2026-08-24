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
//!
//! Regression guard for the fix in idx 190/187 (`expect()` on the
//! `MetricsMiddleware` duration-measurement path): denies
//! `clippy::unwrap_used`/`clippy::expect_used` outside the test module so
//! a future change cannot silently reintroduce a panic here.
#![deny(clippy::unwrap_used, clippy::expect_used)]

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

/// Compute the elapsed seconds (as an `f64`) between a `start_time_ns`
/// value (epoch nanoseconds, string-encoded, as stored by
/// [`MetricsMiddleware::before_task`]) and now.
///
/// Returns `None` if `start_time_ns` cannot be parsed. Never panics: a
/// backwards wall-clock step between the two samples (an NTP correction
/// landing between `before_task` and `after_task`) saturates to a
/// duration of zero instead of underflowing the raw subtraction, which
/// would otherwise panic in debug builds / wrap to an enormous value in
/// release and permanently corrupt the cumulative Prometheus histogram
/// sum for the process's lifetime.
#[cfg(feature = "metrics")]
fn elapsed_secs_since_ns(start_time_ns: &str) -> Option<f64> {
    let start_ns: u128 = start_time_ns.parse().ok()?;
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    Some(now_ns.saturating_sub(start_ns) as f64 / 1_000_000_000.0)
}

/// Metrics middleware (Prometheus)
#[cfg(feature = "metrics")]
pub struct MetricsMiddleware;

#[cfg(feature = "metrics")]
#[async_trait]
impl Middleware for MetricsMiddleware {
    async fn before_task(&self, ctx: &mut TaskContext) -> MiddlewareResult<()> {
        // Nanosecond-precision, string-encoded start time.
        //
        // Ideally this would be a monotonic `std::time::Instant` field on
        // `TaskContext`, but `TaskContext` is built via a plain struct
        // literal at its one construction site (`worker_core.rs`) outside
        // this module, so adding a field there is not a self-contained
        // change here. Storing epoch *nanoseconds* (rather than whole
        // seconds) instead of a monotonic instant still fixes both
        // observed defects without changing `TaskContext`'s shape:
        // - granularity: sub-second tasks no longer all report 0.0.
        // - `unwrap_or(0)` (rather than `expect`) means a clock set before
        //   the Unix epoch can't panic this path.
        ctx.metadata.insert(
            "start_time_ns".to_string(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
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

        if let Some(start_time) = ctx.metadata.get("start_time_ns") {
            if let Some(duration_secs) = elapsed_secs_since_ns(start_time) {
                TASK_EXECUTION_TIME.observe(duration_secs);

                // Track per-task-type execution time
                TASK_EXECUTION_TIME_BY_TYPE
                    .with_label_values(&[&ctx.task_name])
                    .observe(duration_secs);
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
#[allow(clippy::unwrap_used, clippy::expect_used)]
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

    // --- Regression tests for MetricsMiddleware duration measurement ------

    /// Regression test: the previous implementation truncated to whole
    /// seconds, so every task under 1s (the common case) recorded a
    /// duration of exactly 0.0. Nanosecond precision must be able to
    /// represent sub-second durations.
    #[cfg(feature = "metrics")]
    #[test]
    fn test_elapsed_secs_since_ns_sub_second_precision() {
        let now_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let start_ns = now_ns - 250_000_000; // 250ms in the past
        let elapsed =
            elapsed_secs_since_ns(&start_ns.to_string()).expect("valid start_time_ns must parse");
        assert!(
            (0.15..0.6).contains(&elapsed),
            "expected roughly 0.25s of sub-second precision, got {elapsed}"
        );
    }

    /// Regression test: `now - start` on raw `u64`/whole-second wall-clock
    /// values could panic (debug) or wrap to ~1.8e19 (release) if the
    /// clock stepped backwards between `before_task` and `after_task`.
    /// The fixed computation must saturate to a duration of zero instead.
    #[cfg(feature = "metrics")]
    #[test]
    fn test_elapsed_secs_since_ns_never_panics_on_backwards_clock() {
        // A "start" timestamp far in the future relative to "now" stands
        // in for a backwards NTP correction landing between the two
        // `SystemTime::now()` samples.
        let far_future_ns = u128::MAX;
        let elapsed = elapsed_secs_since_ns(&far_future_ns.to_string())
            .expect("a syntactically valid start_time_ns must still parse");
        assert_eq!(
            elapsed, 0.0,
            "backwards clock step must saturate to 0, not underflow"
        );
    }

    #[cfg(feature = "metrics")]
    #[test]
    fn test_elapsed_secs_since_ns_invalid_input_returns_none() {
        assert!(elapsed_secs_since_ns("not-a-number").is_none());
        assert!(elapsed_secs_since_ns("").is_none());
    }

    /// `before_task` must record a value that decodes as nanoseconds, not
    /// the old whole-second string.
    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn test_before_task_stores_nanosecond_precision_start_time() {
        let middleware = MetricsMiddleware;
        let mut ctx = TaskContext::new("t-1".to_string(), "task".to_string());
        middleware.before_task(&mut ctx).await.unwrap();

        let stored = ctx
            .metadata
            .get("start_time_ns")
            .expect("before_task must record a start_time_ns entry");
        let start_ns: u128 = stored.parse().expect("start_time_ns must be numeric");

        // A nanosecond epoch timestamp for "now" is at least 10^18 (the
        // year-2001 boundary in ns); a whole-second value would be at
        // most ~10^10, so this also guards against a granularity
        // regression back to seconds.
        assert!(start_ns > 1_000_000_000_000_000_000);
    }

    /// End-to-end: after_task must not panic when start_time_ns is
    /// present, whatever its value, including the pathological
    /// "backwards clock" case.
    #[cfg(feature = "metrics")]
    #[tokio::test]
    async fn test_after_task_does_not_panic_with_backwards_clock_start_time() {
        let middleware = MetricsMiddleware;
        let mut ctx = TaskContext::new("t-2".to_string(), "task".to_string());
        ctx.metadata
            .insert("start_time_ns".to_string(), u128::MAX.to_string());

        middleware
            .after_task(&ctx, &serde_json::json!({"ok": true}))
            .await
            .unwrap();
    }
}
