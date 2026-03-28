//! W3C Trace Context for distributed tracing
//!
//! Compatible with OpenTelemetry, Jaeger, Zipkin, and other distributed tracing systems.
//! See: <https://www.w3.org/TR/trace-context/>

use celers_core::{CelersError, Result};
use serde::{Deserialize, Serialize};

/// W3C Trace Context for distributed tracing
///
/// Compatible with OpenTelemetry, Jaeger, Zipkin, and other distributed tracing systems.
/// See: <https://www.w3.org/TR/trace-context/>
///
/// # Example
/// ```
/// use celers_broker_sql::{MysqlBroker, TraceContext};
/// use celers_core::{Broker, SerializedTask};
///
/// # async fn example() -> celers_core::Result<()> {
/// # let broker = MysqlBroker::new("mysql://localhost/test").await?;
/// // Create a trace context
/// let trace_ctx = TraceContext::new(
///     "4bf92f3577b34da6a3ce929d0e0e4736",
///     "00f067aa0ba902b7"
/// );
///
/// // Enqueue task with trace context
/// let task = SerializedTask::new("my_task".to_string(), vec![]);
/// broker.enqueue_with_trace_context(task, trace_ctx).await?;
///
/// // Extract trace context when processing
/// if let Some(msg) = broker.dequeue().await? {
///     if let Some(ctx) = broker.extract_trace_context(&msg.task.metadata.id).await? {
///         println!("Processing task with trace_id: {}", ctx.trace_id);
///     }
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TraceContext {
    /// W3C Trace ID (32 hex characters, 16 bytes)
    pub trace_id: String,
    /// W3C Span ID (16 hex characters, 8 bytes)
    pub span_id: String,
    /// Trace flags (8-bit field, typically "01" for sampled)
    #[serde(default = "default_trace_flags")]
    pub trace_flags: String,
    /// Optional trace state for vendor-specific data
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_state: Option<String>,
}

pub(crate) fn default_trace_flags() -> String {
    "01".to_string()
}

impl TraceContext {
    /// Create a new trace context with trace_id and span_id
    ///
    /// # Arguments
    /// * `trace_id` - 32 hex character trace ID (W3C format)
    /// * `span_id` - 16 hex character span ID (W3C format)
    ///
    /// # Example
    /// ```
    /// use celers_broker_sql::TraceContext;
    ///
    /// let ctx = TraceContext::new(
    ///     "4bf92f3577b34da6a3ce929d0e0e4736",
    ///     "00f067aa0ba902b7"
    /// );
    /// assert_eq!(ctx.trace_flags, "01"); // Sampled by default
    /// ```
    pub fn new(trace_id: impl Into<String>, span_id: impl Into<String>) -> Self {
        Self {
            trace_id: trace_id.into(),
            span_id: span_id.into(),
            trace_flags: default_trace_flags(),
            trace_state: None,
        }
    }

    /// Create trace context from W3C traceparent header value
    ///
    /// Format: "00-{trace_id}-{span_id}-{flags}"
    ///
    /// # Example
    /// ```
    /// use celers_broker_sql::TraceContext;
    ///
    /// let ctx = TraceContext::from_traceparent(
    ///     "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    /// ).unwrap();
    /// assert_eq!(ctx.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
    /// assert_eq!(ctx.span_id, "00f067aa0ba902b7");
    /// ```
    pub fn from_traceparent(traceparent: &str) -> Result<Self> {
        let parts: Vec<&str> = traceparent.split('-').collect();
        if parts.len() != 4 || parts[0] != "00" {
            return Err(CelersError::Other(format!(
                "Invalid traceparent format: {}",
                traceparent
            )));
        }

        Ok(Self {
            trace_id: parts[1].to_string(),
            span_id: parts[2].to_string(),
            trace_flags: parts[3].to_string(),
            trace_state: None,
        })
    }

    /// Convert to W3C traceparent header value
    ///
    /// # Example
    /// ```
    /// use celers_broker_sql::TraceContext;
    ///
    /// let ctx = TraceContext::new(
    ///     "4bf92f3577b34da6a3ce929d0e0e4736",
    ///     "00f067aa0ba902b7"
    /// );
    /// assert_eq!(
    ///     ctx.to_traceparent(),
    ///     "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    /// );
    /// ```
    pub fn to_traceparent(&self) -> String {
        format!("00-{}-{}-{}", self.trace_id, self.span_id, self.trace_flags)
    }

    /// Check if trace is sampled (should be recorded)
    pub fn is_sampled(&self) -> bool {
        self.trace_flags == "01"
    }

    /// Generate a new child span ID for this trace
    ///
    /// # Example
    /// ```
    /// use celers_broker_sql::TraceContext;
    ///
    /// let parent_ctx = TraceContext::new(
    ///     "4bf92f3577b34da6a3ce929d0e0e4736",
    ///     "00f067aa0ba902b7"
    /// );
    /// let child_ctx = parent_ctx.create_child_span();
    ///
    /// // Same trace ID, different span ID
    /// assert_eq!(child_ctx.trace_id, parent_ctx.trace_id);
    /// assert_ne!(child_ctx.span_id, parent_ctx.span_id);
    /// ```
    pub fn create_child_span(&self) -> Self {
        let span_id = format!(
            "{:016x}",
            uuid::Uuid::new_v4().as_u128() & 0xFFFFFFFFFFFFFFFF
        );
        Self {
            trace_id: self.trace_id.clone(),
            span_id,
            trace_flags: self.trace_flags.clone(),
            trace_state: self.trace_state.clone(),
        }
    }
}
