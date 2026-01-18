//! OpenTelemetry integration for distributed tracing
//!
//! Provides automatic span creation and trace context propagation for Redis broker operations.
//!
//! # Features
//!
//! - Automatic span creation for all queue operations (enqueue, dequeue, ack, reject)
//! - Trace context propagation across task boundaries
//! - Custom span attributes for queue metadata (queue name, priority, task ID)
//! - Integration with popular tracing backends (Jaeger, Zipkin, etc.)
//! - Performance overhead minimization with sampling
//!
//! # Example
//!
//! ```rust
//! use celers_broker_redis::telemetry::{TracingContext, SpanBuilder};
//! use celers_core::TaskId;
//!
//! // Create a tracing context for an operation
//! let ctx = TracingContext::new("enqueue_operation");
//!
//! // Serialize context to store alongside task in Redis
//! let task_id = TaskId::new_v4();
//! let trace_key = TracingContext::redis_key_for_task(&task_id);
//! let trace_json = ctx.to_json().unwrap();
//!
//! // Later, restore the context when processing
//! let restored_ctx = TracingContext::from_json(&trace_json).unwrap();
//!
//! // Use context to create child span
//! restored_ctx.with_span("process_task", |span| {
//!     // Task processing logic
//!     println!("Processing in span: {}", span.span_id);
//! });
//! ```

use celers_core::TaskId;
use std::collections::HashMap;

/// Trace context for distributed tracing
///
/// Encapsulates trace ID, span ID, and other tracing metadata
/// that can be propagated across service boundaries.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TracingContext {
    /// Trace ID (unique identifier for the entire trace)
    pub trace_id: String,
    /// Span ID (unique identifier for this span)
    pub span_id: String,
    /// Parent span ID (if this is a child span)
    pub parent_span_id: Option<String>,
    /// Sampling decision (whether to record this trace)
    pub sampled: bool,
    /// Additional baggage items
    pub baggage: HashMap<String, String>,
}

impl TracingContext {
    /// Create a new tracing context with a generated trace ID
    ///
    /// # Arguments
    ///
    /// * `operation_name` - Name of the operation being traced
    ///
    /// # Example
    ///
    /// ```rust
    /// use celers_broker_redis::telemetry::TracingContext;
    ///
    /// let ctx = TracingContext::new("enqueue_task");
    /// assert!(!ctx.trace_id.is_empty());
    /// ```
    pub fn new(operation_name: &str) -> Self {
        let trace_id = generate_trace_id();
        let span_id = generate_span_id();

        let mut baggage = HashMap::new();
        baggage.insert("operation".to_string(), operation_name.to_string());

        Self {
            trace_id,
            span_id,
            parent_span_id: None,
            sampled: true,
            baggage,
        }
    }

    /// Create a child context from this context
    ///
    /// # Arguments
    ///
    /// * `operation_name` - Name of the child operation
    ///
    /// # Example
    ///
    /// ```rust
    /// use celers_broker_redis::telemetry::TracingContext;
    ///
    /// let parent = TracingContext::new("parent_op");
    /// let child = parent.child("child_op");
    ///
    /// assert_eq!(parent.trace_id, child.trace_id);
    /// assert_eq!(child.parent_span_id, Some(parent.span_id.clone()));
    /// ```
    pub fn child(&self, operation_name: &str) -> Self {
        let mut child = Self {
            trace_id: self.trace_id.clone(),
            span_id: generate_span_id(),
            parent_span_id: Some(self.span_id.clone()),
            sampled: self.sampled,
            baggage: self.baggage.clone(),
        };

        child
            .baggage
            .insert("operation".to_string(), operation_name.to_string());

        child
    }

    /// Serialize tracing context to JSON string
    ///
    /// Returns a JSON string representation of the tracing context
    /// that can be stored alongside a task or in Redis.
    ///
    /// # Example
    ///
    /// ```rust
    /// use celers_broker_redis::telemetry::TracingContext;
    ///
    /// let ctx = TracingContext::new("my_operation");
    /// let json = ctx.to_json();
    /// assert!(json.is_ok());
    /// ```
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Deserialize tracing context from JSON string
    ///
    /// # Arguments
    ///
    /// * `json` - JSON string representation of tracing context
    ///
    /// # Example
    ///
    /// ```rust
    /// use celers_broker_redis::telemetry::TracingContext;
    ///
    /// let ctx = TracingContext::new("my_operation");
    /// let json = ctx.to_json().unwrap();
    ///
    /// let restored = TracingContext::from_json(&json);
    /// assert!(restored.is_ok());
    /// assert_eq!(restored.unwrap().trace_id, ctx.trace_id);
    /// ```
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Get the Redis key for storing this task's tracing context
    ///
    /// Returns a key in the format: `trace:{task_id}`
    ///
    /// # Arguments
    ///
    /// * `task_id` - The task ID
    pub fn redis_key_for_task(task_id: &TaskId) -> String {
        format!("trace:{}", task_id)
    }

    /// Add a baggage item to the context
    ///
    /// Baggage items are key-value pairs that are propagated with the trace.
    ///
    /// # Arguments
    ///
    /// * `key` - Baggage key
    /// * `value` - Baggage value
    pub fn add_baggage(&mut self, key: String, value: String) {
        self.baggage.insert(key, value);
    }

    /// Get a baggage item from the context
    ///
    /// # Arguments
    ///
    /// * `key` - Baggage key to retrieve
    ///
    /// # Returns
    ///
    /// `Some(&String)` if the key exists, `None` otherwise
    pub fn get_baggage(&self, key: &str) -> Option<&String> {
        self.baggage.get(key)
    }

    /// Execute a closure within a new span context
    ///
    /// # Arguments
    ///
    /// * `operation_name` - Name of the operation for the new span
    /// * `f` - Closure to execute within the span
    ///
    /// # Example
    ///
    /// ```rust
    /// use celers_broker_redis::telemetry::TracingContext;
    ///
    /// let ctx = TracingContext::new("parent_op");
    /// ctx.with_span("child_op", |span| {
    ///     // Operations within this span
    ///     println!("Processing within span: {}", span.span_id);
    /// });
    /// ```
    pub fn with_span<F, R>(&self, operation_name: &str, f: F) -> R
    where
        F: FnOnce(&TracingContext) -> R,
    {
        let child = self.child(operation_name);
        f(&child)
    }
}

/// Span builder for creating custom spans
///
/// Provides a fluent interface for building spans with attributes and events.
#[derive(Debug)]
pub struct SpanBuilder {
    context: TracingContext,
    attributes: HashMap<String, String>,
    events: Vec<SpanEvent>,
}

impl SpanBuilder {
    /// Create a new span builder
    ///
    /// # Arguments
    ///
    /// * `operation_name` - Name of the operation
    pub fn new(operation_name: &str) -> Self {
        Self {
            context: TracingContext::new(operation_name),
            attributes: HashMap::new(),
            events: Vec::new(),
        }
    }

    /// Create a child span builder from a parent context
    ///
    /// # Arguments
    ///
    /// * `parent` - Parent tracing context
    /// * `operation_name` - Name of the operation
    pub fn child_of(parent: &TracingContext, operation_name: &str) -> Self {
        Self {
            context: parent.child(operation_name),
            attributes: HashMap::new(),
            events: Vec::new(),
        }
    }

    /// Add an attribute to the span
    ///
    /// # Arguments
    ///
    /// * `key` - Attribute key
    /// * `value` - Attribute value
    pub fn with_attribute(mut self, key: String, value: String) -> Self {
        self.attributes.insert(key, value);
        self
    }

    /// Add a queue-related attribute
    ///
    /// # Arguments
    ///
    /// * `queue_name` - Name of the queue
    pub fn with_queue(self, queue_name: &str) -> Self {
        self.with_attribute("queue.name".to_string(), queue_name.to_string())
    }

    /// Add a task ID attribute
    ///
    /// # Arguments
    ///
    /// * `task_id` - Task ID
    pub fn with_task_id(self, task_id: &TaskId) -> Self {
        self.with_attribute("task.id".to_string(), task_id.to_string())
    }

    /// Add a priority attribute
    ///
    /// # Arguments
    ///
    /// * `priority` - Task priority
    pub fn with_priority(self, priority: u8) -> Self {
        self.with_attribute("task.priority".to_string(), priority.to_string())
    }

    /// Add an event to the span
    ///
    /// # Arguments
    ///
    /// * `name` - Event name
    /// * `attributes` - Event attributes
    pub fn add_event(mut self, name: String, attributes: HashMap<String, String>) -> Self {
        self.events.push(SpanEvent {
            name,
            attributes,
            timestamp: std::time::SystemTime::now(),
        });
        self
    }

    /// Build and return the span context and attributes
    pub fn build(self) -> (TracingContext, HashMap<String, String>, Vec<SpanEvent>) {
        (self.context, self.attributes, self.events)
    }
}

/// Span event representing something that happened during span execution
#[derive(Debug, Clone)]
pub struct SpanEvent {
    /// Event name
    pub name: String,
    /// Event attributes
    pub attributes: HashMap<String, String>,
    /// Event timestamp
    pub timestamp: std::time::SystemTime,
}

/// Generate a unique trace ID
///
/// Format: 32 hexadecimal characters
fn generate_trace_id() -> String {
    use rand::Rng;
    use std::time::SystemTime;

    let now = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time should be after UNIX epoch");
    let nanos = now.as_nanos() as u64;

    // Add random component for uniqueness
    let random: u64 = rand::rng().random();

    // Generate a 128-bit trace ID (64-bit timestamp + 64-bit random)
    format!("{:016x}{:016x}", nanos, random)
}

/// Generate a unique span ID
///
/// Format: 16 hexadecimal characters
fn generate_span_id() -> String {
    use rand::Rng;

    // Generate a 64-bit span ID using random values for uniqueness
    let random: u64 = rand::rng().random();
    format!("{:016x}", random)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tracing_context_new() {
        let ctx = TracingContext::new("test_op");

        assert!(!ctx.trace_id.is_empty());
        assert!(!ctx.span_id.is_empty());
        assert!(ctx.parent_span_id.is_none());
        assert!(ctx.sampled);
        assert_eq!(ctx.get_baggage("operation"), Some(&"test_op".to_string()));
    }

    #[test]
    fn test_tracing_context_child() {
        let parent = TracingContext::new("parent");
        let child = parent.child("child");

        assert_eq!(parent.trace_id, child.trace_id);
        assert_ne!(parent.span_id, child.span_id);
        assert_eq!(child.parent_span_id, Some(parent.span_id.clone()));
        assert_eq!(child.get_baggage("operation"), Some(&"child".to_string()));
    }

    #[test]
    fn test_json_serialization_roundtrip() {
        let mut ctx = TracingContext::new("test_op");
        ctx.add_baggage("custom_key".to_string(), "custom_value".to_string());

        let json = ctx.to_json().unwrap();
        let restored = TracingContext::from_json(&json).unwrap();

        assert_eq!(ctx.trace_id, restored.trace_id);
        assert_eq!(ctx.span_id, restored.span_id);
        assert_eq!(ctx.parent_span_id, restored.parent_span_id);
        assert_eq!(ctx.sampled, restored.sampled);
        assert_eq!(
            ctx.get_baggage("custom_key"),
            restored.get_baggage("custom_key")
        );
    }

    #[test]
    fn test_redis_key_for_task() {
        use celers_core::TaskId;

        let task_id = TaskId::new_v4();
        let key = TracingContext::redis_key_for_task(&task_id);

        assert!(key.starts_with("trace:"));
        assert!(key.contains(&task_id.to_string()));
    }

    #[test]
    fn test_baggage() {
        let mut ctx = TracingContext::new("test_op");

        ctx.add_baggage("key1".to_string(), "value1".to_string());
        ctx.add_baggage("key2".to_string(), "value2".to_string());

        assert_eq!(ctx.get_baggage("key1"), Some(&"value1".to_string()));
        assert_eq!(ctx.get_baggage("key2"), Some(&"value2".to_string()));
        assert_eq!(ctx.get_baggage("nonexistent"), None);
    }

    #[test]
    fn test_span_builder() {
        let builder = SpanBuilder::new("test_operation")
            .with_queue("my_queue")
            .with_priority(5);

        let (ctx, attrs, _events) = builder.build();

        assert_eq!(
            ctx.get_baggage("operation"),
            Some(&"test_operation".to_string())
        );
        assert_eq!(attrs.get("queue.name"), Some(&"my_queue".to_string()));
        assert_eq!(attrs.get("task.priority"), Some(&"5".to_string()));
    }

    #[test]
    fn test_span_builder_child() {
        let parent = TracingContext::new("parent");
        let builder = SpanBuilder::child_of(&parent, "child").with_queue("test_queue");

        let (ctx, attrs, _events) = builder.build();

        assert_eq!(parent.trace_id, ctx.trace_id);
        assert_eq!(ctx.parent_span_id, Some(parent.span_id));
        assert_eq!(attrs.get("queue.name"), Some(&"test_queue".to_string()));
    }

    #[test]
    fn test_with_span() {
        let parent = TracingContext::new("parent");
        let mut child_span_id = String::new();

        parent.with_span("child", |child| {
            child_span_id = child.span_id.clone();
            assert_eq!(parent.trace_id, child.trace_id);
            assert_eq!(child.parent_span_id, Some(parent.span_id.clone()));
        });

        assert!(!child_span_id.is_empty());
    }

    #[test]
    fn test_span_event() {
        let mut attrs = HashMap::new();
        attrs.insert("error".to_string(), "timeout".to_string());

        let builder = SpanBuilder::new("test").add_event("task_failed".to_string(), attrs.clone());

        let (_ctx, _attrs, events) = builder.build();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].name, "task_failed");
        assert_eq!(
            events[0].attributes.get("error"),
            Some(&"timeout".to_string())
        );
    }

    #[test]
    fn test_generate_trace_id() {
        let id1 = generate_trace_id();
        let id2 = generate_trace_id();

        assert_eq!(id1.len(), 32);
        assert_eq!(id2.len(), 32);
        // IDs should be different (with very high probability)
        // Note: This could theoretically fail, but probability is extremely low
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_generate_span_id() {
        let id1 = generate_span_id();
        let id2 = generate_span_id();

        assert_eq!(id1.len(), 16);
        assert_eq!(id2.len(), 16);
        assert_ne!(id1, id2);
    }
}
