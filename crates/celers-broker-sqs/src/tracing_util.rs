// Copyright (c) 2026 COOLJAPAN OU (Team KitaSan)
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Distributed tracing utilities for SQS message correlation
//!
//! This module provides utilities for tracking message flows across
//! distributed systems using correlation IDs and trace contexts.
//!
//! # Features
//!
//! - Correlation ID generation and propagation
//! - Trace context extraction and injection
//! - Parent-child span relationships
//! - Message flow tracking across queues
//! - Integration with OpenTelemetry and AWS X-Ray
//!
//! # Example
//!
//! ```rust
//! use celers_broker_sqs::tracing_util::{TraceContext, generate_correlation_id};
//!
//! // Generate a correlation ID for a new request
//! let correlation_id = generate_correlation_id();
//!
//! // Create a trace context
//! let trace_ctx = TraceContext::new()
//!     .with_correlation_id(&correlation_id)
//!     .with_operation("process_order")
//!     .with_metadata("user_id", "12345");
//!
//! // Serialize for message attributes
//! let trace_header = trace_ctx.to_header();
//! assert!(!trace_header.is_empty());
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// Generate a unique correlation ID for message tracking
///
/// # Example
///
/// ```
/// use celers_broker_sqs::tracing_util::generate_correlation_id;
///
/// let correlation_id = generate_correlation_id();
/// assert!(!correlation_id.is_empty());
/// assert_eq!(correlation_id.len(), 36); // UUID v4 format
/// ```
pub fn generate_correlation_id() -> String {
    Uuid::new_v4().to_string()
}

/// Trace context for distributed tracing
///
/// Captures correlation IDs, trace IDs, and span IDs for tracking
/// message flows across distributed systems.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TraceContext {
    /// Correlation ID for end-to-end request tracking
    pub correlation_id: String,
    /// Trace ID (compatible with AWS X-Ray and OpenTelemetry)
    pub trace_id: Option<String>,
    /// Parent span ID for distributed tracing
    pub parent_span_id: Option<String>,
    /// Current span ID
    pub span_id: Option<String>,
    /// Operation name (e.g., "process_order", "send_email")
    pub operation: Option<String>,
    /// Service name
    pub service_name: Option<String>,
    /// Timestamp when trace context was created (Unix epoch milliseconds)
    pub timestamp: u64,
    /// Custom metadata (e.g., user_id, tenant_id, request_id)
    pub metadata: HashMap<String, String>,
}

impl Default for TraceContext {
    fn default() -> Self {
        Self::new()
    }
}

impl TraceContext {
    /// Create a new trace context with a generated correlation ID
    ///
    /// # Example
    ///
    /// ```
    /// use celers_broker_sqs::tracing_util::TraceContext;
    ///
    /// let ctx = TraceContext::new();
    /// assert!(!ctx.correlation_id.is_empty());
    /// ```
    pub fn new() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Self {
            correlation_id: generate_correlation_id(),
            trace_id: None,
            parent_span_id: None,
            span_id: None,
            operation: None,
            service_name: None,
            timestamp: now,
            metadata: HashMap::new(),
        }
    }

    /// Set correlation ID
    pub fn with_correlation_id(mut self, correlation_id: &str) -> Self {
        self.correlation_id = correlation_id.to_string();
        self
    }

    /// Set AWS X-Ray trace ID
    ///
    /// Format: 1-{epoch_time}-{random_hex}
    /// Example: 1-67890abc-def123456789abcd
    pub fn with_xray_trace_id(mut self, trace_id: &str) -> Self {
        self.trace_id = Some(trace_id.to_string());
        self
    }

    /// Set OpenTelemetry trace ID
    pub fn with_otel_trace_id(mut self, trace_id: &str) -> Self {
        self.trace_id = Some(trace_id.to_string());
        self
    }

    /// Set parent span ID
    pub fn with_parent_span_id(mut self, span_id: &str) -> Self {
        self.parent_span_id = Some(span_id.to_string());
        self
    }

    /// Set current span ID
    pub fn with_span_id(mut self, span_id: &str) -> Self {
        self.span_id = Some(span_id.to_string());
        self
    }

    /// Set operation name
    pub fn with_operation(mut self, operation: &str) -> Self {
        self.operation = Some(operation.to_string());
        self
    }

    /// Set service name
    pub fn with_service_name(mut self, service_name: &str) -> Self {
        self.service_name = Some(service_name.to_string());
        self
    }

    /// Add custom metadata
    pub fn with_metadata(mut self, key: &str, value: &str) -> Self {
        self.metadata.insert(key.to_string(), value.to_string());
        self
    }

    /// Serialize trace context to JSON for SQS message attributes
    ///
    /// Returns a JSON string suitable for storing in SQS message attributes.
    pub fn to_header(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }

    /// Deserialize trace context from JSON header
    ///
    /// # Example
    ///
    /// ```
    /// use celers_broker_sqs::tracing_util::TraceContext;
    ///
    /// let ctx = TraceContext::new().with_operation("test");
    /// let header = ctx.to_header();
    ///
    /// let restored = TraceContext::from_header(&header).unwrap();
    /// assert_eq!(restored.operation, Some("test".to_string()));
    /// ```
    pub fn from_header(header: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(header)
    }

    /// Create a child span from this context
    ///
    /// The child span inherits the trace ID and correlation ID,
    /// and sets the current span ID as the parent.
    ///
    /// # Example
    ///
    /// ```
    /// use celers_broker_sqs::tracing_util::TraceContext;
    ///
    /// let parent = TraceContext::new()
    ///     .with_span_id("span-123")
    ///     .with_operation("parent_operation");
    ///
    /// let child = parent.create_child_span("child_operation");
    /// assert_eq!(child.parent_span_id, Some("span-123".to_string()));
    /// assert_eq!(child.operation, Some("child_operation".to_string()));
    /// assert_eq!(child.correlation_id, parent.correlation_id);
    /// ```
    pub fn create_child_span(&self, operation: &str) -> Self {
        Self {
            correlation_id: self.correlation_id.clone(),
            trace_id: self.trace_id.clone(),
            parent_span_id: self.span_id.clone(),
            span_id: Some(generate_correlation_id()),
            operation: Some(operation.to_string()),
            service_name: self.service_name.clone(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            metadata: self.metadata.clone(),
        }
    }

    /// Get elapsed time since context creation in milliseconds
    pub fn elapsed_ms(&self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        now.saturating_sub(self.timestamp)
    }
}

/// Message flow tracker for tracking messages across multiple queues
///
/// Useful for analyzing message journeys through complex workflows.
#[derive(Debug, Clone)]
pub struct MessageFlowTracker {
    /// Correlation ID
    pub correlation_id: String,
    /// Queue flow (list of queue names the message passed through)
    pub queue_flow: Vec<String>,
    /// Timestamps for each queue (Unix epoch milliseconds)
    pub timestamps: Vec<u64>,
    /// Operations performed at each step
    pub operations: Vec<String>,
}

impl MessageFlowTracker {
    /// Create a new message flow tracker
    pub fn new(correlation_id: &str, initial_queue: &str, operation: &str) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Self {
            correlation_id: correlation_id.to_string(),
            queue_flow: vec![initial_queue.to_string()],
            timestamps: vec![now],
            operations: vec![operation.to_string()],
        }
    }

    /// Record a queue transition
    pub fn record_transition(&mut self, queue_name: &str, operation: &str) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        self.queue_flow.push(queue_name.to_string());
        self.timestamps.push(now);
        self.operations.push(operation.to_string());
    }

    /// Get total flow duration in milliseconds
    pub fn total_duration_ms(&self) -> u64 {
        if self.timestamps.len() < 2 {
            return 0;
        }
        self.timestamps.last().unwrap() - self.timestamps.first().unwrap()
    }

    /// Get duration between two queue transitions
    pub fn transition_duration_ms(&self, from_index: usize, to_index: usize) -> Option<u64> {
        if from_index >= self.timestamps.len() || to_index >= self.timestamps.len() {
            return None;
        }
        Some(self.timestamps[to_index].saturating_sub(self.timestamps[from_index]))
    }

    /// Get formatted flow summary
    pub fn summary(&self) -> String {
        let flow = self.queue_flow.join(" → ");
        format!(
            "Flow: {} (Duration: {}ms, Steps: {})",
            flow,
            self.total_duration_ms(),
            self.queue_flow.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_correlation_id() {
        let id1 = generate_correlation_id();
        let id2 = generate_correlation_id();

        assert!(!id1.is_empty());
        assert!(!id2.is_empty());
        assert_ne!(id1, id2); // Should be unique
        assert_eq!(id1.len(), 36); // UUID format
    }

    #[test]
    fn test_trace_context_new() {
        let ctx = TraceContext::new();

        assert!(!ctx.correlation_id.is_empty());
        assert!(ctx.trace_id.is_none());
        assert!(ctx.span_id.is_none());
        assert!(ctx.parent_span_id.is_none());
        assert!(ctx.timestamp > 0);
    }

    #[test]
    fn test_trace_context_builder() {
        let ctx = TraceContext::new()
            .with_correlation_id("test-123")
            .with_xray_trace_id("1-67890abc-def123")
            .with_span_id("span-456")
            .with_operation("process_order")
            .with_service_name("order-service")
            .with_metadata("user_id", "12345");

        assert_eq!(ctx.correlation_id, "test-123");
        assert_eq!(ctx.trace_id, Some("1-67890abc-def123".to_string()));
        assert_eq!(ctx.span_id, Some("span-456".to_string()));
        assert_eq!(ctx.operation, Some("process_order".to_string()));
        assert_eq!(ctx.service_name, Some("order-service".to_string()));
        assert_eq!(ctx.metadata.get("user_id"), Some(&"12345".to_string()));
    }

    #[test]
    fn test_trace_context_serialization() {
        let ctx = TraceContext::new()
            .with_correlation_id("test-123")
            .with_operation("test_op");

        let header = ctx.to_header();
        assert!(!header.is_empty());

        let restored = TraceContext::from_header(&header).unwrap();
        assert_eq!(restored.correlation_id, ctx.correlation_id);
        assert_eq!(restored.operation, ctx.operation);
        assert_eq!(restored.timestamp, ctx.timestamp);
    }

    #[test]
    fn test_create_child_span() {
        let parent = TraceContext::new()
            .with_span_id("parent-span")
            .with_xray_trace_id("1-trace-id")
            .with_operation("parent_op")
            .with_metadata("tenant_id", "abc");

        let child = parent.create_child_span("child_op");

        assert_eq!(child.correlation_id, parent.correlation_id);
        assert_eq!(child.trace_id, parent.trace_id);
        assert_eq!(child.parent_span_id, Some("parent-span".to_string()));
        assert_eq!(child.operation, Some("child_op".to_string()));
        assert!(child.span_id.is_some());
        assert_ne!(child.span_id, parent.span_id);
        assert_eq!(child.metadata.get("tenant_id"), Some(&"abc".to_string()));
    }

    #[test]
    fn test_elapsed_ms() {
        let ctx = TraceContext::new();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let elapsed = ctx.elapsed_ms();
        assert!(elapsed >= 10);
        assert!(elapsed < 1000); // Should be reasonable
    }

    #[test]
    fn test_message_flow_tracker() {
        let mut tracker = MessageFlowTracker::new("corr-123", "queue1", "publish");

        assert_eq!(tracker.correlation_id, "corr-123");
        assert_eq!(tracker.queue_flow.len(), 1);
        assert_eq!(tracker.queue_flow[0], "queue1");
        assert_eq!(tracker.operations[0], "publish");

        std::thread::sleep(std::time::Duration::from_millis(10));
        tracker.record_transition("queue2", "consume");

        assert_eq!(tracker.queue_flow.len(), 2);
        assert_eq!(tracker.queue_flow[1], "queue2");
        assert_eq!(tracker.operations[1], "consume");

        let duration = tracker.total_duration_ms();
        assert!(duration >= 10);

        let summary = tracker.summary();
        assert!(summary.contains("queue1 → queue2"));
        assert!(summary.contains("Duration:"));
        assert!(summary.contains("Steps: 2"));
    }

    #[test]
    fn test_transition_duration() {
        let mut tracker = MessageFlowTracker::new("corr-123", "queue1", "publish");
        std::thread::sleep(std::time::Duration::from_millis(10));
        tracker.record_transition("queue2", "consume");

        let duration = tracker.transition_duration_ms(0, 1);
        assert!(duration.is_some());
        assert!(duration.unwrap() >= 10);

        let invalid = tracker.transition_duration_ms(0, 10);
        assert!(invalid.is_none());
    }

    #[test]
    fn test_xray_trace_id() {
        let ctx = TraceContext::new().with_xray_trace_id("1-67890abc-def123456789abcd");

        assert_eq!(
            ctx.trace_id,
            Some("1-67890abc-def123456789abcd".to_string())
        );
    }

    #[test]
    fn test_otel_trace_id() {
        let ctx = TraceContext::new().with_otel_trace_id("4bf92f3577b34da6a3ce929d0e0e4736");

        assert_eq!(
            ctx.trace_id,
            Some("4bf92f3577b34da6a3ce929d0e0e4736".to_string())
        );
    }
}
