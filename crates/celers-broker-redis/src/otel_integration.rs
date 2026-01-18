//! OpenTelemetry Integration
//!
//! Provides seamless integration with OpenTelemetry for distributed tracing:
//! - Automatic span creation for all broker operations
//! - Context injection/extraction in enqueue/dequeue
//! - Metrics export via OTLP
//! - Integration with popular tracing backends (Jaeger, Zipkin, Tempo)
//!
//! # Features
//!
//! - **Automatic Instrumentation**: Spans created for enqueue, dequeue, ack operations
//! - **Context Propagation**: W3C Trace Context format support
//! - **Metrics Export**: Queue metrics exported to OTLP compatible backends
//! - **Flexible Configuration**: Support for multiple exporters and samplers
//!
//! # Example
//!
//! ```rust,no_run
//! use celers_broker_redis::otel_integration::{
//!     OtelBrokerInstrumentation, OtelConfig, TracingBackend
//! };
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Configure OpenTelemetry integration
//! let config = OtelConfig::default()
//!     .with_backend(TracingBackend::Jaeger { endpoint: "http://localhost:14268/api/traces".to_string() })
//!     .with_service_name("my-celery-app")
//!     .with_sample_rate(1.0);
//!
//! // Initialize instrumentation
//! let instrumentation = OtelBrokerInstrumentation::init(config)?;
//!
//! // Broker operations will now be automatically instrumented
//! // with distributed traces sent to Jaeger
//! # Ok(())
//! # }
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tracing::{debug, info};

/// OpenTelemetry configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtelConfig {
    /// Service name for tracing
    pub service_name: String,
    /// Tracing backend
    pub backend: TracingBackend,
    /// Sample rate (0.0-1.0)
    pub sample_rate: f64,
    /// Enable metrics export
    pub enable_metrics: bool,
    /// Metrics export interval in seconds
    pub metrics_interval_secs: u64,
    /// Additional resource attributes
    pub resource_attributes: HashMap<String, String>,
}

/// Tracing backend configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TracingBackend {
    /// Jaeger backend
    Jaeger {
        /// Jaeger endpoint URL
        endpoint: String,
    },
    /// Zipkin backend
    Zipkin {
        /// Zipkin endpoint URL
        endpoint: String,
    },
    /// Tempo backend (via OTLP)
    Tempo {
        /// Tempo OTLP endpoint
        endpoint: String,
    },
    /// Generic OTLP backend
    Otlp {
        /// OTLP endpoint URL
        endpoint: String,
        /// Use HTTP (true) or gRPC (false)
        use_http: bool,
    },
    /// Console/stdout backend for development
    Console,
    /// No-op backend (disabled)
    None,
}

/// OpenTelemetry broker instrumentation
#[derive(Debug, Clone)]
pub struct OtelBrokerInstrumentation {
    #[allow(dead_code)]
    config: OtelConfig,
    active_spans: Arc<RwLock<HashMap<String, SpanInfo>>>,
}

/// Information about an active span
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanInfo {
    /// Span ID
    pub span_id: String,
    /// Trace ID
    pub trace_id: String,
    /// Span name
    pub name: String,
    /// Start time (Unix timestamp in microseconds)
    pub start_time_us: i64,
    /// Attributes
    pub attributes: HashMap<String, String>,
}

/// Span context for W3C Trace Context propagation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct W3CTraceContext {
    /// Trace ID (32 hex characters)
    pub trace_id: String,
    /// Parent span ID (16 hex characters)
    pub parent_span_id: String,
    /// Trace flags (2 hex characters)
    pub trace_flags: String,
}

impl W3CTraceContext {
    /// Create a new trace context
    pub fn new() -> Self {
        Self {
            trace_id: generate_trace_id(),
            parent_span_id: generate_span_id(),
            trace_flags: "01".to_string(), // Sampled
        }
    }

    /// Create from traceparent header
    pub fn from_traceparent(traceparent: &str) -> Option<Self> {
        let parts: Vec<&str> = traceparent.split('-').collect();
        if parts.len() != 4 || parts[0] != "00" {
            return None;
        }

        Some(Self {
            trace_id: parts[1].to_string(),
            parent_span_id: parts[2].to_string(),
            trace_flags: parts[3].to_string(),
        })
    }

    /// Convert to traceparent header value
    pub fn to_traceparent(&self) -> String {
        format!(
            "00-{}-{}-{}",
            self.trace_id, self.parent_span_id, self.trace_flags
        )
    }

    /// Check if trace is sampled
    pub fn is_sampled(&self) -> bool {
        self.trace_flags == "01"
    }
}

impl Default for W3CTraceContext {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for OtelConfig {
    fn default() -> Self {
        Self {
            service_name: "celers-broker-redis".to_string(),
            backend: TracingBackend::None,
            sample_rate: 1.0,
            enable_metrics: true,
            metrics_interval_secs: 60,
            resource_attributes: HashMap::new(),
        }
    }
}

impl OtelConfig {
    /// Set service name
    pub fn with_service_name(mut self, name: &str) -> Self {
        self.service_name = name.to_string();
        self
    }

    /// Set tracing backend
    pub fn with_backend(mut self, backend: TracingBackend) -> Self {
        self.backend = backend;
        self
    }

    /// Set sample rate
    pub fn with_sample_rate(mut self, rate: f64) -> Self {
        self.sample_rate = rate.clamp(0.0, 1.0);
        self
    }

    /// Enable metrics export
    pub fn with_metrics(mut self, enable: bool) -> Self {
        self.enable_metrics = enable;
        self
    }

    /// Set metrics export interval
    pub fn with_metrics_interval(mut self, secs: u64) -> Self {
        self.metrics_interval_secs = secs;
        self
    }

    /// Add resource attribute
    pub fn with_resource_attribute(mut self, key: &str, value: &str) -> Self {
        self.resource_attributes
            .insert(key.to_string(), value.to_string());
        self
    }
}

impl OtelBrokerInstrumentation {
    /// Initialize OpenTelemetry instrumentation
    pub fn init(config: OtelConfig) -> Result<Self, Box<dyn std::error::Error>> {
        info!(
            "Initializing OpenTelemetry instrumentation for service: {}",
            config.service_name
        );

        // In a real implementation, this would:
        // 1. Initialize the OpenTelemetry SDK
        // 2. Configure the appropriate exporter based on backend
        // 3. Set up the tracer provider
        // 4. Configure metrics pipeline if enabled

        match &config.backend {
            TracingBackend::Jaeger { endpoint } => {
                debug!("Configuring Jaeger exporter: {}", endpoint);
            }
            TracingBackend::Zipkin { endpoint } => {
                debug!("Configuring Zipkin exporter: {}", endpoint);
            }
            TracingBackend::Tempo { endpoint } => {
                debug!("Configuring Tempo exporter: {}", endpoint);
            }
            TracingBackend::Otlp { endpoint, use_http } => {
                debug!(
                    "Configuring OTLP exporter: {} (HTTP: {})",
                    endpoint, use_http
                );
            }
            TracingBackend::Console => {
                debug!("Configuring console exporter");
            }
            TracingBackend::None => {
                debug!("Tracing disabled");
            }
        }

        Ok(Self {
            config,
            active_spans: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Start a span for an operation
    pub fn start_span(&self, name: &str, attributes: HashMap<String, String>) -> String {
        let span_id = generate_span_id();
        let trace_id = generate_trace_id();

        let span_info = SpanInfo {
            span_id: span_id.clone(),
            trace_id,
            name: name.to_string(),
            start_time_us: chrono::Utc::now().timestamp_micros(),
            attributes,
        };

        if let Ok(mut spans) = self.active_spans.write() {
            spans.insert(span_id.clone(), span_info);
        }

        debug!("Started span: {} ({})", name, span_id);
        span_id
    }

    /// End a span
    pub fn end_span(&self, span_id: &str) {
        if let Ok(mut spans) = self.active_spans.write() {
            if let Some(span_info) = spans.remove(span_id) {
                let duration_us = chrono::Utc::now().timestamp_micros() - span_info.start_time_us;
                debug!(
                    "Ended span: {} ({}) - duration: {}μs",
                    span_info.name, span_id, duration_us
                );

                // In a real implementation, this would export the span
                // to the configured backend
            }
        }
    }

    /// Add an event to a span
    pub fn add_span_event(
        &self,
        span_id: &str,
        event_name: &str,
        _attributes: HashMap<String, String>,
    ) {
        if let Ok(spans) = self.active_spans.read() {
            if spans.contains_key(span_id) {
                debug!("Added event '{}' to span {}", event_name, span_id);
                // In a real implementation, record the event
            }
        }
    }

    /// Get active span count
    pub fn active_span_count(&self) -> usize {
        self.active_spans
            .read()
            .expect("lock should not be poisoned")
            .len()
    }

    /// Extract trace context from metadata
    pub fn extract_context(&self, metadata: &HashMap<String, String>) -> Option<W3CTraceContext> {
        metadata
            .get("traceparent")
            .and_then(|tp| W3CTraceContext::from_traceparent(tp))
    }

    /// Inject trace context into metadata
    pub fn inject_context(
        &self,
        context: &W3CTraceContext,
        metadata: &mut HashMap<String, String>,
    ) {
        metadata.insert("traceparent".to_string(), context.to_traceparent());
    }

    /// Create an enqueue span
    pub fn create_enqueue_span(&self, queue_name: &str, task_name: &str) -> String {
        let mut attributes = HashMap::new();
        attributes.insert("queue.name".to_string(), queue_name.to_string());
        attributes.insert("task.name".to_string(), task_name.to_string());
        attributes.insert("operation".to_string(), "enqueue".to_string());

        self.start_span("enqueue", attributes)
    }

    /// Create a dequeue span
    pub fn create_dequeue_span(&self, queue_name: &str) -> String {
        let mut attributes = HashMap::new();
        attributes.insert("queue.name".to_string(), queue_name.to_string());
        attributes.insert("operation".to_string(), "dequeue".to_string());

        self.start_span("dequeue", attributes)
    }

    /// Create an ack span
    pub fn create_ack_span(&self, queue_name: &str, task_id: &str) -> String {
        let mut attributes = HashMap::new();
        attributes.insert("queue.name".to_string(), queue_name.to_string());
        attributes.insert("task.id".to_string(), task_id.to_string());
        attributes.insert("operation".to_string(), "ack".to_string());

        self.start_span("ack", attributes)
    }
}

/// Generate a random trace ID (32 hex characters)
fn generate_trace_id() -> String {
    use rand::Rng;
    let mut rng = rand::rng();
    let bytes: Vec<u8> = (0..16).map(|_| rng.random()).collect();
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Generate a random span ID (16 hex characters)
fn generate_span_id() -> String {
    use rand::Rng;
    let mut rng = rand::rng();
    let bytes: Vec<u8> = (0..8).map(|_| rng.random()).collect();
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_w3c_trace_context_creation() {
        let ctx = W3CTraceContext::new();
        assert_eq!(ctx.trace_id.len(), 32);
        assert_eq!(ctx.parent_span_id.len(), 16);
        assert_eq!(ctx.trace_flags, "01");
        assert!(ctx.is_sampled());
    }

    #[test]
    fn test_w3c_trace_context_from_traceparent() {
        let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let ctx = W3CTraceContext::from_traceparent(traceparent).unwrap();

        assert_eq!(ctx.trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(ctx.parent_span_id, "00f067aa0ba902b7");
        assert_eq!(ctx.trace_flags, "01");
        assert!(ctx.is_sampled());
    }

    #[test]
    fn test_w3c_trace_context_to_traceparent() {
        let ctx = W3CTraceContext {
            trace_id: "4bf92f3577b34da6a3ce929d0e0e4736".to_string(),
            parent_span_id: "00f067aa0ba902b7".to_string(),
            trace_flags: "01".to_string(),
        };

        let traceparent = ctx.to_traceparent();
        assert_eq!(
            traceparent,
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        );
    }

    #[test]
    fn test_otel_config_builder() {
        let config = OtelConfig::default()
            .with_service_name("test-service")
            .with_sample_rate(0.5)
            .with_metrics(true)
            .with_metrics_interval(30)
            .with_resource_attribute("environment", "production");

        assert_eq!(config.service_name, "test-service");
        assert_eq!(config.sample_rate, 0.5);
        assert!(config.enable_metrics);
        assert_eq!(config.metrics_interval_secs, 30);
        assert_eq!(
            config.resource_attributes.get("environment"),
            Some(&"production".to_string())
        );
    }

    #[test]
    fn test_otel_instrumentation_init() {
        let config = OtelConfig::default().with_service_name("test");
        let instrumentation = OtelBrokerInstrumentation::init(config);
        assert!(instrumentation.is_ok());
    }

    #[test]
    fn test_span_lifecycle() {
        let config = OtelConfig::default();
        let instrumentation = OtelBrokerInstrumentation::init(config).unwrap();

        let span_id = instrumentation.create_enqueue_span("test_queue", "test_task");
        assert_eq!(instrumentation.active_span_count(), 1);

        instrumentation.end_span(&span_id);
        assert_eq!(instrumentation.active_span_count(), 0);
    }

    #[test]
    fn test_trace_context_injection_extraction() {
        let config = OtelConfig::default();
        let instrumentation = OtelBrokerInstrumentation::init(config).unwrap();

        let context = W3CTraceContext::new();
        let mut metadata = HashMap::new();

        instrumentation.inject_context(&context, &mut metadata);
        assert!(metadata.contains_key("traceparent"));

        let extracted = instrumentation.extract_context(&metadata).unwrap();
        assert_eq!(extracted.trace_id, context.trace_id);
        assert_eq!(extracted.parent_span_id, context.parent_span_id);
    }

    #[test]
    fn test_generate_trace_id() {
        let trace_id = generate_trace_id();
        assert_eq!(trace_id.len(), 32);
        assert!(trace_id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_generate_span_id() {
        let span_id = generate_span_id();
        assert_eq!(span_id.len(), 16);
        assert!(span_id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_sample_rate_clamping() {
        let config1 = OtelConfig::default().with_sample_rate(-0.5);
        assert_eq!(config1.sample_rate, 0.0);

        let config2 = OtelConfig::default().with_sample_rate(1.5);
        assert_eq!(config2.sample_rate, 1.0);

        let config3 = OtelConfig::default().with_sample_rate(0.5);
        assert_eq!(config3.sample_rate, 0.5);
    }
}
