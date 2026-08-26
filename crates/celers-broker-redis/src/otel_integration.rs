//! OpenTelemetry Integration
//!
//! Provides span tracking and W3C Trace Context propagation for broker
//! operations, with a real (`tracing`-based) exporter for local/development
//! use and an honest failure mode for backends this crate cannot actually
//! export to yet.
//!
//! # Features
//!
//! - **Span Tracking**: Spans created for enqueue, dequeue, ack operations,
//!   bounded so a leaked span (task panics/cancelled before `end_span`)
//!   cannot grow memory without limit.
//! - **Context Propagation**: W3C Trace Context format support (genuinely
//!   implemented — this part works regardless of backend).
//! - **Console Exporter**: [`TracingBackend::Console`] really exports every
//!   span through the `tracing` crate when it ends.
//! - **Other Backends Are Honest, Not Faked**: `celers-broker-redis` does
//!   not depend on the `opentelemetry`/`opentelemetry-otlp` crates, so
//!   [`OtelBrokerInstrumentation::init`] returns `Err` for
//!   [`TracingBackend::Jaeger`], [`TracingBackend::Zipkin`],
//!   [`TracingBackend::Tempo`], and [`TracingBackend::Otlp`] instead of
//!   silently accepting a configuration it cannot honor. Wiring a real
//!   exporter for those requires adding those crates as dependencies of this
//!   crate (see the module's `Cargo.toml`) and building a `TracerProvider`
//!   in `init()`.
//!
//! # Example
//!
//! ```rust,no_run
//! use celers_broker_redis::otel_integration::{
//!     OtelBrokerInstrumentation, OtelConfig, TracingBackend
//! };
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // `Console` is the only backend that actually exports today (via the
//! // `tracing` crate) besides `TracingBackend::None`.
//! let config = OtelConfig::default()
//!     .with_backend(TracingBackend::Console)
//!     .with_service_name("my-celery-app")
//!     .with_sample_rate(1.0);
//!
//! // Initialize instrumentation. Backends other than `Console`/`None`
//! // return `Err` here rather than pretending to succeed.
//! let instrumentation = OtelBrokerInstrumentation::init(config)?;
//!
//! // Broker operations can now create spans; ended spans are logged via
//! // `tracing` (target "celers_otel_console_exporter").
//! # Ok(())
//! # }
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tracing::{debug, info, warn};

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
    /// Maximum number of concurrently active (started-but-not-ended) spans
    /// to retain. When a new span would exceed this, the oldest active
    /// span(s) are evicted first. Bounds memory when spans leak (a task
    /// panics, is cancelled, or the process is shut down before `end_span`
    /// runs).
    #[serde(default = "default_max_active_spans")]
    pub max_active_spans: usize,
    /// Maximum age, in seconds, an active span may reach before it is swept
    /// as abandoned. Checked on every [`OtelBrokerInstrumentation::start_span`]
    /// call. `0` disables age-based sweeping (only `max_active_spans` then
    /// bounds the map).
    #[serde(default = "default_max_span_age_secs")]
    pub max_span_age_secs: i64,
}

fn default_max_active_spans() -> usize {
    10_000
}

fn default_max_span_age_secs() -> i64 {
    3600
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
            max_active_spans: default_max_active_spans(),
            max_span_age_secs: default_max_span_age_secs(),
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

    /// Set the maximum number of concurrently active spans (default: 10,000).
    pub fn with_max_active_spans(mut self, max: usize) -> Self {
        self.max_active_spans = max;
        self
    }

    /// Set the maximum age (seconds) an active span may reach before being
    /// swept as abandoned (default: 3600). `0` disables age-based sweeping.
    pub fn with_max_span_age_secs(mut self, secs: i64) -> Self {
        self.max_span_age_secs = secs;
        self
    }
}

/// Build the error returned by [`OtelBrokerInstrumentation::init`] for a
/// backend this crate cannot actually export to.
fn unsupported_backend_error(backend: &str, endpoint: &str) -> Box<dyn std::error::Error> {
    format!(
        "OpenTelemetry backend '{backend}' (endpoint: {endpoint}) is not implemented: \
         celers-broker-redis does not depend on the `opentelemetry`/`opentelemetry-otlp` crates, \
         so no exporter can be constructed for it. Only TracingBackend::Console (spans logged via \
         the `tracing` crate) and TracingBackend::None (tracing disabled) are functional in this \
         build — failing here at startup, instead of silently accepting the configuration and \
         dropping every span, so a misconfigured exporter is caught before it costs you an \
         incident's worth of missing traces. To support this backend for real, add the relevant \
         opentelemetry* crate(s) to celers-broker-redis's Cargo.toml and build a TracerProvider \
         in `OtelBrokerInstrumentation::init`."
    )
    .into()
}

impl OtelBrokerInstrumentation {
    /// Initialize OpenTelemetry instrumentation.
    ///
    /// Succeeds only for [`TracingBackend::Console`] (spans are exported via
    /// the `tracing` crate) and [`TracingBackend::None`] (tracing disabled).
    /// Every other backend returns `Err` — see the module documentation.
    pub fn init(config: OtelConfig) -> Result<Self, Box<dyn std::error::Error>> {
        info!(
            "Initializing OpenTelemetry instrumentation for service: {}",
            config.service_name
        );

        match &config.backend {
            TracingBackend::Jaeger { endpoint } => {
                return Err(unsupported_backend_error("Jaeger", endpoint));
            }
            TracingBackend::Zipkin { endpoint } => {
                return Err(unsupported_backend_error("Zipkin", endpoint));
            }
            TracingBackend::Tempo { endpoint } => {
                return Err(unsupported_backend_error("Tempo", endpoint));
            }
            TracingBackend::Otlp { endpoint, use_http } => {
                let scheme = if *use_http { "http" } else { "grpc" };
                return Err(unsupported_backend_error(
                    "OTLP",
                    &format!("{endpoint} ({scheme})"),
                ));
            }
            TracingBackend::Console => {
                debug!("Configuring console exporter (spans logged via `tracing`)");
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

    /// Start a span for an operation.
    ///
    /// Sweeps stale/excess entries from the active-span map first (see
    /// `evict_stale_spans_locked`) so a leaked span can never grow
    /// the map without bound.
    pub fn start_span(&self, name: &str, attributes: HashMap<String, String>) -> String {
        let span_id = generate_span_id();
        let trace_id = generate_trace_id();
        let now_us = chrono::Utc::now().timestamp_micros();

        let span_info = SpanInfo {
            span_id: span_id.clone(),
            trace_id,
            name: name.to_string(),
            start_time_us: now_us,
            attributes,
        };

        if let Ok(mut spans) = self.active_spans.write() {
            self.evict_stale_spans_locked(&mut spans, now_us);
            spans.insert(span_id.clone(), span_info);
        }

        debug!("Started span: {} ({})", name, span_id);
        span_id
    }

    /// Evict spans older than `max_span_age_secs`, then — if the map is
    /// still at or over `max_active_spans` — evict the oldest span(s) until
    /// back under the cap. Called with the write lock already held.
    fn evict_stale_spans_locked(&self, spans: &mut HashMap<String, SpanInfo>, now_us: i64) {
        if self.config.max_span_age_secs > 0 {
            let max_age_us = self.config.max_span_age_secs.saturating_mul(1_000_000);
            let before = spans.len();
            spans.retain(|_, s| now_us.saturating_sub(s.start_time_us) < max_age_us);
            let evicted = before - spans.len();
            if evicted > 0 {
                warn!(
                    "Evicted {} stale OpenTelemetry span(s) older than {}s (never ended — likely \
                     a panicked, cancelled, or otherwise abandoned operation)",
                    evicted, self.config.max_span_age_secs
                );
            }
        }

        if self.config.max_active_spans > 0 && spans.len() >= self.config.max_active_spans {
            let overflow = spans.len() + 1 - self.config.max_active_spans;
            let mut by_age: Vec<(String, i64)> = spans
                .iter()
                .map(|(id, info)| (id.clone(), info.start_time_us))
                .collect();
            by_age.sort_by_key(|(_, start)| *start);
            for (id, _) in by_age.into_iter().take(overflow) {
                spans.remove(&id);
            }
            warn!(
                "Active span map reached its cap of {} — evicted {} oldest span(s)",
                self.config.max_active_spans, overflow
            );
        }
    }

    /// End a span.
    ///
    /// For [`TracingBackend::Console`] this is a real export: the full span
    /// (trace id, span id, name, duration, attributes) is logged via
    /// `tracing` at `info` level under the `celers_otel_console_exporter`
    /// target.
    pub fn end_span(&self, span_id: &str) {
        if let Ok(mut spans) = self.active_spans.write() {
            if let Some(span_info) = spans.remove(span_id) {
                let duration_us = chrono::Utc::now().timestamp_micros() - span_info.start_time_us;

                match self.config.backend {
                    TracingBackend::Console => {
                        info!(
                            target: "celers_otel_console_exporter",
                            trace_id = %span_info.trace_id,
                            span_id = %span_info.span_id,
                            name = %span_info.name,
                            duration_us,
                            attributes = ?span_info.attributes,
                            "span"
                        );
                    }
                    TracingBackend::None => {
                        // Tracing disabled: nothing to export.
                    }
                    _ => {
                        // Unreachable in practice: `init()` rejects every
                        // other backend. Kept as a safe fallback rather than
                        // a panic if that invariant is ever broken.
                        debug!(
                            "Ended span: {} ({}) - duration: {}μs (no exporter wired for this backend)",
                            span_info.name, span_id, duration_us
                        );
                    }
                }
            }
        }
    }

    /// Add an event to a span. For [`TracingBackend::Console`], logs the
    /// event (with its attributes) via `tracing` immediately.
    pub fn add_span_event(
        &self,
        span_id: &str,
        event_name: &str,
        attributes: HashMap<String, String>,
    ) {
        if let Ok(spans) = self.active_spans.read() {
            if let Some(span_info) = spans.get(span_id) {
                match self.config.backend {
                    TracingBackend::Console => {
                        info!(
                            target: "celers_otel_console_exporter",
                            trace_id = %span_info.trace_id,
                            span_id = %span_id,
                            event = %event_name,
                            attributes = ?attributes,
                            "span event"
                        );
                    }
                    _ => {
                        debug!("Added event '{}' to span {}", event_name, span_id);
                    }
                }
            }
        }
    }

    /// Get active span count
    pub fn active_span_count(&self) -> usize {
        self.active_spans
            .read()
            .map(|spans| spans.len())
            .unwrap_or(0)
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
    use rand::RngExt;
    let mut rng = rand::rng();
    let bytes: Vec<u8> = (0..16).map(|_| rng.random()).collect();
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Generate a random span ID (16 hex characters)
fn generate_span_id() -> String {
    use rand::RngExt;
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
    fn test_init_succeeds_for_console_and_none() {
        assert!(OtelBrokerInstrumentation::init(
            OtelConfig::default().with_backend(TracingBackend::Console)
        )
        .is_ok());
        assert!(OtelBrokerInstrumentation::init(
            OtelConfig::default().with_backend(TracingBackend::None)
        )
        .is_ok());
    }

    #[test]
    fn test_init_rejects_unimplemented_backends_instead_of_faking_success() {
        // Before the fix, every one of these silently returned `Ok` with no
        // exporter ever constructed -- a misconfigured OTLP/Jaeger/Zipkin/
        // Tempo endpoint would only be discovered by the total absence of
        // traces in production. `init()` must fail loudly instead.
        let backends = vec![
            TracingBackend::Jaeger {
                endpoint: "http://localhost:14268/api/traces".to_string(),
            },
            TracingBackend::Zipkin {
                endpoint: "http://localhost:9411/api/v2/spans".to_string(),
            },
            TracingBackend::Tempo {
                endpoint: "http://localhost:4318".to_string(),
            },
            TracingBackend::Otlp {
                endpoint: "http://localhost:4317".to_string(),
                use_http: false,
            },
        ];

        for backend in backends {
            let config = OtelConfig::default().with_backend(backend.clone());
            let result = OtelBrokerInstrumentation::init(config);
            assert!(
                result.is_err(),
                "init() should reject unimplemented backend {backend:?} instead of faking success"
            );
            let msg = result.unwrap_err().to_string();
            assert!(
                msg.contains("opentelemetry"),
                "error should explain the gap: {msg}"
            );
        }
    }

    #[test]
    fn test_active_spans_evict_stale_by_age() {
        let config = OtelConfig::default().with_max_span_age_secs(1);
        let instrumentation = OtelBrokerInstrumentation::init(config).unwrap();

        // Insert a span "started" at the Unix epoch, bypassing `start_span`'s
        // real clock so the test is deterministic (no sleeping needed): it
        // is unconditionally far older than the 1-second max age.
        {
            let mut spans = instrumentation.active_spans.write().unwrap();
            spans.insert(
                "stale-span".to_string(),
                SpanInfo {
                    span_id: "stale-span".to_string(),
                    trace_id: "t".to_string(),
                    name: "old".to_string(),
                    start_time_us: 0,
                    attributes: HashMap::new(),
                },
            );
        }
        assert_eq!(instrumentation.active_span_count(), 1);

        // The next `start_span` call sweeps stale entries before inserting.
        let _new_id = instrumentation.start_span("fresh", HashMap::new());
        assert_eq!(
            instrumentation.active_span_count(),
            1,
            "the stale span should have been evicted, leaving only the fresh one"
        );
    }

    #[test]
    fn test_active_spans_bounded_by_cap() {
        let config = OtelConfig::default()
            .with_max_active_spans(3)
            .with_max_span_age_secs(0); // isolate cap-based eviction
        let instrumentation = OtelBrokerInstrumentation::init(config).unwrap();

        for i in 0..10 {
            instrumentation.start_span(&format!("span-{i}"), HashMap::new());
        }

        assert!(
            instrumentation.active_span_count() <= 3,
            "active span count {} exceeded the configured cap of 3",
            instrumentation.active_span_count()
        );
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
