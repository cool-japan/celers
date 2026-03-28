pub use opentelemetry;
pub use opentelemetry_sdk;
pub use tracing;
pub use tracing_opentelemetry;
pub use tracing_subscriber;

use opentelemetry::trace::SpanKind;
use opentelemetry::KeyValue;
use opentelemetry_sdk::trace::{RandomIdGenerator, Sampler, SdkTracerProvider};
use opentelemetry_sdk::Resource;
use tracing::Span;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Initialize basic tracing with OpenTelemetry support
///
/// This sets up tracing with console output. For production use,
/// configure your own exporter (e.g., Jaeger, OTLP) using the
/// opentelemetry_sdk crate directly.
///
/// # Example
///
/// ```rust,ignore
/// use celers::tracing::init_tracing;
///
/// init_tracing("my-service").expect("Failed to initialize tracing");
/// ```
pub fn init_tracing(_service_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing subscriber with basic formatting
    // Note: service_name is accepted for API consistency but not used
    // in the basic setup. For production use with service identification,
    // use create_tracer_provider() to build a custom TracerProvider.
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .try_init()
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

    Ok(())
}

/// Create a TracerProvider with the given service name and exporter
///
/// # Example
///
/// ```rust,ignore
/// use celers::tracing::create_tracer_provider;
/// use opentelemetry_sdk::trace::Sampler;
///
/// // Configure your own exporter here
/// let provider = create_tracer_provider("my-service");
/// ```
pub fn create_tracer_provider(service_name: &str) -> SdkTracerProvider {
    let resource = Resource::builder()
        .with_attributes([KeyValue::new("service.name", service_name.to_string())])
        .build();

    SdkTracerProvider::builder()
        .with_sampler(Sampler::AlwaysOn)
        .with_id_generator(RandomIdGenerator::default())
        .with_resource(resource)
        .build()
}

/// Create a new span for task execution
///
/// # Example
///
/// ```rust,ignore
/// use celers::tracing::task_span;
///
/// let span = task_span("my_task", "task-id-123");
/// let _guard = span.enter();
/// // Task execution code here
/// ```
pub fn task_span(task_name: &str, task_id: &str) -> Span {
    tracing::info_span!(
        "task.execute",
        task.name = task_name,
        task.id = task_id,
        otel.kind = ?SpanKind::Consumer
    )
}

/// Create a new span for task publish
///
/// # Example
///
/// ```rust,ignore
/// use celers::tracing::publish_span;
///
/// let span = publish_span("my_task", "task-id-123");
/// let _guard = span.enter();
/// // Publish code here
/// ```
pub fn publish_span(task_name: &str, task_id: &str) -> Span {
    tracing::info_span!(
        "task.publish",
        task.name = task_name,
        task.id = task_id,
        otel.kind = ?SpanKind::Producer
    )
}

/// Extract trace context from message headers
///
/// This allows distributed tracing across task boundaries
pub fn extract_trace_context(headers: &std::collections::HashMap<String, String>) {
    use opentelemetry::propagation::TextMapPropagator;
    use opentelemetry_sdk::propagation::TraceContextPropagator;

    let propagator = TraceContextPropagator::new();
    let context = propagator.extract(headers);
    let _ = tracing::Span::current().set_parent(context);
}

/// Inject trace context into message headers
///
/// This allows distributed tracing across task boundaries
pub fn inject_trace_context(headers: &mut std::collections::HashMap<String, String>) {
    use opentelemetry::propagation::TextMapPropagator;
    use opentelemetry_sdk::propagation::TraceContextPropagator;

    let propagator = TraceContextPropagator::new();
    let context = tracing::Span::current().context();
    propagator.inject_context(&context, headers);
}
