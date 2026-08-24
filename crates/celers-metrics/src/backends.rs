//! Backend integration support: a [`MetricBackend`] trait plus two real,
//! in-crate adapters -- [`StatsdExporter`] (StatsD/DogStatsD over UDP,
//! implemented in [`crate::statsd`]) and [`PrometheusBackend`] (a native
//! Prometheus registry) -- current metrics capture, and metric comparison
//! utilities.
//!
//! [`OpenTelemetryConfig`], [`CloudWatchConfig`], and [`DatadogConfig`] are
//! typed *configuration* holders only: this crate does not bundle those
//! vendors' network clients (doing so would drag a large dependency tree,
//! frequently non-pure-Rust, into every consumer of this crate, the
//! overwhelming majority of which never touch those backends). Their
//! `connect()` methods always return [`UnsupportedBackend`], documenting
//! what to wire up yourself instead of silently doing nothing.

use crate::aggregation::{CustomLabels, MetricStats};
use crate::prometheus_metrics::*;
use crate::statsd::{StatsdExporter, StatsdMetric};
use prometheus::Encoder as _;
use std::collections::HashMap;

// ============================================================================
// Backend Integration Support
// ============================================================================

/// Metric export format for different backends
#[derive(Debug, Clone)]
pub enum MetricExport {
    /// Counter metric (name, value, labels)
    Counter {
        name: String,
        value: f64,
        labels: CustomLabels,
    },
    /// Gauge metric (name, value, labels)
    Gauge {
        name: String,
        value: f64,
        labels: CustomLabels,
    },
    /// Histogram metric (name, observations, labels)
    Histogram {
        name: String,
        count: u64,
        sum: f64,
        buckets: Vec<(f64, u64)>,
        labels: CustomLabels,
    },
}

impl MetricExport {
    /// Get metric name
    pub fn name(&self) -> &str {
        match self {
            MetricExport::Counter { name, .. } => name,
            MetricExport::Gauge { name, .. } => name,
            MetricExport::Histogram { name, .. } => name,
        }
    }

    /// Get metric labels
    pub fn labels(&self) -> &CustomLabels {
        match self {
            MetricExport::Counter { labels, .. } => labels,
            MetricExport::Gauge { labels, .. } => labels,
            MetricExport::Histogram { labels, .. } => labels,
        }
    }
}

/// Trait for exporting metrics to different backends
pub trait MetricBackend {
    /// Export a metric to the backend
    fn export(&mut self, metric: &MetricExport) -> Result<(), String>;

    /// Flush any buffered metrics
    fn flush(&mut self) -> Result<(), String>;
}

/// [`MetricBackend`] adapter over a connected [`StatsdExporter`].
///
/// `Counter`/`Gauge` map directly to their StatsD wire types. A StatsD line
/// carries one scalar per metric (there is no wire representation for a
/// pre-aggregated histogram), so `Histogram` is exported as a single StatsD
/// histogram sample of the batch's mean (`sum / count`) -- exact for that one
/// export, an approximation if the same name is exported repeatedly (the same
/// trade-off [`StatsDConfig::format_metric`] makes).
///
/// # Examples
///
/// ```no_run
/// use celers_metrics::{MetricBackend, MetricExport, CustomLabels, StatsdExporter};
///
/// // `connect` returns `Result<_, StatsdError>`; `MetricBackend::export`
/// // returns `Result<_, String>`, so it is handled separately from `?`.
/// let mut backend = StatsdExporter::connect("127.0.0.1:8125").expect("connect");
/// backend.export(&MetricExport::Counter {
///     name: "api.requests".to_string(),
///     value: 1.0,
///     labels: CustomLabels::new(),
/// })?;
/// backend.flush()?;
/// # Ok::<(), String>(())
/// ```
impl MetricBackend for StatsdExporter {
    fn export(&mut self, metric: &MetricExport) -> Result<(), String> {
        let statsd_metric = match metric {
            MetricExport::Counter {
                name,
                value,
                labels,
            } => StatsdMetric::counter(name.clone(), *value).with_labels(labels.clone()),
            MetricExport::Gauge {
                name,
                value,
                labels,
            } => StatsdMetric::gauge(name.clone(), *value).with_labels(labels.clone()),
            MetricExport::Histogram {
                name,
                count,
                sum,
                labels,
                ..
            } => {
                let mean = if *count > 0 {
                    sum / (*count as f64)
                } else {
                    0.0
                };
                StatsdMetric::histogram(name.clone(), mean).with_labels(labels.clone())
            }
        };
        self.send(&statsd_metric).map_err(|err| err.to_string())
    }

    /// UDP datagrams are sent immediately by [`Self::export`]; there is no
    /// internal buffer to flush.
    fn flush(&mut self) -> Result<(), String> {
        Ok(())
    }
}

/// StatsD metric backend helper
#[derive(Debug, Clone)]
pub struct StatsDConfig {
    /// StatsD server host
    pub host: String,
    /// StatsD server port
    pub port: u16,
    /// Metric prefix
    pub prefix: String,
    /// Sample rate (0.0 to 1.0)
    pub sample_rate: f64,
}

impl Default for StatsDConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 8125,
            prefix: "celers".to_string(),
            sample_rate: 1.0,
        }
    }
}

impl StatsDConfig {
    /// Create a new StatsD configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the host
    pub fn with_host(mut self, host: impl Into<String>) -> Self {
        self.host = host.into();
        self
    }

    /// Set the port
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Set the metric prefix
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Set the sample rate
    pub fn with_sample_rate(mut self, rate: f64) -> Self {
        self.sample_rate = rate.clamp(0.0, 1.0);
        self
    }

    /// Format a metric for the StatsD wire protocol.
    ///
    /// Delegates to [`crate::statsd::format_statsd_line`] for the name
    /// sanitisation and tag sanitisation applied there, and honours
    /// [`Self::sample_rate`] (this method previously duplicated the wire
    /// formatting without either: an unsanitised name or tag could corrupt
    /// the datagram, and `sample_rate` was silently dropped).
    pub fn format_metric(&self, metric: &MetricExport) -> String {
        let name = format!("{}.{}", self.prefix, metric.name());
        let sample_rate = if self.sample_rate < 1.0 {
            Some(self.sample_rate)
        } else {
            None
        };

        match metric {
            MetricExport::Counter { value, labels, .. } => crate::statsd::format_statsd_line(
                &name,
                *value,
                crate::statsd::StatsdMetricKind::Counter,
                sample_rate,
                labels,
            ),
            MetricExport::Gauge { value, labels, .. } => crate::statsd::format_statsd_line(
                &name,
                *value,
                crate::statsd::StatsdMetricKind::Gauge,
                sample_rate,
                labels,
            ),
            MetricExport::Histogram {
                sum, count, labels, ..
            } => {
                let avg = if *count > 0 {
                    sum / (*count as f64)
                } else {
                    0.0
                };
                crate::statsd::format_statsd_line(
                    &name,
                    avg,
                    crate::statsd::StatsdMetricKind::Histogram,
                    sample_rate,
                    labels,
                )
            }
        }
    }
}

// ============================================================================
// Native Prometheus Backend
// ============================================================================

/// One dynamically-registered Prometheus metric, tagged by its wire type so
/// [`PrometheusBackend`] can enforce Prometheus's one-type-per-name rule.
#[derive(Debug, Clone)]
enum PrometheusHandle {
    Counter(prometheus::Counter),
    Gauge(prometheus::Gauge),
    Histogram(prometheus::Histogram),
}

impl PrometheusHandle {
    fn kind_name(&self) -> &'static str {
        match self {
            PrometheusHandle::Counter(_) => "counter",
            PrometheusHandle::Gauge(_) => "gauge",
            PrometheusHandle::Histogram(_) => "histogram",
        }
    }
}

/// A [`MetricBackend`] adapter that dynamically registers and updates native
/// Prometheus metrics.
///
/// Unlike the crate's fixed, `lazy_static`-declared metrics in
/// [`crate::prometheus_metrics`] (which exist for the process's lifetime and
/// are scraped via [`crate::prometheus_metrics::gather_metrics`]), this
/// backend registers a metric under its own [`prometheus::Registry`] the
/// first time its name is seen via [`Self::export`] and reuses it on every
/// later call. Using a private registry (rather than the crate-wide default
/// one) means independent `PrometheusBackend` instances -- e.g. one per test,
/// or one per tenant -- never collide over metric names, and this backend
/// never pollutes [`crate::prometheus_metrics::gather_metrics`]'s output.
/// Call [`Self::gather`] (or hand [`Self::registry`] to your own HTTP
/// handler) to expose what has been recorded.
///
/// # Limitations
///
/// * Prometheus counters only ever increase: exporting a
///   [`MetricExport::Counter`] with a negative value returns an error rather
///   than corrupting the counter or panicking (the underlying
///   `prometheus::Counter::inc_by` debug-asserts non-negative).
/// * A Prometheus [`prometheus::Histogram`] only supports incremental
///   `observe()`, not "set from a pre-aggregated `(sum, count)` pair" --
///   there is no API for that. Since [`MetricExport::Histogram`] already
///   carries pre-aggregated `sum`/`count`, each export folds in one
///   synthetic observation at the batch's mean (`sum / count`), matching the
///   same trade-off the StatsD adapter makes.
/// * Re-exporting a name under a different [`MetricExport`] variant (e.g.
///   `Counter` then `Gauge`) returns an error, mirroring Prometheus's
///   one-type-per-name rule.
/// * Non-empty labels are rejected with an error rather than silently
///   dropped: a plain `Counter`/`Gauge`/`Histogram` has no dimensions, and
///   [`CustomLabels`] carries no fixed schema across calls the way
///   `CounterVec`/`GaugeVec`/`HistogramVec` (used by
///   [`crate::prometheus_metrics`]'s `*_BY_TYPE` metrics) require.
///
/// # Examples
///
/// ```
/// use celers_metrics::{MetricBackend, MetricExport, CustomLabels, PrometheusBackend};
///
/// let mut backend = PrometheusBackend::new();
/// backend.export(&MetricExport::Counter {
///     name: "api_requests_total".to_string(),
///     value: 1.0,
///     labels: CustomLabels::new(),
/// }).expect("export");
/// backend.export(&MetricExport::Counter {
///     name: "api_requests_total".to_string(),
///     value: 1.0,
///     labels: CustomLabels::new(),
/// }).expect("export");
///
/// let text = backend.gather();
/// assert!(text.contains("api_requests_total 2"));
/// ```
#[derive(Debug, Default)]
pub struct PrometheusBackend {
    registry: prometheus::Registry,
    metrics: HashMap<String, PrometheusHandle>,
}

impl PrometheusBackend {
    /// Create a backend with a fresh, private [`prometheus::Registry`].
    #[must_use]
    pub fn new() -> Self {
        Self {
            registry: prometheus::Registry::new(),
            metrics: HashMap::new(),
        }
    }

    /// Borrow the private registry backing this instance, e.g. to hand it to
    /// your own HTTP scrape handler instead of using [`Self::gather`].
    #[must_use]
    pub fn registry(&self) -> &prometheus::Registry {
        &self.registry
    }

    /// Render everything recorded so far in Prometheus text exposition
    /// format.
    #[must_use]
    pub fn gather(&self) -> String {
        let families = self.registry.gather();
        let mut buffer = Vec::new();
        let encoder = prometheus::TextEncoder::new();
        match encoder.encode(&families, &mut buffer) {
            Ok(()) => String::from_utf8(buffer).unwrap_or_default(),
            Err(_) => String::new(),
        }
    }

    fn reject_labels(name: &str, labels: &CustomLabels) -> Result<(), String> {
        if labels.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "PrometheusBackend cannot export labeled metric `{name}`: dynamic \
                 Counter/Gauge/Histogram metrics carry no label dimensions; use \
                 crate::prometheus_metrics's *_BY_TYPE CounterVec/HistogramVec for labeled \
                 metrics instead"
            ))
        }
    }

    fn counter(&mut self, name: &str) -> Result<prometheus::Counter, String> {
        if let Some(handle) = self.metrics.get(name) {
            return match handle {
                PrometheusHandle::Counter(counter) => Ok(counter.clone()),
                other => Err(already_registered_error(name, other, "counter")),
            };
        }
        let counter = prometheus::Counter::new(name, format!("celers-metrics counter `{name}`"))
            .map_err(|err| format!("failed to create Prometheus counter `{name}`: {err}"))?;
        self.registry
            .register(Box::new(counter.clone()))
            .map_err(|err| format!("failed to register Prometheus counter `{name}`: {err}"))?;
        self.metrics
            .insert(name.to_string(), PrometheusHandle::Counter(counter.clone()));
        Ok(counter)
    }

    fn gauge(&mut self, name: &str) -> Result<prometheus::Gauge, String> {
        if let Some(handle) = self.metrics.get(name) {
            return match handle {
                PrometheusHandle::Gauge(gauge) => Ok(gauge.clone()),
                other => Err(already_registered_error(name, other, "gauge")),
            };
        }
        let gauge = prometheus::Gauge::new(name, format!("celers-metrics gauge `{name}`"))
            .map_err(|err| format!("failed to create Prometheus gauge `{name}`: {err}"))?;
        self.registry
            .register(Box::new(gauge.clone()))
            .map_err(|err| format!("failed to register Prometheus gauge `{name}`: {err}"))?;
        self.metrics
            .insert(name.to_string(), PrometheusHandle::Gauge(gauge.clone()));
        Ok(gauge)
    }

    fn histogram(&mut self, name: &str) -> Result<prometheus::Histogram, String> {
        if let Some(handle) = self.metrics.get(name) {
            return match handle {
                PrometheusHandle::Histogram(histogram) => Ok(histogram.clone()),
                other => Err(already_registered_error(name, other, "histogram")),
            };
        }
        let opts =
            prometheus::HistogramOpts::new(name, format!("celers-metrics histogram `{name}`"));
        let histogram = prometheus::Histogram::with_opts(opts)
            .map_err(|err| format!("failed to create Prometheus histogram `{name}`: {err}"))?;
        self.registry
            .register(Box::new(histogram.clone()))
            .map_err(|err| format!("failed to register Prometheus histogram `{name}`: {err}"))?;
        self.metrics.insert(
            name.to_string(),
            PrometheusHandle::Histogram(histogram.clone()),
        );
        Ok(histogram)
    }
}

fn already_registered_error(name: &str, existing: &PrometheusHandle, wanted: &str) -> String {
    format!(
        "Prometheus metric `{name}` is already registered as a {}, cannot export it as a {wanted}",
        existing.kind_name()
    )
}

impl MetricBackend for PrometheusBackend {
    fn export(&mut self, metric: &MetricExport) -> Result<(), String> {
        match metric {
            MetricExport::Counter {
                name,
                value,
                labels,
            } => {
                Self::reject_labels(name, labels)?;
                if *value < 0.0 {
                    return Err(format!(
                        "Prometheus counter `{name}` cannot be decreased (got {value}); \
                         counters only ever increase"
                    ));
                }
                self.counter(name)?.inc_by(*value);
            }
            MetricExport::Gauge {
                name,
                value,
                labels,
            } => {
                Self::reject_labels(name, labels)?;
                self.gauge(name)?.set(*value);
            }
            MetricExport::Histogram {
                name,
                count,
                sum,
                labels,
                ..
            } => {
                Self::reject_labels(name, labels)?;
                let histogram = self.histogram(name)?;
                if *count > 0 {
                    histogram.observe(sum / (*count as f64));
                }
            }
        }
        Ok(())
    }

    /// Metrics are updated in place by [`Self::export`]; there is nothing
    /// buffered to flush (matching [`crate::prometheus_metrics::gather_metrics`]'s
    /// semantics for the crate's own fixed metrics).
    fn flush(&mut self) -> Result<(), String> {
        Ok(())
    }
}

// ============================================================================
// Unsupported Vendor Backends
// ============================================================================

/// Returned by [`OpenTelemetryConfig::connect`], [`CloudWatchConfig::connect`],
/// and [`DatadogConfig::connect`]: this crate does not implement a network
/// exporter for that vendor.
///
/// The `Result<Infallible, UnsupportedBackend>` return type on those methods
/// is deliberate: [`std::convert::Infallible`] is uninhabited, so the
/// signature itself proves the call can never succeed, rather than merely
/// documenting it in prose that a caller could miss.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsupportedBackend {
    /// Name of the backend that was requested (e.g. `"CloudWatch"`).
    pub backend: &'static str,
    /// What to do instead.
    pub reason: String,
}

impl std::fmt::Display for UnsupportedBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} backend is not implemented by celers-metrics: {}",
            self.backend, self.reason
        )
    }
}

impl std::error::Error for UnsupportedBackend {}

/// OpenTelemetry metric backend helper
#[derive(Debug, Clone)]
pub struct OpenTelemetryConfig {
    /// Service name
    pub service_name: String,
    /// Service version
    pub service_version: String,
    /// Environment (production, staging, etc.)
    pub environment: String,
    /// Additional resource attributes
    pub attributes: CustomLabels,
}

impl Default for OpenTelemetryConfig {
    fn default() -> Self {
        Self {
            service_name: "celers".to_string(),
            service_version: "1.0.0".to_string(),
            environment: "production".to_string(),
            attributes: CustomLabels::new(),
        }
    }
}

impl OpenTelemetryConfig {
    /// Create a new OpenTelemetry configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the service name
    pub fn with_service_name(mut self, name: impl Into<String>) -> Self {
        self.service_name = name.into();
        self
    }

    /// Set the service version
    pub fn with_service_version(mut self, version: impl Into<String>) -> Self {
        self.service_version = version.into();
        self
    }

    /// Set the environment
    pub fn with_environment(mut self, env: impl Into<String>) -> Self {
        self.environment = env.into();
        self
    }

    /// Add a resource attribute
    pub fn with_attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.attributes = self.attributes.with_label(key, value);
        self
    }

    /// Attempt to obtain a live [`MetricBackend`] that exports via
    /// OpenTelemetry using this configuration.
    ///
    /// `celers-metrics` does not bundle an OpenTelemetry SDK/exporter client.
    /// This configuration exists so a caller can still build the parameters
    /// (service name/version, environment, resource attributes) in a typed
    /// way and hand them to their own `opentelemetry`/`opentelemetry_sdk`
    /// pipeline, or export via [`StatsdExporter`]/[`PrometheusBackend`]
    /// instead (most OpenTelemetry collectors can scrape Prometheus or
    /// receive StatsD).
    ///
    /// # Errors
    ///
    /// Always returns [`UnsupportedBackend`].
    pub fn connect(&self) -> Result<std::convert::Infallible, UnsupportedBackend> {
        Err(UnsupportedBackend {
            backend: "OpenTelemetry",
            reason: "celers-metrics does not bundle an OpenTelemetry SDK/exporter; build your \
                     own `opentelemetry`/`opentelemetry_sdk` pipeline using this configuration's \
                     fields, or export via this crate's PrometheusBackend/StatsdExporter instead"
                .to_string(),
        })
    }
}

/// CloudWatch metric backend helper
#[derive(Debug, Clone)]
pub struct CloudWatchConfig {
    /// CloudWatch namespace
    pub namespace: String,
    /// AWS region
    pub region: String,
    /// Metric dimensions (labels)
    pub dimensions: CustomLabels,
    /// Storage resolution (1 or 60 seconds)
    pub storage_resolution: u32,
}

impl Default for CloudWatchConfig {
    fn default() -> Self {
        Self {
            namespace: "CeleRS".to_string(),
            region: "us-east-1".to_string(),
            dimensions: CustomLabels::new(),
            storage_resolution: 60,
        }
    }
}

impl CloudWatchConfig {
    /// Create a new CloudWatch configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the namespace
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    /// Set the region
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = region.into();
        self
    }

    /// Add a dimension
    pub fn with_dimension(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.dimensions = self.dimensions.with_label(key, value);
        self
    }

    /// Set storage resolution (1 or 60 seconds)
    pub fn with_storage_resolution(mut self, resolution: u32) -> Self {
        self.storage_resolution = if resolution == 1 { 1 } else { 60 };
        self
    }

    /// Attempt to obtain a live [`MetricBackend`] that publishes to Amazon
    /// CloudWatch using this configuration.
    ///
    /// `celers-metrics` does not bundle an AWS SDK client: pulling one in
    /// would drag a large, largely non-pure-Rust dependency tree (HTTP
    /// client, TLS stack, credential providers, ...) into every consumer of
    /// this crate, the overwhelming majority of which never touch
    /// CloudWatch. Construct your own `aws-sdk-cloudwatch` client and drive
    /// it with this configuration's fields (namespace, region, dimensions,
    /// storage resolution), or export via [`PrometheusBackend`]/
    /// [`StatsdExporter`] instead -- the CloudWatch agent can scrape a
    /// Prometheus endpoint or ingest StatsD.
    ///
    /// # Errors
    ///
    /// Always returns [`UnsupportedBackend`].
    pub fn connect(&self) -> Result<std::convert::Infallible, UnsupportedBackend> {
        Err(UnsupportedBackend {
            backend: "CloudWatch",
            reason: "celers-metrics does not bundle an AWS SDK client; construct your own \
                     aws-sdk-cloudwatch client and use this configuration's fields (namespace, \
                     region, dimensions, storage_resolution) to drive it, or export via this \
                     crate's PrometheusBackend/StatsdExporter instead"
                .to_string(),
        })
    }
}

/// Datadog metric backend helper
#[derive(Debug, Clone)]
pub struct DatadogConfig {
    /// Datadog API host
    pub api_host: String,
    /// Datadog API key
    pub api_key: String,
    /// Metric prefix
    pub prefix: String,
    /// Tags
    pub tags: CustomLabels,
}

impl Default for DatadogConfig {
    fn default() -> Self {
        Self {
            api_host: "https://api.datadoghq.com".to_string(),
            api_key: String::new(),
            prefix: "celers".to_string(),
            tags: CustomLabels::new(),
        }
    }
}

impl DatadogConfig {
    /// Create a new Datadog configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the API host
    pub fn with_api_host(mut self, host: impl Into<String>) -> Self {
        self.api_host = host.into();
        self
    }

    /// Set the API key
    pub fn with_api_key(mut self, key: impl Into<String>) -> Self {
        self.api_key = key.into();
        self
    }

    /// Set the metric prefix
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Add a tag
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags = self.tags.with_label(key, value);
        self
    }

    /// Format tags for Datadog
    pub fn format_tags(&self, additional_labels: &CustomLabels) -> Vec<String> {
        let mut all_tags = Vec::new();

        // Add global tags
        for (k, v) in self.tags.as_vec() {
            all_tags.push(format!("{}:{}", k, v));
        }

        // Add metric-specific labels
        for (k, v) in additional_labels.as_vec() {
            all_tags.push(format!("{}:{}", k, v));
        }

        all_tags
    }

    /// Attempt to obtain a live [`MetricBackend`] that publishes to Datadog
    /// using this configuration.
    ///
    /// `celers-metrics` does not bundle a Datadog HTTP client. Use this
    /// configuration's fields (`api_host`, `api_key`, `prefix`,
    /// [`Self::format_tags`]) to drive your own HTTP client against the
    /// Datadog metrics API, or export via [`StatsdExporter`] instead --
    /// `DogStatsD` (the Datadog Agent's StatsD listener, which this crate's
    /// StatsD wire format already targets, tags included) is Datadog's own
    /// recommended low-overhead ingestion path.
    ///
    /// # Errors
    ///
    /// Always returns [`UnsupportedBackend`].
    pub fn connect(&self) -> Result<std::convert::Infallible, UnsupportedBackend> {
        Err(UnsupportedBackend {
            backend: "Datadog",
            reason: "celers-metrics does not bundle a Datadog HTTP client; use this \
                     configuration's fields (api_host, api_key, prefix, format_tags) to drive \
                     your own client, or export via this crate's StatsdExporter to a DogStatsD \
                     listener instead"
                .to_string(),
        })
    }
}

/// Helper function to export metrics to StatsD format
pub fn export_to_statsd(stats: &MetricStats, metric_name: &str, config: &StatsDConfig) -> String {
    let export = MetricExport::Histogram {
        name: metric_name.to_string(),
        count: stats.count,
        sum: stats.sum,
        buckets: vec![],
        labels: CustomLabels::new(),
    };
    config.format_metric(&export)
}

// ============================================================================
// Metric Value Utilities
// ============================================================================

/// Current values of all core metrics
/// Useful for debugging, monitoring, and health checks
#[derive(Debug, Clone)]
pub struct CurrentMetrics {
    /// Total tasks enqueued
    pub tasks_enqueued: f64,
    /// Total tasks completed
    pub tasks_completed: f64,
    /// Total tasks failed
    pub tasks_failed: f64,
    /// Total tasks retried
    pub tasks_retried: f64,
    /// Total tasks cancelled
    pub tasks_cancelled: f64,
    /// Current queue size
    pub queue_size: f64,
    /// Current processing queue size
    pub processing_queue_size: f64,
    /// Current DLQ size
    pub dlq_size: f64,
    /// Number of active workers
    pub active_workers: f64,
    /// Total payload bytes processed (enqueued + results), in bytes
    pub total_payload_bytes: f64,
}

impl CurrentMetrics {
    /// Capture current metric values
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_metrics::CurrentMetrics;
    ///
    /// let metrics = CurrentMetrics::capture();
    /// println!("Queue size: {}", metrics.queue_size);
    /// println!("Active workers: {}", metrics.active_workers);
    /// ```
    pub fn capture() -> Self {
        Self {
            tasks_enqueued: TASKS_ENQUEUED_TOTAL.get(),
            tasks_completed: TASKS_COMPLETED_TOTAL.get(),
            tasks_failed: TASKS_FAILED_TOTAL.get(),
            tasks_retried: TASKS_RETRIED_TOTAL.get(),
            tasks_cancelled: TASKS_CANCELLED_TOTAL.get(),
            queue_size: QUEUE_SIZE.get(),
            processing_queue_size: PROCESSING_QUEUE_SIZE.get(),
            dlq_size: DLQ_SIZE.get(),
            active_workers: ACTIVE_WORKERS.get(),
            total_payload_bytes: TOTAL_PAYLOAD_BYTES_PROCESSED.get(),
        }
    }

    /// Calculate current success rate
    pub fn success_rate(&self) -> f64 {
        calculate_success_rate(self.tasks_completed, self.tasks_failed)
    }

    /// Calculate current error rate
    pub fn error_rate(&self) -> f64 {
        calculate_error_rate(self.tasks_completed, self.tasks_failed)
    }

    /// Get total processed tasks (completed + failed)
    pub fn total_processed(&self) -> f64 {
        self.tasks_completed + self.tasks_failed
    }
}

// ============================================================================
// Metric Comparison Utilities
// ============================================================================

/// Compare two metric snapshots (useful for A/B testing, canary deployments)
#[derive(Debug, Clone)]
pub struct MetricComparison {
    /// Percentage change in success rate
    pub success_rate_change: f64,
    /// Percentage change in error rate
    pub error_rate_change: f64,
    /// Percentage change in throughput
    pub throughput_change: f64,
    /// Difference in queue size
    pub queue_size_diff: f64,
    /// Difference in active workers
    pub workers_diff: f64,
}

impl MetricComparison {
    /// Compare two metric snapshots
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_metrics::{CurrentMetrics, MetricComparison};
    ///
    /// let baseline = CurrentMetrics {
    ///     tasks_enqueued: 1000.0,
    ///     tasks_completed: 900.0,
    ///     tasks_failed: 100.0,
    ///     tasks_retried: 50.0,
    ///     tasks_cancelled: 10.0,
    ///     queue_size: 100.0,
    ///     processing_queue_size: 20.0,
    ///     dlq_size: 5.0,
    ///     active_workers: 10.0,
    ///     total_payload_bytes: 0.0,
    /// };
    ///
    /// let current = CurrentMetrics {
    ///     tasks_enqueued: 1100.0,
    ///     tasks_completed: 1000.0,
    ///     tasks_failed: 100.0,
    ///     tasks_retried: 45.0,
    ///     tasks_cancelled: 8.0,
    ///     queue_size: 80.0,
    ///     processing_queue_size: 18.0,
    ///     dlq_size: 4.0,
    ///     active_workers: 12.0,
    ///     total_payload_bytes: 0.0,
    /// };
    ///
    /// let comparison = MetricComparison::compare(&baseline, &current);
    /// assert!(comparison.queue_size_diff < 0.0); // Queue size decreased
    /// ```
    pub fn compare(baseline: &CurrentMetrics, current: &CurrentMetrics) -> Self {
        let baseline_success_rate = baseline.success_rate();
        let current_success_rate = current.success_rate();
        let success_rate_change = if baseline_success_rate > 0.0 {
            ((current_success_rate - baseline_success_rate) / baseline_success_rate) * 100.0
        } else {
            0.0
        };

        let baseline_error_rate = baseline.error_rate();
        let current_error_rate = current.error_rate();
        let error_rate_change = if baseline_error_rate > 0.0 {
            ((current_error_rate - baseline_error_rate) / baseline_error_rate) * 100.0
        } else if current_error_rate > 0.0 {
            100.0
        } else {
            0.0
        };

        let baseline_throughput = baseline.total_processed();
        let current_throughput = current.total_processed();
        let throughput_change = if baseline_throughput > 0.0 {
            ((current_throughput - baseline_throughput) / baseline_throughput) * 100.0
        } else {
            0.0
        };

        Self {
            success_rate_change,
            error_rate_change,
            throughput_change,
            queue_size_diff: current.queue_size - baseline.queue_size,
            workers_diff: current.active_workers - baseline.active_workers,
        }
    }

    /// Check if the change is significant (beyond threshold)
    pub fn is_significant(&self, threshold_percent: f64) -> bool {
        self.success_rate_change.abs() > threshold_percent
            || self.error_rate_change.abs() > threshold_percent
            || self.throughput_change.abs() > threshold_percent
    }

    /// Check if metrics improved compared to baseline
    pub fn is_improvement(&self) -> bool {
        self.success_rate_change > 0.0 && self.error_rate_change < 0.0
    }

    /// Check if metrics degraded compared to baseline
    pub fn is_degradation(&self) -> bool {
        self.success_rate_change < 0.0 || self.error_rate_change > 0.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::UdpSocket;
    use std::time::Duration;

    // ---- MetricBackend for StatsdExporter ----------------------------------

    #[test]
    fn statsd_exporter_backend_exports_counter_gauge_and_histogram() {
        let receiver = UdpSocket::bind("127.0.0.1:0").expect("bind receiver");
        receiver
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set timeout");
        let receiver_addr = receiver.local_addr().expect("receiver addr");
        let mut backend = StatsdExporter::connect(receiver_addr).expect("connect exporter");

        backend
            .export(&MetricExport::Counter {
                name: "requests".to_string(),
                value: 5.0,
                labels: CustomLabels::new(),
            })
            .expect("export counter");
        let mut buf = [0_u8; 256];
        let n = receiver.recv(&mut buf).expect("recv counter");
        assert_eq!(
            std::str::from_utf8(&buf[..n]).expect("utf8"),
            "requests:5|c"
        );

        backend
            .export(&MetricExport::Gauge {
                name: "queue".to_string(),
                value: 12.0,
                labels: CustomLabels::new(),
            })
            .expect("export gauge");
        let n = receiver.recv(&mut buf).expect("recv gauge");
        assert_eq!(std::str::from_utf8(&buf[..n]).expect("utf8"), "queue:12|g");

        backend
            .export(&MetricExport::Histogram {
                name: "duration".to_string(),
                count: 4,
                sum: 40.0,
                buckets: vec![],
                labels: CustomLabels::new(),
            })
            .expect("export histogram");
        let n = receiver.recv(&mut buf).expect("recv histogram");
        // mean = 40/4 = 10
        assert_eq!(
            std::str::from_utf8(&buf[..n]).expect("utf8"),
            "duration:10|h"
        );

        backend
            .flush()
            .expect("flush is a no-op that always succeeds");
    }

    // ---- PrometheusBackend --------------------------------------------------

    #[test]
    fn prometheus_backend_exports_counter_gauge_and_histogram() {
        let mut backend = PrometheusBackend::new();

        backend
            .export(&MetricExport::Counter {
                name: "pb_requests_total".to_string(),
                value: 3.0,
                labels: CustomLabels::new(),
            })
            .expect("export counter");
        backend
            .export(&MetricExport::Counter {
                name: "pb_requests_total".to_string(),
                value: 2.0,
                labels: CustomLabels::new(),
            })
            .expect("export counter again");

        backend
            .export(&MetricExport::Gauge {
                name: "pb_queue_size".to_string(),
                value: 7.0,
                labels: CustomLabels::new(),
            })
            .expect("export gauge");

        backend
            .export(&MetricExport::Histogram {
                name: "pb_duration_seconds".to_string(),
                count: 4,
                sum: 20.0,
                buckets: vec![],
                labels: CustomLabels::new(),
            })
            .expect("export histogram");

        backend
            .flush()
            .expect("flush is a no-op that always succeeds");

        let text = backend.gather();
        assert!(text.contains("pb_requests_total 5"), "text: {text}");
        assert!(text.contains("pb_queue_size 7"), "text: {text}");
        assert!(text.contains("pb_duration_seconds_sum 5"), "text: {text}");
        assert!(text.contains("pb_duration_seconds_count 1"), "text: {text}");
    }

    #[test]
    fn prometheus_backend_rejects_negative_counter() {
        let mut backend = PrometheusBackend::new();
        let err = backend
            .export(&MetricExport::Counter {
                name: "pb_negative".to_string(),
                value: -1.0,
                labels: CustomLabels::new(),
            })
            .expect_err("negative counter delta must be rejected");
        assert!(err.contains("cannot be decreased"), "error: {err}");
    }

    #[test]
    fn prometheus_backend_rejects_type_change_for_same_name() {
        let mut backend = PrometheusBackend::new();
        backend
            .export(&MetricExport::Counter {
                name: "pb_mixed".to_string(),
                value: 1.0,
                labels: CustomLabels::new(),
            })
            .expect("first export registers a counter");

        let err = backend
            .export(&MetricExport::Gauge {
                name: "pb_mixed".to_string(),
                value: 1.0,
                labels: CustomLabels::new(),
            })
            .expect_err("re-exporting under a different type must be rejected");
        assert!(
            err.contains("already registered as a counter"),
            "error: {err}"
        );
    }

    #[test]
    fn prometheus_backend_rejects_labeled_export() {
        let mut backend = PrometheusBackend::new();
        let labels = CustomLabels::new().with_label("region", "us-east-1");
        let err = backend
            .export(&MetricExport::Counter {
                name: "pb_labeled".to_string(),
                value: 1.0,
                labels,
            })
            .expect_err("labeled metrics must be rejected");
        assert!(err.contains("pb_labeled"), "error: {err}");
    }

    #[test]
    fn prometheus_backend_instances_do_not_collide_on_metric_names() {
        // Each PrometheusBackend owns a private registry, so two instances
        // using the same metric name must not fail to register.
        let mut a = PrometheusBackend::new();
        let mut b = PrometheusBackend::new();
        let metric = MetricExport::Counter {
            name: "shared_name_total".to_string(),
            value: 1.0,
            labels: CustomLabels::new(),
        };
        a.export(&metric).expect("backend a registers fine");
        b.export(&metric).expect("backend b registers fine too");
        assert!(a.gather().contains("shared_name_total 1"));
        assert!(b.gather().contains("shared_name_total 1"));
    }

    // ---- StatsDConfig::format_metric fixes ----------------------------------

    #[test]
    fn statsd_config_format_metric_honours_sample_rate() {
        // Regression: `sample_rate` was silently ignored by the duplicated
        // formatting logic this method used to have.
        let config = StatsDConfig::new()
            .with_prefix("celers")
            .with_sample_rate(0.1);
        let metric = MetricExport::Counter {
            name: "tasks_completed".to_string(),
            value: 42.0,
            labels: CustomLabels::new(),
        };
        assert_eq!(
            config.format_metric(&metric),
            "celers.tasks_completed:42|c|@0.1"
        );
    }

    #[test]
    fn statsd_config_format_metric_sanitizes_name() {
        // Regression: the duplicated formatting logic skipped
        // `sanitize_statsd_name`, so a name containing StatsD delimiter
        // characters (`:`, `|`, `@`, `#`) could corrupt the wire line.
        let config = StatsDConfig::new().with_prefix("celers");
        let metric = MetricExport::Gauge {
            name: "weird|name@here".to_string(),
            value: 1.0,
            labels: CustomLabels::new(),
        };
        assert_eq!(config.format_metric(&metric), "celers.weird_name_here:1|g");
    }

    // ---- Unsupported vendor backends ----------------------------------------

    #[test]
    fn opentelemetry_config_connect_is_always_unsupported() {
        let err = OpenTelemetryConfig::new().connect().unwrap_err();
        assert_eq!(err.backend, "OpenTelemetry");
        assert!(err.to_string().contains("OpenTelemetry"));
    }

    #[test]
    fn cloudwatch_config_connect_is_always_unsupported() {
        let err = CloudWatchConfig::new().connect().unwrap_err();
        assert_eq!(err.backend, "CloudWatch");
        assert!(err.to_string().contains("CloudWatch"));
    }

    #[test]
    fn datadog_config_connect_is_always_unsupported() {
        let err = DatadogConfig::new().connect().unwrap_err();
        assert_eq!(err.backend, "Datadog");
        assert!(err.to_string().contains("Datadog"));
    }
}
