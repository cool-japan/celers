//! Backend integration support: metric export, StatsD, OpenTelemetry, CloudWatch, Datadog,
//! current metrics capture, and metric comparison utilities.

use crate::aggregation::{CustomLabels, MetricStats};
use crate::prometheus_metrics::*;

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

    /// Format a metric for StatsD wire format
    pub fn format_metric(&self, metric: &MetricExport) -> String {
        let name = format!("{}.{}", self.prefix, metric.name());
        let tags = self.format_tags(metric.labels());

        match metric {
            MetricExport::Counter { value, .. } => {
                format!("{}:{}|c{}", name, value, tags)
            }
            MetricExport::Gauge { value, .. } => {
                format!("{}:{}|g{}", name, value, tags)
            }
            MetricExport::Histogram { sum, count, .. } => {
                let avg = if *count > 0 {
                    sum / (*count as f64)
                } else {
                    0.0
                };
                format!("{}:{}|h{}", name, avg, tags)
            }
        }
    }

    fn format_tags(&self, labels: &CustomLabels) -> String {
        if labels.is_empty() {
            return String::new();
        }
        let tags: Vec<String> = labels
            .as_vec()
            .iter()
            .map(|(k, v)| format!("{}:{}", k, v))
            .collect();
        format!("|#{}", tags.join(","))
    }
}

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
