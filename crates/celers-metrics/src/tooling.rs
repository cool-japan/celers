//! Metric tooling: adaptive sampling, batching, validation, heatmap, registry,
//! resource tracking, export utilities, profiling, SLA reports, retention,
//! capacity planning, Prometheus query builder, and collection scheduling.

use crate::backends::{CurrentMetrics, MetricExport};
use crate::history::MetricHistory;
use crate::prometheus_metrics::*;
use crate::slo::{calculate_error_budget, SloTarget};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{SystemTime, UNIX_EPOCH};

// ============================================================================
// Adaptive Sampling with Load Monitoring
// ============================================================================

/// Adaptive sampler that adjusts sampling rate based on system load
pub struct AdaptiveSampler {
    base_rate: AtomicU64, // Fixed-point representation (0-1000000 = 0.0-1.0)
    current_rate: AtomicU64,
    load_threshold_high: f64,
    load_threshold_low: f64,
    rate_step: f64,
}

impl AdaptiveSampler {
    /// Create a new adaptive sampler with base sampling rate
    pub fn new(base_rate: f64) -> Self {
        let rate_fixed = (base_rate.clamp(0.0, 1.0) * 1_000_000.0) as u64;
        Self {
            base_rate: AtomicU64::new(rate_fixed),
            current_rate: AtomicU64::new(rate_fixed),
            load_threshold_high: 0.8, // Reduce sampling at 80% load
            load_threshold_low: 0.5,  // Increase sampling at 50% load
            rate_step: 0.1,           // Adjust by 10% each time
        }
    }

    /// Update sampling rate based on current system load (0.0-1.0)
    pub fn update_based_on_load(&self, load: f64) {
        let load = load.clamp(0.0, 1.0);
        let current = self.current_rate.load(Ordering::Relaxed) as f64 / 1_000_000.0;

        let new_rate = if load > self.load_threshold_high {
            // High load: reduce sampling
            (current - self.rate_step).max(0.01) // Minimum 1% sampling
        } else if load < self.load_threshold_low {
            // Low load: increase sampling
            let base = self.base_rate.load(Ordering::Relaxed) as f64 / 1_000_000.0;
            (current + self.rate_step).min(base)
        } else {
            current // Keep current rate
        };

        let new_rate_fixed = (new_rate * 1_000_000.0) as u64;
        self.current_rate.store(new_rate_fixed, Ordering::Relaxed);
    }

    /// Check if current observation should be sampled
    pub fn should_sample(&self) -> bool {
        let rate = self.current_rate.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        rand::random::<f64>() < rate
    }

    /// Get current sampling rate
    pub fn current_rate(&self) -> f64 {
        self.current_rate.load(Ordering::Relaxed) as f64 / 1_000_000.0
    }

    /// Set base sampling rate
    pub fn set_base_rate(&self, rate: f64) {
        let rate_fixed = (rate.clamp(0.0, 1.0) * 1_000_000.0) as u64;
        self.base_rate.store(rate_fixed, Ordering::Relaxed);
    }
}

// ============================================================================
// Metric Export Batching
// ============================================================================

/// Batch of metrics for efficient export
#[derive(Debug, Clone)]
pub struct MetricBatch {
    /// Metrics in the batch
    pub metrics: Vec<MetricExport>,
    /// Batch creation timestamp
    pub timestamp: u64,
}

impl MetricBatch {
    /// Create a new empty batch
    pub fn new() -> Self {
        Self {
            metrics: Vec::new(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    /// Add a metric to the batch
    pub fn add(&mut self, metric: MetricExport) {
        self.metrics.push(metric);
    }

    /// Get number of metrics in batch
    pub fn len(&self) -> usize {
        self.metrics.len()
    }

    /// Check if batch is empty
    pub fn is_empty(&self) -> bool {
        self.metrics.is_empty()
    }

    /// Clear the batch
    pub fn clear(&mut self) {
        self.metrics.clear();
        self.timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }
}

impl Default for MetricBatch {
    fn default() -> Self {
        Self::new()
    }
}

/// Metric batcher for efficient exports
pub struct MetricBatcher {
    batch: StdMutex<MetricBatch>,
    max_batch_size: usize,
    max_batch_age_seconds: u64,
}

impl MetricBatcher {
    /// Create a new batcher with max batch size and age
    pub fn new(max_batch_size: usize, max_batch_age_seconds: u64) -> Self {
        Self {
            batch: StdMutex::new(MetricBatch::new()),
            max_batch_size,
            max_batch_age_seconds,
        }
    }

    /// Add a metric to the batch, returns true if batch should be flushed
    pub fn add(&self, metric: MetricExport) -> bool {
        let mut batch = self.batch.lock().unwrap();
        batch.add(metric);

        batch.len() >= self.max_batch_size || self.is_batch_stale(&batch)
    }

    /// Check if batch is stale (exceeded max age)
    fn is_batch_stale(&self, batch: &MetricBatch) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        now - batch.timestamp >= self.max_batch_age_seconds
    }

    /// Flush the batch and return metrics
    pub fn flush(&self) -> Vec<MetricExport> {
        let mut batch = self.batch.lock().unwrap();
        let metrics = batch.metrics.clone();
        batch.clear();
        metrics
    }

    /// Get current batch size without flushing
    pub fn current_size(&self) -> usize {
        self.batch.lock().unwrap().len()
    }
}

// ============================================================================
// Label Sanitization and Validation
// ============================================================================

/// Sanitizes a metric label name to conform to Prometheus standards.
///
/// Prometheus label names must match the regex `[a-zA-Z_][a-zA-Z0-9_]*`.
/// This function replaces invalid characters with underscores and ensures
/// the name starts with a letter or underscore.
///
/// # Example
///
/// ```
/// use celers_metrics::sanitize_label_name;
///
/// assert_eq!(sanitize_label_name("task-type"), "task_type");
/// assert_eq!(sanitize_label_name("123invalid"), "_123invalid");
/// assert_eq!(sanitize_label_name("valid_name"), "valid_name");
/// ```
pub fn sanitize_label_name(name: &str) -> String {
    if name.is_empty() {
        return "_".to_string();
    }

    let mut result = String::with_capacity(name.len());

    for (i, ch) in name.chars().enumerate() {
        if i == 0 {
            // First character must be [a-zA-Z_]
            if ch.is_ascii_alphabetic() || ch == '_' {
                result.push(ch);
            } else {
                result.push('_');
                if ch.is_ascii_alphanumeric() {
                    result.push(ch);
                }
            }
        } else {
            // Subsequent characters can be [a-zA-Z0-9_]
            if ch.is_ascii_alphanumeric() || ch == '_' {
                result.push(ch);
            } else {
                result.push('_');
            }
        }
    }

    result
}

/// Sanitizes a metric label value.
///
/// Replaces control characters with spaces and normalizes whitespace.
///
/// # Example
///
/// ```
/// use celers_metrics::sanitize_label_value;
///
/// assert_eq!(sanitize_label_value("hello\nworld"), "hello world");
/// assert_eq!(sanitize_label_value("  spaces  "), "spaces");
/// ```
pub fn sanitize_label_value(value: &str) -> String {
    value
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Validates a metric name according to Prometheus naming conventions.
///
/// Returns true if the name matches `[a-zA-Z_:][a-zA-Z0-9_:]*`.
///
/// # Example
///
/// ```
/// use celers_metrics::is_valid_metric_name;
///
/// assert!(is_valid_metric_name("http_requests_total"));
/// assert!(is_valid_metric_name("http:requests:total"));
/// assert!(!is_valid_metric_name("123invalid"));
/// assert!(!is_valid_metric_name("invalid-name"));
/// ```
pub fn is_valid_metric_name(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }

    let mut chars = name.chars();

    // First character must be [a-zA-Z_:]
    if let Some(first) = chars.next() {
        if !first.is_ascii_alphabetic() && first != '_' && first != ':' {
            return false;
        }
    }

    // Remaining characters must be [a-zA-Z0-9_:]
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':')
}

/// Validates a label name according to Prometheus standards.
///
/// Returns true if the name matches `[a-zA-Z_][a-zA-Z0-9_]*`.
///
/// # Example
///
/// ```
/// use celers_metrics::is_valid_label_name;
///
/// assert!(is_valid_label_name("method"));
/// assert!(is_valid_label_name("status_code"));
/// assert!(!is_valid_label_name("123invalid"));
/// assert!(!is_valid_label_name("invalid-name"));
/// ```
pub fn is_valid_label_name(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }

    let mut chars = name.chars();

    // First character must be [a-zA-Z_]
    if let Some(first) = chars.next() {
        if !first.is_ascii_alphabetic() && first != '_' {
            return false;
        }
    }

    // Remaining characters must be [a-zA-Z0-9_]
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

// ============================================================================
// Histogram Heatmap Generation
// ============================================================================

/// Represents a bucket in a histogram heatmap
#[derive(Debug, Clone)]
pub struct HeatmapBucket {
    /// Upper bound of the bucket
    pub upper_bound: f64,
    /// Count of observations in this bucket
    pub count: u64,
    /// Timestamp when this bucket was recorded
    pub timestamp: u64,
}

/// Time-series histogram data for heatmap visualization
#[derive(Debug)]
pub struct HistogramHeatmap {
    /// Buckets organized by timestamp
    buckets: StdMutex<Vec<(u64, Vec<HeatmapBucket>)>>,
    /// Maximum number of time slices to retain
    max_slices: usize,
}

impl HistogramHeatmap {
    /// Create a new histogram heatmap with retention limit
    pub fn new(max_slices: usize) -> Self {
        Self {
            buckets: StdMutex::new(Vec::new()),
            max_slices,
        }
    }

    /// Record a histogram snapshot at the current time
    ///
    /// # Example
    ///
    /// ```
    /// use celers_metrics::{HistogramHeatmap, HeatmapBucket};
    ///
    /// let heatmap = HistogramHeatmap::new(100);
    /// let buckets = vec![
    ///     HeatmapBucket { upper_bound: 0.1, count: 10, timestamp: 0 },
    ///     HeatmapBucket { upper_bound: 1.0, count: 50, timestamp: 0 },
    ///     HeatmapBucket { upper_bound: 10.0, count: 100, timestamp: 0 },
    /// ];
    /// heatmap.record_snapshot(buckets);
    /// ```
    pub fn record_snapshot(&self, snapshot: Vec<HeatmapBucket>) {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut buckets = self.buckets.lock().unwrap();
        buckets.push((timestamp, snapshot));

        // Enforce retention limit
        if buckets.len() > self.max_slices {
            buckets.remove(0);
        }
    }

    /// Get all recorded snapshots
    pub fn get_snapshots(&self) -> Vec<(u64, Vec<HeatmapBucket>)> {
        self.buckets.lock().unwrap().clone()
    }

    /// Get the number of recorded time slices
    pub fn len(&self) -> usize {
        self.buckets.lock().unwrap().len()
    }

    /// Check if the heatmap is empty
    pub fn is_empty(&self) -> bool {
        self.buckets.lock().unwrap().is_empty()
    }

    /// Clear all recorded data
    pub fn clear(&self) {
        self.buckets.lock().unwrap().clear();
    }
}

// ============================================================================
// Dynamic Metric Registry
// ============================================================================

/// Dynamic metric registry for runtime metric management
///
/// This allows metrics to be registered, queried, and managed at runtime
/// without requiring compile-time static definitions.
pub struct MetricRegistry {
    /// Registered counter metrics
    counters: StdMutex<HashMap<String, Arc<AtomicU64>>>,
    /// Registered gauge metrics
    gauges: StdMutex<HashMap<String, Arc<AtomicU64>>>,
    /// Metadata for registered metrics
    metadata: StdMutex<HashMap<String, MetricMetadata>>,
}

/// Metadata about a registered metric
#[derive(Debug, Clone)]
pub struct MetricMetadata {
    /// Metric name
    pub name: String,
    /// Help text describing the metric
    pub help: String,
    /// Metric type
    pub metric_type: MetricType,
    /// When the metric was registered
    pub registered_at: u64,
}

/// Type of metric
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricType {
    /// Counter that only increases
    Counter,
    /// Gauge that can go up or down
    Gauge,
    /// Histogram for distributions
    Histogram,
    /// Summary for quantiles
    Summary,
}

impl MetricRegistry {
    /// Create a new metric registry
    pub fn new() -> Self {
        Self {
            counters: StdMutex::new(HashMap::new()),
            gauges: StdMutex::new(HashMap::new()),
            metadata: StdMutex::new(HashMap::new()),
        }
    }

    /// Register a counter metric
    ///
    /// # Example
    ///
    /// ```
    /// use celers_metrics::MetricRegistry;
    ///
    /// let registry = MetricRegistry::new();
    /// registry.register_counter("my_counter", "Counts something important");
    ///
    /// // Increment the counter
    /// registry.increment_counter("my_counter", 1);
    /// assert_eq!(registry.get_counter("my_counter"), Some(1));
    /// ```
    pub fn register_counter(&self, name: &str, help: &str) -> bool {
        if !is_valid_metric_name(name) {
            return false;
        }

        let mut counters = self.counters.lock().unwrap();
        let mut metadata = self.metadata.lock().unwrap();

        if counters.contains_key(name) {
            return false; // Already registered
        }

        counters.insert(name.to_string(), Arc::new(AtomicU64::new(0)));
        metadata.insert(
            name.to_string(),
            MetricMetadata {
                name: name.to_string(),
                help: help.to_string(),
                metric_type: MetricType::Counter,
                registered_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            },
        );

        true
    }

    /// Register a gauge metric
    pub fn register_gauge(&self, name: &str, help: &str) -> bool {
        if !is_valid_metric_name(name) {
            return false;
        }

        let mut gauges = self.gauges.lock().unwrap();
        let mut metadata = self.metadata.lock().unwrap();

        if gauges.contains_key(name) {
            return false;
        }

        gauges.insert(name.to_string(), Arc::new(AtomicU64::new(0)));
        metadata.insert(
            name.to_string(),
            MetricMetadata {
                name: name.to_string(),
                help: help.to_string(),
                metric_type: MetricType::Gauge,
                registered_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            },
        );

        true
    }

    /// Increment a counter by a value
    pub fn increment_counter(&self, name: &str, value: u64) -> bool {
        let counters = self.counters.lock().unwrap();
        if let Some(counter) = counters.get(name) {
            counter.fetch_add(value, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Set a gauge to a specific value
    pub fn set_gauge(&self, name: &str, value: u64) -> bool {
        let gauges = self.gauges.lock().unwrap();
        if let Some(gauge) = gauges.get(name) {
            gauge.store(value, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Get counter value
    pub fn get_counter(&self, name: &str) -> Option<u64> {
        let counters = self.counters.lock().unwrap();
        counters.get(name).map(|c| c.load(Ordering::Relaxed))
    }

    /// Get gauge value
    pub fn get_gauge(&self, name: &str) -> Option<u64> {
        let gauges = self.gauges.lock().unwrap();
        gauges.get(name).map(|g| g.load(Ordering::Relaxed))
    }

    /// Get metadata for a metric
    pub fn get_metadata(&self, name: &str) -> Option<MetricMetadata> {
        let metadata = self.metadata.lock().unwrap();
        metadata.get(name).cloned()
    }

    /// List all registered metric names
    pub fn list_metrics(&self) -> Vec<String> {
        let metadata = self.metadata.lock().unwrap();
        metadata.keys().cloned().collect()
    }

    /// Unregister a metric
    pub fn unregister(&self, name: &str) -> bool {
        let mut counters = self.counters.lock().unwrap();
        let mut gauges = self.gauges.lock().unwrap();
        let mut metadata = self.metadata.lock().unwrap();

        let removed = counters.remove(name).is_some() || gauges.remove(name).is_some();
        metadata.remove(name);

        removed
    }

    /// Clear all metrics
    pub fn clear(&self) {
        self.counters.lock().unwrap().clear();
        self.gauges.lock().unwrap().clear();
        self.metadata.lock().unwrap().clear();
    }
}

impl Default for MetricRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Resource Usage Tracking
// ============================================================================

/// Tracks resource usage during metric collection operations
#[derive(Debug, Clone)]
pub struct ResourceTracker {
    /// Total time spent collecting metrics (nanoseconds)
    collection_time_ns: Arc<AtomicU64>,
    /// Number of metric collection operations
    collection_count: Arc<AtomicU64>,
    /// Peak memory usage observed (bytes)
    peak_memory_bytes: Arc<AtomicU64>,
}

impl ResourceTracker {
    /// Create a new resource tracker
    pub fn new() -> Self {
        Self {
            collection_time_ns: Arc::new(AtomicU64::new(0)),
            collection_count: Arc::new(AtomicU64::new(0)),
            peak_memory_bytes: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Track a metric collection operation
    ///
    /// Returns the result of the operation and records timing.
    ///
    /// # Example
    ///
    /// ```
    /// use celers_metrics::ResourceTracker;
    ///
    /// let tracker = ResourceTracker::new();
    /// let result = tracker.track_operation(|| {
    ///     // Some metric collection work
    ///     42
    /// });
    ///
    /// assert_eq!(result, 42);
    /// assert!(tracker.avg_collection_time_micros() > 0.0);
    /// ```
    pub fn track_operation<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let start = std::time::Instant::now();
        let result = f();
        let elapsed = start.elapsed().as_nanos() as u64;

        self.collection_time_ns
            .fetch_add(elapsed, Ordering::Relaxed);
        self.collection_count.fetch_add(1, Ordering::Relaxed);

        result
    }

    /// Record peak memory usage
    pub fn record_memory_usage(&self, bytes: u64) {
        let mut current = self.peak_memory_bytes.load(Ordering::Relaxed);
        while bytes > current {
            match self.peak_memory_bytes.compare_exchange(
                current,
                bytes,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current = x,
            }
        }
    }

    /// Get total collection time in microseconds
    pub fn total_collection_time_micros(&self) -> f64 {
        self.collection_time_ns.load(Ordering::Relaxed) as f64 / 1000.0
    }

    /// Get average collection time in microseconds
    pub fn avg_collection_time_micros(&self) -> f64 {
        let count = self.collection_count.load(Ordering::Relaxed);
        if count == 0 {
            return 0.0;
        }
        self.total_collection_time_micros() / count as f64
    }

    /// Get total number of collections
    pub fn collection_count(&self) -> u64 {
        self.collection_count.load(Ordering::Relaxed)
    }

    /// Get peak memory usage in bytes
    pub fn peak_memory_bytes(&self) -> u64 {
        self.peak_memory_bytes.load(Ordering::Relaxed)
    }

    /// Reset all tracked metrics
    pub fn reset(&self) {
        self.collection_time_ns.store(0, Ordering::Relaxed);
        self.collection_count.store(0, Ordering::Relaxed);
        self.peak_memory_bytes.store(0, Ordering::Relaxed);
    }
}

impl Default for ResourceTracker {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Metric Export Utilities (JSON, CSV)
// ============================================================================

/// Export current metrics to JSON format
///
/// # Examples
///
/// ```
/// use celers_metrics::export_metrics_json;
///
/// // Record some metrics
/// celers_metrics::TASKS_ENQUEUED_TOTAL.inc_by(100.0);
/// celers_metrics::TASKS_COMPLETED_TOTAL.inc_by(95.0);
/// celers_metrics::TASKS_FAILED_TOTAL.inc_by(5.0);
///
/// // Export to JSON
/// let json = export_metrics_json();
/// assert!(json.contains("\"enqueued\""));
/// assert!(json.contains("\"completed\""));
/// ```
pub fn export_metrics_json() -> String {
    let metrics = CurrentMetrics::capture();

    format!(
        r#"{{
  "timestamp": {},
  "tasks": {{
    "enqueued": {},
    "completed": {},
    "failed": {},
    "retried": {},
    "cancelled": {}
  }},
  "rates": {{
    "success_rate": {},
    "error_rate": {}
  }},
  "queues": {{
    "pending": {},
    "processing": {},
    "dlq": {}
  }},
  "workers": {{
    "active": {}
  }},
  "derived": {{
    "total_processed": {}
  }}
}}"#,
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        metrics.tasks_enqueued,
        metrics.tasks_completed,
        metrics.tasks_failed,
        metrics.tasks_retried,
        metrics.tasks_cancelled,
        metrics.success_rate(),
        metrics.error_rate(),
        metrics.queue_size,
        metrics.processing_queue_size,
        metrics.dlq_size,
        metrics.active_workers,
        metrics.total_processed()
    )
}

/// Export current metrics to CSV format
///
/// Returns a CSV string with headers and current metric values.
///
/// # Examples
///
/// ```
/// use celers_metrics::export_metrics_csv;
///
/// // Record some metrics
/// celers_metrics::TASKS_ENQUEUED_TOTAL.inc_by(100.0);
/// celers_metrics::TASKS_COMPLETED_TOTAL.inc_by(95.0);
///
/// // Export to CSV
/// let csv = export_metrics_csv();
/// assert!(csv.contains("timestamp,tasks_enqueued"));
/// ```
pub fn export_metrics_csv() -> String {
    let metrics = CurrentMetrics::capture();
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    format!(
        "timestamp,tasks_enqueued,tasks_completed,tasks_failed,tasks_retried,tasks_cancelled,success_rate,error_rate,queue_size,processing_queue_size,dlq_size,active_workers\n{},{},{},{},{},{},{:.4},{:.4},{},{},{},{}",
        timestamp,
        metrics.tasks_enqueued,
        metrics.tasks_completed,
        metrics.tasks_failed,
        metrics.tasks_retried,
        metrics.tasks_cancelled,
        metrics.success_rate(),
        metrics.error_rate(),
        metrics.queue_size,
        metrics.processing_queue_size,
        metrics.dlq_size,
        metrics.active_workers
    )
}

/// Export metric history to CSV format
///
/// Returns a CSV string with timestamp,value pairs for historical analysis.
///
/// # Examples
///
/// ```
/// use celers_metrics::{MetricHistory, export_history_csv};
///
/// let history = MetricHistory::new(100);
/// history.record(10.0);
/// history.record(20.0);
/// history.record(30.0);
///
/// let csv = export_history_csv(&history, "queue_size");
/// assert!(csv.contains("timestamp,queue_size"));
/// ```
pub fn export_history_csv(history: &MetricHistory, metric_name: &str) -> String {
    let samples = history.get_samples();

    let mut csv = format!("timestamp,{}\n", metric_name);

    for sample in samples {
        csv.push_str(&format!("{},{}\n", sample.timestamp, sample.value));
    }

    csv
}

/// Batch export metrics in multiple formats
///
/// Returns a struct containing JSON, CSV, and Prometheus formats for convenience.
#[derive(Debug, Clone)]
pub struct MetricExportBatch {
    /// JSON format export
    pub json: String,
    /// CSV format export
    pub csv: String,
    /// Prometheus format export
    pub prometheus: String,
}

impl MetricExportBatch {
    /// Export all metrics in all supported formats
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_metrics::MetricExportBatch;
    ///
    /// celers_metrics::TASKS_ENQUEUED_TOTAL.inc_by(100.0);
    ///
    /// let exports = MetricExportBatch::export_all();
    /// assert!(!exports.json.is_empty());
    /// assert!(!exports.csv.is_empty());
    /// assert!(!exports.prometheus.is_empty());
    /// ```
    pub fn export_all() -> Self {
        Self {
            json: export_metrics_json(),
            csv: export_metrics_csv(),
            prometheus: gather_metrics(),
        }
    }
}

// ============================================================================
// Performance Profiling Utilities
// ============================================================================

/// Performance profiler for identifying metric collection bottlenecks
#[derive(Debug)]
pub struct MetricsProfiler {
    tracker: ResourceTracker,
    operation_times: StdMutex<HashMap<String, Vec<f64>>>,
}

impl MetricsProfiler {
    /// Create a new metrics profiler
    pub fn new() -> Self {
        Self {
            tracker: ResourceTracker::new(),
            operation_times: StdMutex::new(HashMap::new()),
        }
    }

    /// Profile a specific metric operation
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_metrics::MetricsProfiler;
    ///
    /// let profiler = MetricsProfiler::new();
    ///
    /// profiler.profile_operation("increment_counter", || {
    ///     celers_metrics::TASKS_ENQUEUED_TOTAL.inc();
    /// });
    ///
    /// let stats = profiler.get_operation_stats("increment_counter");
    /// assert!(stats.is_some());
    /// ```
    pub fn profile_operation<F, R>(&self, operation_name: &str, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let start = std::time::Instant::now();
        let result = f();
        let elapsed_micros = start.elapsed().as_micros() as f64;

        let mut times = self.operation_times.lock().unwrap();
        times
            .entry(operation_name.to_string())
            .or_default()
            .push(elapsed_micros);

        result
    }

    /// Get statistics for a specific operation
    pub fn get_operation_stats(&self, operation_name: &str) -> Option<OperationStats> {
        let times = self.operation_times.lock().unwrap();
        let operation_times = times.get(operation_name)?;

        if operation_times.is_empty() {
            return None;
        }

        let count = operation_times.len();
        let sum: f64 = operation_times.iter().sum();
        let mean = sum / count as f64;

        let min = operation_times
            .iter()
            .copied()
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max = operation_times
            .iter()
            .copied()
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();

        let variance = if count > 1 {
            let sq_diff_sum: f64 = operation_times.iter().map(|t| (t - mean).powi(2)).sum();
            sq_diff_sum / (count - 1) as f64
        } else {
            0.0
        };

        Some(OperationStats {
            operation_name: operation_name.to_string(),
            call_count: count,
            mean_micros: mean,
            min_micros: min,
            max_micros: max,
            std_dev_micros: variance.sqrt(),
        })
    }

    /// Get all operation statistics
    pub fn all_stats(&self) -> Vec<OperationStats> {
        let times = self.operation_times.lock().unwrap();
        let mut stats = Vec::new();

        for (operation_name, operation_times) in times.iter() {
            if operation_times.is_empty() {
                continue;
            }

            let count = operation_times.len();
            let sum: f64 = operation_times.iter().sum();
            let mean = sum / count as f64;

            let min = operation_times
                .iter()
                .copied()
                .min_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();
            let max = operation_times
                .iter()
                .copied()
                .max_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap();

            let variance = if count > 1 {
                let sq_diff_sum: f64 = operation_times.iter().map(|t| (t - mean).powi(2)).sum();
                sq_diff_sum / (count - 1) as f64
            } else {
                0.0
            };

            stats.push(OperationStats {
                operation_name: operation_name.to_string(),
                call_count: count,
                mean_micros: mean,
                min_micros: min,
                max_micros: max,
                std_dev_micros: variance.sqrt(),
            });
        }

        stats.sort_by(|a, b| {
            b.mean_micros
                .partial_cmp(&a.mean_micros)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        stats
    }

    /// Generate a performance report
    pub fn generate_report(&self) -> String {
        let mut report = String::from("=== Metrics Performance Report ===\n\n");

        let all_stats = self.all_stats();

        if all_stats.is_empty() {
            report.push_str("No profiling data available.\n");
            return report;
        }

        report.push_str(&format!(
            "{:<30} {:>10} {:>12} {:>12} {:>12} {:>12}\n",
            "Operation", "Calls", "Mean (μs)", "Min (μs)", "Max (μs)", "StdDev (μs)"
        ));
        report.push_str(&"-".repeat(92));
        report.push('\n');

        for stat in all_stats {
            report.push_str(&format!(
                "{:<30} {:>10} {:>12.2} {:>12.2} {:>12.2} {:>12.2}\n",
                stat.operation_name,
                stat.call_count,
                stat.mean_micros,
                stat.min_micros,
                stat.max_micros,
                stat.std_dev_micros
            ));
        }

        report
    }

    /// Reset all profiling data
    pub fn reset(&self) {
        self.tracker.reset();
        self.operation_times.lock().unwrap().clear();
    }
}

impl Default for MetricsProfiler {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics for a profiled operation
#[derive(Debug, Clone)]
pub struct OperationStats {
    /// Name of the operation
    pub operation_name: String,
    /// Number of times the operation was called
    pub call_count: usize,
    /// Mean execution time in microseconds
    pub mean_micros: f64,
    /// Minimum execution time in microseconds
    pub min_micros: f64,
    /// Maximum execution time in microseconds
    pub max_micros: f64,
    /// Standard deviation in microseconds
    pub std_dev_micros: f64,
}

// ============================================================================
// SLA Report Generator
// ============================================================================

/// SLA compliance report
#[derive(Debug, Clone)]
pub struct SlaReport {
    /// Report generation timestamp
    pub timestamp: u64,
    /// Reporting period in seconds
    pub period_seconds: u64,
    /// Target SLO
    pub slo_target: SloTarget,
    /// Actual success rate achieved
    pub actual_success_rate: f64,
    /// Actual p95 latency (seconds)
    pub actual_p95_latency: Option<f64>,
    /// Actual throughput (tasks/sec)
    pub actual_throughput: f64,
    /// SLA compliance status
    pub is_compliant: bool,
    /// Error budget remaining (0.0 to 1.0)
    pub error_budget_remaining: f64,
    /// Total tasks processed
    pub total_tasks: u64,
    /// Failed tasks
    pub failed_tasks: u64,
    /// Recommendations for improvement
    pub recommendations: Vec<String>,
}

impl SlaReport {
    /// Generate an SLA compliance report
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_metrics::{SlaReport, SloTarget};
    ///
    /// celers_metrics::TASKS_ENQUEUED_TOTAL.inc_by(1000.0);
    /// celers_metrics::TASKS_COMPLETED_TOTAL.inc_by(990.0);
    /// celers_metrics::TASKS_FAILED_TOTAL.inc_by(10.0);
    ///
    /// let slo = SloTarget {
    ///     success_rate: 0.99,
    ///     latency_seconds: 5.0,
    ///     throughput: 0.1, // 0.1 tasks/sec = 360 tasks/hour
    /// };
    ///
    /// let report = SlaReport::generate(&slo, 3600);
    /// assert!(report.is_compliant);
    /// assert_eq!(report.total_tasks, 1000);
    /// ```
    pub fn generate(slo_target: &SloTarget, period_seconds: u64) -> Self {
        let metrics = CurrentMetrics::capture();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let total_tasks = metrics.total_processed() as u64;
        let failed_tasks = metrics.tasks_failed as u64;
        let actual_success_rate = metrics.success_rate();
        let actual_throughput = total_tasks as f64 / period_seconds as f64;

        let error_budget_remaining = calculate_error_budget(
            total_tasks as f64,
            failed_tasks as f64,
            slo_target.success_rate,
        );

        let is_compliant = actual_success_rate >= slo_target.success_rate
            && actual_throughput >= slo_target.throughput;

        let mut recommendations = Vec::new();
        if actual_success_rate < slo_target.success_rate {
            recommendations.push(format!(
                "Success rate ({:.2}%) is below target ({:.2}%). Investigate failure causes.",
                actual_success_rate * 100.0,
                slo_target.success_rate * 100.0
            ));
        }
        if actual_throughput < slo_target.throughput {
            recommendations.push(format!(
                "Throughput ({:.2} tasks/sec) is below target ({:.2} tasks/sec). Consider scaling up.",
                actual_throughput, slo_target.throughput
            ));
        }
        if error_budget_remaining < 0.1 {
            recommendations.push(
                "Error budget critically low. Prioritize reliability over features.".to_string(),
            );
        }

        Self {
            timestamp,
            period_seconds,
            slo_target: slo_target.clone(),
            actual_success_rate,
            actual_p95_latency: None, // Would need histogram data
            actual_throughput,
            is_compliant,
            error_budget_remaining,
            total_tasks,
            failed_tasks,
            recommendations,
        }
    }

    /// Format the report as human-readable text
    pub fn format_text(&self) -> String {
        let status = if self.is_compliant {
            "✓ COMPLIANT"
        } else {
            "✗ NON-COMPLIANT"
        };

        let mut report = format!(
            r"=== SLA Compliance Report ===
Generated: {}
Period: {} hours

Status: {}

Targets:
  Success Rate: {:.2}%
  Throughput:   {:.2} tasks/sec

Actual Performance:
  Success Rate: {:.2}%
  Throughput:   {:.2} tasks/sec
  Total Tasks:  {}
  Failed Tasks: {}

Error Budget:
  Remaining: {:.1}%
",
            self.timestamp,
            self.period_seconds / 3600,
            status,
            self.slo_target.success_rate * 100.0,
            self.slo_target.throughput,
            self.actual_success_rate * 100.0,
            self.actual_throughput,
            self.total_tasks,
            self.failed_tasks,
            self.error_budget_remaining * 100.0
        );

        if !self.recommendations.is_empty() {
            report.push_str("\nRecommendations:\n");
            for (i, rec) in self.recommendations.iter().enumerate() {
                report.push_str(&format!("  {}. {}\n", i + 1, rec));
            }
        }

        report
    }
}

// ============================================================================
// Metric Retention Manager
// ============================================================================

/// Manages metric history retention and cleanup
#[derive(Debug)]
pub struct MetricRetentionManager {
    /// Retention policies by metric name
    policies: StdMutex<HashMap<String, RetentionPolicy>>,
}

/// Retention policy for metrics
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    /// Maximum age of samples in seconds
    pub max_age_seconds: u64,
    /// Maximum number of samples to keep
    pub max_samples: usize,
}

impl RetentionPolicy {
    /// Create a new retention policy
    pub fn new(max_age_seconds: u64, max_samples: usize) -> Self {
        Self {
            max_age_seconds,
            max_samples,
        }
    }

    /// Default policy: 1 hour, 1000 samples
    pub fn default_policy() -> Self {
        Self::new(3600, 1000)
    }

    /// High-frequency policy: 5 minutes, 300 samples
    pub fn high_frequency() -> Self {
        Self::new(300, 300)
    }

    /// Long-term policy: 24 hours, 10000 samples
    pub fn long_term() -> Self {
        Self::new(86400, 10000)
    }
}

impl MetricRetentionManager {
    /// Create a new retention manager
    pub fn new() -> Self {
        Self {
            policies: StdMutex::new(HashMap::new()),
        }
    }

    /// Set retention policy for a metric
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_metrics::{MetricRetentionManager, RetentionPolicy};
    ///
    /// let manager = MetricRetentionManager::new();
    /// manager.set_policy("queue_size", RetentionPolicy::high_frequency());
    /// ```
    pub fn set_policy(&self, metric_name: &str, policy: RetentionPolicy) {
        self.policies
            .lock()
            .unwrap()
            .insert(metric_name.to_string(), policy);
    }

    /// Get retention policy for a metric
    pub fn get_policy(&self, metric_name: &str) -> RetentionPolicy {
        self.policies
            .lock()
            .unwrap()
            .get(metric_name)
            .cloned()
            .unwrap_or_else(RetentionPolicy::default_policy)
    }

    /// Apply retention policy to metric history
    ///
    /// Returns number of samples removed
    pub fn apply_retention(&self, metric_name: &str, history: &MetricHistory) -> usize {
        let policy = self.get_policy(metric_name);
        let samples = history.get_samples();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut removed = 0;

        // This is a simplified version - in real implementation,
        // we'd need mutable access to history to actually remove old samples
        // For now, just count what would be removed
        for sample in &samples {
            let age = now.saturating_sub(sample.timestamp);
            if age > policy.max_age_seconds {
                removed += 1;
            }
        }

        removed
    }
}

impl Default for MetricRetentionManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Capacity Planning
// ============================================================================

/// Capacity planning prediction result
#[derive(Debug, Clone)]
pub struct CapacityPrediction {
    /// Current resource utilization (0.0 to 1.0)
    pub current_utilization: f64,
    /// Predicted time until resource exhaustion (seconds)
    pub time_until_exhaustion: Option<f64>,
    /// Recommended action
    pub recommendation: CapacityRecommendation,
    /// Growth rate (units per second)
    pub growth_rate: f64,
    /// Available capacity remaining
    pub available_capacity: f64,
}

/// Capacity planning recommendations
#[derive(Debug, Clone, PartialEq)]
pub enum CapacityRecommendation {
    /// System has plenty of capacity
    Healthy,
    /// Monitor closely, approaching limits
    Monitor,
    /// Take action soon (within hours)
    ActionNeeded,
    /// Immediate action required (within minutes)
    Critical,
    /// Resource usage is decreasing
    Decreasing,
}

/// Predict when a resource will be exhausted based on current trends
///
/// # Arguments
///
/// * `history` - Historical metric data
/// * `max_capacity` - Maximum capacity (e.g., 100 for percentage, or absolute limit)
/// * `warning_threshold` - Utilization level to trigger warning (0.0-1.0)
///
/// # Example
///
/// ```
/// use celers_metrics::{MetricHistory, predict_capacity_exhaustion};
///
/// let mut queue_size = MetricHistory::new(100);
/// for i in 0..50 {
///     queue_size.record((i * 10) as f64); // Growing queue
/// }
///
/// let prediction = predict_capacity_exhaustion(&queue_size, 1000.0, 0.8);
/// if let Some(seconds) = prediction.time_until_exhaustion {
///     println!("Queue will be full in {} seconds", seconds);
/// }
/// ```
pub fn predict_capacity_exhaustion(
    history: &MetricHistory,
    max_capacity: f64,
    warning_threshold: f64,
) -> CapacityPrediction {
    let samples = history.get_samples();

    if samples.len() < 2 {
        return CapacityPrediction {
            current_utilization: 0.0,
            time_until_exhaustion: None,
            recommendation: CapacityRecommendation::Healthy,
            growth_rate: 0.0,
            available_capacity: max_capacity,
        };
    }

    let latest = samples.last().unwrap();
    let current_value = latest.value;
    let current_utilization = current_value / max_capacity;

    // Calculate trend
    let trend = history.trend().unwrap_or(0.0);

    // Predict time until exhaustion
    let available = max_capacity - current_value;
    let time_until_exhaustion = if trend > 0.0 && available > 0.0 {
        Some(available / trend)
    } else {
        None
    };

    // Determine recommendation
    let recommendation = if trend < 0.0 {
        CapacityRecommendation::Decreasing
    } else if current_utilization >= 0.95 {
        CapacityRecommendation::Critical
    } else if current_utilization >= warning_threshold {
        if let Some(time) = time_until_exhaustion {
            if time < 3600.0 {
                // Less than 1 hour
                CapacityRecommendation::ActionNeeded
            } else if time < 86400.0 {
                // Less than 1 day
                CapacityRecommendation::Monitor
            } else {
                CapacityRecommendation::Healthy
            }
        } else {
            CapacityRecommendation::Monitor
        }
    } else {
        CapacityRecommendation::Healthy
    };

    CapacityPrediction {
        current_utilization,
        time_until_exhaustion,
        recommendation,
        growth_rate: trend,
        available_capacity: available,
    }
}

// ============================================================================
// Prometheus Query Builder
// ============================================================================

/// Prometheus query builder for common metric queries
pub struct PrometheusQueryBuilder {
    metric_name: String,
    labels: Vec<(String, String)>,
    aggregation: Option<String>,
    range: Option<String>,
}

impl PrometheusQueryBuilder {
    /// Create a new query builder for a metric
    ///
    /// # Example
    ///
    /// ```
    /// use celers_metrics::PrometheusQueryBuilder;
    ///
    /// let query = PrometheusQueryBuilder::new("celers_tasks_completed_total")
    ///     .with_label("task_name", "send_email")
    ///     .rate("5m")
    ///     .build();
    ///
    /// assert_eq!(query, "rate(celers_tasks_completed_total{task_name=\"send_email\"}[5m])");
    /// ```
    pub fn new(metric_name: impl Into<String>) -> Self {
        Self {
            metric_name: metric_name.into(),
            labels: Vec::new(),
            aggregation: None,
            range: None,
        }
    }

    /// Add a label filter
    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.push((key.into(), value.into()));
        self
    }

    /// Add rate aggregation
    pub fn rate(mut self, range: impl Into<String>) -> Self {
        self.aggregation = Some("rate".to_string());
        self.range = Some(range.into());
        self
    }

    /// Add increase aggregation
    pub fn increase(mut self, range: impl Into<String>) -> Self {
        self.aggregation = Some("increase".to_string());
        self.range = Some(range.into());
        self
    }

    /// Add sum aggregation
    pub fn sum(mut self) -> Self {
        self.aggregation = Some("sum".to_string());
        self
    }

    /// Add avg aggregation
    pub fn avg(mut self) -> Self {
        self.aggregation = Some("avg".to_string());
        self
    }

    /// Add histogram_quantile for percentile calculation
    pub fn histogram_quantile(mut self, quantile: f64, range: impl Into<String>) -> Self {
        self.aggregation = Some(format!("histogram_quantile({}", quantile));
        self.range = Some(range.into());
        self
    }

    /// Build the Prometheus query string
    pub fn build(self) -> String {
        let mut query = self.metric_name.clone();

        // Add labels
        if !self.labels.is_empty() {
            let labels_str = self
                .labels
                .iter()
                .map(|(k, v)| format!("{}=\"{}\"", k, v))
                .collect::<Vec<_>>()
                .join(",");
            query = format!("{}{{{}}}", query, labels_str);
        }

        // Add range if specified
        if let Some(ref range) = self.range {
            query = format!("{}[{}]", query, range);
        }

        // Wrap with aggregation if specified
        if let Some(agg) = self.aggregation {
            if agg.starts_with("histogram_quantile") {
                // Special handling for histogram_quantile
                if let Some(r) = self.range.as_ref() {
                    let _base_query = format!("{}[{}]", self.metric_name, r);
                    if !self.labels.is_empty() {
                        let labels_str = self
                            .labels
                            .iter()
                            .map(|(k, v)| format!("{}=\"{}\"", k, v))
                            .collect::<Vec<_>>()
                            .join(",");
                        query = format!("{}, rate({{{}}}_bucket[{}]))", agg, labels_str, r);
                    } else {
                        query = format!("{}, rate({}_bucket[{}]))", agg, self.metric_name, r);
                    }
                }
            } else {
                query = format!("{}({})", agg, query);
            }
        }

        query
    }
}

// ============================================================================
// Metric Collection Scheduler
// ============================================================================

/// Metric collection task configuration
#[derive(Debug)]
pub struct CollectionTask {
    /// Task name/identifier
    pub name: String,
    /// Collection interval in seconds
    pub interval_seconds: u64,
    /// Last collection timestamp
    pub last_collected: AtomicU64,
}

impl CollectionTask {
    /// Create a new collection task
    pub fn new(name: impl Into<String>, interval_seconds: u64) -> Self {
        Self {
            name: name.into(),
            interval_seconds,
            last_collected: AtomicU64::new(0),
        }
    }

    /// Check if it's time to collect
    pub fn should_collect(&self) -> bool {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let last = self.last_collected.load(Ordering::Relaxed);
        now >= last + self.interval_seconds
    }

    /// Mark as collected now
    pub fn mark_collected(&self) {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        self.last_collected.store(now, Ordering::Relaxed);
    }

    /// Get seconds until next collection
    pub fn seconds_until_next(&self) -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let last = self.last_collected.load(Ordering::Relaxed);
        let next = last + self.interval_seconds;
        next.saturating_sub(now)
    }
}

/// Metric collection scheduler
pub struct MetricCollectionScheduler {
    tasks: StdMutex<Vec<Arc<CollectionTask>>>,
}

impl MetricCollectionScheduler {
    /// Create a new scheduler
    pub fn new() -> Self {
        Self {
            tasks: StdMutex::new(Vec::new()),
        }
    }

    /// Register a collection task
    pub fn register_task(&self, task: CollectionTask) -> Arc<CollectionTask> {
        let task = Arc::new(task);
        let mut tasks = self
            .tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        tasks.push(Arc::clone(&task));
        task
    }

    /// Get all tasks that should be collected now
    pub fn tasks_to_collect(&self) -> Vec<Arc<CollectionTask>> {
        let tasks = self
            .tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        tasks
            .iter()
            .filter(|t| t.should_collect())
            .cloned()
            .collect()
    }

    /// Get all registered tasks
    pub fn all_tasks(&self) -> Vec<Arc<CollectionTask>> {
        let tasks = self
            .tasks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        tasks.clone()
    }
}

impl Default for MetricCollectionScheduler {
    fn default() -> Self {
        Self::new()
    }
}
