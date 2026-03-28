//! Metric pre-aggregation, custom labels, and distributed aggregation.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

// ============================================================================
// Metric Pre-Aggregation
// ============================================================================

/// Pre-aggregated metric statistics
#[derive(Debug, Clone)]
pub struct MetricStats {
    /// Count of observations
    pub count: u64,
    /// Sum of all observations
    pub sum: f64,
    /// Minimum value
    pub min: f64,
    /// Maximum value
    pub max: f64,
    /// Sum of squares (for variance calculation)
    pub sum_squares: f64,
}

impl Default for MetricStats {
    fn default() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            sum_squares: 0.0,
        }
    }
}

impl MetricStats {
    /// Create new empty metric stats
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an observation to the stats
    pub fn observe(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.sum_squares += value * value;
    }

    /// Calculate mean
    pub fn mean(&self) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        self.sum / self.count as f64
    }

    /// Calculate variance
    pub fn variance(&self) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        let mean = self.mean();
        (self.sum_squares / self.count as f64) - (mean * mean)
    }

    /// Calculate standard deviation
    pub fn std_dev(&self) -> f64 {
        self.variance().sqrt()
    }

    /// Reset all statistics
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    /// Merge another stats into this one
    pub fn merge(&mut self, other: &MetricStats) {
        self.count += other.count;
        self.sum += other.sum;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.sum_squares += other.sum_squares;
    }
}

/// Thread-safe metric aggregator
pub struct MetricAggregator {
    stats: Mutex<MetricStats>,
}

impl MetricAggregator {
    /// Create a new metric aggregator
    pub fn new() -> Self {
        Self {
            stats: Mutex::new(MetricStats::new()),
        }
    }

    /// Record an observation
    pub fn observe(&self, value: f64) {
        if let Ok(mut stats) = self.stats.lock() {
            stats.observe(value);
        }
    }

    /// Get a snapshot of current statistics
    pub fn snapshot(&self) -> MetricStats {
        self.stats
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Reset statistics
    pub fn reset(&self) {
        if let Ok(mut stats) = self.stats.lock() {
            stats.reset();
        }
    }
}

impl Default for MetricAggregator {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Custom Metric Labels
// ============================================================================

/// Custom labels for metrics
/// Allows adding arbitrary key-value labels to metrics for more granular tracking
#[derive(Debug, Clone, Default)]
pub struct CustomLabels {
    labels: HashMap<String, String>,
}

impl CustomLabels {
    /// Create a new empty set of custom labels
    pub fn new() -> Self {
        Self {
            labels: HashMap::new(),
        }
    }

    /// Add a label to the set
    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.insert(key.into(), value.into());
        self
    }

    /// Add multiple labels at once
    pub fn with_labels<K, V>(mut self, labels: impl IntoIterator<Item = (K, V)>) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        for (key, value) in labels {
            self.labels.insert(key.into(), value.into());
        }
        self
    }

    /// Get all labels as a vector of key-value pairs
    pub fn as_vec(&self) -> Vec<(&str, &str)> {
        self.labels
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect()
    }

    /// Get a label value by key
    pub fn get(&self, key: &str) -> Option<&str> {
        self.labels.get(key).map(std::string::String::as_str)
    }

    /// Check if a label exists
    pub fn contains(&self, key: &str) -> bool {
        self.labels.contains_key(key)
    }

    /// Get the number of labels
    pub fn len(&self) -> usize {
        self.labels.len()
    }

    /// Check if there are no labels
    pub fn is_empty(&self) -> bool {
        self.labels.is_empty()
    }

    /// Convert to a vector of label values in a specific order
    /// Useful for working with CounterVec and HistogramVec
    pub fn to_label_values(&self, label_names: &[&str]) -> Vec<&str> {
        label_names
            .iter()
            .map(|name| self.get(name).unwrap_or(""))
            .collect()
    }
}

impl From<HashMap<String, String>> for CustomLabels {
    fn from(labels: HashMap<String, String>) -> Self {
        Self { labels }
    }
}

impl<K, V> FromIterator<(K, V)> for CustomLabels
where
    K: Into<String>,
    V: Into<String>,
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let labels = iter
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        Self { labels }
    }
}

/// Builder for custom labeled metrics
#[derive(Debug, Clone)]
pub struct CustomMetricBuilder {
    labels: CustomLabels,
}

impl CustomMetricBuilder {
    /// Create a new custom metric builder
    pub fn new() -> Self {
        Self {
            labels: CustomLabels::new(),
        }
    }

    /// Add a label
    pub fn label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels = self.labels.with_label(key, value);
        self
    }

    /// Add multiple labels
    pub fn labels<K, V>(mut self, labels: impl IntoIterator<Item = (K, V)>) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.labels = self.labels.with_labels(labels);
        self
    }

    /// Get the custom labels
    pub fn build(self) -> CustomLabels {
        self.labels
    }
}

impl Default for CustomMetricBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Distributed Metric Aggregation
// ============================================================================

/// Metric snapshot with timestamp for distributed aggregation
#[derive(Debug, Clone)]
pub struct MetricSnapshot {
    /// Timestamp when the snapshot was taken
    pub timestamp: u64,
    /// Worker ID or node identifier
    pub worker_id: String,
    /// Pre-aggregated statistics
    pub stats: MetricStats,
    /// Custom labels for this snapshot
    pub labels: CustomLabels,
}

impl MetricSnapshot {
    /// Create a new metric snapshot
    pub fn new(worker_id: impl Into<String>, stats: MetricStats) -> Self {
        Self {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            worker_id: worker_id.into(),
            stats,
            labels: CustomLabels::new(),
        }
    }

    /// Create a snapshot with custom labels
    pub fn with_labels(mut self, labels: CustomLabels) -> Self {
        self.labels = labels;
        self
    }

    /// Check if snapshot is stale (older than threshold in seconds)
    pub fn is_stale(&self, threshold_seconds: u64) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        (now - self.timestamp) > threshold_seconds
    }
}

/// Distributed metric aggregator
/// Aggregates metrics from multiple workers/nodes
#[derive(Debug)]
pub struct DistributedAggregator {
    snapshots: Mutex<HashMap<String, MetricSnapshot>>,
    stale_threshold_seconds: u64,
}

impl DistributedAggregator {
    /// Create a new distributed aggregator with default stale threshold (300 seconds)
    pub fn new() -> Self {
        Self::with_stale_threshold(300)
    }

    /// Create a new distributed aggregator with custom stale threshold
    pub fn with_stale_threshold(threshold_seconds: u64) -> Self {
        Self {
            snapshots: Mutex::new(HashMap::new()),
            stale_threshold_seconds: threshold_seconds,
        }
    }

    /// Add or update a metric snapshot from a worker
    pub fn update(&self, snapshot: MetricSnapshot) {
        if let Ok(mut snapshots) = self.snapshots.lock() {
            snapshots.insert(snapshot.worker_id.clone(), snapshot);
        }
    }

    /// Get aggregated statistics across all workers
    pub fn aggregate(&self) -> MetricStats {
        let snapshots = self.snapshots.lock().unwrap_or_else(|e| e.into_inner());

        let mut combined = MetricStats::new();
        for snapshot in snapshots.values() {
            if !snapshot.is_stale(self.stale_threshold_seconds) {
                combined.merge(&snapshot.stats);
            }
        }
        combined
    }

    /// Get all active (non-stale) snapshots
    pub fn active_snapshots(&self) -> Vec<MetricSnapshot> {
        let snapshots = self.snapshots.lock().unwrap_or_else(|e| e.into_inner());
        snapshots
            .values()
            .filter(|s| !s.is_stale(self.stale_threshold_seconds))
            .cloned()
            .collect()
    }

    /// Remove stale snapshots
    pub fn cleanup_stale(&self) {
        if let Ok(mut snapshots) = self.snapshots.lock() {
            snapshots.retain(|_, snapshot| !snapshot.is_stale(self.stale_threshold_seconds));
        }
    }

    /// Get number of active workers
    pub fn active_worker_count(&self) -> usize {
        self.active_snapshots().len()
    }

    /// Reset all snapshots
    pub fn reset(&self) {
        if let Ok(mut snapshots) = self.snapshots.lock() {
            snapshots.clear();
        }
    }
}

impl Default for DistributedAggregator {
    fn default() -> Self {
        Self::new()
    }
}
