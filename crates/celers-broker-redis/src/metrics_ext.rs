//! Extended metrics for Redis broker monitoring
//!
//! Provides detailed metrics for enqueue/dequeue rates, operation latencies,
//! and percentile tracking (P50, P95, P99).

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

/// Metrics tracker for Redis broker operations
#[derive(Clone)]
pub struct MetricsTracker {
    inner: Arc<Mutex<MetricsInner>>,
}

struct MetricsInner {
    /// Enqueue operations counter
    enqueue_count: u64,
    /// Dequeue operations counter
    dequeue_count: u64,
    /// Last enqueue timestamp
    last_enqueue: Option<Instant>,
    /// Last dequeue timestamp
    last_dequeue: Option<Instant>,
    /// Enqueue rate samples (ops/sec)
    enqueue_rates: VecDeque<f64>,
    /// Dequeue rate samples (ops/sec)
    dequeue_rates: VecDeque<f64>,
    /// Enqueue latencies (in microseconds)
    enqueue_latencies: VecDeque<u64>,
    /// Dequeue latencies (in microseconds)
    dequeue_latencies: VecDeque<u64>,
    /// Maximum samples to keep
    max_samples: usize,
    /// Window duration for rate calculation
    window_duration: Duration,
}

impl Default for MetricsInner {
    fn default() -> Self {
        Self {
            enqueue_count: 0,
            dequeue_count: 0,
            last_enqueue: None,
            last_dequeue: None,
            enqueue_rates: VecDeque::new(),
            dequeue_rates: VecDeque::new(),
            enqueue_latencies: VecDeque::new(),
            dequeue_latencies: VecDeque::new(),
            max_samples: 1000,
            window_duration: Duration::from_secs(60),
        }
    }
}

impl MetricsTracker {
    /// Create a new metrics tracker
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MetricsInner::default())),
        }
    }

    /// Create a metrics tracker with custom configuration
    pub fn with_config(max_samples: usize, window_duration: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(MetricsInner {
                max_samples,
                window_duration,
                ..Default::default()
            })),
        }
    }

    /// Record an enqueue operation
    pub async fn record_enqueue(&self, latency: Duration) {
        let mut inner = self.inner.lock().await;
        inner.enqueue_count += 1;

        let latency_us = latency.as_micros() as u64;
        inner.enqueue_latencies.push_back(latency_us);

        // Trim to max samples
        if inner.enqueue_latencies.len() > inner.max_samples {
            inner.enqueue_latencies.pop_front();
        }

        // Calculate rate if we have a previous timestamp
        if let Some(last) = inner.last_enqueue {
            let elapsed = last.elapsed();
            if elapsed > Duration::from_millis(100) {
                // Avoid division by very small durations
                let rate = 1.0 / elapsed.as_secs_f64();
                inner.enqueue_rates.push_back(rate);

                // Trim to max samples
                if inner.enqueue_rates.len() > inner.max_samples {
                    inner.enqueue_rates.pop_front();
                }
            }
        }

        inner.last_enqueue = Some(Instant::now());
    }

    /// Record a dequeue operation
    pub async fn record_dequeue(&self, latency: Duration) {
        let mut inner = self.inner.lock().await;
        inner.dequeue_count += 1;

        let latency_us = latency.as_micros() as u64;
        inner.dequeue_latencies.push_back(latency_us);

        // Trim to max samples
        if inner.dequeue_latencies.len() > inner.max_samples {
            inner.dequeue_latencies.pop_front();
        }

        // Calculate rate if we have a previous timestamp
        if let Some(last) = inner.last_dequeue {
            let elapsed = last.elapsed();
            if elapsed > Duration::from_millis(100) {
                // Avoid division by very small durations
                let rate = 1.0 / elapsed.as_secs_f64();
                inner.dequeue_rates.push_back(rate);

                // Trim to max samples
                if inner.dequeue_rates.len() > inner.max_samples {
                    inner.dequeue_rates.pop_front();
                }
            }
        }

        inner.last_dequeue = Some(Instant::now());
    }

    /// Get current metrics snapshot
    pub async fn snapshot(&self) -> MetricsSnapshot {
        let inner = self.inner.lock().await;

        MetricsSnapshot {
            enqueue_count: inner.enqueue_count,
            dequeue_count: inner.dequeue_count,
            enqueue_rate: calculate_avg_rate(&inner.enqueue_rates),
            dequeue_rate: calculate_avg_rate(&inner.dequeue_rates),
            enqueue_latency: calculate_latency_stats(&inner.enqueue_latencies),
            dequeue_latency: calculate_latency_stats(&inner.dequeue_latencies),
        }
    }

    /// Reset all metrics
    pub async fn reset(&self) {
        let mut inner = self.inner.lock().await;
        *inner = MetricsInner {
            max_samples: inner.max_samples,
            window_duration: inner.window_duration,
            ..Default::default()
        };
    }
}

impl Default for MetricsTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of current metrics
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    /// Total enqueue operations
    pub enqueue_count: u64,
    /// Total dequeue operations
    pub dequeue_count: u64,
    /// Average enqueue rate (ops/sec)
    pub enqueue_rate: f64,
    /// Average dequeue rate (ops/sec)
    pub dequeue_rate: f64,
    /// Enqueue latency statistics
    pub enqueue_latency: LatencyStats,
    /// Dequeue latency statistics
    pub dequeue_latency: LatencyStats,
}

/// Latency statistics including percentiles
#[derive(Debug, Clone)]
pub struct LatencyStats {
    /// Minimum latency in microseconds
    pub min_us: u64,
    /// Maximum latency in microseconds
    pub max_us: u64,
    /// Average latency in microseconds
    pub avg_us: f64,
    /// P50 (median) latency in microseconds
    pub p50_us: u64,
    /// P95 latency in microseconds
    pub p95_us: u64,
    /// P99 latency in microseconds
    pub p99_us: u64,
    /// Number of samples
    pub sample_count: usize,
}

impl Default for LatencyStats {
    fn default() -> Self {
        Self {
            min_us: 0,
            max_us: 0,
            avg_us: 0.0,
            p50_us: 0,
            p95_us: 0,
            p99_us: 0,
            sample_count: 0,
        }
    }
}

/// Calculate average rate from samples
fn calculate_avg_rate(rates: &VecDeque<f64>) -> f64 {
    if rates.is_empty() {
        return 0.0;
    }

    rates.iter().sum::<f64>() / rates.len() as f64
}

/// Calculate latency statistics from samples
fn calculate_latency_stats(latencies: &VecDeque<u64>) -> LatencyStats {
    if latencies.is_empty() {
        return LatencyStats::default();
    }

    let mut sorted: Vec<u64> = latencies.iter().copied().collect();
    sorted.sort_unstable();

    let min = sorted[0];
    let max = sorted[sorted.len() - 1];
    let avg = sorted.iter().sum::<u64>() as f64 / sorted.len() as f64;

    let p50 = percentile(&sorted, 50.0);
    let p95 = percentile(&sorted, 95.0);
    let p99 = percentile(&sorted, 99.0);

    LatencyStats {
        min_us: min,
        max_us: max,
        avg_us: avg,
        p50_us: p50,
        p95_us: p95,
        p99_us: p99,
        sample_count: sorted.len(),
    }
}

/// Calculate percentile value
fn percentile(sorted_values: &[u64], p: f64) -> u64 {
    if sorted_values.is_empty() {
        return 0;
    }

    let index = (p / 100.0 * (sorted_values.len() - 1) as f64) as usize;
    sorted_values[index.min(sorted_values.len() - 1)]
}

/// Task age histogram for tracking how long tasks stay in queues
#[derive(Clone)]
pub struct TaskAgeHistogram {
    inner: Arc<Mutex<HistogramInner>>,
}

struct HistogramInner {
    /// Map of task ID to enqueue timestamp (Unix epoch in milliseconds)
    task_timestamps: HashMap<String, u64>,
    /// Histogram buckets (in seconds): <1, <5, <10, <30, <60, <300, <600, >=600
    buckets: [u64; 8],
    /// Bucket boundaries in seconds
    bucket_boundaries: [u64; 7],
    /// Total tasks processed
    total_tasks: u64,
    /// Total age in milliseconds (for average calculation)
    total_age_ms: u64,
}

impl Default for HistogramInner {
    fn default() -> Self {
        Self {
            task_timestamps: HashMap::new(),
            buckets: [0; 8],
            bucket_boundaries: [1, 5, 10, 30, 60, 300, 600],
            total_tasks: 0,
            total_age_ms: 0,
        }
    }
}

impl TaskAgeHistogram {
    /// Create a new task age histogram
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HistogramInner::default())),
        }
    }

    /// Record when a task was enqueued
    pub async fn record_enqueue(&self, task_id: String) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let mut inner = self.inner.lock().await;
        inner.task_timestamps.insert(task_id, now);
    }

    /// Record when a task was dequeued (calculates age and updates histogram)
    pub async fn record_dequeue(&self, task_id: &str) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let mut inner = self.inner.lock().await;

        if let Some(enqueue_time) = inner.task_timestamps.remove(task_id) {
            let age_ms = now.saturating_sub(enqueue_time);
            let age_secs = age_ms / 1000;

            // Update histogram bucket
            let bucket_index = inner
                .bucket_boundaries
                .iter()
                .position(|&boundary| age_secs < boundary)
                .unwrap_or(7);

            inner.buckets[bucket_index] += 1;
            inner.total_tasks += 1;
            inner.total_age_ms += age_ms;
        }
    }

    /// Get histogram snapshot
    pub async fn snapshot(&self) -> HistogramSnapshot {
        let inner = self.inner.lock().await;

        HistogramSnapshot {
            buckets: inner.buckets,
            bucket_labels: vec![
                "<1s".to_string(),
                "1-5s".to_string(),
                "5-10s".to_string(),
                "10-30s".to_string(),
                "30-60s".to_string(),
                "1-5m".to_string(),
                "5-10m".to_string(),
                ">=10m".to_string(),
            ],
            total_tasks: inner.total_tasks,
            avg_age_ms: if inner.total_tasks > 0 {
                inner.total_age_ms / inner.total_tasks
            } else {
                0
            },
        }
    }

    /// Reset the histogram
    pub async fn reset(&self) {
        let mut inner = self.inner.lock().await;
        *inner = HistogramInner::default();
    }

    /// Get current pending tasks count
    pub async fn pending_count(&self) -> usize {
        let inner = self.inner.lock().await;
        inner.task_timestamps.len()
    }
}

impl Default for TaskAgeHistogram {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of histogram data
#[derive(Debug, Clone)]
pub struct HistogramSnapshot {
    /// Bucket counts
    pub buckets: [u64; 8],
    /// Bucket labels
    pub bucket_labels: Vec<String>,
    /// Total tasks processed
    pub total_tasks: u64,
    /// Average age in milliseconds
    pub avg_age_ms: u64,
}

impl HistogramSnapshot {
    /// Get percentage of tasks in each bucket
    pub fn percentages(&self) -> Vec<f64> {
        if self.total_tasks == 0 {
            return vec![0.0; 8];
        }

        self.buckets
            .iter()
            .map(|&count| (count as f64 / self.total_tasks as f64) * 100.0)
            .collect()
    }
}

/// Slow operation logger for tracking operations that exceed thresholds
#[derive(Clone)]
pub struct SlowOperationLogger {
    inner: Arc<Mutex<SlowOpInner>>,
}

struct SlowOpInner {
    /// Threshold in milliseconds for what's considered slow
    threshold_ms: u64,
    /// Recent slow operations (limited to last N)
    slow_ops: VecDeque<SlowOperation>,
    /// Maximum slow operations to track
    max_tracked: usize,
    /// Total slow operations encountered
    total_slow_ops: u64,
}

/// Information about a slow operation
#[derive(Debug, Clone)]
pub struct SlowOperation {
    /// Operation name (e.g., "enqueue", "dequeue", "ack")
    pub operation: String,
    /// Duration in milliseconds
    pub duration_ms: u64,
    /// Timestamp when operation occurred (Unix epoch in milliseconds)
    pub timestamp: u64,
    /// Additional context (e.g., queue name, task ID)
    pub context: Option<String>,
}

impl SlowOperationLogger {
    /// Create a new slow operation logger with threshold
    pub fn new(threshold_ms: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SlowOpInner {
                threshold_ms,
                slow_ops: VecDeque::new(),
                max_tracked: 100,
                total_slow_ops: 0,
            })),
        }
    }

    /// Create with default threshold (1 second)
    pub fn default_threshold() -> Self {
        Self::new(1000)
    }

    /// Record an operation (logs if it's slow)
    pub async fn record(&self, operation: String, duration: Duration, context: Option<String>) {
        let duration_ms = duration.as_millis() as u64;
        let mut inner = self.inner.lock().await;

        if duration_ms >= inner.threshold_ms {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);

            let slow_op = SlowOperation {
                operation: operation.clone(),
                duration_ms,
                timestamp,
                context,
            };

            inner.slow_ops.push_back(slow_op.clone());
            inner.total_slow_ops += 1;

            // Trim to max tracked
            if inner.slow_ops.len() > inner.max_tracked {
                inner.slow_ops.pop_front();
            }

            // Log warning
            tracing::warn!(
                operation = %operation,
                duration_ms = duration_ms,
                threshold_ms = inner.threshold_ms,
                "Slow operation detected"
            );
        }
    }

    /// Get recent slow operations
    pub async fn recent_slow_ops(&self, limit: usize) -> Vec<SlowOperation> {
        let inner = self.inner.lock().await;
        inner.slow_ops.iter().rev().take(limit).cloned().collect()
    }

    /// Get total slow operation count
    pub async fn total_slow_ops(&self) -> u64 {
        let inner = self.inner.lock().await;
        inner.total_slow_ops
    }

    /// Clear slow operation history
    pub async fn clear(&self) {
        let mut inner = self.inner.lock().await;
        inner.slow_ops.clear();
        inner.total_slow_ops = 0;
    }

    /// Update threshold
    pub async fn set_threshold(&self, threshold_ms: u64) {
        let mut inner = self.inner.lock().await;
        inner.threshold_ms = threshold_ms;
    }

    /// Get current threshold
    pub async fn threshold(&self) -> u64 {
        let inner = self.inner.lock().await;
        inner.threshold_ms
    }
}

impl Default for SlowOperationLogger {
    fn default() -> Self {
        Self::default_threshold()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_metrics_tracker_new() {
        let tracker = MetricsTracker::new();
        let snapshot = tracker.snapshot().await;

        assert_eq!(snapshot.enqueue_count, 0);
        assert_eq!(snapshot.dequeue_count, 0);
        assert_eq!(snapshot.enqueue_rate, 0.0);
        assert_eq!(snapshot.dequeue_rate, 0.0);
    }

    #[tokio::test]
    async fn test_record_enqueue() {
        let tracker = MetricsTracker::new();

        tracker.record_enqueue(Duration::from_micros(100)).await;
        tracker.record_enqueue(Duration::from_micros(200)).await;
        tracker.record_enqueue(Duration::from_micros(150)).await;

        let snapshot = tracker.snapshot().await;

        assert_eq!(snapshot.enqueue_count, 3);
        assert_eq!(snapshot.enqueue_latency.sample_count, 3);
        assert_eq!(snapshot.enqueue_latency.min_us, 100);
        assert_eq!(snapshot.enqueue_latency.max_us, 200);
    }

    #[tokio::test]
    async fn test_record_dequeue() {
        let tracker = MetricsTracker::new();

        tracker.record_dequeue(Duration::from_micros(50)).await;
        tracker.record_dequeue(Duration::from_micros(100)).await;
        tracker.record_dequeue(Duration::from_micros(75)).await;

        let snapshot = tracker.snapshot().await;

        assert_eq!(snapshot.dequeue_count, 3);
        assert_eq!(snapshot.dequeue_latency.sample_count, 3);
        assert_eq!(snapshot.dequeue_latency.min_us, 50);
        assert_eq!(snapshot.dequeue_latency.max_us, 100);
    }

    #[tokio::test]
    async fn test_percentile_calculation() {
        let values = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

        assert_eq!(percentile(&values, 0.0), 1);
        assert_eq!(percentile(&values, 50.0), 5);
        assert_eq!(percentile(&values, 100.0), 10);
    }

    #[tokio::test]
    async fn test_latency_stats() {
        let mut latencies = VecDeque::new();
        for i in 1..=100 {
            latencies.push_back(i);
        }

        let stats = calculate_latency_stats(&latencies);

        assert_eq!(stats.min_us, 1);
        assert_eq!(stats.max_us, 100);
        assert_eq!(stats.sample_count, 100);
        assert!(stats.p50_us >= 45 && stats.p50_us <= 55);
        assert!(stats.p95_us >= 90 && stats.p95_us <= 100);
        assert!(stats.p99_us >= 95 && stats.p99_us <= 100);
    }

    #[tokio::test]
    async fn test_reset() {
        let tracker = MetricsTracker::new();

        tracker.record_enqueue(Duration::from_micros(100)).await;
        tracker.record_dequeue(Duration::from_micros(50)).await;

        let snapshot1 = tracker.snapshot().await;
        assert_eq!(snapshot1.enqueue_count, 1);
        assert_eq!(snapshot1.dequeue_count, 1);

        tracker.reset().await;

        let snapshot2 = tracker.snapshot().await;
        assert_eq!(snapshot2.enqueue_count, 0);
        assert_eq!(snapshot2.dequeue_count, 0);
    }

    #[tokio::test]
    async fn test_max_samples() {
        let tracker = MetricsTracker::with_config(10, Duration::from_secs(60));

        // Record more than max_samples
        for i in 0..20 {
            tracker.record_enqueue(Duration::from_micros(i * 10)).await;
        }

        let snapshot = tracker.snapshot().await;

        // Should only keep last 10 samples
        assert_eq!(snapshot.enqueue_latency.sample_count, 10);
    }

    #[test]
    fn test_calculate_avg_rate() {
        let mut rates = VecDeque::new();
        rates.push_back(10.0);
        rates.push_back(20.0);
        rates.push_back(30.0);

        assert_eq!(calculate_avg_rate(&rates), 20.0);

        let empty_rates = VecDeque::new();
        assert_eq!(calculate_avg_rate(&empty_rates), 0.0);
    }

    #[tokio::test]
    async fn test_task_age_histogram_new() {
        let histogram = TaskAgeHistogram::new();
        let snapshot = histogram.snapshot().await;

        assert_eq!(snapshot.total_tasks, 0);
        assert_eq!(snapshot.avg_age_ms, 0);
        assert_eq!(snapshot.buckets.len(), 8);
    }

    #[tokio::test]
    async fn test_task_age_histogram_record() {
        let histogram = TaskAgeHistogram::new();

        // Record enqueue
        histogram.record_enqueue("task1".to_string()).await;
        assert_eq!(histogram.pending_count().await, 1);

        // Record dequeue (immediately, so age ~0)
        histogram.record_dequeue("task1").await;
        assert_eq!(histogram.pending_count().await, 0);

        let snapshot = histogram.snapshot().await;
        assert_eq!(snapshot.total_tasks, 1);
        assert!(snapshot.buckets[0] >= 1); // Should be in <1s bucket
    }

    #[tokio::test]
    async fn test_histogram_snapshot_percentages() {
        let snapshot = HistogramSnapshot {
            buckets: [10, 20, 30, 20, 10, 5, 3, 2],
            bucket_labels: vec![],
            total_tasks: 100,
            avg_age_ms: 5000,
        };

        let percentages = snapshot.percentages();
        assert_eq!(percentages.len(), 8);
        assert_eq!(percentages[0], 10.0);
        assert_eq!(percentages[1], 20.0);
        assert_eq!(percentages[2], 30.0);
    }

    #[tokio::test]
    async fn test_task_age_histogram_reset() {
        let histogram = TaskAgeHistogram::new();

        histogram.record_enqueue("task1".to_string()).await;
        histogram.record_dequeue("task1").await;

        let snapshot1 = histogram.snapshot().await;
        assert_eq!(snapshot1.total_tasks, 1);

        histogram.reset().await;

        let snapshot2 = histogram.snapshot().await;
        assert_eq!(snapshot2.total_tasks, 0);
    }

    #[tokio::test]
    async fn test_slow_operation_logger() {
        let logger = SlowOperationLogger::new(100);

        // Fast operation (shouldn't be logged)
        logger
            .record("fast_op".to_string(), Duration::from_millis(50), None)
            .await;

        assert_eq!(logger.total_slow_ops().await, 0);

        // Slow operation (should be logged)
        logger
            .record(
                "slow_op".to_string(),
                Duration::from_millis(200),
                Some("test context".to_string()),
            )
            .await;

        assert_eq!(logger.total_slow_ops().await, 1);

        let recent = logger.recent_slow_ops(10).await;
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].operation, "slow_op");
        assert_eq!(recent[0].duration_ms, 200);
    }

    #[tokio::test]
    async fn test_slow_operation_logger_threshold() {
        let logger = SlowOperationLogger::new(500);

        assert_eq!(logger.threshold().await, 500);

        logger.set_threshold(1000).await;

        assert_eq!(logger.threshold().await, 1000);
    }

    #[tokio::test]
    async fn test_slow_operation_logger_clear() {
        let logger = SlowOperationLogger::new(100);

        logger
            .record("slow_op1".to_string(), Duration::from_millis(200), None)
            .await;

        logger
            .record("slow_op2".to_string(), Duration::from_millis(300), None)
            .await;

        assert_eq!(logger.total_slow_ops().await, 2);

        logger.clear().await;

        assert_eq!(logger.total_slow_ops().await, 0);
        let recent = logger.recent_slow_ops(10).await;
        assert_eq!(recent.len(), 0);
    }
}
