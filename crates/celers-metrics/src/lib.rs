//! Prometheus metrics for CeleRS
//!
//! This module provides Prometheus metrics integration for monitoring task queue performance.

use lazy_static::lazy_static;
use prometheus::{
    register_counter, register_counter_vec, register_gauge, register_histogram,
    register_histogram_vec, Counter, CounterVec, Encoder, Gauge, Histogram, HistogramVec,
    TextEncoder,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

lazy_static! {
    /// Total number of tasks enqueued
    pub static ref TASKS_ENQUEUED_TOTAL: Counter =
        register_counter!("celers_tasks_enqueued_total", "Total number of tasks enqueued")
            .unwrap();

    /// Total number of tasks completed successfully
    pub static ref TASKS_COMPLETED_TOTAL: Counter =
        register_counter!("celers_tasks_completed_total", "Total number of tasks completed successfully")
            .unwrap();

    /// Total number of tasks failed
    pub static ref TASKS_FAILED_TOTAL: Counter =
        register_counter!("celers_tasks_failed_total", "Total number of tasks failed")
            .unwrap();

    /// Total number of tasks retried
    pub static ref TASKS_RETRIED_TOTAL: Counter =
        register_counter!("celers_tasks_retried_total", "Total number of tasks retried")
            .unwrap();

    /// Total number of tasks cancelled
    pub static ref TASKS_CANCELLED_TOTAL: Counter =
        register_counter!("celers_tasks_cancelled_total", "Total number of tasks cancelled")
            .unwrap();

    /// Current queue size
    pub static ref QUEUE_SIZE: Gauge =
        register_gauge!("celers_queue_size", "Current number of tasks in queue")
            .unwrap();

    /// Current processing queue size
    pub static ref PROCESSING_QUEUE_SIZE: Gauge =
        register_gauge!("celers_processing_queue_size", "Current number of tasks being processed")
            .unwrap();

    /// Current dead letter queue size
    pub static ref DLQ_SIZE: Gauge =
        register_gauge!("celers_dlq_size", "Current number of tasks in dead letter queue")
            .unwrap();

    /// Number of active workers
    pub static ref ACTIVE_WORKERS: Gauge =
        register_gauge!("celers_active_workers", "Number of active workers")
            .unwrap();

    /// Task execution time histogram (in seconds)
    pub static ref TASK_EXECUTION_TIME: Histogram =
        register_histogram!(
            "celers_task_execution_seconds",
            "Task execution time in seconds",
            vec![0.001, 0.01, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0]
        )
        .unwrap();

    // Per-Task-Type Metrics (with labels)

    /// Total number of tasks enqueued by task type
    pub static ref TASKS_ENQUEUED_BY_TYPE: CounterVec =
        register_counter_vec!(
            "celers_tasks_enqueued_by_type_total",
            "Total number of tasks enqueued by task type",
            &["task_name"]
        )
        .unwrap();

    /// Total number of tasks completed by task type
    pub static ref TASKS_COMPLETED_BY_TYPE: CounterVec =
        register_counter_vec!(
            "celers_tasks_completed_by_type_total",
            "Total number of tasks completed by task type",
            &["task_name"]
        )
        .unwrap();

    /// Total number of tasks failed by task type
    pub static ref TASKS_FAILED_BY_TYPE: CounterVec =
        register_counter_vec!(
            "celers_tasks_failed_by_type_total",
            "Total number of tasks failed by task type",
            &["task_name"]
        )
        .unwrap();

    /// Total number of tasks retried by task type
    pub static ref TASKS_RETRIED_BY_TYPE: CounterVec =
        register_counter_vec!(
            "celers_tasks_retried_by_type_total",
            "Total number of tasks retried by task type",
            &["task_name"]
        )
        .unwrap();

    /// Total number of tasks cancelled by task type
    pub static ref TASKS_CANCELLED_BY_TYPE: CounterVec =
        register_counter_vec!(
            "celers_tasks_cancelled_by_type_total",
            "Total number of tasks cancelled by task type",
            &["task_name"]
        )
        .unwrap();

    /// Task execution time histogram by task type (in seconds)
    pub static ref TASK_EXECUTION_TIME_BY_TYPE: HistogramVec =
        register_histogram_vec!(
            "celers_task_execution_by_type_seconds",
            "Task execution time by task type in seconds",
            &["task_name"],
            vec![0.001, 0.01, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0]
        )
        .unwrap();

    /// Task result size by task type (in bytes)
    pub static ref TASK_RESULT_SIZE_BY_TYPE: HistogramVec =
        register_histogram_vec!(
            "celers_task_result_size_by_type_bytes",
            "Task result size by task type in bytes",
            &["task_name"],
            vec![100.0, 1_000.0, 10_000.0, 100_000.0, 1_000_000.0, 10_000_000.0]
        )
        .unwrap();

    // Connection Pooling Metrics

    /// Total number of Redis connections acquired
    pub static ref REDIS_CONNECTIONS_ACQUIRED_TOTAL: Counter =
        register_counter!("celers_redis_connections_acquired_total", "Total number of Redis connections acquired")
            .unwrap();

    /// Total number of Redis connection errors
    pub static ref REDIS_CONNECTION_ERRORS_TOTAL: Counter =
        register_counter!("celers_redis_connection_errors_total", "Total number of Redis connection errors")
            .unwrap();

    /// Current number of active Redis connections
    pub static ref REDIS_CONNECTIONS_ACTIVE: Gauge =
        register_gauge!("celers_redis_connections_active", "Current number of active Redis connections")
            .unwrap();

    /// Redis connection acquisition time histogram (in seconds)
    pub static ref REDIS_CONNECTION_ACQUIRE_TIME: Histogram =
        register_histogram!(
            "celers_redis_connection_acquire_seconds",
            "Redis connection acquisition time in seconds",
            vec![0.0001, 0.001, 0.01, 0.1, 0.5, 1.0, 5.0]
        )
        .unwrap();

    // PostgreSQL Connection Pool Metrics

    /// Maximum number of PostgreSQL connections in the pool
    pub static ref POSTGRES_POOL_MAX_SIZE: Gauge =
        register_gauge!("celers_postgres_pool_max_size", "Maximum number of PostgreSQL connections in the pool")
            .unwrap();

    /// Current number of PostgreSQL connections in the pool
    pub static ref POSTGRES_POOL_SIZE: Gauge =
        register_gauge!("celers_postgres_pool_size", "Current number of PostgreSQL connections in the pool")
            .unwrap();

    /// Current number of idle PostgreSQL connections
    pub static ref POSTGRES_POOL_IDLE: Gauge =
        register_gauge!("celers_postgres_pool_idle", "Current number of idle PostgreSQL connections")
            .unwrap();

    /// Current number of in-use PostgreSQL connections
    pub static ref POSTGRES_POOL_IN_USE: Gauge =
        register_gauge!("celers_postgres_pool_in_use", "Current number of in-use PostgreSQL connections")
            .unwrap();

    /// Total number of batch enqueue operations
    pub static ref BATCH_ENQUEUE_TOTAL: Counter =
        register_counter!("celers_batch_enqueue_total", "Total number of batch enqueue operations")
            .unwrap();

    /// Total number of batch dequeue operations
    pub static ref BATCH_DEQUEUE_TOTAL: Counter =
        register_counter!("celers_batch_dequeue_total", "Total number of batch dequeue operations")
            .unwrap();

    /// Batch size histogram (number of tasks per batch)
    pub static ref BATCH_SIZE: Histogram =
        register_histogram!(
            "celers_batch_size",
            "Number of tasks per batch operation",
            vec![1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0, 1000.0]
        )
        .unwrap();

    // Memory Usage Metrics

    /// Worker memory usage in bytes
    pub static ref WORKER_MEMORY_USAGE_BYTES: Gauge =
        register_gauge!("celers_worker_memory_usage_bytes", "Worker memory usage in bytes")
            .unwrap();

    /// Task result size histogram (in bytes)
    pub static ref TASK_RESULT_SIZE_BYTES: Histogram =
        register_histogram!(
            "celers_task_result_size_bytes",
            "Task result size in bytes",
            vec![100.0, 1_000.0, 10_000.0, 100_000.0, 1_000_000.0, 10_000_000.0]
        )
        .unwrap();

    /// Total number of tasks with oversized results (exceeded limit)
    pub static ref OVERSIZED_RESULTS_TOTAL: Counter =
        register_counter!("celers_oversized_results_total", "Total number of tasks with oversized results")
            .unwrap();

    // Task Age Metrics

    /// Task age histogram (time from creation to execution in seconds)
    pub static ref TASK_AGE_SECONDS: Histogram =
        register_histogram!(
            "celers_task_age_seconds",
            "Task age (time from creation to execution) in seconds",
            vec![1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0, 1800.0, 3600.0]
        )
        .unwrap();

    /// Task queue wait time histogram (time in queue before processing in seconds)
    pub static ref TASK_QUEUE_WAIT_TIME_SECONDS: Histogram =
        register_histogram!(
            "celers_task_queue_wait_time_seconds",
            "Task wait time in queue before processing in seconds",
            vec![0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0]
        )
        .unwrap();

    // Worker Utilization Metrics

    /// Worker utilization percentage (0-100)
    pub static ref WORKER_UTILIZATION_PERCENT: Gauge =
        register_gauge!("celers_worker_utilization_percent", "Worker utilization percentage (0-100)")
            .unwrap();

    /// Number of idle workers
    pub static ref IDLE_WORKERS: Gauge =
        register_gauge!("celers_idle_workers", "Number of idle workers")
            .unwrap();

    /// Number of busy workers
    pub static ref BUSY_WORKERS: Gauge =
        register_gauge!("celers_busy_workers", "Number of busy workers")
            .unwrap();

    // Broker Operation Latency Metrics

    /// Broker enqueue operation latency (in seconds)
    pub static ref BROKER_ENQUEUE_LATENCY_SECONDS: Histogram =
        register_histogram!(
            "celers_broker_enqueue_latency_seconds",
            "Broker enqueue operation latency in seconds",
            vec![0.0001, 0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
        )
        .unwrap();

    /// Broker dequeue operation latency (in seconds)
    pub static ref BROKER_DEQUEUE_LATENCY_SECONDS: Histogram =
        register_histogram!(
            "celers_broker_dequeue_latency_seconds",
            "Broker dequeue operation latency in seconds",
            vec![0.0001, 0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
        )
        .unwrap();

    /// Broker ack operation latency (in seconds)
    pub static ref BROKER_ACK_LATENCY_SECONDS: Histogram =
        register_histogram!(
            "celers_broker_ack_latency_seconds",
            "Broker ack operation latency in seconds",
            vec![0.0001, 0.001, 0.01, 0.05, 0.1, 0.5, 1.0]
        )
        .unwrap();

    /// Broker reject operation latency (in seconds)
    pub static ref BROKER_REJECT_LATENCY_SECONDS: Histogram =
        register_histogram!(
            "celers_broker_reject_latency_seconds",
            "Broker reject operation latency in seconds",
            vec![0.0001, 0.001, 0.01, 0.05, 0.1, 0.5, 1.0]
        )
        .unwrap();

    /// Broker queue_size operation latency (in seconds)
    pub static ref BROKER_QUEUE_SIZE_LATENCY_SECONDS: Histogram =
        register_histogram!(
            "celers_broker_queue_size_latency_seconds",
            "Broker queue_size operation latency in seconds",
            vec![0.0001, 0.001, 0.01, 0.05, 0.1, 0.5, 1.0]
        )
        .unwrap();

    // Delayed Task Metrics

    /// Number of delayed tasks currently scheduled
    pub static ref DELAYED_TASKS_SCHEDULED: Gauge =
        register_gauge!("celers_delayed_tasks_scheduled", "Number of delayed tasks currently scheduled")
            .unwrap();

    /// Total number of delayed tasks enqueued
    pub static ref DELAYED_TASKS_ENQUEUED_TOTAL: Counter =
        register_counter!("celers_delayed_tasks_enqueued_total", "Total number of delayed tasks enqueued")
            .unwrap();

    /// Total number of delayed tasks executed
    pub static ref DELAYED_TASKS_EXECUTED_TOTAL: Counter =
        register_counter!("celers_delayed_tasks_executed_total", "Total number of delayed tasks executed")
            .unwrap();
}

/// Get metrics in Prometheus text format
pub fn gather_metrics() -> String {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

/// Reset all metrics (useful for testing)
#[allow(dead_code)]
pub fn reset_metrics() {
    TASKS_ENQUEUED_TOTAL.reset();
    TASKS_COMPLETED_TOTAL.reset();
    TASKS_FAILED_TOTAL.reset();
    TASKS_RETRIED_TOTAL.reset();
    TASKS_CANCELLED_TOTAL.reset();
    QUEUE_SIZE.set(0.0);
    PROCESSING_QUEUE_SIZE.set(0.0);
    DLQ_SIZE.set(0.0);
    ACTIVE_WORKERS.set(0.0);
    TASKS_ENQUEUED_BY_TYPE.reset();
    TASKS_COMPLETED_BY_TYPE.reset();
    TASKS_FAILED_BY_TYPE.reset();
    TASKS_RETRIED_BY_TYPE.reset();
    TASKS_CANCELLED_BY_TYPE.reset();
    TASK_EXECUTION_TIME_BY_TYPE.reset();
    TASK_RESULT_SIZE_BY_TYPE.reset();
    REDIS_CONNECTIONS_ACQUIRED_TOTAL.reset();
    REDIS_CONNECTION_ERRORS_TOTAL.reset();
    REDIS_CONNECTIONS_ACTIVE.set(0.0);
    POSTGRES_POOL_MAX_SIZE.set(0.0);
    POSTGRES_POOL_SIZE.set(0.0);
    POSTGRES_POOL_IDLE.set(0.0);
    POSTGRES_POOL_IN_USE.set(0.0);
    BATCH_ENQUEUE_TOTAL.reset();
    BATCH_DEQUEUE_TOTAL.reset();
    WORKER_MEMORY_USAGE_BYTES.set(0.0);
    OVERSIZED_RESULTS_TOTAL.reset();
    WORKER_UTILIZATION_PERCENT.set(0.0);
    IDLE_WORKERS.set(0.0);
    BUSY_WORKERS.set(0.0);
    DELAYED_TASKS_SCHEDULED.set(0.0);
    DELAYED_TASKS_ENQUEUED_TOTAL.reset();
    DELAYED_TASKS_EXECUTED_TOTAL.reset();
}

// ============================================================================
// Configuration and Advanced Features
// ============================================================================

/// Default histogram buckets for execution time (in seconds)
pub const DEFAULT_EXECUTION_TIME_BUCKETS: &[f64] =
    &[0.001, 0.01, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0];

/// Default histogram buckets for latency (in seconds)
pub const DEFAULT_LATENCY_BUCKETS: &[f64] = &[0.0001, 0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0];

/// Default histogram buckets for size (in bytes)
pub const DEFAULT_SIZE_BUCKETS: &[f64] = &[
    100.0,
    1_000.0,
    10_000.0,
    100_000.0,
    1_000_000.0,
    10_000_000.0,
];

/// Configuration for metrics collection
#[derive(Debug, Clone)]
pub struct MetricsConfig {
    /// Sampling rate for high-frequency metrics (0.0 to 1.0)
    /// 1.0 = collect all metrics, 0.1 = collect 10% of metrics
    pub sampling_rate: f64,
    /// Custom histogram buckets for execution time
    pub execution_time_buckets: Vec<f64>,
    /// Custom histogram buckets for latency
    pub latency_buckets: Vec<f64>,
    /// Custom histogram buckets for size
    pub size_buckets: Vec<f64>,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            sampling_rate: 1.0,
            execution_time_buckets: DEFAULT_EXECUTION_TIME_BUCKETS.to_vec(),
            latency_buckets: DEFAULT_LATENCY_BUCKETS.to_vec(),
            size_buckets: DEFAULT_SIZE_BUCKETS.to_vec(),
        }
    }
}

impl MetricsConfig {
    /// Create a new metrics configuration with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the sampling rate for high-frequency metrics
    pub fn with_sampling_rate(mut self, rate: f64) -> Self {
        self.sampling_rate = rate.clamp(0.0, 1.0);
        self
    }

    /// Set custom histogram buckets for execution time
    pub fn with_execution_time_buckets(mut self, buckets: Vec<f64>) -> Self {
        self.execution_time_buckets = buckets;
        self
    }

    /// Set custom histogram buckets for latency
    pub fn with_latency_buckets(mut self, buckets: Vec<f64>) -> Self {
        self.latency_buckets = buckets;
        self
    }

    /// Set custom histogram buckets for size
    pub fn with_size_buckets(mut self, buckets: Vec<f64>) -> Self {
        self.size_buckets = buckets;
        self
    }

    /// Check if a metric should be sampled based on the configured sampling rate
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_metrics::MetricsConfig;
    ///
    /// let config = MetricsConfig::new().with_sampling_rate(0.5);
    /// // Will return true approximately 50% of the time
    /// let _ = config.should_sample();
    /// ```
    pub fn should_sample(&self) -> bool {
        if self.sampling_rate >= 1.0 {
            return true;
        }
        if self.sampling_rate <= 0.0 {
            return false;
        }
        rand::random::<f64>() < self.sampling_rate
    }
}

// ============================================================================
// Sampling Support
// ============================================================================

/// Sampler for high-frequency metrics
#[derive(Debug)]
pub struct MetricsSampler {
    sampling_rate: f64,
    counter: Arc<AtomicU64>,
}

impl MetricsSampler {
    /// Create a new metrics sampler with the given sampling rate
    pub fn new(sampling_rate: f64) -> Self {
        Self {
            sampling_rate: sampling_rate.clamp(0.0, 1.0),
            counter: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Check if the current metric should be collected based on sampling rate
    pub fn should_sample(&self) -> bool {
        if self.sampling_rate >= 1.0 {
            return true;
        }
        if self.sampling_rate <= 0.0 {
            return false;
        }

        let count = self.counter.fetch_add(1, Ordering::Relaxed);
        let sample_every = (1.0 / self.sampling_rate) as u64;
        count.is_multiple_of(sample_every)
    }
}

lazy_static! {
    /// Global metrics sampler
    static ref METRICS_SAMPLER: MetricsSampler = MetricsSampler::new(1.0);
}

/// Helper function to conditionally observe a metric based on sampling
pub fn observe_sampled<F>(observe_fn: F)
where
    F: FnOnce(),
{
    if METRICS_SAMPLER.should_sample() {
        observe_fn();
    }
}

// ============================================================================
// Rate Calculation Helpers
// ============================================================================

/// Calculate the rate of a counter over a time period
/// Returns events per second
pub fn calculate_rate(current_value: f64, previous_value: f64, time_delta_seconds: f64) -> f64 {
    if time_delta_seconds <= 0.0 {
        return 0.0;
    }
    (current_value - previous_value) / time_delta_seconds
}

/// Calculate success rate from completed and failed counters
pub fn calculate_success_rate(completed: f64, failed: f64) -> f64 {
    let total = completed + failed;
    if total <= 0.0 {
        return 0.0;
    }
    completed / total
}

/// Calculate error rate from completed and failed counters
pub fn calculate_error_rate(completed: f64, failed: f64) -> f64 {
    1.0 - calculate_success_rate(completed, failed)
}

/// Calculate throughput (tasks per second)
pub fn calculate_throughput(task_count: f64, time_seconds: f64) -> f64 {
    calculate_rate(task_count, 0.0, time_seconds)
}

// ============================================================================
// SLO/SLA Tracking
// ============================================================================

/// SLO (Service Level Objective) target
#[derive(Debug, Clone)]
pub struct SloTarget {
    /// Target success rate (0.0 to 1.0)
    pub success_rate: f64,
    /// Target latency in seconds (p95 or p99)
    pub latency_seconds: f64,
    /// Target throughput (tasks per second)
    pub throughput: f64,
}

impl Default for SloTarget {
    fn default() -> Self {
        Self {
            success_rate: 0.99,   // 99% success rate
            latency_seconds: 5.0, // 5 seconds p95 latency
            throughput: 100.0,    // 100 tasks per second
        }
    }
}

/// SLO compliance status
#[derive(Debug, Clone, PartialEq)]
pub enum SloStatus {
    /// Meeting SLO targets
    Compliant,
    /// Not meeting SLO targets
    NonCompliant,
    /// Insufficient data to determine compliance
    Unknown,
}

/// Check if current metrics meet SLO targets
pub fn check_slo_compliance(
    success_rate: f64,
    p95_latency_seconds: f64,
    throughput: f64,
    target: &SloTarget,
) -> SloStatus {
    if success_rate < 0.0 || p95_latency_seconds < 0.0 || throughput < 0.0 {
        return SloStatus::Unknown;
    }

    let meets_success = success_rate >= target.success_rate;
    let meets_latency = p95_latency_seconds <= target.latency_seconds;
    let meets_throughput = throughput >= target.throughput;

    if meets_success && meets_latency && meets_throughput {
        SloStatus::Compliant
    } else {
        SloStatus::NonCompliant
    }
}

/// Calculate error budget remaining (1.0 = 100% budget remaining)
pub fn calculate_error_budget(
    total_requests: f64,
    failed_requests: f64,
    target_success_rate: f64,
) -> f64 {
    if total_requests <= 0.0 {
        return 1.0; // Full budget if no requests
    }

    let allowed_failures = total_requests * (1.0 - target_success_rate);
    let budget_used = failed_requests / allowed_failures;
    (1.0 - budget_used).max(0.0)
}

// ============================================================================
// Anomaly Detection Helpers
// ============================================================================

/// Statistical thresholds for anomaly detection
#[derive(Debug, Clone)]
pub struct AnomalyThreshold {
    /// Mean baseline value
    pub mean: f64,
    /// Standard deviation
    pub std_dev: f64,
    /// Number of standard deviations for anomaly detection
    pub sigma_threshold: f64,
}

impl AnomalyThreshold {
    /// Create a new anomaly threshold with mean, std_dev, and sigma threshold
    pub fn new(mean: f64, std_dev: f64, sigma_threshold: f64) -> Self {
        Self {
            mean,
            std_dev,
            sigma_threshold,
        }
    }

    /// Calculate threshold from a sample of values
    pub fn from_samples(samples: &[f64], sigma_threshold: f64) -> Option<Self> {
        if samples.is_empty() {
            return None;
        }

        let mean = samples.iter().sum::<f64>() / samples.len() as f64;
        let variance =
            samples.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / samples.len() as f64;
        let std_dev = variance.sqrt();

        Some(Self {
            mean,
            std_dev,
            sigma_threshold,
        })
    }

    /// Check if a value is anomalous (outside threshold)
    pub fn is_anomalous(&self, value: f64) -> bool {
        let deviation = (value - self.mean).abs();
        deviation > self.std_dev * self.sigma_threshold
    }

    /// Get upper threshold
    pub fn upper_bound(&self) -> f64 {
        self.mean + (self.std_dev * self.sigma_threshold)
    }

    /// Get lower threshold
    pub fn lower_bound(&self) -> f64 {
        self.mean - (self.std_dev * self.sigma_threshold)
    }
}

/// Anomaly detection result
#[derive(Debug, Clone, PartialEq)]
pub enum AnomalyStatus {
    /// Normal value within expected range
    Normal,
    /// Value is abnormally high
    High,
    /// Value is abnormally low
    Low,
}

/// Detect anomalies in a metric value
pub fn detect_anomaly(value: f64, threshold: &AnomalyThreshold) -> AnomalyStatus {
    if value > threshold.upper_bound() {
        AnomalyStatus::High
    } else if value < threshold.lower_bound() {
        AnomalyStatus::Low
    } else {
        AnomalyStatus::Normal
    }
}

/// Exponential weighted moving average for trend detection
#[derive(Debug, Clone)]
pub struct MovingAverage {
    /// Current average value
    pub value: f64,
    /// Smoothing factor (0.0 to 1.0)
    pub alpha: f64,
}

impl MovingAverage {
    /// Create a new moving average with initial value and smoothing factor
    pub fn new(initial_value: f64, alpha: f64) -> Self {
        Self {
            value: initial_value,
            alpha: alpha.clamp(0.0, 1.0),
        }
    }

    /// Update with a new observation and return the new average
    pub fn update(&mut self, new_value: f64) -> f64 {
        self.value = self.alpha * new_value + (1.0 - self.alpha) * self.value;
        self.value
    }

    /// Get current average value
    pub fn get(&self) -> f64 {
        self.value
    }
}

/// Detect sudden spikes or drops in metrics
pub fn detect_spike(current: f64, baseline: f64, threshold_ratio: f64) -> bool {
    if baseline <= 0.0 {
        return false;
    }
    let ratio = current / baseline;
    ratio > threshold_ratio || ratio < (1.0 / threshold_ratio)
}

// ============================================================================
// Metric Pre-Aggregation
// ============================================================================

use std::sync::Mutex;

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
        self.stats.lock().unwrap_or_else(|e| e.into_inner()).clone()
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

use std::collections::HashMap;

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
        self.labels.get(key).map(|s| s.as_str())
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

use std::time::{SystemTime, UNIX_EPOCH};

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
// Health Check Utilities
// ============================================================================

/// Health status based on metrics and SLO targets
#[derive(Debug, Clone, PartialEq)]
pub enum HealthStatus {
    /// System is healthy and meeting all SLO targets
    Healthy,
    /// System is degraded but operational
    Degraded {
        /// Reasons for degradation
        reasons: Vec<String>,
    },
    /// System is unhealthy and not meeting SLO targets
    Unhealthy {
        /// Reasons for unhealthy status
        reasons: Vec<String>,
    },
}

/// Health check configuration
#[derive(Debug, Clone)]
pub struct HealthCheckConfig {
    /// Maximum acceptable queue size
    pub max_queue_size: f64,
    /// Maximum acceptable DLQ size
    pub max_dlq_size: f64,
    /// Minimum required active workers
    pub min_active_workers: f64,
    /// SLO target for compliance checking
    pub slo_target: Option<SloTarget>,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            max_queue_size: 1000.0,
            max_dlq_size: 100.0,
            min_active_workers: 1.0,
            slo_target: None,
        }
    }
}

impl HealthCheckConfig {
    /// Create a new health check configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set maximum queue size
    pub fn with_max_queue_size(mut self, size: f64) -> Self {
        self.max_queue_size = size;
        self
    }

    /// Set maximum DLQ size
    pub fn with_max_dlq_size(mut self, size: f64) -> Self {
        self.max_dlq_size = size;
        self
    }

    /// Set minimum active workers
    pub fn with_min_active_workers(mut self, count: f64) -> Self {
        self.min_active_workers = count;
        self
    }

    /// Set SLO target
    pub fn with_slo_target(mut self, target: SloTarget) -> Self {
        self.slo_target = Some(target);
        self
    }
}

/// Perform health check based on current metrics
///
/// # Examples
///
/// ```
/// use celers_metrics::{health_check, HealthCheckConfig, HealthStatus, SloTarget};
///
/// let config = HealthCheckConfig::new()
///     .with_max_queue_size(1000.0)
///     .with_min_active_workers(2.0);
///
/// match health_check(&config) {
///     HealthStatus::Healthy => println!("System is healthy"),
///     HealthStatus::Degraded { reasons } => {
///         println!("System is degraded: {:?}", reasons);
///     }
///     HealthStatus::Unhealthy { reasons } => {
///         println!("System is unhealthy: {:?}", reasons);
///     }
/// }
/// ```
pub fn health_check(config: &HealthCheckConfig) -> HealthStatus {
    let mut warnings = Vec::new();
    let mut errors = Vec::new();

    // Check queue sizes
    let queue_size = QUEUE_SIZE.get();
    if queue_size > config.max_queue_size * 0.8 {
        warnings.push(format!(
            "Queue size is high: {:.0}/{:.0}",
            queue_size, config.max_queue_size
        ));
    }
    if queue_size > config.max_queue_size {
        errors.push(format!(
            "Queue size exceeded limit: {:.0}/{:.0}",
            queue_size, config.max_queue_size
        ));
    }

    // Check DLQ size
    let dlq_size = DLQ_SIZE.get();
    if dlq_size > config.max_dlq_size * 0.5 {
        warnings.push(format!(
            "DLQ size is growing: {:.0}/{:.0}",
            dlq_size, config.max_dlq_size
        ));
    }
    if dlq_size > config.max_dlq_size {
        errors.push(format!(
            "DLQ size exceeded limit: {:.0}/{:.0}",
            dlq_size, config.max_dlq_size
        ));
    }

    // Check worker count
    let active_workers = ACTIVE_WORKERS.get();
    if active_workers < config.min_active_workers {
        errors.push(format!(
            "Insufficient workers: {:.0}/{:.0}",
            active_workers, config.min_active_workers
        ));
    }

    // Check SLO compliance if configured
    if let Some(ref slo_target) = config.slo_target {
        let completed = TASKS_COMPLETED_TOTAL.get();
        let failed = TASKS_FAILED_TOTAL.get();
        let success_rate = calculate_success_rate(completed, failed);

        if success_rate < slo_target.success_rate {
            errors.push(format!(
                "Success rate below SLO: {:.2}% < {:.2}%",
                success_rate * 100.0,
                slo_target.success_rate * 100.0
            ));
        }
    }

    if !errors.is_empty() {
        HealthStatus::Unhealthy { reasons: errors }
    } else if !warnings.is_empty() {
        HealthStatus::Degraded { reasons: warnings }
    } else {
        HealthStatus::Healthy
    }
}

// ============================================================================
// Percentile Calculation Helpers
// ============================================================================

/// Calculate percentile from a sorted list of values
///
/// # Arguments
///
/// * `values` - Sorted list of values
/// * `percentile` - Percentile to calculate (0.0 to 1.0)
///
/// # Examples
///
/// ```
/// use celers_metrics::calculate_percentile;
///
/// let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
///
/// let p50 = calculate_percentile(&values, 0.50).unwrap();
/// let p95 = calculate_percentile(&values, 0.95).unwrap();
/// let p99 = calculate_percentile(&values, 0.99).unwrap();
///
/// assert!((p50 - 5.5).abs() < 0.01);
/// assert!((p95 - 9.55).abs() < 0.01);
/// assert!((p99 - 9.91).abs() < 0.01);
/// ```
pub fn calculate_percentile(values: &[f64], percentile: f64) -> Option<f64> {
    if values.is_empty() || !(0.0..=1.0).contains(&percentile) {
        return None;
    }

    if values.len() == 1 {
        return Some(values[0]);
    }

    // Use the "R-7" method (default in R and NumPy)
    // Calculate the fractional index
    let index = percentile * (values.len() as f64 - 1.0);
    let lower_index = index.floor() as usize;
    let upper_index = index.ceil() as usize;
    let fraction = index - lower_index as f64;

    // Linear interpolation
    let lower_value = values[lower_index];
    let upper_value = values[upper_index];
    Some(lower_value + fraction * (upper_value - lower_value))
}

/// Batch percentile calculation for common percentiles (p50, p95, p99)
///
/// Returns a tuple of (p50, p95, p99)
///
/// # Examples
///
/// ```
/// use celers_metrics::calculate_percentiles;
///
/// let mut values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
/// values.sort_by(|a, b| a.partial_cmp(b).unwrap());
///
/// let (p50, p95, p99) = calculate_percentiles(&values).unwrap();
/// println!("p50: {:.2}, p95: {:.2}, p99: {:.2}", p50, p95, p99);
/// ```
pub fn calculate_percentiles(values: &[f64]) -> Option<(f64, f64, f64)> {
    if values.is_empty() {
        return None;
    }

    let p50 = calculate_percentile(values, 0.50)?;
    let p95 = calculate_percentile(values, 0.95)?;
    let p99 = calculate_percentile(values, 0.99)?;

    Some((p50, p95, p99))
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

// ============================================================================
// Alert Threshold Helpers
// ============================================================================

/// Alert condition for monitoring
#[derive(Debug, Clone)]
pub enum AlertCondition {
    /// Gauge exceeds threshold
    GaugeAbove { threshold: f64 },
    /// Gauge below threshold
    GaugeBelow { threshold: f64 },
    /// Success rate below threshold (0.0 to 1.0)
    SuccessRateBelow { threshold: f64 },
    /// Error rate above threshold (0.0 to 1.0)
    ErrorRateAbove { threshold: f64 },
}

/// Alert severity level
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum AlertSeverity {
    /// Informational alert
    Info,
    /// Warning alert
    Warning,
    /// Critical alert requiring immediate attention
    Critical,
}

/// Alert configuration
#[derive(Debug, Clone)]
pub struct AlertRule {
    /// Alert name
    pub name: String,
    /// Alert condition
    pub condition: AlertCondition,
    /// Alert severity
    pub severity: AlertSeverity,
    /// Alert description
    pub description: String,
}

impl AlertRule {
    /// Create a new alert rule
    pub fn new(
        name: impl Into<String>,
        condition: AlertCondition,
        severity: AlertSeverity,
        description: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            condition,
            severity,
            description: description.into(),
        }
    }

    /// Check if alert should fire based on current metrics
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_metrics::{AlertRule, AlertCondition, AlertSeverity, CurrentMetrics};
    ///
    /// let rule = AlertRule::new(
    ///     "high_error_rate",
    ///     AlertCondition::ErrorRateAbove { threshold: 0.05 },
    ///     AlertSeverity::Critical,
    ///     "Error rate exceeded 5%"
    /// );
    ///
    /// let metrics = CurrentMetrics {
    ///     tasks_enqueued: 100.0,
    ///     tasks_completed: 90.0,
    ///     tasks_failed: 10.0,
    ///     tasks_retried: 5.0,
    ///     tasks_cancelled: 2.0,
    ///     queue_size: 50.0,
    ///     processing_queue_size: 10.0,
    ///     dlq_size: 3.0,
    ///     active_workers: 5.0,
    /// };
    ///
    /// if rule.should_fire(&metrics) {
    ///     println!("Alert: {}", rule.name);
    /// }
    /// ```
    pub fn should_fire(&self, metrics: &CurrentMetrics) -> bool {
        match &self.condition {
            AlertCondition::GaugeAbove { threshold } => {
                metrics.queue_size > *threshold || metrics.processing_queue_size > *threshold
            }
            AlertCondition::GaugeBelow { threshold } => metrics.active_workers < *threshold,
            AlertCondition::SuccessRateBelow { threshold } => metrics.success_rate() < *threshold,
            AlertCondition::ErrorRateAbove { threshold } => metrics.error_rate() > *threshold,
        }
    }
}

/// Alert manager for tracking multiple alert rules
#[derive(Debug)]
pub struct AlertManager {
    rules: Vec<AlertRule>,
}

impl AlertManager {
    /// Create a new alert manager
    pub fn new() -> Self {
        Self { rules: Vec::new() }
    }

    /// Add an alert rule
    pub fn add_rule(&mut self, rule: AlertRule) {
        self.rules.push(rule);
    }

    /// Check all rules and return fired alerts
    pub fn check_alerts(&self, metrics: &CurrentMetrics) -> Vec<&AlertRule> {
        self.rules
            .iter()
            .filter(|rule| rule.should_fire(metrics))
            .collect()
    }

    /// Get critical alerts
    pub fn critical_alerts(&self, metrics: &CurrentMetrics) -> Vec<&AlertRule> {
        self.check_alerts(metrics)
            .into_iter()
            .filter(|rule| rule.severity == AlertSeverity::Critical)
            .collect()
    }
}

impl Default for AlertManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Metric Summary and Reporting
// ============================================================================

/// Generate a human-readable summary of current metrics
///
/// # Examples
///
/// ```
/// use celers_metrics::generate_metric_summary;
///
/// let summary = generate_metric_summary();
/// println!("{}", summary);
/// ```
pub fn generate_metric_summary() -> String {
    let metrics = CurrentMetrics::capture();

    format!(
        r#"=== CeleRS Metrics Summary ===

Tasks:
  Enqueued:  {:>10.0}
  Completed: {:>10.0}
  Failed:    {:>10.0}
  Retried:   {:>10.0}
  Cancelled: {:>10.0}

Rates:
  Success:   {:>9.2}%
  Error:     {:>9.2}%

Queues:
  Pending:    {:>9.0}
  Processing: {:>9.0}
  DLQ:        {:>9.0}

Workers:
  Active:     {:>9.0}
"#,
        metrics.tasks_enqueued,
        metrics.tasks_completed,
        metrics.tasks_failed,
        metrics.tasks_retried,
        metrics.tasks_cancelled,
        metrics.success_rate() * 100.0,
        metrics.error_rate() * 100.0,
        metrics.queue_size,
        metrics.processing_queue_size,
        metrics.dlq_size,
        metrics.active_workers,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    #[serial]
    fn test_metrics_increment() {
        reset_metrics();

        TASKS_ENQUEUED_TOTAL.inc();
        TASKS_COMPLETED_TOTAL.inc();
        QUEUE_SIZE.set(5.0);

        let metrics = gather_metrics();
        assert!(metrics.contains("celers_tasks_enqueued_total"));
        assert!(metrics.contains("celers_tasks_completed_total"));
        assert!(metrics.contains("celers_queue_size"));
    }

    #[test]
    #[serial]
    fn test_task_execution_time() {
        reset_metrics();

        TASK_EXECUTION_TIME.observe(1.5);
        TASK_EXECUTION_TIME.observe(0.5);

        let metrics = gather_metrics();
        assert!(metrics.contains("celers_task_execution_seconds"));
    }

    #[test]
    #[serial]
    fn test_per_task_type_metrics() {
        reset_metrics();

        // Track metrics for different task types
        TASKS_ENQUEUED_BY_TYPE
            .with_label_values(&["send_email"])
            .inc();
        TASKS_ENQUEUED_BY_TYPE
            .with_label_values(&["process_image"])
            .inc();
        TASKS_ENQUEUED_BY_TYPE
            .with_label_values(&["send_email"])
            .inc();

        TASKS_COMPLETED_BY_TYPE
            .with_label_values(&["send_email"])
            .inc();
        TASKS_FAILED_BY_TYPE
            .with_label_values(&["process_image"])
            .inc();

        TASK_EXECUTION_TIME_BY_TYPE
            .with_label_values(&["send_email"])
            .observe(1.5);
        TASK_EXECUTION_TIME_BY_TYPE
            .with_label_values(&["process_image"])
            .observe(2.3);

        TASK_RESULT_SIZE_BY_TYPE
            .with_label_values(&["send_email"])
            .observe(1024.0);

        let metrics = gather_metrics();

        // Verify labeled metrics are present
        assert!(metrics.contains("celers_tasks_enqueued_by_type_total"));
        assert!(metrics.contains("celers_tasks_completed_by_type_total"));
        assert!(metrics.contains("celers_tasks_failed_by_type_total"));
        assert!(metrics.contains("celers_task_execution_by_type_seconds"));
        assert!(metrics.contains("celers_task_result_size_by_type_bytes"));

        // Verify labels are present
        assert!(metrics.contains("task_name=\"send_email\""));
        assert!(metrics.contains("task_name=\"process_image\""));
    }

    #[test]
    fn test_metrics_config() {
        let config = MetricsConfig::new()
            .with_sampling_rate(0.5)
            .with_execution_time_buckets(vec![0.1, 1.0, 10.0])
            .with_latency_buckets(vec![0.01, 0.1, 1.0])
            .with_size_buckets(vec![1000.0, 10000.0]);

        assert_eq!(config.sampling_rate, 0.5);
        assert_eq!(config.execution_time_buckets, vec![0.1, 1.0, 10.0]);
        assert_eq!(config.latency_buckets, vec![0.01, 0.1, 1.0]);
        assert_eq!(config.size_buckets, vec![1000.0, 10000.0]);
    }

    #[test]
    fn test_metrics_sampler() {
        // Test 100% sampling
        let sampler = MetricsSampler::new(1.0);
        for _ in 0..100 {
            assert!(sampler.should_sample());
        }

        // Test 0% sampling
        let sampler = MetricsSampler::new(0.0);
        for _ in 0..100 {
            assert!(!sampler.should_sample());
        }

        // Test 50% sampling (approximately)
        let sampler = MetricsSampler::new(0.5);
        let mut sampled = 0;
        for _ in 0..100 {
            if sampler.should_sample() {
                sampled += 1;
            }
        }
        // Should be around 50, allow some variance
        assert!((45..=55).contains(&sampled), "sampled: {}", sampled);
    }

    #[test]
    fn test_rate_calculations() {
        // Test basic rate calculation
        assert_eq!(calculate_rate(100.0, 50.0, 10.0), 5.0);
        assert_eq!(calculate_rate(100.0, 50.0, 0.0), 0.0);

        // Test success rate
        assert!((calculate_success_rate(90.0, 10.0) - 0.9).abs() < 1e-10);
        assert_eq!(calculate_success_rate(100.0, 0.0), 1.0);
        assert_eq!(calculate_success_rate(0.0, 100.0), 0.0);
        assert_eq!(calculate_success_rate(0.0, 0.0), 0.0);

        // Test error rate
        assert!((calculate_error_rate(90.0, 10.0) - 0.1).abs() < 1e-10);
        assert_eq!(calculate_error_rate(100.0, 0.0), 0.0);
        assert_eq!(calculate_error_rate(0.0, 100.0), 1.0);

        // Test throughput
        assert_eq!(calculate_throughput(100.0, 10.0), 10.0);
        assert_eq!(calculate_throughput(100.0, 0.0), 0.0);
    }

    #[test]
    fn test_slo_compliance() {
        let target = SloTarget {
            success_rate: 0.99,
            latency_seconds: 5.0,
            throughput: 100.0,
        };

        // Test compliant case
        assert_eq!(
            check_slo_compliance(0.995, 4.5, 120.0, &target),
            SloStatus::Compliant
        );

        // Test non-compliant success rate
        assert_eq!(
            check_slo_compliance(0.98, 4.5, 120.0, &target),
            SloStatus::NonCompliant
        );

        // Test non-compliant latency
        assert_eq!(
            check_slo_compliance(0.995, 6.0, 120.0, &target),
            SloStatus::NonCompliant
        );

        // Test non-compliant throughput
        assert_eq!(
            check_slo_compliance(0.995, 4.5, 90.0, &target),
            SloStatus::NonCompliant
        );

        // Test unknown (negative values)
        assert_eq!(
            check_slo_compliance(-1.0, 4.5, 120.0, &target),
            SloStatus::Unknown
        );
    }

    #[test]
    fn test_error_budget() {
        // 99% success rate target
        let target_success_rate = 0.99;

        // 100% budget remaining (no failures yet)
        assert_eq!(
            calculate_error_budget(1000.0, 0.0, target_success_rate),
            1.0
        );

        // 50% budget remaining (5 out of 10 allowed failures used)
        assert!((calculate_error_budget(1000.0, 5.0, target_success_rate) - 0.5).abs() < 1e-10);

        // 0% budget remaining (all allowed failures used)
        assert!(calculate_error_budget(1000.0, 10.0, target_success_rate).abs() < 1e-10);

        // Budget exceeded (negative clamped to 0)
        assert_eq!(
            calculate_error_budget(1000.0, 20.0, target_success_rate),
            0.0
        );

        // No requests yet (100% budget)
        assert_eq!(calculate_error_budget(0.0, 0.0, target_success_rate), 1.0);
    }

    #[test]
    #[serial]
    fn test_concurrent_metrics_access() {
        use std::thread;

        reset_metrics();

        let handles: Vec<_> = (0..10)
            .map(|_| {
                thread::spawn(|| {
                    for _ in 0..100 {
                        TASKS_ENQUEUED_TOTAL.inc();
                        TASKS_COMPLETED_TOTAL.inc();
                        TASK_EXECUTION_TIME.observe(1.0);
                        QUEUE_SIZE.set(42.0);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // Verify metrics were incremented
        let metrics = gather_metrics();
        assert!(metrics.contains("celers_tasks_enqueued_total"));
        assert!(metrics.contains("celers_tasks_completed_total"));
        assert!(metrics.contains("celers_task_execution_seconds"));
        assert!(metrics.contains("celers_queue_size"));
    }

    #[test]
    #[serial]
    fn test_observe_sampled() {
        reset_metrics();
        let mut observed = 0;

        // Use observe_sampled with 100% sampling
        for _ in 0..10 {
            observe_sampled(|| {
                observed += 1;
            });
        }

        // All should be observed with default 100% sampling
        assert_eq!(observed, 10);
    }

    #[test]
    fn test_anomaly_threshold() {
        // Create threshold with mean=100, std_dev=10, 3-sigma
        let threshold = AnomalyThreshold::new(100.0, 10.0, 3.0);

        // Normal value
        assert!(!threshold.is_anomalous(100.0));
        assert!(!threshold.is_anomalous(110.0));
        assert!(!threshold.is_anomalous(90.0));

        // Anomalous values (outside 3 sigma)
        assert!(threshold.is_anomalous(131.0));
        assert!(threshold.is_anomalous(69.0));

        // Check bounds
        assert_eq!(threshold.upper_bound(), 130.0);
        assert_eq!(threshold.lower_bound(), 70.0);
    }

    #[test]
    fn test_anomaly_threshold_from_samples() {
        let samples = vec![10.0, 20.0, 30.0, 40.0, 50.0];
        let threshold = AnomalyThreshold::from_samples(&samples, 2.0).unwrap();

        // Mean should be 30
        assert!((threshold.mean - 30.0).abs() < 1e-10);

        // Check that values near mean are not anomalous
        assert!(!threshold.is_anomalous(30.0));

        // Empty samples should return None
        assert!(AnomalyThreshold::from_samples(&[], 2.0).is_none());
    }

    #[test]
    fn test_detect_anomaly() {
        let threshold = AnomalyThreshold::new(100.0, 10.0, 2.0);

        assert_eq!(detect_anomaly(100.0, &threshold), AnomalyStatus::Normal);
        assert_eq!(detect_anomaly(110.0, &threshold), AnomalyStatus::Normal);
        assert_eq!(detect_anomaly(121.0, &threshold), AnomalyStatus::High);
        assert_eq!(detect_anomaly(79.0, &threshold), AnomalyStatus::Low);
    }

    #[test]
    fn test_moving_average() {
        let mut ma = MovingAverage::new(10.0, 0.5);

        // Initial value
        assert_eq!(ma.get(), 10.0);

        // Update with new value
        let new_avg = ma.update(20.0);
        assert_eq!(new_avg, 15.0); // 0.5 * 20 + 0.5 * 10 = 15

        // Update again
        let new_avg = ma.update(30.0);
        assert_eq!(new_avg, 22.5); // 0.5 * 30 + 0.5 * 15 = 22.5
    }

    #[test]
    fn test_detect_spike() {
        // Normal case (within threshold)
        assert!(!detect_spike(100.0, 100.0, 2.0));
        assert!(!detect_spike(150.0, 100.0, 2.0));

        // Spike detected (above threshold)
        assert!(detect_spike(250.0, 100.0, 2.0));

        // Drop detected (below threshold)
        assert!(detect_spike(40.0, 100.0, 2.0));

        // Zero baseline should not detect spike
        assert!(!detect_spike(100.0, 0.0, 2.0));
    }

    #[test]
    fn test_metric_stats() {
        let mut stats = MetricStats::new();

        // Empty stats
        assert_eq!(stats.count, 0);
        assert_eq!(stats.mean(), 0.0);

        // Add observations
        stats.observe(10.0);
        stats.observe(20.0);
        stats.observe(30.0);

        assert_eq!(stats.count, 3);
        assert_eq!(stats.sum, 60.0);
        assert_eq!(stats.min, 10.0);
        assert_eq!(stats.max, 30.0);
        assert_eq!(stats.mean(), 20.0);

        // Variance = E[X²] - E[X]²
        // = (100 + 400 + 900) / 3 - 400
        // = 466.67 - 400 = 66.67
        assert!((stats.variance() - 66.666666).abs() < 0.001);
        assert!((stats.std_dev() - 8.165).abs() < 0.01);
    }

    #[test]
    fn test_metric_stats_merge() {
        let mut stats1 = MetricStats::new();
        stats1.observe(10.0);
        stats1.observe(20.0);

        let mut stats2 = MetricStats::new();
        stats2.observe(30.0);
        stats2.observe(40.0);

        stats1.merge(&stats2);

        assert_eq!(stats1.count, 4);
        assert_eq!(stats1.sum, 100.0);
        assert_eq!(stats1.min, 10.0);
        assert_eq!(stats1.max, 40.0);
        assert_eq!(stats1.mean(), 25.0);
    }

    #[test]
    fn test_metric_aggregator() {
        let aggregator = MetricAggregator::new();

        aggregator.observe(10.0);
        aggregator.observe(20.0);
        aggregator.observe(30.0);

        let snapshot = aggregator.snapshot();
        assert_eq!(snapshot.count, 3);
        assert_eq!(snapshot.mean(), 20.0);

        // Reset
        aggregator.reset();
        let snapshot = aggregator.snapshot();
        assert_eq!(snapshot.count, 0);
    }

    #[test]
    fn test_metric_aggregator_concurrent() {
        use std::thread;

        let aggregator = Arc::new(MetricAggregator::new());

        let handles: Vec<_> = (0..10)
            .map(|_| {
                let agg = Arc::clone(&aggregator);
                thread::spawn(move || {
                    for i in 0..100 {
                        agg.observe(i as f64);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let snapshot = aggregator.snapshot();
        assert_eq!(snapshot.count, 1000); // 10 threads * 100 observations
    }

    #[test]
    fn test_custom_labels() {
        let labels = CustomLabels::new()
            .with_label("environment", "production")
            .with_label("region", "us-west-2")
            .with_label("service", "api");

        assert_eq!(labels.len(), 3);
        assert_eq!(labels.get("environment"), Some("production"));
        assert_eq!(labels.get("region"), Some("us-west-2"));
        assert_eq!(labels.get("service"), Some("api"));
        assert!(labels.contains("environment"));
        assert!(!labels.contains("nonexistent"));
        assert!(!labels.is_empty());
    }

    #[test]
    fn test_custom_labels_builder() {
        let labels = CustomMetricBuilder::new()
            .label("env", "staging")
            .label("version", "1.0.0")
            .build();

        assert_eq!(labels.len(), 2);
        assert_eq!(labels.get("env"), Some("staging"));
        assert_eq!(labels.get("version"), Some("1.0.0"));
    }

    #[test]
    fn test_custom_labels_from_iter() {
        let labels: CustomLabels = vec![
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]
        .into_iter()
        .collect();

        assert_eq!(labels.len(), 2);
        assert_eq!(labels.get("key1"), Some("value1"));
        assert_eq!(labels.get("key2"), Some("value2"));
    }

    #[test]
    fn test_custom_labels_to_label_values() {
        let labels = CustomLabels::new()
            .with_label("task_name", "send_email")
            .with_label("priority", "high");

        let values = labels.to_label_values(&["task_name", "priority", "nonexistent"]);
        assert_eq!(values, vec!["send_email", "high", ""]);
    }

    #[test]
    fn test_metric_snapshot() {
        let mut stats = MetricStats::new();
        stats.observe(10.0);
        stats.observe(20.0);

        let snapshot = MetricSnapshot::new("worker-1", stats.clone());

        assert_eq!(snapshot.worker_id, "worker-1");
        assert_eq!(snapshot.stats.count, 2);
        assert_eq!(snapshot.stats.mean(), 15.0);
        assert!(!snapshot.is_stale(3600)); // Not stale within 1 hour
    }

    #[test]
    fn test_metric_snapshot_with_labels() {
        let stats = MetricStats::new();
        let labels = CustomLabels::new().with_label("region", "us-east-1");

        let snapshot = MetricSnapshot::new("worker-1", stats).with_labels(labels);

        assert_eq!(snapshot.labels.get("region"), Some("us-east-1"));
    }

    #[test]
    fn test_distributed_aggregator() {
        let aggregator = DistributedAggregator::new();

        // Create stats from worker 1
        let mut stats1 = MetricStats::new();
        stats1.observe(10.0);
        stats1.observe(20.0);
        let snapshot1 = MetricSnapshot::new("worker-1", stats1);

        // Create stats from worker 2
        let mut stats2 = MetricStats::new();
        stats2.observe(30.0);
        stats2.observe(40.0);
        let snapshot2 = MetricSnapshot::new("worker-2", stats2);

        // Update aggregator with snapshots
        aggregator.update(snapshot1);
        aggregator.update(snapshot2);

        // Aggregate stats
        let combined = aggregator.aggregate();
        assert_eq!(combined.count, 4);
        assert_eq!(combined.sum, 100.0);
        assert_eq!(combined.mean(), 25.0);
        assert_eq!(combined.min, 10.0);
        assert_eq!(combined.max, 40.0);

        // Check active worker count
        assert_eq!(aggregator.active_worker_count(), 2);
    }

    #[test]
    fn test_distributed_aggregator_update_same_worker() {
        let aggregator = DistributedAggregator::new();

        // First update from worker-1
        let mut stats1 = MetricStats::new();
        stats1.observe(10.0);
        aggregator.update(MetricSnapshot::new("worker-1", stats1));

        // Second update from same worker (should replace)
        let mut stats2 = MetricStats::new();
        stats2.observe(20.0);
        stats2.observe(30.0);
        aggregator.update(MetricSnapshot::new("worker-1", stats2));

        let combined = aggregator.aggregate();
        assert_eq!(combined.count, 2); // Should only have stats2 data
        assert_eq!(combined.sum, 50.0);
    }

    #[test]
    fn test_distributed_aggregator_cleanup() {
        let aggregator = DistributedAggregator::with_stale_threshold(60);

        let stats = MetricStats::new();

        // Create a snapshot with an old timestamp (manually)
        let mut old_snapshot = MetricSnapshot::new("worker-1", stats);
        // Set timestamp to 2 minutes ago
        old_snapshot.timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 120;

        aggregator.update(old_snapshot);

        // Snapshot should be stale now (120 seconds > 60 second threshold)
        assert_eq!(aggregator.active_worker_count(), 0);

        // Cleanup stale snapshots
        aggregator.cleanup_stale();

        // Should have no snapshots after cleanup
        let snapshots = aggregator.active_snapshots();
        assert_eq!(snapshots.len(), 0);
    }

    #[test]
    fn test_distributed_aggregator_reset() {
        let aggregator = DistributedAggregator::new();

        let stats = MetricStats::new();
        aggregator.update(MetricSnapshot::new("worker-1", stats.clone()));
        aggregator.update(MetricSnapshot::new("worker-2", stats));

        assert_eq!(aggregator.active_worker_count(), 2);

        aggregator.reset();

        assert_eq!(aggregator.active_worker_count(), 0);
        let combined = aggregator.aggregate();
        assert_eq!(combined.count, 0);
    }

    #[test]
    fn test_metric_export_name() {
        let counter = MetricExport::Counter {
            name: "test_counter".to_string(),
            value: 10.0,
            labels: CustomLabels::new(),
        };

        assert_eq!(counter.name(), "test_counter");
    }

    #[test]
    fn test_statsd_config() {
        let config = StatsDConfig::new()
            .with_host("statsd.example.com")
            .with_port(9125)
            .with_prefix("myapp")
            .with_sample_rate(0.5);

        assert_eq!(config.host, "statsd.example.com");
        assert_eq!(config.port, 9125);
        assert_eq!(config.prefix, "myapp");
        assert_eq!(config.sample_rate, 0.5);
    }

    #[test]
    fn test_statsd_format_counter() {
        let config = StatsDConfig::new().with_prefix("celers");

        let metric = MetricExport::Counter {
            name: "tasks_completed".to_string(),
            value: 42.0,
            labels: CustomLabels::new(),
        };

        let formatted = config.format_metric(&metric);
        assert_eq!(formatted, "celers.tasks_completed:42|c");
    }

    #[test]
    fn test_statsd_format_gauge() {
        let config = StatsDConfig::new().with_prefix("celers");

        let metric = MetricExport::Gauge {
            name: "queue_size".to_string(),
            value: 100.0,
            labels: CustomLabels::new(),
        };

        let formatted = config.format_metric(&metric);
        assert_eq!(formatted, "celers.queue_size:100|g");
    }

    #[test]
    fn test_statsd_format_with_tags() {
        let config = StatsDConfig::new().with_prefix("celers");

        let labels = CustomLabels::new()
            .with_label("environment", "prod")
            .with_label("region", "us-east-1");

        let metric = MetricExport::Counter {
            name: "tasks_completed".to_string(),
            value: 42.0,
            labels,
        };

        let formatted = config.format_metric(&metric);
        assert!(formatted.starts_with("celers.tasks_completed:42|c|#"));
        assert!(formatted.contains("environment:prod"));
        assert!(formatted.contains("region:us-east-1"));
    }

    #[test]
    fn test_statsd_format_histogram() {
        let config = StatsDConfig::new().with_prefix("celers");

        let metric = MetricExport::Histogram {
            name: "task_duration".to_string(),
            count: 10,
            sum: 100.0,
            buckets: vec![],
            labels: CustomLabels::new(),
        };

        let formatted = config.format_metric(&metric);
        assert_eq!(formatted, "celers.task_duration:10|h");
    }

    #[test]
    fn test_opentelemetry_config() {
        let config = OpenTelemetryConfig::new()
            .with_service_name("my-service")
            .with_service_version("2.0.0")
            .with_environment("staging")
            .with_attribute("host", "server-1");

        assert_eq!(config.service_name, "my-service");
        assert_eq!(config.service_version, "2.0.0");
        assert_eq!(config.environment, "staging");
        assert_eq!(config.attributes.get("host"), Some("server-1"));
    }

    #[test]
    fn test_cloudwatch_config() {
        let config = CloudWatchConfig::new()
            .with_namespace("MyApp")
            .with_region("eu-west-1")
            .with_dimension("Environment", "Production")
            .with_storage_resolution(1);

        assert_eq!(config.namespace, "MyApp");
        assert_eq!(config.region, "eu-west-1");
        assert_eq!(config.dimensions.get("Environment"), Some("Production"));
        assert_eq!(config.storage_resolution, 1);
    }

    #[test]
    fn test_cloudwatch_storage_resolution() {
        let config1 = CloudWatchConfig::new().with_storage_resolution(1);
        assert_eq!(config1.storage_resolution, 1);

        let config2 = CloudWatchConfig::new().with_storage_resolution(60);
        assert_eq!(config2.storage_resolution, 60);

        // Any other value should default to 60
        let config3 = CloudWatchConfig::new().with_storage_resolution(30);
        assert_eq!(config3.storage_resolution, 60);
    }

    #[test]
    fn test_datadog_config() {
        let config = DatadogConfig::new()
            .with_api_host("https://api.datadoghq.eu")
            .with_api_key("test-key-123")
            .with_prefix("myapp")
            .with_tag("env", "prod")
            .with_tag("region", "us-west-2");

        assert_eq!(config.api_host, "https://api.datadoghq.eu");
        assert_eq!(config.api_key, "test-key-123");
        assert_eq!(config.prefix, "myapp");
        assert_eq!(config.tags.get("env"), Some("prod"));
        assert_eq!(config.tags.get("region"), Some("us-west-2"));
    }

    #[test]
    fn test_datadog_format_tags() {
        let config = DatadogConfig::new().with_tag("global_tag", "global_value");

        let metric_labels = CustomLabels::new().with_label("metric_tag", "metric_value");

        let tags = config.format_tags(&metric_labels);

        assert_eq!(tags.len(), 2);
        assert!(tags.contains(&"global_tag:global_value".to_string()));
        assert!(tags.contains(&"metric_tag:metric_value".to_string()));
    }

    #[test]
    fn test_export_to_statsd() {
        let mut stats = MetricStats::new();
        stats.observe(10.0);
        stats.observe(20.0);

        let config = StatsDConfig::new().with_prefix("celers");
        let formatted = export_to_statsd(&stats, "execution_time", &config);

        assert_eq!(formatted, "celers.execution_time:15|h");
    }

    #[test]
    #[serial]
    fn test_current_metrics_capture() {
        reset_metrics();

        // Capture baseline to handle any residual values
        let baseline = CurrentMetrics::capture();

        TASKS_ENQUEUED_TOTAL.inc_by(100.0);
        TASKS_COMPLETED_TOTAL.inc_by(80.0);
        TASKS_FAILED_TOTAL.inc_by(20.0);
        QUEUE_SIZE.set(50.0);
        ACTIVE_WORKERS.set(5.0);

        let metrics = CurrentMetrics::capture();

        // Check relative changes for counters (use approximate comparisons for residual values)
        let enqueued_diff = metrics.tasks_enqueued - baseline.tasks_enqueued;
        let completed_diff = metrics.tasks_completed - baseline.tasks_completed;
        let failed_diff = metrics.tasks_failed - baseline.tasks_failed;

        assert!(
            (enqueued_diff - 100.0).abs() < 5.0,
            "Expected enqueued ~100.0, got {}",
            enqueued_diff
        );
        assert!(
            (completed_diff - 80.0).abs() < 5.0,
            "Expected completed ~80.0, got {}",
            completed_diff
        );
        assert!(
            (failed_diff - 20.0).abs() < 5.0,
            "Expected failed ~20.0, got {}",
            failed_diff
        );

        // Gauges are set to absolute values
        assert_eq!(metrics.queue_size, 50.0);
        assert_eq!(metrics.active_workers, 5.0);
    }

    #[test]
    #[serial]
    fn test_current_metrics_rates() {
        reset_metrics();

        // Capture baseline
        let baseline = CurrentMetrics::capture();

        TASKS_COMPLETED_TOTAL.inc_by(90.0);
        TASKS_FAILED_TOTAL.inc_by(10.0);

        let metrics = CurrentMetrics::capture();

        // Calculate rates from the change, not absolute values
        let completed = metrics.tasks_completed - baseline.tasks_completed;
        let failed = metrics.tasks_failed - baseline.tasks_failed;
        let total = completed + failed;

        // Use approximate comparisons to handle any residual values from previous tests
        assert!(
            (total - 100.0).abs() < 5.0,
            "Expected total ~100.0, got {}",
            total
        );

        // Verify the rates are approximately correct (9:1 ratio)
        let success_rate = completed / total;
        let error_rate = failed / total;

        assert!(
            (success_rate - 0.9).abs() < 0.05,
            "Expected success_rate ~0.9, got {}",
            success_rate
        );
        assert!(
            (error_rate - 0.1).abs() < 0.05,
            "Expected error_rate ~0.1, got {}",
            error_rate
        );
    }

    #[test]
    #[serial]
    fn test_health_check_healthy() {
        reset_metrics();

        QUEUE_SIZE.set(100.0);
        DLQ_SIZE.set(10.0);
        ACTIVE_WORKERS.set(5.0);

        let config = HealthCheckConfig::new()
            .with_max_queue_size(1000.0)
            .with_max_dlq_size(100.0)
            .with_min_active_workers(2.0);

        let status = health_check(&config);
        assert_eq!(status, HealthStatus::Healthy);
    }

    #[test]
    #[serial]
    fn test_health_check_degraded() {
        reset_metrics();

        // Queue size is at 85% (850/1000) - should trigger warning
        QUEUE_SIZE.set(850.0);
        DLQ_SIZE.set(10.0);
        ACTIVE_WORKERS.set(5.0);

        // Verify gauges were set correctly before running health check
        assert_eq!(QUEUE_SIZE.get(), 850.0, "Failed to set QUEUE_SIZE");
        assert_eq!(DLQ_SIZE.get(), 10.0, "Failed to set DLQ_SIZE");
        assert_eq!(ACTIVE_WORKERS.get(), 5.0, "Failed to set ACTIVE_WORKERS");

        let config = HealthCheckConfig::new()
            .with_max_queue_size(1000.0)
            .with_max_dlq_size(100.0)
            .with_min_active_workers(2.0);

        let status = health_check(&config);

        // Check that we got the expected Degraded status with queue-related warning
        match status {
            HealthStatus::Degraded { reasons } => {
                assert!(
                    !reasons.is_empty(),
                    "Expected at least one degradation reason"
                );
                assert!(
                    reasons
                        .iter()
                        .any(|r| r.contains("Queue size") || r.contains("queue")),
                    "Expected queue-related degradation, got reasons: {:?}",
                    reasons
                );
            }
            HealthStatus::Healthy => {
                panic!(
                    "Expected Degraded status, got Healthy. Queue was set to 850 (>800 threshold)"
                );
            }
            HealthStatus::Unhealthy { reasons } => {
                panic!(
                    "Expected Degraded status, got Unhealthy with reasons: {:?}",
                    reasons
                );
            }
        }
    }

    #[test]
    #[serial]
    fn test_health_check_unhealthy() {
        reset_metrics();

        QUEUE_SIZE.set(1500.0); // Exceeds limit
        DLQ_SIZE.set(10.0);
        ACTIVE_WORKERS.set(0.0); // Below minimum

        let config = HealthCheckConfig::new()
            .with_max_queue_size(1000.0)
            .with_max_dlq_size(100.0)
            .with_min_active_workers(2.0);

        let status = health_check(&config);
        match status {
            HealthStatus::Unhealthy { reasons } => {
                assert!(reasons.len() >= 2);
                assert!(reasons.iter().any(|r| r.contains("Queue size exceeded")));
                assert!(reasons.iter().any(|r| r.contains("Insufficient workers")));
            }
            _ => panic!("Expected Unhealthy status"),
        }
    }

    #[test]
    #[serial]
    fn test_health_check_with_slo() {
        reset_metrics();

        TASKS_COMPLETED_TOTAL.inc_by(95.0);
        TASKS_FAILED_TOTAL.inc_by(5.0);
        QUEUE_SIZE.set(100.0);
        DLQ_SIZE.set(10.0);
        ACTIVE_WORKERS.set(5.0);

        let slo = SloTarget {
            success_rate: 0.99,
            latency_seconds: 5.0,
            throughput: 100.0,
        };

        let config = HealthCheckConfig::new()
            .with_max_queue_size(1000.0)
            .with_max_dlq_size(100.0)
            .with_min_active_workers(2.0)
            .with_slo_target(slo);

        let status = health_check(&config);
        match status {
            HealthStatus::Unhealthy { reasons } => {
                assert!(reasons.iter().any(|r| r.contains("Success rate below SLO")));
            }
            _ => panic!("Expected Unhealthy status due to SLO violation"),
        }
    }

    #[test]
    fn test_calculate_percentile() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];

        // p50 (median)
        let p50 = calculate_percentile(&values, 0.50).unwrap();
        assert!((p50 - 5.5).abs() < 1e-10);

        // p0 (min)
        let p0 = calculate_percentile(&values, 0.0).unwrap();
        assert_eq!(p0, 1.0);

        // p100 (max)
        let p100 = calculate_percentile(&values, 1.0).unwrap();
        assert_eq!(p100, 10.0);

        // Empty slice
        assert!(calculate_percentile(&[], 0.5).is_none());

        // Invalid percentile
        assert!(calculate_percentile(&values, -0.1).is_none());
        assert!(calculate_percentile(&values, 1.1).is_none());
    }

    #[test]
    fn test_calculate_percentile_interpolation() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];

        let p95 = calculate_percentile(&values, 0.95).unwrap();
        assert!((p95 - 9.55).abs() < 1e-10);

        let p99 = calculate_percentile(&values, 0.99).unwrap();
        assert!((p99 - 9.91).abs() < 1e-10);
    }

    #[test]
    fn test_calculate_percentiles_batch() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];

        let (p50, p95, p99) = calculate_percentiles(&values).unwrap();

        assert!((p50 - 5.5).abs() < 1e-10);
        assert!((p95 - 9.55).abs() < 1e-10);
        assert!((p99 - 9.91).abs() < 1e-10);

        // Empty slice
        assert!(calculate_percentiles(&[]).is_none());
    }

    #[test]
    fn test_percentile_single_value() {
        let values = vec![42.0];

        let p50 = calculate_percentile(&values, 0.50).unwrap();
        assert_eq!(p50, 42.0);

        let p95 = calculate_percentile(&values, 0.95).unwrap();
        assert_eq!(p95, 42.0);
    }

    #[test]
    fn test_health_check_config_builder() {
        let config = HealthCheckConfig::new()
            .with_max_queue_size(2000.0)
            .with_max_dlq_size(200.0)
            .with_min_active_workers(10.0);

        assert_eq!(config.max_queue_size, 2000.0);
        assert_eq!(config.max_dlq_size, 200.0);
        assert_eq!(config.min_active_workers, 10.0);
        assert!(config.slo_target.is_none());
    }

    #[test]
    fn test_metric_comparison() {
        let baseline = CurrentMetrics {
            tasks_enqueued: 1000.0,
            tasks_completed: 900.0,
            tasks_failed: 100.0,
            tasks_retried: 50.0,
            tasks_cancelled: 10.0,
            queue_size: 100.0,
            processing_queue_size: 20.0,
            dlq_size: 5.0,
            active_workers: 10.0,
        };

        let improved = CurrentMetrics {
            tasks_enqueued: 1100.0,
            tasks_completed: 1050.0,
            tasks_failed: 50.0,
            tasks_retried: 45.0,
            tasks_cancelled: 8.0,
            queue_size: 80.0,
            processing_queue_size: 18.0,
            dlq_size: 4.0,
            active_workers: 12.0,
        };

        let comparison = MetricComparison::compare(&baseline, &improved);

        // Queue size decreased
        assert!(comparison.queue_size_diff < 0.0);
        // More workers
        assert!(comparison.workers_diff > 0.0);
        // Better metrics
        assert!(comparison.is_improvement());
        assert!(!comparison.is_degradation());
    }

    #[test]
    fn test_metric_comparison_degradation() {
        let baseline = CurrentMetrics {
            tasks_enqueued: 1000.0,
            tasks_completed: 950.0,
            tasks_failed: 50.0,
            tasks_retried: 25.0,
            tasks_cancelled: 5.0,
            queue_size: 50.0,
            processing_queue_size: 10.0,
            dlq_size: 2.0,
            active_workers: 10.0,
        };

        let degraded = CurrentMetrics {
            tasks_enqueued: 1100.0,
            tasks_completed: 900.0,
            tasks_failed: 200.0,
            tasks_retried: 100.0,
            tasks_cancelled: 20.0,
            queue_size: 150.0,
            processing_queue_size: 30.0,
            dlq_size: 10.0,
            active_workers: 8.0,
        };

        let comparison = MetricComparison::compare(&baseline, &degraded);

        // Performance degraded
        assert!(comparison.is_degradation());
        assert!(!comparison.is_improvement());
        // Queue size increased
        assert!(comparison.queue_size_diff > 0.0);
    }

    #[test]
    fn test_metric_comparison_significance() {
        let baseline = CurrentMetrics {
            tasks_enqueued: 1000.0,
            tasks_completed: 900.0,
            tasks_failed: 100.0,
            tasks_retried: 50.0,
            tasks_cancelled: 10.0,
            queue_size: 100.0,
            processing_queue_size: 20.0,
            dlq_size: 5.0,
            active_workers: 10.0,
        };

        let slightly_different = CurrentMetrics {
            tasks_enqueued: 1005.0,
            tasks_completed: 903.0,
            tasks_failed: 102.0,
            tasks_retried: 51.0,
            tasks_cancelled: 10.0,
            queue_size: 101.0,
            processing_queue_size: 20.0,
            dlq_size: 5.0,
            active_workers: 10.0,
        };

        let comparison = MetricComparison::compare(&baseline, &slightly_different);

        // Change is not significant (< 5%)
        assert!(!comparison.is_significant(5.0));
        // But is significant for smaller threshold
        assert!(comparison.is_significant(0.1));
    }

    #[test]
    #[serial]
    fn test_alert_rule_error_rate() {
        reset_metrics();

        TASKS_COMPLETED_TOTAL.inc_by(90.0);
        TASKS_FAILED_TOTAL.inc_by(10.0);

        let metrics = CurrentMetrics::capture();

        let rule = AlertRule::new(
            "high_error_rate",
            AlertCondition::ErrorRateAbove { threshold: 0.05 },
            AlertSeverity::Critical,
            "Error rate exceeded 5%",
        );

        // 10% error rate should fire alert (> 5%)
        assert!(rule.should_fire(&metrics));

        let rule2 = AlertRule::new(
            "acceptable_error_rate",
            AlertCondition::ErrorRateAbove { threshold: 0.15 },
            AlertSeverity::Warning,
            "Error rate exceeded 15%",
        );

        // 10% error rate should not fire alert (< 15%)
        assert!(!rule2.should_fire(&metrics));
    }

    #[test]
    #[serial]
    fn test_alert_rule_success_rate() {
        reset_metrics();

        TASKS_COMPLETED_TOTAL.inc_by(95.0);
        TASKS_FAILED_TOTAL.inc_by(5.0);

        let metrics = CurrentMetrics::capture();

        let rule = AlertRule::new(
            "low_success_rate",
            AlertCondition::SuccessRateBelow { threshold: 0.99 },
            AlertSeverity::Warning,
            "Success rate below 99%",
        );

        // 95% success rate should fire alert (< 99%)
        assert!(rule.should_fire(&metrics));
    }

    #[test]
    #[serial]
    fn test_alert_rule_queue_size() {
        reset_metrics();

        QUEUE_SIZE.set(1500.0);

        let metrics = CurrentMetrics::capture();

        let rule = AlertRule::new(
            "high_queue_size",
            AlertCondition::GaugeAbove { threshold: 1000.0 },
            AlertSeverity::Warning,
            "Queue size exceeded 1000",
        );

        assert!(rule.should_fire(&metrics));
    }

    #[test]
    #[serial]
    fn test_alert_rule_workers() {
        reset_metrics();

        ACTIVE_WORKERS.set(2.0);

        let metrics = CurrentMetrics::capture();

        let rule = AlertRule::new(
            "low_workers",
            AlertCondition::GaugeBelow { threshold: 5.0 },
            AlertSeverity::Critical,
            "Worker count below minimum",
        );

        assert!(rule.should_fire(&metrics));
    }

    #[test]
    #[serial]
    fn test_alert_manager() {
        reset_metrics();

        TASKS_COMPLETED_TOTAL.inc_by(90.0);
        TASKS_FAILED_TOTAL.inc_by(10.0);
        QUEUE_SIZE.set(1500.0);
        ACTIVE_WORKERS.set(3.0);

        let mut manager = AlertManager::new();

        manager.add_rule(AlertRule::new(
            "high_error_rate",
            AlertCondition::ErrorRateAbove { threshold: 0.05 },
            AlertSeverity::Critical,
            "Error rate exceeded 5%",
        ));

        manager.add_rule(AlertRule::new(
            "high_queue_size",
            AlertCondition::GaugeAbove { threshold: 1000.0 },
            AlertSeverity::Warning,
            "Queue size exceeded 1000",
        ));

        manager.add_rule(AlertRule::new(
            "low_workers",
            AlertCondition::GaugeBelow { threshold: 5.0 },
            AlertSeverity::Critical,
            "Worker count below minimum",
        ));

        let metrics = CurrentMetrics::capture();
        let fired = manager.check_alerts(&metrics);

        // All 3 alerts should fire
        assert_eq!(fired.len(), 3);

        let critical = manager.critical_alerts(&metrics);
        // 2 critical alerts
        assert_eq!(critical.len(), 2);
    }

    #[test]
    #[serial]
    fn test_metric_summary() {
        reset_metrics();

        TASKS_ENQUEUED_TOTAL.inc_by(100.0);
        TASKS_COMPLETED_TOTAL.inc_by(90.0);
        TASKS_FAILED_TOTAL.inc_by(10.0);
        QUEUE_SIZE.set(50.0);
        ACTIVE_WORKERS.set(5.0);

        let summary = generate_metric_summary();

        assert!(summary.contains("CeleRS Metrics Summary"));
        assert!(summary.contains("100"));
        assert!(summary.contains("90"));
        assert!(summary.contains("10"));
        assert!(summary.contains("50"));
        assert!(summary.contains("5"));
    }
}
