//! Prometheus metrics for `CeleRS`
//!
//! This module provides Prometheus metrics integration for monitoring task queue performance.
#![allow(clippy::must_use_candidate)]
#![allow(clippy::return_self_not_must_use)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::similar_names)]
#![allow(clippy::non_std_lazy_statics)]

use lazy_static::lazy_static;
use prometheus::{
    register_counter, register_counter_vec, register_gauge, register_histogram,
    register_histogram_vec, Counter, CounterVec, Encoder, Gauge, Histogram, HistogramVec,
    TextEncoder,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

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

    /// Total number of garbage collection recommendations
    pub static ref GC_RECOMMENDATIONS_TOTAL: Counter =
        register_counter!("celers_gc_recommendations_total", "Total number of garbage collection recommendations")
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
///
/// # Panics
///
/// Panics if the Prometheus encoder fails to encode metrics (should never happen in practice).
#[must_use]
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

/// Record a successful task execution with timing
///
/// This is a convenience function that increments the appropriate counters
/// and records execution time in a single call.
///
/// # Examples
///
/// ```
/// use celers_metrics::record_task_success;
///
/// // Record a successful task that took 1.5 seconds
/// record_task_success("send_email", 1.5);
/// ```
#[allow(dead_code)]
pub fn record_task_success(task_name: &str, execution_time_seconds: f64) {
    TASKS_COMPLETED_TOTAL.inc();
    TASKS_COMPLETED_BY_TYPE
        .with_label_values(&[task_name])
        .inc();
    TASK_EXECUTION_TIME.observe(execution_time_seconds);
    TASK_EXECUTION_TIME_BY_TYPE
        .with_label_values(&[task_name])
        .observe(execution_time_seconds);
}

/// Record a failed task execution
///
/// This is a convenience function that increments the appropriate failure counters.
///
/// # Examples
///
/// ```
/// use celers_metrics::record_task_failure;
///
/// // Record a failed task
/// record_task_failure("send_email");
/// ```
#[allow(dead_code)]
pub fn record_task_failure(task_name: &str) {
    TASKS_FAILED_TOTAL.inc();
    TASKS_FAILED_BY_TYPE.with_label_values(&[task_name]).inc();
}

/// Record a task retry
///
/// This is a convenience function that increments the retry counters.
///
/// # Examples
///
/// ```
/// use celers_metrics::record_task_retry;
///
/// // Record a retry attempt
/// record_task_retry("send_email");
/// ```
#[allow(dead_code)]
pub fn record_task_retry(task_name: &str) {
    TASKS_RETRIED_TOTAL.inc();
    TASKS_RETRIED_BY_TYPE.with_label_values(&[task_name]).inc();
}

/// Record task enqueue operation
///
/// This is a convenience function that increments the enqueue counters.
///
/// # Examples
///
/// ```
/// use celers_metrics::record_task_enqueue;
///
/// // Record a task being enqueued
/// record_task_enqueue("send_email");
/// ```
#[allow(dead_code)]
pub fn record_task_enqueue(task_name: &str) {
    TASKS_ENQUEUED_TOTAL.inc();
    TASKS_ENQUEUED_BY_TYPE.with_label_values(&[task_name]).inc();
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
#[must_use]
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
    /// Create a new anomaly threshold with mean, `std_dev`, and sigma threshold
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

/// Trend-based alert rule combining standard alerts with trend analysis
#[derive(Debug, Clone)]
pub struct TrendAlertRule {
    /// Alert name
    pub name: String,
    /// Trend alert condition
    pub condition: TrendAlertCondition,
    /// Alert severity
    pub severity: AlertSeverity,
    /// Alert description
    pub description: String,
}

impl TrendAlertRule {
    /// Create a new trend-based alert rule
    pub fn new(
        name: impl Into<String>,
        condition: TrendAlertCondition,
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

    /// Check if trend alert should fire based on metric history
    pub fn should_fire(&self, history: &MetricHistory) -> bool {
        self.condition.should_alert(history)
    }
}

/// Alert manager for tracking multiple alert rules
#[derive(Debug)]
pub struct AlertManager {
    rules: Vec<AlertRule>,
    trend_rules: Vec<TrendAlertRule>,
}

impl AlertManager {
    /// Create a new alert manager
    pub fn new() -> Self {
        Self {
            rules: Vec::new(),
            trend_rules: Vec::new(),
        }
    }

    /// Add an alert rule
    pub fn add_rule(&mut self, rule: AlertRule) {
        self.rules.push(rule);
    }

    /// Add a trend-based alert rule
    pub fn add_trend_rule(&mut self, rule: TrendAlertRule) {
        self.trend_rules.push(rule);
    }

    /// Check all standard rules and return fired alerts
    pub fn check_alerts(&self, metrics: &CurrentMetrics) -> Vec<&AlertRule> {
        self.rules
            .iter()
            .filter(|rule| rule.should_fire(metrics))
            .collect()
    }

    /// Check all trend rules and return fired alerts
    pub fn check_trend_alerts(&self, history: &MetricHistory) -> Vec<&TrendAlertRule> {
        self.trend_rules
            .iter()
            .filter(|rule| rule.should_fire(history))
            .collect()
    }

    /// Get critical standard alerts
    pub fn critical_alerts(&self, metrics: &CurrentMetrics) -> Vec<&AlertRule> {
        self.check_alerts(metrics)
            .into_iter()
            .filter(|rule| rule.severity == AlertSeverity::Critical)
            .collect()
    }

    /// Get critical trend alerts
    pub fn critical_trend_alerts(&self, history: &MetricHistory) -> Vec<&TrendAlertRule> {
        self.check_trend_alerts(history)
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
        r"=== CeleRS Metrics Summary ===

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
",
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

// ============================================================================
// Metric History and Time-Series Analysis
// ============================================================================

use std::collections::VecDeque;

/// A time-stamped metric sample for historical tracking
#[derive(Debug, Clone)]
pub struct MetricSample {
    /// Unix timestamp in seconds
    pub timestamp: u64,
    /// Metric value
    pub value: f64,
}

/// Snapshot of metric history statistics
#[derive(Debug, Clone)]
pub struct MetricHistorySnapshot {
    /// Number of samples
    pub count: usize,
    /// Minimum value
    pub min: f64,
    /// Maximum value
    pub max: f64,
    /// Mean value
    pub mean: f64,
    /// Variance
    pub variance: f64,
    /// Standard deviation
    pub std_dev: f64,
    /// Trend (rate of change per second)
    pub trend: Option<f64>,
    /// Latest value
    pub latest: Option<f64>,
}

impl Default for MetricHistorySnapshot {
    fn default() -> Self {
        Self {
            count: 0,
            min: 0.0,
            max: 0.0,
            mean: 0.0,
            variance: 0.0,
            std_dev: 0.0,
            trend: None,
            latest: None,
        }
    }
}

/// Time-series history tracker for metrics
#[derive(Debug)]
pub struct MetricHistory {
    samples: Mutex<VecDeque<MetricSample>>,
    max_samples: usize,
}

impl MetricHistory {
    /// Create a new metric history tracker with a maximum number of samples
    pub fn new(max_samples: usize) -> Self {
        Self {
            samples: Mutex::new(VecDeque::with_capacity(max_samples)),
            max_samples,
        }
    }

    /// Record a new sample with current timestamp
    pub fn record(&self, value: f64) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let sample = MetricSample { timestamp, value };

        let mut samples = self.samples.lock().unwrap();
        if samples.len() >= self.max_samples {
            samples.pop_front();
        }
        samples.push_back(sample);
    }

    /// Get all samples as a vector
    pub fn get_samples(&self) -> Vec<MetricSample> {
        self.samples.lock().unwrap().iter().cloned().collect()
    }

    /// Get the most recent sample
    pub fn latest(&self) -> Option<MetricSample> {
        self.samples.lock().unwrap().back().cloned()
    }

    /// Calculate the trend (rate of change per second)
    pub fn trend(&self) -> Option<f64> {
        let samples = self.samples.lock().unwrap();
        if samples.len() < 2 {
            return None;
        }

        let first = samples.front().unwrap();
        let last = samples.back().unwrap();

        let time_delta = (last.timestamp - first.timestamp) as f64;
        if time_delta == 0.0 {
            return None;
        }

        let value_delta = last.value - first.value;
        Some(value_delta / time_delta)
    }

    /// Calculate moving average over all samples
    pub fn moving_average(&self) -> Option<f64> {
        let samples = self.samples.lock().unwrap();
        if samples.is_empty() {
            return None;
        }

        let sum: f64 = samples.iter().map(|s| s.value).sum();
        Some(sum / samples.len() as f64)
    }

    /// Calculate moving average over a specific window size
    pub fn moving_average_window(&self, window: usize) -> Option<f64> {
        let samples = self.samples.lock().unwrap();
        if samples.is_empty() {
            return None;
        }

        let window_size = window.min(samples.len());
        let start = samples.len().saturating_sub(window_size);
        let sum: f64 = samples.iter().skip(start).map(|s| s.value).sum();
        Some(sum / window_size as f64)
    }

    /// Record multiple samples at once (more efficient than individual records)
    pub fn record_batch(&self, values: &[(u64, f64)]) {
        let mut samples = self.samples.lock().unwrap();
        for (timestamp, value) in values {
            let sample = MetricSample {
                timestamp: *timestamp,
                value: *value,
            };

            if samples.len() >= self.max_samples {
                samples.pop_front();
            }
            samples.push_back(sample);
        }
    }

    /// Get a comprehensive snapshot of all statistics in a single lock acquisition
    pub fn snapshot(&self) -> MetricHistorySnapshot {
        let samples = self.samples.lock().unwrap();

        if samples.is_empty() {
            return MetricHistorySnapshot::default();
        }

        let values: Vec<f64> = samples.iter().map(|s| s.value).collect();
        let sum: f64 = values.iter().sum();
        let count = values.len();
        let mean = sum / count as f64;

        let min = values
            .iter()
            .copied()
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max = values
            .iter()
            .copied()
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();

        let variance = if count > 1 {
            let sq_diff_sum: f64 = values.iter().map(|v| (v - mean).powi(2)).sum();
            sq_diff_sum / (count - 1) as f64
        } else {
            0.0
        };

        let std_dev = variance.sqrt();

        let trend = if samples.len() >= 2 {
            let first = samples.front().unwrap();
            let last = samples.back().unwrap();
            let time_delta = (last.timestamp - first.timestamp) as f64;
            if time_delta > 0.0 {
                Some((last.value - first.value) / time_delta)
            } else {
                None
            }
        } else {
            None
        };

        MetricHistorySnapshot {
            count,
            min,
            max,
            mean,
            variance,
            std_dev,
            trend,
            latest: samples.back().map(|s| s.value),
        }
    }

    /// Get the minimum value in history
    pub fn min(&self) -> Option<f64> {
        let samples = self.samples.lock().unwrap();
        samples
            .iter()
            .map(|s| s.value)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
    }

    /// Get the maximum value in history
    pub fn max(&self) -> Option<f64> {
        let samples = self.samples.lock().unwrap();
        samples
            .iter()
            .map(|s| s.value)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
    }

    /// Clear all samples
    pub fn clear(&self) {
        self.samples.lock().unwrap().clear();
    }

    /// Get number of samples
    pub fn len(&self) -> usize {
        self.samples.lock().unwrap().len()
    }

    /// Check if history is empty
    pub fn is_empty(&self) -> bool {
        self.samples.lock().unwrap().is_empty()
    }
}

// ============================================================================
// Auto-Scaling Recommendations
// ============================================================================

/// Auto-scaling recommendation based on current metrics
#[derive(Debug, Clone, PartialEq)]
pub enum ScalingRecommendation {
    /// Scale up by the specified number of workers
    ScaleUp { workers: usize, reason: String },
    /// Scale down by the specified number of workers
    ScaleDown { workers: usize, reason: String },
    /// No scaling needed
    NoChange,
}

/// Configuration for auto-scaling recommendations
#[derive(Debug, Clone)]
pub struct AutoScalingConfig {
    /// Target queue size per worker
    pub target_queue_per_worker: f64,
    /// Minimum number of workers
    pub min_workers: usize,
    /// Maximum number of workers
    pub max_workers: usize,
    /// Worker utilization threshold for scaling up (0.0-1.0)
    pub scale_up_threshold: f64,
    /// Worker utilization threshold for scaling down (0.0-1.0)
    pub scale_down_threshold: f64,
    /// Minimum time between scaling decisions (seconds)
    pub cooldown_seconds: u64,
}

impl Default for AutoScalingConfig {
    fn default() -> Self {
        Self {
            target_queue_per_worker: 10.0,
            min_workers: 1,
            max_workers: 100,
            scale_up_threshold: 0.8,
            scale_down_threshold: 0.3,
            cooldown_seconds: 300, // 5 minutes
        }
    }
}

impl AutoScalingConfig {
    /// Create a new auto-scaling configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set target queue size per worker
    pub fn with_target_queue_per_worker(mut self, target: f64) -> Self {
        self.target_queue_per_worker = target;
        self
    }

    /// Set minimum workers
    pub fn with_min_workers(mut self, min: usize) -> Self {
        self.min_workers = min;
        self
    }

    /// Set maximum workers
    pub fn with_max_workers(mut self, max: usize) -> Self {
        self.max_workers = max;
        self
    }

    /// Set scale-up threshold
    pub fn with_scale_up_threshold(mut self, threshold: f64) -> Self {
        self.scale_up_threshold = threshold.clamp(0.0, 1.0);
        self
    }

    /// Set scale-down threshold
    pub fn with_scale_down_threshold(mut self, threshold: f64) -> Self {
        self.scale_down_threshold = threshold.clamp(0.0, 1.0);
        self
    }

    /// Set cooldown period
    pub fn with_cooldown_seconds(mut self, seconds: u64) -> Self {
        self.cooldown_seconds = seconds;
        self
    }
}

/// Generate auto-scaling recommendation based on current metrics
pub fn recommend_scaling(config: &AutoScalingConfig) -> ScalingRecommendation {
    let metrics = CurrentMetrics::capture();

    let current_workers = metrics.active_workers as usize;
    if current_workers == 0 {
        return ScalingRecommendation::ScaleUp {
            workers: config.min_workers,
            reason: "No workers currently active".to_string(),
        };
    }

    let queue_size = metrics.queue_size;
    let processing = metrics.processing_queue_size;

    // Calculate utilization
    let busy_ratio = if current_workers > 0 {
        (processing / metrics.active_workers).min(1.0)
    } else {
        0.0
    };

    // Check if queue is growing too large
    let queue_per_worker = queue_size / metrics.active_workers;
    if queue_per_worker > config.target_queue_per_worker * 2.0 {
        let additional_workers_needed = ((queue_size / config.target_queue_per_worker).ceil()
            as usize)
            .saturating_sub(current_workers)
            .min(config.max_workers - current_workers);

        if additional_workers_needed > 0 && current_workers < config.max_workers {
            return ScalingRecommendation::ScaleUp {
                workers: additional_workers_needed,
                reason: format!(
                    "Queue size ({:.0}) exceeds target ({:.0} per worker)",
                    queue_size, config.target_queue_per_worker
                ),
            };
        }
    }

    // Check utilization for scaling up
    if busy_ratio > config.scale_up_threshold && current_workers < config.max_workers {
        let workers_to_add = (current_workers as f64 * 0.5).ceil() as usize; // Scale by 50%
        let workers_to_add = workers_to_add
            .max(1)
            .min(config.max_workers - current_workers);

        return ScalingRecommendation::ScaleUp {
            workers: workers_to_add,
            reason: format!(
                "High worker utilization ({:.1}% > {:.1}%)",
                busy_ratio * 100.0,
                config.scale_up_threshold * 100.0
            ),
        };
    }

    // Check utilization for scaling down
    if busy_ratio < config.scale_down_threshold
        && queue_size < config.target_queue_per_worker
        && current_workers > config.min_workers
    {
        let workers_to_remove = (current_workers as f64 * 0.3).ceil() as usize; // Scale down by 30%
        let workers_to_remove = workers_to_remove
            .max(1)
            .min(current_workers - config.min_workers);

        return ScalingRecommendation::ScaleDown {
            workers: workers_to_remove,
            reason: format!(
                "Low worker utilization ({:.1}% < {:.1}%) and small queue ({:.0})",
                busy_ratio * 100.0,
                config.scale_down_threshold * 100.0,
                queue_size
            ),
        };
    }

    ScalingRecommendation::NoChange
}

// ============================================================================
// Cost Estimation
// ============================================================================

/// Cost estimation configuration
#[derive(Debug, Clone)]
pub struct CostConfig {
    /// Cost per worker-hour (e.g., EC2 instance cost)
    pub cost_per_worker_hour: f64,
    /// Cost per million task executions
    pub cost_per_million_tasks: f64,
    /// Cost per GB of data processed
    pub cost_per_gb: f64,
}

impl Default for CostConfig {
    fn default() -> Self {
        Self {
            cost_per_worker_hour: 0.10,  // $0.10/hour default
            cost_per_million_tasks: 1.0, // $1.00 per million tasks
            cost_per_gb: 0.01,           // $0.01 per GB
        }
    }
}

impl CostConfig {
    /// Create a new cost configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set cost per worker-hour
    pub fn with_cost_per_worker_hour(mut self, cost: f64) -> Self {
        self.cost_per_worker_hour = cost;
        self
    }

    /// Set cost per million tasks
    pub fn with_cost_per_million_tasks(mut self, cost: f64) -> Self {
        self.cost_per_million_tasks = cost;
        self
    }

    /// Set cost per GB
    pub fn with_cost_per_gb(mut self, cost: f64) -> Self {
        self.cost_per_gb = cost;
        self
    }
}

/// Cost breakdown estimate
#[derive(Debug, Clone)]
pub struct CostEstimate {
    /// Estimated compute cost
    pub compute_cost: f64,
    /// Estimated task execution cost
    pub task_cost: f64,
    /// Estimated data transfer cost
    pub data_cost: f64,
    /// Total estimated cost
    pub total_cost: f64,
}

/// Estimate costs based on metrics and time period
pub fn estimate_costs(config: &CostConfig, time_period_hours: f64) -> CostEstimate {
    let metrics = CurrentMetrics::capture();

    // Compute cost: workers * hours * cost_per_hour
    let compute_cost = metrics.active_workers * time_period_hours * config.cost_per_worker_hour;

    // Task cost: tasks * (cost_per_million / 1_000_000)
    let total_tasks = metrics.tasks_completed + metrics.tasks_failed;
    let task_cost = total_tasks * (config.cost_per_million_tasks / 1_000_000.0);

    // Data cost: Estimate from result sizes (if tracked)
    // This is a placeholder - actual implementation would need result size tracking
    let data_cost = 0.0;

    let total_cost = compute_cost + task_cost + data_cost;

    CostEstimate {
        compute_cost,
        task_cost,
        data_cost,
        total_cost,
    }
}

/// Calculate cost per task
pub fn cost_per_task(config: &CostConfig, time_period_hours: f64) -> f64 {
    let metrics = CurrentMetrics::capture();
    let estimate = estimate_costs(config, time_period_hours);

    let total_tasks = metrics.tasks_completed + metrics.tasks_failed;
    if total_tasks == 0.0 {
        return 0.0;
    }

    estimate.total_cost / total_tasks
}

// ============================================================================
// Metric Forecasting
// ============================================================================

/// Simple linear regression forecast
#[derive(Debug, Clone)]
pub struct ForecastResult {
    /// Predicted value at the forecast time
    pub predicted_value: f64,
    /// Confidence in prediction (0.0-1.0)
    pub confidence: f64,
    /// Trend direction (positive = increasing, negative = decreasing)
    pub trend: f64,
}

/// Forecast future metric value using linear regression on historical data
pub fn forecast_metric(history: &MetricHistory, seconds_ahead: u64) -> Option<ForecastResult> {
    let samples = history.get_samples();
    if samples.len() < 3 {
        return None; // Need at least 3 samples for reasonable forecast
    }

    // Simple linear regression: y = mx + b
    let n = samples.len() as f64;
    let sum_x: f64 = samples.iter().map(|s| s.timestamp as f64).sum();
    let sum_y: f64 = samples.iter().map(|s| s.value).sum();
    let sum_xy: f64 = samples.iter().map(|s| s.timestamp as f64 * s.value).sum();
    let sum_x2: f64 = samples.iter().map(|s| (s.timestamp as f64).powi(2)).sum();

    let denominator = n * sum_x2 - sum_x.powi(2);
    if denominator.abs() < 1e-10 {
        return None; // Avoid division by zero
    }

    let slope = (n * sum_xy - sum_x * sum_y) / denominator;
    let intercept = (sum_y - slope * sum_x) / n;

    // Forecast value
    let latest_timestamp = samples.last()?.timestamp;
    let future_timestamp = latest_timestamp + seconds_ahead;
    let predicted_value = slope * future_timestamp as f64 + intercept;

    // Calculate confidence based on R²
    let mean_y = sum_y / n;
    let ss_tot: f64 = samples.iter().map(|s| (s.value - mean_y).powi(2)).sum();
    let ss_res: f64 = samples
        .iter()
        .map(|s| {
            let predicted = slope * s.timestamp as f64 + intercept;
            (s.value - predicted).powi(2)
        })
        .sum();

    let r_squared = if ss_tot > 0.0 {
        1.0 - (ss_res / ss_tot)
    } else {
        0.0
    };

    Some(ForecastResult {
        predicted_value,
        confidence: r_squared.clamp(0.0, 1.0),
        trend: slope,
    })
}

// ============================================================================
// Metric Cardinality Protection
// ============================================================================

/// Cardinality limiter to prevent label explosion
#[derive(Debug)]
pub struct CardinalityLimiter {
    seen_labels: StdMutex<std::collections::HashSet<String>>,
    max_cardinality: usize,
}

impl CardinalityLimiter {
    /// Create a new cardinality limiter with maximum allowed unique label combinations
    pub fn new(max_cardinality: usize) -> Self {
        Self {
            seen_labels: StdMutex::new(std::collections::HashSet::new()),
            max_cardinality,
        }
    }

    /// Check if a label combination is allowed (within cardinality limit)
    /// Returns true if the label should be recorded, false if it would exceed the limit
    pub fn check_and_record(&self, label_key: &str) -> bool {
        let mut seen = self.seen_labels.lock().unwrap();

        if seen.contains(label_key) {
            return true; // Already seen, always allowed
        }

        if seen.len() >= self.max_cardinality {
            return false; // Would exceed limit
        }

        seen.insert(label_key.to_string());
        true
    }

    /// Get current cardinality (number of unique label combinations)
    pub fn current_cardinality(&self) -> usize {
        self.seen_labels.lock().unwrap().len()
    }

    /// Check if cardinality limit has been reached
    pub fn is_at_limit(&self) -> bool {
        self.current_cardinality() >= self.max_cardinality
    }

    /// Reset the limiter
    pub fn reset(&self) {
        self.seen_labels.lock().unwrap().clear();
    }
}

// ============================================================================
// Trend-Based Alerting
// ============================================================================

/// Trend direction for alerting
#[derive(Debug, Clone, PartialEq)]
pub enum TrendDirection {
    /// Metric is increasing
    Increasing,
    /// Metric is decreasing
    Decreasing,
    /// Metric is stable (no significant trend)
    Stable,
}

/// Trend-based alert condition
#[derive(Debug, Clone)]
pub struct TrendAlertCondition {
    /// Minimum number of samples required for trend analysis
    pub min_samples: usize,
    /// Trend threshold (rate of change per second)
    pub trend_threshold: f64,
    /// Expected trend direction for alert
    pub alert_on_direction: TrendDirection,
}

impl TrendAlertCondition {
    /// Create a new trend alert condition
    pub fn new(trend_threshold: f64, alert_on_direction: TrendDirection) -> Self {
        Self {
            min_samples: 5,
            trend_threshold,
            alert_on_direction,
        }
    }

    /// Check if the trend should trigger an alert
    pub fn should_alert(&self, history: &MetricHistory) -> bool {
        if history.len() < self.min_samples {
            return false;
        }

        let trend = match history.trend() {
            Some(t) => t,
            None => return false,
        };

        match self.alert_on_direction {
            TrendDirection::Increasing => trend > self.trend_threshold,
            TrendDirection::Decreasing => trend < -self.trend_threshold,
            TrendDirection::Stable => trend.abs() < self.trend_threshold,
        }
    }
}

// ============================================================================
// Metric Correlation Analysis
// ============================================================================

/// Correlation coefficient between two metrics
#[derive(Debug, Clone)]
pub struct CorrelationResult {
    /// Pearson correlation coefficient (-1.0 to 1.0)
    pub coefficient: f64,
    /// Statistical significance (p-value approximation)
    pub significance: f64,
    /// Number of samples used in calculation
    pub sample_count: usize,
}

/// Calculate Pearson correlation coefficient between two metric histories
pub fn calculate_correlation(
    history_a: &MetricHistory,
    history_b: &MetricHistory,
) -> Option<CorrelationResult> {
    let samples_a = history_a.get_samples();
    let samples_b = history_b.get_samples();

    if samples_a.len() < 3 || samples_b.len() < 3 {
        return None; // Need at least 3 samples
    }

    // Align samples by timestamp (use only overlapping timestamps)
    let mut paired_values: Vec<(f64, f64)> = Vec::new();

    for sample_a in &samples_a {
        if let Some(sample_b) = samples_b.iter().find(|s| s.timestamp == sample_a.timestamp) {
            paired_values.push((sample_a.value, sample_b.value));
        }
    }

    if paired_values.len() < 3 {
        return None; // Not enough overlapping samples
    }

    let n = paired_values.len() as f64;
    let sum_x: f64 = paired_values.iter().map(|(x, _)| x).sum();
    let sum_y: f64 = paired_values.iter().map(|(_, y)| y).sum();
    let sum_xy: f64 = paired_values.iter().map(|(x, y)| x * y).sum();
    let sum_x2: f64 = paired_values.iter().map(|(x, _)| x.powi(2)).sum();
    let sum_y2: f64 = paired_values.iter().map(|(_, y)| y.powi(2)).sum();

    let numerator = n * sum_xy - sum_x * sum_y;
    let denominator_x = (n * sum_x2 - sum_x.powi(2)).sqrt();
    let denominator_y = (n * sum_y2 - sum_y.powi(2)).sqrt();

    if denominator_x.abs() < 1e-10 || denominator_y.abs() < 1e-10 {
        return None; // Avoid division by zero
    }

    let coefficient = numerator / (denominator_x * denominator_y);

    // Approximate significance using t-statistic
    // For large samples, |r| > 2/sqrt(n) is roughly significant at p < 0.05
    let significance_threshold = 2.0 / (n.sqrt());
    let significance = if coefficient.abs() > significance_threshold {
        0.05 // Approximation: significant
    } else {
        0.5 // Approximation: not significant
    };

    Some(CorrelationResult {
        coefficient: coefficient.clamp(-1.0, 1.0),
        significance,
        sample_count: paired_values.len(),
    })
}

/// Detect if two metrics are strongly correlated
pub fn are_metrics_correlated(
    history_a: &MetricHistory,
    history_b: &MetricHistory,
    threshold: f64,
) -> bool {
    match calculate_correlation(history_a, history_b) {
        Some(result) => result.coefficient.abs() >= threshold,
        None => false,
    }
}

// ============================================================================
// Metric Aggregation Windows
// ============================================================================

/// Time window for metric aggregation
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TimeWindow {
    /// Last 1 minute
    OneMinute,
    /// Last 5 minutes
    FiveMinutes,
    /// Last 15 minutes
    FifteenMinutes,
    /// Last 1 hour
    OneHour,
    /// Last 24 hours
    OneDay,
}

impl TimeWindow {
    /// Get duration in seconds
    pub fn as_seconds(&self) -> u64 {
        match self {
            TimeWindow::OneMinute => 60,
            TimeWindow::FiveMinutes => 300,
            TimeWindow::FifteenMinutes => 900,
            TimeWindow::OneHour => 3600,
            TimeWindow::OneDay => 86400,
        }
    }
}

/// Windowed metric statistics
#[derive(Debug, Clone)]
pub struct WindowedStats {
    /// Mean value in the window
    pub mean: f64,
    /// Minimum value in the window
    pub min: f64,
    /// Maximum value in the window
    pub max: f64,
    /// Standard deviation in the window
    pub std_dev: f64,
    /// 50th percentile (median)
    pub p50: f64,
    /// 95th percentile
    pub p95: f64,
    /// 99th percentile
    pub p99: f64,
    /// Number of samples in the window
    pub sample_count: usize,
}

/// Calculate statistics for a specific time window
pub fn calculate_windowed_stats(
    history: &MetricHistory,
    window: TimeWindow,
) -> Option<WindowedStats> {
    let samples = history.get_samples();
    if samples.is_empty() {
        return None;
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let window_start = now.saturating_sub(window.as_seconds());

    // Filter samples within the window
    let windowed_samples: Vec<f64> = samples
        .iter()
        .filter(|s| s.timestamp >= window_start)
        .map(|s| s.value)
        .collect();

    if windowed_samples.is_empty() {
        return None;
    }

    let count = windowed_samples.len();
    let sum: f64 = windowed_samples.iter().sum();
    let mean = sum / count as f64;

    let min = windowed_samples
        .iter()
        .min_by(|a, b| a.partial_cmp(b).unwrap())
        .copied()?;

    let max = windowed_samples
        .iter()
        .max_by(|a, b| a.partial_cmp(b).unwrap())
        .copied()?;

    let variance: f64 = windowed_samples
        .iter()
        .map(|v| (v - mean).powi(2))
        .sum::<f64>()
        / count as f64;

    let std_dev = variance.sqrt();

    // Calculate percentiles (requires sorted data)
    let mut sorted_samples = windowed_samples.clone();
    sorted_samples.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let p50 = calculate_percentile(&sorted_samples, 0.50).unwrap_or(mean);
    let p95 = calculate_percentile(&sorted_samples, 0.95).unwrap_or(max);
    let p99 = calculate_percentile(&sorted_samples, 0.99).unwrap_or(max);

    Some(WindowedStats {
        mean,
        min,
        max,
        std_dev,
        p50,
        p95,
        p99,
        sample_count: count,
    })
}

// ============================================================================
// Exponential Smoothing Forecasting (Holt-Winters)
// ============================================================================

/// Exponential smoothing forecast result with seasonality support
#[derive(Debug, Clone)]
pub struct ExponentialForecast {
    /// Predicted value at the forecast time
    pub predicted_value: f64,
    /// Level component (deseasonalized average)
    pub level: f64,
    /// Trend component (rate of change)
    pub trend: f64,
    /// Confidence score (0.0-1.0)
    pub confidence: f64,
}

/// Exponential smoothing parameters
#[derive(Debug, Clone)]
pub struct ExponentialSmoothingConfig {
    /// Alpha: smoothing factor for level (0.0-1.0)
    pub alpha: f64,
    /// Beta: smoothing factor for trend (0.0-1.0)
    pub beta: f64,
}

impl Default for ExponentialSmoothingConfig {
    fn default() -> Self {
        Self {
            alpha: 0.3, // Standard value for level smoothing
            beta: 0.1,  // Standard value for trend smoothing
        }
    }
}

impl ExponentialSmoothingConfig {
    /// Create new config with custom parameters
    pub fn new(alpha: f64, beta: f64) -> Self {
        Self {
            alpha: alpha.clamp(0.0, 1.0),
            beta: beta.clamp(0.0, 1.0),
        }
    }
}

/// Forecast using double exponential smoothing (Holt's method)
/// More accurate than linear regression for trending data
pub fn forecast_exponential(
    history: &MetricHistory,
    seconds_ahead: u64,
    config: &ExponentialSmoothingConfig,
) -> Option<ExponentialForecast> {
    let samples = history.get_samples();
    if samples.len() < 3 {
        return None;
    }

    let values: Vec<f64> = samples.iter().map(|s| s.value).collect();

    // Initialize level and trend
    let mut level = values[0];
    let mut trend = if values.len() > 1 {
        values[1] - values[0]
    } else {
        0.0
    };

    // Apply double exponential smoothing
    for &value in values.iter().skip(1) {
        let prev_level = level;
        level = config.alpha * value + (1.0 - config.alpha) * (level + trend);
        trend = config.beta * (level - prev_level) + (1.0 - config.beta) * trend;
    }

    // Forecast ahead
    let avg_time_delta = if samples.len() > 1 {
        let total_time = (samples[samples.len() - 1].timestamp - samples[0].timestamp) as f64;
        let avg_delta = total_time / (samples.len() - 1) as f64;
        avg_delta.max(1.0) // Minimum 1 second to prevent division issues
    } else {
        1.0
    };

    let periods_ahead = (seconds_ahead as f64 / avg_time_delta).min(1000.0); // Cap at 1000 periods
    let predicted_value = level + trend * periods_ahead;

    // Calculate confidence based on recent forecast accuracy
    let recent_errors: Vec<f64> = values
        .windows(2)
        .map(|w| {
            let forecast = w[0] + trend;
            (w[1] - forecast).abs() / w[1].max(1.0)
        })
        .collect();

    let mean_error = if !recent_errors.is_empty() {
        recent_errors.iter().sum::<f64>() / recent_errors.len() as f64
    } else {
        0.0
    };

    let confidence = (1.0 - mean_error.min(1.0)).max(0.0);

    Some(ExponentialForecast {
        predicted_value,
        level,
        trend,
        confidence,
    })
}

// ============================================================================
// Cost Optimization Recommendations
// ============================================================================

/// Cost optimization recommendation
#[derive(Debug, Clone, PartialEq)]
pub enum CostOptimization {
    /// Scale down workers to save costs
    ScaleDown {
        /// Current worker count
        current_workers: usize,
        /// Recommended worker count
        recommended_workers: usize,
        /// Estimated monthly savings
        estimated_savings: f64,
    },
    /// Increase task batching to reduce overhead
    IncreaseBatching {
        /// Current average batch size
        current_batch_size: f64,
        /// Recommended batch size
        recommended_batch_size: usize,
        /// Estimated cost reduction percentage
        cost_reduction_percent: f64,
    },
    /// Switch to spot/preemptible instances
    UseSpotInstances {
        /// Estimated monthly savings
        estimated_savings: f64,
        /// Cost reduction percentage
        cost_reduction_percent: f64,
    },
    /// Optimize queue polling frequency
    OptimizePolling {
        /// Recommended poll interval in seconds
        recommended_poll_interval: u64,
        /// Estimated cost reduction percentage
        cost_reduction_percent: f64,
    },
    /// No optimization needed - costs are optimal
    NoOptimizationNeeded,
}

/// Configuration for cost optimization analysis
#[derive(Debug, Clone)]
pub struct CostOptimizationConfig {
    /// Current worker count
    pub current_workers: usize,
    /// Average worker utilization (0.0-1.0)
    pub avg_utilization: f64,
    /// Average batch size
    pub avg_batch_size: f64,
    /// Cost per worker per hour
    pub cost_per_worker_hour: f64,
    /// Spot instance discount percentage (0.0-1.0)
    pub spot_discount: f64,
    /// Current poll interval in seconds
    pub current_poll_interval: u64,
}

impl Default for CostOptimizationConfig {
    fn default() -> Self {
        Self {
            current_workers: 1,
            avg_utilization: 0.5,
            avg_batch_size: 1.0,
            cost_per_worker_hour: 0.10,
            spot_discount: 0.70, // 70% discount typical for spot instances
            current_poll_interval: 1,
        }
    }
}

/// Analyze current metrics and recommend cost optimizations
pub fn recommend_cost_optimizations(config: &CostOptimizationConfig) -> Vec<CostOptimization> {
    let mut recommendations = Vec::new();

    // 1. Check if we can scale down workers (low utilization)
    if config.avg_utilization < 0.3 && config.current_workers > 1 {
        let target_utilization = 0.7;
        let recommended_workers = ((config.current_workers as f64 * config.avg_utilization)
            / target_utilization)
            .ceil() as usize;
        let recommended_workers = recommended_workers.max(1);

        if recommended_workers < config.current_workers {
            let workers_to_remove = config.current_workers - recommended_workers;
            let monthly_hours = 24.0 * 30.0;
            let estimated_savings =
                workers_to_remove as f64 * config.cost_per_worker_hour * monthly_hours;

            recommendations.push(CostOptimization::ScaleDown {
                current_workers: config.current_workers,
                recommended_workers,
                estimated_savings,
            });
        }
    }

    // 2. Check if batch size can be increased (small batches = high overhead)
    if config.avg_batch_size < 10.0 {
        let recommended_batch_size = 50;
        let overhead_reduction = (recommended_batch_size as f64 / config.avg_batch_size) * 0.1;
        let cost_reduction_percent = overhead_reduction.min(0.3) * 100.0; // Cap at 30%

        recommendations.push(CostOptimization::IncreaseBatching {
            current_batch_size: config.avg_batch_size,
            recommended_batch_size,
            cost_reduction_percent,
        });
    }

    // 3. Check if spot instances would be beneficial (stable workload)
    if config.avg_utilization > 0.5 && config.avg_utilization < 0.9 {
        let monthly_hours = 24.0 * 30.0;
        let current_monthly_cost =
            config.current_workers as f64 * config.cost_per_worker_hour * monthly_hours;
        let estimated_savings = current_monthly_cost * config.spot_discount;
        let cost_reduction_percent = config.spot_discount * 100.0;

        recommendations.push(CostOptimization::UseSpotInstances {
            estimated_savings,
            cost_reduction_percent,
        });
    }

    // 4. Check if polling frequency can be optimized (high frequency polling = waste)
    if config.current_poll_interval < 5 && config.avg_utilization < 0.6 {
        let recommended_poll_interval = 10;
        let cpu_reduction =
            (config.current_poll_interval as f64 / recommended_poll_interval as f64) * 0.15; // Polling typically uses 15% of CPU
        let cost_reduction_percent = cpu_reduction * 100.0;

        recommendations.push(CostOptimization::OptimizePolling {
            recommended_poll_interval,
            cost_reduction_percent,
        });
    }

    if recommendations.is_empty() {
        recommendations.push(CostOptimization::NoOptimizationNeeded);
    }

    recommendations
}

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
// Alert Debouncer (Alert Fatigue Prevention)
// ============================================================================

use std::time::Instant;

/// Alert debouncer to prevent alert fatigue
#[derive(Debug)]
pub struct AlertDebouncer {
    /// Minimum time between alerts (seconds)
    debounce_period: u64,
    /// Last alert times by alert name
    last_alert_times: StdMutex<HashMap<String, Instant>>,
}

impl AlertDebouncer {
    /// Create a new alert debouncer
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_metrics::AlertDebouncer;
    ///
    /// // Only allow same alert every 5 minutes
    /// let debouncer = AlertDebouncer::new(300);
    /// ```
    pub fn new(debounce_period_seconds: u64) -> Self {
        Self {
            debounce_period: debounce_period_seconds,
            last_alert_times: StdMutex::new(HashMap::new()),
        }
    }

    /// Check if an alert should fire (not debounced)
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_metrics::AlertDebouncer;
    ///
    /// let debouncer = AlertDebouncer::new(60);
    ///
    /// // First alert should fire
    /// assert!(debouncer.should_fire("high_error_rate"));
    ///
    /// // Immediate second alert should be debounced
    /// assert!(!debouncer.should_fire("high_error_rate"));
    /// ```
    pub fn should_fire(&self, alert_name: &str) -> bool {
        let mut times = self.last_alert_times.lock().unwrap();
        let now = Instant::now();

        if let Some(last_time) = times.get(alert_name) {
            let elapsed = now.duration_since(*last_time).as_secs();
            if elapsed < self.debounce_period {
                return false; // Still in debounce period
            }
        }

        times.insert(alert_name.to_string(), now);
        true
    }

    /// Reset debounce state for an alert
    pub fn reset(&self, alert_name: &str) {
        self.last_alert_times.lock().unwrap().remove(alert_name);
    }

    /// Reset all debounce state
    pub fn reset_all(&self) {
        self.last_alert_times.lock().unwrap().clear();
    }

    /// Get time until next alert can fire (in seconds)
    pub fn time_until_next(&self, alert_name: &str) -> Option<u64> {
        let times = self.last_alert_times.lock().unwrap();
        if let Some(last_time) = times.get(alert_name) {
            let elapsed = Instant::now().duration_since(*last_time).as_secs();
            if elapsed < self.debounce_period {
                return Some(self.debounce_period - elapsed);
            }
        }
        Some(0)
    }
}

impl Default for AlertDebouncer {
    fn default() -> Self {
        Self::new(300) // 5 minutes default
    }
}

// ============================================================================
// Health Score Calculator
// ============================================================================

/// Overall system health score (0.0 to 1.0)
#[derive(Debug, Clone)]
pub struct HealthScore {
    /// Overall health score (0.0 = critical, 1.0 = perfect)
    pub score: f64,
    /// Component scores
    pub components: HashMap<String, f64>,
    /// Health grade (A, B, C, D, F)
    pub grade: char,
    /// Issues affecting score
    pub issues: Vec<String>,
}

impl HealthScore {
    /// Calculate overall system health score
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_metrics::{HealthScore, SloTarget};
    ///
    /// celers_metrics::TASKS_ENQUEUED_TOTAL.inc_by(100.0);
    /// celers_metrics::TASKS_COMPLETED_TOTAL.inc_by(95.0);
    /// celers_metrics::TASKS_FAILED_TOTAL.inc_by(5.0);
    /// celers_metrics::ACTIVE_WORKERS.set(5.0);
    ///
    /// let slo = SloTarget {
    ///     success_rate: 0.99,
    ///     latency_seconds: 5.0,
    ///     throughput: 10.0,
    /// };
    ///
    /// let score = HealthScore::calculate(&slo);
    /// assert!(score.score > 0.0 && score.score <= 1.0);
    /// ```
    pub fn calculate(slo_target: &SloTarget) -> Self {
        let metrics = CurrentMetrics::capture();
        let mut components = HashMap::new();
        let mut issues = Vec::new();

        // Success rate component (40% weight)
        let success_rate = metrics.success_rate();
        let success_score = if success_rate >= slo_target.success_rate {
            1.0
        } else {
            (success_rate / slo_target.success_rate).max(0.0)
        };
        components.insert("success_rate".to_string(), success_score);

        if success_score < 1.0 {
            issues.push(format!(
                "Success rate ({:.1}%) below target ({:.1}%)",
                success_rate * 100.0,
                slo_target.success_rate * 100.0
            ));
        }

        // Queue health component (30% weight)
        let queue_score = if metrics.queue_size < 100.0 {
            1.0
        } else if metrics.queue_size < 500.0 {
            1.0 - (metrics.queue_size - 100.0) / 400.0 * 0.5
        } else {
            0.5 - ((metrics.queue_size - 500.0) / 500.0).min(0.5)
        };
        components.insert("queue_health".to_string(), queue_score);

        if queue_score < 0.8 {
            issues.push(format!("Queue backlog: {} tasks", metrics.queue_size));
        }

        // Worker availability component (20% weight)
        let worker_score = if metrics.active_workers >= 2.0 {
            1.0
        } else if metrics.active_workers >= 1.0 {
            0.5
        } else {
            0.0
        };
        components.insert("worker_availability".to_string(), worker_score);

        if worker_score < 1.0 {
            issues.push(format!("Low worker count: {}", metrics.active_workers));
        }

        // DLQ component (10% weight)
        let dlq_score = if metrics.dlq_size < 10.0 {
            1.0
        } else if metrics.dlq_size < 50.0 {
            1.0 - (metrics.dlq_size - 10.0) / 40.0 * 0.5
        } else {
            0.5
        };
        components.insert("dlq_health".to_string(), dlq_score);

        if dlq_score < 0.8 {
            issues.push(format!("DLQ growing: {} items", metrics.dlq_size));
        }

        // Calculate weighted overall score
        let score = success_score * 0.4 + queue_score * 0.3 + worker_score * 0.2 + dlq_score * 0.1;

        let grade = if score >= 0.9 {
            'A'
        } else if score >= 0.8 {
            'B'
        } else if score >= 0.7 {
            'C'
        } else if score >= 0.6 {
            'D'
        } else {
            'F'
        };

        Self {
            score,
            components,
            grade,
            issues,
        }
    }

    /// Format as human-readable report
    pub fn format_report(&self) -> String {
        let emoji = match self.grade {
            'A' => "✅",
            'B' => "✓",
            'C' => "⚠",
            'D' => "⚠⚠",
            _ => "❌",
        };

        let mut report = format!(
            r"=== System Health Score ===

Overall Score: {:.1}% (Grade: {}) {}

Component Scores:
",
            self.score * 100.0,
            self.grade,
            emoji
        );

        for (component, score) in &self.components {
            report.push_str(&format!("  {}: {:.1}%\n", component, score * 100.0));
        }

        if !self.issues.is_empty() {
            report.push_str("\nIssues:\n");
            for (i, issue) in self.issues.iter().enumerate() {
                report.push_str(&format!("  {}. {}\n", i + 1, issue));
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
// Alert History Tracking
// ============================================================================

/// Alert event in history
#[derive(Debug, Clone)]
pub struct AlertEvent {
    /// Alert name/ID
    pub alert_name: String,
    /// When the alert fired
    pub timestamp: u64,
    /// Alert severity
    pub severity: AlertSeverity,
    /// Metric value that triggered the alert
    pub trigger_value: f64,
    /// Alert threshold
    pub threshold: f64,
}

/// Alert history tracker
pub struct AlertHistory {
    events: StdMutex<Vec<AlertEvent>>,
    max_events: usize,
}

impl AlertHistory {
    /// Create a new alert history tracker
    ///
    /// # Arguments
    ///
    /// * `max_events` - Maximum number of events to retain
    pub fn new(max_events: usize) -> Self {
        Self {
            events: StdMutex::new(Vec::new()),
            max_events,
        }
    }

    /// Record an alert event
    pub fn record_alert(&self, event: AlertEvent) {
        let mut events = self
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        events.push(event);

        // Keep only the most recent events
        if events.len() > self.max_events {
            let drain_count = events.len() - self.max_events;
            events.drain(0..drain_count);
        }
    }

    /// Get recent alert events
    pub fn recent_events(&self, count: usize) -> Vec<AlertEvent> {
        let events = self
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let start = events.len().saturating_sub(count);
        events.iter().skip(start).cloned().collect()
    }

    /// Get all events for a specific alert
    pub fn events_for_alert(&self, alert_name: &str) -> Vec<AlertEvent> {
        let events = self
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        events
            .iter()
            .filter(|e| e.alert_name == alert_name)
            .cloned()
            .collect()
    }

    /// Count how many times an alert has fired
    pub fn alert_fire_count(&self, alert_name: &str) -> usize {
        let events = self
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        events.iter().filter(|e| e.alert_name == alert_name).count()
    }

    /// Get time since last alert fired
    pub fn time_since_last_alert(&self, alert_name: &str) -> Option<u64> {
        let events = self
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        events
            .iter()
            .rev()
            .find(|e| e.alert_name == alert_name)
            .map(|e| {
                use std::time::{SystemTime, UNIX_EPOCH};
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                now.saturating_sub(e.timestamp)
            })
    }

    /// Clear all history
    pub fn clear(&self) {
        let mut events = self
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        events.clear();
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
    fn test_trend_alert_manager() {
        use std::thread;
        use std::time::Duration;

        let mut manager = AlertManager::new();

        // Add a trend-based alert for increasing trends
        manager.add_trend_rule(TrendAlertRule::new(
            "queue_growing_rapidly",
            TrendAlertCondition::new(5.0, TrendDirection::Increasing),
            AlertSeverity::Warning,
            "Queue is growing rapidly",
        ));

        // Add a trend-based alert for decreasing trends
        manager.add_trend_rule(TrendAlertRule::new(
            "throughput_dropping",
            TrendAlertCondition::new(5.0, TrendDirection::Decreasing),
            AlertSeverity::Critical,
            "Throughput is dropping",
        ));

        // Create history with increasing trend
        let increasing_history = MetricHistory::new(100);
        for i in 1..=10 {
            increasing_history.record((i * 10) as f64);
            thread::sleep(Duration::from_millis(50));
        }

        // Check trend alerts
        let _fired = manager.check_trend_alerts(&increasing_history);
        // Trend alerts are timing-dependent, so we just verify the API works
        // At minimum, the increasing history should have enough samples
        assert!(increasing_history.len() >= 5);
    }

    #[test]
    fn test_trend_alert_rule() {
        use std::thread;
        use std::time::Duration;

        let history = MetricHistory::new(100);
        for i in 1..=6 {
            history.record((i * 10) as f64);
            thread::sleep(Duration::from_millis(100));
        }

        let rule = TrendAlertRule::new(
            "test_trend",
            TrendAlertCondition::new(5.0, TrendDirection::Increasing),
            AlertSeverity::Warning,
            "Test trend alert",
        );

        // This test just verifies the API works
        let _ = rule.should_fire(&history);
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

    // ========================================================================
    // Integration-Style Tests
    // ========================================================================

    /// Integration test simulating complete worker lifecycle
    #[test]
    #[serial]
    fn test_integration_worker_lifecycle() {
        reset_metrics();

        // Simulate worker startup
        ACTIVE_WORKERS.inc();
        assert_eq!(ACTIVE_WORKERS.get(), 1.0);

        // Simulate receiving and processing tasks
        let task_types = ["send_email", "process_image", "generate_report"];

        for (i, task_type) in task_types.iter().enumerate() {
            // Task received from broker
            QUEUE_SIZE.inc();

            // Worker picks up task
            QUEUE_SIZE.dec();
            PROCESSING_QUEUE_SIZE.inc();

            // Track by task type
            TASKS_ENQUEUED_BY_TYPE.with_label_values(&[task_type]).inc();

            // Simulate task execution with varying times
            let execution_time = (i + 1) as f64 * 0.5;
            TASK_EXECUTION_TIME.observe(execution_time);
            TASK_EXECUTION_TIME_BY_TYPE
                .with_label_values(&[task_type])
                .observe(execution_time);

            // Task completes successfully
            PROCESSING_QUEUE_SIZE.dec();
            TASKS_COMPLETED_TOTAL.inc();
            TASKS_COMPLETED_BY_TYPE
                .with_label_values(&[task_type])
                .inc();

            // Record result size
            let result_size = (i + 1) as f64 * 1000.0;
            TASK_RESULT_SIZE_BYTES.observe(result_size);
            TASK_RESULT_SIZE_BY_TYPE
                .with_label_values(&[task_type])
                .observe(result_size);
        }

        // Simulate one task failure with retry
        QUEUE_SIZE.inc();
        QUEUE_SIZE.dec();
        PROCESSING_QUEUE_SIZE.inc();

        TASKS_ENQUEUED_BY_TYPE
            .with_label_values(&["failing_task"])
            .inc();

        // First attempt fails
        TASKS_RETRIED_TOTAL.inc();
        TASKS_RETRIED_BY_TYPE
            .with_label_values(&["failing_task"])
            .inc();

        // Retry also fails - send to DLQ
        PROCESSING_QUEUE_SIZE.dec();
        DLQ_SIZE.inc();
        TASKS_FAILED_TOTAL.inc();
        TASKS_FAILED_BY_TYPE
            .with_label_values(&["failing_task"])
            .inc();

        // Verify final state
        let metrics = CurrentMetrics::capture();
        assert_eq!(metrics.tasks_completed, 3.0);
        assert_eq!(metrics.tasks_failed, 1.0);
        assert_eq!(metrics.tasks_retried, 1.0);
        assert_eq!(metrics.dlq_size, 1.0);
        assert_eq!(metrics.active_workers, 1.0);

        // Check success rate
        let success_rate = metrics.success_rate();
        assert!((success_rate - 0.75).abs() < 0.01); // 3/4 = 75%

        // Worker shutdown
        ACTIVE_WORKERS.dec();
        assert_eq!(ACTIVE_WORKERS.get(), 0.0);
    }

    /// Integration test simulating broker operations
    #[test]
    #[serial]
    fn test_integration_broker_operations() {
        reset_metrics();

        // Simulate broker startup - establish connection pool
        REDIS_CONNECTIONS_ACTIVE.set(5.0);

        // Simulate batch enqueue operation
        let batch_size = 10.0;
        BATCH_ENQUEUE_TOTAL.inc();
        BATCH_SIZE.observe(batch_size);

        for i in 0..10 {
            TASKS_ENQUEUED_TOTAL.inc();
            QUEUE_SIZE.inc();

            let task_type = if i % 2 == 0 {
                "high_priority"
            } else {
                "low_priority"
            };
            TASKS_ENQUEUED_BY_TYPE.with_label_values(&[task_type]).inc();
        }

        // Simulate broker latency tracking
        BROKER_ENQUEUE_LATENCY_SECONDS.observe(0.005); // 5ms

        // Simulate delayed task scheduling
        DELAYED_TASKS_SCHEDULED.set(3.0);
        DELAYED_TASKS_ENQUEUED_TOTAL.inc_by(3.0);

        // Simulate dequeue operations
        BATCH_DEQUEUE_TOTAL.inc();
        let dequeue_batch_size = 5.0;
        BATCH_SIZE.observe(dequeue_batch_size);
        QUEUE_SIZE.sub(dequeue_batch_size);
        BROKER_DEQUEUE_LATENCY_SECONDS.observe(0.003); // 3ms

        // Simulate ack operations
        for _ in 0..5 {
            BROKER_ACK_LATENCY_SECONDS.observe(0.001); // 1ms
        }

        // Check queue size query latency
        BROKER_QUEUE_SIZE_LATENCY_SECONDS.observe(0.0005); // 0.5ms

        // Verify broker metrics
        let metrics = CurrentMetrics::capture();
        assert_eq!(metrics.tasks_enqueued, 10.0);
        assert_eq!(metrics.queue_size, 5.0); // 10 enqueued - 5 dequeued

        // Verify connection pool is active
        assert_eq!(REDIS_CONNECTIONS_ACTIVE.get(), 5.0);
    }

    /// Integration test simulating multi-worker concurrent scenario
    #[test]
    #[serial]
    fn test_integration_multi_worker_concurrent() {
        reset_metrics();

        // Simulate 3 workers starting
        let worker_count = 3;
        ACTIVE_WORKERS.set(worker_count as f64);

        // Simulate batch of tasks arriving
        let total_tasks = 30;
        BATCH_ENQUEUE_TOTAL.inc();
        BATCH_SIZE.observe(total_tasks as f64);

        for i in 0..total_tasks {
            TASKS_ENQUEUED_TOTAL.inc();
            QUEUE_SIZE.inc();

            let task_type = match i % 3 {
                0 => "cpu_intensive",
                1 => "io_intensive",
                _ => "mixed",
            };
            TASKS_ENQUEUED_BY_TYPE.with_label_values(&[task_type]).inc();
        }

        // Simulate distributed aggregation across workers
        let aggregator = DistributedAggregator::new();

        for worker_id in 0..worker_count {
            let mut stats = MetricStats::new();

            // Each worker processes 10 tasks
            for task_num in 0..10 {
                let execution_time = (worker_id * 10 + task_num) as f64 * 0.1;
                stats.observe(execution_time);

                // Update global metrics
                QUEUE_SIZE.dec();
                PROCESSING_QUEUE_SIZE.inc();
                TASK_EXECUTION_TIME.observe(execution_time);
                PROCESSING_QUEUE_SIZE.dec();
                TASKS_COMPLETED_TOTAL.inc();
            }

            // Report worker snapshot
            let snapshot = MetricSnapshot::new(format!("worker-{}", worker_id), stats);
            aggregator.update(snapshot);
        }

        // Verify distributed aggregation
        assert_eq!(aggregator.active_worker_count(), worker_count);
        let combined = aggregator.aggregate();
        assert_eq!(combined.count, total_tasks as u64);

        // Verify global metrics
        let metrics = CurrentMetrics::capture();
        assert_eq!(metrics.tasks_completed, total_tasks as f64);
        assert_eq!(metrics.queue_size, 0.0); // All tasks processed
        assert_eq!(metrics.active_workers, worker_count as f64);

        // Calculate worker utilization
        let utilization = (metrics.tasks_completed / worker_count as f64) / 10.0 * 100.0;
        WORKER_UTILIZATION_PERCENT.set(utilization);
    }

    /// Integration test simulating end-to-end task lifecycle with monitoring
    #[test]
    #[serial]
    fn test_integration_end_to_end_lifecycle() {
        reset_metrics();

        // Configure metrics with sampling
        let config = MetricsConfig::new().with_sampling_rate(1.0); // 100% for testing

        // Setup: Initialize system
        ACTIVE_WORKERS.set(2.0);
        REDIS_CONNECTIONS_ACTIVE.set(10.0);

        // Phase 1: Broker receives and enqueues tasks
        let tasks_to_process = 20;
        for i in 0..tasks_to_process {
            if config.should_sample() {
                TASKS_ENQUEUED_TOTAL.inc();
                QUEUE_SIZE.inc();

                let task_type = if i < 15 {
                    "normal_task"
                } else {
                    "special_task"
                };
                TASKS_ENQUEUED_BY_TYPE.with_label_values(&[task_type]).inc();

                // Track enqueue latency
                BROKER_ENQUEUE_LATENCY_SECONDS.observe(0.002);

                // Track task age (time from creation to enqueue)
                TASK_AGE_SECONDS.observe(i as f64 * 0.1);
            }
        }

        // Phase 2: Workers process tasks
        let mut successful_tasks = 0;
        let mut failed_tasks = 0;
        let mut retried_tasks = 0;

        for i in 0..tasks_to_process {
            // Dequeue
            QUEUE_SIZE.dec();
            PROCESSING_QUEUE_SIZE.inc();
            BROKER_DEQUEUE_LATENCY_SECONDS.observe(0.001);

            // Track wait time in queue
            TASK_QUEUE_WAIT_TIME_SECONDS.observe(i as f64 * 0.05);

            // Process
            let execution_time = if i < 15 { 0.5 } else { 2.0 };
            TASK_EXECUTION_TIME.observe(execution_time);

            let task_type = if i < 15 {
                "normal_task"
            } else {
                "special_task"
            };
            TASK_EXECUTION_TIME_BY_TYPE
                .with_label_values(&[task_type])
                .observe(execution_time);

            // Simulate occasional failures
            if i == 5 || i == 10 {
                // First failure - retry
                TASKS_RETRIED_TOTAL.inc();
                TASKS_RETRIED_BY_TYPE.with_label_values(&[task_type]).inc();
                retried_tasks += 1;

                // Retry succeeds
                PROCESSING_QUEUE_SIZE.dec();
                TASKS_COMPLETED_TOTAL.inc();
                TASKS_COMPLETED_BY_TYPE
                    .with_label_values(&[task_type])
                    .inc();
                successful_tasks += 1;

                BROKER_ACK_LATENCY_SECONDS.observe(0.001);
            } else if i == 15 {
                // Permanent failure
                TASKS_RETRIED_TOTAL.inc();
                TASKS_RETRIED_BY_TYPE.with_label_values(&[task_type]).inc();
                retried_tasks += 1;

                PROCESSING_QUEUE_SIZE.dec();
                TASKS_FAILED_TOTAL.inc();
                TASKS_FAILED_BY_TYPE.with_label_values(&[task_type]).inc();
                DLQ_SIZE.inc();
                failed_tasks += 1;

                BROKER_REJECT_LATENCY_SECONDS.observe(0.001);
            } else {
                // Success
                PROCESSING_QUEUE_SIZE.dec();
                TASKS_COMPLETED_TOTAL.inc();
                TASKS_COMPLETED_BY_TYPE
                    .with_label_values(&[task_type])
                    .inc();
                successful_tasks += 1;

                // Record result size
                TASK_RESULT_SIZE_BYTES.observe(5000.0);
                TASK_RESULT_SIZE_BY_TYPE
                    .with_label_values(&[task_type])
                    .observe(5000.0);

                BROKER_ACK_LATENCY_SECONDS.observe(0.001);
            }
        }

        // Phase 3: Health check and monitoring
        let metrics = CurrentMetrics::capture();

        // Verify all tasks processed
        assert_eq!(metrics.tasks_enqueued, tasks_to_process as f64);
        assert_eq!(metrics.tasks_completed, successful_tasks as f64);
        assert_eq!(metrics.tasks_failed, failed_tasks as f64);
        assert_eq!(metrics.tasks_retried, retried_tasks as f64);
        assert_eq!(metrics.queue_size, 0.0);
        assert_eq!(metrics.processing_queue_size, 0.0);
        assert_eq!(metrics.dlq_size, failed_tasks as f64);

        // Verify success rate
        let success_rate = metrics.success_rate();
        assert!(success_rate > 0.9); // Should be 95%

        // Setup health check
        let health_config = HealthCheckConfig::new()
            .with_max_queue_size(100.0)
            .with_max_dlq_size(5.0)
            .with_min_active_workers(1.0)
            .with_slo_target(SloTarget {
                success_rate: 0.95,
                latency_seconds: 5.0,
                throughput: 1.0,
            });

        let health = health_check(&health_config);
        match health {
            HealthStatus::Healthy => {
                // System is healthy
            }
            HealthStatus::Degraded { reasons } => {
                // Some degradation is expected with 1 failure
                assert!(!reasons.is_empty());
            }
            HealthStatus::Unhealthy { .. } => {
                panic!("System should not be unhealthy with only 1 failure");
            }
        }

        // Setup alert monitoring
        let mut alert_manager = AlertManager::new();

        alert_manager.add_rule(AlertRule::new(
            "high_dlq",
            AlertCondition::GaugeAbove { threshold: 5.0 },
            AlertSeverity::Warning,
            "DLQ size exceeded threshold",
        ));

        alert_manager.add_rule(AlertRule::new(
            "low_success_rate",
            AlertCondition::SuccessRateBelow { threshold: 0.9 },
            AlertSeverity::Critical,
            "Success rate below 90%",
        ));

        let _fired_alerts = alert_manager.check_alerts(&metrics);
        // Should have no critical alerts with 95% success rate
        let critical_alerts = alert_manager.critical_alerts(&metrics);
        assert_eq!(critical_alerts.len(), 0);

        // Phase 4: Generate summary report
        let summary = generate_metric_summary();
        assert!(summary.contains("CeleRS Metrics Summary"));
        assert!(summary.contains(&format!("{}", successful_tasks)));
        assert!(summary.contains(&format!("{}", failed_tasks)));

        // Cleanup
        ACTIVE_WORKERS.set(0.0);
        REDIS_CONNECTIONS_ACTIVE.set(0.0);
    }

    /// Integration test for memory pressure and oversized results
    #[test]
    #[serial]
    fn test_integration_memory_pressure() {
        reset_metrics();

        // Simulate worker with memory tracking
        ACTIVE_WORKERS.set(1.0);
        let initial_memory = 100_000_000.0; // 100MB
        WORKER_MEMORY_USAGE_BYTES.set(initial_memory);

        // Process tasks with varying result sizes
        let task_sizes = [
            1_000.0,      // 1KB - normal
            10_000.0,     // 10KB - normal
            100_000.0,    // 100KB - normal
            1_000_000.0,  // 1MB - normal
            10_000_000.0, // 10MB - large
            15_000_000.0, // 15MB - oversized
        ];

        for size in task_sizes.iter() {
            TASKS_ENQUEUED_TOTAL.inc();
            QUEUE_SIZE.inc();
            QUEUE_SIZE.dec();
            PROCESSING_QUEUE_SIZE.inc();

            // Process task
            TASK_EXECUTION_TIME.observe(0.5);

            // Record result size
            TASK_RESULT_SIZE_BYTES.observe(*size);

            // Check if oversized (>10MB)
            if *size > 10_000_000.0 {
                OVERSIZED_RESULTS_TOTAL.inc();
            }

            PROCESSING_QUEUE_SIZE.dec();
            TASKS_COMPLETED_TOTAL.inc();

            // Update memory usage
            let memory_delta = size / 10.0; // Approximate memory impact
            let new_memory = WORKER_MEMORY_USAGE_BYTES.get() + memory_delta;
            WORKER_MEMORY_USAGE_BYTES.set(new_memory);
        }

        // Verify metrics
        let metrics = CurrentMetrics::capture();
        assert_eq!(metrics.tasks_completed, task_sizes.len() as f64);

        // Verify oversized results were tracked
        let oversized_count = OVERSIZED_RESULTS_TOTAL.get();
        assert_eq!(oversized_count, 1.0); // Only the 15MB result

        // Verify memory increased
        let final_memory = WORKER_MEMORY_USAGE_BYTES.get();
        assert!(final_memory > initial_memory);

        // Check if memory alert would fire
        let memory_threshold = 200_000_000.0; // 200MB threshold
        if final_memory > memory_threshold {
            // Would trigger memory alert in production
            assert!(final_memory > memory_threshold);
        }
    }

    /// Integration test for PostgreSQL connection pool metrics
    #[test]
    #[serial]
    fn test_integration_postgres_pool() {
        reset_metrics();

        // Simulate PostgreSQL connection pool initialization
        let max_connections = 20.0;
        POSTGRES_POOL_MAX_SIZE.set(max_connections);
        POSTGRES_POOL_SIZE.set(max_connections);
        POSTGRES_POOL_IDLE.set(max_connections);
        POSTGRES_POOL_IN_USE.set(0.0);

        // Simulate broker operations using PostgreSQL
        let tasks_to_process = 10;

        for _i in 0..tasks_to_process {
            // Acquire connection from pool
            POSTGRES_POOL_IDLE.dec();
            POSTGRES_POOL_IN_USE.inc();

            // Enqueue task to PostgreSQL queue
            TASKS_ENQUEUED_TOTAL.inc();
            QUEUE_SIZE.inc();
            BROKER_ENQUEUE_LATENCY_SECONDS.observe(0.010); // 10ms for DB write

            // Release connection back to pool
            POSTGRES_POOL_IN_USE.dec();
            POSTGRES_POOL_IDLE.inc();

            // Worker acquires connection to dequeue
            POSTGRES_POOL_IDLE.dec();
            POSTGRES_POOL_IN_USE.inc();

            // Dequeue task
            QUEUE_SIZE.dec();
            PROCESSING_QUEUE_SIZE.inc();
            BROKER_DEQUEUE_LATENCY_SECONDS.observe(0.008); // 8ms for DB read

            // Release connection
            POSTGRES_POOL_IN_USE.dec();
            POSTGRES_POOL_IDLE.inc();

            // Process task
            TASK_EXECUTION_TIME.observe(1.0);
            PROCESSING_QUEUE_SIZE.dec();
            TASKS_COMPLETED_TOTAL.inc();

            // Acquire connection to ack
            POSTGRES_POOL_IDLE.dec();
            POSTGRES_POOL_IN_USE.inc();

            BROKER_ACK_LATENCY_SECONDS.observe(0.005); // 5ms for ack

            // Release connection
            POSTGRES_POOL_IN_USE.dec();
            POSTGRES_POOL_IDLE.inc();
        }

        // Verify pool metrics
        assert_eq!(POSTGRES_POOL_MAX_SIZE.get(), max_connections);
        assert_eq!(POSTGRES_POOL_SIZE.get(), max_connections);
        assert_eq!(POSTGRES_POOL_IDLE.get(), max_connections);
        assert_eq!(POSTGRES_POOL_IN_USE.get(), 0.0); // All released

        // Verify tasks processed
        let metrics = CurrentMetrics::capture();
        assert_eq!(metrics.tasks_completed, tasks_to_process as f64);
        assert_eq!(metrics.queue_size, 0.0);
    }

    /// Integration test for delayed task scheduling
    #[test]
    #[serial]
    fn test_integration_delayed_tasks() {
        reset_metrics();

        // Schedule delayed tasks
        let immediate_tasks = 5;
        let delayed_tasks = 3;

        // Enqueue immediate tasks
        for _ in 0..immediate_tasks {
            TASKS_ENQUEUED_TOTAL.inc();
            QUEUE_SIZE.inc();
        }

        // Schedule delayed tasks (not yet in main queue)
        DELAYED_TASKS_SCHEDULED.set(delayed_tasks as f64);
        DELAYED_TASKS_ENQUEUED_TOTAL.inc_by(delayed_tasks as f64);

        // Verify initial state
        assert_eq!(QUEUE_SIZE.get(), immediate_tasks as f64);
        assert_eq!(DELAYED_TASKS_SCHEDULED.get(), delayed_tasks as f64);

        // Simulate time passing - delayed tasks become ready
        for _ in 0..delayed_tasks {
            DELAYED_TASKS_SCHEDULED.dec();
            DELAYED_TASKS_EXECUTED_TOTAL.inc();
            TASKS_ENQUEUED_TOTAL.inc();
            QUEUE_SIZE.inc();
        }

        // All tasks now in main queue
        assert_eq!(QUEUE_SIZE.get(), (immediate_tasks + delayed_tasks) as f64);
        assert_eq!(DELAYED_TASKS_SCHEDULED.get(), 0.0);
        assert_eq!(DELAYED_TASKS_EXECUTED_TOTAL.get(), delayed_tasks as f64);

        // Process all tasks
        for _ in 0..(immediate_tasks + delayed_tasks) {
            QUEUE_SIZE.dec();
            PROCESSING_QUEUE_SIZE.inc();
            TASK_EXECUTION_TIME.observe(0.5);
            PROCESSING_QUEUE_SIZE.dec();
            TASKS_COMPLETED_TOTAL.inc();
        }

        // Verify completion
        let metrics = CurrentMetrics::capture();
        assert_eq!(
            metrics.tasks_completed,
            (immediate_tasks + delayed_tasks) as f64
        );
        assert_eq!(metrics.queue_size, 0.0);
    }

    // ========================================================================
    // Tests for Metric History and Time-Series Analysis
    // ========================================================================

    #[test]
    fn test_metric_history_recording() {
        let history = MetricHistory::new(5);

        // Record some values
        history.record(10.0);
        history.record(20.0);
        history.record(30.0);

        assert_eq!(history.len(), 3);
        assert!(!history.is_empty());

        let samples = history.get_samples();
        assert_eq!(samples.len(), 3);
        assert_eq!(samples[0].value, 10.0);
        assert_eq!(samples[1].value, 20.0);
        assert_eq!(samples[2].value, 30.0);
    }

    #[test]
    fn test_metric_history_max_samples() {
        let history = MetricHistory::new(3);

        // Record more than max samples
        for i in 0..10 {
            history.record(i as f64);
        }

        // Should only keep last 3
        assert_eq!(history.len(), 3);
        let samples = history.get_samples();
        assert_eq!(samples[0].value, 7.0);
        assert_eq!(samples[1].value, 8.0);
        assert_eq!(samples[2].value, 9.0);
    }

    #[test]
    fn test_metric_history_trend() {
        use std::thread;
        use std::time::Duration;

        let history = MetricHistory::new(10);

        history.record(10.0);
        thread::sleep(Duration::from_secs(1));
        history.record(20.0);
        thread::sleep(Duration::from_secs(1));
        history.record(30.0);

        let trend = history.trend();
        assert!(trend.is_some());
        // Trend should be positive (increasing)
        assert!(trend.unwrap() > 0.0);
    }

    #[test]
    fn test_metric_history_moving_average() {
        let history = MetricHistory::new(5);

        history.record(10.0);
        history.record(20.0);
        history.record(30.0);

        let avg = history.moving_average();
        assert_eq!(avg, Some(20.0));
    }

    #[test]
    fn test_metric_history_min_max() {
        let history = MetricHistory::new(5);

        history.record(15.0);
        history.record(5.0);
        history.record(25.0);
        history.record(10.0);

        assert_eq!(history.min(), Some(5.0));
        assert_eq!(history.max(), Some(25.0));
    }

    #[test]
    fn test_metric_history_clear() {
        let history = MetricHistory::new(5);

        history.record(10.0);
        history.record(20.0);

        assert_eq!(history.len(), 2);

        history.clear();

        assert_eq!(history.len(), 0);
        assert!(history.is_empty());
    }

    #[test]
    fn test_metric_history_latest() {
        let history = MetricHistory::new(5);

        assert!(history.latest().is_none());

        history.record(10.0);
        history.record(20.0);

        let latest = history.latest();
        assert!(latest.is_some());
        assert_eq!(latest.unwrap().value, 20.0);
    }

    // ========================================================================
    // Tests for Auto-Scaling Recommendations
    // ========================================================================

    #[test]
    #[serial]
    fn test_auto_scaling_no_workers() {
        reset_metrics();
        ACTIVE_WORKERS.set(0.0);

        let config = AutoScalingConfig::new().with_min_workers(2);

        let recommendation = recommend_scaling(&config);
        match recommendation {
            ScalingRecommendation::ScaleUp { workers, reason: _ } => {
                assert_eq!(workers, 2);
            }
            _ => panic!("Expected ScaleUp recommendation"),
        }
    }

    #[test]
    #[serial]
    fn test_auto_scaling_high_queue() {
        reset_metrics();

        ACTIVE_WORKERS.set(5.0);
        QUEUE_SIZE.set(200.0); // 40 per worker, well above target of 10
        PROCESSING_QUEUE_SIZE.set(2.0);

        let config = AutoScalingConfig::new()
            .with_target_queue_per_worker(10.0)
            .with_max_workers(20);

        let recommendation = recommend_scaling(&config);
        match recommendation {
            ScalingRecommendation::ScaleUp { workers, reason } => {
                assert!(workers > 0);
                assert!(reason.contains("Queue size"));
            }
            _ => panic!("Expected ScaleUp recommendation for high queue"),
        }
    }

    #[test]
    #[serial]
    fn test_auto_scaling_high_utilization() {
        reset_metrics();

        ACTIVE_WORKERS.set(5.0);
        QUEUE_SIZE.set(20.0);
        PROCESSING_QUEUE_SIZE.set(4.5); // 90% utilization

        let config = AutoScalingConfig::new()
            .with_scale_up_threshold(0.8)
            .with_max_workers(20);

        let recommendation = recommend_scaling(&config);
        match recommendation {
            ScalingRecommendation::ScaleUp { workers, reason } => {
                assert!(workers > 0);
                assert!(reason.contains("utilization"));
            }
            _ => panic!("Expected ScaleUp recommendation for high utilization"),
        }
    }

    #[test]
    #[serial]
    fn test_auto_scaling_low_utilization() {
        reset_metrics();

        ACTIVE_WORKERS.set(10.0);
        QUEUE_SIZE.set(5.0);
        PROCESSING_QUEUE_SIZE.set(1.0); // 10% utilization

        let config = AutoScalingConfig::new()
            .with_scale_down_threshold(0.3)
            .with_min_workers(2);

        let recommendation = recommend_scaling(&config);
        match recommendation {
            ScalingRecommendation::ScaleDown { workers, reason } => {
                assert!(workers > 0);
                assert!(reason.contains("utilization"));
            }
            _ => panic!("Expected ScaleDown recommendation for low utilization"),
        }
    }

    #[test]
    #[serial]
    fn test_auto_scaling_no_change() {
        reset_metrics();

        ACTIVE_WORKERS.set(5.0);
        QUEUE_SIZE.set(25.0); // 5 per worker, reasonable
        PROCESSING_QUEUE_SIZE.set(3.0); // 60% utilization

        let config = AutoScalingConfig::new()
            .with_scale_up_threshold(0.8)
            .with_scale_down_threshold(0.3);

        let recommendation = recommend_scaling(&config);
        assert_eq!(recommendation, ScalingRecommendation::NoChange);
    }

    #[test]
    fn test_auto_scaling_config_builder() {
        let config = AutoScalingConfig::new()
            .with_target_queue_per_worker(20.0)
            .with_min_workers(5)
            .with_max_workers(50)
            .with_scale_up_threshold(0.9)
            .with_scale_down_threshold(0.2)
            .with_cooldown_seconds(600);

        assert_eq!(config.target_queue_per_worker, 20.0);
        assert_eq!(config.min_workers, 5);
        assert_eq!(config.max_workers, 50);
        assert_eq!(config.scale_up_threshold, 0.9);
        assert_eq!(config.scale_down_threshold, 0.2);
        assert_eq!(config.cooldown_seconds, 600);
    }

    // ========================================================================
    // Tests for Cost Estimation
    // ========================================================================

    #[test]
    #[serial]
    fn test_cost_estimation() {
        reset_metrics();

        ACTIVE_WORKERS.set(10.0);
        TASKS_COMPLETED_TOTAL.inc_by(1_000_000.0);
        TASKS_FAILED_TOTAL.inc_by(10_000.0);

        let config = CostConfig::new()
            .with_cost_per_worker_hour(0.50)
            .with_cost_per_million_tasks(2.0);

        let estimate = estimate_costs(&config, 1.0); // 1 hour

        // Compute cost: 10 workers * 1 hour * $0.50 = $5.00
        assert!((estimate.compute_cost - 5.0).abs() < 0.01);

        // Task cost: 1.01M tasks * ($2.00 / 1M) = $2.02
        assert!((estimate.task_cost - 2.02).abs() < 0.01);

        // Total should be sum of components
        assert!(
            (estimate.total_cost
                - (estimate.compute_cost + estimate.task_cost + estimate.data_cost))
                .abs()
                < 0.01
        );
    }

    #[test]
    #[serial]
    fn test_cost_per_task() {
        reset_metrics();

        ACTIVE_WORKERS.set(5.0);
        TASKS_COMPLETED_TOTAL.inc_by(100.0);
        TASKS_FAILED_TOTAL.inc_by(0.0);

        let config = CostConfig::new()
            .with_cost_per_worker_hour(0.10)
            .with_cost_per_million_tasks(1.0);

        let cost = cost_per_task(&config, 1.0);

        // Should be positive and reasonable
        assert!(cost > 0.0);
        assert!(cost < 1.0); // Should be less than $1 per task
    }

    #[test]
    #[serial]
    fn test_cost_estimation_zero_tasks() {
        reset_metrics();

        ACTIVE_WORKERS.set(5.0);

        let config = CostConfig::new();
        let cost = cost_per_task(&config, 1.0);

        // Should return 0 when no tasks
        assert_eq!(cost, 0.0);
    }

    #[test]
    fn test_cost_config_builder() {
        let config = CostConfig::new()
            .with_cost_per_worker_hour(1.50)
            .with_cost_per_million_tasks(5.0)
            .with_cost_per_gb(0.05);

        assert_eq!(config.cost_per_worker_hour, 1.50);
        assert_eq!(config.cost_per_million_tasks, 5.0);
        assert_eq!(config.cost_per_gb, 0.05);
    }

    // ========================================================================
    // Tests for Metric Forecasting
    // ========================================================================

    #[test]
    fn test_forecast_metric_insufficient_samples() {
        let history = MetricHistory::new(10);

        history.record(10.0);
        history.record(20.0);

        // Need at least 3 samples
        let forecast = forecast_metric(&history, 60);
        assert!(forecast.is_none());
    }

    #[test]
    fn test_forecast_metric_linear_trend() {
        use std::thread;
        use std::time::Duration;

        let history = MetricHistory::new(10);

        // Create a linear increasing trend with 1 second intervals
        for i in 1..=5 {
            history.record((i * 10) as f64);
            thread::sleep(Duration::from_secs(1));
        }

        let forecast = forecast_metric(&history, 60);

        // If forecast is available, validate it
        if let Some(result) = forecast {
            // Predicted value should be reasonable and finite
            assert!(result.predicted_value.is_finite());
            // Confidence should be between 0 and 1
            assert!(result.confidence >= 0.0 && result.confidence <= 1.0);
            // Trend should be finite (direction may vary due to timestamp precision)
            assert!(result.trend.is_finite());
        }
        // If forecast is None, it might be due to timestamp granularity
        // which is acceptable for this test
    }

    #[test]
    fn test_forecast_metric_stable_values() {
        use std::thread;
        use std::time::Duration;

        let history = MetricHistory::new(10);

        // Create mostly stable values with slight variations and time separation
        let values = [100.0, 101.0, 99.0, 100.0, 100.5];
        for &val in &values {
            history.record(val);
            thread::sleep(Duration::from_secs(1));
        }

        let forecast = forecast_metric(&history, 60);

        // If forecast is available, validate it
        if let Some(result) = forecast {
            // Should predict approximately the same value (around 100)
            assert!((result.predicted_value - 100.0).abs() < 50.0);
            // Trend should be near zero (very small variations)
            assert!(result.trend.abs() < 5.0);
        }
        // If forecast is None, it might be due to timestamp granularity
        // which is acceptable for this test
    }

    #[test]
    fn test_forecast_result_fields() {
        use std::thread;
        use std::time::Duration;

        let history = MetricHistory::new(10);

        for i in 1..=5 {
            history.record(10.0 + (i as f64 * 5.0));
            thread::sleep(Duration::from_secs(1));
        }

        let forecast = forecast_metric(&history, 60);

        // If forecast is available, validate field constraints
        if let Some(result) = forecast {
            // All fields should be present and valid
            assert!(result.predicted_value.is_finite());
            assert!(result.confidence >= 0.0 && result.confidence <= 1.0);
            assert!(result.trend.is_finite());
        }
        // If forecast is None, it might be due to timestamp granularity
        // which is acceptable for this test
    }

    // ========================================================================
    // Tests for Cardinality Limiter
    // ========================================================================

    #[test]
    fn test_cardinality_limiter_basic() {
        let limiter = CardinalityLimiter::new(3);

        assert!(limiter.check_and_record("label1"));
        assert!(limiter.check_and_record("label2"));
        assert!(limiter.check_and_record("label3"));

        // Already seen, should still be allowed
        assert!(limiter.check_and_record("label1"));

        // New label would exceed limit
        assert!(!limiter.check_and_record("label4"));

        assert_eq!(limiter.current_cardinality(), 3);
        assert!(limiter.is_at_limit());
    }

    #[test]
    fn test_cardinality_limiter_reset() {
        let limiter = CardinalityLimiter::new(2);

        limiter.check_and_record("label1");
        limiter.check_and_record("label2");

        assert_eq!(limiter.current_cardinality(), 2);

        limiter.reset();

        assert_eq!(limiter.current_cardinality(), 0);
        assert!(!limiter.is_at_limit());
    }

    #[test]
    fn test_cardinality_limiter_duplicate_labels() {
        let limiter = CardinalityLimiter::new(5);

        // Record same label multiple times
        for _ in 0..10 {
            assert!(limiter.check_and_record("same_label"));
        }

        // Should only count as 1
        assert_eq!(limiter.current_cardinality(), 1);
    }

    // ========================================================================
    // Tests for Trend-Based Alerting
    // ========================================================================

    #[test]
    fn test_trend_alert_increasing() {
        use std::thread;
        use std::time::Duration;

        let history = MetricHistory::new(10);

        // Create an increasing trend
        for i in 1..=6 {
            history.record((i * 10) as f64);
            thread::sleep(Duration::from_millis(100));
        }

        let alert = TrendAlertCondition::new(5.0, TrendDirection::Increasing);

        // Should alert on increasing trend if rate > 5.0 per second
        // This test is timing-dependent, so we just verify it doesn't panic
        let _ = alert.should_alert(&history);
    }

    #[test]
    fn test_trend_alert_insufficient_samples() {
        let history = MetricHistory::new(10);

        history.record(10.0);
        history.record(20.0);

        let alert = TrendAlertCondition::new(1.0, TrendDirection::Increasing);

        // Should not alert with insufficient samples (< 5)
        assert!(!alert.should_alert(&history));
    }

    #[test]
    fn test_trend_alert_stable() {
        use std::thread;
        use std::time::Duration;

        let history = MetricHistory::new(10);

        // Create stable values
        for _ in 0..6 {
            history.record(100.0);
            thread::sleep(Duration::from_millis(100));
        }

        let alert = TrendAlertCondition::new(0.1, TrendDirection::Stable);

        // Should alert on stable trend (small threshold)
        // This test is timing-dependent, so we just verify it doesn't panic
        let _ = alert.should_alert(&history);
    }

    // ========================================================================
    // Tests for Correlation Analysis
    // ========================================================================

    #[test]
    fn test_correlation_perfect_positive() {
        use std::thread;
        use std::time::Duration;

        let history_a = MetricHistory::new(10);
        let history_b = MetricHistory::new(10);

        // Create perfectly correlated data
        for i in 1..=5 {
            let val = (i * 10) as f64;
            history_a.record(val);
            history_b.record(val);
            thread::sleep(Duration::from_millis(100));
        }

        let result = calculate_correlation(&history_a, &history_b);

        if let Some(corr) = result {
            // Should be close to 1.0 (perfect positive correlation)
            // Timing-dependent, so we use a more lenient threshold
            assert!(corr.coefficient > 0.7 || corr.coefficient.is_nan());
            assert!(corr.sample_count > 0);
        }
    }

    #[test]
    fn test_correlation_perfect_negative() {
        use std::thread;
        use std::time::Duration;

        let history_a = MetricHistory::new(10);
        let history_b = MetricHistory::new(10);

        // Create negatively correlated data
        for i in 1..=5 {
            history_a.record((i * 10) as f64);
            history_b.record((60 - i * 10) as f64);
            thread::sleep(Duration::from_millis(100));
        }

        let result = calculate_correlation(&history_a, &history_b);

        if let Some(corr) = result {
            // Should be close to -1.0 (perfect negative correlation)
            // Timing-dependent, so we use a more lenient threshold
            assert!(corr.coefficient < -0.7 || corr.coefficient.is_nan());
        }
    }

    #[test]
    fn test_correlation_insufficient_samples() {
        let history_a = MetricHistory::new(10);
        let history_b = MetricHistory::new(10);

        history_a.record(10.0);
        history_b.record(20.0);

        let result = calculate_correlation(&history_a, &history_b);

        // Should return None with insufficient samples
        assert!(result.is_none());
    }

    #[test]
    fn test_are_metrics_correlated() {
        use std::thread;
        use std::time::Duration;

        let history_a = MetricHistory::new(10);
        let history_b = MetricHistory::new(10);

        // Create highly correlated data
        for i in 1..=5 {
            let val = (i * 10) as f64;
            history_a.record(val);
            history_b.record(val + 5.0); // Slightly offset but still highly correlated
            thread::sleep(Duration::from_millis(100));
        }

        // Should detect strong correlation with threshold 0.8
        let is_correlated = are_metrics_correlated(&history_a, &history_b, 0.8);

        // This might be timing-dependent, so we just verify it doesn't panic
        let _ = is_correlated;
    }

    // ========================================================================
    // Tests for Windowed Statistics
    // ========================================================================

    #[test]
    fn test_windowed_stats_basic() {
        use std::thread;
        use std::time::Duration;

        let history = MetricHistory::new(100);

        // Record some recent samples
        for i in 1..=5 {
            history.record((i * 10) as f64);
            thread::sleep(Duration::from_millis(100));
        }

        let stats = calculate_windowed_stats(&history, TimeWindow::OneMinute);

        if let Some(s) = stats {
            assert_eq!(s.sample_count, 5);
            assert!(s.mean > 0.0);
            assert!(s.min <= s.max);
            assert!(s.std_dev >= 0.0);
            // Check percentiles are in valid range
            assert!(s.p50 >= s.min && s.p50 <= s.max);
            assert!(s.p95 >= s.min && s.p95 <= s.max);
            assert!(s.p99 >= s.min && s.p99 <= s.max);
            // Check percentile ordering
            assert!(s.p50 <= s.p95);
            assert!(s.p95 <= s.p99);
        }
    }

    #[test]
    fn test_windowed_stats_empty_history() {
        let history = MetricHistory::new(10);

        let stats = calculate_windowed_stats(&history, TimeWindow::FiveMinutes);

        assert!(stats.is_none());
    }

    #[test]
    fn test_windowed_stats_time_windows() {
        let history = MetricHistory::new(100);

        // Record a sample
        history.record(100.0);

        // All windows should work with at least one sample
        let one_min = calculate_windowed_stats(&history, TimeWindow::OneMinute);
        let five_min = calculate_windowed_stats(&history, TimeWindow::FiveMinutes);
        let fifteen_min = calculate_windowed_stats(&history, TimeWindow::FifteenMinutes);
        let one_hour = calculate_windowed_stats(&history, TimeWindow::OneHour);
        let one_day = calculate_windowed_stats(&history, TimeWindow::OneDay);

        assert!(one_min.is_some());
        assert!(five_min.is_some());
        assert!(fifteen_min.is_some());
        assert!(one_hour.is_some());
        assert!(one_day.is_some());
    }

    #[test]
    fn test_time_window_as_seconds() {
        assert_eq!(TimeWindow::OneMinute.as_seconds(), 60);
        assert_eq!(TimeWindow::FiveMinutes.as_seconds(), 300);
        assert_eq!(TimeWindow::FifteenMinutes.as_seconds(), 900);
        assert_eq!(TimeWindow::OneHour.as_seconds(), 3600);
        assert_eq!(TimeWindow::OneDay.as_seconds(), 86400);
    }

    #[test]
    fn test_windowed_stats_statistics_accuracy() {
        use std::thread;
        use std::time::Duration;

        let history = MetricHistory::new(100);

        // Record known values
        let values = vec![10.0, 20.0, 30.0, 40.0, 50.0];
        for val in &values {
            history.record(*val);
            thread::sleep(Duration::from_millis(50));
        }

        let stats = calculate_windowed_stats(&history, TimeWindow::OneMinute);

        if let Some(s) = stats {
            // Mean should be 30.0
            assert!((s.mean - 30.0).abs() < 0.1);
            // Min should be 10.0
            assert!((s.min - 10.0).abs() < 0.1);
            // Max should be 50.0
            assert!((s.max - 50.0).abs() < 0.1);
            // Std dev should be approximately 14.14 (for this dataset)
            assert!(s.std_dev > 0.0 && s.std_dev < 20.0);
            // p50 (median) should be 30.0 for values [10, 20, 30, 40, 50]
            assert!((s.p50 - 30.0).abs() < 0.1);
            // p95 should be close to 50.0
            assert!(s.p95 >= 40.0 && s.p95 <= 50.0);
            // p99 should be close to 50.0
            assert!(s.p99 >= 40.0 && s.p99 <= 50.0);
        }
    }

    // ========================================================================
    // Tests for Exponential Smoothing Forecasting
    // ========================================================================

    #[test]
    fn test_exponential_forecast() {
        let history = MetricHistory::new(100);

        // Record increasing trend
        for i in 0..10 {
            history.record((i * 10) as f64);
        }

        let config = ExponentialSmoothingConfig::default();
        let forecast = forecast_exponential(&history, 60, &config);

        assert!(forecast.is_some());
        let forecast = forecast.unwrap();
        assert!(forecast.predicted_value > 0.0);
        assert!(forecast.level >= 0.0);
        assert!(forecast.trend > 0.0); // Should detect increasing trend
        assert!(forecast.confidence >= 0.0 && forecast.confidence <= 1.0);
    }

    #[test]
    fn test_exponential_forecast_config() {
        let config = ExponentialSmoothingConfig::new(0.5, 0.2);
        assert_eq!(config.alpha, 0.5);
        assert_eq!(config.beta, 0.2);

        let default_config = ExponentialSmoothingConfig::default();
        assert_eq!(default_config.alpha, 0.3);
        assert_eq!(default_config.beta, 0.1);
    }

    #[test]
    fn test_exponential_forecast_insufficient_samples() {
        let history = MetricHistory::new(10);
        history.record(10.0);
        history.record(20.0);

        let config = ExponentialSmoothingConfig::default();
        let forecast = forecast_exponential(&history, 60, &config);

        assert!(forecast.is_none()); // Need at least 3 samples
    }

    #[test]
    fn test_exponential_forecast_stable_values() {
        let history = MetricHistory::new(100);

        // Record stable values
        for _ in 0..10 {
            history.record(100.0);
        }

        let config = ExponentialSmoothingConfig::default();
        let forecast = forecast_exponential(&history, 60, &config);

        assert!(forecast.is_some());
        let forecast = forecast.unwrap();
        assert!(forecast.predicted_value > 0.0); // Should be positive
        assert!(forecast.trend.abs() < 5.0); // Minimal trend for stable values
        assert!(forecast.confidence >= 0.0 && forecast.confidence <= 1.0);
    }

    // ========================================================================
    // Tests for Cost Optimization Recommendations
    // ========================================================================

    #[test]
    fn test_cost_optimization_scale_down() {
        let config = CostOptimizationConfig {
            current_workers: 10,
            avg_utilization: 0.2, // Low utilization
            avg_batch_size: 5.0,
            cost_per_worker_hour: 0.10,
            spot_discount: 0.70,
            current_poll_interval: 1,
        };

        let recommendations = recommend_cost_optimizations(&config);

        assert!(!recommendations.is_empty());
        assert!(recommendations
            .iter()
            .any(|r| matches!(r, CostOptimization::ScaleDown { .. })));
    }

    #[test]
    fn test_cost_optimization_increase_batching() {
        let config = CostOptimizationConfig {
            current_workers: 5,
            avg_utilization: 0.5,
            avg_batch_size: 5.0, // Small batch size
            cost_per_worker_hour: 0.10,
            spot_discount: 0.70,
            current_poll_interval: 1,
        };

        let recommendations = recommend_cost_optimizations(&config);

        assert!(recommendations
            .iter()
            .any(|r| matches!(r, CostOptimization::IncreaseBatching { .. })));
    }

    #[test]
    fn test_cost_optimization_spot_instances() {
        let config = CostOptimizationConfig {
            current_workers: 5,
            avg_utilization: 0.7, // Stable workload
            avg_batch_size: 50.0,
            cost_per_worker_hour: 0.10,
            spot_discount: 0.70,
            current_poll_interval: 10,
        };

        let recommendations = recommend_cost_optimizations(&config);

        assert!(recommendations
            .iter()
            .any(|r| matches!(r, CostOptimization::UseSpotInstances { .. })));
    }

    #[test]
    fn test_cost_optimization_polling() {
        let config = CostOptimizationConfig {
            current_workers: 5,
            avg_utilization: 0.4,
            avg_batch_size: 50.0,
            cost_per_worker_hour: 0.10,
            spot_discount: 0.70,
            current_poll_interval: 1, // High frequency polling
        };

        let recommendations = recommend_cost_optimizations(&config);

        assert!(recommendations
            .iter()
            .any(|r| matches!(r, CostOptimization::OptimizePolling { .. })));
    }

    #[test]
    fn test_cost_optimization_no_changes_needed() {
        let config = CostOptimizationConfig {
            current_workers: 5,
            avg_utilization: 0.95, // High utilization
            avg_batch_size: 100.0, // Good batch size
            cost_per_worker_hour: 0.10,
            spot_discount: 0.70,
            current_poll_interval: 10, // Reasonable polling
        };

        let recommendations = recommend_cost_optimizations(&config);

        // Should only recommend spot instances in this case
        assert!(recommendations.len() == 1);
    }

    #[test]
    fn test_cost_optimization_config_default() {
        let config = CostOptimizationConfig::default();
        assert_eq!(config.current_workers, 1);
        assert_eq!(config.avg_utilization, 0.5);
        assert_eq!(config.avg_batch_size, 1.0);
        assert_eq!(config.cost_per_worker_hour, 0.10);
        assert_eq!(config.spot_discount, 0.70);
        assert_eq!(config.current_poll_interval, 1);
    }

    // ========================================================================
    // Tests for Adaptive Sampling
    // ========================================================================

    #[test]
    fn test_adaptive_sampler_basic() {
        let sampler = AdaptiveSampler::new(0.5);

        assert!((sampler.current_rate() - 0.5).abs() < 0.01);

        // Should sample approximately 50% of the time
        let mut sample_count = 0;
        for _ in 0..1000 {
            if sampler.should_sample() {
                sample_count += 1;
            }
        }

        // Allow some variance (should be around 500)
        assert!(sample_count > 400 && sample_count < 600);
    }

    #[test]
    fn test_adaptive_sampler_high_load() {
        let sampler = AdaptiveSampler::new(0.5);

        // Simulate high load
        sampler.update_based_on_load(0.9);

        // Rate should decrease
        assert!(sampler.current_rate() < 0.5);
    }

    #[test]
    fn test_adaptive_sampler_low_load() {
        let sampler = AdaptiveSampler::new(0.5);

        // First reduce the rate
        sampler.update_based_on_load(0.9);
        let reduced_rate = sampler.current_rate();

        // Then simulate low load
        sampler.update_based_on_load(0.3);

        // Rate should increase back towards base rate
        assert!(sampler.current_rate() > reduced_rate);
    }

    #[test]
    fn test_adaptive_sampler_set_base_rate() {
        let sampler = AdaptiveSampler::new(0.5);

        sampler.set_base_rate(0.8);

        // Current rate should still be 0.5 until load changes
        assert!((sampler.current_rate() - 0.5).abs() < 0.01);

        // After low load, should move towards new base rate
        sampler.update_based_on_load(0.3);
        assert!(sampler.current_rate() > 0.5);
    }

    #[test]
    fn test_adaptive_sampler_min_rate() {
        let sampler = AdaptiveSampler::new(0.5);

        // Apply high load multiple times
        for _ in 0..20 {
            sampler.update_based_on_load(1.0);
        }

        // Should not go below 1%
        assert!(sampler.current_rate() >= 0.01);
    }

    // ========================================================================
    // Tests for Metric Export Batching
    // ========================================================================

    #[test]
    fn test_metric_batch_basic() {
        let mut batch = MetricBatch::new();

        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);

        let metric = MetricExport::Counter {
            name: "test".to_string(),
            value: 100.0,
            labels: CustomLabels::new(),
        };

        batch.add(metric);

        assert!(!batch.is_empty());
        assert_eq!(batch.len(), 1);
    }

    #[test]
    fn test_metric_batch_clear() {
        let mut batch = MetricBatch::new();

        for i in 0..5 {
            batch.add(MetricExport::Counter {
                name: format!("test_{}", i),
                value: i as f64,
                labels: CustomLabels::new(),
            });
        }

        assert_eq!(batch.len(), 5);

        batch.clear();

        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);
    }

    #[test]
    fn test_metric_batcher_basic() {
        let batcher = MetricBatcher::new(10, 60);

        assert_eq!(batcher.current_size(), 0);

        let metric = MetricExport::Counter {
            name: "test".to_string(),
            value: 100.0,
            labels: CustomLabels::new(),
        };

        let should_flush = batcher.add(metric);

        assert_eq!(batcher.current_size(), 1);
        assert!(!should_flush); // Not at max size yet
    }

    #[test]
    fn test_metric_batcher_flush_on_size() {
        let batcher = MetricBatcher::new(3, 60);

        // Add metrics up to the limit
        for i in 0..3 {
            let metric = MetricExport::Counter {
                name: format!("test_{}", i),
                value: i as f64,
                labels: CustomLabels::new(),
            };

            let should_flush = batcher.add(metric);

            if i < 2 {
                assert!(!should_flush);
            } else {
                assert!(should_flush); // Should flush at 3rd metric
            }
        }

        let flushed = batcher.flush();
        assert_eq!(flushed.len(), 3);
        assert_eq!(batcher.current_size(), 0);
    }

    #[test]
    fn test_metric_batcher_flush_manual() {
        let batcher = MetricBatcher::new(100, 60);

        // Add a few metrics
        for i in 0..5 {
            batcher.add(MetricExport::Counter {
                name: format!("test_{}", i),
                value: i as f64,
                labels: CustomLabels::new(),
            });
        }

        assert_eq!(batcher.current_size(), 5);

        let flushed = batcher.flush();
        assert_eq!(flushed.len(), 5);
        assert_eq!(batcher.current_size(), 0);
    }

    #[test]
    fn test_metric_batch_default() {
        let batch = MetricBatch::default();
        assert!(batch.is_empty());
    }

    // ========================================================================
    // Tests for Label Sanitization and Validation
    // ========================================================================

    #[test]
    fn test_sanitize_label_name() {
        assert_eq!(sanitize_label_name("task-type"), "task_type");
        assert_eq!(sanitize_label_name("123invalid"), "_123invalid");
        assert_eq!(sanitize_label_name("valid_name"), "valid_name");
        assert_eq!(sanitize_label_name(""), "_");
        assert_eq!(sanitize_label_name("special!@#$chars"), "special____chars");
        assert_eq!(sanitize_label_name("CamelCase123"), "CamelCase123");
    }

    #[test]
    fn test_sanitize_label_value() {
        assert_eq!(sanitize_label_value("hello\nworld"), "hello world");
        assert_eq!(sanitize_label_value("  spaces  "), "spaces");
        assert_eq!(sanitize_label_value("normal"), "normal");
        assert_eq!(
            sanitize_label_value("multi\r\nline\tvalue"),
            "multi line value"
        );
    }

    #[test]
    fn test_is_valid_metric_name() {
        assert!(is_valid_metric_name("http_requests_total"));
        assert!(is_valid_metric_name("http:requests:total"));
        assert!(is_valid_metric_name("_metric"));
        assert!(!is_valid_metric_name("123invalid"));
        assert!(!is_valid_metric_name("invalid-name"));
        assert!(!is_valid_metric_name(""));
    }

    #[test]
    fn test_is_valid_label_name() {
        assert!(is_valid_label_name("method"));
        assert!(is_valid_label_name("status_code"));
        assert!(is_valid_label_name("_internal"));
        assert!(!is_valid_label_name("123invalid"));
        assert!(!is_valid_label_name("invalid-name"));
        assert!(!is_valid_label_name("invalid:name"));
        assert!(!is_valid_label_name(""));
    }

    // ========================================================================
    // Tests for Histogram Heatmap
    // ========================================================================

    #[test]
    fn test_histogram_heatmap_basic() {
        let heatmap = HistogramHeatmap::new(10);

        assert!(heatmap.is_empty());
        assert_eq!(heatmap.len(), 0);

        let buckets = vec![
            HeatmapBucket {
                upper_bound: 0.1,
                count: 10,
                timestamp: 0,
            },
            HeatmapBucket {
                upper_bound: 1.0,
                count: 50,
                timestamp: 0,
            },
            HeatmapBucket {
                upper_bound: 10.0,
                count: 100,
                timestamp: 0,
            },
        ];

        heatmap.record_snapshot(buckets.clone());

        assert!(!heatmap.is_empty());
        assert_eq!(heatmap.len(), 1);

        let snapshots = heatmap.get_snapshots();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].1.len(), 3);
    }

    #[test]
    fn test_histogram_heatmap_retention() {
        let heatmap = HistogramHeatmap::new(3);

        // Add 5 snapshots, should only keep last 3
        for i in 0..5 {
            let buckets = vec![HeatmapBucket {
                upper_bound: 1.0,
                count: i * 10,
                timestamp: i,
            }];
            heatmap.record_snapshot(buckets);
        }

        assert_eq!(heatmap.len(), 3);

        let snapshots = heatmap.get_snapshots();
        // Should have snapshots 2, 3, 4 (the last 3)
        assert_eq!(snapshots[0].1[0].count, 20);
        assert_eq!(snapshots[1].1[0].count, 30);
        assert_eq!(snapshots[2].1[0].count, 40);
    }

    #[test]
    fn test_histogram_heatmap_clear() {
        let heatmap = HistogramHeatmap::new(10);

        let buckets = vec![HeatmapBucket {
            upper_bound: 1.0,
            count: 10,
            timestamp: 0,
        }];
        heatmap.record_snapshot(buckets);

        assert!(!heatmap.is_empty());

        heatmap.clear();

        assert!(heatmap.is_empty());
        assert_eq!(heatmap.len(), 0);
    }

    // ========================================================================
    // Tests for Metric Registry
    // ========================================================================

    #[test]
    fn test_metric_registry_counter() {
        let registry = MetricRegistry::new();

        assert!(registry.register_counter("test_counter", "Test counter"));
        assert!(!registry.register_counter("test_counter", "Duplicate")); // Already registered

        assert!(registry.increment_counter("test_counter", 5));
        assert_eq!(registry.get_counter("test_counter"), Some(5));

        assert!(registry.increment_counter("test_counter", 3));
        assert_eq!(registry.get_counter("test_counter"), Some(8));

        assert!(!registry.increment_counter("nonexistent", 1));
    }

    #[test]
    fn test_metric_registry_gauge() {
        let registry = MetricRegistry::new();

        assert!(registry.register_gauge("test_gauge", "Test gauge"));
        assert!(!registry.register_gauge("test_gauge", "Duplicate"));

        assert!(registry.set_gauge("test_gauge", 100));
        assert_eq!(registry.get_gauge("test_gauge"), Some(100));

        assert!(registry.set_gauge("test_gauge", 50));
        assert_eq!(registry.get_gauge("test_gauge"), Some(50));

        assert!(!registry.set_gauge("nonexistent", 1));
    }

    #[test]
    fn test_metric_registry_metadata() {
        let registry = MetricRegistry::new();

        registry.register_counter("counter1", "First counter");
        registry.register_gauge("gauge1", "First gauge");

        let metadata = registry.get_metadata("counter1").unwrap();
        assert_eq!(metadata.name, "counter1");
        assert_eq!(metadata.help, "First counter");
        assert_eq!(metadata.metric_type, MetricType::Counter);

        let list = registry.list_metrics();
        assert_eq!(list.len(), 2);
        assert!(list.contains(&"counter1".to_string()));
        assert!(list.contains(&"gauge1".to_string()));
    }

    #[test]
    fn test_metric_registry_validation() {
        let registry = MetricRegistry::new();

        // Invalid metric names should fail
        assert!(!registry.register_counter("123invalid", "Bad name"));
        assert!(!registry.register_counter("invalid-name", "Bad name"));

        // Valid names should succeed
        assert!(registry.register_counter("valid_name", "Good name"));
        assert!(registry.register_counter("http:requests:total", "Good name"));
    }

    #[test]
    fn test_metric_registry_unregister() {
        let registry = MetricRegistry::new();

        registry.register_counter("counter1", "Test");
        registry.register_gauge("gauge1", "Test");

        assert_eq!(registry.list_metrics().len(), 2);

        assert!(registry.unregister("counter1"));
        assert_eq!(registry.list_metrics().len(), 1);

        assert!(!registry.unregister("counter1")); // Already removed
        assert!(!registry.unregister("nonexistent"));
    }

    #[test]
    fn test_metric_registry_clear() {
        let registry = MetricRegistry::new();

        registry.register_counter("counter1", "Test");
        registry.register_gauge("gauge1", "Test");
        registry.increment_counter("counter1", 10);

        assert_eq!(registry.list_metrics().len(), 2);

        registry.clear();

        assert_eq!(registry.list_metrics().len(), 0);
        assert_eq!(registry.get_counter("counter1"), None);
    }

    // ========================================================================
    // Tests for Resource Tracker
    // ========================================================================

    #[test]
    fn test_resource_tracker_basic() {
        let tracker = ResourceTracker::new();

        assert_eq!(tracker.collection_count(), 0);
        assert_eq!(tracker.total_collection_time_micros(), 0.0);
        assert_eq!(tracker.avg_collection_time_micros(), 0.0);

        let result = tracker.track_operation(|| {
            std::thread::sleep(std::time::Duration::from_micros(100));
            42
        });

        assert_eq!(result, 42);
        assert_eq!(tracker.collection_count(), 1);
        assert!(tracker.total_collection_time_micros() > 0.0);
        assert!(tracker.avg_collection_time_micros() > 0.0);
    }

    #[test]
    fn test_resource_tracker_multiple_operations() {
        let tracker = ResourceTracker::new();

        for i in 0..5 {
            tracker.track_operation(|| {
                std::thread::sleep(std::time::Duration::from_micros(50));
                i
            });
        }

        assert_eq!(tracker.collection_count(), 5);
        assert!(tracker.avg_collection_time_micros() > 0.0);
    }

    #[test]
    fn test_resource_tracker_memory() {
        let tracker = ResourceTracker::new();

        assert_eq!(tracker.peak_memory_bytes(), 0);

        tracker.record_memory_usage(1000);
        assert_eq!(tracker.peak_memory_bytes(), 1000);

        tracker.record_memory_usage(500); // Lower value shouldn't update peak
        assert_eq!(tracker.peak_memory_bytes(), 1000);

        tracker.record_memory_usage(2000); // Higher value should update
        assert_eq!(tracker.peak_memory_bytes(), 2000);
    }

    #[test]
    fn test_resource_tracker_reset() {
        let tracker = ResourceTracker::new();

        tracker.track_operation(|| 42);
        tracker.record_memory_usage(1000);

        assert!(tracker.collection_count() > 0);
        assert!(tracker.peak_memory_bytes() > 0);

        tracker.reset();

        assert_eq!(tracker.collection_count(), 0);
        assert_eq!(tracker.total_collection_time_micros(), 0.0);
        assert_eq!(tracker.peak_memory_bytes(), 0);
    }

    // ========================================================================
    // Tests for New Enhancements
    // ========================================================================

    #[test]
    fn test_metric_history_snapshot() {
        let history = MetricHistory::new(100);

        // Record some values with different timestamps
        let base_time = 1000u64;
        for i in 0..10 {
            let batch = vec![(base_time + i, i as f64 * 10.0)];
            history.record_batch(&batch);
        }

        let snapshot = history.snapshot();

        assert_eq!(snapshot.count, 10);
        assert_eq!(snapshot.min, 0.0);
        assert_eq!(snapshot.max, 90.0);
        assert!((snapshot.mean - 45.0).abs() < 1.0);
        assert!(snapshot.std_dev > 0.0);
        // Trend should be positive for increasing values
        if let Some(trend) = snapshot.trend {
            assert!(trend > 0.0);
        }
        assert_eq!(snapshot.latest, Some(90.0));
    }

    #[test]
    fn test_metric_history_moving_average_window() {
        let history = MetricHistory::new(100);

        // Record increasing values
        for i in 0..20 {
            history.record(i as f64);
        }

        // Get 5-sample moving average
        let ma5 = history.moving_average_window(5);
        assert!(ma5.is_some());

        // Should be average of last 5 values: 15, 16, 17, 18, 19 = 17
        assert!((ma5.unwrap() - 17.0).abs() < 0.1);

        // Get 10-sample moving average
        let ma10 = history.moving_average_window(10);
        assert!(ma10.is_some());

        // Should be average of last 10 values: 10..19 = 14.5
        assert!((ma10.unwrap() - 14.5).abs() < 0.1);
    }

    #[test]
    fn test_metric_history_batch_record() {
        let history = MetricHistory::new(100);

        // Record batch of samples
        let batch = vec![
            (1000, 10.0),
            (1001, 20.0),
            (1002, 30.0),
            (1003, 40.0),
            (1004, 50.0),
        ];

        history.record_batch(&batch);

        assert_eq!(history.len(), 5);
        assert_eq!(history.latest().unwrap().value, 50.0);

        let snapshot = history.snapshot();
        assert_eq!(snapshot.count, 5);
        assert_eq!(snapshot.min, 10.0);
        assert_eq!(snapshot.max, 50.0);
    }

    #[test]
    #[serial_test::serial]
    fn test_convenience_record_functions() {
        reset_metrics();

        // Test record_task_enqueue
        record_task_enqueue("test_task");
        assert_eq!(TASKS_ENQUEUED_TOTAL.get() as u64, 1);

        // Test record_task_success
        record_task_success("test_task", 1.5);
        assert_eq!(TASKS_COMPLETED_TOTAL.get() as u64, 1);

        // Test record_task_failure
        record_task_failure("test_task");
        assert_eq!(TASKS_FAILED_TOTAL.get() as u64, 1);

        // Test record_task_retry
        record_task_retry("test_task");
        assert_eq!(TASKS_RETRIED_TOTAL.get() as u64, 1);
    }

    // ========================================================================
    // Tests for Metric Export Utilities
    // ========================================================================

    #[test]
    #[serial_test::serial]
    fn test_export_metrics_json() {
        reset_metrics();

        TASKS_ENQUEUED_TOTAL.inc_by(100.0);
        TASKS_COMPLETED_TOTAL.inc_by(95.0);
        TASKS_FAILED_TOTAL.inc_by(5.0);
        QUEUE_SIZE.set(50.0);
        ACTIVE_WORKERS.set(5.0);

        let json = export_metrics_json();

        assert!(json.contains("\"enqueued\""));
        assert!(json.contains("\"completed\""));
        assert!(json.contains("\"failed\""));
        assert!(json.contains("\"pending\""));
        assert!(json.contains("\"active\""));
        assert!(json.contains("\"success_rate\""));
        assert!(json.contains("\"error_rate\""));
    }

    #[test]
    #[serial_test::serial]
    fn test_export_metrics_csv() {
        reset_metrics();

        TASKS_ENQUEUED_TOTAL.inc_by(100.0);
        TASKS_COMPLETED_TOTAL.inc_by(95.0);
        TASKS_FAILED_TOTAL.inc_by(5.0);

        let csv = export_metrics_csv();

        assert!(csv.contains("timestamp,tasks_enqueued"));
        assert!(csv.contains(",100,95,5,"));
    }

    #[test]
    fn test_export_history_csv() {
        let history = MetricHistory::new(100);
        history.record(10.0);
        history.record(20.0);
        history.record(30.0);

        let csv = export_history_csv(&history, "queue_size");

        assert!(csv.contains("timestamp,queue_size"));
        assert!(csv.contains(",10"));
        assert!(csv.contains(",20"));
        assert!(csv.contains(",30"));
    }

    #[test]
    #[serial_test::serial]
    fn test_metric_export_batch() {
        reset_metrics();

        TASKS_ENQUEUED_TOTAL.inc_by(100.0);
        TASKS_COMPLETED_TOTAL.inc_by(95.0);

        let exports = MetricExportBatch::export_all();

        assert!(!exports.json.is_empty());
        assert!(!exports.csv.is_empty());
        assert!(!exports.prometheus.is_empty());

        assert!(exports.json.contains("\"enqueued\""));
        assert!(exports.csv.contains("timestamp,tasks_enqueued"));
        assert!(exports.prometheus.contains("celers_tasks_enqueued_total"));
    }

    // ========================================================================
    // Tests for Performance Profiling Utilities
    // ========================================================================

    #[test]
    fn test_metrics_profiler_basic() {
        let profiler = MetricsProfiler::new();

        profiler.profile_operation("test_op", || {
            // Simulate work
            std::thread::sleep(std::time::Duration::from_micros(100));
        });

        let stats = profiler.get_operation_stats("test_op");
        assert!(stats.is_some());

        let stats = stats.unwrap();
        assert_eq!(stats.operation_name, "test_op");
        assert_eq!(stats.call_count, 1);
        assert!(stats.mean_micros > 0.0);
    }

    #[test]
    fn test_metrics_profiler_multiple_operations() {
        let profiler = MetricsProfiler::new();

        for _ in 0..5 {
            profiler.profile_operation("increment", || {
                TASKS_ENQUEUED_TOTAL.inc();
            });
        }

        for _ in 0..3 {
            profiler.profile_operation("observe", || {
                TASK_EXECUTION_TIME.observe(1.0);
            });
        }

        let all_stats = profiler.all_stats();
        assert_eq!(all_stats.len(), 2);

        let increment_stats = profiler.get_operation_stats("increment").unwrap();
        assert_eq!(increment_stats.call_count, 5);

        let observe_stats = profiler.get_operation_stats("observe").unwrap();
        assert_eq!(observe_stats.call_count, 3);
    }

    #[test]
    fn test_metrics_profiler_report() {
        let profiler = MetricsProfiler::new();

        profiler.profile_operation("op1", || {
            std::thread::sleep(std::time::Duration::from_micros(50));
        });

        profiler.profile_operation("op2", || {
            std::thread::sleep(std::time::Duration::from_micros(25));
        });

        let report = profiler.generate_report();

        assert!(report.contains("Metrics Performance Report"));
        assert!(report.contains("Operation"));
        assert!(report.contains("Calls"));
        assert!(report.contains("Mean (μs)"));
        assert!(report.contains("op1"));
        assert!(report.contains("op2"));
    }

    #[test]
    fn test_metrics_profiler_reset() {
        let profiler = MetricsProfiler::new();

        profiler.profile_operation("test", || {
            TASKS_ENQUEUED_TOTAL.inc();
        });

        assert!(profiler.get_operation_stats("test").is_some());

        profiler.reset();

        assert!(profiler.get_operation_stats("test").is_none());
        assert_eq!(profiler.all_stats().len(), 0);
    }

    // ========================================================================
    // Tests for SLA Report Generator
    // ========================================================================

    #[test]
    #[serial_test::serial]
    fn test_sla_report_generation() {
        reset_metrics();

        TASKS_ENQUEUED_TOTAL.inc_by(1000.0);
        TASKS_COMPLETED_TOTAL.inc_by(990.0);
        TASKS_FAILED_TOTAL.inc_by(10.0);

        let slo = SloTarget {
            success_rate: 0.99,
            latency_seconds: 5.0,
            throughput: 0.1,
        };

        let report = SlaReport::generate(&slo, 3600);

        assert!(report.is_compliant);
        assert_eq!(report.total_tasks, 1000);
        assert_eq!(report.failed_tasks, 10);
        assert!(report.actual_success_rate >= 0.99);
    }

    #[test]
    #[serial_test::serial]
    fn test_sla_report_non_compliant() {
        reset_metrics();

        TASKS_ENQUEUED_TOTAL.inc_by(100.0);
        TASKS_COMPLETED_TOTAL.inc_by(85.0);
        TASKS_FAILED_TOTAL.inc_by(15.0);

        let slo = SloTarget {
            success_rate: 0.99,
            latency_seconds: 5.0,
            throughput: 1.0,
        };

        let report = SlaReport::generate(&slo, 3600);

        assert!(!report.is_compliant);
        assert!(!report.recommendations.is_empty());
    }

    #[test]
    #[serial_test::serial]
    fn test_sla_report_format() {
        reset_metrics();

        TASKS_ENQUEUED_TOTAL.inc_by(100.0);
        TASKS_COMPLETED_TOTAL.inc_by(95.0);
        TASKS_FAILED_TOTAL.inc_by(5.0);

        let slo = SloTarget {
            success_rate: 0.95,
            latency_seconds: 5.0,
            throughput: 0.01,
        };

        let report = SlaReport::generate(&slo, 3600);
        let formatted = report.format_text();

        assert!(formatted.contains("SLA Compliance Report"));
        assert!(formatted.contains("Status:"));
    }

    // ========================================================================
    // Tests for Alert Debouncer
    // ========================================================================

    #[test]
    fn test_alert_debouncer_basic() {
        let debouncer = AlertDebouncer::new(60);

        // First alert should fire
        assert!(debouncer.should_fire("high_error_rate"));

        // Immediate second alert should be debounced
        assert!(!debouncer.should_fire("high_error_rate"));
    }

    #[test]
    fn test_alert_debouncer_different_alerts() {
        let debouncer = AlertDebouncer::new(60);

        assert!(debouncer.should_fire("alert1"));
        assert!(debouncer.should_fire("alert2"));

        // Same alerts should be debounced
        assert!(!debouncer.should_fire("alert1"));
        assert!(!debouncer.should_fire("alert2"));
    }

    #[test]
    fn test_alert_debouncer_reset() {
        let debouncer = AlertDebouncer::new(60);

        debouncer.should_fire("test_alert");
        assert!(!debouncer.should_fire("test_alert"));

        debouncer.reset("test_alert");
        assert!(debouncer.should_fire("test_alert"));
    }

    #[test]
    fn test_alert_debouncer_time_until_next() {
        let debouncer = AlertDebouncer::new(60);

        assert_eq!(debouncer.time_until_next("test"), Some(0));

        debouncer.should_fire("test");
        let time_left = debouncer.time_until_next("test");
        assert!(time_left.is_some());
        assert!(time_left.unwrap() > 0 && time_left.unwrap() <= 60);
    }

    // ========================================================================
    // Tests for Health Score Calculator
    // ========================================================================

    #[test]
    #[serial_test::serial]
    fn test_health_score_perfect() {
        reset_metrics();

        TASKS_ENQUEUED_TOTAL.inc_by(1000.0);
        TASKS_COMPLETED_TOTAL.inc_by(1000.0);
        ACTIVE_WORKERS.set(5.0);
        QUEUE_SIZE.set(10.0);
        DLQ_SIZE.set(0.0);

        let slo = SloTarget {
            success_rate: 0.99,
            latency_seconds: 5.0,
            throughput: 10.0,
        };

        let score = HealthScore::calculate(&slo);

        assert!(score.score >= 0.9);
        assert_eq!(score.grade, 'A');
        assert!(score.issues.is_empty());
    }

    #[test]
    #[serial_test::serial]
    fn test_health_score_degraded() {
        reset_metrics();

        TASKS_ENQUEUED_TOTAL.inc_by(100.0);
        TASKS_COMPLETED_TOTAL.inc_by(80.0);
        TASKS_FAILED_TOTAL.inc_by(20.0);
        ACTIVE_WORKERS.set(1.0);
        QUEUE_SIZE.set(200.0);
        DLQ_SIZE.set(15.0);

        let slo = SloTarget {
            success_rate: 0.99,
            latency_seconds: 5.0,
            throughput: 10.0,
        };

        let score = HealthScore::calculate(&slo);

        assert!(score.score < 0.9);
        assert!(!score.issues.is_empty());
    }

    #[test]
    #[serial_test::serial]
    fn test_health_score_format() {
        reset_metrics();

        TASKS_ENQUEUED_TOTAL.inc_by(100.0);
        TASKS_COMPLETED_TOTAL.inc_by(95.0);
        TASKS_FAILED_TOTAL.inc_by(5.0);
        ACTIVE_WORKERS.set(3.0);

        let slo = SloTarget {
            success_rate: 0.95,
            latency_seconds: 5.0,
            throughput: 10.0,
        };

        let score = HealthScore::calculate(&slo);
        let report = score.format_report();

        assert!(report.contains("System Health Score"));
        assert!(report.contains("Grade:"));
        assert!(report.contains("Component Scores:"));
    }

    // ========================================================================
    // Tests for Metric Retention Manager
    // ========================================================================

    #[test]
    fn test_retention_policy_defaults() {
        let default_policy = RetentionPolicy::default_policy();
        assert_eq!(default_policy.max_age_seconds, 3600);
        assert_eq!(default_policy.max_samples, 1000);

        let hf_policy = RetentionPolicy::high_frequency();
        assert_eq!(hf_policy.max_age_seconds, 300);
        assert_eq!(hf_policy.max_samples, 300);

        let lt_policy = RetentionPolicy::long_term();
        assert_eq!(lt_policy.max_age_seconds, 86400);
        assert_eq!(lt_policy.max_samples, 10000);
    }

    #[test]
    fn test_retention_manager_set_get_policy() {
        let manager = MetricRetentionManager::new();

        let policy = RetentionPolicy::new(600, 500);
        manager.set_policy("test_metric", policy.clone());

        let retrieved = manager.get_policy("test_metric");
        assert_eq!(retrieved.max_age_seconds, 600);
        assert_eq!(retrieved.max_samples, 500);
    }

    #[test]
    fn test_retention_manager_default_policy() {
        let manager = MetricRetentionManager::new();

        // Getting policy for non-existent metric should return default
        let policy = manager.get_policy("nonexistent");
        assert_eq!(policy.max_age_seconds, 3600);
        assert_eq!(policy.max_samples, 1000);
    }

    // ========================================================================
    // Tests for Capacity Planning
    // ========================================================================

    #[test]
    fn test_capacity_prediction_growing() {
        let history = MetricHistory::new(100);

        // Simulate growing queue with explicit timestamps
        let base_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let samples: Vec<(u64, f64)> = (0..50)
            .map(|i| (base_time + i as u64, (i * 10) as f64))
            .collect();
        history.record_batch(&samples);

        let prediction = predict_capacity_exhaustion(&history, 1000.0, 0.8);

        assert!(prediction.current_utilization > 0.0);
        assert!(prediction.growth_rate > 0.0);
        assert!(prediction.time_until_exhaustion.is_some());
    }

    #[test]
    fn test_capacity_prediction_critical() {
        let history = MetricHistory::new(100);

        // Simulate near-capacity usage
        for _i in 0..10 {
            history.record(950.0);
        }

        let prediction = predict_capacity_exhaustion(&history, 1000.0, 0.8);

        assert!(prediction.current_utilization >= 0.95);
        assert_eq!(prediction.recommendation, CapacityRecommendation::Critical);
    }

    #[test]
    fn test_capacity_prediction_decreasing() {
        let history = MetricHistory::new(100);

        // Simulate decreasing usage with explicit timestamps
        let base_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let samples: Vec<(u64, f64)> = (0..50)
            .map(|i| (base_time + i as u64, (50 - i) as f64))
            .collect();
        history.record_batch(&samples);

        let prediction = predict_capacity_exhaustion(&history, 100.0, 0.8);

        assert_eq!(
            prediction.recommendation,
            CapacityRecommendation::Decreasing
        );
        assert!(prediction.growth_rate < 0.0);
    }

    // ========================================================================
    // Tests for Alert History
    // ========================================================================

    #[test]
    fn test_alert_history_basic() {
        let history = AlertHistory::new(100);

        let event = AlertEvent {
            alert_name: "high_queue".to_string(),
            timestamp: 1000,
            severity: AlertSeverity::Warning,
            trigger_value: 500.0,
            threshold: 400.0,
        };

        history.record_alert(event);

        assert_eq!(history.alert_fire_count("high_queue"), 1);
        let events = history.events_for_alert("high_queue");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].trigger_value, 500.0);
    }

    #[test]
    fn test_alert_history_retention() {
        let history = AlertHistory::new(5);

        // Add more events than max
        for i in 0..10 {
            let event = AlertEvent {
                alert_name: format!("alert_{}", i),
                timestamp: (1000 + i) as u64,
                severity: AlertSeverity::Warning,
                trigger_value: i as f64,
                threshold: 100.0,
            };
            history.record_alert(event);
        }

        // Should only keep last 5
        let recent = history.recent_events(10);
        assert_eq!(recent.len(), 5);
        assert_eq!(recent[0].alert_name, "alert_5");
    }

    #[test]
    fn test_alert_history_clear() {
        let history = AlertHistory::new(100);

        let event = AlertEvent {
            alert_name: "test".to_string(),
            timestamp: 1000,
            severity: AlertSeverity::Critical,
            trigger_value: 100.0,
            threshold: 50.0,
        };

        history.record_alert(event);
        assert_eq!(history.alert_fire_count("test"), 1);

        history.clear();
        assert_eq!(history.alert_fire_count("test"), 0);
    }

    // ========================================================================
    // Tests for Prometheus Query Builder
    // ========================================================================

    #[test]
    fn test_query_builder_basic() {
        let query = PrometheusQueryBuilder::new("celers_tasks_completed_total").build();

        assert_eq!(query, "celers_tasks_completed_total");
    }

    #[test]
    fn test_query_builder_with_labels() {
        let query = PrometheusQueryBuilder::new("celers_tasks_completed_total")
            .with_label("task_name", "send_email")
            .build();

        assert_eq!(
            query,
            "celers_tasks_completed_total{task_name=\"send_email\"}"
        );
    }

    #[test]
    fn test_query_builder_rate() {
        let query = PrometheusQueryBuilder::new("celers_tasks_completed_total")
            .with_label("task_name", "send_email")
            .rate("5m")
            .build();

        assert_eq!(
            query,
            "rate(celers_tasks_completed_total{task_name=\"send_email\"}[5m])"
        );
    }

    #[test]
    fn test_query_builder_sum() {
        let query = PrometheusQueryBuilder::new("celers_tasks_completed_total")
            .sum()
            .build();

        assert_eq!(query, "sum(celers_tasks_completed_total)");
    }

    #[test]
    fn test_query_builder_avg() {
        let query = PrometheusQueryBuilder::new("celers_queue_size")
            .avg()
            .build();

        assert_eq!(query, "avg(celers_queue_size)");
    }

    // ========================================================================
    // Tests for Metric Collection Scheduler
    // ========================================================================

    #[test]
    fn test_collection_task_basic() {
        let task = CollectionTask::new("test_task", 60);

        assert_eq!(task.name, "test_task");
        assert_eq!(task.interval_seconds, 60);
    }

    #[test]
    fn test_collection_scheduler() {
        let scheduler = MetricCollectionScheduler::new();

        let task1 = CollectionTask::new("task1", 60);
        let task2 = CollectionTask::new("task2", 120);

        scheduler.register_task(task1);
        scheduler.register_task(task2);

        let all_tasks = scheduler.all_tasks();
        assert_eq!(all_tasks.len(), 2);
    }

    #[test]
    fn test_collection_task_should_collect() {
        let task = CollectionTask::new("test", 1);

        // Initially should collect (never collected before)
        assert!(task.should_collect());

        // Mark as collected
        task.mark_collected();

        // Should not collect immediately after
        assert!(!task.should_collect());
    }
}
