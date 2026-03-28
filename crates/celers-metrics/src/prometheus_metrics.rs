//! Prometheus metric definitions and core utilities for CeleRS.
//!
//! This module contains the static Prometheus metrics (counters, gauges, histograms),
//! convenience recording functions, configuration, and sampling support.

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
