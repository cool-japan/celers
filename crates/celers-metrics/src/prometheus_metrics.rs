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
            .expect("Prometheus metric registration failed");

    /// Total number of tasks completed successfully
    pub static ref TASKS_COMPLETED_TOTAL: Counter =
        register_counter!("celers_tasks_completed_total", "Total number of tasks completed successfully")
            .expect("Prometheus metric registration failed");

    /// Total number of tasks failed
    pub static ref TASKS_FAILED_TOTAL: Counter =
        register_counter!("celers_tasks_failed_total", "Total number of tasks failed")
            .expect("Prometheus metric registration failed");

    /// Total number of tasks retried
    pub static ref TASKS_RETRIED_TOTAL: Counter =
        register_counter!("celers_tasks_retried_total", "Total number of tasks retried")
            .expect("Prometheus metric registration failed");

    /// Total number of tasks cancelled
    pub static ref TASKS_CANCELLED_TOTAL: Counter =
        register_counter!("celers_tasks_cancelled_total", "Total number of tasks cancelled")
            .expect("Prometheus metric registration failed");

    /// Current queue size
    pub static ref QUEUE_SIZE: Gauge =
        register_gauge!("celers_queue_size", "Current number of tasks in queue")
            .expect("Prometheus metric registration failed");

    /// Current processing queue size
    pub static ref PROCESSING_QUEUE_SIZE: Gauge =
        register_gauge!("celers_processing_queue_size", "Current number of tasks being processed")
            .expect("Prometheus metric registration failed");

    /// Current dead letter queue size
    pub static ref DLQ_SIZE: Gauge =
        register_gauge!("celers_dlq_size", "Current number of tasks in dead letter queue")
            .expect("Prometheus metric registration failed");

    /// Number of active workers
    pub static ref ACTIVE_WORKERS: Gauge =
        register_gauge!("celers_active_workers", "Number of active workers")
            .expect("Prometheus metric registration failed");

    /// Task execution time histogram (in seconds)
    pub static ref TASK_EXECUTION_TIME: Histogram =
        register_histogram!(
            "celers_task_execution_seconds",
            "Task execution time in seconds",
            vec![0.001, 0.01, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0]
        )
        .expect("Prometheus metric registration failed");

    // Per-Task-Type Metrics (with labels)

    /// Total number of tasks enqueued by task type
    pub static ref TASKS_ENQUEUED_BY_TYPE: CounterVec =
        register_counter_vec!(
            "celers_tasks_enqueued_by_type_total",
            "Total number of tasks enqueued by task type",
            &["task_name"]
        )
        .expect("Prometheus metric registration failed");

    /// Total number of tasks completed by task type
    pub static ref TASKS_COMPLETED_BY_TYPE: CounterVec =
        register_counter_vec!(
            "celers_tasks_completed_by_type_total",
            "Total number of tasks completed by task type",
            &["task_name"]
        )
        .expect("Prometheus metric registration failed");

    /// Total number of tasks failed by task type
    pub static ref TASKS_FAILED_BY_TYPE: CounterVec =
        register_counter_vec!(
            "celers_tasks_failed_by_type_total",
            "Total number of tasks failed by task type",
            &["task_name"]
        )
        .expect("Prometheus metric registration failed");

    /// Total number of tasks retried by task type
    pub static ref TASKS_RETRIED_BY_TYPE: CounterVec =
        register_counter_vec!(
            "celers_tasks_retried_by_type_total",
            "Total number of tasks retried by task type",
            &["task_name"]
        )
        .expect("Prometheus metric registration failed");

    /// Total number of tasks cancelled by task type
    pub static ref TASKS_CANCELLED_BY_TYPE: CounterVec =
        register_counter_vec!(
            "celers_tasks_cancelled_by_type_total",
            "Total number of tasks cancelled by task type",
            &["task_name"]
        )
        .expect("Prometheus metric registration failed");

    /// Task execution time histogram by task type (in seconds)
    pub static ref TASK_EXECUTION_TIME_BY_TYPE: HistogramVec =
        register_histogram_vec!(
            "celers_task_execution_by_type_seconds",
            "Task execution time by task type in seconds",
            &["task_name"],
            vec![0.001, 0.01, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0]
        )
        .expect("Prometheus metric registration failed");

    /// Task result size by task type (in bytes)
    pub static ref TASK_RESULT_SIZE_BY_TYPE: HistogramVec =
        register_histogram_vec!(
            "celers_task_result_size_by_type_bytes",
            "Task result size by task type in bytes",
            &["task_name"],
            vec![100.0, 1_000.0, 10_000.0, 100_000.0, 1_000_000.0, 10_000_000.0]
        )
        .expect("Prometheus metric registration failed");

    // Connection Pooling Metrics

    /// Total number of Redis connections acquired
    pub static ref REDIS_CONNECTIONS_ACQUIRED_TOTAL: Counter =
        register_counter!("celers_redis_connections_acquired_total", "Total number of Redis connections acquired")
            .expect("Prometheus metric registration failed");

    /// Total number of Redis connection errors
    pub static ref REDIS_CONNECTION_ERRORS_TOTAL: Counter =
        register_counter!("celers_redis_connection_errors_total", "Total number of Redis connection errors")
            .expect("Prometheus metric registration failed");

    /// Current number of active Redis connections
    pub static ref REDIS_CONNECTIONS_ACTIVE: Gauge =
        register_gauge!("celers_redis_connections_active", "Current number of active Redis connections")
            .expect("Prometheus metric registration failed");

    /// Redis connection acquisition time histogram (in seconds)
    pub static ref REDIS_CONNECTION_ACQUIRE_TIME: Histogram =
        register_histogram!(
            "celers_redis_connection_acquire_seconds",
            "Redis connection acquisition time in seconds",
            vec![0.0001, 0.001, 0.01, 0.1, 0.5, 1.0, 5.0]
        )
        .expect("Prometheus metric registration failed");

    // PostgreSQL Connection Pool Metrics

    /// Maximum number of PostgreSQL connections in the pool
    pub static ref POSTGRES_POOL_MAX_SIZE: Gauge =
        register_gauge!("celers_postgres_pool_max_size", "Maximum number of PostgreSQL connections in the pool")
            .expect("Prometheus metric registration failed");

    /// Current number of PostgreSQL connections in the pool
    pub static ref POSTGRES_POOL_SIZE: Gauge =
        register_gauge!("celers_postgres_pool_size", "Current number of PostgreSQL connections in the pool")
            .expect("Prometheus metric registration failed");

    /// Current number of idle PostgreSQL connections
    pub static ref POSTGRES_POOL_IDLE: Gauge =
        register_gauge!("celers_postgres_pool_idle", "Current number of idle PostgreSQL connections")
            .expect("Prometheus metric registration failed");

    /// Current number of in-use PostgreSQL connections
    pub static ref POSTGRES_POOL_IN_USE: Gauge =
        register_gauge!("celers_postgres_pool_in_use", "Current number of in-use PostgreSQL connections")
            .expect("Prometheus metric registration failed");

    /// Total number of batch enqueue operations
    pub static ref BATCH_ENQUEUE_TOTAL: Counter =
        register_counter!("celers_batch_enqueue_total", "Total number of batch enqueue operations")
            .expect("Prometheus metric registration failed");

    /// Total number of batch dequeue operations
    pub static ref BATCH_DEQUEUE_TOTAL: Counter =
        register_counter!("celers_batch_dequeue_total", "Total number of batch dequeue operations")
            .expect("Prometheus metric registration failed");

    /// Batch size histogram (number of tasks per batch)
    pub static ref BATCH_SIZE: Histogram =
        register_histogram!(
            "celers_batch_size",
            "Number of tasks per batch operation",
            vec![1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0, 1000.0]
        )
        .expect("Prometheus metric registration failed");

    // Memory Usage Metrics

    /// Worker memory usage in bytes
    pub static ref WORKER_MEMORY_USAGE_BYTES: Gauge =
        register_gauge!("celers_worker_memory_usage_bytes", "Worker memory usage in bytes")
            .expect("Prometheus metric registration failed");

    /// Task result size histogram (in bytes)
    pub static ref TASK_RESULT_SIZE_BYTES: Histogram =
        register_histogram!(
            "celers_task_result_size_bytes",
            "Task result size in bytes",
            vec![100.0, 1_000.0, 10_000.0, 100_000.0, 1_000_000.0, 10_000_000.0]
        )
        .expect("Prometheus metric registration failed");

    /// Total number of tasks with oversized results (exceeded limit)
    pub static ref OVERSIZED_RESULTS_TOTAL: Counter =
        register_counter!("celers_oversized_results_total", "Total number of tasks with oversized results")
            .expect("Prometheus metric registration failed");

    /// Total number of garbage collection recommendations
    pub static ref GC_RECOMMENDATIONS_TOTAL: Counter =
        register_counter!("celers_gc_recommendations_total", "Total number of garbage collection recommendations")
            .expect("Prometheus metric registration failed");

    // Task Age Metrics

    /// Task age histogram (time from creation to execution in seconds)
    pub static ref TASK_AGE_SECONDS: Histogram =
        register_histogram!(
            "celers_task_age_seconds",
            "Task age (time from creation to execution) in seconds",
            vec![1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0, 1800.0, 3600.0]
        )
        .expect("Prometheus metric registration failed");

    /// Task queue wait time histogram (time in queue before processing in seconds)
    pub static ref TASK_QUEUE_WAIT_TIME_SECONDS: Histogram =
        register_histogram!(
            "celers_task_queue_wait_time_seconds",
            "Task wait time in queue before processing in seconds",
            vec![0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0]
        )
        .expect("Prometheus metric registration failed");

    // Worker Utilization Metrics

    /// Worker utilization percentage (0-100)
    pub static ref WORKER_UTILIZATION_PERCENT: Gauge =
        register_gauge!("celers_worker_utilization_percent", "Worker utilization percentage (0-100)")
            .expect("Prometheus metric registration failed");

    /// Number of idle workers
    pub static ref IDLE_WORKERS: Gauge =
        register_gauge!("celers_idle_workers", "Number of idle workers")
            .expect("Prometheus metric registration failed");

    /// Number of busy workers
    pub static ref BUSY_WORKERS: Gauge =
        register_gauge!("celers_busy_workers", "Number of busy workers")
            .expect("Prometheus metric registration failed");

    // Broker Operation Latency Metrics

    /// Broker enqueue operation latency (in seconds)
    pub static ref BROKER_ENQUEUE_LATENCY_SECONDS: Histogram =
        register_histogram!(
            "celers_broker_enqueue_latency_seconds",
            "Broker enqueue operation latency in seconds",
            vec![0.0001, 0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
        )
        .expect("Prometheus metric registration failed");

    /// Broker dequeue operation latency (in seconds)
    pub static ref BROKER_DEQUEUE_LATENCY_SECONDS: Histogram =
        register_histogram!(
            "celers_broker_dequeue_latency_seconds",
            "Broker dequeue operation latency in seconds",
            vec![0.0001, 0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
        )
        .expect("Prometheus metric registration failed");

    /// Broker ack operation latency (in seconds)
    pub static ref BROKER_ACK_LATENCY_SECONDS: Histogram =
        register_histogram!(
            "celers_broker_ack_latency_seconds",
            "Broker ack operation latency in seconds",
            vec![0.0001, 0.001, 0.01, 0.05, 0.1, 0.5, 1.0]
        )
        .expect("Prometheus metric registration failed");

    /// Broker reject operation latency (in seconds)
    pub static ref BROKER_REJECT_LATENCY_SECONDS: Histogram =
        register_histogram!(
            "celers_broker_reject_latency_seconds",
            "Broker reject operation latency in seconds",
            vec![0.0001, 0.001, 0.01, 0.05, 0.1, 0.5, 1.0]
        )
        .expect("Prometheus metric registration failed");

    /// Broker queue_size operation latency (in seconds)
    pub static ref BROKER_QUEUE_SIZE_LATENCY_SECONDS: Histogram =
        register_histogram!(
            "celers_broker_queue_size_latency_seconds",
            "Broker queue_size operation latency in seconds",
            vec![0.0001, 0.001, 0.01, 0.05, 0.1, 0.5, 1.0]
        )
        .expect("Prometheus metric registration failed");

    // Delayed Task Metrics

    /// Number of delayed tasks currently scheduled
    pub static ref DELAYED_TASKS_SCHEDULED: Gauge =
        register_gauge!("celers_delayed_tasks_scheduled", "Number of delayed tasks currently scheduled")
            .expect("Prometheus metric registration failed");

    /// Total number of delayed tasks enqueued
    pub static ref DELAYED_TASKS_ENQUEUED_TOTAL: Counter =
        register_counter!("celers_delayed_tasks_enqueued_total", "Total number of delayed tasks enqueued")
            .expect("Prometheus metric registration failed");

    /// Total number of delayed tasks executed
    pub static ref DELAYED_TASKS_EXECUTED_TOTAL: Counter =
        register_counter!("celers_delayed_tasks_executed_total", "Total number of delayed tasks executed")
            .expect("Prometheus metric registration failed");

    /// Total bytes of task payloads processed (enqueued + results)
    pub static ref TOTAL_PAYLOAD_BYTES_PROCESSED: Counter =
        register_counter!(
            "celers_total_payload_bytes_processed",
            "Total bytes of task payloads processed (enqueued + results)"
        )
        .expect("Failed to create metric");
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
    encoder
        .encode(&metric_families, &mut buffer)
        .expect("Prometheus encoder should never fail encoding gathered metrics");
    String::from_utf8(buffer).expect("Prometheus metrics buffer should be valid UTF-8")
}

/// Reset all metrics (useful for testing)
///
/// This resets every `Counter` and `Gauge` declared in this module, including
/// [`GC_RECOMMENDATIONS_TOTAL`] and [`TOTAL_PAYLOAD_BYTES_PROCESSED`] (the
/// latter feeds [`crate::history::estimate_costs`] and
/// [`crate::history::cost_per_task`], so leaving it out let state leak across
/// resets). `Histogram` metrics (e.g. [`TASK_EXECUTION_TIME`],
/// [`TASK_AGE_SECONDS`]) are intentionally not touched: the underlying
/// `prometheus` crate does not expose a `reset()` on `Histogram`, so their
/// accumulated buckets/count/sum persist for the life of the process.
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
    GC_RECOMMENDATIONS_TOTAL.reset();
    WORKER_UTILIZATION_PERCENT.set(0.0);
    IDLE_WORKERS.set(0.0);
    BUSY_WORKERS.set(0.0);
    DELAYED_TASKS_SCHEDULED.set(0.0);
    DELAYED_TASKS_ENQUEUED_TOTAL.reset();
    DELAYED_TASKS_EXECUTED_TOTAL.reset();
    TOTAL_PAYLOAD_BYTES_PROCESSED.reset();
}

/// Record bytes of payload processed (call when enqueuing tasks or storing results)
///
/// # Examples
///
/// ```
/// use celers_metrics::record_payload_bytes;
///
/// // Record 1024 bytes of payload processed
/// record_payload_bytes(1024);
/// ```
pub fn record_payload_bytes(bytes: usize) {
    TOTAL_PAYLOAD_BYTES_PROCESSED.inc_by(bytes as f64);
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
    ///
    /// Uses fractional-credit accumulation rather than a truncated
    /// `1.0 / sampling_rate` stride: `sample_every = (1.0 / rate) as u64`
    /// truncates to `1` for any rate above `0.5` (so a configured `0.9` or
    /// `0.6` rate previously sampled *everything*) and rounds every other
    /// rate to the nearest achievable `1/N` fraction (e.g. `0.34` sampled at
    /// 50%, not 34%). Scoring `floor((count+1) * rate) > floor(count * rate)`
    /// instead samples call number `count` exactly when the running total of
    /// "expected samples so far" crosses an integer boundary, which
    /// telescopes to exactly `floor(N * rate)` samples in any prefix of `N`
    /// calls -- the exact long-run rate for any value in `(0.0, 1.0)`.
    pub fn should_sample(&self) -> bool {
        if self.sampling_rate >= 1.0 {
            return true;
        }
        if self.sampling_rate <= 0.0 {
            return false;
        }

        let count = self.counter.fetch_add(1, Ordering::Relaxed);
        let rate = self.sampling_rate;
        (((count + 1) as f64) * rate).floor() > ((count as f64) * rate).floor()
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
///
/// Returns `0.0` when no tasks have been processed yet (`completed + failed
/// <= 0.0`) rather than the complement of [`calculate_success_rate`]'s `0.0`:
/// `1.0 - calculate_success_rate(0.0, 0.0)` would otherwise report a 100%
/// error rate for an idle system (e.g. a freshly started process, or right
/// after [`reset_metrics`]), which is enough on its own to trip an
/// `ErrorRateAbove` alert with no failures having occurred.
pub fn calculate_error_rate(completed: f64, failed: f64) -> f64 {
    let total = completed + failed;
    if total <= 0.0 {
        return 0.0;
    }
    failed / total
}

/// Calculate throughput (tasks per second)
#[must_use]
pub fn calculate_throughput(task_count: f64, time_seconds: f64) -> f64 {
    calculate_rate(task_count, 0.0, time_seconds)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alerts::{AlertCondition, AlertRule, AlertSeverity};
    use crate::backends::CurrentMetrics;
    use serial_test::serial;

    fn zeroed_metrics() -> CurrentMetrics {
        CurrentMetrics {
            tasks_enqueued: 0.0,
            tasks_completed: 0.0,
            tasks_failed: 0.0,
            tasks_retried: 0.0,
            tasks_cancelled: 0.0,
            queue_size: 0.0,
            processing_queue_size: 0.0,
            dlq_size: 0.0,
            active_workers: 0.0,
            total_payload_bytes: 0.0,
        }
    }

    // ---- calculate_error_rate --------------------------------------------

    #[test]
    fn error_rate_is_zero_when_no_tasks_processed() {
        // Regression: previously `1.0 - calculate_success_rate(0.0, 0.0)`
        // reported a 100% error rate for an idle system.
        assert_eq!(calculate_error_rate(0.0, 0.0), 0.0);
    }

    #[test]
    fn error_rate_and_success_rate_are_complementary_when_data_exists() {
        assert!((calculate_error_rate(90.0, 10.0) - 0.1).abs() < 1e-12);
        assert!((calculate_success_rate(90.0, 10.0) - 0.9).abs() < 1e-12);
        assert!(
            (calculate_error_rate(90.0, 10.0) + calculate_success_rate(90.0, 10.0) - 1.0).abs()
                < 1e-12
        );
        assert_eq!(calculate_error_rate(100.0, 0.0), 0.0);
        assert_eq!(calculate_error_rate(0.0, 100.0), 1.0);
    }

    #[test]
    fn zeroed_metrics_do_not_fire_error_rate_above_alert() {
        // The exact scenario from the audit: a freshly started process (or
        // one right after `reset_metrics()`) must not trip an
        // `ErrorRateAbove` alert just because nothing has happened yet.
        let rule = AlertRule::new(
            "high_error_rate",
            AlertCondition::ErrorRateAbove { threshold: 0.05 },
            AlertSeverity::Critical,
            "Error rate exceeded 5%",
        );
        assert!(!rule.should_fire(&zeroed_metrics()));
    }

    // ---- reset_metrics -----------------------------------------------------

    #[test]
    #[serial]
    fn reset_metrics_resets_gc_recommendations_and_payload_bytes() {
        GC_RECOMMENDATIONS_TOTAL.inc_by(3.0);
        TOTAL_PAYLOAD_BYTES_PROCESSED.inc_by(4096.0);
        assert!(GC_RECOMMENDATIONS_TOTAL.get() > 0.0);
        assert!(TOTAL_PAYLOAD_BYTES_PROCESSED.get() > 0.0);

        reset_metrics();

        assert_eq!(GC_RECOMMENDATIONS_TOTAL.get(), 0.0);
        assert_eq!(TOTAL_PAYLOAD_BYTES_PROCESSED.get(), 0.0);
    }

    // ---- MetricsSampler::should_sample --------------------------------------

    #[test]
    fn sampler_high_rate_is_not_truncated_to_full_sampling() {
        // Regression: `(1.0 / 0.9) as u64 == 1`, which sampled every call.
        let sampler = MetricsSampler::new(0.9);
        let sampled = (0..1000).filter(|_| sampler.should_sample()).count();
        // Exact long-run rate over N calls is floor(N * rate) = 900.
        assert_eq!(sampled, 900, "sampled {sampled}/1000 at rate 0.9");
    }

    #[test]
    fn sampler_awkward_rate_matches_long_run_fraction() {
        // Regression: `(1.0 / 0.34) as u64 == 2`, which sampled 50% instead
        // of 34%.
        let sampler = MetricsSampler::new(0.34);
        let sampled = (0..100_000).filter(|_| sampler.should_sample()).count();
        let expected = (100_000.0_f64 * 0.34).floor() as usize;
        assert_eq!(sampled, expected, "sampled {sampled}/100000 at rate 0.34");
    }

    #[test]
    fn sampler_at_scale_matches_configured_rate() {
        let sampler = MetricsSampler::new(0.9);
        let sampled = (0..100_000).filter(|_| sampler.should_sample()).count();
        assert_eq!(sampled, 90_000, "sampled {sampled}/100000 at rate 0.9");
    }

    #[test]
    fn sampler_boundary_rates_unchanged() {
        let always = MetricsSampler::new(1.0);
        assert!((0..50).all(|_| always.should_sample()));

        let never = MetricsSampler::new(0.0);
        assert!((0..50).all(|_| !never.should_sample()));
    }
}
