//! AWS SQS broker utility functions
//!
//! This module provides utility functions for SQS broker operations,
//! including batch optimization, cost estimation, and performance tuning.

use std::time::Duration;

/// Calculate optimal batch size for SQS operations
///
/// # Arguments
///
/// * `queue_size` - Current queue size
/// * `avg_message_size` - Average message size in bytes
/// * `target_latency_ms` - Target latency in milliseconds
///
/// # Returns
///
/// Recommended batch size (1-10 for SQS)
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::utilities::calculate_optimal_sqs_batch_size;
///
/// let batch_size = calculate_optimal_sqs_batch_size(1000, 1024, 100);
/// assert!(batch_size > 0);
/// assert!(batch_size <= 10);
/// ```
pub fn calculate_optimal_sqs_batch_size(
    queue_size: usize,
    avg_message_size: usize,
    target_latency_ms: u64,
) -> usize {
    // SQS batch size formula:
    // - Max 10 messages per batch (SQS limit)
    // - Max 256KB total payload (SQS limit)
    // - Smaller batches for large messages
    // - Larger batches for small messages

    let max_batch_from_size = 256_000usize
        .checked_div(avg_message_size)
        .map(|v| v.min(10))
        .unwrap_or(10);

    let base_batch_size = if avg_message_size > 100_000 {
        // Large messages (> 100KB): very small batches
        2
    } else if avg_message_size > 50_000 {
        // Medium-large messages (50-100KB): small batches
        4
    } else if avg_message_size > 10_000 {
        // Medium messages (10-50KB): medium batches
        7
    } else {
        // Small messages (< 10KB): max batches
        10
    };

    // Adjust for latency requirements
    let latency_factor = if target_latency_ms < 50 {
        0.7
    } else {
        1.0 // No increase for SQS due to 10 message limit
    };

    let batch_size = (base_batch_size as f64 * latency_factor) as usize;

    // Never exceed queue size, SQS limit (10), or size-based limit
    batch_size
        .min(queue_size)
        .min(10)
        .min(max_batch_from_size)
        .max(1)
}

/// Estimate SQS cost for queue operations
///
/// # Arguments
///
/// * `messages_per_day` - Number of messages per day
/// * `use_fifo` - Whether using FIFO queue (higher cost)
/// * `enable_batching` - Whether using batch operations (lower cost)
///
/// # Returns
///
/// Estimated monthly cost in USD
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::utilities::estimate_sqs_monthly_cost;
///
/// let cost = estimate_sqs_monthly_cost(1_000_000, false, true);
/// assert!(cost > 0.0);
/// ```
pub fn estimate_sqs_monthly_cost(
    messages_per_day: usize,
    use_fifo: bool,
    enable_batching: bool,
) -> f64 {
    // SQS pricing:
    // - Standard: $0.40 per million requests (first 1M free)
    // - FIFO: $0.50 per million requests (first 1M free)
    // - Each message requires: 1 SendMessage + 1 ReceiveMessage + 1 DeleteMessage = 3 requests
    // - Batching reduces requests by up to 10x

    let messages_per_month = messages_per_day * 30;
    let cost_per_million = if use_fifo { 0.50 } else { 0.40 };

    let requests_per_message = if enable_batching {
        // With batching, assume average batch size of 5
        3.0 / 5.0
    } else {
        3.0
    };

    let total_requests = messages_per_month as f64 * requests_per_message;
    let billable_requests = (total_requests - 1_000_000.0).max(0.0); // First 1M free

    (billable_requests / 1_000_000.0) * cost_per_million
}

/// Calculate optimal long polling wait time for SQS
///
/// # Arguments
///
/// * `avg_message_arrival_rate` - Average messages arriving per second
/// * `cost_vs_latency_preference` - 0.0 (minimize cost) to 1.0 (minimize latency)
///
/// # Returns
///
/// Recommended wait time in seconds (0-20)
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::utilities::calculate_optimal_sqs_wait_time;
///
/// let wait_time = calculate_optimal_sqs_wait_time(5.0, 0.5);
/// assert!(wait_time >= 0);
/// assert!(wait_time <= 20);
/// ```
pub fn calculate_optimal_sqs_wait_time(
    avg_message_arrival_rate: f64,
    cost_vs_latency_preference: f64,
) -> u64 {
    // Long polling wait time formula:
    // - Higher wait time = lower cost (fewer empty receives)
    // - Lower wait time = lower latency (faster response)
    // - SQS max: 20 seconds

    let preference = cost_vs_latency_preference.clamp(0.0, 1.0);

    if avg_message_arrival_rate > 10.0 {
        // High traffic: short wait time (messages arriving frequently)
        (5.0 * (1.0 - preference)) as u64
    } else if avg_message_arrival_rate > 1.0 {
        // Medium traffic: medium wait time
        let base = 10.0;
        (base + (10.0 * (1.0 - preference))) as u64
    } else {
        // Low traffic: long wait time (cost optimization)
        let base = 15.0;
        (base + (5.0 * (1.0 - preference))) as u64
    }
}

/// Calculate optimal visibility timeout for SQS
///
/// # Arguments
///
/// * `avg_processing_time_secs` - Average message processing time in seconds
/// * `processing_time_variance` - Processing time variance (0.0-1.0)
///
/// # Returns
///
/// Recommended visibility timeout in seconds
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::utilities::calculate_optimal_sqs_visibility_timeout;
///
/// let timeout = calculate_optimal_sqs_visibility_timeout(30.0, 0.2);
/// assert!(timeout >= 30);
/// ```
pub fn calculate_optimal_sqs_visibility_timeout(
    avg_processing_time_secs: f64,
    processing_time_variance: f64,
) -> u64 {
    // Visibility timeout formula:
    // timeout = avg_time + (variance_factor * avg_time)
    // This prevents premature redelivery while avoiding excessive delays

    let variance = processing_time_variance.clamp(0.0, 1.0);
    let buffer_factor = 1.0 + (variance * 2.0); // 1.0 to 3.0

    let timeout = avg_processing_time_secs * buffer_factor;

    // SQS limits: 0 seconds to 12 hours (43200 seconds)
    timeout.clamp(1.0, 43200.0) as u64
}

/// Estimate time to drain SQS queue
///
/// # Arguments
///
/// * `queue_size` - Current queue size
/// * `worker_count` - Number of workers
/// * `avg_processing_rate_per_worker` - Processing rate per worker (msg/sec)
/// * `use_batching` - Whether using batch operations
///
/// # Returns
///
/// Estimated time to drain queue
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::utilities::estimate_sqs_drain_time;
///
/// let duration = estimate_sqs_drain_time(1000, 5, 10.0, true);
/// assert!(duration.as_secs() > 0);
/// ```
pub fn estimate_sqs_drain_time(
    queue_size: usize,
    worker_count: usize,
    avg_processing_rate_per_worker: f64,
    use_batching: bool,
) -> Duration {
    if worker_count == 0 || avg_processing_rate_per_worker <= 0.0 {
        return Duration::from_secs(u64::MAX);
    }

    let batch_multiplier = if use_batching { 1.5 } else { 1.0 }; // Batching improves throughput
    let total_rate = worker_count as f64 * avg_processing_rate_per_worker * batch_multiplier;
    let seconds = queue_size as f64 / total_rate;

    Duration::from_secs_f64(seconds.max(0.0))
}

/// Calculate SQS API request efficiency
///
/// # Arguments
///
/// * `total_messages` - Total messages processed
/// * `total_api_requests` - Total API requests made
///
/// # Returns
///
/// Messages per API request (higher is better, max ~3.33 with perfect batching)
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::utilities::calculate_sqs_api_efficiency;
///
/// let efficiency = calculate_sqs_api_efficiency(10000, 3000);
/// assert!(efficiency > 0.0);
/// ```
pub fn calculate_sqs_api_efficiency(total_messages: usize, total_api_requests: usize) -> f64 {
    if total_api_requests == 0 {
        return 0.0;
    }

    total_messages as f64 / total_api_requests as f64
}

/// Calculate optimal message retention period
///
/// # Arguments
///
/// * `avg_processing_delay_hours` - Average processing delay in hours
/// * `max_retry_attempts` - Maximum retry attempts
/// * `safety_factor` - Safety factor (1.0-5.0, higher = longer retention)
///
/// # Returns
///
/// Recommended message retention in seconds
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::utilities::calculate_optimal_sqs_retention;
///
/// let retention = calculate_optimal_sqs_retention(2.0, 3, 2.0);
/// assert!(retention >= 60);
/// ```
pub fn calculate_optimal_sqs_retention(
    avg_processing_delay_hours: f64,
    max_retry_attempts: usize,
    safety_factor: f64,
) -> u64 {
    // Retention formula:
    // retention = (processing_delay + retry_time) * safety_factor
    // SQS limits: 60 seconds to 14 days (1209600 seconds)

    let factor = safety_factor.clamp(1.0, 5.0);
    let retry_hours = max_retry_attempts as f64 * avg_processing_delay_hours;
    let total_hours = (avg_processing_delay_hours + retry_hours) * factor;
    let total_seconds = total_hours * 3600.0;

    total_seconds.clamp(60.0, 1_209_600.0) as u64
}

/// Determine if FIFO queue should be used
///
/// # Arguments
///
/// * `requires_ordering` - Whether strict message ordering is required
/// * `requires_deduplication` - Whether message deduplication is required
/// * `expected_throughput` - Expected messages per second
///
/// # Returns
///
/// true if FIFO queue is recommended
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::utilities::should_use_sqs_fifo;
///
/// let use_fifo = should_use_sqs_fifo(true, false, 100.0);
/// assert!(use_fifo);
/// ```
pub fn should_use_sqs_fifo(
    requires_ordering: bool,
    requires_deduplication: bool,
    expected_throughput: f64,
) -> bool {
    // FIFO queue recommendations:
    // - Use if ordering or deduplication is required
    // - Don't use if throughput > 3000 msg/sec (FIFO limit with high throughput mode)
    // - Consider cost increase (25% more expensive)

    (requires_ordering || requires_deduplication) && expected_throughput <= 3000.0
}

/// Calculate SQS receive count threshold for DLQ
///
/// # Arguments
///
/// * `avg_processing_failure_rate` - Processing failure rate (0.0-1.0)
/// * `transient_error_probability` - Probability of transient errors (0.0-1.0)
///
/// # Returns
///
/// Recommended max receive count for DLQ (1-1000)
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::utilities::calculate_sqs_dlq_threshold;
///
/// let threshold = calculate_sqs_dlq_threshold(0.1, 0.3);
/// assert!(threshold >= 1);
/// assert!(threshold <= 1000);
/// ```
pub fn calculate_sqs_dlq_threshold(
    avg_processing_failure_rate: f64,
    transient_error_probability: f64,
) -> usize {
    // DLQ threshold formula:
    // - Low failure rate + low transient errors = low threshold (fail fast)
    // - High transient errors = high threshold (retry more)
    // - SQS limits: 1-1000

    let failure_rate = avg_processing_failure_rate.clamp(0.0, 1.0);
    let transient_prob = transient_error_probability.clamp(0.0, 1.0);

    let base_threshold = if failure_rate < 0.1 {
        3 // Low failure rate: retry 3 times
    } else if failure_rate < 0.3 {
        5 // Medium failure rate: retry 5 times
    } else {
        10 // High failure rate: retry 10 times
    };

    let threshold = if transient_prob > 0.5 {
        // High transient errors: increase retries
        (base_threshold as f64 * 2.0) as usize
    } else if transient_prob > 0.2 {
        // Medium transient errors: moderate increase
        (base_threshold as f64 * 1.5) as usize
    } else {
        base_threshold
    };

    threshold.clamp(1, 1000)
}

/// Calculate optimal concurrent receive operations
///
/// # Arguments
///
/// * `worker_count` - Number of workers
/// * `avg_processing_time_secs` - Average processing time per message
/// * `long_polling_wait_time_secs` - Long polling wait time
///
/// # Returns
///
/// Recommended concurrent receive operations
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::utilities::calculate_optimal_sqs_concurrent_receives;
///
/// let concurrent = calculate_optimal_sqs_concurrent_receives(10, 5.0, 20);
/// assert!(concurrent > 0);
/// ```
pub fn calculate_optimal_sqs_concurrent_receives(
    worker_count: usize,
    avg_processing_time_secs: f64,
    long_polling_wait_time_secs: u64,
) -> usize {
    // Concurrent receive formula:
    // Keep workers busy while messages are in flight
    // concurrent = workers * (processing_time / wait_time)

    if worker_count == 0 || long_polling_wait_time_secs == 0 {
        return 1;
    }

    let ratio = avg_processing_time_secs / long_polling_wait_time_secs as f64;
    let concurrent = (worker_count as f64 * ratio.max(1.0)).ceil() as usize;

    concurrent.clamp(1, worker_count * 2)
}

/// Estimate SQS throughput capacity
///
/// # Arguments
///
/// * `worker_count` - Number of workers
/// * `avg_processing_time_secs` - Average processing time per message
/// * `use_batching` - Whether using batch operations
///
/// # Returns
///
/// Estimated messages per second capacity
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::utilities::estimate_sqs_throughput_capacity;
///
/// let capacity = estimate_sqs_throughput_capacity(10, 2.0, true);
/// assert!(capacity > 0.0);
/// ```
pub fn estimate_sqs_throughput_capacity(
    worker_count: usize,
    avg_processing_time_secs: f64,
    use_batching: bool,
) -> f64 {
    if worker_count == 0 || avg_processing_time_secs <= 0.0 {
        return 0.0;
    }

    // Base capacity: workers / processing_time
    let base_capacity = worker_count as f64 / avg_processing_time_secs;

    // Batching improves throughput (receive up to 10 messages at once)
    let batch_multiplier = if use_batching { 3.0 } else { 1.0 };

    base_capacity * batch_multiplier
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_optimal_batch_size() {
        let batch_size = calculate_optimal_sqs_batch_size(1000, 1024, 100);
        assert!(batch_size > 0);
        assert!(batch_size <= 10);

        let batch_size_large = calculate_optimal_sqs_batch_size(1000, 150000, 100);
        assert!(batch_size_large <= 2);
    }

    #[test]
    fn test_estimate_monthly_cost() {
        let cost = estimate_sqs_monthly_cost(1_000_000, false, true);
        assert!(cost >= 0.0);

        let cost_fifo = estimate_sqs_monthly_cost(1_000_000, true, false);
        assert!(cost_fifo > cost);
    }

    #[test]
    fn test_calculate_wait_time() {
        let wait_time = calculate_optimal_sqs_wait_time(5.0, 0.5);
        assert!(wait_time <= 20);

        let wait_time_high_traffic = calculate_optimal_sqs_wait_time(20.0, 0.5);
        assert!(wait_time_high_traffic < wait_time);
    }

    #[test]
    fn test_calculate_visibility_timeout() {
        let timeout = calculate_optimal_sqs_visibility_timeout(30.0, 0.2);
        assert!(timeout >= 30);
        assert!(timeout <= 43200);
    }

    #[test]
    fn test_estimate_drain_time() {
        let duration = estimate_sqs_drain_time(1000, 5, 10.0, true);
        assert!(duration.as_secs() > 0);
        assert!(duration.as_secs() < 100);
    }

    #[test]
    fn test_calculate_api_efficiency() {
        let efficiency = calculate_sqs_api_efficiency(10000, 3000);
        assert!(efficiency > 3.0);
    }

    #[test]
    fn test_calculate_retention() {
        let retention = calculate_optimal_sqs_retention(2.0, 3, 2.0);
        assert!(retention >= 60);
        assert!(retention <= 1_209_600);
    }

    #[test]
    fn test_should_use_fifo() {
        assert!(should_use_sqs_fifo(true, false, 100.0));
        assert!(should_use_sqs_fifo(false, true, 100.0));
        assert!(!should_use_sqs_fifo(false, false, 100.0));
        assert!(!should_use_sqs_fifo(true, true, 5000.0));
    }

    #[test]
    fn test_calculate_dlq_threshold() {
        let threshold = calculate_sqs_dlq_threshold(0.1, 0.3);
        assert!(threshold >= 1);
        assert!(threshold <= 1000);
    }

    #[test]
    fn test_calculate_concurrent_receives() {
        let concurrent = calculate_optimal_sqs_concurrent_receives(10, 5.0, 20);
        assert!(concurrent > 0);
        assert!(concurrent <= 20);
    }

    #[test]
    fn test_estimate_throughput_capacity() {
        let capacity = estimate_sqs_throughput_capacity(10, 2.0, true);
        assert!(capacity > 0.0);

        let capacity_no_batch = estimate_sqs_throughput_capacity(10, 2.0, false);
        assert!(capacity > capacity_no_batch);
    }
}
