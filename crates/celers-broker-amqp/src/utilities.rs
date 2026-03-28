//! AMQP/RabbitMQ broker utility functions
//!
//! This module provides utility functions for AMQP broker operations,
//! including batch optimization, memory estimation, and performance tuning.

use crate::QueueType;
use std::time::Duration;

/// Calculate optimal batch size for AMQP operations
///
/// # Arguments
///
/// * `queue_size` - Current queue size
/// * `avg_message_size` - Average message size in bytes
/// * `target_latency_ms` - Target latency in milliseconds
///
/// # Returns
///
/// Recommended batch size
///
/// # Examples
///
/// ```
/// use celers_broker_amqp::utilities::calculate_optimal_amqp_batch_size;
///
/// let batch_size = calculate_optimal_amqp_batch_size(1000, 1024, 100);
/// assert!(batch_size > 0);
/// assert!(batch_size <= 1000);
/// ```
pub fn calculate_optimal_amqp_batch_size(
    queue_size: usize,
    avg_message_size: usize,
    target_latency_ms: u64,
) -> usize {
    // AMQP optimal batch size formula:
    // - Smaller batches for large messages (network overhead)
    // - Larger batches for small messages (amortize round-trips)
    // - Consider publisher confirms overhead

    let base_batch_size = if avg_message_size > 100_000 {
        // Large messages (> 100KB): small batches
        10
    } else if avg_message_size > 10_000 {
        // Medium messages (10-100KB): medium batches
        50
    } else {
        // Small messages (< 10KB): large batches
        100
    };

    // Adjust for latency requirements
    let latency_factor = if target_latency_ms < 50 {
        0.5
    } else if target_latency_ms < 100 {
        1.0
    } else {
        1.5
    };

    let batch_size = (base_batch_size as f64 * latency_factor) as usize;

    // Never exceed queue size or reasonable max (1000)
    batch_size.min(queue_size).clamp(1, 1000)
}

/// Estimate RabbitMQ memory usage for queue
///
/// # Arguments
///
/// * `queue_size` - Number of messages in queue
/// * `avg_message_size` - Average message size in bytes
/// * `queue_type` - Queue type (Classic, Quorum, Stream)
///
/// # Returns
///
/// Estimated memory usage in bytes
///
/// # Examples
///
/// ```
/// use celers_broker_amqp::{utilities::estimate_amqp_queue_memory, QueueType};
///
/// let memory = estimate_amqp_queue_memory(1000, 1024, QueueType::Classic);
/// assert!(memory > 0);
/// ```
pub fn estimate_amqp_queue_memory(
    queue_size: usize,
    avg_message_size: usize,
    queue_type: QueueType,
) -> usize {
    // RabbitMQ overhead per message
    let overhead_per_message = match queue_type {
        QueueType::Classic => 200, // Classic queue overhead (metadata, pointers)
        QueueType::Quorum => 400,  // Quorum queue has higher overhead (Raft log)
        QueueType::Stream => 100,  // Stream is more efficient (append-only)
    };

    queue_size * (avg_message_size + overhead_per_message)
}

/// Calculate optimal number of AMQP channels for pool
///
/// # Arguments
///
/// * `expected_concurrency` - Expected concurrent operations
/// * `avg_operation_duration_ms` - Average operation duration in ms
///
/// # Returns
///
/// Recommended channel pool size
///
/// # Examples
///
/// ```
/// use celers_broker_amqp::utilities::calculate_optimal_amqp_channel_pool_size;
///
/// let pool_size = calculate_optimal_amqp_channel_pool_size(100, 50);
/// assert!(pool_size > 0);
/// ```
pub fn calculate_optimal_amqp_channel_pool_size(
    expected_concurrency: usize,
    avg_operation_duration_ms: u64,
) -> usize {
    // Rule of thumb: Channel pool size should handle expected concurrency
    // AMQP channels are lightweight but still have overhead

    let base_size = expected_concurrency;

    // Add buffer based on operation duration
    let buffer = if avg_operation_duration_ms > 100 {
        (expected_concurrency as f64 * 0.5) as usize
    } else if avg_operation_duration_ms > 50 {
        (expected_concurrency as f64 * 0.3) as usize
    } else {
        (expected_concurrency as f64 * 0.2) as usize
    };

    (base_size + buffer).clamp(5, 100)
}

/// Calculate optimal pipeline depth for batch publishing
///
/// # Arguments
///
/// * `batch_size` - Number of messages to publish
/// * `network_latency_ms` - Network latency to RabbitMQ
///
/// # Returns
///
/// Recommended pipeline depth
///
/// # Examples
///
/// ```
/// use celers_broker_amqp::utilities::calculate_amqp_pipeline_depth;
///
/// let depth = calculate_amqp_pipeline_depth(100, 10);
/// assert!(depth > 0);
/// ```
pub fn calculate_amqp_pipeline_depth(batch_size: usize, network_latency_ms: u64) -> usize {
    // Pipeline depth optimizes throughput vs latency
    // Higher latency = deeper pipeline to hide latency

    let base_depth = if network_latency_ms > 50 {
        20
    } else if network_latency_ms > 20 {
        10
    } else {
        5
    };

    // Don't exceed batch size
    base_depth.min(batch_size).max(1)
}

/// Calculate optimal prefetch (QoS) value for consumer
///
/// # Arguments
///
/// * `consumer_count` - Number of consumers
/// * `avg_processing_time_ms` - Average message processing time
/// * `network_latency_ms` - Network latency to RabbitMQ
///
/// # Returns
///
/// Recommended prefetch count
///
/// # Examples
///
/// ```
/// use celers_broker_amqp::utilities::calculate_optimal_amqp_prefetch;
///
/// let prefetch = calculate_optimal_amqp_prefetch(5, 100, 10);
/// assert!(prefetch > 0);
/// ```
pub fn calculate_optimal_amqp_prefetch(
    consumer_count: usize,
    avg_processing_time_ms: u64,
    network_latency_ms: u64,
) -> u16 {
    // Prefetch should keep consumers busy without overwhelming them
    // Formula: prefetch = (processing_time + latency) / (latency) per consumer

    if consumer_count == 0 {
        return 10; // Default
    }

    let total_time = avg_processing_time_ms + network_latency_ms;
    let prefetch_per_consumer = total_time
        .checked_div(network_latency_ms)
        .map(|v| v.max(1))
        .unwrap_or(10);

    // Clamp to reasonable values (1-1000)
    (prefetch_per_consumer as u16).clamp(1, 1000)
}

/// Estimate time to drain queue
///
/// # Arguments
///
/// * `queue_size` - Current queue size
/// * `consumer_count` - Number of consumers
/// * `avg_processing_rate_per_consumer` - Processing rate per consumer (msg/sec)
///
/// # Returns
///
/// Estimated time to drain queue
///
/// # Examples
///
/// ```
/// use celers_broker_amqp::utilities::estimate_amqp_drain_time;
///
/// let duration = estimate_amqp_drain_time(1000, 5, 10.0);
/// assert!(duration.as_secs() > 0);
/// ```
pub fn estimate_amqp_drain_time(
    queue_size: usize,
    consumer_count: usize,
    avg_processing_rate_per_consumer: f64,
) -> Duration {
    if consumer_count == 0 || avg_processing_rate_per_consumer <= 0.0 {
        return Duration::from_secs(u64::MAX);
    }

    let total_rate = consumer_count as f64 * avg_processing_rate_per_consumer;
    let seconds = queue_size as f64 / total_rate;

    Duration::from_secs_f64(seconds.max(0.0))
}

/// Calculate optimal connection pool size for AMQP
///
/// # Arguments
///
/// * `expected_channel_count` - Expected number of channels
/// * `channels_per_connection_limit` - RabbitMQ channel limit per connection (default: 2047)
///
/// # Returns
///
/// Recommended connection pool size
///
/// # Examples
///
/// ```
/// use celers_broker_amqp::utilities::calculate_optimal_amqp_connection_pool_size;
///
/// let pool_size = calculate_optimal_amqp_connection_pool_size(100, 2047);
/// assert!(pool_size > 0);
/// ```
pub fn calculate_optimal_amqp_connection_pool_size(
    expected_channel_count: usize,
    channels_per_connection_limit: usize,
) -> usize {
    if channels_per_connection_limit == 0 {
        return 1;
    }

    // Calculate minimum connections needed
    let min_connections = expected_channel_count.div_ceil(channels_per_connection_limit);

    // Add 20% buffer for spikes
    let with_buffer = (min_connections as f64 * 1.2).ceil() as usize;

    with_buffer.clamp(1, 100)
}

/// Estimate publisher confirm latency based on queue type
///
/// # Arguments
///
/// * `queue_type` - Queue type
/// * `network_latency_ms` - Network latency to RabbitMQ
///
/// # Returns
///
/// Estimated publisher confirm latency in milliseconds
///
/// # Examples
///
/// ```
/// use celers_broker_amqp::{utilities::estimate_amqp_confirm_latency, QueueType};
///
/// let latency = estimate_amqp_confirm_latency(QueueType::Classic, 10);
/// assert!(latency > 0);
/// ```
pub fn estimate_amqp_confirm_latency(queue_type: QueueType, network_latency_ms: u64) -> u64 {
    // Publisher confirm latency varies by queue type
    let base_latency = match queue_type {
        QueueType::Classic => 5, // Fast confirms
        QueueType::Quorum => 15, // Quorum consensus overhead
        QueueType::Stream => 10, // Stream append overhead
    };

    // Total = base + 2x network (publish + confirm)
    base_latency + (network_latency_ms * 2)
}

/// Calculate maximum throughput for queue
///
/// # Arguments
///
/// * `queue_type` - Queue type
/// * `avg_message_size` - Average message size in bytes
/// * `network_bandwidth_mbps` - Network bandwidth in Mbps
///
/// # Returns
///
/// Estimated maximum throughput in messages per second
///
/// # Examples
///
/// ```
/// use celers_broker_amqp::{utilities::calculate_amqp_max_throughput, QueueType};
///
/// let throughput = calculate_amqp_max_throughput(QueueType::Classic, 1024, 1000);
/// assert!(throughput > 0.0);
/// ```
pub fn calculate_amqp_max_throughput(
    queue_type: QueueType,
    avg_message_size: usize,
    network_bandwidth_mbps: u64,
) -> f64 {
    // Convert Mbps to bytes per second
    let bytes_per_sec = (network_bandwidth_mbps * 1_000_000) as f64 / 8.0;

    // Account for protocol overhead
    let overhead_factor = match queue_type {
        QueueType::Classic => 1.2, // 20% overhead
        QueueType::Quorum => 1.5,  // 50% overhead (replication)
        QueueType::Stream => 1.15, // 15% overhead
    };

    let effective_bandwidth = bytes_per_sec / overhead_factor;
    effective_bandwidth / avg_message_size as f64
}

/// Determine if lazy queue mode should be used
///
/// # Arguments
///
/// * `expected_queue_size` - Expected queue size
/// * `avg_message_size` - Average message size in bytes
/// * `available_memory_bytes` - Available RabbitMQ memory
///
/// # Returns
///
/// true if lazy mode is recommended
///
/// # Examples
///
/// ```
/// use celers_broker_amqp::utilities::should_use_amqp_lazy_mode;
///
/// let use_lazy = should_use_amqp_lazy_mode(100000, 10240, 1_000_000_000);
/// assert!(use_lazy);
/// ```
pub fn should_use_amqp_lazy_mode(
    expected_queue_size: usize,
    avg_message_size: usize,
    available_memory_bytes: usize,
) -> bool {
    let estimated_memory = expected_queue_size * avg_message_size;

    // Use lazy mode if estimated memory > 50% of available memory
    // or if queue is expected to be very large (> 100K messages)
    estimated_memory > (available_memory_bytes / 2) || expected_queue_size > 100_000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_optimal_batch_size() {
        let batch_size = calculate_optimal_amqp_batch_size(1000, 1024, 100);
        assert!(batch_size > 0);
        assert!(batch_size <= 1000);
    }

    #[test]
    fn test_estimate_queue_memory() {
        let memory = estimate_amqp_queue_memory(1000, 1024, QueueType::Classic);
        assert_eq!(memory, 1000 * (1024 + 200));
    }

    #[test]
    fn test_calculate_channel_pool_size() {
        let pool_size = calculate_optimal_amqp_channel_pool_size(100, 50);
        assert!(pool_size >= 5);
        assert!(pool_size <= 100);
    }

    #[test]
    fn test_calculate_pipeline_depth() {
        let depth = calculate_amqp_pipeline_depth(100, 10);
        assert!(depth > 0);
        assert!(depth <= 100);
    }

    #[test]
    fn test_calculate_prefetch() {
        let prefetch = calculate_optimal_amqp_prefetch(5, 100, 10);
        assert!(prefetch >= 1);
        assert!(prefetch <= 1000);
    }

    #[test]
    fn test_estimate_drain_time() {
        let duration = estimate_amqp_drain_time(1000, 5, 10.0);
        assert_eq!(duration.as_secs(), 20);
    }

    #[test]
    fn test_calculate_connection_pool_size() {
        let pool_size = calculate_optimal_amqp_connection_pool_size(100, 2047);
        assert!(pool_size >= 1);
        assert!(pool_size <= 2);

        let pool_size = calculate_optimal_amqp_connection_pool_size(5000, 2047);
        assert!(pool_size > 1);
    }

    #[test]
    fn test_estimate_confirm_latency() {
        let latency = estimate_amqp_confirm_latency(QueueType::Classic, 10);
        assert_eq!(latency, 5 + 20);
    }

    #[test]
    fn test_calculate_max_throughput() {
        let throughput = calculate_amqp_max_throughput(QueueType::Classic, 1024, 1000);
        assert!(throughput > 0.0);
    }

    #[test]
    fn test_should_use_lazy_mode() {
        assert!(should_use_amqp_lazy_mode(100000, 10240, 1_000_000_000));
        assert!(!should_use_amqp_lazy_mode(1000, 1024, 1_000_000_000));
    }
}
