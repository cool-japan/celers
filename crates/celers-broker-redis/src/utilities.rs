//! Redis broker utility functions
//!
//! This module provides utility functions for Redis broker operations,
//! including batch optimization, memory estimation, and performance tuning.

use crate::QueueMode;
use std::collections::HashMap;

/// Calculate optimal batch size for Redis operations
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
/// use celers_broker_redis::utilities::calculate_optimal_redis_batch_size;
///
/// let batch_size = calculate_optimal_redis_batch_size(1000, 1024, 100);
/// assert!(batch_size > 0);
/// assert!(batch_size <= 1000);
/// ```
pub fn calculate_optimal_redis_batch_size(
    queue_size: usize,
    avg_message_size: usize,
    target_latency_ms: u64,
) -> usize {
    // Redis optimal batch size formula:
    // - Smaller batches for large messages
    // - Larger batches for small messages
    // - Consider network latency

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

/// Estimate Redis memory usage for queue
///
/// # Arguments
///
/// * `queue_size` - Number of messages in queue
/// * `avg_message_size` - Average message size in bytes
/// * `mode` - Queue mode (FIFO or Priority)
///
/// # Returns
///
/// Estimated memory usage in bytes
///
/// # Examples
///
/// ```
/// use celers_broker_redis::{utilities::estimate_redis_queue_memory, QueueMode};
///
/// let memory = estimate_redis_queue_memory(1000, 1024, QueueMode::Fifo);
/// assert!(memory > 0);
/// ```
pub fn estimate_redis_queue_memory(
    queue_size: usize,
    avg_message_size: usize,
    mode: QueueMode,
) -> usize {
    // Redis overhead per entry
    let overhead_per_entry = match mode {
        QueueMode::Fifo => 64,      // List node overhead
        QueueMode::Priority => 128, // Sorted set + score overhead
    };

    queue_size * (avg_message_size + overhead_per_entry)
}

/// Calculate optimal number of Redis connections for pool
///
/// # Arguments
///
/// * `expected_concurrency` - Expected concurrent operations
/// * `avg_operation_duration_ms` - Average operation duration in ms
///
/// # Returns
///
/// Recommended pool size
///
/// # Examples
///
/// ```
/// use celers_broker_redis::utilities::calculate_optimal_redis_pool_size;
///
/// let pool_size = calculate_optimal_redis_pool_size(100, 50);
/// assert!(pool_size > 0);
/// ```
pub fn calculate_optimal_redis_pool_size(
    expected_concurrency: usize,
    avg_operation_duration_ms: u64,
) -> usize {
    // Rule of thumb: Pool size should handle expected concurrency
    // with some buffer for spikes

    let base_size = expected_concurrency;

    // Add buffer based on operation duration
    let buffer = if avg_operation_duration_ms > 100 {
        (expected_concurrency as f64 * 0.5) as usize
    } else if avg_operation_duration_ms > 50 {
        (expected_concurrency as f64 * 0.3) as usize
    } else {
        (expected_concurrency as f64 * 0.2) as usize
    };

    (base_size + buffer).clamp(5, 1000)
}

/// Calculate pipeline size for batch operations
///
/// # Arguments
///
/// * `batch_size` - Number of operations in batch
/// * `network_latency_ms` - Network latency to Redis
///
/// # Returns
///
/// Recommended pipeline size
///
/// # Examples
///
/// ```
/// use celers_broker_redis::utilities::calculate_redis_pipeline_size;
///
/// let pipeline_size = calculate_redis_pipeline_size(100, 10);
/// assert!(pipeline_size > 0);
/// ```
pub fn calculate_redis_pipeline_size(batch_size: usize, network_latency_ms: u64) -> usize {
    // Pipeline size optimization based on network latency
    let base_size = if network_latency_ms > 50 {
        // High latency: larger pipelines to amortize RTT
        batch_size
    } else if network_latency_ms > 10 {
        // Medium latency: moderate pipelines
        batch_size.min(500)
    } else {
        // Low latency: smaller pipelines are fine
        batch_size.min(100)
    };

    base_size.clamp(1, 10000)
}

/// Estimate time to drain queue
///
/// # Arguments
///
/// * `queue_size` - Current queue size
/// * `processing_rate` - Processing rate (messages per second)
///
/// # Returns
///
/// Estimated drain time in seconds
///
/// # Examples
///
/// ```
/// use celers_broker_redis::utilities::estimate_redis_queue_drain_time;
///
/// let drain_time = estimate_redis_queue_drain_time(1000, 50.0);
/// assert_eq!(drain_time, 20.0);
/// ```
pub fn estimate_redis_queue_drain_time(queue_size: usize, processing_rate: f64) -> f64 {
    if processing_rate > 0.0 {
        queue_size as f64 / processing_rate
    } else {
        f64::INFINITY
    }
}

/// Suggest Redis pipeline optimization strategy
///
/// # Arguments
///
/// * `operation_count` - Number of operations
/// * `operation_type` - Type of operation ("read" or "write")
///
/// # Returns
///
/// Optimization strategy as string
///
/// # Examples
///
/// ```
/// use celers_broker_redis::utilities::suggest_redis_pipeline_strategy;
///
/// let strategy = suggest_redis_pipeline_strategy(100, "write");
/// assert!(!strategy.is_empty());
/// ```
pub fn suggest_redis_pipeline_strategy(operation_count: usize, operation_type: &str) -> String {
    if operation_count < 10 {
        "No pipeline needed - execute operations directly".to_string()
    } else if operation_count < 100 {
        format!(
            "Use single pipeline with all {} operations",
            operation_count
        )
    } else {
        let chunk_size = if operation_type == "write" { 500 } else { 1000 };
        format!(
            "Use chunked pipelines of {} operations each (total: {} chunks)",
            chunk_size,
            operation_count.div_ceil(chunk_size)
        )
    }
}

/// Calculate Redis key expiration time based on priority
///
/// # Arguments
///
/// * `priority` - Task priority (0-255)
/// * `base_ttl_secs` - Base TTL in seconds
///
/// # Returns
///
/// Adjusted TTL in seconds
///
/// # Examples
///
/// ```
/// use celers_broker_redis::utilities::calculate_redis_key_ttl_by_priority;
///
/// let ttl = calculate_redis_key_ttl_by_priority(200, 3600);
/// assert!(ttl >= 3600);
/// ```
pub fn calculate_redis_key_ttl_by_priority(priority: i32, base_ttl_secs: u64) -> u64 {
    // Higher priority = longer TTL
    let priority_multiplier = 1.0 + (priority as f64 / 255.0);
    (base_ttl_secs as f64 * priority_multiplier) as u64
}

/// Analyze Redis command performance
///
/// # Arguments
///
/// * `command_latencies` - Map of command name to latency in ms
///
/// # Returns
///
/// Performance analysis
///
/// # Examples
///
/// ```
/// use celers_broker_redis::utilities::analyze_redis_command_performance;
/// use std::collections::HashMap;
///
/// let mut latencies = HashMap::new();
/// latencies.insert("GET".to_string(), 2.0);
/// latencies.insert("SET".to_string(), 3.0);
/// latencies.insert("ZADD".to_string(), 5.0);
///
/// let analysis = analyze_redis_command_performance(&latencies);
/// assert!(analysis.contains_key("slowest_command"));
/// ```
pub fn analyze_redis_command_performance(
    command_latencies: &HashMap<String, f64>,
) -> HashMap<String, String> {
    let mut analysis = HashMap::new();

    if command_latencies.is_empty() {
        analysis.insert("status".to_string(), "no_data".to_string());
        return analysis;
    }

    // Find slowest command
    let (slowest_cmd, max_latency) = command_latencies
        .iter()
        .max_by(|a, b| a.1.partial_cmp(b.1).unwrap())
        .unwrap();

    analysis.insert("slowest_command".to_string(), slowest_cmd.clone());
    analysis.insert("max_latency_ms".to_string(), format!("{:.2}", max_latency));

    // Calculate average
    let avg_latency: f64 = command_latencies.values().sum::<f64>() / command_latencies.len() as f64;
    analysis.insert("avg_latency_ms".to_string(), format!("{:.2}", avg_latency));

    // Performance status
    let status = if avg_latency < 5.0 {
        "excellent"
    } else if avg_latency < 10.0 {
        "good"
    } else if avg_latency < 20.0 {
        "acceptable"
    } else {
        "poor"
    };
    analysis.insert("overall_status".to_string(), status.to_string());

    analysis
}

/// Suggest Redis persistence strategy
///
/// # Arguments
///
/// * `throughput_msg_per_sec` - Message throughput
/// * `durability_requirement` - Durability level ("low", "medium", "high")
///
/// # Returns
///
/// Recommended persistence configuration
///
/// # Examples
///
/// ```
/// use celers_broker_redis::utilities::suggest_redis_persistence_strategy;
///
/// let strategy = suggest_redis_persistence_strategy(100.0, "high");
/// assert!(strategy.contains("AOF"));
/// ```
pub fn suggest_redis_persistence_strategy(
    throughput_msg_per_sec: f64,
    durability_requirement: &str,
) -> String {
    match (throughput_msg_per_sec, durability_requirement) {
        (t, "high") if t > 1000.0 => {
            "AOF with everysec fsync + RDB snapshots every 5 minutes (high durability, acceptable performance)".to_string()
        }
        (_, "high") => {
            "AOF with always fsync (maximum durability, slower performance)".to_string()
        }
        (t, "medium") if t > 500.0 => {
            "AOF with everysec fsync (good balance of durability and performance)".to_string()
        }
        (_, "medium") => {
            "RDB snapshots every 1-5 minutes (good durability, good performance)".to_string()
        }
        _ => {
            "No persistence or RDB snapshots every 15 minutes (maximum performance, minimal durability)".to_string()
        }
    }
}

/// Calculate optimal Redis timeout values
///
/// # Arguments
///
/// * `avg_operation_ms` - Average operation duration in ms
/// * `p99_operation_ms` - 99th percentile operation duration in ms
///
/// # Returns
///
/// (connection_timeout, operation_timeout) in milliseconds
///
/// # Examples
///
/// ```
/// use celers_broker_redis::utilities::calculate_redis_timeout_values;
///
/// let (conn_timeout, op_timeout) = calculate_redis_timeout_values(50.0, 200.0);
/// assert!(conn_timeout > 0);
/// assert!(op_timeout > 0);
/// ```
pub fn calculate_redis_timeout_values(avg_operation_ms: f64, p99_operation_ms: f64) -> (u64, u64) {
    // Connection timeout: 3x average operation time, min 1000ms
    let connection_timeout = ((avg_operation_ms * 3.0) as u64).max(1000);

    // Operation timeout: 2x p99 latency, min 5000ms
    let operation_timeout = ((p99_operation_ms * 2.0) as u64).max(5000);

    (connection_timeout, operation_timeout)
}

/// Queue migration strategy for mode conversion
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationStrategy {
    /// Preserve all tasks with default priority
    PreserveAll,
    /// Preserve high priority tasks only (priority >= threshold)
    HighPriorityOnly { threshold: i32 },
    /// Sample tasks (keep every Nth task)
    Sample { every_nth: usize },
}

/// Calculate optimal batch size for queue migration
///
/// # Arguments
///
/// * `source_queue_size` - Size of source queue
/// * `target_throughput_per_sec` - Target migration throughput
/// * `available_time_secs` - Available time window for migration
///
/// # Returns
///
/// Optimal batch size for migration
///
/// # Examples
///
/// ```
/// use celers_broker_redis::utilities::calculate_redis_migration_batch_size;
///
/// let batch_size = calculate_redis_migration_batch_size(10000, 100.0, 120);
/// assert!(batch_size > 0);
/// ```
pub fn calculate_redis_migration_batch_size(
    source_queue_size: usize,
    target_throughput_per_sec: f64,
    available_time_secs: u64,
) -> usize {
    // Calculate required rate
    let required_rate = source_queue_size as f64 / available_time_secs as f64;

    // Batch size should enable target throughput
    let batch_size = if required_rate > target_throughput_per_sec {
        // Need larger batches to meet deadline
        (required_rate / target_throughput_per_sec * 100.0) as usize
    } else {
        // Can use moderate batches
        100
    };

    batch_size.clamp(10, 1000)
}

/// Estimate time required for queue migration
///
/// # Arguments
///
/// * `queue_size` - Number of tasks to migrate
/// * `batch_size` - Batch size for migration
/// * `batch_processing_time_ms` - Time to process one batch (ms)
///
/// # Returns
///
/// Estimated migration time in seconds
///
/// # Examples
///
/// ```
/// use celers_broker_redis::utilities::estimate_redis_migration_time;
///
/// let time_secs = estimate_redis_migration_time(10000, 100, 500);
/// assert!(time_secs > 0.0);
/// ```
pub fn estimate_redis_migration_time(
    queue_size: usize,
    batch_size: usize,
    batch_processing_time_ms: u64,
) -> f64 {
    if batch_size == 0 {
        return f64::INFINITY;
    }

    let num_batches = queue_size.div_ceil(batch_size);
    let total_time_ms = num_batches as f64 * batch_processing_time_ms as f64;

    total_time_ms / 1000.0
}

/// Calculate data retention policy for queue cleanup
///
/// # Arguments
///
/// * `queue_age_hours` - Age of oldest message in hours
/// * `queue_size` - Current queue size
/// * `max_recommended_size` - Maximum recommended queue size
///
/// # Returns
///
/// Recommended TTL in seconds, or None if no cleanup needed
///
/// # Examples
///
/// ```
/// use celers_broker_redis::utilities::suggest_redis_data_retention;
///
/// let ttl = suggest_redis_data_retention(72.0, 15000, 10000);
/// assert!(ttl.is_some());
/// ```
pub fn suggest_redis_data_retention(
    queue_age_hours: f64,
    queue_size: usize,
    max_recommended_size: usize,
) -> Option<u64> {
    if queue_size <= max_recommended_size {
        return None; // No cleanup needed
    }

    let overflow_ratio = queue_size as f64 / max_recommended_size as f64;

    // Suggest TTL based on overflow severity
    let suggested_ttl_hours: f64 = if overflow_ratio > 2.0 {
        // Severe overflow: aggressive cleanup
        24.0
    } else if overflow_ratio > 1.5 {
        // Moderate overflow: 3 days
        72.0
    } else {
        // Mild overflow: 1 week
        168.0
    };

    // Don't suggest TTL shorter than current queue age / 2
    let min_ttl_hours = (queue_age_hours / 2.0).max(1.0);
    let ttl_hours = suggested_ttl_hours.max(min_ttl_hours);

    Some((ttl_hours * 3600.0) as u64)
}

/// Analyze queue balance across partitions
///
/// # Arguments
///
/// * `partition_sizes` - Sizes of each partition
///
/// # Returns
///
/// (balance_score, needs_rebalancing, max_diff_percentage)
/// - balance_score: 0.0-1.0 (1.0 = perfectly balanced)
/// - needs_rebalancing: true if imbalance > 30%
/// - max_diff_percentage: maximum difference from average
///
/// # Examples
///
/// ```
/// use celers_broker_redis::utilities::analyze_redis_queue_balance;
///
/// let partitions = vec![100, 150, 120, 130];
/// let (score, needs_rebalance, max_diff) = analyze_redis_queue_balance(&partitions);
/// assert!(score >= 0.0 && score <= 1.0);
/// ```
pub fn analyze_redis_queue_balance(partition_sizes: &[usize]) -> (f64, bool, f64) {
    if partition_sizes.is_empty() {
        return (1.0, false, 0.0);
    }

    if partition_sizes.len() == 1 {
        return (1.0, false, 0.0);
    }

    let total: usize = partition_sizes.iter().sum();
    let avg = total as f64 / partition_sizes.len() as f64;

    if avg == 0.0 {
        return (1.0, false, 0.0);
    }

    // Calculate maximum deviation from average
    let max_diff = partition_sizes
        .iter()
        .map(|&size| (size as f64 - avg).abs() / avg * 100.0)
        .fold(0.0, f64::max);

    // Balance score: 1.0 = perfect, 0.0 = completely imbalanced
    let balance_score = (1.0 - (max_diff / 100.0)).max(0.0);

    // Need rebalancing if max difference > 30%
    let needs_rebalancing = max_diff > 30.0;

    (balance_score, needs_rebalancing, max_diff)
}

/// Calculate optimal shard count for queue partitioning
///
/// # Arguments
///
/// * `total_queue_size` - Total size across all queues
/// * `target_shard_size` - Target size per shard
/// * `max_shards` - Maximum allowed shards
///
/// # Returns
///
/// Recommended number of shards
///
/// # Examples
///
/// ```
/// use celers_broker_redis::utilities::calculate_redis_optimal_shard_count;
///
/// let shards = calculate_redis_optimal_shard_count(100000, 10000, 20);
/// assert!(shards >= 1 && shards <= 20);
/// ```
pub fn calculate_redis_optimal_shard_count(
    total_queue_size: usize,
    target_shard_size: usize,
    max_shards: usize,
) -> usize {
    if target_shard_size == 0 {
        return 1;
    }

    let ideal_shards = total_queue_size.div_ceil(target_shard_size);
    ideal_shards.clamp(1, max_shards)
}

/// Calculate SLA compliance metrics
///
/// Analyzes task processing times against SLA thresholds to determine
/// compliance rates and identify violations.
///
/// # Arguments
///
/// * `processing_times` - Vec of task processing times in seconds
/// * `sla_threshold_seconds` - SLA threshold in seconds
///
/// # Returns
///
/// `SLACompliance` with compliance rate and violation details
///
/// # Examples
///
/// ```
/// use celers_broker_redis::utilities::calculate_redis_sla_compliance;
///
/// let times = vec![5.0, 10.0, 15.0, 25.0, 100.0];
/// let compliance = calculate_redis_sla_compliance(&times, 30.0);
/// assert!(compliance.compliance_rate > 0.0);
/// ```
#[allow(dead_code)]
pub fn calculate_redis_sla_compliance(
    processing_times: &[f64],
    sla_threshold_seconds: f64,
) -> SLACompliance {
    if processing_times.is_empty() {
        return SLACompliance {
            total_tasks: 0,
            tasks_within_sla: 0,
            tasks_violated_sla: 0,
            compliance_rate: 1.0,
            average_processing_time: 0.0,
            max_processing_time: 0.0,
            p95_processing_time: 0.0,
            p99_processing_time: 0.0,
        };
    }

    let total_tasks = processing_times.len();
    let tasks_within_sla = processing_times
        .iter()
        .filter(|&&t| t <= sla_threshold_seconds)
        .count();
    let tasks_violated_sla = total_tasks - tasks_within_sla;

    let compliance_rate = tasks_within_sla as f64 / total_tasks as f64;
    let average_processing_time = processing_times.iter().sum::<f64>() / total_tasks as f64;
    let max_processing_time = processing_times.iter().fold(0.0f64, |acc, &x| acc.max(x));

    // Calculate percentiles
    let mut sorted = processing_times.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let p95_index = ((total_tasks as f64 * 0.95) as usize).min(total_tasks - 1);
    let p99_index = ((total_tasks as f64 * 0.99) as usize).min(total_tasks - 1);

    SLACompliance {
        total_tasks,
        tasks_within_sla,
        tasks_violated_sla,
        compliance_rate,
        average_processing_time,
        max_processing_time,
        p95_processing_time: sorted[p95_index],
        p99_processing_time: sorted[p99_index],
    }
}

/// SLA compliance metrics
#[derive(Debug, Clone)]
pub struct SLACompliance {
    /// Total number of tasks
    pub total_tasks: usize,
    /// Tasks completed within SLA
    pub tasks_within_sla: usize,
    /// Tasks that violated SLA
    pub tasks_violated_sla: usize,
    /// Compliance rate (0.0-1.0)
    pub compliance_rate: f64,
    /// Average processing time
    pub average_processing_time: f64,
    /// Maximum processing time
    pub max_processing_time: f64,
    /// 95th percentile processing time
    pub p95_processing_time: f64,
    /// 99th percentile processing time
    pub p99_processing_time: f64,
}

/// Calculate capacity headroom
///
/// Determines how much capacity headroom remains before system saturation,
/// helping with proactive capacity planning.
///
/// # Arguments
///
/// * `current_queue_size` - Current queue size
/// * `max_queue_capacity` - Maximum queue capacity before degradation
/// * `current_throughput` - Current throughput (messages/sec)
/// * `max_throughput` - Maximum sustainable throughput
///
/// # Returns
///
/// `CapacityHeadroom` with available capacity and time-to-saturation
///
/// # Examples
///
/// ```
/// use celers_broker_redis::utilities::calculate_redis_capacity_headroom;
///
/// let headroom = calculate_redis_capacity_headroom(1000, 10000, 50.0, 100.0);
/// assert!(headroom.queue_headroom_percent > 0.0);
/// ```
#[allow(dead_code)]
pub fn calculate_redis_capacity_headroom(
    current_queue_size: usize,
    max_queue_capacity: usize,
    current_throughput: f64,
    max_throughput: f64,
) -> CapacityHeadroom {
    let queue_utilization = if max_queue_capacity > 0 {
        (current_queue_size as f64 / max_queue_capacity as f64).min(1.0)
    } else {
        0.0
    };

    let throughput_utilization = if max_throughput > 0.0 {
        (current_throughput / max_throughput).min(1.0)
    } else {
        0.0
    };

    let queue_headroom_percent = (1.0 - queue_utilization) * 100.0;
    let throughput_headroom_percent = (1.0 - throughput_utilization) * 100.0;

    // Calculate time to saturation (assuming linear growth)
    let queue_growth_rate = current_throughput; // messages per second
    let available_queue_space = max_queue_capacity.saturating_sub(current_queue_size);

    let time_to_queue_saturation_seconds = if queue_growth_rate > 0.0 {
        available_queue_space as f64 / queue_growth_rate
    } else {
        f64::INFINITY
    };

    // Overall headroom is the minimum of queue and throughput headroom
    let overall_headroom_percent = queue_headroom_percent.min(throughput_headroom_percent);

    let status = if overall_headroom_percent < 10.0 {
        "critical"
    } else if overall_headroom_percent < 30.0 {
        "warning"
    } else {
        "healthy"
    };

    CapacityHeadroom {
        queue_utilization_percent: queue_utilization * 100.0,
        throughput_utilization_percent: throughput_utilization * 100.0,
        queue_headroom_percent,
        throughput_headroom_percent,
        overall_headroom_percent,
        time_to_queue_saturation_seconds,
        status: status.to_string(),
    }
}

/// Capacity headroom metrics
#[derive(Debug, Clone)]
pub struct CapacityHeadroom {
    /// Queue utilization percentage
    pub queue_utilization_percent: f64,
    /// Throughput utilization percentage
    pub throughput_utilization_percent: f64,
    /// Queue headroom percentage
    pub queue_headroom_percent: f64,
    /// Throughput headroom percentage
    pub throughput_headroom_percent: f64,
    /// Overall headroom percentage (minimum of queue and throughput)
    pub overall_headroom_percent: f64,
    /// Time until queue saturation (seconds)
    pub time_to_queue_saturation_seconds: f64,
    /// Status (healthy/warning/critical)
    pub status: String,
}

/// Estimate when to scale based on growth rate
///
/// Predicts when scaling will be needed based on current queue growth rate
/// and capacity headroom, enabling proactive capacity planning.
///
/// # Arguments
///
/// * `current_queue_size` - Current queue size
/// * `queue_growth_rate` - Queue growth rate (messages/sec)
/// * `max_capacity` - Maximum capacity before scaling needed
/// * `scale_threshold_percent` - Threshold % to trigger scaling (e.g., 80)
///
/// # Returns
///
/// `ScalingTimeEstimate` with time until scaling and recommendations
///
/// # Examples
///
/// ```
/// use celers_broker_redis::utilities::estimate_redis_scaling_time;
///
/// let estimate = estimate_redis_scaling_time(1000, 10.0, 10000, 80.0);
/// assert!(estimate.hours_until_scaling > 0.0);
/// ```
#[allow(dead_code)]
pub fn estimate_redis_scaling_time(
    current_queue_size: usize,
    queue_growth_rate: f64,
    max_capacity: usize,
    scale_threshold_percent: f64,
) -> ScalingTimeEstimate {
    let scale_threshold = (max_capacity as f64 * scale_threshold_percent / 100.0) as usize;
    let available_capacity = scale_threshold.saturating_sub(current_queue_size);

    let seconds_until_scaling = if queue_growth_rate > 0.0 {
        available_capacity as f64 / queue_growth_rate
    } else if current_queue_size >= scale_threshold {
        0.0 // Already at threshold
    } else {
        f64::INFINITY // Not growing
    };

    let hours_until_scaling = seconds_until_scaling / 3600.0;
    let days_until_scaling = hours_until_scaling / 24.0;

    let urgency = if hours_until_scaling < 1.0 {
        "immediate"
    } else if hours_until_scaling < 6.0 {
        "urgent"
    } else if hours_until_scaling < 24.0 {
        "soon"
    } else if hours_until_scaling < 72.0 {
        "planned"
    } else {
        "monitor"
    };

    let recommendation = match urgency {
        "immediate" => "Scale NOW - threshold will be reached within 1 hour",
        "urgent" => "Prepare to scale - threshold within 6 hours",
        "soon" => "Plan scaling for today - threshold within 24 hours",
        "planned" => "Schedule scaling this week - threshold within 3 days",
        _ => "Monitor queue growth - scaling not immediately needed",
    };

    ScalingTimeEstimate {
        current_queue_size,
        scale_threshold,
        available_capacity,
        queue_growth_rate,
        seconds_until_scaling,
        hours_until_scaling,
        days_until_scaling,
        urgency: urgency.to_string(),
        recommendation: recommendation.to_string(),
    }
}

/// Scaling time estimate
#[derive(Debug, Clone)]
pub struct ScalingTimeEstimate {
    /// Current queue size
    pub current_queue_size: usize,
    /// Scaling threshold
    pub scale_threshold: usize,
    /// Available capacity before scaling
    pub available_capacity: usize,
    /// Queue growth rate (messages/sec)
    pub queue_growth_rate: f64,
    /// Seconds until scaling needed
    pub seconds_until_scaling: f64,
    /// Hours until scaling needed
    pub hours_until_scaling: f64,
    /// Days until scaling needed
    pub days_until_scaling: f64,
    /// Urgency level (immediate/urgent/soon/planned/monitor)
    pub urgency: String,
    /// Scaling recommendation
    pub recommendation: String,
}

/// Calculate queue efficiency metrics
///
/// Analyzes queue efficiency based on processing patterns, idle time,
/// and resource utilization to identify optimization opportunities.
///
/// # Arguments
///
/// * `tasks_processed` - Total tasks processed
/// * `total_processing_time` - Total time spent processing (seconds)
/// * `total_idle_time` - Total idle time (seconds)
/// * `total_elapsed_time` - Total elapsed time (seconds)
///
/// # Returns
///
/// `QueueEfficiency` with utilization and efficiency metrics
///
/// # Examples
///
/// ```
/// use celers_broker_redis::utilities::calculate_redis_queue_efficiency;
///
/// let efficiency = calculate_redis_queue_efficiency(1000, 3000.0, 600.0, 3600.0);
/// assert!(efficiency.utilization_percent > 0.0);
/// ```
#[allow(dead_code)]
pub fn calculate_redis_queue_efficiency(
    tasks_processed: usize,
    total_processing_time: f64,
    total_idle_time: f64,
    total_elapsed_time: f64,
) -> QueueEfficiency {
    let utilization_percent = if total_elapsed_time > 0.0 {
        (total_processing_time / total_elapsed_time * 100.0).min(100.0)
    } else {
        0.0
    };

    let idle_percent = if total_elapsed_time > 0.0 {
        (total_idle_time / total_elapsed_time * 100.0).min(100.0)
    } else {
        0.0
    };

    let average_task_time = if tasks_processed > 0 {
        total_processing_time / tasks_processed as f64
    } else {
        0.0
    };

    let throughput = if total_elapsed_time > 0.0 {
        tasks_processed as f64 / total_elapsed_time
    } else {
        0.0
    };

    let efficiency_score = utilization_percent / 100.0;

    let status = if utilization_percent > 90.0 {
        "optimal"
    } else if utilization_percent > 70.0 {
        "good"
    } else if utilization_percent > 50.0 {
        "moderate"
    } else {
        "poor"
    };

    let recommendation = if utilization_percent < 50.0 {
        "Low utilization - consider reducing workers or increasing task load"
    } else if utilization_percent > 95.0 {
        "Very high utilization - consider adding workers to improve resilience"
    } else {
        "Utilization is within acceptable range"
    };

    QueueEfficiency {
        tasks_processed,
        utilization_percent,
        idle_percent,
        average_task_time_seconds: average_task_time,
        throughput_per_second: throughput,
        efficiency_score,
        status: status.to_string(),
        recommendation: recommendation.to_string(),
    }
}

/// Queue efficiency metrics
#[derive(Debug, Clone)]
pub struct QueueEfficiency {
    /// Total tasks processed
    pub tasks_processed: usize,
    /// Worker utilization percentage
    pub utilization_percent: f64,
    /// Idle time percentage
    pub idle_percent: f64,
    /// Average task processing time
    pub average_task_time_seconds: f64,
    /// Throughput (tasks/sec)
    pub throughput_per_second: f64,
    /// Efficiency score (0.0-1.0)
    pub efficiency_score: f64,
    /// Status (optimal/good/moderate/poor)
    pub status: String,
    /// Optimization recommendation
    pub recommendation: String,
}

/// Recommend queue rebalancing strategy for multi-queue systems
///
/// Analyzes queue sizes and worker allocations to recommend optimal
/// task and worker distribution across multiple queues.
///
/// # Parameters
/// - `queue_sizes`: Sizes of each queue
/// - `worker_counts`: Number of workers assigned to each queue
/// - `processing_rates`: Processing rate (tasks/sec) for each queue
///
/// # Returns
/// `RebalancingRecommendation` with rebalancing strategy
///
/// # Example
/// ```
/// use celers_broker_redis::utilities::recommend_queue_rebalancing;
///
/// let queue_sizes = vec![500, 1000, 100];
/// let worker_counts = vec![10, 10, 10]; // Equal distribution
/// let processing_rates = vec![50.0, 50.0, 50.0];
///
/// let recommendation = recommend_queue_rebalancing(&queue_sizes, &worker_counts, &processing_rates);
/// assert!(recommendation.rebalancing_needed);
/// ```
#[allow(dead_code)]
pub fn recommend_queue_rebalancing(
    queue_sizes: &[usize],
    worker_counts: &[usize],
    processing_rates: &[f64],
) -> RebalancingRecommendation {
    if queue_sizes.is_empty()
        || queue_sizes.len() != worker_counts.len()
        || queue_sizes.len() != processing_rates.len()
    {
        return RebalancingRecommendation {
            rebalancing_needed: false,
            imbalance_score: 0.0,
            recommended_worker_distribution: vec![],
            queue_priorities: vec![],
            recommendations: vec!["Invalid input: arrays must have same length".to_string()],
        };
    }

    let total_workers: usize = worker_counts.iter().sum();

    if total_workers == 0 {
        return RebalancingRecommendation {
            rebalancing_needed: false,
            imbalance_score: 0.0,
            recommended_worker_distribution: vec![],
            queue_priorities: vec![],
            recommendations: vec!["No workers allocated".to_string()],
        };
    }

    // Calculate ideal worker distribution based on queue sizes and processing rates
    let mut recommended_distribution = vec![0; queue_sizes.len()];
    let mut queue_priorities = vec![0.0; queue_sizes.len()];

    // Calculate workload for each queue (size / processing_rate)
    let mut workloads = vec![0.0; queue_sizes.len()];
    let mut total_workload = 0.0;

    for i in 0..queue_sizes.len() {
        let workload = if processing_rates[i] > 0.0 {
            queue_sizes[i] as f64 / processing_rates[i]
        } else {
            queue_sizes[i] as f64
        };
        workloads[i] = workload;
        total_workload += workload;
        queue_priorities[i] = workload;
    }

    // Distribute workers proportionally to workload
    if total_workload > 0.0 {
        let mut workers_allocated = 0;
        for i in 0..queue_sizes.len() {
            let proportion = workloads[i] / total_workload;
            let ideal_workers = (proportion * total_workers as f64).round() as usize;
            recommended_distribution[i] = ideal_workers.max(1); // At least 1 worker
            workers_allocated += recommended_distribution[i];
        }

        // Adjust for rounding errors
        let diff = total_workers as i32 - workers_allocated as i32;
        if diff != 0 {
            // Add/remove workers from the queue with highest workload
            if let Some((idx, _)) = workloads
                .iter()
                .enumerate()
                .max_by(|a, b| a.1.partial_cmp(b.1).unwrap())
            {
                recommended_distribution[idx] =
                    (recommended_distribution[idx] as i32 + diff).max(1) as usize;
            }
        }
    } else {
        // Equal distribution if no workload data
        let per_queue = total_workers / queue_sizes.len();
        let remainder = total_workers % queue_sizes.len();
        for (i, dist) in recommended_distribution.iter_mut().enumerate() {
            *dist = per_queue + if i < remainder { 1 } else { 0 };
        }
    }

    // Calculate imbalance score (0-1, higher = more imbalanced)
    let mut max_deviation: f64 = 0.0;
    for i in 0..worker_counts.len() {
        let current = worker_counts[i] as f64;
        let recommended = recommended_distribution[i] as f64;
        if recommended > 0.0 {
            let deviation = (current - recommended).abs() / recommended;
            max_deviation = max_deviation.max(deviation);
        }
    }
    let imbalance_score = max_deviation.min(1.0);

    let rebalancing_needed = imbalance_score > 0.2; // >20% deviation

    // Generate recommendations
    let mut recommendations = Vec::new();
    if rebalancing_needed {
        recommendations.push(format!(
            "Rebalancing recommended - current distribution has {:.1}% imbalance",
            imbalance_score * 100.0
        ));

        for i in 0..queue_sizes.len() {
            let current = worker_counts[i];
            let recommended = recommended_distribution[i];
            if current != recommended {
                let change = recommended as i32 - current as i32;
                if change > 0 {
                    recommendations.push(format!(
                        "Queue {} needs {} more workers ({} → {})",
                        i, change, current, recommended
                    ));
                } else {
                    recommendations.push(format!(
                        "Queue {} can release {} workers ({} → {})",
                        i, -change, current, recommended
                    ));
                }
            }
        }
    } else {
        recommendations.push("Worker distribution is well-balanced".to_string());
    }

    RebalancingRecommendation {
        rebalancing_needed,
        imbalance_score,
        recommended_worker_distribution: recommended_distribution,
        queue_priorities,
        recommendations,
    }
}

/// Queue rebalancing recommendation
#[derive(Debug, Clone)]
pub struct RebalancingRecommendation {
    /// Whether rebalancing is needed
    pub rebalancing_needed: bool,
    /// Imbalance score (0.0-1.0, higher = more imbalanced)
    pub imbalance_score: f64,
    /// Recommended worker count for each queue
    pub recommended_worker_distribution: Vec<usize>,
    /// Priority scores for each queue (higher = more urgent)
    pub queue_priorities: Vec<f64>,
    /// Specific recommendations
    pub recommendations: Vec<String>,
}

/// Optimize task priority based on historical completion data
///
/// Analyzes task completion patterns to recommend priority adjustments
/// that optimize overall throughput and reduce latency.
///
/// # Parameters
/// - `task_type`: Task type identifier
/// - `current_priority`: Current priority (0-255)
/// - `avg_completion_time_ms`: Average completion time
/// - `success_rate`: Success rate (0.0-1.0)
/// - `business_value`: Business value score (0-100)
///
/// # Returns
/// `PriorityOptimization` with recommended priority
///
/// # Example
/// ```
/// use celers_broker_redis::utilities::optimize_task_priority;
///
/// let optimization = optimize_task_priority(
///     "payment_processing",
///     128,       // Current priority
///     250.0,     // 250ms avg completion
///     0.98,      // 98% success rate
///     95,        // High business value
/// );
///
/// assert!(optimization.recommended_priority > 128);
/// ```
#[allow(dead_code)]
pub fn optimize_task_priority(
    task_type: &str,
    current_priority: i32,
    avg_completion_time_ms: f64,
    success_rate: f64,
    business_value: usize,
) -> PriorityOptimization {
    // Calculate priority score based on multiple factors
    let mut priority_score = current_priority as f64;

    // Factor 1: Business value (0-100 scale)
    let business_factor = (business_value as f64 / 100.0) * 50.0;
    priority_score += business_factor;

    // Factor 2: Success rate (penalize low success rates)
    let success_factor = if success_rate < 0.8 {
        -30.0 * (1.0 - success_rate) // Reduce priority for unreliable tasks
    } else {
        10.0 * success_rate // Boost reliable tasks
    };
    priority_score += success_factor;

    // Factor 3: Completion time (faster tasks get slight boost)
    let time_factor = if avg_completion_time_ms < 100.0 {
        10.0 // Fast tasks
    } else if avg_completion_time_ms < 1000.0 {
        0.0 // Normal tasks
    } else {
        -10.0 // Slow tasks (deprioritize to avoid blocking)
    };
    priority_score += time_factor;

    // Clamp to valid priority range (0-255)
    let recommended_priority = priority_score.round().clamp(0.0, 255.0) as i32;

    let priority_change = recommended_priority - current_priority;
    let adjustment_needed = priority_change.abs() > 5; // Significant change threshold

    // Generate reasoning
    let mut reasoning = Vec::new();
    if business_value > 80 {
        reasoning.push("High business value justifies elevated priority".to_string());
    }
    if success_rate < 0.9 {
        reasoning.push(format!(
            "Low success rate ({:.1}%) reduces priority",
            success_rate * 100.0
        ));
    }
    if avg_completion_time_ms > 1000.0 {
        reasoning.push("Long completion time reduces priority to prevent blocking".to_string());
    }
    if avg_completion_time_ms < 100.0 {
        reasoning.push("Fast completion allows higher priority".to_string());
    }

    if reasoning.is_empty() {
        reasoning.push("Current priority is appropriate".to_string());
    }

    let recommendation = if priority_change > 0 {
        format!(
            "Increase priority by {} ({}→{})",
            priority_change, current_priority, recommended_priority
        )
    } else if priority_change < 0 {
        format!(
            "Decrease priority by {} ({}→{})",
            -priority_change, current_priority, recommended_priority
        )
    } else {
        "Maintain current priority".to_string()
    };

    PriorityOptimization {
        task_type: task_type.to_string(),
        current_priority,
        recommended_priority,
        priority_change,
        adjustment_needed,
        reasoning,
        recommendation,
    }
}

/// Task priority optimization results
#[derive(Debug, Clone)]
pub struct PriorityOptimization {
    /// Task type identifier
    pub task_type: String,
    /// Current priority
    pub current_priority: i32,
    /// Recommended priority
    pub recommended_priority: i32,
    /// Priority change amount
    pub priority_change: i32,
    /// Whether adjustment is recommended
    pub adjustment_needed: bool,
    /// Reasoning for recommendation
    pub reasoning: Vec<String>,
    /// Summary recommendation
    pub recommendation: String,
}

/// Calculate optimal worker distribution across multiple queues
///
/// Determines how to distribute workers across queues to maximize
/// total throughput while respecting SLA requirements.
///
/// # Parameters
/// - `queue_tasks`: Number of tasks in each queue
/// - `queue_sla_seconds`: SLA target for each queue (seconds)
/// - `total_workers`: Total available workers
/// - `worker_capacity`: Tasks per second each worker can handle
///
/// # Returns
/// `WorkerDistribution` with optimal allocation
///
/// # Example
/// ```
/// use celers_broker_redis::utilities::calculate_worker_distribution;
///
/// let tasks = vec![1000, 500, 2000];
/// let slas = vec![60, 30, 120]; // SLA in seconds
/// let distribution = calculate_worker_distribution(&tasks, &slas, 20, 10.0);
///
/// assert_eq!(distribution.worker_allocation.len(), 3);
/// assert!(distribution.worker_allocation.iter().sum::<usize>() <= 20);
/// assert!(distribution.total_throughput > 0.0);
/// ```
#[allow(dead_code)]
pub fn calculate_worker_distribution(
    queue_tasks: &[usize],
    queue_sla_seconds: &[usize],
    total_workers: usize,
    worker_capacity: f64,
) -> WorkerDistribution {
    if queue_tasks.is_empty() || queue_tasks.len() != queue_sla_seconds.len() || total_workers == 0
    {
        return WorkerDistribution {
            worker_allocation: vec![],
            queue_completion_times: vec![],
            sla_violations: vec![],
            total_throughput: 0.0,
            recommendations: vec!["Invalid parameters".to_string()],
        };
    }

    let num_queues = queue_tasks.len();
    let mut worker_allocation = vec![0; num_queues];
    let mut queue_completion_times = vec![0.0; num_queues];
    let mut sla_violations = vec![false; num_queues];

    // Calculate urgency for each queue (tasks / SLA)
    let mut urgency_scores: Vec<(usize, f64)> = queue_tasks
        .iter()
        .enumerate()
        .map(|(i, &tasks)| {
            let urgency = if queue_sla_seconds[i] > 0 {
                tasks as f64 / queue_sla_seconds[i] as f64
            } else {
                tasks as f64
            };
            (i, urgency)
        })
        .collect();

    // Sort by urgency (descending)
    urgency_scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

    // Allocate workers based on urgency, ensuring each queue gets at least 1
    let mut remaining_workers = total_workers;

    // First pass: Give 1 worker to each queue
    for allocation in worker_allocation.iter_mut().take(num_queues) {
        if remaining_workers > 0 {
            *allocation = 1;
            remaining_workers -= 1;
        }
    }

    // Second pass: Distribute remaining workers by urgency
    for (idx, _urgency) in urgency_scores {
        if remaining_workers == 0 {
            break;
        }

        let tasks = queue_tasks[idx];
        let sla = queue_sla_seconds[idx];

        // Calculate workers needed to meet SLA
        let workers_needed = if sla > 0 {
            let required_throughput = tasks as f64 / sla as f64;
            (required_throughput / worker_capacity).ceil() as usize
        } else {
            1
        };

        let additional_workers =
            (workers_needed.saturating_sub(worker_allocation[idx])).min(remaining_workers);
        worker_allocation[idx] += additional_workers;
        remaining_workers -= additional_workers;
    }

    // Calculate completion times and check SLA violations
    let mut total_throughput = 0.0;
    for i in 0..num_queues {
        let workers = worker_allocation[i];
        let tasks = queue_tasks[i];
        let sla = queue_sla_seconds[i];

        let throughput = workers as f64 * worker_capacity;
        total_throughput += throughput;

        let completion_time = if throughput > 0.0 {
            tasks as f64 / throughput
        } else {
            f64::INFINITY
        };

        queue_completion_times[i] = completion_time;
        sla_violations[i] = completion_time > sla as f64;
    }

    // Generate recommendations
    let mut recommendations = Vec::new();
    let violation_count = sla_violations.iter().filter(|&&v| v).count();

    if violation_count > 0 {
        recommendations.push(format!(
            "{} queue(s) will violate SLA with current worker distribution",
            violation_count
        ));
        for i in 0..num_queues {
            if sla_violations[i] {
                recommendations.push(format!(
                    "Queue {} needs {} more workers to meet SLA (current: {}, estimated time: {:.1}s, SLA: {}s)",
                    i,
                    ((queue_tasks[i] as f64 / queue_sla_seconds[i] as f64 / worker_capacity).ceil() as usize).saturating_sub(worker_allocation[i]),
                    worker_allocation[i],
                    queue_completion_times[i],
                    queue_sla_seconds[i]
                ));
            }
        }
    } else {
        recommendations.push("All queues will meet SLA targets with this distribution".to_string());
    }

    WorkerDistribution {
        worker_allocation,
        queue_completion_times,
        sla_violations,
        total_throughput,
        recommendations,
    }
}

/// Worker distribution across queues
#[derive(Debug, Clone)]
pub struct WorkerDistribution {
    /// Worker count allocated to each queue
    pub worker_allocation: Vec<usize>,
    /// Estimated completion time for each queue (seconds)
    pub queue_completion_times: Vec<f64>,
    /// SLA violation flags for each queue
    pub sla_violations: Vec<bool>,
    /// Total system throughput (tasks/sec)
    pub total_throughput: f64,
    /// Optimization recommendations
    pub recommendations: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_optimal_batch_size() {
        let batch_size = calculate_optimal_redis_batch_size(1000, 1024, 100);
        assert!(batch_size > 0);
        assert!(batch_size <= 1000);
    }

    #[test]
    fn test_calculate_optimal_batch_size_large_messages() {
        let batch_size = calculate_optimal_redis_batch_size(1000, 200_000, 100);
        assert!(batch_size <= 20); // Should be small for large messages
    }

    #[test]
    fn test_estimate_queue_memory_fifo() {
        let memory = estimate_redis_queue_memory(1000, 1024, QueueMode::Fifo);
        assert!(memory > 1024 * 1000); // Should be at least message size * count
    }

    #[test]
    fn test_estimate_queue_memory_priority() {
        let memory = estimate_redis_queue_memory(1000, 1024, QueueMode::Priority);
        let memory_fifo = estimate_redis_queue_memory(1000, 1024, QueueMode::Fifo);
        assert!(memory > memory_fifo); // Priority queue has more overhead
    }

    #[test]
    fn test_calculate_optimal_pool_size() {
        let pool_size = calculate_optimal_redis_pool_size(100, 50);
        assert!(pool_size >= 100);
        assert!(pool_size <= 1000);
    }

    #[test]
    fn test_calculate_pipeline_size() {
        let pipeline_size = calculate_redis_pipeline_size(100, 10);
        assert!(pipeline_size > 0);
        assert!(pipeline_size <= 100);
    }

    #[test]
    fn test_estimate_drain_time() {
        let drain_time = estimate_redis_queue_drain_time(1000, 50.0);
        assert_eq!(drain_time, 20.0);
    }

    #[test]
    fn test_estimate_drain_time_zero_rate() {
        let drain_time = estimate_redis_queue_drain_time(1000, 0.0);
        assert!(drain_time.is_infinite());
    }

    #[test]
    fn test_suggest_pipeline_strategy() {
        let strategy = suggest_redis_pipeline_strategy(5, "write");
        assert!(strategy.contains("No pipeline"));

        let strategy = suggest_redis_pipeline_strategy(50, "write");
        assert!(strategy.contains("single pipeline"));

        let strategy = suggest_redis_pipeline_strategy(1000, "write");
        assert!(strategy.contains("chunked"));
    }

    #[test]
    fn test_calculate_key_ttl_by_priority() {
        let low_priority_ttl = calculate_redis_key_ttl_by_priority(50, 3600);
        let high_priority_ttl = calculate_redis_key_ttl_by_priority(200, 3600);
        assert!(high_priority_ttl > low_priority_ttl);
    }

    #[test]
    fn test_analyze_command_performance() {
        let mut latencies = HashMap::new();
        latencies.insert("GET".to_string(), 2.0);
        latencies.insert("SET".to_string(), 3.0);
        latencies.insert("ZADD".to_string(), 15.0);

        let analysis = analyze_redis_command_performance(&latencies);
        assert_eq!(analysis.get("slowest_command"), Some(&"ZADD".to_string()));
        assert!(analysis.contains_key("avg_latency_ms"));
        assert!(analysis.contains_key("overall_status"));
    }

    #[test]
    fn test_suggest_persistence_strategy() {
        let strategy = suggest_redis_persistence_strategy(100.0, "high");
        assert!(strategy.contains("AOF"));

        let strategy = suggest_redis_persistence_strategy(2000.0, "low");
        assert!(strategy.contains("No persistence") || strategy.contains("RDB"));
    }

    #[test]
    fn test_calculate_timeout_values() {
        let (conn_timeout, op_timeout) = calculate_redis_timeout_values(50.0, 200.0);
        assert!(conn_timeout >= 1000);
        assert!(op_timeout >= 5000);
        assert!(op_timeout >= conn_timeout);
    }

    #[test]
    fn test_calculate_migration_batch_size() {
        let batch_size = calculate_redis_migration_batch_size(10000, 100.0, 120);
        assert!(batch_size >= 10);
        assert!(batch_size <= 1000);
    }

    #[test]
    fn test_estimate_migration_time() {
        let time = estimate_redis_migration_time(10000, 100, 500);
        assert!(time > 0.0);
        assert!(time < 1000.0);
    }

    #[test]
    fn test_estimate_migration_time_zero_batch() {
        let time = estimate_redis_migration_time(10000, 0, 500);
        assert!(time.is_infinite());
    }

    #[test]
    fn test_suggest_data_retention_no_cleanup() {
        let ttl = suggest_redis_data_retention(48.0, 5000, 10000);
        assert!(ttl.is_none());
    }

    #[test]
    fn test_suggest_data_retention_severe_overflow() {
        let ttl = suggest_redis_data_retention(72.0, 25000, 10000);
        assert!(ttl.is_some());
        let ttl_val = ttl.unwrap();
        assert!(ttl_val > 0);
    }

    #[test]
    fn test_analyze_queue_balance_perfect() {
        let partitions = vec![100, 100, 100, 100];
        let (score, needs_rebalance, max_diff) = analyze_redis_queue_balance(&partitions);
        assert_eq!(score, 1.0);
        assert!(!needs_rebalance);
        assert_eq!(max_diff, 0.0);
    }

    #[test]
    fn test_analyze_queue_balance_imbalanced() {
        let partitions = vec![100, 200, 50, 150];
        let (score, needs_rebalance, max_diff) = analyze_redis_queue_balance(&partitions);
        assert!(score < 1.0);
        assert!(needs_rebalance);
        assert!(max_diff > 30.0);
    }

    #[test]
    fn test_analyze_queue_balance_empty() {
        let partitions: Vec<usize> = vec![];
        let (score, needs_rebalance, max_diff) = analyze_redis_queue_balance(&partitions);
        assert_eq!(score, 1.0);
        assert!(!needs_rebalance);
        assert_eq!(max_diff, 0.0);
    }

    #[test]
    fn test_calculate_optimal_shard_count() {
        let shards = calculate_redis_optimal_shard_count(100000, 10000, 20);
        assert!(shards >= 1);
        assert!(shards <= 20);
        assert_eq!(shards, 10);
    }

    #[test]
    fn test_calculate_optimal_shard_count_zero_target() {
        let shards = calculate_redis_optimal_shard_count(100000, 0, 20);
        assert_eq!(shards, 1);
    }

    #[test]
    fn test_calculate_sla_compliance_all_within() {
        let times = vec![5.0, 10.0, 15.0, 20.0, 25.0];
        let compliance = calculate_redis_sla_compliance(&times, 30.0);
        assert_eq!(compliance.total_tasks, 5);
        assert_eq!(compliance.tasks_within_sla, 5);
        assert_eq!(compliance.tasks_violated_sla, 0);
        assert_eq!(compliance.compliance_rate, 1.0);
        assert!(compliance.p95_processing_time <= 30.0);
    }

    #[test]
    fn test_calculate_sla_compliance_violations() {
        let times = vec![5.0, 10.0, 15.0, 25.0, 100.0];
        let compliance = calculate_redis_sla_compliance(&times, 30.0);
        assert_eq!(compliance.total_tasks, 5);
        assert_eq!(compliance.tasks_within_sla, 4);
        assert_eq!(compliance.tasks_violated_sla, 1);
        assert_eq!(compliance.compliance_rate, 0.8);
        assert_eq!(compliance.max_processing_time, 100.0);
    }

    #[test]
    fn test_calculate_sla_compliance_empty() {
        let times: Vec<f64> = vec![];
        let compliance = calculate_redis_sla_compliance(&times, 30.0);
        assert_eq!(compliance.total_tasks, 0);
        assert_eq!(compliance.compliance_rate, 1.0);
    }

    #[test]
    fn test_calculate_capacity_headroom_healthy() {
        let headroom = calculate_redis_capacity_headroom(1000, 10000, 50.0, 100.0);
        assert_eq!(headroom.status, "healthy");
        assert!(headroom.queue_headroom_percent > 50.0);
        assert!(headroom.overall_headroom_percent > 30.0);
    }

    #[test]
    fn test_calculate_capacity_headroom_warning() {
        let headroom = calculate_redis_capacity_headroom(7500, 10000, 80.0, 100.0);
        assert_eq!(headroom.status, "warning");
        assert!(headroom.queue_utilization_percent > 70.0);
        assert!(headroom.overall_headroom_percent < 30.0);
    }

    #[test]
    fn test_calculate_capacity_headroom_critical() {
        let headroom = calculate_redis_capacity_headroom(9500, 10000, 95.0, 100.0);
        assert_eq!(headroom.status, "critical");
        assert!(headroom.queue_utilization_percent > 90.0);
        assert!(headroom.overall_headroom_percent < 10.0);
    }

    #[test]
    fn test_estimate_scaling_time_immediate() {
        let estimate = estimate_redis_scaling_time(7500, 100.0, 10000, 80.0);
        assert_eq!(estimate.urgency, "immediate");
        assert!(estimate.hours_until_scaling < 1.0);
        assert!(estimate.recommendation.contains("NOW"));
    }

    #[test]
    fn test_estimate_scaling_time_planned() {
        let estimate = estimate_redis_scaling_time(1000, 0.01, 10000, 80.0);
        assert_eq!(estimate.urgency, "monitor");
        assert!(estimate.hours_until_scaling > 72.0);
    }

    #[test]
    fn test_estimate_scaling_time_no_growth() {
        let estimate = estimate_redis_scaling_time(1000, 0.0, 10000, 80.0);
        assert_eq!(estimate.urgency, "monitor");
        assert!(estimate.hours_until_scaling.is_infinite());
    }

    #[test]
    fn test_calculate_queue_efficiency_optimal() {
        let efficiency = calculate_redis_queue_efficiency(1000, 3400.0, 200.0, 3600.0);
        assert_eq!(efficiency.status, "optimal");
        assert!(efficiency.utilization_percent > 90.0);
        assert!(efficiency.efficiency_score > 0.9);
    }

    #[test]
    fn test_calculate_queue_efficiency_poor() {
        let efficiency = calculate_redis_queue_efficiency(1000, 1500.0, 2100.0, 3600.0);
        assert_eq!(efficiency.status, "poor");
        assert!(efficiency.utilization_percent < 50.0);
        assert!(efficiency.idle_percent > 50.0);
    }

    #[test]
    fn test_calculate_queue_efficiency_good() {
        let efficiency = calculate_redis_queue_efficiency(1000, 2800.0, 800.0, 3600.0);
        assert_eq!(efficiency.status, "good");
        assert!(efficiency.utilization_percent > 70.0);
        assert!(efficiency.utilization_percent < 90.0);
    }
}
