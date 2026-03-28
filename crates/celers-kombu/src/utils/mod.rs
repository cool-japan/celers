//! Utility functions for broker operations and analysis.

mod analysis;
mod forecasting;

pub use analysis::*;
pub use forecasting::*;

use crate::{BrokerMetrics, Priority};
use celers_protocol::Message;

/// Calculate optimal batch size based on message size and target throughput.
///
/// # Arguments
///
/// * `avg_message_size` - Average message size in bytes
/// * `target_throughput_per_sec` - Target messages per second
/// * `max_batch_size` - Maximum allowed batch size
///
/// # Returns
///
/// Recommended batch size
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::calculate_optimal_batch_size;
///
/// let batch_size = calculate_optimal_batch_size(1024, 1000, 100);
/// assert!(batch_size > 0 && batch_size <= 100);
/// ```
pub fn calculate_optimal_batch_size(
    avg_message_size: usize,
    target_throughput_per_sec: usize,
    max_batch_size: usize,
) -> usize {
    if target_throughput_per_sec == 0 || avg_message_size == 0 {
        return 1;
    }

    // For small messages, larger batches are more efficient
    // For large messages, smaller batches reduce memory pressure
    let size_factor = if avg_message_size < 1024 {
        10 // Small messages: batch more
    } else if avg_message_size < 10240 {
        5 // Medium messages: moderate batching
    } else {
        2 // Large messages: batch less
    };

    // Calculate based on throughput (batch every 100ms)
    let throughput_factor = (target_throughput_per_sec / 10).max(1);

    // Combine factors
    let calculated = (throughput_factor / size_factor).max(1);

    // Clamp to max batch size
    calculated.min(max_batch_size)
}

/// Match a routing key against a topic pattern (AMQP-style).
///
/// Supports wildcards:
/// - `*` matches exactly one word
/// - `#` matches zero or more words
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::match_routing_pattern;
///
/// assert!(match_routing_pattern("stock.usd.nyse", "stock.*.nyse"));
/// assert!(match_routing_pattern("stock.eur.nyse", "stock.#"));
/// assert!(match_routing_pattern("quick.orange.rabbit", "*.orange.*"));
/// assert!(!match_routing_pattern("quick.orange.rabbit", "*.blue.*"));
/// ```
pub fn match_routing_pattern(routing_key: &str, pattern: &str) -> bool {
    let key_parts: Vec<&str> = routing_key.split('.').collect();
    let pattern_parts: Vec<&str> = pattern.split('.').collect();

    match_parts(&key_parts, &pattern_parts)
}

fn match_parts(key_parts: &[&str], pattern_parts: &[&str]) -> bool {
    if pattern_parts.is_empty() {
        return key_parts.is_empty();
    }

    if pattern_parts[0] == "#" {
        if pattern_parts.len() == 1 {
            return true; // # matches everything
        }
        // Try matching # with 0, 1, 2, ... words
        for i in 0..=key_parts.len() {
            if match_parts(&key_parts[i..], &pattern_parts[1..]) {
                return true;
            }
        }
        false
    } else if key_parts.is_empty() {
        false
    } else if pattern_parts[0] == "*" || pattern_parts[0] == key_parts[0] {
        match_parts(&key_parts[1..], &pattern_parts[1..])
    } else {
        false
    }
}

/// Analyze broker performance from metrics.
///
/// Returns a tuple of (success_rate, error_rate, avg_ack_rate).
///
/// # Examples
///
/// ```
/// use celers_kombu::{BrokerMetrics, utils::analyze_broker_performance};
///
/// let mut metrics = BrokerMetrics::new();
/// metrics.messages_published = 100;
/// metrics.messages_consumed = 90;
/// metrics.messages_acknowledged = 85;
/// metrics.publish_errors = 5;
///
/// let (success_rate, error_rate, ack_rate) = analyze_broker_performance(&metrics);
/// assert!(success_rate > 0.9);
/// assert!(error_rate < 0.1);
/// ```
pub fn analyze_broker_performance(metrics: &BrokerMetrics) -> (f64, f64, f64) {
    let total_ops = metrics.messages_published + metrics.messages_consumed;
    let total_errors = metrics.publish_errors + metrics.consume_errors;

    let success_rate = if total_ops > 0 {
        (total_ops - total_errors) as f64 / total_ops as f64
    } else {
        1.0
    };

    let error_rate = if total_ops > 0 {
        total_errors as f64 / total_ops as f64
    } else {
        0.0
    };

    let ack_rate = if metrics.messages_consumed > 0 {
        metrics.messages_acknowledged as f64 / metrics.messages_consumed as f64
    } else {
        0.0
    };

    (success_rate, error_rate, ack_rate)
}

/// Estimate serialized message size.
///
/// Provides a rough estimate for planning purposes.
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::estimate_message_size;
/// use celers_protocol::Message;
/// use uuid::Uuid;
///
/// let message = Message::new("my_task".to_string(), Uuid::new_v4(), vec![1, 2, 3, 4, 5]);
/// let size = estimate_message_size(&message);
/// assert!(size > 0);
/// ```
pub fn estimate_message_size(message: &Message) -> usize {
    // Base overhead for message structure
    let base = 200; // UUID, headers, metadata, etc.
    let task_name = message.task_name().len();
    let body = message.body.len();

    base + task_name + body
}

/// Analyze queue health based on size and thresholds.
///
/// Returns a health status:
/// - "healthy" if queue size is below low threshold
/// - "warning" if queue size is between low and high threshold
/// - "critical" if queue size is above high threshold
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::analyze_queue_health;
///
/// assert_eq!(analyze_queue_health(50, 100, 1000), "healthy");
/// assert_eq!(analyze_queue_health(500, 100, 1000), "warning");
/// assert_eq!(analyze_queue_health(1500, 100, 1000), "critical");
/// ```
pub fn analyze_queue_health(
    queue_size: usize,
    low_threshold: usize,
    high_threshold: usize,
) -> &'static str {
    if queue_size < low_threshold {
        "healthy"
    } else if queue_size < high_threshold {
        "warning"
    } else {
        "critical"
    }
}

/// Calculate message throughput (messages per second).
///
/// # Arguments
///
/// * `message_count` - Number of messages processed
/// * `duration_secs` - Time duration in seconds
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::calculate_throughput;
///
/// let throughput = calculate_throughput(1000, 10.0);
/// assert_eq!(throughput, 100.0);
/// ```
pub fn calculate_throughput(message_count: u64, duration_secs: f64) -> f64 {
    if duration_secs <= 0.0 {
        0.0
    } else {
        message_count as f64 / duration_secs
    }
}

/// Calculate average latency in milliseconds.
///
/// # Arguments
///
/// * `total_latency_ms` - Total latency in milliseconds
/// * `message_count` - Number of messages
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::calculate_avg_latency;
///
/// let avg = calculate_avg_latency(5000.0, 100);
/// assert_eq!(avg, 50.0);
/// ```
pub fn calculate_avg_latency(total_latency_ms: f64, message_count: u64) -> f64 {
    if message_count == 0 {
        0.0
    } else {
        total_latency_ms / message_count as f64
    }
}

/// Estimate time to drain a queue at a given consumption rate.
///
/// # Arguments
///
/// * `queue_size` - Current queue size
/// * `consumption_rate_per_sec` - Messages consumed per second
///
/// # Returns
///
/// Estimated time in seconds to drain the queue
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::estimate_drain_time;
///
/// let time = estimate_drain_time(1000, 100);
/// assert_eq!(time, 10.0);
/// ```
pub fn estimate_drain_time(queue_size: usize, consumption_rate_per_sec: usize) -> f64 {
    if consumption_rate_per_sec == 0 {
        f64::INFINITY
    } else {
        queue_size as f64 / consumption_rate_per_sec as f64
    }
}

/// Calculate exponential backoff delay with optional jitter.
///
/// # Arguments
///
/// * `attempt` - Current retry attempt (0-based)
/// * `base_delay_ms` - Base delay in milliseconds
/// * `max_delay_ms` - Maximum delay cap in milliseconds
/// * `jitter_factor` - Jitter factor (0.0 = no jitter, 1.0 = full jitter)
///
/// # Returns
///
/// Delay in milliseconds with jitter applied
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::calculate_backoff_delay;
///
/// // First retry: 100ms with no jitter
/// let delay = calculate_backoff_delay(0, 100, 60000, 0.0);
/// assert_eq!(delay, 100);
///
/// // Third retry: ~400ms (100 * 2^2) with no jitter
/// let delay = calculate_backoff_delay(2, 100, 60000, 0.0);
/// assert_eq!(delay, 400);
/// ```
pub fn calculate_backoff_delay(
    attempt: u32,
    base_delay_ms: u64,
    max_delay_ms: u64,
    jitter_factor: f64,
) -> u64 {
    use std::cmp::min;

    // Calculate exponential backoff: base * 2^attempt
    let exponential = base_delay_ms.saturating_mul(2u64.saturating_pow(attempt));
    let capped = min(exponential, max_delay_ms);

    // Apply jitter if specified
    if jitter_factor > 0.0 {
        let jitter = (capped as f64 * jitter_factor.clamp(0.0, 1.0)) as u64;
        // Simple deterministic jitter based on attempt number
        let jitter_amount = jitter / 2 + (attempt as u64 * 17) % (jitter / 2 + 1);
        capped.saturating_sub(jitter_amount)
    } else {
        capped
    }
}

/// Analyze circuit breaker state and recommend action.
///
/// Returns a tuple of (health_score, recommendation) where:
/// - health_score: 0.0 (unhealthy) to 1.0 (healthy)
/// - recommendation: suggested action
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::analyze_circuit_breaker;
///
/// let (score, recommendation) = analyze_circuit_breaker(10, 90, 100);
/// assert!(score > 0.8);
/// assert_eq!(recommendation, "healthy");
///
/// let (score, recommendation) = analyze_circuit_breaker(60, 40, 100);
/// assert!(score < 0.5);
/// assert_eq!(recommendation, "critical");
/// ```
pub fn analyze_circuit_breaker(
    failures: u64,
    successes: u64,
    total_requests: u64,
) -> (f64, &'static str) {
    if total_requests == 0 {
        return (1.0, "healthy");
    }

    let failure_rate = failures as f64 / total_requests as f64;
    let success_rate = successes as f64 / total_requests as f64;

    let health_score = success_rate;

    let recommendation = if failure_rate > 0.5 {
        "critical"
    } else if failure_rate > 0.2 {
        "warning"
    } else {
        "healthy"
    };

    (health_score, recommendation)
}

/// Calculate optimal number of consumer workers based on queue size and processing rate.
///
/// # Arguments
///
/// * `queue_size` - Current queue size
/// * `avg_processing_time_ms` - Average message processing time in milliseconds
/// * `target_drain_time_secs` - Target time to drain queue in seconds
/// * `max_workers` - Maximum number of workers allowed
///
/// # Returns
///
/// Recommended number of workers
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::calculate_optimal_workers;
///
/// let workers = calculate_optimal_workers(1000, 100, 60, 10);
/// assert!(workers > 0 && workers <= 10);
/// ```
pub fn calculate_optimal_workers(
    queue_size: usize,
    avg_processing_time_ms: u64,
    target_drain_time_secs: u64,
    max_workers: usize,
) -> usize {
    if queue_size == 0 || target_drain_time_secs == 0 || avg_processing_time_ms == 0 {
        return 1;
    }

    // Messages per second one worker can process
    let msgs_per_sec_per_worker = 1000.0 / avg_processing_time_ms as f64;

    // Total messages per second needed
    let required_throughput = queue_size as f64 / target_drain_time_secs as f64;

    // Calculate needed workers
    let needed_workers = (required_throughput / msgs_per_sec_per_worker).ceil() as usize;

    // Clamp to at least 1 and at most max_workers
    needed_workers.clamp(1, max_workers)
}

/// Generate a stable message ID for deduplication based on task name and arguments.
///
/// This creates a deterministic ID that can be used to detect duplicate messages.
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::generate_deduplication_id;
///
/// let id1 = generate_deduplication_id("my_task", b"args");
/// let id2 = generate_deduplication_id("my_task", b"args");
/// assert_eq!(id1, id2); // Same inputs = same ID
///
/// let id3 = generate_deduplication_id("my_task", b"different");
/// assert_ne!(id1, id3); // Different inputs = different ID
/// ```
pub fn generate_deduplication_id(task_name: &str, args: &[u8]) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    task_name.hash(&mut hasher);
    args.hash(&mut hasher);
    format!("{:x}", hasher.finish())
}

/// Analyze connection pool health and efficiency.
///
/// Returns a tuple of (efficiency_score, status) where:
/// - efficiency_score: 0.0 (inefficient) to 1.0 (efficient)
/// - status: pool health status
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::analyze_pool_health;
///
/// let (efficiency, status) = analyze_pool_health(8, 2, 10, 100, 5);
/// assert!(efficiency > 0.0);
/// assert!(status.len() > 0);
/// ```
#[allow(clippy::too_many_arguments)]
pub fn analyze_pool_health(
    active_connections: usize,
    idle_connections: usize,
    max_connections: usize,
    total_requests: u64,
    timeout_count: u64,
) -> (f64, &'static str) {
    let total_connections = active_connections + idle_connections;

    if max_connections == 0 {
        return (0.0, "invalid");
    }

    // Calculate utilization
    let utilization = total_connections as f64 / max_connections as f64;

    // Calculate timeout rate
    let timeout_rate = if total_requests > 0 {
        timeout_count as f64 / total_requests as f64
    } else {
        0.0
    };

    // Efficiency score: penalize high timeouts and extreme utilization
    let timeout_penalty = timeout_rate * 0.5;
    let utilization_score = if utilization > 0.9 {
        0.7 // High utilization might indicate need for more connections
    } else if utilization < 0.1 {
        0.8 // Low utilization is okay but might indicate oversized pool
    } else {
        1.0 // Good utilization
    };

    let efficiency = (utilization_score - timeout_penalty).clamp(0.0, 1.0);

    let status = if timeout_rate > 0.1 {
        "critical"
    } else if utilization > 0.95 {
        "saturated"
    } else if utilization < 0.05 {
        "underutilized"
    } else {
        "healthy"
    };

    (efficiency, status)
}

/// Calculate load distribution across multiple queues.
///
/// Returns a vector of (queue_index, recommended_workers) tuples.
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::calculate_load_distribution;
///
/// let queue_sizes = vec![100, 50, 200];
/// let distribution = calculate_load_distribution(&queue_sizes, 10);
/// assert_eq!(distribution.len(), 3);
/// assert!(distribution.iter().map(|(_, w)| w).sum::<usize>() <= 10);
/// ```
pub fn calculate_load_distribution(
    queue_sizes: &[usize],
    total_workers: usize,
) -> Vec<(usize, usize)> {
    if queue_sizes.is_empty() || total_workers == 0 {
        return vec![];
    }

    let total_messages: usize = queue_sizes.iter().sum();
    if total_messages == 0 {
        // Distribute evenly if all queues are empty
        let workers_per_queue = total_workers / queue_sizes.len();
        let remainder = total_workers % queue_sizes.len();
        return queue_sizes
            .iter()
            .enumerate()
            .map(|(idx, _)| {
                let workers = workers_per_queue + if idx < remainder { 1 } else { 0 };
                (idx, workers)
            })
            .collect();
    }

    // Distribute proportionally based on queue size
    let mut distribution: Vec<(usize, usize)> = queue_sizes
        .iter()
        .enumerate()
        .map(|(idx, &size)| {
            let proportion = size as f64 / total_messages as f64;
            let workers = (proportion * total_workers as f64).round() as usize;
            (idx, workers)
        })
        .collect();

    // Adjust to ensure total equals total_workers
    let assigned: usize = distribution.iter().map(|(_, w)| w).sum();
    if assigned < total_workers {
        // Give remaining workers to largest queues
        let diff = total_workers - assigned;
        let dist_len = distribution.len();
        for _i in 0..diff {
            if let Some(max_queue) = queue_sizes
                .iter()
                .enumerate()
                .max_by_key(|(_, &size)| size)
                .map(|(idx, _)| idx)
            {
                distribution[max_queue % dist_len].1 += 1;
            }
        }
    }

    distribution
}

/// Check if a routing key matches a direct exchange pattern.
///
/// Direct exchanges route based on exact matching.
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::match_direct_routing;
///
/// assert!(match_direct_routing("user.created", "user.created"));
/// assert!(!match_direct_routing("user.created", "user.deleted"));
/// ```
pub fn match_direct_routing(routing_key: &str, pattern: &str) -> bool {
    routing_key == pattern
}

/// Check if a routing key matches a fanout exchange.
///
/// Fanout exchanges route to all bound queues regardless of routing key.
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::match_fanout_routing;
///
/// assert!(match_fanout_routing("anything"));
/// assert!(match_fanout_routing(""));
/// ```
pub fn match_fanout_routing(_routing_key: &str) -> bool {
    true // Fanout always matches
}

/// Estimate memory usage for a queue based on message count and average size.
///
/// Returns estimated memory in bytes.
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::estimate_queue_memory;
///
/// let memory = estimate_queue_memory(1000, 1024);
/// assert!(memory > 1_000_000);
/// ```
pub fn estimate_queue_memory(message_count: usize, avg_message_size: usize) -> usize {
    // Include overhead for queue metadata and message wrappers
    let overhead_per_message = 100; // bytes
    message_count * (avg_message_size + overhead_per_message)
}

/// Calculate priority score for message processing order.
///
/// Higher scores should be processed first. Factors in priority level,
/// message age, and retry count.
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::calculate_priority_score;
/// use celers_kombu::Priority;
///
/// let score = calculate_priority_score(Priority::High, 100, 0);
/// assert!(score > 0.0);
///
/// // Older messages get higher scores
/// let old_score = calculate_priority_score(Priority::Normal, 1000, 0);
/// let new_score = calculate_priority_score(Priority::Normal, 100, 0);
/// assert!(old_score > new_score);
/// ```
pub fn calculate_priority_score(priority: Priority, age_seconds: u64, retry_count: u32) -> f64 {
    let priority_weight = match priority {
        Priority::Highest => 10.0,
        Priority::High => 7.0,
        Priority::Normal => 5.0,
        Priority::Low => 3.0,
        Priority::Lowest => 1.0,
    };

    let age_factor = (age_seconds as f64).ln().max(1.0);
    let retry_penalty = 0.9_f64.powi(retry_count as i32);

    priority_weight * age_factor * retry_penalty
}

/// Suggest optimal message batch groups based on size constraints.
///
/// Returns a vector of batch sizes that maximizes throughput while
/// staying within size limits.
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::suggest_batch_groups;
///
/// let message_sizes = vec![100, 200, 150, 300, 250, 400];
/// let groups = suggest_batch_groups(&message_sizes, 600);
/// assert!(groups.len() > 0);
/// ```
pub fn suggest_batch_groups(message_sizes: &[usize], max_batch_bytes: usize) -> Vec<Vec<usize>> {
    let mut groups = Vec::new();
    let mut current_group = Vec::new();
    let mut current_size = 0;

    for (idx, &size) in message_sizes.iter().enumerate() {
        if current_size + size <= max_batch_bytes {
            current_group.push(idx);
            current_size += size;
        } else {
            if !current_group.is_empty() {
                groups.push(current_group);
            }
            current_group = vec![idx];
            current_size = size;
        }
    }

    if !current_group.is_empty() {
        groups.push(current_group);
    }

    groups
}

/// Calculate aggregate health score from multiple metrics.
///
/// Returns a score from 0.0 (unhealthy) to 1.0 (healthy).
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::calculate_health_score;
///
/// let score = calculate_health_score(0.95, 100, 0.0, 500);
/// assert!(score > 0.8);
///
/// let bad_score = calculate_health_score(0.5, 10000, 0.3, 5000);
/// assert!(bad_score < 0.5);
/// ```
pub fn calculate_health_score(
    success_rate: f64,
    queue_size: usize,
    error_rate: f64,
    latency_ms: u64,
) -> f64 {
    // Success rate component (0-1)
    let success_component = success_rate;

    // Queue size component (1 when empty, decreases with size)
    let queue_component = if queue_size < 100 {
        1.0
    } else if queue_size < 1000 {
        0.8
    } else if queue_size < 10000 {
        0.5
    } else {
        0.2
    };

    // Error rate component (1 when no errors)
    let error_component = (1.0 - error_rate).max(0.0);

    // Latency component (1 when fast, decreases with latency)
    let latency_component = if latency_ms < 100 {
        1.0
    } else if latency_ms < 500 {
        0.8
    } else if latency_ms < 1000 {
        0.5
    } else {
        0.2
    };

    // Weighted average
    (success_component * 0.4
        + queue_component * 0.2
        + error_component * 0.2
        + latency_component * 0.2)
        .clamp(0.0, 1.0)
}

/// Identify stale messages based on age threshold.
///
/// Returns indices of messages that exceed the age threshold.
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::identify_stale_messages;
///
/// let ages = vec![10, 100, 500, 2000, 5000];
/// let stale = identify_stale_messages(&ages, 1000);
/// assert_eq!(stale, vec![3, 4]);
/// ```
pub fn identify_stale_messages(message_ages: &[u64], threshold_seconds: u64) -> Vec<usize> {
    message_ages
        .iter()
        .enumerate()
        .filter(|(_, &age)| age > threshold_seconds)
        .map(|(idx, _)| idx)
        .collect()
}

/// Predict throughput based on historical data.
///
/// Uses simple linear regression on recent throughput samples.
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::predict_throughput;
///
/// let history = vec![100.0, 110.0, 120.0, 130.0, 140.0];
/// let prediction = predict_throughput(&history, 2);
/// assert!(prediction > 140.0);
/// ```
pub fn predict_throughput(historical_throughput: &[f64], periods_ahead: usize) -> f64 {
    if historical_throughput.is_empty() {
        return 0.0;
    }

    if historical_throughput.len() < 2 {
        return historical_throughput[0];
    }

    // Calculate average rate of change
    let mut changes = Vec::new();
    for i in 1..historical_throughput.len() {
        changes.push(historical_throughput[i] - historical_throughput[i - 1]);
    }

    let avg_change = changes.iter().sum::<f64>() / changes.len() as f64;
    let last_value = historical_throughput[historical_throughput.len() - 1];

    (last_value + avg_change * periods_ahead as f64).max(0.0)
}

/// Calculate queue rebalancing recommendations.
///
/// Returns suggested message redistribution across queues to balance load.
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::calculate_rebalancing;
///
/// let queue_sizes = vec![1000, 100, 500];
/// let target_total = 1600;
/// let balanced = calculate_rebalancing(&queue_sizes, target_total);
/// assert_eq!(balanced.len(), 3);
/// assert_eq!(balanced.iter().sum::<usize>(), target_total);
/// ```
pub fn calculate_rebalancing(current_sizes: &[usize], target_total: usize) -> Vec<usize> {
    if current_sizes.is_empty() {
        return vec![];
    }

    let num_queues = current_sizes.len();
    let per_queue = target_total / num_queues;
    let remainder = target_total % num_queues;

    let mut result = vec![per_queue; num_queues];
    // Distribute remainder to first queues
    for item in result.iter_mut().take(remainder) {
        *item += 1;
    }

    result
}

/// Estimate time to process remaining messages with current rate.
///
/// Returns estimated seconds to completion.
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::estimate_completion_time;
///
/// let time = estimate_completion_time(1000, 100.0);
/// assert_eq!(time, 10);
/// ```
pub fn estimate_completion_time(remaining_messages: usize, current_rate_per_sec: f64) -> u64 {
    if current_rate_per_sec <= 0.0 {
        return u64::MAX;
    }

    (remaining_messages as f64 / current_rate_per_sec).ceil() as u64
}

/// Calculate message processing efficiency ratio.
///
/// Returns ratio of useful work (0.0 to 1.0).
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::calculate_efficiency;
///
/// let efficiency = calculate_efficiency(900, 100, 50);
/// assert!(efficiency > 0.8);
/// ```
pub fn calculate_efficiency(successful: usize, failed: usize, rejected: usize) -> f64 {
    let total = successful + failed + rejected;
    if total == 0 {
        return 1.0;
    }

    successful as f64 / total as f64
}

/// Calculate required queue capacity for a given workload.
///
/// Returns the number of messages the queue should be able to hold
/// based on production rate, consumption rate, and desired buffer time.
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::calculate_queue_capacity;
///
/// // 1000 msg/sec production, 800 msg/sec consumption, 60 sec buffer
/// let capacity = calculate_queue_capacity(1000, 800, 60);
/// assert_eq!(capacity, 12000); // (1000-800) * 60
/// ```
pub fn calculate_queue_capacity(
    production_rate_per_sec: usize,
    consumption_rate_per_sec: usize,
    buffer_duration_secs: usize,
) -> usize {
    if production_rate_per_sec <= consumption_rate_per_sec {
        // No backlog expected, minimal buffer
        production_rate_per_sec * buffer_duration_secs.min(10)
    } else {
        // Calculate backlog accumulation
        let backlog_rate = production_rate_per_sec - consumption_rate_per_sec;
        backlog_rate * buffer_duration_secs
    }
}

/// Suggest optimal partition count for distributed message queue.
///
/// Recommends partition count based on throughput requirements,
/// consumer count, and target parallelism.
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::suggest_partition_count;
///
/// let partitions = suggest_partition_count(10000, 5, 1000);
/// assert!(partitions >= 5 && partitions <= 20);
/// ```
pub fn suggest_partition_count(
    target_throughput_per_sec: usize,
    consumer_count: usize,
    max_partition_throughput: usize,
) -> usize {
    if max_partition_throughput == 0 {
        return consumer_count.max(1);
    }

    // Calculate partitions needed for throughput
    let throughput_partitions = target_throughput_per_sec.div_ceil(max_partition_throughput);

    // Ensure at least one partition per consumer
    let consumer_partitions = consumer_count;

    // Use the larger of the two, but cap at 100
    throughput_partitions.max(consumer_partitions).min(100)
}

/// Estimate operational cost for message broker usage.
///
/// Returns estimated cost units based on message volume, storage,
/// and operation type costs.
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::calculate_cost_estimate;
///
/// // 1M messages/day, 1KB average, $0.0001/msg, $0.10/GB/month
/// let monthly_cost = calculate_cost_estimate(1_000_000, 1024, 0.0001, 0.10, 30);
/// assert!(monthly_cost > 0.0);
/// ```
#[allow(clippy::too_many_arguments)]
pub fn calculate_cost_estimate(
    messages_per_day: usize,
    avg_message_size_bytes: usize,
    cost_per_message: f64,
    storage_cost_per_gb_month: f64,
    retention_days: usize,
) -> f64 {
    // Calculate message operation costs
    let daily_message_cost = messages_per_day as f64 * cost_per_message;

    // Calculate storage costs
    let daily_storage_gb =
        (messages_per_day * avg_message_size_bytes) as f64 / (1024.0 * 1024.0 * 1024.0);
    let total_storage_gb = daily_storage_gb * retention_days as f64;
    let monthly_storage_cost = total_storage_gb * storage_cost_per_gb_month / 30.0;

    // Total monthly cost
    (daily_message_cost * 30.0) + monthly_storage_cost
}

/// Analyze message size and frequency patterns.
///
/// Returns (min_size, max_size, avg_size, std_dev).
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::analyze_message_patterns;
///
/// let sizes = vec![100, 200, 150, 180, 220];
/// let (min, max, avg, std_dev) = analyze_message_patterns(&sizes);
/// assert_eq!(min, 100);
/// assert_eq!(max, 220);
/// assert!(avg > 150.0 && avg < 200.0);
/// assert!(std_dev > 0.0);
/// ```
pub fn analyze_message_patterns(message_sizes: &[usize]) -> (usize, usize, f64, f64) {
    if message_sizes.is_empty() {
        return (0, 0, 0.0, 0.0);
    }

    let min = *message_sizes.iter().min().unwrap();
    let max = *message_sizes.iter().max().unwrap();

    let sum: usize = message_sizes.iter().sum();
    let avg = sum as f64 / message_sizes.len() as f64;

    // Calculate standard deviation
    let variance: f64 = message_sizes
        .iter()
        .map(|&size| {
            let diff = size as f64 - avg;
            diff * diff
        })
        .sum::<f64>()
        / message_sizes.len() as f64;

    let std_dev = variance.sqrt();

    (min, max, avg, std_dev)
}

/// Calculate optimal buffer size for message batching.
///
/// Returns buffer size in bytes based on network MTU, message overhead,
/// and target batch efficiency.
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::calculate_buffer_size;
///
/// let buffer = calculate_buffer_size(1024, 64, 0.9);
/// assert!(buffer > 0);
/// ```
pub fn calculate_buffer_size(
    avg_message_size: usize,
    message_overhead: usize,
    target_efficiency: f64,
) -> usize {
    let effective_message_size = avg_message_size + message_overhead;

    // Network MTU is typically 1500 bytes (Ethernet) or 9000 bytes (Jumbo frames)
    let mtu = if effective_message_size < 1400 {
        1500
    } else {
        9000
    };

    // Calculate messages per packet
    let messages_per_packet = (mtu as f64 / effective_message_size as f64).floor() as usize;

    if messages_per_packet == 0 {
        return effective_message_size;
    }

    // Buffer should hold enough for target efficiency
    let packets_needed = (1.0 / (1.0 - target_efficiency.min(0.99))).ceil() as usize;

    messages_per_packet * packets_needed * effective_message_size
}

/// Estimate total memory footprint for broker operations.
///
/// Returns estimated memory usage in bytes for given queue configuration.
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::estimate_memory_footprint;
///
/// // 10 queues, 1000 messages each, 1KB average message
/// let memory = estimate_memory_footprint(10, 1000, 1024, 128);
/// assert!(memory > 10_000_000); // > 10MB
/// ```
pub fn estimate_memory_footprint(
    queue_count: usize,
    messages_per_queue: usize,
    avg_message_size: usize,
    metadata_overhead_per_msg: usize,
) -> usize {
    let per_message_total = avg_message_size + metadata_overhead_per_msg;
    let per_queue_memory = messages_per_queue * per_message_total;

    // Add per-queue overhead (data structures, indexes, etc.)
    let queue_overhead = 1024 * 64; // ~64KB per queue

    queue_count * (per_queue_memory + queue_overhead)
}

/// Suggest appropriate TTL (time-to-live) for messages based on patterns.
///
/// Returns suggested TTL in seconds based on message processing time
/// and retry characteristics.
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::suggest_ttl;
///
/// // 5 sec avg processing, 3 retries, 2x backoff
/// let ttl = suggest_ttl(5, 3, 2.0);
/// assert!(ttl > 5 * (1 + 2 + 4)); // Enough for all retries
/// ```
pub fn suggest_ttl(
    avg_processing_time_secs: u64,
    max_retries: u32,
    backoff_multiplier: f64,
) -> u64 {
    let mut total_time = avg_processing_time_secs;

    // Calculate total time including retries with exponential backoff
    for retry in 0..max_retries {
        let backoff = avg_processing_time_secs as f64 * backoff_multiplier.powi(retry as i32);
        total_time += backoff as u64;
    }

    // Add 50% safety margin
    (total_time as f64 * 1.5).ceil() as u64
}

/// Calculate acceptable replication lag for distributed queues.
///
/// Returns maximum acceptable lag in milliseconds based on
/// consistency requirements and throughput.
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::calculate_replication_lag;
///
/// let lag_ms = calculate_replication_lag(1000, 3, true);
/// assert!(lag_ms > 0);
/// ```
pub fn calculate_replication_lag(
    throughput_per_sec: usize,
    replica_count: usize,
    require_strong_consistency: bool,
) -> u64 {
    if require_strong_consistency {
        // Strong consistency requires minimal lag
        return 100; // 100ms max
    }

    // For eventual consistency, calculate based on throughput
    let messages_per_ms = throughput_per_sec as f64 / 1000.0;

    // Allow lag of up to 1000 messages per replica
    let max_lag_messages = 1000 * replica_count;

    if messages_per_ms <= 0.0 {
        return 10000; // 10 seconds default
    }

    let lag_ms = (max_lag_messages as f64 / messages_per_ms).ceil() as u64;

    // Cap between 100ms and 60 seconds
    lag_ms.clamp(100, 60000)
}

/// Calculate network bandwidth required for message broker operations.
///
/// Returns bandwidth in bytes per second.
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::calculate_bandwidth_requirement;
///
/// // 1000 msg/sec, 1KB average, with protocol overhead
/// let bandwidth = calculate_bandwidth_requirement(1000, 1024, 1.2);
/// assert_eq!(bandwidth, 1228800); // 1000 * 1024 * 1.2
/// ```
pub fn calculate_bandwidth_requirement(
    messages_per_sec: usize,
    avg_message_size: usize,
    protocol_overhead_factor: f64,
) -> usize {
    let raw_bandwidth = messages_per_sec * avg_message_size;
    (raw_bandwidth as f64 * protocol_overhead_factor) as usize
}

/// Suggest retry policy based on error patterns.
///
/// Returns (max_retries, initial_delay_ms, max_delay_ms).
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::suggest_retry_policy;
///
/// let (max_retries, initial_delay, max_delay) = suggest_retry_policy(0.1, 5);
/// assert!(max_retries > 0);
/// assert!(initial_delay > 0);
/// assert!(max_delay > initial_delay);
/// ```
pub fn suggest_retry_policy(failure_rate: f64, avg_recovery_time_secs: u64) -> (u32, u64, u64) {
    // Higher failure rate = more retries
    let max_retries = if failure_rate > 0.3 {
        10
    } else if failure_rate > 0.1 {
        5
    } else {
        3
    };

    // Initial delay based on recovery time
    let initial_delay_ms = (avg_recovery_time_secs * 100).max(100); // At least 100ms

    // Max delay should be reasonable (not more than 5 minutes)
    let max_delay_ms = (avg_recovery_time_secs * 1000 * 5).min(300000);

    (max_retries, initial_delay_ms, max_delay_ms)
}

/// Analyze consumer lag across queue metrics.
///
/// # Arguments
///
/// * `queue_size` - Current queue depth
/// * `consumption_rate_per_sec` - Current consumption rate (messages/sec)
/// * `production_rate_per_sec` - Current production rate (messages/sec)
///
/// # Returns
///
/// Tuple of (lag_seconds, is_falling_behind, recommended_action)
/// - lag_seconds: Estimated lag in seconds
/// - is_falling_behind: true if producers outpace consumers
/// - recommended_action: "scale_up", "stable", or "scale_down"
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::analyze_consumer_lag;
///
/// let (lag, falling_behind, action) = analyze_consumer_lag(1000, 50, 100);
/// assert!(lag > 0);
/// assert!(falling_behind); // Producers (100/s) > Consumers (50/s)
/// assert_eq!(action, "scale_up");
///
/// let (lag, falling_behind, action) = analyze_consumer_lag(100, 100, 50);
/// assert!(!falling_behind); // Consumers keeping up
/// ```
pub fn analyze_consumer_lag(
    queue_size: usize,
    consumption_rate_per_sec: usize,
    production_rate_per_sec: usize,
) -> (u64, bool, &'static str) {
    let lag_seconds = if consumption_rate_per_sec > 0 {
        (queue_size as f64 / consumption_rate_per_sec as f64).ceil() as u64
    } else {
        u64::MAX // Infinite lag if no consumption
    };

    let is_falling_behind = production_rate_per_sec > consumption_rate_per_sec;

    let recommended_action = if is_falling_behind && lag_seconds > 10 {
        "scale_up" // Significant lag and falling behind
    } else if !is_falling_behind
        && queue_size < 100
        && consumption_rate_per_sec > production_rate_per_sec * 2
    {
        "scale_down" // Over-provisioned
    } else {
        "stable" // Balanced
    };

    (lag_seconds, is_falling_behind, recommended_action)
}

/// Calculate message velocity (rate of change in queue size).
///
/// # Arguments
///
/// * `current_size` - Current queue size
/// * `previous_size` - Previous queue size
/// * `time_window_secs` - Time window between measurements (seconds)
///
/// # Returns
///
/// Tuple of (velocity, trend)
/// - velocity: Messages per second change rate (positive = growing, negative = shrinking)
/// - trend: "growing", "shrinking", or "stable"
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::calculate_message_velocity;
///
/// // Queue grew from 100 to 200 in 10 seconds
/// let (velocity, trend) = calculate_message_velocity(200, 100, 10);
/// assert_eq!(velocity, 10); // +10 messages/sec
/// assert_eq!(trend, "growing");
///
/// // Queue shrunk from 200 to 100 in 10 seconds
/// let (velocity, trend) = calculate_message_velocity(100, 200, 10);
/// assert_eq!(velocity, -10); // -10 messages/sec
/// assert_eq!(trend, "shrinking");
/// ```
pub fn calculate_message_velocity(
    current_size: usize,
    previous_size: usize,
    time_window_secs: u64,
) -> (i64, &'static str) {
    if time_window_secs == 0 {
        return (0, "stable");
    }

    let delta = current_size as i64 - previous_size as i64;
    let velocity = delta / time_window_secs as i64;

    let trend = if velocity > 5 {
        "growing"
    } else if velocity < -5 {
        "shrinking"
    } else {
        "stable"
    };

    (velocity, trend)
}

/// Suggest worker scaling based on queue metrics.
///
/// # Arguments
///
/// * `queue_size` - Current queue depth
/// * `current_workers` - Current number of workers
/// * `avg_processing_time_ms` - Average message processing time (milliseconds)
/// * `target_latency_secs` - Target maximum latency (seconds)
///
/// # Returns
///
/// Tuple of (recommended_workers, scaling_action)
/// - recommended_workers: Suggested number of workers
/// - scaling_action: "add", "remove", or "maintain"
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::suggest_worker_scaling;
///
/// // Large queue, need more workers
/// let (workers, action) = suggest_worker_scaling(5000, 5, 100, 60);
/// assert!(workers > 5);
/// assert_eq!(action, "add");
///
/// // Small queue, can reduce workers
/// let (workers, action) = suggest_worker_scaling(10, 10, 100, 60);
/// assert!(workers < 10);
/// assert_eq!(action, "remove");
/// ```
#[allow(clippy::comparison_chain)]
pub fn suggest_worker_scaling(
    queue_size: usize,
    current_workers: usize,
    avg_processing_time_ms: u64,
    target_latency_secs: u64,
) -> (usize, &'static str) {
    if avg_processing_time_ms == 0 || target_latency_secs == 0 {
        return (current_workers.max(1), "maintain");
    }

    // Calculate required throughput: messages that need processing within target latency
    let messages_per_worker_per_sec = 1000 / avg_processing_time_ms.max(1);

    // Calculate required workers to process queue within target latency
    let required_throughput = (queue_size as f64 / target_latency_secs as f64).ceil() as u64;
    let recommended_workers = if messages_per_worker_per_sec > 0 {
        ((required_throughput as f64 / messages_per_worker_per_sec as f64).ceil() as usize).max(1)
    } else {
        current_workers.max(1)
    };

    // Add safety margin (20% headroom)
    let recommended_workers = ((recommended_workers as f64 * 1.2).ceil() as usize).max(1);

    let scaling_action = if recommended_workers > current_workers {
        "add"
    } else if recommended_workers < current_workers {
        "remove"
    } else {
        "maintain"
    };

    (recommended_workers, scaling_action)
}

/// Calculate message age distribution statistics.
///
/// # Arguments
///
/// * `message_ages_secs` - Slice of message ages in seconds
///
/// # Returns
///
/// Tuple of (p50, p95, p99, max_age)
/// - p50: 50th percentile (median) age in seconds
/// - p95: 95th percentile age in seconds
/// - p99: 99th percentile age in seconds
/// - max_age: Maximum age in seconds
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::calculate_message_age_distribution;
///
/// let ages = vec![1, 2, 3, 4, 5, 10, 20, 30, 40, 100];
/// let (p50, p95, p99, max) = calculate_message_age_distribution(&ages);
/// assert!(p50 <= p95);
/// assert!(p95 <= p99);
/// assert!(p99 <= max);
/// assert_eq!(max, 100);
/// ```
pub fn calculate_message_age_distribution(message_ages_secs: &[u64]) -> (u64, u64, u64, u64) {
    if message_ages_secs.is_empty() {
        return (0, 0, 0, 0);
    }

    let mut sorted_ages = message_ages_secs.to_vec();
    sorted_ages.sort_unstable();

    let len = sorted_ages.len();
    let p50_idx = (len as f64 * 0.50) as usize;
    let p95_idx = (len as f64 * 0.95) as usize;
    let p99_idx = (len as f64 * 0.99) as usize;

    let p50 = sorted_ages.get(p50_idx.min(len - 1)).copied().unwrap_or(0);
    let p95 = sorted_ages.get(p95_idx.min(len - 1)).copied().unwrap_or(0);
    let p99 = sorted_ages.get(p99_idx.min(len - 1)).copied().unwrap_or(0);
    let max_age = sorted_ages.last().copied().unwrap_or(0);

    (p50, p95, p99, max_age)
}

/// Estimate processing capacity of the system.
///
/// # Arguments
///
/// * `num_workers` - Number of worker processes
/// * `avg_processing_time_ms` - Average message processing time (milliseconds)
/// * `concurrency_per_worker` - Number of concurrent tasks per worker
///
/// # Returns
///
/// Tuple of (capacity_per_sec, capacity_per_min, capacity_per_hour)
/// - capacity_per_sec: Theoretical messages per second
/// - capacity_per_min: Theoretical messages per minute
/// - capacity_per_hour: Theoretical messages per hour
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::estimate_processing_capacity;
///
/// // 10 workers, 100ms per message, 4 concurrent tasks each
/// let (per_sec, per_min, per_hour) = estimate_processing_capacity(10, 100, 4);
/// assert_eq!(per_sec, 400); // 10 * 4 * (1000/100)
/// assert_eq!(per_min, 24000);
/// assert_eq!(per_hour, 1440000);
/// ```
pub fn estimate_processing_capacity(
    num_workers: usize,
    avg_processing_time_ms: u64,
    concurrency_per_worker: usize,
) -> (u64, u64, u64) {
    if avg_processing_time_ms == 0 || num_workers == 0 || concurrency_per_worker == 0 {
        return (0, 0, 0);
    }

    // Messages per second per concurrent task
    let msgs_per_sec_per_task = 1000 / avg_processing_time_ms;

    // Total capacity
    let total_tasks = num_workers * concurrency_per_worker;
    let capacity_per_sec = msgs_per_sec_per_task * total_tasks as u64;
    let capacity_per_min = capacity_per_sec * 60;
    let capacity_per_hour = capacity_per_min * 60;

    (capacity_per_sec, capacity_per_min, capacity_per_hour)
}

/// Detect anomalies in message patterns
///
/// Analyzes message flow patterns to detect anomalies such as sudden spikes,
/// drops, or unusual patterns that may indicate issues.
///
/// Returns (is_anomaly, severity, description) where severity is 0.0-1.0
///
/// # Examples
///
/// ```
/// use celers_kombu::utils;
///
/// let current = vec![100, 105, 98, 102, 500]; // Spike at the end
/// let baseline = vec![100, 105, 98, 102, 100];
/// let (is_anomaly, severity, desc) = utils::detect_anomalies(&current, &baseline, 2.0);
/// assert!(is_anomaly);
/// assert!(severity > 0.5);
/// ```
pub fn detect_anomalies(
    current_samples: &[u64],
    baseline_samples: &[u64],
    threshold_multiplier: f64,
) -> (bool, f64, String) {
    if current_samples.is_empty() || baseline_samples.is_empty() {
        return (false, 0.0, "Insufficient data".to_string());
    }

    // Calculate baseline statistics
    let baseline_avg = baseline_samples.iter().sum::<u64>() as f64 / baseline_samples.len() as f64;
    let baseline_variance = baseline_samples
        .iter()
        .map(|&x| {
            let diff = x as f64 - baseline_avg;
            diff * diff
        })
        .sum::<f64>()
        / baseline_samples.len() as f64;
    let baseline_stddev = baseline_variance.sqrt();

    // Calculate current statistics
    let current_avg = current_samples.iter().sum::<u64>() as f64 / current_samples.len() as f64;

    // Check for anomaly
    let deviation = (current_avg - baseline_avg).abs();
    let threshold = baseline_stddev * threshold_multiplier;

    if deviation > threshold {
        let severity = (deviation / (baseline_stddev * 3.0)).min(1.0);
        let direction = if current_avg > baseline_avg {
            "spike"
        } else {
            "drop"
        };
        let description = format!(
            "Anomaly detected: {} in message rate (current: {:.0}, baseline: {:.0}, deviation: {:.0})",
            direction, current_avg, baseline_avg, deviation
        );
        (true, severity, description)
    } else {
        (false, 0.0, "Normal pattern".to_string())
    }
}

/// Calculate SLA compliance metrics
///
/// Calculates SLA compliance based on processing time targets.
///
/// Returns (compliance_percentage, violations_count, avg_processing_time)
///
/// # Examples
///
/// ```
/// use celers_kombu::utils;
///
/// let processing_times = vec![100, 150, 200, 120, 90];
/// let sla_target_ms = 180;
/// let (compliance, violations, avg) = utils::calculate_sla_compliance(&processing_times, sla_target_ms);
/// assert!(compliance > 0.0);
/// assert_eq!(violations, 1); // Only 200ms exceeds target
/// ```
pub fn calculate_sla_compliance(
    processing_times_ms: &[u64],
    sla_target_ms: u64,
) -> (f64, usize, f64) {
    if processing_times_ms.is_empty() {
        return (100.0, 0, 0.0);
    }

    let violations = processing_times_ms
        .iter()
        .filter(|&&t| t > sla_target_ms)
        .count();

    let compliance = 100.0 * (1.0 - (violations as f64 / processing_times_ms.len() as f64));

    let avg_time =
        processing_times_ms.iter().sum::<u64>() as f64 / processing_times_ms.len() as f64;

    (compliance, violations, avg_time)
}

/// Estimate cost for message processing
///
/// Calculates estimated infrastructure cost based on message volume and pricing model.
///
/// Returns estimated cost for the period
///
/// # Examples
///
/// ```
/// use celers_kombu::utils;
///
/// let messages_per_day = 1_000_000;
/// let cost_per_million = 0.50; // $0.50 per million messages
/// let days = 30;
/// let cost = utils::estimate_infrastructure_cost(messages_per_day, cost_per_million, days);
/// assert_eq!(cost, 15.0); // $15 for 30 days
/// ```
pub fn estimate_infrastructure_cost(
    messages_per_day: u64,
    cost_per_million_messages: f64,
    days: u32,
) -> f64 {
    let total_messages = messages_per_day * days as u64;
    let cost = (total_messages as f64 / 1_000_000.0) * cost_per_million_messages;
    (cost * 100.0).round() / 100.0 // Round to 2 decimal places
}

/// Calculate error budget remaining
///
/// Calculates the remaining error budget based on SLA target and actual errors.
///
/// Returns (budget_remaining_percentage, errors_allowed, time_to_exhaustion_hours)
///
/// # Examples
///
/// ```
/// use celers_kombu::utils;
///
/// let sla_target = 99.9; // 99.9% uptime
/// let total_requests = 100_000;
/// let failed_requests = 50;
/// let requests_per_hour = 10_000;
/// let (budget, allowed, hours) = utils::calculate_error_budget(
///     sla_target,
///     total_requests,
///     failed_requests,
///     requests_per_hour
/// );
/// assert!(budget >= 0.0);
/// ```
pub fn calculate_error_budget(
    sla_target_percentage: f64,
    total_requests: u64,
    failed_requests: u64,
    requests_per_hour: u64,
) -> (f64, u64, f64) {
    if total_requests == 0 || requests_per_hour == 0 {
        return (100.0, 0, 0.0);
    }

    let error_budget = 100.0 - sla_target_percentage;
    let current_error_rate = (failed_requests as f64 / total_requests as f64) * 100.0;
    let budget_remaining = ((error_budget - current_error_rate) / error_budget * 100.0).max(0.0);

    // Calculate remaining errors allowed
    let total_errors_allowed = (total_requests as f64 * (error_budget / 100.0)) as u64;
    let errors_remaining = total_errors_allowed.saturating_sub(failed_requests);

    // Time to exhaust budget at current error rate
    let time_to_exhaustion = if current_error_rate > 0.0 {
        (errors_remaining as f64 / (current_error_rate / 100.0)) / requests_per_hour as f64
    } else {
        f64::INFINITY
    };

    (budget_remaining, errors_remaining, time_to_exhaustion)
}

/// Predict queue saturation time
///
/// Predicts when a queue will reach capacity based on current growth rate.
///
/// Returns (hours_to_saturation, messages_per_hour_growth)
///
/// # Examples
///
/// ```
/// use celers_kombu::utils;
///
/// let samples = vec![1000, 1100, 1200, 1300]; // Growing by 100/sample
/// let max_capacity = 2000;
/// let hours_per_sample = 1.0;
/// let (hours, growth) = utils::predict_queue_saturation(&samples, max_capacity, hours_per_sample);
/// assert!(hours > 0.0);
/// assert!(growth > 0.0);
/// ```
pub fn predict_queue_saturation(
    historical_sizes: &[u64],
    max_capacity: u64,
    hours_per_sample: f64,
) -> (f64, f64) {
    if historical_sizes.len() < 2 {
        return (f64::INFINITY, 0.0);
    }

    let current_size = *historical_sizes.last().unwrap();
    if current_size >= max_capacity {
        return (0.0, 0.0);
    }

    // Calculate growth rate using linear regression
    let n = historical_sizes.len() as f64;
    let sum_x: f64 = (0..historical_sizes.len()).map(|i| i as f64).sum();
    let sum_y: f64 = historical_sizes.iter().map(|&s| s as f64).sum();
    let sum_xy: f64 = historical_sizes
        .iter()
        .enumerate()
        .map(|(i, &s)| i as f64 * s as f64)
        .sum();
    let sum_x_squared: f64 = (0..historical_sizes.len())
        .map(|i| (i as f64) * (i as f64))
        .sum();

    let slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x_squared - sum_x * sum_x);
    let growth_per_sample = slope;
    let growth_per_hour = growth_per_sample / hours_per_sample;

    if growth_per_hour <= 0.0 {
        return (f64::INFINITY, growth_per_hour);
    }

    let remaining_capacity = max_capacity.saturating_sub(current_size) as f64;
    let hours_to_saturation = remaining_capacity / growth_per_hour;

    (hours_to_saturation, growth_per_hour)
}

/// Calculate consumer efficiency
///
/// Measures how efficiently consumers are processing messages by comparing
/// active processing time vs total wait time.
///
/// Returns (efficiency_percentage, recommendation) where:
/// - efficiency_percentage: 0.0 (very inefficient) to 100.0 (very efficient)
/// - recommendation: suggested action
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::calculate_consumer_efficiency;
///
/// // Consumer spends 80% time processing, 20% waiting
/// let (efficiency, recommendation) = calculate_consumer_efficiency(8000, 2000, 100);
/// assert!(efficiency > 70.0);
/// assert_eq!(recommendation, "efficient");
///
/// // Consumer spends 30% time processing, 70% waiting
/// let (efficiency, recommendation) = calculate_consumer_efficiency(3000, 7000, 100);
/// assert!(efficiency < 50.0);
/// assert_eq!(recommendation, "underutilized");
/// ```
pub fn calculate_consumer_efficiency(
    processing_time_ms: u64,
    wait_time_ms: u64,
    messages_processed: u64,
) -> (f64, &'static str) {
    if messages_processed == 0 {
        return (0.0, "no_data");
    }

    let total_time = processing_time_ms + wait_time_ms;
    if total_time == 0 {
        return (0.0, "no_data");
    }

    // Efficiency is the percentage of time spent actively processing
    let efficiency_percentage = (processing_time_ms as f64 / total_time as f64) * 100.0;

    // Calculate average processing time per message
    let avg_processing_ms = processing_time_ms as f64 / messages_processed as f64;

    let recommendation = if efficiency_percentage >= 80.0 {
        "efficient" // High efficiency - consumers are busy processing
    } else if efficiency_percentage >= 60.0 {
        "good" // Good efficiency - reasonable balance
    } else if efficiency_percentage >= 40.0 {
        "fair" // Fair efficiency - room for improvement
    } else if avg_processing_ms < 10.0 {
        "bottleneck" // Low efficiency due to messages being too simple
    } else {
        "underutilized" // Low efficiency - consumers are mostly waiting
    };

    (efficiency_percentage, recommendation)
}
