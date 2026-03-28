//\! Advanced forecasting and pattern analysis utility functions.

/// Analyze message flow patterns for anomaly detection.
///
/// Detects unusual patterns in message flow that might indicate issues.
///
/// # Arguments
///
/// * `recent_counts` - Recent message counts (time series)
/// * `window_size` - Size of the analysis window
///
/// # Returns
///
/// Tuple of (is_anomaly, severity, pattern_type)
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::analyze_message_flow_pattern;
///
/// let counts = vec![100, 105, 95, 103];  // Normal variation
/// let (is_anom, _, _) = analyze_message_flow_pattern(&counts, 5);
/// assert_eq!(is_anom, false);
/// ```
pub fn analyze_message_flow_pattern(
    recent_counts: &[usize],
    window_size: usize,
) -> (bool, &'static str, &'static str) {
    if recent_counts.is_empty() || window_size == 0 {
        return (false, "none", "insufficient_data");
    }

    let len = recent_counts.len().min(window_size);
    let window = &recent_counts[recent_counts.len().saturating_sub(len)..];

    if window.len() < 3 {
        return (false, "none", "insufficient_data");
    }

    // Calculate mean and standard deviation
    let mean = window.iter().sum::<usize>() as f64 / window.len() as f64;
    let variance = window
        .iter()
        .map(|&x| {
            let diff = x as f64 - mean;
            diff * diff
        })
        .sum::<f64>()
        / window.len() as f64;
    let std_dev = variance.sqrt();

    // Check for anomalies (values >= 2 std devs from mean)
    let last_value = window[window.len() - 1] as f64;
    let deviation = (last_value - mean).abs();

    if deviation >= 3.0 * std_dev {
        // Severe anomaly
        let pattern = if last_value > mean {
            "sudden_spike"
        } else {
            "sudden_drop"
        };
        (true, "high", pattern)
    } else if deviation >= 2.0 * std_dev {
        // Moderate anomaly
        let pattern = if last_value > mean {
            "moderate_increase"
        } else {
            "moderate_decrease"
        };
        (true, "medium", pattern)
    } else {
        // Check for gradual trends
        let is_increasing = window.windows(2).all(|w| w[1] >= w[0]);
        let is_decreasing = window.windows(2).all(|w| w[1] <= w[0]);

        if is_increasing {
            (false, "low", "gradual_increase")
        } else if is_decreasing {
            (false, "low", "gradual_decrease")
        } else {
            (false, "none", "normal")
        }
    }
}

/// Estimate optimal worker pool size for given workload.
///
/// Calculates the ideal number of workers based on message rate,
/// processing time, and target latency.
///
/// # Arguments
///
/// * `avg_msg_per_sec` - Average messages per second
/// * `avg_processing_ms` - Average processing time in milliseconds
/// * `target_latency_ms` - Target latency in milliseconds
/// * `worker_overhead_pct` - Worker overhead percentage (0-100)
///
/// # Returns
///
/// Tuple of (optimal_workers, max_throughput, utilization)
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::estimate_optimal_worker_pool;
///
/// let (workers, throughput, util) = estimate_optimal_worker_pool(100, 50, 100, 10);
/// assert!(workers > 0);
/// assert!(throughput > 0.0);
/// ```
pub fn estimate_optimal_worker_pool(
    avg_msg_per_sec: usize,
    avg_processing_ms: usize,
    target_latency_ms: usize,
    worker_overhead_pct: u8,
) -> (usize, f64, f64) {
    if avg_msg_per_sec == 0 || avg_processing_ms == 0 {
        return (1, 0.0, 0.0);
    }

    // Calculate base workers needed for throughput
    let processing_time_sec = avg_processing_ms as f64 / 1000.0;
    let base_workers = (avg_msg_per_sec as f64 * processing_time_sec).ceil() as usize;

    // Adjust for overhead
    let overhead_factor = 1.0 + (worker_overhead_pct as f64 / 100.0);
    let adjusted_workers = (base_workers as f64 * overhead_factor).ceil() as usize;

    // Add buffer for latency target
    let latency_buffer = if target_latency_ms < avg_processing_ms {
        // Need more workers to meet latency target
        let ratio = avg_processing_ms as f64 / target_latency_ms as f64;
        (adjusted_workers as f64 * ratio).ceil() as usize
    } else {
        adjusted_workers
    };

    let optimal_workers = latency_buffer.max(1);

    // Calculate max throughput with this worker count
    let worker_capacity = 1000.0 / avg_processing_ms as f64; // msgs/sec per worker
    let max_throughput = worker_capacity * optimal_workers as f64 / overhead_factor;

    // Calculate utilization
    let utilization = (avg_msg_per_sec as f64 / max_throughput).min(1.0);

    (optimal_workers, max_throughput, utilization)
}

/// Analyze compression benefit for messages
///
/// Determines if compression would be beneficial based on message characteristics.
/// Returns (should_compress, estimated_ratio, recommendation).
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::analyze_compression_benefit;
///
/// let (should_compress, ratio, rec) = analyze_compression_benefit(1024, 2000, "application/json");
/// assert!(should_compress);
/// assert!(ratio > 0.0);
/// ```
pub fn analyze_compression_benefit(
    avg_message_size: usize,
    message_count_per_sec: usize,
    content_type: &str,
) -> (bool, f64, String) {
    // Content types that compress well
    let compressible_types = [
        "application/json",
        "text/plain",
        "text/html",
        "text/xml",
        "application/xml",
    ];

    let is_compressible = compressible_types.iter().any(|&t| content_type.contains(t));

    if !is_compressible {
        return (false, 1.0, "content_type_not_compressible".to_string());
    }

    // Small messages (< 500 bytes) often have overhead exceeding benefits
    if avg_message_size < 500 {
        return (false, 1.0, "message_too_small".to_string());
    }

    // Estimate compression ratio based on content type
    let estimated_ratio = if content_type.contains("json") {
        0.3 // JSON typically compresses to 30% of original
    } else if content_type.contains("xml") {
        0.25 // XML compresses even better
    } else if content_type.contains("text") {
        0.4 // Plain text compression
    } else {
        0.5
    };

    // Calculate bytes saved per second
    let bytes_per_sec = avg_message_size * message_count_per_sec;
    let savings_per_sec = (bytes_per_sec as f64 * (1.0 - estimated_ratio)) as usize;

    // Recommend compression if savings > 1MB/sec or message size > 10KB
    let should_compress = savings_per_sec > 1_000_000 || avg_message_size > 10_000;

    let recommendation = if should_compress {
        format!("enable_compression_saves_{}_bytes_per_sec", savings_per_sec)
    } else {
        "compression_overhead_not_worth_it".to_string()
    };

    (should_compress, estimated_ratio, recommendation)
}

/// Calculate queue migration plan
///
/// Plans the migration of messages from one queue to another.
/// Returns (batches, total_time_secs, recommendation).
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::calculate_queue_migration_plan;
///
/// let (batches, time, rec) = calculate_queue_migration_plan(10000, 100, 50);
/// assert!(batches > 0);
/// assert!(time > 0);
/// ```
pub fn calculate_queue_migration_plan(
    message_count: usize,
    batch_size: usize,
    messages_per_sec: usize,
) -> (usize, usize, String) {
    if message_count == 0 || batch_size == 0 {
        return (0, 0, "no_migration_needed".to_string());
    }

    let safe_rate = messages_per_sec.max(1);

    // Calculate number of batches needed
    let batches = (message_count as f64 / batch_size as f64).ceil() as usize;

    // Calculate total migration time
    let total_time_secs = message_count / safe_rate;

    // Generate recommendation based on migration characteristics
    let recommendation = if total_time_secs < 60 {
        "fast_migration_proceed".to_string()
    } else if total_time_secs < 3600 {
        format!("moderate_migration_{}_minutes", total_time_secs / 60)
    } else {
        format!(
            "slow_migration_{}_hours_consider_increasing_rate",
            total_time_secs / 3600
        )
    };

    (batches, total_time_secs, recommendation)
}

/// Profile message patterns for detailed analysis
///
/// Analyzes message patterns to provide insights for optimization.
/// Returns (pattern_type, consistency_score, recommendation).
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::profile_message_patterns;
///
/// let sizes = vec![100, 105, 98, 102];
/// let intervals = vec![10, 11, 9, 10];
/// let (pattern, score, rec) = profile_message_patterns(&sizes, &intervals);
/// assert!(score >= 0.0 && score <= 1.0);
/// ```
pub fn profile_message_patterns(
    message_sizes: &[usize],
    arrival_intervals_ms: &[usize],
) -> (String, f64, String) {
    if message_sizes.is_empty() || arrival_intervals_ms.is_empty() {
        return ("unknown".to_string(), 0.0, "insufficient_data".to_string());
    }

    // Calculate size variance
    let avg_size = message_sizes.iter().sum::<usize>() as f64 / message_sizes.len() as f64;
    let size_variance = message_sizes
        .iter()
        .map(|&s| {
            let diff = s as f64 - avg_size;
            diff * diff
        })
        .sum::<f64>()
        / message_sizes.len() as f64;
    let size_std_dev = size_variance.sqrt();
    let size_cv = if avg_size > 0.0 {
        size_std_dev / avg_size
    } else {
        0.0
    };

    // Calculate interval variance
    let avg_interval =
        arrival_intervals_ms.iter().sum::<usize>() as f64 / arrival_intervals_ms.len() as f64;
    let interval_variance = arrival_intervals_ms
        .iter()
        .map(|&i| {
            let diff = i as f64 - avg_interval;
            diff * diff
        })
        .sum::<f64>()
        / arrival_intervals_ms.len() as f64;
    let interval_std_dev = interval_variance.sqrt();
    let interval_cv = if avg_interval > 0.0 {
        interval_std_dev / avg_interval
    } else {
        0.0
    };

    // Determine pattern type based on consistency
    let pattern_type = if size_cv < 0.1 && interval_cv < 0.1 {
        "highly_regular".to_string()
    } else if size_cv < 0.3 && interval_cv < 0.3 {
        "regular".to_string()
    } else if size_cv > 0.8 || interval_cv > 0.8 {
        "highly_irregular".to_string()
    } else {
        "moderately_irregular".to_string()
    };

    // Calculate consistency score (0.0 = very inconsistent, 1.0 = very consistent)
    let consistency_score = 1.0 - ((size_cv + interval_cv) / 2.0).min(1.0);

    // Generate recommendation
    let recommendation = if consistency_score > 0.8 {
        "predictable_use_static_buffers".to_string()
    } else if consistency_score > 0.5 {
        "moderate_use_adaptive_buffers".to_string()
    } else {
        "unpredictable_use_dynamic_scaling".to_string()
    };

    (pattern_type, consistency_score, recommendation)
}

/// Calculate network efficiency metrics
///
/// Analyzes network utilization and bandwidth efficiency.
/// Returns (efficiency_score, bandwidth_util_pct, recommendation).
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::calculate_network_efficiency;
///
/// let (score, util, rec) = calculate_network_efficiency(1000, 500, 10000);
/// assert!(score >= 0.0 && score <= 1.0);
/// assert!(util >= 0.0 && util <= 100.0);
/// ```
pub fn calculate_network_efficiency(
    bytes_sent: usize,
    bytes_received: usize,
    max_bandwidth_bytes: usize,
) -> (f64, f64, String) {
    if max_bandwidth_bytes == 0 {
        return (0.0, 0.0, "invalid_bandwidth".to_string());
    }

    let total_bytes = bytes_sent + bytes_received;
    let bandwidth_util_pct = (total_bytes as f64 / max_bandwidth_bytes as f64 * 100.0).min(100.0);

    // Calculate efficiency based on send/receive ratio
    let send_ratio = if total_bytes > 0 {
        bytes_sent as f64 / total_bytes as f64
    } else {
        0.0
    };

    // Optimal efficiency is when send and receive are balanced (50/50)
    let balance_score = 1.0 - (send_ratio - 0.5).abs() * 2.0;

    // Overall efficiency combines balance and utilization
    let utilization_score = if bandwidth_util_pct < 70.0 {
        bandwidth_util_pct / 70.0 // Underutilized
    } else if bandwidth_util_pct < 90.0 {
        1.0 // Optimal range
    } else {
        (100.0 - bandwidth_util_pct) / 10.0 // Overutilized
    };

    let efficiency_score = (balance_score * 0.4 + utilization_score * 0.6).clamp(0.0, 1.0);

    // Generate recommendation
    let recommendation = if efficiency_score > 0.8 {
        "excellent_efficiency".to_string()
    } else if efficiency_score > 0.6 {
        "good_efficiency".to_string()
    } else if bandwidth_util_pct > 90.0 {
        "increase_bandwidth_overutilized".to_string()
    } else if bandwidth_util_pct < 30.0 {
        "reduce_bandwidth_underutilized".to_string()
    } else if (send_ratio - 0.5).abs() > 0.3 {
        "imbalanced_traffic_pattern".to_string()
    } else {
        "optimize_message_patterns".to_string()
    };

    (efficiency_score, bandwidth_util_pct, recommendation)
}

/// Detect message hotspots in distributed systems
///
/// Identifies imbalanced message distribution across queues/partitions.
/// Returns (has_hotspot, hotspot_index, imbalance_ratio, recommendation).
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::detect_message_hotspots;
///
/// let counts = vec![100, 500, 120, 110]; // Queue 1 has hotspot
/// let (has_hotspot, index, ratio, rec) = detect_message_hotspots(&counts);
/// assert!(has_hotspot);
/// assert_eq!(index, 1);
/// ```
pub fn detect_message_hotspots(message_counts: &[usize]) -> (bool, usize, f64, String) {
    if message_counts.is_empty() {
        return (false, 0, 1.0, "no_data".to_string());
    }

    if message_counts.len() == 1 {
        return (false, 0, 1.0, "single_queue".to_string());
    }

    // Find max and average
    let max_count = *message_counts.iter().max().unwrap();
    let avg_count = message_counts.iter().sum::<usize>() as f64 / message_counts.len() as f64;

    // Find index of hotspot
    let hotspot_index = message_counts.iter().position(|&c| c == max_count).unwrap();

    // Calculate imbalance ratio (max / avg)
    let imbalance_ratio = if avg_count > 0.0 {
        max_count as f64 / avg_count
    } else {
        1.0
    };

    // Determine if there's a hotspot (threshold: 2x average)
    let has_hotspot = imbalance_ratio > 2.0;

    // Generate recommendation
    let recommendation = if !has_hotspot {
        "balanced_distribution".to_string()
    } else if imbalance_ratio > 5.0 {
        format!("severe_hotspot_rebalance_queue_{}", hotspot_index)
    } else if imbalance_ratio > 3.0 {
        format!(
            "moderate_hotspot_check_routing_keys_queue_{}",
            hotspot_index
        )
    } else {
        format!("minor_hotspot_monitor_queue_{}", hotspot_index)
    };

    (has_hotspot, hotspot_index, imbalance_ratio, recommendation)
}

/// Recommend queue topology based on workload characteristics
///
/// Suggests optimal queue architecture for given requirements.
/// Returns (topology_type, queue_count, recommendation).
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::recommend_queue_topology;
///
/// let (topology, count, rec) = recommend_queue_topology(10000, 100, 50, true);
/// assert!(count > 0);
/// ```
pub fn recommend_queue_topology(
    messages_per_sec: usize,
    avg_processing_ms: usize,
    num_consumers: usize,
    requires_ordering: bool,
) -> (String, usize, String) {
    if messages_per_sec == 0 {
        return ("single_queue".to_string(), 1, "low_volume".to_string());
    }

    // Calculate processing capacity
    let processing_rate = if avg_processing_ms > 0 {
        (1000.0 / avg_processing_ms as f64) as usize
    } else {
        1000
    };
    let total_capacity = processing_rate * num_consumers;

    // Determine if we need partitioning
    let load_ratio = messages_per_sec as f64 / total_capacity as f64;

    let (topology_type, queue_count) = if requires_ordering {
        // Ordering required: use partitioned queues
        if load_ratio > 0.8 {
            ("partitioned".to_string(), num_consumers.max(4))
        } else {
            ("single_queue".to_string(), 1)
        }
    } else {
        // No ordering: can use multiple independent queues
        if load_ratio > 1.5 {
            // Overloaded: need more queues
            let recommended = (messages_per_sec as f64 / (processing_rate as f64 * 0.7)) as usize;
            ("multi_queue".to_string(), recommended.clamp(2, 32))
        } else if load_ratio > 0.8 {
            // High load: partition by priority
            ("priority_queues".to_string(), 3) // High, Medium, Low
        } else if messages_per_sec > 1000 {
            // Medium load: use worker pool
            ("worker_pool".to_string(), num_consumers.max(2))
        } else {
            // Low load: single queue
            ("single_queue".to_string(), 1)
        }
    };

    // Generate recommendation
    let recommendation = if load_ratio > 1.5 {
        format!(
            "overloaded_increase_consumers_or_queues_load_ratio_{:.2}",
            load_ratio
        )
    } else if load_ratio > 1.0 {
        format!("near_capacity_scale_soon_load_ratio_{:.2}", load_ratio)
    } else if load_ratio < 0.3 {
        format!(
            "underutilized_reduce_resources_load_ratio_{:.2}",
            load_ratio
        )
    } else {
        format!("optimal_topology_{}", topology_type)
    };

    (topology_type, queue_count, recommendation)
}

/// Calculate optimal message deduplication window
///
/// Analyzes message patterns to suggest appropriate deduplication window.
///
/// # Arguments
///
/// * `avg_message_interval_ms` - Average time between messages (milliseconds)
/// * `retry_count` - Average number of retries per message
/// * `max_delivery_delay_ms` - Maximum expected delivery delay (milliseconds)
///
/// # Returns
///
/// Tuple of (window_secs, cache_size, recommendation)
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::calculate_message_deduplication_window;
///
/// // Fast message stream with retries
/// let (window, cache_size, rec) = calculate_message_deduplication_window(100, 3, 5000);
/// assert!(window > 0);
/// assert!(cache_size > 0);
/// assert!(rec.contains("window"));
///
/// // Slow message stream
/// let (window, cache_size, _) = calculate_message_deduplication_window(10000, 1, 2000);
/// assert!(window >= 60); // At least 1 minute
/// ```
pub fn calculate_message_deduplication_window(
    avg_message_interval_ms: usize,
    retry_count: usize,
    max_delivery_delay_ms: usize,
) -> (usize, usize, String) {
    // Base window should cover retries + delivery delays
    let retry_window_ms = avg_message_interval_ms * retry_count.max(1);
    let total_window_ms = retry_window_ms + max_delivery_delay_ms;

    // Convert to seconds and add safety margin (2x)
    let window_secs = ((total_window_ms * 2) / 1000).max(60); // At least 1 minute

    // Estimate cache size based on message rate and window
    let messages_per_sec = if avg_message_interval_ms > 0 {
        (1000.0 / avg_message_interval_ms as f64) as usize
    } else {
        100 // Default estimate
    };
    let cache_size = (messages_per_sec * window_secs).clamp(1000, 1000000);

    // Generate recommendation
    let recommendation = if window_secs > 3600 {
        format!(
            "long_window_{}_secs_consider_persistent_storage",
            window_secs
        )
    } else if window_secs > 600 {
        format!("medium_window_{}_secs_cache_{}", window_secs, cache_size)
    } else {
        format!("short_window_{}_secs_cache_{}", window_secs, cache_size)
    };

    (window_secs, cache_size, recommendation)
}

/// Analyze retry effectiveness
///
/// Evaluates how effective retries are at recovering from failures.
///
/// # Arguments
///
/// * `total_messages` - Total messages processed
/// * `failed_messages` - Messages that failed initially
/// * `retry_successes` - Messages that succeeded after retry
/// * `final_failures` - Messages that failed permanently
///
/// # Returns
///
/// Tuple of (effectiveness_pct, success_rate, recommendation)
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::analyze_retry_effectiveness;
///
/// // High retry effectiveness
/// let (eff, rate, rec) = analyze_retry_effectiveness(1000, 100, 80, 20);
/// assert!(eff > 50.0);
/// assert!(rec.contains("effective"));
///
/// // Low retry effectiveness
/// let (eff, _, rec) = analyze_retry_effectiveness(1000, 100, 10, 90);
/// assert!(eff < 20.0);
/// assert!(rec.contains("ineffective"));
/// ```
pub fn analyze_retry_effectiveness(
    total_messages: usize,
    failed_messages: usize,
    retry_successes: usize,
    final_failures: usize,
) -> (f64, f64, String) {
    if failed_messages == 0 {
        return (100.0, 100.0, "no_failures_retries_not_needed".to_string());
    }

    // Calculate retry effectiveness (% of failures recovered by retry)
    let effectiveness = (retry_successes as f64 / failed_messages as f64) * 100.0;

    // Calculate overall success rate
    let total_successes = total_messages - final_failures;
    let success_rate = (total_successes as f64 / total_messages as f64) * 100.0;

    // Generate recommendation
    let recommendation = if effectiveness > 80.0 {
        format!(
            "highly_effective_retries_{:.1}pct_success_keep_current_policy",
            effectiveness
        )
    } else if effectiveness > 50.0 {
        format!(
            "moderately_effective_retries_{:.1}pct_consider_backoff_tuning",
            effectiveness
        )
    } else if effectiveness > 20.0 {
        format!(
            "low_effectiveness_{:.1}pct_review_retry_strategy",
            effectiveness
        )
    } else {
        format!(
            "ineffective_retries_{:.1}pct_investigate_root_cause",
            effectiveness
        )
    };

    (effectiveness, success_rate, recommendation)
}

/// Calculate queue overflow risk
///
/// Predicts the probability of queue overflow based on trends.
///
/// # Arguments
///
/// * `current_size` - Current queue size
/// * `max_size` - Maximum queue capacity
/// * `enqueue_rate` - Messages added per second
/// * `dequeue_rate` - Messages removed per second
///
/// # Returns
///
/// Tuple of (risk_pct, time_to_full_secs, recommendation)
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::calculate_queue_overflow_risk;
///
/// // High risk scenario
/// let (risk, ttf, rec) = calculate_queue_overflow_risk(8000, 10000, 100, 50);
/// assert!(risk > 50.0);
/// assert!(rec.contains("high_risk"));
///
/// // Low risk scenario
/// let (risk, _, rec) = calculate_queue_overflow_risk(100, 10000, 50, 100);
/// assert_eq!(risk, 0.0);
/// assert!(rec.contains("healthy"));
/// ```
pub fn calculate_queue_overflow_risk(
    current_size: usize,
    max_size: usize,
    enqueue_rate: usize,
    dequeue_rate: usize,
) -> (f64, i64, String) {
    if max_size == 0 {
        return (100.0, 0, "invalid_max_size_queue_misconfigured".to_string());
    }

    // Calculate net growth rate
    let net_rate = enqueue_rate as i64 - dequeue_rate as i64;

    // If queue is draining, no risk
    if net_rate <= 0 {
        let drain_time_secs = if dequeue_rate > enqueue_rate && current_size > 0 {
            (current_size as f64 / (dequeue_rate - enqueue_rate) as f64) as i64
        } else {
            -1
        };
        return (
            0.0,
            drain_time_secs,
            "healthy_queue_draining_no_overflow_risk".to_string(),
        );
    }

    // Calculate time to full
    let remaining_capacity = max_size.saturating_sub(current_size);
    let time_to_full_secs = if net_rate > 0 {
        (remaining_capacity as f64 / net_rate as f64) as i64
    } else {
        i64::MAX
    };

    // Calculate current utilization
    let utilization = (current_size as f64 / max_size as f64) * 100.0;

    // Calculate risk based on utilization and time to full
    let risk = if utilization >= 90.0 {
        95.0 // Critical
    } else if utilization >= 80.0 {
        80.0 // High
    } else if utilization >= 60.0 {
        60.0 // Medium
    } else if time_to_full_secs < 60 {
        75.0 // Will fill in < 1 minute
    } else if time_to_full_secs < 300 {
        50.0 // Will fill in < 5 minutes
    } else if time_to_full_secs < 3600 {
        30.0 // Will fill in < 1 hour
    } else {
        10.0 // Low risk
    };

    // Generate recommendation
    let recommendation = if risk >= 90.0 {
        format!(
            "critical_risk_{:.1}pct_utilized_ttf_{}_secs_immediate_action",
            utilization, time_to_full_secs
        )
    } else if risk >= 70.0 {
        format!(
            "high_risk_{:.1}pct_utilized_ttf_{}_secs_scale_consumers",
            utilization, time_to_full_secs
        )
    } else if risk >= 40.0 {
        format!(
            "medium_risk_{:.1}pct_utilized_ttf_{}_secs_monitor_closely",
            utilization, time_to_full_secs
        )
    } else {
        format!("low_risk_{:.1}pct_utilized_queue_healthy", utilization)
    };

    (risk, time_to_full_secs, recommendation)
}

/// Calculate message throughput trend
///
/// Analyzes throughput over time to identify trends and patterns.
///
/// # Arguments
///
/// * `throughput_samples` - Historical throughput samples (messages/sec)
/// * `window_size` - Number of recent samples to analyze
///
/// # Returns
///
/// Tuple of (current_avg, trend_direction, change_pct)
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::calculate_message_throughput_trend;
///
/// // Increasing throughput
/// let samples = vec![100.0, 110.0, 120.0, 130.0, 140.0];
/// let (avg, trend, change) = calculate_message_throughput_trend(&samples, 3);
/// assert!(avg > 0.0);
/// assert_eq!(trend, "increasing");
///
/// // Decreasing throughput
/// let samples = vec![140.0, 130.0, 120.0, 110.0, 100.0];
/// let (_, trend, _) = calculate_message_throughput_trend(&samples, 3);
/// assert_eq!(trend, "decreasing");
/// ```
pub fn calculate_message_throughput_trend(
    throughput_samples: &[f64],
    window_size: usize,
) -> (f64, String, f64) {
    if throughput_samples.is_empty() {
        return (0.0, "no_data".to_string(), 0.0);
    }

    let actual_window = window_size.min(throughput_samples.len());
    let recent_samples =
        &throughput_samples[throughput_samples.len().saturating_sub(actual_window)..];

    if recent_samples.is_empty() {
        return (0.0, "no_data".to_string(), 0.0);
    }

    // Calculate current average
    let current_avg: f64 = recent_samples.iter().sum::<f64>() / recent_samples.len() as f64;

    // Calculate trend by comparing first half vs second half
    if recent_samples.len() < 2 {
        return (current_avg, "stable".to_string(), 0.0);
    }

    let mid = recent_samples.len() / 2;
    let first_half_avg: f64 = recent_samples[..mid].iter().sum::<f64>() / mid as f64;
    let second_half_avg: f64 =
        recent_samples[mid..].iter().sum::<f64>() / (recent_samples.len() - mid) as f64;

    let change_pct = if first_half_avg > 0.0 {
        ((second_half_avg - first_half_avg) / first_half_avg) * 100.0
    } else {
        0.0
    };

    let trend = if change_pct > 10.0 {
        "increasing"
    } else if change_pct < -10.0 {
        "decreasing"
    } else {
        "stable"
    };

    (current_avg, trend.to_string(), change_pct)
}

/// Detect queue starvation
///
/// Identifies when queues are underutilized or idle.
///
/// # Arguments
///
/// * `queue_size` - Current number of messages in queue
/// * `consumer_count` - Number of active consumers
/// * `min_queue_depth` - Minimum healthy queue depth per consumer
///
/// # Returns
///
/// Tuple of (is_starving, severity, recommendation)
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::detect_queue_starvation;
///
/// // Starving queue (depth_ratio = 5 / (10*2) = 0.25 => medium severity)
/// let (starving, severity, rec) = detect_queue_starvation(5, 10, 2);
/// assert!(starving);
/// assert_eq!(severity, "medium");
///
/// // Healthy queue
/// let (starving, _, _) = detect_queue_starvation(100, 10, 2);
/// assert!(!starving);
/// ```
pub fn detect_queue_starvation(
    queue_size: usize,
    consumer_count: usize,
    min_queue_depth: usize,
) -> (bool, String, String) {
    if consumer_count == 0 {
        return (
            false,
            "no_consumers".to_string(),
            "no_consumers_to_starve".to_string(),
        );
    }

    let recommended_depth = consumer_count * min_queue_depth;
    let depth_ratio = queue_size as f64 / recommended_depth as f64;

    let (is_starving, severity) = if depth_ratio < 0.25 {
        (true, "high")
    } else if depth_ratio < 0.5 {
        (true, "medium")
    } else if depth_ratio < 0.75 {
        (true, "low")
    } else {
        (false, "healthy")
    };

    let recommendation = if is_starving {
        if consumer_count > queue_size {
            format!(
                "high_starvation_reduce_consumers_from_{}_to_{}_or_increase_queue_depth",
                consumer_count,
                queue_size.max(1)
            )
        } else {
            format!(
                "{}_starvation_increase_queue_depth_from_{}_to_{}_or_reduce_consumers",
                severity, queue_size, recommended_depth
            )
        }
    } else {
        format!(
            "healthy_queue_depth_{}_for_{}_consumers",
            queue_size, consumer_count
        )
    };

    (is_starving, severity.to_string(), recommendation)
}

/// Estimate broker capacity
///
/// Calculates maximum sustainable throughput based on current metrics.
///
/// # Arguments
///
/// * `avg_processing_ms` - Average message processing time
/// * `worker_count` - Number of workers
/// * `connection_pool_size` - Connection pool size
/// * `target_utilization` - Target utilization percentage (0.0-1.0)
///
/// # Returns
///
/// Tuple of (max_throughput_per_sec, headroom_pct, recommendation)
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::estimate_broker_capacity;
///
/// // Calculate capacity
/// let (throughput, headroom, rec) = estimate_broker_capacity(100, 10, 20, 0.8);
/// assert!(throughput > 0.0);
/// assert!(rec.contains("capacity"));
///
/// // High utilization
/// let (_, headroom, _) = estimate_broker_capacity(50, 5, 10, 0.95);
/// assert!(headroom < 10.0);
/// ```
pub fn estimate_broker_capacity(
    avg_processing_ms: usize,
    worker_count: usize,
    connection_pool_size: usize,
    target_utilization: f64,
) -> (f64, f64, String) {
    if avg_processing_ms == 0 || worker_count == 0 {
        return (
            0.0,
            0.0,
            "invalid_input_processing_time_or_workers_zero".to_string(),
        );
    }

    // Calculate theoretical max throughput
    let msg_per_sec_per_worker = 1000.0 / avg_processing_ms as f64;
    let theoretical_max = msg_per_sec_per_worker * worker_count as f64;

    // Account for connection pool as bottleneck
    let connection_limited = if connection_pool_size < worker_count {
        msg_per_sec_per_worker * connection_pool_size as f64
    } else {
        theoretical_max
    };

    // Apply target utilization
    let safe_max = connection_limited * target_utilization;

    // Calculate headroom
    let headroom_pct = ((1.0 - target_utilization) * 100.0).max(0.0);

    // Generate recommendation
    let recommendation = if headroom_pct < 5.0 {
        format!(
            "critical_capacity_{:.1}_msgs_sec_headroom_{:.1}pct_scale_immediately",
            safe_max, headroom_pct
        )
    } else if headroom_pct < 15.0 {
        format!(
            "limited_capacity_{:.1}_msgs_sec_headroom_{:.1}pct_plan_scaling",
            safe_max, headroom_pct
        )
    } else {
        format!(
            "healthy_capacity_{:.1}_msgs_sec_headroom_{:.1}pct",
            safe_max, headroom_pct
        )
    };

    (safe_max, headroom_pct, recommendation)
}
