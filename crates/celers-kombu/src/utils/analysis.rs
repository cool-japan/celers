//! Advanced analysis utility functions for broker operations.

/// Suggest optimal connection pool size
///
/// Recommends connection pool size based on concurrent request patterns
/// and resource constraints.
///
/// Returns (min_connections, max_connections, recommended_initial)
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::suggest_connection_pool_size;
///
/// // Peak: 50 concurrent, Average: 20, Max allowed: 100
/// let (min, max, initial) = suggest_connection_pool_size(50, 20, 100);
/// assert!(min > 0);
/// assert!(max <= 100);
/// assert!(initial >= min && initial <= max);
///
/// // Very high load
/// let (min, max, initial) = suggest_connection_pool_size(200, 150, 100);
/// assert_eq!(max, 100); // Respects max_allowed limit
/// ```
#[allow(clippy::too_many_arguments)]
pub fn suggest_connection_pool_size(
    peak_concurrent_requests: usize,
    avg_concurrent_requests: usize,
    max_allowed_connections: usize,
) -> (usize, usize, usize) {
    if max_allowed_connections == 0 {
        return (0, 0, 0);
    }

    // Minimum connections: 10% of average, at least 1, at most 5
    let min_connections = (avg_concurrent_requests / 10)
        .clamp(1, 5)
        .min(max_allowed_connections);

    // Maximum connections: 150% of peak to handle bursts
    let recommended_max = ((peak_concurrent_requests as f64 * 1.5).ceil() as usize)
        .clamp(min_connections + 1, max_allowed_connections);

    // Initial/recommended: average + 20% buffer
    let recommended_initial = ((avg_concurrent_requests as f64 * 1.2).ceil() as usize)
        .clamp(min_connections, recommended_max);

    (min_connections, recommended_max, recommended_initial)
}

/// Calculate message processing trend
///
/// Analyzes historical processing times to determine if performance is
/// improving, degrading, or stable over time.
///
/// Returns (trend_direction, trend_strength, recommendation) where:
/// - trend_direction: "improving", "stable", or "degrading"
/// - trend_strength: 0.0 (no trend) to 1.0 (strong trend)
/// - recommendation: suggested action
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::calculate_message_processing_trend;
///
/// // Processing times improving over time
/// let times = vec![100, 95, 90, 85, 80];
/// let (direction, strength, recommendation) = calculate_message_processing_trend(&times);
/// assert_eq!(direction, "improving");
/// assert!(strength > 0.0);
///
/// // Processing times degrading
/// let times = vec![80, 85, 90, 95, 100];
/// let (direction, strength, recommendation) = calculate_message_processing_trend(&times);
/// assert_eq!(direction, "degrading");
/// ```
pub fn calculate_message_processing_trend(
    processing_times_ms: &[u64],
) -> (&'static str, f64, &'static str) {
    if processing_times_ms.len() < 3 {
        return ("stable", 0.0, "insufficient_data");
    }

    // Calculate linear regression slope
    let n = processing_times_ms.len() as f64;
    let sum_x: f64 = (0..processing_times_ms.len()).map(|i| i as f64).sum();
    let sum_y: f64 = processing_times_ms.iter().map(|&t| t as f64).sum();
    let sum_xy: f64 = processing_times_ms
        .iter()
        .enumerate()
        .map(|(i, &t)| i as f64 * t as f64)
        .sum();
    let sum_x_squared: f64 = (0..processing_times_ms.len())
        .map(|i| (i as f64) * (i as f64))
        .sum();

    let slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x_squared - sum_x * sum_x);

    // Calculate average and normalize slope
    let avg_time = sum_y / n;
    let normalized_slope = if avg_time > 0.0 {
        (slope / avg_time).abs()
    } else {
        0.0
    };

    // Determine trend direction and strength
    let (direction, recommendation) = if slope < -1.0 {
        ("improving", "maintain_current_optimizations")
    } else if slope > 1.0 {
        ("degrading", "investigate_performance_issues")
    } else {
        ("stable", "monitor_continuously")
    };

    let strength = normalized_slope.min(1.0);

    (direction, strength, recommendation)
}

/// Suggest optimal prefetch count for consumers
///
/// Recommends the number of messages a consumer should prefetch based on
/// processing characteristics and resource availability.
///
/// Returns optimal prefetch count
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::suggest_prefetch_count;
///
/// // Fast processing, high concurrency
/// let prefetch = suggest_prefetch_count(50, 10, 100);
/// assert!(prefetch > 0);
/// assert!(prefetch <= 100);
///
/// // Slow processing, low concurrency
/// let prefetch = suggest_prefetch_count(1000, 2, 50);
/// assert!(prefetch <= 20); // Lower prefetch for slow processing
/// ```
pub fn suggest_prefetch_count(
    avg_processing_time_ms: u64,
    concurrent_workers: usize,
    max_prefetch: usize,
) -> usize {
    if concurrent_workers == 0 || max_prefetch == 0 {
        return 1;
    }

    // Calculate messages per second one worker can handle
    let msgs_per_sec_per_worker = if avg_processing_time_ms > 0 {
        1000.0 / avg_processing_time_ms as f64
    } else {
        10.0 // Default assumption
    };

    // For fast processing (>10 msg/sec), prefetch more
    // For slow processing (<1 msg/sec), prefetch less
    let base_prefetch = if msgs_per_sec_per_worker >= 10.0 {
        20 // Fast processing: prefetch many
    } else if msgs_per_sec_per_worker >= 1.0 {
        10 // Medium processing: moderate prefetch
    } else {
        5 // Slow processing: prefetch few
    };

    // Adjust for number of workers
    let adjusted_prefetch = (base_prefetch * concurrent_workers).max(1);

    // Clamp to max_prefetch
    adjusted_prefetch.min(max_prefetch)
}

/// Analyze dead letter queue patterns
///
/// Analyzes DLQ statistics to identify patterns and suggest remediation strategies.
///
/// Returns (severity, primary_issue, recommendation) where:
/// - severity: "low", "medium", "high", or "critical"
/// - primary_issue: description of the main issue
/// - recommendation: suggested action
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::analyze_dead_letter_queue;
///
/// // High failure rate - critical issue
/// let (severity, issue, recommendation) = analyze_dead_letter_queue(500, 1000, 100);
/// assert_eq!(severity, "critical");
///
/// // Low failure rate - healthy
/// let (severity, issue, recommendation) = analyze_dead_letter_queue(10, 10000, 100);
/// assert_eq!(severity, "low");
/// ```
pub fn analyze_dead_letter_queue(
    dlq_size: usize,
    total_processed: u64,
    dlq_growth_per_hour: usize,
) -> (&'static str, &'static str, &'static str) {
    if total_processed == 0 {
        return ("low", "no_data", "monitor");
    }

    // Calculate failure rate
    let failure_rate = dlq_size as f64 / total_processed as f64;

    // Determine severity based on failure rate and growth
    let (severity, primary_issue, recommendation) = if failure_rate > 0.1 {
        // >10% failure rate
        (
            "critical",
            "high_failure_rate",
            "immediate_investigation_required",
        )
    } else if failure_rate > 0.05 {
        // >5% failure rate
        (
            "high",
            "elevated_failure_rate",
            "investigate_error_patterns",
        )
    } else if dlq_growth_per_hour > 100 {
        // Rapid growth
        (
            "high",
            "rapid_dlq_growth",
            "monitor_closely_and_investigate",
        )
    } else if failure_rate > 0.01 {
        // >1% failure rate
        ("medium", "moderate_failures", "review_recent_failures")
    } else if dlq_size > 0 {
        // Some failures but low rate
        ("low", "normal_failures", "periodic_dlq_review")
    } else {
        // No failures
        ("low", "healthy", "continue_monitoring")
    };

    (severity, primary_issue, recommendation)
}

/// Forecast queue capacity with trend analysis
///
/// Uses historical data points to forecast future capacity needs with trend analysis.
///
/// # Arguments
///
/// * `historical_sizes` - Historical queue sizes (chronological order)
/// * `forecast_hours` - Hours into the future to forecast
///
/// # Returns
///
/// Tuple of (forecasted_size, trend_slope, confidence_level)
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::forecast_queue_capacity_ml;
///
/// let history = vec![100, 150, 180, 220, 250];
/// let (forecast, slope, confidence) = forecast_queue_capacity_ml(&history, 24);
/// assert!(forecast > 250); // Should predict growth
/// assert!(slope > 0.0); // Positive trend
/// ```
pub fn forecast_queue_capacity_ml(
    historical_sizes: &[usize],
    forecast_hours: usize,
) -> (usize, f64, &'static str) {
    if historical_sizes.len() < 3 {
        return (
            historical_sizes.last().copied().unwrap_or(0),
            0.0,
            "insufficient_data",
        );
    }

    // Calculate linear regression trend
    let n = historical_sizes.len() as f64;
    let x_mean = (n - 1.0) / 2.0;
    let y_mean = historical_sizes.iter().sum::<usize>() as f64 / n;

    let mut numerator = 0.0;
    let mut denominator = 0.0;

    for (i, &size) in historical_sizes.iter().enumerate() {
        let x = i as f64;
        let y = size as f64;
        numerator += (x - x_mean) * (y - y_mean);
        denominator += (x - x_mean).powi(2);
    }

    let slope = if denominator > 0.0 {
        numerator / denominator
    } else {
        0.0
    };

    // Forecast future size
    let future_x = (historical_sizes.len() - 1) as f64 + forecast_hours as f64;
    let forecast = (y_mean + slope * (future_x - x_mean)).max(0.0) as usize;

    // Calculate confidence based on data variance
    let variance: f64 = historical_sizes
        .iter()
        .map(|&s| {
            let predicted = y_mean
                + slope * (historical_sizes.iter().position(|&x| x == s).unwrap() as f64 - x_mean);
            (s as f64 - predicted).powi(2)
        })
        .sum::<f64>()
        / n;

    let std_dev = variance.sqrt();
    let coefficient_of_variation = std_dev / y_mean;

    let confidence = if coefficient_of_variation < 0.1 {
        "high"
    } else if coefficient_of_variation < 0.3 {
        "medium"
    } else {
        "low"
    };

    (forecast, slope, confidence)
}

/// Optimize batch strategy based on multiple factors
///
/// Analyzes message patterns, network latency, and processing characteristics
/// to recommend optimal batching strategy.
///
/// # Arguments
///
/// * `avg_message_size` - Average message size in bytes
/// * `network_latency_ms` - Network round-trip latency in milliseconds
/// * `processing_time_ms` - Average processing time per message in milliseconds
/// * `throughput_target` - Target throughput in messages per second
///
/// # Returns
///
/// Tuple of (batch_size, max_wait_ms, estimated_throughput, strategy)
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::optimize_batch_strategy;
///
/// let (batch_size, wait_ms, throughput, strategy) =
///     optimize_batch_strategy(1024, 50, 10, 1000);
/// assert!(batch_size > 0);
/// assert!(wait_ms > 0);
/// assert_eq!(strategy, "throughput_optimized");
/// ```
pub fn optimize_batch_strategy(
    avg_message_size: usize,
    network_latency_ms: u64,
    processing_time_ms: u64,
    throughput_target: u64,
) -> (usize, u64, u64, &'static str) {
    // Calculate ideal batch size based on latency vs processing time ratio
    let latency_weight =
        network_latency_ms as f64 / (network_latency_ms + processing_time_ms) as f64;

    let base_batch_size = if latency_weight > 0.5 {
        // High latency: use larger batches to amortize network cost
        100
    } else {
        // Low latency: smaller batches for lower latency
        20
    };

    // Adjust for message size
    let max_batch_bytes = 4 * 1024 * 1024; // 4MB max batch
    let size_limited_batch = (max_batch_bytes / avg_message_size.max(1)).min(1000);
    let batch_size = base_batch_size.min(size_limited_batch);

    // Calculate optimal wait time
    let messages_per_ms = throughput_target as f64 / 1000.0;
    let fill_time_ms = if messages_per_ms > 0.0 {
        (batch_size as f64 / messages_per_ms) as u64
    } else {
        100
    };

    let max_wait_ms = fill_time_ms.clamp(10, 5000);

    // Estimate actual throughput
    let batch_overhead = network_latency_ms + 10; // 10ms batch processing overhead
    let time_per_batch = batch_overhead + (batch_size as u64 * processing_time_ms);
    let batches_per_sec = 1000u64.checked_div(time_per_batch).unwrap_or(0);
    let estimated_throughput = batches_per_sec * batch_size as u64;

    // Determine strategy
    let strategy = if batch_size > 50 {
        "throughput_optimized"
    } else if network_latency_ms > 100 {
        "latency_optimized"
    } else {
        "balanced"
    };

    (batch_size, max_wait_ms, estimated_throughput, strategy)
}

/// Calculate efficiency across multiple queues
///
/// Analyzes load distribution and efficiency across multiple queues to identify
/// imbalances and optimization opportunities.
///
/// # Arguments
///
/// * `queue_sizes` - Current size of each queue
/// * `processing_rates` - Processing rate (msg/sec) for each queue
///
/// # Returns
///
/// Tuple of (overall_efficiency, load_balance_score, recommendation)
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::calculate_multi_queue_efficiency;
///
/// let sizes = vec![100, 150, 120];
/// let rates = vec![10, 12, 11];
/// let (efficiency, balance, recommendation) =
///     calculate_multi_queue_efficiency(&sizes, &rates);
/// assert!(efficiency >= 0.0 && efficiency <= 1.0);
/// assert!(balance >= 0.0 && balance <= 1.0);
/// ```
pub fn calculate_multi_queue_efficiency(
    queue_sizes: &[usize],
    processing_rates: &[u64],
) -> (f64, f64, &'static str) {
    if queue_sizes.is_empty() || queue_sizes.len() != processing_rates.len() {
        return (0.0, 0.0, "invalid_input");
    }

    // Calculate per-queue drain times
    let drain_times: Vec<f64> = queue_sizes
        .iter()
        .zip(processing_rates.iter())
        .map(|(&size, &rate)| {
            if rate > 0 {
                size as f64 / rate as f64
            } else {
                f64::INFINITY
            }
        })
        .collect();

    // Overall efficiency: average of (rate / max_rate) for each queue
    let max_rate = *processing_rates.iter().max().unwrap_or(&1);
    let efficiency = if max_rate > 0 {
        processing_rates
            .iter()
            .map(|&r| r as f64 / max_rate as f64)
            .sum::<f64>()
            / processing_rates.len() as f64
    } else {
        0.0
    };

    // Load balance score: how evenly distributed the drain times are
    let avg_drain_time = drain_times.iter().filter(|t| t.is_finite()).sum::<f64>()
        / drain_times.iter().filter(|t| t.is_finite()).count() as f64;

    let variance = drain_times
        .iter()
        .filter(|t| t.is_finite())
        .map(|&t| (t - avg_drain_time).powi(2))
        .sum::<f64>()
        / drain_times.iter().filter(|t| t.is_finite()).count() as f64;

    let coefficient_of_variation = if avg_drain_time > 0.0 {
        variance.sqrt() / avg_drain_time
    } else {
        0.0
    };

    // Lower CV means better balance (invert for score)
    let load_balance_score = (1.0 / (1.0 + coefficient_of_variation)).min(1.0);

    let recommendation = if load_balance_score < 0.5 {
        "rebalance_workers"
    } else if efficiency < 0.6 {
        "increase_capacity"
    } else if load_balance_score > 0.8 && efficiency > 0.8 {
        "optimal"
    } else {
        "monitor"
    };

    (efficiency, load_balance_score, recommendation)
}

/// Predict when resources will be exhausted
///
/// Analyzes resource consumption trends to predict when resources will be exhausted.
///
/// # Arguments
///
/// * `current_usage` - Current resource usage (e.g., queue size, memory MB)
/// * `max_capacity` - Maximum resource capacity
/// * `growth_rate_per_hour` - Resource growth rate per hour
///
/// # Returns
///
/// Tuple of (hours_until_exhausted, severity, action)
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::predict_resource_exhaustion;
///
/// let (hours, severity, action) = predict_resource_exhaustion(7000, 10000, 500);
/// assert!(hours > 0);
/// assert_eq!(severity, "warning");
/// ```
pub fn predict_resource_exhaustion(
    current_usage: usize,
    max_capacity: usize,
    growth_rate_per_hour: usize,
) -> (usize, &'static str, &'static str) {
    if growth_rate_per_hour == 0 {
        return (usize::MAX, "healthy", "monitor");
    }

    if current_usage >= max_capacity {
        return (0, "critical", "immediate_action_required");
    }

    let remaining = max_capacity.saturating_sub(current_usage);
    let hours_until_exhausted = remaining / growth_rate_per_hour.max(1);

    let (severity, action) = if hours_until_exhausted < 1 {
        ("critical", "immediate_scaling_required")
    } else if hours_until_exhausted < 4 {
        ("high", "scale_within_hour")
    } else if hours_until_exhausted < 24 {
        ("warning", "plan_scaling")
    } else if hours_until_exhausted < 168 {
        // 1 week
        ("low", "monitor_trends")
    } else {
        ("healthy", "normal_monitoring")
    };

    (hours_until_exhausted, severity, action)
}

/// Suggest autoscaling policy based on load patterns
///
/// Analyzes historical load patterns to recommend autoscaling policy parameters.
///
/// # Arguments
///
/// * `peak_load` - Peak load (messages/sec)
/// * `average_load` - Average load (messages/sec)
/// * `min_load` - Minimum load (messages/sec)
/// * `load_volatility` - Standard deviation of load
///
/// # Returns
///
/// Tuple of (min_workers, max_workers, scale_up_threshold, scale_down_threshold, policy_type)
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::suggest_autoscaling_policy;
///
/// let (min, max, up_threshold, down_threshold, policy) =
///     suggest_autoscaling_policy(1000, 500, 100, 200);
/// assert!(min > 0);
/// assert!(max >= min);
/// assert!(up_threshold > down_threshold);
/// ```
pub fn suggest_autoscaling_policy(
    peak_load: u64,
    average_load: u64,
    min_load: u64,
    load_volatility: u64,
) -> (usize, usize, f64, f64, &'static str) {
    // Assume each worker can handle ~100 msg/sec
    let worker_capacity = 100;

    // Minimum workers to handle baseline load with headroom
    let min_workers = ((min_load as f64 / worker_capacity as f64) * 1.2).ceil() as usize;
    let min_workers = min_workers.max(2); // Always have at least 2 workers

    // Maximum workers to handle peak load with headroom
    let max_workers = ((peak_load as f64 / worker_capacity as f64) * 1.5).ceil() as usize;
    let max_workers = max_workers.max(min_workers * 2); // At least 2x min

    // Determine policy based on load volatility
    let volatility_ratio = if average_load > 0 {
        load_volatility as f64 / average_load as f64
    } else {
        0.0
    };

    let (scale_up_threshold, scale_down_threshold, policy_type) = if volatility_ratio > 0.5 {
        // High volatility: aggressive scaling
        (0.6, 0.3, "aggressive")
    } else if volatility_ratio > 0.2 {
        // Moderate volatility: balanced scaling
        (0.7, 0.4, "balanced")
    } else {
        // Low volatility: conservative scaling
        (0.8, 0.5, "conservative")
    };

    (
        min_workers,
        max_workers,
        scale_up_threshold,
        scale_down_threshold,
        policy_type,
    )
}

/// Calculate message affinity score for consistent routing.
///
/// This helps route messages with similar characteristics to the same worker
/// for better cache locality and processing efficiency.
///
/// # Arguments
///
/// * `message_key` - Message identifier or routing key
/// * `num_workers` - Total number of workers
///
/// # Returns
///
/// Worker index (0 to num_workers-1)
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::calculate_message_affinity;
///
/// let worker = calculate_message_affinity("user:12345", 8);
/// assert!(worker < 8);
///
/// // Same key always routes to same worker
/// assert_eq!(
///     calculate_message_affinity("order:abc", 10),
///     calculate_message_affinity("order:abc", 10)
/// );
/// ```
pub fn calculate_message_affinity(message_key: &str, num_workers: usize) -> usize {
    if num_workers == 0 {
        return 0;
    }

    // Use a simple hash for consistent routing
    let mut hash: u64 = 0;
    for byte in message_key.bytes() {
        hash = hash.wrapping_mul(31).wrapping_add(byte as u64);
    }

    (hash % num_workers as u64) as usize
}

/// Analyze queue temperature (hot/warm/cold classification).
///
/// This helps identify queue activity levels for resource allocation.
///
/// # Arguments
///
/// * `messages_per_min` - Message throughput rate
/// * `avg_age_secs` - Average message age in seconds
///
/// # Returns
///
/// Tuple of (temperature, recommendation)
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::analyze_queue_temperature;
///
/// let (temp, rec) = analyze_queue_temperature(100, 5);
/// assert_eq!(temp, "hot");
/// assert_eq!(rec, "maintain_resources");
///
/// let (temp, _) = analyze_queue_temperature(1, 300);
/// assert_eq!(temp, "cold");
/// ```
pub fn analyze_queue_temperature(
    messages_per_min: usize,
    avg_age_secs: usize,
) -> (&'static str, &'static str) {
    // Hot queue: high throughput, low age
    if messages_per_min > 50 && avg_age_secs < 60 {
        ("hot", "maintain_resources")
    }
    // Warm queue: moderate throughput
    else if messages_per_min > 10 && avg_age_secs < 300 {
        ("warm", "monitor")
    }
    // Cold queue: low throughput, high age
    else if messages_per_min < 5 || avg_age_secs > 600 {
        ("cold", "consider_scaling_down")
    }
    // Lukewarm: in between
    else {
        ("lukewarm", "monitor")
    }
}

/// Detect processing bottleneck in the message pipeline.
///
/// Identifies where bottlenecks occur based on queue metrics.
///
/// # Arguments
///
/// * `publish_rate` - Messages published per second
/// * `consume_rate` - Messages consumed per second
/// * `queue_size` - Current queue size
/// * `processing_time_ms` - Average processing time in milliseconds
///
/// # Returns
///
/// Tuple of (bottleneck_location, severity, recommendation)
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::detect_processing_bottleneck;
///
/// let (location, severity, _) = detect_processing_bottleneck(100, 50, 2000, 500);
/// assert_eq!(location, "consumer");
/// assert_eq!(severity, "high");
/// ```
pub fn detect_processing_bottleneck(
    publish_rate: usize,
    consume_rate: usize,
    queue_size: usize,
    processing_time_ms: usize,
) -> (&'static str, &'static str, &'static str) {
    // Calculate imbalance ratio
    let rate_ratio = if consume_rate > 0 {
        publish_rate as f64 / consume_rate as f64
    } else {
        f64::INFINITY
    };

    // Check for consumer bottleneck
    if rate_ratio > 1.5 && queue_size > 1000 {
        return (
            "consumer",
            "high",
            "scale_up_consumers_or_optimize_processing",
        );
    }

    // Check for slow processing
    if processing_time_ms > 1000 {
        return ("processing", "medium", "optimize_task_logic_or_add_caching");
    }

    // Check for queue buildup
    if queue_size > 5000 {
        return ("queue", "medium", "increase_consumer_capacity");
    }

    // Check for publisher bottleneck (rare)
    if rate_ratio < 0.5 && queue_size < 100 {
        return (
            "publisher",
            "low",
            "consider_scaling_down_consumers_to_save_cost",
        );
    }

    ("none", "low", "system_healthy")
}

/// Calculate optimal prefetch count multiplier.
///
/// Determines the ideal prefetch multiplier based on processing characteristics.
///
/// # Arguments
///
/// * `avg_processing_time_ms` - Average message processing time
/// * `network_latency_ms` - Network round-trip time
/// * `concurrency` - Number of concurrent workers
///
/// # Returns
///
/// Recommended prefetch multiplier
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::calculate_optimal_prefetch_multiplier;
///
/// let multiplier = calculate_optimal_prefetch_multiplier(100, 10, 4);
/// assert!(multiplier >= 1.0);
/// assert!(multiplier <= 10.0);
/// ```
pub fn calculate_optimal_prefetch_multiplier(
    avg_processing_time_ms: usize,
    network_latency_ms: usize,
    concurrency: usize,
) -> f64 {
    if avg_processing_time_ms == 0 || concurrency == 0 {
        return 1.0;
    }

    // Base multiplier on processing time
    let base_multiplier = if avg_processing_time_ms < 50 {
        // Fast processing: prefetch more
        5.0
    } else if avg_processing_time_ms < 200 {
        // Medium processing
        3.0
    } else {
        // Slow processing: prefetch less
        2.0
    };

    // Adjust for network latency
    let latency_factor = if network_latency_ms > 100 {
        1.5 // High latency: prefetch more to hide latency
    } else if network_latency_ms > 50 {
        1.2
    } else {
        1.0
    };

    // Adjust for concurrency
    let concurrency_factor = (concurrency as f64).sqrt();

    // Calculate final multiplier
    let multiplier = base_multiplier * latency_factor / concurrency_factor;

    // Clamp to reasonable range
    multiplier.clamp(1.0, 10.0)
}

/// Suggest queue consolidation strategies.
///
/// Recommends whether multiple queues should be consolidated based on usage patterns.
///
/// # Arguments
///
/// * `queue_sizes` - Current sizes of all queues
/// * `queue_rates` - Message rates for all queues (messages per minute)
///
/// # Returns
///
/// Tuple of (should_consolidate, reason, recommendation)
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::suggest_queue_consolidation;
///
/// let sizes = vec![10, 5, 8];
/// let rates = vec![2, 1, 3];
/// let (consolidate, reason, _) = suggest_queue_consolidation(&sizes, &rates);
///
/// assert_eq!(consolidate, true);
/// assert_eq!(reason, "low_throughput");
/// ```
pub fn suggest_queue_consolidation(
    queue_sizes: &[usize],
    queue_rates: &[usize],
) -> (bool, &'static str, &'static str) {
    if queue_sizes.is_empty() || queue_rates.is_empty() {
        return (false, "no_data", "n/a");
    }

    if queue_sizes.len() != queue_rates.len() {
        return (false, "invalid_input", "ensure_matching_array_lengths");
    }

    // Calculate average metrics
    let avg_size: usize = queue_sizes.iter().sum::<usize>() / queue_sizes.len();
    let avg_rate: usize = queue_rates.iter().sum::<usize>() / queue_rates.len();

    // Suggest consolidation if all queues are tiny (check this first)
    if avg_size < 20 && avg_rate < 5 && queue_sizes.len() > 3 {
        return (
            true,
            "overhead",
            "reduce_queue_count_to_minimize_management_overhead",
        );
    }

    // Count underutilized queues
    let underutilized = queue_sizes
        .iter()
        .zip(queue_rates.iter())
        .filter(|(&size, &rate)| size < 50 && rate < 10)
        .count();

    // Suggest consolidation if many queues are underutilized
    if underutilized as f64 / queue_sizes.len() as f64 > 0.6 {
        return (
            true,
            "low_throughput",
            "consolidate_into_fewer_queues_with_routing",
        );
    }

    (false, "efficient", "maintain_current_structure")
}

/// Calculate queue utilization efficiency.
///
/// Measures how efficiently a queue is being utilized based on size,
/// capacity, and throughput metrics.
///
/// # Arguments
///
/// * `current_size` - Current number of messages in queue
/// * `capacity` - Maximum queue capacity
/// * `messages_per_sec` - Current throughput rate
/// * `peak_messages_per_sec` - Peak historical throughput
///
/// # Returns
///
/// Tuple of (utilization_ratio, efficiency_score, recommendation)
///
/// # Examples
///
/// ```
/// use celers_kombu::utils::calculate_queue_utilization_efficiency;
///
/// let (util, eff, rec) = calculate_queue_utilization_efficiency(500, 1000, 50, 100);
/// assert!(util >= 0.0 && util <= 1.0);
/// assert!(eff >= 0.0 && eff <= 1.0);
/// ```
pub fn calculate_queue_utilization_efficiency(
    current_size: usize,
    capacity: usize,
    messages_per_sec: usize,
    peak_messages_per_sec: usize,
) -> (f64, f64, &'static str) {
    if capacity == 0 {
        return (0.0, 0.0, "invalid_capacity");
    }

    // Calculate size utilization (0-1)
    let size_util = (current_size as f64 / capacity as f64).min(1.0);

    // Calculate throughput efficiency (0-1)
    let throughput_eff = if peak_messages_per_sec > 0 {
        (messages_per_sec as f64 / peak_messages_per_sec as f64).min(1.0)
    } else {
        0.0
    };

    // Combined efficiency score (weighted average)
    let efficiency_score = (size_util * 0.4 + throughput_eff * 0.6).min(1.0);

    // Recommendation based on efficiency
    let recommendation = if efficiency_score > 0.8 {
        "excellent_utilization"
    } else if efficiency_score > 0.6 {
        "good_utilization"
    } else if efficiency_score > 0.4 {
        "moderate_utilization_consider_optimization"
    } else {
        "poor_utilization_needs_attention"
    };

    (size_util, efficiency_score, recommendation)
}
