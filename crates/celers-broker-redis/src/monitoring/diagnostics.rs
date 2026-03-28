//! Diagnostics for Redis broker monitoring
//!
//! This module contains slowlog analysis, memory fragmentation analysis,
//! performance regression detection, task completion pattern analysis,
//! and burst detection utilities.

use serde::{Deserialize, Serialize};
use std::cmp::Reverse;

/// Analyze Redis slowlog entries to identify performance bottlenecks
///
/// Examines slowlog entries to find slow commands, calculate statistics,
/// and provide optimization recommendations.
///
/// # Parameters
/// - `slowlog_entries`: Vec of (command, duration_micros, timestamp)
/// - `threshold_micros`: Threshold for considering a command slow (microseconds)
///
/// # Returns
/// `SlowlogAnalysis` with statistics and recommendations
///
/// # Example
/// ```
/// use celers_broker_redis::monitoring::analyze_redis_slowlog;
///
/// let slowlog = vec![
///     ("ZADD".to_string(), 15000, 1234567890),
///     ("ZRANGE".to_string(), 25000, 1234567891),
///     ("LPUSH".to_string(), 5000, 1234567892),
///     ("ZADD".to_string(), 20000, 1234567893),
/// ];
///
/// let analysis = analyze_redis_slowlog(&slowlog, 10000);
/// println!("Slowest command: {}", analysis.slowest_command);
/// println!("Avg duration: {} us", analysis.avg_duration_micros);
/// assert!(analysis.total_slow_commands > 0);
/// ```
pub fn analyze_redis_slowlog(
    slowlog_entries: &[(String, u64, i64)],
    threshold_micros: u64,
) -> SlowlogAnalysis {
    if slowlog_entries.is_empty() {
        return SlowlogAnalysis {
            total_slow_commands: 0,
            unique_commands: 0,
            slowest_command: "none".to_string(),
            max_duration_micros: 0,
            avg_duration_micros: 0.0,
            p95_duration_micros: 0.0,
            p99_duration_micros: 0.0,
            command_frequency: std::collections::HashMap::new(),
            time_span_seconds: 0,
            recommendations: vec!["No slowlog entries to analyze".to_string()],
        };
    }

    let total_slow_commands = slowlog_entries.len();
    let mut durations: Vec<u64> = slowlog_entries.iter().map(|(_, d, _)| *d).collect();
    durations.sort_unstable();

    let max_duration_micros = durations.last().copied().unwrap_or(0);
    let avg_duration_micros = durations.iter().sum::<u64>() as f64 / total_slow_commands as f64;

    // Calculate percentiles
    let p95_idx = (0.95 * (total_slow_commands - 1) as f64) as usize;
    let p99_idx = (0.99 * (total_slow_commands - 1) as f64) as usize;
    let p95_duration_micros = durations[p95_idx.min(total_slow_commands - 1)] as f64;
    let p99_duration_micros = durations[p99_idx.min(total_slow_commands - 1)] as f64;

    // Count command frequency
    let mut command_frequency = std::collections::HashMap::new();
    let mut slowest_command = String::new();
    let mut slowest_duration = 0;

    for (cmd, duration, _) in slowlog_entries {
        *command_frequency.entry(cmd.clone()).or_insert(0) += 1;
        if *duration > slowest_duration {
            slowest_duration = *duration;
            slowest_command = cmd.clone();
        }
    }

    let unique_commands = command_frequency.len();

    // Time span analysis
    let timestamps: Vec<i64> = slowlog_entries.iter().map(|(_, _, t)| *t).collect();
    let time_span_seconds = if timestamps.len() > 1 {
        (timestamps
            .iter()
            .max()
            .expect("timestamps validated to be non-empty")
            - timestamps
                .iter()
                .min()
                .expect("timestamps validated to be non-empty"))
        .max(1)
    } else {
        1
    };

    // Generate recommendations
    let mut recommendations = Vec::new();

    if max_duration_micros > threshold_micros * 10 {
        recommendations.push(format!(
            "Critical: Command '{}' took {} ms - investigate immediately",
            slowest_command,
            max_duration_micros / 1000
        ));
    }

    // Find most frequent slow commands
    let mut freq_vec: Vec<_> = command_frequency.iter().collect();
    freq_vec.sort_by_key(|item| Reverse(item.1));
    if let Some((cmd, count)) = freq_vec.first() {
        if **count > total_slow_commands / 4 {
            recommendations.push(format!(
                "Command '{}' appears in {}% of slow queries - optimize this operation",
                cmd,
                (**count * 100) / total_slow_commands
            ));
        }
    }

    if avg_duration_micros > threshold_micros as f64 * 2.0 {
        recommendations
            .push("Average slow command duration is high - review query patterns".to_string());
    }

    if unique_commands < 3 && total_slow_commands > 10 {
        recommendations.push(
            "Slow queries concentrated in few commands - targeted optimization needed".to_string(),
        );
    }

    if recommendations.is_empty() {
        recommendations.push("Slowlog appears normal - continue monitoring".to_string());
    }

    SlowlogAnalysis {
        total_slow_commands,
        unique_commands,
        slowest_command,
        max_duration_micros,
        avg_duration_micros,
        p95_duration_micros,
        p99_duration_micros,
        command_frequency,
        time_span_seconds,
        recommendations,
    }
}

/// Slowlog analysis results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlowlogAnalysis {
    /// Total number of slow commands
    pub total_slow_commands: usize,
    /// Number of unique slow commands
    pub unique_commands: usize,
    /// Name of the slowest command
    pub slowest_command: String,
    /// Maximum duration in microseconds
    pub max_duration_micros: u64,
    /// Average duration in microseconds
    pub avg_duration_micros: f64,
    /// P95 duration in microseconds
    pub p95_duration_micros: f64,
    /// P99 duration in microseconds
    pub p99_duration_micros: f64,
    /// Frequency of each command type
    pub command_frequency: std::collections::HashMap<String, usize>,
    /// Time span of analysis in seconds
    pub time_span_seconds: i64,
    /// Optimization recommendations
    pub recommendations: Vec<String>,
}

/// Analyze Redis memory fragmentation and efficiency
///
/// Examines memory usage patterns to detect fragmentation issues
/// and provide optimization recommendations.
///
/// # Parameters
/// - `used_memory_bytes`: Total memory used by Redis
/// - `used_memory_rss_bytes`: Resident set size (physical memory)
/// - `mem_fragmentation_ratio`: Memory fragmentation ratio from Redis INFO
/// - `allocator_frag_ratio`: Allocator fragmentation ratio
///
/// # Returns
/// `FragmentationAnalysis` with detailed fragmentation metrics
///
/// # Example
/// ```
/// use celers_broker_redis::monitoring::analyze_redis_fragmentation;
///
/// let analysis = analyze_redis_fragmentation(
///     100_000_000,  // 100 MB used
///     150_000_000,  // 150 MB RSS
///     1.5,          // 1.5 fragmentation ratio
///     1.1,          // 1.1 allocator fragmentation
/// );
///
/// assert!(analysis.fragmentation_ratio > 1.0);
/// assert!(analysis.health_status == "warning" || analysis.health_status == "critical");
/// ```
pub fn analyze_redis_fragmentation(
    used_memory_bytes: u64,
    used_memory_rss_bytes: u64,
    mem_fragmentation_ratio: f64,
    allocator_frag_ratio: f64,
) -> FragmentationAnalysis {
    let wasted_memory_bytes = used_memory_rss_bytes.saturating_sub(used_memory_bytes);

    let wasted_memory_pct = if used_memory_rss_bytes > 0 {
        (wasted_memory_bytes as f64 / used_memory_rss_bytes as f64) * 100.0
    } else {
        0.0
    };

    // Determine health status
    let health_status = if mem_fragmentation_ratio < 1.0 {
        "critical_swapping" // Less than 1.0 means swapping
    } else if mem_fragmentation_ratio > 1.5 {
        "critical" // High fragmentation
    } else if mem_fragmentation_ratio > 1.3 {
        "warning"
    } else if mem_fragmentation_ratio < 1.1 {
        "optimal"
    } else {
        "healthy"
    };

    // Calculate severity score (0-100, higher is worse)
    let severity = if mem_fragmentation_ratio < 1.0 {
        100 // Swapping is most severe
    } else {
        let frag_score = ((mem_fragmentation_ratio - 1.0) * 100.0).min(100.0);
        let alloc_score = ((allocator_frag_ratio - 1.0) * 50.0).min(50.0);
        (frag_score + alloc_score).min(100.0) as usize
    };

    // Generate recommendations
    let mut recommendations = Vec::new();

    if mem_fragmentation_ratio < 1.0 {
        recommendations.push(
            "CRITICAL: Swapping detected - increase maxmemory or add more RAM immediately"
                .to_string(),
        );
    } else if mem_fragmentation_ratio > 1.5 {
        recommendations.push(
            "High fragmentation - consider MEMORY PURGE or restart during maintenance window"
                .to_string(),
        );
        recommendations.push(
            "Review data access patterns - frequent updates to large values cause fragmentation"
                .to_string(),
        );
    } else if mem_fragmentation_ratio > 1.3 {
        recommendations
            .push("Moderate fragmentation - monitor closely and plan defragmentation".to_string());
    }

    if allocator_frag_ratio > 1.2 {
        recommendations.push(
            "Allocator fragmentation detected - review memory allocator settings".to_string(),
        );
    }

    if wasted_memory_pct > 30.0 {
        recommendations.push(format!(
            "Significant memory waste ({:.1}%) - defragmentation recommended",
            wasted_memory_pct
        ));
    }

    let defragmentation_recommended = mem_fragmentation_ratio > 1.3;

    if recommendations.is_empty() {
        recommendations.push("Memory fragmentation is within acceptable limits".to_string());
    }

    FragmentationAnalysis {
        fragmentation_ratio: mem_fragmentation_ratio,
        allocator_frag_ratio,
        used_memory_bytes,
        used_memory_rss_bytes,
        wasted_memory_bytes,
        wasted_memory_pct,
        health_status: health_status.to_string(),
        severity,
        defragmentation_recommended,
        recommendations,
    }
}

/// Memory fragmentation analysis results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FragmentationAnalysis {
    /// Memory fragmentation ratio
    pub fragmentation_ratio: f64,
    /// Allocator fragmentation ratio
    pub allocator_frag_ratio: f64,
    /// Used memory in bytes
    pub used_memory_bytes: u64,
    /// RSS memory in bytes
    pub used_memory_rss_bytes: u64,
    /// Wasted memory due to fragmentation (bytes)
    pub wasted_memory_bytes: u64,
    /// Wasted memory percentage
    pub wasted_memory_pct: f64,
    /// Health status (optimal/healthy/warning/critical/critical_swapping)
    pub health_status: String,
    /// Severity score (0-100, higher is worse)
    pub severity: usize,
    /// Whether defragmentation is recommended
    pub defragmentation_recommended: bool,
    /// Optimization recommendations
    pub recommendations: Vec<String>,
}

/// Detect performance regression by comparing current metrics with baseline
///
/// Compares current performance metrics against a baseline to detect
/// degradation in throughput, latency, or error rates.
///
/// # Parameters
/// - `current_throughput`: Current messages per second
/// - `baseline_throughput`: Baseline messages per second
/// - `current_p99_latency_ms`: Current P99 latency in milliseconds
/// - `baseline_p99_latency_ms`: Baseline P99 latency in milliseconds
/// - `current_error_rate`: Current error rate (0.0-1.0)
/// - `baseline_error_rate`: Baseline error rate (0.0-1.0)
///
/// # Returns
/// `RegressionDetection` with detected regressions and severity
///
/// # Example
/// ```
/// use celers_broker_redis::monitoring::detect_performance_regression;
///
/// let regression = detect_performance_regression(
///     80.0,   // Current throughput (80 msg/s)
///     100.0,  // Baseline throughput (100 msg/s)
///     150.0,  // Current P99 latency (150 ms)
///     100.0,  // Baseline P99 latency (100 ms)
///     0.03,   // Current error rate (3%)
///     0.01,   // Baseline error rate (1%)
/// );
///
/// assert!(regression.regression_detected);
/// assert!(regression.throughput_degradation_pct > 0.0);
/// ```
pub fn detect_performance_regression(
    current_throughput: f64,
    baseline_throughput: f64,
    current_p99_latency_ms: f64,
    baseline_p99_latency_ms: f64,
    current_error_rate: f64,
    baseline_error_rate: f64,
) -> RegressionDetection {
    // Calculate degradation percentages
    let throughput_degradation_pct = if baseline_throughput > 0.0 {
        ((baseline_throughput - current_throughput) / baseline_throughput * 100.0).max(0.0)
    } else {
        0.0
    };

    let latency_increase_pct = if baseline_p99_latency_ms > 0.0 {
        ((current_p99_latency_ms - baseline_p99_latency_ms) / baseline_p99_latency_ms * 100.0)
            .max(0.0)
    } else {
        0.0
    };

    let error_rate_increase_pct = if baseline_error_rate > 0.0 {
        ((current_error_rate - baseline_error_rate) / baseline_error_rate * 100.0).max(0.0)
    } else if current_error_rate > 0.0 {
        100.0
    } else {
        0.0
    };

    // Detect regressions (>10% degradation is significant)
    let throughput_regression = throughput_degradation_pct > 10.0;
    let latency_regression = latency_increase_pct > 10.0;
    let error_rate_regression = error_rate_increase_pct > 50.0 || current_error_rate > 0.05;

    let regression_detected = throughput_regression || latency_regression || error_rate_regression;

    // Calculate overall severity (0-100)
    let severity = ((throughput_degradation_pct.min(100.0)
        + latency_increase_pct.min(100.0)
        + error_rate_increase_pct.min(100.0))
        / 3.0) as usize;

    // Generate detailed findings
    let mut regressions_found = Vec::new();
    if throughput_regression {
        regressions_found.push(format!(
            "Throughput degraded by {:.1}% (baseline: {:.1} msg/s, current: {:.1} msg/s)",
            throughput_degradation_pct, baseline_throughput, current_throughput
        ));
    }
    if latency_regression {
        regressions_found.push(format!(
            "P99 latency increased by {:.1}% (baseline: {:.1} ms, current: {:.1} ms)",
            latency_increase_pct, baseline_p99_latency_ms, current_p99_latency_ms
        ));
    }
    if error_rate_regression {
        regressions_found.push(format!(
            "Error rate increased significantly (baseline: {:.2}%, current: {:.2}%)",
            baseline_error_rate * 100.0,
            current_error_rate * 100.0
        ));
    }

    // Generate recommendations
    let mut recommendations = Vec::new();
    if regression_detected {
        if severity > 50 {
            recommendations.push(
                "Critical regression detected - immediate investigation required".to_string(),
            );
        } else {
            recommendations
                .push("Performance regression detected - review recent changes".to_string());
        }

        if throughput_regression && latency_regression {
            recommendations.push(
                "Both throughput and latency degraded - check for resource constraints".to_string(),
            );
        }

        if error_rate_regression {
            recommendations.push("Error rate spike - review logs for failure patterns".to_string());
        }

        recommendations
            .push("Compare with previous deployments or configuration changes".to_string());
    } else {
        recommendations.push("No significant performance regression detected".to_string());
    }

    RegressionDetection {
        regression_detected,
        throughput_degradation_pct,
        latency_increase_pct,
        error_rate_increase_pct,
        severity,
        regressions_found,
        recommendations,
    }
}

/// Performance regression detection results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegressionDetection {
    /// Whether any regression was detected
    pub regression_detected: bool,
    /// Throughput degradation percentage
    pub throughput_degradation_pct: f64,
    /// Latency increase percentage
    pub latency_increase_pct: f64,
    /// Error rate increase percentage
    pub error_rate_increase_pct: f64,
    /// Overall severity score (0-100)
    pub severity: usize,
    /// List of specific regressions found
    pub regressions_found: Vec<String>,
    /// Actionable recommendations
    pub recommendations: Vec<String>,
}

/// Analyze task completion patterns and success rates
///
/// Tracks task completion metrics to identify patterns in success/failure rates,
/// completion times, and task lifecycle characteristics.
///
/// # Parameters
/// - `completed_tasks`: Number of successfully completed tasks
/// - `failed_tasks`: Number of failed tasks
/// - `retried_tasks`: Number of tasks that required retry
/// - `avg_completion_time_ms`: Average task completion time in milliseconds
/// - `total_processing_time_ms`: Total processing time for all tasks
///
/// # Returns
/// `TaskCompletionAnalysis` with success rates and patterns
///
/// # Example
/// ```
/// use celers_broker_redis::monitoring::analyze_task_completion_patterns;
///
/// let analysis = analyze_task_completion_patterns(850, 50, 100, 250.0, 225000.0);
///
/// assert!(analysis.total_tasks == 900);
/// assert!(analysis.success_rate > 0.9);
/// assert!(analysis.retry_rate > 0.0);
/// ```
pub fn analyze_task_completion_patterns(
    completed_tasks: usize,
    failed_tasks: usize,
    retried_tasks: usize,
    avg_completion_time_ms: f64,
    total_processing_time_ms: f64,
) -> TaskCompletionAnalysis {
    let total_tasks = completed_tasks + failed_tasks;
    let success_rate = if total_tasks > 0 {
        completed_tasks as f64 / total_tasks as f64
    } else {
        0.0
    };

    let failure_rate = if total_tasks > 0 {
        failed_tasks as f64 / total_tasks as f64
    } else {
        0.0
    };

    let retry_rate = if total_tasks > 0 {
        retried_tasks as f64 / total_tasks as f64
    } else {
        0.0
    };

    // Estimate efficiency (how much processing time vs total time)
    let efficiency = if total_processing_time_ms > 0.0 && total_tasks > 0 {
        let expected_time = avg_completion_time_ms * total_tasks as f64;
        (expected_time / total_processing_time_ms).min(1.0)
    } else {
        0.0
    };

    // Determine health status
    let health_status = if success_rate >= 0.99 {
        "excellent"
    } else if success_rate >= 0.95 {
        "good"
    } else if success_rate >= 0.90 {
        "acceptable"
    } else if success_rate >= 0.80 {
        "concerning"
    } else {
        "poor"
    };

    // Generate recommendations
    let mut recommendations = Vec::new();

    if failure_rate > 0.10 {
        recommendations.push(format!(
            "High failure rate ({:.1}%) - investigate error causes",
            failure_rate * 100.0
        ));
    }

    if retry_rate > 0.20 {
        recommendations.push(format!(
            "High retry rate ({:.1}%) - review task reliability",
            retry_rate * 100.0
        ));
    }

    if efficiency < 0.7 && total_tasks > 10 {
        recommendations.push(format!(
            "Low processing efficiency ({:.1}%) - check for bottlenecks",
            efficiency * 100.0
        ));
    }

    if avg_completion_time_ms > 5000.0 {
        recommendations.push("High average completion time - optimize task processing".to_string());
    }

    if recommendations.is_empty() {
        recommendations.push("Task completion patterns appear healthy".to_string());
    }

    TaskCompletionAnalysis {
        total_tasks,
        completed_tasks,
        failed_tasks,
        retried_tasks,
        success_rate,
        failure_rate,
        retry_rate,
        avg_completion_time_ms,
        efficiency,
        health_status: health_status.to_string(),
        recommendations,
    }
}

/// Task completion analysis results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskCompletionAnalysis {
    /// Total number of tasks
    pub total_tasks: usize,
    /// Number of completed tasks
    pub completed_tasks: usize,
    /// Number of failed tasks
    pub failed_tasks: usize,
    /// Number of retried tasks
    pub retried_tasks: usize,
    /// Success rate (0.0-1.0)
    pub success_rate: f64,
    /// Failure rate (0.0-1.0)
    pub failure_rate: f64,
    /// Retry rate (0.0-1.0)
    pub retry_rate: f64,
    /// Average completion time in milliseconds
    pub avg_completion_time_ms: f64,
    /// Processing efficiency (0.0-1.0)
    pub efficiency: f64,
    /// Health status (excellent/good/acceptable/concerning/poor)
    pub health_status: String,
    /// Optimization recommendations
    pub recommendations: Vec<String>,
}

/// Detect sudden bursts or spikes in queue activity
///
/// Analyzes recent queue size history to detect sudden increases
/// that might indicate traffic spikes or system issues.
///
/// # Parameters
/// - `recent_queue_sizes`: Recent queue sizes (most recent last)
/// - `window_size`: Number of recent measurements to consider for baseline
///
/// # Returns
/// `BurstDetection` with burst information and severity
///
/// # Example
/// ```
/// use celers_broker_redis::monitoring::detect_queue_burst;
///
/// let queue_sizes = vec![100, 105, 98, 102, 500, 520]; // Spike at end
/// let burst = detect_queue_burst(&queue_sizes, 4);
///
/// assert!(burst.burst_detected);
/// assert!(burst.burst_magnitude > 1.0);
/// ```
pub fn detect_queue_burst(recent_queue_sizes: &[usize], window_size: usize) -> BurstDetection {
    if recent_queue_sizes.len() < 2 {
        return BurstDetection {
            burst_detected: false,
            baseline_size: 0,
            current_size: recent_queue_sizes.last().copied().unwrap_or(0),
            burst_magnitude: 0.0,
            burst_rate: 0.0,
            severity: 0,
            recommendations: vec!["Insufficient data for burst detection".to_string()],
        };
    }

    let current_size = *recent_queue_sizes
        .last()
        .expect("recent_queue_sizes validated to be non-empty");
    let baseline_window = window_size.min(recent_queue_sizes.len() - 1);

    // Calculate baseline from recent history (excluding current)
    let baseline_start = if recent_queue_sizes.len() > baseline_window + 1 {
        recent_queue_sizes.len() - baseline_window - 1
    } else {
        0
    };
    let baseline_slice = &recent_queue_sizes[baseline_start..recent_queue_sizes.len() - 1];
    let baseline_size = if !baseline_slice.is_empty() {
        baseline_slice.iter().sum::<usize>() / baseline_slice.len()
    } else {
        current_size
    };

    // Calculate standard deviation for baseline
    let baseline_variance = if baseline_slice.len() > 1 {
        let mean = baseline_size as f64;
        let sum_sq_diff: f64 = baseline_slice
            .iter()
            .map(|&x| {
                let diff = x as f64 - mean;
                diff * diff
            })
            .sum();
        (sum_sq_diff / baseline_slice.len() as f64).sqrt()
    } else {
        0.0
    };

    // Detect burst (current is significantly higher than baseline)
    let burst_magnitude = if baseline_size > 0 {
        current_size as f64 / baseline_size as f64
    } else if current_size > 0 {
        f64::INFINITY
    } else {
        1.0
    };

    // Calculate burst rate (change from previous)
    let burst_rate = if recent_queue_sizes.len() > 1 {
        let prev_size = recent_queue_sizes[recent_queue_sizes.len() - 2];
        if prev_size > 0 {
            ((current_size as f64 - prev_size as f64) / prev_size as f64).max(0.0)
        } else {
            0.0
        }
    } else {
        0.0
    };

    // Burst detected if current is >2x baseline or >3 std devs above mean
    let threshold_magnitude = 2.0;
    let std_dev_threshold = 3.0;
    let burst_detected = burst_magnitude > threshold_magnitude
        || (baseline_variance > 0.0
            && (current_size as f64 - baseline_size as f64)
                > std_dev_threshold * baseline_variance);

    // Calculate severity (0-100)
    let severity = if burst_detected {
        let mag_score = ((burst_magnitude - 1.0) * 20.0).min(100.0);
        let rate_score = (burst_rate * 50.0).min(100.0);
        ((mag_score + rate_score) / 2.0).min(100.0) as usize
    } else {
        0
    };

    // Generate recommendations
    let mut recommendations = Vec::new();
    if burst_detected {
        if severity > 75 {
            recommendations.push("Critical burst detected - scale workers immediately".to_string());
        } else if severity > 50 {
            recommendations.push("Significant burst detected - prepare to scale".to_string());
        } else {
            recommendations.push("Moderate burst detected - monitor closely".to_string());
        }

        if burst_magnitude > 5.0 {
            recommendations
                .push("Queue size increased >5x - investigate traffic source".to_string());
        }

        if burst_rate > 1.0 {
            recommendations.push("Rapid growth rate - queue doubling or faster".to_string());
        }
    } else {
        recommendations.push("No unusual burst activity detected".to_string());
    }

    BurstDetection {
        burst_detected,
        baseline_size,
        current_size,
        burst_magnitude,
        burst_rate,
        severity,
        recommendations,
    }
}

/// Queue burst detection results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BurstDetection {
    /// Whether a burst was detected
    pub burst_detected: bool,
    /// Baseline queue size
    pub baseline_size: usize,
    /// Current queue size
    pub current_size: usize,
    /// Burst magnitude (current/baseline ratio)
    pub burst_magnitude: f64,
    /// Burst rate (rate of change)
    pub burst_rate: f64,
    /// Severity score (0-100)
    pub severity: usize,
    /// Actionable recommendations
    pub recommendations: Vec<String>,
}

#[cfg(test)]
mod tests {
    use crate::monitoring::*;
    use std::collections::HashMap;

    #[test]
    fn test_analyze_consumer_lag_optimal() {
        let lag = analyze_redis_consumer_lag(100, 50.0, 10);
        assert_eq!(lag.queue_size, 100);
        assert_eq!(lag.processing_rate, 50.0);
        assert_eq!(lag.lag_seconds, 2.0);
        assert!(!lag.is_lagging);
        assert_eq!(lag.recommendation, ScalingRecommendation::Optimal);
    }

    #[test]
    fn test_analyze_consumer_lag_needs_scale_up() {
        let lag = analyze_redis_consumer_lag(1000, 5.0, 10);
        assert!(lag.is_lagging);
        assert!(matches!(
            lag.recommendation,
            ScalingRecommendation::ScaleUp { .. }
        ));
    }

    #[test]
    fn test_calculate_message_velocity_growing() {
        let velocity = calculate_redis_message_velocity(1000, 1600, 60.0);
        assert_eq!(velocity.velocity, 10.0);
        assert_eq!(velocity.trend, QueueTrend::SlowGrowth);
    }

    #[test]
    fn test_calculate_message_velocity_stable() {
        let velocity = calculate_redis_message_velocity(1000, 1010, 60.0);
        assert!(velocity.velocity < 1.0);
        assert_eq!(velocity.trend, QueueTrend::Stable);
    }

    #[test]
    fn test_suggest_worker_scaling() {
        let scaling = suggest_redis_worker_scaling(2000, 5, 40.0, 100);
        assert_eq!(scaling.current_workers, 5);
        assert!(scaling.recommended_workers >= 1);
    }

    #[test]
    fn test_message_age_distribution() {
        let ages = vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0];
        let dist = calculate_redis_message_age_distribution(&ages, 75.0);
        assert_eq!(dist.total_messages, 10);
        assert_eq!(dist.min_age_secs, 10.0);
        assert_eq!(dist.max_age_secs, 100.0);
        assert_eq!(dist.messages_exceeding_sla, 3);
    }

    #[test]
    fn test_message_age_distribution_empty() {
        let ages = vec![];
        let dist = calculate_redis_message_age_distribution(&ages, 60.0);
        assert_eq!(dist.total_messages, 0);
    }

    #[test]
    fn test_estimate_processing_capacity() {
        let capacity = estimate_redis_processing_capacity(10, 50.0, 5000);
        assert_eq!(capacity.workers, 10);
        assert_eq!(capacity.total_capacity_per_sec, 500.0);
        assert_eq!(capacity.total_capacity_per_min, 30000.0);
        assert_eq!(capacity.time_to_clear_backlog_secs, 10.0);
    }

    #[test]
    fn test_calculate_queue_health_score() {
        let score = calculate_redis_queue_health_score(100, 50.0, 1000, 40.0);
        assert!(score > 0.5);
        assert!(score <= 1.0);
    }

    #[test]
    fn test_analyze_broker_performance() {
        let mut metrics = HashMap::new();
        metrics.insert("avg_latency_ms".to_string(), 25.0);
        metrics.insert("throughput_msg_per_sec".to_string(), 500.0);
        metrics.insert("error_rate_percent".to_string(), 0.5);

        let analysis = analyze_redis_broker_performance(&metrics);
        assert_eq!(analysis.get("latency_status"), Some(&"good".to_string()));
        assert_eq!(
            analysis.get("throughput_status"),
            Some(&"medium".to_string())
        );
        assert_eq!(
            analysis.get("error_rate_status"),
            Some(&"healthy".to_string())
        );
    }

    #[test]
    fn test_detect_queue_saturation_healthy() {
        let saturation = detect_redis_queue_saturation(3000, 10000, Some(10.0));
        assert_eq!(saturation.current_size, 3000);
        assert_eq!(saturation.max_capacity, 10000);
        assert_eq!(saturation.saturation_percentage, 0.3);
        assert_eq!(saturation.level, SaturationLevel::Healthy);
        assert_eq!(saturation.time_until_full_secs, Some(700.0));
    }

    #[test]
    fn test_detect_queue_saturation_critical() {
        let saturation = detect_redis_queue_saturation(9500, 10000, None);
        assert_eq!(saturation.saturation_percentage, 0.95);
        assert_eq!(saturation.level, SaturationLevel::Critical);
        assert_eq!(saturation.time_until_full_secs, None);
    }

    #[test]
    fn test_detect_anomaly_no_historical_data() {
        let anomaly = detect_redis_queue_anomaly(100.0, &[], 2.0);
        assert!(!anomaly.is_anomaly);
        assert_eq!(anomaly.baseline, 100.0);
    }

    #[test]
    fn test_detect_anomaly_normal_range() {
        let historical = vec![100.0, 105.0, 98.0, 102.0, 101.0, 99.0];
        let anomaly = detect_redis_queue_anomaly(103.0, &historical, 2.0);
        assert!(!anomaly.is_anomaly);
        assert!(anomaly.severity < 1.0);
    }

    #[test]
    fn test_detect_anomaly_spike() {
        let historical = vec![100.0, 105.0, 98.0, 102.0, 101.0, 99.0];
        let anomaly = detect_redis_queue_anomaly(500.0, &historical, 2.0);
        assert!(anomaly.is_anomaly);
        assert!(anomaly.severity > 1.0);
        assert!(anomaly.description.contains("Spike detected"));
    }

    #[test]
    fn test_predict_queue_size_growth() {
        let prediction = predict_redis_queue_size(1000, 5.0, 300, 5000);
        assert_eq!(prediction.current_size, 1000);
        assert_eq!(prediction.predicted_size, 2500); // 1000 + (5.0 * 300)
        assert!(!prediction.will_exceed_threshold);
        assert!(prediction.confidence > 0.0 && prediction.confidence <= 1.0);
    }

    #[test]
    fn test_predict_queue_size_exceeds_threshold() {
        let prediction = predict_redis_queue_size(1000, 10.0, 600, 5000);
        assert_eq!(prediction.predicted_size, 7000); // 1000 + (10.0 * 600)
        assert!(prediction.will_exceed_threshold);
        assert!(prediction.recommendation.contains("WARNING"));
    }

    #[test]
    fn test_predict_queue_size_draining() {
        let prediction = predict_redis_queue_size(6000, -5.0, 300, 10000);
        assert_eq!(prediction.predicted_size, 4500); // 6000 - (5.0 * 300)
        assert!(!prediction.will_exceed_threshold);
        assert!(
            prediction.recommendation.contains("draining")
                || prediction.recommendation.contains("scaling down")
        );
    }

    #[test]
    fn test_memory_efficiency_optimal() {
        let analysis = analyze_redis_memory_efficiency(
            10_000_000, // 10MB used
            8_000_000,  // 8MB queue data
            11_000_000, // 11MB allocated (low fragmentation)
        );
        assert_eq!(analysis.total_memory_bytes, 10_000_000);
        assert_eq!(analysis.queue_memory_bytes, 8_000_000);
        assert!(analysis.efficiency_score >= 0.8);
        assert!(analysis.fragmentation_ratio < 1.5);
        assert!(!analysis.needs_optimization);
    }

    #[test]
    fn test_memory_efficiency_high_overhead() {
        let analysis = analyze_redis_memory_efficiency(
            10_000_000, // 10MB used
            6_000_000,  // 6MB queue data (40% overhead)
            10_500_000, // 10.5MB allocated
        );
        assert!(analysis.overhead_percentage > 30.0);
        assert!(analysis.needs_optimization);
        assert!(!analysis.recommendations.is_empty());
    }

    #[test]
    fn test_memory_efficiency_high_fragmentation() {
        let analysis = analyze_redis_memory_efficiency(
            10_000_000, // 10MB used
            8_000_000,  // 8MB queue data
            17_000_000, // 17MB allocated (1.7x fragmentation)
        );
        assert!(analysis.fragmentation_ratio > 1.5);
        assert!(analysis.needs_optimization);
        assert!(analysis
            .recommendations
            .iter()
            .any(|r| r.contains("fragmentation")));
    }

    #[test]
    fn test_scaling_strategy_optimal() {
        let strategy = recommend_redis_scaling_strategy(
            500,   // small queue
            100.0, // good processing rate
            5,     // current workers
            50.0,  // low memory
            40.0,  // low CPU
        );
        assert_eq!(strategy.action, "No scaling needed");
        assert_eq!(strategy.priority, 1);
        assert!(strategy.cost_efficiency > 0.8);
    }

    #[test]
    fn test_scaling_strategy_memory_constrained() {
        let strategy = recommend_redis_scaling_strategy(
            5000, // moderate queue
            50.0, // moderate rate
            5,    // current workers
            85.0, // high memory!
            60.0, // moderate CPU
        );
        assert_eq!(strategy.action, "Scale vertically (upgrade instance)");
        assert_eq!(strategy.priority, 3);
        assert!(strategy
            .recommendations
            .iter()
            .any(|r| r.contains("CRITICAL")));
    }

    #[test]
    fn test_scaling_strategy_queue_backed_up() {
        let strategy = recommend_redis_scaling_strategy(
            15000, // large queue!
            50.0,  // moderate rate
            5,     // current workers
            60.0,  // moderate memory
            80.0,  // high CPU
        );
        assert_eq!(strategy.action, "Scale horizontally (add workers)");
        assert_eq!(strategy.priority, 3);
        assert!(strategy.recommended_workers.unwrap_or(0) > 5);
    }

    #[test]
    fn test_analyze_dlq_health_empty() {
        let analysis = analyze_redis_dlq_health(0, 1000, 5000, 0);
        assert_eq!(analysis.total_messages, 0);
        assert_eq!(analysis.dlq_percentage, 0.0);
        assert!(!analysis.is_alarming);
        assert!(analysis.recommendation.contains("empty"));
    }

    #[test]
    fn test_analyze_dlq_health_alarming() {
        // DLQ grew from 50 to 100 (50 new failures out of 800 processed = 6.25% error rate > 5% threshold)
        let analysis = analyze_redis_dlq_health(100, 1000, 800, 50);
        assert!(analysis.is_alarming);
        assert!(analysis.error_rate > 0.05);
        assert!(analysis.recommendation.contains("ALERT"));
    }

    #[test]
    fn test_analyze_dlq_health_acceptable() {
        let analysis = analyze_redis_dlq_health(10, 1000, 10000, 5);
        assert!(!analysis.is_alarming);
        assert!(analysis.dlq_percentage < 0.1);
    }

    #[test]
    fn test_estimate_redis_monthly_cost_aws() {
        let cost = estimate_redis_monthly_cost(1024, 1000.0, "aws");
        assert_eq!(cost.memory_mb, 1024);
        assert_eq!(cost.memory_gb, 1.0);
        assert!(cost.total_monthly_cost > 0.0);
        assert!(cost.memory_cost_monthly > 0.0);
        assert!(cost.operations_cost_monthly > 0.0);
        assert_eq!(cost.provider, "aws");
    }

    #[test]
    fn test_estimate_redis_monthly_cost_gcp() {
        let cost = estimate_redis_monthly_cost(2048, 500.0, "gcp");
        assert_eq!(cost.memory_mb, 2048);
        assert_eq!(cost.provider, "gcp");
        assert!(cost.total_monthly_cost > 0.0);
    }

    #[test]
    fn test_estimate_redis_monthly_cost_optimization() {
        let cost_overprovisioned = estimate_redis_monthly_cost(8192, 50.0, "aws");
        assert!(cost_overprovisioned.optimization_potential.contains("High"));

        let cost_underprovisioned = estimate_redis_monthly_cost(256, 15000.0, "aws");
        assert!(cost_underprovisioned
            .optimization_potential
            .contains("Medium"));

        let cost_optimal = estimate_redis_monthly_cost(1024, 1000.0, "aws");
        assert!(cost_optimal.optimization_potential.contains("Low"));
    }

    #[test]
    fn test_recommend_alert_thresholds() {
        let thresholds = recommend_alert_thresholds(100, 500, 60);
        assert_eq!(thresholds.queue_size_warning, 350); // 70% of 500
        assert_eq!(thresholds.queue_size_critical, 450); // 90% of 500
        assert!(thresholds.queue_size_warning < thresholds.queue_size_critical);
        assert_eq!(thresholds.lag_warning_seconds, 48); // 80% of 60
        assert_eq!(thresholds.lag_critical_seconds, 60);
        assert!(thresholds.dlq_size_warning >= 10);
        assert!(thresholds.dlq_size_critical >= 50);
        assert_eq!(thresholds.error_rate_warning, 0.01);
        assert_eq!(thresholds.error_rate_critical, 0.05);
    }

    #[test]
    fn test_generate_queue_health_report_healthy() {
        let report = generate_queue_health_report(100, 5, 2, 50.0, 1, 1000, 256, 1024);
        assert_eq!(report.overall_status, "healthy");
        assert_eq!(report.queue_size, 100);
        assert_eq!(report.dlq_size, 2);
        assert!(report.error_rate < 0.01);
        assert!(report.memory_usage < 0.75);
    }

    #[test]
    fn test_generate_queue_health_report_warning() {
        let report = generate_queue_health_report(1500, 10, 15, 50.0, 50, 1000, 800, 1024);
        assert_eq!(report.overall_status, "warning");
        assert!(report.queue_health == "warning");
        assert!(report.error_rate > 0.01);
        assert!(!report.recommendations.is_empty());
    }

    #[test]
    fn test_generate_queue_health_report_critical() {
        let report = generate_queue_health_report(12000, 20, 150, 10.0, 100, 1000, 950, 1024);
        assert_eq!(report.overall_status, "critical");
        assert_eq!(report.queue_health, "critical");
        assert_eq!(report.dlq_health, "critical");
        assert_eq!(report.memory_health, "critical");
        assert!(!report.recommendations.is_empty());
    }

    #[test]
    fn test_analyze_performance_trend_empty() {
        let trend = analyze_performance_trend(&[]);
        assert_eq!(trend.data_points, 0);
        assert_eq!(trend.queue_size_trend, "insufficient_data");
        assert!(!trend.recommendations.is_empty());
    }

    #[test]
    fn test_analyze_performance_trend_increasing() {
        let metrics = vec![(0.0, 100, 50.0), (60.0, 200, 48.0), (120.0, 300, 45.0)];
        let trend = analyze_performance_trend(&metrics);
        assert_eq!(trend.data_points, 3);
        assert_eq!(trend.queue_size_trend, "increasing");
        assert_eq!(trend.processing_rate_trend, "stable");
        assert!(trend.predicted_queue_size_1h > 300);
    }

    #[test]
    fn test_analyze_performance_trend_degrading() {
        let metrics = vec![(0.0, 100, 100.0), (60.0, 150, 80.0), (120.0, 200, 50.0)];
        let trend = analyze_performance_trend(&metrics);
        assert_eq!(trend.processing_rate_trend, "degrading");
        assert!(!trend.recommendations.is_empty());
    }

    #[test]
    fn test_analyze_performance_trend_stable() {
        let metrics = vec![(0.0, 100, 50.0), (60.0, 105, 51.0), (120.0, 98, 49.0)];
        let trend = analyze_performance_trend(&metrics);
        assert_eq!(trend.queue_size_trend, "stable");
        assert_eq!(trend.processing_rate_trend, "stable");
    }
}
