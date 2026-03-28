//! Redis broker monitoring utilities
//!
//! This module provides production-grade monitoring and analysis utilities
//! for Redis-based task queues. These utilities help with capacity planning,
//! autoscaling decisions, SLA monitoring, and performance optimization.
//!
//! # Examples
//!
//! ```
//! use celers_broker_redis::monitoring::*;
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Analyze consumer lag and get scaling recommendations
//! let queue_size = 1000;
//! let processing_rate = 50.0; // messages/sec
//! let lag = analyze_redis_consumer_lag(queue_size, processing_rate, 100);
//! println!("Queue lag: {} seconds", lag.lag_seconds);
//! println!("Recommendation: {:?}", lag.recommendation);
//!
//! // Calculate message velocity and growth trends
//! let velocity = calculate_redis_message_velocity(
//!     1000, // previous size
//!     1500, // current size
//!     60.0  // time window (seconds)
//! );
//! println!("Queue growing at {} msg/sec", velocity.velocity);
//!
//! // Get worker scaling recommendation
//! let scaling = suggest_redis_worker_scaling(
//!     2000,  // queue_size
//!     5,     // current_workers
//!     40.0,  // avg_processing_rate (msg/sec per worker)
//!     100    // target_lag_seconds
//! );
//! println!("Suggested workers: {}", scaling.recommended_workers);
//! # Ok(())
//! # }
//! ```

mod analytics;
mod diagnostics;

pub use analytics::*;
pub use diagnostics::*;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Consumer lag analysis with autoscaling recommendations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerLagAnalysis {
    /// Current queue size
    pub queue_size: usize,
    /// Processing rate (messages per second)
    pub processing_rate: f64,
    /// Target acceptable lag (seconds)
    pub target_lag_seconds: u64,
    /// Calculated lag in seconds
    pub lag_seconds: f64,
    /// Whether the lag exceeds the target
    pub is_lagging: bool,
    /// Scaling recommendation
    pub recommendation: ScalingRecommendation,
}

/// Worker scaling recommendation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScalingRecommendation {
    /// Scale up workers
    ScaleUp { additional_workers: usize },
    /// Current workers are sufficient
    Optimal,
    /// Can scale down workers
    ScaleDown { workers_to_remove: usize },
}

/// Message velocity and queue growth trend
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageVelocity {
    /// Previous queue size
    pub previous_size: usize,
    /// Current queue size
    pub current_size: usize,
    /// Time window (seconds)
    pub time_window_secs: f64,
    /// Messages per second (positive = growing, negative = shrinking)
    pub velocity: f64,
    /// Queue growth trend
    pub trend: QueueTrend,
}

/// Queue growth trend classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueueTrend {
    /// Queue is growing rapidly (> 10 msg/sec)
    RapidGrowth,
    /// Queue is growing slowly (1-10 msg/sec)
    SlowGrowth,
    /// Queue is stable (< 1 msg/sec change)
    Stable,
    /// Queue is shrinking slowly (-10 to -1 msg/sec)
    SlowShrink,
    /// Queue is shrinking rapidly (< -10 msg/sec)
    RapidShrink,
}

/// Worker scaling suggestion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerScalingSuggestion {
    /// Current queue size
    pub queue_size: usize,
    /// Current number of workers
    pub current_workers: usize,
    /// Average processing rate per worker (msg/sec)
    pub avg_processing_rate: f64,
    /// Target lag in seconds
    pub target_lag_seconds: u64,
    /// Recommended number of workers
    pub recommended_workers: usize,
    /// Scaling action needed
    pub action: ScalingRecommendation,
}

/// Message age distribution for SLA monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageAgeDistribution {
    /// Total messages analyzed
    pub total_messages: usize,
    /// Minimum age (seconds)
    pub min_age_secs: f64,
    /// Maximum age (seconds)
    pub max_age_secs: f64,
    /// Average age (seconds)
    pub avg_age_secs: f64,
    /// 50th percentile (median) age (seconds)
    pub p50_age_secs: f64,
    /// 95th percentile age (seconds)
    pub p95_age_secs: f64,
    /// 99th percentile age (seconds)
    pub p99_age_secs: f64,
    /// Messages older than SLA threshold
    pub messages_exceeding_sla: usize,
}

/// Processing capacity estimation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingCapacity {
    /// Number of workers
    pub workers: usize,
    /// Average processing rate per worker (msg/sec)
    pub rate_per_worker: f64,
    /// Total system capacity (msg/sec)
    pub total_capacity_per_sec: f64,
    /// Total system capacity (msg/min)
    pub total_capacity_per_min: f64,
    /// Total system capacity (msg/hour)
    pub total_capacity_per_hour: f64,
    /// Time to process backlog (seconds)
    pub time_to_clear_backlog_secs: f64,
}

/// Queue saturation level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SaturationLevel {
    /// Queue is healthy (< 50% capacity)
    Healthy,
    /// Queue is moderate (50-70% capacity)
    Moderate,
    /// Queue is high (70-90% capacity)
    High,
    /// Queue is critical (> 90% capacity)
    Critical,
}

/// Analyze consumer lag and provide scaling recommendations
///
/// # Arguments
///
/// * `queue_size` - Current number of messages in queue
/// * `processing_rate` - Current processing rate (messages per second)
/// * `target_lag_seconds` - Target acceptable lag in seconds
///
/// # Returns
///
/// Consumer lag analysis with scaling recommendation
///
/// # Examples
///
/// ```
/// use celers_broker_redis::monitoring::analyze_redis_consumer_lag;
///
/// let lag = analyze_redis_consumer_lag(1000, 50.0, 100);
/// assert_eq!(lag.queue_size, 1000);
/// assert_eq!(lag.processing_rate, 50.0);
/// ```
pub fn analyze_redis_consumer_lag(
    queue_size: usize,
    processing_rate: f64,
    target_lag_seconds: u64,
) -> ConsumerLagAnalysis {
    let lag_seconds = if processing_rate > 0.0 {
        queue_size as f64 / processing_rate
    } else {
        f64::INFINITY
    };

    let is_lagging = lag_seconds > target_lag_seconds as f64;

    let recommendation = if is_lagging {
        let target_rate = queue_size as f64 / target_lag_seconds as f64;
        let additional_capacity_needed = target_rate - processing_rate;
        let workers_needed = (additional_capacity_needed / processing_rate).ceil() as usize;
        ScalingRecommendation::ScaleUp {
            additional_workers: workers_needed.max(1),
        }
    } else if lag_seconds < (target_lag_seconds as f64 * 0.5) && queue_size > 0 {
        // If lag is less than 50% of target, consider scaling down
        let excess_capacity = processing_rate - (queue_size as f64 / target_lag_seconds as f64);
        let workers_to_remove = (excess_capacity / processing_rate).floor() as usize;
        if workers_to_remove > 0 {
            ScalingRecommendation::ScaleDown { workers_to_remove }
        } else {
            ScalingRecommendation::Optimal
        }
    } else {
        ScalingRecommendation::Optimal
    };

    ConsumerLagAnalysis {
        queue_size,
        processing_rate,
        target_lag_seconds,
        lag_seconds,
        is_lagging,
        recommendation,
    }
}

/// Calculate message velocity and queue growth trend
///
/// # Arguments
///
/// * `previous_size` - Queue size at start of window
/// * `current_size` - Queue size at end of window
/// * `time_window_secs` - Time window in seconds
///
/// # Returns
///
/// Message velocity and trend analysis
///
/// # Examples
///
/// ```
/// use celers_broker_redis::monitoring::{calculate_redis_message_velocity, QueueTrend};
///
/// let velocity = calculate_redis_message_velocity(1000, 1500, 60.0);
/// assert_eq!(velocity.previous_size, 1000);
/// assert_eq!(velocity.current_size, 1500);
/// assert_eq!(velocity.trend, QueueTrend::SlowGrowth);
/// ```
pub fn calculate_redis_message_velocity(
    previous_size: usize,
    current_size: usize,
    time_window_secs: f64,
) -> MessageVelocity {
    let velocity = if time_window_secs > 0.0 {
        (current_size as f64 - previous_size as f64) / time_window_secs
    } else {
        0.0
    };

    let trend = if velocity > 10.0 {
        QueueTrend::RapidGrowth
    } else if velocity > 1.0 {
        QueueTrend::SlowGrowth
    } else if velocity > -1.0 {
        QueueTrend::Stable
    } else if velocity > -10.0 {
        QueueTrend::SlowShrink
    } else {
        QueueTrend::RapidShrink
    };

    MessageVelocity {
        previous_size,
        current_size,
        time_window_secs,
        velocity,
        trend,
    }
}

/// Suggest worker scaling based on queue metrics
///
/// # Arguments
///
/// * `queue_size` - Current queue size
/// * `current_workers` - Current number of workers
/// * `avg_processing_rate` - Average processing rate per worker (msg/sec)
/// * `target_lag_seconds` - Target lag in seconds
///
/// # Returns
///
/// Worker scaling suggestion with recommended worker count
///
/// # Examples
///
/// ```
/// use celers_broker_redis::monitoring::suggest_redis_worker_scaling;
///
/// let scaling = suggest_redis_worker_scaling(2000, 5, 40.0, 100);
/// assert_eq!(scaling.current_workers, 5);
/// assert!(scaling.recommended_workers >= 1);
/// ```
pub fn suggest_redis_worker_scaling(
    queue_size: usize,
    current_workers: usize,
    avg_processing_rate: f64,
    target_lag_seconds: u64,
) -> WorkerScalingSuggestion {
    let current_total_rate = current_workers as f64 * avg_processing_rate;
    let target_rate = queue_size as f64 / target_lag_seconds as f64;

    let recommended_workers = if target_rate > current_total_rate {
        ((target_rate / avg_processing_rate).ceil() as usize).max(1)
    } else {
        ((target_rate / avg_processing_rate).floor() as usize).max(1)
    };

    let action = if recommended_workers > current_workers {
        ScalingRecommendation::ScaleUp {
            additional_workers: recommended_workers - current_workers,
        }
    } else if recommended_workers < current_workers {
        ScalingRecommendation::ScaleDown {
            workers_to_remove: current_workers - recommended_workers,
        }
    } else {
        ScalingRecommendation::Optimal
    };

    WorkerScalingSuggestion {
        queue_size,
        current_workers,
        avg_processing_rate,
        target_lag_seconds,
        recommended_workers,
        action,
    }
}

/// Calculate message age distribution for SLA monitoring
///
/// # Arguments
///
/// * `message_ages` - Slice of message ages in seconds
/// * `sla_threshold_secs` - SLA threshold in seconds
///
/// # Returns
///
/// Message age distribution with percentiles
///
/// # Examples
///
/// ```
/// use celers_broker_redis::monitoring::calculate_redis_message_age_distribution;
///
/// let ages = vec![10.0, 20.0, 30.0, 40.0, 50.0];
/// let dist = calculate_redis_message_age_distribution(&ages, 60.0);
/// assert_eq!(dist.total_messages, 5);
/// assert_eq!(dist.messages_exceeding_sla, 0);
/// ```
pub fn calculate_redis_message_age_distribution(
    message_ages: &[f64],
    sla_threshold_secs: f64,
) -> MessageAgeDistribution {
    if message_ages.is_empty() {
        return MessageAgeDistribution {
            total_messages: 0,
            min_age_secs: 0.0,
            max_age_secs: 0.0,
            avg_age_secs: 0.0,
            p50_age_secs: 0.0,
            p95_age_secs: 0.0,
            p99_age_secs: 0.0,
            messages_exceeding_sla: 0,
        };
    }

    let mut sorted_ages = message_ages.to_vec();
    sorted_ages.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let total_messages = sorted_ages.len();
    let min_age_secs = sorted_ages[0];
    let max_age_secs = sorted_ages[total_messages - 1];
    let avg_age_secs = sorted_ages.iter().sum::<f64>() / total_messages as f64;

    let p50_age_secs = percentile(&sorted_ages, 50.0);
    let p95_age_secs = percentile(&sorted_ages, 95.0);
    let p99_age_secs = percentile(&sorted_ages, 99.0);

    let messages_exceeding_sla = sorted_ages
        .iter()
        .filter(|&&age| age > sla_threshold_secs)
        .count();

    MessageAgeDistribution {
        total_messages,
        min_age_secs,
        max_age_secs,
        avg_age_secs,
        p50_age_secs,
        p95_age_secs,
        p99_age_secs,
        messages_exceeding_sla,
    }
}

/// Estimate processing capacity of the Redis broker system
///
/// # Arguments
///
/// * `workers` - Number of workers
/// * `rate_per_worker` - Processing rate per worker (msg/sec)
/// * `current_backlog` - Current queue backlog size
///
/// # Returns
///
/// Processing capacity estimation
///
/// # Examples
///
/// ```
/// use celers_broker_redis::monitoring::estimate_redis_processing_capacity;
///
/// let capacity = estimate_redis_processing_capacity(10, 50.0, 5000);
/// assert_eq!(capacity.workers, 10);
/// assert_eq!(capacity.total_capacity_per_sec, 500.0);
/// ```
pub fn estimate_redis_processing_capacity(
    workers: usize,
    rate_per_worker: f64,
    current_backlog: usize,
) -> ProcessingCapacity {
    let total_capacity_per_sec = workers as f64 * rate_per_worker;
    let total_capacity_per_min = total_capacity_per_sec * 60.0;
    let total_capacity_per_hour = total_capacity_per_min * 60.0;

    let time_to_clear_backlog_secs = if total_capacity_per_sec > 0.0 {
        current_backlog as f64 / total_capacity_per_sec
    } else {
        f64::INFINITY
    };

    ProcessingCapacity {
        workers,
        rate_per_worker,
        total_capacity_per_sec,
        total_capacity_per_min,
        total_capacity_per_hour,
        time_to_clear_backlog_secs,
    }
}

/// Calculate Redis queue health score (0.0 - 1.0)
///
/// Higher score = healthier queue
///
/// # Arguments
///
/// * `queue_size` - Current queue size
/// * `processing_rate` - Processing rate (msg/sec)
/// * `max_acceptable_size` - Maximum acceptable queue size
/// * `target_processing_rate` - Target processing rate
///
/// # Returns
///
/// Health score between 0.0 (unhealthy) and 1.0 (healthy)
///
/// # Examples
///
/// ```
/// use celers_broker_redis::monitoring::calculate_redis_queue_health_score;
///
/// let score = calculate_redis_queue_health_score(100, 50.0, 1000, 40.0);
/// assert!(score > 0.5);
/// assert!(score <= 1.0);
/// ```
pub fn calculate_redis_queue_health_score(
    queue_size: usize,
    processing_rate: f64,
    max_acceptable_size: usize,
    target_processing_rate: f64,
) -> f64 {
    // Size score: 1.0 when empty, 0.0 when at max
    let size_score = if max_acceptable_size > 0 {
        1.0 - (queue_size as f64 / max_acceptable_size as f64).min(1.0)
    } else {
        1.0
    };

    // Processing rate score: 1.0 when at or above target, 0.0 when zero
    let rate_score = if target_processing_rate > 0.0 {
        (processing_rate / target_processing_rate).min(1.0)
    } else {
        1.0
    };

    // Weighted average: 60% size, 40% rate
    (size_score * 0.6) + (rate_score * 0.4)
}

/// Analyze Redis broker performance metrics
///
/// # Arguments
///
/// * `metrics` - HashMap of metric name to value
///
/// # Returns
///
/// Performance analysis summary
pub fn analyze_redis_broker_performance(metrics: &HashMap<String, f64>) -> HashMap<String, String> {
    let mut analysis = HashMap::new();

    if let Some(&latency) = metrics.get("avg_latency_ms") {
        let status = if latency < 10.0 {
            "excellent"
        } else if latency < 50.0 {
            "good"
        } else if latency < 100.0 {
            "acceptable"
        } else {
            "poor"
        };
        analysis.insert("latency_status".to_string(), status.to_string());
    }

    if let Some(&throughput) = metrics.get("throughput_msg_per_sec") {
        let status = if throughput > 1000.0 {
            "high"
        } else if throughput > 100.0 {
            "medium"
        } else {
            "low"
        };
        analysis.insert("throughput_status".to_string(), status.to_string());
    }

    if let Some(&error_rate) = metrics.get("error_rate_percent") {
        let status = if error_rate < 1.0 {
            "healthy"
        } else if error_rate < 5.0 {
            "warning"
        } else {
            "critical"
        };
        analysis.insert("error_rate_status".to_string(), status.to_string());
    }

    analysis
}

/// Detect queue saturation and provide recommendations
///
/// # Arguments
///
/// * `current_size` - Current number of messages in queue
/// * `max_capacity` - Maximum recommended queue capacity
/// * `growth_rate_per_sec` - Optional growth rate (messages/sec, can be negative)
///
/// # Returns
///
/// Queue saturation analysis with recommendations
///
/// # Examples
///
/// ```
/// use celers_broker_redis::monitoring::detect_redis_queue_saturation;
///
/// let saturation = detect_redis_queue_saturation(8000, 10000, Some(50.0));
/// println!("Saturation: {:.1}%", saturation.saturation_percentage * 100.0);
/// println!("Level: {:?}", saturation.level);
/// if let Some(time) = saturation.time_until_full_secs {
///     println!("Time until full: {:.0} seconds", time);
/// }
/// ```
pub fn detect_redis_queue_saturation(
    current_size: usize,
    max_capacity: usize,
    growth_rate_per_sec: Option<f64>,
) -> QueueSaturationAnalysis {
    let saturation_percentage = current_size as f64 / max_capacity as f64;

    let level = if saturation_percentage < 0.5 {
        SaturationLevel::Healthy
    } else if saturation_percentage < 0.7 {
        SaturationLevel::Moderate
    } else if saturation_percentage < 0.9 {
        SaturationLevel::High
    } else {
        SaturationLevel::Critical
    };

    let time_until_full_secs = if let Some(rate) = growth_rate_per_sec {
        if rate > 0.0 {
            let remaining_capacity = max_capacity.saturating_sub(current_size) as f64;
            Some(remaining_capacity / rate)
        } else {
            None // Shrinking or stable
        }
    } else {
        None
    };

    let recommendation = match level {
        SaturationLevel::Healthy => {
            "Queue is healthy. No action needed.".to_string()
        }
        SaturationLevel::Moderate => {
            "Queue is moderately full. Monitor closely and consider scaling workers.".to_string()
        }
        SaturationLevel::High => {
            "Queue saturation is high. Scale up workers immediately or increase capacity.".to_string()
        }
        SaturationLevel::Critical => {
            "CRITICAL: Queue is near capacity! Immediate action required: scale workers, increase capacity, or enable backpressure.".to_string()
        }
    };

    QueueSaturationAnalysis {
        current_size,
        max_capacity,
        saturation_percentage,
        level,
        time_until_full_secs,
        recommendation,
    }
}

/// Queue saturation analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueSaturationAnalysis {
    /// Current queue size
    pub current_size: usize,
    /// Maximum recommended capacity
    pub max_capacity: usize,
    /// Saturation percentage (0.0-1.0+)
    pub saturation_percentage: f64,
    /// Saturation level
    pub level: SaturationLevel,
    /// Estimated time until full (seconds, if growing)
    pub time_until_full_secs: Option<f64>,
    /// Recommended action
    pub recommendation: String,
}

/// Anomaly detection result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnomalyDetection {
    /// Current metric value
    pub current_value: f64,
    /// Expected baseline value
    pub baseline: f64,
    /// Standard deviation from baseline
    pub std_deviation: f64,
    /// Whether an anomaly was detected
    pub is_anomaly: bool,
    /// Anomaly severity (0.0 = normal, higher = more severe)
    pub severity: f64,
    /// Description of the anomaly
    pub description: String,
}

/// Detect anomalies in queue metrics using statistical analysis
///
/// # Arguments
///
/// * `current_value` - Current metric value
/// * `historical_values` - Historical values for baseline calculation
/// * `sensitivity` - Anomaly detection sensitivity (1.0-5.0, higher = less sensitive)
///
/// # Returns
///
/// Anomaly detection result
///
/// # Examples
///
/// ```
/// use celers_broker_redis::monitoring::detect_redis_queue_anomaly;
///
/// let historical = vec![100.0, 105.0, 98.0, 102.0, 101.0, 99.0];
/// let anomaly = detect_redis_queue_anomaly(500.0, &historical, 2.0);
///
/// if anomaly.is_anomaly {
///     println!("Anomaly detected: {}", anomaly.description);
///     println!("Severity: {:.2}", anomaly.severity);
/// }
/// ```
pub fn detect_redis_queue_anomaly(
    current_value: f64,
    historical_values: &[f64],
    sensitivity: f64,
) -> AnomalyDetection {
    if historical_values.is_empty() {
        return AnomalyDetection {
            current_value,
            baseline: current_value,
            std_deviation: 0.0,
            is_anomaly: false,
            severity: 0.0,
            description: "Insufficient historical data for anomaly detection".to_string(),
        };
    }

    // Calculate baseline (mean)
    let baseline = historical_values.iter().sum::<f64>() / historical_values.len() as f64;

    // Calculate standard deviation
    let variance = historical_values
        .iter()
        .map(|&v| (v - baseline).powi(2))
        .sum::<f64>()
        / historical_values.len() as f64;
    let std_deviation = variance.sqrt();

    // Detect anomaly using z-score
    let z_score = if std_deviation > 0.0 {
        (current_value - baseline).abs() / std_deviation
    } else {
        0.0
    };

    let threshold = sensitivity.max(1.0);
    let is_anomaly = z_score > threshold;
    let severity = (z_score / threshold).min(10.0); // Cap at 10x severity

    let description = if !is_anomaly {
        "Metric is within normal range".to_string()
    } else if current_value > baseline {
        format!(
            "Spike detected: {:.1}% above baseline ({:.1} vs {:.1})",
            ((current_value - baseline) / baseline * 100.0),
            current_value,
            baseline
        )
    } else {
        format!(
            "Drop detected: {:.1}% below baseline ({:.1} vs {:.1})",
            ((baseline - current_value) / baseline * 100.0),
            current_value,
            baseline
        )
    };

    AnomalyDetection {
        current_value,
        baseline,
        std_deviation,
        is_anomaly,
        severity,
        description,
    }
}

// Helper function to calculate percentile
pub(crate) fn percentile(sorted_values: &[f64], p: f64) -> f64 {
    if sorted_values.is_empty() {
        return 0.0;
    }

    let index = (p / 100.0 * (sorted_values.len() - 1) as f64).round() as usize;
    sorted_values[index.min(sorted_values.len() - 1)]
}
