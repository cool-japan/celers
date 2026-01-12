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
    sorted_ages.sort_by(|a, b| a.partial_cmp(b).unwrap());

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

/// DLQ analysis result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DLQAnalysis {
    /// Total messages in DLQ
    pub total_messages: usize,
    /// DLQ size as percentage of main queue
    pub dlq_percentage: f64,
    /// Estimated error rate (DLQ growth / total processed)
    pub error_rate: f64,
    /// Whether DLQ is growing at an alarming rate
    pub is_alarming: bool,
    /// Recommended action
    pub recommendation: String,
}

/// Analyze Dead Letter Queue health and trends
///
/// # Arguments
///
/// * `dlq_size` - Current DLQ size
/// * `main_queue_size` - Main queue size for context
/// * `total_processed` - Total messages processed in time window
/// * `previous_dlq_size` - Previous DLQ size for trend analysis
///
/// # Returns
///
/// DLQ analysis with health assessment
///
/// # Examples
///
/// ```
/// use celers_broker_redis::monitoring::analyze_redis_dlq_health;
///
/// let analysis = analyze_redis_dlq_health(50, 1000, 10000, 30);
/// println!("DLQ error rate: {:.2}%", analysis.error_rate * 100.0);
/// println!("Recommendation: {}", analysis.recommendation);
/// ```
pub fn analyze_redis_dlq_health(
    dlq_size: usize,
    main_queue_size: usize,
    total_processed: usize,
    previous_dlq_size: usize,
) -> DLQAnalysis {
    let total_queue = (main_queue_size + dlq_size).max(1);
    let dlq_percentage = dlq_size as f64 / total_queue as f64;

    // Calculate error rate based on DLQ growth
    let dlq_growth = dlq_size.saturating_sub(previous_dlq_size);
    let error_rate = if total_processed > 0 {
        dlq_growth as f64 / total_processed as f64
    } else {
        0.0
    };

    // Determine if alarming (> 5% error rate or > 20% of queue)
    let is_alarming = error_rate > 0.05 || dlq_percentage > 0.2;

    let recommendation = if dlq_size == 0 {
        "DLQ is empty. System is healthy.".to_string()
    } else if is_alarming {
        format!(
            "ALERT: High error rate ({:.1}%). Investigate failing tasks, check task handlers, and review DLQ messages.",
            error_rate * 100.0
        )
    } else if dlq_percentage > 0.1 {
        "DLQ is growing. Review failed tasks and consider replaying or purging old entries."
            .to_string()
    } else {
        "DLQ is within acceptable limits. Monitor for trends.".to_string()
    };

    DLQAnalysis {
        total_messages: dlq_size,
        dlq_percentage,
        error_rate,
        is_alarming,
        recommendation,
    }
}

/// Queue size prediction based on historical growth
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueSizePrediction {
    /// Current queue size
    pub current_size: usize,
    /// Predicted size in the future
    pub predicted_size: usize,
    /// Time horizon for prediction (seconds)
    pub prediction_horizon_secs: u64,
    /// Growth rate (messages per second)
    pub growth_rate: f64,
    /// Confidence level (0.0-1.0)
    pub confidence: f64,
    /// Whether predicted size exceeds threshold
    pub will_exceed_threshold: bool,
    /// Recommendation
    pub recommendation: String,
}

/// Predict future queue size based on current growth rate
///
/// Uses linear extrapolation to predict queue size at a future time.
/// Includes confidence scoring based on growth stability.
///
/// # Arguments
///
/// * `current_size` - Current queue size
/// * `growth_rate` - Current growth rate (messages/second, can be negative)
/// * `prediction_horizon_secs` - How far into the future to predict
/// * `max_threshold` - Maximum acceptable queue size
///
/// # Returns
///
/// Queue size prediction with recommendations
///
/// # Examples
///
/// ```
/// use celers_broker_redis::monitoring::predict_redis_queue_size;
///
/// let prediction = predict_redis_queue_size(1000, 5.0, 300, 5000);
/// println!("Predicted size in 5min: {}", prediction.predicted_size);
/// if prediction.will_exceed_threshold {
///     println!("Warning: {}", prediction.recommendation);
/// }
/// ```
pub fn predict_redis_queue_size(
    current_size: usize,
    growth_rate: f64,
    prediction_horizon_secs: u64,
    max_threshold: usize,
) -> QueueSizePrediction {
    // Linear prediction: future_size = current + (rate * time)
    let growth_amount = growth_rate * prediction_horizon_secs as f64;
    let predicted_size = ((current_size as f64 + growth_amount).max(0.0) as usize).min(usize::MAX);

    // Confidence decreases with longer prediction horizons and higher growth rates
    let time_factor = 1.0 / (1.0 + (prediction_horizon_secs as f64 / 3600.0));
    let volatility_factor = 1.0 / (1.0 + growth_rate.abs() / 100.0);
    let confidence = (time_factor * volatility_factor).clamp(0.0, 1.0);

    let will_exceed_threshold = predicted_size > max_threshold;

    let recommendation = if will_exceed_threshold {
        let time_to_threshold = if growth_rate > 0.0 {
            (max_threshold.saturating_sub(current_size) as f64 / growth_rate) as u64
        } else {
            u64::MAX
        };
        format!(
            "WARNING: Queue will exceed threshold ({}) in {} seconds. Consider scaling up workers or increasing processing capacity.",
            max_threshold, time_to_threshold
        )
    } else if predicted_size < current_size && current_size > max_threshold / 2 {
        "Queue is draining. Consider scaling down workers to save costs.".to_string()
    } else {
        "Queue size is projected to remain within acceptable limits.".to_string()
    };

    QueueSizePrediction {
        current_size,
        predicted_size,
        prediction_horizon_secs,
        growth_rate,
        confidence,
        will_exceed_threshold,
        recommendation,
    }
}

/// Redis memory efficiency analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryEfficiencyAnalysis {
    /// Total memory used (bytes)
    pub total_memory_bytes: usize,
    /// Memory used by queue data (bytes)
    pub queue_memory_bytes: usize,
    /// Overhead percentage
    pub overhead_percentage: f64,
    /// Efficiency score (0.0-1.0, higher is better)
    pub efficiency_score: f64,
    /// Fragmentation ratio (1.0 = no fragmentation, >1.0 = fragmented)
    pub fragmentation_ratio: f64,
    /// Whether optimization is recommended
    pub needs_optimization: bool,
    /// Recommendations
    pub recommendations: Vec<String>,
}

/// Analyze Redis memory efficiency and fragmentation
///
/// Analyzes memory usage patterns and provides optimization recommendations.
///
/// # Arguments
///
/// * `total_memory_bytes` - Total Redis memory usage
/// * `queue_memory_bytes` - Memory used by queue data
/// * `allocated_memory_bytes` - Memory allocated by OS to Redis
///
/// # Returns
///
/// Memory efficiency analysis with recommendations
///
/// # Examples
///
/// ```
/// use celers_broker_redis::monitoring::analyze_redis_memory_efficiency;
///
/// let analysis = analyze_redis_memory_efficiency(
///     10_000_000,  // 10MB used
///     8_000_000,   // 8MB queue data
///     12_000_000   // 12MB allocated
/// );
/// println!("Efficiency score: {:.2}", analysis.efficiency_score);
/// for rec in &analysis.recommendations {
///     println!("- {}", rec);
/// }
/// ```
pub fn analyze_redis_memory_efficiency(
    total_memory_bytes: usize,
    queue_memory_bytes: usize,
    allocated_memory_bytes: usize,
) -> MemoryEfficiencyAnalysis {
    // Calculate overhead (metadata, Redis structures, etc.)
    let overhead_bytes = total_memory_bytes.saturating_sub(queue_memory_bytes);
    let overhead_percentage = if total_memory_bytes > 0 {
        overhead_bytes as f64 / total_memory_bytes as f64 * 100.0
    } else {
        0.0
    };

    // Efficiency score: how much of total memory is actual data vs overhead
    let efficiency_score = if total_memory_bytes > 0 {
        (queue_memory_bytes as f64 / total_memory_bytes as f64).min(1.0)
    } else {
        1.0
    };

    // Fragmentation ratio: allocated vs used
    let fragmentation_ratio = if total_memory_bytes > 0 {
        allocated_memory_bytes as f64 / total_memory_bytes as f64
    } else {
        1.0
    };

    let mut recommendations = Vec::new();
    let mut needs_optimization = false;

    // High overhead (>30%)
    if overhead_percentage > 30.0 {
        needs_optimization = true;
        recommendations.push(
            "High memory overhead detected. Consider using compression for large messages."
                .to_string(),
        );
        recommendations
            .push("Review data structures - prefer simpler structures where possible.".to_string());
    }

    // High fragmentation (>1.5x)
    if fragmentation_ratio > 1.5 {
        needs_optimization = true;
        recommendations.push(format!(
            "Memory fragmentation detected (ratio: {:.2}). Consider MEMORY PURGE or restart Redis during maintenance window.",
            fragmentation_ratio
        ));
        recommendations
            .push("Enable activedefrag in Redis 4.0+ for automatic defragmentation.".to_string());
    }

    // Low efficiency (<0.6)
    if efficiency_score < 0.6 {
        needs_optimization = true;
        recommendations.push(
            "Low memory efficiency. Review queue design and consider batch operations.".to_string(),
        );
    }

    if recommendations.is_empty() {
        recommendations.push("Memory usage is efficient. No optimization needed.".to_string());
    }

    MemoryEfficiencyAnalysis {
        total_memory_bytes,
        queue_memory_bytes,
        overhead_percentage,
        efficiency_score,
        fragmentation_ratio,
        needs_optimization,
        recommendations,
    }
}

/// Comprehensive scaling strategy recommendation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalingStrategy {
    /// Recommended action
    pub action: String,
    /// Horizontal scaling recommendation (worker count)
    pub recommended_workers: Option<usize>,
    /// Vertical scaling recommendation (instance size)
    pub recommended_instance_type: String,
    /// Cost-efficiency score (0.0-1.0, higher is better)
    pub cost_efficiency: f64,
    /// Performance score (0.0-1.0, higher is better)
    pub performance_score: f64,
    /// Priority (1=low, 2=medium, 3=high)
    pub priority: u8,
    /// Detailed recommendations
    pub recommendations: Vec<String>,
}

/// Recommend comprehensive Redis scaling strategy
///
/// Provides both horizontal (more workers) and vertical (larger instance)
/// scaling recommendations based on queue metrics and resource usage.
///
/// # Arguments
///
/// * `queue_size` - Current queue size
/// * `processing_rate` - Messages per second being processed
/// * `current_workers` - Current worker count
/// * `memory_usage_percent` - Redis memory usage percentage (0.0-100.0)
/// * `cpu_usage_percent` - CPU usage percentage (0.0-100.0)
///
/// # Returns
///
/// Comprehensive scaling strategy
///
/// # Examples
///
/// ```
/// use celers_broker_redis::monitoring::recommend_redis_scaling_strategy;
///
/// let strategy = recommend_redis_scaling_strategy(
///     5000,    // queue size
///     50.0,    // processing rate
///     5,       // current workers
///     85.0,    // memory usage %
///     60.0     // CPU usage %
/// );
/// println!("Action: {}", strategy.action);
/// println!("Priority: {}", strategy.priority);
/// ```
#[allow(clippy::too_many_arguments)]
pub fn recommend_redis_scaling_strategy(
    queue_size: usize,
    processing_rate: f64,
    current_workers: usize,
    memory_usage_percent: f64,
    cpu_usage_percent: f64,
) -> ScalingStrategy {
    let mut recommendations = Vec::new();

    // Determine bottleneck
    let memory_constrained = memory_usage_percent > 80.0;
    let cpu_constrained = cpu_usage_percent > 75.0;
    let queue_backed_up = queue_size > 10000;

    // Calculate performance score
    let queue_score = if queue_size < 1000 {
        1.0
    } else if queue_size < 10000 {
        0.7
    } else {
        0.3
    };
    let rate_score = if processing_rate > 100.0 {
        1.0
    } else if processing_rate > 50.0 {
        0.7
    } else {
        0.4
    };
    let performance_score = (queue_score + rate_score) / 2.0;

    let (action, recommended_workers, recommended_instance_type, priority) = if memory_constrained {
        recommendations.push(
            "CRITICAL: Memory usage is high. Vertical scaling (larger instance) is recommended."
                .to_string(),
        );
        recommendations
            .push("Consider enabling eviction policies or increasing maxmemory limit.".to_string());
        recommendations
            .push("Review data compression settings to reduce memory footprint.".to_string());
        (
            "Scale vertically (upgrade instance)".to_string(),
            Some(current_workers),
            "larger-instance".to_string(),
            3u8,
        )
    } else if cpu_constrained && queue_backed_up {
        recommendations.push(
            "CRITICAL: Both CPU and queue are under pressure. Horizontal scaling needed."
                .to_string(),
        );
        let optimal_workers =
            ((queue_size as f64 / 1000.0).ceil() as usize).max(current_workers + 2);
        recommendations.push(format!(
            "Scale from {} to {} workers immediately.",
            current_workers, optimal_workers
        ));
        (
            "Scale horizontally (add workers)".to_string(),
            Some(optimal_workers),
            "current-instance".to_string(),
            3u8,
        )
    } else if queue_backed_up {
        let optimal_workers = current_workers + ((queue_size / 2000).max(1));
        recommendations.push(format!(
            "Queue is backed up. Add {} workers to increase throughput.",
            optimal_workers - current_workers
        ));
        (
            "Scale horizontally (add workers)".to_string(),
            Some(optimal_workers),
            "current-instance".to_string(),
            2u8,
        )
    } else if cpu_constrained {
        recommendations.push(
            "CPU usage is high but queue is manageable. Monitor and consider scaling if CPU stays high.".to_string()
        );
        (
            "Monitor and prepare to scale".to_string(),
            Some(current_workers + 1),
            "current-instance".to_string(),
            2u8,
        )
    } else {
        recommendations
            .push("System is running optimally. No immediate scaling needed.".to_string());
        (
            "No scaling needed".to_string(),
            Some(current_workers),
            "current-instance".to_string(),
            1u8,
        )
    };

    // Cost efficiency: lower is better if we don't need to scale
    let cost_efficiency = if priority == 1 {
        1.0
    } else if priority == 2 {
        0.6
    } else {
        0.3
    };

    ScalingStrategy {
        action,
        recommended_workers,
        recommended_instance_type,
        cost_efficiency,
        performance_score,
        priority,
        recommendations,
    }
}

/// Cost estimation for Redis resources
///
/// Estimates the monthly cost of running a Redis instance based on
/// memory usage, operation throughput, and cloud provider pricing.
///
/// # Arguments
///
/// * `memory_mb` - Total memory usage in megabytes
/// * `operations_per_second` - Average operations per second
/// * `provider` - Cloud provider ("aws", "gcp", "azure")
///
/// # Returns
///
/// `RedisCostEstimate` with monthly cost breakdown
///
/// # Examples
///
/// ```
/// use celers_broker_redis::monitoring::estimate_redis_monthly_cost;
///
/// let cost = estimate_redis_monthly_cost(1024, 1000.0, "aws");
/// assert!(cost.total_monthly_cost > 0.0);
/// ```
#[allow(dead_code)]
pub fn estimate_redis_monthly_cost(
    memory_mb: usize,
    operations_per_second: f64,
    provider: &str,
) -> RedisCostEstimate {
    // Approximate pricing (as of 2024, varies by region)
    let (memory_cost_per_gb, operations_cost_per_million) = match provider.to_lowercase().as_str() {
        "aws" => (0.025, 0.05),    // ElastiCache approximate
        "gcp" => (0.020, 0.04),    // Memorystore approximate
        "azure" => (0.023, 0.045), // Azure Cache approximate
        _ => (0.025, 0.05),        // Default to AWS pricing
    };

    let memory_gb = memory_mb as f64 / 1024.0;
    let operations_per_month = operations_per_second * 60.0 * 60.0 * 24.0 * 30.0;
    let operations_millions = operations_per_month / 1_000_000.0;

    let memory_cost = memory_gb * memory_cost_per_gb * 730.0; // 730 hours per month
    let operations_cost = operations_millions * operations_cost_per_million;
    let total_cost = memory_cost + operations_cost;

    // Cost optimization suggestions
    let optimization_potential = if memory_mb > 4096 && operations_per_second < 100.0 {
        "High - Consider downsizing instance or using serverless Redis"
    } else if memory_mb < 512 && operations_per_second > 10000.0 {
        "Medium - Consider upgrading instance for better performance"
    } else {
        "Low - Current configuration appears optimal"
    };

    RedisCostEstimate {
        memory_mb,
        memory_gb,
        operations_per_second,
        memory_cost_monthly: memory_cost,
        operations_cost_monthly: operations_cost,
        total_monthly_cost: total_cost,
        provider: provider.to_string(),
        optimization_potential: optimization_potential.to_string(),
    }
}

/// Redis cost estimate breakdown
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisCostEstimate {
    /// Memory usage in MB
    pub memory_mb: usize,
    /// Memory usage in GB
    pub memory_gb: f64,
    /// Average operations per second
    pub operations_per_second: f64,
    /// Monthly cost for memory (USD)
    pub memory_cost_monthly: f64,
    /// Monthly cost for operations (USD)
    pub operations_cost_monthly: f64,
    /// Total monthly cost (USD)
    pub total_monthly_cost: f64,
    /// Cloud provider
    pub provider: String,
    /// Cost optimization potential
    pub optimization_potential: String,
}

/// Recommend monitoring alert thresholds
///
/// Generates recommended alert thresholds based on queue characteristics
/// and SLA requirements for production monitoring systems.
///
/// # Arguments
///
/// * `avg_queue_size` - Average queue size under normal load
/// * `peak_queue_size` - Peak queue size observed
/// * `target_sla_seconds` - Target SLA in seconds
///
/// # Returns
///
/// `AlertThresholds` with recommended warning and critical levels
///
/// # Examples
///
/// ```
/// use celers_broker_redis::monitoring::recommend_alert_thresholds;
///
/// let thresholds = recommend_alert_thresholds(100, 500, 60);
/// assert!(thresholds.queue_size_warning < thresholds.queue_size_critical);
/// ```
#[allow(dead_code)]
pub fn recommend_alert_thresholds(
    avg_queue_size: usize,
    peak_queue_size: usize,
    target_sla_seconds: u64,
) -> AlertThresholds {
    // Queue size thresholds (warning at 70%, critical at 90% of peak)
    let queue_size_warning = (peak_queue_size as f64 * 0.7) as usize;
    let queue_size_critical = (peak_queue_size as f64 * 0.9) as usize;

    // Processing lag thresholds
    let lag_warning_seconds = (target_sla_seconds as f64 * 0.8) as u64;
    let lag_critical_seconds = target_sla_seconds;

    // DLQ thresholds (warning at 1%, critical at 5% of average queue size)
    let dlq_warning = (avg_queue_size as f64 * 0.01).max(10.0) as usize;
    let dlq_critical = (avg_queue_size as f64 * 0.05).max(50.0) as usize;

    // Error rate thresholds
    let error_rate_warning = 0.01; // 1%
    let error_rate_critical = 0.05; // 5%

    // Memory usage thresholds
    let memory_usage_warning = 0.75; // 75%
    let memory_usage_critical = 0.90; // 90%

    AlertThresholds {
        queue_size_warning,
        queue_size_critical,
        lag_warning_seconds,
        lag_critical_seconds,
        dlq_size_warning: dlq_warning,
        dlq_size_critical: dlq_critical,
        error_rate_warning,
        error_rate_critical,
        memory_usage_warning,
        memory_usage_critical,
    }
}

/// Monitoring alert thresholds
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertThresholds {
    /// Warning threshold for queue size
    pub queue_size_warning: usize,
    /// Critical threshold for queue size
    pub queue_size_critical: usize,
    /// Warning threshold for processing lag (seconds)
    pub lag_warning_seconds: u64,
    /// Critical threshold for processing lag (seconds)
    pub lag_critical_seconds: u64,
    /// Warning threshold for DLQ size
    pub dlq_size_warning: usize,
    /// Critical threshold for DLQ size
    pub dlq_size_critical: usize,
    /// Warning threshold for error rate (0.0-1.0)
    pub error_rate_warning: f64,
    /// Critical threshold for error rate (0.0-1.0)
    pub error_rate_critical: f64,
    /// Warning threshold for memory usage (0.0-1.0)
    pub memory_usage_warning: f64,
    /// Critical threshold for memory usage (0.0-1.0)
    pub memory_usage_critical: f64,
}

/// Generate comprehensive queue health report
///
/// Combines multiple monitoring metrics into a single comprehensive
/// health report for operational dashboards and alerting.
///
/// # Arguments
///
/// * `queue_size` - Current main queue size
/// * `processing_queue_size` - Current processing queue size
/// * `dlq_size` - Current DLQ size
/// * `processing_rate` - Messages processed per second
/// * `error_count` - Total errors in time window
/// * `total_processed` - Total messages processed in time window
/// * `memory_used_mb` - Current memory usage in MB
/// * `memory_max_mb` - Maximum available memory in MB
///
/// # Returns
///
/// `QueueHealthReport` with overall health status and recommendations
///
/// # Examples
///
/// ```
/// use celers_broker_redis::monitoring::generate_queue_health_report;
///
/// let report = generate_queue_health_report(
///     100, 5, 2, 50.0, 1, 1000, 256, 1024
/// );
/// println!("Health status: {:?}", report.overall_status);
/// ```
#[allow(dead_code)]
#[allow(clippy::too_many_arguments)]
pub fn generate_queue_health_report(
    queue_size: usize,
    processing_queue_size: usize,
    dlq_size: usize,
    processing_rate: f64,
    error_count: usize,
    total_processed: usize,
    memory_used_mb: usize,
    memory_max_mb: usize,
) -> QueueHealthReport {
    let total_tasks = queue_size + processing_queue_size + dlq_size;

    // Calculate metrics
    let error_rate = if total_processed > 0 {
        error_count as f64 / total_processed as f64
    } else {
        0.0
    };

    let memory_usage = if memory_max_mb > 0 {
        memory_used_mb as f64 / memory_max_mb as f64
    } else {
        0.0
    };

    let dlq_ratio = if total_processed > 0 {
        dlq_size as f64 / total_processed as f64
    } else {
        0.0
    };

    // Determine health status
    let queue_health = if queue_size > 10000 {
        "critical"
    } else if queue_size > 1000 {
        "warning"
    } else {
        "healthy"
    };

    let dlq_health = if dlq_ratio > 0.05 || dlq_size > 100 {
        "critical"
    } else if dlq_ratio > 0.01 || dlq_size > 10 {
        "warning"
    } else {
        "healthy"
    };

    let error_health = if error_rate > 0.05 {
        "critical"
    } else if error_rate > 0.01 {
        "warning"
    } else {
        "healthy"
    };

    let memory_health = if memory_usage > 0.90 {
        "critical"
    } else if memory_usage > 0.75 {
        "warning"
    } else {
        "healthy"
    };

    // Overall status (worst of all metrics)
    let overall_status = if queue_health == "critical"
        || dlq_health == "critical"
        || error_health == "critical"
        || memory_health == "critical"
    {
        "critical"
    } else if queue_health == "warning"
        || dlq_health == "warning"
        || error_health == "warning"
        || memory_health == "warning"
    {
        "warning"
    } else {
        "healthy"
    };

    // Generate recommendations
    let mut recommendations = Vec::new();
    if queue_size > 1000 {
        recommendations.push("Consider adding more workers to reduce queue backlog".to_string());
    }
    if dlq_size > 10 {
        recommendations.push("Investigate and replay tasks from DLQ".to_string());
    }
    if error_rate > 0.01 {
        recommendations.push("High error rate detected - review task failures".to_string());
    }
    if memory_usage > 0.75 {
        recommendations.push("Memory usage high - consider scaling up instance".to_string());
    }
    if processing_rate < 1.0 && queue_size > 0 {
        recommendations.push("Low processing rate - check worker health".to_string());
    }

    QueueHealthReport {
        overall_status: overall_status.to_string(),
        queue_size,
        processing_queue_size,
        dlq_size,
        total_tasks,
        processing_rate,
        error_rate,
        memory_usage,
        queue_health: queue_health.to_string(),
        dlq_health: dlq_health.to_string(),
        error_health: error_health.to_string(),
        memory_health: memory_health.to_string(),
        recommendations,
    }
}

/// Comprehensive queue health report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueHealthReport {
    /// Overall health status (healthy/warning/critical)
    pub overall_status: String,
    /// Main queue size
    pub queue_size: usize,
    /// Processing queue size
    pub processing_queue_size: usize,
    /// DLQ size
    pub dlq_size: usize,
    /// Total tasks across all queues
    pub total_tasks: usize,
    /// Processing rate (messages/sec)
    pub processing_rate: f64,
    /// Error rate (0.0-1.0)
    pub error_rate: f64,
    /// Memory usage (0.0-1.0)
    pub memory_usage: f64,
    /// Queue health status
    pub queue_health: String,
    /// DLQ health status
    pub dlq_health: String,
    /// Error health status
    pub error_health: String,
    /// Memory health status
    pub memory_health: String,
    /// Actionable recommendations
    pub recommendations: Vec<String>,
}

/// Analyze performance trends over time
///
/// Analyzes performance metrics collected over time to identify trends,
/// anomalies, and predict future performance issues.
///
/// # Arguments
///
/// * `historical_metrics` - Vec of (timestamp_seconds, queue_size, processing_rate) tuples
///
/// # Returns
///
/// `PerformanceTrend` with trend analysis and predictions
///
/// # Examples
///
/// ```
/// use celers_broker_redis::monitoring::analyze_performance_trend;
///
/// let metrics = vec![
///     (0.0, 100, 50.0),
///     (60.0, 150, 45.0),
///     (120.0, 200, 40.0),
/// ];
/// let trend = analyze_performance_trend(&metrics);
/// println!("Queue trend: {}", trend.queue_size_trend);
/// ```
#[allow(dead_code)]
pub fn analyze_performance_trend(historical_metrics: &[(f64, usize, f64)]) -> PerformanceTrend {
    if historical_metrics.is_empty() {
        return PerformanceTrend {
            data_points: 0,
            time_span_seconds: 0.0,
            queue_size_trend: "insufficient_data".to_string(),
            processing_rate_trend: "insufficient_data".to_string(),
            predicted_queue_size_1h: 0,
            predicted_processing_rate_1h: 0.0,
            anomalies_detected: 0,
            recommendations: vec!["Collect more historical data for trend analysis".to_string()],
        };
    }

    let data_points = historical_metrics.len();
    let first_timestamp = historical_metrics[0].0;
    let last_timestamp = historical_metrics[data_points - 1].0;
    let time_span = last_timestamp - first_timestamp;

    // Calculate queue size trend
    let first_queue_size = historical_metrics[0].1 as f64;
    let last_queue_size = historical_metrics[data_points - 1].1 as f64;
    let queue_change_rate = if time_span > 0.0 {
        (last_queue_size - first_queue_size) / time_span
    } else {
        0.0
    };

    let queue_size_trend = if queue_change_rate > 1.0 {
        "increasing"
    } else if queue_change_rate < -1.0 {
        "decreasing"
    } else {
        "stable"
    };

    // Calculate processing rate trend
    let first_rate = historical_metrics[0].2;
    let last_rate = historical_metrics[data_points - 1].2;
    let rate_change = last_rate - first_rate;

    let processing_rate_trend = if rate_change > 5.0 {
        "improving"
    } else if rate_change < -5.0 {
        "degrading"
    } else {
        "stable"
    };

    // Simple linear extrapolation for 1 hour prediction
    let predicted_queue_size_1h = (last_queue_size + queue_change_rate * 3600.0).max(0.0) as usize;
    let predicted_processing_rate_1h = (last_rate + (rate_change / time_span) * 3600.0).max(0.0);

    // Detect anomalies (simple threshold-based)
    let mut anomalies = 0;
    let avg_queue_size: f64 = historical_metrics
        .iter()
        .map(|(_, q, _)| *q as f64)
        .sum::<f64>()
        / data_points as f64;
    for (_, queue_size, _) in historical_metrics {
        if (*queue_size as f64 - avg_queue_size).abs() > avg_queue_size * 0.5 {
            anomalies += 1;
        }
    }

    // Generate recommendations
    let mut recommendations = Vec::new();
    if queue_size_trend == "increasing" && processing_rate_trend == "degrading" {
        recommendations.push(
            "Critical: Queue growing and processing slowing - scale up immediately".to_string(),
        );
    } else if queue_size_trend == "increasing" {
        recommendations.push("Queue growing - consider adding workers".to_string());
    }
    if processing_rate_trend == "degrading" {
        recommendations
            .push("Processing rate declining - investigate worker performance".to_string());
    }
    if anomalies > data_points / 4 {
        recommendations
            .push("High variability detected - review for inconsistent workloads".to_string());
    }
    if predicted_queue_size_1h > last_queue_size as usize * 2 {
        recommendations
            .push("Queue predicted to double in 1 hour - prepare for scaling".to_string());
    }

    PerformanceTrend {
        data_points,
        time_span_seconds: time_span,
        queue_size_trend: queue_size_trend.to_string(),
        processing_rate_trend: processing_rate_trend.to_string(),
        predicted_queue_size_1h,
        predicted_processing_rate_1h,
        anomalies_detected: anomalies,
        recommendations,
    }
}

/// Performance trend analysis over time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceTrend {
    /// Number of data points analyzed
    pub data_points: usize,
    /// Time span of analysis (seconds)
    pub time_span_seconds: f64,
    /// Queue size trend (increasing/decreasing/stable)
    pub queue_size_trend: String,
    /// Processing rate trend (improving/degrading/stable)
    pub processing_rate_trend: String,
    /// Predicted queue size in 1 hour
    pub predicted_queue_size_1h: usize,
    /// Predicted processing rate in 1 hour
    pub predicted_processing_rate_1h: f64,
    /// Number of anomalies detected
    pub anomalies_detected: usize,
    /// Actionable recommendations
    pub recommendations: Vec<String>,
}

// Helper function to calculate percentile
fn percentile(sorted_values: &[f64], p: f64) -> f64 {
    if sorted_values.is_empty() {
        return 0.0;
    }

    let index = (p / 100.0 * (sorted_values.len() - 1) as f64).round() as usize;
    sorted_values[index.min(sorted_values.len() - 1)]
}

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
/// println!("Avg duration: {} μs", analysis.avg_duration_micros);
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

    let max_duration_micros = *durations.last().unwrap_or(&0);
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
        (timestamps.iter().max().unwrap() - timestamps.iter().min().unwrap()).max(1)
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
    freq_vec.sort_by(|a, b| b.1.cmp(a.1));
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

    let current_size = *recent_queue_sizes.last().unwrap();
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
    use super::*;

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
        assert!(strategy.recommended_workers.unwrap() > 5);
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
