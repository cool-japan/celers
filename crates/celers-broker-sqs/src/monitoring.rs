//! AWS SQS broker monitoring utilities
//!
//! This module provides production-grade monitoring and analysis utilities
//! for AWS SQS-based task queues. These utilities help with capacity planning,
//! autoscaling decisions, SLA monitoring, and cost optimization.
//!
//! # Examples
//!
//! ```
//! use celers_broker_sqs::monitoring::*;
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Analyze consumer lag and get scaling recommendations
//! let queue_size = 1000;
//! let processing_rate = 50.0; // messages/sec
//! let lag = analyze_sqs_consumer_lag(queue_size, processing_rate, 100);
//! println!("Queue lag: {} seconds", lag.lag_seconds);
//! println!("Recommendation: {:?}", lag.recommendation);
//!
//! // Calculate message velocity and growth trends
//! let velocity = calculate_sqs_message_velocity(
//!     1000, // previous size
//!     1500, // current size
//!     60.0  // time window (seconds)
//! );
//! println!("Queue growing at {} msg/sec", velocity.velocity);
//!
//! // Get worker scaling recommendation
//! let scaling = suggest_sqs_worker_scaling(
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

/// Consumer lag analysis with autoscaling recommendations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerLagAnalysis {
    /// Current queue size (ApproximateNumberOfMessages)
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
    /// Time to drain queue at current capacity
    pub time_to_drain_secs: Option<f64>,
}

/// Queue health assessment
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueHealthAssessment {
    /// Queue name
    pub queue_name: String,
    /// Total messages (ApproximateNumberOfMessages)
    pub total_messages: usize,
    /// In-flight messages (ApproximateNumberOfMessagesNotVisible)
    pub in_flight_messages: usize,
    /// Delayed messages (ApproximateNumberOfMessagesDelayed)
    pub delayed_messages: usize,
    /// Message processing rate (msg/sec)
    pub processing_rate: f64,
    /// Age of oldest message (seconds)
    pub oldest_message_age_secs: Option<f64>,
    /// Health status
    pub health: QueueHealth,
    /// Issues detected
    pub issues: Vec<String>,
    /// Recommendations
    pub recommendations: Vec<String>,
}

/// Queue health classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueueHealth {
    /// Queue is healthy
    Healthy,
    /// Queue has minor issues
    Warning,
    /// Queue has critical issues
    Critical,
}

/// SQS-specific performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqsPerformanceMetrics {
    /// Publish rate (msg/sec)
    pub publish_rate: f64,
    /// Receive rate (msg/sec)
    pub receive_rate: f64,
    /// Delete (acknowledge) rate (msg/sec)
    pub delete_rate: f64,
    /// Average visibility timeout (seconds)
    pub avg_visibility_timeout_secs: u64,
    /// Average receive count (redelivery indicator)
    pub avg_receive_count: f64,
    /// Long polling efficiency (% of receives with messages)
    pub long_polling_efficiency: f64,
}

/// Analyze consumer lag for SQS queue
///
/// # Arguments
///
/// * `queue_size` - Current queue size (ApproximateNumberOfMessages)
/// * `processing_rate` - Messages processed per second
/// * `target_lag_seconds` - Target acceptable lag
///
/// # Returns
///
/// Consumer lag analysis with scaling recommendation
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::monitoring::analyze_sqs_consumer_lag;
///
/// let analysis = analyze_sqs_consumer_lag(1000, 50.0, 100);
/// assert!(analysis.queue_size == 1000);
/// ```
pub fn analyze_sqs_consumer_lag(
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
        // Calculate how many additional workers needed
        let target_rate = queue_size as f64 / target_lag_seconds as f64;
        let rate_deficit = target_rate - processing_rate;
        let additional_workers = (rate_deficit / (processing_rate / 1.0)).ceil() as usize;
        ScalingRecommendation::ScaleUp {
            additional_workers: additional_workers.max(1),
        }
    } else if lag_seconds < (target_lag_seconds as f64 * 0.5) && queue_size < 100 {
        // Can potentially scale down if lag is very low
        ScalingRecommendation::ScaleDown {
            workers_to_remove: 1,
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

/// Calculate message velocity for SQS queue
///
/// # Arguments
///
/// * `previous_size` - Queue size at previous measurement
/// * `current_size` - Current queue size
/// * `time_window_secs` - Time between measurements (seconds)
///
/// # Returns
///
/// Message velocity analysis
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::monitoring::calculate_sqs_message_velocity;
///
/// let velocity = calculate_sqs_message_velocity(1000, 1500, 60.0);
/// assert!(velocity.velocity > 0.0);
/// ```
pub fn calculate_sqs_message_velocity(
    previous_size: usize,
    current_size: usize,
    time_window_secs: f64,
) -> MessageVelocity {
    let velocity = if time_window_secs > 0.0 {
        (current_size as f64 - previous_size as f64) / time_window_secs
    } else {
        0.0
    };

    let trend = match velocity {
        v if v > 10.0 => QueueTrend::RapidGrowth,
        v if v > 1.0 => QueueTrend::SlowGrowth,
        v if v > -1.0 => QueueTrend::Stable,
        v if v > -10.0 => QueueTrend::SlowShrink,
        _ => QueueTrend::RapidShrink,
    };

    MessageVelocity {
        previous_size,
        current_size,
        time_window_secs,
        velocity,
        trend,
    }
}

/// Suggest worker scaling for SQS queue
///
/// # Arguments
///
/// * `queue_size` - Current queue size
/// * `current_workers` - Current number of workers
/// * `avg_processing_rate` - Average processing rate per worker (msg/sec)
/// * `target_lag_seconds` - Target lag threshold
///
/// # Returns
///
/// Worker scaling suggestion
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::monitoring::suggest_sqs_worker_scaling;
///
/// let suggestion = suggest_sqs_worker_scaling(2000, 5, 40.0, 100);
/// assert!(suggestion.current_workers == 5);
/// ```
pub fn suggest_sqs_worker_scaling(
    queue_size: usize,
    current_workers: usize,
    avg_processing_rate: f64,
    target_lag_seconds: u64,
) -> WorkerScalingSuggestion {
    let total_processing_rate = current_workers as f64 * avg_processing_rate;
    let required_rate = queue_size as f64 / target_lag_seconds as f64;

    let recommended_workers = if required_rate > total_processing_rate {
        ((required_rate / avg_processing_rate).ceil() as usize).max(1)
    } else {
        ((required_rate / avg_processing_rate).floor() as usize).max(1)
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

/// Calculate message age distribution from timestamps
///
/// # Arguments
///
/// * `message_ages_secs` - Vector of message ages in seconds
/// * `sla_threshold_secs` - SLA threshold in seconds
///
/// # Returns
///
/// Message age distribution analysis
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::monitoring::calculate_sqs_message_age_distribution;
///
/// let ages = vec![10.0, 20.0, 30.0, 40.0, 50.0];
/// let dist = calculate_sqs_message_age_distribution(ages, 45.0);
/// assert_eq!(dist.total_messages, 5);
/// ```
pub fn calculate_sqs_message_age_distribution(
    mut message_ages_secs: Vec<f64>,
    sla_threshold_secs: f64,
) -> MessageAgeDistribution {
    if message_ages_secs.is_empty() {
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

    message_ages_secs.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let total_messages = message_ages_secs.len();
    let min_age_secs = message_ages_secs[0];
    let max_age_secs = message_ages_secs[total_messages - 1];
    let avg_age_secs = message_ages_secs.iter().sum::<f64>() / total_messages as f64;

    let p50_age_secs = message_ages_secs[total_messages * 50 / 100];
    let p95_age_secs = message_ages_secs[total_messages * 95 / 100];
    let p99_age_secs = message_ages_secs[total_messages * 99 / 100];

    let messages_exceeding_sla = message_ages_secs
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

/// Estimate processing capacity
///
/// # Arguments
///
/// * `workers` - Number of workers
/// * `rate_per_worker` - Processing rate per worker (msg/sec)
/// * `current_queue_size` - Optional current queue size for drain time estimate
///
/// # Returns
///
/// Processing capacity estimation
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::monitoring::estimate_sqs_processing_capacity;
///
/// let capacity = estimate_sqs_processing_capacity(10, 50.0, Some(5000));
/// assert_eq!(capacity.total_capacity_per_sec, 500.0);
/// ```
pub fn estimate_sqs_processing_capacity(
    workers: usize,
    rate_per_worker: f64,
    current_queue_size: Option<usize>,
) -> ProcessingCapacity {
    let total_capacity_per_sec = workers as f64 * rate_per_worker;
    let total_capacity_per_min = total_capacity_per_sec * 60.0;
    let total_capacity_per_hour = total_capacity_per_sec * 3600.0;

    let time_to_drain_secs = current_queue_size.map(|size| {
        if total_capacity_per_sec > 0.0 {
            size as f64 / total_capacity_per_sec
        } else {
            f64::INFINITY
        }
    });

    ProcessingCapacity {
        workers,
        rate_per_worker,
        total_capacity_per_sec,
        total_capacity_per_min,
        total_capacity_per_hour,
        time_to_drain_secs,
    }
}

/// Assess SQS queue health
///
/// # Arguments
///
/// * `queue_name` - Queue name
/// * `total_messages` - Total messages in queue (ApproximateNumberOfMessages)
/// * `in_flight_messages` - Messages being processed (ApproximateNumberOfMessagesNotVisible)
/// * `delayed_messages` - Delayed messages (ApproximateNumberOfMessagesDelayed)
/// * `processing_rate` - Processing rate (msg/sec)
/// * `oldest_message_age_secs` - Age of oldest message in seconds
///
/// # Returns
///
/// Queue health assessment
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::monitoring::assess_sqs_queue_health;
///
/// let health = assess_sqs_queue_health("test_queue", 1000, 50, 0, 50.0, Some(120.0));
/// assert_eq!(health.queue_name, "test_queue");
/// ```
#[allow(clippy::too_many_arguments)]
pub fn assess_sqs_queue_health(
    queue_name: impl Into<String>,
    total_messages: usize,
    in_flight_messages: usize,
    delayed_messages: usize,
    processing_rate: f64,
    oldest_message_age_secs: Option<f64>,
) -> QueueHealthAssessment {
    let mut issues = Vec::new();
    let mut recommendations = Vec::new();
    let mut health = QueueHealth::Healthy;

    // Check for message backlog
    if total_messages > 10000 {
        issues.push(format!(
            "Large message backlog: {} messages",
            total_messages
        ));
        recommendations
            .push("Consider scaling up workers or enabling batch processing".to_string());
        if health == QueueHealth::Healthy {
            health = QueueHealth::Warning;
        }
    }

    // Check for low processing rate
    if processing_rate < 1.0 && total_messages > 100 {
        issues.push("Low processing rate detected".to_string());
        recommendations.push("Investigate worker performance or scale up workers".to_string());
        if health == QueueHealth::Healthy {
            health = QueueHealth::Warning;
        }
    }

    // Check for stuck messages (old messages)
    if let Some(age) = oldest_message_age_secs {
        if age > 3600.0 {
            // > 1 hour
            issues.push(format!("Oldest message is {:.0} minutes old", age / 60.0));
            recommendations
                .push("Check for processing errors or increase visibility timeout".to_string());
            health = QueueHealth::Critical;
        } else if age > 600.0 {
            // > 10 minutes
            issues.push(format!("Oldest message is {:.0} minutes old", age / 60.0));
            recommendations.push("Monitor message age trend".to_string());
            if health == QueueHealth::Healthy {
                health = QueueHealth::Warning;
            }
        }
    }

    // Check for high in-flight ratio
    if in_flight_messages > 0 {
        let in_flight_ratio =
            in_flight_messages as f64 / (total_messages + in_flight_messages) as f64;
        if in_flight_ratio > 0.8 {
            issues.push(format!(
                "High ratio of in-flight messages: {:.0}%",
                in_flight_ratio * 100.0
            ));
            recommendations
                .push("Check for long-running tasks or adjust visibility timeout".to_string());
            if health == QueueHealth::Healthy {
                health = QueueHealth::Warning;
            }
        }
    }

    // Check for excessive delayed messages
    if delayed_messages > 1000 {
        issues.push(format!(
            "High number of delayed messages: {}",
            delayed_messages
        ));
        recommendations.push("Review message delay configuration".to_string());
        if health == QueueHealth::Healthy {
            health = QueueHealth::Warning;
        }
    }

    QueueHealthAssessment {
        queue_name: queue_name.into(),
        total_messages,
        in_flight_messages,
        delayed_messages,
        processing_rate,
        oldest_message_age_secs,
        health,
        issues,
        recommendations,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_analyze_consumer_lag() {
        let analysis = analyze_sqs_consumer_lag(1000, 50.0, 100);
        assert_eq!(analysis.queue_size, 1000);
        assert_eq!(analysis.processing_rate, 50.0);
        assert_eq!(analysis.lag_seconds, 20.0);
        assert!(!analysis.is_lagging);
    }

    #[test]
    fn test_calculate_message_velocity() {
        let velocity = calculate_sqs_message_velocity(1000, 1700, 60.0);
        assert!(velocity.velocity > 10.0);
        assert_eq!(velocity.trend, QueueTrend::RapidGrowth);
    }

    #[test]
    fn test_suggest_worker_scaling() {
        let suggestion = suggest_sqs_worker_scaling(2000, 5, 40.0, 100);
        assert_eq!(suggestion.queue_size, 2000);
        assert_eq!(suggestion.current_workers, 5);
    }

    #[test]
    fn test_message_age_distribution() {
        let ages = vec![10.0, 20.0, 30.0, 40.0, 50.0];
        let dist = calculate_sqs_message_age_distribution(ages, 45.0);
        assert_eq!(dist.total_messages, 5);
        assert_eq!(dist.min_age_secs, 10.0);
        assert_eq!(dist.max_age_secs, 50.0);
        assert_eq!(dist.messages_exceeding_sla, 1);
    }

    #[test]
    fn test_estimate_processing_capacity() {
        let capacity = estimate_sqs_processing_capacity(10, 50.0, Some(5000));
        assert_eq!(capacity.total_capacity_per_sec, 500.0);
        assert_eq!(capacity.total_capacity_per_min, 30000.0);
        assert_eq!(capacity.time_to_drain_secs, Some(10.0));
    }

    #[test]
    fn test_assess_queue_health() {
        let health = assess_sqs_queue_health("test_queue", 100, 5, 0, 50.0, Some(60.0));
        assert_eq!(health.health, QueueHealth::Healthy);
        assert!(health.issues.is_empty());
    }

    #[test]
    fn test_assess_queue_health_old_messages() {
        let health = assess_sqs_queue_health("test_queue", 1000, 50, 0, 50.0, Some(4000.0));
        assert_eq!(health.health, QueueHealth::Critical);
        assert!(!health.issues.is_empty());
    }

    #[test]
    fn test_assess_queue_health_backlog() {
        let health = assess_sqs_queue_health("test_queue", 15000, 100, 0, 50.0, None);
        assert_eq!(health.health, QueueHealth::Warning);
        assert!(!health.issues.is_empty());
    }
}
