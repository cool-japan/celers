//! MySQL broker monitoring utilities
//!
//! This module provides production-grade monitoring and analysis utilities
//! for MySQL-based task queues. These utilities help with capacity planning,
//! autoscaling decisions, SLA monitoring, and performance optimization.
//!
//! # Examples
//!
//! ```
//! use celers_broker_sql::monitoring::*;
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Analyze consumer lag and get scaling recommendations
//! let queue_size = 1000;
//! let processing_rate = 50.0; // messages/sec
//! let lag = analyze_mysql_consumer_lag(queue_size, processing_rate, 100);
//! println!("Queue lag: {} seconds", lag.lag_seconds);
//! println!("Recommendation: {:?}", lag.recommendation);
//!
//! // Calculate message velocity and growth trends
//! let velocity = calculate_mysql_message_velocity(
//!     1000, // previous size
//!     1500, // current size
//!     60.0  // time window (seconds)
//! );
//! println!("Queue growing at {} msg/sec", velocity.velocity);
//!
//! // Get worker scaling recommendation
//! let scaling = suggest_mysql_worker_scaling(
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
/// use celers_broker_sql::monitoring::analyze_mysql_consumer_lag;
///
/// let lag = analyze_mysql_consumer_lag(1000, 50.0, 100);
/// assert_eq!(lag.queue_size, 1000);
/// assert_eq!(lag.processing_rate, 50.0);
/// ```
pub fn analyze_mysql_consumer_lag(
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
/// use celers_broker_sql::monitoring::{calculate_mysql_message_velocity, QueueTrend};
///
/// let velocity = calculate_mysql_message_velocity(1000, 1500, 60.0);
/// assert_eq!(velocity.previous_size, 1000);
/// assert_eq!(velocity.current_size, 1500);
/// assert_eq!(velocity.trend, QueueTrend::SlowGrowth);
/// ```
pub fn calculate_mysql_message_velocity(
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
/// use celers_broker_sql::monitoring::suggest_mysql_worker_scaling;
///
/// let scaling = suggest_mysql_worker_scaling(2000, 5, 40.0, 100);
/// assert_eq!(scaling.current_workers, 5);
/// assert!(scaling.recommended_workers >= 1);
/// ```
pub fn suggest_mysql_worker_scaling(
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
/// use celers_broker_sql::monitoring::calculate_mysql_message_age_distribution;
///
/// let ages = vec![10.0, 20.0, 30.0, 40.0, 50.0];
/// let dist = calculate_mysql_message_age_distribution(&ages, 60.0);
/// assert_eq!(dist.total_messages, 5);
/// assert_eq!(dist.messages_exceeding_sla, 0);
/// ```
pub fn calculate_mysql_message_age_distribution(
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

/// Estimate processing capacity of the MySQL broker system
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
/// use celers_broker_sql::monitoring::estimate_mysql_processing_capacity;
///
/// let capacity = estimate_mysql_processing_capacity(10, 50.0, 5000);
/// assert_eq!(capacity.workers, 10);
/// assert_eq!(capacity.total_capacity_per_sec, 500.0);
/// ```
pub fn estimate_mysql_processing_capacity(
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

/// Calculate MySQL queue health score (0.0 - 1.0)
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
/// use celers_broker_sql::monitoring::calculate_mysql_queue_health_score;
///
/// let score = calculate_mysql_queue_health_score(100, 50.0, 1000, 40.0);
/// assert!(score > 0.5);
/// assert!(score <= 1.0);
/// ```
pub fn calculate_mysql_queue_health_score(
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

/// Analyze MySQL broker performance metrics
///
/// # Arguments
///
/// * `metrics` - HashMap of metric name to value
///
/// # Returns
///
/// Performance analysis summary
///
/// # Examples
///
/// ```
/// use celers_broker_sql::monitoring::analyze_mysql_broker_performance;
/// use std::collections::HashMap;
///
/// let mut metrics = HashMap::new();
/// metrics.insert("avg_latency_ms".to_string(), 25.0);
/// metrics.insert("throughput_msg_per_sec".to_string(), 500.0);
/// metrics.insert("error_rate_percent".to_string(), 0.5);
///
/// let analysis = analyze_mysql_broker_performance(&metrics);
/// assert!(analysis.contains_key("latency_status"));
/// ```
pub fn analyze_mysql_broker_performance(metrics: &HashMap<String, f64>) -> HashMap<String, String> {
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

// Helper function to calculate percentile
fn percentile(sorted_values: &[f64], p: f64) -> f64 {
    if sorted_values.is_empty() {
        return 0.0;
    }

    let index = (p / 100.0 * (sorted_values.len() - 1) as f64).round() as usize;
    sorted_values[index.min(sorted_values.len() - 1)]
}

/// Cost analysis for MySQL broker operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MysqlCostAnalysis {
    /// Total database storage used (GB)
    pub storage_gb: f64,
    /// Total IOPS (read + write operations per second)
    pub total_iops: f64,
    /// Network egress (GB per day)
    pub network_egress_gb_per_day: f64,
    /// Estimated monthly cost (USD)
    pub estimated_monthly_cost_usd: f64,
    /// Cost per 1000 messages processed
    pub cost_per_1000_messages: f64,
    /// Recommendations for cost optimization
    pub optimization_recommendations: Vec<String>,
}

/// SLA compliance tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlaComplianceReport {
    /// Total messages processed in period
    pub total_messages: usize,
    /// Messages within SLA
    pub messages_within_sla: usize,
    /// Messages exceeding SLA
    pub messages_exceeding_sla: usize,
    /// SLA compliance percentage (0-100)
    pub compliance_percentage: f64,
    /// Average processing time (seconds)
    pub avg_processing_time_secs: f64,
    /// P95 processing time (seconds)
    pub p95_processing_time_secs: f64,
    /// P99 processing time (seconds)
    pub p99_processing_time_secs: f64,
    /// SLA status
    pub status: SlaStatus,
}

/// SLA compliance status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SlaStatus {
    /// Compliant (>= 99% within SLA)
    Compliant,
    /// Warning (95-99% within SLA)
    Warning,
    /// Violation (< 95% within SLA)
    Violation,
}

/// Alert threshold recommendations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertThresholds {
    /// Queue size alert threshold
    pub queue_size_warning: usize,
    /// Queue size critical threshold
    pub queue_size_critical: usize,
    /// Processing lag warning (seconds)
    pub lag_warning_secs: u64,
    /// Processing lag critical (seconds)
    pub lag_critical_secs: u64,
    /// Error rate warning (percentage)
    pub error_rate_warning_percent: f64,
    /// Error rate critical (percentage)
    pub error_rate_critical_percent: f64,
    /// DLQ size warning threshold
    pub dlq_size_warning: usize,
    /// DLQ size critical threshold
    pub dlq_size_critical: usize,
}

/// Capacity forecast for future load
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapacityForecast {
    /// Current capacity (messages per hour)
    pub current_capacity_per_hour: f64,
    /// Projected load (messages per hour)
    pub projected_load_per_hour: f64,
    /// Capacity utilization percentage
    pub utilization_percent: f64,
    /// Time until capacity exhausted (hours), None if capacity sufficient
    pub time_to_exhaustion_hours: Option<f64>,
    /// Recommended additional workers
    pub recommended_additional_workers: usize,
    /// Forecast status
    pub status: CapacityStatus,
}

/// Capacity status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CapacityStatus {
    /// Capacity sufficient (< 60% utilization)
    Sufficient,
    /// Approaching capacity (60-80% utilization)
    Warning,
    /// At capacity (80-100% utilization)
    Critical,
    /// Over capacity (> 100% utilization)
    Exceeded,
}

/// Estimate MySQL broker operational costs
///
/// # Arguments
///
/// * `storage_gb` - Total database storage in GB
/// * `total_iops` - Total IOPS (read + write operations per second)
/// * `network_egress_gb_per_day` - Network egress in GB per day
/// * `messages_per_day` - Total messages processed per day
/// * `storage_cost_per_gb` - Cost per GB of storage per month (default: $0.10 for AWS RDS)
/// * `iops_cost_per_1000` - Cost per 1000 IOPS per month (default: $0.10 for AWS RDS)
/// * `network_cost_per_gb` - Cost per GB of network egress (default: $0.09 for AWS)
///
/// # Returns
///
/// Cost analysis with optimization recommendations
///
/// # Examples
///
/// ```
/// use celers_broker_sql::monitoring::estimate_mysql_operational_cost;
///
/// let cost = estimate_mysql_operational_cost(
///     100.0,  // 100 GB storage
///     5000.0, // 5000 IOPS
///     50.0,   // 50 GB egress per day
///     1_000_000, // 1M messages per day
///     0.10,   // $0.10 per GB storage
///     0.10,   // $0.10 per 1000 IOPS
///     0.09    // $0.09 per GB egress
/// );
/// assert!(cost.estimated_monthly_cost_usd > 0.0);
/// ```
#[allow(clippy::too_many_arguments)]
pub fn estimate_mysql_operational_cost(
    storage_gb: f64,
    total_iops: f64,
    network_egress_gb_per_day: f64,
    messages_per_day: usize,
    storage_cost_per_gb: f64,
    iops_cost_per_1000: f64,
    network_cost_per_gb: f64,
) -> MysqlCostAnalysis {
    // Monthly storage cost
    let monthly_storage_cost = storage_gb * storage_cost_per_gb;

    // Monthly IOPS cost (IOPS is per second, need to convert to monthly)
    let iops_per_month = total_iops * 60.0 * 60.0 * 24.0 * 30.0;
    let monthly_iops_cost = (iops_per_month / 1000.0) * iops_cost_per_1000;

    // Monthly network egress cost
    let monthly_network_cost = network_egress_gb_per_day * 30.0 * network_cost_per_gb;

    let estimated_monthly_cost_usd =
        monthly_storage_cost + monthly_iops_cost + monthly_network_cost;

    let messages_per_month = messages_per_day * 30;
    let cost_per_1000_messages = if messages_per_month > 0 {
        (estimated_monthly_cost_usd / messages_per_month as f64) * 1000.0
    } else {
        0.0
    };

    let mut optimization_recommendations = Vec::new();

    // Storage optimization
    if storage_gb > 500.0 {
        optimization_recommendations.push(
            "Consider implementing data archival policy for completed tasks older than 30 days"
                .to_string(),
        );
    }
    if storage_gb > 1000.0 {
        optimization_recommendations.push(
            "Large database: consider table partitioning to improve query performance and enable efficient archival".to_string()
        );
    }

    // IOPS optimization
    if total_iops > 10000.0 {
        optimization_recommendations.push(
            "High IOPS: consider batch operations to reduce database round-trips".to_string(),
        );
        optimization_recommendations.push(
            "Review indexes to ensure optimal query performance and reduce unnecessary scans"
                .to_string(),
        );
    }

    // Network optimization
    if network_egress_gb_per_day > 100.0 {
        optimization_recommendations.push(
            "High network egress: consider payload compression for large task data".to_string(),
        );
    }

    // Cost per message optimization
    if cost_per_1000_messages > 1.0 {
        optimization_recommendations.push(
            "High cost per message: review task retention policies and optimize query patterns"
                .to_string(),
        );
    }

    MysqlCostAnalysis {
        storage_gb,
        total_iops,
        network_egress_gb_per_day,
        estimated_monthly_cost_usd,
        cost_per_1000_messages,
        optimization_recommendations,
    }
}

/// Calculate SLA compliance report
///
/// # Arguments
///
/// * `processing_times_secs` - Slice of processing times in seconds
/// * `sla_threshold_secs` - SLA threshold in seconds
///
/// # Returns
///
/// SLA compliance report with status
///
/// # Examples
///
/// ```
/// use celers_broker_sql::monitoring::calculate_sla_compliance;
///
/// let times = vec![5.0, 10.0, 15.0, 20.0, 25.0, 100.0]; // One SLA violation
/// let report = calculate_sla_compliance(&times, 30.0);
/// assert_eq!(report.total_messages, 6);
/// assert_eq!(report.messages_within_sla, 5);
/// ```
pub fn calculate_sla_compliance(
    processing_times_secs: &[f64],
    sla_threshold_secs: f64,
) -> SlaComplianceReport {
    if processing_times_secs.is_empty() {
        return SlaComplianceReport {
            total_messages: 0,
            messages_within_sla: 0,
            messages_exceeding_sla: 0,
            compliance_percentage: 0.0,
            avg_processing_time_secs: 0.0,
            p95_processing_time_secs: 0.0,
            p99_processing_time_secs: 0.0,
            status: SlaStatus::Violation,
        };
    }

    let total_messages = processing_times_secs.len();
    let messages_within_sla = processing_times_secs
        .iter()
        .filter(|&&time| time <= sla_threshold_secs)
        .count();
    let messages_exceeding_sla = total_messages - messages_within_sla;

    let compliance_percentage = (messages_within_sla as f64 / total_messages as f64) * 100.0;

    let mut sorted_times = processing_times_secs.to_vec();
    sorted_times.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let avg_processing_time_secs = sorted_times.iter().sum::<f64>() / total_messages as f64;
    let p95_processing_time_secs = percentile(&sorted_times, 95.0);
    let p99_processing_time_secs = percentile(&sorted_times, 99.0);

    let status = if compliance_percentage >= 99.0 {
        SlaStatus::Compliant
    } else if compliance_percentage >= 95.0 {
        SlaStatus::Warning
    } else {
        SlaStatus::Violation
    };

    SlaComplianceReport {
        total_messages,
        messages_within_sla,
        messages_exceeding_sla,
        compliance_percentage,
        avg_processing_time_secs,
        p95_processing_time_secs,
        p99_processing_time_secs,
        status,
    }
}

/// Calculate recommended alert thresholds
///
/// # Arguments
///
/// * `avg_queue_size` - Average queue size under normal load
/// * `max_queue_size` - Maximum observed queue size
/// * `avg_processing_rate` - Average processing rate (messages per second)
/// * `target_lag_secs` - Target processing lag in seconds
///
/// # Returns
///
/// Recommended alert thresholds for monitoring
///
/// # Examples
///
/// ```
/// use celers_broker_sql::monitoring::calculate_alert_thresholds;
///
/// let thresholds = calculate_alert_thresholds(100, 1000, 50.0, 60);
/// assert!(thresholds.queue_size_warning > 0);
/// assert!(thresholds.queue_size_critical > thresholds.queue_size_warning);
/// ```
pub fn calculate_alert_thresholds(
    avg_queue_size: usize,
    max_queue_size: usize,
    _avg_processing_rate: f64,
    target_lag_secs: u64,
) -> AlertThresholds {
    // Queue size thresholds based on observed patterns
    let queue_size_warning = ((avg_queue_size as f64 * 2.0) as usize).min(max_queue_size);
    let queue_size_critical = ((avg_queue_size as f64 * 5.0) as usize).min(max_queue_size * 2);

    // Lag thresholds based on target
    let lag_warning_secs = target_lag_secs * 2;
    let lag_critical_secs = target_lag_secs * 5;

    // Error rate thresholds (industry standard)
    let error_rate_warning_percent = 1.0;
    let error_rate_critical_percent = 5.0;

    // DLQ thresholds based on queue size
    let dlq_size_warning = (avg_queue_size as f64 * 0.1) as usize;
    let dlq_size_critical = (avg_queue_size as f64 * 0.5) as usize;

    AlertThresholds {
        queue_size_warning,
        queue_size_critical,
        lag_warning_secs,
        lag_critical_secs,
        error_rate_warning_percent,
        error_rate_critical_percent,
        dlq_size_warning,
        dlq_size_critical,
    }
}

/// Forecast capacity needs based on growth trends
///
/// # Arguments
///
/// * `current_load_per_hour` - Current load in messages per hour
/// * `growth_rate_percent` - Expected growth rate percentage (e.g., 20.0 for 20%)
/// * `forecast_horizon_days` - Number of days to forecast
/// * `current_workers` - Current number of workers
/// * `processing_rate_per_worker` - Processing rate per worker (messages per hour)
///
/// # Returns
///
/// Capacity forecast with scaling recommendations
///
/// # Examples
///
/// ```
/// use celers_broker_sql::monitoring::forecast_capacity_needs;
///
/// let forecast = forecast_capacity_needs(10000.0, 20.0, 30, 10, 1200.0);
/// assert!(forecast.current_capacity_per_hour > 0.0);
/// ```
pub fn forecast_capacity_needs(
    current_load_per_hour: f64,
    growth_rate_percent: f64,
    forecast_horizon_days: u64,
    current_workers: usize,
    processing_rate_per_worker: f64,
) -> CapacityForecast {
    let current_capacity_per_hour = current_workers as f64 * processing_rate_per_worker;

    // Calculate projected load using compound growth
    let growth_multiplier = 1.0 + (growth_rate_percent / 100.0);
    let days_factor = forecast_horizon_days as f64 / 30.0; // Convert to months
    let projected_load_per_hour = current_load_per_hour * growth_multiplier.powf(days_factor);

    let utilization_percent = (projected_load_per_hour / current_capacity_per_hour) * 100.0;

    let time_to_exhaustion_hours = if projected_load_per_hour > current_capacity_per_hour {
        // Calculate when capacity will be exhausted
        let daily_growth = current_load_per_hour * (growth_multiplier.powf(1.0 / 30.0) - 1.0);
        if daily_growth > 0.0 {
            Some((current_capacity_per_hour - current_load_per_hour) / daily_growth * 24.0)
        } else {
            None
        }
    } else {
        None
    };

    let recommended_additional_workers = if utilization_percent > 80.0 {
        let needed_capacity = projected_load_per_hour * 1.2; // 20% buffer
        let total_workers_needed = (needed_capacity / processing_rate_per_worker).ceil() as usize;
        total_workers_needed.saturating_sub(current_workers)
    } else {
        0
    };

    let status = if utilization_percent > 100.0 {
        CapacityStatus::Exceeded
    } else if utilization_percent > 80.0 {
        CapacityStatus::Critical
    } else if utilization_percent > 60.0 {
        CapacityStatus::Warning
    } else {
        CapacityStatus::Sufficient
    };

    CapacityForecast {
        current_capacity_per_hour,
        projected_load_per_hour,
        utilization_percent,
        time_to_exhaustion_hours,
        recommended_additional_workers,
        status,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_analyze_consumer_lag_optimal() {
        let lag = analyze_mysql_consumer_lag(100, 50.0, 10);
        assert_eq!(lag.queue_size, 100);
        assert_eq!(lag.processing_rate, 50.0);
        assert_eq!(lag.lag_seconds, 2.0);
        assert!(!lag.is_lagging);
        assert_eq!(lag.recommendation, ScalingRecommendation::Optimal);
    }

    #[test]
    fn test_analyze_consumer_lag_needs_scale_up() {
        let lag = analyze_mysql_consumer_lag(1000, 5.0, 10);
        assert!(lag.is_lagging);
        assert!(matches!(
            lag.recommendation,
            ScalingRecommendation::ScaleUp { .. }
        ));
    }

    #[test]
    fn test_calculate_message_velocity_growing() {
        let velocity = calculate_mysql_message_velocity(1000, 1600, 60.0);
        assert_eq!(velocity.velocity, 10.0);
        assert_eq!(velocity.trend, QueueTrend::SlowGrowth);
    }

    #[test]
    fn test_calculate_message_velocity_stable() {
        let velocity = calculate_mysql_message_velocity(1000, 1010, 60.0);
        assert!(velocity.velocity < 1.0);
        assert_eq!(velocity.trend, QueueTrend::Stable);
    }

    #[test]
    fn test_suggest_worker_scaling() {
        let scaling = suggest_mysql_worker_scaling(2000, 5, 40.0, 100);
        assert_eq!(scaling.current_workers, 5);
        assert!(scaling.recommended_workers >= 1);
    }

    #[test]
    fn test_message_age_distribution() {
        let ages = vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0];
        let dist = calculate_mysql_message_age_distribution(&ages, 75.0);
        assert_eq!(dist.total_messages, 10);
        assert_eq!(dist.min_age_secs, 10.0);
        assert_eq!(dist.max_age_secs, 100.0);
        assert_eq!(dist.messages_exceeding_sla, 3);
    }

    #[test]
    fn test_message_age_distribution_empty() {
        let ages = vec![];
        let dist = calculate_mysql_message_age_distribution(&ages, 60.0);
        assert_eq!(dist.total_messages, 0);
    }

    #[test]
    fn test_estimate_processing_capacity() {
        let capacity = estimate_mysql_processing_capacity(10, 50.0, 5000);
        assert_eq!(capacity.workers, 10);
        assert_eq!(capacity.total_capacity_per_sec, 500.0);
        assert_eq!(capacity.total_capacity_per_min, 30000.0);
        assert_eq!(capacity.time_to_clear_backlog_secs, 10.0);
    }

    #[test]
    fn test_calculate_queue_health_score() {
        let score = calculate_mysql_queue_health_score(100, 50.0, 1000, 40.0);
        assert!(score > 0.5);
        assert!(score <= 1.0);
    }

    #[test]
    fn test_analyze_broker_performance() {
        let mut metrics = HashMap::new();
        metrics.insert("avg_latency_ms".to_string(), 25.0);
        metrics.insert("throughput_msg_per_sec".to_string(), 500.0);
        metrics.insert("error_rate_percent".to_string(), 0.5);

        let analysis = analyze_mysql_broker_performance(&metrics);
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
    fn test_estimate_operational_cost() {
        let cost = estimate_mysql_operational_cost(
            100.0,     // 100 GB storage
            5000.0,    // 5000 IOPS
            50.0,      // 50 GB egress per day
            1_000_000, // 1M messages per day
            0.10,      // $0.10 per GB storage
            0.10,      // $0.10 per 1000 IOPS
            0.09,      // $0.09 per GB egress
        );
        assert!(cost.estimated_monthly_cost_usd > 0.0);
        assert!(cost.cost_per_1000_messages > 0.0);
        assert_eq!(cost.storage_gb, 100.0);
    }

    #[test]
    fn test_estimate_operational_cost_high_storage() {
        let cost = estimate_mysql_operational_cost(
            1200.0, // Large storage
            10000.0, 50.0, 1_000_000, 0.10, 0.10, 0.09,
        );
        // Should have recommendations for high storage
        assert!(!cost.optimization_recommendations.is_empty());
        assert!(cost
            .optimization_recommendations
            .iter()
            .any(|r| r.contains("partition")));
    }

    #[test]
    fn test_calculate_sla_compliance_compliant() {
        let times = vec![5.0, 10.0, 15.0, 20.0, 25.0];
        let report = calculate_sla_compliance(&times, 30.0);
        assert_eq!(report.total_messages, 5);
        assert_eq!(report.messages_within_sla, 5);
        assert_eq!(report.messages_exceeding_sla, 0);
        assert_eq!(report.compliance_percentage, 100.0);
        assert_eq!(report.status, SlaStatus::Compliant);
    }

    #[test]
    fn test_calculate_sla_compliance_violation() {
        let times = vec![5.0, 10.0, 15.0, 20.0, 25.0, 100.0, 150.0]; // 2 violations
        let report = calculate_sla_compliance(&times, 30.0);
        assert_eq!(report.total_messages, 7);
        assert_eq!(report.messages_within_sla, 5);
        assert_eq!(report.messages_exceeding_sla, 2);
        assert!(report.compliance_percentage < 95.0);
        assert_eq!(report.status, SlaStatus::Violation);
    }

    #[test]
    fn test_calculate_sla_compliance_empty() {
        let times = vec![];
        let report = calculate_sla_compliance(&times, 30.0);
        assert_eq!(report.total_messages, 0);
        assert_eq!(report.status, SlaStatus::Violation);
    }

    #[test]
    fn test_calculate_alert_thresholds() {
        let thresholds = calculate_alert_thresholds(100, 1000, 50.0, 60);
        assert!(thresholds.queue_size_warning > 0);
        assert!(thresholds.queue_size_critical > thresholds.queue_size_warning);
        assert_eq!(thresholds.lag_warning_secs, 120);
        assert_eq!(thresholds.lag_critical_secs, 300);
        assert_eq!(thresholds.error_rate_warning_percent, 1.0);
        assert_eq!(thresholds.error_rate_critical_percent, 5.0);
    }

    #[test]
    fn test_forecast_capacity_sufficient() {
        let forecast = forecast_capacity_needs(
            5000.0, // Current load per hour (lower load)
            10.0,   // 10% growth
            30,     // 30 days
            10,     // 10 workers
            1500.0, // 1500 msg/hour per worker
        );
        assert_eq!(forecast.current_capacity_per_hour, 15000.0);
        assert!(forecast.projected_load_per_hour > 5000.0);
        // With 5000 load and 15000 capacity, utilization should be ~33%, which is Sufficient
        assert_eq!(forecast.status, CapacityStatus::Sufficient);
        assert_eq!(forecast.recommended_additional_workers, 0);
    }

    #[test]
    fn test_forecast_capacity_exceeded() {
        let forecast = forecast_capacity_needs(
            10000.0, // Current load per hour
            50.0,    // 50% growth
            60,      // 60 days (2 months)
            5,       // Only 5 workers
            1000.0,  // 1000 msg/hour per worker
        );
        assert_eq!(forecast.current_capacity_per_hour, 5000.0);
        assert!(forecast.projected_load_per_hour > forecast.current_capacity_per_hour);
        assert_eq!(forecast.status, CapacityStatus::Exceeded);
        assert!(forecast.recommended_additional_workers > 0);
    }

    #[test]
    fn test_forecast_capacity_warning() {
        let forecast = forecast_capacity_needs(
            8000.0, // Current load
            20.0,   // 20% growth
            30,     // 30 days
            10,     // 10 workers
            1200.0, // 1200 msg/hour per worker
        );
        // Capacity should be in warning range (60-80%)
        assert!(forecast.utilization_percent > 60.0);
        assert!(forecast.utilization_percent <= 80.0);
        assert_eq!(forecast.status, CapacityStatus::Warning);
    }
}
