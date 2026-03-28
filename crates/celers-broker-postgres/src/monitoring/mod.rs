//! PostgreSQL broker monitoring utilities
//!
//! This module provides production-grade monitoring and analysis utilities
//! for PostgreSQL-based task queues. These utilities help with capacity planning,
//! autoscaling decisions, SLA monitoring, and performance optimization.
//!
//! # Examples
//!
//! ```
//! use celers_broker_postgres::monitoring::*;
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Analyze consumer lag and get scaling recommendations
//! let queue_size = 1000;
//! let processing_rate = 50.0; // messages/sec
//! let lag = analyze_postgres_consumer_lag(queue_size, processing_rate, 100);
//! println!("Queue lag: {} seconds", lag.lag_seconds);
//! println!("Recommendation: {:?}", lag.recommendation);
//!
//! // Calculate message velocity and growth trends
//! let velocity = calculate_postgres_message_velocity(
//!     1000, // previous size
//!     1500, // current size
//!     60.0  // time window (seconds)
//! );
//! println!("Queue growing at {} msg/sec", velocity.velocity);
//!
//! // Get worker scaling recommendation
//! let scaling = suggest_postgres_worker_scaling(
//!     2000,  // queue_size
//!     5,     // current_workers
//!     40.0,  // avg_processing_rate (msg/sec per worker)
//!     100    // target_lag_seconds
//! );
//! println!("Suggested workers: {}", scaling.recommended_workers);
//! # Ok(())
//! # }
//! ```

pub mod advanced;

// Re-export all advanced monitoring types and functions
pub use advanced::{
    analyze_postgres_cache_hit_ratio, analyze_postgres_checkpoint_performance,
    analyze_postgres_connection_states, analyze_postgres_lock_contention,
    calculate_postgres_connection_pool_efficiency, calculate_postgres_index_bloat,
    calculate_postgres_table_bloat_ratio, detect_postgres_long_running_query,
    detect_postgres_sequential_scans, detect_postgres_table_stats_staleness,
    estimate_postgres_vacuum_urgency, monitor_postgres_autovacuum_effectiveness,
    AutovacuumEffectiveness, BloatSeverity, CacheHitRatioAnalysis, CheckpointPerformanceAnalysis,
    ConnectionPoolEfficiency, ConnectionStateAnalysis, IndexBloatAnalysis, LockContentionAnalysis,
    LockContentionSeverity, LongRunningQueryInfo, SequentialScanAnalysis, TableBloatAnalysis,
    TableStatsStaleness, VacuumUrgencyAnalysis,
};

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
/// use celers_broker_postgres::monitoring::analyze_postgres_consumer_lag;
///
/// let lag = analyze_postgres_consumer_lag(1000, 50.0, 100);
/// assert_eq!(lag.queue_size, 1000);
/// assert_eq!(lag.processing_rate, 50.0);
/// ```
pub fn analyze_postgres_consumer_lag(
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
/// use celers_broker_postgres::monitoring::{calculate_postgres_message_velocity, QueueTrend};
///
/// let velocity = calculate_postgres_message_velocity(1000, 1500, 60.0);
/// assert_eq!(velocity.previous_size, 1000);
/// assert_eq!(velocity.current_size, 1500);
/// assert_eq!(velocity.trend, QueueTrend::SlowGrowth);
/// ```
pub fn calculate_postgres_message_velocity(
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
/// use celers_broker_postgres::monitoring::suggest_postgres_worker_scaling;
///
/// let scaling = suggest_postgres_worker_scaling(2000, 5, 40.0, 100);
/// assert_eq!(scaling.current_workers, 5);
/// assert!(scaling.recommended_workers >= 1);
/// ```
pub fn suggest_postgres_worker_scaling(
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
/// use celers_broker_postgres::monitoring::calculate_postgres_message_age_distribution;
///
/// let ages = vec![10.0, 20.0, 30.0, 40.0, 50.0];
/// let dist = calculate_postgres_message_age_distribution(&ages, 60.0);
/// assert_eq!(dist.total_messages, 5);
/// assert_eq!(dist.messages_exceeding_sla, 0);
/// ```
pub fn calculate_postgres_message_age_distribution(
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

/// Estimate processing capacity of the PostgreSQL broker system
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
/// use celers_broker_postgres::monitoring::estimate_postgres_processing_capacity;
///
/// let capacity = estimate_postgres_processing_capacity(10, 50.0, 5000);
/// assert_eq!(capacity.workers, 10);
/// assert_eq!(capacity.total_capacity_per_sec, 500.0);
/// ```
pub fn estimate_postgres_processing_capacity(
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

/// Calculate PostgreSQL queue health score (0.0 - 1.0)
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
/// use celers_broker_postgres::monitoring::calculate_postgres_queue_health_score;
///
/// let score = calculate_postgres_queue_health_score(100, 50.0, 1000, 40.0);
/// assert!(score > 0.5);
/// assert!(score <= 1.0);
/// ```
pub fn calculate_postgres_queue_health_score(
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

/// Analyze PostgreSQL broker performance metrics
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
/// use celers_broker_postgres::monitoring::analyze_postgres_broker_performance;
/// use std::collections::HashMap;
///
/// let mut metrics = HashMap::new();
/// metrics.insert("avg_latency_ms".to_string(), 25.0);
/// metrics.insert("throughput_msg_per_sec".to_string(), 500.0);
/// metrics.insert("error_rate_percent".to_string(), 0.5);
///
/// let analysis = analyze_postgres_broker_performance(&metrics);
/// assert!(analysis.contains_key("latency_status"));
/// ```
pub fn analyze_postgres_broker_performance(
    metrics: &HashMap<String, f64>,
) -> HashMap<String, String> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_analyze_consumer_lag_optimal() {
        let lag = analyze_postgres_consumer_lag(100, 50.0, 10);
        assert_eq!(lag.queue_size, 100);
        assert_eq!(lag.processing_rate, 50.0);
        assert_eq!(lag.lag_seconds, 2.0);
        assert!(!lag.is_lagging);
        assert_eq!(lag.recommendation, ScalingRecommendation::Optimal);
    }

    #[test]
    fn test_analyze_consumer_lag_needs_scale_up() {
        let lag = analyze_postgres_consumer_lag(1000, 5.0, 10);
        assert!(lag.is_lagging);
        assert!(matches!(
            lag.recommendation,
            ScalingRecommendation::ScaleUp { .. }
        ));
    }

    #[test]
    fn test_calculate_message_velocity_growing() {
        let velocity = calculate_postgres_message_velocity(1000, 1600, 60.0);
        assert_eq!(velocity.velocity, 10.0);
        assert_eq!(velocity.trend, QueueTrend::SlowGrowth);
    }

    #[test]
    fn test_calculate_message_velocity_stable() {
        let velocity = calculate_postgres_message_velocity(1000, 1010, 60.0);
        assert!(velocity.velocity < 1.0);
        assert_eq!(velocity.trend, QueueTrend::Stable);
    }

    #[test]
    fn test_suggest_worker_scaling() {
        let scaling = suggest_postgres_worker_scaling(2000, 5, 40.0, 100);
        assert_eq!(scaling.current_workers, 5);
        assert!(scaling.recommended_workers >= 1);
    }

    #[test]
    fn test_message_age_distribution() {
        let ages = vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0];
        let dist = calculate_postgres_message_age_distribution(&ages, 75.0);
        assert_eq!(dist.total_messages, 10);
        assert_eq!(dist.min_age_secs, 10.0);
        assert_eq!(dist.max_age_secs, 100.0);
        assert_eq!(dist.messages_exceeding_sla, 3);
    }

    #[test]
    fn test_message_age_distribution_empty() {
        let ages = vec![];
        let dist = calculate_postgres_message_age_distribution(&ages, 60.0);
        assert_eq!(dist.total_messages, 0);
    }

    #[test]
    fn test_estimate_processing_capacity() {
        let capacity = estimate_postgres_processing_capacity(10, 50.0, 5000);
        assert_eq!(capacity.workers, 10);
        assert_eq!(capacity.total_capacity_per_sec, 500.0);
        assert_eq!(capacity.total_capacity_per_min, 30000.0);
        assert_eq!(capacity.time_to_clear_backlog_secs, 10.0);
    }

    #[test]
    fn test_calculate_queue_health_score() {
        let score = calculate_postgres_queue_health_score(100, 50.0, 1000, 40.0);
        assert!(score > 0.5);
        assert!(score <= 1.0);
    }

    #[test]
    fn test_analyze_broker_performance() {
        let mut metrics = HashMap::new();
        metrics.insert("avg_latency_ms".to_string(), 25.0);
        metrics.insert("throughput_msg_per_sec".to_string(), 500.0);
        metrics.insert("error_rate_percent".to_string(), 0.5);

        let analysis = analyze_postgres_broker_performance(&metrics);
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
    fn test_analyze_lock_contention() {
        let analysis = analyze_postgres_lock_contention(5, 50, 2);
        assert_eq!(analysis.active_locks, 5);
        assert_eq!(analysis.waiting_locks, 50);
        assert_eq!(analysis.deadlocks_detected, 2);
        // contention_ratio = 50/5 = 10.0, which is > 5.0, so Critical
        assert_eq!(analysis.severity, LockContentionSeverity::Critical);
        assert!(!analysis.recommendations.is_empty());
    }

    #[test]
    fn test_analyze_lock_contention_low() {
        let analysis = analyze_postgres_lock_contention(10, 5, 0);
        assert_eq!(analysis.severity, LockContentionSeverity::Low);
    }

    #[test]
    fn test_calculate_table_bloat_ratio() {
        let bloat = calculate_postgres_table_bloat_ratio(1000000, 750000);
        assert_eq!(bloat.total_bytes, 1000000);
        assert_eq!(bloat.live_bytes, 750000);
        assert_eq!(bloat.bloat_bytes, 250000);
        assert_eq!(bloat.bloat_ratio, 0.25);
        assert_eq!(bloat.severity, BloatSeverity::Medium);
    }

    #[test]
    fn test_calculate_table_bloat_ratio_high() {
        let bloat = calculate_postgres_table_bloat_ratio(1000000, 400000);
        assert_eq!(bloat.bloat_ratio, 0.6);
        // 0.6 > 0.5, so Critical
        assert_eq!(bloat.severity, BloatSeverity::Critical);
    }

    #[test]
    fn test_analyze_checkpoint_performance() {
        let analysis = analyze_postgres_checkpoint_performance(300.0, 120.0, 1000, 5.0);
        assert_eq!(analysis.checkpoint_interval_secs, 300.0);
        assert_eq!(analysis.avg_checkpoint_duration_secs, 120.0);
        assert!(analysis.checkpoints_per_hour > 0.0);
        assert!(!analysis.recommendations.is_empty());
    }

    #[test]
    fn test_estimate_vacuum_urgency() {
        let urgency = estimate_postgres_vacuum_urgency(1000000, 500000, 0.15, 72.0);
        assert!(urgency.urgency_score >= 0.0 && urgency.urgency_score <= 1.0);
        assert!(urgency.estimated_time_secs > 0.0);
    }

    #[test]
    fn test_connection_pool_efficiency() {
        let efficiency = calculate_postgres_connection_pool_efficiency(100, 80, 20, 1000);
        assert_eq!(efficiency.total_connections, 100);
        assert_eq!(efficiency.active_connections, 80);
        assert_eq!(efficiency.idle_connections, 20);
        assert_eq!(efficiency.total_queries, 1000);
        assert!(efficiency.utilization_percent >= 0.0 && efficiency.utilization_percent <= 100.0);
    }
}
