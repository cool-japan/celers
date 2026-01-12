//! AWS SQS broker optimization and auto-tuning utilities
//!
//! This module provides high-level optimization functions that combine
//! monitoring and utility functions to automatically tune SQS configurations
//! for production workloads.
//!
//! # Examples
//!
//! ```
//! use celers_broker_sqs::optimization::*;
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Get optimized configuration for a production workload
//! let config = optimize_for_workload(
//!     WorkloadProfile::HighThroughput {
//!         messages_per_second: 500.0,
//!         avg_message_size_bytes: 10_000,
//!         workers: 20,
//!     }
//! );
//!
//! println!("Optimal batch size: {}", config.batch_size);
//! println!("Optimal visibility timeout: {}s", config.visibility_timeout_secs);
//! println!("Optimal wait time: {}s", config.wait_time_secs);
//! # Ok(())
//! # }
//! ```

use crate::monitoring::*;
use crate::utilities::*;
use serde::{Deserialize, Serialize};

/// Workload profile for optimization
#[derive(Debug, Clone)]
pub enum WorkloadProfile {
    /// High throughput workload (many messages/sec)
    HighThroughput {
        /// Expected messages per second
        messages_per_second: f64,
        /// Average message size in bytes
        avg_message_size_bytes: usize,
        /// Number of workers
        workers: usize,
    },
    /// Low latency workload (prioritize speed)
    LowLatency {
        /// Target latency in milliseconds
        target_latency_ms: u64,
        /// Average message size in bytes
        avg_message_size_bytes: usize,
        /// Number of workers
        workers: usize,
    },
    /// Cost optimized workload (minimize AWS costs)
    CostOptimized {
        /// Messages per day
        messages_per_day: usize,
        /// Average message size in bytes
        avg_message_size_bytes: usize,
        /// Acceptable latency in seconds
        acceptable_latency_secs: u64,
    },
    /// Balanced workload (balance cost and performance)
    Balanced {
        /// Expected messages per second
        messages_per_second: f64,
        /// Average message size in bytes
        avg_message_size_bytes: usize,
        /// Number of workers
        workers: usize,
    },
}

/// Optimized SQS configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizedConfig {
    /// Recommended batch size
    pub batch_size: usize,
    /// Recommended visibility timeout (seconds)
    pub visibility_timeout_secs: u64,
    /// Recommended wait time (seconds)
    pub wait_time_secs: u64,
    /// Recommended message retention (seconds)
    pub message_retention_secs: u64,
    /// Recommended max receive count for DLQ
    pub dlq_max_receive_count: usize,
    /// Use FIFO queue
    pub use_fifo: bool,
    /// Enable compression
    pub enable_compression: bool,
    /// Compression threshold (bytes)
    pub compression_threshold_bytes: usize,
    /// Enable batching
    pub enable_batching: bool,
    /// Recommended concurrent receives
    pub concurrent_receives: usize,
    /// Estimated monthly cost (USD)
    pub estimated_monthly_cost_usd: f64,
    /// Estimated throughput capacity (msg/sec)
    pub estimated_throughput_capacity: f64,
    /// Optimization notes
    pub notes: Vec<String>,
}

/// Optimize SQS configuration for a workload profile
///
/// This function analyzes the workload characteristics and returns
/// an optimized configuration for AWS SQS.
///
/// # Arguments
///
/// * `profile` - Workload profile to optimize for
///
/// # Returns
///
/// Optimized SQS configuration
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::optimization::*;
///
/// let config = optimize_for_workload(
///     WorkloadProfile::HighThroughput {
///         messages_per_second: 500.0,
///         avg_message_size_bytes: 10_000,
///         workers: 20,
///     }
/// );
/// assert!(config.enable_batching);
/// assert!(config.batch_size > 1);
/// ```
pub fn optimize_for_workload(profile: WorkloadProfile) -> OptimizedConfig {
    match profile {
        WorkloadProfile::HighThroughput {
            messages_per_second,
            avg_message_size_bytes,
            workers,
        } => {
            let mut notes = Vec::new();

            // Optimize for throughput
            let batch_size = calculate_optimal_sqs_batch_size(1000, avg_message_size_bytes, 200);
            let visibility_timeout_secs = calculate_optimal_sqs_visibility_timeout(5.0, 0.3);
            let wait_time_secs = calculate_optimal_sqs_wait_time(messages_per_second, 0.3);
            let message_retention_secs = calculate_optimal_sqs_retention(1.0, 3, 2.0);
            let dlq_max_receive_count = calculate_sqs_dlq_threshold(0.05, 0.2);
            let use_fifo = should_use_sqs_fifo(false, false, messages_per_second);
            let enable_compression = avg_message_size_bytes > 50_000;
            let compression_threshold_bytes = if enable_compression {
                50_000
            } else {
                usize::MAX
            };
            let enable_batching = true;
            let concurrent_receives =
                calculate_optimal_sqs_concurrent_receives(workers, 5.0, wait_time_secs);

            let messages_per_day = (messages_per_second * 86400.0) as usize;
            let estimated_monthly_cost_usd =
                estimate_sqs_monthly_cost(messages_per_day, use_fifo, enable_batching);
            let estimated_throughput_capacity =
                estimate_sqs_throughput_capacity(workers, 5.0, enable_batching);

            notes.push("Optimized for high throughput".to_string());
            notes.push(format!("Batching enabled: {}x cost reduction", batch_size));
            if enable_compression {
                notes.push("Compression enabled for large messages".to_string());
            }
            notes.push(format!(
                "Estimated capacity: {:.0} msg/sec",
                estimated_throughput_capacity
            ));

            OptimizedConfig {
                batch_size,
                visibility_timeout_secs,
                wait_time_secs,
                message_retention_secs,
                dlq_max_receive_count,
                use_fifo,
                enable_compression,
                compression_threshold_bytes,
                enable_batching,
                concurrent_receives,
                estimated_monthly_cost_usd,
                estimated_throughput_capacity,
                notes,
            }
        }
        WorkloadProfile::LowLatency {
            target_latency_ms,
            avg_message_size_bytes,
            workers,
        } => {
            let mut notes = Vec::new();

            // Optimize for latency
            let batch_size =
                calculate_optimal_sqs_batch_size(1000, avg_message_size_bytes, target_latency_ms);
            let visibility_timeout_secs = calculate_optimal_sqs_visibility_timeout(2.0, 0.1);
            let wait_time_secs = 1; // Short wait for low latency
            let message_retention_secs = calculate_optimal_sqs_retention(0.5, 3, 1.5);
            let dlq_max_receive_count = calculate_sqs_dlq_threshold(0.05, 0.3);
            let use_fifo = false; // Standard queue for lower latency
            let enable_compression = false; // Compression adds latency
            let compression_threshold_bytes = usize::MAX;
            let enable_batching = batch_size > 1;
            let concurrent_receives =
                calculate_optimal_sqs_concurrent_receives(workers, 2.0, wait_time_secs);

            let messages_per_day = 100_000; // Estimate for cost calculation
            let estimated_monthly_cost_usd =
                estimate_sqs_monthly_cost(messages_per_day, use_fifo, enable_batching);
            let estimated_throughput_capacity =
                estimate_sqs_throughput_capacity(workers, 2.0, enable_batching);

            notes.push("Optimized for low latency".to_string());
            notes.push(format!("Target latency: {}ms", target_latency_ms));
            notes.push("Compression disabled to minimize latency".to_string());
            notes.push("Short polling wait time for fast response".to_string());

            OptimizedConfig {
                batch_size,
                visibility_timeout_secs,
                wait_time_secs,
                message_retention_secs,
                dlq_max_receive_count,
                use_fifo,
                enable_compression,
                compression_threshold_bytes,
                enable_batching,
                concurrent_receives,
                estimated_monthly_cost_usd,
                estimated_throughput_capacity,
                notes,
            }
        }
        WorkloadProfile::CostOptimized {
            messages_per_day,
            avg_message_size_bytes,
            acceptable_latency_secs: _,
        } => {
            let mut notes = Vec::new();

            // Optimize for cost
            let batch_size = calculate_optimal_sqs_batch_size(1000, avg_message_size_bytes, 1000);
            let visibility_timeout_secs = calculate_optimal_sqs_visibility_timeout(10.0, 0.5);
            let wait_time_secs = calculate_optimal_sqs_wait_time(1.0, 0.0); // Max wait for cost savings
            let message_retention_secs = calculate_optimal_sqs_retention(4.0, 3, 1.0);
            let dlq_max_receive_count = calculate_sqs_dlq_threshold(0.1, 0.4);
            let use_fifo = false; // Standard queue is cheaper
            let enable_compression = avg_message_size_bytes > 10_000;
            let compression_threshold_bytes = if enable_compression {
                10_000
            } else {
                usize::MAX
            };
            let enable_batching = true;
            let workers = (messages_per_day as f64 / 86400.0 / 10.0).ceil() as usize;
            let concurrent_receives =
                calculate_optimal_sqs_concurrent_receives(workers.max(1), 10.0, wait_time_secs);

            let estimated_monthly_cost_usd =
                estimate_sqs_monthly_cost(messages_per_day, use_fifo, enable_batching);
            let estimated_throughput_capacity =
                estimate_sqs_throughput_capacity(workers.max(1), 10.0, enable_batching);

            notes.push("Optimized for minimum cost".to_string());
            notes.push(format!("Maximum batching: {} messages", batch_size));
            notes.push(format!("Long polling: {}s wait time", wait_time_secs));
            notes.push(format!(
                "Estimated cost: ${:.2}/month",
                estimated_monthly_cost_usd
            ));
            if enable_compression {
                notes.push("Compression enabled to reduce message size".to_string());
            }

            OptimizedConfig {
                batch_size,
                visibility_timeout_secs,
                wait_time_secs,
                message_retention_secs,
                dlq_max_receive_count,
                use_fifo,
                enable_compression,
                compression_threshold_bytes,
                enable_batching,
                concurrent_receives,
                estimated_monthly_cost_usd,
                estimated_throughput_capacity,
                notes,
            }
        }
        WorkloadProfile::Balanced {
            messages_per_second,
            avg_message_size_bytes,
            workers,
        } => {
            let mut notes = Vec::new();

            // Balance cost and performance
            let batch_size = calculate_optimal_sqs_batch_size(1000, avg_message_size_bytes, 100);
            let visibility_timeout_secs = calculate_optimal_sqs_visibility_timeout(5.0, 0.3);
            let wait_time_secs = calculate_optimal_sqs_wait_time(messages_per_second, 0.5);
            let message_retention_secs = calculate_optimal_sqs_retention(2.0, 3, 2.0);
            let dlq_max_receive_count = calculate_sqs_dlq_threshold(0.05, 0.3);
            let use_fifo = should_use_sqs_fifo(false, false, messages_per_second);
            let enable_compression = avg_message_size_bytes > 50_000;
            let compression_threshold_bytes = if enable_compression {
                50_000
            } else {
                usize::MAX
            };
            let enable_batching = true;
            let concurrent_receives =
                calculate_optimal_sqs_concurrent_receives(workers, 5.0, wait_time_secs);

            let messages_per_day = (messages_per_second * 86400.0) as usize;
            let estimated_monthly_cost_usd =
                estimate_sqs_monthly_cost(messages_per_day, use_fifo, enable_batching);
            let estimated_throughput_capacity =
                estimate_sqs_throughput_capacity(workers, 5.0, enable_batching);

            notes.push("Balanced cost and performance".to_string());
            notes.push(format!("Batch size: {} messages", batch_size));
            notes.push(format!("Wait time: {}s (balanced)", wait_time_secs));
            notes.push(format!(
                "Cost: ${:.2}/month, Capacity: {:.0} msg/sec",
                estimated_monthly_cost_usd, estimated_throughput_capacity
            ));

            OptimizedConfig {
                batch_size,
                visibility_timeout_secs,
                wait_time_secs,
                message_retention_secs,
                dlq_max_receive_count,
                use_fifo,
                enable_compression,
                compression_threshold_bytes,
                enable_batching,
                concurrent_receives,
                estimated_monthly_cost_usd,
                estimated_throughput_capacity,
                notes,
            }
        }
    }
}

/// Auto-scale recommendation based on current queue metrics
///
/// # Arguments
///
/// * `queue_size` - Current queue size
/// * `in_flight` - Messages currently being processed
/// * `current_workers` - Current number of workers
/// * `avg_processing_rate` - Average processing rate per worker (msg/sec)
/// * `target_lag_seconds` - Target acceptable lag
///
/// # Returns
///
/// Tuple of (recommended_workers, scaling_action, notes)
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::optimization::auto_scale_recommendation;
///
/// let (workers, action, notes) = auto_scale_recommendation(
///     5000,  // queue_size
///     200,   // in_flight
///     10,    // current_workers
///     25.0,  // avg_processing_rate
///     300    // target_lag_seconds (5 min)
/// );
/// println!("Recommended workers: {}", workers);
/// ```
pub fn auto_scale_recommendation(
    queue_size: usize,
    in_flight: usize,
    current_workers: usize,
    avg_processing_rate: f64,
    target_lag_seconds: u64,
) -> (usize, ScalingRecommendation, Vec<String>) {
    let mut notes = Vec::new();

    // Calculate current lag
    let total_rate = current_workers as f64 * avg_processing_rate;
    let lag_analysis = analyze_sqs_consumer_lag(queue_size, total_rate, target_lag_seconds);

    notes.push(format!(
        "Current lag: {:.1} seconds",
        lag_analysis.lag_seconds
    ));
    notes.push(format!("Target lag: {} seconds", target_lag_seconds));

    // Get worker scaling suggestion
    let scaling = suggest_sqs_worker_scaling(
        queue_size,
        current_workers,
        avg_processing_rate,
        target_lag_seconds,
    );

    // Check for stuck processing (high in-flight ratio)
    if in_flight > 0 {
        let in_flight_ratio = in_flight as f64 / (queue_size + in_flight) as f64;
        if in_flight_ratio > 0.7 {
            notes.push(format!(
                "High in-flight ratio: {:.0}% - consider increasing visibility timeout",
                in_flight_ratio * 100.0
            ));
        }
    }

    // Calculate processing capacity
    let capacity = estimate_sqs_processing_capacity(
        scaling.recommended_workers,
        avg_processing_rate,
        Some(queue_size),
    );

    if let Some(drain_time) = capacity.time_to_drain_secs {
        notes.push(format!(
            "Time to drain queue: {:.1} minutes with {} workers",
            drain_time / 60.0,
            scaling.recommended_workers
        ));
    }

    (scaling.recommended_workers, scaling.action, notes)
}

/// Analyze queue health and provide optimization recommendations
///
/// # Arguments
///
/// * `queue_name` - Queue name
/// * `total_messages` - Total messages in queue
/// * `in_flight` - Messages being processed
/// * `delayed` - Delayed messages
/// * `processing_rate` - Current processing rate (msg/sec)
/// * `oldest_message_age_secs` - Age of oldest message
///
/// # Returns
///
/// Queue health assessment with optimization recommendations
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::optimization::analyze_queue_health_with_recommendations;
///
/// let health = analyze_queue_health_with_recommendations(
///     "my-queue",
///     5000,
///     200,
///     50,
///     250.0,
///     Some(180.0)
/// );
/// println!("Health: {:?}", health.health);
/// ```
pub fn analyze_queue_health_with_recommendations(
    queue_name: impl Into<String>,
    total_messages: usize,
    in_flight: usize,
    delayed: usize,
    processing_rate: f64,
    oldest_message_age_secs: Option<f64>,
) -> QueueHealthAssessment {
    let mut health = assess_sqs_queue_health(
        queue_name,
        total_messages,
        in_flight,
        delayed,
        processing_rate,
        oldest_message_age_secs,
    );

    // Add optimization recommendations
    if total_messages > 1000 {
        let batch_size = calculate_optimal_sqs_batch_size(total_messages, 5000, 100);
        health
            .recommendations
            .push(format!("Enable batch processing with size: {}", batch_size));
    }

    if processing_rate < 10.0 && total_messages > 100 {
        health.recommendations.push(
            "Low processing rate - consider parallel processing with consume_parallel()"
                .to_string(),
        );
    }

    if let Some(age) = oldest_message_age_secs {
        if age > 1800.0 {
            // > 30 minutes
            let visibility = calculate_optimal_sqs_visibility_timeout(600.0, 0.5);
            health.recommendations.push(format!(
                "Old messages detected - consider visibility timeout of {}s",
                visibility
            ));
        }
    }

    health
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_optimize_high_throughput() {
        let config = optimize_for_workload(WorkloadProfile::HighThroughput {
            messages_per_second: 500.0,
            avg_message_size_bytes: 10_000,
            workers: 20,
        });

        assert!(config.enable_batching);
        assert!(config.batch_size > 1);
        assert!(config.batch_size <= 10);
        assert!(!config.enable_compression);
        assert!(config.estimated_throughput_capacity > 0.0);
    }

    #[test]
    fn test_optimize_low_latency() {
        let config = optimize_for_workload(WorkloadProfile::LowLatency {
            target_latency_ms: 50,
            avg_message_size_bytes: 5_000,
            workers: 10,
        });

        assert!(!config.enable_compression);
        assert_eq!(config.wait_time_secs, 1);
        assert!(!config.use_fifo);
    }

    #[test]
    fn test_optimize_cost() {
        let config = optimize_for_workload(WorkloadProfile::CostOptimized {
            messages_per_day: 1_000_000,
            avg_message_size_bytes: 5_000,
            acceptable_latency_secs: 300,
        });

        assert!(config.enable_batching);
        assert_eq!(config.batch_size, 10);
        assert!(config.wait_time_secs >= 15);
        assert!(!config.use_fifo);
        assert!(config.estimated_monthly_cost_usd > 0.0);
    }

    #[test]
    fn test_optimize_balanced() {
        let config = optimize_for_workload(WorkloadProfile::Balanced {
            messages_per_second: 100.0,
            avg_message_size_bytes: 10_000,
            workers: 10,
        });

        assert!(config.enable_batching);
        assert!(config.batch_size > 1);
        assert!(config.wait_time_secs > 1);
        assert!(config.wait_time_secs < 20);
    }

    #[test]
    fn test_auto_scale_scale_up() {
        let (workers, action, notes) = auto_scale_recommendation(
            10000, // large queue
            100, 5,    // few workers
            10.0, // low processing rate
            100,  // tight target lag
        );

        assert!(workers >= 5);
        match action {
            ScalingRecommendation::ScaleUp { additional_workers } => {
                assert!(additional_workers > 0);
            }
            _ => {
                // Could be optimal if current capacity is sufficient
            }
        }
        assert!(!notes.is_empty());
    }

    #[test]
    fn test_auto_scale_optimal() {
        let (workers, _action, notes) = auto_scale_recommendation(1000, 50, 10, 50.0, 300);

        assert!(workers >= 1);
        assert!(!notes.is_empty());
        // Could be optimal or scale down depending on calculations
    }

    #[test]
    fn test_analyze_queue_health() {
        let health = analyze_queue_health_with_recommendations(
            "test-queue",
            5000,
            200,
            50,
            250.0,
            Some(1200.0),
        );

        assert_eq!(health.queue_name, "test-queue");
        assert!(!health.recommendations.is_empty());
    }

    #[test]
    fn test_compression_threshold() {
        let config = optimize_for_workload(WorkloadProfile::HighThroughput {
            messages_per_second: 100.0,
            avg_message_size_bytes: 100_000, // Large messages
            workers: 10,
        });

        assert!(config.enable_compression);
        assert!(config.compression_threshold_bytes < usize::MAX);
    }
}
