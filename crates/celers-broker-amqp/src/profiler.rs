//! Performance Profiler for AMQP Operations
//!
//! Provides detailed performance profiling and analysis for AMQP broker operations.
//!
//! # Features
//!
//! - **Operation Timing**: Track latencies for publish, consume, and acknowledgment operations
//! - **Percentile Analysis**: Calculate p50, p95, p99 latencies
//! - **Throughput Tracking**: Monitor messages per second
//! - **Resource Utilization**: Track connection and channel usage
//! - **Bottleneck Detection**: Identify performance bottlenecks
//! - **Historical Trends**: Analyze performance over time
//!
//! # Example
//!
//! ```rust
//! use celers_broker_amqp::profiler::{AmqpProfiler, OperationType};
//! use std::time::Instant;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let profiler = AmqpProfiler::new();
//!
//! // Profile a publish operation
//! let start = Instant::now();
//! // ... perform publish ...
//! profiler.record_operation(
//!     OperationType::Publish,
//!     start.elapsed(),
//!     1024,  // payload size
//!     true,  // success
//! ).await;
//!
//! // Get performance report
//! let report = profiler.generate_report().await;
//! println!("Total operations: {}", report.total_operations);
//! println!("Avg latency: {:?}", report.avg_latency);
//! println!("p95 latency: {:?}", report.p95_latency);
//! # Ok(())
//! # }
//! ```

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Type of AMQP operation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OperationType {
    /// Message publish operation
    Publish,
    /// Message consume operation
    Consume,
    /// Message acknowledgment
    Acknowledge,
    /// Message rejection
    Reject,
    /// Queue declaration
    QueueDeclare,
    /// Exchange declaration
    ExchangeDeclare,
    /// Queue binding
    Binding,
    /// Transaction commit
    TransactionCommit,
    /// Transaction rollback
    TransactionRollback,
}

impl OperationType {
    /// Get a human-readable name
    pub fn name(&self) -> &'static str {
        match self {
            OperationType::Publish => "Publish",
            OperationType::Consume => "Consume",
            OperationType::Acknowledge => "Acknowledge",
            OperationType::Reject => "Reject",
            OperationType::QueueDeclare => "Queue Declare",
            OperationType::ExchangeDeclare => "Exchange Declare",
            OperationType::Binding => "Binding",
            OperationType::TransactionCommit => "Transaction Commit",
            OperationType::TransactionRollback => "Transaction Rollback",
        }
    }
}

/// Record of a single operation
#[derive(Debug, Clone)]
struct OperationRecord {
    /// Type of operation
    operation_type: OperationType,
    /// Duration of the operation
    duration: Duration,
    /// Payload size in bytes (if applicable)
    payload_size: Option<usize>,
    /// Whether the operation succeeded
    success: bool,
    /// Timestamp when the operation occurred
    timestamp: Instant,
}

/// Statistics for a specific operation type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationStats {
    /// Operation type
    pub operation_type: OperationType,
    /// Total count of operations
    pub total_operations: u64,
    /// Count of successful operations
    pub successful_operations: u64,
    /// Count of failed operations
    pub failed_operations: u64,
    /// Success rate (0.0 to 1.0)
    pub success_rate: f64,
    /// Average latency
    pub avg_latency: Duration,
    /// Minimum latency
    pub min_latency: Duration,
    /// Maximum latency
    pub max_latency: Duration,
    /// 50th percentile latency (median)
    pub p50_latency: Duration,
    /// 95th percentile latency
    pub p95_latency: Duration,
    /// 99th percentile latency
    pub p99_latency: Duration,
    /// Throughput (operations per second)
    pub throughput: f64,
    /// Total payload bytes processed
    pub total_payload_bytes: u64,
    /// Average payload size
    pub avg_payload_size: f64,
}

/// Comprehensive performance report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceReport {
    /// Total operations across all types
    pub total_operations: u64,
    /// Average latency across all operations
    pub avg_latency: Duration,
    /// p95 latency across all operations
    pub p95_latency: Duration,
    /// Overall throughput (operations per second)
    pub overall_throughput: f64,
    /// Stats broken down by operation type
    pub operation_stats: HashMap<OperationType, OperationStats>,
    /// Slowest operation type
    pub slowest_operation: Option<OperationType>,
    /// Fastest operation type
    pub fastest_operation: Option<OperationType>,
    /// Operation with highest failure rate
    pub most_error_prone: Option<OperationType>,
    /// Total payload bytes processed
    pub total_payload_bytes: u64,
    /// Recommendations for improvement
    pub recommendations: Vec<String>,
}

/// Configuration for the profiler
#[derive(Debug, Clone)]
pub struct ProfilerConfig {
    /// Maximum number of records to keep in memory
    pub max_records: usize,
    /// Whether to enable detailed profiling (higher memory usage)
    pub detailed_profiling: bool,
}

impl Default for ProfilerConfig {
    fn default() -> Self {
        Self {
            max_records: 10_000,
            detailed_profiling: true,
        }
    }
}

/// Performance profiler for AMQP operations
pub struct AmqpProfiler {
    config: ProfilerConfig,
    records: Arc<RwLock<VecDeque<OperationRecord>>>,
}

impl AmqpProfiler {
    /// Create a new profiler with default configuration
    pub fn new() -> Self {
        Self::with_config(ProfilerConfig::default())
    }

    /// Create a new profiler with custom configuration
    pub fn with_config(config: ProfilerConfig) -> Self {
        Self {
            config,
            records: Arc::new(RwLock::new(VecDeque::new())),
        }
    }

    /// Record an operation
    ///
    /// # Arguments
    ///
    /// * `operation_type` - Type of operation
    /// * `duration` - How long the operation took
    /// * `payload_size` - Size of the payload (if applicable)
    /// * `success` - Whether the operation succeeded
    pub async fn record_operation(
        &self,
        operation_type: OperationType,
        duration: Duration,
        payload_size: usize,
        success: bool,
    ) {
        if !self.config.detailed_profiling {
            return;
        }

        let mut records = self.records.write().await;

        records.push_back(OperationRecord {
            operation_type,
            duration,
            payload_size: Some(payload_size),
            success,
            timestamp: Instant::now(),
        });

        // Evict oldest records if capacity exceeded
        while records.len() > self.config.max_records {
            records.pop_front();
        }
    }

    /// Record an operation without payload size
    pub async fn record_operation_simple(
        &self,
        operation_type: OperationType,
        duration: Duration,
        success: bool,
    ) {
        if !self.config.detailed_profiling {
            return;
        }

        let mut records = self.records.write().await;

        records.push_back(OperationRecord {
            operation_type,
            duration,
            payload_size: None,
            success,
            timestamp: Instant::now(),
        });

        // Evict oldest records if capacity exceeded
        while records.len() > self.config.max_records {
            records.pop_front();
        }
    }

    /// Generate a comprehensive performance report
    pub async fn generate_report(&self) -> PerformanceReport {
        let records = self.records.read().await;

        if records.is_empty() {
            return PerformanceReport {
                total_operations: 0,
                avg_latency: Duration::from_secs(0),
                p95_latency: Duration::from_secs(0),
                overall_throughput: 0.0,
                operation_stats: HashMap::new(),
                slowest_operation: None,
                fastest_operation: None,
                most_error_prone: None,
                total_payload_bytes: 0,
                recommendations: Vec::new(),
            };
        }

        // Group by operation type
        let mut grouped: HashMap<OperationType, Vec<&OperationRecord>> = HashMap::new();
        for record in records.iter() {
            grouped
                .entry(record.operation_type)
                .or_default()
                .push(record);
        }

        // Calculate stats for each operation type
        let mut operation_stats = HashMap::new();
        for (op_type, ops) in grouped.iter() {
            let stats = Self::calculate_stats(*op_type, ops);
            operation_stats.insert(*op_type, stats);
        }

        // Overall statistics
        let total_operations = records.len() as u64;

        let total_duration: Duration = records.iter().map(|r| r.duration).sum();
        let avg_latency = total_duration / records.len() as u32;

        let mut all_durations: Vec<Duration> = records.iter().map(|r| r.duration).collect();
        all_durations.sort();
        let p95_idx = (all_durations.len() as f64 * 0.95) as usize;
        let p95_latency = all_durations.get(p95_idx).copied().unwrap_or_default();

        // Calculate throughput
        let time_span = if let Some(first) = records.front() {
            if let Some(last) = records.back() {
                last.timestamp.duration_since(first.timestamp)
            } else {
                Duration::from_secs(1)
            }
        } else {
            Duration::from_secs(1)
        };

        let overall_throughput = if time_span.as_secs_f64() > 0.0 {
            total_operations as f64 / time_span.as_secs_f64()
        } else {
            0.0
        };

        // Find slowest and fastest operations
        let slowest_operation = operation_stats
            .iter()
            .max_by_key(|(_, stats)| stats.avg_latency)
            .map(|(op, _)| *op);

        let fastest_operation = operation_stats
            .iter()
            .min_by_key(|(_, stats)| stats.avg_latency)
            .map(|(op, _)| *op);

        // Find most error-prone operation
        let most_error_prone = operation_stats
            .iter()
            .filter(|(_, stats)| stats.failed_operations > 0)
            .min_by(|(_, a), (_, b)| {
                a.success_rate
                    .partial_cmp(&b.success_rate)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(op, _)| *op);

        let total_payload_bytes =
            records.iter().filter_map(|r| r.payload_size).sum::<usize>() as u64;

        // Generate recommendations
        let recommendations = Self::generate_recommendations(&operation_stats);

        PerformanceReport {
            total_operations,
            avg_latency,
            p95_latency,
            overall_throughput,
            operation_stats,
            slowest_operation,
            fastest_operation,
            most_error_prone,
            total_payload_bytes,
            recommendations,
        }
    }

    /// Clear all recorded data
    pub async fn clear(&self) {
        let mut records = self.records.write().await;
        records.clear();
    }

    /// Get the number of recorded operations
    pub async fn record_count(&self) -> usize {
        let records = self.records.read().await;
        records.len()
    }

    fn calculate_stats(op_type: OperationType, ops: &[&OperationRecord]) -> OperationStats {
        let total_operations = ops.len() as u64;
        let successful_operations = ops.iter().filter(|op| op.success).count() as u64;
        let failed_operations = total_operations - successful_operations;
        let success_rate = successful_operations as f64 / total_operations as f64;

        let total_duration: Duration = ops.iter().map(|op| op.duration).sum();
        let avg_latency = total_duration / ops.len() as u32;

        let min_latency = ops.iter().map(|op| op.duration).min().unwrap_or_default();
        let max_latency = ops.iter().map(|op| op.duration).max().unwrap_or_default();

        // Calculate percentiles
        let mut sorted_durations: Vec<Duration> = ops.iter().map(|op| op.duration).collect();
        sorted_durations.sort();

        let p50_idx = (sorted_durations.len() as f64 * 0.50) as usize;
        let p95_idx = (sorted_durations.len() as f64 * 0.95) as usize;
        let p99_idx = (sorted_durations.len() as f64 * 0.99) as usize;

        let p50_latency = sorted_durations.get(p50_idx).copied().unwrap_or_default();
        let p95_latency = sorted_durations.get(p95_idx).copied().unwrap_or_default();
        let p99_latency = sorted_durations.get(p99_idx).copied().unwrap_or_default();

        // Calculate throughput
        let time_span = if let Some(first) = ops.first() {
            if let Some(last) = ops.last() {
                last.timestamp.duration_since(first.timestamp)
            } else {
                Duration::from_secs(1)
            }
        } else {
            Duration::from_secs(1)
        };

        let throughput = if time_span.as_secs_f64() > 0.0 {
            total_operations as f64 / time_span.as_secs_f64()
        } else {
            0.0
        };

        let total_payload_bytes = ops.iter().filter_map(|op| op.payload_size).sum::<usize>() as u64;

        let avg_payload_size = if total_payload_bytes > 0 {
            total_payload_bytes as f64 / ops.len() as f64
        } else {
            0.0
        };

        OperationStats {
            operation_type: op_type,
            total_operations,
            successful_operations,
            failed_operations,
            success_rate,
            avg_latency,
            min_latency,
            max_latency,
            p50_latency,
            p95_latency,
            p99_latency,
            throughput,
            total_payload_bytes,
            avg_payload_size,
        }
    }

    fn generate_recommendations(stats: &HashMap<OperationType, OperationStats>) -> Vec<String> {
        let mut recommendations = Vec::new();

        for (op_type, stat) in stats.iter() {
            // High latency
            if stat.p95_latency > Duration::from_millis(500) {
                recommendations.push(format!(
                    "{} operations are slow (p95: {:?}). Consider optimizing or increasing resources.",
                    op_type.name(),
                    stat.p95_latency
                ));
            }

            // High failure rate
            if stat.success_rate < 0.9 {
                recommendations.push(format!(
                    "{} has low success rate ({:.1}%). Investigate error causes.",
                    op_type.name(),
                    stat.success_rate * 100.0
                ));
            }

            // Large payloads
            if stat.avg_payload_size > 1_000_000.0 {
                recommendations.push(format!(
                    "{} operations have large payloads (avg {:.1} KB). Consider compression.",
                    op_type.name(),
                    stat.avg_payload_size / 1024.0
                ));
            }
        }

        if recommendations.is_empty() {
            recommendations.push("Performance looks good! No issues detected.".to_string());
        }

        recommendations
    }
}

impl Default for AmqpProfiler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_profiler_creation() {
        let profiler = AmqpProfiler::new();
        assert_eq!(profiler.record_count().await, 0);
    }

    #[tokio::test]
    async fn test_record_operation() {
        let profiler = AmqpProfiler::new();

        profiler
            .record_operation(
                OperationType::Publish,
                Duration::from_millis(50),
                1024,
                true,
            )
            .await;

        assert_eq!(profiler.record_count().await, 1);
    }

    #[tokio::test]
    async fn test_generate_report() {
        let profiler = AmqpProfiler::new();

        for _ in 0..10 {
            profiler
                .record_operation(
                    OperationType::Publish,
                    Duration::from_millis(50),
                    1024,
                    true,
                )
                .await;
        }

        let report = profiler.generate_report().await;
        assert_eq!(report.total_operations, 10);
        assert!(report.avg_latency > Duration::from_secs(0));
    }

    #[tokio::test]
    async fn test_operation_type_names() {
        assert_eq!(OperationType::Publish.name(), "Publish");
        assert_eq!(OperationType::Consume.name(), "Consume");
        assert_eq!(OperationType::Acknowledge.name(), "Acknowledge");
    }

    #[tokio::test]
    async fn test_clear() {
        let profiler = AmqpProfiler::new();

        profiler
            .record_operation(
                OperationType::Publish,
                Duration::from_millis(50),
                1024,
                true,
            )
            .await;

        assert_eq!(profiler.record_count().await, 1);
        profiler.clear().await;
        assert_eq!(profiler.record_count().await, 0);
    }
}
