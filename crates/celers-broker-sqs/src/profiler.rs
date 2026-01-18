// Copyright (c) 2026 COOLJAPAN OU (Team KitaSan)
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Performance profiling utilities for SQS operations
//!
//! This module provides detailed latency tracking and performance
//! analysis for SQS broker operations.
//!
//! # Features
//!
//! - Operation latency tracking (publish, consume, ack, etc.)
//! - Percentile calculations (P50, P95, P99)
//! - Throughput measurement
//! - Bottleneck detection
//! - Performance regression detection
//!
//! # Example
//!
//! ```rust
//! use celers_broker_sqs::profiler::PerformanceProfiler;
//! use std::time::Duration;
//!
//! let mut profiler = PerformanceProfiler::new();
//!
//! // Record operation latencies
//! profiler.record_publish(Duration::from_millis(25));
//! profiler.record_consume(Duration::from_millis(150));
//! profiler.record_ack(Duration::from_millis(30));
//!
//! // Get performance summary
//! let summary = profiler.summary();
//! println!("Publish P95: {:?}", summary.publish.p95_latency);
//! println!("Consume P95: {:?}", summary.consume.p95_latency);
//! ```

use std::collections::VecDeque;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Maximum number of samples to keep per operation type
const MAX_SAMPLES: usize = 10_000;

/// Operation type for profiling
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperationType {
    /// Publish operation
    Publish,
    /// Consume operation
    Consume,
    /// Acknowledge operation
    Ack,
    /// Reject operation
    Reject,
    /// Batch publish operation
    BatchPublish,
    /// Batch consume operation
    BatchConsume,
    /// Batch acknowledge operation
    BatchAck,
    /// Queue size check
    QueueSize,
    /// Custom operation
    Custom(u8),
}

impl OperationType {
    /// Get operation name as string
    pub fn name(&self) -> &str {
        match self {
            OperationType::Publish => "publish",
            OperationType::Consume => "consume",
            OperationType::Ack => "ack",
            OperationType::Reject => "reject",
            OperationType::BatchPublish => "batch_publish",
            OperationType::BatchConsume => "batch_consume",
            OperationType::BatchAck => "batch_ack",
            OperationType::QueueSize => "queue_size",
            OperationType::Custom(_) => "custom",
        }
    }
}

/// Latency sample
#[derive(Debug, Clone)]
struct LatencySample {
    #[allow(dead_code)]
    timestamp: u64,
    duration_micros: u64,
}

/// Operation statistics
#[derive(Debug, Clone)]
pub struct OperationStats {
    /// Total number of operations
    pub count: u64,
    /// Total duration in microseconds
    pub total_duration_micros: u64,
    /// Minimum latency
    pub min_latency: Duration,
    /// Maximum latency
    pub max_latency: Duration,
    /// Mean latency
    pub mean_latency: Duration,
    /// P50 (median) latency
    pub p50_latency: Duration,
    /// P95 latency
    pub p95_latency: Duration,
    /// P99 latency
    pub p99_latency: Duration,
    /// Throughput (operations per second)
    pub throughput: f64,
}

impl Default for OperationStats {
    fn default() -> Self {
        Self {
            count: 0,
            total_duration_micros: 0,
            min_latency: Duration::from_secs(0),
            max_latency: Duration::from_secs(0),
            mean_latency: Duration::from_secs(0),
            p50_latency: Duration::from_secs(0),
            p95_latency: Duration::from_secs(0),
            p99_latency: Duration::from_secs(0),
            throughput: 0.0,
        }
    }
}

/// Performance profiler for SQS operations
#[derive(Debug)]
pub struct PerformanceProfiler {
    publish_samples: VecDeque<LatencySample>,
    consume_samples: VecDeque<LatencySample>,
    ack_samples: VecDeque<LatencySample>,
    reject_samples: VecDeque<LatencySample>,
    batch_publish_samples: VecDeque<LatencySample>,
    batch_consume_samples: VecDeque<LatencySample>,
    batch_ack_samples: VecDeque<LatencySample>,
    queue_size_samples: VecDeque<LatencySample>,
    start_time: u64,
}

impl Default for PerformanceProfiler {
    fn default() -> Self {
        Self::new()
    }
}

impl PerformanceProfiler {
    /// Create a new performance profiler
    pub fn new() -> Self {
        let start_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            publish_samples: VecDeque::with_capacity(MAX_SAMPLES),
            consume_samples: VecDeque::with_capacity(MAX_SAMPLES),
            ack_samples: VecDeque::with_capacity(MAX_SAMPLES),
            reject_samples: VecDeque::with_capacity(MAX_SAMPLES),
            batch_publish_samples: VecDeque::with_capacity(MAX_SAMPLES),
            batch_consume_samples: VecDeque::with_capacity(MAX_SAMPLES),
            batch_ack_samples: VecDeque::with_capacity(MAX_SAMPLES),
            queue_size_samples: VecDeque::with_capacity(MAX_SAMPLES),
            start_time,
        }
    }

    /// Record publish operation latency
    pub fn record_publish(&mut self, latency: Duration) {
        Self::record_sample(&mut self.publish_samples, latency);
    }

    /// Record consume operation latency
    pub fn record_consume(&mut self, latency: Duration) {
        Self::record_sample(&mut self.consume_samples, latency);
    }

    /// Record acknowledge operation latency
    pub fn record_ack(&mut self, latency: Duration) {
        Self::record_sample(&mut self.ack_samples, latency);
    }

    /// Record reject operation latency
    pub fn record_reject(&mut self, latency: Duration) {
        Self::record_sample(&mut self.reject_samples, latency);
    }

    /// Record batch publish operation latency
    pub fn record_batch_publish(&mut self, latency: Duration) {
        Self::record_sample(&mut self.batch_publish_samples, latency);
    }

    /// Record batch consume operation latency
    pub fn record_batch_consume(&mut self, latency: Duration) {
        Self::record_sample(&mut self.batch_consume_samples, latency);
    }

    /// Record batch acknowledge operation latency
    pub fn record_batch_ack(&mut self, latency: Duration) {
        Self::record_sample(&mut self.batch_ack_samples, latency);
    }

    /// Record queue size check operation latency
    pub fn record_queue_size(&mut self, latency: Duration) {
        Self::record_sample(&mut self.queue_size_samples, latency);
    }

    /// Record a sample in the appropriate queue
    fn record_sample(samples: &mut VecDeque<LatencySample>, latency: Duration) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let sample = LatencySample {
            timestamp,
            duration_micros: latency.as_micros() as u64,
        };

        samples.push_back(sample);

        // Keep only last MAX_SAMPLES
        if samples.len() > MAX_SAMPLES {
            samples.pop_front();
        }
    }

    /// Calculate statistics for a sample set
    fn calculate_stats(&self, samples: &VecDeque<LatencySample>) -> OperationStats {
        if samples.is_empty() {
            return OperationStats::default();
        }

        let mut durations: Vec<u64> = samples.iter().map(|s| s.duration_micros).collect();
        durations.sort_unstable();

        let count = durations.len() as u64;
        let total: u64 = durations.iter().sum();
        let min = durations[0];
        let max = durations[durations.len() - 1];
        let mean = total / count;

        let p50_idx = (durations.len() as f64 * 0.50) as usize;
        let p95_idx = (durations.len() as f64 * 0.95) as usize;
        let p99_idx = (durations.len() as f64 * 0.99) as usize;

        let p50 = durations[p50_idx.min(durations.len() - 1)];
        let p95 = durations[p95_idx.min(durations.len() - 1)];
        let p99 = durations[p99_idx.min(durations.len() - 1)];

        // Calculate throughput (ops per second)
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let elapsed = now.saturating_sub(self.start_time).max(1);
        let throughput = count as f64 / elapsed as f64;

        OperationStats {
            count,
            total_duration_micros: total,
            min_latency: Duration::from_micros(min),
            max_latency: Duration::from_micros(max),
            mean_latency: Duration::from_micros(mean),
            p50_latency: Duration::from_micros(p50),
            p95_latency: Duration::from_micros(p95),
            p99_latency: Duration::from_micros(p99),
            throughput,
        }
    }

    /// Get publish operation statistics
    pub fn publish_stats(&self) -> OperationStats {
        self.calculate_stats(&self.publish_samples)
    }

    /// Get consume operation statistics
    pub fn consume_stats(&self) -> OperationStats {
        self.calculate_stats(&self.consume_samples)
    }

    /// Get acknowledge operation statistics
    pub fn ack_stats(&self) -> OperationStats {
        self.calculate_stats(&self.ack_samples)
    }

    /// Get reject operation statistics
    pub fn reject_stats(&self) -> OperationStats {
        self.calculate_stats(&self.reject_samples)
    }

    /// Get batch publish operation statistics
    pub fn batch_publish_stats(&self) -> OperationStats {
        self.calculate_stats(&self.batch_publish_samples)
    }

    /// Get batch consume operation statistics
    pub fn batch_consume_stats(&self) -> OperationStats {
        self.calculate_stats(&self.batch_consume_samples)
    }

    /// Get batch acknowledge operation statistics
    pub fn batch_ack_stats(&self) -> OperationStats {
        self.calculate_stats(&self.batch_ack_samples)
    }

    /// Get queue size operation statistics
    pub fn queue_size_stats(&self) -> OperationStats {
        self.calculate_stats(&self.queue_size_samples)
    }

    /// Get performance summary
    pub fn summary(&self) -> PerformanceSummary {
        PerformanceSummary {
            publish: self.publish_stats(),
            consume: self.consume_stats(),
            ack: self.ack_stats(),
            reject: self.reject_stats(),
            batch_publish: self.batch_publish_stats(),
            batch_consume: self.batch_consume_stats(),
            batch_ack: self.batch_ack_stats(),
            queue_size: self.queue_size_stats(),
        }
    }

    /// Reset all statistics
    pub fn reset(&mut self) {
        self.publish_samples.clear();
        self.consume_samples.clear();
        self.ack_samples.clear();
        self.reject_samples.clear();
        self.batch_publish_samples.clear();
        self.batch_consume_samples.clear();
        self.batch_ack_samples.clear();
        self.queue_size_samples.clear();
        self.start_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    /// Detect bottlenecks (operations with P95 > threshold)
    ///
    /// Returns list of slow operations.
    pub fn detect_bottlenecks(&self, threshold_ms: u64) -> Vec<String> {
        let threshold = Duration::from_millis(threshold_ms);
        let mut bottlenecks = Vec::new();

        let summary = self.summary();

        if summary.publish.count > 0 && summary.publish.p95_latency > threshold {
            bottlenecks.push(format!("publish (P95: {:?})", summary.publish.p95_latency));
        }
        if summary.consume.count > 0 && summary.consume.p95_latency > threshold {
            bottlenecks.push(format!("consume (P95: {:?})", summary.consume.p95_latency));
        }
        if summary.ack.count > 0 && summary.ack.p95_latency > threshold {
            bottlenecks.push(format!("ack (P95: {:?})", summary.ack.p95_latency));
        }

        bottlenecks
    }
}

/// Performance summary across all operation types
#[derive(Debug, Clone)]
pub struct PerformanceSummary {
    /// Publish operation statistics
    pub publish: OperationStats,
    /// Consume operation statistics
    pub consume: OperationStats,
    /// Acknowledge operation statistics
    pub ack: OperationStats,
    /// Reject operation statistics
    pub reject: OperationStats,
    /// Batch publish operation statistics
    pub batch_publish: OperationStats,
    /// Batch consume operation statistics
    pub batch_consume: OperationStats,
    /// Batch acknowledge operation statistics
    pub batch_ack: OperationStats,
    /// Queue size operation statistics
    pub queue_size: OperationStats,
}

impl PerformanceSummary {
    /// Format summary as a readable string
    pub fn format(&self) -> String {
        let mut output = String::new();

        output.push_str("Performance Summary:\n");
        output.push_str("===================\n\n");

        if self.publish.count > 0 {
            output.push_str(&format!(
                "Publish: {} ops, P50={:?}, P95={:?}, P99={:?}, throughput={:.2} ops/s\n",
                self.publish.count,
                self.publish.p50_latency,
                self.publish.p95_latency,
                self.publish.p99_latency,
                self.publish.throughput
            ));
        }

        if self.consume.count > 0 {
            output.push_str(&format!(
                "Consume: {} ops, P50={:?}, P95={:?}, P99={:?}, throughput={:.2} ops/s\n",
                self.consume.count,
                self.consume.p50_latency,
                self.consume.p95_latency,
                self.consume.p99_latency,
                self.consume.throughput
            ));
        }

        if self.ack.count > 0 {
            output.push_str(&format!(
                "Ack: {} ops, P50={:?}, P95={:?}, P99={:?}, throughput={:.2} ops/s\n",
                self.ack.count,
                self.ack.p50_latency,
                self.ack.p95_latency,
                self.ack.p99_latency,
                self.ack.throughput
            ));
        }

        if self.batch_publish.count > 0 {
            output.push_str(&format!(
                "Batch Publish: {} ops, P50={:?}, P95={:?}, P99={:?}\n",
                self.batch_publish.count,
                self.batch_publish.p50_latency,
                self.batch_publish.p95_latency,
                self.batch_publish.p99_latency,
            ));
        }

        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_publish() {
        let mut profiler = PerformanceProfiler::new();

        profiler.record_publish(Duration::from_millis(10));
        profiler.record_publish(Duration::from_millis(20));
        profiler.record_publish(Duration::from_millis(30));

        let stats = profiler.publish_stats();
        assert_eq!(stats.count, 3);
        assert!(stats.mean_latency >= Duration::from_millis(10));
        assert!(stats.max_latency >= Duration::from_millis(30));
    }

    #[test]
    fn test_percentile_calculation() {
        let mut profiler = PerformanceProfiler::new();

        // Record 100 samples with increasing latency
        for i in 1..=100 {
            profiler.record_consume(Duration::from_millis(i));
        }

        let stats = profiler.consume_stats();
        assert_eq!(stats.count, 100);

        // P50 should be around 50ms
        assert!(stats.p50_latency >= Duration::from_millis(45));
        assert!(stats.p50_latency <= Duration::from_millis(55));

        // P95 should be around 95ms
        assert!(stats.p95_latency >= Duration::from_millis(90));
        assert!(stats.p95_latency <= Duration::from_millis(100));

        // P99 should be around 99ms
        assert!(stats.p99_latency >= Duration::from_millis(95));
        assert!(stats.p99_latency <= Duration::from_millis(100));
    }

    #[test]
    fn test_summary() {
        let mut profiler = PerformanceProfiler::new();

        profiler.record_publish(Duration::from_millis(10));
        profiler.record_consume(Duration::from_millis(50));
        profiler.record_ack(Duration::from_millis(5));

        let summary = profiler.summary();
        assert_eq!(summary.publish.count, 1);
        assert_eq!(summary.consume.count, 1);
        assert_eq!(summary.ack.count, 1);
        assert_eq!(summary.reject.count, 0);
    }

    #[test]
    fn test_reset() {
        let mut profiler = PerformanceProfiler::new();

        profiler.record_publish(Duration::from_millis(10));
        profiler.record_consume(Duration::from_millis(50));

        assert_eq!(profiler.publish_stats().count, 1);
        assert_eq!(profiler.consume_stats().count, 1);

        profiler.reset();

        assert_eq!(profiler.publish_stats().count, 0);
        assert_eq!(profiler.consume_stats().count, 0);
    }

    #[test]
    fn test_detect_bottlenecks() {
        let mut profiler = PerformanceProfiler::new();

        // Record fast publish operations
        for _ in 0..10 {
            profiler.record_publish(Duration::from_millis(5));
        }

        // Record slow consume operations
        for _ in 0..10 {
            profiler.record_consume(Duration::from_millis(200));
        }

        let bottlenecks = profiler.detect_bottlenecks(100);
        assert_eq!(bottlenecks.len(), 1);
        assert!(bottlenecks[0].contains("consume"));
    }

    #[test]
    fn test_batch_operations() {
        let mut profiler = PerformanceProfiler::new();

        profiler.record_batch_publish(Duration::from_millis(25));
        profiler.record_batch_consume(Duration::from_millis(150));
        profiler.record_batch_ack(Duration::from_millis(30));

        let summary = profiler.summary();
        assert_eq!(summary.batch_publish.count, 1);
        assert_eq!(summary.batch_consume.count, 1);
        assert_eq!(summary.batch_ack.count, 1);
    }

    #[test]
    fn test_operation_type_name() {
        assert_eq!(OperationType::Publish.name(), "publish");
        assert_eq!(OperationType::Consume.name(), "consume");
        assert_eq!(OperationType::BatchPublish.name(), "batch_publish");
    }

    #[test]
    fn test_max_samples() {
        let mut profiler = PerformanceProfiler::new();

        // Record more than MAX_SAMPLES
        for _ in 0..(MAX_SAMPLES + 100) {
            profiler.record_publish(Duration::from_millis(10));
        }

        // Should keep only MAX_SAMPLES
        assert_eq!(profiler.publish_samples.len(), MAX_SAMPLES);
    }

    #[test]
    fn test_empty_stats() {
        let profiler = PerformanceProfiler::new();
        let stats = profiler.publish_stats();

        assert_eq!(stats.count, 0);
        assert_eq!(stats.throughput, 0.0);
    }

    #[test]
    fn test_throughput_calculation() {
        let mut profiler = PerformanceProfiler::new();

        // Record 100 operations
        for _ in 0..100 {
            profiler.record_publish(Duration::from_millis(10));
        }

        let stats = profiler.publish_stats();
        assert!(stats.throughput > 0.0);
    }

    #[test]
    fn test_format_summary() {
        let mut profiler = PerformanceProfiler::new();

        profiler.record_publish(Duration::from_millis(10));
        profiler.record_consume(Duration::from_millis(50));

        let summary = profiler.summary();
        let formatted = summary.format();

        assert!(formatted.contains("Performance Summary"));
        assert!(formatted.contains("Publish"));
        assert!(formatted.contains("Consume"));
    }
}
