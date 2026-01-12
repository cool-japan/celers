//! Performance optimization strategies for AMQP
//!
//! Provides advanced optimization techniques for AMQP broker performance,
//! including connection tuning, prefetch optimization, and resource management.
//!
//! # Features
//!
//! - **Connection Tuning** - Optimize connection parameters
//! - **Prefetch Optimization** - Dynamic prefetch adjustment
//! - **Resource Pool Sizing** - Calculate optimal pool sizes
//! - **Batch Size Tuning** - Optimize batch operations
//! - **Memory Management** - Prevent memory issues
//! - **Performance Profiling** - Track and analyze performance
//!
//! # Example
//!
//! ```rust
//! use celers_broker_amqp::optimization::{PerformanceOptimizer, OptimizationConfig};
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = OptimizationConfig::default();
//! let mut optimizer = PerformanceOptimizer::new(config);
//!
//! // Record performance metrics
//! optimizer.record_latency(50.0);
//! optimizer.record_throughput(1000.0);
//! optimizer.record_memory_usage(100_000_000);
//!
//! // Get optimization recommendations
//! let recommendations = optimizer.get_recommendations();
//! for rec in recommendations {
//!     println!("{}: {}", rec.category, rec.recommendation);
//! }
//! # Ok(())
//! # }
//! ```

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::time::Duration;

/// Optimization configuration
#[derive(Debug, Clone)]
pub struct OptimizationConfig {
    /// Target latency (milliseconds)
    pub target_latency_ms: f64,
    /// Target throughput (messages/second)
    pub target_throughput: f64,
    /// Maximum memory usage (bytes)
    pub max_memory_bytes: u64,
    /// Sample window size for averaging
    pub sample_window: usize,
    /// Enable automatic tuning
    pub auto_tune: bool,
}

impl Default for OptimizationConfig {
    fn default() -> Self {
        Self {
            target_latency_ms: 100.0,
            target_throughput: 1000.0,
            max_memory_bytes: 1024 * 1024 * 1024, // 1GB
            sample_window: 100,
            auto_tune: true,
        }
    }
}

/// Optimization recommendation category
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OptimizationCategory {
    /// Connection-related optimization
    Connection,
    /// Channel-related optimization
    Channel,
    /// Prefetch/QoS optimization
    Prefetch,
    /// Batch operation optimization
    Batch,
    /// Memory optimization
    Memory,
    /// Throughput optimization
    Throughput,
    /// Latency optimization
    Latency,
}

impl std::fmt::Display for OptimizationCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connection => write!(f, "Connection"),
            Self::Channel => write!(f, "Channel"),
            Self::Prefetch => write!(f, "Prefetch"),
            Self::Batch => write!(f, "Batch"),
            Self::Memory => write!(f, "Memory"),
            Self::Throughput => write!(f, "Throughput"),
            Self::Latency => write!(f, "Latency"),
        }
    }
}

/// Optimization recommendation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizationRecommendation {
    /// Category of recommendation
    pub category: OptimizationCategory,
    /// Recommendation text
    pub recommendation: String,
    /// Current value
    pub current_value: f64,
    /// Recommended value
    pub recommended_value: f64,
    /// Expected improvement percentage
    pub expected_improvement: f64,
}

/// Performance metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    /// Average latency (milliseconds)
    pub avg_latency_ms: f64,
    /// p95 latency (milliseconds)
    pub p95_latency_ms: f64,
    /// p99 latency (milliseconds)
    pub p99_latency_ms: f64,
    /// Average throughput (messages/second)
    pub avg_throughput: f64,
    /// Peak throughput (messages/second)
    pub peak_throughput: f64,
    /// Average memory usage (bytes)
    pub avg_memory_bytes: u64,
    /// Peak memory usage (bytes)
    pub peak_memory_bytes: u64,
    /// CPU utilization (0.0 - 1.0)
    pub cpu_utilization: f64,
}

/// Performance optimizer
#[derive(Debug)]
pub struct PerformanceOptimizer {
    config: OptimizationConfig,
    latency_samples: VecDeque<f64>,
    throughput_samples: VecDeque<f64>,
    memory_samples: VecDeque<u64>,
    metrics: PerformanceMetrics,
}

impl PerformanceOptimizer {
    /// Create a new performance optimizer
    pub fn new(config: OptimizationConfig) -> Self {
        Self {
            config,
            latency_samples: VecDeque::new(),
            throughput_samples: VecDeque::new(),
            memory_samples: VecDeque::new(),
            metrics: PerformanceMetrics::default(),
        }
    }

    /// Record a latency sample (milliseconds)
    pub fn record_latency(&mut self, latency_ms: f64) {
        self.latency_samples.push_back(latency_ms);
        if self.latency_samples.len() > self.config.sample_window {
            self.latency_samples.pop_front();
        }
        self.update_metrics();
    }

    /// Record a throughput sample (messages/second)
    pub fn record_throughput(&mut self, throughput: f64) {
        self.throughput_samples.push_back(throughput);
        if self.throughput_samples.len() > self.config.sample_window {
            self.throughput_samples.pop_front();
        }
        self.update_metrics();
    }

    /// Record memory usage (bytes)
    pub fn record_memory_usage(&mut self, bytes: u64) {
        self.memory_samples.push_back(bytes);
        if self.memory_samples.len() > self.config.sample_window {
            self.memory_samples.pop_front();
        }
        self.update_metrics();
    }

    /// Update calculated metrics
    fn update_metrics(&mut self) {
        // Calculate latency metrics
        if !self.latency_samples.is_empty() {
            self.metrics.avg_latency_ms =
                self.latency_samples.iter().sum::<f64>() / self.latency_samples.len() as f64;

            let mut sorted = self.latency_samples.iter().cloned().collect::<Vec<_>>();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

            let p95_idx = (sorted.len() as f64 * 0.95) as usize;
            let p99_idx = (sorted.len() as f64 * 0.99) as usize;

            self.metrics.p95_latency_ms = sorted.get(p95_idx).cloned().unwrap_or(0.0);
            self.metrics.p99_latency_ms = sorted.get(p99_idx).cloned().unwrap_or(0.0);
        }

        // Calculate throughput metrics
        if !self.throughput_samples.is_empty() {
            self.metrics.avg_throughput =
                self.throughput_samples.iter().sum::<f64>() / self.throughput_samples.len() as f64;
            self.metrics.peak_throughput =
                self.throughput_samples.iter().cloned().fold(0.0, f64::max);
        }

        // Calculate memory metrics
        if !self.memory_samples.is_empty() {
            self.metrics.avg_memory_bytes =
                self.memory_samples.iter().sum::<u64>() / self.memory_samples.len() as u64;
            self.metrics.peak_memory_bytes = *self.memory_samples.iter().max().unwrap_or(&0);
        }
    }

    /// Get optimization recommendations
    pub fn get_recommendations(&self) -> Vec<OptimizationRecommendation> {
        let mut recommendations = Vec::new();

        // Latency recommendations
        if self.metrics.avg_latency_ms > self.config.target_latency_ms {
            let recommended_prefetch = self.calculate_optimal_prefetch();
            recommendations.push(OptimizationRecommendation {
                category: OptimizationCategory::Latency,
                recommendation: format!(
                    "Reduce prefetch count to {} to lower latency",
                    recommended_prefetch
                ),
                current_value: self.metrics.avg_latency_ms,
                recommended_value: self.config.target_latency_ms,
                expected_improvement: ((self.metrics.avg_latency_ms
                    - self.config.target_latency_ms)
                    / self.metrics.avg_latency_ms)
                    * 100.0,
            });
        }

        // Throughput recommendations
        if self.metrics.avg_throughput < self.config.target_throughput {
            let recommended_batch = self.calculate_optimal_batch_size();
            recommendations.push(OptimizationRecommendation {
                category: OptimizationCategory::Throughput,
                recommendation: format!(
                    "Increase batch size to {} for better throughput",
                    recommended_batch
                ),
                current_value: self.metrics.avg_throughput,
                recommended_value: self.config.target_throughput,
                expected_improvement: ((self.config.target_throughput
                    - self.metrics.avg_throughput)
                    / self.config.target_throughput)
                    * 100.0,
            });
        }

        // Memory recommendations
        if self.metrics.peak_memory_bytes > (self.config.max_memory_bytes as f64 * 0.8) as u64 {
            recommendations.push(OptimizationRecommendation {
                category: OptimizationCategory::Memory,
                recommendation: "Consider enabling lazy queue mode or reducing prefetch count"
                    .to_string(),
                current_value: self.metrics.peak_memory_bytes as f64,
                recommended_value: (self.config.max_memory_bytes as f64 * 0.7),
                expected_improvement: 20.0,
            });
        }

        recommendations
    }

    /// Calculate optimal prefetch count
    pub fn calculate_optimal_prefetch(&self) -> u16 {
        if self.metrics.avg_latency_ms == 0.0 {
            return 10; // Default
        }

        // Higher latency = lower prefetch
        let latency_factor = (self.config.target_latency_ms / self.metrics.avg_latency_ms).min(2.0);
        let prefetch = (10.0 * latency_factor) as u16;

        prefetch.clamp(1, 100)
    }

    /// Calculate optimal batch size
    pub fn calculate_optimal_batch_size(&self) -> usize {
        if self.metrics.avg_throughput == 0.0 {
            return 100; // Default
        }

        // Lower throughput = larger batches (up to a point)
        let throughput_factor =
            (self.config.target_throughput / self.metrics.avg_throughput).min(5.0);
        let batch_size = (100.0 * throughput_factor) as usize;

        batch_size.clamp(10, 1000)
    }

    /// Calculate optimal channel pool size
    pub fn calculate_optimal_channel_pool(&self, concurrent_operations: usize) -> usize {
        let base_size = (concurrent_operations as f64 / 10.0).ceil() as usize;

        // Adjust based on throughput
        let throughput_multiplier = if self.metrics.avg_throughput > self.config.target_throughput {
            0.8
        } else {
            1.2
        };

        ((base_size as f64) * throughput_multiplier).ceil() as usize
    }

    /// Calculate optimal connection pool size
    pub fn calculate_optimal_connection_pool(&self, concurrent_operations: usize) -> usize {
        // AMQP typically needs fewer connections than channels
        let channels_per_connection = 100;
        let base_size = (concurrent_operations / channels_per_connection).max(1);

        base_size.clamp(1, 10)
    }

    /// Estimate time to drain queue
    pub fn estimate_drain_time(&self, queue_depth: u64) -> Duration {
        if self.metrics.avg_throughput == 0.0 {
            return Duration::from_secs(u64::MAX); // Can't estimate
        }

        let seconds = queue_depth as f64 / self.metrics.avg_throughput;
        Duration::from_secs(seconds as u64)
    }

    /// Get current metrics
    pub fn metrics(&self) -> &PerformanceMetrics {
        &self.metrics
    }

    /// Reset all metrics
    pub fn reset(&mut self) {
        self.latency_samples.clear();
        self.throughput_samples.clear();
        self.memory_samples.clear();
        self.metrics = PerformanceMetrics::default();
    }

    /// Check if performance meets targets
    pub fn is_meeting_targets(&self) -> bool {
        self.metrics.avg_latency_ms <= self.config.target_latency_ms
            && self.metrics.avg_throughput >= self.config.target_throughput
            && self.metrics.peak_memory_bytes <= self.config.max_memory_bytes
    }

    /// Get performance score (0.0 - 1.0, higher is better)
    pub fn performance_score(&self) -> f64 {
        let latency_score = if self.metrics.avg_latency_ms > 0.0 {
            (self.config.target_latency_ms / self.metrics.avg_latency_ms).min(1.0)
        } else {
            1.0
        };

        let throughput_score = if self.config.target_throughput > 0.0 {
            (self.metrics.avg_throughput / self.config.target_throughput).min(1.0)
        } else {
            1.0
        };

        let memory_score = if self.metrics.peak_memory_bytes > 0 {
            let memory_ratio =
                self.metrics.peak_memory_bytes as f64 / self.config.max_memory_bytes as f64;
            (1.0 - memory_ratio).max(0.0)
        } else {
            1.0
        };

        // Weighted average
        (latency_score * 0.4 + throughput_score * 0.4 + memory_score * 0.2).clamp(0.0, 1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_optimizer_creation() {
        let config = OptimizationConfig::default();
        let optimizer = PerformanceOptimizer::new(config);
        assert!(optimizer.latency_samples.is_empty());
        assert!(optimizer.throughput_samples.is_empty());
    }

    #[test]
    fn test_record_latency() {
        let config = OptimizationConfig::default();
        let mut optimizer = PerformanceOptimizer::new(config);

        optimizer.record_latency(50.0);
        optimizer.record_latency(60.0);
        optimizer.record_latency(70.0);

        assert_eq!(optimizer.metrics.avg_latency_ms, 60.0);
    }

    #[test]
    fn test_record_throughput() {
        let config = OptimizationConfig::default();
        let mut optimizer = PerformanceOptimizer::new(config);

        optimizer.record_throughput(1000.0);
        optimizer.record_throughput(1200.0);

        assert_eq!(optimizer.metrics.avg_throughput, 1100.0);
        assert_eq!(optimizer.metrics.peak_throughput, 1200.0);
    }

    #[test]
    fn test_record_memory() {
        let config = OptimizationConfig::default();
        let mut optimizer = PerformanceOptimizer::new(config);

        optimizer.record_memory_usage(100_000_000);
        optimizer.record_memory_usage(150_000_000);

        assert_eq!(optimizer.metrics.avg_memory_bytes, 125_000_000);
        assert_eq!(optimizer.metrics.peak_memory_bytes, 150_000_000);
    }

    #[test]
    fn test_percentile_calculation() {
        let config = OptimizationConfig::default();
        let mut optimizer = PerformanceOptimizer::new(config);

        for i in 1..=100 {
            optimizer.record_latency(i as f64);
        }

        assert!(optimizer.metrics.p95_latency_ms >= 95.0);
        assert!(optimizer.metrics.p99_latency_ms >= 99.0);
    }

    #[test]
    fn test_sample_window() {
        let config = OptimizationConfig {
            sample_window: 5,
            ..Default::default()
        };
        let mut optimizer = PerformanceOptimizer::new(config);

        for i in 1..=10 {
            optimizer.record_latency(i as f64);
        }

        assert_eq!(optimizer.latency_samples.len(), 5);
    }

    #[test]
    fn test_recommendations() {
        let config = OptimizationConfig {
            target_latency_ms: 50.0,
            target_throughput: 1000.0,
            ..Default::default()
        };
        let mut optimizer = PerformanceOptimizer::new(config);

        // Add samples that exceed targets
        optimizer.record_latency(100.0);
        optimizer.record_throughput(500.0);

        let recommendations = optimizer.get_recommendations();
        assert!(!recommendations.is_empty());

        // Should have latency and throughput recommendations
        assert!(recommendations
            .iter()
            .any(|r| matches!(r.category, OptimizationCategory::Latency)));
        assert!(recommendations
            .iter()
            .any(|r| matches!(r.category, OptimizationCategory::Throughput)));
    }

    #[test]
    fn test_optimal_prefetch_calculation() {
        let config = OptimizationConfig::default();
        let mut optimizer = PerformanceOptimizer::new(config);

        optimizer.record_latency(200.0); // High latency
        let prefetch = optimizer.calculate_optimal_prefetch();
        assert!(prefetch < 10); // Should recommend lower prefetch

        optimizer.reset();
        optimizer.record_latency(50.0); // Low latency
        let prefetch = optimizer.calculate_optimal_prefetch();
        assert!(prefetch >= 10); // Should allow higher prefetch
    }

    #[test]
    fn test_optimal_batch_calculation() {
        let config = OptimizationConfig::default();
        let mut optimizer = PerformanceOptimizer::new(config);

        optimizer.record_throughput(500.0); // Low throughput
        let batch = optimizer.calculate_optimal_batch_size();
        assert!(batch > 100); // Should recommend larger batches
    }

    #[test]
    fn test_drain_time_estimation() {
        let config = OptimizationConfig::default();
        let mut optimizer = PerformanceOptimizer::new(config);

        optimizer.record_throughput(1000.0);

        let drain_time = optimizer.estimate_drain_time(5000);
        assert_eq!(drain_time.as_secs(), 5); // 5000 messages at 1000/sec = 5 seconds
    }

    #[test]
    fn test_meeting_targets() {
        let config = OptimizationConfig {
            target_latency_ms: 100.0,
            target_throughput: 1000.0,
            max_memory_bytes: 1_000_000_000,
            ..Default::default()
        };
        let mut optimizer = PerformanceOptimizer::new(config);

        optimizer.record_latency(50.0);
        optimizer.record_throughput(1500.0);
        optimizer.record_memory_usage(500_000_000);

        assert!(optimizer.is_meeting_targets());

        optimizer.record_latency(200.0);
        assert!(!optimizer.is_meeting_targets());
    }

    #[test]
    fn test_performance_score() {
        let config = OptimizationConfig::default();
        let mut optimizer = PerformanceOptimizer::new(config);

        optimizer.record_latency(100.0);
        optimizer.record_throughput(1000.0);
        optimizer.record_memory_usage(500_000_000);

        let score = optimizer.performance_score();
        assert!(score > 0.0 && score <= 1.0);
    }

    #[test]
    fn test_channel_pool_sizing() {
        let config = OptimizationConfig::default();
        let mut optimizer = PerformanceOptimizer::new(config);

        optimizer.record_throughput(1000.0);

        let pool_size = optimizer.calculate_optimal_channel_pool(100);
        assert!(pool_size > 0);
    }

    #[test]
    fn test_connection_pool_sizing() {
        let config = OptimizationConfig::default();
        let optimizer = PerformanceOptimizer::new(config);

        let pool_size = optimizer.calculate_optimal_connection_pool(1000);
        assert!((1..=10).contains(&pool_size));
    }
}
