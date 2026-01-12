//! Adaptive Batch Size Optimizer
//!
//! Dynamically adjusts batch sizes based on system load, latency, and throughput metrics.
//!
//! # Features
//!
//! - **Adaptive Sizing**: Automatically adjusts batch sizes based on performance
//! - **Latency Monitoring**: Tracks publish and consume latencies
//! - **Throughput Optimization**: Maximizes throughput while maintaining acceptable latency
//! - **Load-Based Scaling**: Adjusts batch sizes based on queue depth and consumer count
//! - **Safety Bounds**: Ensures batch sizes stay within safe operational limits
//!
//! # Example
//!
//! ```rust
//! use celers_broker_amqp::batch_optimizer::{
//!     BatchOptimizer, OptimizationMetrics,
//! };
//! use std::time::Duration;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create optimizer with target latency
//! let optimizer = BatchOptimizer::new()
//!     .with_target_latency(Duration::from_millis(100))
//!     .with_min_batch_size(10)
//!     .with_max_batch_size(1000);
//!
//! // Record metrics
//! optimizer.record_batch_operation(
//!     50,  // batch size
//!     Duration::from_millis(80),  // latency
//!     true,  // success
//! ).await;
//!
//! // Get optimal batch size
//! let optimal_size = optimizer.get_optimal_batch_size().await;
//! println!("Optimal batch size: {}", optimal_size);
//!
//! // Adjust based on queue state
//! let adjusted_size = optimizer.adjust_for_queue_state(
//!     5000,  // queue_depth
//!     10,    // consumer_count
//! ).await;
//! # Ok(())
//! # }
//! ```

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Metrics for a batch operation
#[derive(Debug, Clone)]
pub struct BatchOperationMetric {
    /// Size of the batch
    pub batch_size: usize,
    /// Latency of the operation
    pub latency: Duration,
    /// Whether the operation succeeded
    pub success: bool,
    /// Timestamp when the operation occurred
    pub timestamp: Instant,
}

/// Optimization metrics and statistics
#[derive(Debug, Clone)]
pub struct OptimizationMetrics {
    /// Current optimal batch size
    pub optimal_batch_size: usize,
    /// Average latency for current batch size
    pub avg_latency: Duration,
    /// Success rate (0.0 to 1.0)
    pub success_rate: f64,
    /// Throughput (messages per second)
    pub throughput: f64,
    /// Number of operations recorded
    pub total_operations: usize,
}

/// Configuration for the batch optimizer
#[derive(Debug, Clone)]
pub struct BatchOptimizerConfig {
    /// Minimum batch size
    pub min_batch_size: usize,
    /// Maximum batch size
    pub max_batch_size: usize,
    /// Target latency for batch operations
    pub target_latency: Duration,
    /// Maximum acceptable latency
    pub max_acceptable_latency: Duration,
    /// Number of recent operations to consider
    pub window_size: usize,
    /// Adjustment step size (percentage)
    pub adjustment_step: f64,
}

impl Default for BatchOptimizerConfig {
    fn default() -> Self {
        Self {
            min_batch_size: 1,
            max_batch_size: 1000,
            target_latency: Duration::from_millis(100),
            max_acceptable_latency: Duration::from_millis(500),
            window_size: 100,
            adjustment_step: 0.1, // 10% adjustment
        }
    }
}

/// Adaptive batch size optimizer
pub struct BatchOptimizer {
    config: BatchOptimizerConfig,
    metrics: Arc<RwLock<VecDeque<BatchOperationMetric>>>,
    current_batch_size: Arc<RwLock<usize>>,
}

impl BatchOptimizer {
    /// Create a new batch optimizer with default configuration
    pub fn new() -> Self {
        Self::with_config(BatchOptimizerConfig::default())
    }

    /// Create a new batch optimizer with custom configuration
    pub fn with_config(config: BatchOptimizerConfig) -> Self {
        let initial_batch_size = (config.min_batch_size + config.max_batch_size) / 2;

        Self {
            config,
            metrics: Arc::new(RwLock::new(VecDeque::new())),
            current_batch_size: Arc::new(RwLock::new(initial_batch_size)),
        }
    }

    /// Set minimum batch size
    pub fn with_min_batch_size(mut self, min: usize) -> Self {
        self.config.min_batch_size = min;
        self
    }

    /// Set maximum batch size
    pub fn with_max_batch_size(mut self, max: usize) -> Self {
        self.config.max_batch_size = max;
        self
    }

    /// Set target latency
    pub fn with_target_latency(mut self, latency: Duration) -> Self {
        self.config.target_latency = latency;
        self
    }

    /// Record a batch operation
    pub async fn record_batch_operation(
        &self,
        batch_size: usize,
        latency: Duration,
        success: bool,
    ) {
        let mut metrics = self.metrics.write().await;

        metrics.push_back(BatchOperationMetric {
            batch_size,
            latency,
            success,
            timestamp: Instant::now(),
        });

        // Keep only recent operations
        while metrics.len() > self.config.window_size {
            metrics.pop_front();
        }

        // Drop lock before calling optimize
        drop(metrics);

        // Trigger optimization
        self.optimize().await;
    }

    /// Get the current optimal batch size
    pub async fn get_optimal_batch_size(&self) -> usize {
        *self.current_batch_size.read().await
    }

    /// Get optimization metrics
    pub async fn get_metrics(&self) -> OptimizationMetrics {
        let metrics = self.metrics.read().await;
        let current_batch_size = *self.current_batch_size.read().await;

        if metrics.is_empty() {
            return OptimizationMetrics {
                optimal_batch_size: current_batch_size,
                avg_latency: Duration::from_secs(0),
                success_rate: 1.0,
                throughput: 0.0,
                total_operations: 0,
            };
        }

        // Calculate average latency
        let total_latency: Duration = metrics.iter().map(|m| m.latency).sum();
        let avg_latency = total_latency / metrics.len() as u32;

        // Calculate success rate
        let successful = metrics.iter().filter(|m| m.success).count();
        let success_rate = successful as f64 / metrics.len() as f64;

        // Calculate throughput
        let total_messages: usize = metrics.iter().map(|m| m.batch_size).sum();
        let time_span = if let Some(first) = metrics.front() {
            if let Some(last) = metrics.back() {
                last.timestamp.duration_since(first.timestamp)
            } else {
                Duration::from_secs(1)
            }
        } else {
            Duration::from_secs(1)
        };

        let throughput = if time_span.as_secs_f64() > 0.0 {
            total_messages as f64 / time_span.as_secs_f64()
        } else {
            0.0
        };

        OptimizationMetrics {
            optimal_batch_size: current_batch_size,
            avg_latency,
            success_rate,
            throughput,
            total_operations: metrics.len(),
        }
    }

    /// Adjust batch size based on queue state
    ///
    /// # Arguments
    ///
    /// * `queue_depth` - Number of messages waiting in the queue
    /// * `consumer_count` - Number of active consumers
    ///
    /// # Returns
    ///
    /// Recommended batch size for the current queue state
    pub async fn adjust_for_queue_state(&self, queue_depth: usize, consumer_count: usize) -> usize {
        let base_size = self.get_optimal_batch_size().await;

        if consumer_count == 0 {
            return self.config.min_batch_size;
        }

        // Calculate messages per consumer
        let messages_per_consumer = queue_depth / consumer_count;

        // If queue is deep, increase batch size
        let adjustment_factor = if messages_per_consumer > 1000 {
            2.0 // Double batch size for very deep queues
        } else if messages_per_consumer > 500 {
            1.5 // 50% increase for deep queues
        } else if messages_per_consumer > 100 {
            1.2 // 20% increase for moderate queues
        } else if messages_per_consumer < 10 {
            0.5 // Halve batch size for shallow queues
        } else {
            1.0 // No adjustment
        };

        let adjusted = (base_size as f64 * adjustment_factor) as usize;
        adjusted.clamp(self.config.min_batch_size, self.config.max_batch_size)
    }

    /// Internal optimization logic
    async fn optimize(&self) {
        let metrics = self.metrics.read().await;

        if metrics.len() < 10 {
            return; // Need more data
        }

        // Get recent metrics (last 20% of window)
        let recent_count = (metrics.len() / 5).max(5);
        let recent_metrics: Vec<_> = metrics.iter().rev().take(recent_count).collect();

        // Calculate average latency for recent operations
        let total_latency: Duration = recent_metrics.iter().map(|m| m.latency).sum();
        let avg_latency = total_latency / recent_metrics.len() as u32;

        // Calculate success rate
        let successful = recent_metrics.iter().filter(|m| m.success).count();
        let success_rate = successful as f64 / recent_metrics.len() as f64;

        drop(metrics);

        let mut current_size = self.current_batch_size.write().await;

        // Decision logic
        if success_rate < 0.9 {
            // High failure rate - reduce batch size
            *current_size =
                (*current_size as f64 * (1.0 - self.config.adjustment_step * 2.0)) as usize;
        } else if avg_latency > self.config.max_acceptable_latency {
            // Latency too high - reduce batch size
            *current_size = (*current_size as f64 * (1.0 - self.config.adjustment_step)) as usize;
        } else if avg_latency < self.config.target_latency && success_rate > 0.95 {
            // Latency good and high success rate - increase batch size
            *current_size = (*current_size as f64 * (1.0 + self.config.adjustment_step)) as usize;
        }

        // Apply bounds
        *current_size =
            (*current_size).clamp(self.config.min_batch_size, self.config.max_batch_size);
    }

    /// Reset optimizer state
    pub async fn reset(&self) {
        let mut metrics = self.metrics.write().await;
        metrics.clear();

        let mut current_size = self.current_batch_size.write().await;
        *current_size = (self.config.min_batch_size + self.config.max_batch_size) / 2;
    }
}

impl Default for BatchOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_batch_optimizer_creation() {
        let optimizer = BatchOptimizer::new();
        let size = optimizer.get_optimal_batch_size().await;
        assert!(size > 0);
    }

    #[tokio::test]
    async fn test_record_operation() {
        let optimizer = BatchOptimizer::new();

        optimizer
            .record_batch_operation(50, Duration::from_millis(80), true)
            .await;

        let metrics = optimizer.get_metrics().await;
        assert_eq!(metrics.total_operations, 1);
        assert_eq!(metrics.success_rate, 1.0);
    }

    #[tokio::test]
    async fn test_adjust_for_queue_state() {
        let optimizer = BatchOptimizer::new()
            .with_min_batch_size(10)
            .with_max_batch_size(1000);

        // Deep queue should increase batch size
        let size = optimizer.adjust_for_queue_state(5000, 10).await;
        assert!(size > 100);

        // Shallow queue should decrease batch size (should be roughly half of base)
        let size = optimizer.adjust_for_queue_state(50, 10).await;
        assert!(size < 300); // Base is ~500, with 0.5x adjustment = ~250

        // No consumers should return minimum
        let size = optimizer.adjust_for_queue_state(1000, 0).await;
        assert_eq!(size, 10);
    }

    #[tokio::test]
    async fn test_custom_config() {
        let optimizer = BatchOptimizer::new()
            .with_min_batch_size(5)
            .with_max_batch_size(500)
            .with_target_latency(Duration::from_millis(50));

        let size = optimizer.get_optimal_batch_size().await;
        assert!((5..=500).contains(&size));
    }

    #[tokio::test]
    async fn test_reset() {
        let optimizer = BatchOptimizer::new();

        optimizer
            .record_batch_operation(50, Duration::from_millis(80), true)
            .await;

        assert_eq!(optimizer.get_metrics().await.total_operations, 1);

        optimizer.reset().await;
        assert_eq!(optimizer.get_metrics().await.total_operations, 0);
    }

    #[tokio::test]
    async fn test_high_latency_reduces_batch_size() {
        let optimizer = BatchOptimizer::new()
            .with_min_batch_size(10)
            .with_max_batch_size(1000)
            .with_target_latency(Duration::from_millis(100));

        // Record operations with high latency
        for _ in 0..20 {
            optimizer
                .record_batch_operation(500, Duration::from_millis(600), true)
                .await;
        }

        let initial_size = optimizer.get_optimal_batch_size().await;

        // Add a few more high-latency operations
        for _ in 0..10 {
            optimizer
                .record_batch_operation(500, Duration::from_millis(600), true)
                .await;
        }

        let final_size = optimizer.get_optimal_batch_size().await;

        // Batch size should have decreased
        assert!(final_size <= initial_size);
    }
}
