//! Advanced batch optimization for AWS SQS operations
//!
//! This module provides intelligent batch sizing algorithms that dynamically
//! adjust batch sizes based on queue metrics, message size, latency requirements,
//! and cost optimization goals.
//!
//! # Examples
//!
//! ```
//! use celers_broker_sqs::batch_optimizer::*;
//!
//! # fn example() {
//! // Create optimizer with latency target
//! let optimizer = BatchOptimizer::new(OptimizerGoal::LowLatency { target_ms: 100 });
//!
//! // Get optimal batch size
//! let batch_size = optimizer.calculate_optimal_batch_size(
//!     500,     // queue_size
//!     5000,    // avg_message_size_bytes
//!     50.0,    // processing_rate (msg/sec)
//! );
//!
//! println!("Optimal batch size: {}", batch_size);
//! # }
//! ```

use serde::{Deserialize, Serialize};

/// Optimization goal for batch sizing
#[derive(Debug, Clone)]
pub enum OptimizerGoal {
    /// Minimize cost (maximize batching)
    MinimizeCost,
    /// Minimize latency (smaller batches for faster processing)
    LowLatency {
        /// Target latency in milliseconds
        target_ms: u64,
    },
    /// Balance cost and latency
    Balanced {
        /// Weight for cost optimization (0.0 - 1.0)
        cost_weight: f64,
    },
    /// Maximize throughput
    MaxThroughput {
        /// Number of concurrent workers
        workers: usize,
    },
}

/// Batch optimizer for dynamic batch sizing
#[derive(Debug, Clone)]
pub struct BatchOptimizer {
    goal: OptimizerGoal,
}

/// Batch optimization result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchOptimizationResult {
    /// Recommended batch size
    pub batch_size: usize,
    /// Estimated latency (milliseconds)
    pub estimated_latency_ms: f64,
    /// Estimated cost per message (USD)
    pub estimated_cost_per_message: f64,
    /// Estimated throughput (messages/sec)
    pub estimated_throughput: f64,
    /// Reasoning for the recommendation
    pub reasoning: String,
}

impl BatchOptimizer {
    /// Create a new batch optimizer with a specific goal
    pub fn new(goal: OptimizerGoal) -> Self {
        Self { goal }
    }

    /// Calculate optimal batch size based on current metrics
    ///
    /// # Arguments
    /// * `queue_size` - Current number of messages in queue
    /// * `avg_message_size_bytes` - Average message size in bytes
    /// * `processing_rate` - Messages processed per second
    ///
    /// # Returns
    /// Optimal batch size (1-10 for SQS)
    pub fn calculate_optimal_batch_size(
        &self,
        queue_size: usize,
        _avg_message_size_bytes: usize,
        processing_rate: f64,
    ) -> usize {
        match &self.goal {
            OptimizerGoal::MinimizeCost => {
                // Always use maximum batch size to minimize API calls
                self.cost_optimized_batch_size(queue_size)
            }
            OptimizerGoal::LowLatency { target_ms } => {
                self.latency_optimized_batch_size(*target_ms, processing_rate)
            }
            OptimizerGoal::Balanced { cost_weight } => {
                self.balanced_batch_size(queue_size, processing_rate, *cost_weight)
            }
            OptimizerGoal::MaxThroughput { workers } => {
                self.throughput_optimized_batch_size(queue_size, *workers, processing_rate)
            }
        }
    }

    /// Get detailed optimization result with reasoning
    pub fn optimize(
        &self,
        queue_size: usize,
        _avg_message_size_bytes: usize,
        processing_rate: f64,
    ) -> BatchOptimizationResult {
        let batch_size =
            self.calculate_optimal_batch_size(queue_size, _avg_message_size_bytes, processing_rate);

        let estimated_latency_ms = self.estimate_latency(batch_size, processing_rate);
        let estimated_cost_per_message =
            self.estimate_cost_per_message(batch_size, _avg_message_size_bytes);
        let estimated_throughput = self.estimate_throughput(batch_size, processing_rate);

        let reasoning = self.generate_reasoning(
            batch_size,
            queue_size,
            processing_rate,
            estimated_latency_ms,
        );

        BatchOptimizationResult {
            batch_size,
            estimated_latency_ms,
            estimated_cost_per_message,
            estimated_throughput,
            reasoning,
        }
    }

    /// Cost-optimized batch size (maximize batching)
    fn cost_optimized_batch_size(&self, queue_size: usize) -> usize {
        if queue_size >= 10 {
            10 // Maximum batch size
        } else if queue_size > 0 {
            queue_size.min(10)
        } else {
            10 // Default to max even if queue is empty
        }
    }

    /// Latency-optimized batch size
    fn latency_optimized_batch_size(&self, target_ms: u64, processing_rate: f64) -> usize {
        // Calculate batch size that keeps latency under target
        // Assuming network overhead of ~20ms + processing time

        let processing_time_per_msg = if processing_rate > 0.0 {
            1000.0 / processing_rate // milliseconds
        } else {
            100.0 // Default assumption
        };

        let available_time = target_ms as f64 - 20.0; // Subtract network overhead
        let max_messages = (available_time / processing_time_per_msg).floor() as usize;

        max_messages.clamp(1, 10)
    }

    /// Balanced batch size (cost vs latency)
    fn balanced_batch_size(
        &self,
        _queue_size: usize,
        processing_rate: f64,
        cost_weight: f64,
    ) -> usize {
        let cost_weight = cost_weight.clamp(0.0, 1.0);
        let latency_weight = 1.0 - cost_weight;

        // Cost optimal is always 10
        let cost_optimal = 10;

        // Latency optimal depends on processing rate
        let latency_optimal = if processing_rate > 100.0 {
            5 // High throughput, can handle larger batches
        } else if processing_rate > 50.0 {
            3
        } else {
            1 // Low throughput, prefer smaller batches
        };

        // Weighted average
        let weighted =
            (cost_optimal as f64 * cost_weight) + (latency_optimal as f64 * latency_weight);

        (weighted.round() as usize).clamp(1, 10)
    }

    /// Throughput-optimized batch size
    fn throughput_optimized_batch_size(
        &self,
        queue_size: usize,
        workers: usize,
        processing_rate: f64,
    ) -> usize {
        // Try to distribute work evenly across workers
        if queue_size == 0 {
            return 10; // Default to max
        }

        if workers == 0 {
            return 10;
        }

        // Ideal: queue_size / workers, but clamped to 1-10
        let messages_per_worker = queue_size.div_ceil(workers);

        // Also consider processing rate
        let rate_based = if processing_rate > 100.0 {
            10
        } else if processing_rate > 50.0 {
            7
        } else {
            5
        };

        // Take minimum to avoid overwhelming workers
        messages_per_worker.min(rate_based).clamp(1, 10)
    }

    /// Estimate latency for a given batch size
    fn estimate_latency(&self, batch_size: usize, processing_rate: f64) -> f64 {
        let network_overhead_ms = 20.0; // Average AWS SQS network latency

        let processing_time_per_msg = if processing_rate > 0.0 {
            1000.0 / processing_rate
        } else {
            100.0
        };

        let processing_time_ms = batch_size as f64 * processing_time_per_msg;

        network_overhead_ms + processing_time_ms
    }

    /// Estimate cost per message for a given batch size
    fn estimate_cost_per_message(&self, batch_size: usize, _avg_message_size_bytes: usize) -> f64 {
        // Standard queue pricing: $0.40 per million requests
        let price_per_request = 0.40 / 1_000_000.0;

        // Batch operations reduce cost by factor of batch_size
        price_per_request / batch_size as f64
    }

    /// Estimate throughput for a given batch size
    fn estimate_throughput(&self, batch_size: usize, processing_rate: f64) -> f64 {
        // Throughput = messages per batch * batches per second
        let network_overhead_s = 0.02; // 20ms
        let processing_time_per_msg_s = if processing_rate > 0.0 {
            1.0 / processing_rate
        } else {
            0.1
        };

        let time_per_batch_s = network_overhead_s + (batch_size as f64 * processing_time_per_msg_s);
        let batches_per_second = 1.0 / time_per_batch_s;

        batch_size as f64 * batches_per_second
    }

    /// Generate reasoning for the recommendation
    fn generate_reasoning(
        &self,
        batch_size: usize,
        queue_size: usize,
        processing_rate: f64,
        estimated_latency_ms: f64,
    ) -> String {
        match &self.goal {
            OptimizerGoal::MinimizeCost => {
                format!(
                    "Cost optimization: Using maximum batch size ({}) to minimize API costs. \
                     With queue size {}, this maximizes batching efficiency.",
                    batch_size, queue_size
                )
            }
            OptimizerGoal::LowLatency { target_ms } => {
                format!(
                    "Latency optimization: Batch size {} keeps estimated latency ({:.1}ms) \
                     under target ({}ms) with processing rate {:.1} msg/s.",
                    batch_size, estimated_latency_ms, target_ms, processing_rate
                )
            }
            OptimizerGoal::Balanced { cost_weight } => {
                format!(
                    "Balanced optimization: Batch size {} balances cost (weight: {:.2}) \
                     and latency for processing rate {:.1} msg/s.",
                    batch_size, cost_weight, processing_rate
                )
            }
            OptimizerGoal::MaxThroughput { workers } => {
                format!(
                    "Throughput optimization: Batch size {} optimizes throughput \
                     across {} workers with queue size {} and rate {:.1} msg/s.",
                    batch_size, workers, queue_size, processing_rate
                )
            }
        }
    }

    /// Recommend batch strategy based on queue metrics
    pub fn recommend_strategy(
        queue_size: usize,
        processing_rate: f64,
        target_latency_ms: Option<u64>,
    ) -> OptimizerGoal {
        // If queue is very large, prioritize throughput
        if queue_size > 10000 {
            return OptimizerGoal::MaxThroughput {
                workers: (queue_size / 1000).max(1),
            };
        }

        // If latency target is specified and tight
        if let Some(target_ms) = target_latency_ms {
            if target_ms < 200 {
                return OptimizerGoal::LowLatency { target_ms };
            }
        }

        // If processing rate is slow, optimize for cost
        if processing_rate < 10.0 {
            return OptimizerGoal::MinimizeCost;
        }

        // Default: balanced approach
        OptimizerGoal::Balanced { cost_weight: 0.6 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cost_optimized_batch_size() {
        let optimizer = BatchOptimizer::new(OptimizerGoal::MinimizeCost);

        // Large queue should return max batch size
        assert_eq!(optimizer.calculate_optimal_batch_size(1000, 1000, 50.0), 10);

        // Small queue should return queue size
        assert_eq!(optimizer.calculate_optimal_batch_size(5, 1000, 50.0), 5);

        // Empty queue should still return max for cost optimization
        assert_eq!(optimizer.calculate_optimal_batch_size(0, 1000, 50.0), 10);
    }

    #[test]
    fn test_latency_optimized_batch_size() {
        let optimizer = BatchOptimizer::new(OptimizerGoal::LowLatency { target_ms: 100 });

        // Fast processing rate should allow larger batches
        let batch_size = optimizer.calculate_optimal_batch_size(1000, 1000, 100.0);
        assert!(batch_size >= 5);

        // Slow processing rate should prefer smaller batches
        let optimizer = BatchOptimizer::new(OptimizerGoal::LowLatency { target_ms: 50 });
        let batch_size = optimizer.calculate_optimal_batch_size(1000, 1000, 10.0);
        assert!(batch_size <= 3);
    }

    #[test]
    fn test_balanced_batch_size() {
        let optimizer = BatchOptimizer::new(OptimizerGoal::Balanced { cost_weight: 0.5 });

        let batch_size = optimizer.calculate_optimal_batch_size(1000, 1000, 50.0);
        assert!((1..=10).contains(&batch_size));
    }

    #[test]
    fn test_throughput_optimized_batch_size() {
        let optimizer = BatchOptimizer::new(OptimizerGoal::MaxThroughput { workers: 5 });

        let batch_size = optimizer.calculate_optimal_batch_size(50, 1000, 100.0);
        assert!((1..=10).contains(&batch_size));
    }

    #[test]
    fn test_optimize_with_reasoning() {
        let optimizer = BatchOptimizer::new(OptimizerGoal::MinimizeCost);

        let result = optimizer.optimize(1000, 5000, 50.0);

        assert_eq!(result.batch_size, 10);
        assert!(result.estimated_latency_ms > 0.0);
        assert!(result.estimated_cost_per_message > 0.0);
        assert!(result.estimated_throughput > 0.0);
        assert!(!result.reasoning.is_empty());
    }

    #[test]
    fn test_recommend_strategy() {
        // Large queue should recommend throughput optimization
        let goal = BatchOptimizer::recommend_strategy(15000, 50.0, None);
        assert!(matches!(goal, OptimizerGoal::MaxThroughput { .. }));

        // Tight latency target should recommend low latency
        let goal = BatchOptimizer::recommend_strategy(1000, 50.0, Some(100));
        assert!(matches!(goal, OptimizerGoal::LowLatency { .. }));

        // Slow processing should recommend cost optimization
        let goal = BatchOptimizer::recommend_strategy(100, 5.0, None);
        assert!(matches!(goal, OptimizerGoal::MinimizeCost));

        // Default should be balanced
        let goal = BatchOptimizer::recommend_strategy(1000, 50.0, None);
        assert!(matches!(goal, OptimizerGoal::Balanced { .. }));
    }

    #[test]
    fn test_estimate_latency() {
        let optimizer = BatchOptimizer::new(OptimizerGoal::MinimizeCost);

        let latency = optimizer.estimate_latency(10, 100.0);
        assert!(latency > 20.0); // At least network overhead

        let latency = optimizer.estimate_latency(1, 100.0);
        assert!(latency < 50.0); // Should be low for small batch
    }

    #[test]
    fn test_estimate_cost() {
        let optimizer = BatchOptimizer::new(OptimizerGoal::MinimizeCost);

        let cost10 = optimizer.estimate_cost_per_message(10, 1000);
        let cost1 = optimizer.estimate_cost_per_message(1, 1000);

        // Batching should reduce cost per message
        assert!(cost10 < cost1);
        assert!((cost10 * 10.0 - cost1).abs() < 0.0000001); // Should be 10x cheaper
    }
}
