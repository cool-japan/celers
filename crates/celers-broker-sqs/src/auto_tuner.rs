//! Auto-Tuning System
//!
//! Automatically adjusts SQS configuration based on runtime metrics and workload patterns.
//! Uses machine learning-inspired algorithms to optimize for cost, performance, or reliability.
//!
//! # Features
//!
//! - Adaptive visibility timeout based on processing time
//! - Dynamic batch size optimization
//! - Intelligent polling strategy adjustment
//! - Worker scaling recommendations
//! - Cost-aware tuning
//! - Performance-aware tuning
//!
//! # Example
//!
//! ```
//! use celers_broker_sqs::auto_tuner::{AutoTuner, TuningGoal, RuntimeMetrics};
//!
//! let mut tuner = AutoTuner::new(TuningGoal::Balanced);
//!
//! // Collect runtime metrics
//! let metrics = RuntimeMetrics {
//!     avg_processing_time_ms: 250.0,
//!     p95_processing_time_ms: 500.0,
//!     queue_depth: 5000,
//!     messages_per_second: 100.0,
//!     error_rate: 0.01,
//!     ..Default::default()
//! };
//!
//! // Get tuning recommendations
//! let recommendations = tuner.tune(&metrics);
//! println!("Recommended visibility timeout: {}s", recommendations.visibility_timeout_seconds);
//! println!("Recommended batch size: {}", recommendations.batch_size);
//! ```

/// Auto-tuner for SQS configuration
pub struct AutoTuner {
    goal: TuningGoal,
    learning_rate: f64,
    history: Vec<TuningSnapshot>,
    max_history: usize,
}

impl AutoTuner {
    /// Create new auto-tuner with specific goal
    pub fn new(goal: TuningGoal) -> Self {
        Self {
            goal,
            learning_rate: 0.1,
            history: Vec::new(),
            max_history: 100,
        }
    }

    /// Set learning rate (0.0 to 1.0)
    /// Higher values adapt faster but may be less stable
    pub fn with_learning_rate(mut self, rate: f64) -> Self {
        self.learning_rate = rate.clamp(0.01, 1.0);
        self
    }

    /// Set maximum history size
    pub fn with_max_history(mut self, size: usize) -> Self {
        self.max_history = size;
        self
    }

    /// Tune configuration based on current metrics
    pub fn tune(&mut self, metrics: &RuntimeMetrics) -> TuningRecommendations {
        // Store snapshot
        let snapshot = TuningSnapshot {
            timestamp: std::time::SystemTime::now(),
            metrics: metrics.clone(),
        };
        self.history.push(snapshot);

        // Cleanup old history
        if self.history.len() > self.max_history {
            self.history.remove(0);
        }

        // Generate recommendations based on goal
        match self.goal {
            TuningGoal::MinimizeCost => self.tune_for_cost(metrics),
            TuningGoal::MaximizePerformance => self.tune_for_performance(metrics),
            TuningGoal::MaximizeReliability => self.tune_for_reliability(metrics),
            TuningGoal::Balanced => self.tune_balanced(metrics),
        }
    }

    /// Tune for minimum cost
    fn tune_for_cost(&self, metrics: &RuntimeMetrics) -> TuningRecommendations {
        // Maximize batch size and long polling to minimize API calls
        let batch_size = if metrics.queue_depth > 1000 { 10 } else { 5 };

        let wait_time_seconds = if metrics.messages_per_second < 10.0 {
            20 // Maximum long polling when traffic is low
        } else {
            15
        };

        // Set visibility timeout to 2x P95 processing time (avoid retries)
        let visibility_timeout_seconds =
            ((metrics.p95_processing_time_ms / 1000.0) * 2.0).ceil() as u32;

        // Reduce concurrent consumers when possible
        let recommended_consumers = self.calculate_min_consumers(metrics);

        TuningRecommendations {
            visibility_timeout_seconds: visibility_timeout_seconds.max(30),
            wait_time_seconds,
            batch_size,
            max_messages_per_receive: 10,
            recommended_consumers,
            enable_compression: metrics.avg_message_size_bytes > 20_000,
            reasoning: "Cost-optimized: Maximum batching, long polling, minimal workers"
                .to_string(),
        }
    }

    /// Tune for maximum performance
    fn tune_for_performance(&self, metrics: &RuntimeMetrics) -> TuningRecommendations {
        // Minimize latency
        let wait_time_seconds = if metrics.queue_depth > 100 {
            1 // Minimal latency when queue has messages
        } else {
            5 // Moderate when empty
        };

        // Smaller batches for faster individual message processing
        let batch_size = if metrics.avg_processing_time_ms > 1000.0 {
            1 // Process one at a time for heavy tasks
        } else {
            5
        };

        // Set visibility timeout to 1.5x P95 (balance retry and hold time)
        let visibility_timeout_seconds =
            ((metrics.p95_processing_time_ms / 1000.0) * 1.5).ceil() as u32;

        // Scale up consumers aggressively
        let recommended_consumers = self.calculate_max_consumers(metrics);

        TuningRecommendations {
            visibility_timeout_seconds: visibility_timeout_seconds.max(30),
            wait_time_seconds,
            batch_size,
            max_messages_per_receive: batch_size,
            recommended_consumers,
            enable_compression: false, // Skip compression for speed
            reasoning: "Performance-optimized: Low latency, high concurrency".to_string(),
        }
    }

    /// Tune for maximum reliability
    fn tune_for_reliability(&self, metrics: &RuntimeMetrics) -> TuningRecommendations {
        // Conservative settings to avoid message loss

        // Set visibility timeout to 3x P95 (generous retry window)
        let visibility_timeout_seconds =
            ((metrics.p95_processing_time_ms / 1000.0) * 3.0).ceil() as u32;

        let wait_time_seconds = 15; // Balanced

        // Moderate batch size
        let batch_size = 5;

        // Conservative consumer count
        let recommended_consumers = self.calculate_optimal_consumers(metrics);

        TuningRecommendations {
            visibility_timeout_seconds: visibility_timeout_seconds.max(60),
            wait_time_seconds,
            batch_size,
            max_messages_per_receive: batch_size,
            recommended_consumers,
            enable_compression: metrics.avg_message_size_bytes > 50_000,
            reasoning: "Reliability-optimized: Conservative timeouts, moderate concurrency"
                .to_string(),
        }
    }

    /// Tune for balanced performance/cost/reliability
    fn tune_balanced(&self, metrics: &RuntimeMetrics) -> TuningRecommendations {
        // Adaptive approach based on workload characteristics

        let is_high_traffic = metrics.messages_per_second > 100.0;
        let is_slow_processing = metrics.avg_processing_time_ms > 500.0;
        let has_backlog = metrics.queue_depth > 1000;

        let wait_time_seconds = if has_backlog || is_high_traffic {
            5 // Low latency when busy
        } else {
            15 // Higher when idle
        };

        let batch_size = if is_slow_processing {
            3 // Smaller batches for heavy processing
        } else if has_backlog {
            10 // Larger batches to clear backlog
        } else {
            5
        };

        // Set visibility timeout to 2x P95 + buffer
        let visibility_timeout_seconds =
            (metrics.p95_processing_time_ms / 1000.0 * 2.0 + 30.0).ceil() as u32;

        let recommended_consumers = if has_backlog {
            self.calculate_max_consumers(metrics)
        } else {
            self.calculate_optimal_consumers(metrics)
        };

        TuningRecommendations {
            visibility_timeout_seconds: visibility_timeout_seconds.clamp(30, 3600),
            wait_time_seconds,
            batch_size,
            max_messages_per_receive: batch_size,
            recommended_consumers,
            enable_compression: metrics.avg_message_size_bytes > 30_000,
            reasoning: format!(
                "Balanced: Adapted to {} traffic, {} processing, {} backlog",
                if is_high_traffic { "high" } else { "low" },
                if is_slow_processing { "slow" } else { "fast" },
                if has_backlog { "large" } else { "small" }
            ),
        }
    }

    fn calculate_min_consumers(&self, metrics: &RuntimeMetrics) -> usize {
        // Minimum consumers needed to keep up with traffic
        let processing_capacity_per_consumer = 1000.0 / metrics.avg_processing_time_ms;
        let required = (metrics.messages_per_second / processing_capacity_per_consumer).ceil();
        (required as usize).max(1)
    }

    fn calculate_max_consumers(&self, metrics: &RuntimeMetrics) -> usize {
        // Maximum beneficial consumers
        let min = self.calculate_min_consumers(metrics);
        if metrics.queue_depth > 5000 {
            min * 3
        } else if metrics.queue_depth > 1000 {
            min * 2
        } else {
            min
        }
    }

    fn calculate_optimal_consumers(&self, metrics: &RuntimeMetrics) -> usize {
        let min = self.calculate_min_consumers(metrics);
        let max = self.calculate_max_consumers(metrics);
        ((min + max) / 2).max(1)
    }

    /// Get tuning history for analysis
    pub fn get_history(&self) -> &[TuningSnapshot] {
        &self.history
    }

    /// Reset tuning history
    pub fn reset_history(&mut self) {
        self.history.clear();
    }

    /// Calculate tuning confidence score (0-100)
    pub fn confidence_score(&self) -> u8 {
        if self.history.len() < 5 {
            20 // Low confidence with little data
        } else if self.history.len() < 20 {
            60 // Medium confidence
        } else {
            90 // High confidence with sufficient history
        }
    }
}

/// Tuning goal
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TuningGoal {
    /// Minimize cost (maximize batching, long polling)
    MinimizeCost,
    /// Maximize performance (minimize latency, high concurrency)
    MaximizePerformance,
    /// Maximize reliability (generous timeouts, conservative settings)
    MaximizeReliability,
    /// Balance cost, performance, and reliability
    Balanced,
}

/// Runtime metrics for tuning
#[derive(Debug, Clone, Default)]
pub struct RuntimeMetrics {
    /// Average message processing time (ms)
    pub avg_processing_time_ms: f64,
    /// P95 message processing time (ms)
    pub p95_processing_time_ms: f64,
    /// Current queue depth
    pub queue_depth: u64,
    /// Messages processed per second
    pub messages_per_second: f64,
    /// Error rate (0.0 to 1.0)
    pub error_rate: f64,
    /// Average message size in bytes
    pub avg_message_size_bytes: usize,
    /// Number of active consumers
    pub active_consumers: usize,
    /// Current visibility timeout (seconds)
    pub current_visibility_timeout: u32,
}

/// Tuning recommendations
#[derive(Debug, Clone)]
pub struct TuningRecommendations {
    pub visibility_timeout_seconds: u32,
    pub wait_time_seconds: u32,
    pub batch_size: usize,
    pub max_messages_per_receive: usize,
    pub recommended_consumers: usize,
    pub enable_compression: bool,
    pub reasoning: String,
}

/// Historical tuning snapshot
#[derive(Debug, Clone)]
pub struct TuningSnapshot {
    pub timestamp: std::time::SystemTime,
    pub metrics: RuntimeMetrics,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auto_tuner_creation() {
        let tuner = AutoTuner::new(TuningGoal::Balanced);
        assert_eq!(tuner.goal, TuningGoal::Balanced);
        assert_eq!(tuner.history.len(), 0);
    }

    #[test]
    fn test_cost_optimization() {
        let mut tuner = AutoTuner::new(TuningGoal::MinimizeCost);
        let metrics = RuntimeMetrics {
            avg_processing_time_ms: 100.0,
            p95_processing_time_ms: 200.0,
            queue_depth: 5000,
            messages_per_second: 50.0,
            ..Default::default()
        };

        let recommendations = tuner.tune(&metrics);
        assert_eq!(recommendations.batch_size, 10); // Max batching
        assert_eq!(recommendations.wait_time_seconds, 15);
        assert!(recommendations.reasoning.contains("Cost"));
    }

    #[test]
    fn test_performance_optimization() {
        let mut tuner = AutoTuner::new(TuningGoal::MaximizePerformance);
        let metrics = RuntimeMetrics {
            avg_processing_time_ms: 50.0,
            p95_processing_time_ms: 100.0,
            queue_depth: 500,
            messages_per_second: 200.0,
            ..Default::default()
        };

        let recommendations = tuner.tune(&metrics);
        assert_eq!(recommendations.wait_time_seconds, 1); // Min latency
        assert!(!recommendations.enable_compression); // No compression for speed
    }

    #[test]
    fn test_reliability_optimization() {
        let mut tuner = AutoTuner::new(TuningGoal::MaximizeReliability);
        let metrics = RuntimeMetrics {
            avg_processing_time_ms: 500.0,
            p95_processing_time_ms: 1000.0,
            queue_depth: 1000,
            messages_per_second: 10.0,
            error_rate: 0.05,
            ..Default::default()
        };

        let recommendations = tuner.tune(&metrics);
        assert!(recommendations.visibility_timeout_seconds >= 3000 / 1000 * 3); // 3x P95
        assert_eq!(recommendations.batch_size, 5); // Moderate batching
    }

    #[test]
    fn test_balanced_high_traffic() {
        let mut tuner = AutoTuner::new(TuningGoal::Balanced);
        let metrics = RuntimeMetrics {
            avg_processing_time_ms: 100.0,
            p95_processing_time_ms: 200.0,
            queue_depth: 2000,
            messages_per_second: 150.0,
            ..Default::default()
        };

        let recommendations = tuner.tune(&metrics);
        assert!(recommendations.batch_size > 5); // Larger batches for backlog
        assert_eq!(recommendations.wait_time_seconds, 5); // Low latency
    }

    #[test]
    fn test_balanced_low_traffic() {
        let mut tuner = AutoTuner::new(TuningGoal::Balanced);
        let metrics = RuntimeMetrics {
            avg_processing_time_ms: 100.0,
            p95_processing_time_ms: 200.0,
            queue_depth: 50,
            messages_per_second: 5.0,
            ..Default::default()
        };

        let recommendations = tuner.tune(&metrics);
        assert_eq!(recommendations.wait_time_seconds, 15); // Higher when idle
    }

    #[test]
    fn test_compression_recommendation() {
        let mut tuner = AutoTuner::new(TuningGoal::Balanced);

        // Small messages - no compression
        let metrics = RuntimeMetrics {
            avg_processing_time_ms: 100.0,
            p95_processing_time_ms: 200.0,
            avg_message_size_bytes: 5000,
            ..Default::default()
        };
        let recommendations = tuner.tune(&metrics);
        assert!(!recommendations.enable_compression);

        // Large messages - enable compression
        let metrics = RuntimeMetrics {
            avg_processing_time_ms: 100.0,
            p95_processing_time_ms: 200.0,
            avg_message_size_bytes: 50000,
            ..Default::default()
        };
        let recommendations = tuner.tune(&metrics);
        assert!(recommendations.enable_compression);
    }

    #[test]
    fn test_history_tracking() {
        let mut tuner = AutoTuner::new(TuningGoal::Balanced);
        let metrics = RuntimeMetrics::default();

        assert_eq!(tuner.get_history().len(), 0);

        tuner.tune(&metrics);
        assert_eq!(tuner.get_history().len(), 1);

        tuner.tune(&metrics);
        assert_eq!(tuner.get_history().len(), 2);

        tuner.reset_history();
        assert_eq!(tuner.get_history().len(), 0);
    }

    #[test]
    fn test_confidence_score() {
        let mut tuner = AutoTuner::new(TuningGoal::Balanced);

        // Low confidence with no data
        assert_eq!(tuner.confidence_score(), 20);

        // Add some history
        for _ in 0..10 {
            tuner.tune(&RuntimeMetrics::default());
        }
        assert_eq!(tuner.confidence_score(), 60);

        // High confidence with sufficient data
        for _ in 0..20 {
            tuner.tune(&RuntimeMetrics::default());
        }
        assert_eq!(tuner.confidence_score(), 90);
    }

    #[test]
    fn test_learning_rate() {
        let tuner = AutoTuner::new(TuningGoal::Balanced).with_learning_rate(0.5);
        assert_eq!(tuner.learning_rate, 0.5);
    }

    #[test]
    fn test_max_history() {
        let mut tuner = AutoTuner::new(TuningGoal::Balanced).with_max_history(10);

        // Add more than max
        for _ in 0..20 {
            tuner.tune(&RuntimeMetrics::default());
        }

        // Should be capped at max
        assert_eq!(tuner.get_history().len(), 10);
    }

    #[test]
    fn test_visibility_timeout_clamping() {
        let mut tuner = AutoTuner::new(TuningGoal::Balanced);

        // Very fast processing
        let metrics = RuntimeMetrics {
            avg_processing_time_ms: 10.0,
            p95_processing_time_ms: 20.0,
            ..Default::default()
        };
        let recommendations = tuner.tune(&metrics);
        assert!(recommendations.visibility_timeout_seconds >= 30); // Minimum

        // Very slow processing
        let metrics = RuntimeMetrics {
            avg_processing_time_ms: 10000.0,
            p95_processing_time_ms: 20000.0,
            ..Default::default()
        };
        let recommendations = tuner.tune(&metrics);
        assert!(recommendations.visibility_timeout_seconds <= 3600); // Maximum
    }

    #[test]
    fn test_consumer_scaling() {
        let mut tuner = AutoTuner::new(TuningGoal::Balanced);

        // Low traffic
        let metrics = RuntimeMetrics {
            avg_processing_time_ms: 100.0,
            messages_per_second: 5.0,
            queue_depth: 100,
            ..Default::default()
        };
        let recommendations = tuner.tune(&metrics);
        let low_consumers = recommendations.recommended_consumers;

        // High traffic
        let metrics = RuntimeMetrics {
            avg_processing_time_ms: 100.0,
            messages_per_second: 100.0,
            queue_depth: 5000,
            ..Default::default()
        };
        let recommendations = tuner.tune(&metrics);
        let high_consumers = recommendations.recommended_consumers;

        assert!(high_consumers > low_consumers);
    }
}
