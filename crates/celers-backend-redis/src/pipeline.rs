//! Redis pipeline optimization utilities
//!
//! This module provides automatic pipeline optimization for Redis operations,
//! grouping commands together to reduce round-trips and improve performance.

use std::time::Duration;

/// Pipeline optimization strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PipelineStrategy {
    /// Always use pipelining for batch operations
    Always,
    /// Use pipelining only when batch size exceeds threshold
    Adaptive { threshold: usize },
    /// Never use pipelining (sequential operations)
    Never,
}

impl Default for PipelineStrategy {
    fn default() -> Self {
        PipelineStrategy::Adaptive { threshold: 10 }
    }
}

/// Pipeline configuration
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    /// Strategy for when to use pipelining
    pub strategy: PipelineStrategy,
    /// Maximum number of commands per pipeline
    pub max_batch_size: usize,
    /// Whether to use atomic transactions (MULTI/EXEC)
    pub use_transactions: bool,
    /// Command timeout
    pub timeout: Option<Duration>,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            strategy: PipelineStrategy::default(),
            max_batch_size: 1000,
            use_transactions: false,
            timeout: Some(Duration::from_secs(30)),
        }
    }
}

impl PipelineConfig {
    /// Create a new pipeline configuration with defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the pipeline strategy
    pub fn with_strategy(mut self, strategy: PipelineStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Set the maximum batch size
    pub fn with_max_batch_size(mut self, size: usize) -> Self {
        self.max_batch_size = size;
        self
    }

    /// Enable or disable transactions
    pub fn with_transactions(mut self, enabled: bool) -> Self {
        self.use_transactions = enabled;
        self
    }

    /// Set the command timeout
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Remove the timeout
    pub fn without_timeout(mut self) -> Self {
        self.timeout = None;
        self
    }

    /// Check if pipelining should be used for a given batch size
    pub fn should_pipeline(&self, batch_size: usize) -> bool {
        match self.strategy {
            PipelineStrategy::Always => true,
            PipelineStrategy::Adaptive { threshold } => batch_size >= threshold,
            PipelineStrategy::Never => false,
        }
    }

    /// Calculate the number of pipeline batches needed
    pub fn calculate_batches(&self, total_items: usize) -> usize {
        if !self.should_pipeline(total_items) {
            return total_items; // One operation per item
        }

        total_items.div_ceil(self.max_batch_size)
    }

    /// Get the optimal chunk size for splitting operations
    pub fn chunk_size(&self, total_items: usize) -> usize {
        if !self.should_pipeline(total_items) {
            return 1;
        }

        self.max_batch_size.min(total_items)
    }
}

/// Pipeline performance metrics
#[derive(Debug, Clone, Default)]
pub struct PipelineMetrics {
    /// Number of pipelined operations
    pub pipelined_ops: u64,
    /// Number of sequential operations
    pub sequential_ops: u64,
    /// Total commands executed
    pub total_commands: u64,
    /// Total pipeline batches
    pub total_batches: u64,
    /// Average commands per pipeline
    commands_sum: u64,
}

impl PipelineMetrics {
    /// Create new empty metrics
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a pipelined operation
    pub fn record_pipelined(&mut self, commands: usize) {
        self.pipelined_ops += 1;
        self.total_commands += commands as u64;
        self.total_batches += 1;
        self.commands_sum += commands as u64;
    }

    /// Record a sequential operation
    pub fn record_sequential(&mut self) {
        self.sequential_ops += 1;
        self.total_commands += 1;
    }

    /// Get average commands per pipeline
    pub fn avg_commands_per_pipeline(&self) -> f64 {
        if self.total_batches == 0 {
            0.0
        } else {
            self.commands_sum as f64 / self.total_batches as f64
        }
    }

    /// Get the pipeline efficiency (0.0 - 1.0)
    /// Higher is better (more commands per batch)
    pub fn pipeline_efficiency(&self) -> f64 {
        if self.total_commands == 0 {
            0.0
        } else {
            let ideal_commands = self.total_commands;
            let actual_ops = self.pipelined_ops + self.sequential_ops;
            if actual_ops == 0 {
                0.0
            } else {
                1.0 - (actual_ops as f64 / ideal_commands as f64)
            }
        }
    }

    /// Reset all metrics
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

impl std::fmt::Display for PipelineMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Pipeline Metrics")?;
        writeln!(f, "  Pipelined Operations: {}", self.pipelined_ops)?;
        writeln!(f, "  Sequential Operations: {}", self.sequential_ops)?;
        writeln!(f, "  Total Commands: {}", self.total_commands)?;
        writeln!(f, "  Total Batches: {}", self.total_batches)?;
        writeln!(
            f,
            "  Avg Commands/Pipeline: {:.2}",
            self.avg_commands_per_pipeline()
        )?;
        writeln!(
            f,
            "  Pipeline Efficiency: {:.1}%",
            self.pipeline_efficiency() * 100.0
        )?;
        Ok(())
    }
}

/// Pipeline optimizer for analyzing and recommending pipeline strategies
pub struct PipelineOptimizer {
    metrics: PipelineMetrics,
}

impl Default for PipelineOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl PipelineOptimizer {
    /// Create a new pipeline optimizer
    pub fn new() -> Self {
        Self {
            metrics: PipelineMetrics::new(),
        }
    }

    /// Get the current metrics
    pub fn metrics(&self) -> &PipelineMetrics {
        &self.metrics
    }

    /// Record a pipelined operation
    pub fn record_pipelined(&mut self, commands: usize) {
        self.metrics.record_pipelined(commands);
    }

    /// Record a sequential operation
    pub fn record_sequential(&mut self) {
        self.metrics.record_sequential();
    }

    /// Analyze patterns and recommend optimal configuration
    pub fn recommend_config(&self) -> PipelineConfig {
        let avg_commands = self.metrics.avg_commands_per_pipeline();

        // If average is low, use adaptive strategy with lower threshold
        let strategy = if avg_commands < 5.0 {
            PipelineStrategy::Adaptive { threshold: 5 }
        } else if avg_commands < 20.0 {
            PipelineStrategy::Adaptive { threshold: 10 }
        } else {
            PipelineStrategy::Always
        };

        // Recommend batch size based on observed patterns
        let max_batch_size = if avg_commands > 0.0 {
            (avg_commands * 1.5) as usize
        } else {
            1000
        };

        PipelineConfig::new()
            .with_strategy(strategy)
            .with_max_batch_size(max_batch_size.max(100))
    }

    /// Reset the optimizer
    pub fn reset(&mut self) {
        self.metrics.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_strategy_default() {
        let strategy = PipelineStrategy::default();
        assert_eq!(strategy, PipelineStrategy::Adaptive { threshold: 10 });
    }

    #[test]
    fn test_pipeline_config_default() {
        let config = PipelineConfig::default();
        assert_eq!(
            config.strategy,
            PipelineStrategy::Adaptive { threshold: 10 }
        );
        assert_eq!(config.max_batch_size, 1000);
        assert!(!config.use_transactions);
        assert!(config.timeout.is_some());
    }

    #[test]
    fn test_pipeline_config_builder() {
        let config = PipelineConfig::new()
            .with_strategy(PipelineStrategy::Always)
            .with_max_batch_size(500)
            .with_transactions(true)
            .with_timeout(Duration::from_secs(60));

        assert_eq!(config.strategy, PipelineStrategy::Always);
        assert_eq!(config.max_batch_size, 500);
        assert!(config.use_transactions);
        assert_eq!(config.timeout, Some(Duration::from_secs(60)));
    }

    #[test]
    fn test_pipeline_config_without_timeout() {
        let config = PipelineConfig::new().without_timeout();
        assert!(config.timeout.is_none());
    }

    #[test]
    fn test_should_pipeline_always() {
        let config = PipelineConfig::new().with_strategy(PipelineStrategy::Always);
        assert!(config.should_pipeline(1));
        assert!(config.should_pipeline(10));
        assert!(config.should_pipeline(100));
    }

    #[test]
    fn test_should_pipeline_never() {
        let config = PipelineConfig::new().with_strategy(PipelineStrategy::Never);
        assert!(!config.should_pipeline(1));
        assert!(!config.should_pipeline(10));
        assert!(!config.should_pipeline(100));
    }

    #[test]
    fn test_should_pipeline_adaptive() {
        let config =
            PipelineConfig::new().with_strategy(PipelineStrategy::Adaptive { threshold: 10 });
        assert!(!config.should_pipeline(5));
        assert!(!config.should_pipeline(9));
        assert!(config.should_pipeline(10));
        assert!(config.should_pipeline(20));
    }

    #[test]
    fn test_calculate_batches() {
        let config = PipelineConfig::new()
            .with_strategy(PipelineStrategy::Always)
            .with_max_batch_size(100);

        assert_eq!(config.calculate_batches(50), 1);
        assert_eq!(config.calculate_batches(100), 1);
        assert_eq!(config.calculate_batches(150), 2);
        assert_eq!(config.calculate_batches(250), 3);
    }

    #[test]
    fn test_chunk_size() {
        let config = PipelineConfig::new()
            .with_strategy(PipelineStrategy::Always)
            .with_max_batch_size(100);

        assert_eq!(config.chunk_size(50), 50);
        assert_eq!(config.chunk_size(100), 100);
        assert_eq!(config.chunk_size(200), 100);
    }

    #[test]
    fn test_pipeline_metrics() {
        let mut metrics = PipelineMetrics::new();

        metrics.record_pipelined(10);
        metrics.record_pipelined(20);
        metrics.record_sequential();

        assert_eq!(metrics.pipelined_ops, 2);
        assert_eq!(metrics.sequential_ops, 1);
        assert_eq!(metrics.total_commands, 31);
        assert_eq!(metrics.total_batches, 2);
        assert_eq!(metrics.avg_commands_per_pipeline(), 15.0);
    }

    #[test]
    fn test_pipeline_metrics_reset() {
        let mut metrics = PipelineMetrics::new();
        metrics.record_pipelined(10);
        metrics.reset();

        assert_eq!(metrics.pipelined_ops, 0);
        assert_eq!(metrics.total_commands, 0);
    }

    #[test]
    fn test_pipeline_optimizer() {
        let mut optimizer = PipelineOptimizer::new();

        optimizer.record_pipelined(10);
        optimizer.record_pipelined(20);
        optimizer.record_sequential();

        assert_eq!(optimizer.metrics().pipelined_ops, 2);
        assert_eq!(optimizer.metrics().avg_commands_per_pipeline(), 15.0);
    }

    #[test]
    fn test_pipeline_optimizer_recommend() {
        let mut optimizer = PipelineOptimizer::new();

        // Record some high-volume operations
        optimizer.record_pipelined(50);
        optimizer.record_pipelined(60);
        optimizer.record_pipelined(70);

        let config = optimizer.recommend_config();
        assert_eq!(config.strategy, PipelineStrategy::Always);
        assert!(config.max_batch_size >= 100);
    }

    #[test]
    fn test_pipeline_optimizer_recommend_adaptive() {
        let mut optimizer = PipelineOptimizer::new();

        // Record some low-volume operations
        optimizer.record_pipelined(3);
        optimizer.record_pipelined(4);
        optimizer.record_sequential();

        let config = optimizer.recommend_config();
        matches!(config.strategy, PipelineStrategy::Adaptive { .. });
    }

    #[test]
    fn test_pipeline_metrics_display() {
        let mut metrics = PipelineMetrics::new();
        metrics.record_pipelined(10);

        let display = format!("{}", metrics);
        assert!(display.contains("Pipeline Metrics"));
        assert!(display.contains("Pipelined Operations: 1"));
    }
}
