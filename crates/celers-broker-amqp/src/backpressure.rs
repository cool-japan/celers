//! Backpressure management for AMQP message flow control
//!
//! This module provides mechanisms to prevent overwhelming the AMQP broker or consumers
//! by implementing intelligent flow control strategies.
//!
//! # Features
//!
//! - **Dynamic Prefetch Adjustment** - Automatically adjust QoS prefetch based on consumer lag
//! - **Flow Control** - Pause/resume message consumption based on system load
//! - **Adaptive Throttling** - Reduce message publishing rate under high load
//! - **Queue Depth Monitoring** - Track queue depth and trigger backpressure
//! - **Consumer Capacity Tracking** - Monitor consumer processing capacity
//!
//! # Example
//!
//! ```rust
//! use celers_broker_amqp::backpressure::{BackpressureManager, BackpressureConfig};
//! use std::time::Duration;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = BackpressureConfig {
//!     max_queue_depth: 10_000,
//!     warning_threshold: 0.7,
//!     critical_threshold: 0.9,
//!     check_interval: Duration::from_secs(5),
//!     min_prefetch: 1,
//!     max_prefetch: 100,
//!     prefetch_adjustment_factor: 0.2,
//! };
//!
//! let mut manager = BackpressureManager::new(config);
//!
//! // Update queue metrics
//! manager.update_queue_depth(5000);
//! manager.update_consumer_lag(200);
//!
//! // Check if backpressure should be applied
//! if manager.should_apply_backpressure() {
//!     println!("Applying backpressure!");
//!     let recommended_prefetch = manager.calculate_optimal_prefetch();
//!     println!("Recommended prefetch: {}", recommended_prefetch);
//! }
//! # Ok(())
//! # }
//! ```

use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

/// Backpressure configuration
#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    /// Maximum acceptable queue depth before triggering backpressure
    pub max_queue_depth: u64,
    /// Warning threshold (0.0 - 1.0) relative to max_queue_depth
    pub warning_threshold: f64,
    /// Critical threshold (0.0 - 1.0) relative to max_queue_depth
    pub critical_threshold: f64,
    /// Interval between backpressure checks
    pub check_interval: Duration,
    /// Minimum prefetch count
    pub min_prefetch: u16,
    /// Maximum prefetch count
    pub max_prefetch: u16,
    /// Factor for adjusting prefetch (0.0 - 1.0)
    pub prefetch_adjustment_factor: f64,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            max_queue_depth: 10_000,
            warning_threshold: 0.7,
            critical_threshold: 0.9,
            check_interval: Duration::from_secs(5),
            min_prefetch: 1,
            max_prefetch: 100,
            prefetch_adjustment_factor: 0.2,
        }
    }
}

/// Backpressure level
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum BackpressureLevel {
    /// No backpressure needed
    #[default]
    None,
    /// Warning level - queue filling up
    Warning,
    /// Critical level - immediate action needed
    Critical,
}

/// Backpressure manager for flow control
#[derive(Debug)]
pub struct BackpressureManager {
    config: BackpressureConfig,
    current_queue_depth: u64,
    consumer_lag: u64,
    current_prefetch: u16,
    last_check: Instant,
    backpressure_triggered: bool,
    stats: BackpressureStats,
}

/// Backpressure statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BackpressureStats {
    /// Number of times backpressure was triggered
    pub trigger_count: u64,
    /// Number of prefetch adjustments made
    pub prefetch_adjustments: u64,
    /// Total time under backpressure (seconds)
    pub total_backpressure_time: f64,
    /// Current backpressure level
    pub current_level: BackpressureLevel,
    /// Average queue depth
    pub avg_queue_depth: f64,
    /// Peak queue depth observed
    pub peak_queue_depth: u64,
}

impl BackpressureManager {
    /// Create a new backpressure manager
    pub fn new(config: BackpressureConfig) -> Self {
        Self {
            current_prefetch: config.max_prefetch,
            config,
            current_queue_depth: 0,
            consumer_lag: 0,
            last_check: Instant::now(),
            backpressure_triggered: false,
            stats: BackpressureStats::default(),
        }
    }

    /// Update current queue depth
    pub fn update_queue_depth(&mut self, depth: u64) {
        self.current_queue_depth = depth;

        // Update stats
        if depth > self.stats.peak_queue_depth {
            self.stats.peak_queue_depth = depth;
        }

        // Update average (simple moving average)
        if self.stats.avg_queue_depth == 0.0 {
            self.stats.avg_queue_depth = depth as f64;
        } else {
            self.stats.avg_queue_depth = self.stats.avg_queue_depth * 0.9 + (depth as f64) * 0.1;
        }
    }

    /// Update consumer lag
    pub fn update_consumer_lag(&mut self, lag: u64) {
        self.consumer_lag = lag;
    }

    /// Check if backpressure should be applied
    pub fn should_apply_backpressure(&mut self) -> bool {
        let level = self.calculate_backpressure_level();

        match level {
            BackpressureLevel::None => {
                if self.backpressure_triggered {
                    self.backpressure_triggered = false;
                }
                false
            }
            BackpressureLevel::Warning | BackpressureLevel::Critical => {
                if !self.backpressure_triggered {
                    self.backpressure_triggered = true;
                    self.stats.trigger_count += 1;
                }
                true
            }
        }
    }

    /// Calculate current backpressure level
    pub fn calculate_backpressure_level(&mut self) -> BackpressureLevel {
        let depth_ratio = self.current_queue_depth as f64 / self.config.max_queue_depth as f64;

        let level = if depth_ratio >= self.config.critical_threshold {
            BackpressureLevel::Critical
        } else if depth_ratio >= self.config.warning_threshold {
            BackpressureLevel::Warning
        } else {
            BackpressureLevel::None
        };

        self.stats.current_level = level;
        level
    }

    /// Calculate optimal prefetch count based on current conditions
    pub fn calculate_optimal_prefetch(&mut self) -> u16 {
        let level = self.calculate_backpressure_level();

        let new_prefetch = match level {
            BackpressureLevel::None => {
                // Gradually increase prefetch when no backpressure
                let increase = (self.current_prefetch as f64
                    * (1.0 + self.config.prefetch_adjustment_factor))
                    as u16;
                increase.min(self.config.max_prefetch)
            }
            BackpressureLevel::Warning => {
                // Moderate reduction
                let decrease = (self.current_prefetch as f64
                    * (1.0 - self.config.prefetch_adjustment_factor))
                    as u16;
                decrease.max(self.config.min_prefetch)
            }
            BackpressureLevel::Critical => {
                // Aggressive reduction
                let decrease = (self.current_prefetch as f64
                    * (1.0 - self.config.prefetch_adjustment_factor * 2.0))
                    as u16;
                decrease.max(self.config.min_prefetch)
            }
        };

        if new_prefetch != self.current_prefetch {
            self.stats.prefetch_adjustments += 1;
            self.current_prefetch = new_prefetch;
        }

        new_prefetch
    }

    /// Get recommended action based on backpressure level
    pub fn get_recommended_action(&self) -> BackpressureAction {
        match self.stats.current_level {
            BackpressureLevel::None => BackpressureAction::Continue,
            BackpressureLevel::Warning => BackpressureAction::ReduceRate {
                factor: 0.5,
                recommended_prefetch: self.current_prefetch,
            },
            BackpressureLevel::Critical => BackpressureAction::PauseConsumption {
                duration: Duration::from_secs(5),
                recommended_prefetch: self.config.min_prefetch,
            },
        }
    }

    /// Check if it's time for a backpressure check
    pub fn should_check(&self) -> bool {
        self.last_check.elapsed() >= self.config.check_interval
    }

    /// Perform a backpressure check and return recommended action
    pub fn check(&mut self) -> BackpressureAction {
        self.last_check = Instant::now();

        if self.should_apply_backpressure() {
            self.get_recommended_action()
        } else {
            BackpressureAction::Continue
        }
    }

    /// Get current statistics
    pub fn stats(&self) -> &BackpressureStats {
        &self.stats
    }

    /// Reset statistics
    pub fn reset_stats(&mut self) {
        self.stats = BackpressureStats::default();
    }

    /// Get current queue depth utilization (0.0 - 1.0)
    pub fn queue_utilization(&self) -> f64 {
        self.current_queue_depth as f64 / self.config.max_queue_depth as f64
    }

    /// Check if system is healthy (no backpressure)
    pub fn is_healthy(&self) -> bool {
        matches!(self.stats.current_level, BackpressureLevel::None)
    }
}

/// Recommended action based on backpressure level
#[derive(Debug, Clone, PartialEq)]
pub enum BackpressureAction {
    /// Continue normal operation
    Continue,
    /// Reduce publishing/consumption rate
    ReduceRate {
        /// Factor to reduce rate by (0.0 - 1.0)
        factor: f64,
        /// Recommended prefetch count
        recommended_prefetch: u16,
    },
    /// Pause consumption temporarily
    PauseConsumption {
        /// Duration to pause
        duration: Duration,
        /// Recommended prefetch when resuming
        recommended_prefetch: u16,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backpressure_manager_creation() {
        let config = BackpressureConfig::default();
        let manager = BackpressureManager::new(config);
        assert!(!manager.backpressure_triggered);
        assert_eq!(manager.stats.current_level, BackpressureLevel::None);
    }

    #[test]
    fn test_backpressure_levels() {
        let config = BackpressureConfig {
            max_queue_depth: 1000,
            warning_threshold: 0.7,
            critical_threshold: 0.9,
            ..Default::default()
        };
        let mut manager = BackpressureManager::new(config);

        // No backpressure
        manager.update_queue_depth(500);
        assert_eq!(
            manager.calculate_backpressure_level(),
            BackpressureLevel::None
        );

        // Warning level
        manager.update_queue_depth(750);
        assert_eq!(
            manager.calculate_backpressure_level(),
            BackpressureLevel::Warning
        );

        // Critical level
        manager.update_queue_depth(950);
        assert_eq!(
            manager.calculate_backpressure_level(),
            BackpressureLevel::Critical
        );
    }

    #[test]
    fn test_prefetch_adjustment() {
        let config = BackpressureConfig {
            max_queue_depth: 1000,
            min_prefetch: 1,
            max_prefetch: 100,
            prefetch_adjustment_factor: 0.2,
            ..Default::default()
        };
        let mut manager = BackpressureManager::new(config);

        // Critical level should reduce prefetch
        manager.update_queue_depth(950);
        let prefetch = manager.calculate_optimal_prefetch();
        assert!(prefetch < 100);

        // No backpressure should increase prefetch
        manager.update_queue_depth(100);
        let new_prefetch = manager.calculate_optimal_prefetch();
        assert!(new_prefetch >= prefetch);
    }

    #[test]
    fn test_queue_utilization() {
        let config = BackpressureConfig {
            max_queue_depth: 1000,
            ..Default::default()
        };
        let mut manager = BackpressureManager::new(config);

        manager.update_queue_depth(500);
        assert_eq!(manager.queue_utilization(), 0.5);

        manager.update_queue_depth(750);
        assert_eq!(manager.queue_utilization(), 0.75);
    }

    #[test]
    fn test_stats_tracking() {
        let config = BackpressureConfig::default();
        let mut manager = BackpressureManager::new(config);

        assert_eq!(manager.stats.trigger_count, 0);

        // Trigger backpressure
        manager.update_queue_depth(8000);
        manager.should_apply_backpressure();

        assert_eq!(manager.stats.trigger_count, 1);
        assert!(manager.stats.peak_queue_depth >= 8000);
    }

    #[test]
    fn test_recommended_actions() {
        let config = BackpressureConfig::default();
        let mut manager = BackpressureManager::new(config);

        // No backpressure
        manager.update_queue_depth(1000);
        match manager.get_recommended_action() {
            BackpressureAction::Continue => {}
            _ => panic!("Expected Continue action"),
        }

        // Warning level
        manager.update_queue_depth(7500);
        manager.calculate_backpressure_level();
        match manager.get_recommended_action() {
            BackpressureAction::ReduceRate { .. } => {}
            _ => panic!("Expected ReduceRate action"),
        }

        // Critical level
        manager.update_queue_depth(9500);
        manager.calculate_backpressure_level();
        match manager.get_recommended_action() {
            BackpressureAction::PauseConsumption { .. } => {}
            _ => panic!("Expected PauseConsumption action"),
        }
    }

    #[test]
    fn test_health_check() {
        let config = BackpressureConfig::default();
        let mut manager = BackpressureManager::new(config);

        manager.update_queue_depth(1000);
        assert!(manager.is_healthy());

        manager.update_queue_depth(8000);
        manager.calculate_backpressure_level();
        assert!(!manager.is_healthy());
    }
}
