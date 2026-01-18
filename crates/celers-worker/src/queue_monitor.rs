//! Queue depth monitoring and alerting
//!
//! Tracks queue size over time to detect backlog growth and trigger alerts.
//! Useful for auto-scaling and performance monitoring.
//!
//! # Features
//!
//! - **Real-time tracking**: Continuously monitor queue depth
//! - **Growth rate detection**: Calculate queue growth velocity
//! - **Alert thresholds**: Configure warning and critical levels
//! - **Historical data**: Track queue size over time windows
//! - **Metrics integration**: Export to Prometheus
//!
//! # Example
//!
//! ```rust
//! use celers_worker::queue_monitor::{QueueMonitor, QueueMonitorConfig};
//!
//! # async fn example() {
//! let config = QueueMonitorConfig {
//!     warning_threshold: 100,
//!     critical_threshold: 500,
//!     sample_interval_secs: 10,
//!     ..Default::default()
//! };
//!
//! let monitor = QueueMonitor::new(config);
//!
//! // Record queue size
//! monitor.record_depth(150).await;
//!
//! // Check if backlog is growing
//! if monitor.is_backlog_growing().await {
//!     println!("Queue is growing, consider scaling up!");
//! }
//! # }
//! ```

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Queue monitor configuration
#[derive(Debug, Clone)]
pub struct QueueMonitorConfig {
    /// Queue size threshold for warnings
    pub warning_threshold: usize,

    /// Queue size threshold for critical alerts
    pub critical_threshold: usize,

    /// Interval between samples in seconds
    pub sample_interval_secs: u64,

    /// Number of historical samples to keep
    pub history_size: usize,

    /// Growth rate threshold (tasks/sec) for alerts
    pub growth_rate_threshold: f64,

    /// Enable monitoring
    pub enabled: bool,
}

impl Default for QueueMonitorConfig {
    fn default() -> Self {
        Self {
            warning_threshold: 100,
            critical_threshold: 1000,
            sample_interval_secs: 30,
            history_size: 60, // 30 minutes of history at 30s intervals
            growth_rate_threshold: 10.0,
            enabled: true,
        }
    }
}

impl QueueMonitorConfig {
    /// Validate configuration
    pub fn is_valid(&self) -> bool {
        self.warning_threshold < self.critical_threshold
            && self.sample_interval_secs > 0
            && self.history_size > 0
    }

    /// Create a lenient configuration (high thresholds)
    pub fn lenient() -> Self {
        Self {
            warning_threshold: 1000,
            critical_threshold: 10000,
            ..Default::default()
        }
    }

    /// Create a strict configuration (low thresholds)
    pub fn strict() -> Self {
        Self {
            warning_threshold: 50,
            critical_threshold: 200,
            growth_rate_threshold: 5.0,
            ..Default::default()
        }
    }
}

/// Queue depth sample
#[derive(Debug, Clone, Copy)]
pub struct QueueSample {
    /// Queue depth at sample time
    pub depth: usize,

    /// Timestamp of sample (Unix epoch seconds)
    pub timestamp: u64,
}

impl QueueSample {
    fn new(depth: usize) -> Self {
        Self {
            depth,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
}

/// Queue alert level
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueAlertLevel {
    /// Queue size is normal
    Normal,
    /// Queue size exceeds warning threshold
    Warning,
    /// Queue size exceeds critical threshold
    Critical,
}

impl QueueAlertLevel {
    pub fn is_normal(&self) -> bool {
        matches!(self, QueueAlertLevel::Normal)
    }

    pub fn is_warning(&self) -> bool {
        matches!(self, QueueAlertLevel::Warning)
    }

    pub fn is_critical(&self) -> bool {
        matches!(self, QueueAlertLevel::Critical)
    }
}

impl std::fmt::Display for QueueAlertLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueueAlertLevel::Normal => write!(f, "Normal"),
            QueueAlertLevel::Warning => write!(f, "Warning"),
            QueueAlertLevel::Critical => write!(f, "Critical"),
        }
    }
}

/// Queue monitor statistics
#[derive(Debug, Clone)]
pub struct QueueStats {
    /// Current queue depth
    pub current_depth: usize,

    /// Average depth over recent history
    pub avg_depth: f64,

    /// Maximum depth in recent history
    pub max_depth: usize,

    /// Minimum depth in recent history
    pub min_depth: usize,

    /// Growth rate (tasks per second, positive = growing)
    pub growth_rate: f64,

    /// Current alert level
    pub alert_level: QueueAlertLevel,

    /// Number of samples in history
    pub sample_count: usize,
}

/// Queue depth monitor
pub struct QueueMonitor {
    config: QueueMonitorConfig,
    history: Arc<RwLock<VecDeque<QueueSample>>>,
    last_alert_level: Arc<RwLock<QueueAlertLevel>>,
}

impl QueueMonitor {
    /// Create a new queue monitor
    pub fn new(config: QueueMonitorConfig) -> Self {
        if !config.is_valid() {
            warn!("Invalid queue monitor configuration");
        }

        let history_size = config.history_size;

        Self {
            config,
            history: Arc::new(RwLock::new(VecDeque::with_capacity(history_size))),
            last_alert_level: Arc::new(RwLock::new(QueueAlertLevel::Normal)),
        }
    }

    /// Create with default configuration
    pub fn with_defaults() -> Self {
        Self::new(QueueMonitorConfig::default())
    }

    /// Record current queue depth
    pub async fn record_depth(&self, depth: usize) {
        if !self.config.enabled {
            return;
        }

        let sample = QueueSample::new(depth);
        let mut history = self.history.write().await;

        // Add new sample
        history.push_back(sample);

        // Trim to max history size
        if history.len() > self.config.history_size {
            history.pop_front();
        }

        debug!("Recorded queue depth: {} tasks", depth);

        // Check alert thresholds
        let alert_level = self.calculate_alert_level(depth);
        let mut last_level = self.last_alert_level.write().await;

        if alert_level != *last_level {
            match alert_level {
                QueueAlertLevel::Normal => {
                    info!("Queue depth returned to normal: {}", depth);
                }
                QueueAlertLevel::Warning => {
                    warn!(
                        "Queue depth WARNING: {} tasks (threshold: {})",
                        depth, self.config.warning_threshold
                    );
                }
                QueueAlertLevel::Critical => {
                    warn!(
                        "Queue depth CRITICAL: {} tasks (threshold: {})",
                        depth, self.config.critical_threshold
                    );
                }
            }
            *last_level = alert_level;
        }

        #[cfg(feature = "metrics")]
        {
            use celers_metrics::QUEUE_SIZE;
            QUEUE_SIZE.set(depth as f64);
        }
    }

    /// Get current queue statistics
    pub async fn get_stats(&self) -> QueueStats {
        let history = self.history.read().await;

        if history.is_empty() {
            return QueueStats {
                current_depth: 0,
                avg_depth: 0.0,
                max_depth: 0,
                min_depth: 0,
                growth_rate: 0.0,
                alert_level: QueueAlertLevel::Normal,
                sample_count: 0,
            };
        }

        let current_depth = history.back().map(|s| s.depth).unwrap_or(0);
        let depths: Vec<usize> = history.iter().map(|s| s.depth).collect();

        let avg_depth = depths.iter().sum::<usize>() as f64 / depths.len() as f64;
        let max_depth = *depths.iter().max().unwrap_or(&0);
        let min_depth = *depths.iter().min().unwrap_or(&0);

        let growth_rate = self.calculate_growth_rate(&history);
        let alert_level = self.calculate_alert_level(current_depth);

        QueueStats {
            current_depth,
            avg_depth,
            max_depth,
            min_depth,
            growth_rate,
            alert_level,
            sample_count: history.len(),
        }
    }

    /// Check if queue backlog is growing
    pub async fn is_backlog_growing(&self) -> bool {
        let stats = self.get_stats().await;
        stats.growth_rate > self.config.growth_rate_threshold
    }

    /// Check if queue is in warning or critical state
    pub async fn needs_attention(&self) -> bool {
        let level = self.last_alert_level.read().await;
        !level.is_normal()
    }

    /// Get current alert level
    pub async fn alert_level(&self) -> QueueAlertLevel {
        *self.last_alert_level.read().await
    }

    /// Clear history (useful for testing)
    pub async fn clear_history(&self) {
        let mut history = self.history.write().await;
        history.clear();
        debug!("Cleared queue depth history");
    }

    /// Calculate alert level based on depth
    fn calculate_alert_level(&self, depth: usize) -> QueueAlertLevel {
        if depth >= self.config.critical_threshold {
            QueueAlertLevel::Critical
        } else if depth >= self.config.warning_threshold {
            QueueAlertLevel::Warning
        } else {
            QueueAlertLevel::Normal
        }
    }

    /// Calculate queue growth rate (tasks per second)
    fn calculate_growth_rate(&self, history: &VecDeque<QueueSample>) -> f64 {
        if history.len() < 2 {
            return 0.0;
        }

        let first = history.front().unwrap();
        let last = history.back().unwrap();

        let depth_change = last.depth as i64 - first.depth as i64;
        let time_change = last.timestamp.saturating_sub(first.timestamp) as f64;

        if time_change > 0.0 {
            depth_change as f64 / time_change
        } else {
            0.0
        }
    }
}

impl Default for QueueMonitor {
    fn default() -> Self {
        Self::with_defaults()
    }
}

impl Clone for QueueMonitor {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            history: Arc::clone(&self.history),
            last_alert_level: Arc::clone(&self.last_alert_level),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[test]
    fn test_queue_monitor_config_default() {
        let config = QueueMonitorConfig::default();
        assert_eq!(config.warning_threshold, 100);
        assert_eq!(config.critical_threshold, 1000);
        assert!(config.enabled);
        assert!(config.is_valid());
    }

    #[test]
    fn test_queue_monitor_config_validation() {
        let valid = QueueMonitorConfig::default();
        assert!(valid.is_valid());

        let invalid = QueueMonitorConfig {
            warning_threshold: 1000,
            critical_threshold: 100, // Invalid: critical < warning
            ..Default::default()
        };
        assert!(!invalid.is_valid());
    }

    #[test]
    fn test_queue_monitor_config_presets() {
        let lenient = QueueMonitorConfig::lenient();
        assert_eq!(lenient.warning_threshold, 1000);

        let strict = QueueMonitorConfig::strict();
        assert_eq!(strict.warning_threshold, 50);
    }

    #[test]
    fn test_queue_alert_level() {
        assert!(QueueAlertLevel::Normal.is_normal());
        assert!(!QueueAlertLevel::Normal.is_warning());
        assert!(!QueueAlertLevel::Normal.is_critical());

        assert!(QueueAlertLevel::Warning.is_warning());
        assert!(QueueAlertLevel::Critical.is_critical());
    }

    #[tokio::test]
    async fn test_queue_monitor_record_depth() {
        let config = QueueMonitorConfig::default();
        let monitor = QueueMonitor::new(config);

        monitor.record_depth(50).await;
        let stats = monitor.get_stats().await;

        assert_eq!(stats.current_depth, 50);
        assert_eq!(stats.sample_count, 1);
        assert_eq!(stats.alert_level, QueueAlertLevel::Normal);
    }

    #[tokio::test]
    async fn test_queue_monitor_alert_levels() {
        let config = QueueMonitorConfig {
            warning_threshold: 100,
            critical_threshold: 500,
            ..Default::default()
        };
        let monitor = QueueMonitor::new(config);

        // Normal
        monitor.record_depth(50).await;
        assert_eq!(monitor.alert_level().await, QueueAlertLevel::Normal);

        // Warning
        monitor.record_depth(150).await;
        assert_eq!(monitor.alert_level().await, QueueAlertLevel::Warning);

        // Critical
        monitor.record_depth(600).await;
        assert_eq!(monitor.alert_level().await, QueueAlertLevel::Critical);

        // Back to normal
        monitor.record_depth(50).await;
        assert_eq!(monitor.alert_level().await, QueueAlertLevel::Normal);
    }

    #[tokio::test]
    async fn test_queue_monitor_stats() {
        let monitor = QueueMonitor::with_defaults();

        monitor.record_depth(10).await;
        monitor.record_depth(20).await;
        monitor.record_depth(30).await;

        let stats = monitor.get_stats().await;
        assert_eq!(stats.current_depth, 30);
        assert_eq!(stats.avg_depth, 20.0);
        assert_eq!(stats.max_depth, 30);
        assert_eq!(stats.min_depth, 10);
        assert_eq!(stats.sample_count, 3);
    }

    #[tokio::test]
    async fn test_queue_monitor_growth_rate() {
        let config = QueueMonitorConfig {
            sample_interval_secs: 1,
            ..Default::default()
        };
        let monitor = QueueMonitor::new(config);

        monitor.record_depth(100).await;
        sleep(Duration::from_secs(1)).await;
        monitor.record_depth(110).await;

        let stats = monitor.get_stats().await;
        // Growth rate should be approximately 10 tasks/sec
        assert!(stats.growth_rate >= 0.0);
    }

    #[tokio::test]
    async fn test_queue_monitor_is_backlog_growing() {
        let config = QueueMonitorConfig {
            growth_rate_threshold: 5.0,
            ..Default::default()
        };
        let monitor = QueueMonitor::new(config);

        monitor.record_depth(100).await;
        sleep(Duration::from_millis(100)).await;
        monitor.record_depth(100).await;

        // Stable queue
        assert!(!monitor.is_backlog_growing().await);
    }

    #[tokio::test]
    async fn test_queue_monitor_needs_attention() {
        let config = QueueMonitorConfig {
            warning_threshold: 100,
            ..Default::default()
        };
        let monitor = QueueMonitor::new(config);

        monitor.record_depth(50).await;
        assert!(!monitor.needs_attention().await);

        monitor.record_depth(150).await;
        assert!(monitor.needs_attention().await);
    }

    #[tokio::test]
    async fn test_queue_monitor_clear_history() {
        let monitor = QueueMonitor::with_defaults();

        monitor.record_depth(100).await;
        monitor.record_depth(200).await;

        let stats = monitor.get_stats().await;
        assert_eq!(stats.sample_count, 2);

        monitor.clear_history().await;
        let stats = monitor.get_stats().await;
        assert_eq!(stats.sample_count, 0);
    }

    #[tokio::test]
    async fn test_queue_monitor_history_limit() {
        let config = QueueMonitorConfig {
            history_size: 3,
            ..Default::default()
        };
        let monitor = QueueMonitor::new(config);

        monitor.record_depth(10).await;
        monitor.record_depth(20).await;
        monitor.record_depth(30).await;
        monitor.record_depth(40).await; // Should evict first sample

        let stats = monitor.get_stats().await;
        assert_eq!(stats.sample_count, 3);
        assert_eq!(stats.min_depth, 20); // 10 was evicted
    }

    #[tokio::test]
    async fn test_queue_monitor_disabled() {
        let config = QueueMonitorConfig {
            enabled: false,
            ..Default::default()
        };
        let monitor = QueueMonitor::new(config);

        monitor.record_depth(100).await;
        let stats = monitor.get_stats().await;
        assert_eq!(stats.sample_count, 0);
    }
}
