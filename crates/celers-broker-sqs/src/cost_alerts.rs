//! Cost alert system for AWS SQS budget monitoring
//!
//! This module provides automatic cost monitoring and alerting when
//! AWS SQS costs exceed configured thresholds.
//!
//! # Features
//!
//! - Real-time cost tracking with threshold alerts
//! - Multiple alert levels (Warning, Critical)
//! - Configurable alert callbacks
//! - Integration with cost_tracker module
//! - Daily and monthly budget monitoring
//! - Alert history and statistics
//!
//! # Example
//!
//! ```
//! use celers_broker_sqs::cost_alerts::{CostAlertSystem, CostAlertConfig, AlertLevel};
//! use std::sync::Arc;
//!
//! # fn main() {
//! // Configure cost alerts
//! let config = CostAlertConfig::new()
//!     .with_daily_warning_threshold(5.0)    // Warn at $5/day
//!     .with_daily_critical_threshold(10.0)   // Critical at $10/day
//!     .with_monthly_warning_threshold(100.0) // Warn at $100/month
//!     .with_monthly_critical_threshold(200.0); // Critical at $200/month
//!
//! let mut alert_system = CostAlertSystem::new(config);
//!
//! // Register alert callback
//! alert_system.register_callback(Arc::new(|alert| {
//!     println!("COST ALERT: {} - ${:.2}", alert.level, alert.amount_usd);
//! }));
//!
//! // Track costs (typically integrated with CostTracker)
//! alert_system.track_cost(0.0004); // Track single request cost
//!
//! // Check if within budget
//! if !alert_system.is_within_budget() {
//!     println!("Budget exceeded!");
//! }
//! # }
//! ```

use std::sync::{Arc, Mutex};
use std::time::SystemTime;

/// Alert level
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertLevel {
    /// Warning level - approaching threshold
    Warning,

    /// Critical level - exceeded threshold
    Critical,
}

impl std::fmt::Display for AlertLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlertLevel::Warning => write!(f, "WARNING"),
            AlertLevel::Critical => write!(f, "CRITICAL"),
        }
    }
}

/// Cost alert information
#[derive(Debug, Clone)]
pub struct CostAlert {
    /// Alert level
    pub level: AlertLevel,

    /// Budget type (Daily or Monthly)
    pub budget_type: BudgetType,

    /// Current amount (USD)
    pub amount_usd: f64,

    /// Threshold that was exceeded (USD)
    pub threshold_usd: f64,

    /// Alert message
    pub message: String,

    /// Timestamp when alert was triggered
    pub timestamp: SystemTime,
}

/// Budget type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetType {
    /// Daily budget
    Daily,

    /// Monthly budget
    Monthly,
}

impl std::fmt::Display for BudgetType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BudgetType::Daily => write!(f, "Daily"),
            BudgetType::Monthly => write!(f, "Monthly"),
        }
    }
}

/// Configuration for cost alerts
#[derive(Debug, Clone)]
pub struct CostAlertConfig {
    /// Daily warning threshold (USD)
    pub daily_warning_threshold: Option<f64>,

    /// Daily critical threshold (USD)
    pub daily_critical_threshold: Option<f64>,

    /// Monthly warning threshold (USD)
    pub monthly_warning_threshold: Option<f64>,

    /// Monthly critical threshold (USD)
    pub monthly_critical_threshold: Option<f64>,

    /// Enable alert deduplication (prevents repeated alerts)
    pub enable_deduplication: bool,

    /// Deduplication window (in seconds)
    pub deduplication_window_secs: u64,
}

impl CostAlertConfig {
    /// Create new configuration
    pub fn new() -> Self {
        Self {
            daily_warning_threshold: None,
            daily_critical_threshold: None,
            monthly_warning_threshold: None,
            monthly_critical_threshold: None,
            enable_deduplication: true,
            deduplication_window_secs: 300, // 5 minutes
        }
    }

    /// Set daily warning threshold
    pub fn with_daily_warning_threshold(mut self, threshold: f64) -> Self {
        self.daily_warning_threshold = Some(threshold);
        self
    }

    /// Set daily critical threshold
    pub fn with_daily_critical_threshold(mut self, threshold: f64) -> Self {
        self.daily_critical_threshold = Some(threshold);
        self
    }

    /// Set monthly warning threshold
    pub fn with_monthly_warning_threshold(mut self, threshold: f64) -> Self {
        self.monthly_warning_threshold = Some(threshold);
        self
    }

    /// Set monthly critical threshold
    pub fn with_monthly_critical_threshold(mut self, threshold: f64) -> Self {
        self.monthly_critical_threshold = Some(threshold);
        self
    }

    /// Enable or disable alert deduplication
    pub fn with_deduplication(mut self, enabled: bool) -> Self {
        self.enable_deduplication = enabled;
        self
    }
}

impl Default for CostAlertConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Alert callback function type
pub type AlertCallback = Arc<dyn Fn(&CostAlert) + Send + Sync>;

/// Alert history entry
#[derive(Debug, Clone)]
pub struct AlertHistory {
    /// Alert level
    pub level: AlertLevel,

    /// Budget type
    pub budget_type: BudgetType,

    /// Timestamp when alert was triggered
    pub timestamp: SystemTime,
}

/// Cost alert system
pub struct CostAlertSystem {
    config: CostAlertConfig,
    daily_cost: Arc<Mutex<f64>>,
    monthly_cost: Arc<Mutex<f64>>,
    callbacks: Arc<Mutex<Vec<AlertCallback>>>,
    alert_history: Arc<Mutex<Vec<AlertHistory>>>,
    last_daily_warning: Arc<Mutex<Option<SystemTime>>>,
    last_daily_critical: Arc<Mutex<Option<SystemTime>>>,
    last_monthly_warning: Arc<Mutex<Option<SystemTime>>>,
    last_monthly_critical: Arc<Mutex<Option<SystemTime>>>,
}

impl CostAlertSystem {
    /// Create new cost alert system
    pub fn new(config: CostAlertConfig) -> Self {
        Self {
            config,
            daily_cost: Arc::new(Mutex::new(0.0)),
            monthly_cost: Arc::new(Mutex::new(0.0)),
            callbacks: Arc::new(Mutex::new(Vec::new())),
            alert_history: Arc::new(Mutex::new(Vec::new())),
            last_daily_warning: Arc::new(Mutex::new(None)),
            last_daily_critical: Arc::new(Mutex::new(None)),
            last_monthly_warning: Arc::new(Mutex::new(None)),
            last_monthly_critical: Arc::new(Mutex::new(None)),
        }
    }

    /// Register an alert callback
    pub fn register_callback(&mut self, callback: AlertCallback) {
        self.callbacks.lock().unwrap().push(callback);
    }

    /// Track a cost and check for alerts
    pub fn track_cost(&mut self, cost_usd: f64) {
        // Update costs
        {
            let mut daily = self.daily_cost.lock().unwrap();
            *daily += cost_usd;
        }
        {
            let mut monthly = self.monthly_cost.lock().unwrap();
            *monthly += cost_usd;
        }

        // Check thresholds
        self.check_daily_thresholds();
        self.check_monthly_thresholds();
    }

    /// Check daily cost thresholds
    fn check_daily_thresholds(&self) {
        let daily_cost = *self.daily_cost.lock().unwrap();

        // Check critical threshold
        if let Some(threshold) = self.config.daily_critical_threshold {
            if daily_cost >= threshold {
                if self.should_alert(&self.last_daily_critical) {
                    self.trigger_alert(CostAlert {
                        level: AlertLevel::Critical,
                        budget_type: BudgetType::Daily,
                        amount_usd: daily_cost,
                        threshold_usd: threshold,
                        message: format!(
                            "Daily cost ${:.2} exceeded critical threshold ${:.2}",
                            daily_cost, threshold
                        ),
                        timestamp: SystemTime::now(),
                    });
                    *self.last_daily_critical.lock().unwrap() = Some(SystemTime::now());
                }
                return;
            }
        }

        // Check warning threshold
        if let Some(threshold) = self.config.daily_warning_threshold {
            if daily_cost >= threshold && self.should_alert(&self.last_daily_warning) {
                self.trigger_alert(CostAlert {
                    level: AlertLevel::Warning,
                    budget_type: BudgetType::Daily,
                    amount_usd: daily_cost,
                    threshold_usd: threshold,
                    message: format!(
                        "Daily cost ${:.2} exceeded warning threshold ${:.2}",
                        daily_cost, threshold
                    ),
                    timestamp: SystemTime::now(),
                });
                *self.last_daily_warning.lock().unwrap() = Some(SystemTime::now());
            }
        }
    }

    /// Check monthly cost thresholds
    fn check_monthly_thresholds(&self) {
        let monthly_cost = *self.monthly_cost.lock().unwrap();

        // Check critical threshold
        if let Some(threshold) = self.config.monthly_critical_threshold {
            if monthly_cost >= threshold {
                if self.should_alert(&self.last_monthly_critical) {
                    self.trigger_alert(CostAlert {
                        level: AlertLevel::Critical,
                        budget_type: BudgetType::Monthly,
                        amount_usd: monthly_cost,
                        threshold_usd: threshold,
                        message: format!(
                            "Monthly cost ${:.2} exceeded critical threshold ${:.2}",
                            monthly_cost, threshold
                        ),
                        timestamp: SystemTime::now(),
                    });
                    *self.last_monthly_critical.lock().unwrap() = Some(SystemTime::now());
                }
                return;
            }
        }

        // Check warning threshold
        if let Some(threshold) = self.config.monthly_warning_threshold {
            if monthly_cost >= threshold && self.should_alert(&self.last_monthly_warning) {
                self.trigger_alert(CostAlert {
                    level: AlertLevel::Warning,
                    budget_type: BudgetType::Monthly,
                    amount_usd: monthly_cost,
                    threshold_usd: threshold,
                    message: format!(
                        "Monthly cost ${:.2} exceeded warning threshold ${:.2}",
                        monthly_cost, threshold
                    ),
                    timestamp: SystemTime::now(),
                });
                *self.last_monthly_warning.lock().unwrap() = Some(SystemTime::now());
            }
        }
    }

    /// Check if we should trigger an alert (considering deduplication)
    fn should_alert(&self, last_alert: &Arc<Mutex<Option<SystemTime>>>) -> bool {
        if !self.config.enable_deduplication {
            return true;
        }

        if let Some(last_time) = *last_alert.lock().unwrap() {
            if let Ok(elapsed) = SystemTime::now().duration_since(last_time) {
                return elapsed.as_secs() >= self.config.deduplication_window_secs;
            }
        }

        true
    }

    /// Trigger an alert
    fn trigger_alert(&self, alert: CostAlert) {
        // Add to history
        self.alert_history.lock().unwrap().push(AlertHistory {
            level: alert.level,
            budget_type: alert.budget_type,
            timestamp: alert.timestamp,
        });

        // Call all registered callbacks
        let callbacks = self.callbacks.lock().unwrap();
        for callback in callbacks.iter() {
            callback(&alert);
        }
    }

    /// Check if currently within budget (no critical alerts)
    pub fn is_within_budget(&self) -> bool {
        let daily_cost = *self.daily_cost.lock().unwrap();
        let monthly_cost = *self.monthly_cost.lock().unwrap();

        // Check daily critical
        if let Some(threshold) = self.config.daily_critical_threshold {
            if daily_cost >= threshold {
                return false;
            }
        }

        // Check monthly critical
        if let Some(threshold) = self.config.monthly_critical_threshold {
            if monthly_cost >= threshold {
                return false;
            }
        }

        true
    }

    /// Get current daily cost
    pub fn daily_cost(&self) -> f64 {
        *self.daily_cost.lock().unwrap()
    }

    /// Get current monthly cost
    pub fn monthly_cost(&self) -> f64 {
        *self.monthly_cost.lock().unwrap()
    }

    /// Reset daily costs (call at start of each day)
    pub fn reset_daily_costs(&mut self) {
        *self.daily_cost.lock().unwrap() = 0.0;
        *self.last_daily_warning.lock().unwrap() = None;
        *self.last_daily_critical.lock().unwrap() = None;
    }

    /// Reset monthly costs (call at start of each month)
    pub fn reset_monthly_costs(&mut self) {
        *self.monthly_cost.lock().unwrap() = 0.0;
        *self.last_monthly_warning.lock().unwrap() = None;
        *self.last_monthly_critical.lock().unwrap() = None;
    }

    /// Get alert history
    pub fn alert_history(&self) -> Vec<AlertHistory> {
        self.alert_history.lock().unwrap().clone()
    }

    /// Get alert statistics
    pub fn alert_statistics(&self) -> AlertStatistics {
        let history = self.alert_history.lock().unwrap();

        let total_alerts = history.len();
        let warning_alerts = history
            .iter()
            .filter(|a| a.level == AlertLevel::Warning)
            .count();
        let critical_alerts = history
            .iter()
            .filter(|a| a.level == AlertLevel::Critical)
            .count();
        let daily_alerts = history
            .iter()
            .filter(|a| a.budget_type == BudgetType::Daily)
            .count();
        let monthly_alerts = history
            .iter()
            .filter(|a| a.budget_type == BudgetType::Monthly)
            .count();

        AlertStatistics {
            total_alerts,
            warning_alerts,
            critical_alerts,
            daily_alerts,
            monthly_alerts,
        }
    }

    /// Clear alert history
    pub fn clear_history(&mut self) {
        self.alert_history.lock().unwrap().clear();
    }
}

/// Alert statistics
#[derive(Debug, Clone)]
pub struct AlertStatistics {
    /// Total number of alerts triggered
    pub total_alerts: usize,

    /// Number of warning alerts
    pub warning_alerts: usize,

    /// Number of critical alerts
    pub critical_alerts: usize,

    /// Number of daily budget alerts
    pub daily_alerts: usize,

    /// Number of monthly budget alerts
    pub monthly_alerts: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn test_cost_alert_config_defaults() {
        let config = CostAlertConfig::new();
        assert!(config.daily_warning_threshold.is_none());
        assert!(config.daily_critical_threshold.is_none());
        assert!(config.enable_deduplication);
    }

    #[test]
    fn test_cost_alert_config_builder() {
        let config = CostAlertConfig::new()
            .with_daily_warning_threshold(5.0)
            .with_daily_critical_threshold(10.0)
            .with_monthly_warning_threshold(100.0)
            .with_monthly_critical_threshold(200.0);

        assert_eq!(config.daily_warning_threshold, Some(5.0));
        assert_eq!(config.daily_critical_threshold, Some(10.0));
        assert_eq!(config.monthly_warning_threshold, Some(100.0));
        assert_eq!(config.monthly_critical_threshold, Some(200.0));
    }

    #[test]
    fn test_track_cost() {
        let config = CostAlertConfig::new();
        let mut system = CostAlertSystem::new(config);

        system.track_cost(0.5);
        assert_eq!(system.daily_cost(), 0.5);
        assert_eq!(system.monthly_cost(), 0.5);

        system.track_cost(0.3);
        assert_eq!(system.daily_cost(), 0.8);
        assert_eq!(system.monthly_cost(), 0.8);
    }

    #[test]
    fn test_daily_warning_alert() {
        let config = CostAlertConfig::new()
            .with_daily_warning_threshold(1.0)
            .with_deduplication(false);

        let mut system = CostAlertSystem::new(config);

        let alert_count = Arc::new(AtomicUsize::new(0));
        let alert_count_clone = alert_count.clone();

        system.register_callback(Arc::new(move |alert| {
            assert_eq!(alert.level, AlertLevel::Warning);
            assert_eq!(alert.budget_type, BudgetType::Daily);
            alert_count_clone.fetch_add(1, Ordering::SeqCst);
        }));

        system.track_cost(1.5);

        assert_eq!(alert_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_daily_critical_alert() {
        let config = CostAlertConfig::new()
            .with_daily_critical_threshold(5.0)
            .with_deduplication(false);

        let mut system = CostAlertSystem::new(config);

        let alert_count = Arc::new(AtomicUsize::new(0));
        let alert_count_clone = alert_count.clone();

        system.register_callback(Arc::new(move |alert| {
            assert_eq!(alert.level, AlertLevel::Critical);
            alert_count_clone.fetch_add(1, Ordering::SeqCst);
        }));

        system.track_cost(6.0);

        assert_eq!(alert_count.load(Ordering::SeqCst), 1);
        assert!(!system.is_within_budget());
    }

    #[test]
    fn test_monthly_alert() {
        let config = CostAlertConfig::new()
            .with_monthly_warning_threshold(10.0)
            .with_deduplication(false);

        let mut system = CostAlertSystem::new(config);

        let alert_count = Arc::new(AtomicUsize::new(0));
        let alert_count_clone = alert_count.clone();

        system.register_callback(Arc::new(move |alert| {
            assert_eq!(alert.budget_type, BudgetType::Monthly);
            alert_count_clone.fetch_add(1, Ordering::SeqCst);
        }));

        system.track_cost(11.0);

        assert_eq!(alert_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_reset_daily_costs() {
        let config = CostAlertConfig::new();
        let mut system = CostAlertSystem::new(config);

        system.track_cost(5.0);
        assert_eq!(system.daily_cost(), 5.0);

        system.reset_daily_costs();
        assert_eq!(system.daily_cost(), 0.0);
        assert_eq!(system.monthly_cost(), 5.0); // Monthly not reset
    }

    #[test]
    fn test_reset_monthly_costs() {
        let config = CostAlertConfig::new();
        let mut system = CostAlertSystem::new(config);

        system.track_cost(50.0);
        assert_eq!(system.monthly_cost(), 50.0);

        system.reset_monthly_costs();
        assert_eq!(system.monthly_cost(), 0.0);
    }

    #[test]
    fn test_alert_deduplication() {
        let config = CostAlertConfig::new()
            .with_daily_warning_threshold(1.0)
            .with_deduplication(true);

        let mut system = CostAlertSystem::new(config);

        let alert_count = Arc::new(AtomicUsize::new(0));
        let alert_count_clone = alert_count.clone();

        system.register_callback(Arc::new(move |_| {
            alert_count_clone.fetch_add(1, Ordering::SeqCst);
        }));

        // First alert should trigger
        system.track_cost(1.5);
        assert_eq!(alert_count.load(Ordering::SeqCst), 1);

        // Second alert should be deduplicated
        system.track_cost(0.5);
        assert_eq!(alert_count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_alert_statistics() {
        let config = CostAlertConfig::new()
            .with_daily_warning_threshold(1.0)
            .with_daily_critical_threshold(5.0)
            .with_deduplication(false);

        let mut system = CostAlertSystem::new(config);

        system.register_callback(Arc::new(|_| {}));

        system.track_cost(2.0); // Trigger warning
        system.track_cost(4.0); // Trigger critical

        let stats = system.alert_statistics();
        assert_eq!(stats.total_alerts, 2);
        assert_eq!(stats.warning_alerts, 1);
        assert_eq!(stats.critical_alerts, 1);
        assert_eq!(stats.daily_alerts, 2);
    }

    #[test]
    fn test_is_within_budget() {
        let config = CostAlertConfig::new().with_daily_critical_threshold(10.0);

        let mut system = CostAlertSystem::new(config);

        assert!(system.is_within_budget());

        system.track_cost(5.0);
        assert!(system.is_within_budget());

        system.track_cost(6.0);
        assert!(!system.is_within_budget());
    }

    #[test]
    fn test_clear_history() {
        let config = CostAlertConfig::new()
            .with_daily_warning_threshold(1.0)
            .with_deduplication(false);

        let mut system = CostAlertSystem::new(config);
        system.register_callback(Arc::new(|_| {}));

        system.track_cost(2.0);

        let stats = system.alert_statistics();
        assert_eq!(stats.total_alerts, 1);

        system.clear_history();

        let stats = system.alert_statistics();
        assert_eq!(stats.total_alerts, 0);
    }
}
