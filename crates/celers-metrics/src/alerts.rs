//! Alert management: conditions, rules, trend alerts, debouncing, and alert history.

use crate::backends::CurrentMetrics;
use crate::history::MetricHistory;
use std::collections::HashMap;
use std::sync::Mutex as StdMutex;
use std::time::Instant;

// ============================================================================
// Alert Threshold Helpers
// ============================================================================

/// Alert condition for monitoring
#[derive(Debug, Clone)]
pub enum AlertCondition {
    /// Gauge exceeds threshold
    GaugeAbove { threshold: f64 },
    /// Gauge below threshold
    GaugeBelow { threshold: f64 },
    /// Success rate below threshold (0.0 to 1.0)
    SuccessRateBelow { threshold: f64 },
    /// Error rate above threshold (0.0 to 1.0)
    ErrorRateAbove { threshold: f64 },
}

/// Alert severity level
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum AlertSeverity {
    /// Informational alert
    Info,
    /// Warning alert
    Warning,
    /// Critical alert requiring immediate attention
    Critical,
}

/// Alert configuration
#[derive(Debug, Clone)]
pub struct AlertRule {
    /// Alert name
    pub name: String,
    /// Alert condition
    pub condition: AlertCondition,
    /// Alert severity
    pub severity: AlertSeverity,
    /// Alert description
    pub description: String,
}

impl AlertRule {
    /// Create a new alert rule
    pub fn new(
        name: impl Into<String>,
        condition: AlertCondition,
        severity: AlertSeverity,
        description: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            condition,
            severity,
            description: description.into(),
        }
    }

    /// Check if alert should fire based on current metrics
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_metrics::{AlertRule, AlertCondition, AlertSeverity, CurrentMetrics};
    ///
    /// let rule = AlertRule::new(
    ///     "high_error_rate",
    ///     AlertCondition::ErrorRateAbove { threshold: 0.05 },
    ///     AlertSeverity::Critical,
    ///     "Error rate exceeded 5%"
    /// );
    ///
    /// let metrics = CurrentMetrics {
    ///     tasks_enqueued: 100.0,
    ///     tasks_completed: 90.0,
    ///     tasks_failed: 10.0,
    ///     tasks_retried: 5.0,
    ///     tasks_cancelled: 2.0,
    ///     queue_size: 50.0,
    ///     processing_queue_size: 10.0,
    ///     dlq_size: 3.0,
    ///     active_workers: 5.0,
    /// };
    ///
    /// if rule.should_fire(&metrics) {
    ///     println!("Alert: {}", rule.name);
    /// }
    /// ```
    pub fn should_fire(&self, metrics: &CurrentMetrics) -> bool {
        match &self.condition {
            AlertCondition::GaugeAbove { threshold } => {
                metrics.queue_size > *threshold || metrics.processing_queue_size > *threshold
            }
            AlertCondition::GaugeBelow { threshold } => metrics.active_workers < *threshold,
            AlertCondition::SuccessRateBelow { threshold } => metrics.success_rate() < *threshold,
            AlertCondition::ErrorRateAbove { threshold } => metrics.error_rate() > *threshold,
        }
    }
}

// ============================================================================
// Trend-Based Alerting
// ============================================================================

/// Trend direction for alerting
#[derive(Debug, Clone, PartialEq)]
pub enum TrendDirection {
    /// Metric is increasing
    Increasing,
    /// Metric is decreasing
    Decreasing,
    /// Metric is stable (no significant trend)
    Stable,
}

/// Trend-based alert condition
#[derive(Debug, Clone)]
pub struct TrendAlertCondition {
    /// Minimum number of samples required for trend analysis
    pub min_samples: usize,
    /// Trend threshold (rate of change per second)
    pub trend_threshold: f64,
    /// Expected trend direction for alert
    pub alert_on_direction: TrendDirection,
}

impl TrendAlertCondition {
    /// Create a new trend alert condition
    pub fn new(trend_threshold: f64, alert_on_direction: TrendDirection) -> Self {
        Self {
            min_samples: 5,
            trend_threshold,
            alert_on_direction,
        }
    }

    /// Check if the trend should trigger an alert
    pub fn should_alert(&self, history: &MetricHistory) -> bool {
        if history.len() < self.min_samples {
            return false;
        }

        let trend = match history.trend() {
            Some(t) => t,
            None => return false,
        };

        match self.alert_on_direction {
            TrendDirection::Increasing => trend > self.trend_threshold,
            TrendDirection::Decreasing => trend < -self.trend_threshold,
            TrendDirection::Stable => trend.abs() < self.trend_threshold,
        }
    }
}

/// Trend-based alert rule combining standard alerts with trend analysis
#[derive(Debug, Clone)]
pub struct TrendAlertRule {
    /// Alert name
    pub name: String,
    /// Trend alert condition
    pub condition: TrendAlertCondition,
    /// Alert severity
    pub severity: AlertSeverity,
    /// Alert description
    pub description: String,
}

impl TrendAlertRule {
    /// Create a new trend-based alert rule
    pub fn new(
        name: impl Into<String>,
        condition: TrendAlertCondition,
        severity: AlertSeverity,
        description: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            condition,
            severity,
            description: description.into(),
        }
    }

    /// Check if trend alert should fire based on metric history
    pub fn should_fire(&self, history: &MetricHistory) -> bool {
        self.condition.should_alert(history)
    }
}

/// Alert manager for tracking multiple alert rules
#[derive(Debug)]
pub struct AlertManager {
    rules: Vec<AlertRule>,
    trend_rules: Vec<TrendAlertRule>,
}

impl AlertManager {
    /// Create a new alert manager
    pub fn new() -> Self {
        Self {
            rules: Vec::new(),
            trend_rules: Vec::new(),
        }
    }

    /// Add an alert rule
    pub fn add_rule(&mut self, rule: AlertRule) {
        self.rules.push(rule);
    }

    /// Add a trend-based alert rule
    pub fn add_trend_rule(&mut self, rule: TrendAlertRule) {
        self.trend_rules.push(rule);
    }

    /// Check all standard rules and return fired alerts
    pub fn check_alerts(&self, metrics: &CurrentMetrics) -> Vec<&AlertRule> {
        self.rules
            .iter()
            .filter(|rule| rule.should_fire(metrics))
            .collect()
    }

    /// Check all trend rules and return fired alerts
    pub fn check_trend_alerts(&self, history: &MetricHistory) -> Vec<&TrendAlertRule> {
        self.trend_rules
            .iter()
            .filter(|rule| rule.should_fire(history))
            .collect()
    }

    /// Get critical standard alerts
    pub fn critical_alerts(&self, metrics: &CurrentMetrics) -> Vec<&AlertRule> {
        self.check_alerts(metrics)
            .into_iter()
            .filter(|rule| rule.severity == AlertSeverity::Critical)
            .collect()
    }

    /// Get critical trend alerts
    pub fn critical_trend_alerts(&self, history: &MetricHistory) -> Vec<&TrendAlertRule> {
        self.check_trend_alerts(history)
            .into_iter()
            .filter(|rule| rule.severity == AlertSeverity::Critical)
            .collect()
    }
}

impl Default for AlertManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Alert Debouncer (Alert Fatigue Prevention)
// ============================================================================

/// Alert debouncer to prevent alert fatigue
#[derive(Debug)]
pub struct AlertDebouncer {
    /// Minimum time between alerts (seconds)
    debounce_period: u64,
    /// Last alert times by alert name
    last_alert_times: StdMutex<HashMap<String, Instant>>,
}

impl AlertDebouncer {
    /// Create a new alert debouncer
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_metrics::AlertDebouncer;
    ///
    /// // Only allow same alert every 5 minutes
    /// let debouncer = AlertDebouncer::new(300);
    /// ```
    pub fn new(debounce_period_seconds: u64) -> Self {
        Self {
            debounce_period: debounce_period_seconds,
            last_alert_times: StdMutex::new(HashMap::new()),
        }
    }

    /// Check if an alert should fire (not debounced)
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_metrics::AlertDebouncer;
    ///
    /// let debouncer = AlertDebouncer::new(60);
    ///
    /// // First alert should fire
    /// assert!(debouncer.should_fire("high_error_rate"));
    ///
    /// // Immediate second alert should be debounced
    /// assert!(!debouncer.should_fire("high_error_rate"));
    /// ```
    pub fn should_fire(&self, alert_name: &str) -> bool {
        let mut times = self.last_alert_times.lock().unwrap();
        let now = Instant::now();

        if let Some(last_time) = times.get(alert_name) {
            let elapsed = now.duration_since(*last_time).as_secs();
            if elapsed < self.debounce_period {
                return false; // Still in debounce period
            }
        }

        times.insert(alert_name.to_string(), now);
        true
    }

    /// Reset debounce state for an alert
    pub fn reset(&self, alert_name: &str) {
        self.last_alert_times.lock().unwrap().remove(alert_name);
    }

    /// Reset all debounce state
    pub fn reset_all(&self) {
        self.last_alert_times.lock().unwrap().clear();
    }

    /// Get time until next alert can fire (in seconds)
    pub fn time_until_next(&self, alert_name: &str) -> Option<u64> {
        let times = self.last_alert_times.lock().unwrap();
        if let Some(last_time) = times.get(alert_name) {
            let elapsed = Instant::now().duration_since(*last_time).as_secs();
            if elapsed < self.debounce_period {
                return Some(self.debounce_period - elapsed);
            }
        }
        Some(0)
    }
}

impl Default for AlertDebouncer {
    fn default() -> Self {
        Self::new(300) // 5 minutes default
    }
}

// ============================================================================
// Alert History Tracking
// ============================================================================

/// Alert event in history
#[derive(Debug, Clone)]
pub struct AlertEvent {
    /// Alert name/ID
    pub alert_name: String,
    /// When the alert fired
    pub timestamp: u64,
    /// Alert severity
    pub severity: AlertSeverity,
    /// Metric value that triggered the alert
    pub trigger_value: f64,
    /// Alert threshold
    pub threshold: f64,
}

/// Alert history tracker
pub struct AlertHistory {
    events: StdMutex<Vec<AlertEvent>>,
    max_events: usize,
}

impl AlertHistory {
    /// Create a new alert history tracker
    ///
    /// # Arguments
    ///
    /// * `max_events` - Maximum number of events to retain
    pub fn new(max_events: usize) -> Self {
        Self {
            events: StdMutex::new(Vec::new()),
            max_events,
        }
    }

    /// Record an alert event
    pub fn record_alert(&self, event: AlertEvent) {
        let mut events = self
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        events.push(event);

        // Keep only the most recent events
        if events.len() > self.max_events {
            let drain_count = events.len() - self.max_events;
            events.drain(0..drain_count);
        }
    }

    /// Get recent alert events
    pub fn recent_events(&self, count: usize) -> Vec<AlertEvent> {
        let events = self
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let start = events.len().saturating_sub(count);
        events.iter().skip(start).cloned().collect()
    }

    /// Get all events for a specific alert
    pub fn events_for_alert(&self, alert_name: &str) -> Vec<AlertEvent> {
        let events = self
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        events
            .iter()
            .filter(|e| e.alert_name == alert_name)
            .cloned()
            .collect()
    }

    /// Count how many times an alert has fired
    pub fn alert_fire_count(&self, alert_name: &str) -> usize {
        let events = self
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        events.iter().filter(|e| e.alert_name == alert_name).count()
    }

    /// Get time since last alert fired
    pub fn time_since_last_alert(&self, alert_name: &str) -> Option<u64> {
        let events = self
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        events
            .iter()
            .rev()
            .find(|e| e.alert_name == alert_name)
            .map(|e| {
                use std::time::{SystemTime, UNIX_EPOCH};
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                now.saturating_sub(e.timestamp)
            })
    }

    /// Clear all history
    pub fn clear(&self) {
        let mut events = self
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        events.clear();
    }
}
