//! Alert management and monitoring
//!
//! Provides alert types, conditions, and the `AlertManager` for tracking
//! and deduplicating alerts. Also includes webhook configuration for
//! alert delivery.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Alert severity level
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
pub enum AlertLevel {
    /// Informational alert
    Info,
    /// Warning alert - requires attention
    Warning,
    /// Critical alert - requires immediate action
    Critical,
}

impl std::fmt::Display for AlertLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlertLevel::Info => write!(f, "INFO"),
            AlertLevel::Warning => write!(f, "WARNING"),
            AlertLevel::Critical => write!(f, "CRITICAL"),
        }
    }
}

/// Alert condition that triggered the alert
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash, Eq)]
#[serde(tag = "type")]
pub enum AlertCondition {
    /// Schedule was missed (task didn't execute when expected)
    MissedSchedule {
        /// Expected run time
        expected_at: DateTime<Utc>,
        /// Current time when missed was detected
        detected_at: DateTime<Utc>,
    },
    /// Multiple consecutive failures
    ConsecutiveFailures {
        /// Number of consecutive failures
        count: u32,
        /// Failure threshold that triggered the alert
        threshold: u32,
    },
    /// High failure rate detected
    HighFailureRate {
        /// Current failure rate (0.0 to 1.0)
        rate: String, // String to make it hashable
        /// Threshold that was exceeded
        threshold: String,
    },
    /// Slow execution detected
    SlowExecution {
        /// Actual duration in milliseconds
        duration_ms: u64,
        /// Expected/threshold duration in milliseconds
        threshold_ms: u64,
    },
    /// Task is stuck (not executing for extended period)
    TaskStuck {
        /// Time since last execution
        idle_duration_seconds: i64,
        /// Expected interval in seconds
        expected_interval_seconds: u64,
    },
    /// Task has become unhealthy
    TaskUnhealthy {
        /// Health issues detected
        issues: Vec<String>,
    },
}

impl std::fmt::Display for AlertCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlertCondition::MissedSchedule {
                expected_at,
                detected_at,
            } => {
                let delay = detected_at
                    .signed_duration_since(*expected_at)
                    .num_seconds();
                write!(
                    f,
                    "Missed schedule ({}s late, expected at {})",
                    delay,
                    expected_at.format("%Y-%m-%d %H:%M:%S UTC")
                )
            }
            AlertCondition::ConsecutiveFailures { count, threshold } => {
                write!(
                    f,
                    "Consecutive failures ({} failures, threshold: {})",
                    count, threshold
                )
            }
            AlertCondition::HighFailureRate { rate, threshold } => {
                write!(
                    f,
                    "High failure rate (rate: {}, threshold: {})",
                    rate, threshold
                )
            }
            AlertCondition::SlowExecution {
                duration_ms,
                threshold_ms,
            } => {
                write!(
                    f,
                    "Slow execution ({}ms, threshold: {}ms)",
                    duration_ms, threshold_ms
                )
            }
            AlertCondition::TaskStuck {
                idle_duration_seconds,
                expected_interval_seconds,
            } => {
                write!(
                    f,
                    "Task stuck (idle: {}s, expected interval: {}s)",
                    idle_duration_seconds, expected_interval_seconds
                )
            }
            AlertCondition::TaskUnhealthy { issues } => {
                write!(f, "Task unhealthy: {}", issues.join(", "))
            }
        }
    }
}

/// Alert record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    /// Alert timestamp
    pub timestamp: DateTime<Utc>,
    /// Task name that triggered the alert
    pub task_name: String,
    /// Alert severity level
    pub level: AlertLevel,
    /// Condition that triggered the alert
    pub condition: AlertCondition,
    /// Human-readable message
    pub message: String,
    /// Additional metadata (optional)
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

impl Alert {
    /// Create a new alert
    pub fn new(
        task_name: String,
        level: AlertLevel,
        condition: AlertCondition,
        message: String,
    ) -> Self {
        Self {
            timestamp: Utc::now(),
            task_name,
            level,
            condition,
            message,
            metadata: HashMap::new(),
        }
    }

    /// Add metadata to the alert
    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }

    /// Check if this is a critical alert
    pub fn is_critical(&self) -> bool {
        self.level == AlertLevel::Critical
    }

    /// Check if this is a warning alert
    pub fn is_warning(&self) -> bool {
        self.level == AlertLevel::Warning
    }

    /// Check if this is an info alert
    pub fn is_info(&self) -> bool {
        self.level == AlertLevel::Info
    }

    /// Get a unique key for deduplication
    fn dedup_key(&self) -> String {
        format!("{}::{:?}", self.task_name, self.condition)
    }
}

impl std::fmt::Display for Alert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}] {} - {} - {}",
            self.timestamp.format("%Y-%m-%d %H:%M:%S UTC"),
            self.level,
            self.task_name,
            self.message
        )
    }
}

/// Alert callback type
///
/// Called when an alert is triggered. Receives the alert details.
pub type AlertCallback = Arc<dyn Fn(&Alert) + Send + Sync>;

/// Alert manager for tracking and deduplicating alerts
#[derive(Clone, Serialize, Deserialize)]
pub struct AlertManager {
    /// Recent alerts (limited size)
    alerts: Vec<Alert>,
    /// Maximum number of alerts to keep in history
    max_history: usize,
    /// Deduplication window in seconds
    dedup_window_seconds: i64,
    /// Last alert time by dedup key
    last_alert_time: HashMap<String, DateTime<Utc>>,
    /// Alert callbacks (not serialized)
    #[serde(skip)]
    callbacks: Vec<AlertCallback>,
}

impl AlertManager {
    /// Create a new alert manager
    ///
    /// # Arguments
    /// * `max_history` - Maximum number of alerts to keep in history
    /// * `dedup_window_seconds` - Time window for deduplicating alerts (e.g., 300 = 5 minutes)
    pub fn new(max_history: usize, dedup_window_seconds: i64) -> Self {
        Self {
            alerts: Vec::new(),
            max_history,
            dedup_window_seconds,
            last_alert_time: HashMap::new(),
            callbacks: Vec::new(),
        }
    }

    /// Add an alert callback
    pub fn add_callback(&mut self, callback: AlertCallback) {
        self.callbacks.push(callback);
    }

    /// Record an alert (with deduplication)
    ///
    /// # Arguments
    /// * `alert` - Alert to record
    ///
    /// # Returns
    /// * `true` if alert was recorded (not deduplicated)
    /// * `false` if alert was suppressed due to deduplication
    pub fn record_alert(&mut self, alert: Alert) -> bool {
        let dedup_key = alert.dedup_key();
        let now = Utc::now();

        // Check if we should deduplicate this alert
        if let Some(last_time) = self.last_alert_time.get(&dedup_key) {
            let elapsed = now.signed_duration_since(*last_time).num_seconds();
            if elapsed < self.dedup_window_seconds {
                // Suppress duplicate alert
                return false;
            }
        }

        // Record the alert
        self.last_alert_time.insert(dedup_key, now);

        // Trigger callbacks
        for callback in &self.callbacks {
            callback(&alert);
        }

        // Add to history
        self.alerts.push(alert);

        // Trim history if needed
        if self.alerts.len() > self.max_history {
            self.alerts.drain(0..self.alerts.len() - self.max_history);
        }

        // Cleanup old dedup entries (older than window)
        self.last_alert_time.retain(|_, last_time| {
            now.signed_duration_since(*last_time).num_seconds() < self.dedup_window_seconds * 2
        });

        true
    }

    /// Get all alerts
    pub fn get_alerts(&self) -> &[Alert] {
        &self.alerts
    }

    /// Get critical alerts
    pub fn get_critical_alerts(&self) -> Vec<&Alert> {
        self.alerts.iter().filter(|a| a.is_critical()).collect()
    }

    /// Get warning alerts
    pub fn get_warning_alerts(&self) -> Vec<&Alert> {
        self.alerts.iter().filter(|a| a.is_warning()).collect()
    }

    /// Get alerts for a specific task
    pub fn get_task_alerts(&self, task_name: &str) -> Vec<&Alert> {
        self.alerts
            .iter()
            .filter(|a| a.task_name == task_name)
            .collect()
    }

    /// Get recent alerts (within specified seconds)
    pub fn get_recent_alerts(&self, seconds: i64) -> Vec<&Alert> {
        let cutoff = Utc::now() - Duration::seconds(seconds);
        self.alerts
            .iter()
            .filter(|a| a.timestamp > cutoff)
            .collect()
    }

    /// Clear all alerts
    pub fn clear(&mut self) {
        self.alerts.clear();
        self.last_alert_time.clear();
    }

    /// Clear alerts for a specific task
    pub fn clear_task_alerts(&mut self, task_name: &str) {
        self.alerts.retain(|a| a.task_name != task_name);
        self.last_alert_time
            .retain(|k, _| !k.starts_with(&format!("{}::", task_name)));
    }

    /// Get alert count
    pub fn alert_count(&self) -> usize {
        self.alerts.len()
    }

    /// Get critical alert count
    pub fn critical_alert_count(&self) -> usize {
        self.alerts.iter().filter(|a| a.is_critical()).count()
    }

    /// Get warning alert count
    pub fn warning_alert_count(&self) -> usize {
        self.alerts.iter().filter(|a| a.is_warning()).count()
    }
}

impl Default for AlertManager {
    fn default() -> Self {
        Self::new(1000, 300) // Keep 1000 alerts, 5-minute dedup window
    }
}

impl std::fmt::Debug for AlertManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlertManager")
            .field("alerts_count", &self.alerts.len())
            .field("max_history", &self.max_history)
            .field("dedup_window_seconds", &self.dedup_window_seconds)
            .field("callbacks_count", &self.callbacks.len())
            .finish()
    }
}

/// Alert configuration for task monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertConfig {
    /// Enable alerting for this task
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Threshold for consecutive failures before alerting
    #[serde(default = "default_consecutive_failures_threshold")]
    pub consecutive_failures_threshold: u32,
    /// Threshold for failure rate (0.0 to 1.0) before alerting
    #[serde(default = "default_failure_rate_threshold")]
    pub failure_rate_threshold: f64,
    /// Threshold for slow execution (milliseconds)
    pub slow_execution_threshold_ms: Option<u64>,
    /// Enable alerts for missed schedules
    #[serde(default = "default_true")]
    pub alert_on_missed_schedule: bool,
    /// Enable alerts for task stuck
    #[serde(default = "default_true")]
    pub alert_on_stuck: bool,
}

#[allow(dead_code)]
fn default_true() -> bool {
    true
}

#[allow(dead_code)]
fn default_consecutive_failures_threshold() -> u32 {
    3
}

#[allow(dead_code)]
fn default_failure_rate_threshold() -> f64 {
    0.5
}

impl Default for AlertConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            consecutive_failures_threshold: 3,
            failure_rate_threshold: 0.5,
            slow_execution_threshold_ms: None,
            alert_on_missed_schedule: true,
            alert_on_stuck: true,
        }
    }
}

impl AlertConfig {
    /// Create a new alert configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Disable all alerts for this task
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }

    /// Set consecutive failures threshold
    pub fn with_consecutive_failures_threshold(mut self, threshold: u32) -> Self {
        self.consecutive_failures_threshold = threshold;
        self
    }

    /// Set failure rate threshold
    pub fn with_failure_rate_threshold(mut self, threshold: f64) -> Self {
        self.failure_rate_threshold = threshold;
        self
    }

    /// Set slow execution threshold
    pub fn with_slow_execution_threshold_ms(mut self, threshold_ms: u64) -> Self {
        self.slow_execution_threshold_ms = Some(threshold_ms);
        self
    }

    /// Disable missed schedule alerts
    pub fn without_missed_schedule_alerts(mut self) -> Self {
        self.alert_on_missed_schedule = false;
        self
    }

    /// Disable stuck task alerts
    pub fn without_stuck_alerts(mut self) -> Self {
        self.alert_on_stuck = false;
        self
    }
}

// ============================================================================
// Webhook Alert Delivery
// ============================================================================

/// Webhook configuration for alert delivery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookConfig {
    /// Webhook endpoint URL
    pub url: String,
    /// HTTP headers to include in webhook requests
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// Timeout for webhook requests in seconds
    #[serde(default = "default_webhook_timeout")]
    pub timeout_seconds: u64,
    /// Filter: only send alerts matching these levels (empty = all levels)
    #[serde(default)]
    pub alert_levels: Vec<AlertLevel>,
}

#[allow(dead_code)]
fn default_webhook_timeout() -> u64 {
    30
}

impl WebhookConfig {
    /// Create a new webhook configuration
    ///
    /// # Arguments
    /// * `url` - Webhook endpoint URL
    ///
    /// # Examples
    /// ```
    /// use celers_beat::WebhookConfig;
    ///
    /// let webhook = WebhookConfig::new("https://example.com/alerts");
    /// ```
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            headers: HashMap::new(),
            timeout_seconds: default_webhook_timeout(),
            alert_levels: Vec::new(),
        }
    }

    /// Add a custom HTTP header
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Set the timeout for webhook requests
    pub fn with_timeout(mut self, timeout_seconds: u64) -> Self {
        self.timeout_seconds = timeout_seconds;
        self
    }

    /// Filter alerts by level (empty = send all alerts)
    pub fn with_alert_levels(mut self, levels: Vec<AlertLevel>) -> Self {
        self.alert_levels = levels;
        self
    }

    /// Check if this webhook should receive the given alert
    pub fn should_send(&self, alert: &Alert) -> bool {
        if self.alert_levels.is_empty() {
            return true;
        }
        self.alert_levels.contains(&alert.level)
    }

    /// Create a JSON payload for the webhook
    pub fn create_payload(&self, alert: &Alert) -> serde_json::Value {
        serde_json::json!({
            "timestamp": alert.timestamp.to_rfc3339(),
            "task_name": alert.task_name,
            "level": format!("{:?}", alert.level),
            "condition": format!("{:?}", alert.condition),
            "message": alert.message,
            "metadata": alert.metadata,
        })
    }
}
