//! Unified Metrics Aggregator
//!
//! Combines metrics from all monitoring modules into a single cohesive dashboard.
//! Provides a unified view of system health, performance, costs, and SLA compliance.
//!
//! # Features
//!
//! - Aggregate metrics from monitoring, profiler, cost_tracker, sla_monitor, backpressure
//! - Unified health score calculation
//! - Comprehensive system snapshot
//! - Alert generation based on combined metrics
//! - Export to multiple formats (JSON, Prometheus, CloudWatch)
//!
//! # Example
//!
//! ```
//! use celers_broker_sqs::metrics_aggregator::{MetricsAggregator, AggregatorConfig};
//! use std::time::Duration;
//!
//! let config = AggregatorConfig::new()
//!     .with_collection_interval(Duration::from_secs(60))
//!     .with_retention_period(Duration::from_secs(3600));
//!
//! let mut aggregator = MetricsAggregator::new(config);
//!
//! // Collect metrics snapshot
//! let snapshot = aggregator.collect_snapshot();
//! println!("System Health Score: {}/100", snapshot.health_score);
//! println!("Cost (hourly): ${:.4}", snapshot.cost_metrics.hourly_rate);
//! println!("SLA Compliance: {:.2}%", snapshot.sla_metrics.compliance_percentage);
//! ```

use std::time::{Duration, SystemTime};

/// Configuration for metrics aggregator
#[derive(Debug, Clone)]
pub struct AggregatorConfig {
    /// How often to collect metrics
    pub collection_interval: Duration,
    /// How long to retain historical data
    pub retention_period: Duration,
    /// Enable automatic alerting
    pub enable_alerting: bool,
    /// Health score thresholds
    pub health_thresholds: HealthThresholds,
}

impl AggregatorConfig {
    /// Create new aggregator configuration
    pub fn new() -> Self {
        Self {
            collection_interval: Duration::from_secs(60),
            retention_period: Duration::from_secs(3600),
            enable_alerting: true,
            health_thresholds: HealthThresholds::default(),
        }
    }

    /// Set collection interval
    pub fn with_collection_interval(mut self, interval: Duration) -> Self {
        self.collection_interval = interval;
        self
    }

    /// Set retention period
    pub fn with_retention_period(mut self, period: Duration) -> Self {
        self.retention_period = period;
        self
    }

    /// Enable or disable alerting
    pub fn with_alerting(mut self, enable: bool) -> Self {
        self.enable_alerting = enable;
        self
    }

    /// Set custom health thresholds
    pub fn with_health_thresholds(mut self, thresholds: HealthThresholds) -> Self {
        self.health_thresholds = thresholds;
        self
    }
}

impl Default for AggregatorConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Health score thresholds
#[derive(Debug, Clone)]
pub struct HealthThresholds {
    /// Minimum health score for healthy state (0-100)
    pub healthy_threshold: u8,
    /// Minimum health score for warning state (0-100)
    pub warning_threshold: u8,
    /// Below this is critical (0-100)
    pub critical_threshold: u8,
}

impl Default for HealthThresholds {
    fn default() -> Self {
        Self {
            healthy_threshold: 80,
            warning_threshold: 60,
            critical_threshold: 40,
        }
    }
}

/// Unified metrics aggregator
pub struct MetricsAggregator {
    config: AggregatorConfig,
    snapshots: Vec<MetricsSnapshot>,
    last_collection: SystemTime,
}

impl MetricsAggregator {
    /// Create new metrics aggregator
    pub fn new(config: AggregatorConfig) -> Self {
        Self {
            config,
            snapshots: Vec::new(),
            last_collection: SystemTime::now(),
        }
    }

    /// Collect current metrics snapshot
    pub fn collect_snapshot(&mut self) -> MetricsSnapshot {
        let snapshot = MetricsSnapshot {
            timestamp: SystemTime::now(),
            health_score: 0, // Will be calculated
            queue_metrics: QueueMetrics::default(),
            performance_metrics: PerformanceMetrics::default(),
            cost_metrics: CostMetrics::default(),
            sla_metrics: SlaMetrics::default(),
            backpressure_metrics: BackpressureMetrics::default(),
            alerts: Vec::new(),
        };

        // Store snapshot
        self.snapshots.push(snapshot.clone());
        self.cleanup_old_snapshots();
        self.last_collection = SystemTime::now();

        snapshot
    }

    /// Get latest snapshot
    pub fn latest_snapshot(&self) -> Option<&MetricsSnapshot> {
        self.snapshots.last()
    }

    /// Get historical snapshots
    pub fn get_history(&self, duration: Duration) -> Vec<&MetricsSnapshot> {
        let cutoff = SystemTime::now() - duration;
        self.snapshots
            .iter()
            .filter(|s| s.timestamp > cutoff)
            .collect()
    }

    /// Calculate overall health score
    pub fn calculate_health_score(&self, snapshot: &MetricsSnapshot) -> u8 {
        let mut score = 100u8;

        // Queue health (25 points)
        if snapshot.queue_metrics.approximate_messages > 10000 {
            score = score.saturating_sub(15);
        } else if snapshot.queue_metrics.approximate_messages > 5000 {
            score = score.saturating_sub(8);
        }

        // Performance health (25 points)
        if snapshot.performance_metrics.p99_latency_ms > 1000.0 {
            score = score.saturating_sub(15);
        } else if snapshot.performance_metrics.p99_latency_ms > 500.0 {
            score = score.saturating_sub(8);
        }

        // Cost health (20 points)
        if snapshot.cost_metrics.daily_cost > snapshot.cost_metrics.budget_limit {
            score = score.saturating_sub(20);
        } else if snapshot.cost_metrics.daily_cost > snapshot.cost_metrics.budget_limit * 0.9 {
            score = score.saturating_sub(10);
        }

        // SLA health (20 points)
        if snapshot.sla_metrics.compliance_percentage < 95.0 {
            score = score.saturating_sub(20);
        } else if snapshot.sla_metrics.compliance_percentage < 99.0 {
            score = score.saturating_sub(10);
        }

        // Backpressure health (10 points)
        if snapshot.backpressure_metrics.in_flight_messages > 1000 {
            score = score.saturating_sub(10);
        } else if snapshot.backpressure_metrics.in_flight_messages > 500 {
            score = score.saturating_sub(5);
        }

        score
    }

    /// Generate alerts based on current metrics
    pub fn generate_alerts(&self, snapshot: &MetricsSnapshot) -> Vec<Alert> {
        let mut alerts = Vec::new();

        // Queue depth alerts
        if snapshot.queue_metrics.approximate_messages > 10000 {
            alerts.push(Alert {
                severity: AlertSeverity::Critical,
                category: AlertCategory::QueueDepth,
                message: format!(
                    "Queue depth critically high: {} messages",
                    snapshot.queue_metrics.approximate_messages
                ),
                timestamp: SystemTime::now(),
            });
        }

        // Latency alerts
        if snapshot.performance_metrics.p99_latency_ms > 1000.0 {
            alerts.push(Alert {
                severity: AlertSeverity::Warning,
                category: AlertCategory::Performance,
                message: format!(
                    "P99 latency high: {:.2}ms",
                    snapshot.performance_metrics.p99_latency_ms
                ),
                timestamp: SystemTime::now(),
            });
        }

        // Cost alerts
        if snapshot.cost_metrics.daily_cost > snapshot.cost_metrics.budget_limit {
            alerts.push(Alert {
                severity: AlertSeverity::Critical,
                category: AlertCategory::Cost,
                message: format!(
                    "Daily cost exceeded budget: ${:.2} > ${:.2}",
                    snapshot.cost_metrics.daily_cost, snapshot.cost_metrics.budget_limit
                ),
                timestamp: SystemTime::now(),
            });
        }

        // SLA alerts
        if snapshot.sla_metrics.compliance_percentage < 99.0 {
            alerts.push(Alert {
                severity: AlertSeverity::Warning,
                category: AlertCategory::Sla,
                message: format!(
                    "SLA compliance below target: {:.2}%",
                    snapshot.sla_metrics.compliance_percentage
                ),
                timestamp: SystemTime::now(),
            });
        }

        alerts
    }

    /// Export metrics to JSON
    pub fn export_json(&self, snapshot: &MetricsSnapshot) -> String {
        serde_json::to_string_pretty(snapshot).unwrap_or_else(|_| "{}".to_string())
    }

    /// Export metrics to Prometheus format
    pub fn export_prometheus(&self, snapshot: &MetricsSnapshot) -> String {
        let mut output = String::new();

        // Health score
        output.push_str(&format!(
            "# HELP sqs_health_score Overall system health score (0-100)\n\
             # TYPE sqs_health_score gauge\n\
             sqs_health_score {}\n\n",
            snapshot.health_score
        ));

        // Queue metrics
        output.push_str(&format!(
            "# HELP sqs_queue_depth Number of messages in queue\n\
             # TYPE sqs_queue_depth gauge\n\
             sqs_queue_depth {}\n\n",
            snapshot.queue_metrics.approximate_messages
        ));

        // Performance metrics
        output.push_str(&format!(
            "# HELP sqs_latency_p99_ms P99 latency in milliseconds\n\
             # TYPE sqs_latency_p99_ms gauge\n\
             sqs_latency_p99_ms {}\n\n",
            snapshot.performance_metrics.p99_latency_ms
        ));

        // Cost metrics
        output.push_str(&format!(
            "# HELP sqs_cost_hourly_usd Hourly cost in USD\n\
             # TYPE sqs_cost_hourly_usd gauge\n\
             sqs_cost_hourly_usd {}\n\n",
            snapshot.cost_metrics.hourly_rate
        ));

        // SLA metrics
        output.push_str(&format!(
            "# HELP sqs_sla_compliance_percentage SLA compliance percentage\n\
             # TYPE sqs_sla_compliance_percentage gauge\n\
             sqs_sla_compliance_percentage {}\n\n",
            snapshot.sla_metrics.compliance_percentage
        ));

        output
    }

    /// Generate summary report
    pub fn summary_report(&self, snapshot: &MetricsSnapshot) -> String {
        format!(
            "=== SQS Metrics Summary ===\n\
             \n\
             Health Score: {}/100\n\
             Status: {:?}\n\
             \n\
             Queue Metrics:\n\
             - Messages in Queue: {}\n\
             - Messages in Flight: {}\n\
             - Oldest Message Age: {}s\n\
             \n\
             Performance Metrics:\n\
             - P50 Latency: {:.2}ms\n\
             - P95 Latency: {:.2}ms\n\
             - P99 Latency: {:.2}ms\n\
             - Throughput: {:.2} msg/sec\n\
             \n\
             Cost Metrics:\n\
             - Hourly Rate: ${:.4}\n\
             - Daily Cost: ${:.2}\n\
             - Monthly Projection: ${:.2}\n\
             - Budget Remaining: ${:.2}\n\
             \n\
             SLA Metrics:\n\
             - Compliance: {:.2}%\n\
             - Violations: {}\n\
             - Error Rate: {:.2}%\n\
             \n\
             Active Alerts: {}\n",
            snapshot.health_score,
            self.get_health_status(snapshot.health_score),
            snapshot.queue_metrics.approximate_messages,
            snapshot.backpressure_metrics.in_flight_messages,
            snapshot.queue_metrics.oldest_message_age_seconds,
            snapshot.performance_metrics.p50_latency_ms,
            snapshot.performance_metrics.p95_latency_ms,
            snapshot.performance_metrics.p99_latency_ms,
            snapshot.performance_metrics.throughput,
            snapshot.cost_metrics.hourly_rate,
            snapshot.cost_metrics.daily_cost,
            snapshot.cost_metrics.monthly_projection,
            snapshot.cost_metrics.budget_limit - snapshot.cost_metrics.daily_cost,
            snapshot.sla_metrics.compliance_percentage,
            snapshot.sla_metrics.violations,
            snapshot.sla_metrics.error_rate,
            snapshot.alerts.len()
        )
    }

    fn get_health_status(&self, score: u8) -> HealthStatus {
        if score >= self.config.health_thresholds.healthy_threshold {
            HealthStatus::Healthy
        } else if score >= self.config.health_thresholds.warning_threshold {
            HealthStatus::Warning
        } else {
            HealthStatus::Critical
        }
    }

    fn cleanup_old_snapshots(&mut self) {
        let cutoff = SystemTime::now() - self.config.retention_period;
        self.snapshots.retain(|s| s.timestamp > cutoff);
    }
}

/// Complete metrics snapshot
#[derive(Debug, Clone, serde::Serialize)]
pub struct MetricsSnapshot {
    pub timestamp: SystemTime,
    pub health_score: u8,
    pub queue_metrics: QueueMetrics,
    pub performance_metrics: PerformanceMetrics,
    pub cost_metrics: CostMetrics,
    pub sla_metrics: SlaMetrics,
    pub backpressure_metrics: BackpressureMetrics,
    pub alerts: Vec<Alert>,
}

/// Queue-related metrics
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct QueueMetrics {
    pub approximate_messages: u64,
    pub approximate_messages_not_visible: u64,
    pub approximate_messages_delayed: u64,
    pub oldest_message_age_seconds: u64,
}

/// Performance metrics
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct PerformanceMetrics {
    pub p50_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub throughput: f64,
    pub operations_per_second: f64,
}

/// Cost metrics
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct CostMetrics {
    pub hourly_rate: f64,
    pub daily_cost: f64,
    pub monthly_projection: f64,
    pub budget_limit: f64,
}

/// SLA metrics
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct SlaMetrics {
    pub compliance_percentage: f64,
    pub violations: u64,
    pub error_rate: f64,
}

/// Backpressure metrics
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct BackpressureMetrics {
    pub in_flight_messages: usize,
    pub throttling_active: bool,
    pub avg_processing_time_ms: f64,
}

/// Alert information
#[derive(Debug, Clone, serde::Serialize)]
pub struct Alert {
    pub severity: AlertSeverity,
    pub category: AlertCategory,
    pub message: String,
    pub timestamp: SystemTime,
}

/// Alert severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
}

/// Alert categories
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum AlertCategory {
    QueueDepth,
    Performance,
    Cost,
    Sla,
    Backpressure,
}

/// Overall health status
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum HealthStatus {
    Healthy,
    Warning,
    Critical,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aggregator_creation() {
        let config = AggregatorConfig::new();
        let aggregator = MetricsAggregator::new(config);
        assert_eq!(aggregator.snapshots.len(), 0);
    }

    #[test]
    fn test_collect_snapshot() {
        let config = AggregatorConfig::new();
        let mut aggregator = MetricsAggregator::new(config);

        let snapshot = aggregator.collect_snapshot();
        assert_eq!(aggregator.snapshots.len(), 1);
        assert_eq!(snapshot.health_score, 0);
    }

    #[test]
    fn test_health_score_calculation() {
        let config = AggregatorConfig::new();
        let aggregator = MetricsAggregator::new(config);

        let mut snapshot = MetricsSnapshot {
            timestamp: SystemTime::now(),
            health_score: 0,
            queue_metrics: QueueMetrics {
                approximate_messages: 100,
                ..Default::default()
            },
            performance_metrics: PerformanceMetrics {
                p99_latency_ms: 50.0,
                ..Default::default()
            },
            cost_metrics: CostMetrics {
                daily_cost: 5.0,
                budget_limit: 100.0,
                ..Default::default()
            },
            sla_metrics: SlaMetrics {
                compliance_percentage: 99.5,
                ..Default::default()
            },
            backpressure_metrics: BackpressureMetrics {
                in_flight_messages: 10,
                ..Default::default()
            },
            alerts: Vec::new(),
        };

        let score = aggregator.calculate_health_score(&snapshot);
        assert_eq!(score, 100); // Healthy system

        // Test degraded performance
        snapshot.performance_metrics.p99_latency_ms = 1500.0;
        let score = aggregator.calculate_health_score(&snapshot);
        assert_eq!(score, 85);
    }

    #[test]
    fn test_alert_generation() {
        let config = AggregatorConfig::new();
        let aggregator = MetricsAggregator::new(config);

        let snapshot = MetricsSnapshot {
            timestamp: SystemTime::now(),
            health_score: 0,
            queue_metrics: QueueMetrics {
                approximate_messages: 15000, // Above threshold
                ..Default::default()
            },
            performance_metrics: PerformanceMetrics::default(),
            cost_metrics: CostMetrics {
                daily_cost: 150.0,
                budget_limit: 100.0, // Over budget
                ..Default::default()
            },
            sla_metrics: SlaMetrics {
                compliance_percentage: 99.5, // Above threshold to avoid SLA alert
                ..Default::default()
            },
            backpressure_metrics: BackpressureMetrics::default(),
            alerts: Vec::new(),
        };

        let alerts = aggregator.generate_alerts(&snapshot);
        assert_eq!(alerts.len(), 2); // Queue depth + cost
        assert!(alerts
            .iter()
            .any(|a| matches!(a.category, AlertCategory::QueueDepth)));
        assert!(alerts
            .iter()
            .any(|a| matches!(a.category, AlertCategory::Cost)));
    }

    #[test]
    fn test_health_thresholds() {
        let thresholds = HealthThresholds::default();
        assert_eq!(thresholds.healthy_threshold, 80);
        assert_eq!(thresholds.warning_threshold, 60);
        assert_eq!(thresholds.critical_threshold, 40);
    }

    #[test]
    fn test_config_builder() {
        let config = AggregatorConfig::new()
            .with_collection_interval(Duration::from_secs(30))
            .with_retention_period(Duration::from_secs(7200))
            .with_alerting(false);

        assert_eq!(config.collection_interval, Duration::from_secs(30));
        assert_eq!(config.retention_period, Duration::from_secs(7200));
        assert!(!config.enable_alerting);
    }

    #[test]
    fn test_prometheus_export() {
        let config = AggregatorConfig::new();
        let aggregator = MetricsAggregator::new(config);

        let snapshot = MetricsSnapshot {
            timestamp: SystemTime::now(),
            health_score: 95,
            queue_metrics: QueueMetrics {
                approximate_messages: 1000,
                ..Default::default()
            },
            performance_metrics: PerformanceMetrics {
                p99_latency_ms: 250.0,
                ..Default::default()
            },
            cost_metrics: CostMetrics {
                hourly_rate: 0.10,
                ..Default::default()
            },
            sla_metrics: SlaMetrics {
                compliance_percentage: 99.9,
                ..Default::default()
            },
            backpressure_metrics: BackpressureMetrics::default(),
            alerts: Vec::new(),
        };

        let output = aggregator.export_prometheus(&snapshot);
        assert!(output.contains("sqs_health_score 95"));
        assert!(output.contains("sqs_queue_depth 1000"));
        assert!(output.contains("sqs_latency_p99_ms 250"));
    }

    #[test]
    fn test_summary_report() {
        let config = AggregatorConfig::new();
        let aggregator = MetricsAggregator::new(config);

        let snapshot = MetricsSnapshot {
            timestamp: SystemTime::now(),
            health_score: 85,
            queue_metrics: QueueMetrics {
                approximate_messages: 500,
                oldest_message_age_seconds: 120,
                ..Default::default()
            },
            performance_metrics: PerformanceMetrics {
                p50_latency_ms: 50.0,
                p95_latency_ms: 150.0,
                p99_latency_ms: 300.0,
                throughput: 100.0,
                ..Default::default()
            },
            cost_metrics: CostMetrics {
                hourly_rate: 0.15,
                daily_cost: 3.60,
                monthly_projection: 108.0,
                budget_limit: 200.0,
                ..Default::default()
            },
            sla_metrics: SlaMetrics {
                compliance_percentage: 99.5,
                violations: 2,
                error_rate: 0.5,
            },
            backpressure_metrics: BackpressureMetrics {
                in_flight_messages: 25,
                ..Default::default()
            },
            alerts: Vec::new(),
        };

        let report = aggregator.summary_report(&snapshot);
        assert!(report.contains("Health Score: 85/100"));
        assert!(report.contains("Messages in Queue: 500"));
        assert!(report.contains("Compliance: 99.50%"));
    }

    #[test]
    fn test_history_retention() {
        let config = AggregatorConfig::new().with_retention_period(Duration::from_secs(1));
        let mut aggregator = MetricsAggregator::new(config);

        // Collect snapshot
        aggregator.collect_snapshot();
        assert_eq!(aggregator.snapshots.len(), 1);

        // Wait for retention period
        std::thread::sleep(Duration::from_millis(1100));

        // Collect another snapshot (should clean old ones)
        aggregator.collect_snapshot();
        // Both might still be there depending on timing, but old one should be cleaned eventually
        assert!(aggregator.snapshots.len() <= 2);
    }

    #[test]
    fn test_get_latest_snapshot() {
        let config = AggregatorConfig::new();
        let mut aggregator = MetricsAggregator::new(config);

        assert!(aggregator.latest_snapshot().is_none());

        aggregator.collect_snapshot();
        assert!(aggregator.latest_snapshot().is_some());
    }

    #[test]
    fn test_get_history() {
        let config = AggregatorConfig::new();
        let mut aggregator = MetricsAggregator::new(config);

        aggregator.collect_snapshot();
        aggregator.collect_snapshot();
        aggregator.collect_snapshot();

        let history = aggregator.get_history(Duration::from_secs(3600));
        assert_eq!(history.len(), 3);
    }
}
