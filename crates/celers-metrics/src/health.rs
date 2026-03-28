//! Health check utilities, health scoring, and metric summary/reporting.

use crate::backends::CurrentMetrics;
use crate::prometheus_metrics::*;
use crate::slo::SloTarget;
use std::collections::HashMap;

// ============================================================================
// Health Check Utilities
// ============================================================================

/// Health status based on metrics and SLO targets
#[derive(Debug, Clone, PartialEq)]
pub enum HealthStatus {
    /// System is healthy and meeting all SLO targets
    Healthy,
    /// System is degraded but operational
    Degraded {
        /// Reasons for degradation
        reasons: Vec<String>,
    },
    /// System is unhealthy and not meeting SLO targets
    Unhealthy {
        /// Reasons for unhealthy status
        reasons: Vec<String>,
    },
}

/// Health check configuration
#[derive(Debug, Clone)]
pub struct HealthCheckConfig {
    /// Maximum acceptable queue size
    pub max_queue_size: f64,
    /// Maximum acceptable DLQ size
    pub max_dlq_size: f64,
    /// Minimum required active workers
    pub min_active_workers: f64,
    /// SLO target for compliance checking
    pub slo_target: Option<SloTarget>,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            max_queue_size: 1000.0,
            max_dlq_size: 100.0,
            min_active_workers: 1.0,
            slo_target: None,
        }
    }
}

impl HealthCheckConfig {
    /// Create a new health check configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set maximum queue size
    pub fn with_max_queue_size(mut self, size: f64) -> Self {
        self.max_queue_size = size;
        self
    }

    /// Set maximum DLQ size
    pub fn with_max_dlq_size(mut self, size: f64) -> Self {
        self.max_dlq_size = size;
        self
    }

    /// Set minimum active workers
    pub fn with_min_active_workers(mut self, count: f64) -> Self {
        self.min_active_workers = count;
        self
    }

    /// Set SLO target
    pub fn with_slo_target(mut self, target: SloTarget) -> Self {
        self.slo_target = Some(target);
        self
    }
}

/// Perform health check based on current metrics
///
/// # Examples
///
/// ```
/// use celers_metrics::{health_check, HealthCheckConfig, HealthStatus, SloTarget};
///
/// let config = HealthCheckConfig::new()
///     .with_max_queue_size(1000.0)
///     .with_min_active_workers(2.0);
///
/// match health_check(&config) {
///     HealthStatus::Healthy => println!("System is healthy"),
///     HealthStatus::Degraded { reasons } => {
///         println!("System is degraded: {:?}", reasons);
///     }
///     HealthStatus::Unhealthy { reasons } => {
///         println!("System is unhealthy: {:?}", reasons);
///     }
/// }
/// ```
pub fn health_check(config: &HealthCheckConfig) -> HealthStatus {
    let mut warnings = Vec::new();
    let mut errors = Vec::new();

    // Check queue sizes
    let queue_size = QUEUE_SIZE.get();
    if queue_size > config.max_queue_size * 0.8 {
        warnings.push(format!(
            "Queue size is high: {:.0}/{:.0}",
            queue_size, config.max_queue_size
        ));
    }
    if queue_size > config.max_queue_size {
        errors.push(format!(
            "Queue size exceeded limit: {:.0}/{:.0}",
            queue_size, config.max_queue_size
        ));
    }

    // Check DLQ size
    let dlq_size = DLQ_SIZE.get();
    if dlq_size > config.max_dlq_size * 0.5 {
        warnings.push(format!(
            "DLQ size is growing: {:.0}/{:.0}",
            dlq_size, config.max_dlq_size
        ));
    }
    if dlq_size > config.max_dlq_size {
        errors.push(format!(
            "DLQ size exceeded limit: {:.0}/{:.0}",
            dlq_size, config.max_dlq_size
        ));
    }

    // Check worker count
    let active_workers = ACTIVE_WORKERS.get();
    if active_workers < config.min_active_workers {
        errors.push(format!(
            "Insufficient workers: {:.0}/{:.0}",
            active_workers, config.min_active_workers
        ));
    }

    // Check SLO compliance if configured
    if let Some(ref slo_target) = config.slo_target {
        let completed = TASKS_COMPLETED_TOTAL.get();
        let failed = TASKS_FAILED_TOTAL.get();
        let success_rate = calculate_success_rate(completed, failed);

        if success_rate < slo_target.success_rate {
            errors.push(format!(
                "Success rate below SLO: {:.2}% < {:.2}%",
                success_rate * 100.0,
                slo_target.success_rate * 100.0
            ));
        }
    }

    if !errors.is_empty() {
        HealthStatus::Unhealthy { reasons: errors }
    } else if !warnings.is_empty() {
        HealthStatus::Degraded { reasons: warnings }
    } else {
        HealthStatus::Healthy
    }
}

// ============================================================================
// Metric Summary and Reporting
// ============================================================================

/// Generate a human-readable summary of current metrics
///
/// # Examples
///
/// ```
/// use celers_metrics::generate_metric_summary;
///
/// let summary = generate_metric_summary();
/// println!("{}", summary);
/// ```
pub fn generate_metric_summary() -> String {
    let metrics = CurrentMetrics::capture();

    format!(
        r"=== CeleRS Metrics Summary ===

Tasks:
  Enqueued:  {:>10.0}
  Completed: {:>10.0}
  Failed:    {:>10.0}
  Retried:   {:>10.0}
  Cancelled: {:>10.0}

Rates:
  Success:   {:>9.2}%
  Error:     {:>9.2}%

Queues:
  Pending:    {:>9.0}
  Processing: {:>9.0}
  DLQ:        {:>9.0}

Workers:
  Active:     {:>9.0}
",
        metrics.tasks_enqueued,
        metrics.tasks_completed,
        metrics.tasks_failed,
        metrics.tasks_retried,
        metrics.tasks_cancelled,
        metrics.success_rate() * 100.0,
        metrics.error_rate() * 100.0,
        metrics.queue_size,
        metrics.processing_queue_size,
        metrics.dlq_size,
        metrics.active_workers,
    )
}

// ============================================================================
// Health Score Calculator
// ============================================================================

/// Overall system health score (0.0 to 1.0)
#[derive(Debug, Clone)]
pub struct HealthScore {
    /// Overall health score (0.0 = critical, 1.0 = perfect)
    pub score: f64,
    /// Component scores
    pub components: HashMap<String, f64>,
    /// Health grade (A, B, C, D, F)
    pub grade: char,
    /// Issues affecting score
    pub issues: Vec<String>,
}

impl HealthScore {
    /// Calculate overall system health score
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_metrics::{HealthScore, SloTarget};
    ///
    /// celers_metrics::TASKS_ENQUEUED_TOTAL.inc_by(100.0);
    /// celers_metrics::TASKS_COMPLETED_TOTAL.inc_by(95.0);
    /// celers_metrics::TASKS_FAILED_TOTAL.inc_by(5.0);
    /// celers_metrics::ACTIVE_WORKERS.set(5.0);
    ///
    /// let slo = SloTarget {
    ///     success_rate: 0.99,
    ///     latency_seconds: 5.0,
    ///     throughput: 10.0,
    /// };
    ///
    /// let score = HealthScore::calculate(&slo);
    /// assert!(score.score > 0.0 && score.score <= 1.0);
    /// ```
    pub fn calculate(slo_target: &SloTarget) -> Self {
        let metrics = CurrentMetrics::capture();
        let mut components = HashMap::new();
        let mut issues = Vec::new();

        // Success rate component (40% weight)
        let success_rate = metrics.success_rate();
        let success_score = if success_rate >= slo_target.success_rate {
            1.0
        } else {
            (success_rate / slo_target.success_rate).max(0.0)
        };
        components.insert("success_rate".to_string(), success_score);

        if success_score < 1.0 {
            issues.push(format!(
                "Success rate ({:.1}%) below target ({:.1}%)",
                success_rate * 100.0,
                slo_target.success_rate * 100.0
            ));
        }

        // Queue health component (30% weight)
        let queue_score = if metrics.queue_size < 100.0 {
            1.0
        } else if metrics.queue_size < 500.0 {
            1.0 - (metrics.queue_size - 100.0) / 400.0 * 0.5
        } else {
            0.5 - ((metrics.queue_size - 500.0) / 500.0).min(0.5)
        };
        components.insert("queue_health".to_string(), queue_score);

        if queue_score < 0.8 {
            issues.push(format!("Queue backlog: {} tasks", metrics.queue_size));
        }

        // Worker availability component (20% weight)
        let worker_score = if metrics.active_workers >= 2.0 {
            1.0
        } else if metrics.active_workers >= 1.0 {
            0.5
        } else {
            0.0
        };
        components.insert("worker_availability".to_string(), worker_score);

        if worker_score < 1.0 {
            issues.push(format!("Low worker count: {}", metrics.active_workers));
        }

        // DLQ component (10% weight)
        let dlq_score = if metrics.dlq_size < 10.0 {
            1.0
        } else if metrics.dlq_size < 50.0 {
            1.0 - (metrics.dlq_size - 10.0) / 40.0 * 0.5
        } else {
            0.5
        };
        components.insert("dlq_health".to_string(), dlq_score);

        if dlq_score < 0.8 {
            issues.push(format!("DLQ growing: {} items", metrics.dlq_size));
        }

        // Calculate weighted overall score
        let score = success_score * 0.4 + queue_score * 0.3 + worker_score * 0.2 + dlq_score * 0.1;

        let grade = if score >= 0.9 {
            'A'
        } else if score >= 0.8 {
            'B'
        } else if score >= 0.7 {
            'C'
        } else if score >= 0.6 {
            'D'
        } else {
            'F'
        };

        Self {
            score,
            components,
            grade,
            issues,
        }
    }

    /// Format as human-readable report
    pub fn format_report(&self) -> String {
        let emoji = match self.grade {
            'A' => "✅",
            'B' => "✓",
            'C' => "⚠",
            'D' => "⚠⚠",
            _ => "❌",
        };

        let mut report = format!(
            r"=== System Health Score ===

Overall Score: {:.1}% (Grade: {}) {}

Component Scores:
",
            self.score * 100.0,
            self.grade,
            emoji
        );

        for (component, score) in &self.components {
            report.push_str(&format!("  {}: {:.1}%\n", component, score * 100.0));
        }

        if !self.issues.is_empty() {
            report.push_str("\nIssues:\n");
            for (i, issue) in self.issues.iter().enumerate() {
                report.push_str(&format!("  {}. {}\n", i + 1, issue));
            }
        }

        report
    }
}
