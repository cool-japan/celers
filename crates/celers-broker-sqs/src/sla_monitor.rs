//! SLA (Service Level Agreement) monitoring for queue performance
//!
//! This module provides utilities for tracking and monitoring SLA compliance
//! for message processing, including latency targets, throughput targets,
//! and error rate targets.
//!
//! # Features
//!
//! - Message processing latency tracking (P50, P95, P99)
//! - Throughput monitoring (messages/sec)
//! - Error rate tracking
//! - SLA compliance reporting
//! - Alert generation for SLA violations
//! - Historical trend analysis
//!
//! # Example
//!
//! ```rust
//! use celers_broker_sqs::sla_monitor::{SlaMonitor, SlaTarget};
//! use std::time::Duration;
//!
//! // Define SLA targets
//! let targets = SlaTarget::new()
//!     .with_latency_p99(Duration::from_secs(5))
//!     .with_min_throughput(100.0)  // 100 messages/sec
//!     .with_max_error_rate(0.01);   // 1% error rate
//!
//! let mut monitor = SlaMonitor::new(targets);
//!
//! // Record message processing
//! monitor.record_message(Duration::from_millis(150), true);
//! monitor.record_message(Duration::from_millis(200), true);
//! monitor.record_message(Duration::from_millis(300), false);  // Failed
//!
//! // Check SLA compliance
//! let report = monitor.generate_report();
//! println!("SLA Compliance: {:.2}%", report.overall_compliance * 100.0);
//! ```

use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// SLA target thresholds
#[derive(Debug, Clone)]
pub struct SlaTarget {
    /// Target P50 latency (median)
    pub latency_p50: Option<Duration>,
    /// Target P95 latency
    pub latency_p95: Option<Duration>,
    /// Target P99 latency
    pub latency_p99: Option<Duration>,
    /// Maximum acceptable latency
    pub max_latency: Option<Duration>,
    /// Minimum throughput (messages/sec)
    pub min_throughput: Option<f64>,
    /// Maximum error rate (0.0-1.0)
    pub max_error_rate: Option<f64>,
    /// Target message age (for backlog monitoring)
    pub max_message_age: Option<Duration>,
}

impl SlaTarget {
    /// Create a new SLA target with no defaults
    pub fn new() -> Self {
        Self {
            latency_p50: None,
            latency_p95: None,
            latency_p99: None,
            max_latency: None,
            min_throughput: None,
            max_error_rate: None,
            max_message_age: None,
        }
    }

    /// Set P50 latency target
    pub fn with_latency_p50(mut self, duration: Duration) -> Self {
        self.latency_p50 = Some(duration);
        self
    }

    /// Set P95 latency target
    pub fn with_latency_p95(mut self, duration: Duration) -> Self {
        self.latency_p95 = Some(duration);
        self
    }

    /// Set P99 latency target
    pub fn with_latency_p99(mut self, duration: Duration) -> Self {
        self.latency_p99 = Some(duration);
        self
    }

    /// Set maximum latency
    pub fn with_max_latency(mut self, duration: Duration) -> Self {
        self.max_latency = Some(duration);
        self
    }

    /// Set minimum throughput (messages/sec)
    pub fn with_min_throughput(mut self, throughput: f64) -> Self {
        self.min_throughput = Some(throughput);
        self
    }

    /// Set maximum error rate (0.0-1.0)
    pub fn with_max_error_rate(mut self, rate: f64) -> Self {
        self.max_error_rate = Some(rate.clamp(0.0, 1.0));
        self
    }

    /// Set maximum message age
    pub fn with_max_message_age(mut self, age: Duration) -> Self {
        self.max_message_age = Some(age);
        self
    }

    /// Create common production SLA targets
    pub fn production() -> Self {
        Self::new()
            .with_latency_p50(Duration::from_secs(1))
            .with_latency_p95(Duration::from_secs(3))
            .with_latency_p99(Duration::from_secs(5))
            .with_max_latency(Duration::from_secs(30))
            .with_min_throughput(10.0)
            .with_max_error_rate(0.01) // 1%
            .with_max_message_age(Duration::from_secs(300)) // 5 minutes
    }

    /// Create strict SLA targets for critical systems
    pub fn critical() -> Self {
        Self::new()
            .with_latency_p50(Duration::from_millis(500))
            .with_latency_p95(Duration::from_secs(1))
            .with_latency_p99(Duration::from_secs(2))
            .with_max_latency(Duration::from_secs(10))
            .with_min_throughput(50.0)
            .with_max_error_rate(0.001) // 0.1%
            .with_max_message_age(Duration::from_secs(60)) // 1 minute
    }
}

impl Default for SlaTarget {
    fn default() -> Self {
        Self::production()
    }
}

/// SLA compliance status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComplianceStatus {
    /// Meeting all SLA targets
    Compliant,
    /// Approaching SLA violation (warning threshold)
    Warning,
    /// SLA violation detected
    Violation,
}

/// SLA violation details
#[derive(Debug, Clone)]
pub struct SlaViolation {
    /// Type of violation
    pub violation_type: ViolationType,
    /// Target value
    pub target: f64,
    /// Actual value
    pub actual: f64,
    /// Timestamp of violation
    pub timestamp: Instant,
    /// Severity (0.0-1.0, higher is more severe)
    pub severity: f64,
}

/// Type of SLA violation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ViolationType {
    /// Latency exceeded target
    LatencyP50,
    LatencyP95,
    LatencyP99,
    MaxLatency,
    /// Throughput below target
    Throughput,
    /// Error rate above target
    ErrorRate,
    /// Message age exceeded target
    MessageAge,
}

/// Message processing record
#[derive(Debug, Clone)]
struct MessageRecord {
    /// Processing latency
    latency: Duration,
    /// Success/failure
    success: bool,
    /// Timestamp
    timestamp: Instant,
}

/// SLA compliance report
#[derive(Debug, Clone)]
pub struct SlaReport {
    /// Overall compliance status
    pub status: ComplianceStatus,
    /// Overall compliance percentage (0.0-1.0)
    pub overall_compliance: f64,
    /// Latency compliance
    pub latency_compliance: Option<f64>,
    /// Throughput compliance
    pub throughput_compliance: Option<f64>,
    /// Error rate compliance
    pub error_rate_compliance: Option<f64>,
    /// Current P50 latency
    pub current_p50: Duration,
    /// Current P95 latency
    pub current_p95: Duration,
    /// Current P99 latency
    pub current_p99: Duration,
    /// Current throughput (messages/sec)
    pub current_throughput: f64,
    /// Current error rate (0.0-1.0)
    pub current_error_rate: f64,
    /// Active violations
    pub violations: Vec<SlaViolation>,
    /// Total messages processed
    pub total_messages: usize,
    /// Time window of report
    pub window_duration: Duration,
}

/// SLA monitor
pub struct SlaMonitor {
    /// SLA targets
    targets: SlaTarget,
    /// Message processing records (sliding window)
    records: VecDeque<MessageRecord>,
    /// Maximum records to keep (for sliding window)
    max_records: usize,
    /// Window duration for metrics
    window_duration: Duration,
    /// Start time for throughput calculation
    start_time: Option<Instant>,
}

impl SlaMonitor {
    /// Create a new SLA monitor
    pub fn new(targets: SlaTarget) -> Self {
        Self {
            targets,
            records: VecDeque::new(),
            max_records: 10000,
            window_duration: Duration::from_secs(60), // 1 minute window
            start_time: None,
        }
    }

    /// Set maximum records to keep
    pub fn with_max_records(mut self, max: usize) -> Self {
        self.max_records = max;
        self
    }

    /// Set window duration for metrics
    pub fn with_window_duration(mut self, duration: Duration) -> Self {
        self.window_duration = duration;
        self
    }

    /// Record a processed message
    pub fn record_message(&mut self, latency: Duration, success: bool) {
        if self.start_time.is_none() {
            self.start_time = Some(Instant::now());
        }

        let record = MessageRecord {
            latency,
            success,
            timestamp: Instant::now(),
        };

        self.records.push_back(record);

        // Trim old records outside the window
        self.trim_old_records();

        // Enforce max records limit
        while self.records.len() > self.max_records {
            self.records.pop_front();
        }
    }

    /// Remove records outside the time window
    fn trim_old_records(&mut self) {
        let cutoff = Instant::now() - self.window_duration;
        while let Some(record) = self.records.front() {
            if record.timestamp < cutoff {
                self.records.pop_front();
            } else {
                break;
            }
        }
    }

    /// Calculate percentile latency
    fn calculate_percentile(&self, percentile: f64) -> Duration {
        if self.records.is_empty() {
            return Duration::from_secs(0);
        }

        let mut latencies: Vec<Duration> = self.records.iter().map(|r| r.latency).collect();
        latencies.sort();

        let index = ((latencies.len() as f64 - 1.0) * percentile) as usize;
        latencies[index.min(latencies.len() - 1)]
    }

    /// Calculate current throughput (messages/sec)
    fn calculate_throughput(&self) -> f64 {
        if self.records.is_empty() || self.start_time.is_none() {
            return 0.0;
        }

        let elapsed = self.start_time.unwrap().elapsed();
        if elapsed.as_secs_f64() == 0.0 {
            return 0.0;
        }

        self.records.len() as f64 / elapsed.as_secs_f64()
    }

    /// Calculate current error rate
    fn calculate_error_rate(&self) -> f64 {
        if self.records.is_empty() {
            return 0.0;
        }

        let errors = self.records.iter().filter(|r| !r.success).count();
        errors as f64 / self.records.len() as f64
    }

    /// Check for SLA violations
    fn check_violations(&self) -> Vec<SlaViolation> {
        let mut violations = Vec::new();
        let now = Instant::now();

        // Check latency violations
        if let Some(target) = self.targets.latency_p50 {
            let actual = self.calculate_percentile(0.5);
            if actual > target {
                violations.push(SlaViolation {
                    violation_type: ViolationType::LatencyP50,
                    target: target.as_secs_f64(),
                    actual: actual.as_secs_f64(),
                    timestamp: now,
                    severity: (actual.as_secs_f64() / target.as_secs_f64()) - 1.0,
                });
            }
        }

        if let Some(target) = self.targets.latency_p95 {
            let actual = self.calculate_percentile(0.95);
            if actual > target {
                violations.push(SlaViolation {
                    violation_type: ViolationType::LatencyP95,
                    target: target.as_secs_f64(),
                    actual: actual.as_secs_f64(),
                    timestamp: now,
                    severity: (actual.as_secs_f64() / target.as_secs_f64()) - 1.0,
                });
            }
        }

        if let Some(target) = self.targets.latency_p99 {
            let actual = self.calculate_percentile(0.99);
            if actual > target {
                violations.push(SlaViolation {
                    violation_type: ViolationType::LatencyP99,
                    target: target.as_secs_f64(),
                    actual: actual.as_secs_f64(),
                    timestamp: now,
                    severity: (actual.as_secs_f64() / target.as_secs_f64()) - 1.0,
                });
            }
        }

        // Check throughput violation
        if let Some(target) = self.targets.min_throughput {
            let actual = self.calculate_throughput();
            if actual < target {
                violations.push(SlaViolation {
                    violation_type: ViolationType::Throughput,
                    target,
                    actual,
                    timestamp: now,
                    severity: (target - actual) / target,
                });
            }
        }

        // Check error rate violation
        if let Some(target) = self.targets.max_error_rate {
            let actual = self.calculate_error_rate();
            if actual > target {
                violations.push(SlaViolation {
                    violation_type: ViolationType::ErrorRate,
                    target,
                    actual,
                    timestamp: now,
                    severity: (actual - target) / (1.0 - target),
                });
            }
        }

        violations
    }

    /// Generate SLA compliance report
    pub fn generate_report(&self) -> SlaReport {
        let violations = self.check_violations();
        let p50 = self.calculate_percentile(0.5);
        let p95 = self.calculate_percentile(0.95);
        let p99 = self.calculate_percentile(0.99);
        let throughput = self.calculate_throughput();
        let error_rate = self.calculate_error_rate();

        // Calculate compliance scores
        let mut compliance_scores = Vec::new();

        // Latency compliance
        let latency_compliance = self.calculate_latency_compliance(p50, p95, p99);
        if let Some(score) = latency_compliance {
            compliance_scores.push(score);
        }

        // Throughput compliance
        let throughput_compliance = self
            .targets
            .min_throughput
            .map(|target| (throughput / target).min(1.0));
        if let Some(score) = throughput_compliance {
            compliance_scores.push(score);
        }

        // Error rate compliance
        let error_rate_compliance = self.targets.max_error_rate.map(|target| {
            if target == 0.0 {
                if error_rate == 0.0 {
                    1.0
                } else {
                    0.0
                }
            } else {
                (1.0 - (error_rate / target)).clamp(0.0, 1.0)
            }
        });
        if let Some(score) = error_rate_compliance {
            compliance_scores.push(score);
        }

        // Overall compliance
        let overall_compliance = if compliance_scores.is_empty() {
            1.0
        } else {
            compliance_scores.iter().sum::<f64>() / compliance_scores.len() as f64
        };

        // Determine status
        let status = if !violations.is_empty() {
            ComplianceStatus::Violation
        } else if overall_compliance < 0.95 {
            ComplianceStatus::Warning
        } else {
            ComplianceStatus::Compliant
        };

        SlaReport {
            status,
            overall_compliance,
            latency_compliance,
            throughput_compliance,
            error_rate_compliance,
            current_p50: p50,
            current_p95: p95,
            current_p99: p99,
            current_throughput: throughput,
            current_error_rate: error_rate,
            violations,
            total_messages: self.records.len(),
            window_duration: self.window_duration,
        }
    }

    /// Calculate latency compliance score
    fn calculate_latency_compliance(
        &self,
        p50: Duration,
        p95: Duration,
        p99: Duration,
    ) -> Option<f64> {
        let mut scores = Vec::new();

        if let Some(target) = self.targets.latency_p50 {
            let score = (target.as_secs_f64() / p50.as_secs_f64()).min(1.0);
            scores.push(score);
        }

        if let Some(target) = self.targets.latency_p95 {
            let score = (target.as_secs_f64() / p95.as_secs_f64()).min(1.0);
            scores.push(score);
        }

        if let Some(target) = self.targets.latency_p99 {
            let score = (target.as_secs_f64() / p99.as_secs_f64()).min(1.0);
            scores.push(score);
        }

        if scores.is_empty() {
            None
        } else {
            Some(scores.iter().sum::<f64>() / scores.len() as f64)
        }
    }

    /// Reset all metrics
    pub fn reset(&mut self) {
        self.records.clear();
        self.start_time = None;
    }

    /// Get current metrics count
    pub fn record_count(&self) -> usize {
        self.records.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sla_target_builder() {
        let target = SlaTarget::new()
            .with_latency_p99(Duration::from_secs(5))
            .with_min_throughput(100.0)
            .with_max_error_rate(0.01);

        assert_eq!(target.latency_p99, Some(Duration::from_secs(5)));
        assert_eq!(target.min_throughput, Some(100.0));
        assert_eq!(target.max_error_rate, Some(0.01));
    }

    #[test]
    fn test_sla_target_production() {
        let target = SlaTarget::production();
        assert!(target.latency_p50.is_some());
        assert!(target.latency_p95.is_some());
        assert!(target.latency_p99.is_some());
        assert!(target.min_throughput.is_some());
        assert!(target.max_error_rate.is_some());
    }

    #[test]
    fn test_sla_monitor_record_message() {
        let target = SlaTarget::new();
        let mut monitor = SlaMonitor::new(target);

        monitor.record_message(Duration::from_millis(100), true);
        monitor.record_message(Duration::from_millis(200), true);
        monitor.record_message(Duration::from_millis(150), false);

        assert_eq!(monitor.record_count(), 3);
    }

    #[test]
    fn test_sla_monitor_percentile_calculation() {
        let target = SlaTarget::new();
        let mut monitor = SlaMonitor::new(target);

        // Add sorted latencies for easy verification
        for ms in [100, 200, 300, 400, 500] {
            monitor.record_message(Duration::from_millis(ms), true);
        }

        let p50 = monitor.calculate_percentile(0.5);
        assert!(p50 >= Duration::from_millis(250) && p50 <= Duration::from_millis(350));
    }

    #[test]
    fn test_sla_monitor_error_rate() {
        let target = SlaTarget::new();
        let mut monitor = SlaMonitor::new(target);

        monitor.record_message(Duration::from_millis(100), true);
        monitor.record_message(Duration::from_millis(100), true);
        monitor.record_message(Duration::from_millis(100), true);
        monitor.record_message(Duration::from_millis(100), false); // 1 error

        let error_rate = monitor.calculate_error_rate();
        assert_eq!(error_rate, 0.25); // 25%
    }

    #[test]
    fn test_sla_violation_detection() {
        let target = SlaTarget::new()
            .with_latency_p99(Duration::from_millis(100))
            .with_max_error_rate(0.1);

        let mut monitor = SlaMonitor::new(target);

        // Add messages that violate P99 latency
        for _ in 0..100 {
            monitor.record_message(Duration::from_millis(200), true);
        }

        let violations = monitor.check_violations();
        assert!(!violations.is_empty());
        assert!(violations
            .iter()
            .any(|v| v.violation_type == ViolationType::LatencyP99));
    }

    #[test]
    fn test_sla_report_generation() {
        let target = SlaTarget::new()
            .with_latency_p99(Duration::from_secs(1))
            .with_min_throughput(10.0)
            .with_max_error_rate(0.05);

        let mut monitor = SlaMonitor::new(target);

        for _ in 0..50 {
            monitor.record_message(Duration::from_millis(100), true);
        }

        let report = monitor.generate_report();
        assert_eq!(report.total_messages, 50);
        assert!(report.current_error_rate < 0.05);
    }

    #[test]
    fn test_compliance_status() {
        let target = SlaTarget::new()
            .with_latency_p99(Duration::from_secs(1))
            .with_max_error_rate(0.01);

        let mut monitor = SlaMonitor::new(target);

        // Good performance
        for _ in 0..100 {
            monitor.record_message(Duration::from_millis(100), true);
        }

        let report = monitor.generate_report();
        assert_eq!(report.status, ComplianceStatus::Compliant);
    }

    #[test]
    fn test_sla_monitor_reset() {
        let target = SlaTarget::new();
        let mut monitor = SlaMonitor::new(target);

        monitor.record_message(Duration::from_millis(100), true);
        monitor.record_message(Duration::from_millis(200), true);

        assert_eq!(monitor.record_count(), 2);

        monitor.reset();
        assert_eq!(monitor.record_count(), 0);
    }

    #[test]
    fn test_max_records_limit() {
        let target = SlaTarget::new();
        let mut monitor = SlaMonitor::new(target).with_max_records(100);

        for _ in 0..150 {
            monitor.record_message(Duration::from_millis(100), true);
        }

        assert!(monitor.record_count() <= 100);
    }

    #[test]
    fn test_error_rate_clamping() {
        let target = SlaTarget::new().with_max_error_rate(1.5);
        assert_eq!(target.max_error_rate, Some(1.0));

        let target2 = SlaTarget::new().with_max_error_rate(-0.1);
        assert_eq!(target2.max_error_rate, Some(0.0));
    }
}
