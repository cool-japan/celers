// Copyright (c) 2026 COOLJAPAN OU (Team KitaSan)
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Advanced Dead Letter Queue (DLQ) analytics for AWS SQS.
//!
//! This module provides sophisticated analysis tools for DLQ messages:
//! - Error pattern detection and classification
//! - Retry recommendations based on failure analysis
//! - Root cause analysis for message failures
//! - DLQ message grouping and aggregation
//! - Automatic remediation suggestions
//!
//! # Examples
//!
//! ```
//! use celers_broker_sqs::dlq_analytics::{DlqAnalyzer, ErrorPattern};
//!
//! let analyzer = DlqAnalyzer::new();
//! let stats = analyzer.statistics();
//! println!("Total errors: {}", stats.total_errors);
//! ```

use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::HashMap;
use std::time::SystemTime;

/// Error pattern classification
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ErrorPattern {
    /// Transient errors (network timeout, temporary unavailability)
    Transient,
    /// Permanent errors (invalid data, business logic failure)
    Permanent,
    /// Resource errors (quota exceeded, rate limiting)
    Resource,
    /// Dependency errors (external service failure)
    Dependency,
    /// Unknown error pattern
    Unknown,
}

/// Error severity level
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ErrorSeverity {
    /// Low severity - can be retried without concern
    Low,
    /// Medium severity - should be retried with caution
    Medium,
    /// High severity - requires investigation before retry
    High,
    /// Critical severity - do not retry, immediate attention required
    Critical,
}

/// Retry recommendation based on error analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryRecommendation {
    /// Whether the message should be retried
    pub should_retry: bool,
    /// Recommended delay before retry (in seconds)
    pub recommended_delay_secs: u64,
    /// Maximum number of retry attempts
    pub max_retries: u32,
    /// Reason for the recommendation
    pub reason: String,
    /// Confidence level (0.0 - 1.0)
    pub confidence: f64,
}

/// DLQ message metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqMessage {
    /// Message ID
    pub message_id: String,
    /// Task name
    pub task_name: String,
    /// Error message
    pub error_message: String,
    /// Error pattern classification
    pub error_pattern: ErrorPattern,
    /// Error severity
    pub severity: ErrorSeverity,
    /// Number of times this message failed
    pub failure_count: u32,
    /// Timestamp of first failure
    pub first_failure: SystemTime,
    /// Timestamp of last failure
    pub last_failure: SystemTime,
}

/// DLQ analytics statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqStatistics {
    /// Total number of errors
    pub total_errors: usize,
    /// Errors by pattern
    pub errors_by_pattern: HashMap<ErrorPattern, usize>,
    /// Errors by severity
    pub errors_by_severity: HashMap<ErrorSeverity, usize>,
    /// Errors by task name
    pub errors_by_task: HashMap<String, usize>,
    /// Average failure count
    pub avg_failure_count: f64,
    /// Messages that can be retried
    pub retryable_count: usize,
    /// Messages that should not be retried
    pub non_retryable_count: usize,
}

/// DLQ analyzer for advanced analytics
///
/// Analyzes DLQ messages to detect patterns, classify errors, and provide
/// retry recommendations.
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::dlq_analytics::DlqAnalyzer;
///
/// let analyzer = DlqAnalyzer::new();
/// assert_eq!(analyzer.message_count(), 0);
/// ```
#[derive(Debug)]
pub struct DlqAnalyzer {
    /// Analyzed messages
    messages: Vec<DlqMessage>,
}

impl DlqAnalyzer {
    /// Create a new DLQ analyzer
    pub fn new() -> Self {
        Self {
            messages: Vec::new(),
        }
    }

    /// Add a message for analysis
    ///
    /// # Arguments
    ///
    /// * `message` - DLQ message to analyze
    pub fn add_message(&mut self, message: DlqMessage) {
        self.messages.push(message);
    }

    /// Classify error pattern from error message
    ///
    /// # Arguments
    ///
    /// * `error_message` - Error message to classify
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_broker_sqs::dlq_analytics::{DlqAnalyzer, ErrorPattern};
    ///
    /// let pattern = DlqAnalyzer::classify_error("Connection timeout");
    /// assert_eq!(pattern, ErrorPattern::Transient);
    /// ```
    pub fn classify_error(error_message: &str) -> ErrorPattern {
        let error_lower = error_message.to_lowercase();

        if error_lower.contains("timeout")
            || error_lower.contains("temporary")
            || error_lower.contains("retry")
            || error_lower.contains("network")
        {
            ErrorPattern::Transient
        } else if error_lower.contains("quota")
            || error_lower.contains("rate limit")
            || error_lower.contains("throttle")
        {
            ErrorPattern::Resource
        } else if error_lower.contains("service unavailable")
            || error_lower.contains("connection refused")
            || error_lower.contains("external")
        {
            ErrorPattern::Dependency
        } else if error_lower.contains("invalid")
            || error_lower.contains("parse")
            || error_lower.contains("validation")
            || error_lower.contains("not found")
        {
            ErrorPattern::Permanent
        } else {
            ErrorPattern::Unknown
        }
    }

    /// Determine error severity
    ///
    /// # Arguments
    ///
    /// * `error_pattern` - Classified error pattern
    /// * `failure_count` - Number of failures
    pub fn determine_severity(error_pattern: &ErrorPattern, failure_count: u32) -> ErrorSeverity {
        match error_pattern {
            ErrorPattern::Transient if failure_count < 3 => ErrorSeverity::Low,
            ErrorPattern::Transient => ErrorSeverity::Medium,
            ErrorPattern::Resource => ErrorSeverity::Medium,
            ErrorPattern::Dependency if failure_count < 5 => ErrorSeverity::Medium,
            ErrorPattern::Dependency => ErrorSeverity::High,
            ErrorPattern::Permanent => ErrorSeverity::High,
            ErrorPattern::Unknown if failure_count > 5 => ErrorSeverity::Critical,
            ErrorPattern::Unknown => ErrorSeverity::High,
        }
    }

    /// Generate retry recommendation for a message
    ///
    /// # Arguments
    ///
    /// * `message` - DLQ message to analyze
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_broker_sqs::dlq_analytics::{DlqAnalyzer, DlqMessage, ErrorPattern, ErrorSeverity};
    /// use std::time::SystemTime;
    ///
    /// let message = DlqMessage {
    ///     message_id: "msg-1".to_string(),
    ///     task_name: "tasks.process".to_string(),
    ///     error_message: "Connection timeout".to_string(),
    ///     error_pattern: ErrorPattern::Transient,
    ///     severity: ErrorSeverity::Low,
    ///     failure_count: 2,
    ///     first_failure: SystemTime::now(),
    ///     last_failure: SystemTime::now(),
    /// };
    ///
    /// let recommendation = DlqAnalyzer::recommend_retry(&message);
    /// assert!(recommendation.should_retry);
    /// ```
    pub fn recommend_retry(message: &DlqMessage) -> RetryRecommendation {
        match message.error_pattern {
            ErrorPattern::Transient if message.failure_count < 5 => RetryRecommendation {
                should_retry: true,
                recommended_delay_secs: 60 * (1 << message.failure_count.min(6)),
                max_retries: 5,
                reason: "Transient error - likely to succeed on retry".to_string(),
                confidence: 0.8,
            },
            ErrorPattern::Resource if message.failure_count < 3 => RetryRecommendation {
                should_retry: true,
                recommended_delay_secs: 300 * (1 << message.failure_count.min(4)),
                max_retries: 3,
                reason: "Resource constraint - retry with longer delay".to_string(),
                confidence: 0.7,
            },
            ErrorPattern::Dependency if message.failure_count < 10 => RetryRecommendation {
                should_retry: true,
                recommended_delay_secs: 180 * (1 << message.failure_count.min(5)),
                max_retries: 10,
                reason: "Dependency failure - service may recover".to_string(),
                confidence: 0.6,
            },
            ErrorPattern::Permanent => RetryRecommendation {
                should_retry: false,
                recommended_delay_secs: 0,
                max_retries: 0,
                reason: "Permanent error - requires code or data fix".to_string(),
                confidence: 0.9,
            },
            ErrorPattern::Unknown if message.failure_count < 3 => RetryRecommendation {
                should_retry: true,
                recommended_delay_secs: 120,
                max_retries: 3,
                reason: "Unknown error - cautious retry recommended".to_string(),
                confidence: 0.4,
            },
            _ => RetryRecommendation {
                should_retry: false,
                recommended_delay_secs: 0,
                max_retries: 0,
                reason: "Too many failures - manual intervention required".to_string(),
                confidence: 0.95,
            },
        }
    }

    /// Get statistics about DLQ messages
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_broker_sqs::dlq_analytics::DlqAnalyzer;
    ///
    /// let analyzer = DlqAnalyzer::new();
    /// let stats = analyzer.statistics();
    /// assert_eq!(stats.total_errors, 0);
    /// ```
    pub fn statistics(&self) -> DlqStatistics {
        let mut errors_by_pattern: HashMap<ErrorPattern, usize> = HashMap::new();
        let mut errors_by_severity: HashMap<ErrorSeverity, usize> = HashMap::new();
        let mut errors_by_task: HashMap<String, usize> = HashMap::new();

        let mut total_failures = 0u64;
        let mut retryable_count = 0;
        let mut non_retryable_count = 0;

        for message in &self.messages {
            *errors_by_pattern
                .entry(message.error_pattern.clone())
                .or_insert(0) += 1;
            *errors_by_severity.entry(message.severity).or_insert(0) += 1;
            *errors_by_task.entry(message.task_name.clone()).or_insert(0) += 1;
            total_failures += message.failure_count as u64;

            let recommendation = Self::recommend_retry(message);
            if recommendation.should_retry {
                retryable_count += 1;
            } else {
                non_retryable_count += 1;
            }
        }

        DlqStatistics {
            total_errors: self.messages.len(),
            errors_by_pattern,
            errors_by_severity,
            errors_by_task,
            avg_failure_count: if self.messages.is_empty() {
                0.0
            } else {
                total_failures as f64 / self.messages.len() as f64
            },
            retryable_count,
            non_retryable_count,
        }
    }

    /// Get messages that should be retried
    pub fn get_retryable_messages(&self) -> Vec<&DlqMessage> {
        self.messages
            .iter()
            .filter(|msg| Self::recommend_retry(msg).should_retry)
            .collect()
    }

    /// Get messages that should not be retried
    pub fn get_non_retryable_messages(&self) -> Vec<&DlqMessage> {
        self.messages
            .iter()
            .filter(|msg| !Self::recommend_retry(msg).should_retry)
            .collect()
    }

    /// Get top error patterns
    ///
    /// Returns the most common error patterns sorted by frequency
    pub fn top_error_patterns(&self, limit: usize) -> Vec<(ErrorPattern, usize)> {
        let stats = self.statistics();
        let mut patterns: Vec<_> = stats.errors_by_pattern.into_iter().collect();
        patterns.sort_by_key(|item| Reverse(item.1));
        patterns.into_iter().take(limit).collect()
    }

    /// Get top failing tasks
    ///
    /// Returns tasks with the most failures sorted by frequency
    pub fn top_failing_tasks(&self, limit: usize) -> Vec<(String, usize)> {
        let stats = self.statistics();
        let mut tasks: Vec<_> = stats.errors_by_task.into_iter().collect();
        tasks.sort_by_key(|item| Reverse(item.1));
        tasks.into_iter().take(limit).collect()
    }

    /// Get message count
    pub fn message_count(&self) -> usize {
        self.messages.len()
    }

    /// Clear all messages
    pub fn clear(&mut self) {
        self.messages.clear();
    }
}

impl Default for DlqAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dlq_analyzer_new() {
        let analyzer = DlqAnalyzer::new();
        assert_eq!(analyzer.message_count(), 0);
    }

    #[test]
    fn test_classify_error_transient() {
        assert_eq!(
            DlqAnalyzer::classify_error("Connection timeout"),
            ErrorPattern::Transient
        );
        assert_eq!(
            DlqAnalyzer::classify_error("Network error"),
            ErrorPattern::Transient
        );
    }

    #[test]
    fn test_classify_error_resource() {
        assert_eq!(
            DlqAnalyzer::classify_error("Quota exceeded"),
            ErrorPattern::Resource
        );
        assert_eq!(
            DlqAnalyzer::classify_error("Rate limit exceeded"),
            ErrorPattern::Resource
        );
    }

    #[test]
    fn test_classify_error_dependency() {
        assert_eq!(
            DlqAnalyzer::classify_error("Service unavailable"),
            ErrorPattern::Dependency
        );
        assert_eq!(
            DlqAnalyzer::classify_error("Connection refused"),
            ErrorPattern::Dependency
        );
    }

    #[test]
    fn test_classify_error_permanent() {
        assert_eq!(
            DlqAnalyzer::classify_error("Invalid data format"),
            ErrorPattern::Permanent
        );
        assert_eq!(
            DlqAnalyzer::classify_error("Validation failed"),
            ErrorPattern::Permanent
        );
    }

    #[test]
    fn test_determine_severity() {
        assert_eq!(
            DlqAnalyzer::determine_severity(&ErrorPattern::Transient, 1),
            ErrorSeverity::Low
        );
        assert_eq!(
            DlqAnalyzer::determine_severity(&ErrorPattern::Transient, 5),
            ErrorSeverity::Medium
        );
        assert_eq!(
            DlqAnalyzer::determine_severity(&ErrorPattern::Permanent, 1),
            ErrorSeverity::High
        );
    }

    #[test]
    fn test_recommend_retry_transient() {
        let message = DlqMessage {
            message_id: "msg-1".to_string(),
            task_name: "tasks.process".to_string(),
            error_message: "Connection timeout".to_string(),
            error_pattern: ErrorPattern::Transient,
            severity: ErrorSeverity::Low,
            failure_count: 2,
            first_failure: SystemTime::now(),
            last_failure: SystemTime::now(),
        };

        let recommendation = DlqAnalyzer::recommend_retry(&message);
        assert!(recommendation.should_retry);
        assert!(recommendation.confidence > 0.5);
    }

    #[test]
    fn test_recommend_retry_permanent() {
        let message = DlqMessage {
            message_id: "msg-1".to_string(),
            task_name: "tasks.process".to_string(),
            error_message: "Invalid data".to_string(),
            error_pattern: ErrorPattern::Permanent,
            severity: ErrorSeverity::High,
            failure_count: 1,
            first_failure: SystemTime::now(),
            last_failure: SystemTime::now(),
        };

        let recommendation = DlqAnalyzer::recommend_retry(&message);
        assert!(!recommendation.should_retry);
        assert!(recommendation.confidence > 0.8);
    }

    #[test]
    fn test_statistics() {
        let mut analyzer = DlqAnalyzer::new();

        analyzer.add_message(DlqMessage {
            message_id: "msg-1".to_string(),
            task_name: "tasks.process".to_string(),
            error_message: "Timeout".to_string(),
            error_pattern: ErrorPattern::Transient,
            severity: ErrorSeverity::Low,
            failure_count: 1,
            first_failure: SystemTime::now(),
            last_failure: SystemTime::now(),
        });

        let stats = analyzer.statistics();
        assert_eq!(stats.total_errors, 1);
        assert_eq!(
            stats.errors_by_pattern.get(&ErrorPattern::Transient),
            Some(&1)
        );
    }

    #[test]
    fn test_get_retryable_messages() {
        let mut analyzer = DlqAnalyzer::new();

        analyzer.add_message(DlqMessage {
            message_id: "msg-1".to_string(),
            task_name: "tasks.process".to_string(),
            error_message: "Timeout".to_string(),
            error_pattern: ErrorPattern::Transient,
            severity: ErrorSeverity::Low,
            failure_count: 1,
            first_failure: SystemTime::now(),
            last_failure: SystemTime::now(),
        });

        analyzer.add_message(DlqMessage {
            message_id: "msg-2".to_string(),
            task_name: "tasks.validate".to_string(),
            error_message: "Invalid".to_string(),
            error_pattern: ErrorPattern::Permanent,
            severity: ErrorSeverity::High,
            failure_count: 1,
            first_failure: SystemTime::now(),
            last_failure: SystemTime::now(),
        });

        let retryable = analyzer.get_retryable_messages();
        assert_eq!(retryable.len(), 1);
        assert_eq!(retryable[0].message_id, "msg-1");
    }

    #[test]
    fn test_top_error_patterns() {
        let mut analyzer = DlqAnalyzer::new();

        for i in 0..5 {
            analyzer.add_message(DlqMessage {
                message_id: format!("msg-{}", i),
                task_name: "tasks.process".to_string(),
                error_message: "Timeout".to_string(),
                error_pattern: ErrorPattern::Transient,
                severity: ErrorSeverity::Low,
                failure_count: 1,
                first_failure: SystemTime::now(),
                last_failure: SystemTime::now(),
            });
        }

        for i in 5..7 {
            analyzer.add_message(DlqMessage {
                message_id: format!("msg-{}", i),
                task_name: "tasks.validate".to_string(),
                error_message: "Invalid".to_string(),
                error_pattern: ErrorPattern::Permanent,
                severity: ErrorSeverity::High,
                failure_count: 1,
                first_failure: SystemTime::now(),
                last_failure: SystemTime::now(),
            });
        }

        let top = analyzer.top_error_patterns(2);
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].0, ErrorPattern::Transient);
        assert_eq!(top[0].1, 5);
    }

    #[test]
    fn test_clear() {
        let mut analyzer = DlqAnalyzer::new();
        analyzer.add_message(DlqMessage {
            message_id: "msg-1".to_string(),
            task_name: "tasks.process".to_string(),
            error_message: "Timeout".to_string(),
            error_pattern: ErrorPattern::Transient,
            severity: ErrorSeverity::Low,
            failure_count: 1,
            first_failure: SystemTime::now(),
            last_failure: SystemTime::now(),
        });

        assert_eq!(analyzer.message_count(), 1);
        analyzer.clear();
        assert_eq!(analyzer.message_count(), 0);
    }
}
