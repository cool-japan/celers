// Copyright (c) 2026 COOLJAPAN OU (Team KitaSan)
// SPDX-License-Identifier: Apache-2.0

//! Advanced Dead Letter Queue (DLQ) analytics for AWS SQS.
//!
//! This module provides sophisticated analysis tools for DLQ messages:
//! - Error pattern detection and classification
//! - Retry recommendations based on failure analysis
//! - Root cause analysis for message failures
//! - DLQ message grouping and aggregation
//! - Automatic remediation suggestions
//!
//! [`DlqMessage::failure_count`] should come from the broker's authoritative
//! `receive_count` (SQS's `ApproximateReceiveCount`) rather than a
//! caller-tracked counter, the same source
//! `replayable_from_envelope` (private to [`crate::replay`]) uses for
//! `ReplayableMessage::failure_count`.
//! [`DlqMessage::from_receive_count`] takes it directly, and
//! [`dlq_message_from_envelope`] reads it (plus the message id and an
//! enqueue-time lower bound for `first_failure`) straight from the broker
//! for a received envelope.
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
use std::time::{Duration, SystemTime};

use celers_kombu::Envelope;

use crate::broker_core::SqsBroker;
use crate::delivery::ReceiptMetadata;

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

impl DlqMessage {
    /// Build a [`DlqMessage`] from an id, task name and error, classifying
    /// the error and taking the failure count directly from the broker's
    /// authoritative `receive_count` (SQS's `ApproximateReceiveCount`)
    /// instead of a caller-tracked counter.
    ///
    /// `first_failure` and `last_failure` are both set to `now`: SQS system
    /// attributes record `SentTimestamp` (when the message was *enqueued*),
    /// never a failure time, so there is no better default available here.
    /// [`dlq_message_from_envelope`] uses `SentTimestamp` as a `first_failure`
    /// lower bound when it is available; set either field explicitly
    /// afterwards when a more precise time is known.
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_broker_sqs::dlq_analytics::{DlqMessage, ErrorPattern};
    ///
    /// let message = DlqMessage::from_receive_count(
    ///     "msg-1",
    ///     "tasks.process",
    ///     "Connection timeout",
    ///     4,
    /// );
    /// assert_eq!(message.error_pattern, ErrorPattern::Transient);
    /// assert_eq!(message.failure_count, 4);
    /// ```
    pub fn from_receive_count(
        message_id: impl Into<String>,
        task_name: impl Into<String>,
        error_message: impl Into<String>,
        receive_count: u32,
    ) -> Self {
        let error_message = error_message.into();
        let error_pattern = DlqAnalyzer::classify_error(&error_message);
        let severity = DlqAnalyzer::determine_severity(&error_pattern, receive_count);
        let now = SystemTime::now();

        Self {
            message_id: message_id.into(),
            task_name: task_name.into(),
            error_message,
            error_pattern,
            severity,
            failure_count: receive_count,
            first_failure: now,
            last_failure: now,
        }
    }
}

/// Build a [`DlqMessage`] from a DLQ envelope, taking the failure count from
/// the broker's authoritative [`SqsBroker::receive_count`] for the
/// envelope's delivery tag rather than a caller-tracked counter -- the same
/// source `replayable_from_envelope` (private to [`crate::replay`]) uses for
/// `ReplayableMessage::failure_count`. Falls back to `1` (first delivery)
/// when the broker holds no metadata for the tag.
///
/// `first_failure` is set from the message's `SentTimestamp` when the broker
/// recorded one: that is enqueue time, not failure time (SQS records no
/// failure timestamp), but it is a reasonable lower bound and the closest
/// proxy available. `last_failure` is `now`, matching
/// [`DlqMessage::from_receive_count`].
pub fn dlq_message_from_envelope(
    broker: &SqsBroker,
    envelope: &Envelope,
    error_message: impl Into<String>,
) -> DlqMessage {
    let metadata = broker.receipt_metadata(&envelope.delivery_tag);
    let receive_count = metadata.map(|m| m.receive_count).unwrap_or(1);
    let message_id = metadata
        .and_then(|m| m.message_id.clone())
        .unwrap_or_else(|| envelope.message.headers.id.to_string());

    let mut message = DlqMessage::from_receive_count(
        message_id,
        envelope.message.headers.task.clone(),
        error_message,
        receive_count,
    );

    if let Some(sent_secs) = metadata.and_then(ReceiptMetadata::sent_timestamp_secs) {
        if let Some(sent_at) = SystemTime::UNIX_EPOCH.checked_add(Duration::from_secs(sent_secs)) {
            message.first_failure = sent_at;
        }
    }

    message
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

    #[test]
    fn from_receive_count_classifies_and_carries_the_broker_count_through() {
        let message =
            DlqMessage::from_receive_count("msg-1", "tasks.process", "Invalid payload", 7);

        assert_eq!(message.message_id, "msg-1");
        assert_eq!(message.task_name, "tasks.process");
        assert_eq!(message.error_pattern, ErrorPattern::Permanent);
        // failure_count comes straight from receive_count, not a
        // caller-tracked counter.
        assert_eq!(message.failure_count, 7);
        assert_eq!(message.first_failure, message.last_failure);
    }

    #[tokio::test]
    async fn dlq_message_from_envelope_uses_the_brokers_receive_count() {
        use crate::delivery::{encode_delivery_tag, ReceiptMetadata};
        use celers_kombu::Envelope;
        use celers_protocol::Message;
        use uuid::Uuid;

        let mut broker = SqsBroker::new("tasks")
            .await
            .expect("broker construction does not touch the network");

        let tag = encode_delivery_tag("tasks", "AQEB");
        // SentTimestamp well in the past: a real DLQ message that has been
        // failing for a while, distinct from "now".
        broker.remember_receipt_metadata(
            &tag,
            ReceiptMetadata {
                message_id: Some("sqs-message-id".to_string()),
                receive_count: 6,
                sent_timestamp_ms: Some(1_700_000_000_000),
            },
        );

        let envelope = Envelope {
            delivery_tag: tag,
            message: Message::new("tasks.process".to_string(), Uuid::new_v4(), b"{}".to_vec()),
            redelivered: true,
        };

        let message = dlq_message_from_envelope(&broker, &envelope, "Service unavailable");

        assert_eq!(message.message_id, "sqs-message-id");
        assert_eq!(message.task_name, "tasks.process");
        assert_eq!(message.failure_count, 6);
        assert_eq!(message.error_pattern, ErrorPattern::Dependency);
        // first_failure is derived from SentTimestamp (enqueue time), which
        // is fixed and in the past -- it must not collapse to `now`, the
        // way `DlqMessage::from_receive_count` alone would set it.
        assert_eq!(
            message.first_failure,
            SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000)
        );
        assert!(message.first_failure < message.last_failure);
    }

    #[tokio::test]
    async fn dlq_message_from_envelope_falls_back_without_broker_metadata() {
        use celers_kombu::Envelope;
        use celers_protocol::Message;
        use uuid::Uuid;

        let broker = SqsBroker::new("tasks")
            .await
            .expect("broker construction does not touch the network");

        let message_id = Uuid::new_v4();
        let envelope = Envelope {
            // No `remember_receipt_metadata` call for this tag: the broker
            // has nothing recorded for it.
            delivery_tag: "AQEB-unknown".to_string(),
            message: Message::new("tasks.process".to_string(), message_id, b"{}".to_vec()),
            redelivered: false,
        };

        let message = dlq_message_from_envelope(&broker, &envelope, "Connection timeout");

        // Falls back to first delivery (1) rather than panicking or
        // reporting an arbitrary count.
        assert_eq!(message.failure_count, 1);
        assert_eq!(message.message_id, message_id.to_string());
    }
}
