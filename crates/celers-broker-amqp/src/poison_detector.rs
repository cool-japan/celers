//! Poison message detection for AMQP
//!
//! Identifies and handles messages that repeatedly fail processing, preventing
//! them from blocking healthy message flow.
//!
//! # Features
//!
//! - **Retry Tracking** - Track number of retries per message
//! - **Failure Pattern Detection** - Identify systematic failures
//! - **Automatic Quarantine** - Move poison messages to quarantine queue
//! - **Configurable Thresholds** - Customize retry limits and detection criteria
//! - **Detailed Analytics** - Analyze poison message patterns
//!
//! # Example
//!
//! ```rust
//! use celers_broker_amqp::poison_detector::{PoisonDetector, PoisonDetectorConfig};
//! use std::time::Duration;
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = PoisonDetectorConfig {
//!     max_retries: 3,
//!     retry_window: Duration::from_secs(300),
//!     failure_rate_threshold: 0.8,
//!     min_samples: 5,
//! };
//!
//! let mut detector = PoisonDetector::new(config);
//!
//! // Track message processing
//! let message_id = "msg-123";
//! detector.record_failure(message_id, "Processing error");
//! detector.record_failure(message_id, "Timeout");
//! detector.record_failure(message_id, "Invalid data");
//!
//! // Check if message is poisoned
//! if detector.is_poison(message_id) {
//!     println!("Message {} is poisoned!", message_id);
//!     let info = detector.get_message_info(message_id).unwrap();
//!     println!("Failures: {}", info.failure_count);
//!     println!("Last error: {:?}", info.last_error);
//! }
//! # Ok(())
//! # }
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Poison detector configuration
#[derive(Debug, Clone)]
pub struct PoisonDetectorConfig {
    /// Maximum retries before considering a message poisoned
    pub max_retries: u32,
    /// Time window for counting retries
    pub retry_window: Duration,
    /// Failure rate threshold (0.0 - 1.0) for pattern detection
    pub failure_rate_threshold: f64,
    /// Minimum samples needed for pattern detection
    pub min_samples: usize,
}

impl Default for PoisonDetectorConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_window: Duration::from_secs(300), // 5 minutes
            failure_rate_threshold: 0.8,
            min_samples: 5,
        }
    }
}

/// Information about a message being tracked
#[derive(Debug, Clone)]
pub struct MessageInfo {
    /// Message identifier
    pub message_id: String,
    /// Number of failures
    pub failure_count: u32,
    /// Number of successes
    pub success_count: u32,
    /// First seen timestamp
    pub first_seen: Instant,
    /// Last failure timestamp
    pub last_failure: Option<Instant>,
    /// Last error message
    pub last_error: Option<String>,
    /// Whether the message is marked as poisoned
    pub is_poisoned: bool,
    /// Failure history (error messages)
    pub failure_history: Vec<String>,
}

/// Poison message analytics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PoisonAnalytics {
    /// Total messages tracked
    pub total_tracked: u64,
    /// Number of poison messages detected
    pub poison_count: u64,
    /// Total failures across all messages
    pub total_failures: u64,
    /// Total successes across all messages
    pub total_successes: u64,
    /// Overall failure rate
    pub failure_rate: f64,
    /// Most common error patterns
    pub common_errors: Vec<(String, u64)>,
}

/// Poison message detector
#[derive(Debug)]
pub struct PoisonDetector {
    config: PoisonDetectorConfig,
    messages: HashMap<String, MessageInfo>,
    analytics: PoisonAnalytics,
    error_counts: HashMap<String, u64>,
}

impl PoisonDetector {
    /// Create a new poison detector
    pub fn new(config: PoisonDetectorConfig) -> Self {
        Self {
            config,
            messages: HashMap::new(),
            analytics: PoisonAnalytics::default(),
            error_counts: HashMap::new(),
        }
    }

    /// Record a message failure
    pub fn record_failure(&mut self, message_id: &str, error: &str) {
        let now = Instant::now();

        let info = self
            .messages
            .entry(message_id.to_string())
            .or_insert_with(|| {
                self.analytics.total_tracked += 1;
                MessageInfo {
                    message_id: message_id.to_string(),
                    failure_count: 0,
                    success_count: 0,
                    first_seen: now,
                    last_failure: None,
                    last_error: None,
                    is_poisoned: false,
                    failure_history: Vec::new(),
                }
            });

        // Check if within retry window
        if let Some(last_failure) = info.last_failure {
            if now.duration_since(last_failure) > self.config.retry_window {
                // Reset counters if outside retry window
                info.failure_count = 0;
                info.failure_history.clear();
            }
        }

        info.failure_count += 1;
        info.last_failure = Some(now);
        info.last_error = Some(error.to_string());
        info.failure_history.push(error.to_string());

        self.analytics.total_failures += 1;

        // Track error patterns
        *self.error_counts.entry(error.to_string()).or_insert(0) += 1;

        // Check if message should be marked as poisoned
        if info.failure_count >= self.config.max_retries && !info.is_poisoned {
            info.is_poisoned = true;
            self.analytics.poison_count += 1;
        }

        self.update_analytics();
    }

    /// Record a message success
    pub fn record_success(&mut self, message_id: &str) {
        let now = Instant::now();

        let info = self
            .messages
            .entry(message_id.to_string())
            .or_insert_with(|| {
                self.analytics.total_tracked += 1;
                MessageInfo {
                    message_id: message_id.to_string(),
                    failure_count: 0,
                    success_count: 0,
                    first_seen: now,
                    last_failure: None,
                    last_error: None,
                    is_poisoned: false,
                    failure_history: Vec::new(),
                }
            });

        info.success_count += 1;
        self.analytics.total_successes += 1;

        // Reset failure count on success (message is healthy)
        info.failure_count = 0;
        info.is_poisoned = false;

        self.update_analytics();
    }

    /// Check if a message is poisoned
    pub fn is_poison(&self, message_id: &str) -> bool {
        self.messages
            .get(message_id)
            .map(|info| info.is_poisoned)
            .unwrap_or(false)
    }

    /// Get information about a tracked message
    pub fn get_message_info(&self, message_id: &str) -> Option<&MessageInfo> {
        self.messages.get(message_id)
    }

    /// Get all poisoned messages
    pub fn get_poisoned_messages(&self) -> Vec<&MessageInfo> {
        self.messages
            .values()
            .filter(|info| info.is_poisoned)
            .collect()
    }

    /// Detect failure patterns across messages
    pub fn detect_failure_pattern(&self, error_pattern: &str) -> bool {
        let pattern_count = self
            .messages
            .values()
            .filter(|info| {
                info.last_error
                    .as_ref()
                    .map(|e| e.contains(error_pattern))
                    .unwrap_or(false)
            })
            .count();

        pattern_count >= self.config.min_samples
            && (pattern_count as f64 / self.messages.len() as f64)
                >= self.config.failure_rate_threshold
    }

    /// Get analytics
    pub fn analytics(&self) -> &PoisonAnalytics {
        &self.analytics
    }

    /// Clean up old messages outside the retry window
    pub fn cleanup_old_messages(&mut self) {
        let now = Instant::now();
        let retry_window = self.config.retry_window;

        self.messages.retain(|_, info| {
            if let Some(last_failure) = info.last_failure {
                now.duration_since(last_failure) < retry_window * 2
            } else {
                now.duration_since(info.first_seen) < retry_window * 2
            }
        });
    }

    /// Mark a message as quarantined (externally handled)
    pub fn quarantine_message(&mut self, message_id: &str) {
        if let Some(info) = self.messages.get_mut(message_id) {
            info.is_poisoned = true;
        }
    }

    /// Remove a message from tracking
    pub fn remove_message(&mut self, message_id: &str) -> Option<MessageInfo> {
        self.messages.remove(message_id)
    }

    /// Reset all statistics
    pub fn reset(&mut self) {
        self.messages.clear();
        self.analytics = PoisonAnalytics::default();
        self.error_counts.clear();
    }

    /// Update analytics based on current state
    fn update_analytics(&mut self) {
        // Calculate failure rate
        let total_attempts = self.analytics.total_failures + self.analytics.total_successes;
        if total_attempts > 0 {
            self.analytics.failure_rate =
                self.analytics.total_failures as f64 / total_attempts as f64;
        }

        // Update common errors
        let mut errors: Vec<(String, u64)> = self
            .error_counts
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        errors.sort_by(|a, b| b.1.cmp(&a.1));
        self.analytics.common_errors = errors.into_iter().take(10).collect();
    }

    /// Get failure rate for a specific message
    pub fn get_message_failure_rate(&self, message_id: &str) -> Option<f64> {
        self.messages.get(message_id).map(|info| {
            let total = info.failure_count + info.success_count;
            if total > 0 {
                info.failure_count as f64 / total as f64
            } else {
                0.0
            }
        })
    }

    /// Check if system-wide failure rate exceeds threshold
    pub fn is_systemic_failure(&self) -> bool {
        self.messages.len() >= self.config.min_samples
            && self.analytics.failure_rate >= self.config.failure_rate_threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_poison_detector_creation() {
        let config = PoisonDetectorConfig::default();
        let detector = PoisonDetector::new(config);
        assert_eq!(detector.analytics.total_tracked, 0);
        assert_eq!(detector.analytics.poison_count, 0);
    }

    #[test]
    fn test_failure_tracking() {
        let config = PoisonDetectorConfig {
            max_retries: 3,
            ..Default::default()
        };
        let mut detector = PoisonDetector::new(config);

        let msg_id = "test-msg-1";

        // Record failures
        detector.record_failure(msg_id, "Error 1");
        detector.record_failure(msg_id, "Error 2");
        assert!(!detector.is_poison(msg_id));

        detector.record_failure(msg_id, "Error 3");
        assert!(detector.is_poison(msg_id));

        let info = detector.get_message_info(msg_id).unwrap();
        assert_eq!(info.failure_count, 3);
        assert_eq!(info.failure_history.len(), 3);
    }

    #[test]
    fn test_success_resets_poison() {
        let config = PoisonDetectorConfig::default();
        let mut detector = PoisonDetector::new(config);

        let msg_id = "test-msg-2";

        // Make it poisoned
        for i in 0..5 {
            detector.record_failure(msg_id, &format!("Error {}", i));
        }
        assert!(detector.is_poison(msg_id));

        // Success should reset
        detector.record_success(msg_id);
        assert!(!detector.is_poison(msg_id));

        let info = detector.get_message_info(msg_id).unwrap();
        assert_eq!(info.failure_count, 0);
        assert_eq!(info.success_count, 1);
    }

    #[test]
    fn test_get_poisoned_messages() {
        let config = PoisonDetectorConfig::default();
        let mut detector = PoisonDetector::new(config);

        // Create some poisoned messages
        for i in 0..5 {
            let msg_id = format!("msg-{}", i);
            for _ in 0..5 {
                detector.record_failure(&msg_id, "Test error");
            }
        }

        let poisoned = detector.get_poisoned_messages();
        assert_eq!(poisoned.len(), 5);
    }

    #[test]
    fn test_failure_rate_calculation() {
        let config = PoisonDetectorConfig::default();
        let mut detector = PoisonDetector::new(config);

        let msg_id = "test-msg-3";

        // Test while failures exist
        detector.record_failure(msg_id, "Error");
        detector.record_failure(msg_id, "Error");

        let rate = detector.get_message_failure_rate(msg_id).unwrap();
        assert_eq!(rate, 1.0); // 2 failures, 0 successes = 100% failure

        // After success, failure count resets (message is healthy again)
        detector.record_success(msg_id);
        let rate = detector.get_message_failure_rate(msg_id).unwrap();
        assert_eq!(rate, 0.0); // Failure count reset to 0, 1 success = 0% failure
    }

    #[test]
    fn test_pattern_detection() {
        let config = PoisonDetectorConfig {
            min_samples: 3,
            failure_rate_threshold: 0.6,
            ..Default::default()
        };
        let mut detector = PoisonDetector::new(config);

        // Create pattern of "Connection timeout" errors
        for i in 0..5 {
            detector.record_failure(&format!("msg-{}", i), "Connection timeout");
        }

        assert!(detector.detect_failure_pattern("Connection timeout"));
        assert!(!detector.detect_failure_pattern("Unknown error"));
    }

    #[test]
    fn test_analytics() {
        let config = PoisonDetectorConfig::default();
        let mut detector = PoisonDetector::new(config);

        detector.record_failure("msg-1", "Error A");
        detector.record_failure("msg-2", "Error B");
        detector.record_success("msg-3");

        let analytics = detector.analytics();
        assert_eq!(analytics.total_tracked, 3);
        assert_eq!(analytics.total_failures, 2);
        assert_eq!(analytics.total_successes, 1);
        assert!(analytics.failure_rate > 0.6);
    }

    #[test]
    fn test_quarantine() {
        let config = PoisonDetectorConfig::default();
        let mut detector = PoisonDetector::new(config);

        let msg_id = "test-msg-4";
        detector.record_failure(msg_id, "Error");

        assert!(!detector.is_poison(msg_id));

        detector.quarantine_message(msg_id);
        assert!(detector.is_poison(msg_id));
    }

    #[test]
    fn test_remove_message() {
        let config = PoisonDetectorConfig::default();
        let mut detector = PoisonDetector::new(config);

        let msg_id = "test-msg-5";
        detector.record_failure(msg_id, "Error");

        assert!(detector.get_message_info(msg_id).is_some());

        let removed = detector.remove_message(msg_id);
        assert!(removed.is_some());
        assert!(detector.get_message_info(msg_id).is_none());
    }

    #[test]
    fn test_systemic_failure_detection() {
        let config = PoisonDetectorConfig {
            min_samples: 3,
            failure_rate_threshold: 0.8,
            ..Default::default()
        };
        let mut detector = PoisonDetector::new(config);

        // Create high failure rate
        for i in 0..10 {
            detector.record_failure(&format!("msg-{}", i), "System error");
        }

        assert!(detector.is_systemic_failure());
    }
}
